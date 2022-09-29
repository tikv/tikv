// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::RefCell,
    fmt::{self, Debug, Formatter},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc::{self, Receiver, TryRecvError},
        Arc,
    },
};

use engine_traits::{RaftEngine, RaftLogBatch};
use kvproto::{
    metapb::{self, Region},
    raft_serverpb::{
        PeerState, RaftApplyState, RaftLocalState, RaftSnapshotData, RegionLocalState,
    },
};
use protobuf::Message;
use raft::{
    eraftpb::{ConfState, Entry, HardState, Snapshot},
    GetEntriesContext, RaftState, INVALID_ID,
};
use raftstore::store::{
    metrics::STORE_SNAPSHOT_VALIDATION_FAILURE_COUNTER, util, EntryStorage, RaftlogFetchTask,
    SnapState, RAFT_INIT_LOG_INDEX, RAFT_INIT_LOG_TERM,
};
use slog::{error, info, o, warn, Logger};
use tikv_util::{box_err, store::find_peer, worker::Scheduler};

use crate::{router::GenSnapTask, worker::RegionTask, Result};

const MAX_SNAP_TRY_CNT: usize = 5;

pub fn write_initial_states(wb: &mut impl RaftLogBatch, region: Region) -> Result<()> {
    let region_id = region.get_id();

    let mut state = RegionLocalState::default();
    state.set_region(region);
    state.set_tablet_index(RAFT_INIT_LOG_INDEX);
    wb.put_region_state(region_id, &state)?;

    let mut apply_state = RaftApplyState::default();
    apply_state.set_applied_index(RAFT_INIT_LOG_INDEX);
    apply_state
        .mut_truncated_state()
        .set_index(RAFT_INIT_LOG_INDEX);
    apply_state
        .mut_truncated_state()
        .set_term(RAFT_INIT_LOG_TERM);
    wb.put_apply_state(region_id, &apply_state)?;

    let mut raft_state = RaftLocalState::default();
    raft_state.set_last_index(RAFT_INIT_LOG_INDEX);
    raft_state.mut_hard_state().set_term(RAFT_INIT_LOG_TERM);
    raft_state.mut_hard_state().set_commit(RAFT_INIT_LOG_INDEX);
    wb.put_raft_state(region_id, &raft_state)?;

    Ok(())
}

/// A storage for raft.
///
/// It's similar to `PeerStorage` in v1.
pub struct Storage<ER> {
    entry_storage: EntryStorage<ER>,
    peer: metapb::Peer,
    region_state: RegionLocalState,
    /// Whether states has been persisted before. If a peer is just created by
    /// by messages, it has not persisted any states, we need to persist them
    /// at least once dispite whether the state changes since create.
    ever_persisted: bool,
    logger: Logger,

    /// Snapshot state
    snap_state: RefCell<SnapState>,
    gen_snap_task: RefCell<Option<GenSnapTask>>,
    snap_tried_cnt: RefCell<usize>,
}

impl<ER> Debug for Storage<ER> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Storage of [region {}] {}",
            self.region().get_id(),
            self.peer.get_id()
        )
    }
}

impl<ER> Storage<ER> {
    #[inline]
    pub fn entry_storage(&self) -> &EntryStorage<ER> {
        &self.entry_storage
    }

    #[inline]
    pub fn entry_storage_mut(&mut self) -> &mut EntryStorage<ER> {
        &mut self.entry_storage
    }

    #[inline]
    pub fn region_state(&self) -> &RegionLocalState {
        &self.region_state
    }

    #[inline]
    pub fn region(&self) -> &metapb::Region {
        self.region_state.get_region()
    }

    #[inline]
    pub fn peer(&self) -> &metapb::Peer {
        &self.peer
    }

    #[inline]
    pub fn logger(&self) -> &Logger {
        &self.logger
    }
}

impl<ER: RaftEngine> Storage<ER> {
    /// Creates a new storage with uninit states.
    ///
    /// This should only be used for creating new peer from raft message.
    pub fn uninit(
        store_id: u64,
        region: Region,
        engine: ER,
        log_fetch_scheduler: Scheduler<RaftlogFetchTask>,
        logger: &Logger,
    ) -> Result<Self> {
        let mut region_state = RegionLocalState::default();
        region_state.set_region(region);
        Self::create(
            store_id,
            region_state,
            RaftLocalState::default(),
            RaftApplyState::default(),
            engine,
            log_fetch_scheduler,
            false,
            logger,
        )
    }

    /// Creates a new storage.
    ///
    /// All metadata should be initialized before calling this method. If the
    /// region is destroyed, `None` will be returned.
    pub fn new(
        region_id: u64,
        store_id: u64,
        engine: ER,
        log_fetch_scheduler: Scheduler<RaftlogFetchTask>,
        logger: &Logger,
    ) -> Result<Option<Storage<ER>>> {
        let region_state = match engine.get_region_state(region_id) {
            Ok(Some(s)) => s,
            res => {
                return Err(box_err!(
                    "failed to get region state for region {}: {:?}",
                    region_id,
                    res
                ));
            }
        };

        if region_state.get_state() == PeerState::Tombstone {
            return Ok(None);
        }

        let raft_state = match engine.get_raft_state(region_id) {
            Ok(Some(s)) => s,
            res => {
                return Err(box_err!("failed to get raft state: {:?}", res));
            }
        };

        let apply_state = match engine.get_apply_state(region_id) {
            Ok(Some(s)) => s,
            res => {
                return Err(box_err!("failed to get apply state: {:?}", res));
            }
        };

        Self::create(
            store_id,
            region_state,
            raft_state,
            apply_state,
            engine,
            log_fetch_scheduler,
            true,
            logger,
        )
        .map(Some)
    }

    fn create(
        store_id: u64,
        region_state: RegionLocalState,
        raft_state: RaftLocalState,
        apply_state: RaftApplyState,
        engine: ER,
        log_fetch_scheduler: Scheduler<RaftlogFetchTask>,
        persisted: bool,
        logger: &Logger,
    ) -> Result<Self> {
        let peer = find_peer(region_state.get_region(), store_id);
        let peer = match peer {
            Some(p) if p.get_id() != INVALID_ID => p,
            _ => {
                return Err(box_err!("no valid peer found in {:?}", region_state));
            }
        };
        let region = region_state.get_region();
        let logger = logger.new(o!("region_id" => region.id, "peer_id" => peer.get_id()));
        let entry_storage = EntryStorage::new(
            peer.get_id(),
            engine,
            raft_state,
            apply_state,
            region,
            log_fetch_scheduler,
        )?;

        Ok(Storage {
            entry_storage,
            peer: peer.clone(),
            region_state,
            ever_persisted: persisted,
            logger,
            snap_state: RefCell::new(SnapState::Relax),
            gen_snap_task: RefCell::new(None),
            snap_tried_cnt: RefCell::new(0),
        })
    }

    #[inline]
    pub fn raft_state(&self) -> &RaftLocalState {
        self.entry_storage.raft_state()
    }

    #[inline]
    pub fn apply_state(&self) -> &RaftApplyState {
        self.entry_storage.apply_state()
    }

    #[inline]
    pub fn is_initialized(&self) -> bool {
        self.region_state.get_tablet_index() != 0
    }

    pub fn ever_persisted(&self) -> bool {
        self.ever_persisted
    }

    pub fn set_ever_persisted(&mut self) {
        self.ever_persisted = true;
    }

    pub fn take_gen_snap_task(&mut self) -> Option<GenSnapTask> {
        self.gen_snap_task.get_mut().take()
    }

    fn get_tablet_index(&self) -> u64 {
        match self.region_state.get_state() {
            PeerState::Tombstone | PeerState::Applying => 0,
            _ => self.region_state.get_tablet_index(),
        }
    }

    fn validate_snap(&self, snap: &Snapshot, request_index: u64) -> bool {
        let idx = snap.get_metadata().get_index();
        // TODO(nolouch): check tuncated index
        if idx < request_index {
            // stale snapshot, should generate again.
            info!(
                self.logger(),
                "snapshot is stale, generate again";
                "region_id" => self.region_state.get_region().get_id(),
                "peer_id" => self.peer.get_id(),
                "snap_index" => idx,
                "request_index" => request_index,
            );
            STORE_SNAPSHOT_VALIDATION_FAILURE_COUNTER.stale.inc();
            return false;
        }

        let mut snap_data = RaftSnapshotData::default();
        if let Err(e) = snap_data.merge_from_bytes(snap.get_data()) {
            error!(
                self.logger(),
                "failed to decode snapshot, it may be corrupted";
                "region_id" => self.region_state.get_region().get_id(),
                "peer_id" => self.peer.get_id(),
                "err" => ?e,
            );
            STORE_SNAPSHOT_VALIDATION_FAILURE_COUNTER.decode.inc();
            return false;
        }
        let snap_epoch = snap_data.get_region().get_region_epoch();
        let latest_epoch = self.region().get_region_epoch();
        if snap_epoch.get_conf_ver() < latest_epoch.get_conf_ver() {
            info!(
                self.logger(),
                "snapshot epoch is stale";
                "region_id" => self.region_state.get_region().get_id(),
                "peer_id" => self.peer.get_id(),
                "snap_epoch" => ?snap_epoch,
                "latest_epoch" => ?latest_epoch,
            );
            STORE_SNAPSHOT_VALIDATION_FAILURE_COUNTER.epoch.inc();
            return false;
        }

        true
    }

    /// Gets a snapshot. Returns `SnapshotTemporarilyUnavailable` if there is no
    /// unavailable snapshot.
    pub fn snapshot(&self, request_index: u64, to: u64) -> raft::Result<Snapshot> {
        let mut snap_state = self.snap_state.borrow_mut();
        let mut tried_cnt = self.snap_tried_cnt.borrow_mut();

        let mut tried = false;
        let mut last_canceled = false;
        if let SnapState::Generating {
            ref canceled,
            ref receiver,
            ..
        } = *snap_state
        {
            tried = true;
            last_canceled = canceled.load(Ordering::SeqCst);
            match receiver.try_recv() {
                Err(TryRecvError::Empty) => {
                    return Err(raft::Error::Store(
                        raft::StorageError::SnapshotTemporarilyUnavailable,
                    ));
                }
                Ok(s) if !last_canceled => {
                    *snap_state = SnapState::Relax;
                    *tried_cnt = 0;
                    if self.validate_snap(&s, request_index) {
                        return Ok(s);
                    }
                }
                Err(TryRecvError::Disconnected) | Ok(_) => {
                    *snap_state = SnapState::Relax;
                    warn!(
                        self.logger(),
                        "failed to try generating snapshot";
                        "region_id" => self.region().get_id(),
                        "peer_id" => self.peer().get_id(),
                        "times" => *tried_cnt,
                        "request_peer" => to,
                    );
                }
            }
        }

        if SnapState::Relax != *snap_state {
            panic!(
                "[region {}] unexpected state: {:?}",
                self.region().get_id(),
                *snap_state
            );
        }

        if *tried_cnt >= MAX_SNAP_TRY_CNT {
            let cnt = *tried_cnt;
            *tried_cnt = 0;
            return Err(raft::Error::Store(box_err!(
                "failed to get snapshot after {} times",
                cnt
            )));
        }
        if !tried || !last_canceled {
            *tried_cnt += 1;
        }

        info!(
            self.logger(),
            "requesting snapshot";
            "region_id" => self.region().get_id(),
            "peer_id" => self.peer().get_id(),
            "tablet_index" => self.get_tablet_index(),
            "request_index" => request_index,
            "request_peer" => to,
        );

        let (sender, receiver) = mpsc::sync_channel(1);
        // TODO(nolouch): remove cancel?
        let canceled = Arc::new(AtomicBool::new(false));
        let index = Arc::new(AtomicU64::new(0));
        *snap_state = SnapState::Generating {
            canceled: canceled.clone(),
            index,
            receiver,
        };

        let store_id = self
            .region()
            .get_peers()
            .iter()
            .find(|p| p.id == to)
            .map(|p| p.store_id)
            .unwrap_or(0);

        let task = GenSnapTask::new(
            self.region().get_id(),
            self.get_tablet_index(),
            canceled,
            sender,
        );

        let mut gen_snap_task = self.gen_snap_task.borrow_mut();
        assert!(gen_snap_task.is_none());
        *gen_snap_task = Some(task);
        Err(raft::Error::Store(
            raft::StorageError::SnapshotTemporarilyUnavailable,
        ))
    }
}

impl<ER: RaftEngine> raft::Storage for Storage<ER> {
    fn initial_state(&self) -> raft::Result<RaftState> {
        let hard_state = self.raft_state().get_hard_state().clone();
        // We will persist hard state no matter if it's initialized or not in
        // v2, So hard state may not be empty. But when it becomes initialized,
        // commit must be changed.
        assert_eq!(
            hard_state.commit == 0,
            !self.is_initialized(),
            "region state doesn't match raft state {:?} vs {:?}",
            self.region_state(),
            self.raft_state()
        );

        if hard_state.commit == 0 {
            // If it's uninitialized, return empty state as we consider every
            // states are empty at the very beginning.
            return Ok(RaftState::new(hard_state, ConfState::default()));
        }
        Ok(RaftState::new(
            hard_state,
            util::conf_state_from_region(self.region()),
        ))
    }

    #[inline]
    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        context: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        self.entry_storage
            .entries(low, high, max_size.into().unwrap_or(u64::MAX), context)
    }

    #[inline]
    fn term(&self, idx: u64) -> raft::Result<u64> {
        self.entry_storage.term(idx)
    }

    #[inline]
    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.entry_storage.first_index())
    }

    #[inline]
    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.entry_storage.last_index())
    }

    fn snapshot(&self, request_index: u64, to: u64) -> raft::Result<Snapshot> {
        self.snapshot(request_index, to)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use engine_traits::{RaftEngine, RaftEngineReadOnly, RaftLogBatch};
    use kvproto::{
        metapb::{Peer, Region},
        raft_serverpb::PeerState,
    };
    use raft::{eraftpb::Snapshot as RaftSnapshot, Error as RaftError, StorageError};
    use raftstore::store::{RAFT_INIT_LOG_INDEX, RAFT_INIT_LOG_TERM};
    use slog::o;
    use tempfile::TempDir;
    use tikv_util::worker::{dummy_scheduler, Runnable, Worker};

    use super::*;

    struct MockRegionRunner {}
    impl Runnable for MockRegionRunner {
        type Task = RegionTask;
        fn run(&mut self, task: RegionTask) {
            match task {
                RegionTask::Gen { notifier, .. } => {
                    notifier.send(RaftSnapshot::default()).unwrap();
                }
                _ => unreachable!(),
            }
        }
    }

    fn new_region() -> Region {
        let mut region = Region::default();
        region.set_id(4);
        let mut p = Peer::default();
        p.set_id(5);
        p.set_store_id(6);
        region.mut_peers().push(p);
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(4);
        region
    }

    #[test]

    fn test_write_initial_states() {
        let region = new_region();
        let path = TempDir::new().unwrap();
        let engine = engine_test::new_temp_engine(&path);
        let raft_engine = &engine.raft;
        let mut wb = raft_engine.log_batch(10);
        write_initial_states(&mut wb, region.clone()).unwrap();
        assert!(!wb.is_empty());
        raft_engine.consume(&mut wb, true).unwrap();

        let local_state = raft_engine.get_region_state(4).unwrap().unwrap();
        assert_eq!(local_state.get_state(), PeerState::Normal);
        assert_eq!(*local_state.get_region(), region);
        assert_eq!(local_state.get_tablet_index(), RAFT_INIT_LOG_INDEX);

        let raft_state = raft_engine.get_raft_state(4).unwrap().unwrap();
        assert_eq!(raft_state.get_last_index(), RAFT_INIT_LOG_INDEX);
        let hs = raft_state.get_hard_state();
        assert_eq!(hs.get_term(), RAFT_INIT_LOG_TERM);
        assert_eq!(hs.get_commit(), RAFT_INIT_LOG_INDEX);

        let apply_state = raft_engine.get_apply_state(4).unwrap().unwrap();
        assert_eq!(apply_state.get_applied_index(), RAFT_INIT_LOG_INDEX);
        let ts = apply_state.get_truncated_state();
        assert_eq!(ts.get_index(), RAFT_INIT_LOG_INDEX);
        assert_eq!(ts.get_term(), RAFT_INIT_LOG_TERM);
    }

    #[test]
    fn test_storage_create_snapshot_with_mock_runner() {
        let region = new_region();
        let path = TempDir::new().unwrap();
        let raft_engine =
            engine_test::raft::new_engine(&format!("{}", path.path().join("raft").display()), None)
                .unwrap();
        let mut wb = raft_engine.log_batch(10);
        write_initial_states(&mut wb, region.clone()).unwrap();
        assert!(!wb.is_empty());
        raft_engine.consume(&mut wb, true).unwrap();

        let (dummy_scheduler, _) = dummy_scheduler();
        let mut s = Storage::uninit(
            6,
            region,
            raft_engine,
            dummy_scheduler,
            &slog_global::borrow_global().new(o!()),
        )
        .unwrap();
        let snap = s.snapshot(0, 0);
        let unavailable = RaftError::Store(StorageError::SnapshotTemporarilyUnavailable);
        assert_eq!(snap.unwrap_err(), unavailable);
        assert_eq!(*s.snap_tried_cnt.borrow(), 1);
        let mut worker = Worker::new("region-worker").lazy_build("region-worker");
        let sched = worker.scheduler();
        worker.start(MockRegionRunner {});
        let gen_task = s.gen_snap_task.borrow_mut().take().unwrap();
        gen_task.generate_and_schedule_snapshot(&sched);
        let snap = match *s.snap_state.borrow() {
            SnapState::Generating { ref receiver, .. } => {
                receiver.recv_timeout(Duration::from_secs(3)).unwrap()
            }
            ref s => panic!("unexpected state: {:?}", s),
        };
        assert_eq!(snap.get_metadata().get_index(), 0);
        assert_eq!(snap.get_metadata().get_term(), 0);
        assert!(snap.get_data().is_empty());
    }
}
