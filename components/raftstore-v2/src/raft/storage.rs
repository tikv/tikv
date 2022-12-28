// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::{RefCell, RefMut},
    fmt::{self, Debug, Formatter},
};

use collections::HashMap;
use engine_traits::{KvEngine, RaftEngine};
use kvproto::{
    metapb,
    raft_serverpb::{PeerState, RaftApplyState, RaftLocalState, RegionLocalState},
};
use raft::{
    eraftpb::{ConfState, Entry, Snapshot},
    GetEntriesContext, RaftState, INVALID_ID,
};
use raftstore::store::{util, EntryStorage, ReadTask};
use slog::{o, Logger};
use tikv_util::{box_err, store::find_peer, worker::Scheduler};

use crate::{
    operation::{ApplyTrace, GenSnapTask, SnapState, SplitInit},
    Result,
};

/// A storage for raft.
///
/// It's similar to `PeerStorage` in v1.
pub struct Storage<EK: KvEngine, ER> {
    entry_storage: EntryStorage<EK, ER>,
    peer: metapb::Peer,
    region_state: RegionLocalState,
    /// Whether states has been persisted before. If a peer is just created by
    /// by messages, it has not persisted any states, we need to persist them
    /// at least once dispite whether the state changes since create.
    ever_persisted: bool,
    logger: Logger,

    /// Snapshot part.
    pub snap_states: RefCell<HashMap<u64, SnapState>>,
    pub gen_snap_task: RefCell<Box<Option<GenSnapTask>>>,
    split_init: Option<Box<SplitInit>>,
    /// The flushed index of all CFs.
    apply_trace: ApplyTrace,
}

impl<EK: KvEngine, ER> Debug for Storage<EK, ER> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Storage of [region {}] {}",
            self.region().get_id(),
            self.peer.get_id()
        )
    }
}

impl<EK: KvEngine, ER> Storage<EK, ER> {
    #[inline]
    pub fn entry_storage(&self) -> &EntryStorage<EK, ER> {
        &self.entry_storage
    }

    #[inline]
    pub fn entry_storage_mut(&mut self) -> &mut EntryStorage<EK, ER> {
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

    #[inline]
    pub fn gen_snap_task_mut(&self) -> RefMut<'_, Box<Option<GenSnapTask>>> {
        self.gen_snap_task.borrow_mut()
    }

    #[inline]
    pub fn cancel_snap_task(&self, to_peer_id: Option<u64>) {
        if to_peer_id.is_none() {
            self.gen_snap_task.borrow_mut().take();
            return;
        }
        let to = to_peer_id.unwrap();
        let mut task = self.gen_snap_task.borrow_mut();
        if let Some(t) = &**task {
            if to == t.to_peer() {
                *task = Box::new(None);
            };
        }
    }

    #[inline]
    pub fn apply_trace_mut(&mut self) -> &mut ApplyTrace {
        &mut self.apply_trace
    }

    #[inline]
    pub fn apply_trace(&self) -> &ApplyTrace {
        &self.apply_trace
    }
}

impl<EK: KvEngine, ER: RaftEngine> Storage<EK, ER> {
    pub(crate) fn create(
        store_id: u64,
        region_state: RegionLocalState,
        raft_state: RaftLocalState,
        apply_state: RaftApplyState,
        engine: ER,
        read_scheduler: Scheduler<ReadTask<EK>>,
        persisted: bool,
        apply_trace: ApplyTrace,
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
            read_scheduler,
        )?;

        Ok(Storage {
            entry_storage,
            peer: peer.clone(),
            region_state,
            ever_persisted: persisted,
            logger,
            snap_states: RefCell::new(HashMap::default()),
            gen_snap_task: RefCell::new(Box::new(None)),
            split_init: None,
            apply_trace,
        })
    }

    #[inline]
    pub fn region_state_mut(&mut self) -> &mut RegionLocalState {
        &mut self.region_state
    }

    #[inline]
    pub fn split_init_mut(&mut self) -> &mut Option<Box<SplitInit>> {
        &mut self.split_init
    }

    #[inline]
    pub fn raft_state(&self) -> &RaftLocalState {
        self.entry_storage.raft_state()
    }

    #[inline]
    pub fn read_scheduler(&self) -> Scheduler<ReadTask<EK>> {
        self.entry_storage.read_scheduler()
    }

    #[inline]
    pub fn apply_state(&self) -> &RaftApplyState {
        self.entry_storage.apply_state()
    }

    /// Check if the storage is initialized.
    ///
    /// The storage is considered initialized when data is applied in memory.
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

    #[inline]
    pub fn take_gen_snap_task(&mut self) -> Option<GenSnapTask> {
        self.gen_snap_task.get_mut().take()
    }

    #[inline]
    pub fn tablet_index(&self) -> u64 {
        match self.region_state.get_state() {
            PeerState::Tombstone | PeerState::Applying => 0,
            _ => self.region_state.get_tablet_index(),
        }
    }

    #[inline]
    pub fn set_region_state(&mut self, state: RegionLocalState) {
        self.region_state = state;
        for peer in self.region_state.get_region().get_peers() {
            if peer.get_id() == self.peer.get_id() {
                self.peer = peer.clone();
                break;
            }
        }
    }
}

impl<EK: KvEngine, ER: RaftEngine> raft::Storage for Storage<EK, ER> {
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
    use std::{
        sync::{
            mpsc::{sync_channel, Receiver, SyncSender},
            Arc,
        },
        time::Duration,
    };

    use engine_test::{
        ctor::{CfOptions, DbOptions},
        kv::TestTabletFactory,
    };
    use engine_traits::{RaftEngine, RaftLogBatch, TabletContext, TabletRegistry, DATA_CFS};
    use kvproto::{
        metapb::{Peer, Region},
        raft_serverpb::PeerState,
    };
    use raft::{Error as RaftError, StorageError};
    use raftstore::store::{
        util::new_empty_snapshot, AsyncReadNotifier, FetchedLogs, GenSnapRes, ReadRunner,
        TabletSnapKey, TabletSnapManager, WriteTask,
    };
    use slog::o;
    use tempfile::TempDir;
    use tikv_util::worker::Worker;

    use super::*;
    use crate::{
        fsm::ApplyResReporter, operation::write_initial_states, raft::Apply, router::ApplyRes,
    };

    #[derive(Clone)]
    pub struct TestRouter {
        ch: SyncSender<GenSnapRes>,
    }

    impl TestRouter {
        pub fn new() -> (Self, Receiver<GenSnapRes>) {
            let (tx, rx) = sync_channel(1);
            (Self { ch: tx }, rx)
        }
    }

    impl AsyncReadNotifier for TestRouter {
        fn notify_logs_fetched(&self, _region_id: u64, _fetched_logs: FetchedLogs) {
            unreachable!();
        }

        fn notify_snapshot_generated(&self, _region_id: u64, res: GenSnapRes) {
            self.ch.send(res).unwrap();
        }
    }

    impl ApplyResReporter for TestRouter {
        fn report(&self, _res: ApplyRes) {}
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
    fn test_apply_snapshot() {
        let region = new_region();
        let path = TempDir::new().unwrap();
        let mgr = TabletSnapManager::new(path.path().join("snap_dir").to_str().unwrap()).unwrap();
        let raft_engine =
            engine_test::raft::new_engine(&format!("{}", path.path().join("raft").display()), None)
                .unwrap();
        let mut wb = raft_engine.log_batch(10);
        write_initial_states(&mut wb, region.clone()).unwrap();
        assert!(!wb.is_empty());
        raft_engine.consume(&mut wb, true).unwrap();
        // building a tablet factory
        let ops = DbOptions::default();
        let cf_opts = DATA_CFS.iter().map(|cf| (*cf, CfOptions::new())).collect();
        let factory = Box::new(TestTabletFactory::new(ops, cf_opts));
        let reg = TabletRegistry::new(factory, path.path().join("tablets")).unwrap();
        let worker = Worker::new("test-read-worker").lazy_build("test-read-worker");
        let sched = worker.scheduler();
        let logger = slog_global::borrow_global().new(o!());
        let mut s = Storage::new(4, 6, raft_engine.clone(), sched, &logger.clone())
            .unwrap()
            .unwrap();

        let snapshot = new_empty_snapshot(region.clone(), 10, 1, false);
        let mut task = WriteTask::new(region.get_id(), 5, 0);
        s.apply_snapshot(&snapshot, &mut task, mgr, reg).unwrap();

        // It can be set before load tablet.
        assert_eq!(PeerState::Normal, s.region_state().get_state());
        assert_eq!(10, s.entry_storage().truncated_index());
        assert_eq!(1, s.entry_storage().truncated_term());
        assert_eq!(1, s.entry_storage().last_term());
        assert_eq!(10, s.entry_storage().raft_state().last_index);
        // This index can't be set before load tablet.
        assert_ne!(10, s.entry_storage().applied_index());
        assert_ne!(1, s.entry_storage().applied_term());
        assert_eq!(10, s.region_state().get_tablet_index());
        assert!(!task.persisted_cbs.is_empty());

        s.on_applied_snapshot();
        assert_eq!(10, s.entry_storage().applied_index());
        assert_eq!(1, s.entry_storage().applied_term());
        assert_eq!(10, s.region_state().get_tablet_index());
    }

    #[test]
    fn test_storage_create_snapshot() {
        let region = new_region();
        let path = TempDir::new().unwrap();
        let raft_engine =
            engine_test::raft::new_engine(&format!("{}", path.path().join("raft").display()), None)
                .unwrap();
        let mut wb = raft_engine.log_batch(10);
        write_initial_states(&mut wb, region.clone()).unwrap();
        assert!(!wb.is_empty());
        raft_engine.consume(&mut wb, true).unwrap();
        let mgr = TabletSnapManager::new(path.path().join("snap_dir").to_str().unwrap()).unwrap();
        // building a tablet factory
        let ops = DbOptions::default();
        let cf_opts = DATA_CFS.iter().map(|cf| (*cf, CfOptions::new())).collect();
        let factory = Box::new(TestTabletFactory::new(ops, cf_opts));
        let reg = TabletRegistry::new(factory, path.path().join("tablets")).unwrap();
        let tablet_ctx = TabletContext::new(&region, Some(10));
        reg.load(tablet_ctx, true).unwrap();
        // setup read runner worker and peer storage
        let mut worker = Worker::new("test-read-worker").lazy_build("test-read-worker");
        let sched = worker.scheduler();
        let logger = slog_global::borrow_global().new(o!());
        let s = Storage::new(4, 6, raft_engine.clone(), sched.clone(), &logger.clone())
            .unwrap()
            .unwrap();
        let (router, rx) = TestRouter::new();
        let mut read_runner = ReadRunner::new(router.clone(), raft_engine);
        read_runner.set_snap_mgr(mgr.clone());
        worker.start(read_runner);
        let mut state = RegionLocalState::default();
        state.set_region(region.clone());
        // setup peer applyer
        let mut apply = Apply::new(
            region.get_peers()[0].clone(),
            state,
            router,
            reg,
            sched,
            Arc::default(),
            None,
            5,
            5,
            logger,
        );

        // Test get snapshot
        let to_peer_id = 7;
        let snap = s.snapshot(0, to_peer_id);
        let unavailable = RaftError::Store(StorageError::SnapshotTemporarilyUnavailable);
        assert_eq!(snap.unwrap_err(), unavailable);
        let gen_task = s.gen_snap_task.borrow_mut().take().unwrap();
        apply.schedule_gen_snapshot(gen_task);
        let res = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        s.on_snapshot_generated(res);
        assert_eq!(s.snapshot(0, 8).unwrap_err(), unavailable);
        assert!(s.snap_states.borrow().get(&8).is_some());
        let snap = match *s.snap_states.borrow().get(&to_peer_id).unwrap() {
            SnapState::Generated(ref snap) => *snap.clone(),
            ref s => panic!("unexpected state: {:?}", s),
        };
        assert_eq!(snap.get_metadata().get_index(), 5);
        assert_eq!(snap.get_metadata().get_term(), 5);
        assert_eq!(snap.get_data().is_empty(), false);
        let snap_key = TabletSnapKey::from_region_snap(4, 7, &snap);
        let checkpointer_path = mgr.tablet_gen_path(&snap_key);
        assert!(checkpointer_path.exists());
        s.snapshot(0, to_peer_id).unwrap();

        // Test cancel snapshot
        let snap = s.snapshot(0, 7);
        assert_eq!(snap.unwrap_err(), unavailable);
        let gen_task = s.gen_snap_task.borrow_mut().take().unwrap();
        apply.schedule_gen_snapshot(gen_task);
        let _res = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        s.cancel_generating_snap(None);
        assert!(s.snap_states.borrow().get(&to_peer_id).is_none());

        // Test get twice snapshot and cancel once.
        // get snapshot a
        let snap = s.snapshot(0, 0);
        assert_eq!(snap.unwrap_err(), unavailable);
        let gen_task_a = s.gen_snap_task.borrow_mut().take().unwrap();
        apply.set_apply_progress(1, 5);
        apply.schedule_gen_snapshot(gen_task_a);
        let res = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        s.cancel_generating_snap(None);
        // cancel get snapshot a, try get snaphsot b
        let snap = s.snapshot(0, 0);
        assert_eq!(snap.unwrap_err(), unavailable);
        let gen_task_b = s.gen_snap_task.borrow_mut().take().unwrap();
        apply.set_apply_progress(10, 5);
        apply.schedule_gen_snapshot(gen_task_b);
        // on snapshot a and b
        assert_eq!(s.on_snapshot_generated(res), false);
        let res = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(s.on_snapshot_generated(res), true);
    }
}
