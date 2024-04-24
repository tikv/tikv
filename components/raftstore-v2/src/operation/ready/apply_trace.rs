// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! In raftstore v2, WAL is always disabled for tablet. So we need a way to
//! trace what have been persisted what haven't, and recover those missing
//! data when restart.
//!
//! In summary, we trace the persist progress by recording flushed event.
//! Because memtable is flushed one by one, so a flushed memtable must contain
//! all the data within the CF before certain apply index. So the minimun
//! flushed apply index + 1 of all data CFs is the recovery start point. In
//! some cases, a CF may not have any updates at all for a long time. In some
//! cases, we may still need to recover from smaller index even if flushed
//! index of all data CFs have advanced. So a special flushed index is
//! introduced and stored with raft CF (only using the name, raft CF is
//! dropped). It's the recommended recovery start point. How these two indexes
//! interact with each other can be found in the `ApplyTrace::recover` and
//! `ApplyTrace::maybe_advance_admin_flushed`.
//!
//! The correctness of raft cf index relies on the fact that:
//! - apply is sequential, so if any apply index is updated to apply trace, all
//!   modification events before that must be processed.
//! - admin commands that marked by raft cf index must flush all data before
//!   being executed. Note this contraint is not just for recovery, but also
//!   necessary to guarantee safety of operations like split init or log gc.
//! So data of logs before raft cf index must be applied and flushed to disk.
//!
//! All apply related states are associated with an apply index. During
//! recovery states corresponding to the start index should be used.

use std::{
    cmp,
    collections::VecDeque,
    path::Path,
    sync::{atomic::Ordering, mpsc::SyncSender, Mutex},
};

use encryption_export::DataKeyManager;
use engine_traits::{
    data_cf_offset, offset_to_cf, ApplyProgress, KvEngine, RaftEngine, RaftLogBatch,
    TabletRegistry, ALL_CFS, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE, DATA_CFS, DATA_CFS_LEN,
};
use fail::fail_point;
use kvproto::{
    metapb::Region,
    raft_serverpb::{PeerState, RaftApplyState, RaftLocalState, RegionLocalState},
};
use raftstore::store::{
    util, ReadTask, TabletSnapManager, WriteTask, RAFT_INIT_LOG_INDEX, RAFT_INIT_LOG_TERM,
};
use slog::{info, trace, warn, Logger};
use tikv_util::{box_err, slog_panic, worker::Scheduler};

use crate::{
    batch::StoreContext,
    operation::{
        command::temp_split_path,
        ready::snapshot::{install_tablet, recv_snap_path},
    },
    raft::{Peer, Storage},
    router::{PeerMsg, SstApplyIndex},
    worker::tablet,
    Result, StoreRouter,
};

/// Write states for the given region. The region is supposed to have all its
/// data persisted and not governed by any raft group before.
pub fn write_initial_states(wb: &mut impl RaftLogBatch, region: Region) -> Result<()> {
    let region_id = region.get_id();

    let mut state = RegionLocalState::default();
    state.set_region(region);
    state.set_tablet_index(RAFT_INIT_LOG_INDEX);
    wb.put_region_state(region_id, RAFT_INIT_LOG_INDEX, &state)?;

    let mut apply_state = RaftApplyState::default();
    apply_state.set_applied_index(RAFT_INIT_LOG_INDEX);
    apply_state
        .mut_truncated_state()
        .set_index(RAFT_INIT_LOG_INDEX);
    apply_state
        .mut_truncated_state()
        .set_term(RAFT_INIT_LOG_TERM);
    wb.put_apply_state(region_id, RAFT_INIT_LOG_INDEX, &apply_state)?;

    let mut raft_state = RaftLocalState::default();
    raft_state.set_last_index(RAFT_INIT_LOG_INDEX);
    raft_state.mut_hard_state().set_term(RAFT_INIT_LOG_TERM);
    raft_state.mut_hard_state().set_commit(RAFT_INIT_LOG_INDEX);
    wb.put_raft_state(region_id, &raft_state)?;

    for cf in ALL_CFS {
        wb.put_flushed_index(region_id, cf, RAFT_INIT_LOG_INDEX, RAFT_INIT_LOG_INDEX)?;
    }

    Ok(())
}

fn to_static_cf(cf: &str) -> &'static str {
    match cf {
        CF_DEFAULT => CF_DEFAULT,
        CF_RAFT => CF_RAFT,
        CF_WRITE => CF_WRITE,
        CF_LOCK => CF_LOCK,
        _ => unreachable!("unexpected cf: {cf}"),
    }
}

pub struct StateStorage<EK: KvEngine, ER: RaftEngine> {
    raft_engine: ER,
    router: Mutex<StoreRouter<EK, ER>>,
}

impl<EK: KvEngine, ER: RaftEngine> StateStorage<EK, ER> {
    pub fn new(raft_engine: ER, router: StoreRouter<EK, ER>) -> Self {
        Self {
            raft_engine,
            router: Mutex::new(router),
        }
    }
}

impl<EK: KvEngine, ER: RaftEngine> engine_traits::StateStorage for StateStorage<EK, ER> {
    fn persist_progress(&self, region_id: u64, tablet_index: u64, pr: ApplyProgress) {
        let cf = to_static_cf(pr.cf());
        let flushed_index = pr.applied_index();
        self.raft_engine
            .persist_progress(region_id, tablet_index, pr);
        let _ = self.router.lock().unwrap().send(
            region_id,
            PeerMsg::DataFlushed {
                cf,
                tablet_index,
                flushed_index,
            },
        );
    }
}

/// Mapping from data cf to an u64 index.
pub type DataTrace = [u64; DATA_CFS_LEN];

#[derive(Clone, Default, Debug)]
struct Progress {
    flushed: u64,
    /// The index of last entry that has modification to the CF. The value
    /// can be larger than the index that actually modifies the CF in apply.
    ///
    /// If `flushed` == `last_modified`, then all data in the CF is persisted.
    last_modified: u64,
    // applied indexes ranges that represent sst is ingested but not flushed indexes.
    pending_sst_ranges: VecDeque<IndexRange>,
}

// A range representing [start, end], upper bound inclusive for handling
// convenience.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct IndexRange(u64, u64);

#[derive(Debug)]
// track the global flushed index related to the write task.
struct ReadyFlushedIndex {
    ready_number: u64,
    flushed_index: u64,
}

/// `ApplyTrace` is used to track the indexes of modifications and flushes.
///
/// It has 3 core functionalities:
/// - recover from stopped state and figure out the correct log replay start
///   point.
/// - trace the admin flushed index and issue persistence once admin operation
///   is considered finished. Note only those admin commands that needs to
///   interact with other peers will be traced.
/// - support query the flushed progress without actually scanning raft engine,
///   which is useful for cleaning up stale flush records.
#[derive(Default, Debug)]
pub struct ApplyTrace {
    /// The modified indexes and flushed index of each data CF.
    data_cfs: Box<[Progress; DATA_CFS_LEN]>,
    /// The modified indexes and flushed index of raft CF.
    ///
    /// raft CF is a virtual CF that only used for recording apply index of
    /// certain admin commands (like split/merge). So there is no flush at all.
    /// The `flushed` field is advanced when the admin command doesn't need to
    /// be replayed after restart. A write should be triggered to persist the
    /// record.
    admin: Progress,
    /// Index that is issued to be written. It may not be truely persisted.
    persisted_applied: u64,
    /// Flush will be triggered explicitly when there are too many pending
    /// writes. It marks the last index that is flushed to avoid too many
    /// flushes.
    last_flush_trigger: u64,
    /// `true` means the raft cf record should be persisted in next ready.
    try_persist: bool,
    // Because we persist the global flushed in the write task, so we should track
    // the task and handle sst cleanup after the write task finished.
    flushed_index_queue: VecDeque<ReadyFlushedIndex>,
}

impl ApplyTrace {
    fn recover(region_id: u64, engine: &impl RaftEngine) -> Result<(Self, RegionLocalState)> {
        let mut trace = ApplyTrace::default();
        // Get all the recorded apply index from data CFs.
        for (off, cf) in DATA_CFS.iter().enumerate() {
            // There should be at least one record.
            let i = engine.get_flushed_index(region_id, cf)?.unwrap();
            trace.data_cfs[off].flushed = i;
            trace.data_cfs[off].last_modified = i;
        }
        let i = engine.get_flushed_index(region_id, CF_RAFT)?.unwrap();
        // Index of raft CF means all data before that must be persisted.
        trace.admin.flushed = i;
        trace.admin.last_modified = i;
        trace.persisted_applied = i;
        trace.last_flush_trigger = i;
        let applied_region_state = match engine.get_region_state(region_id, trace.admin.flushed)? {
            Some(s) => s,
            None => panic!(
                "failed to get region state [region_id={}] [apply_trace={:?}]",
                region_id, trace
            ),
        };
        Ok((trace, applied_region_state))
    }

    fn on_flush(&mut self, cf: &str, index: u64) {
        let off = data_cf_offset(cf);
        // Technically it should always be true.
        if index > self.data_cfs[off].flushed {
            self.data_cfs[off].flushed = index;
        }
    }

    fn on_modify(&mut self, cf: &str, index: u64) {
        let off = data_cf_offset(cf);
        self.data_cfs[off].last_modified = index;
    }

    pub fn on_admin_flush(&mut self, index: u64) {
        if index > self.admin.flushed {
            self.admin.flushed = index;
            self.try_persist = true;
        }
    }

    pub fn on_admin_modify(&mut self, index: u64) {
        self.admin.last_modified = index;
    }

    pub fn on_sst_ingested(&mut self, sst_applied_index: &[SstApplyIndex]) {
        use std::cmp::Ordering;
        for &SstApplyIndex { cf_index, index } in sst_applied_index {
            let p = &mut self.data_cfs[cf_index];
            if p.flushed < index {
                let max_idx = p.pending_sst_ranges.iter().last().map(|r| r.1).unwrap_or(0) + 1;
                match max_idx.cmp(&index) {
                    Ordering::Less => {
                        p.pending_sst_ranges.push_back(IndexRange(index, index));
                    }
                    Ordering::Equal => {
                        p.pending_sst_ranges.iter_mut().last().unwrap().1 = index;
                    }
                    _ => {}
                }
            }
        }
    }

    pub fn persisted_apply_index(&self) -> u64 {
        self.persisted_applied
    }

    pub fn should_flush(&mut self) -> bool {
        if self.admin.flushed < self.admin.last_modified {
            // It's waiting for other peers, flush will not help.
            return false;
        }
        let last_modified = self
            .data_cfs
            .iter()
            .filter_map(|pr| {
                if pr.last_modified != pr.flushed {
                    Some(pr.last_modified)
                } else {
                    None
                }
            })
            .max();
        if let Some(m) = last_modified
            && m >= self.admin.flushed + 4096000
            && m >= self.last_flush_trigger + 4096000
        {
            self.last_flush_trigger = m;
            true
        } else {
            false
        }
    }

    // All events before `mem_index` must be consumed before calling this function.
    fn maybe_advance_admin_flushed(&mut self, mem_index: u64) {
        if self.admin.flushed < self.admin.last_modified {
            return;
        }
        let min_flushed = self
            .data_cfs
            .iter_mut()
            // Only unflushed CFs are considered. Flushed CF always have uptodate changes
            // persisted.
            .filter_map(|pr| {
                // All modifications before mem_index must be seen. If following condition is
                // true, it means the modification comes beyond general apply process (like
                // transaction GC unsafe write). Align `last_modified` to `flushed` to avoid
                // blocking raft log GC.
                if mem_index >= pr.flushed && pr.flushed > pr.last_modified {
                    pr.last_modified = pr.flushed;
                }
                if pr.last_modified != pr.flushed {
                    Some(pr.flushed)
                } else {
                    None
                }
            })
            .min();

        // At best effort, we can only advance the index to `mem_index`.
        let candidate = cmp::min(mem_index, min_flushed.unwrap_or(u64::MAX));
        // try advance the index if there are any sst ingestion next to the flushed
        // index, and always trigger a flush if there is any sst ingestion.
        let (candidate, has_ingested_sst) = self.advance_flushed_index_for_ingest(candidate);
        if candidate > self.admin.flushed {
            self.admin.flushed = candidate;
            if has_ingested_sst || (self.admin.flushed > self.persisted_applied + 100) {
                self.try_persist = true;
            }
        }
        // TODO: persist admin.flushed every 10 minutes.
    }

    fn advance_flushed_index_for_ingest(&mut self, mut max_index: u64) -> (u64, bool) {
        let mut has_ingest = false;
        loop {
            let mut has_change = false;
            for p in self.data_cfs.iter_mut() {
                while let Some(r) = p.pending_sst_ranges.front_mut() {
                    if r.0 > max_index + 1 {
                        break;
                    } else if r.1 > max_index {
                        max_index = r.1;
                        has_change = true;
                    }
                    p.pending_sst_ranges.pop_front();
                    has_ingest = true;
                }
            }
            if !has_change {
                break;
            }
        }

        (max_index, has_ingest)
    }

    /// Get the flushed indexes of all data CF that is needed when recoverying
    /// logs.
    ///
    /// Logs may be replayed from the persisted apply index, but those data may
    /// have been flushed in the past, so we need the flushed indexes to decide
    /// what logs can be skipped for certain CFs. If all CFs are flushed before
    /// the persisted apply index, then there is nothing to skipped, so
    /// `None` is returned.
    #[inline]
    pub fn log_recovery(&self) -> Option<Box<DataTrace>> {
        let flushed_indexes = self.flushed_indexes();
        for i in flushed_indexes {
            if i > self.admin.flushed {
                return Some(Box::new(flushed_indexes));
            }
        }
        None
    }

    /// Get the flushed indexes of all data CF that is needed when recoverying
    /// logs. It does not check the admin cf.
    pub fn flushed_indexes(&self) -> DataTrace {
        let mut flushed_indexes = [0; DATA_CFS_LEN];
        for (off, pr) in self.data_cfs.iter().enumerate() {
            flushed_indexes[off] = pr.flushed;
        }
        flushed_indexes
    }

    pub fn restore_snapshot(&mut self, index: u64) {
        for pr in self.data_cfs.iter_mut() {
            pr.last_modified = index;
        }
        self.admin.last_modified = index;
        // Snapshot is a special case that KVs are not flushed yet, so all flushed
        // state should not be changed. But persisted_applied is updated whenever an
        // asynchronous write is triggered. So it can lead to a special case that
        // persisted_applied < admin.flushed. It seems no harm ATM though.
        self.persisted_applied = index;
        self.try_persist = false;
    }

    pub fn on_applied_snapshot(&mut self, index: u64) {
        for pr in self.data_cfs.iter_mut() {
            pr.flushed = index;
        }
        self.admin.flushed = index;
    }

    #[inline]
    pub fn should_persist(&self) -> bool {
        fail_point!("should_persist_apply_trace", |_| true);
        self.try_persist
    }

    #[inline]
    pub fn register_flush_task(&mut self, ready_number: u64, flushed_index: u64) {
        assert!(
            self.flushed_index_queue
                .iter()
                .last()
                .map(|f| f.ready_number)
                .unwrap_or(0)
                < ready_number
        );
        self.flushed_index_queue.push_back(ReadyFlushedIndex {
            ready_number,
            flushed_index,
        });
    }

    #[inline]
    pub fn take_flush_index(&mut self, ready_number: u64) -> Option<u64> {
        use std::cmp::Ordering;
        while let Some(r) = self.flushed_index_queue.pop_front() {
            match r.ready_number.cmp(&ready_number) {
                Ordering::Equal => return Some(r.flushed_index),
                Ordering::Greater => {
                    self.flushed_index_queue.push_front(r);
                    break;
                }
                _ => {}
            }
        }
        None
    }
}

impl<EK: KvEngine, ER: RaftEngine> Storage<EK, ER> {
    /// Creates a new storage with uninit states.
    ///
    /// This should only be used for creating new peer from raft message.
    pub fn uninit(
        store_id: u64,
        region: Region,
        engine: ER,
        read_scheduler: Scheduler<ReadTask<EK>>,
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
            read_scheduler,
            false,
            ApplyTrace::default(),
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
        read_scheduler: Scheduler<ReadTask<EK>>,
        logger: &Logger,
    ) -> Result<Option<Storage<EK, ER>>> {
        // Check latest region state to determine whether the peer is destroyed.
        let region_state = match engine.get_region_state(region_id, u64::MAX) {
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

        let (trace, region_state) = ApplyTrace::recover(region_id, &engine)?;
        info!(
            logger,
            "initial apply trace";
            "apply_trace" => ?trace,
            "region_id" => region_id,
        );

        let raft_state = match engine.get_raft_state(region_id) {
            Ok(Some(s)) => s,
            res => {
                return Err(box_err!("failed to get raft state: {:?}", res));
            }
        };

        let applied_index = trace.persisted_apply_index();
        let mut apply_state = match engine.get_apply_state(region_id, applied_index) {
            Ok(Some(s)) => s,
            res => {
                return Err(box_err!("failed to get apply state: {:?}", res));
            }
        };
        apply_state.set_applied_index(applied_index);
        (|| {
            // Make node reply from start.
            fail_point!("RESET_APPLY_INDEX_WHEN_RESTART", |_| {
                apply_state.set_applied_index(5);
            });
        })();

        Self::create(
            store_id,
            region_state,
            raft_state,
            apply_state,
            engine,
            read_scheduler,
            true,
            trace,
            logger,
        )
        .map(Some)
    }

    /// Region state is written before actually moving data. It's possible that
    /// the tablet is missing after restart. We need to move the data again
    /// after being restarted.
    pub fn recover_tablet(
        &self,
        registry: &TabletRegistry<EK>,
        key_manager: Option<&DataKeyManager>,
        snap_mgr: &TabletSnapManager,
    ) {
        let tablet_index = self.region_state().get_tablet_index();
        if tablet_index == 0 {
            // It's an uninitialized peer, nothing to recover.
            return;
        }
        let region_id = self.region().get_id();
        let target_path = registry.tablet_path(region_id, tablet_index);
        if target_path.exists() {
            // Move data succeeded before restart, nothing to recover.
            return;
        }
        if tablet_index == RAFT_INIT_LOG_INDEX {
            // Its data may come from split or snapshot. Try split first.
            let split_path = temp_split_path(registry, region_id);
            if install_tablet(registry, key_manager, &split_path, region_id, tablet_index) {
                return;
            }
        }
        let truncated_index = self.entry_storage().truncated_index();
        if truncated_index == tablet_index {
            // Try snapshot.
            let peer_id = self.peer().get_id();
            let snap_path = recv_snap_path(
                snap_mgr,
                region_id,
                peer_id,
                self.entry_storage().truncated_term(),
                tablet_index,
            );
            if install_tablet(registry, key_manager, &snap_path, region_id, tablet_index) {
                return;
            }
        }
        slog_panic!(
            self.logger(),
            "tablet loss detected";
            "tablet_index" => tablet_index
        );
    }

    /// Write initial persist trace for uninit peer.
    pub fn init_apply_trace(&self, write_task: &mut WriteTask<EK, ER>) {
        let region_id = self.region().get_id();
        let raft_engine = self.entry_storage().raft_engine();
        let lb = write_task
            .extra_write
            .ensure_v2(|| raft_engine.log_batch(3));
        lb.put_apply_state(region_id, 0, self.apply_state())
            .unwrap();
        lb.put_region_state(region_id, 0, self.region_state())
            .unwrap();
        for cf in ALL_CFS {
            lb.put_flushed_index(region_id, cf, 0, 0).unwrap();
        }
        write_task.flushed_epoch =
            Some(self.region_state().get_region().get_region_epoch().clone());
    }

    pub fn record_apply_trace(&mut self, write_task: &mut WriteTask<EK, ER>) {
        let trace = self.apply_trace();
        // Maybe tablet index can be different?
        if trace.persisted_applied > trace.admin.flushed {
            return;
        }
        let region_id = self.region().get_id();
        let raft_engine = self.entry_storage().raft_engine();
        // must use the persistent epoch to avoid epoch rollback, the restart
        // logic can see ApplyTrace::recover. self.epoch is not reliable because
        // it maybe too newest, so the epoch maybe rollback after the node restarted.
        let epoch = raft_engine
            .get_region_state(region_id, trace.admin.flushed)
            .unwrap()
            .unwrap()
            .get_region()
            .get_region_epoch()
            .clone();
        if util::is_epoch_stale(self.flushed_epoch(), &epoch) {
            write_task.flushed_epoch = Some(epoch);
        }

        let tablet_index = self.tablet_index();
        let lb = write_task
            .extra_write
            .ensure_v2(|| raft_engine.log_batch(1));
        info!(self.logger(), "persisting admin flushed"; "tablet_index" => tablet_index, "flushed" => trace.admin.flushed);
        let trace = self.apply_trace_mut();
        lb.put_flushed_index(region_id, CF_RAFT, tablet_index, trace.admin.flushed)
            .unwrap();
        trace.try_persist = false;
        trace.persisted_applied = trace.admin.flushed;
        trace.register_flush_task(write_task.ready_number(), trace.admin.flushed);
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub fn on_data_flushed<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        cf: &str,
        tablet_index: u64,
        index: u64,
    ) {
        trace!(self.logger, "data flushed"; "cf" => cf, "tablet_index" => tablet_index, "index" => index, "trace" => ?self.storage().apply_trace());
        if tablet_index < self.storage().tablet_index() {
            // Stale tablet.
            return;
        }
        let apply_index = self.storage().entry_storage().applied_index();
        let apply_trace = self.storage_mut().apply_trace_mut();
        apply_trace.on_flush(cf, index);
        apply_trace.maybe_advance_admin_flushed(apply_index);
        self.cleanup_stale_ssts(ctx, &[cf], index, apply_index);
    }

    pub fn on_data_modified(&mut self, modification: DataTrace) {
        trace!(self.logger, "on data modified"; "modification" => ?modification, "trace" => ?self.storage().apply_trace());
        let apply_index = self.storage().entry_storage().applied_index();
        let apply_trace = self.storage_mut().apply_trace_mut();
        for (cf, index) in DATA_CFS.iter().zip(modification) {
            if index != 0 {
                apply_trace.on_modify(cf, index);
            }
        }
        apply_trace.maybe_advance_admin_flushed(apply_index);
    }

    pub fn cleanup_stale_ssts<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        cfs: &[&str],
        index: u64,
        apply_index: u64,
    ) {
        let mut stale_ssts = vec![];
        for cf in cfs {
            let ssts = self.sst_apply_state().stale_ssts(cf, index);
            if !ssts.is_empty() {
                info!(
                    self.logger,
                    "schedule delete stale ssts after flush";
                    "stale_ssts" => ?stale_ssts,
                    "apply_index" => apply_index,
                    "cf" => cf,
                    "flushed_index" => index,
                );
                stale_ssts.extend(ssts);
            }
        }
        if !stale_ssts.is_empty() {
            _ = ctx
                .schedulers
                .tablet
                .schedule(tablet::Task::CleanupImportSst(
                    stale_ssts.into_boxed_slice(),
                ));
        }
    }

    pub fn flush_before_close<T>(&mut self, ctx: &StoreContext<EK, ER, T>, tx: SyncSender<()>) {
        info!(
            self.logger,
            "region flush before close begin";
        );
        let region_id = self.region_id();
        let flush_threshold: u64 = (|| {
            fail_point!("flush_before_close_threshold", |t| {
                t.unwrap().parse::<u64>().unwrap()
            });
            50
        })();

        if let Some(tablet) = self.tablet().cloned() {
            let applied_index = self.storage().entry_storage().applied_index();

            let mut tried_count: usize = 0;
            let mut flushed = false;
            // flush the oldest cf one by one until we are under the replay count threshold
            loop {
                let replay_count = self.storage().estimate_replay_count();
                if replay_count < flush_threshold || tried_count == 3 {
                    // Ideally, the replay count should be 0 after three flush_oldest_cf. If not,
                    // there may exist bug, but it's not desireable to block here, so we at most try
                    // three times.
                    if replay_count >= flush_threshold && tried_count == 3 {
                        warn!(
                            self.logger,
                            "after three flush_oldest_cf, the expected replay count still exceeds the threshold";
                            "replay_count" => replay_count,
                            "threshold" => flush_threshold,
                        );
                    }
                    if flushed {
                        let admin_flush = self.storage_mut().apply_trace_mut().admin.flushed;
                        let (_, _, tablet_index) = ctx
                            .tablet_registry
                            .parse_tablet_name(Path::new(tablet.path()))
                            .unwrap();
                        let mut lb = ctx.engine.log_batch(1);
                        lb.put_flushed_index(region_id, CF_RAFT, tablet_index, admin_flush)
                            .unwrap();
                        ctx.engine.consume(&mut lb, true).unwrap();
                        info!(
                            self.logger,
                            "flush before close flush admin for region";
                            "admin_flush" => admin_flush,
                        );
                    }
                    break;
                }

                info!(
                    self.logger,
                    "flush-before-close: replay count exceeds threshold, pick the oldest cf to flush";
                    "count" => replay_count,
                    "tried" => tried_count,
                );
                tried_count += 1;
                tablet.flush_oldest_cf(true, None).unwrap();
                flushed = true;

                let flush_state = self.flush_state().clone();
                let mut apply_trace = self.storage_mut().apply_trace_mut();

                let flushed_indexes = flush_state.as_ref().flushed_index();
                for i in 0..flushed_indexes.len() {
                    let flush_index = flushed_indexes[i].load(Ordering::SeqCst);
                    let cf = offset_to_cf(i);
                    apply_trace.on_flush(cf, flush_index);
                }

                // We should use applied_index rather than flushed_index here. Memtable flush
                // may be earlier than `on_apply_res` which means flushed_index can be larger
                // than applied_index, and using flush_index can cause data loss which is
                // described on the comment of `test_flush_index_exceed_last_modified`.
                apply_trace.maybe_advance_admin_flushed(applied_index);
                apply_trace.persisted_applied = apply_trace.admin.flushed;
            }
        }

        info!(
            self.logger,
            "region flush before close done";
        );
        let _ = tx.send(());
    }
}

#[cfg(test)]
mod tests {
    use engine_traits::{CfName, RaftEngineReadOnly};
    use kvproto::metapb::Peer;
    use tempfile::TempDir;

    use super::*;

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

        let local_state = raft_engine.get_region_state(4, u64::MAX).unwrap().unwrap();
        assert_eq!(local_state.get_state(), PeerState::Normal);
        assert_eq!(*local_state.get_region(), region);
        assert_eq!(local_state.get_tablet_index(), RAFT_INIT_LOG_INDEX);
        assert_eq!(
            local_state,
            raft_engine
                .get_region_state(4, RAFT_INIT_LOG_INDEX)
                .unwrap()
                .unwrap()
        );
        assert_eq!(
            None,
            raft_engine
                .get_region_state(4, RAFT_INIT_LOG_INDEX - 1)
                .unwrap()
        );

        let raft_state = raft_engine.get_raft_state(4).unwrap().unwrap();
        assert_eq!(raft_state.get_last_index(), RAFT_INIT_LOG_INDEX);
        let hs = raft_state.get_hard_state();
        assert_eq!(hs.get_term(), RAFT_INIT_LOG_TERM);
        assert_eq!(hs.get_commit(), RAFT_INIT_LOG_INDEX);

        let apply_state = raft_engine.get_apply_state(4, u64::MAX).unwrap().unwrap();
        assert_eq!(apply_state.get_applied_index(), RAFT_INIT_LOG_INDEX);
        let ts = apply_state.get_truncated_state();
        assert_eq!(ts.get_index(), RAFT_INIT_LOG_INDEX);
        assert_eq!(ts.get_term(), RAFT_INIT_LOG_TERM);
        assert_eq!(
            apply_state,
            raft_engine
                .get_apply_state(4, RAFT_INIT_LOG_INDEX)
                .unwrap()
                .unwrap()
        );
        assert_eq!(
            None,
            raft_engine
                .get_apply_state(4, RAFT_INIT_LOG_INDEX - 1)
                .unwrap()
        );
    }

    #[test]
    fn test_apply_trace() {
        let mut trace = ApplyTrace::default();
        assert_eq!(0, trace.admin.flushed);
        // If there is no modifications, index should be advanced anyway.
        trace.maybe_advance_admin_flushed(2);
        assert_eq!(2, trace.admin.flushed);
        for cf in DATA_CFS {
            trace.on_modify(cf, 3);
        }
        trace.maybe_advance_admin_flushed(3);
        // Modification is not flushed.
        assert_eq!(2, trace.admin.flushed);
        for cf in DATA_CFS {
            trace.on_flush(cf, 3);
        }
        trace.maybe_advance_admin_flushed(3);
        // No admin is recorded, index should be advanced.
        assert_eq!(3, trace.admin.flushed);
        trace.on_admin_modify(4);
        for cf in DATA_CFS {
            trace.on_flush(cf, 4);
        }
        for cf in DATA_CFS {
            trace.on_modify(cf, 4);
        }
        trace.maybe_advance_admin_flushed(4);
        // Unflushed admin modification should hold index.
        assert_eq!(3, trace.admin.flushed);
        trace.on_admin_flush(4);
        trace.maybe_advance_admin_flushed(4);
        // Admin is flushed, index should be advanced.
        assert_eq!(4, trace.admin.flushed);
        for cf in DATA_CFS {
            trace.on_flush(cf, 5);
        }
        trace.maybe_advance_admin_flushed(4);
        // Though all data CFs are flushed, but index should not be
        // advanced as we don't know whether there is admin modification.
        assert_eq!(4, trace.admin.flushed);
        for cf in DATA_CFS {
            trace.on_modify(cf, 5);
        }
        trace.maybe_advance_admin_flushed(5);
        // Because modify is recorded, so we know there should be no admin
        // modification and index can be advanced.
        assert_eq!(5, trace.admin.flushed);

        fn range_equals(trace: &ApplyTrace, cf: &str, expected: Vec<IndexRange>) {
            let pending_ranges = &trace.data_cfs[data_cf_offset(cf)].pending_sst_ranges;
            assert_eq!(
                pending_ranges.len(),
                expected.len(),
                "actual: {:?}, expected: {:?}",
                pending_ranges,
                &expected
            );
            pending_ranges
                .iter()
                .zip(expected.iter())
                .for_each(|(r, e)| {
                    assert_eq!(r, e);
                });
        }

        trace.on_modify(CF_DEFAULT, 8);
        let ingested_ssts_idx =
            make_sst_apply_index(vec![(CF_DEFAULT, 6), (CF_WRITE, 6), (CF_WRITE, 7)]);
        trace.on_sst_ingested(&ingested_ssts_idx);
        range_equals(&trace, CF_DEFAULT, vec![IndexRange(6, 6)]);
        range_equals(&trace, CF_WRITE, vec![IndexRange(6, 7)]);
        trace.maybe_advance_admin_flushed(8);
        assert_eq!(7, trace.admin.flushed);
        for cf in [CF_DEFAULT, CF_WRITE] {
            assert_eq!(
                trace.data_cfs[data_cf_offset(cf)].pending_sst_ranges.len(),
                0
            );
        }
        trace.on_modify(CF_DEFAULT, 10);
        let ingested_ssts_idx = make_sst_apply_index(vec![(CF_DEFAULT, 10)]);
        trace.on_sst_ingested(&ingested_ssts_idx);
        trace.on_flush(CF_DEFAULT, 8);
        trace.maybe_advance_admin_flushed(10);
        assert_eq!(8, trace.admin.flushed);
        range_equals(&trace, CF_DEFAULT, vec![IndexRange(10, 10)]);

        trace.on_modify(CF_DEFAULT, 16);
        let ingested_ssts_idx = make_sst_apply_index(vec![
            (CF_DEFAULT, 11),
            (CF_WRITE, 12),
            (CF_LOCK, 13),
            (CF_DEFAULT, 14),
            (CF_WRITE, 14),
            (CF_WRITE, 15),
            (CF_LOCK, 16),
        ]);
        trace.on_sst_ingested(&ingested_ssts_idx);
        range_equals(
            &trace,
            CF_DEFAULT,
            vec![IndexRange(10, 11), IndexRange(14, 14)],
        );
        range_equals(
            &trace,
            CF_WRITE,
            vec![IndexRange(12, 12), IndexRange(14, 15)],
        );
        range_equals(
            &trace,
            CF_LOCK,
            vec![IndexRange(13, 13), IndexRange(16, 16)],
        );
        trace.maybe_advance_admin_flushed(16);
        assert_eq!(8, trace.admin.flushed);

        trace.on_flush(CF_DEFAULT, 9);
        trace.maybe_advance_admin_flushed(16);
        assert_eq!(16, trace.admin.flushed);
        for cf in DATA_CFS {
            assert_eq!(
                trace.data_cfs[data_cf_offset(cf)].pending_sst_ranges.len(),
                0
            );
        }
    }

    fn make_sst_apply_index(data: Vec<(CfName, u64)>) -> Vec<SstApplyIndex> {
        data.into_iter()
            .map(|d| SstApplyIndex {
                cf_index: data_cf_offset(d.0),
                index: d.1,
            })
            .collect()
    }

    #[test]
    fn test_advance_admin_flushed() {
        let cases = &[
            // When all are flushed, admin index should be advanced to latest.
            ([(2, 2), (3, 3), (5, 5)], (3, 3), 5, 5),
            ([(2, 2), (3, 3), (5, 5)], (5, 3), 6, 6),
            // Any unflushed result should block advancing.
            ([(2, 3), (3, 3), (5, 5)], (2, 2), 5, 2),
            ([(2, 4), (3, 4), (5, 6)], (2, 2), 6, 2),
            // But it should not make index go back.
            ([(2, 4), (3, 4), (5, 6)], (3, 3), 6, 3),
            // Unflush admin should not be advanced.
            ([(2, 2), (3, 3), (5, 5)], (2, 3), 5, 2),
            // Flushed may race with modification.
            ([(2, 2), (3, 3), (6, 5)], (2, 2), 5, 5),
            ([(8, 2), (9, 3), (7, 5)], (4, 4), 5, 5),
            ([(8, 2), (9, 3), (7, 5)], (5, 5), 5, 5),
            ([(2, 3), (9, 3), (7, 5)], (2, 2), 5, 2),
            // In special cae, some CF may be flushed without any modification recorded,
            // we should still able to advance the apply index forward.
            ([(5, 2), (9, 3), (7, 3)], (2, 2), 3, 3),
            ([(5, 2), (9, 3), (7, 3)], (2, 2), 6, 6),
            ([(5, 2), (9, 3), (7, 3)], (2, 2), 10, 10),
            ([(5, 2), (9, 3), (7, 3)], (2, 3), 10, 2),
        ];
        for (case, (data_cfs, admin, mem_index, exp)) in cases.iter().enumerate() {
            let mut trace = ApplyTrace::default();
            for (i, (flushed, modified)) in data_cfs.iter().enumerate() {
                trace.data_cfs[i].flushed = *flushed;
                trace.data_cfs[i].last_modified = *modified;
            }
            trace.admin.flushed = admin.0;
            trace.admin.last_modified = admin.1;
            trace.maybe_advance_admin_flushed(*mem_index);
            assert_eq!(trace.admin.flushed, *exp, "{case}");
        }
    }
}
