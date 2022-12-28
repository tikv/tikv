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

use std::{cmp, sync::Mutex};

use engine_traits::{
    FlushProgress, KvEngine, RaftEngine, RaftLogBatch, TabletRegistry, ALL_CFS, CF_DEFAULT,
    CF_LOCK, CF_RAFT, CF_WRITE, DATA_CFS, DATA_CFS_LEN,
};
use kvproto::{
    metapb::Region,
    raft_serverpb::{PeerState, RaftApplyState, RaftLocalState, RegionLocalState},
};
use raftstore::store::{
    ReadTask, TabletSnapManager, WriteTask, RAFT_INIT_LOG_INDEX, RAFT_INIT_LOG_TERM,
};
use slog::Logger;
use tikv_util::{box_err, worker::Scheduler};

use crate::{
    operation::{
        command::temp_split_path,
        ready::snapshot::{install_tablet, recv_snap_path},
    },
    raft::{Peer, Storage},
    router::PeerMsg,
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
    fn persist_progress(&self, region_id: u64, tablet_index: u64, pr: FlushProgress) {
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

#[derive(Clone, Copy, Default)]
struct Progress {
    flushed: u64,
    /// The index of last entry that has modification to the CF.
    ///
    /// If `flushed` == `last_modified`, then all data in the CF is persisted.
    last_modified: u64,
}

pub fn cf_offset(cf: &str) -> usize {
    let cf = if cf.is_empty() { CF_DEFAULT } else { cf };
    DATA_CFS.iter().position(|c| *c == cf).expect(cf)
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
#[derive(Default)]
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
        let applied_region_state = engine
            .get_region_state(region_id, trace.admin.flushed)?
            .unwrap();
        Ok((trace, applied_region_state))
    }

    fn on_flush(&mut self, cf: &str, index: u64) {
        let off = cf_offset(cf);
        // Technically it should always be true.
        if index > self.data_cfs[off].flushed {
            self.data_cfs[off].flushed = index;
        }
    }

    fn on_modify(&mut self, cf: &str, index: u64) {
        let off = cf_offset(cf);
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

    pub fn persisted_apply_index(&self) -> u64 {
        self.admin.flushed
    }

    pub fn should_flush(&mut self) -> bool {
        if self.admin.flushed != self.admin.last_modified {
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
        if let Some(m) = last_modified && m >= self.admin.flushed + 4096 && m >= self.last_flush_trigger + 4096 {
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
            .iter()
            // Only unflushed CFs are considered. Flushed CF always have uptodate changes
            // persisted.
            .filter_map(|pr| {
                if pr.last_modified != pr.flushed {
                    Some(pr.flushed)
                } else {
                    None
                }
            })
            .min();
        // At best effort, we can only advance the index to `mem_index`.
        let candidate = cmp::min(mem_index, min_flushed.unwrap_or(u64::MAX));
        if candidate > self.admin.flushed {
            self.admin.flushed = candidate;
            if self.admin.flushed > self.persisted_applied + 100 {
                self.try_persist = true;
            }
        }
        // TODO: persist admin.flushed every 10 minutes.
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
        let mut flushed_indexes = [0; DATA_CFS_LEN];
        for (off, pr) in self.data_cfs.iter().enumerate() {
            flushed_indexes[off] = pr.flushed;
        }
        for i in flushed_indexes {
            if i > self.admin.flushed {
                return Some(Box::new(flushed_indexes));
            }
        }
        None
    }

    pub fn reset_snapshot(&mut self, index: u64) {
        for pr in self.data_cfs.iter_mut() {
            pr.flushed = index;
            pr.last_modified = index;
        }
        self.admin.flushed = index;
        self.persisted_applied = index;
        self.try_persist = false;
    }

    #[inline]
    pub fn reset_should_persist(&mut self) {
        self.try_persist = false;
    }

    #[inline]
    pub fn should_persist(&self) -> bool {
        self.try_persist
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
    pub fn recover_tablet(&self, registry: &TabletRegistry<EK>, snap_mgr: &TabletSnapManager) {
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
            if install_tablet(registry, &split_path, region_id, tablet_index) {
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
            if install_tablet(registry, &snap_path, region_id, tablet_index) {
                return;
            }
        }
        panic!(
            "{:?} data loss detected: {}_{} not found",
            self.logger().list(),
            region_id,
            tablet_index
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
    }

    pub fn record_apply_trace(&mut self, write_task: &mut WriteTask<EK, ER>) {
        let region_id = self.region().get_id();
        let raft_engine = self.entry_storage().raft_engine();
        let tablet_index = self.tablet_index();
        let lb = write_task
            .extra_write
            .ensure_v2(|| raft_engine.log_batch(1));
        let trace = self.apply_trace_mut();
        lb.put_flushed_index(region_id, CF_RAFT, tablet_index, trace.admin.flushed)
            .unwrap();
        trace.try_persist = false;
        trace.persisted_applied = trace.admin.flushed;
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub fn on_data_flushed(&mut self, cf: &str, tablet_index: u64, index: u64) {
        if tablet_index < self.storage().tablet_index() {
            // Stale tablet.
            return;
        }
        let apply_index = self.storage().entry_storage().applied_index();
        let apply_trace = self.storage_mut().apply_trace_mut();
        apply_trace.on_flush(cf, index);
        apply_trace.maybe_advance_admin_flushed(apply_index);
    }

    pub fn on_data_modified(&mut self, modification: DataTrace) {
        let apply_index = self.storage().entry_storage().applied_index();
        let apply_trace = self.storage_mut().apply_trace_mut();
        for (cf, index) in DATA_CFS.iter().zip(modification) {
            if index != 0 {
                apply_trace.on_modify(cf, index);
            }
        }
        apply_trace.maybe_advance_admin_flushed(apply_index);
    }
}

#[cfg(test)]
mod tests {
    use engine_traits::RaftEngineReadOnly;
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
        assert_eq!(0, trace.persisted_apply_index());
        // If there is no modifications, index should be advanced anyway.
        trace.maybe_advance_admin_flushed(2);
        assert_eq!(2, trace.persisted_apply_index());
        for cf in DATA_CFS {
            trace.on_modify(cf, 3);
        }
        trace.maybe_advance_admin_flushed(3);
        // Modification is not flushed.
        assert_eq!(2, trace.persisted_apply_index());
        for cf in DATA_CFS {
            trace.on_flush(cf, 3);
        }
        trace.maybe_advance_admin_flushed(3);
        // No admin is recorded, index should be advanced.
        assert_eq!(3, trace.persisted_apply_index());
        trace.on_admin_modify(4);
        for cf in DATA_CFS {
            trace.on_flush(cf, 4);
        }
        for cf in DATA_CFS {
            trace.on_modify(cf, 4);
        }
        trace.maybe_advance_admin_flushed(4);
        // Unflushed admin modification should hold index.
        assert_eq!(3, trace.persisted_apply_index());
        trace.on_admin_flush(4);
        trace.maybe_advance_admin_flushed(4);
        // Admin is flushed, index should be advanced.
        assert_eq!(4, trace.persisted_apply_index());
        for cf in DATA_CFS {
            trace.on_flush(cf, 5);
        }
        trace.maybe_advance_admin_flushed(4);
        // Though all data CFs are flushed, but index should not be
        // advanced as we don't know whether there is admin modification.
        assert_eq!(4, trace.persisted_apply_index());
        for cf in DATA_CFS {
            trace.on_modify(cf, 5);
        }
        trace.maybe_advance_admin_flushed(5);
        // Because modify is recorded, so we know there should be no admin
        // modification and index can be advanced.
        assert_eq!(5, trace.persisted_apply_index());
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
