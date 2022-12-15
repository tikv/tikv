// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! In raftstore v2, WAL is always disabled for tablet. So we need a way to
//! trace what have been persisted what haven't, and recover those missing
//! data when restart.
//!
//! In summary, we trace the persist progress by recording flushed event.
//! Because memtable is flushed one by one, so a flushed memtable must contain
//! all the data within the CF before some certain apply index. So the minimun
//! flushed apply index + 1 of all data CFs is the recovery start point. In
//! some cases, a CF may not have any updates at all for a long time. In some
//! cases, we may still need to recover from smaller index even if flushed
//! index of all data CFs have advanced. So a special flushed index is
//! introduced and stored with raft CF (only using the name, raft CF is
//! dropped). It's the recommended recovery start point. How these two indexes
//! interact with each other can be found in the `figure_applied_index`.
//!
//! All apply related states are associated with an apply index. During
//! recovery states corresponding to the start index should be used.

use std::cmp;

use engine_traits::{
    KvEngine, RaftEngine, RaftLogBatch, ALL_CFS, CF_DEFAULT, CF_RAFT, DATA_CFS, DATA_CFS_LEN,
};
use kvproto::{
    metapb::Region,
    raft_cmdpb::RaftCmdResponse,
    raft_serverpb::{PeerState, RaftApplyState, RaftLocalState, RegionLocalState},
};
use raftstore::store::{cmd_resp, ReadTask, WriteTask, RAFT_INIT_LOG_INDEX, RAFT_INIT_LOG_TERM};
use slog::Logger;
use tikv_util::{box_err, worker::Scheduler};

use crate::{
    fsm::ApplyResReporter,
    raft::{Apply, Peer, Storage},
    router::{ApplyTask, CmdResChannel},
    Result,
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

/// An alias of frequent use type that each data cf has a u64.
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

#[derive(Default)]
pub struct ApplyTrace {
    data_cfs: Box<[Progress; DATA_CFS_LEN]>,
    admin: Progress,
    // Index that is issued to be written. It may not be truely persisted.
    persisted_applied: u64,
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
        let applied_region_state = engine
            .get_region_state(region_id, trace.admin.flushed)?
            .unwrap();
        let data_index = trace.data_index();
        // If index is not larger than applied_index, it means some CF doesn't have any
        // data to flush.
        if trace.admin.flushed < data_index {
            let region_state = engine.get_region_state(region_id, data_index)?.unwrap();
            // If there is no admin change, it means `applied_index` is notadvanced in
            // time. Otherwise, it may be waiting for flush of other tablets in cases like
            // split.
            if applied_region_state
                .get_region()
                .get_region_epoch()
                .get_version()
                == region_state.get_region().get_region_epoch().get_version()
            {
                trace.admin.flushed = data_index;
                trace.admin.last_modified = data_index;
            }
        }
        Ok((trace, applied_region_state))
    }

    #[inline]
    fn data_index(&self) -> u64 {
        self.data_cfs.iter().map(|p| p.flushed).min().unwrap()
    }

    fn record_flush(&mut self, cf: &str, index: u64) {
        let off = cf_offset(cf);
        // Technically it should always be true.
        if index > self.data_cfs[off].flushed {
            self.data_cfs[off].flushed = index;
        }
    }

    fn record_modify(&mut self, cf: &str, index: u64) {
        let off = cf_offset(cf);
        self.data_cfs[off].last_modified = index;
    }

    pub fn record_admin_flush(&mut self, index: u64) {
        if index > self.admin.flushed {
            self.admin.flushed = index;
            self.try_persist = true;
        }
    }

    pub fn record_admin_modify(&mut self, index: u64) {
        self.admin.last_modified = index;
    }

    fn persisted_apply_index(&self) -> u64 {
        self.admin.flushed
    }

    fn maybe_advance_admin_flushed(&mut self, mem_index: u64) {
        if self.admin.flushed < self.admin.last_modified {
            return;
        }
        let mut min_index = u64::MAX;
        let mut max_modified = 0;
        let mut all_flushed = true;
        for pr in self.data_cfs.iter() {
            if pr.flushed < min_index {
                min_index = pr.flushed;
            }
            // Flush may race with recording modified, using the min index to make
            // sure admin pr should also record all modification.
            if pr.last_modified < min_index {
                min_index = pr.last_modified;
            }
            if pr.last_modified >= max_modified {
                max_modified = pr.last_modified;
            }
            all_flushed &= pr.flushed == pr.last_modified;
        }
        if min_index > self.admin.flushed {
            if max_modified > self.admin.flushed {
                // It means all modification must be received and there is no admin
                // modification.
                self.admin.flushed = cmp::min(max_modified, min_index);
            }
        } else if all_flushed {
            // So all are flushed and no blocking admin result, we can advance the
            // apply index up to memory.
            self.admin.flushed = mem_index;
        }
        // TODO: persist admin.flushed every 10 minutes.
    }

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
        apply_trace.record_flush(cf, index);
        apply_trace.maybe_advance_admin_flushed(apply_index);
    }

    pub fn record_data_trace(&mut self, modification: DataTrace) {
        let apply_index = self.storage().entry_storage().applied_index();
        let apply_trace = self.storage_mut().apply_trace_mut();
        for (cf, index) in DATA_CFS.iter().zip(modification) {
            if index != 0 {
                apply_trace.record_modify(cf, index);
            }
        }
        apply_trace.maybe_advance_admin_flushed(apply_index);
    }

    pub fn on_manual_flush(&mut self, cfs: Vec<&'static str>, ch: CmdResChannel) {
        if let Some(sched) = self.apply_scheduler() {
            sched.send(ApplyTask::ManualFlush { cfs, ch });
        }
    }
}

impl<EK: KvEngine, R: ApplyResReporter> Apply<EK, R> {
    pub fn on_manual_flush(&mut self, cfs: Vec<&'static str>, ch: CmdResChannel) {
        self.flush();
        // TODO: make it async
        let res = match self.tablet().flush_cfs(&cfs, true) {
            Ok(()) => RaftCmdResponse::default(),
            Err(e) => cmd_resp::new_error(e.into()),
        };
        ch.set_result(res);
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
            trace.record_modify(cf, 3);
        }
        trace.maybe_advance_admin_flushed(3);
        // Modification is not flushed.
        assert_eq!(2, trace.persisted_apply_index());
        for cf in DATA_CFS {
            trace.record_flush(cf, 3);
        }
        trace.maybe_advance_admin_flushed(3);
        // No admin is recorded, index should be advanced.
        assert_eq!(3, trace.persisted_apply_index());
        trace.record_admin_modify(4);
        for cf in DATA_CFS {
            trace.record_flush(cf, 4);
        }
        for cf in DATA_CFS {
            trace.record_modify(cf, 4);
        }
        trace.maybe_advance_admin_flushed(4);
        // Unflushed admin modification should hold index.
        assert_eq!(3, trace.persisted_apply_index());
        trace.record_admin_flush(4);
        trace.maybe_advance_admin_flushed(4);
        // Admin is flushed, index should be advanced.
        assert_eq!(4, trace.persisted_apply_index());
        for cf in DATA_CFS {
            trace.record_flush(cf, 5);
        }
        trace.maybe_advance_admin_flushed(5);
        // Though all data CFs are flushed, but index should not be
        // advanced as we don't know whether there is admin modification.
        assert_eq!(4, trace.persisted_apply_index());
        for cf in DATA_CFS {
            trace.record_modify(cf, 5);
        }
        trace.maybe_advance_admin_flushed(5);
        // Because modify is recorded, so we know there should be no admin
        // modification and index can be advanced.
        assert_eq!(5, trace.persisted_apply_index());
    }
}
