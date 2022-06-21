// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{RaftEngine, RaftLogBatch};
use kvproto::{
    metapb::Region,
    raft_serverpb::{RaftApplyState, RaftLocalState, RegionLocalState},
};
use raft::{
    eraftpb::{Entry, Snapshot},
    GetEntriesContext, RaftState,
};
use raftstore::store::{RAFT_INIT_LOG_INDEX, RAFT_INIT_LOG_TERM};
use slog::Logger;

use crate::Result;

pub fn write_initial_states(wb: &mut impl RaftLogBatch, region: Region) -> Result<()> {
    let region_id = region.get_id();

    let mut state = RegionLocalState::default();
    state.set_region(region);
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
pub struct Storage<ER> {
    engine: ER,
    logger: Logger,
}

impl<ER> Storage<ER> {
    pub fn new(engine: ER, logger: Logger) -> Storage<ER> {
        Storage { engine, logger }
    }

    pub fn applied_index(&self) -> u64 {
        unimplemented!()
    }
}

impl<ER: RaftEngine> raft::Storage for Storage<ER> {
    fn initial_state(&self) -> raft::Result<RaftState> {
        unimplemented!()
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        context: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        unimplemented!()
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        unimplemented!()
    }

    fn first_index(&self) -> raft::Result<u64> {
        unimplemented!()
    }

    fn last_index(&self) -> raft::Result<u64> {
        unimplemented!()
    }

    fn snapshot(&self, request_index: u64, to: u64) -> raft::Result<Snapshot> {
        unimplemented!()
    }
}
