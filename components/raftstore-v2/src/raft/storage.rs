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

#[cfg(test)]
mod tests {
    use engine_traits::{RaftEngine, RaftEngineReadOnly, RaftLogBatch};
    use kvproto::{
        metapb::{Peer, Region},
        raft_serverpb::PeerState,
    };
    use raftstore::store::{RAFT_INIT_LOG_INDEX, RAFT_INIT_LOG_TERM};
    use tempfile::TempDir;

    #[test]
    fn test_write_initial_states() {
        let mut region = Region::default();
        region.set_id(4);
        let mut p = Peer::default();
        p.set_id(5);
        p.set_store_id(6);
        region.mut_peers().push(p);
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(4);

        let path = TempDir::new().unwrap();
        let engine = engine_test::new_temp_engine(&path);
        let raft_engine = &engine.raft;
        let mut wb = raft_engine.log_batch(10);
        super::write_initial_states(&mut wb, region.clone()).unwrap();
        assert!(!wb.is_empty());
        raft_engine.consume(&mut wb, true).unwrap();

        let local_state = raft_engine.get_region_state(4).unwrap().unwrap();
        assert_eq!(local_state.get_state(), PeerState::Normal);
        assert_eq!(*local_state.get_region(), region);

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
}
