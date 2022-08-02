// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{RaftEngine, RaftLogBatch};
use kvproto::{
    metapb::{self, Region},
    raft_serverpb::{PeerState, RaftApplyState, RaftLocalState, RegionLocalState},
};
use raft::{
    eraftpb::{Entry, Snapshot},
    GetEntriesContext, RaftState, INVALID_ID,
};
use raftstore::store::{util::find_peer, RAFT_INIT_LOG_INDEX, RAFT_INIT_LOG_TERM};
use slog::{o, Logger};
use tikv_util::box_err;

use crate::{Error, Result};

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
#[derive(Debug)]
pub struct Storage<ER> {
    engine: ER,
    peer: metapb::Peer,
    region_state: RegionLocalState,
    raft_state: RaftLocalState,
    apply_state: RaftApplyState,
    logger: Logger,
}

impl<ER: RaftEngine> Storage<ER> {
    /// Creates a new storage.
    ///
    /// All metadata should be initialized before calling this method. If the
    /// region is destroyed, `None` will be returned.
    pub fn new(
        region_id: u64,
        store_id: u64,
        engine: ER,
        logger: &Logger,
    ) -> Result<Option<Storage<ER>>> {
        let region_state = match engine.get_region_state(region_id) {
            Ok(Some(s)) => s,
            res => {
                return Err(box_err!("failed to get region state: {:?}", res));
            }
        };

        if region_state.get_state() == PeerState::Tombstone {
            return Ok(None);
        }

        let peer = find_peer(region_state.get_region(), store_id);
        let peer = match peer {
            Some(p) if p.get_id() != INVALID_ID => p,
            _ => {
                return Err(box_err!("no valid peer found in {:?}", region_state));
            }
        };

        let logger = logger.new(o!("region_id" => region_id, "peer_id" => peer.get_id()));

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

        let mut s = Storage {
            engine,
            peer: peer.clone(),
            region_state,
            raft_state,
            apply_state,
            logger,
        };
        s.validate_state()?;
        Ok(Some(s))
    }

    fn validate_state(&mut self) -> Result<()> {
        unimplemented!()
    }

    #[inline]
    pub fn region_state(&self) -> &RegionLocalState {
        &self.region_state
    }

    #[inline]
    pub fn raft_state(&self) -> &RaftLocalState {
        &self.raft_state
    }

    #[inline]
    pub fn apply_state(&self) -> &RaftApplyState {
        &self.apply_state
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
}
