// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_rocks::RocksEngine;
use engine_traits::{RaftEngine, TabletRegistry};
use raft::prelude::Entry;

use super::debug::RegionInfo;
use crate::{
    config::ConfigController,
    server::debug::{Error, Result},
};

// Debugger for raftstore-v2
#[derive(Clone)]
pub struct DebuggerV2<ER: RaftEngine> {
    tablet_reg: TabletRegistry<RocksEngine>,
    raft_engine: ER,
    cfg_controller: ConfigController,
}

impl<ER: RaftEngine> DebuggerV2<ER> {
    pub fn new(
        tablet_reg: TabletRegistry<RocksEngine>,
        raft_engine: ER,
        cfg_controller: ConfigController,
    ) -> Self {
        println!("Debugger for raftstore-v2 is used");
        DebuggerV2 {
            tablet_reg,
            raft_engine,
            cfg_controller,
        }
    }

    pub fn get_all_regions_in_store(&self) -> Result<Vec<u64>> {
        unimplemented!()
    }

    pub fn raft_log(&self, region_id: u64, log_index: u64) -> Result<Entry> {
        if let Some(log) = box_try!(self.raft_engine.get_entry(region_id, log_index)) {
            return Ok(log);
        }
        Err(Error::NotFound(format!(
            "raft log for region {} at index {}",
            region_id, log_index
        )))
    }

    pub fn region_info(&self, region_id: u64) -> Result<RegionInfo> {
        let raft_state = box_try!(self.raft_engine.get_raft_state(region_id));
        let apply_state = box_try!(self.raft_engine.get_apply_state(region_id, u64::MAX));
        let region_state = box_try!(self.raft_engine.get_region_state(region_id, u64::MAX));

        match (raft_state, apply_state, region_state) {
            (None, None, None) => Err(Error::NotFound(format!("info for region {}", region_id))),
            (raft_state, apply_state, region_state) => {
                Ok(RegionInfo::new(raft_state, apply_state, region_state))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use engine_test::raft::RaftTestEngine;
    use engine_traits::RaftLogBatch;
    use kvproto::raft_serverpb::*;
    use raft::prelude::EntryType;

    use super::*;
    use crate::{config::TikvConfig, server::KvEngineFactoryBuilder};

    fn new_debugger() -> DebuggerV2<RaftTestEngine> {
        let dir = test_util::temp_dir("test-debugger", false);

        let mut cfg = TikvConfig::default();
        cfg.storage.data_dir = dir.path().to_str().unwrap().to_string();
        cfg.raft_store.raftdb_path = cfg.infer_raft_db_path(None).unwrap();
        cfg.raft_engine.mut_config().dir = cfg.infer_raft_engine_path(None).unwrap();
        let cache = cfg
            .storage
            .block_cache
            .build_shared_cache(cfg.storage.engine);
        let env = cfg.build_shared_rocks_env(None, None).unwrap();

        let factory = KvEngineFactoryBuilder::new(env, &cfg, cache).build();
        let reg = TabletRegistry::new(Box::new(factory), dir.path()).unwrap();

        let raft_engine = RaftTestEngine::new(cfg.raft_engine.config(), None, None).unwrap();

        DebuggerV2::new(reg, raft_engine, ConfigController::default())
    }

    #[test]
    fn test_raft_log() {
        let debugger = new_debugger();
        let raft_engine = &debugger.raft_engine;
        let (region_id, log_index) = (1, 1);

        let mut entry = Entry::default();
        entry.set_term(1);
        entry.set_index(1);
        entry.set_entry_type(EntryType::EntryNormal);
        entry.set_data(vec![42].into());
        let mut wb = raft_engine.log_batch(10);
        RaftLogBatch::append(&mut wb, region_id, None, vec![entry.clone()]).unwrap();
        raft_engine.consume(&mut wb, true).unwrap();

        assert_eq!(debugger.raft_log(region_id, log_index).unwrap(), entry);
        match debugger.raft_log(region_id + 1, log_index + 1) {
            Err(Error::NotFound(_)) => (),
            _ => panic!("expect Error::NotFound(_)"),
        }
    }

    #[test]
    fn test_region_info() {
        let debugger = new_debugger();
        let raft_engine = &debugger.raft_engine;
        let region_id = 1;

        let mut wb = raft_engine.log_batch(10);
        let mut raft_state = RaftLocalState::default();
        raft_state.set_last_index(42);
        RaftLogBatch::put_raft_state(&mut wb, region_id, &raft_state).unwrap();

        let mut apply_state = RaftApplyState::default();
        apply_state.set_applied_index(42);
        RaftLogBatch::put_apply_state(&mut wb, region_id, 42, &apply_state).unwrap();

        let mut region_state = RegionLocalState::default();
        region_state.set_state(PeerState::Tombstone);
        RaftLogBatch::put_region_state(&mut wb, region_id, 42, &region_state).unwrap();

        raft_engine.consume(&mut wb, true).unwrap();

        assert_eq!(
            debugger.region_info(region_id).unwrap(),
            RegionInfo::new(Some(raft_state), Some(apply_state), Some(region_state))
        );
        match debugger.region_info(region_id + 1) {
            Err(Error::NotFound(_)) => (),
            _ => panic!("expect Error::NotFound(_)"),
        }
    }
}
