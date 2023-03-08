// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_rocks::RocksEngine;
use engine_traits::TabletRegistry;
use raft_log_engine::RaftLogEngine;

use crate::{config::ConfigController, server::debug::Result};

// Debugger for raftstore-v2
#[derive(Clone)]
pub struct DebuggerV2 {
    tablet_reg: TabletRegistry<RocksEngine>,
    raft_engine: RaftLogEngine,
    cfg_controller: ConfigController,
}

impl DebuggerV2 {
    pub fn new(
        tablet_reg: TabletRegistry<RocksEngine>,
        raft_engine: RaftLogEngine,
        cfg_controller: ConfigController,
    ) -> Self {
        DebuggerV2 {
            tablet_reg,
            raft_engine,
            cfg_controller,
        }
    }

    pub fn get_all_regions_in_store(&self) -> Result<Vec<u64>> {
        unimplemented!()
    }
}
