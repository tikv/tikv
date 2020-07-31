// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::KvEngines;

use crate::RocksEngine;

#[derive(Clone)]
pub struct TwoRocksEngines {
    kv: RocksEngine,
    raft: RocksEngine,
    shared_block_cache: bool,
}

impl TwoRocksEngines {
    pub fn new(kv: RocksEngine, raft: RocksEngine, shared_block_cache: bool) -> Self {
        TwoRocksEngines {
            kv,
            raft,
            shared_block_cache,
        }
    }
}

impl KvEngines for TwoRocksEngines {
    type Kv = RocksEngine;
    type Raft = RocksEngine;
    fn kv(&self) -> &Self::Kv {
        &self.kv
    }
    fn raft(&self) -> &Self::Raft {
        &self.raft
    }
    fn shared_block_cache(&self) -> bool {
        self.shared_block_cache
    }
}
