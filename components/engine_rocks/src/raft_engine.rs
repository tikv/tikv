// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::RocksEngine;
use crate::write_batch::RocksWriteBatch;

use engine_traits::def_raft_engine;

def_raft_engine!(RocksEngine, RocksWriteBatch);
