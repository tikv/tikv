// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{RocksEngine, RocksWriteBatch};

use engine_traits::def_raft_engine;

def_raft_engine!(RocksEngine, RocksWriteBatch);
