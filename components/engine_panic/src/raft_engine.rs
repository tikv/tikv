// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::PanicEngine;
use crate::write_batch::PanicWriteBatch;

use engine_traits::def_raft_engine;

def_raft_engine!(PanicEngine, PanicWriteBatch);
