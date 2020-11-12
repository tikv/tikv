// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::RocksEngine;
use engine_traits::{PerfContextExt, PerfContext};

impl PerfContextExt for RocksEngine {
    type PerfContext = RocksPerfContext;

    fn get() -> Option<Self::PerfContext> {
        Some(RocksPerfContext)
    }
}

pub struct RocksPerfContext;

impl PerfContext for RocksPerfContext {
}
