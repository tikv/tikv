// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{PerfContext, PerfContextExt, PerfContextKind, PerfLevel};

use crate::{engine::RocksEngine, perf_context_impl::PerfContextStatistics};

impl PerfContextExt for RocksEngine {
    type PerfContext = RocksPerfContext;

    fn get_perf_context(&self, level: PerfLevel, kind: PerfContextKind) -> Self::PerfContext {
        RocksPerfContext::new(level, kind)
    }
}

#[derive(Debug)]
pub struct RocksPerfContext {
    pub stats: PerfContextStatistics,
}

impl RocksPerfContext {
    pub fn new(level: PerfLevel, kind: PerfContextKind) -> Self {
        RocksPerfContext {
            stats: PerfContextStatistics::new(level, kind),
        }
    }
}

impl PerfContext for RocksPerfContext {
    fn start_observe(&mut self) {
        self.stats.start()
    }

    fn report_metrics(&mut self) {
        self.stats.report()
    }
}
