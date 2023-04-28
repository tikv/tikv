// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{PerfContext, PerfContextExt, PerfContextKind, PerfLevel};
use tracker::TrackerToken;

use crate::{engine::RocksEngine, perf_context_impl::PerfContextStatistics};

impl PerfContextExt for RocksEngine {
    type PerfContext = RocksPerfContext;

    fn get_perf_context(level: PerfLevel, kind: PerfContextKind) -> Self::PerfContext {
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

    #[allow(unreachable_code)]
    #[allow(unused_variables)]
    fn report_metrics(&mut self, trackers: &[TrackerToken]) {
        #[cfg(any(test, feature = "testexport"))]
        {
            // TODO When test v2_compat, it will create different RocksEngine, that raise
            // error here.
            return;
        }
        self.stats.report(trackers)
    }
}
