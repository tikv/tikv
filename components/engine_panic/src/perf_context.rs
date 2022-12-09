// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{PerfContext, PerfContextExt, PerfContextKind, PerfLevel};
use tracker::TrackerToken;

use crate::engine::PanicEngine;

impl PerfContextExt for PanicEngine {
    type PerfContext = PanicPerfContext;

    fn get_perf_context(&self, level: PerfLevel, kind: PerfContextKind) -> Self::PerfContext {
        panic!()
    }
}

pub struct PanicPerfContext;

impl PerfContext for PanicPerfContext {
    fn start_observe(&mut self) {
        panic!()
    }

    fn report_metrics(&mut self, _: &[TrackerToken]) {
        panic!()
    }
}
