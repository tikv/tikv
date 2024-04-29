// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, RangeCacheEngine, StatisticsReporter};

use crate::engine::HybridEngine;

pub struct HybridEngineStatisticsReporter {}

impl<EK, EC> StatisticsReporter<HybridEngine<EK, EC>> for HybridEngineStatisticsReporter
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    fn new(name: &str) -> Self {
        unimplemented!()
    }

    fn collect(&mut self, engine: &HybridEngine<EK, EC>) {
        unimplemented!()
    }

    fn flush(&mut self) {
        unimplemented!()
    }
}
