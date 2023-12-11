// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, MemoryEngine, StatisticsReporter};

use crate::engine::HybridEngine;

pub struct HybridEngineStatisticsReporter {}

impl<EK, EM> StatisticsReporter<HybridEngine<EK, EM>> for HybridEngineStatisticsReporter
where
    EK: KvEngine,
    EM: MemoryEngine,
{
    fn new(name: &str) -> Self {
        unimplemented!()
    }

    fn collect(&mut self, engine: &HybridEngine<EK, EM>) {
        unimplemented!()
    }

    fn flush(&mut self) {
        unimplemented!()
    }
}
