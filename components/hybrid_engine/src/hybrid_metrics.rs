// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{StatisticsReporter, KvEngine, MemoryEngine};

use crate::engine::HybridEngine;

pub struct HybridStatisticsReporter {}

impl<EK, EM> StatisticsReporter<HybridEngine<EK, EM>> for HybridStatisticsReporter
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
