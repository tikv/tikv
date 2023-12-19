// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, PerfContextExt, PerfContextKind, RegionCacheEngine};

use crate::engine::HybridEngine;

impl<EK, EC> PerfContextExt for HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
{
    type PerfContext = EK::PerfContext;

    fn get_perf_context(
        level: engine_traits::PerfLevel,
        kind: PerfContextKind,
    ) -> Self::PerfContext {
        EK::get_perf_context(level, kind)
    }
}
