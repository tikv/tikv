// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, MemoryEngine, PerfContextExt, PerfContextKind};

use crate::engine::HybridEngine;

impl<EK, EM> PerfContextExt for HybridEngine<EK, EM>
where
    EK: KvEngine,
    EM: MemoryEngine,
{
    type PerfContext = EK::PerfContext;

    fn get_perf_context(
        level: engine_traits::PerfLevel,
        kind: PerfContextKind,
    ) -> Self::PerfContext {
        EK::get_perf_context(level, kind)
    }
}
