// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, RangeCacheEngine, RangeCacheEngineExt, RegionEvent};

use crate::HybridEngine;

impl<EK, EC> RangeCacheEngineExt for HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    fn range_cache_engine_enabled(&self) -> bool {
        true
    }

    #[inline]
    fn on_region_event(&self, event: RegionEvent) {
        self.range_cache_engine().on_region_event(event);
    }
}
