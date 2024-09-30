// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, RegionCacheEngine, RegionCacheEngineExt, RegionEvent};

use crate::HybridEngine;

impl<EK, EC> RegionCacheEngineExt for HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
{
    #[inline]
    fn on_region_event(&self, event: RegionEvent) {
        self.region_cache_engine().on_region_event(event);
    }

    #[inline]
    fn region_cached(&self, region: &kvproto::metapb::Region) -> bool {
        self.region_cache_engine().region_cached(region)
    }

    #[inline]
    fn load_region(&self, region: &kvproto::metapb::Region) {
        self.region_cache_engine().load_region(region)
    }
}
