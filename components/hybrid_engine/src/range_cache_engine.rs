// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{CacheRange, EvictReason, KvEngine, RangeCacheEngine, RangeCacheEngineExt};

use crate::HybridEngine;

impl<EK, EC> RangeCacheEngineExt for HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    fn range_cache_engine_enabled(&self) -> bool {
        true
    }

    fn range_cached(&self, range: &CacheRange) -> bool {
        self.range_cache_engine().range_cached(range)
    }

    #[inline]
    fn evict_range(&self, range: &CacheRange, evict_range: EvictReason) {
        self.range_cache_engine().evict_range(range, evict_range);
    }

    #[inline]
    fn load_range(&self, range: CacheRange) {
        self.range_cache_engine().load_range(range);
    }
}
