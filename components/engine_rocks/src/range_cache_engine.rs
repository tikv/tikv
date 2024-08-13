// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{CacheRange, EvictReason, RangeCacheEngineExt};

use crate::RocksEngine;

impl RangeCacheEngineExt for RocksEngine {
    fn range_cache_engine_enabled(&self) -> bool {
        false
    }

    #[inline]
    fn range_cached(&self, _: &CacheRange) -> bool {
        false
    }

    #[inline]
    fn load_range(&self, _: CacheRange) {}

    #[inline]
    fn evict_range(&self, _: &engine_traits::CacheRange, _: EvictReason) {}
}
