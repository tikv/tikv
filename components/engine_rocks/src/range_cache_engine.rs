// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::RangeCacheEngineExt;

use crate::RocksEngine;

impl RangeCacheEngineExt for RocksEngine {
    fn range_cache_engine_enabled(&self) -> bool {
        false
    }

    #[inline]
    fn evict_range(&self, _: &engine_traits::CacheRange) {}
}
