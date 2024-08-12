// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{CacheRange, EvictReason, RangeCacheEngineExt};

use crate::PanicEngine;

impl RangeCacheEngineExt for PanicEngine {
    fn range_cache_engine_enabled(&self) -> bool {
        panic!()
    }

    fn load_range(&self, range: CacheRange) {
        panic!()
    }

    fn evict_range(&self, range: &engine_traits::CacheRange, evict_range: EvictReason) {
        panic!()
    }
}
