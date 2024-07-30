// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::RangeCacheEngineExt;

use crate::PanicEngine;

impl RangeCacheEngineExt for PanicEngine {
    fn range_cache_engine_enabled(&self) -> bool {
        panic!()
    }

    fn evict_range(&self, range: &engine_traits::CacheRange) {
        panic!()
    }
}
