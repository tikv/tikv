// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use engine_rocks::RocksEngine;
use engine_traits::CacheRange;
use keys::{DATA_MAX_KEY, DATA_MIN_KEY};

use crate::HybridEngineImpl;

pub trait RangCacheEngineExt {
    fn cache_all(&self);
}

impl RangCacheEngineExt for HybridEngineImpl {
    fn cache_all(&self) {
        self.range_cache_engine().new_range(CacheRange::new(
            DATA_MIN_KEY.to_vec(),
            DATA_MAX_KEY.to_vec(),
        ));
    }
}

impl RangCacheEngineExt for RocksEngine {
    fn cache_all(&self) {}
}
