// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use engine_rocks::RocksEngine;
use kvproto::metapb::Region;

use crate::HybridEngineImpl;

pub trait RangCacheEngineExt {
    fn cache_regions(&self, _regions: &[Region]);
}

impl RangCacheEngineExt for HybridEngineImpl {
    fn cache_regions(&self, regions: &[Region]) {
        for r in regions {
            self.range_cache_engine().new_region(r.clone());
        }
    }
}

impl RangCacheEngineExt for RocksEngine {
    fn cache_regions(&self, _regions: &[Region]) {}
}
