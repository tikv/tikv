// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{RangeCacheEngineExt, RegionEvent};

use crate::RocksEngine;

impl RangeCacheEngineExt for RocksEngine {
    fn range_cache_engine_enabled(&self) -> bool {
        false
    }

    #[inline]
    fn on_region_event(&self, _: RegionEvent) {}

    #[inline]
    fn region_cached(&self, _: &kvproto::metapb::Region) -> bool {
        false
    }

    #[inline]
    fn load_region(&self, _: kvproto::metapb::Region) {}
}
