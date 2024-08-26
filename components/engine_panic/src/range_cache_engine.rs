// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{EvictReason, RangeCacheEngineExt, RegionEvent};

use crate::PanicEngine;

impl RangeCacheEngineExt for PanicEngine {
    fn range_cache_engine_enabled(&self) -> bool {
        panic!()
    }

    fn on_region_event(&self, event: RegionEvent) {
        panic!()
    }

    fn region_cached(&self, range: &kvproto::metapb::Region) -> bool {
        panic!()
    }

    fn load_region(&self, range: kvproto::metapb::Region) {
        panic!()
    }
}
