// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{RangeCacheEngine, RangeGarbageCollection};

use crate::{PanicEngine, PanicSnapshot};

impl RangeCacheEngine for PanicEngine {
    type Snapshot = PanicSnapshot;
    fn snapshot(
        &self,
        range: engine_traits::CacheRange,
        read_ts: u64,
        seq_num: u64,
    ) -> Option<Self::Snapshot> {
        panic!()
    }
}

impl RangeGarbageCollection for PanicEngine {
    fn gc_finished(&mut self) {
        panic!()
    }

    fn gc_range(&self, range: &engine_traits::CacheRange, safe_point: u64) {
        panic!()
    }

    fn ranges_for_gc(&self) -> Vec<engine_traits::CacheRange> {
        panic!()
    }
}
