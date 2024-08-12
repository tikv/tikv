// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use engine_rocks::RocksEngine;
use keys::{DATA_MAX_KEY, DATA_MIN_KEY};
use kvproto::metapb::{Peer, Region};

use crate::HybridEngineImpl;

pub trait RangCacheEngineExt {
    fn cache_all(&self);
}

impl RangCacheEngineExt for HybridEngineImpl {
    fn cache_all(&self) {
        let mut region = Region::default();
        region.id = 1;
        region.start_key = DATA_MIN_KEY.to_vec();
        region.end_key = DATA_MAX_KEY.to_vec();
        region.mut_peers().push(Peer::default());

        self.range_cache_engine().new_region(region);
    }
}

impl RangCacheEngineExt for RocksEngine {
    fn cache_all(&self) {}
}
