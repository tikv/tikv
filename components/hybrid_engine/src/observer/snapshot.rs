// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use engine_traits::{CacheRange, KvEngine, RangeCacheEngine};
use kvproto::metapb::Region;
use raftstore::coprocessor::{
    dispatcher::BoxSnapshotObserver, CoprocessorHost, SnapshotObserver, SnapshotPin,
};
use range_cache_memory_engine::{RangeCacheMemoryEngine, RangeCacheSnapshot};

struct RangeCacheSnapshotPin {
    snap: Option<RangeCacheSnapshot>,
}

impl SnapshotPin for RangeCacheSnapshotPin {}

#[derive(Clone)]
pub struct HybridSnapshotObserver {
    cache_engine: RangeCacheMemoryEngine,
}

impl HybridSnapshotObserver {
    pub fn new(cache_engine: RangeCacheMemoryEngine) -> Self {
        HybridSnapshotObserver { cache_engine }
    }

    pub fn register_to(&self, coprocessor_host: &mut CoprocessorHost<impl KvEngine>) {
        coprocessor_host
            .registry
            .register_snapshot_observer(BoxSnapshotObserver::new(self.clone()));
    }
}

impl SnapshotObserver for HybridSnapshotObserver {
    fn on_snapshot(&self, region: &Region, seqno: u64) -> Arc<dyn SnapshotPin> {
        let snap = self
            .cache_engine
            .snapshot(
                region.id,
                region.get_region_epoch().version,
                CacheRange::from_region(region),
                u64::MAX,
                seqno,
            )
            .ok();
        Arc::new(RangeCacheSnapshotPin { snap })
    }
}
