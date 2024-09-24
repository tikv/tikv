// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{CacheRegion, KvEngine};
use kvproto::metapb::Region;
use raftstore::coprocessor::{
    dispatcher::BoxSnapshotObserver, CoprocessorHost, ObservedSnapshot, SnapshotObserver,
};
use region_cache_memory_engine::{RegionCacheMemoryEngine, RegionCacheSnapshot};

use crate::new_in_memory_snapshot;

/// RegionCacheSnapshotPin pins data of a RegionCacheMemoryEngine during taking
/// snapshot. It prevents the data from being evicted or deleted from the cache.
// TODO: Remove it, theoretically it can be remove if we don't need an
// in-memory engine snapshot when a region is removed or splitted.
pub struct RegionCacheSnapshotPin {
    pub snap: Option<RegionCacheSnapshot>,
}

impl ObservedSnapshot for RegionCacheSnapshotPin {}

#[derive(Clone)]
pub struct HybridSnapshotObserver {
    cache_engine: RegionCacheMemoryEngine,
}

impl HybridSnapshotObserver {
    pub fn new(cache_engine: RegionCacheMemoryEngine) -> Self {
        HybridSnapshotObserver { cache_engine }
    }

    pub fn register_to(&self, coprocessor_host: &mut CoprocessorHost<impl KvEngine>) {
        coprocessor_host
            .registry
            .register_snapshot_observer(BoxSnapshotObserver::new(self.clone()));
    }
}

impl SnapshotObserver for HybridSnapshotObserver {
    fn on_snapshot(
        &self,
        region: &Region,
        read_ts: u64,
        sequence_number: u64,
    ) -> Box<dyn ObservedSnapshot> {
        // Taking a snapshot to pin data in the cache engine which prevents the
        // data from being evicted or deleted from the cache.
        // The data should be released when the snapshot is dropped.
        let region = CacheRegion::from_region(region);
        let snap = new_in_memory_snapshot(&self.cache_engine, region, read_ts, sequence_number);
        Box::new(RegionCacheSnapshotPin { snap })
    }
}
