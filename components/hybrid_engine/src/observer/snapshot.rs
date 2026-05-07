// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::result;

use engine_traits::{CacheRegion, FailedReason, KvEngine, RegionCacheEngine};
use in_memory_engine::{RegionCacheMemoryEngine, RegionCacheSnapshot};
use kvproto::metapb::Region;
use raftstore::coprocessor::{
    dispatcher::BoxSnapshotObserver, CoprocessorHost, ObservedSnapshot, SnapshotObserver,
};

use crate::metrics::{
    IN_MEMORY_ENGINE_SNAPSHOT_ACQUIRE_FAILED_REASON_COUNT_STAIC, SNAPSHOT_TYPE_COUNT_STATIC,
};

/// RegionCacheSnapshotPin pins data of a RegionCacheMemoryEngine during taking
/// snapshot. It prevents the data from being evicted or deleted from the cache.
// TODO: Remove it, theoretically it can be remove if we don't need an
// in-memory engine snapshot when a region is removed or splitted.
pub struct RegionCacheSnapshotPin {
    pub snap: Option<result::Result<RegionCacheSnapshot, FailedReason>>,
}

impl Drop for RegionCacheSnapshotPin {
    fn drop(&mut self) {
        if matches!(self.snap, Some(Ok(_))) {
            // ime snapshot is acquired successfully but not used in coprocessor request.
            SNAPSHOT_TYPE_COUNT_STATIC.wasted.inc();
        }
    }
}

impl RegionCacheSnapshotPin {
    pub fn take(&mut self) -> Option<RegionCacheSnapshot> {
        if let Some(snap_result) = self.snap.take() {
            match snap_result {
                Ok(snap) => {
                    SNAPSHOT_TYPE_COUNT_STATIC.in_memory_engine.inc();
                    Some(snap)
                }
                Err(FailedReason::TooOldRead) => {
                    IN_MEMORY_ENGINE_SNAPSHOT_ACQUIRE_FAILED_REASON_COUNT_STAIC
                        .too_old_read
                        .inc();
                    SNAPSHOT_TYPE_COUNT_STATIC.rocksdb.inc();
                    None
                }
                Err(FailedReason::NotCached) => {
                    IN_MEMORY_ENGINE_SNAPSHOT_ACQUIRE_FAILED_REASON_COUNT_STAIC
                        .not_cached
                        .inc();
                    SNAPSHOT_TYPE_COUNT_STATIC.rocksdb.inc();
                    None
                }
                Err(FailedReason::EpochNotMatch) => {
                    IN_MEMORY_ENGINE_SNAPSHOT_ACQUIRE_FAILED_REASON_COUNT_STAIC
                        .epoch_not_match
                        .inc();
                    SNAPSHOT_TYPE_COUNT_STATIC.rocksdb.inc();
                    None
                }
            }
        } else {
            None
        }
    }
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
        let snap = Some(self.cache_engine.snapshot(region, read_ts, sequence_number));
        Box::new(RegionCacheSnapshotPin { snap })
    }
}
