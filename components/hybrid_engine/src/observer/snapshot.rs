// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, SnapshotContext};
use raftstore::coprocessor::{
    dispatcher::BoxSnapshotObserver, CoprocessorHost, ObservedSnapshot, SnapshotObserver,
};
use range_cache_memory_engine::{RangeCacheMemoryEngine, RangeCacheSnapshot};

use crate::new_in_memory_snapshot;

/// RangeCacheSnapshotPin pins data of a RangeCacheMemoryEngine during taking
/// snapshot. It prevents the data from being evicted or deleted from the cache.
pub struct RangeCacheSnapshotPin {
    pub snap: Option<RangeCacheSnapshot>,
}

impl ObservedSnapshot for RangeCacheSnapshotPin {}

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
    fn on_snapshot(&self, ctx: SnapshotContext, sequence_number: u64) -> Box<dyn ObservedSnapshot> {
        let snap = new_in_memory_snapshot(&self.cache_engine, ctx, sequence_number);
        Box::new(RangeCacheSnapshotPin { snap })
    }
}
