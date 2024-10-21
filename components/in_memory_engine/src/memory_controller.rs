// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};

use tikv_util::config::VersionTrack;

use crate::{
    engine::SkiplistEngine, write_batch::NODE_OVERHEAD_SIZE_EXPECTATION, InMemoryEngineConfig,
};

#[derive(Debug, PartialEq)]
pub(crate) enum MemoryUsage {
    NormalUsage(usize),
    EvictThresholdReached(usize),
    // usize here means the current memory usage and it's the usize in it adding
    // with the memory acquiring exceeds the capacity
    CapacityReached(usize),
}

/// MemoryController is used to control the memory usage of the region cache
/// memory engine. The memory consumption is calculated by the allocated memory
/// for keys and values, and the overhead of the skiplist where the overhead is
/// estimated by using the node count of the skiplist times the
/// `NODE_OVERHEAD_SIZE_EXPECTATION`.
pub struct MemoryController {
    // Allocated memory for keys and values (node overhead is not included)
    // The number of writes that are buffered but not yet written.
    allocated: AtomicUsize,
    config: Arc<VersionTrack<InMemoryEngineConfig>>,
    memory_checking: AtomicBool,
    skiplist_engine: SkiplistEngine,
}

impl fmt::Debug for MemoryController {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryController")
            .field("allocated", &self.allocated)
            .field("capacity", &self.config.value().capacity())
            .field("evict_threshold", &self.config.value().evict_threshold())
            .field("memory_checking", &self.memory_checking)
            .field("skiplist_engine", &self.skiplist_engine)
            .finish()
    }
}
impl MemoryController {
    pub fn new(
        config: Arc<VersionTrack<InMemoryEngineConfig>>,
        skiplist_engine: SkiplistEngine,
    ) -> Self {
        Self {
            allocated: AtomicUsize::new(0),
            config,
            memory_checking: AtomicBool::new(false),
            skiplist_engine,
        }
    }

    pub(crate) fn acquire(&self, n: usize) -> MemoryUsage {
        let node_count = self.skiplist_engine.node_count();

        // We don't count the node overhead in the write batch to reduce
        // complexity as the overhead should be negligible.
        let mem_usage = self.allocated.fetch_add(n, Ordering::Relaxed)
            + n
            + node_count * NODE_OVERHEAD_SIZE_EXPECTATION;
        if mem_usage >= self.config.value().capacity() {
            self.allocated.fetch_sub(n, Ordering::Relaxed);
            return MemoryUsage::CapacityReached(mem_usage - n);
        }

        if mem_usage >= self.config.value().evict_threshold() {
            return MemoryUsage::EvictThresholdReached(mem_usage);
        }

        MemoryUsage::NormalUsage(mem_usage)
    }

    pub(crate) fn release(&self, n: usize) {
        self.allocated.fetch_sub(n, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn reached_stop_load_threshold(&self) -> bool {
        self.mem_usage() >= self.config.value().stop_load_threshold()
    }

    #[inline]
    pub(crate) fn evict_threshold(&self) -> usize {
        self.config.value().evict_threshold()
    }

    #[inline]
    // return the previous status.
    pub(crate) fn set_memory_checking(&self, v: bool) -> bool {
        self.memory_checking.swap(v, Ordering::Relaxed)
    }

    #[inline]
    pub(crate) fn memory_checking(&self) -> bool {
        self.memory_checking.load(Ordering::Relaxed)
    }

    #[inline]
    pub(crate) fn mem_usage(&self) -> usize {
        self.allocated.load(Ordering::Relaxed)
            + self.skiplist_engine.node_count() * NODE_OVERHEAD_SIZE_EXPECTATION
    }
}

#[cfg(test)]
mod tests {
    use crossbeam::epoch;
    use tikv_util::config::ReadableSize;

    use super::*;
    use crate::keys::{encode_key, InternalBytes, ValueType};

    #[test]
    fn test_memory_controller() {
        let skiplist_engine = SkiplistEngine::new();
        let mut config = InMemoryEngineConfig::config_for_test();
        config.stop_load_threshold = Some(ReadableSize(300));
        config.evict_threshold = Some(ReadableSize(300));
        config.capacity = Some(ReadableSize(500));
        let config = Arc::new(VersionTrack::new(config));
        let mc = MemoryController::new(config, skiplist_engine.clone());
        assert_eq!(mc.acquire(100), MemoryUsage::NormalUsage(100));
        assert_eq!(mc.acquire(150), MemoryUsage::NormalUsage(250));
        assert_eq!(mc.acquire(50), MemoryUsage::EvictThresholdReached(300));
        assert_eq!(mc.acquire(50), MemoryUsage::EvictThresholdReached(350));
        assert_eq!(mc.acquire(200), MemoryUsage::CapacityReached(350));
        mc.release(50);
        assert_eq!(mc.mem_usage(), 300);

        let guard = &epoch::pin();
        // Now, the mem_usage should be 300 + 96
        let encoded_key = encode_key(b"k", 100, ValueType::Value);
        let entry = skiplist_engine.data[0].insert(
            encoded_key,
            InternalBytes::from_vec(b"".to_vec()),
            guard,
        );
        assert_eq!(mc.mem_usage(), 396);
        assert_eq!(mc.acquire(100), MemoryUsage::EvictThresholdReached(496));
        skiplist_engine.data[0].remove(entry.key(), guard);
        assert_eq!(mc.acquire(99), MemoryUsage::EvictThresholdReached(499));
    }
}
