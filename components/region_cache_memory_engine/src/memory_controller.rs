// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crossbeam::utils::CachePadded;

use crate::{engine::SkiplistEngine, write_batch::NODE_OVERHEAD_SIZE_EXPECTATION};

#[derive(Debug, PartialEq)]
pub(crate) enum MemoryUsage {
    NormalUsage(usize),
    SoftLimitReached(usize),
    // usize here means the current memory usage and it's the usize in it adding with the memory
    // acquiring exceeds the hard limit
    HardLimitReached(usize),
}

#[derive(Debug)]
struct HotData {
    // Allocated memory for keys and values (node overhead is not included)
    allocated: AtomicUsize,
    // The number of writes that are buffered but not yet written.
    pending_node_count: AtomicUsize,
}

/// MemoryController is used to control the memory usage of the region cache
/// memory engine. The memory consumption is calculated by the allocated memory
/// for keys and values, and the overhead of the skiplist where the overhead is
/// estimated by using the node count of the skiplist times the
/// `NODE_OVERHEAD_SIZE_EXPECTATION`.
#[derive(Debug)]
pub struct MemoryController {
    hot_data: CachePadded<HotData>,
    soft_limit_threshold: usize,
    hard_limit_threshold: usize,
    memory_checking: AtomicBool,
    skiplist_engine: SkiplistEngine,
}

impl MemoryController {
    pub fn new(
        soft_limit_threshold: usize,
        hard_limit_threshold: usize,
        skiplist_engine: SkiplistEngine,
    ) -> Self {
        Self {
            hot_data: CachePadded::new(HotData {
                allocated: AtomicUsize::new(0),
                pending_node_count: AtomicUsize::new(0),
            }),
            soft_limit_threshold,
            hard_limit_threshold,
            memory_checking: AtomicBool::new(false),
            skiplist_engine,
        }
    }

    pub(crate) fn acquire(&self, n: usize) -> MemoryUsage {
        let pending_count = self
            .hot_data
            .pending_node_count
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        let node_count = self.skiplist_engine.node_count();

        let mem_usage = self.hot_data.allocated.fetch_add(n, Ordering::Relaxed)
            + n
            + (node_count + pending_count) * NODE_OVERHEAD_SIZE_EXPECTATION;
        if mem_usage >= self.hard_limit_threshold {
            self.hot_data.allocated.fetch_sub(n, Ordering::Relaxed);
            self.hot_data
                .pending_node_count
                .fetch_sub(1, Ordering::Relaxed);
            return MemoryUsage::HardLimitReached(mem_usage - n - NODE_OVERHEAD_SIZE_EXPECTATION);
        }

        if mem_usage >= self.soft_limit_threshold {
            return MemoryUsage::SoftLimitReached(mem_usage);
        }

        MemoryUsage::NormalUsage(mem_usage)
    }

    pub(crate) fn on_node_written(&self, counts: usize) {
        self.hot_data
            .pending_node_count
            .fetch_sub(counts, Ordering::Relaxed);
    }

    pub(crate) fn release(&self, n: usize) {
        self.hot_data.allocated.fetch_sub(n, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn reached_soft_limit(&self) -> bool {
        self.mem_usage() >= self.soft_limit_threshold
    }

    #[inline]
    pub(crate) fn soft_limit_threshold(&self) -> usize {
        self.soft_limit_threshold
    }

    #[inline]
    pub(crate) fn set_memory_checking(&self, v: bool) {
        self.memory_checking.store(v, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn memory_checking(&self) -> bool {
        self.memory_checking.load(Ordering::Relaxed)
    }

    #[inline]
    pub(crate) fn mem_usage(&self) -> usize {
        self.hot_data.allocated.load(Ordering::Relaxed)
            + (self.skiplist_engine.node_count()
                + self.hot_data.pending_node_count.load(Ordering::Relaxed))
                * NODE_OVERHEAD_SIZE_EXPECTATION
    }
}

#[cfg(test)]
mod tests {
    use crossbeam::epoch;

    use super::*;
    use crate::keys::{encode_key, InternalBytes, ValueType};

    #[test]
    fn test_memory_controller() {
        let skiplist_engine = SkiplistEngine::new();
        let mc = MemoryController::new(500, 1000, skiplist_engine.clone());
        assert_eq!(mc.acquire(100), MemoryUsage::NormalUsage(196));
        assert_eq!(mc.acquire(150), MemoryUsage::NormalUsage(442));
        assert_eq!(mc.acquire(50), MemoryUsage::SoftLimitReached(588));
        assert_eq!(mc.acquire(50), MemoryUsage::SoftLimitReached(734));
        assert_eq!(mc.acquire(200), MemoryUsage::HardLimitReached(734));
        mc.release(50);
        assert_eq!(mc.mem_usage(), 684);
        assert_eq!(mc.hot_data.pending_node_count.load(Ordering::Relaxed), 4);

        mc.on_node_written(4);
        assert_eq!(mc.hot_data.pending_node_count.load(Ordering::Relaxed), 0);
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
        assert_eq!(mc.acquire(104), MemoryUsage::SoftLimitReached(596));
        skiplist_engine.data[0].remove(entry.key(), guard);
        assert_eq!(mc.acquire(104), MemoryUsage::SoftLimitReached(700));
    }
}
