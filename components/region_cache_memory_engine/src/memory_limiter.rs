// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::{engine::SkiplistEngine, write_batch::NODE_OVERHEAD_SIZE_EXPECTATION};

pub(crate) const _OVER_HEAD_CHECK_INTERVAL: usize = 1_000_000;

#[derive(Debug, PartialEq)]
pub(crate) enum MemoryUsage {
    NormalUsage(usize),
    SoftLimitReached(usize),
    // usize here means the current memory usage and it's the usize in it adding with the memory
    // acquiring exceeds the hard limit
    HardLimitReached(usize),
}

/// MemoryController is used to control the memory usage of the region cache
/// memory engine. The memory consumption is calculated by the allocated memory
/// for keys and values, and the overhead of the skiplist where the overhead is
/// estimated by using the node count of the skiplist times the
/// `NODE_OVERHEAD_SIZE_EXPECTATION`.
#[derive(Debug)]
pub struct MemoryController {
    allocated: AtomicUsize,
    soft_limit_threshold: usize,
    hard_limit_threshold: usize,
    memory_checking: AtomicBool,

    acquire_count: AtomicUsize,
    over_head_check_interval: usize,
    nodes_overhead: AtomicUsize,
    skiplist_engine: SkiplistEngine,
}

impl MemoryController {
    pub fn new(
        soft_limit_threshold: usize,
        hard_limit_threshold: usize,
        over_head_check_interval: usize,
        skiplist_engine: SkiplistEngine,
    ) -> Self {
        Self {
            soft_limit_threshold,
            hard_limit_threshold,
            allocated: AtomicUsize::new(0),
            memory_checking: AtomicBool::new(false),
            acquire_count: AtomicUsize::new(0),
            nodes_overhead: AtomicUsize::new(0),
            over_head_check_interval,
            skiplist_engine,
        }
    }

    pub(crate) fn acquire(&self, n: usize) -> MemoryUsage {
        let count = self.acquire_count.fetch_add(1, Ordering::Relaxed) + 1;
        if count % self.over_head_check_interval == 0 {
            let overhead = self.skiplist_engine.node_count() * NODE_OVERHEAD_SIZE_EXPECTATION;
            self.nodes_overhead.store(overhead, Ordering::SeqCst);
        }

        let mem_usage = self.allocated.fetch_add(n, Ordering::SeqCst)
            + self.nodes_overhead.load(Ordering::Relaxed)
            + n;
        if mem_usage >= self.hard_limit_threshold {
            self.allocated.fetch_sub(n, Ordering::SeqCst);
            return MemoryUsage::HardLimitReached(mem_usage - n);
        }

        if mem_usage >= self.soft_limit_threshold {
            return MemoryUsage::SoftLimitReached(mem_usage);
        }

        MemoryUsage::NormalUsage(mem_usage)
    }

    pub(crate) fn release(&self, n: usize) {
        self.allocated.fetch_sub(n, Ordering::SeqCst);
    }

    #[inline]
    pub(crate) fn soft_limit_threshold(&self) -> usize {
        self.soft_limit_threshold
    }

    #[inline]
    pub(crate) fn hard_limit_threshold(&self) -> usize {
        self.hard_limit_threshold
    }

    pub(crate) fn set_memory_checking(&self, v: bool) {
        self.memory_checking.store(v, Ordering::SeqCst);
    }

    pub(crate) fn memory_checking(&self) -> bool {
        self.memory_checking.load(Ordering::SeqCst)
    }

    pub(crate) fn mem_usage(&self) -> usize {
        self.allocated.load(Ordering::Relaxed) + self.nodes_overhead.load(Ordering::Relaxed)
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
        let mc = MemoryController::new(300, 500, 1, skiplist_engine.clone());
        assert_eq!(mc.acquire(100), MemoryUsage::NormalUsage(100));
        assert_eq!(mc.acquire(150), MemoryUsage::NormalUsage(250));
        assert_eq!(mc.acquire(50), MemoryUsage::SoftLimitReached(300));
        assert_eq!(mc.acquire(50), MemoryUsage::SoftLimitReached(350));
        assert_eq!(mc.acquire(150), MemoryUsage::HardLimitReached(350));
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
        assert_eq!(mc.acquire(104), MemoryUsage::HardLimitReached(396));
        skiplist_engine.data[0].remove(entry.key(), guard);
        assert_eq!(mc.acquire(104), MemoryUsage::SoftLimitReached(404));
    }
}
