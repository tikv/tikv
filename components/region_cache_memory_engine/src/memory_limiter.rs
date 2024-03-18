// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

pub(crate) enum MemoryUsage {
    NormalUsage(usize),
    SoftLimitReached(usize),
    HardLimitReached(usize),
}

pub struct MemoryLimiter {
    allocated: AtomicUsize,
    soft_limit_threshold: usize,
    hard_limit_threshold: usize,
    memory_checking: AtomicBool,
}

impl MemoryLimiter {
    pub fn new(soft_limit_threshold: usize, hard_limit_threshold: usize) -> Self {
        Self {
            soft_limit_threshold,
            hard_limit_threshold,
            allocated: AtomicUsize::new(0),
            memory_checking: AtomicBool::new(false),
        }
    }

    pub(crate) fn acquire(&self, n: usize) -> MemoryUsage {
        let mem_usage = self.allocated.fetch_add(n, Ordering::SeqCst) + n;
        if mem_usage >= self.hard_limit_threshold {
            self.allocated.fetch_sub(n, Ordering::SeqCst);
            return MemoryUsage::HardLimitReached(mem_usage);
        }

        if mem_usage >= self.soft_limit_threshold {
            return MemoryUsage::HardLimitReached(mem_usage);
        }

        MemoryUsage::NormalUsage(mem_usage)
    }

    pub(crate) fn set_memory_checking(&self, v: bool) {
        self.memory_checking.store(v, Ordering::SeqCst);
    }

    pub(crate) fn memory_checking(&self) -> bool {
        self.memory_checking.load(Ordering::SeqCst)
    }

    pub(crate) fn mem_usage(&self) -> MemoryUsage {
        let n = self.allocated.load(Ordering::Relaxed);
        if n >= self.hard_limit_threshold {
            return MemoryUsage::HardLimitReached(n);
        }
        if n >= self.soft_limit_threshold {
            return MemoryUsage::SoftLimitReached(n);
        }
        MemoryUsage::NormalUsage(n)
    }
}
