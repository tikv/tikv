// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};

use collections::{HashMap, HashSet};
use skiplist_rs::{AllocationRecorder, MemoryLimiter, Node};

pub(crate) enum MemoryUsage {
    NormalUsage(usize),
    SoftLimitReached(usize),
    HardLimitReached(usize),
}

pub struct MemoryController {
    allocated: AtomicUsize,
    soft_limit_threshold: usize,
    hard_limit_threshold: usize,
    memory_checking: AtomicBool,
}

impl MemoryController {
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

// todo: implement a real memory limiter. Now, it is used for test.
#[derive(Clone, Default)]
pub struct GlobalMemoryLimiter {
    pub(crate) recorder: Arc<Mutex<HashMap<usize, usize>>>,
    pub(crate) removed: Arc<Mutex<HashSet<Vec<u8>>>>,
}

impl MemoryLimiter for GlobalMemoryLimiter {
    fn acquire(&self, n: usize) -> bool {
        true
    }

    fn mem_usage(&self) -> usize {
        0
    }

    fn reclaim(&self, n: usize) {}
}

impl AllocationRecorder for GlobalMemoryLimiter {
    fn allocated(&self, addr: usize, size: usize) {
        let mut recorder = self.recorder.lock().unwrap();
        assert!(!recorder.contains_key(&addr));
        recorder.insert(addr, size);
    }

    fn freed(&self, addr: usize, size: usize) {
        let node = addr as *mut Node;
        let mut removed = self.removed.lock().unwrap();
        removed.insert(unsafe { (*node).key().to_vec() });
        let mut recorder = self.recorder.lock().unwrap();
        assert_eq!(recorder.remove(&addr).unwrap(), size);
    }
}

impl Drop for GlobalMemoryLimiter {
    fn drop(&mut self) {
        assert!(self.recorder.lock().unwrap().is_empty());
    }
}
