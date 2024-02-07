// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex};

use collections::{HashMap, HashSet};
use skiplist_rs::{AllocationRecorder, MemoryLimiter, Node};

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
    fn alloc(&self, addr: usize, size: usize) {
        let mut recorder = self.recorder.lock().unwrap();
        assert!(!recorder.contains_key(&addr));
        recorder.insert(addr, size);
    }

    fn free(&self, addr: usize, size: usize) {
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
