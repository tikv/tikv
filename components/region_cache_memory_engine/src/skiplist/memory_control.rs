// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

#[cfg(test)]
use std::sync::{Arc, Mutex};

#[cfg(test)]
use collections::HashMap;

pub trait MemoryController: NodeAllocationRecorder {
    fn acquire(&self, n: usize) -> bool;
    fn reclaim(&self, n: usize);
    fn mem_usage(&self) -> usize;
}

// todo(SpadeA): This is used for the purpose of recording the memory footprint.
// It should be removed in the future.
pub trait NodeAllocationRecorder: Clone {
    fn allocated(&self, addr: usize, size: usize);
    fn freed(&self, addr: usize, size: usize);
}

#[cfg(test)]
#[derive(Clone, Default)]
pub struct RecorderLimiter {
    recorder: Arc<Mutex<HashMap<usize, usize>>>,
}

#[cfg(test)]
impl Drop for RecorderLimiter {
    fn drop(&mut self) {
        let recorder = self.recorder.lock().unwrap();
        assert!(recorder.is_empty());
    }
}

#[cfg(test)]
impl NodeAllocationRecorder for RecorderLimiter {
    fn allocated(&self, addr: usize, size: usize) {
        let mut recorder = self.recorder.lock().unwrap();
        assert!(!recorder.contains_key(&addr));
        recorder.insert(addr, size);
    }

    fn freed(&self, addr: usize, size: usize) {
        let mut recorder = self.recorder.lock().unwrap();
        assert_eq!(recorder.remove(&addr).unwrap(), size);
    }
}

#[cfg(test)]
impl MemoryController for RecorderLimiter {
    fn acquire(&self, _: usize) -> bool {
        true
    }

    fn mem_usage(&self) -> usize {
        0
    }

    fn reclaim(&self, _: usize) {}
}

#[cfg(test)]
#[derive(Clone, Default)]
pub struct DummyLimiter {}

#[cfg(test)]
impl NodeAllocationRecorder for DummyLimiter {
    fn allocated(&self, _: usize, _: usize) {}

    fn freed(&self, _: usize, _: usize) {}
}

#[cfg(test)]
impl MemoryController for DummyLimiter {
    fn acquire(&self, _: usize) -> bool {
        true
    }

    fn mem_usage(&self) -> usize {
        0
    }

    fn reclaim(&self, _: usize) {}
}
