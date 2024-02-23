use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

pub trait MemoryLimiter: AllocationRecorder {
    fn acquire(&self, n: usize) -> bool;
    fn reclaim(&self, n: usize);
    fn mem_usage(&self) -> usize;
}

// todo(SpadeA): This is used for the purpose of recording the memory footprint.
// It should be removed in the future.
pub trait AllocationRecorder: Clone {
    fn allocated(&self, addr: usize, size: usize);
    fn freed(&self, addr: usize, size: usize);
}

// a test purpose limiter
#[derive(Clone, Default)]
pub struct RecorderLimiter {
    recorder: Arc<Mutex<HashMap<usize, usize>>>,
}

impl Drop for RecorderLimiter {
    fn drop(&mut self) {
        let recorder = self.recorder.lock().unwrap();
        assert!(recorder.is_empty());
    }
}

impl AllocationRecorder for RecorderLimiter {
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

impl MemoryLimiter for RecorderLimiter {
    fn acquire(&self, _: usize) -> bool {
        true
    }

    fn mem_usage(&self) -> usize {
        0
    }

    fn reclaim(&self, _: usize) {}
}
