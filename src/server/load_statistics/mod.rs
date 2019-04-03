// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicUsize, Ordering};

/// A load metric for all threads.
pub struct ThreadLoad {
    term: AtomicUsize,
    load: AtomicUsize,
    threshold: usize,
}

impl ThreadLoad {
    /// Constructs a new `ThreadLoad` with the specified threshold.
    pub fn with_threshold(threshold: usize) -> Self {
        ThreadLoad {
            term: AtomicUsize::new(0),
            load: AtomicUsize::new(0),
            threshold,
        }
    }

    /// Returns true if the current load exceeds its threshold.
    #[allow(dead_code)]
    pub fn in_heavy_load(&self) -> bool {
        self.load.load(Ordering::Acquire) > self.threshold
    }

    /// Increases when updating `load`.
    #[allow(dead_code)]
    pub fn term(&self) -> usize {
        self.term.load(Ordering::Acquire)
    }

    /// Gets the current load. For example, 200 means the threads consuming 200% of the CPU resources.
    #[allow(dead_code)]
    pub fn load(&self) -> usize {
        self.load.load(Ordering::Acquire)
    }
}

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub use self::linux::*;

#[cfg(not(target_os = "linux"))]
mod other_os {
    use super::ThreadLoad;
    use std::sync::Arc;
    use std::time::Instant;

    /// A dummy `ThreadLoadStatistics` implementation for non-Linux platforms
    pub struct ThreadLoadStatistics {}

    impl ThreadLoadStatistics {
        /// Constructs a new `ThreadLoadStatistics`.
        pub fn new(_slots: usize, _prefix: &str, _thread_load: Arc<ThreadLoad>) -> Self {
            ThreadLoadStatistics {}
        }
        /// Records current thread load statistics.
        pub fn record(&mut self, _instant: Instant) {}
    }
}
#[cfg(not(target_os = "linux"))]
pub use self::other_os::ThreadLoadStatistics;
