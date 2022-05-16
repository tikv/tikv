// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::RefCell,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use collections::HashMap;
use parking_lot::Mutex;
use tikv_util::sys::thread::{self, Pid};

thread_local! {
    static CURRENT_LOAD: RefCell<Option<Arc<AtomicUsize>>> = RefCell::new(None);
}

/// A load metric for all threads.
pub struct ThreadLoadPool {
    stats: Mutex<HashMap<Pid, Arc<AtomicUsize>>>,
    threshold: usize,
    total_load: AtomicUsize,
}

impl ThreadLoadPool {
    pub fn with_threshold(threshold: usize) -> ThreadLoadPool {
        ThreadLoadPool {
            stats: Mutex::default(),
            threshold,
            total_load: AtomicUsize::new(0),
        }
    }

    /// Check if current thread is in heavy load.
    pub fn current_thread_in_heavy_load(&self) -> bool {
        CURRENT_LOAD.with(|l| {
            let mut l = l.borrow_mut();
            if l.is_none() {
                let mut loads = self.stats.lock();
                *l = Some(loads.entry(thread::thread_id()).or_default().clone());
            }
            l.as_ref().unwrap().load(Ordering::Relaxed) >= self.threshold
        })
    }

    /// Gets the current load. For example, 200 means the threads consuming 200% of the CPU resources.
    pub fn total_load(&self) -> usize {
        self.total_load.load(Ordering::Relaxed)
    }
}

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub use self::linux::ThreadLoadStatistics;

#[cfg(not(target_os = "linux"))]
mod other_os {
    use std::{sync::Arc, time::Instant};

    use super::ThreadLoadPool;

    /// A dummy `ThreadLoadStatistics` implementation for non-Linux platforms
    pub struct ThreadLoadStatistics;

    impl ThreadLoadStatistics {
        /// Constructs a new `ThreadLoadStatistics`.
        pub fn new(_slots: usize, _prefix: &str, _thread_load: Arc<ThreadLoadPool>) -> Self {
            ThreadLoadStatistics
        }
        /// Designate target thread count of this collector.
        pub fn set_thread_target(&mut self, _target: usize) {}
        /// Records current thread load statistics.
        pub fn record(&mut self, _instant: Instant) {}
    }
}
#[cfg(not(target_os = "linux"))]
pub use self::other_os::ThreadLoadStatistics;
