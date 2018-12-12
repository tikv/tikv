// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct ThreadLoad {
    term: AtomicUsize,
    load: AtomicUsize,
    threshold: usize,
}

impl ThreadLoad {
    pub fn with_threshold(threshold: usize) -> Self {
        ThreadLoad {
            term: AtomicUsize::new(0),
            load: AtomicUsize::new(0),
            threshold,
        }
    }

    #[allow(dead_code)]
    pub fn in_heavy_load(&self) -> bool {
        self.load.load(Ordering::Acquire) > self.threshold
    }

    /// Incease when every time updating `load`.
    #[allow(dead_code)]
    pub fn term(&self) -> usize {
        self.term.load(Ordering::Acquire)
    }

    /// For example, 200 means the threads eat 200% CPU.
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

    pub struct ThreadLoadStatistics {}

    impl ThreadLoadStatistics {
        pub fn new(_slots: usize, _prefix: &str, _thread_load: Arc<ThreadLoad>) -> Self {
            ThreadLoadStatistics {}
        }
        pub fn record(&mut self, _instant: Instant) {}
    }
}
#[cfg(not(target_os = "linux"))]
pub use self::other_os::ThreadLoadStatistics;
