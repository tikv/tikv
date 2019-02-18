// Copyright 2019 PingCAP, Inc.
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

use storage::engine::{PerfStatisticsDelta, PerfStatisticsInstant};
use util::time::{Duration, Instant};

/// A PerfContext tracker for Storage.
///
/// It records PerfContext statistics when created, and logs delta PerfContext statistics as slow
/// operation when `track()` is called or it is dropped after specified slow threshold.
pub struct PerfTracker {
    cmd: &'static str,
    slow_log_threshold: Duration,
    time_start: Instant,
    perf_start: PerfStatisticsInstant,
    perf_delta: Option<PerfStatisticsDelta>,
}

impl PerfTracker {
    pub fn new(cmd: &'static str, slow_log_threshold: Duration) -> Self {
        Self {
            cmd,
            slow_log_threshold,
            time_start: Instant::now_coarse(),
            perf_start: PerfStatisticsInstant::new(),
            perf_delta: None,
        }
    }

    /// Records and returns the delta PerfContext. If elapsed time is larger than specified slow
    /// log threshold, a slow log will be printed.
    pub fn record(mut self) -> PerfStatisticsDelta {
        let perf_delta = self.perf_start.delta();
        self.perf_delta = Some(perf_delta);
        drop(self);
        perf_delta
    }
}

impl Drop for PerfTracker {
    /// Similar to `record()`, but does not return the delta PerfContext.
    fn drop(&mut self) {
        if self.perf_delta.is_none() {
            self.perf_delta = Some(self.perf_start.delta());
        }
        let elapsed = self.time_start.elapsed();
        if elapsed > self.slow_log_threshold {
            info!(
                "[slow-kv] cmd {:?} process takes {:?}, perf: {:?}",
                self.cmd,
                elapsed,
                self.perf_delta.take().unwrap(),
            );
        }
    }
}

impl !Send for PerfTracker {}
