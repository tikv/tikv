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

use storage::engine::Statistics;
use util::collections::HashMap;

/// `ExecutorMetrics` is metrics collected from executors group by request.
#[derive(Default)]
pub struct ExecutorMetrics {
    pub cf_stats: Statistics,
    pub scan_counter: ScanCounter,
    pub executor_count: ExecCounter,
}

impl ExecutorMetrics {
    /// Merge records from `other` into `self`, and clear `other`.
    #[inline]
    pub fn merge(&mut self, other: &mut ExecutorMetrics) {
        self.cf_stats.add(&other.cf_stats);
        other.cf_stats = Default::default();
        self.scan_counter.merge(&mut other.scan_counter);
        self.executor_count.merge(&mut other.executor_count);
    }
}

/// `ScanCounter` is for recording range query and point query.
#[derive(Default, Clone)]
pub struct ScanCounter {
    pub range: usize,
    pub point: usize,
}

impl ScanCounter {
    #[inline]
    pub fn inc_range(&mut self) {
        self.range += 1;
    }

    #[inline]
    pub fn inc_point(&mut self) {
        self.point += 1;
    }

    /// Merge records from `other` into `self`, and clear `other`.
    #[inline]
    pub fn merge(&mut self, other: &mut ScanCounter) {
        self.range += other.range;
        self.point += other.point;
        other.range = 0;
        other.point = 0;
    }
}

/// `ExecCounter` is for recording number of each executor.
#[derive(Default)]
pub struct ExecCounter {
    pub data: HashMap<&'static str, i64>,
}

impl ExecCounter {
    pub fn increase(&mut self, tag: &'static str) {
        let count = self.data.entry(tag).or_insert(0);
        *count += 1;
    }

    pub fn merge(&mut self, other: &mut ExecCounter) {
        for (k, v) in other.data.drain() {
            let mut va = self.data.entry(k).or_insert(0);
            *va += v;
        }
    }
}
