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

use prometheus::local::LocalIntCounterVec;
use std::time::Duration;
use storage::engine::Statistics;
use util::time::Instant;

/// `ExecutorMetrics` is metrics collected from executors group by request.
#[derive(Default, Debug)]
pub struct ExecutorMetrics {
    pub cf_stats: Statistics,
    pub scan_counter: ScanCounter,
    pub executor_count: ExecCounter,
    pub runtime_stats: RuntimeStats,
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

    pub fn record_runtime_stats(&mut self, now: Instant) -> Guard {
        return Guard {
            now: now,
            runtime_stats: &mut self.runtime_stats,
        };

        /*
        self.runtime_stats.record_and_set(
            d,
            self.scan_counter
                .range
                .checked_add(self.scan_counter.point)
                .unwrap() as i64,
        );
        */
    }
}

/// `ScanCounter` is for recording range query and point query.
#[derive(Default, Clone, Debug)]
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

    pub fn consume(self, metrics: &mut LocalIntCounterVec) {
        if self.point > 0 {
            metrics
                .with_label_values(&["point"])
                .inc_by(self.point as i64);
        }
        if self.range > 0 {
            metrics
                .with_label_values(&["range"])
                .inc_by(self.range as i64);
        }
    }
}

/// `ExecCounter` is for recording number of each executor.
#[derive(Default, Debug)]
pub struct ExecCounter {
    pub aggregation: i64,
    pub index_scan: i64,
    pub limit: i64,
    pub selection: i64,
    pub table_scan: i64,
    pub topn: i64,
}

impl ExecCounter {
    pub fn merge(&mut self, other: &mut ExecCounter) {
        self.aggregation += other.aggregation;
        self.index_scan += other.index_scan;
        self.limit += other.limit;
        self.selection += other.selection;
        self.table_scan += other.table_scan;
        self.topn += other.topn;
        *other = ExecCounter::default();
    }

    pub fn consume(self, metrics: &mut LocalIntCounterVec) {
        metrics
            .with_label_values(&["tblscan"])
            .inc_by(self.table_scan);
        metrics
            .with_label_values(&["idxscan"])
            .inc_by(self.index_scan);
        metrics
            .with_label_values(&["selection"])
            .inc_by(self.selection);
        metrics.with_label_values(&["topn"]).inc_by(self.topn);
        metrics.with_label_values(&["limit"]).inc_by(self.limit);
        metrics
            .with_label_values(&["aggregation"])
            .inc_by(self.aggregation);
    }
}

// RuntimeStats collects one executor's execution info.
#[derive(Debug, Default)]
pub struct RuntimeStats {
    // executor consume time.
    pub consume: Duration,
    // executor's Next() called times.
    pub count: i64,
    // executor return row count.
    pub rows: i64,
}

impl RuntimeStats {
    // Record records executor's execution.
    pub fn record(&mut self, d: Duration, row_num: i64) {
        self.consume += d;
        self.count += 1;
        self.rows += row_num;
    }

    // Record and set records executor's elapsed time and set row_num
    pub fn record_and_set(&mut self, d: Duration, row_num: i64) {
        self.consume += d;
        self.count += 1;
        self.rows = row_num;
    }
}

#[derive(Debug)]
pub struct Guard<'a> {
    //pub struct Guard {
    pub now: Instant,
    pub runtime_stats: &'a mut RuntimeStats,
}

impl<'a> Drop for Guard<'a> {
    //impl Drop for Guard {
    fn drop(&mut self) {
        self.runtime_stats.consume += self.now.elapsed();
        self.runtime_stats.rows += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record() {
        let mut runtime_stats = RuntimeStats {
            consume: Duration::new(0, 1),
            count: 2,
            rows: 3,
        };
        let cases = vec![(1, 1, 2, 3, 4), (2, 2, 4, 4, 6)];

        for (d, row_num, exp_time, exp_count, exp_rows) in cases {
            runtime_stats.record(Duration::from_nanos(d), row_num);
            assert_eq!(runtime_stats.consume.as_nanos(), exp_time);
            assert_eq!(runtime_stats.count, exp_count);
            assert_eq!(runtime_stats.rows, exp_rows);
        }
    }

    #[test]
    fn test_record_and_set() {
        let mut runtime_stats = RuntimeStats {
            consume: Duration::new(0, 1),
            count: 2,
            rows: 3,
        };
        let cases = vec![(1, 1, 2, 3, 1), (2, 2, 4, 4, 2)];

        for (d, row_num, exp_time, exp_count, exp_rows) in cases {
            runtime_stats.record_and_set(Duration::from_nanos(d), row_num);
            assert_eq!(runtime_stats.consume.as_nanos(), exp_time);
            assert_eq!(runtime_stats.count, exp_count);
            assert_eq!(runtime_stats.rows, exp_rows);
        }
    }
}
