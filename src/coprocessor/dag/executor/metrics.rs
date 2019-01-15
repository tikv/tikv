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
use storage::engine::Statistics;

/// `ExecutorMetrics` is metrics collected from executors group by request.
#[derive(Default, Debug)]
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

/// Collector that collects the number of scanned keys of each range.
pub trait CountCollector {
    /// Notifies the collector that a new range is started.
    fn handle_new_range(&mut self);

    /// Increases the counting of number of scanned keys of current range. If no range is started
    /// this function has no effect.
    fn inc_counter(&mut self);

    /// Takes and appends number of scanned keys into `target`.
    fn collect(&mut self, _target: &mut Vec<i64>);
}

/// A normal `CountCollector`.
#[derive(Default)]
pub struct CountCollectorNormal {
    counts: Vec<i64>,
}

impl CountCollector for CountCollectorNormal {
    #[inline]
    fn handle_new_range(&mut self) {
        self.counts.push(0);
    }

    #[inline]
    fn inc_counter(&mut self) {
        self.counts.last_mut().map_or((), |val| *val += 1);
    }

    #[inline]
    fn collect(&mut self, target: &mut Vec<i64>) {
        target.append(&mut self.counts);
        self.counts.push(0);
    }
}

/// A `CountCollector` that does not collect anything.
pub struct CountCollectorDisabled;

impl CountCollector for CountCollectorDisabled {
    #[inline]
    fn handle_new_range(&mut self) {}

    #[inline]
    fn inc_counter(&mut self) {}

    #[inline]
    fn collect(&mut self, _target: &mut Vec<i64>) {}
}

#[derive(Default)]
pub struct ExecutionSummary {
    /// Total time cost in this executor.
    pub time_processed_ms: ::std::sync::Arc<::std::sync::atomic::AtomicUsize>,

    /// How many rows this executor produced totally.
    pub num_produced_rows: usize,

    /// How many times executor's `next()` is called.
    pub num_iterations: usize,
}

pub trait ExecutionSummaryCollector {
    type TimeRecorderGuard;

    /// Creates a guard that will record and accumulate processed time when it is dropped.
    fn accumulate_time(&self) -> Self::TimeRecorderGuard;

    /// Increases produced rows counter.
    fn inc_produced_rows(&mut self);

    /// Increases iterations counter.
    fn inc_iterations(&mut self);

    /// Takes and appends current execution summary into `target`.
    fn collect(&mut self, _target: &mut [Option<ExecutionSummary>]);
}

pub struct ExecutionSummaryTimeGuard {
    start_time: ::util::time::Instant,
    collect_target: ::std::sync::Arc<::std::sync::atomic::AtomicUsize>,
}

impl Drop for ExecutionSummaryTimeGuard {
    #[inline]
    fn drop(&mut self) {
        let elapsed = (self.start_time.elapsed_secs() * 1000f64) as usize;
        self.collect_target
            .fetch_add(elapsed, ::std::sync::atomic::Ordering::Relaxed);
    }
}

/// A normal `ExecutionSummaryCollector`.
pub struct ExecutionSummaryCollectorNormal {
    output_index: usize,
    counts: ExecutionSummary,
}

impl ExecutionSummaryCollectorNormal {
    pub fn new(output_index: usize) -> ExecutionSummaryCollectorNormal {
        ExecutionSummaryCollectorNormal {
            output_index,
            counts: Default::default(),
        }
    }
}

impl ExecutionSummaryCollector for ExecutionSummaryCollectorNormal {
    type TimeRecorderGuard = ExecutionSummaryTimeGuard;

    #[inline]
    fn accumulate_time(&self) -> Self::TimeRecorderGuard {
        ExecutionSummaryTimeGuard {
            start_time: ::util::time::Instant::now_coarse(),
            collect_target: self.counts.time_processed_ms.clone(),
        }
    }

    #[inline]
    fn inc_produced_rows(&mut self) {
        self.counts.num_produced_rows += 1;
    }

    #[inline]
    fn inc_iterations(&mut self) {
        self.counts.num_iterations += 1;
    }

    #[inline]
    fn collect(&mut self, target: &mut [Option<ExecutionSummary>]) {
        let current_summary = ::std::mem::replace(&mut self.counts, ExecutionSummary::default());
        target[self.output_index] = Some(current_summary);
    }
}

/// A `ExecutionSummaryCollector` that does not collect anything.
pub struct ExecutionSummaryCollectorDisabled;

impl ExecutionSummaryCollector for ExecutionSummaryCollectorDisabled {
    type TimeRecorderGuard = ();

    #[inline]
    fn accumulate_time(&self) -> Self::TimeRecorderGuard {
        ()
    }

    #[inline]
    fn inc_produced_rows(&mut self) {}

    #[inline]
    fn inc_iterations(&mut self) {}

    #[inline]
    fn collect(&mut self, _target: &mut [Option<ExecutionSummary>]) {}
}
