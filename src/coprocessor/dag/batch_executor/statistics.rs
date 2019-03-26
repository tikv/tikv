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

use std::sync::{atomic, Arc};

/// Data to be flowed between parent and child executors at once during `collect_statistics()`
/// invocation.
///
/// Each batch executor aggregates and updates corresponding slots in this structure.
pub struct BatchExecuteStatistics {
    /// The execution summary of each executor.
    pub summary_per_executor: Vec<Option<ExecSummary>>,

    /// For each range given in the request, how many rows are scanned.
    pub scanned_rows_per_range: Vec<usize>,

    /// Scanning statistics for each CF during execution.
    pub cf_stats: crate::storage::Statistics,
}

impl BatchExecuteStatistics {
    pub fn new(executors: usize, ranges: usize) -> Self {
        let mut summary_per_executor = Vec::new();
        summary_per_executor.resize_with(executors, || None);

        Self {
            summary_per_executor,
            scanned_rows_per_range: vec![0; ranges],
            cf_stats: crate::storage::Statistics::default(),
        }
    }

    pub fn clear(&mut self) {
        for item in self.summary_per_executor.iter_mut() {
            *item = None;
        }
        for item in self.scanned_rows_per_range.iter_mut() {
            *item = 0;
        }
        self.cf_stats = crate::storage::Statistics::default();
    }
}

/// Execution summaries to support `EXPLAIN ANALYZE` statements.
#[derive(Default)]
pub struct ExecSummary {
    /// Total time cost in this executor.
    pub time_processed_ms: Arc<atomic::AtomicUsize>,

    /// How many rows this executor produced totally.
    pub num_produced_rows: usize,

    /// How many times executor's `next()` is called.
    pub num_iterations: usize,
}

impl ExecSummary {
    #[inline]
    pub fn merge_into(self, target: &mut Self) {
        target.time_processed_ms.fetch_add(
            self.time_processed_ms.load(atomic::Ordering::Relaxed),
            atomic::Ordering::Relaxed,
        );
        target.num_produced_rows += self.num_produced_rows;
        target.num_iterations += self.num_iterations;
    }
}

pub trait ExecSummaryCollector: Send {
    type TimeRecorderGuard;

    /// Creates a new instance with specified output slot index.
    fn new(output_index: usize) -> Self
    where
        Self: Sized;

    /// Creates a guard that will collect elapsed time as the scope duration when it is dropped.
    fn collect_scope_duration(&self) -> Self::TimeRecorderGuard;

    /// Increases produced rows counter.
    fn inc_produced_rows(&mut self, rows: usize);

    /// Increases iterations counter.
    fn inc_iterations(&mut self);

    /// Takes and appends current execution summary into `target`.
    fn collect_into(&mut self, target: &mut [Option<ExecSummary>]);
}

/// A helper to record duration for timing fields for `ExecSummary` based on `Drop`.
pub struct ExecSummaryTimeGuard {
    start_time: crate::util::time::Instant,
    collect_target: Arc<atomic::AtomicUsize>,
}

impl Drop for ExecSummaryTimeGuard {
    #[inline]
    fn drop(&mut self) {
        let elapsed = (self.start_time.elapsed_secs() * 1000f64) as usize;
        self.collect_target
            .fetch_add(elapsed, atomic::Ordering::Relaxed);
    }
}

/// A normal `ExecSummaryCollector`. Acts like `collect = true`.
pub struct ExecSummaryCollectorNormal {
    output_index: usize,
    counts: ExecSummary,
}

impl ExecSummaryCollector for ExecSummaryCollectorNormal {
    type TimeRecorderGuard = ExecSummaryTimeGuard;

    #[inline]
    fn new(output_index: usize) -> ExecSummaryCollectorNormal {
        ExecSummaryCollectorNormal {
            output_index,
            counts: Default::default(),
        }
    }

    #[inline]
    fn collect_scope_duration(&self) -> Self::TimeRecorderGuard {
        ExecSummaryTimeGuard {
            start_time: crate::util::time::Instant::now_coarse(),
            collect_target: self.counts.time_processed_ms.clone(),
        }
    }

    #[inline]
    fn inc_produced_rows(&mut self, rows: usize) {
        self.counts.num_produced_rows += rows;
    }

    #[inline]
    fn inc_iterations(&mut self) {
        self.counts.num_iterations += 1;
    }

    #[inline]
    fn collect_into(&mut self, target: &mut [Option<ExecSummary>]) {
        let current_summary = std::mem::replace(&mut self.counts, ExecSummary::default());
        if let Some(t) = &mut target[self.output_index] {
            current_summary.merge_into(t);
        } else {
            target[self.output_index] = Some(current_summary);
        }
    }
}

/// A `ExecSummaryCollector` that does not collect anything. Acts like `collect = false`.
pub struct ExecSummaryCollectorDisabled;

impl ExecSummaryCollector for ExecSummaryCollectorDisabled {
    type TimeRecorderGuard = ();

    #[inline]
    fn new(_output_index: usize) -> ExecSummaryCollectorDisabled {
        ExecSummaryCollectorDisabled
    }

    #[inline]
    fn collect_scope_duration(&self) -> Self::TimeRecorderGuard {}

    #[inline]
    fn inc_produced_rows(&mut self, _rows: usize) {}

    #[inline]
    fn inc_iterations(&mut self) {}

    #[inline]
    fn collect_into(&mut self, _target: &mut [Option<ExecSummary>]) {}
}
