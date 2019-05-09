// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::coprocessor::dag::exec_summary::ExecSummary;

/// Data to be flowed between parent and child executors at once during `collect_statistics()`
/// invocation.
///
/// Each batch executor aggregates and updates corresponding slots in this structure.
pub struct BatchExecuteStatistics {
    /// The execution summary of each executor. If execution summary is not needed, it will
    /// be zero sized.
    pub summary_per_executor: Vec<ExecSummary>,

    /// For each range given in the request, how many rows are scanned.
    pub scanned_rows_per_range: Vec<usize>,

    /// Scanning statistics for each CF during execution.
    pub cf_stats: crate::storage::Statistics,
}

impl BatchExecuteStatistics {
    /// Creates a new statistics instance.
    ///
    /// If execution summary does not need to be collected, it is safe to pass 0 to the `executors`
    /// argument, which will avoid one allocation.
    pub fn new(executors_len: usize, ranges_len: usize) -> Self {
        Self {
            summary_per_executor: vec![ExecSummary::default(); executors_len],
            scanned_rows_per_range: vec![0; ranges_len],
            cf_stats: crate::storage::Statistics::default(),
        }
    }

    /// Clears the statistics instance.
    pub fn clear(&mut self) {
        for item in self.summary_per_executor.iter_mut() {
            *item = ExecSummary::default();
        }
        for item in self.scanned_rows_per_range.iter_mut() {
            *item = 0;
        }
        self.cf_stats = crate::storage::Statistics::default();
    }
}
