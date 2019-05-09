// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::exec_summary::ExecSummary;

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
