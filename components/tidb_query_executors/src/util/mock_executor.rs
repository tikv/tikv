// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_common::storage::IntervalRange;
use tidb_query_datatype::{
    codec::{batch::LazyBatchColumnVec, data_type::VectorValue},
    expr::EvalWarnings,
};
use tipb::FieldType;

use crate::interface::*;

/// A simple mock executor that will return batch data according to a fixture without any
/// modification.
///
/// Normally this should be only used in tests.
pub struct MockExecutor {
    schema: Vec<FieldType>,
    results: std::vec::IntoIter<BatchExecuteResult>,
}

impl MockExecutor {
    pub fn new(schema: Vec<FieldType>, results: Vec<BatchExecuteResult>) -> Self {
        assert!(!results.is_empty());
        Self {
            schema,
            results: results.into_iter(),
        }
    }
}

impl BatchExecutor for MockExecutor {
    type StorageStats = ();

    fn schema(&self) -> &[FieldType] {
        &self.schema
    }

    fn next_batch(&mut self, _scan_rows: usize) -> BatchExecuteResult {
        self.results.next().unwrap()
    }

    fn collect_exec_stats(&mut self, _dest: &mut ExecuteStats) {
        // Do nothing
    }

    fn collect_storage_stats(&mut self, _dest: &mut Self::StorageStats) {
        // Do nothing
    }

    fn take_scanned_range(&mut self) -> IntervalRange {
        // Do nothing
        unreachable!()
    }

    fn can_be_cached(&self) -> bool {
        false
    }
}

pub struct MockScanExecutor {
    pub rows: Vec<i64>,
    pub pos: usize,
    schema: Vec<FieldType>,
}

impl MockScanExecutor {
    pub fn new(rows: Vec<i64>, schema: Vec<FieldType>) -> Self {
        MockScanExecutor {
            rows,
            pos: 0,
            schema,
        }
    }
}

impl BatchExecutor for MockScanExecutor {
    type StorageStats = ();

    fn schema(&self) -> &[FieldType] {
        &self.schema
    }

    fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult {
        let real_scan_rows = std::cmp::min(scan_rows, self.rows.len());
        // just one column
        let mut res_col = Vec::new();
        let mut res_logical_rows = Vec::new();
        let mut cur_row_idx = 0;
        while self.pos < self.rows.len() && cur_row_idx < real_scan_rows {
            res_col.push(Some(self.rows[self.pos]));
            res_logical_rows.push(cur_row_idx);
            self.pos += 1;
            cur_row_idx += 1;
        }
        let is_drained = self.pos >= self.rows.len();
        BatchExecuteResult {
            physical_columns: LazyBatchColumnVec::from(vec![VectorValue::Int(res_col.into())]),
            logical_rows: res_logical_rows,
            warnings: EvalWarnings::default(),
            is_drained: Ok(is_drained),
        }
    }

    fn collect_exec_stats(&mut self, _dest: &mut ExecuteStats) {
        // Do nothing
    }

    fn collect_storage_stats(&mut self, _dest: &mut Self::StorageStats) {
        // Do nothing
    }

    fn take_scanned_range(&mut self) -> IntervalRange {
        // Do nothing
        unreachable!()
    }

    fn can_be_cached(&self) -> bool {
        false
    }
}
