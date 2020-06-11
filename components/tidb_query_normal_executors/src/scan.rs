// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use kvproto::coprocessor::KeyRange;
use tipb::ColumnInfo;

use super::{Executor, Row};
use tidb_query_common::execute_stats::ExecuteStats;
use tidb_query_common::storage::scanner::{RangesScanner, RangesScannerOptions};
use tidb_query_common::storage::{IntervalRange, Range, Storage};
use tidb_query_common::Result;
use tidb_query_datatype::codec::table;
use tidb_query_datatype::expr::{EvalContext, EvalWarnings};

// an InnerExecutor is used in ScanExecutor,
// hold the different logics between table scan and index scan
pub trait InnerExecutor: Send {
    fn decode_row(
        &self,
        ctx: &mut EvalContext,
        key: Vec<u8>,
        value: Vec<u8>,
        columns: Arc<Vec<ColumnInfo>>,
    ) -> Result<Option<Row>>;
}

// Executor for table scan and index scan
pub struct ScanExecutor<S: Storage, T: InnerExecutor> {
    inner: T,
    context: EvalContext,
    scanner: RangesScanner<S>,
    columns: Arc<Vec<ColumnInfo>>,
}

pub struct ScanExecutorOptions<S, T> {
    pub inner: T,
    pub context: EvalContext,
    pub columns: Vec<ColumnInfo>,
    pub key_ranges: Vec<KeyRange>,
    pub storage: S,
    pub is_backward: bool,
    pub is_key_only: bool,
    pub accept_point_range: bool,
    pub is_scanned_range_aware: bool,
}

impl<S: Storage, T: InnerExecutor> ScanExecutor<S, T> {
    pub fn new(
        ScanExecutorOptions {
            inner,
            context,
            columns,
            mut key_ranges,
            storage,
            is_backward,
            is_key_only,
            accept_point_range,
            is_scanned_range_aware,
        }: ScanExecutorOptions<S, T>,
    ) -> Result<Self> {
        box_try!(table::check_table_ranges(&key_ranges));
        if is_backward {
            key_ranges.reverse();
        }

        let scanner = RangesScanner::new(RangesScannerOptions {
            storage,
            ranges: key_ranges
                .into_iter()
                .map(|r| Range::from_pb_range(r, accept_point_range))
                .collect(),
            scan_backward_in_range: is_backward,
            is_key_only,
            is_scanned_range_aware,
        });

        Ok(Self {
            inner,
            context,
            scanner,
            columns: Arc::new(columns),
        })
    }
}

impl<S: Storage, T: InnerExecutor> Executor for ScanExecutor<S, T> {
    type StorageStats = S::Statistics;

    fn next(&mut self) -> Result<Option<Row>> {
        let some_row = self.scanner.next()?;
        if let Some((key, value)) = some_row {
            self.inner
                .decode_row(&mut self.context, key, value, self.columns.clone())
        } else {
            Ok(None)
        }
    }

    #[inline]
    fn collect_exec_stats(&mut self, dest: &mut ExecuteStats) {
        self.scanner
            .collect_scanned_rows_per_range(&mut dest.scanned_rows_per_range);
    }

    #[inline]
    fn collect_storage_stats(&mut self, dest: &mut Self::StorageStats) {
        self.scanner.collect_storage_stats(dest);
    }

    #[inline]
    fn get_len_of_columns(&self) -> usize {
        self.columns.len()
    }

    #[inline]
    fn take_eval_warnings(&mut self) -> Option<EvalWarnings> {
        None
    }

    #[inline]
    fn take_scanned_range(&mut self) -> IntervalRange {
        self.scanner.take_scanned_range()
    }

    #[inline]
    fn can_be_cached(&self) -> bool {
        self.scanner.can_be_cached()
    }
}
