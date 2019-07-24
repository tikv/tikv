// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use kvproto::coprocessor::KeyRange;
use tipb::schema::ColumnInfo;

use super::{Executor, Row};
use crate::coprocessor::codec::table;
use crate::coprocessor::dag::execute_stats::ExecuteStats;
use crate::coprocessor::dag::expr::EvalWarnings;
use crate::coprocessor::dag::storage::scanner::RangesScanner;
use crate::coprocessor::dag::storage::{IntervalRange, Range};
use crate::coprocessor::dag::storage_impl::TiKVStorage;
use crate::coprocessor::Result;
use crate::storage::{Statistics, Store};

// an InnerExecutor is used in ScanExecutor,
// hold the different logics between table scan and index scan
pub trait InnerExecutor: Send {
    fn decode_row(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        columns: Arc<Vec<ColumnInfo>>,
    ) -> Result<Option<Row>>;
}

// Executor for table scan and index scan
pub struct ScanExecutor<S: Store, T: InnerExecutor> {
    inner: T,
    scanner: RangesScanner<TiKVStorage<S>>,
    columns: Arc<Vec<ColumnInfo>>,
}

impl<S: Store, T: InnerExecutor> ScanExecutor<S, T> {
    pub fn new(
        inner: T,
        is_backward: bool,
        columns: Vec<ColumnInfo>,
        mut key_ranges: Vec<KeyRange>,
        store: S,
        accept_point_range: bool,
        is_key_only: bool,
        is_scanned_range_aware: bool,
    ) -> Result<Self> {
        box_try!(table::check_table_ranges(&key_ranges));
        if is_backward {
            key_ranges.reverse();
        }

        let scanner = RangesScanner::new(
            TiKVStorage::from(store),
            key_ranges
                .into_iter()
                .map(|r| Range::from_pb_range(r, accept_point_range))
                .collect(),
            is_backward,
            is_key_only,
            is_scanned_range_aware,
        );

        Ok(Self {
            inner,
            scanner,
            columns: Arc::new(columns),
        })
    }
}

impl<S: Store, T: InnerExecutor> Executor for ScanExecutor<S, T> {
    fn next(&mut self) -> Result<Option<Row>> {
        let some_row = self.scanner.next()?;
        if let Some((key, value)) = some_row {
            self.inner.decode_row(key, value, self.columns.clone())
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
    fn collect_storage_stats(&mut self, dest: &mut Statistics) {
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
}
