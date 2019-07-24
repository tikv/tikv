// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::coprocessor::KeyRange;
use tipb::expression::FieldType;
use tipb::schema::ColumnInfo;

use crate::coprocessor::codec::batch::LazyBatchColumnVec;
use crate::coprocessor::dag::batch::interface::*;
use crate::coprocessor::dag::expr::EvalContext;
use crate::coprocessor::dag::storage::scanner::RangesScanner;
use crate::coprocessor::dag::storage::Range;
use crate::coprocessor::dag::storage_impl::TiKVStorage;
use crate::coprocessor::Result;
use crate::storage::{Statistics, Store};

/// Common interfaces for table scan and index scan implementations.
pub trait ScanExecutorImpl: Send {
    /// Gets the schema.
    fn schema(&self) -> &[FieldType];

    /// Gets a mutable reference of the executor context.
    fn mut_context(&mut self) -> &mut EvalContext;

    fn build_column_vec(&self, scan_rows: usize) -> LazyBatchColumnVec;

    /// Accepts a key value pair and fills the column vector.
    ///
    /// The column vector does not need to be regular when there are errors during this process.
    /// However if there is no error, the column vector must be regular.
    fn process_kv_pair(
        &mut self,
        key: &[u8],
        value: &[u8],
        columns: &mut LazyBatchColumnVec,
    ) -> Result<()>;
}

/// A shared executor implementation for both table scan and index scan. Implementation differences
/// between table scan and index scan are further given via `ScanExecutorImpl`.
pub struct ScanExecutor<S: Store, I: ScanExecutorImpl> {
    /// The internal scanning implementation.
    imp: I,

    /// The scanner that scans over ranges.
    scanner: RangesScanner<TiKVStorage<S>>,

    /// A flag indicating whether this executor is ended. When table is drained or there was an
    /// error scanning the table, this flag will be set to `true` and `next_batch` should be never
    /// called again.
    is_ended: bool,
}

impl<S: Store, I: ScanExecutorImpl> ScanExecutor<S, I> {
    pub fn new(
        imp: I,
        store: S,
        is_backward: bool,
        mut key_ranges: Vec<KeyRange>,
        is_key_only: bool,
        accept_point_range: bool,
    ) -> Result<Self> {
        crate::coprocessor::codec::table::check_table_ranges(&key_ranges)?;
        if is_backward {
            key_ranges.reverse();
        }
        Ok(Self {
            imp,
            scanner: RangesScanner::new(
                TiKVStorage::from(store),
                key_ranges
                    .into_iter()
                    .map(|r| Range::from_pb_range(r, accept_point_range))
                    .collect(),
                is_backward,
                is_key_only,
                false,
            ),
            is_ended: false,
        })
    }

    /// Fills a column vector and returns whether or not all ranges are drained.
    ///
    /// The columns are ensured to be regular even if there are errors during the process.
    fn fill_column_vec(
        &mut self,
        scan_rows: usize,
        columns: &mut LazyBatchColumnVec,
    ) -> Result<bool> {
        assert!(scan_rows > 0);

        for _ in 0..scan_rows {
            let some_row = self.scanner.next()?;
            if let Some((key, value)) = some_row {
                // Retrieved one row from point range or non-point range.

                if let Err(e) = self.imp.process_kv_pair(&key, &value, columns) {
                    // When there are errors in `process_kv_pair`, columns' length may not be
                    // identical. For example, the filling process may be partially done so that
                    // first several columns have N rows while the rest have N-1 rows. Since we do
                    // not immediately fail when there are errors, these irregular columns may
                    // further cause future executors to panic. So let's truncate these columns to
                    // make they all have N-1 rows in that case.
                    columns.truncate_into_equal_length();
                    return Err(e);
                }
            } else {
                // Drained
                return Ok(true);
            }
        }

        // Not drained
        Ok(false)
    }
}

/// Extracts `FieldType` from `ColumnInfo`.
// TODO: Embed FieldType in ColumnInfo directly in Cop DAG v2 to remove this function.
pub fn field_type_from_column_info(ci: &ColumnInfo) -> FieldType {
    let mut field_type = FieldType::default();
    field_type.set_tp(ci.get_tp());
    field_type.set_flag(ci.get_flag() as u32); // FIXME: This `as u32` is really awful.
    field_type.set_flen(ci.get_columnLen());
    field_type.set_decimal(ci.get_decimal());
    field_type.set_collate(ci.get_collation());
    // Note: Charset is not provided in column info.
    field_type
}

/// Checks whether the given columns info are supported.
pub fn check_columns_info_supported(columns_info: &[ColumnInfo]) -> Result<()> {
    use cop_datatype::EvalType;
    use cop_datatype::FieldTypeAccessor;
    use std::convert::TryFrom;

    for column in columns_info {
        if column.has_pk_handle() {
            box_try!(EvalType::try_from(column.tp()));
        }
    }
    Ok(())
}

impl<S: Store, I: ScanExecutorImpl> BatchExecutor for ScanExecutor<S, I> {
    #[inline]
    fn schema(&self) -> &[FieldType] {
        self.imp.schema()
    }

    #[inline]
    fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult {
        assert!(!self.is_ended);
        assert!(scan_rows > 0);

        let mut logical_columns = self.imp.build_column_vec(scan_rows);
        let is_drained = self.fill_column_vec(scan_rows, &mut logical_columns);

        logical_columns.assert_columns_equal_length();
        let logical_rows = (0..logical_columns.rows_len()).collect();

        // TODO
        // If `is_drained.is_err()`, it means that there is an error after *successfully* retrieving
        // these rows. After that, if we only consumes some of the rows (TopN / Limit), we should
        // ignore this error.

        match &is_drained {
            // Note: `self.is_ended` is only used for assertion purpose.
            Err(_) | Ok(true) => self.is_ended = true,
            Ok(false) => {}
        };

        BatchExecuteResult {
            physical_columns: logical_columns,
            logical_rows,
            is_drained,
            warnings: self.imp.mut_context().take_warnings(),
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
}
