// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use smallvec::SmallVec;
use std::sync::Arc;

use kvproto::coprocessor::KeyRange;
use tidb_query_datatype::{EvalType, FieldTypeAccessor};
use tikv_util::collections::HashMap;
use tipb::ColumnInfo;
use tipb::FieldType;
use tipb::TableScan;

use super::util::scan_executor::*;
use crate::batch::interface::*;
use crate::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};
use crate::expr::{EvalConfig, EvalContext};
use crate::storage::scanner2::{RangesScanner2, RangesScanner2Options};
use crate::storage::{IntervalRange, Range, Storage};
use crate::Result;

pub struct BatchTableRangeScanExecutor<S: Storage> {
    /// Note: Although called `EvalContext`, it is some kind of execution context instead.
    // TODO: Rename EvalContext to ExecContext.
    context: EvalContext,

    /// The schema of the output. All of the output come from specific columns in the underlying
    /// storage.
    schema: Vec<FieldType>,

    /// The default value of corresponding columns in the schema. When column data is missing,
    /// the default value will be used to fill the output.
    columns_default_value: Vec<Vec<u8>>,

    /// The output position in the schema giving the column id.
    column_id_index: HashMap<i64, usize>,

    /// Vec of indices in output row to put the handle. The indices must be sorted in the vec.
    handle_indices: HandleIndicesVec,

    /// A vector of flags indicating whether corresponding column is filled in `next_batch`.
    /// It is a struct level field in order to prevent repeated memory allocations since its length
    /// is fixed for each `next_batch` call.
    is_column_filled: Vec<bool>,

    /// The scanner that scans over ranges.
    scanner: RangesScanner2<S>,

    /// A flag indicating whether this executor is ended. When table is drained or there was an
    /// error scanning the table, this flag will be set to `true` and `next_batch` should be never
    /// called again.
    is_ended: bool,
}

type HandleIndicesVec = SmallVec<[usize; 2]>;

// We assign a dummy type `Box<dyn Storage<Statistics = ()>>` so that we can omit the type
// when calling `check_supported`.
impl BatchTableRangeScanExecutor<Box<dyn Storage<Statistics = ()>>> {
    /// Checks whether this executor can be used.
    #[inline]
    pub fn check_supported(descriptor: &TableScan) -> Result<()> {
        check_columns_info_supported(descriptor.get_columns())
    }
}

impl<S: Storage> BatchTableRangeScanExecutor<S> {
    pub fn new(
        storage: S,
        config: Arc<EvalConfig>,
        columns_info: Vec<ColumnInfo>,
        key_ranges: Vec<KeyRange>,
    ) -> Result<Self> {
        let is_column_filled = vec![false; columns_info.len()];
        let mut is_key_only = true;
        let mut handle_indices = HandleIndicesVec::new();
        let mut schema = Vec::with_capacity(columns_info.len());
        let mut columns_default_value = Vec::with_capacity(columns_info.len());
        let mut column_id_index = HashMap::default();

        for (index, mut ci) in columns_info.into_iter().enumerate() {
            // For each column info, we need to extract the following info:
            // - Corresponding field type (push into `schema`).
            schema.push(field_type_from_column_info(&ci));

            // - Prepare column default value (will be used to fill missing column later).
            columns_default_value.push(ci.take_default_val());

            // - Store the index of the PK handles.
            // - Check whether or not we don't need KV values (iff PK handle is given).
            if ci.get_pk_handle() {
                handle_indices.push(index);
            } else {
                is_key_only = false;
                column_id_index.insert(ci.get_column_id(), index);
            }

            // Note: if two PK handles are given, we will only preserve the *last* one. Also if two
            // columns with the same column id are given, we will only preserve the *last* one.
        }
        Ok(Self {
            context: EvalContext::new(config),
            schema,
            columns_default_value,
            column_id_index,
            handle_indices,
            is_column_filled,
            scanner: RangesScanner2::new(RangesScanner2Options {
                storage,
                ranges: key_ranges
                    .into_iter()
                    .map(|r| Range::from_pb_range(r, true))
                    .collect(),
                is_key_only,
            }),
            is_ended: false,
        })
    }

    /// Constructs empty columns, with PK in decoded format and the rest in raw format.
    fn build_column_vec(&self, scan_rows: usize) -> LazyBatchColumnVec {
        let columns_len = self.schema.len();
        let mut columns = Vec::with_capacity(columns_len);

        // If there are any PK columns, for each of them, fill non-PK columns before it and push the
        // PK column.
        // For example, consider:
        //                  non-pk non-pk non-pk pk non-pk non-pk pk pk non-pk non-pk
        // handle_indices:                       ^3               ^6 ^7
        // Each turn of the following loop will push this to `columns`:
        // 1st turn: [non-pk, non-pk, non-pk, pk]
        // 2nd turn: [non-pk, non-pk, pk]
        // 3rd turn: [pk]
        let mut last_index = 0usize;
        for handle_index in &self.handle_indices {
            // `handle_indices` is expected to be sorted.
            assert!(*handle_index >= last_index);

            // Fill last `handle_index - 1` columns.
            for _ in last_index..*handle_index {
                columns.push(LazyBatchColumn::raw_with_capacity(scan_rows));
            }

            // For PK handles, we construct a decoded `VectorValue` because it is directly
            // stored as i64, without a datum flag, at the end of key.
            columns.push(LazyBatchColumn::decoded_with_capacity_and_tp(
                scan_rows,
                EvalType::Int,
            ));

            last_index = *handle_index + 1;
        }

        // Then fill remaining columns after the last handle column. If there are no PK columns,
        // the previous loop will be skipped and this loop will be run on 0..columns_len.
        // For the example above, this loop will push: [non-pk, non-pk]
        for _ in last_index..columns_len {
            columns.push(LazyBatchColumn::raw_with_capacity(scan_rows));
        }

        assert_eq!(columns.len(), columns_len);
        LazyBatchColumnVec::from(columns)
    }

    fn fill_column_vec(
        &mut self,
        scan_rows: usize,
        columns: &mut LazyBatchColumnVec,
    ) -> Result<bool> {
        assert!(scan_rows > 0);

        let scanned_rows = self.scanner.next_batch(scan_rows)?;
        if scanned_rows > 0 {
            if let Err(e) = self.process_rows(scanned_rows, columns) {
                // When there are errors in `process_kv_pair`, columns' length may not be
                // identical. For example, the filling process may be partially done so that
                // first several columns have N rows while the rest have N-1 rows. Since we do
                // not immediately fail when there are errors, these irregular columns may
                // further cause future executors to panic. So let's truncate these columns to
                // make they all have N-1 rows in that case.
                columns.truncate_into_equal_length();
                return Err(e);
            }
        }

        // Drained if scanner returns rows less than we specified.
        Ok(scanned_rows < scan_rows)
    }

    fn process_rows(&mut self, rows: usize, columns: &mut LazyBatchColumnVec) -> Result<()> {
        use crate::codec::{datum, table};
        use tikv_util::codec::number;

        let columns_len = self.schema.len();

        for r in 0..rows {
            let key = &self.scanner.keys()[r];
            let value = &self.scanner.values()[r];

            let mut decoded_columns = 0;
            if !self.handle_indices.is_empty() {
                let handle_id = table::decode_handle(key)?;
                for handle_index in &self.handle_indices {
                    // TODO: We should avoid calling `push_int` repeatedly. Instead we should specialize
                    // a `&mut Vec` first. However it is hard to program due to lifetime restriction.
                    columns[*handle_index]
                        .mut_decoded()
                        .push_int(Some(handle_id));
                    decoded_columns += 1;
                    self.is_column_filled[*handle_index] = true;
                }
            }

            if value.is_empty() || (value.len() == 1 && value[0] == datum::NIL_FLAG) {
                // Do nothing
            } else {
                // The layout of value is: [col_id_1, value_1, col_id_2, value_2, ...]
                // where each element is datum encoded.
                // The column id datum must be in var i64 type.
                let mut remaining = value;
                while !remaining.is_empty() && decoded_columns < columns_len {
                    if remaining[0] != datum::VAR_INT_FLAG {
                        return Err(other_err!(
                            "Unable to decode row: column id must be VAR_INT"
                        ));
                    }
                    remaining = &remaining[1..];
                    let column_id = box_try!(number::decode_var_i64(&mut remaining));
                    let (val, new_remaining) = datum::split_datum(remaining, false)?;
                    // Note: The produced columns may be not in the same length if there is error due
                    // to corrupted data. It will be handled in `ScanExecutor`.
                    let some_index = self.column_id_index.get(&column_id);
                    if let Some(index) = some_index {
                        let index = *index;
                        if !self.is_column_filled[index] {
                            columns[index].mut_raw().push(val);
                            decoded_columns += 1;
                            self.is_column_filled[index] = true;
                        } else {
                            // This indicates that there are duplicated elements in the row, which is
                            // unexpected. We won't abort the request or overwrite the previous element,
                            // but will output a log anyway.
                            warn!(
                                "Ignored duplicated row datum in table scan";
                                "key" => hex::encode_upper(&key),
                                "value" => hex::encode_upper(&value),
                                "dup_column_id" => column_id,
                            );
                        }
                    }
                    remaining = new_remaining;
                }
            }

            // Some fields may be missing in the row, we push corresponding default value to make all
            // columns in same length.
            for i in 0..columns_len {
                if !self.is_column_filled[i] {
                    // Missing fields must not be a primary key, so it must be
                    // `LazyBatchColumn::raw`.

                    let default_value = if !self.columns_default_value[i].is_empty() {
                        // default value is provided, use the default value
                        self.columns_default_value[i].as_slice()
                    } else if !self.schema[i]
                        .as_accessor()
                        .flag()
                        .contains(tidb_query_datatype::FieldTypeFlag::NOT_NULL)
                    {
                        // NULL is allowed, use NULL
                        datum::DATUM_DATA_NULL
                    } else {
                        return Err(other_err!(
                            "Data is corrupted, missing data for NOT NULL column (offset = {})",
                            i
                        ));
                    };

                    columns[i].mut_raw().push(default_value);
                } else {
                    // Reset to not-filled, prepare for next function call.
                    self.is_column_filled[i] = false;
                }
            }
        }

        Ok(())
    }
}

impl<S: Storage> BatchExecutor for BatchTableRangeScanExecutor<S> {
    type StorageStats = S::Statistics;

    #[inline]
    fn schema(&self) -> &[FieldType] {
        &self.schema
    }

    #[inline]
    fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult {
        assert!(!self.is_ended);
        assert!(scan_rows > 0);

        let mut logical_columns = self.build_column_vec(scan_rows);
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
            warnings: self.context.take_warnings(),
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
    fn take_scanned_range(&mut self) -> IntervalRange {
        unimplemented!()
    }
}
