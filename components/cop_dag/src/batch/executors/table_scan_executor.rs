// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use cop_datatype::{EvalType, FieldTypeAccessor};
use kvproto::coprocessor::KeyRange;
use tipb::executor::TableScan;
use tipb::expression::FieldType;
use tipb::schema::ColumnInfo;

use crate::storage::Store;
use tikv_util::collections::HashMap;

use crate::batch::interface::*;
use crate::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};
use crate::expr::{EvalConfig, EvalContext};
use crate::Result;
use crate::Scanner;

pub struct BatchTableScanExecutor<C: ExecSummaryCollector, S: Store>(
    super::util::scan_executor::ScanExecutor<
        C,
        S,
        TableScanExecutorImpl,
        super::util::ranges_iter::PointRangeEnable,
    >,
);

#[inline]
pub fn check_supported(descriptor: &TableScan) -> Result<()> {
    super::util::scan_executor::check_columns_info_supported(descriptor.get_columns())
        .map_err(|e| box_err!("Unable to use BatchTableScanExecutor: {}", e))
}

impl<C: ExecSummaryCollector, S: Store> BatchTableScanExecutor<C, S> {
    pub fn new(
        summary_collector: C,
        store: S,
        config: Arc<EvalConfig>,
        columns_info: Vec<ColumnInfo>,
        key_ranges: Vec<KeyRange>,
        desc: bool,
    ) -> Result<Self> {
        let is_column_filled = vec![false; columns_info.len()];
        let mut key_only = true;
        let mut handle_index = None;
        let mut schema = Vec::with_capacity(columns_info.len());
        let mut columns_default_value = Vec::with_capacity(columns_info.len());
        let mut column_id_index = HashMap::default();

        for (index, mut ci) in columns_info.into_iter().enumerate() {
            // For each column info, we need to extract the following info:
            // - Corresponding field type (push into `schema`).
            schema.push(super::util::scan_executor::field_type_from_column_info(&ci));

            // - Prepare column default value (will be used to fill missing column later).
            columns_default_value.push(ci.take_default_val());

            // - Store the index of the PK handle.
            // - Check whether or not we don't need KV values (iff PK handle is given).
            if ci.get_pk_handle() {
                handle_index = Some(index);
            } else {
                key_only = false;
                column_id_index.insert(ci.get_column_id(), index);
            }

            // Note: if two PK handles are given, we will only preserve the *last* one. Also if two
            // columns with the same column id are given, we will only preserve the *last* one.
        }

        let imp = TableScanExecutorImpl {
            context: EvalContext::new(config),
            schema,
            columns_default_value,
            column_id_index,
            key_only,
            handle_index,
            is_column_filled,
        };
        let wrapper = super::util::scan_executor::ScanExecutor::new(
            summary_collector,
            imp,
            store,
            desc,
            key_ranges,
            super::util::ranges_iter::PointRangeEnable,
        )?;
        Ok(Self(wrapper))
    }
}

impl<C: ExecSummaryCollector, S: Store> BatchExecutor for BatchTableScanExecutor<C, S> {
    #[inline]
    fn schema(&self) -> &[FieldType] {
        self.0.schema()
    }

    #[inline]
    fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult {
        self.0.next_batch(scan_rows)
    }

    #[inline]
    fn collect_statistics(&mut self, destination: &mut BatchExecuteStatistics) {
        self.0.collect_statistics(destination);
    }
}

struct TableScanExecutorImpl {
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

    /// Whether or not KV value can be omitted.
    ///
    /// It will be set to `true` if only PK handle column exists in `schema`.
    key_only: bool,

    /// The index in output row to put the handle.
    handle_index: Option<usize>,

    /// A vector of flags indicating whether corresponding column is filled in `next_batch`.
    /// It is a struct level field in order to prevent repeated memory allocations since its length
    /// is fixed for each `next_batch` call.
    is_column_filled: Vec<bool>,
}

impl super::util::scan_executor::ScanExecutorImpl for TableScanExecutorImpl {
    #[inline]
    fn schema(&self) -> &[FieldType] {
        &self.schema
    }

    #[inline]
    fn mut_context(&mut self) -> &mut EvalContext {
        &mut self.context
    }

    #[inline]
    fn build_scanner<S: Store>(
        &self,
        store: &S,
        desc: bool,
        range: KeyRange,
    ) -> Result<Scanner<S>> {
        Scanner::new(store, crate::ScanOn::Table, desc, self.key_only, range)
    }

    /// Constructs empty columns, with PK in decoded format and the rest in raw format.
    fn build_column_vec(&self, scan_rows: usize) -> LazyBatchColumnVec {
        let columns_len = self.schema.len();
        let mut columns = Vec::with_capacity(columns_len);

        if let Some(handle_index) = self.handle_index {
            // PK is specified in schema. PK column should be decoded and the rest is in raw format
            // like this:
            // non-pk non-pk non-pk pk non-pk non-pk non-pk
            //                      ^handle_index = 3
            //                                             ^columns_len = 7

            // Columns before `handle_index` (if any) should be raw.
            for _ in 0..handle_index {
                columns.push(LazyBatchColumn::raw_with_capacity(scan_rows));
            }
            // For PK handle, we construct a decoded `VectorValue` because it is directly
            // stored as i64, without a datum flag, at the end of key.
            columns.push(LazyBatchColumn::decoded_with_capacity_and_tp(
                scan_rows,
                EvalType::Int,
            ));
            // Columns after `handle_index` (if any) should also be raw.
            for _ in handle_index + 1..columns_len {
                columns.push(LazyBatchColumn::raw_with_capacity(scan_rows));
            }
        } else {
            // PK is unspecified in schema. All column should be in raw format.
            for _ in 0..columns_len {
                columns.push(LazyBatchColumn::raw_with_capacity(scan_rows));
            }
        }

        assert_eq!(columns.len(), columns_len);
        LazyBatchColumnVec::from(columns)
    }

    fn process_kv_pair(
        &mut self,
        key: &[u8],
        value: &[u8],
        columns: &mut LazyBatchColumnVec,
    ) -> Result<()> {
        use crate::codec::{datum, table};
        use tikv_util::codec::number;

        let columns_len = self.schema.len();
        let mut decoded_columns = 0;

        if let Some(handle_index) = self.handle_index {
            let handle_id = table::decode_handle(key)?;
            // TODO: We should avoid calling `push_int` repeatedly. Instead we should specialize
            // a `&mut Vec` first. However it is hard to program due to lifetime restriction.
            columns[handle_index]
                .mut_decoded()
                .push_int(Some(handle_id));
            decoded_columns += 1;
            self.is_column_filled[handle_index] = true;
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
                    return Err(box_err!("Unable to decode row: column id must be VAR_INT"));
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
                        columns[index].push_raw(val);
                        decoded_columns += 1;
                        self.is_column_filled[index] = true;
                    } else {
                        // This indicates that there are duplicated elements in the row, which is
                        // unexpected. We won't abort the request or overwrite the previous element,
                        // but will output a log anyway.
                        warn!(
                            "Ignored duplicated row datum in table scan";
                            "key" => log_wrappers::Key(&key),
                            "value" => log_wrappers::Key(&value),
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
                    .flag()
                    .contains(cop_datatype::FieldTypeFlag::NOT_NULL)
                {
                    // NULL is allowed, use NULL
                    datum::DATUM_DATA_NULL
                } else {
                    return Err(box_err!(
                        "Data is corrupted, missing data for NOT NULL column (offset = {})",
                        i
                    ));
                };

                columns[i].push_raw(default_value);
            } else {
                // Reset to not-filled, prepare for next function call.
                self.is_column_filled[i] = false;
            }
        }

        Ok(())
    }
}
