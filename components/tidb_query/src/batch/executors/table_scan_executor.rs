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
use crate::codec::table::check_record_key;
use crate::expr::{EvalConfig, EvalContext};
use crate::storage::{IntervalRange, Storage};
use crate::Result;

pub struct BatchTableScanExecutor<S: Storage>(ScanExecutor<S, TableScanExecutorImpl>);

type HandleIndicesVec = SmallVec<[usize; 2]>;

// We assign a dummy type `Box<dyn Storage<Statistics = ()>>` so that we can omit the type
// when calling `check_supported`.
impl BatchTableScanExecutor<Box<dyn Storage<Statistics = ()>>> {
    /// Checks whether this executor can be used.
    #[inline]
    pub fn check_supported(descriptor: &TableScan) -> Result<()> {
        check_columns_info_supported(descriptor.get_columns())
    }
}

impl<S: Storage> BatchTableScanExecutor<S> {
    pub fn new(
        storage: S,
        config: Arc<EvalConfig>,
        columns_info: Vec<ColumnInfo>,
        key_ranges: Vec<KeyRange>,
        is_backward: bool,
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

        let imp = TableScanExecutorImpl {
            context: EvalContext::new(config),
            schema,
            columns_default_value,
            column_id_index,
            handle_indices,
            is_column_filled,
        };
        let wrapper = ScanExecutor::new(ScanExecutorOptions {
            imp,
            storage,
            key_ranges,
            is_backward,
            is_key_only,
            accept_point_range: true,
        })?;
        Ok(Self(wrapper))
    }
}

impl<S: Storage> BatchExecutor for BatchTableScanExecutor<S> {
    type StorageStats = S::Statistics;

    #[inline]
    fn schema(&self) -> &[FieldType] {
        self.0.schema()
    }

    #[inline]
    fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult {
        self.0.next_batch(scan_rows)
    }

    #[inline]
    fn collect_exec_stats(&mut self, dest: &mut ExecuteStats) {
        self.0.collect_exec_stats(dest);
    }

    #[inline]
    fn collect_storage_stats(&mut self, dest: &mut Self::StorageStats) {
        self.0.collect_storage_stats(dest);
    }

    #[inline]
    fn take_scanned_range(&mut self) -> IntervalRange {
        self.0.take_scanned_range()
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

    /// Vec of indices in output row to put the handle. The indices must be sorted in the vec.
    handle_indices: HandleIndicesVec,

    /// A vector of flags indicating whether corresponding column is filled in `next_batch`.
    /// It is a struct level field in order to prevent repeated memory allocations since its length
    /// is fixed for each `next_batch` call.
    is_column_filled: Vec<bool>,
}

impl ScanExecutorImpl for TableScanExecutorImpl {
    #[inline]
    fn schema(&self) -> &[FieldType] {
        &self.schema
    }

    #[inline]
    fn mut_context(&mut self) -> &mut EvalContext {
        &mut self.context
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

    fn process_kv_pair(
        &mut self,
        key: &[u8],
        value: &[u8],
        columns: &mut LazyBatchColumnVec,
    ) -> Result<()> {
        use crate::codec::{datum, table};
        use codec::prelude::NumberDecoder;

        check_record_key(&key)?;
        let columns_len = self.schema.len();
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
                let column_id = box_try!(remaining.read_var_i64());
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

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::iter;
    use std::sync::Arc;

    use kvproto::coprocessor::KeyRange;
    use tidb_query_datatype::{EvalType, FieldTypeAccessor, FieldTypeTp};
    use tipb::ColumnInfo;
    use tipb::FieldType;

    use crate::codec::batch::LazyBatchColumnVec;
    use crate::codec::data_type::*;
    use crate::codec::{datum, table, Datum};
    use crate::execute_stats::*;
    use crate::expr::EvalConfig;
    use crate::storage::fixture::FixtureStorage;
    use crate::util::convert_to_prefix_next;

    /// Test Helper for normal test with fixed schema and data.
    /// Table Schema:  ID (INT, PK),   Foo (INT),     Bar (FLOAT, Default 4.5)
    /// Column id:     1,              2,             4
    /// Column offset: 0,              1,             2
    /// Table Data:    1,              10,            5.2
    ///                3,              -5,            NULL
    ///                4,              NULL,          4.5 (DEFAULT)
    ///                5,              NULL,          0.1
    ///                6,              NULL,          4.5 (DEFAULT)
    struct TableScanTestHelper {
        // ID(INT,PK), Foo(INT), Bar(Float,Default 4.5)
        pub data: Vec<(i64, Option<i64>, Option<Real>)>,
        pub table_id: i64,
        pub columns_info: Vec<ColumnInfo>,
        pub field_types: Vec<FieldType>,
        pub store: FixtureStorage,
    }

    impl TableScanTestHelper {
        /// create the TableScanTestHelper with fixed schema and data.
        fn new() -> TableScanTestHelper {
            const TABLE_ID: i64 = 7;
            // [(row_id, columns)] where each column: (column id, datum)
            let data = vec![
                (
                    1,
                    vec![
                        // A full row.
                        (2, Datum::I64(10)),
                        (4, Datum::F64(5.2)),
                    ],
                ),
                (
                    3,
                    vec![
                        (4, Datum::Null),
                        // Bar column is null, even if default value is provided the final result
                        // should be null.
                        (2, Datum::I64(-5)),
                        // Orders should not matter.
                    ],
                ),
                (
                    4,
                    vec![
                        (2, Datum::Null),
                        // Bar column is missing, default value should be used.
                    ],
                ),
                (
                    5,
                    vec![
                        // Foo column is missing, NULL should be used.
                        (4, Datum::F64(0.1)),
                    ],
                ),
                (
                    6,
                    vec![
                        // Empty row
                    ],
                ),
            ];

            let expect_rows = vec![
                (1, Some(10), Real::new(5.2).ok()),
                (3, Some(-5), None),
                (4, None, Real::new(4.5).ok()),
                (5, None, Real::new(0.1).ok()),
                (6, None, Real::new(4.5).ok()),
            ];

            let mut ctx = EvalContext::default();

            // The column info for each column in `data`.
            let columns_info = vec![
                {
                    let mut ci = ColumnInfo::default();
                    ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                    ci.set_pk_handle(true);
                    ci.set_column_id(1);
                    ci
                },
                {
                    let mut ci = ColumnInfo::default();
                    ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                    ci.set_column_id(2);
                    ci
                },
                {
                    let mut ci = ColumnInfo::default();
                    ci.as_mut_accessor().set_tp(FieldTypeTp::Double);
                    ci.set_column_id(4);
                    ci.set_default_val(datum::encode_value(&mut ctx, &[Datum::F64(4.5)]).unwrap());
                    ci
                },
            ];

            let field_types = vec![
                FieldTypeTp::LongLong.into(),
                FieldTypeTp::LongLong.into(),
                FieldTypeTp::Double.into(),
            ];

            let store = {
                let kv: Vec<_> = data
                    .iter()
                    .map(|(row_id, columns)| {
                        let key = table::encode_row_key(TABLE_ID, *row_id);
                        let value = {
                            let row = columns.iter().map(|(_, datum)| datum.clone()).collect();
                            let col_ids: Vec<_> = columns.iter().map(|(id, _)| *id).collect();
                            table::encode_row(&mut ctx, row, &col_ids).unwrap()
                        };
                        (key, value)
                    })
                    .collect();
                FixtureStorage::from(kv)
            };

            TableScanTestHelper {
                data: expect_rows,
                table_id: TABLE_ID,
                columns_info,
                field_types,
                store,
            }
        }

        /// The point range representation for each row in `data`.
        fn point_ranges(&self) -> Vec<KeyRange> {
            self.data
                .iter()
                .map(|(row_id, _, _)| {
                    let mut r = KeyRange::default();
                    r.set_start(table::encode_row_key(self.table_id, *row_id));
                    r.set_end(r.get_start().to_vec());
                    convert_to_prefix_next(r.mut_end());
                    r
                })
                .collect()
        }

        /// Returns whole table's ranges which include point range and non-point range.
        fn mixed_ranges_for_whole_table(&self) -> Vec<KeyRange> {
            vec![
                self.table_range(std::i64::MIN, 3),
                {
                    let mut r = KeyRange::default();
                    r.set_start(table::encode_row_key(self.table_id, 3));
                    r.set_end(r.get_start().to_vec());
                    convert_to_prefix_next(r.mut_end());
                    r
                },
                self.table_range(4, std::i64::MAX),
            ]
        }

        fn store(&self) -> FixtureStorage {
            self.store.clone()
        }

        /// index of pk in self.columns_info.
        fn idx_pk(&self) -> usize {
            0
        }

        fn columns_info_by_idx(&self, col_index: &[usize]) -> Vec<ColumnInfo> {
            col_index
                .iter()
                .map(|id| self.columns_info[*id].clone())
                .collect()
        }

        /// Get column's field type by the index in self.columns_info.
        fn get_field_type(&self, col_idx: usize) -> &FieldType {
            &self.field_types[col_idx]
        }

        /// Returns the range for handle in [start_id,end_id)
        fn table_range(&self, start_id: i64, end_id: i64) -> KeyRange {
            let mut range = KeyRange::default();
            range.set_start(table::encode_row_key(self.table_id, start_id));
            range.set_end(table::encode_row_key(self.table_id, end_id));
            range
        }

        /// Returns the range for the whole table.
        fn whole_table_range(&self) -> KeyRange {
            self.table_range(std::i64::MIN, std::i64::MAX)
        }

        /// Returns the values start from `start_row` limit `rows`.
        fn get_expect_values_by_range(&self, start_row: usize, rows: usize) -> Vec<VectorValue> {
            let mut pks = VectorValue::with_capacity(self.data.len(), EvalType::Int);
            let mut foos = VectorValue::with_capacity(self.data.len(), EvalType::Int);
            let mut bars = VectorValue::with_capacity(self.data.len(), EvalType::Real);
            assert!(start_row + rows <= self.data.len());
            for id in start_row..start_row + rows {
                let (handle, foo, bar) = self.data[id];
                pks.push_int(Some(handle));
                foos.push_int(foo);
                bars.push_real(bar);
            }
            vec![pks, foos, bars]
        }

        /// check whether the data of columns in `col_idxs` are as expected.
        /// col_idxs: the idx of column which the `columns` included.
        fn expect_table_values(
            &self,
            col_idxs: &[usize],
            start_row: usize,
            expect_rows: usize,
            mut columns: LazyBatchColumnVec,
        ) {
            let values = self.get_expect_values_by_range(start_row, expect_rows);
            assert_eq!(columns.columns_len(), col_idxs.len());
            assert_eq!(columns.rows_len(), expect_rows);
            for id in 0..col_idxs.len() {
                let col_idx = col_idxs[id];
                if col_idx == self.idx_pk() {
                    assert!(columns[id].is_decoded());
                } else {
                    assert!(columns[id].is_raw());
                    columns[id]
                        .ensure_all_decoded(
                            &mut EvalContext::default(),
                            self.get_field_type(col_idx),
                        )
                        .unwrap();
                }
                assert_eq!(columns[id].decoded(), &values[col_idx]);
            }
        }
    }

    /// test basic `tablescan` with ranges,
    /// `col_idxs`: idxs of columns used in scan.
    /// `batch_expect_rows`: `expect_rows` used in `next_batch`.
    fn test_basic_scan(
        helper: &TableScanTestHelper,
        ranges: Vec<KeyRange>,
        col_idxs: &[usize],
        batch_expect_rows: &[usize],
    ) {
        let columns_info = helper.columns_info_by_idx(col_idxs);
        let mut executor = BatchTableScanExecutor::new(
            helper.store(),
            Arc::new(EvalConfig::default()),
            columns_info.clone(),
            ranges,
            false,
        )
        .unwrap();

        let total_rows = helper.data.len();
        let mut start_row = 0;
        for expect_rows in batch_expect_rows {
            let expect_rows = *expect_rows;
            let expect_drained = start_row + expect_rows > total_rows;
            let result = executor.next_batch(expect_rows);
            assert_eq!(*result.is_drained.as_ref().unwrap(), expect_drained);
            if expect_drained {
                // all remaining rows are fetched
                helper.expect_table_values(
                    col_idxs,
                    start_row,
                    total_rows - start_row,
                    result.physical_columns,
                );
                return;
            }
            // we should get expect_rows in this case.
            helper.expect_table_values(col_idxs, start_row, expect_rows, result.physical_columns);
            start_row += expect_rows;
        }
    }

    #[test]
    fn test_basic() {
        let helper = TableScanTestHelper::new();
        // ranges to scan in each test case
        let test_ranges = vec![
            helper.point_ranges(),                 // point scan
            vec![helper.whole_table_range()],      // range scan
            helper.mixed_ranges_for_whole_table(), // mixed range scan and point scan
        ];
        // cols to scan in each test case.
        let test_cols = vec![
            // scan single column
            vec![0],
            vec![1],
            vec![2],
            // scan multiple columns
            vec![0, 1],
            vec![0, 2],
            vec![1, 2],
            //PK is the last column in schema
            vec![2, 1, 0],
            //PK is the first column in schema
            vec![0, 1, 2],
            // PK is in the middle of the schema
            vec![1, 0, 2],
        ];
        // expect_rows used in next_batch for each test case.
        let test_batch_rows = vec![
            // Fetched multiple times but totally it fetched exactly the same number of rows
            // (so that it will be drained next time and at that time no row will be get).
            vec![1, 1, 1, 1, 1, 1],
            vec![1, 2, 2, 2],
            // Fetch a lot of rows once.
            vec![10, 10],
        ];

        for ranges in test_ranges {
            for cols in &test_cols {
                for batch_expect_rows in &test_batch_rows {
                    test_basic_scan(&helper, ranges.clone(), cols, batch_expect_rows);
                }
            }
        }
    }

    #[test]
    fn test_execution_summary() {
        let helper = TableScanTestHelper::new();

        let mut executor = BatchTableScanExecutor::new(
            helper.store(),
            Arc::new(EvalConfig::default()),
            helper.columns_info_by_idx(&[0]),
            vec![helper.whole_table_range()],
            false,
        )
        .unwrap()
        .collect_summary(1);

        executor.next_batch(1);
        executor.next_batch(2);

        let mut s = ExecuteStats::new(2);
        executor.collect_exec_stats(&mut s);

        assert_eq!(s.scanned_rows_per_range.len(), 1);
        assert_eq!(s.scanned_rows_per_range[0], 3);
        // 0 remains Default because our output index is 1
        assert_eq!(s.summary_per_executor[0], ExecSummary::default());
        let exec_summary = s.summary_per_executor[1];
        assert_eq!(3, exec_summary.num_produced_rows);
        assert_eq!(2, exec_summary.num_iterations);

        executor.collect_exec_stats(&mut s);

        // Collected statistics remain unchanged because of no newly generated delta statistics.
        assert_eq!(s.scanned_rows_per_range.len(), 2);
        assert_eq!(s.scanned_rows_per_range[0], 3);
        assert_eq!(s.scanned_rows_per_range[1], 0);
        assert_eq!(s.summary_per_executor[0], ExecSummary::default());
        let exec_summary = s.summary_per_executor[1];
        assert_eq!(3, exec_summary.num_produced_rows);
        assert_eq!(2, exec_summary.num_iterations);

        // Reset collected statistics so that now we will only collect statistics in this round.
        s.clear();
        executor.next_batch(10);
        executor.collect_exec_stats(&mut s);

        assert_eq!(s.scanned_rows_per_range.len(), 1);
        assert_eq!(s.scanned_rows_per_range[0], 2);
        assert_eq!(s.summary_per_executor[0], ExecSummary::default());
        let exec_summary = s.summary_per_executor[1];
        assert_eq!(2, exec_summary.num_produced_rows);
        assert_eq!(1, exec_summary.num_iterations);
    }

    #[test]
    fn test_corrupted_data() {
        const TABLE_ID: i64 = 5;

        let columns_info = vec![
            {
                let mut ci = ColumnInfo::default();
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci.set_pk_handle(true);
                ci.set_column_id(1);
                ci
            },
            {
                let mut ci = ColumnInfo::default();
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci.set_column_id(2);
                ci
            },
            {
                let mut ci = ColumnInfo::default();
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci.set_column_id(3);
                ci
            },
        ];
        let schema = vec![
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::LongLong.into(),
        ];

        let mut ctx = EvalContext::default();
        let mut kv = vec![];
        {
            // row 0, which is not corrupted
            let key = table::encode_row_key(TABLE_ID, 0);
            let value =
                table::encode_row(&mut ctx, vec![Datum::I64(5), Datum::I64(7)], &[2, 3]).unwrap();
            kv.push((key, value));
        }
        {
            // row 1, which is not corrupted
            let key = table::encode_row_key(TABLE_ID, 1);
            let value = vec![];
            kv.push((key, value));
        }
        {
            // row 2, which is partially corrupted
            let key = table::encode_row_key(TABLE_ID, 2);
            let mut value =
                table::encode_row(&mut ctx, vec![Datum::I64(5), Datum::I64(7)], &[2, 3]).unwrap();
            // resize the value to make it partially corrupted
            value.truncate(value.len() - 3);
            kv.push((key, value));
        }
        {
            // row 3, which is totally corrupted due to invalid datum flag for column id
            let key = table::encode_row_key(TABLE_ID, 3);
            // this datum flag does not exist
            let value = vec![255];
            kv.push((key, value));
        }
        {
            // row 4, which is totally corrupted due to missing datum for column value
            let key = table::encode_row_key(TABLE_ID, 4);
            let value = datum::encode_value(&mut ctx, &[Datum::I64(2)]).unwrap(); // col_id = 2
            kv.push((key, value));
        }

        let key_range_point: Vec<_> = kv
            .iter()
            .enumerate()
            .map(|(index, _)| {
                let mut r = KeyRange::default();
                r.set_start(table::encode_row_key(TABLE_ID, index as i64));
                r.set_end(r.get_start().to_vec());
                convert_to_prefix_next(r.mut_end());
                r
            })
            .collect();

        let store = FixtureStorage::from(kv);

        // For row 0 + row 1 + (row 2 ~ row 4), we should only get row 0, row 1 and an error.
        for corrupted_row_index in 2..=4 {
            let mut executor = BatchTableScanExecutor::new(
                store.clone(),
                Arc::new(EvalConfig::default()),
                columns_info.clone(),
                vec![
                    key_range_point[0].clone(),
                    key_range_point[1].clone(),
                    key_range_point[corrupted_row_index].clone(),
                ],
                false,
            )
            .unwrap();

            let mut result = executor.next_batch(10);
            assert!(result.is_drained.is_err());
            assert_eq!(result.physical_columns.columns_len(), 3);
            assert_eq!(result.physical_columns.rows_len(), 2);
            assert!(result.physical_columns[0].is_decoded());
            assert_eq!(
                result.physical_columns[0].decoded().as_int_slice(),
                &[Some(0), Some(1)]
            );
            assert!(result.physical_columns[1].is_raw());
            result.physical_columns[1]
                .ensure_all_decoded(&mut ctx, &schema[1])
                .unwrap();
            assert_eq!(
                result.physical_columns[1].decoded().as_int_slice(),
                &[Some(5), None]
            );
            assert!(result.physical_columns[2].is_raw());
            result.physical_columns[2]
                .ensure_all_decoded(&mut ctx, &schema[2])
                .unwrap();
            assert_eq!(
                result.physical_columns[2].decoded().as_int_slice(),
                &[Some(7), None]
            );
        }
    }

    #[test]
    fn test_locked_data() {
        const TABLE_ID: i64 = 42;

        let columns_info = vec![
            {
                let mut ci = ColumnInfo::default();
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci.set_pk_handle(true);
                ci.set_column_id(1);
                ci
            },
            {
                let mut ci = ColumnInfo::default();
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci.set_column_id(2);
                ci
            },
        ];
        let schema = vec![FieldTypeTp::LongLong.into(), FieldTypeTp::LongLong.into()];

        let mut ctx = EvalContext::default();
        let mut kv = vec![];
        {
            // row 0: ok
            let key = table::encode_row_key(TABLE_ID, 0);
            let value = table::encode_row(&mut ctx, vec![Datum::I64(7)], &[2]).unwrap();
            kv.push((key, Ok(value)));
        }
        {
            // row 1: storage error
            let key = table::encode_row_key(TABLE_ID, 1);
            let value: std::result::Result<
                _,
                Box<dyn Send + Sync + Fn() -> crate::error::StorageError>,
            > = Err(Box::new(|| failure::format_err!("locked").into()));
            kv.push((key, value));
        }
        {
            // row 2: not locked
            let key = table::encode_row_key(TABLE_ID, 2);
            let value = table::encode_row(&mut ctx, vec![Datum::I64(5)], &[2]).unwrap();
            kv.push((key, Ok(value)));
        }

        let key_range_point: Vec<_> = kv
            .iter()
            .enumerate()
            .map(|(index, _)| {
                let mut r = KeyRange::default();
                r.set_start(table::encode_row_key(TABLE_ID, index as i64));
                r.set_end(r.get_start().to_vec());
                convert_to_prefix_next(r.mut_end());
                r
            })
            .collect();

        let store = FixtureStorage::new(kv.into_iter().collect());

        // Case 1: row 0 + row 1 + row 2
        // We should get row 0 and error because no further rows should be scanned when there is
        // an error.
        {
            let mut executor = BatchTableScanExecutor::new(
                store.clone(),
                Arc::new(EvalConfig::default()),
                columns_info.clone(),
                vec![
                    key_range_point[0].clone(),
                    key_range_point[1].clone(),
                    key_range_point[2].clone(),
                ],
                false,
            )
            .unwrap();

            let mut result = executor.next_batch(10);
            assert!(result.is_drained.is_err());
            assert_eq!(result.physical_columns.columns_len(), 2);
            assert_eq!(result.physical_columns.rows_len(), 1);
            assert!(result.physical_columns[0].is_decoded());
            assert_eq!(
                result.physical_columns[0].decoded().as_int_slice(),
                &[Some(0)]
            );
            assert!(result.physical_columns[1].is_raw());
            result.physical_columns[1]
                .ensure_all_decoded(&mut ctx, &schema[1])
                .unwrap();
            assert_eq!(
                result.physical_columns[1].decoded().as_int_slice(),
                &[Some(7)]
            );
        }

        // Let's also repeat case 1 for smaller batch size
        {
            let mut executor = BatchTableScanExecutor::new(
                store.clone(),
                Arc::new(EvalConfig::default()),
                columns_info.clone(),
                vec![
                    key_range_point[0].clone(),
                    key_range_point[1].clone(),
                    key_range_point[2].clone(),
                ],
                false,
            )
            .unwrap();

            let mut result = executor.next_batch(1);
            assert!(!result.is_drained.is_err());
            assert_eq!(result.physical_columns.columns_len(), 2);
            assert_eq!(result.physical_columns.rows_len(), 1);
            assert!(result.physical_columns[0].is_decoded());
            assert_eq!(
                result.physical_columns[0].decoded().as_int_slice(),
                &[Some(0)]
            );
            assert!(result.physical_columns[1].is_raw());
            result.physical_columns[1]
                .ensure_all_decoded(&mut ctx, &schema[1])
                .unwrap();
            assert_eq!(
                result.physical_columns[1].decoded().as_int_slice(),
                &[Some(7)]
            );

            let result = executor.next_batch(1);
            assert!(result.is_drained.is_err());
            assert_eq!(result.physical_columns.columns_len(), 2);
            assert_eq!(result.physical_columns.rows_len(), 0);
        }

        // Case 2: row 1 + row 2
        // We should get error and no row, for the same reason as above.
        {
            let mut executor = BatchTableScanExecutor::new(
                store.clone(),
                Arc::new(EvalConfig::default()),
                columns_info.clone(),
                vec![key_range_point[1].clone(), key_range_point[2].clone()],
                false,
            )
            .unwrap();

            let result = executor.next_batch(10);
            assert!(result.is_drained.is_err());
            assert_eq!(result.physical_columns.columns_len(), 2);
            assert_eq!(result.physical_columns.rows_len(), 0);
        }

        // Case 3: row 2 + row 0
        // We should get row 2 and row 0. There is no error.
        {
            let mut executor = BatchTableScanExecutor::new(
                store.clone(),
                Arc::new(EvalConfig::default()),
                columns_info.clone(),
                vec![key_range_point[2].clone(), key_range_point[0].clone()],
                false,
            )
            .unwrap();

            let mut result = executor.next_batch(10);
            assert!(!result.is_drained.is_err());
            assert_eq!(result.physical_columns.columns_len(), 2);
            assert_eq!(result.physical_columns.rows_len(), 2);
            assert!(result.physical_columns[0].is_decoded());
            assert_eq!(
                result.physical_columns[0].decoded().as_int_slice(),
                &[Some(2), Some(0)]
            );
            assert!(result.physical_columns[1].is_raw());
            result.physical_columns[1]
                .ensure_all_decoded(&mut ctx, &schema[1])
                .unwrap();
            assert_eq!(
                result.physical_columns[1].decoded().as_int_slice(),
                &[Some(5), Some(7)]
            );
        }

        // Case 4: row 1
        // We should get error.
        {
            let mut executor = BatchTableScanExecutor::new(
                store.clone(),
                Arc::new(EvalConfig::default()),
                columns_info.clone(),
                vec![key_range_point[1].clone()],
                false,
            )
            .unwrap();

            let result = executor.next_batch(10);
            assert!(result.is_drained.is_err());
            assert_eq!(result.physical_columns.columns_len(), 2);
            assert_eq!(result.physical_columns.rows_len(), 0);
        }
    }

    fn test_multi_handle_column_impl(columns_is_pk: &[bool]) {
        const TABLE_ID: i64 = 42;

        // This test makes a pk column with id = 1 and non-pk columns with id
        // in 10 to 10 + columns_is_pk.len().
        // PK columns will be set to column 1 and others will be set to column 10 + i, where i is
        // the index of each column.

        let mut columns_info = Vec::new();
        for (i, is_pk) in columns_is_pk.iter().enumerate() {
            let mut ci = ColumnInfo::default();
            ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
            ci.set_pk_handle(*is_pk);
            ci.set_column_id(if *is_pk { 1 } else { i as i64 + 10 });
            columns_info.push(ci);
        }

        let mut schema = Vec::new();
        schema.resize(columns_is_pk.len(), FieldTypeTp::LongLong.into());

        let key = table::encode_row_key(TABLE_ID, 1);
        let col_ids = (10..10 + schema.len() as i64).collect::<Vec<_>>();
        let row = col_ids.iter().map(|i| Datum::I64(*i)).collect();
        let value = table::encode_row(&mut EvalContext::default(), row, &col_ids).unwrap();

        let mut key_range = KeyRange::default();
        key_range.set_start(table::encode_row_key(TABLE_ID, std::i64::MIN));
        key_range.set_end(table::encode_row_key(TABLE_ID, std::i64::MAX));

        let store = FixtureStorage::new(iter::once((key, (Ok(value)))).collect());

        let mut executor = BatchTableScanExecutor::new(
            store.clone(),
            Arc::new(EvalConfig::default()),
            columns_info,
            vec![key_range],
            false,
        )
        .unwrap();

        let mut result = executor.next_batch(10);
        assert_eq!(result.is_drained.unwrap(), true);
        assert_eq!(result.logical_rows.len(), 1);
        assert_eq!(result.physical_columns.columns_len(), columns_is_pk.len());
        for i in 0..columns_is_pk.len() {
            result.physical_columns[i]
                .ensure_all_decoded(&mut EvalContext::default(), &schema[i])
                .unwrap();
            if columns_is_pk[i] {
                assert_eq!(
                    result.physical_columns[i].decoded().as_int_slice(),
                    &[Some(1)]
                );
            } else {
                assert_eq!(
                    result.physical_columns[i].decoded().as_int_slice(),
                    &[Some(i as i64 + 10)]
                );
            }
        }
    }

    #[test]
    fn test_multi_handle_column() {
        test_multi_handle_column_impl(&[true]);
        test_multi_handle_column_impl(&[false]);
        test_multi_handle_column_impl(&[true, false]);
        test_multi_handle_column_impl(&[false, true]);
        test_multi_handle_column_impl(&[true, true]);
        test_multi_handle_column_impl(&[true, false, true]);
        test_multi_handle_column_impl(&[
            false, false, false, true, false, false, true, true, false, false,
        ]);
    }
}
