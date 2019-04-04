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

use std::sync::Arc;

use cop_datatype::{EvalType, FieldTypeAccessor};
use kvproto::coprocessor::KeyRange;
use tipb::expression::FieldType;
use tipb::schema::ColumnInfo;

use crate::storage::Store;
use crate::util::collections::HashMap;

use super::interface::*;
use crate::coprocessor::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};
use crate::coprocessor::dag::expr::{EvalConfig, EvalContext};
use crate::coprocessor::dag::Scanner;
use crate::coprocessor::Result;

pub struct BatchTableScanExecutor<S: Store>(
    super::scan_executor::ScanExecutor<
        S,
        TableScanExecutorImpl,
        super::ranges_iter::PointRangeEnable,
    >,
);

impl<S: Store> BatchTableScanExecutor<S> {
    #[inline(never)]
    pub fn new(
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
            schema.push(super::scan_executor::field_type_from_column_info(&ci));

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
        let wrapper = super::scan_executor::ScanExecutor::new(
            imp,
            store,
            desc,
            key_ranges,
            super::ranges_iter::PointRangeEnable,
        )?;
        Ok(Self(wrapper))
    }
}

impl<S: Store> BatchExecutor for BatchTableScanExecutor<S> {
    #[inline(never)]
    fn schema(&self) -> &[FieldType] {
        self.0.schema()
    }

    #[inline(never)]
    fn next_batch(&mut self, expect_rows: usize) -> BatchExecuteResult {
        self.0.next_batch(expect_rows)
    }

    #[inline(never)]
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

impl super::scan_executor::ScanExecutorImpl for TableScanExecutorImpl {
    #[inline(never)]
    fn schema(&self) -> &[FieldType] {
        &self.schema
    }

    #[inline(never)]
    fn mut_context(&mut self) -> &mut EvalContext {
        &mut self.context
    }

    #[inline(never)]
    fn build_scanner<S: Store>(
        &self,
        store: &S,
        desc: bool,
        range: KeyRange,
    ) -> Result<Scanner<S>> {
        Ok(Scanner::new(
            store,
            crate::coprocessor::dag::ScanOn::Table,
            desc,
            self.key_only,
            range,
        )?)
    }

    /// Constructs empty columns, with PK in decoded format and the rest in raw format.
    fn build_column_vec(&self, expect_rows: usize) -> LazyBatchColumnVec {
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
                columns.push(LazyBatchColumn::raw_with_capacity(expect_rows));
            }
            // For PK handle, we construct a decoded `VectorValue` because it is directly
            // stored as i64, without a datum flag, at the end of key.
            columns.push(LazyBatchColumn::decoded_with_capacity_and_tp(
                expect_rows,
                EvalType::Int,
            ));
            // Columns after `handle_index` (if any) should also be raw.
            for _ in handle_index + 1..columns_len {
                columns.push(LazyBatchColumn::raw_with_capacity(expect_rows));
            }
        } else {
            // PK is unspecified in schema. All column should be in raw format.
            for _ in 0..columns_len {
                columns.push(LazyBatchColumn::raw_with_capacity(expect_rows));
            }
        }

        LazyBatchColumnVec::from(columns)
    }

    fn process_kv_pair(
        &mut self,
        key: &[u8],
        value: &[u8],
        columns: &mut LazyBatchColumnVec,
    ) -> Result<()> {
        use crate::coprocessor::codec::{datum, table};
        use crate::util::codec::number;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coprocessor::codec::batch::LazyBatchColumnVec;
    use crate::coprocessor::codec::data_type::VectorValue;
    use crate::coprocessor::codec::datum::encode_value;
    use crate::coprocessor::codec::mysql::Tz;
    use crate::coprocessor::codec::{datum, table, Datum};
    use crate::coprocessor::dag::expr::EvalConfig;
    use crate::coprocessor::util::convert_to_prefix_next;
    use crate::storage::{FixtureStore, Key};
    use cop_datatype::{EvalType, FieldTypeAccessor, FieldTypeTp};
    use kvproto::coprocessor::KeyRange;
    use std::sync::Arc;
    use tipb::expression::FieldType;
    use tipb::schema::ColumnInfo;

    fn field_type(ft: FieldTypeTp) -> FieldType {
        let mut f = FieldType::new();
        f.as_mut_accessor().set_tp(ft);
        f
    }

    /// Test Helper for normal test with fixed schema and data.
    /// Table Schema: ID (INT, PK), Foo (INT), Bar (FLOAT, Default 4.5)
    /// Column id:    1,            2,         4
    /// Column offset:   0,            1          2
    /// Table Data:  (1,            Some(10),  Some(5.2)),
    ///              (3,            Some(-5),   None),
    ///              (4,             None,      default(4.5)),
    ///              (5,             None,      Some(0.1)),
    ///              (6,             None,      default(4.5)),     
    struct TableScanTestHelper {
        // ID(INT,PK), Foo(INT), Bar(Float,Default 4.5)
        pub data: Vec<(i64, Option<i64>, Option<f64>)>,
        pub table_id: i64,
        pub columns_info: Vec<ColumnInfo>,
        pub field_types: Vec<FieldType>,
        pub store: FixtureStore,
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
                (1, Some(10), Some(5.2)),
                (3, Some(-5), None),
                (4, None, Some(4.5)),
                (5, None, Some(0.1)),
                (6, None, Some(4.5)),
            ];

            // The column info for each column in `data`.
            let columns_info = vec![
                {
                    let mut ci = ColumnInfo::new();
                    ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                    ci.set_pk_handle(true);
                    ci.set_column_id(1);
                    ci
                },
                {
                    let mut ci = ColumnInfo::new();
                    ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                    ci.set_column_id(2);
                    ci
                },
                {
                    let mut ci = ColumnInfo::new();
                    ci.as_mut_accessor().set_tp(FieldTypeTp::Double);
                    ci.set_column_id(4);
                    ci.set_default_val(encode_value(&[Datum::F64(4.5)]).unwrap());
                    ci
                },
            ];

            let field_types = vec![
                field_type(FieldTypeTp::LongLong),
                field_type(FieldTypeTp::LongLong),
                field_type(FieldTypeTp::Double),
            ];

            let store = {
                let kv = data
                    .iter()
                    .map(|(row_id, columns)| {
                        let key = Key::from_raw(&table::encode_row_key(TABLE_ID, *row_id));
                        let value = {
                            let row = columns.iter().map(|(_, datum)| datum.clone()).collect();
                            let col_ids: Vec<_> = columns.iter().map(|(id, _)| *id).collect();
                            table::encode_row(row, &col_ids).unwrap()
                        };
                        (key, Ok(value))
                    })
                    .collect();
                FixtureStore::new(kv)
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
                    let mut r = KeyRange::new();
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
                    let mut r = KeyRange::new();
                    r.set_start(table::encode_row_key(self.table_id, 3));
                    r.set_end(r.get_start().to_vec());
                    convert_to_prefix_next(r.mut_end());
                    r
                },
                self.table_range(4, std::i64::MAX),
            ]
        }

        fn store(&self) -> FixtureStore {
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
            let mut range = KeyRange::new();
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
                        .decode(&Tz::utc(), self.get_field_type(col_idx))
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
        store: &TableScanTestHelper,
        ranges: Vec<KeyRange>,
        col_idxs: &[usize],
        batch_expect_rows: &[usize],
    ) {
        let columns_info = store.columns_info_by_idx(col_idxs);
        let mut executor = BatchTableScanExecutor::new(
            store.store(),
            Arc::new(EvalConfig::default()),
            columns_info.clone(),
            ranges,
            false,
        )
        .unwrap();

        let total_rows = store.data.len();
        let mut start_row = 0;
        for expect_rows in batch_expect_rows {
            let expect_rows = *expect_rows;
            let expect_drained = start_row + expect_rows > total_rows;
            let result = executor.next_batch(expect_rows);
            assert_eq!(*result.is_drained.as_ref().unwrap(), expect_drained);
            if expect_drained {
                // all remaining rows are fetched
                store.expect_table_values(col_idxs, start_row, total_rows - start_row, result.data);
                return;
            }
            // we should get expect_rows in this case.
            store.expect_table_values(col_idxs, start_row, expect_rows, result.data);
            start_row += expect_rows;
        }
    }

    #[test]
    fn test_basic() {
        let store = TableScanTestHelper::new();
        // ranges to scan in each test case
        let test_ranges = vec![
            store.point_ranges(),                 // point scan
            vec![store.whole_table_range()],      // range scan
            store.mixed_ranges_for_whole_table(), // mixed range scan and point scan
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
                    test_basic_scan(&store, ranges.clone(), cols, batch_expect_rows);
                }
            }
        }
    }

    #[test]
    fn test_corrupted_data() {
        const TABLE_ID: i64 = 5;

        let columns_info = vec![
            {
                let mut ci = ColumnInfo::new();
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci.set_pk_handle(true);
                ci.set_column_id(1);
                ci
            },
            {
                let mut ci = ColumnInfo::new();
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci.set_column_id(2);
                ci
            },
            {
                let mut ci = ColumnInfo::new();
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci.set_column_id(3);
                ci
            },
        ];

        let schema = vec![
            {
                let mut ft = FieldType::new();
                ft.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ft
            },
            {
                let mut ft = FieldType::new();
                ft.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ft
            },
            {
                let mut ft = FieldType::new();
                ft.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ft
            },
        ];

        let mut kv = vec![];
        {
            // row 0, which is not corrupted
            let key = Key::from_raw(&table::encode_row_key(TABLE_ID, 0));
            let value = table::encode_row(vec![Datum::I64(5), Datum::I64(7)], &[2, 3]).unwrap();
            kv.push((key, Ok(value)));
        }
        {
            // row 1, which is not corrupted
            let key = Key::from_raw(&table::encode_row_key(TABLE_ID, 1));
            let value = vec![];
            kv.push((key, Ok(value)));
        }
        {
            // row 2, which is partially corrupted
            let key = Key::from_raw(&table::encode_row_key(TABLE_ID, 2));
            let mut value = table::encode_row(vec![Datum::I64(5), Datum::I64(7)], &[2, 3]).unwrap();
            // resize the value to make it partially corrupted
            value.truncate(value.len() - 3);
            kv.push((key, Ok(value)));
        }
        {
            // row 3, which is totally corrupted due to invalid datum flag for column id
            let key = Key::from_raw(&table::encode_row_key(TABLE_ID, 3));
            // this datum flag does not exist
            let value = vec![255];
            kv.push((key, Ok(value)));
        }
        {
            // row 4, which is totally corrupted due to missing datum for column value
            let key = Key::from_raw(&table::encode_row_key(TABLE_ID, 4));
            let value = datum::encode_value(&[Datum::I64(2)]).unwrap(); // col_id = 2
            kv.push((key, Ok(value)));
        }

        let key_range_point: Vec<_> = kv
            .iter()
            .enumerate()
            .map(|(index, _)| {
                let mut r = KeyRange::new();
                r.set_start(table::encode_row_key(TABLE_ID, index as i64));
                r.set_end(r.get_start().to_vec());
                convert_to_prefix_next(r.mut_end());
                r
            })
            .collect();

        let store = FixtureStore::new(kv.into_iter().collect());

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
            assert_eq!(result.data.columns_len(), 3);
            assert_eq!(result.data.rows_len(), 2);
            assert!(result.data[0].is_decoded());
            assert_eq!(result.data[0].decoded().as_int_slice(), &[Some(0), Some(1)]);
            assert!(result.data[1].is_raw());
            result.data[1].decode(&Tz::utc(), &schema[1]).unwrap();
            assert_eq!(result.data[1].decoded().as_int_slice(), &[Some(5), None]);
            assert!(result.data[2].is_raw());
            result.data[2].decode(&Tz::utc(), &schema[2]).unwrap();
            assert_eq!(result.data[2].decoded().as_int_slice(), &[Some(7), None]);
        }
    }

    #[test]
    fn test_locked_data() {
        const TABLE_ID: i64 = 42;

        let columns_info = vec![
            {
                let mut ci = ColumnInfo::new();
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci.set_pk_handle(true);
                ci.set_column_id(1);
                ci
            },
            {
                let mut ci = ColumnInfo::new();
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci.set_column_id(2);
                ci
            },
        ];

        let schema = vec![
            {
                let mut ft = FieldType::new();
                ft.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ft
            },
            {
                let mut ft = FieldType::new();
                ft.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ft
            },
        ];

        let mut kv = vec![];
        {
            // row 0: not locked
            let key = Key::from_raw(&table::encode_row_key(TABLE_ID, 0));
            let value = table::encode_row(vec![Datum::I64(7)], &[2]).unwrap();
            kv.push((key, Ok(value)));
        }
        {
            // row 1: locked
            let key = Key::from_raw(&table::encode_row_key(TABLE_ID, 1));
            let value =
                crate::storage::txn::Error::Mvcc(crate::storage::mvcc::Error::KeyIsLocked {
                    // We won't check error detail in tests, so we can just fill fields casually.
                    key: vec![],
                    primary: vec![],
                    ts: 1,
                    ttl: 2,
                });
            kv.push((key, Err(value)));
        }
        {
            // row 2: not locked
            let key = Key::from_raw(&table::encode_row_key(TABLE_ID, 2));
            let value = table::encode_row(vec![Datum::I64(5)], &[2]).unwrap();
            kv.push((key, Ok(value)));
        }

        let key_range_point: Vec<_> = kv
            .iter()
            .enumerate()
            .map(|(index, _)| {
                let mut r = KeyRange::new();
                r.set_start(table::encode_row_key(TABLE_ID, index as i64));
                r.set_end(r.get_start().to_vec());
                convert_to_prefix_next(r.mut_end());
                r
            })
            .collect();

        let store = FixtureStore::new(kv.into_iter().collect());

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
            assert_eq!(result.data.columns_len(), 2);
            assert_eq!(result.data.rows_len(), 1);
            assert!(result.data[0].is_decoded());
            assert_eq!(result.data[0].decoded().as_int_slice(), &[Some(0)]);
            assert!(result.data[1].is_raw());
            result.data[1].decode(&Tz::utc(), &schema[1]).unwrap();
            assert_eq!(result.data[1].decoded().as_int_slice(), &[Some(7)]);
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
            assert_eq!(result.data.columns_len(), 2);
            assert_eq!(result.data.rows_len(), 1);
            assert!(result.data[0].is_decoded());
            assert_eq!(result.data[0].decoded().as_int_slice(), &[Some(0)]);
            assert!(result.data[1].is_raw());
            result.data[1].decode(&Tz::utc(), &schema[1]).unwrap();
            assert_eq!(result.data[1].decoded().as_int_slice(), &[Some(7)]);

            let result = executor.next_batch(1);
            assert!(result.is_drained.is_err());
            assert_eq!(result.data.columns_len(), 2);
            assert_eq!(result.data.rows_len(), 0);
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
            assert_eq!(result.data.columns_len(), 2);
            assert_eq!(result.data.rows_len(), 0);
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
            assert_eq!(result.data.columns_len(), 2);
            assert_eq!(result.data.rows_len(), 2);
            assert!(result.data[0].is_decoded());
            assert_eq!(result.data[0].decoded().as_int_slice(), &[Some(2), Some(0)]);
            assert!(result.data[1].is_raw());
            result.data[1].decode(&Tz::utc(), &schema[1]).unwrap();
            assert_eq!(result.data[1].decoded().as_int_slice(), &[Some(5), Some(7)]);
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
            assert_eq!(result.data.columns_len(), 2);
            assert_eq!(result.data.rows_len(), 0);
        }
    }
}
