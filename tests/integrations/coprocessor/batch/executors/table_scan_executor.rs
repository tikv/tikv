// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#[cfg(test)]
mod tests {
    use cop_dag::batch::executors::BatchTableScanExecutor;
    use cop_dag::batch::statistics::BatchExecuteStatistics;

    use std::sync::Arc;

    use cop_datatype::{EvalType, FieldTypeAccessor, FieldTypeTp};
    use kvproto::coprocessor::KeyRange;
    use tipb::expression::FieldType;
    use tipb::schema::ColumnInfo;

    use cop_dag::batch::interface::BatchExecutor;
    use cop_dag::codec::batch::LazyBatchColumnVec;
    use cop_dag::codec::data_type::VectorValue;
    use cop_dag::codec::mysql::Tz;
    use cop_dag::codec::{datum, table, Datum};
    use cop_dag::exec_summary::*;
    use cop_dag::expr::EvalConfig;
    use cop_dag::util::convert_to_prefix_next;
    use tikv::storage::{FixtureStore, Key};

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
                    ci.set_default_val(datum::encode_value(&[Datum::F64(4.5)]).unwrap());
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
        helper: &TableScanTestHelper,
        ranges: Vec<KeyRange>,
        col_idxs: &[usize],
        batch_expect_rows: &[usize],
    ) {
        let columns_info = helper.columns_info_by_idx(col_idxs);
        let mut executor = BatchTableScanExecutor::new(
            ExecSummaryCollectorDisabled,
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
                    result.data,
                );
                return;
            }
            // we should get expect_rows in this case.
            helper.expect_table_values(col_idxs, start_row, expect_rows, result.data);
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
            ExecSummaryCollectorEnabled::new(1),
            helper.store(),
            Arc::new(EvalConfig::default()),
            helper.columns_info_by_idx(&[0]),
            vec![helper.whole_table_range()],
            false,
        )
        .unwrap();

        executor.next_batch(1);
        executor.next_batch(2);

        let mut s = BatchExecuteStatistics::new(2, 1);
        executor.collect_statistics(&mut s);

        assert_eq!(s.scanned_rows_per_range[0], 3);
        // 0 is none because our output index is 1
        assert!(s.summary_per_executor[0].is_none());
        let exec_summary = s.summary_per_executor[1].as_ref().unwrap();
        assert_eq!(3, exec_summary.num_produced_rows);
        assert_eq!(2, exec_summary.num_iterations);

        executor.collect_statistics(&mut s);

        // Collected statistics remain unchanged because of no newly generated delta statistics.
        assert_eq!(s.scanned_rows_per_range[0], 3);
        assert!(s.summary_per_executor[0].is_none());
        let exec_summary = s.summary_per_executor[1].as_ref().unwrap();
        assert_eq!(3, exec_summary.num_produced_rows);
        assert_eq!(2, exec_summary.num_iterations);

        // Reset collected statistics so that now we will only collect statistics in this round.
        s.clear();
        executor.next_batch(10);
        executor.collect_statistics(&mut s);

        assert_eq!(s.scanned_rows_per_range[0], 2);
        assert!(s.summary_per_executor[0].is_none());
        let exec_summary = s.summary_per_executor[1].as_ref().unwrap();
        assert_eq!(2, exec_summary.num_produced_rows);
        assert_eq!(1, exec_summary.num_iterations);
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
                ExecSummaryCollectorDisabled,
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
            let value = tikv::storage::txn::Error::Mvcc(tikv::storage::mvcc::Error::KeyIsLocked {
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
                ExecSummaryCollectorDisabled,
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
                ExecSummaryCollectorDisabled,
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
                ExecSummaryCollectorDisabled,
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
                ExecSummaryCollectorDisabled,
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
                ExecSummaryCollectorDisabled,
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
