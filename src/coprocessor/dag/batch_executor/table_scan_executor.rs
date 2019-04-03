// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
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
    #[inline]
    fn schema(&self) -> &[FieldType] {
        self.0.schema()
    }

    #[inline]
    fn next_batch(&mut self, expect_rows: usize) -> BatchExecuteResult {
        self.0.next_batch(expect_rows)
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

impl super::scan_executor::ScanExecutorImpl for TableScanExecutorImpl {
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

    use std::sync::Arc;

    use cop_datatype::{FieldTypeAccessor, FieldTypeTp};
    use kvproto::coprocessor::KeyRange;
    use tipb::expression::FieldType;
    use tipb::schema::ColumnInfo;

    use crate::coprocessor::codec::mysql::Tz;
    use crate::coprocessor::codec::{datum, table, Datum};
    use crate::coprocessor::dag::expr::EvalConfig;
    use crate::coprocessor::util::convert_to_prefix_next;
    use crate::storage::{FixtureStore, Key};

    #[test]
    fn test_basic() {
        // Table Schema: ID (INT, PK), Foo (INT), Bar (FLOAT, Default 4.5)
        // Column id:    1,            2,         4

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

        // The point range representation for each row in `data`.
        let range_each_row: Vec<_> = data
            .iter()
            .map(|(row_id, _)| {
                let mut r = KeyRange::new();
                r.set_start(table::encode_row_key(TABLE_ID, *row_id));
                r.set_end(r.get_start().to_vec());
                convert_to_prefix_next(r.mut_end());
                r
            })
            .collect();

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

        // The schema of these columns.
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
                ft.as_mut_accessor().set_tp(FieldTypeTp::Double);
                ft
            },
        ];

        let store = {
            let kv = data
                .into_iter()
                .map(|(row_id, columns)| {
                    let key = Key::from_raw(&table::encode_row_key(TABLE_ID, row_id));
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

        let key_ranges_point = vec![
            range_each_row[2].clone(),
            range_each_row[3].clone(),
            range_each_row[1].clone(),
            range_each_row[0].clone(),
            range_each_row[4].clone(),
        ];

        // Case 1: Point scan PK
        {
            let mut executor = BatchTableScanExecutor::new(
                store.clone(),
                Arc::new(EvalConfig::default()),
                vec![columns_info[0].clone()],
                key_ranges_point.clone(),
                false,
            )
            .unwrap();

            // First, let's fetch 1 row. There should be 4 rows remaining.
            let result = executor.next_batch(1);
            assert!(!result.is_drained.as_ref().unwrap());
            assert_eq!(result.data.columns_len(), 1);
            assert_eq!(result.data.rows_len(), 1);
            assert!(result.data[0].is_decoded());
            assert_eq!(result.data[0].decoded().as_int_slice(), &[Some(4)]);

            // Then, fetch all remaining rows using a larger batch size.
            let result = executor.next_batch(10);
            // This time it should be drained.
            assert!(result.is_drained.as_ref().unwrap());
            assert_eq!(result.data.columns_len(), 1);
            assert_eq!(result.data.rows_len(), 4);
            assert!(result.data[0].is_decoded());
            assert_eq!(
                result.data[0].decoded().as_int_slice(),
                &[Some(5), Some(3), Some(1), Some(6)]
            );
        }

        // Case 2: Point scan column foo, which does not have default value.
        {
            let mut executor = BatchTableScanExecutor::new(
                store.clone(),
                Arc::new(EvalConfig::default()),
                vec![columns_info[1].clone()],
                key_ranges_point.clone(),
                false,
            )
            .unwrap();

            // First, fetch 2 rows. 3 rows remaining.
            let mut result = executor.next_batch(2);
            assert!(!result.is_drained.as_ref().unwrap());
            assert_eq!(result.data.columns_len(), 1);
            assert_eq!(result.data.rows_len(), 2);
            assert!(result.data[0].is_raw());
            result.data[0].decode(&Tz::utc(), &schema[1]).unwrap();
            assert_eq!(result.data[0].decoded().as_int_slice(), &[None, None]);

            // Then, fetch 3 rows. 0 rows remaining (but we allow that is_drained == false).
            let mut result = executor.next_batch(3);
            assert!(!result.is_drained.as_ref().unwrap());
            assert_eq!(result.data.columns_len(), 1);
            assert_eq!(result.data.rows_len(), 3);
            assert!(result.data[0].is_raw());
            result.data[0].decode(&Tz::utc(), &schema[1]).unwrap();
            assert_eq!(
                result.data[0].decoded().as_int_slice(),
                &[Some(-5), Some(10), None]
            );

            // Finally, even if we want only 1 row, the executor should be drained.
            let result = executor.next_batch(1);
            assert!(result.is_drained.as_ref().unwrap());
            assert_eq!(result.data.columns_len(), 1);
            assert_eq!(result.data.rows_len(), 0);
        }

        // Case 3: Point scan column bar, which has default values.
        {
            let mut executor = BatchTableScanExecutor::new(
                store.clone(),
                Arc::new(EvalConfig::default()),
                vec![columns_info[2].clone()],
                key_ranges_point.clone(),
                false,
            )
            .unwrap();

            let mut result = executor.next_batch(10);
            assert!(result.is_drained.as_ref().unwrap());
            assert_eq!(result.data.columns_len(), 1);
            assert_eq!(result.data.rows_len(), 5);
            assert!(result.data[0].is_raw());
            result.data[0].decode(&Tz::utc(), &schema[2]).unwrap();
            assert_eq!(
                result.data[0].decoded().as_real_slice(),
                &[Some(4.5), Some(0.1), None, Some(5.2), Some(4.5)]
            );
        }

        // Case 4: Point scan multiple columns
        {
            let mut executor = BatchTableScanExecutor::new(
                store.clone(),
                Arc::new(EvalConfig::default()),
                vec![columns_info[0].clone(), columns_info[2].clone()],
                key_ranges_point.clone(),
                false,
            )
            .unwrap();

            let mut result = executor.next_batch(10);
            assert!(result.is_drained.as_ref().unwrap());
            assert_eq!(result.data.columns_len(), 2);
            assert_eq!(result.data.rows_len(), 5);
            assert!(result.data[0].is_decoded());
            assert_eq!(
                result.data[0].decoded().as_int_slice(),
                &[Some(4), Some(5), Some(3), Some(1), Some(6)]
            );
            assert!(result.data[1].is_raw());
            result.data[1].decode(&Tz::utc(), &schema[2]).unwrap();
            assert_eq!(
                result.data[1].decoded().as_real_slice(),
                &[Some(4.5), Some(0.1), None, Some(5.2), Some(4.5)]
            );
        }

        let key_ranges_all = vec![{
            let mut range = KeyRange::new();
            range.set_start(table::encode_row_key(TABLE_ID, std::i64::MIN));
            range.set_end(table::encode_row_key(TABLE_ID, std::i64::MAX));
            range
        }];

        // Case 5: Range scan PK
        {
            let mut executor = BatchTableScanExecutor::new(
                store.clone(),
                Arc::new(EvalConfig::default()),
                vec![columns_info[0].clone()],
                key_ranges_all.clone(),
                false,
            )
            .unwrap();

            // First, let's fetch 1 row. There should be 4 rows remaining.
            let result = executor.next_batch(1);
            assert!(!result.is_drained.as_ref().unwrap());
            assert_eq!(result.data.columns_len(), 1);
            assert_eq!(result.data.rows_len(), 1);
            assert!(result.data[0].is_decoded());
            assert_eq!(result.data[0].decoded().as_int_slice(), &[Some(1)]);

            // Then, fetch 4 rows. There should be 0 remaining rows.
            let result = executor.next_batch(4);
            // This time it should be drained.
            assert!(!result.is_drained.as_ref().unwrap());
            assert_eq!(result.data.columns_len(), 1);
            assert_eq!(result.data.rows_len(), 4);
            assert!(result.data[0].is_decoded());
            assert_eq!(
                result.data[0].decoded().as_int_slice(),
                &[Some(3), Some(4), Some(5), Some(6)]
            );

            // There should be no more results and the executor is drained.
            let result = executor.next_batch(1);
            assert!(result.is_drained.as_ref().unwrap());
            assert_eq!(result.data.columns_len(), 1);
            assert_eq!(result.data.rows_len(), 0);
        }

        // Case 6: Range scan multiple columns
        {
            let mut executor = BatchTableScanExecutor::new(
                store.clone(),
                Arc::new(EvalConfig::default()),
                vec![
                    columns_info[2].clone(),
                    columns_info[0].clone(),
                    columns_info[1].clone(),
                ],
                key_ranges_all.clone(),
                false,
            )
            .unwrap();

            // First, let's fetch 3 rows. There should be 2 rows remaining.
            let mut result = executor.next_batch(3);
            assert!(!result.is_drained.as_ref().unwrap());
            assert_eq!(result.data.columns_len(), 3);
            assert_eq!(result.data.rows_len(), 3);
            assert!(result.data[0].is_raw());
            result.data[0].decode(&Tz::utc(), &schema[2]).unwrap();
            assert_eq!(
                result.data[0].decoded().as_real_slice(),
                &[Some(5.2), None, Some(4.5)]
            );
            assert!(result.data[1].is_decoded());
            assert_eq!(
                result.data[1].decoded().as_int_slice(),
                &[Some(1), Some(3), Some(4)]
            );
            assert!(result.data[2].is_raw());
            result.data[2].decode(&Tz::utc(), &schema[1]).unwrap();
            assert_eq!(
                result.data[2].decoded().as_int_slice(),
                &[Some(10), Some(-5), None]
            );

            // Fetch 3 rows. We should only get 2 rows and the executor should be drained.
            let mut result = executor.next_batch(3);
            assert!(result.is_drained.as_ref().unwrap());
            assert_eq!(result.data.columns_len(), 3);
            assert_eq!(result.data.rows_len(), 2);
            assert!(result.data[0].is_raw());
            result.data[0].decode(&Tz::utc(), &schema[2]).unwrap();
            assert_eq!(
                result.data[0].decoded().as_real_slice(),
                &[Some(0.1), Some(4.5)]
            );
            assert!(result.data[1].is_decoded());
            assert_eq!(result.data[1].decoded().as_int_slice(), &[Some(5), Some(6)]);
            assert!(result.data[2].is_raw());
            result.data[2].decode(&Tz::utc(), &schema[1]).unwrap();
            assert_eq!(result.data[2].decoded().as_int_slice(), &[None, None]);
        }

        // Case 7. Mixed range scan and point scan
        {
            let key_ranges = vec![
                {
                    let mut range = KeyRange::new();
                    range.set_start(table::encode_row_key(TABLE_ID, std::i64::MIN));
                    range.set_end(table::encode_row_key(TABLE_ID, 3));
                    range
                },
                {
                    let mut range = KeyRange::new();
                    range.set_start(table::encode_row_key(TABLE_ID, 3));
                    range.set_end(table::encode_row_key(TABLE_ID, 4));
                    range
                },
                {
                    let mut range = KeyRange::new();
                    range.set_start(table::encode_row_key(TABLE_ID, 4));
                    range.set_end(table::encode_row_key(TABLE_ID, std::i64::MAX));
                    range
                },
            ];

            let mut executor = BatchTableScanExecutor::new(
                store.clone(),
                Arc::new(EvalConfig::default()),
                vec![columns_info[2].clone()],
                key_ranges.clone(),
                false,
            )
            .unwrap();

            let mut result = executor.next_batch(10);
            assert!(result.is_drained.as_ref().unwrap());
            assert_eq!(result.data.columns_len(), 1);
            assert_eq!(result.data.rows_len(), 5);
            assert!(result.data[0].is_raw());
            result.data[0].decode(&Tz::utc(), &schema[2]).unwrap();
            assert_eq!(
                result.data[0].decoded().as_real_slice(),
                &[Some(5.2), None, Some(4.5), Some(0.1), Some(4.5)]
            );
        }

        // Case 8. PK is in the middle of the schema
        {
            let mut executor = BatchTableScanExecutor::new(
                store.clone(),
                Arc::new(EvalConfig::default()),
                vec![
                    columns_info[1].clone(),
                    columns_info[0].clone(),
                    columns_info[2].clone(),
                ],
                key_ranges_all.clone(),
                false,
            )
            .unwrap();

            let mut result = executor.next_batch(10);
            assert!(result.is_drained.as_ref().unwrap());
            assert_eq!(result.data.columns_len(), 3);
            assert_eq!(result.data.rows_len(), 5);
            assert!(result.data[0].is_raw());
            result.data[0].decode(&Tz::utc(), &schema[1]).unwrap();
            assert_eq!(
                result.data[0].decoded().as_int_slice(),
                &[Some(10), Some(-5), None, None, None]
            );
            assert!(result.data[1].is_decoded());
            assert_eq!(
                result.data[1].decoded().as_int_slice(),
                &[Some(1), Some(3), Some(4), Some(5), Some(6)]
            );
            assert!(result.data[2].is_raw());
            result.data[2].decode(&Tz::utc(), &schema[2]).unwrap();
            assert_eq!(
                result.data[2].decoded().as_real_slice(),
                &[Some(5.2), None, Some(4.5), Some(0.1), Some(4.5)]
            );
        }

        // Case 9. PK is the last column in schema
        {
            let mut executor = BatchTableScanExecutor::new(
                store.clone(),
                Arc::new(EvalConfig::default()),
                vec![
                    columns_info[1].clone(),
                    columns_info[2].clone(),
                    columns_info[0].clone(),
                ],
                key_ranges_all.clone(),
                false,
            )
            .unwrap();

            let mut result = executor.next_batch(10);
            assert!(result.is_drained.as_ref().unwrap());
            assert_eq!(result.data.columns_len(), 3);
            assert_eq!(result.data.rows_len(), 5);
            assert!(result.data[0].is_raw());
            result.data[0].decode(&Tz::utc(), &schema[1]).unwrap();
            assert_eq!(
                result.data[0].decoded().as_int_slice(),
                &[Some(10), Some(-5), None, None, None]
            );
            assert!(result.data[1].is_raw());
            result.data[1].decode(&Tz::utc(), &schema[2]).unwrap();
            assert_eq!(
                result.data[1].decoded().as_real_slice(),
                &[Some(5.2), None, Some(4.5), Some(0.1), Some(4.5)]
            );
            assert!(result.data[2].is_decoded());
            assert_eq!(
                result.data[2].decoded().as_int_slice(),
                &[Some(1), Some(3), Some(4), Some(5), Some(6)]
            );
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
