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

use cop_datatype::EvalType;
use kvproto::coprocessor::KeyRange;
use tipb::executor::IndexScan;
use tipb::expression::FieldType;
use tipb::schema::ColumnInfo;

use super::super::interface::*;
use crate::coprocessor::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};
use crate::coprocessor::dag::expr::{EvalConfig, EvalContext};
use crate::coprocessor::dag::Scanner;
use crate::coprocessor::{Error, Result};
use crate::storage::{FixtureStore, Store};

pub struct BatchIndexScanExecutor<C: ExecSummaryCollector, S: Store>(
    super::scan_executor::ScanExecutor<
        C,
        S,
        IndexScanExecutorImpl,
        super::ranges_iter::PointRangeConditional,
    >,
);

impl
    BatchIndexScanExecutor<
        crate::coprocessor::dag::batch_executor::statistics::ExecSummaryCollectorDisabled,
        FixtureStore,
    >
{
    /// Checks whether this executor can be used.
    #[inline]
    pub fn check_supported(descriptor: &IndexScan) -> Result<()> {
        super::scan_executor::check_columns_info_supported(descriptor.get_columns())
            .map_err(|e| box_err!("Unable to use BatchIndexScanExecutor: {}", e))
    }
}

impl<C: ExecSummaryCollector, S: Store> BatchIndexScanExecutor<C, S> {
    pub fn new(
        summary_collector: C,
        store: S,
        config: Arc<EvalConfig>,
        columns_info: Vec<ColumnInfo>,
        key_ranges: Vec<KeyRange>,
        desc: bool,
        unique: bool,
    ) -> Result<Self> {
        // Note: `unique = true` doesn't completely mean that it is a unique index scan. Instead it
        // just means that we can use point-get for this index. In the following scenarios `unique`
        // will be `false`:
        // - scan from a non-unique index
        // - scan from a unique index with like: where unique-index like xxx

        // Note: Unlike table scan executor, the accepted `columns_info` of index scan executor is
        // strictly stipulated. The order of columns in the schema must be the same as index data
        // stored and if PK handle is needed it must be placed as the last one.

        let mut schema = Vec::with_capacity(columns_info.len());
        let mut columns_len_without_handle = 0;
        let mut decode_handle = false;
        for ci in &columns_info {
            schema.push(super::scan_executor::field_type_from_column_info(&ci));
            if ci.get_pk_handle() {
                decode_handle = true;
            } else {
                columns_len_without_handle += 1;
            }
        }

        let imp = IndexScanExecutorImpl {
            context: EvalContext::new(config),
            schema,
            columns_len_without_handle,
            decode_handle,
        };
        let wrapper = super::scan_executor::ScanExecutor::new(
            summary_collector,
            imp,
            store,
            desc,
            key_ranges,
            super::ranges_iter::PointRangeConditional::new(unique),
        )?;
        Ok(Self(wrapper))
    }
}

impl<C: ExecSummaryCollector, S: Store> BatchExecutor for BatchIndexScanExecutor<C, S> {
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

struct IndexScanExecutorImpl {
    /// See `TableScanExecutorImpl`'s `context`.
    context: EvalContext,

    /// See `TableScanExecutorImpl`'s `schema`.
    schema: Vec<FieldType>,

    /// Number of interested columns (exclude PK handle column).
    columns_len_without_handle: usize,

    /// Whether PK handle column is interested. Handle will be always placed in the last column.
    decode_handle: bool,
}

impl super::scan_executor::ScanExecutorImpl for IndexScanExecutorImpl {
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
            crate::coprocessor::dag::ScanOn::Index,
            desc,
            false,
            range,
        )?)
    }

    /// Constructs empty columns, with PK in decoded format and the rest in raw format.
    ///
    /// Note: the structure of the constructed column is the same as table scan executor but due
    /// to different reasons.
    fn build_column_vec(&self, expect_rows: usize) -> LazyBatchColumnVec {
        let columns_len = self.schema.len();
        let mut columns = Vec::with_capacity(columns_len);
        for _ in 0..self.columns_len_without_handle {
            columns.push(LazyBatchColumn::raw_with_capacity(expect_rows));
        }
        if self.decode_handle {
            // For primary key, we construct a decoded `VectorValue` because it is directly
            // stored as i64, without a datum flag, in the value (for unique index).
            // Note that for normal index, primary key is appended at the end of key with a
            // datum flag.
            columns.push(LazyBatchColumn::decoded_with_capacity_and_tp(
                expect_rows,
                EvalType::Int,
            ));
        }

        LazyBatchColumnVec::from(columns)
    }

    fn process_kv_pair(
        &mut self,
        key: &[u8],
        mut value: &[u8],
        columns: &mut LazyBatchColumnVec,
    ) -> Result<()> {
        use crate::coprocessor::codec::{datum, table};
        use crate::util::codec::number;
        use byteorder::{BigEndian, ReadBytesExt};

        // The payload part of the key
        let mut key_payload = &key[table::PREFIX_LEN + table::ID_LEN..];

        for i in 0..self.columns_len_without_handle {
            let (val, remaining) = datum::split_datum(key_payload, false)?;
            columns[i].push_raw(val);
            key_payload = remaining;
        }

        if self.decode_handle {
            // For normal index, it is placed at the end and any columns prior to it are
            // ensured to be interested. For unique index, it is placed in the value.
            let handle_val = if key_payload.is_empty() {
                // This is a unique index, and we should look up PK handle in value.

                // NOTE: it is not `number::decode_i64`.
                value.read_i64::<BigEndian>().map_err(|_| {
                    Error::Other(box_err!("Failed to decode handle in value as i64"))
                })?
            } else {
                // This is a normal index. The remaining payload part is the PK handle.
                // Let's decode it and put in the column.

                let flag = key_payload[0];
                let mut val = &key_payload[1..];

                // TODO: Better to use `push_datum`. This requires us to allow `push_datum`
                // receiving optional time zone first.

                match flag {
                    datum::INT_FLAG => number::decode_i64(&mut val).map_err(|_| {
                        Error::Other(box_err!("Failed to decode handle in key as i64"))
                    })?,
                    datum::UINT_FLAG => {
                        (number::decode_u64(&mut val).map_err(|_| {
                            Error::Other(box_err!("Failed to decode handle in key as u64"))
                        })?) as i64
                    }
                    _ => {
                        return Err(Error::Other(box_err!("Unexpected handle flag {}", flag)));
                    }
                }
            };

            columns[self.columns_len_without_handle]
                .mut_decoded()
                .push_int(Some(handle_val));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use byteorder::{BigEndian, WriteBytesExt};

    use cop_datatype::{FieldTypeAccessor, FieldTypeTp};
    use kvproto::coprocessor::KeyRange;
    use tipb::expression::FieldType;
    use tipb::schema::ColumnInfo;

    use crate::coprocessor::codec::mysql::Tz;
    use crate::coprocessor::codec::{datum, table, Datum};
    use crate::coprocessor::dag::batch_executor::statistics::*;
    use crate::coprocessor::dag::expr::EvalConfig;
    use crate::coprocessor::util::convert_to_prefix_next;
    use crate::storage::{FixtureStore, Key};

    #[test]
    fn test_basic() {
        const TABLE_ID: i64 = 3;
        const INDEX_ID: i64 = 42;

        // Index schema: (INT, FLOAT)

        // the elements in data are: [int index, float index, handle id].
        let data = vec![
            [Datum::I64(-5), Datum::F64(0.3), Datum::I64(10)],
            [Datum::I64(5), Datum::F64(5.1), Datum::I64(5)],
            [Datum::I64(5), Datum::F64(10.5), Datum::I64(2)],
        ];

        // The column info for each column in `data`. Used to build the executor.
        let columns_info = vec![
            {
                let mut ci = ColumnInfo::new();
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
            {
                let mut ci = ColumnInfo::new();
                ci.as_mut_accessor().set_tp(FieldTypeTp::Double);
                ci
            },
            {
                let mut ci = ColumnInfo::new();
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci.set_pk_handle(true);
                ci
            },
        ];

        // The schema of these columns. Used to check executor output.
        let schema = vec![
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
            {
                let mut ft = FieldType::new();
                ft.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ft
            },
        ];

        // Case 1. Normal index.

        // For a normal index, the PK handle is stored in the key and nothing interesting is stored
        // in the value. So let's build corresponding KV data.

        let store = {
            let kv = data
                .iter()
                .map(|datums| {
                    let index_data = datum::encode_key(datums).unwrap();
                    let key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &index_data);
                    let key = Key::from_raw(key.as_slice());
                    let value = vec![];
                    (key, Ok(value))
                })
                .collect();
            FixtureStore::new(kv)
        };

        {
            // Case 1.1. Normal index, without PK, scan total index in reverse order.

            let key_ranges = vec![{
                let mut range = KeyRange::new();
                let start_data = datum::encode_key(&[Datum::Min]).unwrap();
                let start_key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &start_data);
                range.set_start(start_key);
                let end_data = datum::encode_key(&[Datum::Max]).unwrap();
                let end_key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &end_data);
                range.set_end(end_key);
                range
            }];

            let mut executor = BatchIndexScanExecutor::new(
                ExecSummaryCollectorDisabled,
                store.clone(),
                Arc::new(EvalConfig::default()),
                vec![columns_info[0].clone(), columns_info[1].clone()],
                key_ranges,
                true,
                false,
            )
            .unwrap();

            let mut result = executor.next_batch(10);
            assert!(result.is_drained.as_ref().unwrap());
            assert_eq!(result.data.columns_len(), 2);
            assert_eq!(result.data.rows_len(), 3);
            assert!(result.data[0].is_raw());
            result.data[0].decode(&Tz::utc(), &schema[0]).unwrap();
            assert_eq!(
                result.data[0].decoded().as_int_slice(),
                &[Some(5), Some(5), Some(-5)]
            );
            assert!(result.data[1].is_raw());
            result.data[1].decode(&Tz::utc(), &schema[1]).unwrap();
            assert_eq!(
                result.data[1].decoded().as_real_slice(),
                &[Some(10.5), Some(5.1), Some(0.3)]
            );
        }

        {
            // Case 1.2. Normal index, with PK, scan index prefix.

            let key_ranges = vec![{
                let mut range = KeyRange::new();
                let start_data = datum::encode_key(&[Datum::I64(2)]).unwrap();
                let start_key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &start_data);
                range.set_start(start_key);
                let end_data = datum::encode_key(&[Datum::I64(6)]).unwrap();
                let end_key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &end_data);
                range.set_end(end_key);
                range
            }];

            let mut executor = BatchIndexScanExecutor::new(
                ExecSummaryCollectorDisabled,
                store.clone(),
                Arc::new(EvalConfig::default()),
                vec![
                    columns_info[0].clone(),
                    columns_info[1].clone(),
                    columns_info[2].clone(),
                ],
                key_ranges,
                false,
                false,
            )
            .unwrap();

            let mut result = executor.next_batch(10);
            assert!(result.is_drained.as_ref().unwrap());
            assert_eq!(result.data.columns_len(), 3);
            assert_eq!(result.data.rows_len(), 2);
            assert!(result.data[0].is_raw());
            result.data[0].decode(&Tz::utc(), &schema[0]).unwrap();
            assert_eq!(result.data[0].decoded().as_int_slice(), &[Some(5), Some(5)]);
            assert!(result.data[1].is_raw());
            result.data[1].decode(&Tz::utc(), &schema[1]).unwrap();
            assert_eq!(
                result.data[1].decoded().as_real_slice(),
                &[Some(5.1), Some(10.5)]
            );
            assert!(result.data[2].is_decoded());
            assert_eq!(result.data[2].decoded().as_int_slice(), &[Some(5), Some(2)]);
        }

        // Case 2. Unique index.

        // For a unique index, the PK handle is stored in the value.

        let store = {
            let kv = data
                .iter()
                .map(|datums| {
                    let index_data = datum::encode_key(&datums[0..2]).unwrap();
                    let key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &index_data);
                    let key = Key::from_raw(key.as_slice());
                    // PK handle in the value
                    let mut value = vec![];
                    value
                        .write_i64::<BigEndian>(datums[2].as_int().unwrap().unwrap())
                        .unwrap();
                    (key, Ok(value))
                })
                .collect();
            FixtureStore::new(kv)
        };

        {
            // Case 2.1. Unique index, prefix range scan.

            let key_ranges = vec![{
                let mut range = KeyRange::new();
                let start_data = datum::encode_key(&[Datum::I64(5)]).unwrap();
                let start_key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &start_data);
                range.set_start(start_key);
                range.set_end(range.get_start().to_vec());
                convert_to_prefix_next(range.mut_end());
                range
            }];

            let mut executor = BatchIndexScanExecutor::new(
                ExecSummaryCollectorDisabled,
                store.clone(),
                Arc::new(EvalConfig::default()),
                vec![
                    columns_info[0].clone(),
                    columns_info[1].clone(),
                    columns_info[2].clone(),
                ],
                key_ranges,
                false,
                false,
            )
            .unwrap();

            let mut result = executor.next_batch(10);
            assert!(result.is_drained.as_ref().unwrap());
            assert_eq!(result.data.columns_len(), 3);
            assert_eq!(result.data.rows_len(), 2);
            assert!(result.data[0].is_raw());
            result.data[0].decode(&Tz::utc(), &schema[0]).unwrap();
            assert_eq!(result.data[0].decoded().as_int_slice(), &[Some(5), Some(5)]);
            assert!(result.data[1].is_raw());
            result.data[1].decode(&Tz::utc(), &schema[1]).unwrap();
            assert_eq!(
                result.data[1].decoded().as_real_slice(),
                &[Some(5.1), Some(10.5)]
            );
            assert!(result.data[2].is_decoded());
            assert_eq!(result.data[2].decoded().as_int_slice(), &[Some(5), Some(2)]);
        }

        {
            // Case 2.2. Unique index, point scan.

            let key_ranges = vec![{
                let mut range = KeyRange::new();
                let start_data = datum::encode_key(&[Datum::I64(5), Datum::F64(5.1)]).unwrap();
                let start_key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &start_data);
                range.set_start(start_key);
                range.set_end(range.get_start().to_vec());
                convert_to_prefix_next(range.mut_end());
                range
            }];

            let mut executor = BatchIndexScanExecutor::new(
                ExecSummaryCollectorDisabled,
                store.clone(),
                Arc::new(EvalConfig::default()),
                vec![
                    columns_info[0].clone(),
                    columns_info[1].clone(),
                    columns_info[2].clone(),
                ],
                key_ranges,
                false,
                true,
            )
            .unwrap();

            let mut result = executor.next_batch(10);
            assert!(result.is_drained.as_ref().unwrap());
            assert_eq!(result.data.columns_len(), 3);
            assert_eq!(result.data.rows_len(), 1);
            assert!(result.data[0].is_raw());
            result.data[0].decode(&Tz::utc(), &schema[0]).unwrap();
            assert_eq!(result.data[0].decoded().as_int_slice(), &[Some(5)]);
            assert!(result.data[1].is_raw());
            result.data[1].decode(&Tz::utc(), &schema[1]).unwrap();
            assert_eq!(result.data[1].decoded().as_real_slice(), &[Some(5.1)]);
            assert!(result.data[2].is_decoded());
            assert_eq!(result.data[2].decoded().as_int_slice(), &[Some(5)]);
        }
    }
}
