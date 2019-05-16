// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#[cfg(test)]
mod tests {
    use cop_dag::batch::executors::BatchIndexScanExecutor;
    use cop_dag::batch::interface::BatchExecutor;

    use std::sync::Arc;

    use byteorder::{BigEndian, WriteBytesExt};

    use cop_datatype::{FieldTypeAccessor, FieldTypeTp};
    use kvproto::coprocessor::KeyRange;
    use tipb::expression::FieldType;
    use tipb::schema::ColumnInfo;

    use cop_dag::codec::mysql::Tz;
    use cop_dag::codec::{datum, table, Datum};
    use cop_dag::exec_summary::*;
    use cop_dag::expr::EvalConfig;
    use cop_dag::util::convert_to_prefix_next;
    use tikv::storage::{FixtureStore, Key};

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
