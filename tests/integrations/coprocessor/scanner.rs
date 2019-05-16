// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#[cfg(test)]
pub mod tests {
    use std::i64;

    use cop_datatype::FieldTypeTp;
    use kvproto::coprocessor::KeyRange;
    use kvproto::kvrpcpb::IsolationLevel;
    use tipb::schema::ColumnInfo;

    use super::super::executor::tests::{get_range, new_col_info, TestStore};
    use cop_dag::codec::datum::{self, Datum};
    use cop_dag::codec::table;
    use cop_dag::util;
    use cop_dag::{ScanOn, Scanner};
    use tikv::storage::SnapshotStore;
    use tikv_util::collections::HashMap;

    pub struct Data {
        pub kv_data: Vec<(Vec<u8>, Vec<u8>)>,
        // expect_rows[row_id][column_id]=>value
        pub expect_rows: Vec<HashMap<i64, Vec<u8>>>,
        pub cols: Vec<ColumnInfo>,
    }

    impl Data {
        pub fn get_prev_2_cols(&self) -> Vec<ColumnInfo> {
            let col1 = self.cols[0].clone();
            let col2 = self.cols[1].clone();
            vec![col1, col2]
        }

        pub fn get_col_pk(&self) -> ColumnInfo {
            let mut pk_col = new_col_info(0, FieldTypeTp::Long);
            pk_col.set_pk_handle(true);
            pk_col
        }
    }

    pub fn prepare_table_data(key_number: usize, table_id: i64) -> Data {
        let cols = vec![
            new_col_info(1, FieldTypeTp::LongLong),
            new_col_info(2, FieldTypeTp::VarChar),
            new_col_info(3, FieldTypeTp::NewDecimal),
        ];

        let mut kv_data = Vec::new();
        let mut expect_rows = Vec::new();

        for handle in 0..key_number {
            let row = map![
                1 => Datum::I64(handle as i64),
                2 => Datum::Bytes(b"abc".to_vec()),
                3 => Datum::Dec(10.into())
            ];
            let mut expect_row = HashMap::default();
            let col_ids: Vec<_> = row.iter().map(|(&id, _)| id).collect();
            let col_values: Vec<_> = row
                .iter()
                .map(|(cid, v)| {
                    let f = table::flatten(v.clone()).unwrap();
                    let value = datum::encode_value(&[f]).unwrap();
                    expect_row.insert(*cid, value);
                    v.clone()
                })
                .collect();

            let value = table::encode_row(col_values, &col_ids).unwrap();
            let key = table::encode_row_key(table_id, handle as i64);
            expect_rows.push(expect_row);
            kv_data.push((key, value));
        }
        Data {
            kv_data,
            expect_rows,
            cols,
        }
    }

    pub fn get_point_range(table_id: i64, handle: i64) -> KeyRange {
        let start_key = table::encode_row_key(table_id, handle);
        let mut end = start_key.clone();
        util::convert_to_prefix_next(&mut end);
        let mut key_range = KeyRange::new();
        key_range.set_start(start_key);
        key_range.set_end(end);
        key_range
    }

    #[test]
    fn test_scan() {
        let table_id = 1;
        let pk = table::encode_row_key(table_id, 1);
        let pv = b"value1";
        let test_data = vec![
            (pk.clone(), pv.to_vec()),
            (table::encode_row_key(table_id, 2), b"value2".to_vec()),
        ];
        let mut test_store = TestStore::new(&test_data);
        let (snapshot, start_ts) = test_store.get_snapshot();
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);
        let range = get_range(table_id, i64::MIN, i64::MAX);
        let mut scanner = Scanner::new(&store, ScanOn::Table, false, false, range).unwrap();
        for &(ref k, ref v) in &test_data {
            let (key, value) = scanner.next_row().unwrap().unwrap();
            assert_eq!(k, &key);
            assert_eq!(*v, value);
        }
        assert!(scanner.next_row().unwrap().is_none());
    }

    #[test]
    fn test_reverse_scan() {
        let table_id = 1;
        let key_number = 10;
        let mut data = prepare_table_data(key_number, table_id);
        let mut test_store = TestStore::new(&data.kv_data);
        let (snapshot, start_ts) = test_store.get_snapshot();
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);
        let range = get_range(table_id, i64::MIN, i64::MAX);
        let mut scanner = Scanner::new(&store, ScanOn::Table, true, false, range).unwrap();
        data.kv_data.reverse();
        for &(ref k, ref v) in &data.kv_data {
            let (key, value) = scanner.next_row().unwrap().unwrap();
            assert_eq!(*k, key);
            assert_eq!(*v, value);
        }
        assert!(scanner.next_row().unwrap().is_none());
    }

    #[test]
    fn test_scan_key_only() {
        let table_id = 1;
        let pk = table::encode_row_key(table_id, 1);
        let pv = b"value1";
        let test_data = vec![
            (pk.clone(), pv.to_vec()),
            (table::encode_row_key(table_id, 2), b"value2".to_vec()),
        ];
        let mut test_store = TestStore::new(&test_data);
        let (snapshot, start_ts) = test_store.get_snapshot();
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);
        let range = get_range(table_id, i64::MIN, i64::MAX);
        let mut scanner = Scanner::new(&store, ScanOn::Table, false, true, range).unwrap();
        let (_, value) = scanner.next_row().unwrap().unwrap();
        assert!(value.is_empty());
    }

    #[test]
    fn test_scan_start_stop() {
        let table_id = 1;
        let pks = vec![1, 2, 3, 4, 5, 7, 10, 15, 20, 25, 26, 27];
        let values: Vec<_> = pks
            .iter()
            .map(|pk| format!("value{}", pk).into_bytes())
            .collect();
        let test_data: Vec<_> = pks
            .into_iter()
            .map(|pk| table::encode_row_key(table_id, pk))
            .zip(values.into_iter())
            .collect();
        let mut test_store = TestStore::new(&test_data);
        let (snapshot, start_ts) = test_store.get_snapshot();
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);

        // `test_take` is used to take `count` keys from the scanner. It calls `start_scan` at
        // beginning, `stop_scan` in the end, producing a range. the range will be checked against
        // `expect_start_pk` and `expect_end_pk`. Pass -1 as pk means the end.
        let test_take = |scanner: &mut Scanner<_>, count, expect_start_pk, expect_end_pk| {
            let mut range = KeyRange::new();
            scanner.start_scan(&mut range);

            let mut keys = Vec::new();
            for _ in 0..count {
                if let Some((key, _)) = scanner.next_row().unwrap() {
                    keys.push(key);
                } else {
                    break;
                }
            }

            let has_more = scanner.stop_scan(&mut range);
            if has_more || !scanner.desc {
                assert_eq!(
                    range.get_start(),
                    table::encode_row_key(table_id, expect_start_pk).as_slice()
                );
            } else {
                assert_eq!(expect_start_pk, -1);
            }
            if has_more || scanner.desc {
                assert_eq!(
                    range.get_end(),
                    table::encode_row_key(table_id, expect_end_pk).as_slice()
                );
            } else {
                assert_eq!(expect_end_pk, -1);
            }

            keys
        };

        let range = get_range(table_id, 1, 26);
        let mut scanner = Scanner::new(&store, ScanOn::Table, false, true, range.clone()).unwrap();
        let mut res = test_take(&mut scanner, 3, 1, 4);
        res.append(&mut test_take(&mut scanner, 3, 4, 8));
        res.append(&mut test_take(&mut scanner, 3, 8, 21));
        res.append(&mut test_take(&mut scanner, 10, 21, -1));

        let expect_keys: Vec<_> = [1, 2, 3, 4, 5, 7, 10, 15, 20, 25]
            .iter()
            .map(|pk| table::encode_row_key(table_id, *pk))
            .collect();
        assert_eq!(res, expect_keys);

        let mut scanner = Scanner::new(&store, ScanOn::Table, true, true, range).unwrap();
        let mut res = test_take(&mut scanner, 3, 15, 26);
        res.append(&mut test_take(&mut scanner, 3, 5, 15));
        res.append(&mut test_take(&mut scanner, 10, -1, 5));
        assert_eq!(res, expect_keys.into_iter().rev().collect::<Vec<_>>());
    }
}
