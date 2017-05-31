// Copyright 2017 PingCAP, Inc.
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

use storage::txn::Result;
use kvproto::coprocessor::KeyRange;
use storage::{Key, Value, Snapshot, ScanMode, Statistics};
use storage::mvcc::MvccReader;
use util::escape;
// `Scanner` is a helper struct to wrap all common scan operations
// for `TableScanExecutor` and `IndexScanExecutor`
pub struct Scanner<'a> {
    reader: MvccReader<'a>,
    seek_key: Option<Vec<u8>>,
    desc: bool,
    start_ts: u64,
}

impl<'a> Scanner<'a> {
    pub fn new(desc: bool,
               key_only: bool,
               snapshot: &'a Snapshot,
               statistics: &'a mut Statistics,
               start_ts: u64)
               -> Scanner<'a> {
        let scan_mode = if desc {
            ScanMode::Backward
        } else {
            ScanMode::Forward
        };
        let mut reader = MvccReader::new(snapshot, statistics, Some(scan_mode), true, None);
        reader.set_key_only(key_only);
        Scanner {
            reader: reader,
            seek_key: None,
            desc: desc,
            start_ts: start_ts,
        }
    }

    pub fn next_row(&mut self, range: &KeyRange) -> Result<Option<(Vec<u8>, Value)>> {
        if self.seek_key.is_none() {
            self.init_with_range(range);
        }
        let seek_key = self.seek_key.take().unwrap();
        if range.get_start() > range.get_end() {
            return Ok(None);
        }
        let kv = if self.desc {
            try!(self.reader.reverse_seek(Key::from_raw(&seek_key), self.start_ts))
        } else {
            try!(self.reader.seek(Key::from_raw(&seek_key), self.start_ts))
        };

        let (key, value) = match kv {
            Some((key, value)) => (box_try!(key.raw()), value),
            None => return Ok(None),
        };

        if range.get_start() > key.as_slice() || range.get_end() <= key.as_slice() {
            debug!("key: {} out of range [{}, {})",
                   escape(&key),
                   escape(range.get_start()),
                   escape(range.get_end()));
            return Ok(None);
        }
        Ok(Some((key, value)))
    }

    pub fn get_row(&mut self, key: &[u8]) -> Result<Option<Value>> {
        let data = try!(self.reader
            .get(&Key::from_raw(key), self.start_ts));
        Ok(data)
    }

    #[inline]
    pub fn set_seek_key(&mut self, seek_key: Option<Vec<u8>>) {
        self.seek_key = seek_key;
    }

    pub fn init_with_range(&mut self, range: &KeyRange) {
        if self.desc {
            self.reader.reset(None);
            self.seek_key = Some(range.get_end().to_vec());
            return;
        }
        let upper_bound = Some(Key::from_raw(range.get_end()).encoded().to_vec());
        self.reader.reset(upper_bound);
        self.seek_key = Some(range.get_start().to_vec());
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use std::i64;
    use util::codec::mysql::types;
    use util::codec::datum::{self, Datum};
    use util::codec::number::NumberEncoder;
    use util::codec::table;
    use util::collections::HashMap;
    use tipb::schema::ColumnInfo;
    use kvproto::kvrpcpb::Context;
    use storage::mvcc::MvccTxn;
    use storage::{make_key, Mutation, ALL_CFS, Options, Statistics, Snapshot};
    use storage::engine::{self, Engine, TEMP_DIR, Modify};
    use server::coprocessor::endpoint::prefix_next;

    pub fn new_col_info(cid: i64, tp: u8) -> ColumnInfo {
        let mut col_info = ColumnInfo::new();
        col_info.set_tp(tp as i32);
        col_info.set_column_id(cid);
        col_info
    }

    pub struct Data {
        pub kv_data: Vec<(Vec<u8>, Vec<u8>)>,
        pub pk: Vec<u8>,
        pub pk_handle: i64,
        // encode_data[row_id][column_id]=>value
        pub encode_data: Vec<HashMap<i64, Vec<u8>>>,
        pub cols: Vec<ColumnInfo>,
    }

    impl Data {
        pub fn get_prev_2_cols(&self) -> Vec<ColumnInfo> {
            let col1 = self.cols[0].clone();
            let col2 = self.cols[1].clone();
            vec![col1, col2]
        }

        pub fn get_index_cols(&self) -> Vec<ColumnInfo> {
            vec![self.cols[1].clone(), self.cols[2].clone()]
        }

        pub fn get_col_pk(&self) -> ColumnInfo {
            let mut pk_col = new_col_info(0, types::LONG);
            pk_col.set_pk_handle(true);
            pk_col
        }
    }

    pub fn prepare_table_data(key_number: usize, table_id: i64) -> Data {
        let cols = vec![new_col_info(1, types::LONG_LONG),
                        new_col_info(2, types::VARCHAR),
                        new_col_info(3, types::NEW_DECIMAL)];

        let mut kv_data = Vec::new();
        let mut pk = Vec::new();
        let mut pk_handle = 0 as i64;
        let mut encode_data = Vec::new();

        for handle in 0..key_number {
            let row = map![
                1 => Datum::I64(handle as i64),
                2 => Datum::Bytes(b"abc".to_vec()),
                3 => Datum::Dec(10.into())
            ];
            let mut encode_value = HashMap::default();
            let col_ids: Vec<_> = row.iter().map(|(&id, _)| id).collect();
            let col_values: Vec<_> = row.iter()
                .map(|(k, v)| {
                    let f = table::flatten(v.clone()).unwrap();
                    let value = datum::encode_value(&[f]).unwrap();
                    encode_value.insert(*k, value);
                    v.clone()
                })
                .collect();

            let value = table::encode_row(col_values, &col_ids).unwrap();
            let mut buf = vec![];
            buf.encode_i64(handle as i64).unwrap();
            let key = table::encode_row_key(table_id, &buf);
            if pk.is_empty() {
                pk = key.clone();
                pk_handle = handle as i64;
            }
            encode_data.push(encode_value);
            kv_data.push((key, value));
        }
        Data {
            kv_data: kv_data,
            pk: pk,
            pk_handle: pk_handle,
            encode_data: encode_data,
            cols: cols,
        }
    }

    const START_TS: u64 = 10;
    const COMMIT_TS: u64 = 20;

    pub struct TestStore {
        pk: Vec<u8>,
        snapshot: Box<Snapshot>,
        ctx: Context,
        engine: Box<Engine>,
    }

    impl TestStore {
        pub fn new(kv_data: &[(Vec<u8>, Vec<u8>)], pk: Vec<u8>) -> TestStore {
            let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();
            let ctx = Context::new();
            let snapshot = engine.snapshot(&ctx).unwrap();
            let mut store = TestStore {
                pk: pk,
                snapshot: snapshot,
                ctx: ctx,
                engine: engine,
            };
            store.init_data(kv_data);
            store
        }

        fn init_data(&mut self, kv_data: &[(Vec<u8>, Vec<u8>)]) {
            let mut statistics = Statistics::default();
            // do prewrite.
            let txn_motifies = {
                let mut txn = MvccTxn::new(self.snapshot.as_ref(), &mut statistics, START_TS, None);
                for &(ref key, ref value) in kv_data {
                    txn.prewrite(Mutation::Put((make_key(key), value.to_vec())),
                                  &self.pk,
                                  &Options::default())
                        .unwrap();
                }
                txn.modifies()
            };
            self.write_modifies(txn_motifies);
            // do commit
            let txn_modifies = {
                let mut txn = MvccTxn::new(self.snapshot.as_ref(), &mut statistics, START_TS, None);
                for &(ref key, _) in kv_data {
                    txn.commit(&make_key(key), COMMIT_TS).unwrap();
                }
                txn.modifies()
            };
            self.write_modifies(txn_modifies);
        }

        #[inline]
        fn write_modifies(&mut self, txn: Vec<Modify>) {
            self.engine.write(&self.ctx, txn).unwrap();
            self.snapshot = self.engine.snapshot(&self.ctx).unwrap()
        }

        pub fn get_snapshot(&mut self) -> (&Snapshot, u64) {
            (self.snapshot.as_ref(), COMMIT_TS + 1)
        }
    }

    #[inline]
    pub fn get_range(table_id: i64, start: i64, end: i64) -> KeyRange {
        let mut start_buf = Vec::with_capacity(8);
        start_buf.encode_i64(start).unwrap();
        let mut end_buf = Vec::with_capacity(8);
        end_buf.encode_i64(end).unwrap();
        let mut key_range = KeyRange::new();
        key_range.set_start(table::encode_row_key(table_id, &start_buf));
        key_range.set_end(table::encode_row_key(table_id, &end_buf));
        key_range
    }

    #[test]
    fn test_point_get() {
        let pk = b"key1".to_vec();
        let pv = b"value1";
        let test_data = vec![
            (pk.clone(), pv.to_vec()),
            (b"key2".to_vec(), b"value2".to_vec()),
        ];
        let mut statistics = Statistics::default();
        let mut test_store = TestStore::new(&test_data, pk.clone());
        let (snapshot, start_ts) = test_store.get_snapshot();
        let mut scanner = Scanner::new(false, false, snapshot, &mut statistics, start_ts);
        let data = scanner.get_row(&pk).unwrap().unwrap();
        assert_eq!(data, pv);
    }

    #[test]
    fn test_scan() {
        let table_id = 1;
        let pk = table::encode_row_key(table_id, b"key1");
        let pv = b"value1";
        let test_data = vec![
            (pk.clone(), pv.to_vec()),
            (table::encode_row_key(table_id, b"key2"), b"value2".to_vec()),
        ];
        let mut statistics = Statistics::default();
        let mut test_store = TestStore::new(&test_data, pk.clone());
        let (snapshot, start_ts) = test_store.get_snapshot();
        let mut scanner = Scanner::new(false, false, snapshot, &mut statistics, start_ts);
        let range = get_range(table_id, i64::MIN, i64::MAX);
        for &(ref k, ref v) in &test_data {
            let (key, value) = scanner.next_row(&range).unwrap().unwrap();
            let seek_key = prefix_next(&key);
            scanner.set_seek_key(Some(seek_key));
            assert_eq!(*k, key);
            assert_eq!(*v, value);
        }
        assert!(scanner.next_row(&range).unwrap().is_none());
    }

    #[test]
    fn test_reverse_scan() {
        let table_id = 1;
        let key_number = 10;
        let mut data = prepare_table_data(key_number, table_id);
        let mut statistics = Statistics::default();
        let mut test_store = TestStore::new(&data.kv_data, data.pk.clone());
        let (snapshot, start_ts) = test_store.get_snapshot();
        let mut scanner = Scanner::new(true, false, snapshot, &mut statistics, start_ts);
        let range = get_range(table_id, i64::MIN, i64::MAX);
        data.kv_data.reverse();
        for &(ref k, ref v) in &data.kv_data {
            let (key, value) = scanner.next_row(&range).unwrap().unwrap();
            let seek_key = table::truncate_as_row_key(&key).unwrap().to_vec();
            scanner.set_seek_key(Some(seek_key));
            assert_eq!(*k, key);
            assert_eq!(*v, value);
        }
        assert!(scanner.next_row(&range).unwrap().is_none());
    }

    #[test]
    fn test_scan_key_only() {
        let table_id = 1;
        let pk = table::encode_row_key(table_id, b"key1");
        let pv = b"value1";
        let test_data = vec![
            (pk.clone(), pv.to_vec()),
            (table::encode_row_key(table_id,b"key2"), b"value2".to_vec()),
        ];
        let mut statistics = Statistics::default();
        let mut test_store = TestStore::new(&test_data, pk.clone());
        let (snapshot, start_ts) = test_store.get_snapshot();
        let mut scanner = Scanner::new(false, true, snapshot, &mut statistics, start_ts);

        let range = get_range(table_id, i64::MIN, i64::MAX);
        let (_, value) = scanner.next_row(&range).unwrap().unwrap();
        assert!(value.is_empty());
    }

    #[test]
    fn test_init_with_range() {
        let table_id = 1;
        let pk = table::encode_row_key(table_id, b"key1");
        let pv = b"value1";
        let test_data = vec![
            (pk.clone(), pv.to_vec()),
        ];
        let mut statistics = Statistics::default();
        let mut test_store = TestStore::new(&test_data, pk.clone());
        let (snapshot, start_ts) = test_store.get_snapshot();
        let mut scanner = Scanner::new(true, false, snapshot, &mut statistics, start_ts);
        let range = get_range(table_id, i64::MIN, i64::MAX);
        // 1. seek_key is some
        scanner.set_seek_key(Some(pk.clone()));
        assert_eq!(scanner.seek_key.take().unwrap(), pk.clone());

        // 1. desc scan
        scanner.desc = true;
        scanner.init_with_range(&range);
        assert_eq!(scanner.seek_key.take().unwrap(), range.get_end());

        // 1.asc scan
        scanner.desc = false;
        scanner.init_with_range(&range);
        assert_eq!(scanner.seek_key.take().unwrap(), range.get_start());
    }
}
