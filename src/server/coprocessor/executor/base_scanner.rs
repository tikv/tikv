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

use server::coprocessor::Result;
use kvproto::coprocessor::KeyRange;
use storage::{Key, Value, SnapshotStore, Statistics, ScanMode};
use util::escape;

// Scanner for TableScan && IndexScan
pub struct BaseScanner<'a> {
    store: SnapshotStore<'a>,
    seek_key: Option<Vec<u8>>,
    scan_mode: ScanMode,
    upper_bound: Option<Vec<u8>>,
    region_start: Vec<u8>,
    region_end: Vec<u8>,
    statistics: &'a mut Statistics,
    desc: bool,
    key_only: bool,
}


impl<'a> BaseScanner<'a> {
    pub fn new(desc: bool,
               key_only: bool,
               store: SnapshotStore<'a>,
               region_start: Vec<u8>,
               region_end: Vec<u8>,
               statistics: &'a mut Statistics)
               -> BaseScanner<'a> {

        let scan_mode = if desc {
            ScanMode::Backward
        } else {
            ScanMode::Forward
        };
        BaseScanner {
            store: store,
            seek_key: None,
            scan_mode: scan_mode,
            upper_bound: None,
            region_start: region_start,
            region_end: region_end,
            statistics: statistics,
            desc: desc,
            key_only: key_only,
        }
    }

    pub fn get_row_from_range(&mut self, range: &KeyRange) -> (Result<Option<(Vec<u8>, Value)>>) {
        let seek_key = self.prepare_and_get_seek_key(range);
        if range.get_start() > range.get_end() {
            return Ok(None);
        }
        let mut scanner = box_try!(self.store.scanner(self.scan_mode,
                                                      self.key_only,
                                                      self.upper_bound.clone(),
                                                      self.statistics));
        let kv = if self.desc {
            box_try!(scanner.reverse_seek(Key::from_raw(&seek_key)))
        } else {
            box_try!(scanner.seek(Key::from_raw(&seek_key)))
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

    pub fn get_row_from_point(&mut self, key: &[u8]) -> Result<Option<Value>> {
        let data = box_try!(self.store
            .get(&Key::from_raw(key), &mut self.statistics));
        Ok(data)
    }

    #[inline]
    pub fn set_seek_key(&mut self, seek_key: Option<Vec<u8>>) {
        self.seek_key = seek_key;
    }

    fn prepare_and_get_seek_key(&mut self, range: &KeyRange) -> (Vec<u8>) {
        if self.seek_key.is_some() {
            let seek_key = self.seek_key.take().unwrap();
            self.seek_key = None;
            return seek_key;
        }
        self.upper_bound = None;
        if self.desc {
            let range_end = range.get_end().to_vec();
            let seek_key = if self.region_end.is_empty() || range_end < self.region_end {
                self.region_end.clone()
            } else {
                range_end
            };
            return seek_key;
        }

        if range.has_end() {
            self.upper_bound = Some(Key::from_raw(range.get_end()).encoded().clone());
        }

        let range_start = range.get_start().to_vec();
        if range_start > self.region_start {
            range_start
        } else {
            self.region_start.clone()
        }
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
    use util::HashMap;
    use tipb::schema::ColumnInfo;
    use kvproto::kvrpcpb::Context;
    use storage::SnapshotStore;
    use storage::mvcc::MvccTxn;
    use storage::{make_key, Mutation, ALL_CFS, Options, Statistics};
    use storage::engine::{self, Engine, TEMP_DIR, Snapshot};
    use server::coprocessor::endpoint::prefix_next;


    fn new_col_info(cid: i64, tp: u8) -> ColumnInfo {
        let mut col_info = ColumnInfo::new();
        col_info.set_tp(tp as i32);
        col_info.set_column_id(cid);
        col_info
    }

    fn flatten(data: Datum) -> Result<Datum> {
        match data {
            Datum::Dur(d) => Ok(Datum::I64(d.to_nanos())),
            Datum::Time(t) => Ok(Datum::U64(t.to_packed_u64())),
            _ => Ok(data),
        }
    }

    pub struct Data {
        pub data: Vec<(Vec<u8>, Vec<u8>)>,
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
    }

    pub fn prepare_table_data(key_number: usize, table_id: i64) -> Data {
        let cols = vec![new_col_info(1, types::LONG_LONG),
                        new_col_info(2, types::VARCHAR),
                        new_col_info(3, types::NEW_DECIMAL)];

        let mut data = Vec::new();
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
                    let f = flatten(v.clone()).unwrap();
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
            data.push((key, value));
        }
        Data {
            data: data,
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
        pub fn new(data: &[(Vec<u8>, Vec<u8>)], pk: Vec<u8>) -> TestStore {
            let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();
            let ctx = Context::new();
            let snapshot = engine.snapshot(&ctx).unwrap();
            let mut store = TestStore {
                pk: pk,
                snapshot: snapshot,
                ctx: ctx,
                engine: engine,
            };
            store.init_data(data);
            store
        }

        fn init_data(&mut self, data: &[(Vec<u8>, Vec<u8>)]) {
            let mut statistics = Statistics::default();
            // do prewrite.
            {
                let mut txn = MvccTxn::new(self.snapshot.as_ref(), &mut statistics, START_TS, None);
                for &(ref key, ref value) in data {
                    txn.prewrite(Mutation::Put((make_key(key), value.to_vec())),
                                  &self.pk,
                                  &Options::default())
                        .unwrap();
                }
                self.engine.write(&self.ctx, txn.modifies()).unwrap();
            }
            self.refresh_snapshot();
            // do commit
            {
                let mut txn = MvccTxn::new(self.snapshot.as_ref(), &mut statistics, START_TS, None);
                for &(ref key, _) in data {
                    txn.commit(&make_key(key), COMMIT_TS).unwrap();
                }
                self.engine.write(&self.ctx, txn.modifies()).unwrap();
            }
            self.refresh_snapshot();
        }

        #[inline]
        fn refresh_snapshot(&mut self) {
            self.snapshot = self.engine.snapshot(&self.ctx).unwrap()
        }

        pub fn store(&self) -> SnapshotStore {
            SnapshotStore::new(self.snapshot.as_ref(), COMMIT_TS + 1)
        }
    }

    #[inline]
    pub fn get_region(start: i64, end: i64) -> (Vec<u8>, Vec<u8>) {
        let mut start_buf = Vec::with_capacity(8);
        start_buf.encode_i64(start).unwrap();
        let mut end_buf = Vec::with_capacity(8);
        end_buf.encode_i64(end).unwrap();
        (start_buf, end_buf)
    }

    #[inline]
    pub fn get_range(table_id: i64, start: i64, end: i64) -> KeyRange {
        let mut key_range = KeyRange::new();
        let (left, right) = get_region(start, end);
        key_range.set_start(table::encode_row_key(table_id, &left));
        key_range.set_end(table::encode_row_key(table_id, &right));
        key_range
    }


    #[test]
    fn test_point_get() {
        let pk = b"key1".to_vec();
        let pv = b"value1";
        let test_data = vec![
            (pk.clone(),pv.clone().to_vec()),
            (b"key2".to_vec(),b"value2".to_vec()),
        ];
        let test_store = TestStore::new(&test_data, pk.clone());
        let mut statistics = Statistics::default();
        let store = test_store.store();
        let (region_start, region_end) = get_region(i64::MIN, i64::MAX);
        let mut scanner = BaseScanner::new(false,
                                           false,
                                           store,
                                           region_start,
                                           region_end,
                                           &mut statistics);
        let data = scanner.get_row_from_point(&pk).unwrap().unwrap();
        assert_eq!(data, pv);
    }

    #[test]
    fn test_scan() {
        let table_id = 1;
        let pk = table::encode_row_key(table_id, b"key1");
        let pv = b"value1";
        let test_data = vec![
            (pk.clone(),pv.clone().to_vec()),
            (table::encode_row_key(table_id,b"key2"),b"value2".to_vec()),
        ];
        let test_store = TestStore::new(&test_data, pk.clone());
        let mut statistics = Statistics::default();
        let store = test_store.store();
        let (region_start, region_end) = get_region(i64::MIN, i64::MAX);
        // prepare region_start/region_end
        let mut range = KeyRange::new();
        range.set_start(region_start.clone());
        range.set_end(region_end.clone());

        let mut scanner = BaseScanner::new(false,
                                           false,
                                           store,
                                           region_start,
                                           region_end,
                                           &mut statistics);

        for &(ref k, ref v) in &test_data {
            let (key, value) = scanner.get_row_from_range(&range).unwrap().unwrap();
            let seek_key = prefix_next(&key);
            scanner.set_seek_key(Some(seek_key));
            assert_eq!(*k, key);
            assert_eq!(*v, value);
        }
        assert!(scanner.get_row_from_range(&range).unwrap().is_none());
    }

    #[test]
    fn test_reverse_scan() {
        let table_id = 1;
        let key_number = 10;
        let mut data = prepare_table_data(key_number, table_id);
        let test_store = TestStore::new(&data.data, data.pk.clone());
        let mut statistics = Statistics::default();
        let store = test_store.store();
        let (region_start, region_end) = get_region(i64::MIN, i64::MAX);
        // prepare region_start/region_end
        let mut range = KeyRange::new();
        range.set_start(region_start.clone());
        range.set_end(region_end.clone());

        let mut scanner = BaseScanner::new(true,
                                           false,
                                           store,
                                           region_start,
                                           region_end,
                                           &mut statistics);

        data.data.reverse();
        for &(ref k, ref v) in &data.data {
            let (key, value) = scanner.get_row_from_range(&range).unwrap().unwrap();
            print!("key:{:?},value:{:?}", key, value);
            let seek_key = table::truncate_as_row_key(&key).unwrap().to_vec();
            scanner.set_seek_key(Some(seek_key));
            assert_eq!(*k, key);
            assert_eq!(*v, value);
        }
        assert!(scanner.get_row_from_range(&range).unwrap().is_none());
    }

    #[test]
    fn test_scan_key_only() {
        let table_id = 1;
        let pk = table::encode_row_key(table_id, b"key1");
        let pv = b"value1";
        let test_data = vec![
            (pk.clone(),pv.clone().to_vec()),
            (table::encode_row_key(table_id,b"key2"),b"value2".to_vec()),
        ];
        let test_store = TestStore::new(&test_data, pk.clone());
        let mut statistics = Statistics::default();
        let store = test_store.store();
        let (region_start, region_end) = get_region(i64::MIN, i64::MAX);
        // prepare region_start/region_end
        let mut range = KeyRange::new();
        range.set_start(region_start.clone());
        range.set_end(region_end.clone());

        let mut scanner = BaseScanner::new(false,
                                           true,
                                           store,
                                           region_start,
                                           region_end,
                                           &mut statistics);

        let (_, value) = scanner.get_row_from_range(&range).unwrap().unwrap();
        assert!(value.is_empty());
    }

    #[test]
    fn test_prepare_and_get_seek_key() {
        let table_id = 1;
        let pk = table::encode_row_key(table_id, b"key1");
        let pv = b"value1";
        let test_data = vec![
            (pk.clone(),pv.clone().to_vec()),
        ];
        let test_store = TestStore::new(&test_data, pk.clone());
        let mut statistics = Statistics::default();
        let store = test_store.store();
        let (small_buf, big_buf) = get_region(i64::MIN, i64::MAX);
        // prepare range
        let mut range = KeyRange::new();
        range.set_start(small_buf.clone());
        range.set_end(big_buf.clone());
        let mut scanner = BaseScanner::new(true,
                                           false,
                                           store,
                                           small_buf.clone(),
                                           big_buf.clone(),
                                           &mut statistics);
        // 1. seek_key is some
        scanner.set_seek_key(Some(pk.clone()));
        let seek_key = scanner.prepare_and_get_seek_key(&range);
        assert_eq!(seek_key, pk.clone());

        // 1. seek_key is none. 2. desc scan 3. range.end < region_end
        scanner.region_end = big_buf.clone();
        range.set_end(small_buf.clone());
        let seek_key = scanner.prepare_and_get_seek_key(&range);
        assert_eq!(seek_key, scanner.region_end.clone());
        assert!(scanner.upper_bound.is_none());

        // 1. seek_key is none. 2.desc scan 3.range.end > region_end
        scanner.region_end = small_buf.clone();
        range.set_end(big_buf.clone());
        let seek_key = scanner.prepare_and_get_seek_key(&range);
        assert_eq!(seek_key, range.get_end());

        // 1.seek_key is none. 2.asc scan 3.range.start <= region.start
        scanner.desc = false;
        scanner.region_start = big_buf.clone();
        range.set_start(small_buf.clone());
        let seek_key = scanner.prepare_and_get_seek_key(&range);
        assert_eq!(seek_key, scanner.region_start.clone());
        // assert!(self.upper_bound.is_some());

        // 1. seek_key is none. 2.asc scan 3.range.start > buf_small
        range.set_start(big_buf.clone());
        range.clear_end();
        scanner.region_start = small_buf.clone();
        let seek_key = scanner.prepare_and_get_seek_key(&range);
        assert_eq!(seek_key, range.get_start());
        // assert!(self.upper_bound.is_none());
    }
}
