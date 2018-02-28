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

use kvproto::coprocessor::KeyRange;

use coprocessor::endpoint::prefix_next;
use coprocessor::codec::table::truncate_as_row_key;
use storage::{Key, ScanMode, SnapshotStore, Statistics, StoreScanner, Value};
use storage::txn::Result;
use util::escape;

#[derive(Copy, Clone)]
pub enum ScanOn {
    Table,
    Index,
}

// `Scanner` is a helper struct to wrap all common scan operations
// for `TableScanExecutor` and `IndexScanExecutor`
pub struct Scanner {
    scan_mode: ScanMode,
    scan_on: ScanOn,
    key_only: bool,
    seek_key: Vec<u8>,
    scanner: StoreScanner,
    range: KeyRange,
    no_more: bool,
    // statistics_cache caches Statistics because
    // reset_range may re-initialize a StoreScanner.
    statistics_cache: Statistics,
}

impl Scanner {
    pub fn new(
        store: &SnapshotStore,
        scan_on: ScanOn,
        desc: bool,
        key_only: bool,
        range: KeyRange,
    ) -> Result<Scanner> {
        let (scan_mode, seek_key) = if desc {
            (ScanMode::Backward, range.get_end().to_vec())
        } else {
            (ScanMode::Forward, range.get_start().to_vec())
        };
        let scanner = Self::range_scanner(store, scan_mode, key_only, &range)?;

        Ok(Scanner {
            scan_mode: scan_mode,
            scan_on: scan_on,
            key_only: key_only,
            seek_key: seek_key,
            scanner: scanner,
            range: range,
            no_more: false,
            statistics_cache: Statistics::default(),
        })
    }

    fn range_scanner(
        store: &SnapshotStore,
        scan_mode: ScanMode,
        key_only: bool,
        range: &KeyRange,
    ) -> Result<StoreScanner> {
        let lower_bound = Some(Key::from_raw(range.get_start()).encoded().to_vec());
        let upper_bound = Some(Key::from_raw(range.get_end()).encoded().to_vec());
        store.scanner(scan_mode, key_only, lower_bound, upper_bound)
    }

    pub fn reset_range(&mut self, range: KeyRange, store: &SnapshotStore) -> Result<()> {
        self.range = range;
        self.no_more = false;
        match self.scan_mode {
            ScanMode::Backward => self.seek_key = self.range.get_end().to_vec(),
            ScanMode::Forward => self.seek_key = self.range.get_start().to_vec(),
            _ => unreachable!(),
        };

        self.statistics_cache.add(self.scanner.get_statistics());
        self.scanner = Self::range_scanner(store, self.scan_mode, self.key_only, &self.range)?;

        Ok(())
    }

    pub fn next_row(&mut self) -> Result<Option<(Vec<u8>, Value)>> {
        if self.no_more {
            return Ok(None);
        }

        let kv = match self.scan_mode {
            ScanMode::Backward => self.scanner.reverse_seek(Key::from_raw(&self.seek_key))?,
            ScanMode::Forward => self.scanner.seek(Key::from_raw(&self.seek_key))?,
            _ => unreachable!(),
        };

        let (key, value) = match kv {
            Some((key, value)) => (box_try!(key.raw()), value),
            None => {
                self.no_more = true;
                return Ok(None);
            }
        };

        if self.range.start > key || self.range.end <= key {
            debug!(
                "key: {} out of range [{}, {})",
                escape(&key),
                escape(self.range.get_start()),
                escape(self.range.get_end())
            );
            self.no_more = true;
            return Ok(None);
        }

        self.seek_key = match (self.scan_mode, self.scan_on) {
            (ScanMode::Forward, _) => prefix_next(&key),
            (ScanMode::Backward, ScanOn::Table) => box_try!(truncate_as_row_key(&key)).to_vec(),
            (ScanMode::Backward, ScanOn::Index) => key.clone(),
            _ => unreachable!(),
        };
        Ok(Some((key, value)))
    }

    pub fn collect_statistics_into(self, stats: &mut Statistics) {
        stats.add(&self.statistics_cache);
        stats.add(self.scanner.get_statistics());
    }
}

#[cfg(test)]
pub mod test {
    use std::i64;

    use kvproto::kvrpcpb::{Context, IsolationLevel};
    use tipb::schema::ColumnInfo;

    use coprocessor::codec::mysql::types;
    use coprocessor::codec::datum::{self, Datum};
    use coprocessor::codec::table;
    use coprocessor::endpoint::prefix_next;
    use util::collections::HashMap;
    use util::codec::number::NumberEncoder;
    use storage::mvcc::MvccTxn;
    use storage::{make_key, Mutation, Options, Snapshot, SnapshotStore, ALL_CFS};
    use storage::engine::{self, Engine, Modify, TEMP_DIR};

    use super::*;

    pub fn new_col_info(cid: i64, tp: u8) -> ColumnInfo {
        let mut col_info = ColumnInfo::new();
        col_info.set_tp(i32::from(tp));
        col_info.set_column_id(cid);
        col_info
    }

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
        let cols = vec![
            new_col_info(1, types::LONG_LONG),
            new_col_info(2, types::VARCHAR),
            new_col_info(3, types::NEW_DECIMAL),
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
            let col_values: Vec<_> = row.iter()
                .map(|(cid, v)| {
                    let f = table::flatten(v.clone()).unwrap();
                    let value = datum::encode_value(&[f]).unwrap();
                    expect_row.insert(*cid, value);
                    v.clone()
                })
                .collect();

            let value = table::encode_row(col_values, &col_ids).unwrap();
            let mut buf = vec![];
            buf.encode_i64(handle as i64).unwrap();
            let key = table::encode_row_key(table_id, &buf);
            expect_rows.push(expect_row);
            kv_data.push((key, value));
        }
        Data {
            kv_data: kv_data,
            expect_rows: expect_rows,
            cols: cols,
        }
    }

    const START_TS: u64 = 10;
    const COMMIT_TS: u64 = 20;

    pub struct TestStore {
        snapshot: Box<Snapshot>,
        ctx: Context,
        engine: Box<Engine>,
    }

    impl TestStore {
        pub fn new(kv_data: &[(Vec<u8>, Vec<u8>)]) -> TestStore {
            let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();
            let ctx = Context::new();
            let snapshot = engine.snapshot(&ctx).unwrap();
            let mut store = TestStore {
                snapshot: snapshot,
                ctx: ctx,
                engine: engine,
            };
            store.init_data(kv_data);
            store
        }

        fn init_data(&mut self, kv_data: &[(Vec<u8>, Vec<u8>)]) {
            if kv_data.is_empty() {
                return;
            }

            // do prewrite.
            let txn_motifies = {
                let mut txn = MvccTxn::new(
                    self.snapshot.clone(),
                    START_TS,
                    None,
                    IsolationLevel::SI,
                    true,
                );
                let mut pk = vec![];
                for &(ref key, ref value) in kv_data {
                    if pk.is_empty() {
                        pk = key.clone();
                    }
                    txn.prewrite(
                        Mutation::Put((make_key(key), value.to_vec())),
                        &pk,
                        &Options::default(),
                    ).unwrap();
                }
                txn.into_modifies()
            };
            self.write_modifies(txn_motifies);

            // do commit
            let txn_modifies = {
                let mut txn = MvccTxn::new(
                    self.snapshot.clone(),
                    START_TS,
                    None,
                    IsolationLevel::SI,
                    true,
                );
                for &(ref key, _) in kv_data {
                    txn.commit(&make_key(key), COMMIT_TS).unwrap();
                }
                txn.into_modifies()
            };
            self.write_modifies(txn_modifies);
        }

        #[inline]
        fn write_modifies(&mut self, txn: Vec<Modify>) {
            self.engine.write(&self.ctx, txn).unwrap();
            self.snapshot = self.engine.snapshot(&self.ctx).unwrap()
        }

        pub fn get_snapshot(&mut self) -> (Box<Snapshot>, u64) {
            (self.snapshot.clone(), COMMIT_TS + 1)
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

    pub fn get_point_range(table_id: i64, handle: i64) -> KeyRange {
        let mut start_buf = Vec::with_capacity(8);
        start_buf.encode_i64(handle).unwrap();
        let start_key = table::encode_row_key(table_id, &start_buf);
        let end = prefix_next(&start_key);
        let mut key_range = KeyRange::new();
        key_range.set_start(start_key);
        key_range.set_end(end);
        key_range
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
        let pk = table::encode_row_key(table_id, b"key1");
        let pv = b"value1";
        let test_data = vec![
            (pk.clone(), pv.to_vec()),
            (table::encode_row_key(table_id, b"key2"), b"value2".to_vec()),
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
    fn test_seek_key() {
        let table_id = 1;
        let pk = table::encode_row_key(table_id, b"key1");
        let pv = b"value1";
        let test_data = vec![(pk.clone(), pv.to_vec())];
        let mut test_store = TestStore::new(&test_data);
        let (snapshot, start_ts) = test_store.get_snapshot();
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);
        let range = get_range(table_id, i64::MIN, i64::MAX);

        // 1. desc scan
        let scanner = Scanner::new(&store, ScanOn::Table, true, false, range.clone()).unwrap();
        assert_eq!(scanner.seek_key, range.get_end());

        // 2.asc scan
        let scanner = Scanner::new(&store, ScanOn::Table, false, false, range.clone()).unwrap();
        assert_eq!(scanner.seek_key, range.get_start());
    }
}
