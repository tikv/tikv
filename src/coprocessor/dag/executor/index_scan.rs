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

use byteorder::{BigEndian, ReadBytesExt};
use std::iter::Peekable;
use std::mem;
use std::vec::IntoIter;

use kvproto::coprocessor::KeyRange;
use tipb::executor::IndexScan;
use tipb::schema::ColumnInfo;

use coprocessor::codec::{datum, mysql, table};
use coprocessor::util;
use coprocessor::*;

use storage::{Key, SnapshotStore};

use super::scanner::{ScanOn, Scanner};
use super::ExecutorMetrics;
use super::{Executor, Row};

pub struct IndexScanExecutor {
    store: SnapshotStore,
    desc: bool,
    col_ids: Vec<i64>,
    pk_col: Option<ColumnInfo>,
    key_ranges: Peekable<IntoIter<KeyRange>>,
    // The current `KeyRange` scanning on, used to build `scan_range`.
    current_range: Option<KeyRange>,
    // The `KeyRange` scaned between `start_scan` and `stop_scan`.
    scan_range: KeyRange,
    scanner: Option<Scanner>,
    unique: bool,
    // The number of scan keys for each range.
    counts: Option<Vec<i64>>,
    metrics: ExecutorMetrics,
    first_collect: bool,
}

impl IndexScanExecutor {
    pub fn new(
        mut meta: IndexScan,
        mut key_ranges: Vec<KeyRange>,
        store: SnapshotStore,
        unique: bool,
        collect: bool,
    ) -> Result<IndexScanExecutor> {
        box_try!(table::check_table_ranges(&key_ranges));
        let mut pk_col = None;
        let desc = meta.get_desc();
        if desc {
            key_ranges.reverse();
        }
        let cols = meta.mut_columns();
        if cols.last().map_or(false, |c| c.get_pk_handle()) {
            pk_col = Some(cols.pop().unwrap());
        }
        let col_ids = cols.iter().map(|c| c.get_column_id()).collect();
        let counts = if collect { Some(Vec::default()) } else { None };
        Ok(IndexScanExecutor {
            store,
            desc,
            col_ids,
            pk_col,
            key_ranges: key_ranges.into_iter().peekable(),
            current_range: None,
            scan_range: KeyRange::default(),
            scanner: None,
            unique,
            counts,
            metrics: Default::default(),
            first_collect: true,
        })
    }

    pub fn new_with_cols_len(
        cols: i64,
        key_ranges: Vec<KeyRange>,
        store: SnapshotStore,
    ) -> Result<IndexScanExecutor> {
        box_try!(table::check_table_ranges(&key_ranges));
        let col_ids: Vec<i64> = (0..cols).collect();
        Ok(IndexScanExecutor {
            store,
            desc: false,
            col_ids,
            pk_col: None,
            key_ranges: key_ranges.into_iter().peekable(),
            current_range: None,
            scan_range: KeyRange::default(),
            scanner: None,
            unique: false,
            counts: None,
            metrics: ExecutorMetrics::default(),
            first_collect: true,
        })
    }

    fn get_row_from_range_scanner(&mut self) -> Result<Option<Row>> {
        if self.scanner.is_none() {
            return Ok(None);
        }
        self.metrics.scan_counter.inc_range();

        let (key, value) = {
            let scanner = self.scanner.as_mut().unwrap();
            match scanner.next_row()? {
                Some((key, value)) => (key, value),
                None => return Ok(None),
            }
        };
        self.decode_index_key_value(key, value)
    }

    fn decode_index_key_value(&self, key: Vec<u8>, value: Vec<u8>) -> Result<Option<Row>> {
        let (mut values, handle) = box_try!(table::cut_idx_key(key, &self.col_ids));
        let handle = match handle {
            None => box_try!(value.as_slice().read_i64::<BigEndian>()),
            Some(h) => h,
        };

        if let Some(ref pk_col) = self.pk_col {
            let handle_datum = if mysql::has_unsigned_flag(pk_col.get_flag() as u64) {
                // PK column is unsigned
                datum::Datum::U64(handle as u64)
            } else {
                datum::Datum::I64(handle)
            };
            let mut bytes = box_try!(datum::encode_key(&[handle_datum]));
            values.append(pk_col.get_column_id(), &mut bytes);
        }
        Ok(Some(Row::new(handle, values)))
    }

    fn get_row_from_point(&mut self, mut range: KeyRange) -> Result<Option<Row>> {
        self.metrics.scan_counter.inc_point();
        let key = range.take_start();
        let value = self
            .store
            .get(&Key::from_raw(&key), &mut self.metrics.cf_stats)?;
        if let Some(value) = value {
            return self.decode_index_key_value(key, value);
        }
        Ok(None)
    }

    fn new_scanner(&self, range: KeyRange) -> Result<Scanner> {
        // Since the unique index wouldn't always come with
        // self.unique = true. so the key-only would always be false.
        Scanner::new(&self.store, ScanOn::Index, self.desc, false, range).map_err(Error::from)
    }

    fn is_point(&self, range: &KeyRange) -> bool {
        self.unique && util::is_point(range)
    }
}

impl Executor for IndexScanExecutor {
    fn next(&mut self) -> Result<Option<Row>> {
        loop {
            if let Some(row) = self.get_row_from_range_scanner()? {
                if let Some(counts) = self.counts.as_mut() {
                    counts.last_mut().map_or((), |val| *val += 1);
                }
                return Ok(Some(row));
            }
            if let Some(range) = self.key_ranges.next() {
                if let Some(counts) = self.counts.as_mut() {
                    counts.push(0)
                };
                self.current_range = Some(range.clone());
                if self.is_point(&range) {
                    if let Some(row) = self.get_row_from_point(range)? {
                        if let Some(counts) = self.counts.as_mut() {
                            counts.last_mut().map_or((), |val| *val += 1);
                        }
                        return Ok(Some(row));
                    }
                    continue;
                }
                self.scanner = match self.scanner.take() {
                    Some(mut scanner) => {
                        box_try!(scanner.reset_range(range, &self.store));
                        Some(scanner)
                    }
                    None => Some(self.new_scanner(range)?),
                };
                continue;
            }
            return Ok(None);
        }
    }

    fn start_scan(&mut self) {
        if let Some(range) = self.current_range.as_ref() {
            if !util::is_point(range) {
                let scanner = self.scanner.as_ref().unwrap();
                return scanner.start_scan(&mut self.scan_range);
            }
        }

        if let Some(range) = self.key_ranges.peek() {
            if !self.desc {
                self.scan_range.set_start(range.get_start().to_owned());
            } else {
                self.scan_range.set_end(range.get_end().to_owned());
            }
        }
    }

    fn stop_scan(&mut self) -> Option<KeyRange> {
        let mut ret_range = mem::replace(&mut self.scan_range, KeyRange::default());
        match self.current_range.as_ref() {
            Some(range) => {
                if !util::is_point(range) {
                    let scanner = self.scanner.as_ref().unwrap();
                    if scanner.stop_scan(&mut ret_range) {
                        return Some(ret_range);
                    }
                }
                if !self.desc {
                    ret_range.set_end(range.get_end().to_owned());
                } else {
                    ret_range.set_start(range.get_start().to_owned());
                }
            }
            // `stop_scan` will be called only if we get some data from
            // `current_range` so that it's unreachable.
            None => unreachable!(),
        }
        Some(ret_range)
    }

    fn collect_output_counts(&mut self, counts: &mut Vec<i64>) {
        if let Some(cur_counts) = self.counts.as_mut() {
            counts.append(cur_counts);
            cur_counts.push(0);
        }
    }

    fn collect_metrics_into(&mut self, metrics: &mut ExecutorMetrics) {
        metrics.merge(&mut self.metrics);
        if let Some(scanner) = self.scanner.as_mut() {
            scanner.collect_statistics_into(&mut metrics.cf_stats);
        }
        if self.first_collect {
            metrics.executor_count.index_scan += 1;
            self.first_collect = false;
        }
    }
}

#[cfg(test)]
pub mod test {
    use byteorder::{BigEndian, WriteBytesExt};
    use std::i64;

    use kvproto::kvrpcpb::IsolationLevel;
    use protobuf::RepeatedField;
    use tipb::schema::ColumnInfo;

    use coprocessor::codec::datum::{self, Datum};
    use coprocessor::codec::mysql::types;
    use storage::SnapshotStore;
    use util::collections::HashMap;

    use super::super::scanner::test::{new_col_info, Data, TestStore};
    use super::*;

    const TABLE_ID: i64 = 1;
    const INDEX_ID: i64 = 1;
    const KEY_NUMBER: usize = 10;

    // get_idx_range get range for index in [("abc",start),("abc", end))
    pub fn get_idx_range(
        table_id: i64,
        idx_id: i64,
        start: i64,
        end: i64,
        val_start: &Datum,
        val_end: &Datum,
        unique: bool,
    ) -> KeyRange {
        let (_, start_key) = generate_index_data(table_id, idx_id, start, val_start, unique);
        let (_, end_key) = generate_index_data(table_id, idx_id, end, val_end, unique);
        let mut key_range = KeyRange::new();
        key_range.set_start(start_key);
        key_range.set_end(end_key);
        key_range
    }

    pub fn generate_index_data(
        table_id: i64,
        index_id: i64,
        handle: i64,
        col_val: &Datum,
        unique: bool,
    ) -> (HashMap<i64, Vec<u8>>, Vec<u8>) {
        let indice = vec![(2, (*col_val).clone()), (3, Datum::Dec(handle.into()))];
        let mut expect_row = HashMap::default();
        let mut v: Vec<_> = indice
            .iter()
            .map(|&(ref cid, ref value)| {
                expect_row.insert(*cid, datum::encode_key(&[value.clone()]).unwrap());
                value.clone()
            })
            .collect();
        if !unique {
            v.push(Datum::I64(handle));
        }
        let encoded = datum::encode_key(&v).unwrap();
        let idx_key = table::encode_index_seek_key(table_id, index_id, &encoded);
        (expect_row, idx_key)
    }

    pub fn prepare_index_data(
        key_number: usize,
        table_id: i64,
        index_id: i64,
        unique: bool,
    ) -> Data {
        let cols = vec![
            new_col_info(1, types::LONG_LONG),
            new_col_info(2, types::VARCHAR),
            new_col_info(3, types::NEW_DECIMAL),
        ];

        let mut kv_data = Vec::new();
        let mut expect_rows = Vec::new();

        let idx_col_val = Datum::Bytes(b"abc".to_vec());
        for handle in 0..key_number {
            let (expect_row, idx_key) =
                generate_index_data(table_id, index_id, handle as i64, &idx_col_val, unique);
            expect_rows.push(expect_row);
            let value = if unique {
                let mut value = Vec::with_capacity(8);
                value.write_i64::<BigEndian>(handle as i64).unwrap();
                value
            } else {
                vec![1; 0]
            };
            kv_data.push((idx_key, value));
        }
        Data {
            kv_data,
            expect_rows,
            cols,
        }
    }

    pub struct IndexTestWrapper {
        data: Data,
        pub store: TestStore,
        pub scan: IndexScan,
        pub ranges: Vec<KeyRange>,
        cols: Vec<ColumnInfo>,
    }

    impl IndexTestWrapper {
        fn include_pk_cols() -> IndexTestWrapper {
            let unique = false;
            let test_data = prepare_index_data(KEY_NUMBER, TABLE_ID, INDEX_ID, unique);
            let mut wrapper = IndexTestWrapper::new(unique, test_data);
            let mut cols = wrapper.data.get_index_cols();
            cols.push(wrapper.data.get_col_pk());
            wrapper
                .scan
                .set_columns(RepeatedField::from_vec(cols.clone()));
            wrapper.cols = cols;
            wrapper
        }

        pub fn new(unique: bool, test_data: Data) -> IndexTestWrapper {
            let test_store = TestStore::new(&test_data.kv_data);
            let mut scan = IndexScan::new();
            // prepare cols
            let cols = test_data.get_index_cols();
            let col_req = RepeatedField::from_vec(cols.clone());
            scan.set_columns(col_req);
            // prepare range
            let val_start = Datum::Bytes(b"a".to_vec());
            let val_end = Datum::Bytes(b"z".to_vec());
            let range = get_idx_range(
                TABLE_ID,
                INDEX_ID,
                0,
                i64::MAX,
                &val_start,
                &val_end,
                unique,
            );
            let key_ranges = vec![range];
            IndexTestWrapper {
                data: test_data,
                store: test_store,
                scan,
                ranges: key_ranges,
                cols,
            }
        }
    }

    #[test]
    fn test_multiple_ranges() {
        let unique = false;
        let test_data = prepare_index_data(KEY_NUMBER, TABLE_ID, INDEX_ID, unique);
        let mut wrapper = IndexTestWrapper::new(unique, test_data);
        let val_start = Datum::Bytes(b"abc".to_vec());
        let val_end = Datum::Bytes(b"abc".to_vec());
        let r1 = get_idx_range(
            TABLE_ID,
            INDEX_ID,
            0,
            (KEY_NUMBER / 3) as i64,
            &val_start,
            &val_end,
            unique,
        );
        let r2 = get_idx_range(
            TABLE_ID,
            INDEX_ID,
            (KEY_NUMBER / 3) as i64,
            (KEY_NUMBER / 2) as i64,
            &val_start,
            &val_end,
            unique,
        );
        wrapper.ranges = vec![r1, r2];
        let (snapshot, start_ts) = wrapper.store.get_snapshot();
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);

        let mut scanner =
            IndexScanExecutor::new(wrapper.scan, wrapper.ranges, store, false, false).unwrap();

        for handle in 0..KEY_NUMBER / 2 {
            let row = scanner.next().unwrap().unwrap();
            assert_eq!(row.handle, handle as i64);
            assert_eq!(row.data.len(), wrapper.cols.len());
            let expect_row = &wrapper.data.expect_rows[handle];
            for col in &wrapper.cols {
                let cid = col.get_column_id();
                let v = row.data.get(cid).unwrap();
                assert_eq!(expect_row[&cid], v.to_vec());
            }
        }
        assert!(scanner.next().unwrap().is_none());
    }

    #[test]
    fn test_unique_index_scan() {
        let unique = true;
        let test_data = prepare_index_data(KEY_NUMBER, TABLE_ID, INDEX_ID, unique);
        let mut wrapper = IndexTestWrapper::new(unique, test_data);

        let val_start = Datum::Bytes(b"abc".to_vec());
        let val_end = Datum::Bytes(b"abc".to_vec());
        // point get
        let r1 = get_idx_range(TABLE_ID, INDEX_ID, 0, 1, &val_start, &val_end, unique);
        // range seek
        let r2 = get_idx_range(TABLE_ID, INDEX_ID, 1, 4, &val_start, &val_end, unique);
        // point get
        let r3 = get_idx_range(TABLE_ID, INDEX_ID, 4, 5, &val_start, &val_end, unique);
        //range seek
        let r4 = get_idx_range(
            TABLE_ID,
            INDEX_ID,
            5,
            (KEY_NUMBER + 1) as i64,
            &val_start,
            &val_end,
            unique,
        );
        let r5 = get_idx_range(
            TABLE_ID,
            INDEX_ID,
            (KEY_NUMBER + 1) as i64,
            (KEY_NUMBER + 2) as i64,
            &val_start,
            &val_end,
            unique,
        ); // point get but miss
        wrapper.ranges = vec![r1, r2, r3, r4, r5];

        let (snapshot, start_ts) = wrapper.store.get_snapshot();
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);
        let mut scanner =
            IndexScanExecutor::new(wrapper.scan, wrapper.ranges, store, unique, true).unwrap();
        for handle in 0..KEY_NUMBER {
            let row = scanner.next().unwrap().unwrap();
            assert_eq!(row.handle, handle as i64);
            assert_eq!(row.data.len(), 2);
            let expect_row = &wrapper.data.expect_rows[handle];
            for col in &wrapper.cols {
                let cid = col.get_column_id();
                let v = row.data.get(cid).unwrap();
                assert_eq!(expect_row[&cid], v.to_vec());
            }
        }
        assert!(scanner.next().unwrap().is_none());
        let expected_counts = vec![1, 3, 1, 5, 0];
        let mut counts = Vec::with_capacity(5);
        scanner.collect_output_counts(&mut counts);
        assert_eq!(expected_counts, counts);
    }

    #[test]
    fn test_reverse_scan() {
        let unique = false;
        let test_data = prepare_index_data(KEY_NUMBER, TABLE_ID, INDEX_ID, unique);
        let mut wrapper = IndexTestWrapper::new(unique, test_data);
        wrapper.scan.set_desc(true);

        let val_start = Datum::Bytes(b"abc".to_vec());
        let val_end = Datum::Bytes(b"abc".to_vec());
        let r1 = get_idx_range(
            TABLE_ID,
            INDEX_ID,
            0,
            (KEY_NUMBER / 2) as i64,
            &val_start,
            &val_end,
            unique,
        );
        let r2 = get_idx_range(
            TABLE_ID,
            INDEX_ID,
            (KEY_NUMBER / 2) as i64,
            i64::MAX,
            &val_start,
            &val_end,
            unique,
        );
        wrapper.ranges = vec![r1, r2];

        let (snapshot, start_ts) = wrapper.store.get_snapshot();
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);

        let mut scanner =
            IndexScanExecutor::new(wrapper.scan, wrapper.ranges, store, unique, false).unwrap();

        for tid in 0..KEY_NUMBER {
            let handle = KEY_NUMBER - tid - 1;
            let row = scanner.next().unwrap().unwrap();
            assert_eq!(row.handle, handle as i64);
            assert_eq!(row.data.len(), 2);
            let expect_row = &wrapper.data.expect_rows[handle];
            for col in &wrapper.cols {
                let cid = col.get_column_id();
                let v = row.data.get(cid).unwrap();
                assert_eq!(expect_row[&cid], v.to_vec());
            }
        }
        assert!(scanner.next().unwrap().is_none());
    }

    #[test]
    fn test_include_pk() {
        let mut wrapper = IndexTestWrapper::include_pk_cols();
        let (snapshot, start_ts) = wrapper.store.get_snapshot();
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);

        let mut scanner =
            IndexScanExecutor::new(wrapper.scan, wrapper.ranges, store, false, false).unwrap();

        for handle in 0..KEY_NUMBER {
            let row = scanner.next().unwrap().unwrap();
            assert_eq!(row.handle, handle as i64);
            assert_eq!(row.data.len(), wrapper.cols.len());
            let expect_row = &wrapper.data.expect_rows[handle];
            let handle_datum = datum::Datum::I64(handle as i64);
            let pk = datum::encode_key(&[handle_datum]).unwrap();
            for col in &wrapper.cols {
                let cid = col.get_column_id();
                let v = row.data.get(cid).unwrap();
                if col.get_pk_handle() {
                    assert_eq!(pk, v.to_vec());
                    continue;
                }
                assert_eq!(expect_row[&cid], v.to_vec());
            }
        }
        assert!(scanner.next().unwrap().is_none());
    }
}
