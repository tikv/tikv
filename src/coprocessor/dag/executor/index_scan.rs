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

use std::vec::IntoIter;
use byteorder::{BigEndian, ReadBytesExt};

use kvproto::coprocessor::KeyRange;
use tipb::executor::IndexScan;
use tipb::schema::ColumnInfo;

use coprocessor::codec::{datum, mysql, table};
use coprocessor::metrics::*;
use coprocessor::{Error, Result};
use storage::{SnapshotStore, Statistics};

use super::{Executor, Row};
use super::scanner::{ScanOn, Scanner};


pub struct IndexScanExecutor {
    store: SnapshotStore,
    desc: bool,
    col_ids: Vec<i64>,
    pk_col: Option<ColumnInfo>,
    key_ranges: IntoIter<KeyRange>,
    scanner: Option<Scanner>,
}

impl IndexScanExecutor {
    pub fn new(
        mut meta: IndexScan,
        mut key_ranges: Vec<KeyRange>,
        store: SnapshotStore,
    ) -> IndexScanExecutor {
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

        COPR_EXECUTOR_COUNT.with_label_values(&["idxscan"]).inc();
        IndexScanExecutor {
            store: store,
            desc: desc,
            col_ids: col_ids,
            pk_col: pk_col,
            key_ranges: key_ranges.into_iter(),
            scanner: None,
        }
    }

    pub fn new_with_cols_len(
        cols: i64,
        key_ranges: Vec<KeyRange>,
        store: SnapshotStore,
    ) -> IndexScanExecutor {
        let col_ids: Vec<i64> = (0..cols).collect();
        COPR_EXECUTOR_COUNT.with_label_values(&["idxscan"]).inc();
        IndexScanExecutor {
            store: store,
            desc: false,
            col_ids: col_ids,
            pk_col: None,
            key_ranges: key_ranges.into_iter(),
            scanner: None,
        }
    }

    fn get_row_from_range_scanner(&mut self) -> Result<Option<Row>> {
        if self.scanner.is_none() {
            return Ok(None);
        }
        COPR_GET_OR_SCAN_COUNT.with_label_values(&["range"]).inc();

        let scanner = self.scanner.as_mut().unwrap();
        let (key, value) = match scanner.next_row()? {
            Some((key, value)) => (key, value),
            None => return Ok(None),
        };

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

    fn new_scanner(&self, range: KeyRange) -> Result<Scanner> {
        Scanner::new(&self.store, ScanOn::Index, self.desc, false, range).map_err(Error::from)
    }
}

impl Executor for IndexScanExecutor {
    fn next(&mut self) -> Result<Option<Row>> {
        loop {
            if let Some(row) = self.get_row_from_range_scanner()? {
                return Ok(Some(row));
            }
            if let Some(range) = self.key_ranges.next() {
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

    fn collect_statistics_into(&mut self, statistics: &mut Statistics) {
        if let Some(scanner) = self.scanner.take() {
            scanner.collect_statistics_into(statistics);
        }
    }
}

#[cfg(test)]
mod test {
    use std::i64;
    use byteorder::{BigEndian, WriteBytesExt};

    use kvproto::kvrpcpb::IsolationLevel;
    use protobuf::RepeatedField;
    use tipb::schema::ColumnInfo;

    use coprocessor::codec::mysql::types;
    use coprocessor::codec::datum::{self, Datum};
    use util::collections::HashMap;
    use storage::SnapshotStore;

    use super::*;
    use super::super::scanner::test::{new_col_info, Data, TestStore};

    const TABLE_ID: i64 = 1;
    const INDEX_ID: i64 = 1;
    const KEY_NUMBER: usize = 10;

    // get_idx_range get range for index in [("abc",start),("abc", end))
    pub fn get_idx_range(table_id: i64, idx_id: i64, start: i64, end: i64) -> KeyRange {
        let (_, start_key) = generate_index_data(table_id, idx_id, start);
        let (_, end_key) = generate_index_data(table_id, idx_id, end);
        let mut key_range = KeyRange::new();
        key_range.set_start(start_key);
        key_range.set_end(end_key);
        key_range
    }

    pub fn generate_index_data(
        table_id: i64,
        index_id: i64,
        handle: i64,
    ) -> (HashMap<i64, Vec<u8>>, Vec<u8>) {
        let indice = vec![
            (2, Datum::Bytes(b"abc".to_vec())),
            (3, Datum::Dec(handle.into())),
        ];
        let mut expect_row = HashMap::default();
        let v: Vec<_> = indice
            .iter()
            .map(|&(ref cid, ref value)| {
                expect_row.insert(*cid, datum::encode_key(&[value.clone()]).unwrap());
                value.clone()
            })
            .collect();
        let encoded = datum::encode_key(&v).unwrap();
        let idx_key = table::encode_index_seek_key(table_id, index_id, &encoded);
        (expect_row, idx_key)
    }

    pub fn prepare_index_data(key_number: usize, table_id: i64, index_id: i64) -> Data {
        let cols = vec![
            new_col_info(1, types::LONG_LONG),
            new_col_info(2, types::VARCHAR),
            new_col_info(3, types::NEW_DECIMAL),
        ];

        let mut kv_data = Vec::new();
        let mut expect_rows = Vec::new();

        for handle in 0..key_number {
            let (expect_row, idx_key) = generate_index_data(table_id, index_id, handle as i64);
            expect_rows.push(expect_row);
            let mut value = Vec::with_capacity(8);
            value.write_i64::<BigEndian>(handle as i64).unwrap();
            kv_data.push((idx_key, value));
        }
        Data {
            kv_data: kv_data,
            expect_rows: expect_rows,
            cols: cols,
        }
    }

    struct IndexTestWrapper {
        data: Data,
        store: TestStore,
        scan: IndexScan,
        ranges: Vec<KeyRange>,
        cols: Vec<ColumnInfo>,
    }

    impl IndexTestWrapper {
        fn include_pk_cols() -> IndexTestWrapper {
            let mut wrapper = IndexTestWrapper::default();
            let mut cols = wrapper.data.get_index_cols();
            cols.push(wrapper.data.get_col_pk());
            wrapper
                .scan
                .set_columns(RepeatedField::from_vec(cols.clone()));
            wrapper.cols = cols;
            wrapper
        }
    }

    impl Default for IndexTestWrapper {
        fn default() -> IndexTestWrapper {
            let test_data = prepare_index_data(KEY_NUMBER, TABLE_ID, INDEX_ID);
            let test_store = TestStore::new(&test_data.kv_data);
            let mut scan = IndexScan::new();
            // prepare cols
            let cols = test_data.get_index_cols();
            let col_req = RepeatedField::from_vec(cols.clone());
            scan.set_columns(col_req);
            // prepare range
            let range = get_idx_range(TABLE_ID, INDEX_ID, 0, i64::MAX);
            let key_ranges = vec![range];
            IndexTestWrapper {
                data: test_data,
                store: test_store,
                scan: scan,
                ranges: key_ranges,
                cols: cols,
            }
        }
    }

    #[test]
    fn test_multiple_ranges() {
        let mut wrapper = IndexTestWrapper::default();
        let r1 = get_idx_range(TABLE_ID, INDEX_ID, 0, (KEY_NUMBER / 3) as i64);
        let r2 = get_idx_range(
            TABLE_ID,
            INDEX_ID,
            (KEY_NUMBER / 3) as i64,
            (KEY_NUMBER / 2) as i64,
        );
        wrapper.ranges = vec![r1, r2];
        let (snapshot, start_ts) = wrapper.store.get_snapshot();
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);

        let mut scanner = IndexScanExecutor::new(wrapper.scan, wrapper.ranges, store);

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
    fn test_reverse_scan() {
        let mut wrapper = IndexTestWrapper::default();
        wrapper.scan.set_desc(true);

        let r1 = get_idx_range(TABLE_ID, INDEX_ID, i64::MIN, 0);
        let r2 = get_idx_range(TABLE_ID, INDEX_ID, 0, (KEY_NUMBER / 2) as i64);
        let r3 = get_idx_range(TABLE_ID, INDEX_ID, (KEY_NUMBER / 2) as i64, i64::MAX);
        wrapper.ranges = vec![r1, r2, r3];

        let (snapshot, start_ts) = wrapper.store.get_snapshot();
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);

        let mut scanner = IndexScanExecutor::new(wrapper.scan, wrapper.ranges, store);

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

        let mut scanner = IndexScanExecutor::new(wrapper.scan, wrapper.ranges, store);

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
