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


use coprocessor::endpoint::prefix_next;
use coprocessor::Result;
use tipb::executor::IndexScan;
use tipb::schema::ColumnInfo;
use kvproto::coprocessor::KeyRange;
use kvproto::kvrpcpb::IsolationLevel;
use storage::{Snapshot, Statistics};
use util::codec::{table, datum, mysql};
use byteorder::{BigEndian, ReadBytesExt};
use super::{Executor, Row};
use super::scanner::Scanner;

pub struct IndexScanExecutor<'a> {
    desc: bool,
    col_ids: Vec<i64>,
    cursor: usize,
    key_ranges: Vec<KeyRange>,
    scanner: Scanner<'a>,
    pk_col: Option<ColumnInfo>,
}

impl<'a> IndexScanExecutor<'a> {
    #[allow(dead_code)] //TODO:remove it
    pub fn new(mut meta: IndexScan,
               key_ranges: Vec<KeyRange>,
               snapshot: &'a Snapshot,
               statistics: &'a mut Statistics,
               start_ts: u64,
               isolation_level: IsolationLevel)
               -> IndexScanExecutor<'a> {
        let mut pk_col = None;
        let desc = meta.get_desc();
        let mut cols = meta.mut_columns();
        if cols.last().map_or(false, |c| c.get_pk_handle()) {
            pk_col = Some(cols.pop().unwrap());
        }
        let col_ids = cols.iter()
            .map(|c| c.get_column_id())
            .collect();
        let scanner = Scanner::new(desc, false, snapshot, statistics, start_ts, isolation_level);
        IndexScanExecutor {
            desc: desc,
            col_ids: col_ids,
            scanner: scanner,
            key_ranges: key_ranges,
            cursor: Default::default(),
            pk_col: pk_col,
        }
    }

    pub fn get_row_from_range(&mut self) -> Result<Option<Row>> {
        let range = &self.key_ranges[self.cursor];
        if range.get_start() > range.get_end() {
            return Ok(None);
        }
        let kv = try!(self.scanner.next_row(range));
        let (key, value) = match kv {
            Some((key, value)) => (key, value),
            None => return Ok(None),
        };

        let seek_key = if self.desc {
            key.clone()
        } else {
            prefix_next(&key)
        };
        self.scanner.set_seek_key(Some(seek_key));

        let (mut values, handle) = {
            box_try!(table::cut_idx_key(key, &self.col_ids))
        };

        let handle = if handle.is_none() {
            box_try!(value.as_slice().read_i64::<BigEndian>())
        } else {
            handle.unwrap()
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
}

impl<'a> Executor for IndexScanExecutor<'a> {
    fn next(&mut self) -> Result<Option<Row>> {
        while self.cursor < self.key_ranges.len() {
            let data = box_try!(self.get_row_from_range());
            if data.is_none() {
                self.scanner.set_seek_key(None);
                self.cursor += 1;
                continue;
            }
            return Ok(data);
        }
        Ok(None)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use super::super::scanner::test::{Data, TestStore, new_col_info};
    use util::codec::mysql::types;
    use util::codec::datum::{self, Datum};
    use util::codec::number::NumberEncoder;
    use util::collections::HashMap;
    use std::i64;
    use tipb::schema::ColumnInfo;
    use storage::Statistics;
    use protobuf::RepeatedField;

    const TABLE_ID: i64 = 1;
    const INDEX_ID: i64 = 1;
    const KEY_NUMBER: usize = 10;

    #[inline]
    pub fn get_idx_range(table_id: i64, idx_id: i64, start: i64, end: i64) -> KeyRange {
        let mut start_buf = Vec::with_capacity(8);
        start_buf.encode_i64(start).unwrap();
        let mut end_buf = Vec::with_capacity(8);
        end_buf.encode_i64(end).unwrap();
        let mut key_range = KeyRange::new();
        key_range.set_start(table::encode_index_seek_key(table_id, idx_id, &start_buf));
        key_range.set_end(table::encode_index_seek_key(table_id, idx_id, &end_buf));
        key_range
    }

    pub fn prepare_index_data(key_number: usize, table_id: i64, index_id: i64) -> Data {
        let cols = vec![new_col_info(1, types::LONG_LONG),
                        new_col_info(2, types::VARCHAR),
                        new_col_info(3, types::NEW_DECIMAL)];

        let mut kv_data = Vec::new();
        let mut expect_rows = Vec::new();

        for handle in 0..key_number {
            let indice = map![
                2 => Datum::Bytes(b"abc".to_vec()),
                3 => Datum::Dec(handle.into())
            ];
            let mut expect_row = HashMap::default();
            let mut v: Vec<_> = indice.iter()
                .map(|(cid, value)| {
                    expect_row.insert(*cid, datum::encode_key(&[value.clone()]).unwrap());
                    value.clone()
                })
                .collect();
            let h = Datum::I64(handle as i64);
            v.push(h);
            let encoded = datum::encode_key(&v).unwrap();
            let idx_key = table::encode_index_seek_key(table_id, index_id, &encoded);
            expect_rows.push(expect_row);
            kv_data.push((idx_key, vec![0]));
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
            wrapper.scan.set_columns(RepeatedField::from_vec(cols.clone()));
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
            let range = get_idx_range(TABLE_ID, INDEX_ID, i64::MIN, i64::MAX);
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
        let mut statistics = Statistics::default();
        let mut wrapper = IndexTestWrapper::default();
        let (ref start_key, _) = wrapper.data.kv_data[0];
        let (ref split_key, _) = wrapper.data.kv_data[KEY_NUMBER / 3];
        let (ref end_key, _) = wrapper.data.kv_data[KEY_NUMBER / 2];
        let mut r1 = KeyRange::new();
        r1.set_start(start_key.clone());
        r1.set_end(split_key.clone());
        let mut r2 = KeyRange::new();
        r2.set_start(split_key.clone());
        r2.set_end(end_key.clone());
        wrapper.ranges = vec![r1, r2];
        let (snapshot, start_ts) = wrapper.store.get_snapshot();
        let mut scanner = IndexScanExecutor::new(wrapper.scan,
                                                 wrapper.ranges,
                                                 snapshot,
                                                 &mut statistics,
                                                 start_ts,
                                                 IsolationLevel::SI);

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
        let mut statistics = Statistics::default();
        let mut wrapper = IndexTestWrapper::default();;
        wrapper.scan.set_desc(true);
        let (snapshot, start_ts) = wrapper.store.get_snapshot();
        let mut scanner = IndexScanExecutor::new(wrapper.scan,
                                                 wrapper.ranges,
                                                 snapshot,
                                                 &mut statistics,
                                                 start_ts,
                                                 IsolationLevel::SI);

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
        let mut statistics = Statistics::default();
        let mut wrapper = IndexTestWrapper::include_pk_cols();
        let (snapshot, start_ts) = wrapper.store.get_snapshot();
        let mut scanner = IndexScanExecutor::new(wrapper.scan,
                                                 wrapper.ranges,
                                                 snapshot,
                                                 &mut statistics,
                                                 start_ts,
                                                 IsolationLevel::SI);

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
