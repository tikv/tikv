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


use server::coprocessor::endpoint::prefix_next;
use server::coprocessor::Result;
use tipb::executor::IndexScan;
use tipb::schema::ColumnInfo;
use kvproto::coprocessor::KeyRange;
use storage::{SnapshotStore, Statistics};
use util::codec::{table, datum, mysql};
use byteorder::{BigEndian, ReadBytesExt};
use super::{Executor, Row};
use super::base_scanner::BaseScanner;

struct IndexScanExec<'a> {
    meta: IndexScan,
    col_ids: Vec<i64>,
    cursor: usize,
    key_ranges: Vec<KeyRange>,
    scanner: BaseScanner<'a>,
    pk_col: Option<ColumnInfo>,
}

impl<'a> IndexScanExec<'a> {
    #[allow(dead_code)] //TODO:remove it
    pub fn new(meta: IndexScan,
               key_ranges: Vec<KeyRange>,
               store: SnapshotStore<'a>,
               statistics: &'a mut Statistics)
               -> IndexScanExec<'a> {
        let mut pk_col = None;
        let col_ids = meta.get_columns()
            .iter()
            .filter(|c| {
                if c.get_pk_handle() {
                    pk_col = Some((*c).clone());
                    false
                } else {
                    true
                }
            })
            .map(|c| c.get_column_id())
            .collect();
        let scanner = BaseScanner::new(meta.get_desc(), false, store, statistics);
        IndexScanExec {
            meta: meta,
            col_ids: col_ids,
            scanner: scanner,
            key_ranges: key_ranges,
            cursor: 0,
            pk_col: pk_col,
        }
    }

    pub fn get_row_from_range(&mut self) -> Result<Option<Row>> {
        let range = &self.key_ranges[self.cursor];
        if range.get_start() > range.get_end() {
            return Ok(None);
        }
        let kv = try!(self.scanner.get_row_from_range(range));
        let (key, value) = match kv {
            Some((key, value)) => (key, value),
            None => return Ok(None),
        };

        let seek_key = if self.meta.get_desc() {
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
            let mut bytes = datum::encode_value(&[handle_datum]).unwrap();
            values.append(pk_col.get_column_id(), &mut bytes);
        }
        Ok(Some(Row::new(handle, values)))
    }
}

impl<'a> Executor for IndexScanExec<'a> {
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
    use super::super::base_scanner::test::{Data, TestStore, prepare_index_data, get_idx_range};
    use std::i64;
    use tipb::schema::ColumnInfo;
    use storage::Statistics;
    use protobuf::RepeatedField;

    const TABLE_ID: i64 = 1;
    const INDEX_ID: i64 = 1;
    const KEY_NUMBER: usize = 10;

    struct IndexScanExecutorMeta {
        data: Data,
        store: TestStore,
        scan: IndexScan,
        ranges: Vec<KeyRange>,
        cols: Vec<ColumnInfo>,
    }

    impl Default for IndexScanExecutorMeta {
        fn default() -> IndexScanExecutorMeta {
            let test_data = prepare_index_data(KEY_NUMBER, TABLE_ID, INDEX_ID);
            let test_store = TestStore::new(&test_data.data, test_data.pk.clone());
            let mut scan = IndexScan::new();
            // prepare cols
            let cols = test_data.get_index_cols();
            let col_req = RepeatedField::from_vec(cols.clone());
            scan.set_columns(col_req);
            // prepare range
            let range = get_idx_range(TABLE_ID, INDEX_ID, i64::MIN, i64::MAX);
            let key_ranges = vec![range];
            IndexScanExecutorMeta {
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
        let mut meta = IndexScanExecutorMeta::default();
        let r1 = get_idx_range(TABLE_ID, INDEX_ID, i64::MIN, 0);
        let r2 = get_idx_range(TABLE_ID, INDEX_ID, 0, (KEY_NUMBER / 2) as i64);
        let r3 = get_idx_range(TABLE_ID, INDEX_ID, (KEY_NUMBER / 2) as i64, i64::MAX);
        meta.ranges = vec![r1, r2, r3];

        let mut scanner =
            IndexScanExec::new(meta.scan, meta.ranges, meta.store.store(), &mut statistics);

        for handle in 0..KEY_NUMBER {
            let row = scanner.next().unwrap().unwrap();
            assert_eq!(row.handle, handle as i64);
            assert_eq!(row.data.len(), meta.cols.len());
            let encode_data = &meta.data.encode_data[handle];
            for col in &meta.cols {
                let cid = col.get_column_id();
                let v = row.data.get(cid).unwrap();
                assert_eq!(encode_data[&cid], v.to_vec());
            }
        }
        assert!(scanner.next().unwrap().is_none());
    }

    #[test]
    fn test_reverse_scan() {
        let mut statistics = Statistics::default();
        let mut meta = IndexScanExecutorMeta::default();;
        meta.scan.set_desc(true);
        let mut scanner =
            IndexScanExec::new(meta.scan, meta.ranges, meta.store.store(), &mut statistics);

        for tid in 0..KEY_NUMBER {
            let handle = KEY_NUMBER - tid - 1;
            let row = scanner.next().unwrap().unwrap();
            assert_eq!(row.handle, handle as i64);
            assert_eq!(row.data.len(), 2);
            let encode_data = &meta.data.encode_data[handle];
            for col in &meta.cols {
                let cid = col.get_column_id();
                let v = row.data.get(cid).unwrap();
                assert_eq!(encode_data[&cid], v.to_vec());
            }
        }
        assert!(scanner.next().unwrap().is_none());
    }
}
