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

use server::coprocessor::endpoint::{prefix_next, is_point};
use server::coprocessor::Result;
use tipb::executor::TableScan;
use kvproto::coprocessor::KeyRange;
use storage::{Key, SnapshotStore, Statistics, ScanMode};
use util::codec::table;
use util::codec::table::RowColsDict;
use util::HashSet;
use super::Executor;

struct TableScanExec<'a> {
    executor: TableScan,
    col_ids: HashSet<i64>,
    key_ranges: Vec<KeyRange>,
    store: SnapshotStore<'a>,
    cursor: usize,
    seek_key: Option<Vec<u8>>,
    scan_mode: ScanMode,
    upper_bound: Option<Vec<u8>>,
    region_start: Vec<u8>,
    region_end: Vec<u8>,
    statistics: &'a mut Statistics,
}


impl<'a> TableScanExec<'a> {
    #[allow(dead_code)] //TODO:remove it
    pub fn new(executor: TableScan,
               key_ranges: Vec<KeyRange>,
               store: SnapshotStore<'a>,
               region_start: Vec<u8>,
               region_end: Vec<u8>,
               statistics: &'a mut Statistics)
               -> TableScanExec<'a> {
        let col_ids = executor.get_columns()
            .iter()
            .filter(|c| !c.get_pk_handle())
            .map(|c| c.get_column_id())
            .collect();
        let scan_mode = if executor.get_desc() {
            ScanMode::Backward
        } else {
            ScanMode::Forward
        };
        TableScanExec {
            executor: executor,
            col_ids: col_ids,
            key_ranges: key_ranges,
            store: store,
            cursor: 0,
            seek_key: None,
            scan_mode: scan_mode,
            upper_bound: None,
            region_start: region_start,
            region_end: region_end,
            statistics: statistics,
        }
    }

    fn get_row_from_point(&mut self) -> Result<Option<(i64, RowColsDict)>> {
        let range = &self.key_ranges[self.cursor];
        let value = match try!(self.store
            .get(&Key::from_raw(range.get_start()), &mut self.statistics)) {
            None => return Ok(None),
            Some(v) => v,
        };
        let values = box_try!(table::cut_row(value, &self.col_ids));
        let h = box_try!(table::decode_handle(range.get_start()));
        Ok(Some((h, values)))
    }

    fn get_row_from_range(&mut self) -> Result<Option<(i64, RowColsDict)>> {
        self.init_seek_key_and_upper_bound();
        let range = &self.key_ranges[self.cursor];
        if range.get_start() > range.get_end() {
            return Ok(None);
        }
        let desc = self.executor.get_desc();
        let mut scanner = try!(self.store.scanner(self.scan_mode,
                                                  false,
                                                  self.upper_bound.clone(),
                                                  self.statistics));
        let seek_key = self.seek_key.clone().unwrap();
        let kv = if desc {
            try!(scanner.reverse_seek(Key::from_raw(&seek_key)))
        } else {
            try!(scanner.seek(Key::from_raw(&seek_key)))
        };

        let (key, value) = match kv {
            Some((key, value)) => (box_try!(key.raw()), value),
            None => return Ok(None),
        };

        let h = box_try!(table::decode_handle(&key));
        let row_data = {
            box_try!(table::cut_row(value, &self.col_ids))
        };

        let seek_key = if desc {
            box_try!(table::truncate_as_row_key(&key)).to_vec()
        } else {
            prefix_next(&key)
        };
        self.seek_key = Some(seek_key);
        Ok(Some((h, row_data)))
    }

    fn init_seek_key_and_upper_bound(&mut self) {
        if self.seek_key.is_some() {
            return;
        }
        let range = &self.key_ranges[self.cursor];
        self.upper_bound = None;
        if self.executor.get_desc() {
            let range_end = range.get_end().to_vec();
            self.seek_key = if self.region_end.is_empty() || range_end < self.region_end {
                Some(self.region_end.clone())
            } else {
                Some(range_end)
            };
            return;
        }

        if range.has_end() {
            self.upper_bound = Some(Key::from_raw(range.get_end()).encoded().clone());
        }

        let range_start = range.get_start().to_vec();
        self.seek_key = if range_start > self.region_start {
            Some(range_start)
        } else {
            Some(self.region_start.clone())
        };
    }
}

impl<'a> Executor for TableScanExec<'a> {
    fn next(&mut self) -> Result<Option<(i64, RowColsDict)>> {
        while self.cursor < self.key_ranges.len() {
            // let range = &self.key_ranges[self.cursor];
            if is_point(&self.key_ranges[self.cursor]) {
                let data = box_try!(self.get_row_from_point());
                self.seek_key = None;
                self.cursor += 1;
                return Ok(data);
            }

            let data = box_try!(self.get_row_from_range());
            if data.is_none() {
                self.seek_key = None;
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
    use std::i64;
    use util::codec::mysql::types;
    use util::codec::datum::{self, Datum};
    use util::codec::number::NumberEncoder;
    use util::HashMap;
    use tipb::schema::ColumnInfo;
    use kvproto::kvrpcpb::Context;
    use storage::SnapshotStore;
    use storage::mvcc::MvccTxn;
    use storage::{make_key, Mutation, ALL_CFS, Options, Statistics};
    use storage::engine::{self, Engine, TEMP_DIR, Snapshot};
    use protobuf::RepeatedField;
    use server::coprocessor::endpoint::{is_point, prefix_next};

    const TABLE_ID: i64 = 1;
    const START_TS: u64 = 10;
    const COMMIT_TS: u64 = 20;
    const KEY_NUMBER: usize = 10;

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

    struct Data {
        data: Vec<(Vec<u8>, Vec<u8>)>,
        pk: Vec<u8>,
        // encode_data[row_id][column_id]=>value
        encode_data: Vec<HashMap<i64, Vec<u8>>>,

        cols: Vec<ColumnInfo>,
    }

    impl Data {
        fn get_prev_2_cols(&self) -> Vec<ColumnInfo> {
            let col1 = self.cols[0].clone();
            let col2 = self.cols[1].clone();
            vec![col1, col2]
        }
    }

    fn prepare_data() -> Data {
        let cols = vec![new_col_info(1, types::LONG_LONG),
                        new_col_info(2, types::VARCHAR),
                        new_col_info(3, types::NEW_DECIMAL)];

        let mut data = Vec::new();
        let mut pk = Vec::new();
        let mut encode_data = Vec::new();

        for tid in 0..KEY_NUMBER {
            let row = map![
                1 => Datum::I64(tid as i64),
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
            buf.encode_i64(tid as i64).unwrap();
            let key = table::encode_row_key(TABLE_ID, &buf);
            if pk.is_empty() {
                pk = key.clone();
            }
            encode_data.push(encode_value);
            data.push((key, value));

        }
        Data {
            data: data,
            pk: pk,
            encode_data: encode_data,
            cols: cols,
        }
    }

    struct TestStore {
        data: Data,
        snapshot: Box<Snapshot>,
        ctx: Context,
        engine: Box<Engine>,
    }

    impl TestStore {
        fn new() -> TestStore {
            let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();
            let data = prepare_data();
            let ctx = Context::new();
            let snapshot = engine.snapshot(&ctx).unwrap();
            let mut store = TestStore {
                data: data,
                snapshot: snapshot,
                ctx: ctx,
                engine: engine,
            };
            store.init_data();
            store
        }

        fn init_data(&mut self) {
            let mut statistics = Statistics::default();
            // do prewrite.
            {
                let mut txn = MvccTxn::new(self.snapshot.as_ref(), &mut statistics, START_TS, None);
                for &(ref key, ref value) in &self.data.data {
                    txn.prewrite(Mutation::Put((make_key(key), value.to_vec())),
                                  &self.data.pk,
                                  &Options::default())
                        .unwrap();
                }
                self.engine.write(&self.ctx, txn.modifies()).unwrap();
            }
            self.refresh_snapshot();
            // do commit
            {
                let mut txn = MvccTxn::new(self.snapshot.as_ref(), &mut statistics, START_TS, None);
                for &(ref key, _) in &self.data.data {
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

        fn store(&self) -> SnapshotStore {
            SnapshotStore::new(self.snapshot.as_ref(), COMMIT_TS + 1)
        }
    }

    #[inline]
    fn get_region(start: i64, end: i64) -> (Vec<u8>, Vec<u8>) {
        let mut start_buf = Vec::with_capacity(8);
        start_buf.encode_i64(start).unwrap();
        let mut end_buf = Vec::with_capacity(8);
        end_buf.encode_i64(end).unwrap();
        (start_buf, end_buf)
    }

    #[inline]
    fn get_range(start: i64, end: i64) -> KeyRange {
        let mut key_range = KeyRange::new();
        let (left, right) = get_region(start, end);
        key_range.set_start(table::encode_row_key(TABLE_ID, &left));
        key_range.set_end(table::encode_row_key(TABLE_ID, &right));
        key_range
    }

    #[test]
    fn test_point_get() {
        let test_store = TestStore::new();
        let store = test_store.store();
        let mut statistics = Statistics::default();
        let mut table_scan = TableScan::new();
        // prepare cols
        let cols = test_store.data.get_prev_2_cols();
        let col_req = RepeatedField::from_vec(cols.clone());
        table_scan.set_columns(col_req);

        // prepare region_start/region_end
        let (region_start, region_end) = get_region(i64::MIN, i64::MAX);

        let point_handler = 0 as i64;
        // prepare range
        let mut range = KeyRange::new();
        range.set_start(test_store.data.pk.clone());
        let end = prefix_next(&test_store.data.pk.clone());
        range.set_end(end);
        assert!(is_point(&range));
        let key_ranges = vec![range];

        let mut table_scanner = TableScanExec::new(table_scan,
                                                   key_ranges,
                                                   store,
                                                   region_start,
                                                   region_end,
                                                   &mut statistics);


        let (handler, data) = table_scanner.next().unwrap().unwrap();
        assert_eq!(handler, point_handler);
        assert_eq!(data.len(), cols.len());
        let encode_data = &test_store.data.encode_data[0];
        for col in &cols {
            let cid = col.get_column_id();
            let v = data.get(cid).unwrap();
            assert_eq!(encode_data[&cid], v.to_vec());
        }
        assert!(table_scanner.next().unwrap().is_none());
    }

    #[test]
    fn test_multiple_ranges() {
        let test_store = TestStore::new();
        let store = test_store.store();
        let mut statistics = Statistics::default();
        let mut table_scan = TableScan::new();
        // prepare cols
        let cols = test_store.data.get_prev_2_cols();
        let col_req = RepeatedField::from_vec(cols.clone());
        table_scan.set_columns(col_req);

        // prepare region_start/region_end
        let (region_start, region_end) = get_region(i64::MIN, i64::MAX);
        // prepare range
        let r1 = get_range(i64::MIN, 0);
        let r2 = get_range(0, (KEY_NUMBER / 2) as i64);
        let r3 = get_range((KEY_NUMBER / 2) as i64, i64::MAX);
        let key_ranges = vec![r1, r2, r3];

        let mut table_scanner = TableScanExec::new(table_scan,
                                                   key_ranges,
                                                   store,
                                                   region_start,
                                                   region_end,
                                                   &mut statistics);

        for col_id in 0..KEY_NUMBER {
            let (handler, data) = table_scanner.next().unwrap().unwrap();
            assert_eq!(handler, col_id as i64);
            assert_eq!(data.len(), cols.len());
            let encode_data = &test_store.data.encode_data[col_id];
            for col in &cols {
                let cid = col.get_column_id();
                let v = data.get(cid).unwrap();
                assert_eq!(encode_data[&cid], v.to_vec());
            }
        }
        assert!(table_scanner.next().unwrap().is_none());
    }

    #[test]
    fn test_reverse_scan() {
        let test_store = TestStore::new();
        let store = test_store.store();
        let mut statistics = Statistics::default();
        let mut table_scan = TableScan::new();
        // prepare cols
        let cols = test_store.data.get_prev_2_cols();
        let col_req = RepeatedField::from_vec(cols.clone());
        table_scan.set_columns(col_req);

        // prepare region_start/region_end
        let (region_start, region_end) = get_region(i64::MIN, i64::MAX);
        // prepare range
        let range = get_range(i64::MIN, i64::MAX);
        let key_ranges = vec![range];

        table_scan.set_desc(true);

        let mut table_scanner = TableScanExec::new(table_scan,
                                                   key_ranges,
                                                   store,
                                                   region_start,
                                                   region_end,
                                                   &mut statistics);

        for tid in 0..KEY_NUMBER {
            let col_id = KEY_NUMBER - tid - 1;
            let (handler, data) = table_scanner.next().unwrap().unwrap();
            assert_eq!(handler, col_id as i64);
            assert_eq!(data.len(), 2);
            let encode_data = &test_store.data.encode_data[col_id];
            for col in &cols {
                let cid = col.get_column_id();
                let v = data.get(cid).unwrap();
                assert_eq!(encode_data[&cid], v.to_vec());
            }
        }
        assert!(table_scanner.next().unwrap().is_none());
    }
}
