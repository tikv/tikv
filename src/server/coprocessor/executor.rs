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

use super::endpoint::{prefix_next, is_point};
use super::Result;
use tipb::executor::TableScan;
use kvproto::coprocessor::KeyRange;
use storage::{Key, SnapshotStore, Statistics, ScanMode};
use util::codec::table;
use util::codec::table::RowColsDict;
use util::HashSet;

pub trait Executor<E> {
    fn set_src_executor(&mut self, e: E);
    fn next(&mut self) -> Result<Option<(i64, RowColsDict)>>;
}

struct TableScanExec<'a, E> {
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
    src: Option<E>,
    statistics: &'a mut Statistics,
}


impl<'a, E> TableScanExec<'a, E> {
    pub fn new(executor: TableScan,
               key_ranges: Vec<KeyRange>,
               store: SnapshotStore<'a>,
               region_start: Vec<u8>,
               region_end: Vec<u8>,
               statistics: &'a mut Statistics)
               -> TableScanExec<'a, E> {
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
            src: None,
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

impl<'a, E> Executor<E> for TableScanExec<'a, E> {
    fn set_src_executor(&mut self, e: E) {
        self.src = Some(e);
    }

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
    use util::codec::datum::{self, Datum, DatumDecoder};
    use util::codec::number::NumberEncoder;
    use util::{HashMap, HashSet, BuildHasherDefault};
    use util::codec::table::*;
    use tipb::schema::ColumnInfo;
    use kvproto::kvrpcpb::Context;
    use storage::SnapshotStore;
    use storage::mvcc::MvccTxn;
    use storage::{make_key, Mutation, ALL_CFS, Options, Statistics, ScanMode, KvPair, Value};
    use storage::engine::{self, Engine, TEMP_DIR, Snapshot};
    use protobuf::RepeatedField;

    const TableID: i64 = 1;
    const START_TS: u64 = 10;
    const COMMIT_TS: u64 = 20;

    fn new_col_info(tp: u8) -> ColumnInfo {
        let mut col_info = ColumnInfo::new();
        col_info.set_tp(tp as i32);
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
    }
    fn prepare_data() -> Data {
        let mut cols = map![
            1 => new_col_info(types::LONG_LONG),
            2 => new_col_info(types::VARCHAR),
            3 => new_col_info(types::NEW_DECIMAL)
        ];

        let mut data = Vec::new();
        let mut pk = Vec::new();

        for tid in 0..100 {
            let mut row = map![
                1 => Datum::I64(tid),
                2 => Datum::Bytes(b"abc".to_vec()),
                3 => Datum::Dec(10.into())
            ];

            let col_ids: Vec<_> = row.iter().map(|(&id, _)| id).collect();
            let col_values: Vec<_> = row.iter().map(|(_, v)| v.clone()).collect();
            let mut col_encoded: HashMap<_, _> = row.iter()
                .map(|(k, v)| {
                    let f = flatten(v.clone()).unwrap();
                    (*k, datum::encode_value(&[f]).unwrap())
                })
                .collect();
            let mut col_id_set: HashSet<_> = col_ids.iter().cloned().collect();

            let value = table::encode_row(col_values, &col_ids).unwrap();
            let mut buf = vec![];
            buf.encode_i64(tid).unwrap();
            let key = table::encode_row_key(TableID, &buf);
            if pk.is_empty() {
                pk = key.clone();
            }
            data.push((key, value));

        }
        Data {
            data: data,
            pk: pk,
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

    #[test]
    fn test_scan_executor() {
        let store = TestStore::new();
        let store = store.store();
        let mut statistics = Statistics::default();
        let mut table_scan = TableScan::new();
        let mut col1 = new_col_info(types::LONG_LONG);
        col1.set_column_id(1);
        let mut col2 = new_col_info(types::VARCHAR);
        col2.set_column_id(2);
        let col_req = RepeatedField::from_vec(vec![col1, col2]);
        table_scan.set_columns(col_req);
        let mut range = KeyRange::new();
        let mut buf = Vec::with_capacity(8);
        buf.encode_i64(i64::MIN).unwrap();
        let region_start = buf.clone();
        range.set_start(table::encode_row_key(TableID, &buf));
        let mut end_buf = Vec::with_capacity(8);
        end_buf.encode_i64(i64::MAX).unwrap();
        let region_end = end_buf.clone();
        range.set_end(end_buf);
        let key_ranges = vec![range];
        let mut table_scanner: TableScanExec<TableScanExec> = TableScanExec::new(table_scan,
                                                                                 key_ranges,
                                                                                 store,
                                                                                 region_start,
                                                                                 region_end,
                                                                                 &mut statistics);
        let (handler, data) = table_scanner.next().unwrap().unwrap();

    }
}
