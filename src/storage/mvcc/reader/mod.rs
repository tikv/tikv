// Copyright 2016 PingCAP, Inc.
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

mod backward_scanner;
mod cf_reader;
mod forward_scanner;
mod point_getter;
pub mod util;

//use super::lock::Lock;
use super::Result;
//use kvproto::kvrpcpb::IsolationLevel;
use raftstore::store::engine::IterOption;
use std::u64;
use storage::engine::{Cursor, ScanMode, Snapshot, Statistics};
use storage::{Key, Value};

pub use self::backward_scanner::{BackwardScanner, BackwardScannerBuilder};
pub use self::cf_reader::{CfReader, CfReaderBuilder};
pub use self::forward_scanner::{ForwardScanner, ForwardScannerBuilder};
pub use self::point_getter::{PointGetter, PointGetterBuilder};

// When there are many versions for the user key, after several tries,
// we will use seek to locate the right position. But this will turn around
// the write cf's iterator's direction inside RocksDB, and the next user key
// need to turn back the direction to backward. As we have tested, turn around
// iterator's direction from forward to backward is as expensive as seek in
// RocksDB, so don't set REVERSE_SEEK_BOUND too small.
const REVERSE_SEEK_BOUND: u64 = 32;

pub struct MvccReader<S: Snapshot> {
    snapshot: S,
    statistics: Statistics,
    // cursors are used for speeding up scans.
    data_cursor: Option<Cursor<S::Iter>>,
    //    lock_cursor: Option<Cursor<S::Iter>>,
    //    write_cursor: Option<Cursor<S::Iter>>,
    scan_mode: Option<ScanMode>,
    key_only: bool,

    fill_cache: bool,
    //    lower_bound: Option<Vec<u8>>,
    //    upper_bound: Option<Vec<u8>>,
    //    isolation_level: IsolationLevel,
}

impl<S: Snapshot> MvccReader<S> {
    pub fn new(
        snapshot: S,
        scan_mode: Option<ScanMode>,
        fill_cache: bool,
        //        lower_bound: Option<Vec<u8>>,
        //        upper_bound: Option<Vec<u8>>,
        //        isolation_level: IsolationLevel,
    ) -> Self {
        Self {
            snapshot,
            statistics: Statistics::default(),
            data_cursor: None,
            //            lock_cursor: None,
            //            write_cursor: None,
            scan_mode,
            //            isolation_level,
            key_only: false,
            fill_cache,
            //            lower_bound,
            //            upper_bound,
        }
    }

    pub fn get_statistics(&self) -> &Statistics {
        &self.statistics
    }

    pub fn collect_statistics_into(&mut self, stats: &mut Statistics) {
        stats.add(&self.statistics);
        self.statistics = Statistics::default();
    }

    pub fn set_key_only(&mut self, key_only: bool) {
        self.key_only = key_only;
    }

    pub fn load_data(&mut self, key: &Key, ts: u64) -> Result<Value> {
        if self.key_only {
            return Ok(vec![]);
        }
        if self.scan_mode.is_some() && self.data_cursor.is_none() {
            let iter_opt = IterOption::new(None, None, self.fill_cache);
            self.data_cursor = Some(self.snapshot.iter(iter_opt, self.get_scan_mode(true))?);
        }

        let k = key.clone().append_ts(ts);
        let res = if let Some(ref mut cursor) = self.data_cursor {
            match cursor.near_seek_get(&k, false, &mut self.statistics.data)? {
                None => panic!("key {} not found, ts {}", key, ts),
                Some(v) => v.to_vec(),
            }
        } else {
            self.statistics.data.get += 1;
            let value = match self.snapshot.get(&k)? {
                None => panic!("key {} not found, ts: {}", key, ts),
                Some(v) => v,
            };
            self.statistics.data.flow_stats.read_bytes +=
                k.raw().unwrap_or_default().len() + value.len();
            self.statistics.data.flow_stats.read_keys += 1;
            value
        };

        self.statistics.data.processed += 1;
        Ok(res)
    }

    fn get_scan_mode(&self, allow_backward: bool) -> ScanMode {
        match self.scan_mode {
            Some(ScanMode::Forward) => ScanMode::Forward,
            Some(ScanMode::Backward) if allow_backward => ScanMode::Backward,
            _ => ScanMode::Mixed,
        }
    }
}

#[cfg(test)]
mod tests {
    //    use kvproto::kvrpcpb::IsolationLevel;
    use kvproto::metapb::{Peer, Region};
    use raftstore::store::keys;
    use raftstore::store::RegionSnapshot;
    use rocksdb::{self, Writable, WriteBatch, DB};
    use std::sync::Arc;
    use std::u64;
    use storage::engine::Modify;
    use storage::mvcc::write::WriteType;
    use storage::mvcc::{self, BackwardScannerBuilder, CfReaderBuilder, MvccTxn};
    use storage::{Key, Mutation, Options, ALL_CFS, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
    use tempdir::TempDir;
    use util::properties::{MvccProperties, MvccPropertiesCollectorFactory};
    use util::rocksdb::{self as rocksdb_util, CFOptions};

    //    use super::REVERSE_SEEK_BOUND;

    struct RegionEngine {
        db: Arc<DB>,
        region: Region,
    }

    impl RegionEngine {
        pub fn new(db: Arc<DB>, region: Region) -> RegionEngine {
            RegionEngine {
                db: Arc::clone(&db),
                region,
            }
        }

        pub fn put(&mut self, pk: &[u8], start_ts: u64, commit_ts: u64) {
            let m = Mutation::Put((Key::from_raw(pk), vec![]));
            self.prewrite(m, pk, start_ts);
            self.commit(pk, start_ts, commit_ts);
        }

        pub fn lock(&mut self, pk: &[u8], start_ts: u64, commit_ts: u64) {
            let m = Mutation::Lock(Key::from_raw(pk));
            self.prewrite(m, pk, start_ts);
            self.commit(pk, start_ts, commit_ts);
        }

        pub fn delete(&mut self, pk: &[u8], start_ts: u64, commit_ts: u64) {
            let m = Mutation::Delete(Key::from_raw(pk));
            self.prewrite(m, pk, start_ts);
            self.commit(pk, start_ts, commit_ts);
        }

        fn prewrite(&mut self, m: Mutation, pk: &[u8], start_ts: u64) {
            let snap = RegionSnapshot::from_raw(Arc::clone(&self.db), self.region.clone());
            let mut txn = MvccTxn::new(snap, start_ts, true).unwrap();
            txn.prewrite(m, pk, &Options::default()).unwrap();
            self.write(txn.into_modifies());
        }

        fn commit(&mut self, pk: &[u8], start_ts: u64, commit_ts: u64) {
            let k = Key::from_raw(pk);
            let snap = RegionSnapshot::from_raw(Arc::clone(&self.db), self.region.clone());
            let mut txn = MvccTxn::new(snap, start_ts, true).unwrap();
            txn.commit(&k, commit_ts).unwrap();
            self.write(txn.into_modifies());
        }

        fn rollback(&mut self, pk: &[u8], start_ts: u64) {
            let k = Key::from_raw(pk);
            let snap = RegionSnapshot::from_raw(Arc::clone(&self.db), self.region.clone());
            let mut txn = MvccTxn::new(snap, start_ts, true).unwrap();
            txn.collapse_rollback(false);
            txn.rollback(&k).unwrap();
            self.write(txn.into_modifies());
        }

        fn gc(&mut self, pk: &[u8], safe_point: u64) {
            let k = Key::from_raw(pk);
            loop {
                let snap = RegionSnapshot::from_raw(Arc::clone(&self.db), self.region.clone());
                let mut txn = MvccTxn::new(snap, safe_point, true).unwrap();
                txn.gc(&k, safe_point).unwrap();
                let modifies = txn.into_modifies();
                if modifies.is_empty() {
                    return;
                }
                self.write(modifies);
            }
        }

        fn write(&mut self, modifies: Vec<Modify>) {
            let db = &self.db;
            let wb = WriteBatch::new();
            for rev in modifies {
                match rev {
                    Modify::Put(cf, k, v) => {
                        let k = keys::data_key(k.encoded());
                        let handle = rocksdb_util::get_cf_handle(db, cf).unwrap();
                        wb.put_cf(handle, &k, &v).unwrap();
                    }
                    Modify::Delete(cf, k) => {
                        let k = keys::data_key(k.encoded());
                        let handle = rocksdb_util::get_cf_handle(db, cf).unwrap();
                        wb.delete_cf(handle, &k).unwrap();
                    }
                    Modify::DeleteRange(cf, k1, k2) => {
                        let k1 = keys::data_key(k1.encoded());
                        let k2 = keys::data_key(k2.encoded());
                        let handle = rocksdb_util::get_cf_handle(db, cf).unwrap();
                        wb.delete_range_cf(handle, &k1, &k2).unwrap();
                    }
                }
            }
            db.write(wb).unwrap();
        }

        fn flush(&mut self) {
            for cf in ALL_CFS {
                let cf = rocksdb_util::get_cf_handle(&self.db, cf).unwrap();
                self.db.flush_cf(cf, true).unwrap();
            }
        }

        fn compact(&mut self) {
            for cf in ALL_CFS {
                let cf = rocksdb_util::get_cf_handle(&self.db, cf).unwrap();
                self.db.compact_range_cf(cf, None, None);
            }
        }
    }

    fn open_db(path: &str, with_properties: bool) -> Arc<DB> {
        let db_opts = rocksdb::DBOptions::new();
        let mut cf_opts = rocksdb::ColumnFamilyOptions::new();
        cf_opts.set_write_buffer_size(32 * 1024 * 1024);
        if with_properties {
            let f = Box::new(MvccPropertiesCollectorFactory::default());
            cf_opts.add_table_properties_collector_factory("tikv.test-collector", f);
        }
        let cfs_opts = vec![
            CFOptions::new(CF_DEFAULT, rocksdb::ColumnFamilyOptions::new()),
            CFOptions::new(CF_RAFT, rocksdb::ColumnFamilyOptions::new()),
            CFOptions::new(CF_LOCK, rocksdb::ColumnFamilyOptions::new()),
            CFOptions::new(CF_WRITE, cf_opts),
        ];
        Arc::new(rocksdb_util::new_engine_opt(path, db_opts, cfs_opts).unwrap())
    }

    fn make_region(id: u64, start_key: Vec<u8>, end_key: Vec<u8>) -> Region {
        let mut peer = Peer::new();
        peer.set_id(id);
        peer.set_store_id(id);
        let mut region = Region::new();
        region.set_id(id);
        region.set_start_key(start_key);
        region.set_end_key(end_key);
        region.mut_peers().push(peer);
        region
    }

    fn check_need_gc(
        db: Arc<DB>,
        region: Region,
        safe_point: u64,
        need_gc: bool,
    ) -> Option<MvccProperties> {
        let snap = RegionSnapshot::from_raw(Arc::clone(&db), region.clone());
        assert_eq!(mvcc::reader::util::need_gc(&snap, safe_point, 1.0), need_gc);
        mvcc::reader::util::get_mvcc_properties(&snap, safe_point)
    }

    #[test]
    fn test_need_gc() {
        let path = TempDir::new("_test_storage_mvcc_reader").expect("");
        let path = path.path().to_str().unwrap();
        let region = make_region(1, vec![0], vec![10]);
        test_without_properties(path, &region);
        test_with_properties(path, &region);
    }

    fn test_without_properties(path: &str, region: &Region) {
        let db = open_db(path, false);
        let mut engine = RegionEngine::new(Arc::clone(&db), region.clone());

        // Put 2 keys.
        engine.put(&[1], 1, 1);
        engine.put(&[4], 2, 2);
        assert!(check_need_gc(Arc::clone(&db), region.clone(), 10, true).is_none());
        engine.flush();
        // After this flush, we have a SST file without properties.
        // Without properties, we always need GC.
        assert!(check_need_gc(Arc::clone(&db), region.clone(), 10, true).is_none());
    }

    fn test_with_properties(path: &str, region: &Region) {
        let db = open_db(path, true);
        let mut engine = RegionEngine::new(Arc::clone(&db), region.clone());

        // Put 2 keys.
        engine.put(&[2], 3, 3);
        engine.put(&[3], 4, 4);
        engine.flush();
        // After this flush, we have a SST file w/ properties, plus the SST
        // file w/o properties from previous flush. We always need GC as
        // long as we can't get properties from any SST files.
        assert!(check_need_gc(Arc::clone(&db), region.clone(), 10, true).is_none());
        engine.compact();
        // After this compact, the two SST files are compacted into a new
        // SST file with properties. Now all SST files have properties and
        // all keys have only one version, so we don't need gc.
        let props = check_need_gc(Arc::clone(&db), region.clone(), 10, false).unwrap();
        assert_eq!(props.min_ts, 1);
        assert_eq!(props.max_ts, 4);
        assert_eq!(props.num_rows, 4);
        assert_eq!(props.num_puts, 4);
        assert_eq!(props.num_versions, 4);
        assert_eq!(props.max_row_versions, 1);

        // Put 2 more keys and delete them.
        engine.put(&[5], 5, 5);
        engine.put(&[6], 6, 6);
        engine.delete(&[5], 7, 7);
        engine.delete(&[6], 8, 8);
        engine.flush();
        // After this flush, keys 5,6 in the new SST file have more than one
        // versions, so we need gc.
        let props = check_need_gc(Arc::clone(&db), region.clone(), 10, true).unwrap();
        assert_eq!(props.min_ts, 1);
        assert_eq!(props.max_ts, 8);
        assert_eq!(props.num_rows, 6);
        assert_eq!(props.num_puts, 6);
        assert_eq!(props.num_versions, 8);
        assert_eq!(props.max_row_versions, 2);
        // But if the `safe_point` is older than all versions, we don't need gc too.
        let props = check_need_gc(Arc::clone(&db), region.clone(), 0, false).unwrap();
        assert_eq!(props.min_ts, u64::MAX);
        assert_eq!(props.max_ts, 0);
        assert_eq!(props.num_rows, 0);
        assert_eq!(props.num_puts, 0);
        assert_eq!(props.num_versions, 0);
        assert_eq!(props.max_row_versions, 0);

        // We gc the two deleted keys manually.
        engine.gc(&[5], 10);
        engine.gc(&[6], 10);
        engine.compact();
        // After this compact, all versions of keys 5,6 are deleted,
        // no keys have more than one versions, so we don't need gc.
        let props = check_need_gc(Arc::clone(&db), region.clone(), 10, false).unwrap();
        assert_eq!(props.min_ts, 1);
        assert_eq!(props.max_ts, 4);
        assert_eq!(props.num_rows, 4);
        assert_eq!(props.num_puts, 4);
        assert_eq!(props.num_versions, 4);
        assert_eq!(props.max_row_versions, 1);

        // A single lock version need gc.
        engine.lock(&[7], 9, 9);
        engine.flush();
        let props = check_need_gc(Arc::clone(&db), region.clone(), 10, true).unwrap();
        assert_eq!(props.min_ts, 1);
        assert_eq!(props.max_ts, 9);
        assert_eq!(props.num_rows, 5);
        assert_eq!(props.num_puts, 4);
        assert_eq!(props.num_versions, 5);
        assert_eq!(props.max_row_versions, 1);
    }

    #[test]
    fn test_mvcc_reader_reverse_scan_many_tombstones() {
        let path =
            TempDir::new("_test_storage_mvcc_reader_reverse_seek_many_tombstones").expect("");
        let path = path.path().to_str().unwrap();
        let region = make_region(1, vec![], vec![]);
        let db = open_db(path, true);
        let mut engine = RegionEngine::new(Arc::clone(&db), region.clone());

        // Generate RocksDB tombstones in write cf.
        let start_ts = 1;
        let safe_point = 2;
        for i in 0..256 {
            for y in 0..256 {
                let pk = &[i as u8, y as u8];
                let m = Mutation::Put((Key::from_raw(pk), vec![]));
                engine.prewrite(m, pk, start_ts);
                engine.rollback(pk, start_ts);
                // Generate 65534 RocksDB tombstones between [0,0] and [255,255].
                if !((i == 0 && y == 0) || (i == 255 && y == 255)) {
                    engine.gc(pk, safe_point);
                }
            }
        }

        // Generate 256 locks in lock cf.
        let start_ts = 3;
        for i in 0..256 {
            let pk = &[i as u8];
            let m = Mutation::Put((Key::from_raw(pk), vec![]));
            engine.prewrite(m, pk, start_ts);
        }

        let snap = RegionSnapshot::from_raw(Arc::clone(&db), region.clone());
        let row = &[255 as u8];
        let k = Key::from_raw(row);

        // Call reverse scan
        let ts = 2;
        let mut scanner = BackwardScannerBuilder::new(snap, ts)
            .range(None, Some(k.take_encoded()))
            .build()
            .unwrap();
        assert_eq!(scanner.read_next().unwrap(), None);
        let statistics = scanner.get_statistics();
        assert_eq!(statistics.lock.prev, 255);
        assert_eq!(statistics.write.prev, 1);
    }

    // TODO: Fix this test
    //    #[test]
    //    fn test_mvcc_reader_reverse_seek_basic() {
    //        let path = TempDir::new("_test_storage_mvcc_reader_reverse_seek_basic").expect("");
    //        let path = path.path().to_str().unwrap();
    //        let region = make_region(1, vec![], vec![]);
    //        let db = open_db(path, true);
    //        let mut engine = RegionEngine::new(Arc::clone(&db), region.clone());
    //
    //        // Generate REVERSE_SEEK_BOUND / 2 Put for key [10].
    //        let k = &[10 as u8];
    //        for ts in 0..REVERSE_SEEK_BOUND / 2 {
    //            let m = Mutation::Put((Key::from_raw(k), vec![ts as u8]));
    //            engine.prewrite(m, k, ts);
    //            engine.commit(k, ts, ts);
    //        }
    //
    //        // Generate REVERSE_SEEK_BOUND + 1 Put for key [9].
    //        let k = &[9 as u8];
    //        for ts in 0..REVERSE_SEEK_BOUND + 1 {
    //            let m = Mutation::Put((Key::from_raw(k), vec![ts as u8]));
    //            engine.prewrite(m, k, ts);
    //            engine.commit(k, ts, ts);
    //        }
    //
    //        // Generate REVERSE_SEEK_BOUND / 2 Put and REVERSE_SEEK_BOUND / 2 + 1 Rollback for key [8].
    //        let k = &[8 as u8];
    //        for ts in 0..REVERSE_SEEK_BOUND + 1 {
    //            let m = Mutation::Put((Key::from_raw(k), vec![ts as u8]));
    //            engine.prewrite(m, k, ts);
    //            if ts < REVERSE_SEEK_BOUND / 2 {
    //                engine.commit(k, ts, ts);
    //            } else {
    //                engine.rollback(k, ts);
    //            }
    //        }
    //
    //        // Generate REVERSE_SEEK_BOUND / 2 Put 1 delete and REVERSE_SEEK_BOUND/2 Rollback for key [7].
    //        let k = &[7 as u8];
    //        for ts in 0..REVERSE_SEEK_BOUND / 2 {
    //            let m = Mutation::Put((Key::from_raw(k), vec![ts as u8]));
    //            engine.prewrite(m, k, ts);
    //            engine.commit(k, ts, ts);
    //        }
    //        {
    //            let ts = REVERSE_SEEK_BOUND / 2;
    //            let m = Mutation::Delete(Key::from_raw(k));
    //            engine.prewrite(m, k, ts);
    //            engine.commit(k, ts, ts);
    //        }
    //        for ts in REVERSE_SEEK_BOUND / 2 + 1..REVERSE_SEEK_BOUND + 1 {
    //            let m = Mutation::Put((Key::from_raw(k), vec![ts as u8]));
    //            engine.prewrite(m, k, ts);
    //            engine.rollback(k, ts);
    //        }
    //
    //        // Generate 1 PUT for key [6].
    //        let k = &[6 as u8];
    //        for ts in 0..1 {
    //            let m = Mutation::Put((Key::from_raw(k), vec![ts as u8]));
    //            engine.prewrite(m, k, ts);
    //            engine.commit(k, ts, ts);
    //        }
    //
    //        // Generate REVERSE_SEEK_BOUND + 1 Rollback for key [5].
    //        let k = &[5 as u8];
    //        for ts in 0..REVERSE_SEEK_BOUND + 1 {
    //            let m = Mutation::Put((Key::from_raw(k), vec![ts as u8]));
    //            engine.prewrite(m, k, ts);
    //            engine.rollback(k, ts);
    //        }
    //
    //        // Generate 1 PUT with ts = REVERSE_SEEK_BOUND and 1 PUT
    //        // with ts = REVERSE_SEEK_BOUND + 1 for key [4].
    //        let k = &[4 as u8];
    //        for ts in REVERSE_SEEK_BOUND..REVERSE_SEEK_BOUND + 2 {
    //            let m = Mutation::Put((Key::from_raw(k), vec![ts as u8]));
    //            engine.prewrite(m, k, ts);
    //            engine.commit(k, ts, ts);
    //        }
    //
    //        let snap = RegionSnapshot::from_raw(Arc::clone(&db), region.clone());
    //        let mut reader = MvccReader::new(
    //            snap,
    //            Some(ScanMode::Backward),
    //            false,
    //            None,
    //            None,
    //            IsolationLevel::SI,
    //        );
    //
    //        let ts = REVERSE_SEEK_BOUND;
    //        // Use REVERSE_SEEK_BOUND / 2 prev to get key [10].
    //        assert_eq!(
    //            reader.reverse_seek(Key::from_raw(&[11 as u8]), ts).unwrap(),
    //            Some((
    //                Key::from_raw(&[10 as u8]),
    //                vec![(REVERSE_SEEK_BOUND / 2 - 1) as u8]
    //            ))
    //        );
    //        let mut total_prev = REVERSE_SEEK_BOUND as usize / 2;
    //        let mut total_seek = 0;
    //        let mut total_next = 0;
    //        assert_eq!(reader.get_statistics().write.prev, total_prev);
    //        assert_eq!(reader.get_statistics().write.seek, total_seek);
    //        assert_eq!(reader.get_statistics().write.next, total_next);
    //        assert_eq!(reader.get_statistics().write.seek_for_prev, 1);
    //        assert_eq!(reader.get_statistics().write.get, 0);
    //
    //        // Use REVERSE_SEEK_BOUND prev and 1 seek to get key [9].
    //        // So the total prev += REVERSE_SEEK_BOUND, total seek = 1.
    //        assert_eq!(
    //            reader.reverse_seek(Key::from_raw(&[10 as u8]), ts).unwrap(),
    //            Some((Key::from_raw(&[9 as u8]), vec![REVERSE_SEEK_BOUND as u8]))
    //        );
    //        total_prev += REVERSE_SEEK_BOUND as usize;
    //        total_seek += 1;
    //        assert_eq!(reader.get_statistics().write.prev, total_prev);
    //        assert_eq!(reader.get_statistics().write.seek, total_seek);
    //        assert_eq!(reader.get_statistics().write.next, total_next);
    //        assert_eq!(reader.get_statistics().write.seek_for_prev, 1);
    //        assert_eq!(reader.get_statistics().write.get, 0);
    //
    //        // Use REVERSE_SEEK_BOUND + 1 prev (1 in near_reverse_seek and REVERSE_SEEK_BOUND
    //        // in reverse_get_impl), 1 seek and 1 next to get key [8].
    //        // So the total prev += REVERSE_SEEK_BOUND + 1, total next += 1, total seek += 1.
    //        assert_eq!(
    //            reader.reverse_seek(Key::from_raw(&[9 as u8]), ts).unwrap(),
    //            Some((
    //                Key::from_raw(&[8 as u8]),
    //                vec![(REVERSE_SEEK_BOUND / 2 - 1) as u8]
    //            ))
    //        );
    //        total_prev += REVERSE_SEEK_BOUND as usize + 1;
    //        total_seek += 1;
    //        total_next += 1;
    //        assert_eq!(reader.get_statistics().write.prev, total_prev);
    //        assert_eq!(reader.get_statistics().write.seek, total_seek);
    //        assert_eq!(reader.get_statistics().write.next, total_next);
    //        assert_eq!(reader.get_statistics().write.seek_for_prev, 1);
    //        assert_eq!(reader.get_statistics().write.get, 0);
    //
    //        // key [7] will cause REVERSE_SEEK_BOUND + 2 prev (2 in near_reverse_seek and
    //        // REVERSE_SEEK_BOUND in reverse_get_impl), 1 seek and 1 next and get DEL.
    //        // key [6] will cause 3 prev (2 in near_reverse_seek and 1 in reverse_get_impl).
    //        // So the total prev += REVERSE_SEEK_BOUND + 6, total next += 1, total seek += 1.
    //        assert_eq!(
    //            reader.reverse_seek(Key::from_raw(&[8 as u8]), ts).unwrap(),
    //            Some((Key::from_raw(&[6 as u8]), vec![0 as u8]))
    //        );
    //        total_prev += REVERSE_SEEK_BOUND as usize + 5;
    //        total_seek += 1;
    //        total_next += 1;
    //        assert_eq!(reader.get_statistics().write.prev, total_prev);
    //        assert_eq!(reader.get_statistics().write.seek, total_seek);
    //        assert_eq!(reader.get_statistics().write.next, total_next);
    //        assert_eq!(reader.get_statistics().write.seek_for_prev, 1);
    //        assert_eq!(reader.get_statistics().write.get, 0);
    //
    //        // key [5] will cause REVERSE_SEEK_BOUND prev (REVERSE_SEEK_BOUND in reverse_get_impl)
    //        // and 1 seek but get none.
    //        // And then will call near_reverse_seek(key[5]) to fetch the previous key, this will cause
    //        // 2 prev in near_reverse_seek.
    //        // key [4] will cause 1 prev.
    //        // So the total prev += REVERSE_SEEK_BOUND + 3, total next += 1, total seek += 1.
    //        assert_eq!(
    //            reader.reverse_seek(Key::from_raw(&[6 as u8]), ts).unwrap(),
    //            Some((Key::from_raw(&[4 as u8]), vec![REVERSE_SEEK_BOUND as u8]))
    //        );
    //        total_prev += REVERSE_SEEK_BOUND as usize + 3;
    //        total_seek += 1;
    //        total_next += 1;
    //        assert_eq!(reader.get_statistics().write.prev, total_prev);
    //        assert_eq!(reader.get_statistics().write.seek, total_seek);
    //        assert_eq!(reader.get_statistics().write.next, total_next);
    //        assert_eq!(reader.get_statistics().write.seek_for_prev, 1);
    //        assert_eq!(reader.get_statistics().write.get, 0);
    //
    //        // Use a prev and reach the very beginning.
    //        assert_eq!(
    //            reader.reverse_seek(Key::from_raw(&[4 as u8]), ts).unwrap(),
    //            None
    //        );
    //        total_prev += 1;
    //        assert_eq!(reader.get_statistics().write.prev, total_prev);
    //        assert_eq!(reader.get_statistics().write.seek, total_seek);
    //        assert_eq!(reader.get_statistics().write.next, total_next);
    //        assert_eq!(reader.get_statistics().write.seek_for_prev, 1);
    //        assert_eq!(reader.get_statistics().write.get, 0);
    //    }

    #[test]
    fn test_near_reverse_seek_write_by_start_ts() {
        let path =
            TempDir::new("_test_storage_mvcc_reader_near_reverse_seek_write_by_start_ts").unwrap();
        let path = path.path().to_str().unwrap();
        let region = make_region(1, vec![], vec![]);
        let db = open_db(path, true);
        let mut engine = RegionEngine::new(Arc::clone(&db), region.clone());

        let (k, v) = (b"k", b"v");
        let m = Mutation::Put((Key::from_raw(k), v.to_vec()));
        engine.prewrite(m, k, 1);
        engine.commit(k, 1, 10);

        engine.rollback(k, 5);
        engine.rollback(k, 20);

        let m = Mutation::Put((Key::from_raw(k), v.to_vec()));
        engine.prewrite(m, k, 25);
        engine.commit(k, 25, 30);

        let m = Mutation::Put((Key::from_raw(k), v.to_vec()));
        engine.prewrite(m, k, 35);
        engine.commit(k, 35, 40);

        let snap = RegionSnapshot::from_raw(Arc::clone(&db), region.clone());
        let mut cf_reader = CfReaderBuilder::new(snap).build().unwrap();

        // Let's assume `40_35 PUT` means a commit version with start ts is 35 and commit ts
        // is 40.
        // Commit versions: [40_35 PUT, 30_25 PUT, 20_20 Rollback, 10_1 PUT, 5_5 Rollback].
        let key = Key::from_raw(k);
        let (commit_ts, write) = cf_reader
            .near_reverse_seek_write_by_start_ts(&key, 35, true)
            .unwrap()
            .unwrap();
        assert_eq!(commit_ts, 40);
        assert_eq!(write.write_type, WriteType::Put);
        assert_eq!(write.start_ts, 35);

        let (commit_ts, write) = cf_reader
            .near_reverse_seek_write_by_start_ts(&key, 25, true)
            .unwrap()
            .unwrap();
        assert_eq!(commit_ts, 30);
        assert_eq!(write.write_type, WriteType::Put);
        assert_eq!(write.start_ts, 25);

        let (commit_ts, write) = cf_reader
            .near_reverse_seek_write_by_start_ts(&key, 20, true)
            .unwrap()
            .unwrap();
        assert_eq!(commit_ts, 20);
        assert_eq!(write.write_type, WriteType::Rollback);
        assert_eq!(write.start_ts, 20);

        let (commit_ts, write) = cf_reader
            .near_reverse_seek_write_by_start_ts(&key, 1, true)
            .unwrap()
            .unwrap();
        assert_eq!(commit_ts, 10);
        assert_eq!(write.write_type, WriteType::Put);
        assert_eq!(write.start_ts, 1);

        let (commit_ts, write) = cf_reader
            .near_reverse_seek_write_by_start_ts(&key, 5, true)
            .unwrap()
            .unwrap();
        assert_eq!(commit_ts, 5);
        assert_eq!(write.write_type, WriteType::Rollback);
        assert_eq!(write.start_ts, 5);

        cf_reader.take_statistics();
        assert!(
            cf_reader
                .near_reverse_seek_write_by_start_ts(&key, 15, true)
                .unwrap()
                .is_none()
        );
        // `near_reverse_seek_write_by_start_ts(&key, 15)` starts from `5_5 Rollback`,
        // stopped at `30_25 PUT`.
        assert_eq!(cf_reader.take_statistics().write.prev, 3);
    }
}
