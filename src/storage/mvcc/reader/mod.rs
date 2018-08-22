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

pub use self::backward_scanner::{BackwardScanner, BackwardScannerBuilder};
pub use self::cf_reader::{CfReader, CfReaderBuilder};
pub use self::forward_scanner::{ForwardScanner, ForwardScannerBuilder};
pub use self::point_getter::{PointGetter, PointGetterBuilder};

#[cfg(test)]
mod tests {
    use kvproto::metapb::{Peer, Region};
    use raftstore::store::keys;
    use raftstore::store::RegionSnapshot;
    use rocksdb::{self, Writable, WriteBatch, DB};
    use std::sync::Arc;
    use std::u64;
    use storage::engine::Modify;
    use storage::mvcc::{self, MvccTxn};
    use storage::{Key, Mutation, Options, ALL_CFS, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
    use tempdir::TempDir;
    use util::properties::{MvccProperties, MvccPropertiesCollectorFactory};
    use util::rocksdb::{self as rocksdb_util, CFOptions};

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
                        let k = keys::data_key(k.as_encoded());
                        let handle = rocksdb_util::get_cf_handle(db, cf).unwrap();
                        wb.put_cf(handle, &k, &v).unwrap();
                    }
                    Modify::Delete(cf, k) => {
                        let k = keys::data_key(k.as_encoded());
                        let handle = rocksdb_util::get_cf_handle(db, cf).unwrap();
                        wb.delete_cf(handle, &k).unwrap();
                    }
                    Modify::DeleteRange(cf, k1, k2) => {
                        let k1 = keys::data_key(k1.as_encoded());
                        let k2 = keys::data_key(k2.as_encoded());
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
}
