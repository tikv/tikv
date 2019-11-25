// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::kv::{Cursor, ScanMode, Snapshot, Statistics};
use crate::storage::mvcc::lock::{Lock, LockType};
use crate::storage::mvcc::write::WriteType;
use crate::storage::mvcc::{default_not_found_error, Write, WriteRef};
use crate::storage::mvcc::{Result, TimeStamp};
use crate::storage::{Key, Value};
use engine::{IterOption, DATA_KEY_PREFIX_LEN};
use engine::{CF_HISTORY, CF_LATEST, CF_LOCK, CF_ROLLBACK};
use kvproto::kvrpcpb::IsolationLevel;

pub struct MvccReader<S: Snapshot> {
    snapshot: S,
    statistics: Statistics,
    // cursors are used for speeding up scans.
    lock_cursor: Option<Cursor<S::Iter>>,
    history_cursor: Option<Cursor<S::Iter>>,
    rollback_cursor: Option<Cursor<S::Iter>>,

    scan_mode: Option<ScanMode>,
    key_only: bool,

    fill_cache: bool,
    isolation_level: IsolationLevel,
}

impl<S: Snapshot> MvccReader<S> {
    pub fn new(
        snapshot: S,
        scan_mode: Option<ScanMode>,
        fill_cache: bool,
        isolation_level: IsolationLevel,
    ) -> Self {
        Self {
            snapshot,
            statistics: Statistics::default(),
            lock_cursor: None,
            history_cursor: None,
            rollback_cursor: None,
            scan_mode,
            isolation_level,
            key_only: false,
            fill_cache,
        }
    }

    pub fn get_latest(&self, key: &Key) -> Result<Option<Write>> {
        if let Some(v) = self.snapshot.get_cf(CF_LATEST, key)? {
            Ok(Some(WriteRef::parse(&v)?.to_owned()))
        } else {
            Ok(None)
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

    pub fn load_lock(&mut self, key: &Key) -> Result<Option<Lock>> {
        if self.scan_mode.is_some() && self.lock_cursor.is_none() {
            let iter_opt = IterOption::new(None, None, true);
            let iter = self
                .snapshot
                .iter_cf(CF_LOCK, iter_opt, self.get_scan_mode(true))?;
            self.lock_cursor = Some(iter);
        }

        let res = if let Some(ref mut cursor) = self.lock_cursor {
            match cursor.get(key, &mut self.statistics.lock)? {
                Some(v) => Some(Lock::parse(v)?),
                None => None,
            }
        } else {
            self.statistics.lock.get += 1;
            match self.snapshot.get_cf(CF_LOCK, key)? {
                Some(v) => Some(Lock::parse(&v)?),
                None => None,
            }
        };

        if res.is_some() {
            self.statistics.lock.processed += 1;
        }

        Ok(res)
    }

    fn get_scan_mode(&self, allow_backward: bool) -> ScanMode {
        match self.scan_mode {
            Some(ScanMode::Forward) => ScanMode::Forward,
            Some(ScanMode::Backward) if allow_backward => ScanMode::Backward,
            _ => ScanMode::Mixed,
        }
    }

    pub fn get_rollback(&mut self, key: &Key, start_ts: TimeStamp) -> Result<Option<Write>> {
        let key = key.clone().append_ts(start_ts);
        if let Some(v) = self.snapshot.get_cf(CF_ROLLBACK, &key)? {
            Ok(Some(WriteRef::parse(&v)?.to_owned()))
        } else {
            Ok(None)
        }
    }

    pub fn seek_rollback(&mut self, key: &Key, ts: TimeStamp) -> Result<Option<Write>> {
        if self.scan_mode.is_some() {
            if self.rollback_cursor.is_none() {
                let iter_opt = IterOption::new(None, None, self.fill_cache);
                let iter =
                    self.snapshot
                        .iter_cf(CF_ROLLBACK, iter_opt, self.get_scan_mode(false))?;
                self.rollback_cursor = Some(iter);
            }
        } else {
            // use prefix bloom filter
            let iter_opt = IterOption::default()
                .use_prefix_seek()
                .set_prefix_same_as_start(true);
            let iter = self
                .snapshot
                .iter_cf(CF_ROLLBACK, iter_opt, ScanMode::Mixed)?;
            self.rollback_cursor = Some(iter);
        }

        let cursor = self.rollback_cursor.as_mut().unwrap();
        let ok = cursor.near_seek(&key.clone().append_ts(ts), &mut self.statistics.rollback)?;
        if !ok {
            return Ok(None);
        }
        let rollback_key = cursor.key(&mut self.statistics.rollback);
        if !Key::is_user_key_eq(rollback_key, key.as_encoded()) {
            return Ok(None);
        }
        let write = WriteRef::parse(cursor.value(&mut self.statistics.rollback))?.to_owned();
        self.statistics.rollback.processed += 1;
        Ok(Some(write))
    }

    pub fn seek_history(&mut self, key: &Key, ts: TimeStamp) -> Result<Option<Write>> {
        if self.scan_mode.is_some() {
            if self.history_cursor.is_none() {
                let iter_opt = IterOption::new(None, None, self.fill_cache);
                let iter =
                    self.snapshot
                        .iter_cf(CF_HISTORY, iter_opt, self.get_scan_mode(false))?;
                self.history_cursor = Some(iter);
            }
        } else {
            // use prefix bloom filter
            let iter_opt = IterOption::default()
                .use_prefix_seek()
                .set_prefix_same_as_start(true);
            let iter = self
                .snapshot
                .iter_cf(CF_HISTORY, iter_opt, ScanMode::Mixed)?;
            self.history_cursor = Some(iter);
        }

        let cursor = self.history_cursor.as_mut().unwrap();
        let ok = cursor.near_seek(&key.clone().append_ts(ts), &mut self.statistics.history)?;
        if !ok {
            return Ok(None);
        }
        let rollback_key = cursor.key(&mut self.statistics.history);
        if !Key::is_user_key_eq(rollback_key, key.as_encoded()) {
            return Ok(None);
        }
        let write = WriteRef::parse(cursor.value(&mut self.statistics.history))?.to_owned();
        self.statistics.history.processed += 1;
        Ok(Some(write))
    }

    /// Checks if there is a lock which blocks reading the key at the given ts.
    /// Returns the blocking lock as the `Err` variant.
    fn check_lock(&mut self, key: &Key, ts: TimeStamp) -> Result<()> {
        if let Some(lock) = self.load_lock(key)? {
            return lock.check_ts_conflict(key, ts, &Default::default());
        }
        Ok(())
    }

    pub fn get(&mut self, key: &Key, ts: TimeStamp) -> Result<Option<Value>> {
        // Check for locks that signal concurrent writes.
        match self.isolation_level {
            IsolationLevel::Si => self.check_lock(key, ts)?,
            IsolationLevel::Rc => {}
        }

        if let Some(mut write) = self.get_latest(key)? {
            if ts >= write.commit_ts {
                if write.write_type == WriteType::Put {
                    return Ok(Some(write.take_value().unwrap()));
                } else {
                    return Ok(None);
                }
            }
            // seek history
            if let Some(mut history) = self.seek_history(key, ts)? {
                match history.write_type {
                    WriteType::Put => return Ok(Some(history.take_value().unwrap())),
                    WriteType::Delete => return Ok(None),
                    t => panic!("unexpected write type {:?} in history cf", t),
                }
            }
        }
        Ok(None)
    }

    pub fn get_commit(&mut self, key: &Key, ts: TimeStamp) -> Result<Option<Write>> {
        if let Some(write) = self.get_latest(key)? {
            if ts >= write.commit_ts {
                if write.write_type == WriteType::Put {
                    return Ok(Some(write));
                } else {
                    return Ok(None);
                }
            }
            // seek history
            if let Some(history) = self.seek_history(key, ts)? {
                match history.write_type {
                    WriteType::Put => return Ok(Some(history)),
                    WriteType::Delete => return Ok(None),
                    t => panic!("unexpected write type {:?} in history cf", t),
                }
            }
        }
        Ok(None)
    }

    pub fn get_txn_commit_info(
        &mut self,
        key: &Key,
        start_ts: TimeStamp,
    ) -> Result<Option<(TimeStamp, WriteType)>> {
        // Check latest
        let mut has_latest = false;
        if let Some(latest) = self.get_latest(key)? {
            has_latest = true;
            if latest.start_ts == start_ts {
                return Ok(Some((latest.commit_ts, latest.write_type)));
            }
        }

        // It's possible a txn with a small `start_ts` has a greater `commit_ts` than a txn with
        // a greater `start_ts` in pessimistic transaction.
        // I.e., txn_1.commit_ts > txn_2.commit_ts > txn_2.start_ts > txn_1.start_ts.
        //
        // Scan all the versions from `u64::max_value()` to `start_ts`.

        // Check rollback
        let mut seek_ts = TimeStamp::max();;
        while let Some(rollback) = self.seek_rollback(key, seek_ts)? {
            if rollback.start_ts == start_ts {
                return Ok(Some((rollback.commit_ts, rollback.write_type)));
            }
            if rollback.commit_ts <= start_ts {
                break;
            }
            seek_ts = rollback.commit_ts.prev();
        }

        // check history only when latest exists
        if has_latest {
            let mut seek_ts = TimeStamp::max();
            while let Some(history) = self.seek_history(key, seek_ts)? {
                if history.start_ts == start_ts {
                    return Ok(Some((history.commit_ts, history.write_type)));
                }
                if history.commit_ts <= start_ts {
                    break;
                }
                seek_ts = history.commit_ts.prev();
            }
        }

        Ok(None)
    }

    fn create_lock_cursor(&mut self) -> Result<()> {
        if self.lock_cursor.is_none() {
            let iter_opt = IterOption::new(None, None, true);
            let iter = self
                .snapshot
                .iter_cf(CF_LOCK, iter_opt, self.get_scan_mode(true))?;
            self.lock_cursor = Some(iter);
        }
        Ok(())
    }

    /// The return type is `(locks, is_remain)`. `is_remain` indicates whether there MAY be
    /// remaining locks that can be scanned.
    pub fn scan_locks<F>(
        &mut self,
        start: Option<&Key>,
        filter: F,
        limit: usize,
    ) -> Result<(Vec<(Key, Lock)>, bool)>
    where
        F: Fn(&Lock) -> bool,
    {
        self.create_lock_cursor()?;
        let cursor = self.lock_cursor.as_mut().unwrap();
        let ok = match start {
            Some(ref x) => cursor.seek(x, &mut self.statistics.lock)?,
            None => cursor.seek_to_first(&mut self.statistics.lock),
        };
        if !ok {
            return Ok((vec![], false));
        }
        let mut locks = Vec::with_capacity(limit);
        while cursor.valid()? {
            let key = Key::from_encoded_slice(cursor.key(&mut self.statistics.lock));
            let lock = Lock::parse(cursor.value(&mut self.statistics.lock))?;
            if filter(&lock) {
                locks.push((key, lock));
                if limit > 0 && locks.len() == limit {
                    return Ok((locks, true));
                }
            }
            cursor.next(&mut self.statistics.lock);
        }
        self.statistics.lock.processed += locks.len();
        // If we reach here, `cursor.valid()` is `false`, so there MUST be no more locks.
        Ok((locks, false))
    }

    pub fn need_gc(&self, _safe_point: u64, _ratio_threshold: f64) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use crate::raftstore::coprocessor::properties::MvccPropertiesCollectorFactory;
    use crate::raftstore::store::keys;
    use crate::raftstore::store::RegionSnapshot;
    use crate::storage::kv::Modify;
    use crate::storage::mvcc::lock::{Lock, LockType};
    use crate::storage::mvcc::write::{Write, WriteType};
    use crate::storage::mvcc::{MvccReader, MvccTxn, TimeStamp};
    use crate::storage::{Key, Mutation, Options};
    use engine::rocks::util::CFOptions;
    use engine::rocks::{self, ColumnFamilyOptions, DBOptions};
    use engine::rocks::{Writable, WriteBatch, DB};
    use engine::{CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
    use kvproto::kvrpcpb::IsolationLevel;
    use kvproto::metapb::{Peer, Region};
    use std::sync::Arc;
    use std::u64;
    use tempfile::Builder;

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

        pub fn lock(&mut self, pk: &[u8], start_ts: u64, commit_ts: u64) {
            let m = Mutation::Lock(Key::from_raw(pk));
            self.prewrite(m, pk, start_ts);
            self.commit(pk, start_ts, commit_ts);
        }

        pub fn delete(
            &mut self,
            pk: &[u8],
            start_ts: impl Into<TimeStamp>,
            commit_ts: impl Into<TimeStamp>,
        ) {
            let start_ts = start_ts.into();
            let m = Mutation::Delete(Key::from_raw(pk));
            self.prewrite(m, pk, start_ts);
            self.commit(pk, start_ts, commit_ts);
        }

        fn prewrite(&mut self, m: Mutation, pk: &[u8], start_ts: impl Into<TimeStamp>) {
            let snap = RegionSnapshot::from_raw(Arc::clone(&self.db), self.region.clone());
            let mut txn = MvccTxn::new(snap, start_ts.into(), true).unwrap();
            txn.prewrite(m, pk, &Options::default()).unwrap();
            self.write(txn.into_modifies());
        }

        fn prewrite_pessimistic_lock(
            &mut self,
            m: Mutation,
            pk: &[u8],
            start_ts: impl Into<TimeStamp>,
        ) {
            let snap = RegionSnapshot::from_raw(Arc::clone(&self.db), self.region.clone());
            let mut txn = MvccTxn::new(snap, start_ts.into(), true).unwrap();
            let options = Options::default();
            txn.pessimistic_prewrite(m, pk, true, &options).unwrap();
            self.write(txn.into_modifies());
        }

        fn acquire_pessimistic_lock(
            &mut self,
            k: Key,
            pk: &[u8],
            start_ts: impl Into<TimeStamp>,
            for_update_ts: impl Into<TimeStamp>,
        ) {
            let snap = RegionSnapshot::from_raw(Arc::clone(&self.db), self.region.clone());
            let mut txn = MvccTxn::new(snap, start_ts.into(), true).unwrap();
            let mut options = Options::default();
            options.for_update_ts = for_update_ts.into();
            txn.acquire_pessimistic_lock(k, pk, false, &options)
                .unwrap();
            self.write(txn.into_modifies());
        }

        fn commit(
            &mut self,
            pk: &[u8],
            start_ts: impl Into<TimeStamp>,
            commit_ts: impl Into<TimeStamp>,
        ) {
            let snap = RegionSnapshot::from_raw(Arc::clone(&self.db), self.region.clone());
            let mut txn = MvccTxn::new(snap, start_ts.into(), true).unwrap();
            txn.commit(Key::from_raw(pk), commit_ts.into()).unwrap();
            self.write(txn.into_modifies());
        }

        fn rollback(&mut self, pk: &[u8], start_ts: impl Into<TimeStamp>) {
            let snap = RegionSnapshot::from_raw(Arc::clone(&self.db), self.region.clone());
            let mut txn = MvccTxn::new(snap, start_ts.into(), true).unwrap();
            txn.collapse_rollback(false);
            txn.rollback(Key::from_raw(pk)).unwrap();
            self.write(txn.into_modifies());
        }

        //        fn gc(&mut self, pk: &[u8], safe_point: u64) {
        //            loop {
        //                let snap = RegionSnapshot::from_raw(Arc::clone(&self.db), self.region.clone());
        //                let mut txn = MvccTxn::new(snap, safe_point, true).unwrap();
        //                txn.gc(Key::from_raw(pk), safe_point).unwrap();
        //                let modifies = txn.into_modifies();
        //                if modifies.is_empty() {
        //                    return;
        //                }
        //                self.write(modifies);
        //            }
        //        }

        fn write(&mut self, modifies: Vec<Modify>) {
            let db = &self.db;
            let wb = WriteBatch::default();
            for rev in modifies {
                match rev {
                    Modify::Put(cf, k, v) => {
                        let k = keys::data_key(k.as_encoded());
                        let handle = rocks::util::get_cf_handle(db, cf).unwrap();
                        wb.put_cf(handle, &k, &v).unwrap();
                    }
                    Modify::Delete(cf, k) => {
                        let k = keys::data_key(k.as_encoded());
                        let handle = rocks::util::get_cf_handle(db, cf).unwrap();
                        wb.delete_cf(handle, &k).unwrap();
                    }
                    Modify::DeleteRange(cf, k1, k2, notify_only) => {
                        if !notify_only {
                            let k1 = keys::data_key(k1.as_encoded());
                            let k2 = keys::data_key(k2.as_encoded());
                            let handle = rocks::util::get_cf_handle(db, cf).unwrap();
                            wb.delete_range_cf(handle, &k1, &k2).unwrap();
                        }
                    }
                }
            }
            db.write(&wb).unwrap();
        }

        //        fn flush(&mut self) {
        //            for cf in ALL_CFS {
        //                let cf = rocks::util::get_cf_handle(&self.db, cf).unwrap();
        //                self.db.flush_cf(cf, true).unwrap();
        //            }
        //        }

        //        fn compact(&mut self) {
        //            for cf in ALL_CFS {
        //                let cf = rocks::util::get_cf_handle(&self.db, cf).unwrap();
        //                self.db.compact_range_cf(cf, None, None);
        //            }
        //        }
    }

    fn open_db(path: &str, with_properties: bool) -> Arc<DB> {
        let db_opts = DBOptions::new();
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_write_buffer_size(32 * 1024 * 1024);
        if with_properties {
            let f = Box::new(MvccPropertiesCollectorFactory::default());
            cf_opts.add_table_properties_collector_factory("tikv.test-collector", f);
        }
        let cfs_opts = vec![
            CFOptions::new(CF_DEFAULT, ColumnFamilyOptions::new()),
            CFOptions::new(CF_RAFT, ColumnFamilyOptions::new()),
            CFOptions::new(CF_LOCK, ColumnFamilyOptions::new()),
            CFOptions::new(CF_WRITE, cf_opts),
        ];
        Arc::new(rocks::util::new_engine_opt(path, db_opts, cfs_opts).unwrap())
    }

    fn make_region(id: u64, start_key: Vec<u8>, end_key: Vec<u8>) -> Region {
        let mut peer = Peer::default();
        peer.set_id(id);
        peer.set_store_id(id);
        let mut region = Region::default();
        region.set_id(id);
        region.set_start_key(start_key);
        region.set_end_key(end_key);
        region.mut_peers().push(peer);
        region
    }

    #[test]
    fn test_get_txn_commit_info() {
        let path = Builder::new()
            .prefix("_test_storage_mvcc_reader_get_txn_commit_info")
            .tempdir()
            .unwrap();
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

        let m = Mutation::Put((Key::from_raw(k), v.to_vec()));
        engine.acquire_pessimistic_lock(Key::from_raw(k), k, 45, 45);
        engine.prewrite_pessimistic_lock(m, k, 45);
        engine.commit(k, 45, 50);

        let snap = RegionSnapshot::from_raw(Arc::clone(&db), region.clone());
        let mut reader = MvccReader::new(snap, None, false, IsolationLevel::Si);

        // Let's assume `50_45 PUT` means a commit version with start ts is 45 and commit ts
        // is 50.
        // Commit versions: [50_45 PUT, 45_40 PUT, 40_35 PUT, 30_25 PUT, 20_20 Rollback, 10_1 PUT, 5_5 Rollback].
        let key = Key::from_raw(k);
        let (commit_ts, write_type) = reader
            .get_txn_commit_info(&key, 45.into())
            .unwrap()
            .unwrap();
        assert_eq!(commit_ts, 50.into());
        assert_eq!(write_type, WriteType::Put);

        let (commit_ts, write_type) = reader
            .get_txn_commit_info(&key, 35.into())
            .unwrap()
            .unwrap();
        assert_eq!(commit_ts, 40.into());
        assert_eq!(write_type, WriteType::Put);

        let (commit_ts, write_type) = reader
            .get_txn_commit_info(&key, 25.into())
            .unwrap()
            .unwrap();
        assert_eq!(commit_ts, 30.into());
        assert_eq!(write_type, WriteType::Put);

        let (commit_ts, write_type) = reader
            .get_txn_commit_info(&key, 20.into())
            .unwrap()
            .unwrap();
        assert_eq!(commit_ts, 20.into());
        assert_eq!(write_type, WriteType::Rollback);

        let (commit_ts, write_type) = reader.get_txn_commit_info(&key, 1.into()).unwrap().unwrap();
        assert_eq!(commit_ts, 10.into());
        assert_eq!(write_type, WriteType::Put);

        let (commit_ts, write_type) = reader.get_txn_commit_info(&key, 5.into()).unwrap().unwrap();
        assert_eq!(commit_ts, 5.into());
        assert_eq!(write_type, WriteType::Rollback);

        let seek_old = reader.get_statistics().write.seek;
        assert!(reader
            .get_txn_commit_info(&key, 30.into())
            .unwrap()
            .is_none());
        let seek_new = reader.get_statistics().write.seek;

        // `get_txn_commit_info(&key, 30)` stopped at `30_25 PUT`.
        assert_eq!(seek_new - seek_old, 3);
    }

    #[test]
    fn test_get_txn_commit_info_of_pessimistic_txn() {
        let path = Builder::new()
            .prefix("_test_storage_mvcc_reader_get_txn_commit_info_of_pessimistic_txn")
            .tempdir()
            .unwrap();
        let path = path.path().to_str().unwrap();
        let region = make_region(1, vec![], vec![]);
        let db = open_db(path, true);
        let mut engine = RegionEngine::new(Arc::clone(&db), region.clone());

        let (k, v) = (b"k", b"v");
        let key = Key::from_raw(k);
        let m = Mutation::Put((key.clone(), v.to_vec()));

        // txn: start_ts = 2, commit_ts = 3
        engine.acquire_pessimistic_lock(key.clone(), k, 2, 2);
        engine.prewrite_pessimistic_lock(m.clone(), k, 2);
        engine.commit(k, 2, 3);
        // txn: start_ts = 1, commit_ts = 4
        engine.acquire_pessimistic_lock(key.clone(), k, 1, 3);
        engine.prewrite_pessimistic_lock(m, k, 1);
        engine.commit(k, 1, 4);

        let snap = RegionSnapshot::from_raw(Arc::clone(&db), region.clone());
        let mut reader = MvccReader::new(snap, None, false, IsolationLevel::Si);
        let (commit_ts, write_type) = reader.get_txn_commit_info(&key, 2.into()).unwrap().unwrap();
        assert_eq!(commit_ts, 3.into());
        assert_eq!(write_type, WriteType::Put);

        let (commit_ts, write_type) = reader.get_txn_commit_info(&key, 1.into()).unwrap().unwrap();
        assert_eq!(commit_ts, 4.into());
        assert_eq!(write_type, WriteType::Put);
    }

    #[test]
    fn test_get_commit() {
        let path = Builder::new()
            .prefix("_test_storage_mvcc_reader_get_commit")
            .tempdir()
            .unwrap();
        let path = path.path().to_str().unwrap();
        let region = make_region(1, vec![], vec![]);
        let db = open_db(path, true);
        let mut engine = RegionEngine::new(Arc::clone(&db), region.clone());

        let (k, v) = (b"k", b"v");
        let m = Mutation::Put((Key::from_raw(k), v.to_vec()));
        engine.prewrite(m, k, 1);
        engine.commit(k, 1, 2);

        engine.rollback(k, 5);

        engine.lock(k, 6, 7);

        engine.delete(k, 8, 9);

        let m = Mutation::Put((Key::from_raw(k), v.to_vec()));
        engine.prewrite(m, k, 12);
        engine.commit(k, 12, 14);

        let m = Mutation::Lock(Key::from_raw(k));
        engine.acquire_pessimistic_lock(Key::from_raw(k), k, 13, 15);
        engine.prewrite_pessimistic_lock(m, k, 13);
        engine.commit(k, 13, 15);

        let m = Mutation::Put((Key::from_raw(k), v.to_vec()));
        engine.acquire_pessimistic_lock(Key::from_raw(k), k, 18, 18);
        engine.prewrite_pessimistic_lock(m, k, 18);
        engine.commit(k, 18, 20);

        let m = Mutation::Lock(Key::from_raw(k));
        engine.acquire_pessimistic_lock(Key::from_raw(k), k, 17, 21);
        engine.prewrite_pessimistic_lock(m, k, 17);
        engine.commit(k, 17, 21);

        let m = Mutation::Put((Key::from_raw(k), v.to_vec()));
        engine.prewrite(m, k, 24);

        let snap = RegionSnapshot::from_raw(Arc::clone(&db), region.clone());
        let mut reader = MvccReader::new(snap, None, false, IsolationLevel::Si);

        // Let's assume `2_1 PUT` means a commit version with start ts is 1 and commit ts
        // is 2.
        // Commit versions: [21_17 LOCK, 20_18 PUT, 15_13 LOCK, 14_12 PUT, 9_8 DELETE, 7_6 LOCK,
        //                   5_5 Rollback, 2_1 PUT].
        let key = Key::from_raw(k);
        let write = reader.get_commit(&key, 2.into()).unwrap().unwrap();
        assert_eq!(write.write_type, WriteType::Put);
        assert_eq!(write.start_ts, 1.into());

        let write = reader.get_commit(&key, 5.into()).unwrap().unwrap();
        assert_eq!(write.write_type, WriteType::Put);
        assert_eq!(write.start_ts, 1.into());

        let write = reader.get_commit(&key, 7.into()).unwrap().unwrap();
        assert_eq!(write.write_type, WriteType::Put);
        assert_eq!(write.start_ts, 1.into());

        assert!(reader.get_commit(&key, 9.into()).unwrap().is_none());

        let write = reader.get_commit(&key, 11.into()).unwrap().unwrap();
        assert_eq!(write.write_type, WriteType::Put);
        assert_eq!(write.start_ts, 12.into());

        let write = reader.get_commit(&key, 13.into()).unwrap().unwrap();
        assert_eq!(write.write_type, WriteType::Put);
        assert_eq!(write.start_ts, 12.into());

        let write = reader.get_commit(&key, 15.into()).unwrap().unwrap();
        assert_eq!(write.write_type, WriteType::Put);
        assert_eq!(write.start_ts, 18.into());

        let write = reader.get_commit(&key, 24.into()).unwrap().unwrap();
        assert_eq!(write.write_type, WriteType::Put);
        assert_eq!(write.start_ts, 18.into());

        assert!(reader
            .get_commit(&Key::from_raw(b"j"), 100.into())
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_check_lock() {
        let path = Builder::new()
            .prefix("_test_storage_mvcc_reader_check_lock")
            .tempdir()
            .unwrap();
        let path = path.path().to_str().unwrap();
        let region = make_region(1, vec![], vec![]);
        let db = open_db(path, true);
        let mut engine = RegionEngine::new(Arc::clone(&db), region.clone());

        let (k1, k2, k3, k4, v) = (b"k1", b"k2", b"k3", b"k4", b"v");
        engine.prewrite(Mutation::Put((Key::from_raw(k1), v.to_vec())), k1, 5);
        engine.prewrite(Mutation::Put((Key::from_raw(k2), v.to_vec())), k1, 5);
        engine.prewrite(Mutation::Lock(Key::from_raw(k3)), k1, 5);

        let snap = RegionSnapshot::from_raw(Arc::clone(&db), region.clone());
        let mut reader = MvccReader::new(snap, None, false, IsolationLevel::Si);
        // Ignore the lock if read ts is less than the lock version
        assert!(reader.check_lock(&Key::from_raw(k1), 4.into()).is_ok());
        assert!(reader.check_lock(&Key::from_raw(k2), 4.into()).is_ok());
        // Returns the lock if read ts >= lock version
        assert!(reader.check_lock(&Key::from_raw(k1), 6.into()).is_err());
        assert!(reader.check_lock(&Key::from_raw(k2), 6.into()).is_err());
        // Read locks don't block any read operation
        assert!(reader.check_lock(&Key::from_raw(k3), 6.into()).is_ok());
        // Ignore the primary lock when reading the latest committed version by setting TimeStamp::max() as ts
        assert!(reader
            .check_lock(&Key::from_raw(k1), TimeStamp::max())
            .is_ok());
        // Should not ignore the secondary lock even though reading the latest version
        assert!(reader
            .check_lock(&Key::from_raw(k2), TimeStamp::max())
            .is_err());

        // Commit the primary lock only
        engine.commit(k1, 5, 7);
        let snap = RegionSnapshot::from_raw(Arc::clone(&db), region.clone());
        let mut reader = MvccReader::new(snap, None, false, IsolationLevel::Si);
        // Then reading the primary key should succeed
        assert!(reader.check_lock(&Key::from_raw(k1), 6.into()).is_ok());
        // Reading secondary keys should still fail
        assert!(reader.check_lock(&Key::from_raw(k2), 6.into()).is_err());
        assert!(reader
            .check_lock(&Key::from_raw(k2), TimeStamp::max())
            .is_err());

        // Pessimistic locks
        engine.acquire_pessimistic_lock(Key::from_raw(k4), k4, 9, 9);
        let snap = RegionSnapshot::from_raw(Arc::clone(&db), region.clone());
        let mut reader = MvccReader::new(snap, None, false, IsolationLevel::Si);
        // Pessimistic locks don't block any read operation
        assert!(reader.check_lock(&Key::from_raw(k4), 10.into()).is_ok());
    }

    #[test]
    fn test_scan_locks() {
        let path = Builder::new()
            .prefix("_test_storage_mvcc_reader_scan_locks")
            .tempdir()
            .unwrap();
        let path = path.path().to_str().unwrap();
        let region = make_region(1, vec![], vec![]);
        let db = open_db(path, true);
        let mut engine = RegionEngine::new(Arc::clone(&db), region.clone());

        // Put some locks to the db.
        engine.prewrite(
            Mutation::Put((Key::from_raw(b"k1"), b"v1".to_vec())),
            b"k1",
            5,
        );
        engine.prewrite(
            Mutation::Put((Key::from_raw(b"k2"), b"v2".to_vec())),
            b"k1",
            10,
        );
        engine.prewrite(Mutation::Delete(Key::from_raw(b"k3")), b"k1", 10);
        engine.prewrite(Mutation::Lock(Key::from_raw(b"k3\x00")), b"k1", 10);
        engine.prewrite(Mutation::Delete(Key::from_raw(b"k4")), b"k1", 12);
        engine.acquire_pessimistic_lock(Key::from_raw(b"k5"), b"k1", 10, 12);
        engine.acquire_pessimistic_lock(Key::from_raw(b"k6"), b"k1", 12, 12);

        // All locks whose ts <= 10.
        let visible_locks: Vec<_> = vec![
            // key, lock_type, short_value, ts, for_update_ts
            (
                b"k1".to_vec(),
                LockType::Put,
                Some(b"v1".to_vec()),
                5.into(),
                TimeStamp::zero(),
            ),
            (
                b"k2".to_vec(),
                LockType::Put,
                Some(b"v2".to_vec()),
                10.into(),
                TimeStamp::zero(),
            ),
            (
                b"k3".to_vec(),
                LockType::Delete,
                None,
                10.into(),
                TimeStamp::zero(),
            ),
            (
                b"k3\x00".to_vec(),
                LockType::Lock,
                None,
                10.into(),
                TimeStamp::zero(),
            ),
            (
                b"k5".to_vec(),
                LockType::Pessimistic,
                None,
                10.into(),
                12.into(),
            ),
        ]
        .into_iter()
        .map(|(k, lock_type, short_value, ts, for_update_ts)| {
            (
                Key::from_raw(&k),
                Lock::new(
                    lock_type,
                    b"k1".to_vec(),
                    ts,
                    0,
                    short_value,
                    for_update_ts,
                    0,
                    TimeStamp::zero(),
                ),
            )
        })
        .collect();

        // Creates a reader and scan locks,
        let check_scan_lock =
            |start_key: Option<Key>, limit, expect_res: &[_], expect_is_remain| {
                let snap = RegionSnapshot::from_raw(Arc::clone(&db), region.clone());
                let mut reader = MvccReader::new(snap, None, false, IsolationLevel::Si);
                let res = reader
                    .scan_locks(start_key.as_ref(), |l| l.ts <= 10.into(), limit)
                    .unwrap();
                assert_eq!(res.0, expect_res);
                assert_eq!(res.1, expect_is_remain);
            };

        check_scan_lock(None, 6, &visible_locks, false);
        check_scan_lock(None, 5, &visible_locks, true);
        check_scan_lock(None, 4, &visible_locks[0..4], true);
        check_scan_lock(Some(Key::from_raw(b"k2")), 3, &visible_locks[1..4], true);
        check_scan_lock(
            Some(Key::from_raw(b"k3\x00")),
            1,
            &visible_locks[3..4],
            true,
        );
        check_scan_lock(
            Some(Key::from_raw(b"k3\x00")),
            10,
            &visible_locks[3..],
            false,
        );
        // limit = 0 means unlimited.
        check_scan_lock(None, 0, &visible_locks, false);
    }
}
