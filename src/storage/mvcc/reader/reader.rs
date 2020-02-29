// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::kv::{Cursor, ScanMode, Snapshot, Statistics};
use crate::storage::mvcc::{default_not_found_error, Result};
use engine::IterOption;
use engine_rocks::RocksTablePropertiesCollection;
use engine_traits::{TableProperties, TablePropertiesCollection};
use engine_traits::{CF_LOCK, CF_WRITE};
use kvproto::kvrpcpb::IsolationLevel;
use raftstore::coprocessor::properties::MvccProperties;
use txn_types::{Key, Lock, TimeStamp, Value, Write, WriteRef, WriteType};

const GC_MAX_ROW_VERSIONS_THRESHOLD: u64 = 100;

pub struct MvccReader<S: Snapshot> {
    snapshot: S,
    statistics: Statistics,
    // cursors are used for speeding up scans.
    data_cursor: Option<Cursor<S::Iter>>,
    lock_cursor: Option<Cursor<S::Iter>>,
    write_cursor: Option<Cursor<S::Iter>>,

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
            data_cursor: None,
            lock_cursor: None,
            write_cursor: None,
            scan_mode,
            isolation_level,
            key_only: false,
            fill_cache,
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

    pub fn load_data(&mut self, key: &Key, write: Write) -> Result<Value> {
        assert_eq!(write.write_type, WriteType::Put);
        if self.key_only {
            return Ok(vec![]);
        }
        if let Some(val) = write.short_value {
            return Ok(val);
        }
        if self.scan_mode.is_some() && self.data_cursor.is_none() {
            let iter_opt = IterOption::new(None, None, self.fill_cache);
            self.data_cursor = Some(self.snapshot.iter(iter_opt, self.get_scan_mode(true))?);
        }

        let k = key.clone().append_ts(write.start_ts);
        let val = if let Some(ref mut cursor) = self.data_cursor {
            cursor
                .get(&k, &mut self.statistics.data)?
                .map(|v| v.to_vec())
        } else {
            self.statistics.data.get += 1;
            self.snapshot.get(&k)?
        };
        self.statistics.data.processed += 1;

        match val {
            Some(val) => Ok(val),
            None => Err(default_not_found_error(key.to_raw()?, "get")),
        }
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

    pub fn seek_write(&mut self, key: &Key, ts: TimeStamp) -> Result<Option<(TimeStamp, Write)>> {
        if self.scan_mode.is_some() {
            if self.write_cursor.is_none() {
                let iter_opt = IterOption::new(None, None, self.fill_cache);
                let iter = self
                    .snapshot
                    .iter_cf(CF_WRITE, iter_opt, self.get_scan_mode(false))?;
                self.write_cursor = Some(iter);
            }
        } else {
            // use prefix bloom filter
            let iter_opt = IterOption::default()
                .use_prefix_seek()
                .set_prefix_same_as_start(true);
            let iter = self.snapshot.iter_cf(CF_WRITE, iter_opt, ScanMode::Mixed)?;
            self.write_cursor = Some(iter);
        }

        let cursor = self.write_cursor.as_mut().unwrap();
        let ok = cursor.near_seek(&key.clone().append_ts(ts), &mut self.statistics.write)?;
        if !ok {
            return Ok(None);
        }
        let write_key = cursor.key(&mut self.statistics.write);
        let commit_ts = Key::decode_ts_from(write_key)?;
        if !Key::is_user_key_eq(write_key, key.as_encoded()) {
            return Ok(None);
        }
        let write = WriteRef::parse(cursor.value(&mut self.statistics.write))?.to_owned();
        self.statistics.write.processed += 1;
        Ok(Some((commit_ts, write)))
    }

    /// Checks if there is a lock which blocks reading the key at the given ts.
    /// Returns the blocking lock as the `Err` variant.
    fn check_lock(&mut self, key: &Key, ts: TimeStamp) -> Result<()> {
        if let Some(lock) = self.load_lock(key)? {
            return lock
                .check_ts_conflict(key, ts, &Default::default())
                .map_err(From::from);
        }
        Ok(())
    }

    pub fn get(
        &mut self,
        key: &Key,
        ts: TimeStamp,
        skip_lock_check: bool,
    ) -> Result<Option<Value>> {
        if !skip_lock_check {
            // Check for locks that signal concurrent writes.
            match self.isolation_level {
                IsolationLevel::Si => self.check_lock(key, ts)?,
                IsolationLevel::Rc => {}
            }
        }
        if let Some(write) = self.get_write(key, ts)? {
            Ok(Some(self.load_data(key, write)?))
        } else {
            Ok(None)
        }
    }

    pub fn get_write(&mut self, key: &Key, mut ts: TimeStamp) -> Result<Option<Write>> {
        loop {
            match self.seek_write(key, ts)? {
                Some((commit_ts, write)) => match write.write_type {
                    WriteType::Put => {
                        return Ok(Some(write));
                    }
                    WriteType::Delete => {
                        return Ok(None);
                    }
                    WriteType::Lock | WriteType::Rollback => ts = commit_ts.prev(),
                },
                None => return Ok(None),
            }
        }
    }

    pub fn get_txn_commit_info(
        &mut self,
        key: &Key,
        start_ts: TimeStamp,
    ) -> Result<Option<(TimeStamp, WriteType)>> {
        // It's possible a txn with a small `start_ts` has a greater `commit_ts` than a txn with
        // a greater `start_ts` in pessimistic transaction.
        // I.e., txn_1.commit_ts > txn_2.commit_ts > txn_2.start_ts > txn_1.start_ts.
        //
        // Scan all the versions from `TimeStamp::max()` to `start_ts`.
        let mut seek_ts = TimeStamp::max();
        while let Some((commit_ts, write)) = self.seek_write(key, seek_ts)? {
            if write.start_ts == start_ts {
                return Ok(Some((commit_ts, write.write_type)));
            }
            if commit_ts <= start_ts {
                break;
            }
            seek_ts = commit_ts.prev();
        }
        Ok(None)
    }

    fn create_data_cursor(&mut self) -> Result<()> {
        if self.data_cursor.is_none() {
            let iter_opt = IterOption::new(None, None, true);
            let iter = self.snapshot.iter(iter_opt, self.get_scan_mode(true))?;
            self.data_cursor = Some(iter);
        }
        Ok(())
    }

    fn create_write_cursor(&mut self) -> Result<()> {
        if self.write_cursor.is_none() {
            let iter_opt = IterOption::new(None, None, true);
            let iter = self
                .snapshot
                .iter_cf(CF_WRITE, iter_opt, self.get_scan_mode(true))?;
            self.write_cursor = Some(iter);
        }
        Ok(())
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

    /// Return the first committed key for which `start_ts` equals to `ts`
    pub fn seek_ts(&mut self, ts: TimeStamp) -> Result<Option<Key>> {
        assert!(self.scan_mode.is_some());
        self.create_write_cursor()?;

        let cursor = self.write_cursor.as_mut().unwrap();
        let mut ok = cursor.seek_to_first(&mut self.statistics.write);

        while ok {
            if WriteRef::parse(cursor.value(&mut self.statistics.write))?.start_ts == ts {
                return Ok(Some(
                    Key::from_encoded(cursor.key(&mut self.statistics.write).to_vec())
                        .truncate_ts()?,
                ));
            }
            ok = cursor.next(&mut self.statistics.write);
        }
        Ok(None)
    }

    /// Scan locks that satisfies `filter(lock)` returns true, from the given start key `start`.
    /// At most `limit` locks will be returned. If `limit` is set to `0`, it means unlimited.
    ///
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

    pub fn scan_keys(
        &mut self,
        mut start: Option<Key>,
        limit: usize,
    ) -> Result<(Vec<Key>, Option<Key>)> {
        let iter_opt = IterOption::new(None, None, self.fill_cache);
        let scan_mode = self.get_scan_mode(false);
        let mut cursor = self.snapshot.iter_cf(CF_WRITE, iter_opt, scan_mode)?;
        let mut keys = vec![];
        loop {
            let ok = match start {
                Some(ref x) => cursor.near_seek(x, &mut self.statistics.write)?,
                None => cursor.seek_to_first(&mut self.statistics.write),
            };
            if !ok {
                return Ok((keys, None));
            }
            if keys.len() >= limit {
                self.statistics.write.processed += keys.len();
                return Ok((keys, start));
            }
            let key =
                Key::from_encoded(cursor.key(&mut self.statistics.write).to_vec()).truncate_ts()?;
            start = Some(key.clone().append_ts(TimeStamp::zero()));
            keys.push(key);
        }
    }

    // Get all Value of the given key in CF_DEFAULT
    pub fn scan_values_in_default(&mut self, key: &Key) -> Result<Vec<(TimeStamp, Value)>> {
        self.create_data_cursor()?;
        let cursor = self.data_cursor.as_mut().unwrap();
        let mut ok = cursor.seek(key, &mut self.statistics.data)?;
        if !ok {
            return Ok(vec![]);
        }
        let mut v = vec![];
        while ok {
            let cur_key = cursor.key(&mut self.statistics.data);
            let ts = Key::decode_ts_from(cur_key)?;
            if Key::is_user_key_eq(cur_key, key.as_encoded()) {
                v.push((ts, cursor.value(&mut self.statistics.data).to_vec()));
            } else {
                break;
            }
            ok = cursor.next(&mut self.statistics.data);
        }
        Ok(v)
    }

    pub fn need_gc(&self, safe_point: TimeStamp, ratio_threshold: f64) -> bool {
        let prop = match self.snapshot.get_properties_cf(CF_WRITE) {
            Ok(v) => v,
            Err(_) => return true,
        };

        check_need_gc(safe_point, ratio_threshold, prop)
    }
}

// Returns true if it needs gc.
// This is for optimization purpose, does not mean to be accurate.
pub fn check_need_gc(
    safe_point: TimeStamp,
    ratio_threshold: f64,
    write_properties: RocksTablePropertiesCollection,
) -> bool {
    // Always GC.
    if ratio_threshold < 1.0 {
        return true;
    }

    let props = match get_mvcc_properties(safe_point, write_properties) {
        Some(v) => v,
        None => return true,
    };

    // No data older than safe_point to GC.
    if props.min_ts > safe_point {
        return false;
    }

    // Note: Since the properties are file-based, it can be false positive.
    // For example, multiple files can have a different version of the same row.

    // A lot of MVCC versions to GC.
    if props.num_versions as f64 > props.num_rows as f64 * ratio_threshold {
        return true;
    }
    // A lot of non-effective MVCC versions to GC.
    if props.num_versions as f64 > props.num_puts as f64 * ratio_threshold {
        return true;
    }

    // A lot of MVCC versions of a single row to GC.
    props.max_row_versions > GC_MAX_ROW_VERSIONS_THRESHOLD
}

fn get_mvcc_properties(
    safe_point: TimeStamp,
    collection: RocksTablePropertiesCollection,
) -> Option<MvccProperties> {
    if collection.is_empty() {
        return None;
    }
    // Aggregate MVCC properties.
    let mut props = MvccProperties::new();
    for (_, v) in collection.iter() {
        let mvcc = match MvccProperties::decode(&v.user_collected_properties()) {
            Ok(v) => v,
            Err(_) => return None,
        };
        // Filter out properties after safe_point.
        if mvcc.min_ts > safe_point {
            continue;
        }
        props.add(&mvcc);
    }
    Some(props)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::storage::kv::Modify;
    use crate::storage::mvcc::{MvccReader, MvccTxn};
    use engine::rocks::util::CFOptions;
    use engine::rocks::DB;
    use engine::rocks::{self, ColumnFamilyOptions, DBOptions};
    use engine::IterOption;
    use engine_rocks::{Compat, RocksEngine};
    use engine_traits::{Mutable, WriteBatchExt};
    use engine_traits::{ALL_CFS, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
    use kvproto::kvrpcpb::IsolationLevel;
    use kvproto::metapb::{Peer, Region};
    use raftstore::coprocessor::properties::MvccPropertiesCollectorFactory;
    use raftstore::store::RegionSnapshot;
    use std::ops::Bound;
    use std::sync::Arc;
    use std::u64;
    use txn_types::{LockType, Mutation};

    struct RegionEngine {
        db: Arc<DB>,
        region: Region,
    }

    impl RegionEngine {
        fn new(db: &Arc<DB>, region: &Region) -> RegionEngine {
            RegionEngine {
                db: Arc::clone(&db),
                region: region.clone(),
            }
        }

        fn put(
            &mut self,
            pk: &[u8],
            start_ts: impl Into<TimeStamp>,
            commit_ts: impl Into<TimeStamp>,
        ) {
            let start_ts = start_ts.into();
            let m = Mutation::Put((Key::from_raw(pk), vec![]));
            self.prewrite(m, pk, start_ts);
            self.commit(pk, start_ts, commit_ts);
        }

        fn lock(
            &mut self,
            pk: &[u8],
            start_ts: impl Into<TimeStamp>,
            commit_ts: impl Into<TimeStamp>,
        ) {
            let start_ts = start_ts.into();
            let m = Mutation::Lock(Key::from_raw(pk));
            self.prewrite(m, pk, start_ts);
            self.commit(pk, start_ts, commit_ts);
        }

        fn delete(
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
            let snap =
                RegionSnapshot::<RocksEngine>::from_raw(Arc::clone(&self.db), self.region.clone());
            let mut txn = MvccTxn::new(snap, start_ts.into(), true);
            txn.prewrite(m, pk, false, 0, 0, TimeStamp::default())
                .unwrap();
            self.write(txn.into_modifies());
        }

        fn prewrite_pessimistic_lock(
            &mut self,
            m: Mutation,
            pk: &[u8],
            start_ts: impl Into<TimeStamp>,
        ) {
            let snap =
                RegionSnapshot::<RocksEngine>::from_raw(Arc::clone(&self.db), self.region.clone());
            let mut txn = MvccTxn::new(snap, start_ts.into(), true);
            txn.pessimistic_prewrite(
                m,
                pk,
                true,
                0,
                TimeStamp::default(),
                0,
                TimeStamp::default(),
            )
            .unwrap();
            self.write(txn.into_modifies());
        }

        fn acquire_pessimistic_lock(
            &mut self,
            k: Key,
            pk: &[u8],
            start_ts: impl Into<TimeStamp>,
            for_update_ts: impl Into<TimeStamp>,
        ) {
            let snap =
                RegionSnapshot::<RocksEngine>::from_raw(Arc::clone(&self.db), self.region.clone());
            let mut txn = MvccTxn::new(snap, start_ts.into(), true);
            txn.acquire_pessimistic_lock(k, pk, false, 0, for_update_ts.into(), false)
                .unwrap();
            self.write(txn.into_modifies());
        }

        fn commit(
            &mut self,
            pk: &[u8],
            start_ts: impl Into<TimeStamp>,
            commit_ts: impl Into<TimeStamp>,
        ) {
            let snap =
                RegionSnapshot::<RocksEngine>::from_raw(Arc::clone(&self.db), self.region.clone());
            let mut txn = MvccTxn::new(snap, start_ts.into(), true);
            txn.commit(Key::from_raw(pk), commit_ts.into()).unwrap();
            self.write(txn.into_modifies());
        }

        fn rollback(&mut self, pk: &[u8], start_ts: impl Into<TimeStamp>) {
            let snap =
                RegionSnapshot::<RocksEngine>::from_raw(Arc::clone(&self.db), self.region.clone());
            let mut txn = MvccTxn::new(snap, start_ts.into(), true);
            txn.collapse_rollback(false);
            txn.rollback(Key::from_raw(pk)).unwrap();
            self.write(txn.into_modifies());
        }

        fn gc(&mut self, pk: &[u8], safe_point: impl Into<TimeStamp> + Copy) {
            loop {
                let snap = RegionSnapshot::<RocksEngine>::from_raw(
                    Arc::clone(&self.db),
                    self.region.clone(),
                );
                let mut txn = MvccTxn::new(snap, safe_point.into(), true);
                txn.gc(Key::from_raw(pk), safe_point.into()).unwrap();
                let modifies = txn.into_modifies();
                if modifies.is_empty() {
                    return;
                }
                self.write(modifies);
            }
        }

        fn write(&mut self, modifies: Vec<Modify>) {
            let db = &self.db;
            let wb = db.c().write_batch();
            for rev in modifies {
                match rev {
                    Modify::Put(cf, k, v) => {
                        let k = keys::data_key(k.as_encoded());
                        wb.put_cf(cf, &k, &v).unwrap();
                    }
                    Modify::Delete(cf, k) => {
                        let k = keys::data_key(k.as_encoded());
                        wb.delete_cf(cf, &k).unwrap();
                    }
                    Modify::DeleteRange(cf, k1, k2, notify_only) => {
                        if !notify_only {
                            let k1 = keys::data_key(k1.as_encoded());
                            let k2 = keys::data_key(k2.as_encoded());
                            wb.delete_range_cf(cf, &k1, &k2).unwrap();
                        }
                    }
                }
            }
            db.c().write(&wb).unwrap();
        }

        fn flush(&mut self) {
            for cf in ALL_CFS {
                let cf = rocks::util::get_cf_handle(&self.db, cf).unwrap();
                self.db.flush_cf(cf, true).unwrap();
            }
        }

        fn compact(&mut self) {
            for cf in ALL_CFS {
                let cf = rocks::util::get_cf_handle(&self.db, cf).unwrap();
                self.db.compact_range_cf(cf, None, None);
            }
        }
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

    fn check_need_gc(
        db: Arc<DB>,
        region: Region,
        safe_point: impl Into<TimeStamp>,
        need_gc: bool,
    ) -> Option<MvccProperties> {
        let snap = RegionSnapshot::<RocksEngine>::from_raw(db, region);
        let reader = MvccReader::new(snap, None, false, IsolationLevel::Si);
        let safe_point = safe_point.into();
        assert_eq!(reader.need_gc(safe_point, 1.0), need_gc);
        get_mvcc_properties(
            safe_point,
            reader.snapshot.get_properties_cf(CF_WRITE).unwrap(),
        )
    }

    #[test]
    fn test_need_gc() {
        let path = tempfile::Builder::new()
            .prefix("test_storage_mvcc_reader")
            .tempdir()
            .unwrap();
        let path = path.path().to_str().unwrap();
        let region = make_region(1, vec![0], vec![10]);
        test_without_properties(path, &region);
        test_with_properties(path, &region);
    }

    fn test_without_properties(path: &str, region: &Region) {
        let db = open_db(path, false);
        let mut engine = RegionEngine::new(&db, &region);

        // Put 2 keys.
        engine.put(&[1], 1, 1);
        engine.put(&[4], 2, 2);
        assert!(check_need_gc(Arc::clone(&db), region.clone(), 10, true).is_none());
        engine.flush();
        // After this flush, we have a SST file without properties.
        // Without properties, we always need GC.
        assert!(check_need_gc(Arc::clone(&db), region.clone(), 10, true).is_none());
    }

    #[test]
    fn test_ts_filter() {
        let path = tempfile::Builder::new()
            .prefix("test_ts_filter")
            .tempdir()
            .unwrap();
        let path = path.path().to_str().unwrap();
        let region = make_region(1, vec![0], vec![13]);

        let db = open_db(path, true);
        let mut engine = RegionEngine::new(&db, &region);

        engine.put(&[2], 1, 2);
        engine.put(&[4], 3, 4);
        engine.flush();
        engine.put(&[6], 5, 6);
        engine.put(&[8], 7, 8);
        engine.flush();
        engine.put(&[10], 9, 10);
        engine.put(&[12], 11, 12);
        engine.flush();

        let snap = RegionSnapshot::<RocksEngine>::from_raw(db, region);

        let tests = vec![
            // set nothing.
            (
                Bound::Unbounded,
                Bound::Unbounded,
                vec![2u64, 4, 6, 8, 10, 12],
            ),
            // test set both hint_min_ts and hint_max_ts.
            (Bound::Included(6), Bound::Included(8), vec![6u64, 8]),
            (Bound::Excluded(5), Bound::Included(8), vec![6u64, 8]),
            (Bound::Included(6), Bound::Excluded(9), vec![6u64, 8]),
            (Bound::Excluded(5), Bound::Excluded(9), vec![6u64, 8]),
            // test set only hint_min_ts.
            (Bound::Included(10), Bound::Unbounded, vec![10u64, 12]),
            (Bound::Excluded(9), Bound::Unbounded, vec![10u64, 12]),
            // test set only hint_max_ts.
            (Bound::Unbounded, Bound::Included(7), vec![2u64, 4, 6, 8]),
            (Bound::Unbounded, Bound::Excluded(8), vec![2u64, 4, 6, 8]),
        ];

        for (_, &(min, max, ref res)) in tests.iter().enumerate() {
            let mut iopt = IterOption::default();
            iopt.set_hint_min_ts(min);
            iopt.set_hint_max_ts(max);

            let mut iter = snap.iter_cf(CF_WRITE, iopt).unwrap();

            for (i, expect_ts) in res.iter().enumerate() {
                if i == 0 {
                    assert_eq!(iter.seek_to_first().unwrap(), true);
                } else {
                    assert_eq!(iter.next().unwrap(), true);
                }

                let ts = Key::decode_ts_from(iter.key()).unwrap();
                assert_eq!(ts.into_inner(), *expect_ts);
            }

            assert_eq!(iter.next().unwrap(), false);
        }
    }

    fn test_with_properties(path: &str, region: &Region) {
        let db = open_db(path, true);
        let mut engine = RegionEngine::new(&db, &region);

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
        assert_eq!(props.min_ts, 1.into());
        assert_eq!(props.max_ts, 4.into());
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
        assert_eq!(props.min_ts, 1.into());
        assert_eq!(props.max_ts, 8.into());
        assert_eq!(props.num_rows, 6);
        assert_eq!(props.num_puts, 6);
        assert_eq!(props.num_versions, 8);
        assert_eq!(props.max_row_versions, 2);
        // But if the `safe_point` is older than all versions, we don't need gc too.
        let props = check_need_gc(Arc::clone(&db), region.clone(), 0, false).unwrap();
        assert_eq!(props.min_ts, TimeStamp::max());
        assert_eq!(props.max_ts, TimeStamp::zero());
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
        assert_eq!(props.min_ts, 1.into());
        assert_eq!(props.max_ts, 4.into());
        assert_eq!(props.num_rows, 4);
        assert_eq!(props.num_puts, 4);
        assert_eq!(props.num_versions, 4);
        assert_eq!(props.max_row_versions, 1);

        // A single lock version need gc.
        engine.lock(&[7], 9, 9);
        engine.flush();
        let props = check_need_gc(Arc::clone(&db), region.clone(), 10, true).unwrap();
        assert_eq!(props.min_ts, 1.into());
        assert_eq!(props.max_ts, 9.into());
        assert_eq!(props.num_rows, 5);
        assert_eq!(props.num_puts, 4);
        assert_eq!(props.num_versions, 5);
        assert_eq!(props.max_row_versions, 1);
    }

    #[test]
    fn test_get_txn_commit_info() {
        let path = tempfile::Builder::new()
            .prefix("_test_storage_mvcc_reader_get_txn_commit_info")
            .tempdir()
            .unwrap();
        let path = path.path().to_str().unwrap();
        let region = make_region(1, vec![], vec![]);
        let db = open_db(path, true);
        let mut engine = RegionEngine::new(&db, &region);

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

        let snap = RegionSnapshot::<RocksEngine>::from_raw(db, region);
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
        let path = tempfile::Builder::new()
            .prefix("_test_storage_mvcc_reader_get_txn_commit_info_of_pessimistic_txn")
            .tempdir()
            .unwrap();
        let path = path.path().to_str().unwrap();
        let region = make_region(1, vec![], vec![]);
        let db = open_db(path, true);
        let mut engine = RegionEngine::new(&db, &region);

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

        let snap = RegionSnapshot::<RocksEngine>::from_raw(db, region);
        let mut reader = MvccReader::new(snap, None, false, IsolationLevel::Si);
        let (commit_ts, write_type) = reader.get_txn_commit_info(&key, 2.into()).unwrap().unwrap();
        assert_eq!(commit_ts, 3.into());
        assert_eq!(write_type, WriteType::Put);

        let (commit_ts, write_type) = reader.get_txn_commit_info(&key, 1.into()).unwrap().unwrap();
        assert_eq!(commit_ts, 4.into());
        assert_eq!(write_type, WriteType::Put);
    }

    #[test]
    fn test_seek_write() {
        let path = tempfile::Builder::new()
            .prefix("_test_storage_mvcc_reader_seek_write")
            .tempdir()
            .unwrap();
        let path = path.path().to_str().unwrap();
        let region = make_region(1, vec![], vec![]);
        let db = open_db(path, true);
        let mut engine = RegionEngine::new(&db, &region);

        let (k, v) = (b"k", b"v");
        let m = Mutation::Put((Key::from_raw(k), v.to_vec()));
        engine.prewrite(m.clone(), k, 1);
        engine.commit(k, 1, 5);

        engine.rollback(k, 3);
        engine.rollback(k, 7);

        engine.prewrite(m.clone(), k, 15);
        engine.commit(k, 15, 17);

        // Timestamp overlap with the previous transaction.
        engine.acquire_pessimistic_lock(Key::from_raw(k), k, 10, 18);
        engine.prewrite_pessimistic_lock(Mutation::Lock(Key::from_raw(k)), k, 10);
        engine.commit(k, 10, 20);

        engine.prewrite(m, k, 23);
        engine.commit(k, 23, 25);

        // Let's assume `2_1 PUT` means a commit version with start ts is 1 and commit ts
        // is 2.
        // Commit versions: [25_23 PUT, 20_10 PUT, 17_15 PUT, 7_7 Rollback, 5_1 PUT, 3_3 Rollback].
        let snap = RegionSnapshot::<RocksEngine>::from_raw(Arc::clone(&db), region.clone());
        let mut reader = MvccReader::new(snap, None, false, IsolationLevel::Si);

        let k = Key::from_raw(k);
        let (commit_ts, write) = reader.seek_write(&k, 30.into()).unwrap().unwrap();
        assert_eq!(commit_ts, 25.into());
        assert_eq!(
            write,
            Write::new(WriteType::Put, 23.into(), Some(v.to_vec()))
        );

        let (commit_ts, write) = reader.seek_write(&k, 25.into()).unwrap().unwrap();
        assert_eq!(commit_ts, 25.into());
        assert_eq!(
            write,
            Write::new(WriteType::Put, 23.into(), Some(v.to_vec()))
        );

        let (commit_ts, write) = reader.seek_write(&k, 20.into()).unwrap().unwrap();
        assert_eq!(commit_ts, 20.into());
        assert_eq!(write, Write::new(WriteType::Lock, 10.into(), None));

        let (commit_ts, write) = reader.seek_write(&k, 19.into()).unwrap().unwrap();
        assert_eq!(commit_ts, 17.into());
        assert_eq!(
            write,
            Write::new(WriteType::Put, 15.into(), Some(v.to_vec()))
        );

        let (commit_ts, write) = reader.seek_write(&k, 3.into()).unwrap().unwrap();
        assert_eq!(commit_ts, 3.into());
        assert_eq!(write, Write::new(WriteType::Rollback, 3.into(), None));

        let (commit_ts, write) = reader.seek_write(&k, 16.into()).unwrap().unwrap();
        assert_eq!(commit_ts, 7.into());
        assert_eq!(write, Write::new(WriteType::Rollback, 7.into(), None));

        let (commit_ts, write) = reader.seek_write(&k, 6.into()).unwrap().unwrap();
        assert_eq!(commit_ts, 5.into());
        assert_eq!(
            write,
            Write::new(WriteType::Put, 1.into(), Some(v.to_vec()))
        );

        assert!(reader.seek_write(&k, 2.into()).unwrap().is_none());

        // Test seek_write should not see the next key.
        let (k2, v2) = (b"k2", b"v2");
        let m2 = Mutation::Put((Key::from_raw(k2), v2.to_vec()));
        engine.prewrite(m2, k2, 1);
        engine.commit(k2, 1, 2);

        let snap = RegionSnapshot::<RocksEngine>::from_raw(Arc::clone(&db), region);
        let mut reader = MvccReader::new(snap, None, false, IsolationLevel::Si);

        let (commit_ts, write) = reader
            .seek_write(&Key::from_raw(k2), 3.into())
            .unwrap()
            .unwrap();
        assert_eq!(commit_ts, 2.into());
        assert_eq!(
            write,
            Write::new(WriteType::Put, 1.into(), Some(v2.to_vec()))
        );

        assert!(reader.seek_write(&k, 2.into()).unwrap().is_none());

        // Test seek_write touches region's end.
        let region1 = make_region(1, vec![], Key::from_raw(b"k1").into_encoded());
        let snap = RegionSnapshot::<RocksEngine>::from_raw(Arc::clone(&db), region1);
        let mut reader = MvccReader::new(snap, None, false, IsolationLevel::Si);

        assert!(reader.seek_write(&k, 2.into()).unwrap().is_none());
    }

    #[test]
    fn test_get_write() {
        let path = tempfile::Builder::new()
            .prefix("_test_storage_mvcc_reader_get_write")
            .tempdir()
            .unwrap();
        let path = path.path().to_str().unwrap();
        let region = make_region(1, vec![], vec![]);
        let db = open_db(path, true);
        let mut engine = RegionEngine::new(&db, &region);

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

        let snap = RegionSnapshot::<RocksEngine>::from_raw(db, region);
        let mut reader = MvccReader::new(snap, None, false, IsolationLevel::Si);

        // Let's assume `2_1 PUT` means a commit version with start ts is 1 and commit ts
        // is 2.
        // Commit versions: [21_17 LOCK, 20_18 PUT, 15_13 LOCK, 14_12 PUT, 9_8 DELETE, 7_6 LOCK,
        //                   5_5 Rollback, 2_1 PUT].
        let key = Key::from_raw(k);

        assert!(reader.get_write(&key, 1.into()).unwrap().is_none());

        let write = reader.get_write(&key, 2.into()).unwrap().unwrap();
        assert_eq!(write.write_type, WriteType::Put);
        assert_eq!(write.start_ts, 1.into());

        let write = reader.get_write(&key, 5.into()).unwrap().unwrap();
        assert_eq!(write.write_type, WriteType::Put);
        assert_eq!(write.start_ts, 1.into());

        let write = reader.get_write(&key, 7.into()).unwrap().unwrap();
        assert_eq!(write.write_type, WriteType::Put);
        assert_eq!(write.start_ts, 1.into());

        assert!(reader.get_write(&key, 9.into()).unwrap().is_none());

        let write = reader.get_write(&key, 14.into()).unwrap().unwrap();
        assert_eq!(write.write_type, WriteType::Put);
        assert_eq!(write.start_ts, 12.into());

        let write = reader.get_write(&key, 16.into()).unwrap().unwrap();
        assert_eq!(write.write_type, WriteType::Put);
        assert_eq!(write.start_ts, 12.into());

        let write = reader.get_write(&key, 20.into()).unwrap().unwrap();
        assert_eq!(write.write_type, WriteType::Put);
        assert_eq!(write.start_ts, 18.into());

        let write = reader.get_write(&key, 24.into()).unwrap().unwrap();
        assert_eq!(write.write_type, WriteType::Put);
        assert_eq!(write.start_ts, 18.into());

        assert!(reader
            .get_write(&Key::from_raw(b"j"), 100.into())
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_check_lock() {
        let path = tempfile::Builder::new()
            .prefix("_test_storage_mvcc_reader_check_lock")
            .tempdir()
            .unwrap();
        let path = path.path().to_str().unwrap();
        let region = make_region(1, vec![], vec![]);
        let db = open_db(path, true);
        let mut engine = RegionEngine::new(&db, &region);

        let (k1, k2, k3, k4, v) = (b"k1", b"k2", b"k3", b"k4", b"v");
        engine.prewrite(Mutation::Put((Key::from_raw(k1), v.to_vec())), k1, 5);
        engine.prewrite(Mutation::Put((Key::from_raw(k2), v.to_vec())), k1, 5);
        engine.prewrite(Mutation::Lock(Key::from_raw(k3)), k1, 5);

        let snap = RegionSnapshot::<RocksEngine>::from_raw(Arc::clone(&db), region.clone());
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
        let snap = RegionSnapshot::<RocksEngine>::from_raw(Arc::clone(&db), region.clone());
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
        let snap = RegionSnapshot::<RocksEngine>::from_raw(db, region);
        let mut reader = MvccReader::new(snap, None, false, IsolationLevel::Si);
        // Pessimistic locks don't block any read operation
        assert!(reader.check_lock(&Key::from_raw(k4), 10.into()).is_ok());
    }

    #[test]
    fn test_scan_locks() {
        let path = tempfile::Builder::new()
            .prefix("_test_storage_mvcc_reader_scan_locks")
            .tempdir()
            .unwrap();
        let path = path.path().to_str().unwrap();
        let region = make_region(1, vec![], vec![]);
        let db = open_db(path, true);
        let mut engine = RegionEngine::new(&db, &region);

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
                let snap = RegionSnapshot::<RocksEngine>::from_raw(Arc::clone(&db), region.clone());
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

    #[test]
    fn test_get() {
        let path = tempfile::Builder::new()
            .prefix("_test_storage_mvcc_reader_get_write")
            .tempdir()
            .unwrap();
        let path = path.path().to_str().unwrap();
        let region = make_region(1, vec![], vec![]);
        let db = open_db(path, true);
        let mut engine = RegionEngine::new(&db, &region);

        let (k, short_value, long_value) = (
            b"k",
            b"v",
            "v".repeat(txn_types::SHORT_VALUE_MAX_LEN + 1).into_bytes(),
        );
        let m = Mutation::Put((Key::from_raw(k), short_value.to_vec()));
        engine.prewrite(m, k, 1);
        engine.commit(k, 1, 2);

        engine.rollback(k, 5);

        engine.lock(k, 6, 7);

        engine.delete(k, 8, 9);

        let m = Mutation::Put((Key::from_raw(k), long_value.to_vec()));
        engine.prewrite(m, k, 10);

        let snap = RegionSnapshot::<RocksEngine>::from_raw(db.clone(), region.clone());
        let mut reader = MvccReader::new(snap, None, false, IsolationLevel::Si);
        let key = Key::from_raw(k);

        for &skip_lock_check in &[false, true] {
            assert_eq!(
                reader.get(&key, 2.into(), skip_lock_check).unwrap(),
                Some(short_value.to_vec())
            );
            assert_eq!(
                reader.get(&key, 5.into(), skip_lock_check).unwrap(),
                Some(short_value.to_vec())
            );
            assert_eq!(
                reader.get(&key, 7.into(), skip_lock_check).unwrap(),
                Some(short_value.to_vec())
            );
            assert_eq!(reader.get(&key, 9.into(), skip_lock_check).unwrap(), None);
        }
        assert!(reader.get(&key, 11.into(), false).is_err());
        assert_eq!(reader.get(&key, 9.into(), true).unwrap(), None);

        // Commit the long value
        engine.commit(k, 10, 11);
        let snap = RegionSnapshot::<RocksEngine>::from_raw(db, region);
        let mut reader = MvccReader::new(snap, None, false, IsolationLevel::Si);
        for &skip_lock_check in &[false, true] {
            assert_eq!(
                reader.get(&key, 11.into(), skip_lock_check).unwrap(),
                Some(long_value.to_vec())
            );
        }
    }
}
