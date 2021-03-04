// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::kv::{Cursor, CursorBuilder, ScanMode, Snapshot, Statistics};
use crate::storage::mvcc::{default_not_found_error, Result};
use engine_traits::{IterOptions, MvccProperties};
use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE};
use kvproto::kvrpcpb::IsolationLevel;
use std::borrow::Cow;
use txn_types::{Key, Lock, TimeStamp, Value, Write, WriteRef, WriteType};

/// The result of `get_txn_commit_record`, which is used to get the status of a specified
/// transaction from write cf.
#[derive(Debug)]
pub enum TxnCommitRecord {
    /// The commit record of the given transaction is not found. But it's possible that there's
    /// another transaction's commit record, whose `commit_ts` equals to the current transaction's
    /// `start_ts`. That kind of record will be returned via the `overlapped_write` field.
    /// In this case, if the current transaction is to be rolled back, the `overlapped_write` must not
    /// be overwritten.
    None {
        overlapped_write: Option<OverlappedWrite>,
    },
    /// Found the transaction's write record.
    SingleRecord { commit_ts: TimeStamp, write: Write },
    /// The transaction's status is found in another transaction's record's `overlapped_rollback`
    /// field. This may happen when the current transaction's `start_ts` is the same as the
    /// `commit_ts` of another transaction on this key.
    OverlappedRollback { commit_ts: TimeStamp },
}

#[derive(Clone, Debug)]
pub struct OverlappedWrite {
    pub write: Write,
    /// GC fence for `overlapped_write`. PTAL at `txn_types::Write::gc_fence`.
    pub gc_fence: TimeStamp,
}

impl TxnCommitRecord {
    pub fn exist(&self) -> bool {
        match self {
            Self::None { .. } => false,
            Self::SingleRecord { .. } | Self::OverlappedRollback { .. } => true,
        }
    }

    pub fn info(&self) -> Option<(TimeStamp, WriteType)> {
        match self {
            Self::None { .. } => None,
            Self::SingleRecord { commit_ts, write } => Some((*commit_ts, write.write_type)),
            Self::OverlappedRollback { commit_ts } => Some((*commit_ts, WriteType::Rollback)),
        }
    }

    pub fn unwrap_single_record(self) -> (TimeStamp, WriteType) {
        match self {
            Self::SingleRecord { commit_ts, write } => (commit_ts, write.write_type),
            _ => panic!("not a single record: {:?}", self),
        }
    }

    pub fn unwrap_overlapped_rollback(self) -> TimeStamp {
        match self {
            Self::OverlappedRollback { commit_ts } => commit_ts,
            _ => panic!("not an overlapped rollback record: {:?}", self),
        }
    }

    pub fn unwrap_none(self) -> Option<OverlappedWrite> {
        match self {
            Self::None { overlapped_write } => overlapped_write,
            _ => panic!("txn record found but not expected: {:?}", self),
        }
    }
}

pub struct MvccReader<S: Snapshot> {
    snapshot: S,
    pub statistics: Statistics,
    // cursors are used for speeding up scans.
    data_cursor: Option<Cursor<S::Iter>>,
    lock_cursor: Option<Cursor<S::Iter>>,
    pub write_cursor: Option<Cursor<S::Iter>>,

    /// None means following operations are performed on a single user key, i.e.,
    /// different versions of the same key. It can use prefix seek to speed up reads
    /// from the write-cf.
    scan_mode: Option<ScanMode>,
    // Records the current key for prefix seek. Will Reset the write cursor when switching to another key.
    current_key: Option<Key>,

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
            current_key: None,
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

    pub fn load_data(&mut self, key: &Key, write: Write) -> Result<Value> {
        assert_eq!(write.write_type, WriteType::Put);
        if self.key_only {
            return Ok(vec![]);
        }
        if let Some(val) = write.short_value {
            return Ok(val);
        }
        if self.scan_mode.is_some() {
            self.create_data_cursor()?;
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

        match val {
            Some(val) => {
                self.statistics.data.processed_keys += 1;
                Ok(val)
            }
            None => Err(default_not_found_error(key.to_raw()?, "get")),
        }
    }

    pub fn load_lock(&mut self, key: &Key) -> Result<Option<Lock>> {
        if self.scan_mode.is_some() {
            self.create_lock_cursor()?;
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

        Ok(res)
    }

    fn get_scan_mode(&self, allow_backward: bool) -> ScanMode {
        match self.scan_mode {
            Some(ScanMode::Forward) => ScanMode::Forward,
            Some(ScanMode::Backward) if allow_backward => ScanMode::Backward,
            _ => ScanMode::Mixed,
        }
    }

    /// Return:
    ///   (commit_ts, write_record) of the write record for `key` committed before or equal to`ts`
    /// Post Condition:
    ///   leave the write_cursor at the first record which key is less or equal to the `ts` encoded version of `key`
    pub fn seek_write(&mut self, key: &Key, ts: TimeStamp) -> Result<Option<(TimeStamp, Write)>> {
        // Get the cursor for write record
        //
        // When it switches to another key in prefix seek mode, creates a new cursor for it
        // because the current position of the cursor is seldom around `key`.
        if self.scan_mode.is_none() && self.current_key.as_ref().map_or(true, |k| k != key) {
            self.current_key = Some(key.clone());
            self.write_cursor.take();
        }
        self.create_write_cursor()?;
        let cursor = self.write_cursor.as_mut().unwrap();
        // find a `ts` encoded key which is less than the `ts` encoded version of the `key`
        let found = cursor.near_seek(&key.clone().append_ts(ts), &mut self.statistics.write)?;
        if !found {
            return Ok(None);
        }
        let write_key = cursor.key(&mut self.statistics.write);
        let commit_ts = Key::decode_ts_from(write_key)?;
        // check whether the found written_key's "real key" part equals the `key` we want to find
        if !Key::is_user_key_eq(write_key, key.as_encoded()) {
            return Ok(None);
        }
        // parse out the write record
        let write = WriteRef::parse(cursor.value(&mut self.statistics.write))?.to_owned();
        Ok(Some((commit_ts, write)))
    }

    /// Checks if there is a lock which blocks reading the key at the given ts.
    /// Returns the blocking lock as the `Err` variant.
    fn check_lock(&mut self, key: &Key, ts: TimeStamp) -> Result<()> {
        if let Some(lock) = self.load_lock(key)? {
            if let Err(e) = Lock::check_ts_conflict(Cow::Owned(lock), key, ts, &Default::default())
            {
                self.statistics.lock.processed_keys += 1;
                return Err(e.into());
            }
        }
        Ok(())
    }

    /// Gets the value of the specified key's latest version before specified `ts`.
    /// It tries to ensure the write record's `gc_fence`'s ts, if any, greater than specified
    /// `gc_fence_limit`. Pass `None` to `gc_fence_limit` to skip the check.
    /// The caller must guarantee that there's no other `PUT` or `DELETE` versions whose `commit_ts`
    /// is between the found version and the provided `gc_fence_limit` (`gc_fence_limit` is
    /// inclusive).
    /// For transactional reads, the `gc_fence_limit` must be provided to ensure the result is
    /// correct. Generally, it should be the read_ts of the current transaction, which might be
    /// different from the `ts` passed to this function.
    pub fn get(
        &mut self,
        key: &Key,
        ts: TimeStamp,
        gc_fence_limit: Option<TimeStamp>,
        skip_lock_check: bool,
    ) -> Result<Option<Value>> {
        if !skip_lock_check {
            // Check for locks that signal concurrent writes.
            match self.isolation_level {
                IsolationLevel::Si => self.check_lock(key, ts)?,
                IsolationLevel::Rc => {}
            }
        }
        if let Some(write) = self.get_write(key, ts, gc_fence_limit)? {
            Ok(Some(self.load_data(key, write)?))
        } else {
            Ok(None)
        }
    }

    /// Gets the write record of the specified key's latest version before specified `ts`.
    /// It tries to ensure the write record's `gc_fence`'s ts, if any, greater than specified
    /// `gc_fence_limit`. Pass `None` to `gc_fence_limit` to skip the check.
    /// The caller must guarantee that there's no other `PUT` or `DELETE` versions whose `commit_ts`
    /// is between the found version and the provided `gc_fence_limit` (`gc_fence_limit` is
    /// inclusive).
    /// For transactional reads, the `gc_fence_limit` must be provided to ensure the result is
    /// correct. Generally, it should be the read_ts of the current transaction, which might be
    /// different from the `ts` passed to this function.
    pub fn get_write(
        &mut self,
        key: &Key,
        mut ts: TimeStamp,
        gc_fence_limit: Option<TimeStamp>,
    ) -> Result<Option<Write>> {
        loop {
            match self.seek_write(key, ts)? {
                Some((commit_ts, write)) => {
                    if let Some(limit) = gc_fence_limit {
                        if !write.as_ref().check_gc_fence_as_latest_version(limit) {
                            return Ok(None);
                        }
                    }
                    match write.write_type {
                        WriteType::Put => {
                            return Ok(Some(write));
                        }
                        WriteType::Delete => {
                            return Ok(None);
                        }
                        WriteType::Lock | WriteType::Rollback => ts = commit_ts.prev(),
                    }
                }
                None => return Ok(None),
            }
        }
    }

    pub fn get_txn_commit_record(
        &mut self,
        key: &Key,
        start_ts: TimeStamp,
    ) -> Result<TxnCommitRecord> {
        // It's possible a txn with a small `start_ts` has a greater `commit_ts` than a txn with
        // a greater `start_ts` in pessimistic transaction.
        // I.e., txn_1.commit_ts > txn_2.commit_ts > txn_2.start_ts > txn_1.start_ts.
        //
        // Scan all the versions from `TimeStamp::max()` to `start_ts`.
        let mut seek_ts = TimeStamp::max();
        let mut gc_fence = TimeStamp::from(0);
        while let Some((commit_ts, write)) = self.seek_write(key, seek_ts)? {
            if write.start_ts == start_ts {
                return Ok(TxnCommitRecord::SingleRecord { commit_ts, write });
            }
            if commit_ts == start_ts {
                if write.has_overlapped_rollback {
                    return Ok(TxnCommitRecord::OverlappedRollback { commit_ts });
                }
                return Ok(TxnCommitRecord::None {
                    overlapped_write: Some(OverlappedWrite { write, gc_fence }),
                });
            }
            if write.write_type == WriteType::Put || write.write_type == WriteType::Delete {
                gc_fence = commit_ts;
            }
            if commit_ts < start_ts {
                break;
            }
            seek_ts = commit_ts.prev();
        }
        Ok(TxnCommitRecord::None {
            overlapped_write: None,
        })
    }

    fn create_data_cursor(&mut self) -> Result<()> {
        if self.data_cursor.is_none() {
            let cursor = CursorBuilder::new(&self.snapshot, CF_DEFAULT)
                .fill_cache(self.fill_cache)
                .scan_mode(self.get_scan_mode(true))
                .build()?;
            self.data_cursor = Some(cursor);
        }
        Ok(())
    }

    fn create_write_cursor(&mut self) -> Result<()> {
        if self.write_cursor.is_none() {
            let cursor = CursorBuilder::new(&self.snapshot, CF_WRITE)
                .fill_cache(self.fill_cache)
                // Only use prefix seek in non-scan mode.
                .prefix_seek(self.scan_mode.is_none())
                .scan_mode(self.get_scan_mode(true))
                .build()?;
            self.write_cursor = Some(cursor);
        }
        Ok(())
    }

    fn create_lock_cursor(&mut self) -> Result<()> {
        if self.lock_cursor.is_none() {
            let cursor = CursorBuilder::new(&self.snapshot, CF_LOCK)
                .fill_cache(self.fill_cache)
                .scan_mode(self.get_scan_mode(true))
                .build()?;
            self.lock_cursor = Some(cursor);
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
        end: Option<&Key>,
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
            if let Some(end) = end {
                if key >= *end {
                    return Ok((locks, false));
                }
            }

            let lock = Lock::parse(cursor.value(&mut self.statistics.lock))?;
            if filter(&lock) {
                locks.push((key, lock));
                if limit > 0 && locks.len() == limit {
                    return Ok((locks, true));
                }
            }
            cursor.next(&mut self.statistics.lock);
        }
        self.statistics.lock.processed_keys += locks.len();
        // If we reach here, `cursor.valid()` is `false`, so there MUST be no more locks.
        Ok((locks, false))
    }

    pub fn scan_keys(
        &mut self,
        mut start: Option<Key>,
        limit: usize,
    ) -> Result<(Vec<Key>, Option<Key>)> {
        let iter_opt = IterOptions::new(None, None, self.fill_cache);
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
                self.statistics.write.processed_keys += keys.len();
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
}

// Returns true if it needs gc.
// This is for optimization purpose, does not mean to be accurate.
pub fn check_need_gc(safe_point: TimeStamp, ratio_threshold: f64, props: &MvccProperties) -> bool {
    // Always GC.
    if ratio_threshold < 1.0 {
        return true;
    }

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

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::storage::kv::Modify;
    use crate::storage::mvcc::{MvccReader, MvccTxn};

    use crate::storage::txn::{
        acquire_pessimistic_lock, cleanup, commit, gc, prewrite, CommitKind, TransactionKind,
        TransactionProperties,
    };
    use concurrency_manager::ConcurrencyManager;
    use engine_rocks::properties::MvccPropertiesCollectorFactory;
    use engine_rocks::raw::DB;
    use engine_rocks::raw::{ColumnFamilyOptions, DBOptions};
    use engine_rocks::raw_util::CFOptions;
    use engine_rocks::{Compat, RocksSnapshot};
    use engine_traits::{Mutable, MvccPropertiesExt, WriteBatch, WriteBatchExt};
    use engine_traits::{ALL_CFS, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
    use kvproto::kvrpcpb::IsolationLevel;
    use kvproto::metapb::{Peer, Region};
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

        fn txn_props(
            start_ts: TimeStamp,
            primary: &[u8],
            pessimistic: bool,
        ) -> TransactionProperties {
            let kind = if pessimistic {
                TransactionKind::Pessimistic(TimeStamp::default())
            } else {
                TransactionKind::Optimistic(false)
            };

            TransactionProperties {
                start_ts,
                kind,
                commit_kind: CommitKind::TwoPc,
                primary,
                txn_size: 0,
                lock_ttl: 0,
                min_commit_ts: TimeStamp::default(),
                need_old_value: false,
            }
        }

        fn prewrite(&mut self, m: Mutation, pk: &[u8], start_ts: impl Into<TimeStamp>) {
            let snap =
                RegionSnapshot::<RocksSnapshot>::from_raw(self.db.c().clone(), self.region.clone());
            let start_ts = start_ts.into();
            let cm = ConcurrencyManager::new(start_ts);
            let mut txn = MvccTxn::new(snap, start_ts, true, cm);

            prewrite(
                &mut txn,
                &Self::txn_props(start_ts, pk, false),
                m,
                &None,
                false,
            )
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
                RegionSnapshot::<RocksSnapshot>::from_raw(self.db.c().clone(), self.region.clone());
            let start_ts = start_ts.into();
            let cm = ConcurrencyManager::new(start_ts);
            let mut txn = MvccTxn::new(snap, start_ts, true, cm);

            prewrite(
                &mut txn,
                &Self::txn_props(start_ts, pk, true),
                m,
                &None,
                true,
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
                RegionSnapshot::<RocksSnapshot>::from_raw(self.db.c().clone(), self.region.clone());
            let for_update_ts = for_update_ts.into();
            let cm = ConcurrencyManager::new(for_update_ts);
            let mut txn = MvccTxn::new(snap, start_ts.into(), true, cm);
            acquire_pessimistic_lock(
                &mut txn,
                k,
                pk,
                false,
                0,
                for_update_ts,
                false,
                TimeStamp::zero(),
            )
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
                RegionSnapshot::<RocksSnapshot>::from_raw(self.db.c().clone(), self.region.clone());
            let start_ts = start_ts.into();
            let cm = ConcurrencyManager::new(start_ts);
            let mut txn = MvccTxn::new(snap, start_ts, true, cm);
            commit(&mut txn, Key::from_raw(pk), commit_ts.into()).unwrap();
            self.write(txn.into_modifies());
        }

        fn rollback(&mut self, pk: &[u8], start_ts: impl Into<TimeStamp>) {
            let snap =
                RegionSnapshot::<RocksSnapshot>::from_raw(self.db.c().clone(), self.region.clone());
            let start_ts = start_ts.into();
            let cm = ConcurrencyManager::new(start_ts);
            let mut txn = MvccTxn::new(snap, start_ts, true, cm);
            txn.collapse_rollback(false);
            cleanup(&mut txn, Key::from_raw(pk), TimeStamp::zero(), false).unwrap();
            self.write(txn.into_modifies());
        }

        fn rollback_protected(&mut self, pk: &[u8], start_ts: impl Into<TimeStamp>) {
            let snap =
                RegionSnapshot::<RocksSnapshot>::from_raw(self.db.c().clone(), self.region.clone());
            let start_ts = start_ts.into();
            let cm = ConcurrencyManager::new(start_ts);
            let mut txn = MvccTxn::new(snap, start_ts, true, cm);
            txn.collapse_rollback(false);
            cleanup(&mut txn, Key::from_raw(pk), TimeStamp::zero(), true).unwrap();
            self.write(txn.into_modifies());
        }

        fn gc(&mut self, pk: &[u8], safe_point: impl Into<TimeStamp> + Copy) {
            let cm = ConcurrencyManager::new(safe_point.into());
            loop {
                let snap = RegionSnapshot::<RocksSnapshot>::from_raw(
                    self.db.c().clone(),
                    self.region.clone(),
                );
                let mut txn = MvccTxn::new(snap, safe_point.into(), true, cm.clone());
                gc(&mut txn, Key::from_raw(pk), safe_point.into()).unwrap();
                let modifies = txn.into_modifies();
                if modifies.is_empty() {
                    return;
                }
                self.write(modifies);
            }
        }

        fn write(&mut self, modifies: Vec<Modify>) {
            let db = &self.db;
            let mut wb = db.c().write_batch();
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
            wb.write().unwrap();
        }

        fn flush(&mut self) {
            for cf in ALL_CFS {
                let cf = engine_rocks::util::get_cf_handle(&self.db, cf).unwrap();
                self.db.flush_cf(cf, true).unwrap();
            }
        }

        fn compact(&mut self) {
            for cf in ALL_CFS {
                let cf = engine_rocks::util::get_cf_handle(&self.db, cf).unwrap();
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
        Arc::new(engine_rocks::raw_util::new_engine_opt(path, db_opts, cfs_opts).unwrap())
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

    fn get_mvcc_properties_and_check_gc(
        db: Arc<DB>,
        region: Region,
        safe_point: impl Into<TimeStamp>,
        need_gc: bool,
    ) -> Option<MvccProperties> {
        let safe_point = safe_point.into();

        let start = keys::data_key(region.get_start_key());
        let end = keys::data_end_key(region.get_end_key());
        let props = db
            .c()
            .get_mvcc_properties_cf(CF_WRITE, safe_point, &start, &end);
        if let Some(props) = props.as_ref() {
            assert_eq!(check_need_gc(safe_point, 1.0, &props), need_gc);
        }
        props
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
        assert!(
            get_mvcc_properties_and_check_gc(Arc::clone(&db), region.clone(), 10, true).is_none()
        );
        engine.flush();
        // After this flush, we have a SST file without properties.
        // Without properties, we always need GC.
        assert!(
            get_mvcc_properties_and_check_gc(Arc::clone(&db), region.clone(), 10, true).is_none()
        );
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

        let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db.c().clone(), region);

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
            let mut iopt = IterOptions::default();
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

    #[test]
    fn test_ts_filter_lost_delete() {
        let dir = tempfile::Builder::new()
            .prefix("test_ts_filter_lost_deletion")
            .tempdir()
            .unwrap();
        let path = dir.path().to_str().unwrap();
        let region = make_region(1, vec![0], vec![]);

        let db = open_db(&path, true);
        let mut engine = RegionEngine::new(&db, &region);

        let key1 = &[1];
        engine.put(key1, 2, 3);
        engine.flush();
        engine.compact();

        // Delete key 1 commit ts@5 and GC@6
        // Put key 2 commit ts@7
        let key2 = &[2];
        engine.put(key2, 6, 7);
        engine.delete(key1, 4, 5);
        engine.gc(key1, 6);
        engine.flush();

        // Scan kv with ts filter [1, 6].
        let mut iopt = IterOptions::default();
        iopt.set_hint_min_ts(Bound::Included(1));
        iopt.set_hint_max_ts(Bound::Included(6));

        let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db.c().clone(), region);
        let mut iter = snap.iter_cf(CF_WRITE, iopt).unwrap();

        // Must not omit the latest deletion of key1 to prevent seeing outdated record.
        assert_eq!(iter.seek_to_first().unwrap(), true);
        assert_eq!(
            Key::from_encoded_slice(iter.key())
                .to_raw()
                .unwrap()
                .as_slice(),
            key2
        );
        assert_eq!(iter.next().unwrap(), false);
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
        assert!(
            get_mvcc_properties_and_check_gc(Arc::clone(&db), region.clone(), 10, true).is_none()
        );
        engine.compact();
        // After this compact, the two SST files are compacted into a new
        // SST file with properties. Now all SST files have properties and
        // all keys have only one version, so we don't need gc.
        let props =
            get_mvcc_properties_and_check_gc(Arc::clone(&db), region.clone(), 10, false).unwrap();
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
        let props =
            get_mvcc_properties_and_check_gc(Arc::clone(&db), region.clone(), 10, true).unwrap();
        assert_eq!(props.min_ts, 1.into());
        assert_eq!(props.max_ts, 8.into());
        assert_eq!(props.num_rows, 6);
        assert_eq!(props.num_puts, 6);
        assert_eq!(props.num_versions, 8);
        assert_eq!(props.max_row_versions, 2);
        // But if the `safe_point` is older than all versions, we don't need gc too.
        let props =
            get_mvcc_properties_and_check_gc(Arc::clone(&db), region.clone(), 0, false).unwrap();
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
        let props =
            get_mvcc_properties_and_check_gc(Arc::clone(&db), region.clone(), 10, false).unwrap();
        assert_eq!(props.min_ts, 1.into());
        assert_eq!(props.max_ts, 4.into());
        assert_eq!(props.num_rows, 4);
        assert_eq!(props.num_puts, 4);
        assert_eq!(props.num_versions, 4);
        assert_eq!(props.max_row_versions, 1);

        // A single lock version need gc.
        engine.lock(&[7], 9, 9);
        engine.flush();
        let props =
            get_mvcc_properties_and_check_gc(Arc::clone(&db), region.clone(), 10, true).unwrap();
        assert_eq!(props.min_ts, 1.into());
        assert_eq!(props.max_ts, 9.into());
        assert_eq!(props.num_rows, 5);
        assert_eq!(props.num_puts, 4);
        assert_eq!(props.num_versions, 5);
        assert_eq!(props.max_row_versions, 1);
    }

    #[test]
    fn test_get_txn_commit_record() {
        let path = tempfile::Builder::new()
            .prefix("_test_storage_mvcc_reader_get_txn_commit_record")
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

        // Overlapped rollback on the commit record at 40.
        engine.rollback_protected(k, 40);

        let m = Mutation::Put((Key::from_raw(k), v.to_vec()));
        engine.acquire_pessimistic_lock(Key::from_raw(k), k, 45, 45);
        engine.prewrite_pessimistic_lock(m, k, 45);
        engine.commit(k, 45, 50);

        let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db.c().clone(), region);
        let mut reader = MvccReader::new(snap, None, false, IsolationLevel::Si);

        // Let's assume `50_45 PUT` means a commit version with start ts is 45 and commit ts
        // is 50.
        // Commit versions: [50_45 PUT, 45_40 PUT, 40_35 PUT, 30_25 PUT, 20_20 Rollback, 10_1 PUT, 5_5 Rollback].
        let key = Key::from_raw(k);
        let overlapped_write = reader
            .get_txn_commit_record(&key, 55.into())
            .unwrap()
            .unwrap_none();
        assert!(overlapped_write.is_none());

        // When no such record is found but a record of another txn has a write record with
        // its commit_ts equals to current start_ts, it
        let overlapped_write = reader
            .get_txn_commit_record(&key, 50.into())
            .unwrap()
            .unwrap_none()
            .unwrap();
        assert_eq!(overlapped_write.write.start_ts, 45.into());
        assert_eq!(overlapped_write.write.write_type, WriteType::Put);

        let (commit_ts, write_type) = reader
            .get_txn_commit_record(&key, 45.into())
            .unwrap()
            .unwrap_single_record();
        assert_eq!(commit_ts, 50.into());
        assert_eq!(write_type, WriteType::Put);

        let commit_ts = reader
            .get_txn_commit_record(&key, 40.into())
            .unwrap()
            .unwrap_overlapped_rollback();
        assert_eq!(commit_ts, 40.into());

        let (commit_ts, write_type) = reader
            .get_txn_commit_record(&key, 35.into())
            .unwrap()
            .unwrap_single_record();
        assert_eq!(commit_ts, 40.into());
        assert_eq!(write_type, WriteType::Put);

        let (commit_ts, write_type) = reader
            .get_txn_commit_record(&key, 25.into())
            .unwrap()
            .unwrap_single_record();
        assert_eq!(commit_ts, 30.into());
        assert_eq!(write_type, WriteType::Put);

        let (commit_ts, write_type) = reader
            .get_txn_commit_record(&key, 20.into())
            .unwrap()
            .unwrap_single_record();
        assert_eq!(commit_ts, 20.into());
        assert_eq!(write_type, WriteType::Rollback);

        let (commit_ts, write_type) = reader
            .get_txn_commit_record(&key, 1.into())
            .unwrap()
            .unwrap_single_record();
        assert_eq!(commit_ts, 10.into());
        assert_eq!(write_type, WriteType::Put);

        let (commit_ts, write_type) = reader
            .get_txn_commit_record(&key, 5.into())
            .unwrap()
            .unwrap_single_record();
        assert_eq!(commit_ts, 5.into());
        assert_eq!(write_type, WriteType::Rollback);

        let seek_old = reader.get_statistics().write.seek;
        let next_old = reader.get_statistics().write.next;
        assert!(!reader
            .get_txn_commit_record(&key, 30.into())
            .unwrap()
            .exist());
        let seek_new = reader.get_statistics().write.seek;
        let next_new = reader.get_statistics().write.next;

        // `get_txn_commit_record(&key, 30)` stopped at `30_25 PUT`.
        assert_eq!(seek_new - seek_old, 1);
        assert_eq!(next_new - next_old, 2);
    }

    #[test]
    fn test_get_txn_commit_record_of_pessimistic_txn() {
        let path = tempfile::Builder::new()
            .prefix("_test_storage_mvcc_reader_get_txn_commit_record_of_pessimistic_txn")
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

        let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db.c().clone(), region);
        let mut reader = MvccReader::new(snap, None, false, IsolationLevel::Si);

        let (commit_ts, write_type) = reader
            .get_txn_commit_record(&key, 2.into())
            .unwrap()
            .unwrap_single_record();
        assert_eq!(commit_ts, 3.into());
        assert_eq!(write_type, WriteType::Put);

        let (commit_ts, write_type) = reader
            .get_txn_commit_record(&key, 1.into())
            .unwrap()
            .unwrap_single_record();
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
        let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db.c().clone(), region.clone());
        let mut reader = MvccReader::new(snap, None, false, IsolationLevel::Si);

        let k = Key::from_raw(k);
        let (commit_ts, write) = reader.seek_write(&k, 30.into()).unwrap().unwrap();
        assert_eq!(commit_ts, 25.into());
        assert_eq!(
            write,
            Write::new(WriteType::Put, 23.into(), Some(v.to_vec()))
        );
        assert_eq!(reader.get_statistics().write.seek, 1);
        assert_eq!(reader.get_statistics().write.next, 0);

        let (commit_ts, write) = reader.seek_write(&k, 25.into()).unwrap().unwrap();
        assert_eq!(commit_ts, 25.into());
        assert_eq!(
            write,
            Write::new(WriteType::Put, 23.into(), Some(v.to_vec()))
        );
        assert_eq!(reader.get_statistics().write.seek, 1);
        assert_eq!(reader.get_statistics().write.next, 0);

        let (commit_ts, write) = reader.seek_write(&k, 20.into()).unwrap().unwrap();
        assert_eq!(commit_ts, 20.into());
        assert_eq!(write, Write::new(WriteType::Lock, 10.into(), None));
        assert_eq!(reader.get_statistics().write.seek, 1);
        assert_eq!(reader.get_statistics().write.next, 1);

        let (commit_ts, write) = reader.seek_write(&k, 19.into()).unwrap().unwrap();
        assert_eq!(commit_ts, 17.into());
        assert_eq!(
            write,
            Write::new(WriteType::Put, 15.into(), Some(v.to_vec()))
        );
        assert_eq!(reader.get_statistics().write.seek, 1);
        assert_eq!(reader.get_statistics().write.next, 2);

        let (commit_ts, write) = reader.seek_write(&k, 3.into()).unwrap().unwrap();
        assert_eq!(commit_ts, 3.into());
        assert_eq!(write, Write::new_rollback(3.into(), false));
        assert_eq!(reader.get_statistics().write.seek, 1);
        assert_eq!(reader.get_statistics().write.next, 5);

        let (commit_ts, write) = reader.seek_write(&k, 16.into()).unwrap().unwrap();
        assert_eq!(commit_ts, 7.into());
        assert_eq!(write, Write::new_rollback(7.into(), false));
        assert_eq!(reader.get_statistics().write.seek, 1);
        assert_eq!(reader.get_statistics().write.next, 6);
        assert_eq!(reader.get_statistics().write.prev, 3);

        let (commit_ts, write) = reader.seek_write(&k, 6.into()).unwrap().unwrap();
        assert_eq!(commit_ts, 5.into());
        assert_eq!(
            write,
            Write::new(WriteType::Put, 1.into(), Some(v.to_vec()))
        );
        assert_eq!(reader.get_statistics().write.seek, 1);
        assert_eq!(reader.get_statistics().write.next, 7);
        assert_eq!(reader.get_statistics().write.prev, 3);

        assert!(reader.seek_write(&k, 2.into()).unwrap().is_none());
        assert_eq!(reader.get_statistics().write.seek, 1);
        assert_eq!(reader.get_statistics().write.next, 9);
        assert_eq!(reader.get_statistics().write.prev, 3);

        // Test seek_write should not see the next key.
        let (k2, v2) = (b"k2", b"v2");
        let m2 = Mutation::Put((Key::from_raw(k2), v2.to_vec()));
        engine.prewrite(m2, k2, 1);
        engine.commit(k2, 1, 2);

        let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db.c().clone(), region);
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
        assert_eq!(reader.get_statistics().write.seek, 1);
        assert_eq!(reader.get_statistics().write.next, 0);

        // Should seek for another key.
        assert!(reader.seek_write(&k, 2.into()).unwrap().is_none());
        assert_eq!(reader.get_statistics().write.seek, 2);
        assert_eq!(reader.get_statistics().write.next, 0);

        // Test seek_write touches region's end.
        let region1 = make_region(1, vec![], Key::from_raw(b"k1").into_encoded());
        let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db.c().clone(), region1);
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

        let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db.c().clone(), region);
        let mut reader = MvccReader::new(snap, None, false, IsolationLevel::Si);

        // Let's assume `2_1 PUT` means a commit version with start ts is 1 and commit ts
        // is 2.
        // Commit versions: [21_17 LOCK, 20_18 PUT, 15_13 LOCK, 14_12 PUT, 9_8 DELETE, 7_6 LOCK,
        //                   5_5 Rollback, 2_1 PUT].
        let key = Key::from_raw(k);

        assert!(reader.get_write(&key, 1.into(), None).unwrap().is_none());

        let write = reader.get_write(&key, 2.into(), None).unwrap().unwrap();
        assert_eq!(write.write_type, WriteType::Put);
        assert_eq!(write.start_ts, 1.into());

        let write = reader.get_write(&key, 5.into(), None).unwrap().unwrap();
        assert_eq!(write.write_type, WriteType::Put);
        assert_eq!(write.start_ts, 1.into());

        let write = reader.get_write(&key, 7.into(), None).unwrap().unwrap();
        assert_eq!(write.write_type, WriteType::Put);
        assert_eq!(write.start_ts, 1.into());

        assert!(reader.get_write(&key, 9.into(), None).unwrap().is_none());

        let write = reader.get_write(&key, 14.into(), None).unwrap().unwrap();
        assert_eq!(write.write_type, WriteType::Put);
        assert_eq!(write.start_ts, 12.into());

        let write = reader.get_write(&key, 16.into(), None).unwrap().unwrap();
        assert_eq!(write.write_type, WriteType::Put);
        assert_eq!(write.start_ts, 12.into());

        let write = reader.get_write(&key, 20.into(), None).unwrap().unwrap();
        assert_eq!(write.write_type, WriteType::Put);
        assert_eq!(write.start_ts, 18.into());

        let write = reader.get_write(&key, 24.into(), None).unwrap().unwrap();
        assert_eq!(write.write_type, WriteType::Put);
        assert_eq!(write.start_ts, 18.into());

        assert!(reader
            .get_write(&Key::from_raw(b"j"), 100.into(), None)
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

        let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db.c().clone(), region.clone());
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
        let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db.c().clone(), region.clone());
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
        let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db.c().clone(), region);
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
        let check_scan_lock = |start_key: Option<Key>,
                               end_key: Option<Key>,
                               limit,
                               expect_res: &[_],
                               expect_is_remain| {
            let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db.c().clone(), region.clone());
            let mut reader = MvccReader::new(snap, None, false, IsolationLevel::Si);
            let res = reader
                .scan_locks(
                    start_key.as_ref(),
                    end_key.as_ref(),
                    |l| l.ts <= 10.into(),
                    limit,
                )
                .unwrap();
            assert_eq!(res.0, expect_res);
            assert_eq!(res.1, expect_is_remain);
        };

        check_scan_lock(None, None, 6, &visible_locks, false);
        check_scan_lock(None, None, 5, &visible_locks, true);
        check_scan_lock(None, None, 4, &visible_locks[0..4], true);
        check_scan_lock(
            Some(Key::from_raw(b"k2")),
            None,
            3,
            &visible_locks[1..4],
            true,
        );
        check_scan_lock(
            Some(Key::from_raw(b"k3\x00")),
            None,
            1,
            &visible_locks[3..4],
            true,
        );
        check_scan_lock(
            Some(Key::from_raw(b"k3\x00")),
            None,
            10,
            &visible_locks[3..],
            false,
        );
        // limit = 0 means unlimited.
        check_scan_lock(None, None, 0, &visible_locks, false);
        // Test scanning with limited end_key
        check_scan_lock(
            None,
            Some(Key::from_raw(b"k3")),
            0,
            &visible_locks[..2],
            false,
        );
        check_scan_lock(
            None,
            Some(Key::from_raw(b"k3\x00")),
            0,
            &visible_locks[..3],
            false,
        );
        check_scan_lock(
            None,
            Some(Key::from_raw(b"k3\x00")),
            3,
            &visible_locks[..3],
            true,
        );
        check_scan_lock(
            None,
            Some(Key::from_raw(b"k3\x00")),
            2,
            &visible_locks[..2],
            true,
        );
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

        let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db.c().clone(), region.clone());
        let mut reader = MvccReader::new(snap, None, false, IsolationLevel::Si);
        let key = Key::from_raw(k);

        for &skip_lock_check in &[false, true] {
            assert_eq!(
                reader.get(&key, 2.into(), None, skip_lock_check).unwrap(),
                Some(short_value.to_vec())
            );
            assert_eq!(
                reader.get(&key, 5.into(), None, skip_lock_check).unwrap(),
                Some(short_value.to_vec())
            );
            assert_eq!(
                reader.get(&key, 7.into(), None, skip_lock_check).unwrap(),
                Some(short_value.to_vec())
            );
            assert_eq!(
                reader.get(&key, 9.into(), None, skip_lock_check).unwrap(),
                None
            );

            assert!(reader.get(&key, 11.into(), None, false).is_err());
            assert_eq!(reader.get(&key, 9.into(), None, true).unwrap(), None);
        }

        // Commit the long value
        engine.commit(k, 10, 11);
        let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db.c().clone(), region);
        let mut reader = MvccReader::new(snap, None, false, IsolationLevel::Si);
        for &skip_lock_check in &[false, true] {
            assert_eq!(
                reader.get(&key, 11.into(), None, skip_lock_check).unwrap(),
                Some(long_value.to_vec())
            );
        }
    }
}
