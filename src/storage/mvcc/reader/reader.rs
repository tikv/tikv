// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE};
use kvproto::{
    errorpb::{self, EpochNotMatch, StaleCommand},
    kvrpcpb::Context,
};
use tikv_kv::SnapshotExt;
use txn_types::{Key, Lock, OldValue, TimeStamp, Value, Write, WriteRef, WriteType};

use crate::storage::{
    kv::{
        Cursor, CursorBuilder, Error as KvError, ScanMode, Snapshot as EngineSnapshot, Statistics,
    },
    mvcc::{
        default_not_found_error,
        reader::{OverlappedWrite, TxnCommitRecord},
        Result,
    },
};

/// Read from an MVCC snapshot, i.e., a logical view of the database at a specific timestamp (the
/// start_ts).
///
/// This represents the view of the database from a single transaction.
///
/// Confusingly, there are two meanings of the word 'snapshot' here. In the name of the struct,
/// 'snapshot' means an mvcc snapshot. In the type parameter bound (of `S`), 'snapshot' means a view
/// of the underlying storage engine at a given point in time. This latter snapshot will include
/// values for keys at multiple timestamps.
pub struct SnapshotReader<S: EngineSnapshot> {
    pub reader: MvccReader<S>,
    pub start_ts: TimeStamp,
}

impl<S: EngineSnapshot> SnapshotReader<S> {
    pub fn new(start_ts: TimeStamp, snapshot: S, fill_cache: bool) -> Self {
        SnapshotReader {
            reader: MvccReader::new(snapshot, None, fill_cache),
            start_ts,
        }
    }

    pub fn new_with_ctx(start_ts: TimeStamp, snapshot: S, ctx: &Context) -> Self {
        SnapshotReader {
            reader: MvccReader::new_with_ctx(snapshot, None, ctx),
            start_ts,
        }
    }

    #[inline(always)]
    pub fn get_txn_commit_record(&mut self, key: &Key) -> Result<TxnCommitRecord> {
        self.reader.get_txn_commit_record(key, self.start_ts)
    }

    #[inline(always)]
    pub fn load_lock(&mut self, key: &Key) -> Result<Option<Lock>> {
        self.reader.load_lock(key)
    }

    #[inline(always)]
    pub fn key_exist(&mut self, key: &Key, ts: TimeStamp) -> Result<bool> {
        Ok(self
            .reader
            .get_write(key, ts, Some(self.start_ts))?
            .is_some())
    }

    #[inline(always)]
    pub fn get(&mut self, key: &Key, ts: TimeStamp) -> Result<Option<Value>> {
        self.reader.get(key, ts, Some(self.start_ts))
    }

    #[inline(always)]
    pub fn get_write(&mut self, key: &Key, ts: TimeStamp) -> Result<Option<Write>> {
        self.reader.get_write(key, ts, Some(self.start_ts))
    }

    #[inline(always)]
    pub fn get_write_with_commit_ts(
        &mut self,
        key: &Key,
        ts: TimeStamp,
    ) -> Result<Option<(Write, TimeStamp)>> {
        self.reader
            .get_write_with_commit_ts(key, ts, Some(self.start_ts))
    }

    #[inline(always)]
    pub fn seek_write(&mut self, key: &Key, ts: TimeStamp) -> Result<Option<(TimeStamp, Write)>> {
        self.reader.seek_write(key, ts)
    }

    #[inline(always)]
    pub fn load_data(&mut self, key: &Key, write: Write) -> Result<Value> {
        self.reader.load_data(key, write)
    }

    #[inline(always)]
    pub fn get_old_value(
        &mut self,
        key: &Key,
        ts: TimeStamp,
        prev_write_loaded: bool,
        prev_write: Option<Write>,
    ) -> Result<OldValue> {
        self.reader
            .get_old_value(key, ts, prev_write_loaded, prev_write)
    }

    #[inline(always)]
    pub fn take_statistics(&mut self) -> Statistics {
        std::mem::take(&mut self.reader.statistics)
    }
}

pub struct MvccReader<S: EngineSnapshot> {
    snapshot: S,
    pub statistics: Statistics,
    // cursors are used for speeding up scans.
    data_cursor: Option<Cursor<S::Iter>>,
    lock_cursor: Option<Cursor<S::Iter>>,
    write_cursor: Option<Cursor<S::Iter>>,

    /// None means following operations are performed on a single user key, i.e.,
    /// different versions of the same key. It can use prefix seek to speed up reads
    /// from the write-cf.
    scan_mode: Option<ScanMode>,
    // Records the current key for prefix seek. Will Reset the write cursor when switching to another key.
    current_key: Option<Key>,

    fill_cache: bool,

    // The term and the epoch version when the snapshot is created. They will be zero
    // if the two properties are not available.
    term: u64,
    #[allow(dead_code)]
    version: u64,
}

impl<S: EngineSnapshot> MvccReader<S> {
    pub fn new(snapshot: S, scan_mode: Option<ScanMode>, fill_cache: bool) -> Self {
        Self {
            snapshot,
            statistics: Statistics::default(),
            data_cursor: None,
            lock_cursor: None,
            write_cursor: None,
            scan_mode,
            current_key: None,
            fill_cache,
            term: 0,
            version: 0,
        }
    }

    pub fn new_with_ctx(snapshot: S, scan_mode: Option<ScanMode>, ctx: &Context) -> Self {
        Self {
            snapshot,
            statistics: Statistics::default(),
            data_cursor: None,
            lock_cursor: None,
            write_cursor: None,
            scan_mode,
            current_key: None,
            fill_cache: !ctx.get_not_fill_cache(),
            term: ctx.get_term(),
            version: ctx.get_region_epoch().get_version(),
        }
    }

    /// load the value associated with `key` and pointed by `write`
    fn load_data(&mut self, key: &Key, write: Write) -> Result<Value> {
        assert_eq!(write.write_type, WriteType::Put);
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
        if let Some(pessimistic_lock) = self.load_in_memory_pessimistic_lock(key)? {
            return Ok(Some(pessimistic_lock));
        }

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

    fn load_in_memory_pessimistic_lock(&self, key: &Key) -> Result<Option<Lock>> {
        self.snapshot
            .ext()
            .get_txn_ext()
            .and_then(|txn_ext| {
                // If the term or region version has changed, do not read the lock table.
                // Instead, just return a StaleCommand or EpochNotMatch error, so the
                // client will not receive a false error because the lock table has been
                // cleared.
                let locks = txn_ext.pessimistic_locks.read();
                if self.term != 0 && locks.term != self.term {
                    let mut err = errorpb::Error::default();
                    err.set_stale_command(StaleCommand::default());
                    return Some(Err(KvError::from(err).into()));
                }
                if self.version != 0 && locks.version != self.version {
                    let mut err = errorpb::Error::default();
                    // We don't know the current regions. Just return an empty EpochNotMatch error.
                    err.set_epoch_not_match(EpochNotMatch::default());
                    return Some(Err(KvError::from(err).into()));
                }

                locks.get(key).map(|(lock, _)| {
                    // For write commands that are executed in serial, it should be impossible
                    // to read a deleted lock.
                    // For read commands in the scheduler, it should read the lock marked deleted
                    // because the lock is not actually deleted from the underlying storage.
                    Ok(lock.to_lock())
                })
            })
            .transpose()
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

    /// Gets the value of the specified key's latest version before specified `ts`.
    ///
    /// It tries to ensure the write record's `gc_fence`'s ts, if any, greater than specified
    /// `gc_fence_limit`. Pass `None` to `gc_fence_limit` to skip the check.
    /// The caller must guarantee that there's no other `PUT` or `DELETE` versions whose `commit_ts`
    /// is between the found version and the provided `gc_fence_limit` (`gc_fence_limit` is
    /// inclusive).
    ///
    /// For transactional reads, the `gc_fence_limit` must be provided to ensure the result is
    /// correct. Generally, it should be the read_ts of the current transaction, which might be
    /// different from the `ts` passed to this function.
    ///
    /// Note that this function does not check for locks on `key`.
    fn get(
        &mut self,
        key: &Key,
        ts: TimeStamp,
        gc_fence_limit: Option<TimeStamp>,
    ) -> Result<Option<Value>> {
        Ok(match self.get_write(key, ts, gc_fence_limit)? {
            Some(write) => Some(self.load_data(key, write)?),
            None => None,
        })
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
        ts: TimeStamp,
        gc_fence_limit: Option<TimeStamp>,
    ) -> Result<Option<Write>> {
        Ok(self
            .get_write_with_commit_ts(key, ts, gc_fence_limit)?
            .map(|(w, _)| w))
    }

    /// Gets the write record of the specified key's latest version before specified `ts`, and
    /// additionally the write record's `commit_ts`, if any.
    ///
    /// See also [`MvccReader::get_write`].
    pub fn get_write_with_commit_ts(
        &mut self,
        key: &Key,
        mut ts: TimeStamp,
        gc_fence_limit: Option<TimeStamp>,
    ) -> Result<Option<(Write, TimeStamp)>> {
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
                            return Ok(Some((write, commit_ts)));
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

    fn get_txn_commit_record(&mut self, key: &Key, start_ts: TimeStamp) -> Result<TxnCommitRecord> {
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
            Some(x) => cursor.seek(x, &mut self.statistics.lock)?,
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
        let mut cursor = CursorBuilder::new(&self.snapshot, CF_WRITE)
            .fill_cache(self.fill_cache)
            .scan_mode(self.get_scan_mode(false))
            .build()?;
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
                resource_metering::record_read_keys(keys.len() as u32);
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

    /// Read the old value for key for CDC.
    /// `prev_write` stands for the previous write record of the key
    /// it must be read in the caller and be passed in for optimization
    fn get_old_value(
        &mut self,
        key: &Key,
        start_ts: TimeStamp,
        prev_write_loaded: bool,
        prev_write: Option<Write>,
    ) -> Result<OldValue> {
        if prev_write_loaded && prev_write.is_none() {
            return Ok(OldValue::None);
        }
        if let Some(prev_write) = prev_write {
            if !prev_write
                .as_ref()
                .check_gc_fence_as_latest_version(start_ts)
            {
                return Ok(OldValue::None);
            }

            match prev_write.write_type {
                WriteType::Put => {
                    // For Put, there must be an old value either in its
                    // short value or in the default CF.
                    return Ok(match prev_write.short_value {
                        Some(value) => OldValue::Value { value },
                        None => OldValue::ValueTimeStamp {
                            start_ts: prev_write.start_ts,
                        },
                    });
                }
                WriteType::Delete => {
                    // For Delete, no old value.
                    return Ok(OldValue::None);
                }
                // For Rollback and Lock, it's unknown whether there is a more
                // previous valid write. Call `get_write` to get a valid
                // previous write.
                WriteType::Rollback | WriteType::Lock => (),
            }
        }
        Ok(match self.get_write(key, start_ts, Some(start_ts))? {
            Some(write) => match write.short_value {
                Some(value) => OldValue::Value { value },
                None => OldValue::ValueTimeStamp {
                    start_ts: write.start_ts,
                },
            },
            None => OldValue::None,
        })
    }
}

#[cfg(test)]
pub mod tests {
    use std::{ops::Bound, sync::Arc, u64};

    use concurrency_manager::ConcurrencyManager;
    use engine_rocks::{
        properties::MvccPropertiesCollectorFactory,
        raw::{ColumnFamilyOptions, DBOptions, DB},
        raw_util::CFOptions,
        Compat, RocksSnapshot,
    };
    use engine_traits::{
        IterOptions, Mutable, WriteBatch, WriteBatchExt, ALL_CFS, CF_DEFAULT, CF_LOCK, CF_RAFT,
        CF_WRITE,
    };
    use kvproto::{
        kvrpcpb::{AssertionLevel, Context},
        metapb::{Peer, Region},
    };
    use raftstore::store::RegionSnapshot;
    use txn_types::{LockType, Mutation};

    use super::*;
    use crate::storage::{
        kv::Modify,
        mvcc::{tests::write, MvccReader, MvccTxn},
        txn::{
            acquire_pessimistic_lock, cleanup, commit, gc, prewrite, CommitKind, TransactionKind,
            TransactionProperties,
        },
        Engine, TestEngineBuilder,
    };

    pub struct RegionEngine {
        db: Arc<DB>,
        region: Region,
    }

    impl RegionEngine {
        pub fn new(db: &Arc<DB>, region: &Region) -> RegionEngine {
            RegionEngine {
                db: Arc::clone(db),
                region: region.clone(),
            }
        }

        pub fn snapshot(&self) -> RegionSnapshot<RocksSnapshot> {
            let db = self.db.c().clone();
            RegionSnapshot::<RocksSnapshot>::from_raw(db, self.region.clone())
        }

        pub fn put(
            &mut self,
            pk: &[u8],
            start_ts: impl Into<TimeStamp>,
            commit_ts: impl Into<TimeStamp>,
        ) {
            let start_ts = start_ts.into();
            let m = Mutation::make_put(Key::from_raw(pk), vec![]);
            self.prewrite(m, pk, start_ts);
            self.commit(pk, start_ts, commit_ts);
        }

        pub fn lock(
            &mut self,
            pk: &[u8],
            start_ts: impl Into<TimeStamp>,
            commit_ts: impl Into<TimeStamp>,
        ) {
            let start_ts = start_ts.into();
            let m = Mutation::make_lock(Key::from_raw(pk));
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
            let m = Mutation::make_delete(Key::from_raw(pk));
            self.prewrite(m, pk, start_ts);
            self.commit(pk, start_ts, commit_ts);
        }

        pub fn txn_props(
            start_ts: TimeStamp,
            primary: &[u8],
            pessimistic: bool,
        ) -> TransactionProperties<'_> {
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
                is_retry_request: false,
                assertion_level: AssertionLevel::Off,
            }
        }

        pub fn prewrite(&mut self, m: Mutation, pk: &[u8], start_ts: impl Into<TimeStamp>) {
            let snap = self.snapshot();
            let start_ts = start_ts.into();
            let cm = ConcurrencyManager::new(start_ts);
            let mut txn = MvccTxn::new(start_ts, cm);
            let mut reader = SnapshotReader::new(start_ts, snap, true);

            prewrite(
                &mut txn,
                &mut reader,
                &Self::txn_props(start_ts, pk, false),
                m,
                &None,
                false,
            )
            .unwrap();
            self.write(txn.into_modifies());
        }

        pub fn prewrite_pessimistic_lock(
            &mut self,
            m: Mutation,
            pk: &[u8],
            start_ts: impl Into<TimeStamp>,
        ) {
            let snap = self.snapshot();
            let start_ts = start_ts.into();
            let cm = ConcurrencyManager::new(start_ts);
            let mut txn = MvccTxn::new(start_ts, cm);
            let mut reader = SnapshotReader::new(start_ts, snap, true);

            prewrite(
                &mut txn,
                &mut reader,
                &Self::txn_props(start_ts, pk, true),
                m,
                &None,
                true,
            )
            .unwrap();
            self.write(txn.into_modifies());
        }

        pub fn acquire_pessimistic_lock(
            &mut self,
            k: Key,
            pk: &[u8],
            start_ts: impl Into<TimeStamp>,
            for_update_ts: impl Into<TimeStamp>,
        ) {
            let snap = self.snapshot();
            let for_update_ts = for_update_ts.into();
            let cm = ConcurrencyManager::new(for_update_ts);
            let start_ts = start_ts.into();
            let mut txn = MvccTxn::new(start_ts, cm);
            let mut reader = SnapshotReader::new(start_ts, snap, true);
            acquire_pessimistic_lock(
                &mut txn,
                &mut reader,
                k,
                pk,
                false,
                0,
                for_update_ts,
                false,
                false,
                TimeStamp::zero(),
                true,
            )
            .unwrap();
            self.write(txn.into_modifies());
        }

        pub fn commit(
            &mut self,
            pk: &[u8],
            start_ts: impl Into<TimeStamp>,
            commit_ts: impl Into<TimeStamp>,
        ) {
            let snap = self.snapshot();
            let start_ts = start_ts.into();
            let cm = ConcurrencyManager::new(start_ts);
            let mut txn = MvccTxn::new(start_ts, cm);
            let mut reader = SnapshotReader::new(start_ts, snap, true);
            commit(&mut txn, &mut reader, Key::from_raw(pk), commit_ts.into()).unwrap();
            self.write(txn.into_modifies());
        }

        pub fn rollback(&mut self, pk: &[u8], start_ts: impl Into<TimeStamp>) {
            let snap = self.snapshot();
            let start_ts = start_ts.into();
            let cm = ConcurrencyManager::new(start_ts);
            let mut txn = MvccTxn::new(start_ts, cm);
            let mut reader = SnapshotReader::new(start_ts, snap, true);
            cleanup(
                &mut txn,
                &mut reader,
                Key::from_raw(pk),
                TimeStamp::zero(),
                true,
            )
            .unwrap();
            self.write(txn.into_modifies());
        }

        pub fn gc(&mut self, pk: &[u8], safe_point: impl Into<TimeStamp> + Copy) {
            let cm = ConcurrencyManager::new(safe_point.into());
            loop {
                let snap = self.snapshot();
                let mut txn = MvccTxn::new(safe_point.into(), cm.clone());
                let mut reader = MvccReader::new(snap, None, true);
                gc(&mut txn, &mut reader, Key::from_raw(pk), safe_point.into()).unwrap();
                let modifies = txn.into_modifies();
                if modifies.is_empty() {
                    return;
                }
                self.write(modifies);
            }
        }

        pub fn write(&mut self, modifies: Vec<Modify>) {
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
                    Modify::PessimisticLock(k, lock) => {
                        let k = keys::data_key(k.as_encoded());
                        let v = lock.into_lock().to_bytes();
                        wb.put_cf(CF_LOCK, &k, &v).unwrap();
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

        pub fn flush(&mut self) {
            for cf in ALL_CFS {
                let cf = engine_rocks::util::get_cf_handle(&self.db, cf).unwrap();
                self.db.flush_cf(cf, true).unwrap();
            }
        }

        pub fn compact(&mut self) {
            for cf in ALL_CFS {
                let cf = engine_rocks::util::get_cf_handle(&self.db, cf).unwrap();
                self.db.compact_range_cf(cf, None, None);
            }
        }
    }

    pub fn open_db(path: &str, with_properties: bool) -> Arc<DB> {
        let db_opts = DBOptions::new();
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_write_buffer_size(32 * 1024 * 1024);
        if with_properties {
            cf_opts.add_table_properties_collector_factory(
                "tikv.test-collector",
                MvccPropertiesCollectorFactory::default(),
            );
        }
        let cfs_opts = vec![
            CFOptions::new(CF_DEFAULT, ColumnFamilyOptions::new()),
            CFOptions::new(CF_RAFT, ColumnFamilyOptions::new()),
            CFOptions::new(CF_LOCK, ColumnFamilyOptions::new()),
            CFOptions::new(CF_WRITE, cf_opts),
        ];
        Arc::new(engine_rocks::raw_util::new_engine_opt(path, db_opts, cfs_opts).unwrap())
    }

    pub fn make_region(id: u64, start_key: Vec<u8>, end_key: Vec<u8>) -> Region {
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

        let db = open_db(path, true);
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
        let m = Mutation::make_put(Key::from_raw(k), v.to_vec());
        engine.prewrite(m, k, 1);
        engine.commit(k, 1, 10);

        engine.rollback(k, 5);
        engine.rollback(k, 20);

        let m = Mutation::make_put(Key::from_raw(k), v.to_vec());
        engine.prewrite(m, k, 25);
        engine.commit(k, 25, 30);

        let m = Mutation::make_put(Key::from_raw(k), v.to_vec());
        engine.prewrite(m, k, 35);
        engine.commit(k, 35, 40);

        // Overlapped rollback on the commit record at 40.
        engine.rollback(k, 40);

        let m = Mutation::make_put(Key::from_raw(k), v.to_vec());
        engine.acquire_pessimistic_lock(Key::from_raw(k), k, 45, 45);
        engine.prewrite_pessimistic_lock(m, k, 45);
        engine.commit(k, 45, 50);

        let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db.c().clone(), region);
        let mut reader = MvccReader::new(snap, None, false);

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

        let seek_old = reader.statistics.write.seek;
        let next_old = reader.statistics.write.next;
        assert!(
            !reader
                .get_txn_commit_record(&key, 30.into())
                .unwrap()
                .exist()
        );
        let seek_new = reader.statistics.write.seek;
        let next_new = reader.statistics.write.next;

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
        let m = Mutation::make_put(key.clone(), v.to_vec());

        // txn: start_ts = 2, commit_ts = 3
        engine.acquire_pessimistic_lock(key.clone(), k, 2, 2);
        engine.prewrite_pessimistic_lock(m.clone(), k, 2);
        engine.commit(k, 2, 3);
        // txn: start_ts = 1, commit_ts = 4
        engine.acquire_pessimistic_lock(key.clone(), k, 1, 3);
        engine.prewrite_pessimistic_lock(m, k, 1);
        engine.commit(k, 1, 4);

        let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db.c().clone(), region);
        let mut reader = MvccReader::new(snap, None, false);

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
        let m = Mutation::make_put(Key::from_raw(k), v.to_vec());
        engine.prewrite(m.clone(), k, 1);
        engine.commit(k, 1, 5);

        engine.write(vec![
            Modify::Put(
                CF_WRITE,
                Key::from_raw(k).append_ts(TimeStamp::new(3)),
                vec![b'R', 3],
            ),
            Modify::Put(
                CF_WRITE,
                Key::from_raw(k).append_ts(TimeStamp::new(7)),
                vec![b'R', 7],
            ),
        ]);

        engine.prewrite(m.clone(), k, 15);
        engine.commit(k, 15, 17);

        // Timestamp overlap with the previous transaction.
        engine.acquire_pessimistic_lock(Key::from_raw(k), k, 10, 18);
        engine.prewrite_pessimistic_lock(Mutation::make_lock(Key::from_raw(k)), k, 10);
        engine.commit(k, 10, 20);

        engine.prewrite(m, k, 23);
        engine.commit(k, 23, 25);

        // Let's assume `2_1 PUT` means a commit version with start ts is 1 and commit ts
        // is 2.
        // Commit versions: [25_23 PUT, 20_10 PUT, 17_15 PUT, 7_7 Rollback, 5_1 PUT, 3_3 Rollback].
        let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db.c().clone(), region.clone());
        let mut reader = MvccReader::new(snap, None, false);

        let k = Key::from_raw(k);
        let (commit_ts, write) = reader.seek_write(&k, 30.into()).unwrap().unwrap();
        assert_eq!(commit_ts, 25.into());
        assert_eq!(
            write,
            Write::new(WriteType::Put, 23.into(), Some(v.to_vec()))
        );
        assert_eq!(reader.statistics.write.seek, 1);
        assert_eq!(reader.statistics.write.next, 0);

        let (commit_ts, write) = reader.seek_write(&k, 25.into()).unwrap().unwrap();
        assert_eq!(commit_ts, 25.into());
        assert_eq!(
            write,
            Write::new(WriteType::Put, 23.into(), Some(v.to_vec()))
        );
        assert_eq!(reader.statistics.write.seek, 1);
        assert_eq!(reader.statistics.write.next, 0);

        let (commit_ts, write) = reader.seek_write(&k, 20.into()).unwrap().unwrap();
        assert_eq!(commit_ts, 20.into());
        assert_eq!(write, Write::new(WriteType::Lock, 10.into(), None));
        assert_eq!(reader.statistics.write.seek, 1);
        assert_eq!(reader.statistics.write.next, 1);

        let (commit_ts, write) = reader.seek_write(&k, 19.into()).unwrap().unwrap();
        assert_eq!(commit_ts, 17.into());
        assert_eq!(
            write,
            Write::new(WriteType::Put, 15.into(), Some(v.to_vec()))
        );
        assert_eq!(reader.statistics.write.seek, 1);
        assert_eq!(reader.statistics.write.next, 2);

        let (commit_ts, write) = reader.seek_write(&k, 3.into()).unwrap().unwrap();
        assert_eq!(commit_ts, 3.into());
        assert_eq!(write, Write::new_rollback(3.into(), false));
        assert_eq!(reader.statistics.write.seek, 1);
        assert_eq!(reader.statistics.write.next, 5);

        let (commit_ts, write) = reader.seek_write(&k, 16.into()).unwrap().unwrap();
        assert_eq!(commit_ts, 7.into());
        assert_eq!(write, Write::new_rollback(7.into(), false));
        assert_eq!(reader.statistics.write.seek, 1);
        assert_eq!(reader.statistics.write.next, 6);
        assert_eq!(reader.statistics.write.prev, 3);

        let (commit_ts, write) = reader.seek_write(&k, 6.into()).unwrap().unwrap();
        assert_eq!(commit_ts, 5.into());
        assert_eq!(
            write,
            Write::new(WriteType::Put, 1.into(), Some(v.to_vec()))
        );
        assert_eq!(reader.statistics.write.seek, 1);
        assert_eq!(reader.statistics.write.next, 7);
        assert_eq!(reader.statistics.write.prev, 3);

        assert!(reader.seek_write(&k, 2.into()).unwrap().is_none());
        assert_eq!(reader.statistics.write.seek, 1);
        assert_eq!(reader.statistics.write.next, 9);
        assert_eq!(reader.statistics.write.prev, 3);

        // Test seek_write should not see the next key.
        let (k2, v2) = (b"k2", b"v2");
        let m2 = Mutation::make_put(Key::from_raw(k2), v2.to_vec());
        engine.prewrite(m2, k2, 1);
        engine.commit(k2, 1, 2);

        let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db.c().clone(), region);
        let mut reader = MvccReader::new(snap, None, false);

        let (commit_ts, write) = reader
            .seek_write(&Key::from_raw(k2), 3.into())
            .unwrap()
            .unwrap();
        assert_eq!(commit_ts, 2.into());
        assert_eq!(
            write,
            Write::new(WriteType::Put, 1.into(), Some(v2.to_vec()))
        );
        assert_eq!(reader.statistics.write.seek, 1);
        assert_eq!(reader.statistics.write.next, 0);

        // Should seek for another key.
        assert!(reader.seek_write(&k, 2.into()).unwrap().is_none());
        assert_eq!(reader.statistics.write.seek, 2);
        assert_eq!(reader.statistics.write.next, 0);

        // Test seek_write touches region's end.
        let region1 = make_region(1, vec![], Key::from_raw(b"k1").into_encoded());
        let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db.c().clone(), region1);
        let mut reader = MvccReader::new(snap, None, false);

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
        let m = Mutation::make_put(Key::from_raw(k), v.to_vec());
        engine.prewrite(m, k, 1);
        engine.commit(k, 1, 2);

        engine.rollback(k, 5);

        engine.lock(k, 6, 7);

        engine.delete(k, 8, 9);

        let m = Mutation::make_put(Key::from_raw(k), v.to_vec());
        engine.prewrite(m, k, 12);
        engine.commit(k, 12, 14);

        let m = Mutation::make_lock(Key::from_raw(k));
        engine.acquire_pessimistic_lock(Key::from_raw(k), k, 13, 15);
        engine.prewrite_pessimistic_lock(m, k, 13);
        engine.commit(k, 13, 15);

        let m = Mutation::make_put(Key::from_raw(k), v.to_vec());
        engine.acquire_pessimistic_lock(Key::from_raw(k), k, 18, 18);
        engine.prewrite_pessimistic_lock(m, k, 18);
        engine.commit(k, 18, 20);

        let m = Mutation::make_lock(Key::from_raw(k));
        engine.acquire_pessimistic_lock(Key::from_raw(k), k, 17, 21);
        engine.prewrite_pessimistic_lock(m, k, 17);
        engine.commit(k, 17, 21);

        let m = Mutation::make_put(Key::from_raw(k), v.to_vec());
        engine.prewrite(m, k, 24);

        let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db.c().clone(), region);
        let mut reader = MvccReader::new(snap, None, false);

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

        assert!(
            reader
                .get_write(&Key::from_raw(b"j"), 100.into(), None)
                .unwrap()
                .is_none()
        );
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
            Mutation::make_put(Key::from_raw(b"k1"), b"v1".to_vec()),
            b"k1",
            5,
        );
        engine.prewrite(
            Mutation::make_put(Key::from_raw(b"k2"), b"v2".to_vec()),
            b"k1",
            10,
        );
        engine.prewrite(Mutation::make_delete(Key::from_raw(b"k3")), b"k1", 10);
        engine.prewrite(Mutation::make_lock(Key::from_raw(b"k3\x00")), b"k1", 10);
        engine.prewrite(Mutation::make_delete(Key::from_raw(b"k4")), b"k1", 12);
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
            let mut reader = MvccReader::new(snap, None, false);
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
    fn test_load_data() {
        let path = tempfile::Builder::new()
            .prefix("_test_storage_mvcc_reader_load_data")
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

        struct Case {
            expected: Result<Value>,

            // modifies to put into the engine
            modifies: Vec<Modify>,
            // these are used to construct the mvcc reader
            scan_mode: Option<ScanMode>,
            key: Key,
            write: Write,
        }

        let cases = vec![
            Case {
                // write has short_value
                expected: Ok(short_value.to_vec()),

                modifies: vec![Modify::Put(
                    CF_DEFAULT,
                    Key::from_raw(k).append_ts(TimeStamp::new(1)),
                    vec![],
                )],
                scan_mode: None,
                key: Key::from_raw(k),
                write: Write::new(
                    WriteType::Put,
                    TimeStamp::new(1),
                    Some(short_value.to_vec()),
                ),
            },
            Case {
                // write has no short_value, the reader has a cursor, got something
                expected: Ok(long_value.to_vec()),
                modifies: vec![Modify::Put(
                    CF_DEFAULT,
                    Key::from_raw(k).append_ts(TimeStamp::new(2)),
                    long_value.to_vec(),
                )],
                scan_mode: Some(ScanMode::Forward),
                key: Key::from_raw(k),
                write: Write::new(WriteType::Put, TimeStamp::new(2), None),
            },
            Case {
                // write has no short_value, the reader has a cursor, got nothing
                expected: Err(default_not_found_error(k.to_vec(), "get")),
                modifies: vec![Modify::Put(
                    CF_WRITE,
                    Key::from_raw(k).append_ts(TimeStamp::new(1)),
                    Write::new(WriteType::Put, TimeStamp::new(1), None)
                        .as_ref()
                        .to_bytes(),
                )],
                scan_mode: Some(ScanMode::Forward),
                key: Key::from_raw(k),
                write: Write::new(WriteType::Put, TimeStamp::new(3), None),
            },
            Case {
                // write has no short_value, the reader has no cursor, got something
                expected: Ok(long_value.to_vec()),
                modifies: vec![Modify::Put(
                    CF_DEFAULT,
                    Key::from_raw(k).append_ts(TimeStamp::new(4)),
                    long_value.to_vec(),
                )],
                scan_mode: None,
                key: Key::from_raw(k),
                write: Write::new(WriteType::Put, TimeStamp::new(4), None),
            },
            Case {
                // write has no short_value, the reader has no cursor, got nothing
                expected: Err(default_not_found_error(k.to_vec(), "get")),
                modifies: vec![],
                scan_mode: None,
                key: Key::from_raw(k),
                write: Write::new(WriteType::Put, TimeStamp::new(5), None),
            },
        ];

        for case in cases {
            engine.write(case.modifies);
            let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db.c().clone(), region.clone());
            let mut reader = MvccReader::new(snap, case.scan_mode, false);
            let result = reader.load_data(&case.key, case.write);
            assert_eq!(format!("{:?}", result), format!("{:?}", case.expected));
        }
    }

    #[test]
    fn test_get() {
        let path = tempfile::Builder::new()
            .prefix("_test_storage_mvcc_reader_get")
            .tempdir()
            .unwrap();
        let path = path.path().to_str().unwrap();
        let region = make_region(1, vec![], vec![]);
        let db = open_db(path, true);
        let mut engine = RegionEngine::new(&db, &region);

        let (k, long_value) = (
            b"k",
            "v".repeat(txn_types::SHORT_VALUE_MAX_LEN + 1).into_bytes(),
        );

        struct Case {
            expected: Result<Option<Value>>,
            // modifies to put into the engine
            modifies: Vec<Modify>,
            // arguments to do the function call
            key: Key,
            ts: TimeStamp,
            gc_fence_limit: Option<TimeStamp>,
        }

        let cases = vec![
            Case {
                // no write for `key` at `ts` exists
                expected: Ok(None),
                modifies: vec![Modify::Delete(
                    CF_DEFAULT,
                    Key::from_raw(k).append_ts(TimeStamp::new(1)),
                )],
                key: Key::from_raw(k),
                ts: TimeStamp::new(1),
                gc_fence_limit: None,
            },
            Case {
                // some write for `key` at `ts` exists, load data return Err
                // todo: "some write for `key` at `ts` exists" should be checked by `test_get_write`
                // "load data return Err" is checked by test_load_data
                expected: Err(default_not_found_error(k.to_vec(), "get")),
                modifies: vec![Modify::Put(
                    CF_WRITE,
                    Key::from_raw(k).append_ts(TimeStamp::new(2)),
                    Write::new(WriteType::Put, TimeStamp::new(2), None)
                        .as_ref()
                        .to_bytes(),
                )],
                key: Key::from_raw(k),
                ts: TimeStamp::new(3),
                gc_fence_limit: None,
            },
            Case {
                // some write for `key` at `ts` exists, load data success
                // todo: "some write for `key` at `ts` exists" should be checked by `test_get_write`
                // "load data success" is checked by test_load_data
                expected: Ok(Some(long_value.to_vec())),
                modifies: vec![
                    Modify::Put(
                        CF_WRITE,
                        Key::from_raw(k).append_ts(TimeStamp::new(4)),
                        Write::new(WriteType::Put, TimeStamp::new(4), None)
                            .as_ref()
                            .to_bytes(),
                    ),
                    Modify::Put(
                        CF_DEFAULT,
                        Key::from_raw(k).append_ts(TimeStamp::new(4)),
                        long_value,
                    ),
                ],
                key: Key::from_raw(k),
                ts: TimeStamp::new(5),
                gc_fence_limit: None,
            },
        ];

        for case in cases {
            engine.write(case.modifies);
            let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db.c().clone(), region.clone());
            let mut reader = MvccReader::new(snap, None, false);
            let result = reader.get(&case.key, case.ts, case.gc_fence_limit);
            assert_eq!(format!("{:?}", result), format!("{:?}", case.expected));
        }
    }

    #[test]
    fn test_get_old_value() {
        struct Case {
            expected: OldValue,

            // (write_record, put_ts)
            // all data to write to the engine
            // current write_cursor will be on the last record in `written`
            // which also means prev_write is `Write` in the record
            written: Vec<(Write, TimeStamp)>,
        }
        let cases = vec![
            // prev_write is None
            Case {
                expected: OldValue::None,
                written: vec![],
            },
            // prev_write is Rollback, and there exists a more previous valid write
            Case {
                expected: OldValue::ValueTimeStamp {
                    start_ts: TimeStamp::new(4),
                },

                written: vec![
                    (
                        Write::new(WriteType::Put, TimeStamp::new(4), None),
                        TimeStamp::new(6),
                    ),
                    (
                        Write::new(WriteType::Rollback, TimeStamp::new(5), None),
                        TimeStamp::new(7),
                    ),
                ],
            },
            Case {
                expected: OldValue::Value {
                    value: b"v".to_vec(),
                },

                written: vec![
                    (
                        Write::new(WriteType::Put, TimeStamp::new(4), Some(b"v".to_vec())),
                        TimeStamp::new(6),
                    ),
                    (
                        Write::new(WriteType::Rollback, TimeStamp::new(5), None),
                        TimeStamp::new(7),
                    ),
                ],
            },
            // prev_write is Rollback, and there isn't a more previous valid write
            Case {
                expected: OldValue::None,
                written: vec![(
                    Write::new(WriteType::Rollback, TimeStamp::new(5), None),
                    TimeStamp::new(6),
                )],
            },
            // prev_write is Lock, and there exists a more previous valid write
            Case {
                expected: OldValue::ValueTimeStamp {
                    start_ts: TimeStamp::new(3),
                },

                written: vec![
                    (
                        Write::new(WriteType::Put, TimeStamp::new(3), None),
                        TimeStamp::new(6),
                    ),
                    (
                        Write::new(WriteType::Lock, TimeStamp::new(5), None),
                        TimeStamp::new(7),
                    ),
                ],
            },
            // prev_write is Lock, and there isn't a more previous valid write
            Case {
                expected: OldValue::None,
                written: vec![(
                    Write::new(WriteType::Lock, TimeStamp::new(5), None),
                    TimeStamp::new(6),
                )],
            },
            // prev_write is not Rollback or Lock, check_gc_fence_as_latest_version is true
            Case {
                expected: OldValue::ValueTimeStamp {
                    start_ts: TimeStamp::new(7),
                },
                written: vec![(
                    Write::new(WriteType::Put, TimeStamp::new(7), None)
                        .set_overlapped_rollback(true, Some(27.into())),
                    TimeStamp::new(5),
                )],
            },
            // prev_write is not Rollback or Lock, check_gc_fence_as_latest_version is false
            Case {
                expected: OldValue::None,
                written: vec![(
                    Write::new(WriteType::Put, TimeStamp::new(4), None)
                        .set_overlapped_rollback(true, Some(3.into())),
                    TimeStamp::new(5),
                )],
            },
            // prev_write is Delete, check_gc_fence_as_latest_version is true
            Case {
                expected: OldValue::None,
                written: vec![
                    (
                        Write::new(WriteType::Put, TimeStamp::new(3), None),
                        TimeStamp::new(6),
                    ),
                    (
                        Write::new(WriteType::Delete, TimeStamp::new(7), None),
                        TimeStamp::new(8),
                    ),
                ],
            },
            // prev_write is Delete, check_gc_fence_as_latest_version is false
            Case {
                expected: OldValue::None,
                written: vec![
                    (
                        Write::new(WriteType::Put, TimeStamp::new(3), None),
                        TimeStamp::new(6),
                    ),
                    (
                        Write::new(WriteType::Delete, TimeStamp::new(7), None)
                            .set_overlapped_rollback(true, Some(6.into())),
                        TimeStamp::new(8),
                    ),
                ],
            },
        ];
        for (i, case) in cases.into_iter().enumerate() {
            let engine = TestEngineBuilder::new().build().unwrap();
            let cm = ConcurrencyManager::new(42.into());
            let mut txn = MvccTxn::new(TimeStamp::new(10), cm.clone());
            for (write_record, put_ts) in case.written.iter() {
                txn.put_write(
                    Key::from_raw(b"a"),
                    *put_ts,
                    write_record.as_ref().to_bytes(),
                );
            }
            write(&engine, &Context::default(), txn.into_modifies());
            let snapshot = engine.snapshot(Default::default()).unwrap();
            let mut reader = MvccReader::new(snapshot, None, true);
            if !case.written.is_empty() {
                let prev_write = reader
                    .seek_write(&Key::from_raw(b"a"), case.written.last().unwrap().1)
                    .unwrap()
                    .map(|w| w.1);
                let prev_write_loaded = true;
                let result = reader
                    .get_old_value(
                        &Key::from_raw(b"a"),
                        TimeStamp::new(25),
                        prev_write_loaded,
                        prev_write,
                    )
                    .unwrap();
                assert_eq!(result, case.expected, "case #{}", i);
            }
        }

        // Must return Oldvalue::None when prev_write_loaded is true and prev_write is None.
        let engine = TestEngineBuilder::new().build().unwrap();
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true);
        let prev_write_loaded = true;
        let prev_write = None;
        let result = reader
            .get_old_value(
                &Key::from_raw(b"a"),
                TimeStamp::new(25),
                prev_write_loaded,
                prev_write,
            )
            .unwrap();
        assert_eq!(result, OldValue::None);
    }

    #[test]
    fn test_reader_prefix_seek() {
        let dir = tempfile::TempDir::new().unwrap();
        let builder = TestEngineBuilder::new().path(dir.path());
        let db = builder.build().unwrap().kv_engine().get_sync_db();
        let cf = engine_rocks::util::get_cf_handle(&db, CF_WRITE).unwrap();

        let region = make_region(1, vec![], vec![]);
        let mut engine = RegionEngine::new(&db, &region);

        // Put some tombstones into the DB.
        for i in 1..100 {
            let commit_ts = (i * 2 + 1).into();
            let mut k = vec![b'z'];
            k.extend_from_slice(Key::from_raw(b"k1").append_ts(commit_ts).as_encoded());
            use engine_rocks::raw::Writable;
            engine.db.delete_cf(cf, &k).unwrap();
        }
        engine.flush();

        #[allow(clippy::useless_vec)]
        for (k, scan_mode, tombstones) in vec![
            (b"k0", Some(ScanMode::Forward), 99),
            (b"k0", None, 0),
            (b"k1", Some(ScanMode::Forward), 99),
            (b"k1", None, 99),
            (b"k2", Some(ScanMode::Forward), 0),
            (b"k2", None, 0),
        ] {
            let mut reader = MvccReader::new(engine.snapshot(), scan_mode, false);
            let (k, ts) = (Key::from_raw(k), 199.into());
            reader.seek_write(&k, ts).unwrap();
            assert_eq!(reader.statistics.write.seek_tombstone, tombstones);
        }
    }
}
