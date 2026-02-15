// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::ops::Bound;

use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE};
use kvproto::{
    errorpb::{self, EpochNotMatch, FlashbackInProgress, StaleCommand},
    kvrpcpb::Context,
};
use raftstore::store::{LocksStatus, PeerPessimisticLocks};
use tikv_kv::{SEEK_BOUND, SnapshotExt};
use tikv_util::{Either, time::Instant};
use txn_types::{
    Key, LastChange, Lock, LockOrSharedLocks, OldValue, PessimisticLock, TimeStamp, TxnLockRef,
    Value, Write, WriteRef, WriteType,
};

use crate::storage::{
    kv::{
        Cursor, CursorBuilder, Error as KvError, ScanMode, Snapshot as EngineSnapshot, Statistics,
    },
    mvcc::{
        Result, default_not_found_error,
        metrics::{SCAN_LOCK_READ_TIME_VEC, ScanLockReadTimeSource},
        reader::{OverlappedWrite, TxnCommitRecord},
    },
};

/// Read from an MVCC snapshot, i.e., a logical view of the database at a
/// specific timestamp (the start_ts).
///
/// This represents the view of the database from a single transaction.
///
/// Confusingly, there are two meanings of the word 'snapshot' here. In the name
/// of the struct, 'snapshot' means an mvcc snapshot. In the type parameter
/// bound (of `S`), 'snapshot' means a view of the underlying storage engine at
/// a given point in time. This latter snapshot will include values for keys at
/// multiple timestamps.
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
    pub fn load_lock(&mut self, key: &Key) -> Result<Option<LockOrSharedLocks>> {
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

    lower_bound: Option<Key>,
    upper_bound: Option<Key>,

    hint_min_ts: Option<Bound<TimeStamp>>,

    /// None means following operations are performed on a single user key,
    /// i.e., different versions of the same key. It can use prefix seek to
    /// speed up reads from the write-cf.
    scan_mode: Option<ScanMode>,
    // Records the current key for prefix seek. Will Reset the write cursor when switching to
    // another key.
    current_key: Option<Key>,

    fill_cache: bool,

    // The term and the epoch version when the snapshot is created. They will be zero
    // if the two properties are not available.
    term: u64,
    #[allow(dead_code)]
    version: u64,

    allow_in_flashback: bool,
}

impl<S: EngineSnapshot> MvccReader<S> {
    pub fn new(snapshot: S, scan_mode: Option<ScanMode>, fill_cache: bool) -> Self {
        Self {
            snapshot,
            statistics: Statistics::default(),
            data_cursor: None,
            lock_cursor: None,
            write_cursor: None,
            lower_bound: None,
            upper_bound: None,
            hint_min_ts: None,
            scan_mode,
            current_key: None,
            fill_cache,
            term: 0,
            version: 0,
            allow_in_flashback: false,
        }
    }

    pub fn new_with_ctx(snapshot: S, scan_mode: Option<ScanMode>, ctx: &Context) -> Self {
        Self {
            snapshot,
            statistics: Statistics::default(),
            data_cursor: None,
            lock_cursor: None,
            write_cursor: None,
            lower_bound: None,
            upper_bound: None,
            hint_min_ts: None,
            scan_mode,
            current_key: None,
            fill_cache: !ctx.get_not_fill_cache(),
            term: ctx.get_term(),
            version: ctx.get_region_epoch().get_version(),
            allow_in_flashback: false,
        }
    }

    /// get the value of a user key with the given `start_ts`.
    pub fn get_value(&mut self, key: &Key, start_ts: TimeStamp) -> Result<Option<Value>> {
        if self.scan_mode.is_some() {
            self.create_data_cursor()?;
        }
        let k = key.clone().append_ts(start_ts);
        let val = if let Some(ref mut cursor) = self.data_cursor {
            cursor
                .get(&k, &mut self.statistics.data)?
                .map(|v| v.to_vec())
        } else {
            self.statistics.data.get += 1;
            self.snapshot.get(&k)?
        };
        if val.is_some() {
            self.statistics.data.processed_keys += 1;
        }
        Ok(val)
    }

    /// load the value associated with `key` and pointed by `write`
    pub fn load_data(&mut self, key: &Key, write: Write) -> Result<Value> {
        assert_eq!(write.write_type, WriteType::Put);
        if let Some(val) = write.short_value {
            return Ok(val);
        }
        let start_ts = write.start_ts;
        match self.get_value(key, start_ts)? {
            Some(val) => Ok(val),
            None => Err(default_not_found_error(
                key.clone().append_ts(start_ts).into_encoded(),
                "get",
            )),
        }
    }

    pub fn load_lock(&mut self, key: &Key) -> Result<Option<LockOrSharedLocks>> {
        if let Some(pessimistic_lock) = self.load_in_memory_pessimistic_lock(key)? {
            return Ok(Some(Either::Left(pessimistic_lock)));
        }

        if self.scan_mode.is_some() {
            self.create_lock_cursor_if_not_exist()?;
        }

        let res = if let Some(ref mut cursor) = self.lock_cursor {
            match cursor.get(key, &mut self.statistics.lock)? {
                Some(v) => Some(txn_types::parse_lock(v)?),
                None => None,
            }
        } else {
            self.statistics.lock.get += 1;
            match self.snapshot.get_cf(CF_LOCK, key)? {
                Some(v) => Some(txn_types::parse_lock(&v)?),
                None => None,
            }
        };

        Ok(res)
    }

    fn check_term_version_status(&self, locks: &PeerPessimisticLocks) -> Result<()> {
        // If the term or region version has changed, do not read the lock table.
        // Instead, just return a StaleCommand or EpochNotMatch error, so the
        // client will not receive a false error because the lock table has been
        // cleared.
        if self.term != 0 && locks.term != self.term {
            let mut err = errorpb::Error::default();
            err.set_stale_command(StaleCommand::default());
            return Err(KvError::from(err).into());
        }
        if self.version != 0 && locks.version != self.version {
            let mut err = errorpb::Error::default();
            err.set_epoch_not_match(EpochNotMatch::default());
            return Err(KvError::from(err).into());
        }
        if locks.status == LocksStatus::IsInFlashback && !self.allow_in_flashback {
            let mut err = errorpb::Error::default();
            err.set_flashback_in_progress(FlashbackInProgress::default());
            return Err(KvError::from(err).into());
        }
        Ok(())
    }

    /// Scan all types of locks(pessimitic, prewrite) satisfying `filter`
    /// condition from both in-memory pessimitic lock table and the storage
    /// within [start_key, end_key) .
    pub fn scan_locks<F>(
        &mut self,
        start_key: Option<&Key>,
        end_key: Option<&Key>,
        filter: F,
        limit: usize,
        source: ScanLockReadTimeSource,
    ) -> Result<(Vec<(Key, LockOrSharedLocks)>, bool)>
    where
        F: Fn(&Key, TxnLockRef<'_>) -> bool,
    {
        let (memory_locks, memory_has_remain) = self.load_in_memory_pessimistic_lock_range(
            start_key,
            end_key,
            |k, l| filter(k, l.into()),
            limit,
            source,
        )?;
        let memory_locks: Vec<(Key, LockOrSharedLocks)> = memory_locks
            .into_iter()
            .map(|(k, l)| (k, Either::Left(l)))
            .collect();
        if memory_locks.is_empty() {
            return self.scan_locks_from_storage(
                start_key,
                end_key,
                |k, l| filter(k, TxnLockRef::Persisted(l)),
                limit,
            );
        }

        let mut lock_cursor_seeked = false;
        let mut storage_iteration_finished = false;
        let mut next_pair_from_storage = || -> Result<Option<(Key, LockOrSharedLocks)>> {
            if storage_iteration_finished {
                return Ok(None);
            }
            self.create_lock_cursor_if_not_exist()?;
            let cursor = self.lock_cursor.as_mut().unwrap();
            if !lock_cursor_seeked {
                let ok = match start_key {
                    Some(x) => cursor.seek(x, &mut self.statistics.lock)?,
                    None => cursor.seek_to_first(&mut self.statistics.lock),
                };
                if !ok {
                    storage_iteration_finished = true;
                    return Ok(None);
                }
                lock_cursor_seeked = true;
            } else {
                cursor.next(&mut self.statistics.lock);
            }

            while cursor.valid()? {
                let key = Key::from_encoded_slice(cursor.key(&mut self.statistics.lock));
                if let Some(end) = end_key {
                    if key >= *end {
                        storage_iteration_finished = true;
                        return Ok(None);
                    }
                }
                let mut lock_or_shared_locks =
                    txn_types::parse_lock(cursor.value(&mut self.statistics.lock))?;
                match &mut lock_or_shared_locks {
                    Either::Left(l) => {
                        if filter(&key, TxnLockRef::Persisted(l)) {
                            self.statistics.lock.processed_keys += 1;
                            return Ok(Some((key, lock_or_shared_locks)));
                        }
                    }
                    Either::Right(shared_locks) => {
                        shared_locks.filter_shared_locks(|shared_lock| {
                            filter(&key, TxnLockRef::Persisted(shared_lock))
                        })?;
                        if !shared_locks.is_empty() {
                            self.statistics.lock.processed_keys += 1;
                            return Ok(Some((key, lock_or_shared_locks)));
                        }
                    }
                }
                cursor.next(&mut self.statistics.lock);
            }
            storage_iteration_finished = true;
            Ok(None)
        };

        let mut locks: Vec<(Key, LockOrSharedLocks)> =
            Vec::with_capacity(limit.min(memory_locks.len()));
        let mut memory_iter = memory_locks.into_iter();
        let mut memory_pair = memory_iter.next();
        let mut storage_pair = next_pair_from_storage()?;
        let has_remain = loop {
            let next_key = match (memory_pair.as_ref(), storage_pair.as_ref()) {
                (Some((memory_key, _)), Some((storage_key, _))) => {
                    if storage_key <= memory_key {
                        let next_key = storage_pair.take().unwrap();
                        storage_pair = next_pair_from_storage()?;
                        next_key
                    } else {
                        let next_key = memory_pair.take().unwrap();
                        memory_pair = memory_iter.next();
                        next_key
                    }
                }
                (Some(_), None) => {
                    let next_key = memory_pair.take().unwrap();
                    memory_pair = memory_iter.next();
                    next_key
                }
                (None, Some(_)) => {
                    let next_key = storage_pair.take().unwrap();
                    storage_pair = next_pair_from_storage()?;
                    next_key
                }
                (None, None) => break memory_has_remain,
            };
            // The same key could exist in both memory and storage when there is ongoing
            // leader transfer, split or merge on this region. In this case, duplicated
            // keys should be ignored.
            if locks.is_empty() || locks.last().unwrap().0 != next_key.0 {
                locks.push(next_key);
            }
            if limit > 0 && locks.len() >= limit {
                break memory_pair.is_some() || storage_pair.is_some() || memory_has_remain;
            }
        };
        Ok((locks, has_remain))
    }

    pub fn load_in_memory_pessimistic_lock_range<F>(
        &self,
        start_key: Option<&Key>,
        end_key: Option<&Key>,
        filter: F,
        scan_limit: usize,
        source: ScanLockReadTimeSource,
    ) -> Result<(Vec<(Key, Lock)>, bool)>
    where
        F: Fn(&Key, &PessimisticLock) -> bool,
    {
        if let Some(txn_ext) = self.snapshot.ext().get_txn_ext() {
            let begin_instant = Instant::now();
            let pessimistic_locks_guard = txn_ext.pessimistic_locks.read();
            let res = match self.check_term_version_status(&pessimistic_locks_guard) {
                Ok(_) => {
                    // Scan locks within the specified range and filter.
                    Ok(pessimistic_locks_guard.scan_locks(start_key, end_key, filter, scan_limit))
                }
                Err(e) => Err(e),
            };
            drop(pessimistic_locks_guard);

            let elapsed = begin_instant.saturating_elapsed();
            SCAN_LOCK_READ_TIME_VEC
                .get(source)
                .observe(elapsed.as_secs_f64());

            res
        } else {
            Ok((vec![], false))
        }
    }

    fn load_in_memory_pessimistic_lock(&self, key: &Key) -> Result<Option<Lock>> {
        if let Some(txn_ext) = self.snapshot.ext().get_txn_ext() {
            let locks = txn_ext.pessimistic_locks.read();
            self.check_term_version_status(&locks)?;
            Ok(locks.get(key).map(|(lock, _)| {
                // For write commands that are executed in serial, it should be impossible
                // to read a deleted lock.
                // For read commands in the scheduler, it should read the lock marked deleted
                // because the lock is not actually deleted from the underlying storage.
                lock.to_lock()
            }))
        } else {
            Ok(None)
        }
    }

    fn get_scan_mode(&self, allow_backward: bool) -> ScanMode {
        match self.scan_mode {
            Some(ScanMode::Forward) => ScanMode::Forward,
            Some(ScanMode::Backward) if allow_backward => ScanMode::Backward,
            _ => ScanMode::Mixed,
        }
    }

    /// Return:
    ///   (commit_ts, write_record) of the write record for `key` committed
    /// before or equal to`ts` Post Condition:
    ///   leave the write_cursor at the first record which key is less or equal
    /// to the `ts` encoded version of `key`
    pub fn seek_write(&mut self, key: &Key, ts: TimeStamp) -> Result<Option<(TimeStamp, Write)>> {
        // Get the cursor for write record
        //
        // When it switches to another key in prefix seek mode, creates a new cursor for
        // it because the current position of the cursor is seldom around `key`.
        if self.scan_mode.is_none() && self.current_key.as_ref().is_none_or(|k| k != key) {
            self.current_key = Some(key.clone());
            self.write_cursor.take();
        }
        self.create_write_cursor()?;
        let cursor = self.write_cursor.as_mut().unwrap();
        // find a `ts` encoded key which is less than the `ts` encoded version of the
        // `key`
        let found = cursor.near_seek(&key.clone().append_ts(ts), &mut self.statistics.write)?;
        if !found {
            return Ok(None);
        }
        let write_key = cursor.key(&mut self.statistics.write);
        let commit_ts = Key::decode_ts_from(write_key)?;
        // check whether the found written_key's "real key" part equals the `key` we
        // want to find
        if !Key::is_user_key_eq(write_key, key.as_encoded()) {
            return Ok(None);
        }
        // parse out the write record
        let write = WriteRef::parse(cursor.value(&mut self.statistics.write))?.to_owned();
        Ok(Some((commit_ts, write)))
    }

    /// Gets the value of the specified key's latest version before specified
    /// `ts`.
    ///
    /// It tries to ensure the write record's `gc_fence`'s ts, if any, greater
    /// than specified `gc_fence_limit`. Pass `None` to `gc_fence_limit` to
    /// skip the check. The caller must guarantee that there's no other `PUT` or
    /// `DELETE` versions whose `commit_ts` is between the found version and
    /// the provided `gc_fence_limit` (`gc_fence_limit` is inclusive).
    ///
    /// For transactional reads, the `gc_fence_limit` must be provided to ensure
    /// the result is correct. Generally, it should be the read_ts of the
    /// current transaction, which might be different from the `ts` passed to
    /// this function.
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

    /// Gets the write record of the specified key's latest version before
    /// specified `ts`. It tries to ensure the write record's `gc_fence`'s
    /// ts, if any, greater than specified `gc_fence_limit`. Pass `None` to
    /// `gc_fence_limit` to skip the check. The caller must guarantee that
    /// there's no other `PUT` or `DELETE` versions whose `commit_ts` is between
    /// the found version and the provided `gc_fence_limit` (`gc_fence_limit` is
    /// inclusive).
    /// For transactional reads, the `gc_fence_limit` must be provided to ensure
    /// the result is correct. Generally, it should be the read_ts of the
    /// current transaction, which might be different from the `ts` passed to
    /// this function.
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

    /// Gets the write record of the specified key's latest version before
    /// specified `ts` (i.e. a PUT or a DELETE), and additionally the write
    /// record's `commit_ts`, if any.
    ///
    /// See also [`MvccReader::get_write`].
    pub fn get_write_with_commit_ts(
        &mut self,
        key: &Key,
        mut ts: TimeStamp,
        gc_fence_limit: Option<TimeStamp>,
    ) -> Result<Option<(Write, TimeStamp)>> {
        let mut seek_res = self.seek_write(key, ts)?;
        loop {
            match seek_res {
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
                        WriteType::Lock | WriteType::Rollback => match write.last_change {
                            LastChange::NotExist => {
                                return Ok(None);
                            }
                            LastChange::Exist {
                                last_change_ts: commit_ts,
                                estimated_versions_to_last_change,
                            } if estimated_versions_to_last_change >= SEEK_BOUND => {
                                let key_with_ts = key.clone().append_ts(commit_ts);
                                let Some(value) = self.snapshot.get_cf(CF_WRITE, &key_with_ts)?
                                else {
                                    return Ok(None);
                                };
                                self.statistics.write.get += 1;
                                let write = WriteRef::parse(&value)?.to_owned();
                                assert!(
                                    write.write_type == WriteType::Put
                                        || write.write_type == WriteType::Delete,
                                    "Write record pointed by last_change_ts {} should be Put or Delete, but got {:?}",
                                    commit_ts,
                                    write.write_type,
                                );
                                seek_res = Some((commit_ts, write));
                                continue;
                            }
                            _ => {
                                if ts.is_zero() {
                                    // this should only happen in tests
                                    return Ok(None);
                                }
                                ts = commit_ts.prev();
                            }
                        },
                    }
                }
                None => return Ok(None),
            }
            seek_res = self.seek_write(key, ts)?;
        }
    }

    fn get_txn_commit_record(&mut self, key: &Key, start_ts: TimeStamp) -> Result<TxnCommitRecord> {
        // It's possible a txn with a small `start_ts` has a greater `commit_ts` than a
        // txn with a greater `start_ts` in pessimistic transaction.
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
                .range(self.lower_bound.clone(), self.upper_bound.clone())
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
                .range(self.lower_bound.clone(), self.upper_bound.clone())
                // `hint_min_ts` filters data by the `commit_ts`.
                .hint_min_ts(self.hint_min_ts)
                .build()?;
            self.write_cursor = Some(cursor);
        }
        Ok(())
    }

    fn create_lock_cursor_if_not_exist(&mut self) -> Result<()> {
        if self.lock_cursor.is_none() {
            let cursor = CursorBuilder::new(&self.snapshot, CF_LOCK)
                .fill_cache(self.fill_cache)
                .scan_mode(self.get_scan_mode(true))
                .range(self.lower_bound.clone(), self.upper_bound.clone())
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

    /// Scan locks that satisfies `filter(lock)` from storage in the key range
    /// [start, end). At most `limit` locks will be returned. If `limit` is
    /// set to `0`, it means unlimited.
    ///
    /// The return type is `(locks, has_remain)`. `has_remain` indicates whether
    /// there MAY be remaining locks that can be scanned.
    pub fn scan_locks_from_storage<F>(
        &mut self,
        start: Option<&Key>,
        end: Option<&Key>,
        filter: F,
        limit: usize,
    ) -> Result<(Vec<(Key, LockOrSharedLocks)>, bool)>
    where
        F: Fn(&Key, &Lock) -> bool,
    {
        self.create_lock_cursor_if_not_exist()?;
        let cursor = self.lock_cursor.as_mut().unwrap();
        let ok = match start {
            Some(x) => cursor.seek(x, &mut self.statistics.lock)?,
            None => cursor.seek_to_first(&mut self.statistics.lock),
        };
        if !ok {
            return Ok((vec![], false));
        }
        let mut locks = Vec::with_capacity(limit);
        let mut lock_count: usize = 0;
        let mut has_remain = false;
        while cursor.valid()? {
            let key = Key::from_encoded_slice(cursor.key(&mut self.statistics.lock));
            if let Some(end) = end {
                if key >= *end {
                    has_remain = false;
                    break;
                }
            }

            let mut lock_or_shared_locks =
                txn_types::parse_lock(cursor.value(&mut self.statistics.lock))?;
            match &mut lock_or_shared_locks {
                Either::Left(l) => {
                    if filter(&key, l) {
                        lock_count += 1;
                        locks.push((key, lock_or_shared_locks));
                    }
                }
                Either::Right(shared_locks) => {
                    shared_locks.filter_shared_locks(|shared_lock| filter(&key, shared_lock))?;
                    if !shared_locks.is_empty() {
                        if limit > 0 {
                            let remaining = limit - lock_count;
                            if shared_locks.len() > remaining {
                                shared_locks.truncate_to(remaining);
                                has_remain = true;
                            }
                        }
                        lock_count += shared_locks.len();
                        locks.push((key, lock_or_shared_locks));
                    }
                }
            }
            if limit > 0 && lock_count >= limit {
                has_remain = true;
                break;
            }
            cursor.next(&mut self.statistics.lock);
        }
        self.statistics.lock.processed_keys += lock_count;
        Ok((locks, has_remain))
    }

    /// Scan the writes to get all the latest user keys. This scan will skip
    /// `WriteType::Lock` and `WriteType::Rollback`, only return the key that
    /// has a latest `WriteType::Put` or `WriteType::Delete` record. The return
    /// type is:
    /// * `(Vec<key>, has_remain)`.
    ///   - `key` is the encoded user key without `commit_ts`.
    ///   - `has_remain` indicates whether there MAY be remaining user keys that
    ///     can be scanned.
    ///
    /// This function is mainly used by
    /// `txn::commands::FlashbackToVersionReadPhase`
    /// and `txn::commands::FlashbackToVersion` to achieve the MVCC
    /// overwriting.
    pub fn scan_latest_user_keys<F>(
        &mut self,
        start: Option<&Key>,
        end: Option<&Key>,
        filter: F,
        limit: usize,
    ) -> Result<(Vec<Key>, bool)>
    where
        F: Fn(&Key /* user key */, TimeStamp /* latest `commit_ts` */) -> bool,
    {
        self.create_write_cursor()?;
        let cursor = self.write_cursor.as_mut().unwrap();
        let ok = match start {
            Some(x) => cursor.seek(x, &mut self.statistics.write)?,
            None => cursor.seek_to_first(&mut self.statistics.write),
        };
        if !ok {
            return Ok((vec![], false));
        }
        let mut cur_user_key = None;
        let mut keys = Vec::with_capacity(limit);
        let mut has_remain = false;
        while cursor.valid()? {
            let key = Key::from_encoded_slice(cursor.key(&mut self.statistics.write));
            if let Some(end) = end {
                if key >= *end {
                    has_remain = false;
                    break;
                }
            }
            let commit_ts = key.decode_ts()?;
            let user_key = key.truncate_ts()?;
            // Skip the key if its latest write type is not `WriteType::Put` or
            // `WriteType::Delete`.
            match WriteRef::parse(cursor.value(&mut self.statistics.write))?.write_type {
                WriteType::Put | WriteType::Delete => {}
                WriteType::Lock | WriteType::Rollback => {
                    cursor.next(&mut self.statistics.write);
                    continue;
                }
            }
            // To make sure we only check each unique user key once and the filter returns
            // true.
            let is_same_user_key = cur_user_key.as_ref() == Some(&user_key);
            if !is_same_user_key {
                cur_user_key = Some(user_key.clone());
            }
            if is_same_user_key || !filter(&user_key, commit_ts) {
                cursor.next(&mut self.statistics.write);
                continue;
            }
            keys.push(user_key.clone());
            if limit > 0 && keys.len() == limit {
                has_remain = true;
                break;
            }
            // Seek once to skip all the writes of the same user key.
            cursor.near_seek(
                &user_key.append_ts(TimeStamp::zero()),
                &mut self.statistics.write,
            )?;
        }
        self.statistics.write.processed_keys += keys.len();
        resource_metering::record_read_keys(keys.len() as u32);
        Ok((keys, has_remain))
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

    pub fn set_range(&mut self, lower: Option<Key>, upper: Option<Key>) {
        self.lower_bound = lower;
        self.upper_bound = upper;
    }

    pub fn set_hint_min_ts(&mut self, ts_bound: Option<Bound<TimeStamp>>) {
        self.hint_min_ts = ts_bound;
    }

    pub fn snapshot_ext(&self) -> S::Ext<'_> {
        self.snapshot.ext()
    }

    pub fn snapshot(&self) -> &S {
        &self.snapshot
    }

    pub fn set_allow_in_flashback(&mut self, set_allow_in_flashback: bool) {
        self.allow_in_flashback = set_allow_in_flashback;
    }
}

#[cfg(test)]
pub mod tests {
    use std::ops::Bound;

    use concurrency_manager::ConcurrencyManager;
    use engine_rocks::{
        RocksCfOptions, RocksDbOptions, RocksEngine, RocksSnapshot,
        properties::MvccPropertiesCollectorFactory,
    };
    use engine_traits::{
        ALL_CFS, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE, CompactExt, IterOptions,
        ManualCompactionOptions, MiscExt, Mutable, SyncMutable, WriteBatch, WriteBatchExt,
    };
    use kvproto::{
        kvrpcpb::{AssertionLevel, Context, PrewriteRequestPessimisticAction::*},
        metapb::{Peer, Region},
    };
    use pd_client::FeatureGate;
    use raftstore::store::RegionSnapshot;
    use txn_types::{LastChange, Lock, LockType, Mutation, SharedLocks, TimeStamp};

    use super::*;
    use crate::storage::{
        Engine, TestEngineBuilder,
        kv::Modify,
        mvcc::{MvccReader, MvccTxn, tests::write},
        txn::{
            CommitKind, TransactionKind, TransactionProperties, acquire_pessimistic_lock, cleanup,
            commit, gc, prewrite, sched_pool::set_tls_feature_gate,
        },
    };

    pub struct RegionEngine {
        db: RocksEngine,
        region: Region,
    }

    impl RegionEngine {
        pub fn new(db: &RocksEngine, region: &Region) -> RegionEngine {
            RegionEngine {
                db: db.clone(),
                region: region.clone(),
            }
        }

        pub fn snapshot(&self) -> RegionSnapshot<RocksSnapshot> {
            let db = self.db.clone();
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
                txn_source: 0,
            }
        }

        pub fn prewrite(&mut self, m: Mutation, pk: &[u8], start_ts: impl Into<TimeStamp>) {
            let snap = self.snapshot();
            let start_ts = start_ts.into();
            let cm = ConcurrencyManager::new_for_test(start_ts);
            let mut txn = MvccTxn::new(start_ts, cm);
            let mut reader = SnapshotReader::new(start_ts, snap, true);

            prewrite(
                &mut txn,
                &mut reader,
                &Self::txn_props(start_ts, pk, false),
                m,
                &None,
                SkipPessimisticCheck,
                None,
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
            let cm = ConcurrencyManager::new_for_test(start_ts);
            let mut txn = MvccTxn::new(start_ts, cm);
            let mut reader = SnapshotReader::new(start_ts, snap, true);

            prewrite(
                &mut txn,
                &mut reader,
                &Self::txn_props(start_ts, pk, true),
                m,
                &None,
                DoPessimisticCheck,
                None,
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
            let cm = ConcurrencyManager::new_for_test(for_update_ts);
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
                false,
                false,
                false,
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
            let cm = ConcurrencyManager::new_for_test(start_ts);
            let mut txn = MvccTxn::new(start_ts, cm);
            let mut reader = SnapshotReader::new(start_ts, snap, true);
            commit(
                &mut txn,
                &mut reader,
                Key::from_raw(pk),
                commit_ts.into(),
                None,
            )
            .unwrap();
            self.write(txn.into_modifies());
        }

        pub fn rollback(&mut self, pk: &[u8], start_ts: impl Into<TimeStamp>) {
            let snap = self.snapshot();
            let start_ts = start_ts.into();
            let cm = ConcurrencyManager::new_for_test(start_ts);
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
            let cm = ConcurrencyManager::new_for_test(safe_point.into());
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
            let mut wb = db.write_batch();
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
                    Modify::Ingest(_) => unimplemented!(),
                }
            }
            wb.write().unwrap();
        }

        pub fn flush(&mut self) {
            for cf in ALL_CFS {
                self.db.flush_cf(cf, true).unwrap();
            }
        }

        pub fn compact(&mut self) {
            for cf in ALL_CFS {
                self.db
                    .compact_range_cf(
                        cf,
                        None,
                        None,
                        ManualCompactionOptions::new(false, 1, false),
                    )
                    .unwrap();
            }
        }
    }

    pub fn open_db(path: &str, with_properties: bool) -> RocksEngine {
        let db_opt = RocksDbOptions::default();
        let mut cf_opts = RocksCfOptions::default();
        cf_opts.set_write_buffer_size(32 * 1024 * 1024);
        if with_properties {
            cf_opts.add_table_properties_collector_factory(
                "tikv.test-collector",
                MvccPropertiesCollectorFactory::default(),
            );
        }
        let cfs_opts = vec![
            (CF_DEFAULT, RocksCfOptions::default()),
            (CF_RAFT, RocksCfOptions::default()),
            (CF_LOCK, RocksCfOptions::default()),
            (CF_WRITE, cf_opts),
        ];
        engine_rocks::util::new_engine_opt(path, db_opt, cfs_opts).unwrap()
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

        let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db, region);

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

        for &(min, max, ref res) in tests.iter() {
            let mut iopt = IterOptions::default();
            iopt.set_hint_min_ts(min);
            iopt.set_hint_max_ts(max);

            let mut iter = snap.iter(CF_WRITE, iopt).unwrap();

            for (i, expect_ts) in res.iter().enumerate() {
                if i == 0 {
                    assert!(iter.seek_to_first().unwrap());
                } else {
                    assert!(iter.next().unwrap());
                }

                let ts = Key::decode_ts_from(iter.key()).unwrap();
                assert_eq!(ts.into_inner(), *expect_ts);
            }

            assert!(!iter.next().unwrap());
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

        let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db, region);
        let mut iter = snap.iter(CF_WRITE, iopt).unwrap();

        // Must not omit the latest deletion of key1 to prevent seeing outdated record.
        assert!(iter.seek_to_first().unwrap());
        assert_eq!(
            Key::from_encoded_slice(iter.key())
                .to_raw()
                .unwrap()
                .as_slice(),
            key2
        );
        assert!(!iter.next().unwrap());
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

        let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db, region);
        let mut reader = MvccReader::new(snap, None, false);

        // Let's assume `50_45 PUT` means a commit version with start ts is 45 and
        // commit ts is 50.
        // Commit versions: [50_45 PUT, 45_40 PUT, 40_35 PUT, 30_25 PUT, 20_20 Rollback,
        // 10_1 PUT, 5_5 Rollback].
        let key = Key::from_raw(k);
        let overlapped_write = reader
            .get_txn_commit_record(&key, 55.into())
            .unwrap()
            .unwrap_none(0);
        assert!(overlapped_write.is_none());

        // When no such record is found but a record of another txn has a write record
        // with its commit_ts equals to current start_ts, it
        let overlapped_write = reader
            .get_txn_commit_record(&key, 50.into())
            .unwrap()
            .unwrap_none(0)
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

        let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db, region);
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

        // Let's assume `2_1 PUT` means a commit version with start ts is 1 and commit
        // ts is 2.
        // Commit versions: [25_23 PUT, 20_10 PUT, 17_15 PUT, 7_7 Rollback, 5_1 PUT, 3_3
        // Rollback].
        let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db.clone(), region.clone());
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
        assert_eq!(write.write_type, WriteType::Lock);
        assert_eq!(write.start_ts, 10.into());
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

        let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db.clone(), region);
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
        let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db, region1);
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

        let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db, region);
        let mut reader = MvccReader::new(snap, None, false);

        // Let's assume `2_1 PUT` means a commit version with start ts is 1 and commit
        // ts is 2.
        // Commit versions: [21_17 LOCK, 20_18 PUT, 15_13 LOCK, 14_12 PUT, 9_8 DELETE,
        // 7_6 LOCK, 5_5 Rollback, 2_1 PUT].
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
                Either::Left(
                    Lock::new(
                        lock_type,
                        b"k1".to_vec(),
                        ts,
                        0,
                        short_value,
                        for_update_ts,
                        0,
                        TimeStamp::zero(),
                        false,
                    )
                    .set_last_change(LastChange::from_parts(
                        TimeStamp::zero(),
                        (lock_type == LockType::Lock || lock_type == LockType::Pessimistic) as u64,
                    )),
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
            let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db.clone(), region.clone());
            let mut reader = MvccReader::new(snap, None, false);
            let res = reader
                .scan_locks_from_storage(
                    start_key.as_ref(),
                    end_key.as_ref(),
                    |_, l| l.ts <= 10.into(),
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
    fn test_scan_locks_from_storage_shared_locks_limit() {
        // This test guarantees that the limit parameter take effect on shared locks as
        // well.
        let path = tempfile::Builder::new()
            .prefix("_test_storage_mvcc_reader_scan_locks_from_storage_shared_locks_limit")
            .tempdir()
            .unwrap();
        let path = path.path().to_str().unwrap();
        let region = make_region(1, vec![], vec![]);
        let db = open_db(path, true);
        let mut engine = RegionEngine::new(&db, &region);

        // Create a SharedLocks with 5 individual locks on key "k1"
        let mut shared_locks = SharedLocks::new();
        for i in 1..=5u64 {
            let lock = Lock::new(
                LockType::Pessimistic,
                format!("k{}", i).into_bytes(),
                i.into(),
                100,
                None,
                TimeStamp::zero(),
                0,
                TimeStamp::zero(),
                false,
            );
            shared_locks.insert_lock(lock).unwrap();
        }

        // Write SharedLocks directly to CF_LOCK
        let key = Key::from_raw(b"shared_lock_key");
        engine.write(vec![Modify::Put(
            CF_LOCK,
            key.clone(),
            shared_locks.to_bytes(),
        )]);

        // Scan with limit=1
        let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db.clone(), region.clone());
        let mut reader = MvccReader::new(snap, None, false);
        let (locks, _has_remain) = reader
            .scan_locks_from_storage(None, None, |_, _| true, 1)
            .unwrap();

        // Count total individual locks returned
        let total_locks: usize = locks
            .iter()
            .map(|(_, lor)| match lor {
                Either::Left(_) => 1,
                Either::Right(shared) => shared.len(),
            })
            .sum();

        assert!(
            total_locks <= 1,
            "Expected at most 1 lock when limit=1, but got {} locks.",
            total_locks
        );
    }

    #[test]
    fn test_scan_latest_user_keys() {
        let path = tempfile::Builder::new()
            .prefix("_test_storage_mvcc_reader_scan_latest_user_keys")
            .tempdir()
            .unwrap();
        let path = path.path().to_str().unwrap();
        let region = make_region(1, vec![], vec![]);
        let db = open_db(path, true);
        let mut engine = RegionEngine::new(&db, &region);

        // Prewrite and rollback k0.
        engine.prewrite(
            Mutation::make_put(Key::from_raw(b"k0"), b"v0@1".to_vec()),
            b"k0",
            1,
        );
        engine.rollback(b"k0", 1);
        // Prewrite and commit k0 with a large ts.
        engine.prewrite(
            Mutation::make_put(Key::from_raw(b"k0"), b"v0@999".to_vec()),
            b"k0",
            999,
        );
        engine.commit(b"k0", 999, 1000);
        // Prewrite and commit k1.
        engine.prewrite(
            Mutation::make_put(Key::from_raw(b"k1"), b"v1@1".to_vec()),
            b"k1",
            1,
        );
        engine.commit(b"k1", 1, 2);
        engine.prewrite(
            Mutation::make_put(Key::from_raw(b"k1"), b"v1@3".to_vec()),
            b"k1",
            3,
        );
        engine.commit(b"k1", 3, 4);
        // Uncommitted prewrite k1.
        engine.prewrite(
            Mutation::make_put(Key::from_raw(b"k1"), b"v1@5".to_vec()),
            b"k1",
            5,
        );

        // Prewrite and commit k2.
        engine.prewrite(
            Mutation::make_put(Key::from_raw(b"k2"), b"v2@1".to_vec()),
            b"k2",
            1,
        );
        engine.commit(b"k2", 1, 2);
        engine.prewrite(
            Mutation::make_put(Key::from_raw(b"k2"), b"v2@3".to_vec()),
            b"k2",
            3,
        );
        engine.commit(b"k2", 3, 4);

        // Prewrite and commit k3.
        engine.prewrite(
            Mutation::make_put(Key::from_raw(b"k3"), b"v3@5".to_vec()),
            b"k3",
            5,
        );
        engine.commit(b"k3", 5, 6);
        // Prewrite and rollback k3.
        engine.prewrite(
            Mutation::make_put(Key::from_raw(b"k3"), b"v3@7".to_vec()),
            b"k3",
            7,
        );
        engine.rollback(b"k3", 7);
        // Prewrite and commit k3.
        engine.prewrite(
            Mutation::make_put(Key::from_raw(b"k3"), b"v3@8".to_vec()),
            b"k3",
            8,
        );
        engine.commit(b"k3", 8, 9);
        // Prewrite and commit k4.
        engine.prewrite(
            Mutation::make_put(Key::from_raw(b"k4"), b"v4@1".to_vec()),
            b"k4",
            10,
        );
        engine.commit(b"k4", 10, 11);
        // Prewrite and rollback k4.
        engine.prewrite(
            Mutation::make_put(Key::from_raw(b"k4"), b"v4@2".to_vec()),
            b"k4",
            12,
        );
        engine.rollback(b"k4", 12);
        // Prewrite and rollback k5.
        engine.prewrite(
            Mutation::make_put(Key::from_raw(b"k5"), b"v5@1".to_vec()),
            b"k5",
            13,
        );
        engine.rollback(b"k5", 13);

        // Current MVCC keys in `CF_WRITE` should be:
        // PUT      k0 -> v0@999
        // ROLLBACK k0 -> v0@1
        // PUT      k1 -> v1@3
        // PUT      k1 -> v1@1
        // PUT      k2 -> v2@3
        // PUT      k2 -> v2@1
        // PUT      k3 -> v3@8
        // ROLLBACK k3 -> v3@7
        // PUT      k3 -> v3@5
        // ROLLBACK k4 -> v4@2
        // PUT      k4 -> v4@1
        // ROLLBACK k5 -> v5@1

        struct Case {
            start_key: Option<Key>,
            end_key: Option<Key>,
            limit: usize,
            expect_res: Vec<Key>,
            expect_is_remain: bool,
        }

        let cases = [
            // Test the limit.
            Case {
                start_key: None,
                end_key: None,
                limit: 1,
                expect_res: vec![Key::from_raw(b"k0")],
                expect_is_remain: true,
            },
            Case {
                start_key: None,
                end_key: None,
                limit: 6,
                expect_res: vec![
                    Key::from_raw(b"k0"),
                    Key::from_raw(b"k1"),
                    Key::from_raw(b"k2"),
                    Key::from_raw(b"k3"),
                    Key::from_raw(b"k4"),
                ],
                expect_is_remain: false,
            },
            // Test the start/end key.
            Case {
                start_key: Some(Key::from_raw(b"k2")),
                end_key: None,
                limit: 4,
                expect_res: vec![
                    Key::from_raw(b"k2"),
                    Key::from_raw(b"k3"),
                    Key::from_raw(b"k4"),
                ],
                expect_is_remain: false,
            },
            Case {
                start_key: None,
                end_key: Some(Key::from_raw(b"k3")),
                limit: 4,
                expect_res: vec![
                    Key::from_raw(b"k0"),
                    Key::from_raw(b"k1"),
                    Key::from_raw(b"k2"),
                ],
                expect_is_remain: false,
            },
        ];

        for (idx, case) in cases.iter().enumerate() {
            let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db.clone(), region.clone());
            let mut reader = MvccReader::new(snap, Some(ScanMode::Forward), false);
            let res = reader
                .scan_latest_user_keys(
                    case.start_key.as_ref(),
                    case.end_key.as_ref(),
                    |_, _| true,
                    case.limit,
                )
                .unwrap();
            assert_eq!(res.0, case.expect_res, "case #{}", idx);
            assert_eq!(res.1, case.expect_is_remain, "case #{}", idx);
        }
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
                expected: Err(default_not_found_error(
                    Key::from_raw(k).append_ts(TimeStamp::new(3)).into_encoded(),
                    "get",
                )),
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
                expected: Err(default_not_found_error(
                    Key::from_raw(k).append_ts(TimeStamp::new(5)).into_encoded(),
                    "get",
                )),
                modifies: vec![],
                scan_mode: None,
                key: Key::from_raw(k),
                write: Write::new(WriteType::Put, TimeStamp::new(5), None),
            },
        ];

        for case in cases {
            engine.write(case.modifies);
            let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db.clone(), region.clone());
            let mut reader = MvccReader::new(snap, case.scan_mode, false);
            let result = reader.load_data(&case.key, case.write.clone());
            assert_eq!(format!("{:?}", result), format!("{:?}", case.expected));
            if let Ok(expected) = case.expected {
                if expected == long_value.to_vec() {
                    let result = reader
                        .get_value(&case.key, case.write.start_ts)
                        .unwrap()
                        .unwrap();
                    assert_eq!(format!("{:?}", result), format!("{:?}", expected));
                }
            }
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
                expected: Err(default_not_found_error(
                    Key::from_raw(k).append_ts(TimeStamp::new(2)).into_encoded(),
                    "get",
                )),
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
            let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db.clone(), region.clone());
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
            let mut engine = TestEngineBuilder::new().build().unwrap();
            let cm = ConcurrencyManager::new_for_test(42.into());
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

        // Must return Oldvalue::None when prev_write_loaded is true and prev_write is
        // None.
        let mut engine = TestEngineBuilder::new().build().unwrap();
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
        let db = builder.build().unwrap().kv_engine().unwrap();

        let region = make_region(1, vec![], vec![]);
        let mut engine = RegionEngine::new(&db, &region);

        // Put some tombstones into the DB.
        for i in 1..100 {
            let commit_ts = (i * 2 + 1).into();
            let mut k = vec![b'z'];
            k.extend_from_slice(Key::from_raw(b"k1").append_ts(commit_ts).as_encoded());
            engine.db.delete_cf(CF_WRITE, &k).unwrap();
        }
        engine.flush();

        for (k, scan_mode, tombstones) in &[
            (b"k0" as &[u8], Some(ScanMode::Forward), 99),
            (b"k0", None, 0),
            (b"k1", Some(ScanMode::Forward), 99),
            (b"k1", None, 99),
            (b"k2", Some(ScanMode::Forward), 0),
            (b"k2", None, 0),
        ] {
            let mut reader = MvccReader::new(engine.snapshot(), *scan_mode, false);
            let (k, ts) = (Key::from_raw(k), 199.into());
            reader.seek_write(&k, ts).unwrap();
            assert_eq!(reader.statistics.write.seek_tombstone, *tombstones);
        }
    }

    #[test]
    fn test_get_write_second_get() {
        let path = tempfile::Builder::new()
            .prefix("_test_storage_mvcc_reader_get_write_second_get")
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

        // Write enough LOCK recrods
        for start_ts in (6..30).step_by(2) {
            engine.lock(k, start_ts, start_ts + 1);
        }

        let m = Mutation::make_delete(Key::from_raw(k));
        engine.prewrite(m, k, 45);
        engine.commit(k, 45, 46);

        // Write enough LOCK recrods
        for start_ts in (50..80).step_by(2) {
            engine.lock(k, start_ts, start_ts + 1);
        }

        let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db, region);
        let mut reader = MvccReader::new(snap, None, false);

        let key = Key::from_raw(k);
        // Get write record whose commit_ts = 2
        let w2 = reader
            .get_write(&key, TimeStamp::new(2), None)
            .unwrap()
            .unwrap();

        // Clear statistics first
        reader.statistics = Statistics::default();
        let (write, commit_ts) = reader
            .get_write_with_commit_ts(&key, 40.into(), None)
            .unwrap()
            .unwrap();
        assert_eq!(commit_ts, 2.into());
        assert_eq!(write, w2);
        // estimated_versions_to_last_change should be large enough to trigger a second
        // get instead of calling a series of next, so the count of next should
        // be 0 instead
        assert_eq!(reader.statistics.write.next, 0);
        assert_eq!(reader.statistics.write.get, 1);

        // Clear statistics first
        reader.statistics = Statistics::default();
        let res = reader
            .get_write_with_commit_ts(&key, 80.into(), None)
            .unwrap();
        // If the type is Delete, get_write_with_commit_ts should return None.
        assert!(res.is_none());
        // estimated_versions_to_last_change should be large enough to trigger a second
        // get instead of calling a series of next, so the count of next should
        // be 0 instead
        assert_eq!(reader.statistics.write.next, 0);
        assert_eq!(reader.statistics.write.get, 1);
    }

    #[test]
    fn test_get_write_not_exist_skip_lock() {
        let path = tempfile::Builder::new()
            .prefix("_test_storage_mvcc_reader_get_write_not_exist_skip_lock")
            .tempdir()
            .unwrap();
        let path = path.path().to_str().unwrap();
        let region = make_region(1, vec![], vec![]);
        let db = open_db(path, true);
        let mut engine = RegionEngine::new(&db, &region);
        let k = b"k";

        // Write enough LOCK recrods
        for start_ts in (6..30).step_by(2) {
            engine.lock(k, start_ts, start_ts + 1);
        }

        let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db, region);
        let mut reader = MvccReader::new(snap, None, false);

        let res = reader
            .get_write_with_commit_ts(&Key::from_raw(k), 40.into(), None)
            .unwrap();
        // We can know the key doesn't exist without skipping all these locks according
        // to last_change_ts and estimated_versions_to_last_change.
        assert!(res.is_none());
        assert_eq!(reader.statistics.write.seek, 1);
        assert_eq!(reader.statistics.write.next, 0);
        assert_eq!(reader.statistics.write.get, 0);
    }

    #[test]
    fn test_skip_lock_after_upgrade_6_5() {
        let path = tempfile::Builder::new()
            .prefix("_test_storage_mvcc_reader_skip_lock_after_upgrade_6_5")
            .tempdir()
            .unwrap();
        let path = path.path().to_str().unwrap();
        let region = make_region(1, vec![], vec![]);
        let db = open_db(path, true);
        let mut engine = RegionEngine::new(&db, &region);
        let k = b"k";

        // 6.1.0, locks were written
        let feature_gate = FeatureGate::default();
        feature_gate.set_version("6.1.0").unwrap();
        set_tls_feature_gate(feature_gate);

        engine.put(k, 1, 2);
        // 10 locks were put
        for start_ts in (6..30).step_by(2) {
            engine.lock(k, start_ts, start_ts + 1);
        }

        // in 6.5 a new lock was put, and it should contain a `last_change_ts`.
        let feature_gate = FeatureGate::default();
        feature_gate.set_version("6.5.0").unwrap();
        set_tls_feature_gate(feature_gate);

        engine.lock(k, 30, 31);
        let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db.clone(), region.clone());
        let mut reader = MvccReader::new(snap, None, false);
        let res = reader
            .get_write_with_commit_ts(&Key::from_raw(k), 100.into(), None)
            .unwrap();
        assert!(res.is_some());
        let res = res.unwrap();
        assert_eq!(res.1, 2.into());
        assert_eq!(res.0.write_type, WriteType::Put);
        assert_eq!(reader.statistics.write.seek, 1);
        assert_eq!(reader.statistics.write.next, 0);

        // same as above, but for delete
        let feature_gate = FeatureGate::default();
        feature_gate.set_version("6.1.0").unwrap();
        set_tls_feature_gate(feature_gate);
        engine.delete(k, 51, 52);
        for start_ts in (56..80).step_by(2) {
            engine.lock(k, start_ts, start_ts + 1);
        }
        let feature_gate = FeatureGate::default();
        feature_gate.set_version("6.5.0").unwrap();
        set_tls_feature_gate(feature_gate);
        engine.lock(k, 80, 81);
        let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db, region);
        let mut reader = MvccReader::new(snap, None, false);
        let res = reader
            .get_write_with_commit_ts(&Key::from_raw(k), 100.into(), None)
            .unwrap();
        assert!(res.is_none());
        assert_eq!(reader.statistics.write.seek, 1);
        assert_eq!(reader.statistics.write.next, 0);
    }

    #[test]
    fn test_locks_interleaving_rollbacks() {
        // a ROLLBACK inside a chain of LOCKs won't prevent LOCKs from tracking the
        // correct `last_change_ts`
        let path = tempfile::Builder::new()
            .prefix("_test_storage_mvcc_reader_locks_interleaving_rollbacks")
            .tempdir()
            .unwrap();
        let path = path.path().to_str().unwrap();
        let region = make_region(1, vec![], vec![]);
        let db = open_db(path, true);
        let mut engine = RegionEngine::new(&db, &region);
        let k = b"k";
        engine.put(k, 1, 2);

        for start_ts in (6..30).step_by(2) {
            engine.lock(k, start_ts, start_ts + 1);
        }
        engine.rollback(k, 30);
        engine.lock(k, 31, 32);

        let snap = RegionSnapshot::<RocksSnapshot>::from_raw(db, region);
        let mut reader = MvccReader::new(snap, None, false);
        let res = reader
            .get_write_with_commit_ts(&Key::from_raw(k), 100.into(), None)
            .unwrap();
        assert!(res.is_some());
        let res = res.unwrap();
        assert_eq!(res.0.write_type, WriteType::Put);
        assert_eq!(res.1, 2.into());
        assert_eq!(reader.statistics.write.seek, 1);
        assert_eq!(reader.statistics.write.next, 2);
        assert_eq!(reader.statistics.write.get, 1);
    }
}
