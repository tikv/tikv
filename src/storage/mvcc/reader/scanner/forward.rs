// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::{borrow::Cow, cmp::Ordering};

use engine_traits::CF_DEFAULT;
use kvproto::kvrpcpb::{ExtraOp, IsolationLevel};
use txn_types::{Key, Lock, LockType, OldValue, TimeStamp, Value, WriteRef, WriteType};

use super::ScannerConfig;
use crate::storage::{
    kv::SEEK_BOUND,
    mvcc::{ErrorInner::WriteConflict, NewerTsCheckState, Result},
    txn::{Result as TxnResult, TxnEntry, TxnEntryScanner},
    Cursor, Snapshot, Statistics,
};

/// Defines the behavior of the scanner.
pub trait ScanPolicy<S: Snapshot> {
    /// The type that the scanner outputs.
    type Output;

    /// Handles the lock that the scanner meets.
    ///
    /// If `HandleRes::Return(val)` is returned, `val` will be returned to the
    /// caller of the scanner. Otherwise, `HandleRes::Skip(current_user_key)`
    /// should be returned. Then, the scanner will handle the write records
    /// if the write cursor points to the same user key, or continue scanning.
    ///
    /// Note that the method should also take care of moving the cursors.
    fn handle_lock(
        &mut self,
        current_user_key: Key,
        cfg: &mut ScannerConfig<S>,
        cursors: &mut Cursors<S>,
        statistics: &mut Statistics,
    ) -> Result<HandleRes<Self::Output>>;

    /// Handles the write record that the scanner meets.
    ///
    /// If `HandleRes::Return(val)` is returned, `val` will be returned to the
    /// caller of the scanner. Otherwise, `HandleRes::Skip(current_user_key)`
    /// should be returned and the scanner will continue scanning.
    ///
    /// Note that the method should also take care of moving the cursors.
    fn handle_write(
        &mut self,
        current_user_key: Key,
        cfg: &mut ScannerConfig<S>,
        cursors: &mut Cursors<S>,
        statistics: &mut Statistics,
    ) -> Result<HandleRes<Self::Output>>;

    /// Returns the size of the specified output.
    fn output_size(&mut self, output: &Self::Output) -> usize;
}

pub enum HandleRes<T> {
    Return(T),
    Skip(Key),
    MoveToNext,
}

pub struct Cursors<S: Snapshot> {
    lock: Option<Cursor<S::Iter>>,
    write: Cursor<S::Iter>,
    /// `default cursor` is lazy created only when it's needed.
    default: Option<Cursor<S::Iter>>,
}

impl<S: Snapshot> Cursors<S> {
    #[inline]
    fn move_write_cursor_to_next_user_key(
        &mut self,
        current_user_key: &Key,
        statistics: &mut Statistics,
    ) -> Result<()> {
        for i in 0..SEEK_BOUND {
            if i > 0 {
                self.write.next(&mut statistics.write);
            }
            if !self.write.valid()? {
                // Key space ended. We are done here.
                return Ok(());
            }
            {
                let current_key = self.write.key(&mut statistics.write);
                if !Key::is_user_key_eq(current_key, current_user_key.as_encoded().as_slice()) {
                    // Found another user key. We are done here.
                    return Ok(());
                }
            }
        }

        // We have not found another user key for now, so we directly `seek()`.
        // After that, we must pointing to another key, or out of bound.
        // `current_user_key` must have reserved space here, so its clone has reserved space too.
        // So no reallocation happens in `append_ts`.
        self.write.internal_seek(
            &current_user_key.clone().append_ts(TimeStamp::zero()),
            &mut statistics.write,
        )?;

        Ok(())
    }

    /// Create the default cursor if it doesn't exist.
    #[inline]
    fn ensure_default_cursor(&mut self, cfg: &mut ScannerConfig<S>) -> Result<()> {
        if self.default.is_some() {
            return Ok(());
        }
        self.default = Some(cfg.create_cf_cursor(CF_DEFAULT)?);
        Ok(())
    }
}

pub struct ForwardScanner<S: Snapshot, P: ScanPolicy<S>> {
    cfg: ScannerConfig<S>,
    cursors: Cursors<S>,
    /// Is iteration started
    is_started: bool,
    statistics: Statistics,
    scan_policy: P,
    met_newer_ts_data: NewerTsCheckState,
}

impl<S: Snapshot, P: ScanPolicy<S>> ForwardScanner<S, P> {
    pub fn new(
        cfg: ScannerConfig<S>,
        lock_cursor: Option<Cursor<S::Iter>>,
        write_cursor: Cursor<S::Iter>,
        default_cursor: Option<Cursor<S::Iter>>,
        scan_policy: P,
    ) -> ForwardScanner<S, P> {
        let cursors = Cursors {
            lock: lock_cursor,
            write: write_cursor,
            default: default_cursor,
        };
        ForwardScanner {
            met_newer_ts_data: if cfg.check_has_newer_ts_data {
                NewerTsCheckState::NotMetYet
            } else {
                NewerTsCheckState::Unknown
            },
            cfg,
            cursors,
            statistics: Statistics::default(),
            is_started: false,
            scan_policy,
        }
    }

    /// Take out and reset the statistics collected so far.
    pub fn take_statistics(&mut self) -> Statistics {
        std::mem::take(&mut self.statistics)
    }

    /// Whether we met newer ts data.
    /// The result is always `Unknown` if `check_has_newer_ts_data` is not set.
    #[inline]
    pub fn met_newer_ts_data(&self) -> NewerTsCheckState {
        self.met_newer_ts_data
    }

    /// Get the next key-value pair, in forward order.
    pub fn read_next(&mut self) -> Result<Option<P::Output>> {
        if !self.is_started {
            if self.cfg.lower_bound.is_some() {
                // TODO: `seek_to_first` is better, however it has performance issues currently.
                self.cursors.write.seek(
                    self.cfg.lower_bound.as_ref().unwrap(),
                    &mut self.statistics.write,
                )?;
                if let Some(lock_cursor) = self.cursors.lock.as_mut() {
                    lock_cursor.seek(
                        self.cfg.lower_bound.as_ref().unwrap(),
                        &mut self.statistics.lock,
                    )?;
                }
            } else {
                self.cursors.write.seek_to_first(&mut self.statistics.write);
                if let Some(lock_cursor) = self.cursors.lock.as_mut() {
                    lock_cursor.seek_to_first(&mut self.statistics.lock);
                }
            }
            self.is_started = true;
        }

        // The general idea is to simultaneously step write cursor and lock cursor.

        // TODO: We don't need to seek lock CF if isolation level is RC.

        loop {
            // `current_user_key` is `min(user_key(write_cursor), lock_cursor)`, indicating
            // the encoded user key we are currently dealing with. It may not have a write, or
            // may not have a lock. It is not a slice to avoid data being invalidated after
            // cursor moving.
            //
            // `has_write` indicates whether `current_user_key` has at least one corresponding
            // `write`. If there is one, it is what current write cursor pointing to. The pointed
            // `write` must be the most recent (i.e. largest `commit_ts`) write of
            // `current_user_key`.
            //
            // `has_lock` indicates whether `current_user_key` has a corresponding `lock`. If
            // there is one, it is what current lock cursor pointing to.
            let (mut current_user_key, has_write, has_lock) = {
                let w_key = if self.cursors.write.valid()? {
                    Some(self.cursors.write.key(&mut self.statistics.write))
                } else {
                    None
                };
                let l_key = if let Some(lock_cursor) = self.cursors.lock.as_mut() {
                    if lock_cursor.valid()? {
                        Some(lock_cursor.key(&mut self.statistics.lock))
                    } else {
                        None
                    }
                } else {
                    None
                };

                // `res` is `(current_user_key_slice, has_write, has_lock)`
                let res = match (w_key, l_key) {
                    (None, None) => {
                        // Both cursors yield `None`: we know that there is nothing remaining.
                        return Ok(None);
                    }
                    (None, Some(k)) => {
                        // Write cursor yields `None` but lock cursor yields something:
                        // In RC, it means we got nothing.
                        // In SI, we need to check if the lock will cause conflict.
                        (k, false, true)
                    }
                    (Some(k), None) => {
                        // Write cursor yields something but lock cursor yields `None`:
                        // We need to further step write cursor to our desired version
                        (Key::truncate_ts_for(k)?, true, false)
                    }
                    (Some(wk), Some(lk)) => {
                        let write_user_key = Key::truncate_ts_for(wk)?;
                        match write_user_key.cmp(lk) {
                            Ordering::Less => {
                                // Write cursor user key < lock cursor, it means the lock of the
                                // current key that write cursor is pointing to does not exist.
                                (write_user_key, true, false)
                            }
                            Ordering::Greater => {
                                // Write cursor user key > lock cursor, it means we got a lock of a
                                // key that does not have a write. In SI, we need to check if the
                                // lock will cause conflict.
                                (lk, false, true)
                            }
                            Ordering::Equal => {
                                // Write cursor user key == lock cursor, it means the lock of the
                                // current key that write cursor is pointing to *exists*.
                                (lk, true, true)
                            }
                        }
                    }
                };

                // Use `from_encoded_slice` to reserve space for ts, so later we can append ts to
                // the key or its clones without reallocation.
                (Key::from_encoded_slice(res.0), res.1, res.2)
            };

            if has_lock {
                if self.met_newer_ts_data == NewerTsCheckState::NotMetYet {
                    self.met_newer_ts_data = NewerTsCheckState::Met;
                }
                current_user_key = match self.scan_policy.handle_lock(
                    current_user_key,
                    &mut self.cfg,
                    &mut self.cursors,
                    &mut self.statistics,
                )? {
                    HandleRes::Return(output) => {
                        self.statistics.processed_size += self.scan_policy.output_size(&output);
                        return Ok(Some(output));
                    }
                    HandleRes::Skip(key) => key,
                    HandleRes::MoveToNext => continue,
                };
            }
            if has_write {
                let is_current_user_key = self.move_write_cursor_to_ts(&current_user_key)?;
                if is_current_user_key {
                    if let HandleRes::Return(output) = self.scan_policy.handle_write(
                        current_user_key,
                        &mut self.cfg,
                        &mut self.cursors,
                        &mut self.statistics,
                    )? {
                        self.statistics.write.processed_keys += 1;
                        self.statistics.processed_size += self.scan_policy.output_size(&output);
                        resource_metering::record_read_keys(1);
                        return Ok(Some(output));
                    }
                }
            }
        }
    }

    /// Try to move the write cursor to the `self.cfg.ts` version of the given key.
    /// Because it is possible that the cursor is moved to the next user key or
    /// the end of key space, the method returns whether the write cursor still
    /// points to the given user key.
    fn move_write_cursor_to_ts(&mut self, user_key: &Key) -> Result<bool> {
        assert!(self.cursors.write.valid()?);

        // Try to iterate to `${user_key}_${ts}`. We first `next()` for a few times,
        // and if we have not reached where we want, we use `seek()`.

        // Whether we have *not* reached where we want by `next()`.
        let mut needs_seek = true;

        for i in 0..SEEK_BOUND {
            if i > 0 {
                self.cursors.write.next(&mut self.statistics.write);
                if !self.cursors.write.valid()? {
                    // Key space ended.
                    return Ok(false);
                }
            }
            {
                let current_key = self.cursors.write.key(&mut self.statistics.write);
                if !Key::is_user_key_eq(current_key, user_key.as_encoded().as_slice()) {
                    // Meet another key.
                    return Ok(false);
                }
                let key_commit_ts = Key::decode_ts_from(current_key)?;
                if key_commit_ts <= self.cfg.ts {
                    // Founded, don't need to seek again.
                    needs_seek = false;
                    break;
                } else if self.met_newer_ts_data == NewerTsCheckState::NotMetYet {
                    self.met_newer_ts_data = NewerTsCheckState::Met;
                }

                // Report error if there's a more recent version if the isolation level is RcCheckTs.
                if self.cfg.isolation_level == IsolationLevel::RcCheckTs {
                    // TODO: the more write recent version with `LOCK` or `ROLLBACK` write type
                    //       could be skipped.
                    return Err(WriteConflict {
                        start_ts: self.cfg.ts,
                        conflict_start_ts: Default::default(),
                        conflict_commit_ts: key_commit_ts,
                        key: current_key.into(),
                        primary: vec![],
                    }
                    .into());
                }
            }
        }
        // If we have not found `${user_key}_${ts}` in a few `next()`, directly `seek()`.
        if needs_seek {
            // `user_key` must have reserved space here, so its clone has reserved space too. So no
            // reallocation happens in `append_ts`.
            self.cursors.write.seek(
                &user_key.clone().append_ts(self.cfg.ts),
                &mut self.statistics.write,
            )?;
            if !self.cursors.write.valid()? {
                // Key space ended.
                return Ok(false);
            }
            let current_key = self.cursors.write.key(&mut self.statistics.write);
            if !Key::is_user_key_eq(current_key, user_key.as_encoded().as_slice()) {
                // Meet another key.
                return Ok(false);
            }
        }
        Ok(true)
    }
}

/// `ForwardScanner` with this policy outputs the latest key value pairs.
pub struct LatestKvPolicy;

impl<S: Snapshot> ScanPolicy<S> for LatestKvPolicy {
    type Output = (Key, Value);

    fn handle_lock(
        &mut self,
        current_user_key: Key,
        cfg: &mut ScannerConfig<S>,
        cursors: &mut Cursors<S>,
        statistics: &mut Statistics,
    ) -> Result<HandleRes<Self::Output>> {
        if cfg.isolation_level == IsolationLevel::Rc {
            return Ok(HandleRes::Skip(current_user_key));
        }
        // Only needs to check lock in SI
        let lock_cursor = cursors.lock.as_mut().unwrap();
        let lock = {
            let lock_value = lock_cursor.value(&mut statistics.lock);
            Lock::parse(lock_value)?
        };
        lock_cursor.next(&mut statistics.lock);
        if let Err(e) = Lock::check_ts_conflict(
            Cow::Borrowed(&lock),
            &current_user_key,
            cfg.ts,
            &cfg.bypass_locks,
            cfg.isolation_level,
        ) {
            statistics.lock.processed_keys += 1;
            // Skip current_user_key because this key is either blocked or handled.
            cursors.move_write_cursor_to_next_user_key(&current_user_key, statistics)?;
            if cfg.access_locks.contains(lock.ts) {
                cursors.ensure_default_cursor(cfg)?;
                return super::load_data_by_lock(
                    &current_user_key,
                    cfg,
                    cursors.default.as_mut().unwrap(),
                    lock,
                    statistics,
                )
                .map(|val| match val {
                    Some(v) => HandleRes::Return((current_user_key, v)),
                    None => HandleRes::MoveToNext,
                })
                .map_err(Into::into);
            }
            return Err(e.into());
        }
        Ok(HandleRes::Skip(current_user_key))
    }

    fn handle_write(
        &mut self,
        current_user_key: Key,
        cfg: &mut ScannerConfig<S>,
        cursors: &mut Cursors<S>,
        statistics: &mut Statistics,
    ) -> Result<HandleRes<Self::Output>> {
        let value: Option<Value> = loop {
            let write = WriteRef::parse(cursors.write.value(&mut statistics.write))?;

            if !write.check_gc_fence_as_latest_version(cfg.ts) {
                break None;
            }

            match write.write_type {
                WriteType::Put => {
                    if cfg.omit_value {
                        break Some(vec![]);
                    }
                    match write.short_value {
                        Some(value) => {
                            // Value is carried in `write`.
                            break Some(value.to_vec());
                        }
                        None => {
                            // Value is in the default CF.
                            let start_ts = write.start_ts;
                            cursors.ensure_default_cursor(cfg)?;
                            let value = super::near_load_data_by_write(
                                cursors.default.as_mut().unwrap(),
                                &current_user_key,
                                start_ts,
                                statistics,
                            )?;
                            break Some(value);
                        }
                    }
                }
                WriteType::Delete => break None,
                WriteType::Lock | WriteType::Rollback => {
                    // Continue iterate next `write`.
                }
            }

            cursors.write.next(&mut statistics.write);

            if !cursors.write.valid()? {
                // Key space ended. Needn't move write cursor to next key.
                return Ok(HandleRes::Skip(current_user_key));
            }
            let current_key = cursors.write.key(&mut statistics.write);
            if !Key::is_user_key_eq(current_key, current_user_key.as_encoded().as_slice()) {
                // Meet another key. Needn't move write cursor to next key.
                return Ok(HandleRes::Skip(current_user_key));
            }
        };
        cursors.move_write_cursor_to_next_user_key(&current_user_key, statistics)?;
        Ok(match value {
            Some(v) => HandleRes::Return((current_user_key, v)),
            _ => HandleRes::Skip(current_user_key),
        })
    }

    fn output_size(&mut self, output: &Self::Output) -> usize {
        output.0.len() + output.1.len()
    }
}

/// The ScanPolicy for outputting `TxnEntry`.
///
/// The `ForwardScanner` with this policy only outputs records whose commit_ts
/// is greater than `after_ts`. It also supports outputting delete records
/// if `output_delete` is set to `true`.
pub struct LatestEntryPolicy {
    after_ts: TimeStamp,
    output_delete: bool,
}

impl LatestEntryPolicy {
    pub fn new(after_ts: TimeStamp, output_delete: bool) -> Self {
        LatestEntryPolicy {
            after_ts,
            output_delete,
        }
    }
}

impl<S: Snapshot> ScanPolicy<S> for LatestEntryPolicy {
    type Output = TxnEntry;

    fn handle_lock(
        &mut self,
        current_user_key: Key,
        cfg: &mut ScannerConfig<S>,
        cursors: &mut Cursors<S>,
        statistics: &mut Statistics,
    ) -> Result<HandleRes<Self::Output>> {
        scan_latest_handle_lock(current_user_key, cfg, cursors, statistics)
    }

    fn handle_write(
        &mut self,
        current_user_key: Key,
        cfg: &mut ScannerConfig<S>,
        cursors: &mut Cursors<S>,
        statistics: &mut Statistics,
    ) -> Result<HandleRes<Self::Output>> {
        // Now we must have reached the first key >= `${user_key}_${ts}`. However, we may
        // meet `Lock` or `Rollback`. In this case, more versions needs to be looked up.
        let mut write_key = cursors.write.key(&mut statistics.write);
        let entry: Option<TxnEntry> = loop {
            if Key::decode_ts_from(write_key)? <= self.after_ts {
                // There are no newer records of this key since `after_ts`.
                break None;
            }
            let write_value = cursors.write.value(&mut statistics.write);
            let write = WriteRef::parse(write_value)?;

            if !write.check_gc_fence_as_latest_version(cfg.ts) {
                break None;
            }

            match write.write_type {
                WriteType::Put => {
                    let entry_write = (write_key.to_vec(), write_value.to_vec());
                    let entry_default = if write.short_value.is_none() {
                        let start_ts = write.start_ts;
                        cursors.ensure_default_cursor(cfg)?;
                        let default_cursor = cursors.default.as_mut().unwrap();
                        let default_value = super::near_load_data_by_write(
                            default_cursor,
                            &current_user_key,
                            start_ts,
                            statistics,
                        )?;
                        let default_key = default_cursor.key(&mut statistics.data).to_vec();
                        (default_key, default_value)
                    } else {
                        (Vec::new(), Vec::new())
                    };
                    break Some(TxnEntry::Commit {
                        default: entry_default,
                        write: entry_write,
                        old_value: OldValue::None,
                    });
                }
                WriteType::Delete => {
                    if self.output_delete {
                        break Some(TxnEntry::Commit {
                            default: (Vec::new(), Vec::new()),
                            write: (write_key.to_vec(), write_value.to_vec()),
                            old_value: OldValue::None,
                        });
                    } else {
                        break None;
                    }
                }
                _ => {}
            }

            cursors.write.next(&mut statistics.write);

            if !cursors.write.valid()? {
                // Key space ended. Needn't move write cursor to next key.
                return Ok(HandleRes::Skip(current_user_key));
            }
            write_key = cursors.write.key(&mut statistics.write);
            if !Key::is_user_key_eq(write_key, current_user_key.as_encoded().as_slice()) {
                // Meet another key. Needn't move write cursor to next key.
                return Ok(HandleRes::Skip(current_user_key));
            }
        };
        cursors.move_write_cursor_to_next_user_key(&current_user_key, statistics)?;
        Ok(match entry {
            Some(entry) => HandleRes::Return(entry),
            _ => HandleRes::Skip(current_user_key),
        })
    }

    fn output_size(&mut self, output: &Self::Output) -> usize {
        output.size()
    }
}

fn scan_latest_handle_lock<S: Snapshot, T>(
    current_user_key: Key,
    cfg: &mut ScannerConfig<S>,
    cursors: &mut Cursors<S>,
    statistics: &mut Statistics,
) -> Result<HandleRes<T>> {
    if cfg.isolation_level == IsolationLevel::Rc {
        return Ok(HandleRes::Skip(current_user_key));
    }
    // Only needs to check lock in SI
    let lock_cursor = cursors.lock.as_mut().unwrap();
    let lock = {
        let lock_value = lock_cursor.value(&mut statistics.lock);
        Lock::parse(lock_value)?
    };
    lock_cursor.next(&mut statistics.lock);

    Lock::check_ts_conflict(
        Cow::Owned(lock),
        &current_user_key,
        cfg.ts,
        &cfg.bypass_locks,
        cfg.isolation_level,
    )
    .or_else(|e| {
        // Even if there is a lock error, we still need to step the cursor for future
        // calls.
        statistics.lock.processed_keys += 1;
        cursors
            .move_write_cursor_to_next_user_key(&current_user_key, statistics)
            .and(Err(e.into()))
    })
    .map(|_| HandleRes::Skip(current_user_key))
}

/// The ScanPolicy for outputting `TxnEntry` for every locks or commits in specified ts range.
///
/// The `ForwardScanner` with this policy scans all entries whose `commit_ts`s
/// (or locks' `start_ts`s) in range (`from_ts`, `cfg.ts`].
pub struct DeltaEntryPolicy {
    from_ts: TimeStamp,
    extra_op: ExtraOp,
}

impl DeltaEntryPolicy {
    pub fn new(from_ts: TimeStamp, extra_op: ExtraOp) -> Self {
        Self { from_ts, extra_op }
    }
}

impl<S: Snapshot> ScanPolicy<S> for DeltaEntryPolicy {
    type Output = TxnEntry;

    fn handle_lock(
        &mut self,
        current_user_key: Key,
        cfg: &mut ScannerConfig<S>,
        cursors: &mut Cursors<S>,
        statistics: &mut Statistics,
    ) -> Result<HandleRes<Self::Output>> {
        if cfg.isolation_level == IsolationLevel::Rc {
            return Ok(HandleRes::Skip(current_user_key));
        }
        // TODO: Skip pessimistic locks.
        let lock_value = cursors
            .lock
            .as_mut()
            .unwrap()
            .value(&mut statistics.lock)
            .to_owned();
        let lock = Lock::parse(&lock_value)?;
        let result = if lock.ts > cfg.ts {
            Ok(HandleRes::Skip(current_user_key))
        } else {
            let load_default_res = if lock.lock_type == LockType::Put && lock.short_value.is_none()
            {
                let default_cursor = cursors.default.as_mut().unwrap();
                super::near_load_data_by_write(
                    default_cursor,
                    &current_user_key,
                    lock.ts,
                    statistics,
                )
                .map(|v| {
                    let key = default_cursor.key(&mut statistics.data).to_vec();
                    (key, v)
                })
            } else {
                Ok((vec![], vec![]))
            };

            let mut old_value = OldValue::None;
            if self.extra_op == ExtraOp::ReadOldValue
                && matches!(lock.lock_type, LockType::Put | LockType::Delete)
            {
                // When meet a lock, the write cursor must indicate the same user key.
                // Seek for the last valid committed here.
                old_value = super::seek_for_valid_value(
                    &mut cursors.write,
                    cursors.default.as_mut().unwrap(),
                    &current_user_key,
                    std::cmp::max(lock.ts, lock.for_update_ts),
                    self.from_ts,
                    cfg.hint_min_ts,
                    statistics,
                )?;
            }
            load_default_res.map(|default| {
                HandleRes::Return(TxnEntry::Prewrite {
                    default,
                    lock: (current_user_key.into_encoded(), lock_value),
                    old_value,
                })
            })
        };

        cursors.lock.as_mut().unwrap().next(&mut statistics.lock);

        result.map_err(Into::into)
    }

    fn handle_write(
        &mut self,
        current_user_key: Key,
        cfg: &mut ScannerConfig<S>,
        cursors: &mut Cursors<S>,
        statistics: &mut Statistics,
    ) -> Result<HandleRes<Self::Output>> {
        loop {
            let write_value = cursors.write.value(&mut statistics.write);
            let commit_ts = Key::decode_ts_from(cursors.write.key(&mut statistics.write))?;

            // commit_ts > cfg.ts never happens since the ForwardScanner will skip those greater
            // versions.

            if commit_ts <= self.from_ts {
                cursors.move_write_cursor_to_next_user_key(&current_user_key, statistics)?;
                return Ok(HandleRes::Skip(current_user_key));
            }

            let (write_type, start_ts, short_value) = {
                // DeltaEntryScanner only returns commit records between `from_ts` and `cfg.ts`.
                // We can assume that it must ensure GC safepoint doesn't exceed `from_ts`, so GC
                // fence checking can be skipped. But it's still needed when loading the old value.
                let write_ref = WriteRef::parse(write_value)?;
                (
                    write_ref.write_type,
                    write_ref.start_ts,
                    write_ref.short_value,
                )
            };

            if write_type == WriteType::Rollback || write_type == WriteType::Lock {
                // Skip it and try the next record.
                cursors.write.next(&mut statistics.write);
                if !cursors.write.valid()? {
                    return Ok(HandleRes::Skip(current_user_key));
                }
                if !Key::is_user_key_eq(
                    cursors.write.key(&mut statistics.write),
                    current_user_key.as_encoded(),
                ) {
                    return Ok(HandleRes::Skip(current_user_key));
                }

                continue;
            }

            let default = if write_type == WriteType::Put && short_value.is_none() {
                let default_cursor = cursors.default.as_mut().unwrap();
                let value = super::near_load_data_by_write(
                    default_cursor,
                    &current_user_key,
                    start_ts,
                    statistics,
                )?;
                let key = default_cursor.key(&mut statistics.data).to_vec();
                (key, value)
            } else {
                (vec![], vec![])
            };

            let write = (
                cursors.write.key(&mut statistics.write).to_owned(),
                cursors.write.value(&mut statistics.write).to_owned(),
            );
            // Move to the next write record early for getting the old value.
            cursors.write.next(&mut statistics.write);

            let mut old_value = OldValue::None;
            if self.extra_op == ExtraOp::ReadOldValue
                && matches!(write_type, WriteType::Put | WriteType::Delete)
            {
                old_value = super::seek_for_valid_value(
                    &mut cursors.write,
                    cursors.default.as_mut().unwrap(),
                    &current_user_key,
                    commit_ts.prev(),
                    self.from_ts,
                    cfg.hint_min_ts,
                    statistics,
                )?;
            }

            let res = Ok(HandleRes::Return(TxnEntry::Commit {
                default,
                write,
                old_value,
            }));

            return res;
        }
    }

    fn output_size(&mut self, output: &Self::Output) -> usize {
        output.size()
    }
}

/// This type can be used to scan keys starting from the given user key (greater than or equal).
///
/// Internally, for each key, rollbacks are ignored and smaller version will be tried. If the
/// isolation level is SI, locks will be checked first.
///
/// Use `ScannerBuilder` to build `ForwardKvScanner`.
pub type ForwardKvScanner<S> = ForwardScanner<S, LatestKvPolicy>;

/// This scanner is like `ForwardKvScanner` but outputs `TxnEntry`.
pub type EntryScanner<S> = ForwardScanner<S, LatestEntryPolicy>;

/// This scanner scans all entries whose commit_ts (or locks' start_ts) is in range
/// (from_ts, cfg.ts].
pub type DeltaScanner<S> = ForwardScanner<S, DeltaEntryPolicy>;

impl<S, P> TxnEntryScanner for ForwardScanner<S, P>
where
    S: Snapshot,
    P: ScanPolicy<S, Output = TxnEntry> + Send,
{
    fn next_entry(&mut self) -> TxnResult<Option<TxnEntry>> {
        Ok(self.read_next()?)
    }
    fn take_statistics(&mut self) -> Statistics {
        std::mem::take(&mut self.statistics)
    }
}

pub mod test_util {
    use super::*;
    use crate::storage::{
        mvcc::Write,
        txn::tests::{
            must_cleanup_with_gc_fence, must_commit, must_prewrite_delete, must_prewrite_lock,
            must_prewrite_put,
        },
        Engine,
    };

    pub struct EntryBuilder {
        pub key: Vec<u8>,
        pub value: Vec<u8>,
        pub primary: Vec<u8>,
        pub start_ts: TimeStamp,
        pub commit_ts: TimeStamp,
        pub for_update_ts: TimeStamp,
        pub old_value: OldValue,
    }

    impl Default for EntryBuilder {
        fn default() -> Self {
            EntryBuilder {
                key: vec![],
                value: vec![],
                primary: vec![],
                start_ts: 0.into(),
                commit_ts: 0.into(),
                for_update_ts: 0.into(),
                old_value: OldValue::None,
            }
        }
    }

    impl EntryBuilder {
        pub fn key(&mut self, key: &[u8]) -> &mut Self {
            self.key = key.to_owned();
            self
        }
        pub fn value(&mut self, val: &[u8]) -> &mut Self {
            self.value = val.to_owned();
            self
        }
        pub fn primary(&mut self, val: &[u8]) -> &mut Self {
            self.primary = val.to_owned();
            self
        }
        pub fn start_ts(&mut self, start_ts: TimeStamp) -> &mut Self {
            self.start_ts = start_ts;
            self
        }
        pub fn commit_ts(&mut self, commit_ts: TimeStamp) -> &mut Self {
            self.commit_ts = commit_ts;
            self
        }
        pub fn for_update_ts(&mut self, for_update_ts: TimeStamp) -> &mut Self {
            self.for_update_ts = for_update_ts;
            self
        }
        pub fn old_value(&mut self, old_value: &[u8]) -> &mut Self {
            self.old_value = OldValue::value(old_value.to_owned());
            self
        }
        pub fn build_commit(&self, wt: WriteType, is_short_value: bool) -> TxnEntry {
            let write_key = Key::from_raw(&self.key).append_ts(self.commit_ts);
            let (key, value, short) = if is_short_value {
                let short = if wt == WriteType::Put {
                    Some(self.value.clone())
                } else {
                    None
                };
                (vec![], vec![], short)
            } else {
                (
                    Key::from_raw(&self.key)
                        .append_ts(self.start_ts)
                        .into_encoded(),
                    self.value.clone(),
                    None,
                )
            };
            let write_value = Write::new(wt, self.start_ts, short);
            TxnEntry::Commit {
                default: (key, value),
                write: (write_key.into_encoded(), write_value.as_ref().to_bytes()),
                old_value: self.old_value.clone(),
            }
        }
        pub fn build_prewrite(&self, lt: LockType, is_short_value: bool) -> TxnEntry {
            let lock_key = Key::from_raw(&self.key);
            let (key, value, short) = if is_short_value {
                // TODO: Rollback records may use short value to mark it as protected. Keep it.
                let short = if lt == LockType::Put {
                    Some(self.value.clone())
                } else {
                    None
                };
                (vec![], vec![], short)
            } else {
                (
                    Key::from_raw(&self.key)
                        .append_ts(self.start_ts)
                        .into_encoded(),
                    self.value.clone(),
                    None,
                )
            };
            let lock_value = Lock::new(
                lt,
                self.primary.clone(),
                self.start_ts,
                0,
                short,
                self.for_update_ts,
                0,
                0.into(),
            );
            TxnEntry::Prewrite {
                default: (key, value),
                lock: (lock_key.into_encoded(), lock_value.to_bytes()),
                old_value: self.old_value.clone(),
            }
        }
        pub fn build_rollback(&self) -> TxnEntry {
            let write_key = Key::from_raw(&self.key).append_ts(self.start_ts);
            let write_value = Write::new(WriteType::Rollback, self.start_ts, None);
            // For now, rollback is enclosed in Commit.
            TxnEntry::Commit {
                default: (vec![], vec![]),
                write: (write_key.into_encoded(), write_value.as_ref().to_bytes()),
                old_value: OldValue::None,
            }
        }
    }

    #[allow(clippy::type_complexity)]
    pub fn prepare_test_data_for_check_gc_fence(
        engine: &impl Engine,
    ) -> (TimeStamp, Vec<(Vec<u8>, Option<Vec<u8>>)>) {
        // Generates test data that is consistent after timestamp 40.

        // PUT,   Read,  PUT
        //  `-------------^
        must_prewrite_put(engine, b"k1", b"v1", b"k1", 10);
        must_commit(engine, b"k1", 10, 20);
        // Put a record to be pointed by GC fence 50.
        must_prewrite_put(engine, b"k1", b"v1x", b"k1", 49);
        must_commit(engine, b"k1", 49, 50);
        must_cleanup_with_gc_fence(engine, b"k1", 20, 0, 50, false);

        // PUT,      Read
        //  `---------^
        must_prewrite_put(engine, b"k2", b"v2", b"k2", 11);
        must_commit(engine, b"k2", 11, 20);
        must_cleanup_with_gc_fence(engine, b"k2", 20, 0, 40, true);

        // PUT,      Read
        //  `-----^
        must_prewrite_put(engine, b"k3", b"v3", b"k3", 12);
        must_commit(engine, b"k3", 12, 20);
        must_cleanup_with_gc_fence(engine, b"k3", 20, 0, 30, true);

        // PUT,   PUT,       Read
        //  `-----^ `----^
        must_prewrite_put(engine, b"k4", b"v4", b"k4", 13);
        must_commit(engine, b"k4", 13, 14);
        must_prewrite_put(engine, b"k4", b"v4x", b"k4", 15);
        must_commit(engine, b"k4", 15, 20);
        must_cleanup_with_gc_fence(engine, b"k4", 14, 0, 20, false);
        must_cleanup_with_gc_fence(engine, b"k4", 20, 0, 30, true);

        // PUT,   DEL,       Read
        //  `-----^ `----^
        must_prewrite_put(engine, b"k5", b"v5", b"k5", 13);
        must_commit(engine, b"k5", 13, 14);
        must_prewrite_delete(engine, b"k5", b"v5", 15);
        must_commit(engine, b"k5", 15, 20);
        must_cleanup_with_gc_fence(engine, b"k5", 14, 0, 20, false);
        must_cleanup_with_gc_fence(engine, b"k5", 20, 0, 30, true);

        // PUT, LOCK, LOCK,   Read,  PUT
        //  `-------------------------^
        must_prewrite_put(engine, b"k6", b"v6", b"k6", 16);
        must_commit(engine, b"k6", 16, 20);
        must_prewrite_lock(engine, b"k6", b"k6", 25);
        must_commit(engine, b"k6", 25, 26);
        must_prewrite_lock(engine, b"k6", b"k6", 28);
        must_commit(engine, b"k6", 28, 29);
        // Put a record to be pointed by GC fence 50.
        must_prewrite_put(engine, b"k6", b"v6x", b"k6", 49);
        must_commit(engine, b"k6", 49, 50);
        must_cleanup_with_gc_fence(engine, b"k6", 20, 0, 50, false);

        // PUT, LOCK,   LOCK,   Read
        //  `---------^
        must_prewrite_put(engine, b"k7", b"v7", b"k7", 16);
        must_commit(engine, b"k7", 16, 20);
        must_prewrite_lock(engine, b"k7", b"k7", 25);
        must_commit(engine, b"k7", 25, 26);
        must_cleanup_with_gc_fence(engine, b"k7", 20, 0, 27, true);
        must_prewrite_lock(engine, b"k7", b"k7", 28);
        must_commit(engine, b"k7", 28, 29);

        // PUT,  Read
        //  * (GC fence ts is 0)
        must_prewrite_put(engine, b"k8", b"v8", b"k8", 17);
        must_commit(engine, b"k8", 17, 30);
        must_cleanup_with_gc_fence(engine, b"k8", 30, 0, 0, true);

        // PUT, LOCK,     Read
        // `-----------^
        must_prewrite_put(engine, b"k9", b"v9", b"k9", 18);
        must_commit(engine, b"k9", 18, 20);
        must_prewrite_lock(engine, b"k9", b"k9", 25);
        must_commit(engine, b"k9", 25, 26);
        must_cleanup_with_gc_fence(engine, b"k9", 20, 0, 27, true);

        // Returns the read ts to be checked and the expected result.
        (
            40.into(),
            vec![
                (b"k1".to_vec(), Some(b"v1".to_vec())),
                (b"k2".to_vec(), None),
                (b"k3".to_vec(), None),
                (b"k4".to_vec(), None),
                (b"k5".to_vec(), None),
                (b"k6".to_vec(), Some(b"v6".to_vec())),
                (b"k7".to_vec(), None),
                (b"k8".to_vec(), Some(b"v8".to_vec())),
                (b"k9".to_vec(), None),
            ],
        )
    }
}

#[cfg(test)]
mod latest_kv_tests {
    use engine_traits::{CF_LOCK, CF_WRITE};
    use kvproto::kvrpcpb::Context;

    use super::{super::ScannerBuilder, test_util::prepare_test_data_for_check_gc_fence, *};
    use crate::storage::{
        kv::{Engine, Modify, TestEngineBuilder},
        mvcc::tests::write,
        txn::tests::*,
        Scanner,
    };

    /// Check whether everything works as usual when `ForwardKvScanner::get()` goes out of bound.
    #[test]
    fn test_get_out_of_bound() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let ctx = Context::default();

        // Generate 1 put for [a].
        must_prewrite_put(&engine, b"a", b"value", b"a", 7);
        must_commit(&engine, b"a", 7, 7);

        // Generate 5 rollback for [b].
        for ts in 0..5 {
            let modifies = vec![
                // ts is rather small, so it is ok to `as u8`
                Modify::Put(
                    CF_WRITE,
                    Key::from_raw(b"b").append_ts(TimeStamp::new(ts)),
                    vec![b'R', ts as u8],
                ),
                Modify::Delete(CF_LOCK, Key::from_raw(b"b")),
            ];
            write(&engine, &ctx, modifies);
        }

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, 10.into())
            .range(None, None)
            .build()
            .unwrap();

        // Initial position: 1 seek_to_first:
        //   a_7 b_4 b_3 b_2 b_1 b_0
        //   ^cursor
        // After get the value, use 1 next to reach next user key:
        //   a_7 b_4 b_3 b_2 b_1 b_0
        //       ^cursor
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"a"), b"value".to_vec())),
        );
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, 1);
        assert_eq!(
            statistics.processed_size,
            Key::from_raw(b"a").len() + b"value".len()
        );

        // Use 5 next and reach out of bound:
        //   a_7 b_4 b_3 b_2 b_1 b_0
        //                           ^cursor
        assert_eq!(scanner.next().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 5);
        assert_eq!(statistics.processed_size, 0);

        // Cursor remains invalid, so nothing should happen.
        assert_eq!(scanner.next().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 0);
        assert_eq!(statistics.processed_size, 0);
    }

    /// Check whether everything works as usual when
    /// `ForwardKvScanner::move_write_cursor_to_next_user_key()` goes out of bound.
    ///
    /// Case 1. next() out of bound
    #[test]
    fn test_move_next_user_key_out_of_bound_1() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let ctx = Context::default();
        // Generate 1 put for [a].
        must_prewrite_put(&engine, b"a", b"a_value", b"a", SEEK_BOUND * 2);
        must_commit(&engine, b"a", SEEK_BOUND * 2, SEEK_BOUND * 2);

        // Generate SEEK_BOUND / 2 rollback and 1 put for [b] .
        for ts in 0..SEEK_BOUND / 2 {
            let modifies = vec![
                // ts is rather small, so it is ok to `as u8`
                Modify::Put(
                    CF_WRITE,
                    Key::from_raw(b"b").append_ts(TimeStamp::new(ts)),
                    vec![b'R', ts as u8],
                ),
                Modify::Delete(CF_LOCK, Key::from_raw(b"b")),
            ];
            write(&engine, &ctx, modifies);
        }
        must_prewrite_put(&engine, b"b", b"b_value", b"a", SEEK_BOUND / 2);
        must_commit(&engine, b"b", SEEK_BOUND / 2, SEEK_BOUND / 2);

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, (SEEK_BOUND * 2).into())
            .range(None, None)
            .build()
            .unwrap();

        // The following illustration comments assume that SEEK_BOUND = 4.

        // Initial position: 1 seek_to_first:
        //   a_8 b_2 b_1 b_0
        //   ^cursor
        // After get the value, use 1 next to reach next user key:
        //   a_8 b_2 b_1 b_0
        //       ^cursor
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"a"), b"a_value".to_vec())),
        );
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, 1);
        assert_eq!(
            statistics.processed_size,
            Key::from_raw(b"a").len() + b"a_value".len()
        );

        // Before:
        //   a_8 b_2 b_1 b_0
        //       ^cursor
        // We should be able to get wanted value without any operation.
        // After get the value, use SEEK_BOUND / 2 + 1 next to reach next user key and stop:
        //   a_8 b_2 b_1 b_0
        //                   ^cursor
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"b"), b"b_value".to_vec())),
        );
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, (SEEK_BOUND / 2 + 1) as usize);
        assert_eq!(
            statistics.processed_size,
            Key::from_raw(b"b").len() + b"b_value".len()
        );

        // Next we should get nothing.
        assert_eq!(scanner.next().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 0);
        assert_eq!(statistics.processed_size, 0);
    }

    /// Check whether everything works as usual when
    /// `ForwardKvScanner::move_write_cursor_to_next_user_key()` goes out of bound.
    ///
    /// Case 2. seek() out of bound
    #[test]
    fn test_move_next_user_key_out_of_bound_2() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let ctx = Context::default();

        // Generate 1 put for [a].
        must_prewrite_put(&engine, b"a", b"a_value", b"a", SEEK_BOUND * 2);
        must_commit(&engine, b"a", SEEK_BOUND * 2, SEEK_BOUND * 2);

        // Generate SEEK_BOUND-1 rollback and 1 put for [b] .
        for ts in 1..SEEK_BOUND {
            let modifies = vec![
                // ts is rather small, so it is ok to `as u8`
                Modify::Put(
                    CF_WRITE,
                    Key::from_raw(b"b").append_ts(TimeStamp::new(ts)),
                    vec![b'R', ts as u8],
                ),
                Modify::Delete(CF_LOCK, Key::from_raw(b"b")),
            ];
            write(&engine, &ctx, modifies);
        }
        must_prewrite_put(&engine, b"b", b"b_value", b"a", SEEK_BOUND);
        must_commit(&engine, b"b", SEEK_BOUND, SEEK_BOUND);

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, (SEEK_BOUND * 2).into())
            .range(None, None)
            .build()
            .unwrap();

        // The following illustration comments assume that SEEK_BOUND = 4.

        // Initial position: 1 seek_to_first:
        //   a_8 b_4 b_3 b_2 b_1
        //   ^cursor
        // After get the value, use 1 next to reach next user key:
        //   a_8 b_4 b_3 b_2 b_1
        //       ^cursor
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"a"), b"a_value".to_vec())),
        );
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, 1);
        assert_eq!(
            statistics.processed_size,
            Key::from_raw(b"a").len() + b"a_value".len()
        );

        // Before:
        //   a_8 b_4 b_3 b_2 b_1
        //       ^cursor
        // We should be able to get wanted value without any operation.
        // After get the value, use SEEK_BOUND-1 next: (TODO: fix it to SEEK_BOUND)
        //   a_8 b_4 b_3 b_2 b_1
        //                   ^cursor
        // We still pointing at current user key, so a seek:
        //   a_8 b_4 b_3 b_2 b_1
        //                       ^cursor
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"b"), b"b_value".to_vec())),
        );
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, (SEEK_BOUND - 1) as usize);
        assert_eq!(
            statistics.processed_size,
            Key::from_raw(b"b").len() + b"b_value".len()
        );

        // Next we should get nothing.
        assert_eq!(scanner.next().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 0);
        assert_eq!(statistics.processed_size, 0);
    }

    /// Range is left open right closed.
    #[test]
    fn test_range() {
        let engine = TestEngineBuilder::new().build().unwrap();

        // Generate 1 put for [1], [2] ... [6].
        for i in 1..7 {
            // ts = 1: value = []
            must_prewrite_put(&engine, &[i], &[], &[i], 1);
            must_commit(&engine, &[i], 1, 1);

            // ts = 7: value = [ts]
            must_prewrite_put(&engine, &[i], &[i], &[i], 7);
            must_commit(&engine, &[i], 7, 7);

            // ts = 14: value = []
            must_prewrite_put(&engine, &[i], &[], &[i], 14);
            must_commit(&engine, &[i], 14, 14);
        }

        let snapshot = engine.snapshot(Default::default()).unwrap();

        // Test both bound specified.
        let mut scanner = ScannerBuilder::new(snapshot.clone(), 10.into())
            .range(Some(Key::from_raw(&[3u8])), Some(Key::from_raw(&[5u8])))
            .build()
            .unwrap();
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[3u8]), vec![3u8]))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[4u8]), vec![4u8]))
        );
        assert_eq!(scanner.next().unwrap(), None);
        assert_eq!(
            scanner.take_statistics().processed_size,
            Key::from_raw(&[3u8]).len()
                + vec![3u8].len()
                + Key::from_raw(&[4u8]).len()
                + vec![4u8].len()
        );

        // Test left bound not specified.
        let mut scanner = ScannerBuilder::new(snapshot.clone(), 10.into())
            .range(None, Some(Key::from_raw(&[3u8])))
            .build()
            .unwrap();
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[1u8]), vec![1u8]))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[2u8]), vec![2u8]))
        );
        assert_eq!(scanner.next().unwrap(), None);
        assert_eq!(
            scanner.take_statistics().processed_size,
            Key::from_raw(&[1u8]).len()
                + vec![1u8].len()
                + Key::from_raw(&[2u8]).len()
                + vec![2u8].len()
        );

        // Test right bound not specified.
        let mut scanner = ScannerBuilder::new(snapshot.clone(), 10.into())
            .range(Some(Key::from_raw(&[5u8])), None)
            .build()
            .unwrap();
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[5u8]), vec![5u8]))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[6u8]), vec![6u8]))
        );
        assert_eq!(scanner.next().unwrap(), None);
        assert_eq!(
            scanner.take_statistics().processed_size,
            Key::from_raw(&[5u8]).len()
                + vec![5u8].len()
                + Key::from_raw(&[6u8]).len()
                + vec![6u8].len()
        );

        // Test both bound not specified.
        let mut scanner = ScannerBuilder::new(snapshot, 10.into())
            .range(None, None)
            .build()
            .unwrap();
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[1u8]), vec![1u8]))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[2u8]), vec![2u8]))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[3u8]), vec![3u8]))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[4u8]), vec![4u8]))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[5u8]), vec![5u8]))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(&[6u8]), vec![6u8]))
        );
        assert_eq!(scanner.next().unwrap(), None);
        assert_eq!(
            scanner.take_statistics().processed_size,
            (1u8..=6u8)
                .map(|k| Key::from_raw(&[k]).len() + vec![k].len())
                .sum::<usize>()
        );
    }

    #[test]
    fn test_latest_kv_check_gc_fence() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let (read_ts, expected_result) = prepare_test_data_for_check_gc_fence(&engine);
        let expected_result: Vec<_> = expected_result
            .into_iter()
            .filter_map(|(key, value)| value.map(|v| (key, v)))
            .collect();

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, read_ts)
            .range(None, None)
            .build()
            .unwrap();
        let result: Vec<_> = scanner
            .scan(100, 0)
            .unwrap()
            .into_iter()
            .map(|result| result.unwrap())
            .collect();
        assert_eq!(result, expected_result);
    }

    #[test]
    fn test_rc_read_check_ts() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let (key0, val0) = (b"k0", b"v0");
        must_prewrite_put(&engine, key0, val0, key0, 1);
        must_commit(&engine, key0, 1, 5);

        let (key1, val1) = (b"k1", b"v1");
        must_prewrite_put(&engine, key1, val1, key1, 10);
        must_commit(&engine, key1, 10, 20);

        let (key2, val2, val22) = (b"k2", b"v2", b"v22");
        must_prewrite_put(&engine, key2, val2, key2, 30);
        must_commit(&engine, key2, 30, 40);
        must_prewrite_put(&engine, key2, val22, key2, 41);
        must_commit(&engine, key2, 41, 42);

        let (key3, val3) = (b"k3", b"v3");
        must_prewrite_put(&engine, key3, val3, key3, 50);
        must_commit(&engine, key3, 50, 51);

        let (key4, val4) = (b"k4", b"val4");
        must_prewrite_put(&engine, key4, val4, key4, 55);
        must_commit(&engine, key4, 55, 56);
        must_prewrite_lock(&engine, key4, key4, 60);

        let (key5, val5) = (b"k5", b"val5");
        must_prewrite_put(&engine, key5, val5, key5, 57);
        must_commit(&engine, key5, 57, 58);
        must_acquire_pessimistic_lock(&engine, key5, key5, 65, 65);

        let (key6, val6) = (b"k6", b"v6");
        must_prewrite_put(&engine, key6, val6, key6, 75);

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, 35.into())
            .range(None, None)
            .isolation_level(IsolationLevel::RcCheckTs)
            .build()
            .unwrap();

        // Scanner has met a more recent version.
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(key0), val0.to_vec()))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(key1), val1.to_vec()))
        );
        assert!(scanner.next().is_err());

        // Scanner has met a lock though lock.ts > read_ts.
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, 70.into())
            .range(None, None)
            .isolation_level(IsolationLevel::RcCheckTs)
            .build()
            .unwrap();
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(key0), val0.to_vec()))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(key1), val1.to_vec()))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(key2), val22.to_vec()))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(key3), val3.to_vec()))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(key4), val4.to_vec()))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(key5), val5.to_vec()))
        );
        assert!(scanner.next().is_err());
    }
}

#[cfg(test)]
mod latest_entry_tests {
    use engine_traits::{CF_LOCK, CF_WRITE};
    use kvproto::kvrpcpb::Context;

    use super::{super::ScannerBuilder, test_util::*, *};
    use crate::storage::{
        mvcc::tests::write,
        txn::{
            tests::{must_commit, must_prewrite_delete, must_prewrite_put},
            EntryBatch,
        },
        Engine, Modify, TestEngineBuilder,
    };

    /// Check whether everything works as usual when `EntryScanner::get()` goes out of bound.
    #[test]
    fn test_get_out_of_bound() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let ctx = Context::default();

        // Generate 1 put for [a].
        must_prewrite_put(&engine, b"a", b"value", b"a", 7);
        must_commit(&engine, b"a", 7, 7);

        // Generate 5 rollback for [b].
        for ts in 0..5 {
            let modifies = vec![
                // ts is rather small, so it is ok to `as u8`
                Modify::Put(
                    CF_WRITE,
                    Key::from_raw(b"b").append_ts(TimeStamp::new(ts)),
                    vec![b'R', ts as u8],
                ),
                Modify::Delete(CF_LOCK, Key::from_raw(b"b")),
            ];
            write(&engine, &ctx, modifies);
        }

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, 10.into())
            .range(None, None)
            .build_entry_scanner(0.into(), false)
            .unwrap();

        // Initial position: 1 seek_to_first:
        //   a_7 b_4 b_3 b_2 b_1 b_0
        //   ^cursor
        // After get the value, use 1 next to reach next user key:
        //   a_7 b_4 b_3 b_2 b_1 b_0
        //       ^cursor

        let mut builder: EntryBuilder = EntryBuilder::default();
        let entry = builder
            .key(b"a")
            .value(b"value")
            .start_ts(7.into())
            .commit_ts(7.into())
            .build_commit(WriteType::Put, true);
        let size = entry.size();
        assert_eq!(scanner.next_entry().unwrap(), Some(entry),);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, 1);
        assert_eq!(statistics.processed_size, size);

        // Use 5 next and reach out of bound:
        //   a_7 b_4 b_3 b_2 b_1 b_0
        //                           ^cursor
        assert_eq!(scanner.next_entry().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 5);
        assert_eq!(statistics.processed_size, 0);

        // Cursor remains invalid, so nothing should happen.
        assert_eq!(scanner.next_entry().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 0);
        assert_eq!(statistics.processed_size, 0);
    }

    /// Check whether everything works as usual when
    /// `EntryScanner::move_write_cursor_to_next_user_key()` goes out of bound.
    ///
    /// Case 1. next() out of bound
    #[test]
    fn test_move_next_user_key_out_of_bound_1() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let ctx = Context::default();

        // Generate 1 put for [a].
        must_prewrite_put(&engine, b"a", b"a_value", b"a", SEEK_BOUND * 2);
        must_commit(&engine, b"a", SEEK_BOUND * 2, SEEK_BOUND * 2);

        // Generate SEEK_BOUND / 2 rollback and 1 put for [b] .
        for ts in 0..SEEK_BOUND / 2 {
            let modifies = vec![
                // ts is rather small, so it is ok to `as u8`
                Modify::Put(
                    CF_WRITE,
                    Key::from_raw(b"b").append_ts(TimeStamp::new(ts)),
                    vec![b'R', ts as u8],
                ),
                Modify::Delete(CF_LOCK, Key::from_raw(b"b")),
            ];
            write(&engine, &ctx, modifies);
        }
        must_prewrite_put(&engine, b"b", b"b_value", b"a", SEEK_BOUND / 2);
        must_commit(&engine, b"b", SEEK_BOUND / 2, SEEK_BOUND / 2);

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, (SEEK_BOUND * 2).into())
            .range(None, None)
            .build_entry_scanner(0.into(), false)
            .unwrap();

        // The following illustration comments assume that SEEK_BOUND = 4.

        // Initial position: 1 seek_to_first:
        //   a_8 b_2 b_1 b_0
        //   ^cursor
        // After get the value, use 1 next to reach next user key:
        //   a_8 b_2 b_1 b_0
        //       ^cursor
        let entry = EntryBuilder::default()
            .key(b"a")
            .value(b"a_value")
            .start_ts(16.into())
            .commit_ts(16.into())
            .build_commit(WriteType::Put, true);
        let size = entry.size();
        assert_eq!(scanner.next_entry().unwrap(), Some(entry),);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, 1);
        assert_eq!(statistics.processed_size, size);

        // Before:
        //   a_8 b_2 b_1 b_0
        //       ^cursor
        // We should be able to get wanted value without any operation.
        // After get the value, use SEEK_BOUND / 2 + 1 next to reach next user key and stop:
        //   a_8 b_2 b_1 b_0
        //                   ^cursor
        let entry = EntryBuilder::default()
            .key(b"b")
            .value(b"b_value")
            .start_ts(4.into())
            .commit_ts(4.into())
            .build_commit(WriteType::Put, true);
        let size = entry.size();
        assert_eq!(scanner.next_entry().unwrap(), Some(entry),);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, (SEEK_BOUND / 2 + 1) as usize);
        assert_eq!(statistics.processed_size, size);

        // Next we should get nothing.
        assert_eq!(scanner.next_entry().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 0);
        assert_eq!(statistics.processed_size, 0);
    }

    /// Check whether everything works as usual when
    /// `EntryScanner::move_write_cursor_to_next_user_key()` goes out of bound.
    ///
    /// Case 2. seek() out of bound
    #[test]
    fn test_move_next_user_key_out_of_bound_2() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let ctx = Context::default();

        // Generate 1 put for [a].
        must_prewrite_put(&engine, b"a", b"a_value", b"a", SEEK_BOUND * 2);
        must_commit(&engine, b"a", SEEK_BOUND * 2, SEEK_BOUND * 2);

        // Generate SEEK_BOUND-1 rollback and 1 put for [b] .
        for ts in 1..SEEK_BOUND {
            let modifies = vec![
                // ts is rather small, so it is ok to `as u8`
                Modify::Put(
                    CF_WRITE,
                    Key::from_raw(b"b").append_ts(TimeStamp::new(ts)),
                    vec![b'R', ts as u8],
                ),
                Modify::Delete(CF_LOCK, Key::from_raw(b"b")),
            ];
            write(&engine, &ctx, modifies);
        }
        must_prewrite_put(&engine, b"b", b"b_value", b"a", SEEK_BOUND);
        must_commit(&engine, b"b", SEEK_BOUND, SEEK_BOUND);

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, (SEEK_BOUND * 2).into())
            .range(None, None)
            .build_entry_scanner(0.into(), false)
            .unwrap();

        // The following illustration comments assume that SEEK_BOUND = 4.

        // Initial position: 1 seek_to_first:
        //   a_8 b_4 b_3 b_2 b_1
        //   ^cursor
        // After get the value, use 1 next to reach next user key:
        //   a_8 b_4 b_3 b_2 b_1
        //       ^cursor
        let entry = EntryBuilder::default()
            .key(b"a")
            .value(b"a_value")
            .start_ts(16.into())
            .commit_ts(16.into())
            .build_commit(WriteType::Put, true);
        let size = entry.size();
        assert_eq!(scanner.next_entry().unwrap(), Some(entry));
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, 1);
        assert_eq!(statistics.processed_size, size);

        // Before:
        //   a_8 b_4 b_3 b_2 b_1
        //       ^cursor
        // We should be able to get wanted value without any operation.
        // After get the value, use SEEK_BOUND-1 next: (TODO: fix it to SEEK_BOUND)
        //   a_8 b_4 b_3 b_2 b_1
        //                   ^cursor
        // We still pointing at current user key, so a seek:
        //   a_8 b_4 b_3 b_2 b_1
        //                       ^cursor
        let entry = EntryBuilder::default()
            .key(b"b")
            .value(b"b_value")
            .start_ts(8.into())
            .commit_ts(8.into())
            .build_commit(WriteType::Put, true);
        let size = entry.size();
        assert_eq!(scanner.next_entry().unwrap(), Some(entry),);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, (SEEK_BOUND - 1) as usize);
        assert_eq!(statistics.processed_size, size);

        // Next we should get nothing.
        assert_eq!(scanner.next_entry().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 0);
        assert_eq!(statistics.processed_size, 0);
    }

    /// Range is left open right closed.
    #[test]
    fn test_range() {
        let engine = TestEngineBuilder::new().build().unwrap();

        // Generate 1 put for [1], [2] ... [6].
        for i in 1..7 {
            // ts = 1: value = []
            must_prewrite_put(&engine, &[i], &[], &[i], 1);
            must_commit(&engine, &[i], 1, 1);

            // ts = 7: value = [ts]
            must_prewrite_put(&engine, &[i], &[i], &[i], 7);
            must_commit(&engine, &[i], 7, 7);

            // ts = 14: value = []
            must_prewrite_put(&engine, &[i], &[], &[i], 14);
            must_commit(&engine, &[i], 14, 14);
        }

        let snapshot = engine.snapshot(Default::default()).unwrap();

        // Test both bound specified.
        let mut scanner = ScannerBuilder::new(snapshot.clone(), 10.into())
            .range(Some(Key::from_raw(&[3u8])), Some(Key::from_raw(&[5u8])))
            .build_entry_scanner(0.into(), false)
            .unwrap();

        let entry = |key, ts| {
            EntryBuilder::default()
                .key(key)
                .value(key)
                .start_ts(ts)
                .commit_ts(ts)
                .build_commit(WriteType::Put, true)
        };

        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[3u8], 7.into())));
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[4u8], 7.into())));
        assert_eq!(scanner.next_entry().unwrap(), None);

        // Test left bound not specified.
        let mut scanner = ScannerBuilder::new(snapshot.clone(), 10.into())
            .range(None, Some(Key::from_raw(&[3u8])))
            .build_entry_scanner(0.into(), false)
            .unwrap();
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[1u8], 7.into())));
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[2u8], 7.into())));
        assert_eq!(scanner.next_entry().unwrap(), None);

        // Test right bound not specified.
        let mut scanner = ScannerBuilder::new(snapshot.clone(), 10.into())
            .range(Some(Key::from_raw(&[5u8])), None)
            .build_entry_scanner(0.into(), false)
            .unwrap();
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[5u8], 7.into())));
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[6u8], 7.into())));
        assert_eq!(scanner.next_entry().unwrap(), None);

        // Test both bound not specified.
        let mut scanner = ScannerBuilder::new(snapshot, 10.into())
            .range(None, None)
            .build_entry_scanner(0.into(), false)
            .unwrap();
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[1u8], 7.into())));
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[2u8], 7.into())));
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[3u8], 7.into())));
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[4u8], 7.into())));
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[5u8], 7.into())));
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[6u8], 7.into())));
        assert_eq!(scanner.next_entry().unwrap(), None);
    }

    #[test]
    fn test_output_delete_and_after_ts() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let ctx = Context::default();

        // Generate put for [a] at 3.
        must_prewrite_put(&engine, b"a", b"a_3", b"a", 3);
        must_commit(&engine, b"a", 3, 3);

        // Generate put for [a] at 7.
        must_prewrite_put(&engine, b"a", b"a_7", b"a", 7);
        must_commit(&engine, b"a", 7, 7);

        // Generate put for [b] at 1.
        must_prewrite_put(&engine, b"b", b"b_1", b"b", 1);
        must_commit(&engine, b"b", 1, 1);

        // Generate rollbacks for [b] at 2, 3, 4.
        for ts in 2..5 {
            let modifies = vec![
                // ts is rather small, so it is ok to `as u8`
                Modify::Put(
                    CF_WRITE,
                    Key::from_raw(b"b").append_ts(TimeStamp::new(ts)),
                    vec![b'R', ts as u8],
                ),
                Modify::Delete(CF_LOCK, Key::from_raw(b"b")),
            ];
            write(&engine, &ctx, modifies);
        }

        // Generate delete for [b] at 10.
        must_prewrite_delete(&engine, b"b", b"b", 10);
        must_commit(&engine, b"b", 10, 10);

        let entry_a_3 = EntryBuilder::default()
            .key(b"a")
            .value(b"a_3")
            .start_ts(3.into())
            .commit_ts(3.into())
            .build_commit(WriteType::Put, true);
        let entry_a_7 = EntryBuilder::default()
            .key(b"a")
            .value(b"a_7")
            .start_ts(7.into())
            .commit_ts(7.into())
            .build_commit(WriteType::Put, true);
        let entry_b_1 = EntryBuilder::default()
            .key(b"b")
            .value(b"b_1")
            .start_ts(1.into())
            .commit_ts(1.into())
            .build_commit(WriteType::Put, true);
        let entry_b_10 = EntryBuilder::default()
            .key(b"b")
            .start_ts(10.into())
            .commit_ts(10.into())
            .build_commit(WriteType::Delete, true);

        let check = |ts: u64, after_ts: u64, output_delete, expected: Vec<&TxnEntry>| {
            let snapshot = engine.snapshot(Default::default()).unwrap();
            let mut scanner = ScannerBuilder::new(snapshot, ts.into())
                .range(None, None)
                .build_entry_scanner(after_ts.into(), output_delete)
                .unwrap();
            for entry in expected {
                assert_eq!(scanner.next_entry().unwrap().as_ref(), Some(entry));
            }
            assert!(scanner.next_entry().unwrap().is_none());
        };

        // Scanning entries in (10, 15] should get None
        check(15, 10, true, vec![]);
        // Scanning entries without delete in (7, 10] should get None
        check(10, 7, false, vec![]);
        // Scanning entries include delete in (7, 10] should get entry_b_10
        check(10, 7, true, vec![&entry_b_10]);
        // Scanning entries include delete in (3, 10] should get a_7 and b_10
        check(10, 3, true, vec![&entry_a_7, &entry_b_10]);
        // Scanning entries in (0, 5] should get a_3 and b_1
        check(5, 0, true, vec![&entry_a_3, &entry_b_1]);
        // Scanning entries without delete in (0, 10] should get a_7
        check(10, 0, false, vec![&entry_a_7]);
    }

    #[test]
    fn test_latest_entry_check_gc_fence() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let (read_ts, expected_result) = prepare_test_data_for_check_gc_fence(&engine);
        let expected_result: Vec<_> = expected_result
            .into_iter()
            .filter_map(|(key, value)| value.map(|v| (key, v)))
            .collect();

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, read_ts)
            .range(None, None)
            .build_entry_scanner(0.into(), false)
            .unwrap();
        let mut result = EntryBatch::with_capacity(20);
        scanner.scan_entries(&mut result).unwrap();
        let result: Vec<_> = result
            .drain()
            .map(|entry| entry.into_kvpair().unwrap())
            .collect();

        assert_eq!(result, expected_result);
    }
}

#[cfg(test)]
mod delta_entry_tests {
    use engine_traits::{CF_LOCK, CF_WRITE};
    use kvproto::kvrpcpb::Context;
    use txn_types::{is_short_value, SHORT_VALUE_MAX_LEN};

    use super::{super::ScannerBuilder, test_util::*, *};
    use crate::storage::{mvcc::tests::write, txn::tests::*, Engine, Modify, TestEngineBuilder};
    /// Check whether everything works as usual when `Delta::get()` goes out of bound.
    #[test]
    fn test_get_out_of_bound() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let ctx = Context::default();

        // Generate 1 put for [a].
        must_prewrite_put(&engine, b"a", b"value", b"a", 7);
        must_commit(&engine, b"a", 7, 7);

        // Generate 5 rollback for [b].
        for ts in 0..5 {
            let modifies = vec![
                // ts is rather small, so it is ok to `as u8`
                Modify::Put(
                    CF_WRITE,
                    Key::from_raw(b"b").append_ts(TimeStamp::new(ts)),
                    vec![b'R', ts as u8],
                ),
                Modify::Delete(CF_LOCK, Key::from_raw(b"b")),
            ];
            write(&engine, &ctx, modifies);
        }

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, 10.into())
            .range(None, None)
            .build_delta_scanner(0.into(), ExtraOp::Noop)
            .unwrap();

        // Initial position: 1 seek_to_first:
        //   a_7 b_4 b_3 b_2 b_1 b_0
        //   ^cursor
        // After get the value, use 1 next to reach next user key:
        //   a_7 b_4 b_3 b_2 b_1 b_0
        //       ^cursor

        let mut builder: EntryBuilder = EntryBuilder::default();
        let entry = builder
            .key(b"a")
            .value(b"value")
            .start_ts(7.into())
            .commit_ts(7.into())
            .build_commit(WriteType::Put, true);
        let size = entry.size();
        assert_eq!(scanner.next_entry().unwrap(), Some(entry),);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, 1);
        assert_eq!(statistics.processed_size, size);

        // Use 5 next and reach out of bound:
        //   a_7 b_4 b_3 b_2 b_1 b_0
        //                           ^cursor
        assert_eq!(scanner.next_entry().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 5);
        assert_eq!(statistics.processed_size, 0);

        // Cursor remains invalid, so nothing should happen.
        assert_eq!(scanner.next_entry().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 0);
        assert_eq!(statistics.processed_size, 0);
    }

    /// Check whether everything works as usual when
    /// `DeltaScanner::move_write_cursor_to_next_user_key()` goes out of bound.
    ///
    /// Case 1. next() out of bound
    #[test]
    fn test_move_next_user_key_out_of_bound_1() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let ctx = Context::default();
        // Generate 1 put for [a].
        must_prewrite_put(&engine, b"a", b"a_value", b"a", SEEK_BOUND * 2);
        must_commit(&engine, b"a", SEEK_BOUND * 2, SEEK_BOUND * 2);

        // Generate SEEK_BOUND / 2 rollback and 1 put for [b] .
        for ts in 0..SEEK_BOUND / 2 {
            let modifies = vec![
                // ts is rather small, so it is ok to `as u8`
                Modify::Put(
                    CF_WRITE,
                    Key::from_raw(b"b").append_ts(TimeStamp::new(ts)),
                    vec![b'R', ts as u8],
                ),
                Modify::Delete(CF_LOCK, Key::from_raw(b"b")),
            ];
            write(&engine, &ctx, modifies);
        }
        must_prewrite_put(&engine, b"b", b"b_value", b"a", SEEK_BOUND / 2);
        must_commit(&engine, b"b", SEEK_BOUND / 2, SEEK_BOUND / 2);

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, (SEEK_BOUND * 2).into())
            .range(None, None)
            .build_delta_scanner(0.into(), ExtraOp::Noop)
            .unwrap();

        // The following illustration comments assume that SEEK_BOUND = 4.

        // Initial position: 1 seek_to_first:
        //   a_8 b_2 b_1 b_0
        //   ^cursor
        // After get the value, use 1 next to reach next user key:
        //   a_8 b_2 b_1 b_0
        //       ^cursor
        let entry = EntryBuilder::default()
            .key(b"a")
            .value(b"a_value")
            .start_ts(16.into())
            .commit_ts(16.into())
            .build_commit(WriteType::Put, true);
        let size = entry.size();
        assert_eq!(scanner.next_entry().unwrap(), Some(entry),);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, 1);
        assert_eq!(statistics.processed_size, size);

        // Before:
        //   a_8 b_2 b_1 b_0
        //       ^cursor
        // We should be able to get wanted value without any operation.
        // After get the value, use SEEK_BOUND / 2 + 1 next to reach next user key and stop:
        //   a_8 b_2 b_1 b_0
        //           ^cursor
        let entry = EntryBuilder::default()
            .key(b"b")
            .value(b"b_value")
            .start_ts(4.into())
            .commit_ts(4.into())
            .build_commit(WriteType::Put, true);
        let size = entry.size();
        assert_eq!(scanner.next_entry().unwrap(), Some(entry),);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 1);
        assert_eq!(statistics.processed_size, size);

        // Next we should get nothing.
        assert_eq!(scanner.next_entry().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 4);
        assert_eq!(statistics.processed_size, 0);
    }

    /// Check whether everything works as usual when
    /// `EntryScanner::move_write_cursor_to_next_user_key()` goes out of bound.
    ///
    /// Case 2. seek() out of bound
    #[test]
    fn test_move_next_user_key_out_of_bound_2() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let ctx = Context::default();

        // Generate 1 put for [a].
        must_prewrite_put(&engine, b"a", b"a_value", b"a", SEEK_BOUND * 2);
        must_commit(&engine, b"a", SEEK_BOUND * 2, SEEK_BOUND * 2);

        // Generate SEEK_BOUND rollback and 1 put for [b] .
        // It differs from EntryScanner that this will try to fetch multiple versions of each key.
        // So in this test it needs one more next than EntryScanner.
        for ts in 1..=SEEK_BOUND {
            let modifies = vec![
                // ts is rather small, so it is ok to `as u8`
                Modify::Put(
                    CF_WRITE,
                    Key::from_raw(b"b").append_ts(TimeStamp::new(ts)),
                    vec![b'R', ts as u8],
                ),
                Modify::Delete(CF_LOCK, Key::from_raw(b"b")),
            ];
            write(&engine, &ctx, modifies);
        }
        must_prewrite_put(&engine, b"b", b"b_value", b"a", SEEK_BOUND + 1);
        must_commit(&engine, b"b", SEEK_BOUND + 1, SEEK_BOUND + 1);

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, (SEEK_BOUND * 2).into())
            .range(None, None)
            .build_delta_scanner(8.into(), ExtraOp::Noop)
            .unwrap();

        // The following illustration comments assume that SEEK_BOUND = 4.

        // Initial position: 1 seek_to_first:
        //   a_8 b_4 b_3 b_2 b_1
        //   ^cursor
        // After get the value, use 1 next to reach next user key:
        //   a_8 b_4 b_3 b_2 b_1
        //       ^cursor
        let entry = EntryBuilder::default()
            .key(b"a")
            .value(b"a_value")
            .start_ts(16.into())
            .commit_ts(16.into())
            .build_commit(WriteType::Put, true);
        let size = entry.size();
        assert_eq!(scanner.next_entry().unwrap(), Some(entry));
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, 1);
        assert_eq!(statistics.processed_size, size);

        // Before:
        //   a_8 b_4 b_3 b_2 b_1
        //       ^cursor
        // We should be able to get wanted value without any operation.
        // After get the value, use SEEK_BOUND-1 next: (TODO: fix it to SEEK_BOUND)
        //   a_8 b_4 b_3 b_2 b_1
        //                   ^cursor
        // We still pointing at current user key, so a seek:
        //   a_8 b_4 b_3 b_2 b_1
        //                       ^cursor
        let entry = EntryBuilder::default()
            .key(b"b")
            .value(b"b_value")
            .start_ts(9.into())
            .commit_ts(9.into())
            .build_commit(WriteType::Put, true);
        let size = entry.size();
        assert_eq!(scanner.next_entry().unwrap(), Some(entry),);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 1);
        assert_eq!(statistics.processed_size, size);

        // Next we should get nothing.
        assert_eq!(scanner.next_entry().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, (SEEK_BOUND - 1) as usize);
        assert_eq!(statistics.processed_size, 0);
    }

    /// Range is left open right closed.
    #[test]
    fn test_range() {
        let engine = TestEngineBuilder::new().build().unwrap();

        // Generate 1 put for [1], [2] ... [6].
        for i in 1..7 {
            // ts = 1: value = []
            must_prewrite_put(&engine, &[i], &[], &[i], 1);
            must_commit(&engine, &[i], 1, 1);

            // ts = 7: value = [ts]
            must_prewrite_put(&engine, &[i], &[i], &[i], 7);
            must_commit(&engine, &[i], 7, 7);

            // ts = 14: value = []
            must_prewrite_put(&engine, &[i], &[], &[i], 14);
            must_commit(&engine, &[i], 14, 14);
        }

        let snapshot = engine.snapshot(Default::default()).unwrap();

        // Test both bound specified.
        let mut scanner = ScannerBuilder::new(snapshot.clone(), 10.into())
            .range(Some(Key::from_raw(&[3u8])), Some(Key::from_raw(&[5u8])))
            .build_delta_scanner(4.into(), ExtraOp::Noop)
            .unwrap();

        let entry = |key, ts| {
            EntryBuilder::default()
                .key(key)
                .value(key)
                .start_ts(ts)
                .commit_ts(ts)
                .build_commit(WriteType::Put, true)
        };

        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[3u8], 7.into())));
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[4u8], 7.into())));
        assert_eq!(scanner.next_entry().unwrap(), None);

        // Test left bound not specified.
        let mut scanner = ScannerBuilder::new(snapshot.clone(), 10.into())
            .range(None, Some(Key::from_raw(&[3u8])))
            .build_delta_scanner(4.into(), ExtraOp::Noop)
            .unwrap();
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[1u8], 7.into())));
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[2u8], 7.into())));
        assert_eq!(scanner.next_entry().unwrap(), None);

        // Test right bound not specified.
        let mut scanner = ScannerBuilder::new(snapshot.clone(), 10.into())
            .range(Some(Key::from_raw(&[5u8])), None)
            .build_delta_scanner(4.into(), ExtraOp::Noop)
            .unwrap();
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[5u8], 7.into())));
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[6u8], 7.into())));
        assert_eq!(scanner.next_entry().unwrap(), None);

        // Test both bound not specified.
        let mut scanner = ScannerBuilder::new(snapshot, 10.into())
            .range(None, None)
            .build_delta_scanner(4.into(), ExtraOp::Noop)
            .unwrap();
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[1u8], 7.into())));
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[2u8], 7.into())));
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[3u8], 7.into())));
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[4u8], 7.into())));
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[5u8], 7.into())));
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[6u8], 7.into())));
        assert_eq!(scanner.next_entry().unwrap(), None);
    }

    #[test]
    fn test_mess() {
        // TODO: non-pessimistic lock should be returned enven if its ts < from_ts.
        // (key, lock, [commit1, commit2, ...])
        // Values ends with 'L' will be made larger than `SHORT_VALUE_MAX_LEN` so it will be saved
        // in default cf.
        let test_data = vec![
            (
                b"a" as &[u8],
                None,
                vec![
                    (2, 4, WriteType::Put, b"va1" as &[u8]),
                    (12, 14, WriteType::Put, b"va2"),
                    (22, 24, WriteType::Put, b"va3"),
                ],
            ),
            (b"b", Some((12, LockType::Put, b"vb1" as &[u8])), vec![]),
            (b"c", Some((22, LockType::Put, b"vc1")), vec![]),
            (b"d", Some((22, LockType::Put, b"vdL")), vec![]),
            (b"e", Some((100, LockType::Delete, b"")), vec![]),
            (
                b"f",
                Some((15, LockType::Pessimistic, b"")),
                vec![
                    (2, 10, WriteType::Put, b"vf1"),
                    (5, 22, WriteType::Delete, b""),
                    (23, 25, WriteType::Lock, b""),
                    (26, 26, WriteType::Rollback, b""),
                    (21, 27, WriteType::Put, b"vf2L"),
                    (24, 50, WriteType::Delete, b""),
                ],
            ),
            (
                b"g",
                Some((51, LockType::Put, b"vg1L")),
                vec![
                    (2, 10, WriteType::Put, b"vg2L"),
                    (5, 22, WriteType::Put, b"vg3L"),
                    (23, 25, WriteType::Put, b"vg4L"),
                    (26, 26, WriteType::Rollback, b""),
                    (21, 27, WriteType::Put, b"vg5L"),
                    (24, 50, WriteType::Put, b"vg6L"),
                ],
            ),
            (
                b"h",
                None,
                vec![
                    (8, 10, WriteType::Put, b"vh1"),
                    (12, 12, WriteType::Rollback, b""),
                    (14, 14, WriteType::Rollback, b""),
                    (16, 16, WriteType::Rollback, b""),
                    (18, 18, WriteType::Rollback, b""),
                    (22, 24, WriteType::Put, b"vh2"),
                ],
            ),
        ];

        let make_value = |v: &[u8]| {
            let mut res = v.to_vec();
            if res.last().map(|b| *b == b'L').unwrap_or(false) {
                res = res
                    .into_iter()
                    .cycle()
                    .take(SHORT_VALUE_MAX_LEN + 5)
                    .collect();
            }
            res
        };
        let expected_entries = |from_key: &[u8], to_key: &[u8], from_ts: u64, to_ts: u64| {
            test_data
                .iter()
                .filter(|(key, ..)| *key >= from_key && (to_key.is_empty() || *key < to_key))
                .flat_map(|(key, lock, writes)| {
                    let mut entries_of_key = vec![];

                    if let Some((ts, lock_type, value)) = lock {
                        let max_commit_ts = writes
                            .last()
                            .cloned()
                            .map(|(_, commit_ts, ..)| commit_ts)
                            .unwrap_or(0);
                        let for_update_ts = std::cmp::max(*ts, max_commit_ts + 1);

                        if *ts <= to_ts {
                            let value = make_value(value);
                            let entry = EntryBuilder::default()
                                .key(key)
                                .start_ts(ts.into())
                                .for_update_ts(for_update_ts.into())
                                .primary(key)
                                .value(&value)
                                .build_prewrite(*lock_type, is_short_value(&value));
                            entries_of_key.push(entry);
                        }
                    }

                    for (start_ts, commit_ts, write_type, value) in writes.iter().rev() {
                        // Commits not in timestamp range will not be scanned
                        if *commit_ts > to_ts || *commit_ts <= from_ts {
                            continue;
                        }

                        // Rollbacks are ignored.
                        if *write_type == WriteType::Rollback || *write_type == WriteType::Lock {
                            continue;
                        }

                        let value = make_value(value);
                        let entry = EntryBuilder::default()
                            .key(key)
                            .start_ts(start_ts.into())
                            .commit_ts(commit_ts.into())
                            .value(&value)
                            .build_commit(*write_type, is_short_value(&value));
                        entries_of_key.push(entry);
                    }

                    entries_of_key
                })
                .collect::<Vec<TxnEntry>>()
        };

        let engine = TestEngineBuilder::new().build().unwrap();
        for (key, lock, writes) in &test_data {
            for (start_ts, commit_ts, write_type, value) in writes {
                let value = make_value(value);
                if *write_type != WriteType::Rollback {
                    must_acquire_pessimistic_lock(&engine, key, key, start_ts, commit_ts - 1);
                }
                match write_type {
                    WriteType::Put => must_pessimistic_prewrite_put(
                        &engine,
                        key,
                        &value,
                        key,
                        start_ts,
                        commit_ts - 1,
                        true,
                    ),
                    WriteType::Delete => must_pessimistic_prewrite_delete(
                        &engine,
                        key,
                        key,
                        start_ts,
                        commit_ts - 1,
                        true,
                    ),
                    WriteType::Lock => must_pessimistic_prewrite_lock(
                        &engine,
                        key,
                        key,
                        start_ts,
                        commit_ts - 1,
                        true,
                    ),
                    WriteType::Rollback => must_rollback(&engine, key, start_ts, false),
                }
                if *write_type != WriteType::Rollback {
                    must_commit(&engine, key, start_ts, commit_ts);
                }
            }

            if let Some((ts, lock_type, value)) = lock {
                let value = make_value(value);
                let max_commit_ts = writes
                    .last()
                    .cloned()
                    .map(|(_, commit_ts, ..)| commit_ts)
                    .unwrap_or(0);
                let for_update_ts = std::cmp::max(*ts, max_commit_ts + 1);
                must_acquire_pessimistic_lock(&engine, key, key, *ts, for_update_ts);
                match lock_type {
                    LockType::Put => must_pessimistic_prewrite_put(
                        &engine,
                        key,
                        &value,
                        key,
                        ts,
                        for_update_ts,
                        true,
                    ),
                    LockType::Delete => {
                        must_pessimistic_prewrite_delete(&engine, key, key, ts, for_update_ts, true)
                    }
                    LockType::Lock => {
                        must_pessimistic_prewrite_lock(&engine, key, key, ts, for_update_ts, true)
                    }
                    LockType::Pessimistic => {}
                }
            }
        }

        let check = |from_key, to_key, from_ts, to_ts| {
            let expected = expected_entries(from_key, to_key, from_ts, to_ts);

            let from_key = if from_key.is_empty() {
                None
            } else {
                Some(Key::from_raw(from_key))
            };
            let to_key = if to_key.is_empty() {
                None
            } else {
                Some(Key::from_raw(to_key))
            };
            let mut scanner =
                ScannerBuilder::new(engine.snapshot(Default::default()).unwrap(), to_ts.into())
                    .hint_min_ts(Some(from_ts.into()))
                    .hint_max_ts(Some(to_ts.into()))
                    .range(from_key, to_key)
                    .build_delta_scanner(from_ts.into(), ExtraOp::Noop)
                    .unwrap();

            let mut actual = vec![];
            while let Some(entry) = scanner.next_entry().unwrap() {
                actual.push(entry);
            }
            // Do assertions one by one so that if it fails it won't print too long panic message.
            for i in 0..std::cmp::max(actual.len(), expected.len()) {
                assert_eq!(
                    actual[i], expected[i],
                    "item {} not match: expected {:?}, but got {:?}",
                    i, &expected[i], &actual[i]
                );
            }
        };

        check(b"", b"", 0, u64::max_value());
        check(b"", b"", 20, 30);
        check(b"", b"", 14, 24);
        check(b"", b"", 15, 16);
        check(b"", b"", 80, 90);
        check(b"", b"", 24, u64::max_value());
        check(b"a", b"g", 0, u64::max_value());
        check(b"b", b"c", 20, 30);
        check(b"g", b"h", 14, 24);
        check(b"", b"a", 80, 90);
        check(b"h", b"", 24, u64::max_value());
        check(b"c", b"d", 0, u64::max_value());
    }

    #[test]
    fn test_output_old_value() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let ctx = Context::default();

        // Generate put for [a] at 1.
        must_prewrite_put(&engine, b"a", b"a_1", b"a", 1);
        must_commit(&engine, b"a", 1, 1);

        // Generate put for [a] at 3.
        must_prewrite_put(&engine, b"a", b"a_3", b"a", 3);
        must_commit(&engine, b"a", 3, 3);

        // Generate delete for [a] at 5.
        must_prewrite_delete(&engine, b"a", b"a", 5);

        // Generate put for [b] at 2.
        must_prewrite_put(&engine, b"b", b"b_2", b"b", 2);
        must_commit(&engine, b"b", 2, 2);

        // Generate rollbacks for [b] at 6, 7, 8.
        for ts in 6..9 {
            let modifies = vec![
                // ts is rather small, so it is ok to `as u8`
                Modify::Put(
                    CF_WRITE,
                    Key::from_raw(b"b").append_ts(TimeStamp::new(ts)),
                    vec![b'R', ts as u8],
                ),
                Modify::Delete(CF_LOCK, Key::from_raw(b"b")),
            ];
            write(&engine, &ctx, modifies);
        }

        // Generate delete for [b] at 10.
        must_prewrite_delete(&engine, b"b", b"b", 10);
        must_commit(&engine, b"b", 10, 10);

        // Generate put for [b] at 15.
        must_acquire_pessimistic_lock(&engine, b"b", b"b", 9, 15);
        must_pessimistic_prewrite_put(&engine, b"b", b"b_15", b"b", 9, 15, true);

        must_prewrite_put(&engine, b"c", b"c_4", b"c", 4);
        must_commit(&engine, b"c", 4, 6);
        must_acquire_pessimistic_lock(&engine, b"c", b"c", 5, 15);
        must_pessimistic_prewrite_put(&engine, b"c", b"c_5", b"c", 5, 15, true);
        must_cleanup(&engine, b"c", 20, 0);

        let entry_a_1 = EntryBuilder::default()
            .key(b"a")
            .value(b"a_1")
            .start_ts(1.into())
            .commit_ts(1.into())
            .build_commit(WriteType::Put, true);
        let entry_a_3 = EntryBuilder::default()
            .key(b"a")
            .value(b"a_3")
            .start_ts(3.into())
            .commit_ts(3.into())
            .old_value(b"a_1")
            .build_commit(WriteType::Put, true);
        let entry_a_5 = EntryBuilder::default()
            .key(b"a")
            .start_ts(5.into())
            .primary(b"a")
            .old_value(b"a_3")
            .build_prewrite(LockType::Delete, true);
        let entry_b_2 = EntryBuilder::default()
            .key(b"b")
            .value(b"b_2")
            .start_ts(2.into())
            .commit_ts(2.into())
            .build_commit(WriteType::Put, true);
        let entry_b_10 = EntryBuilder::default()
            .key(b"b")
            .start_ts(10.into())
            .commit_ts(10.into())
            .old_value(b"b_2")
            .build_commit(WriteType::Delete, true);
        let entry_b_15 = EntryBuilder::default()
            .key(b"b")
            .value(b"b_15")
            .primary(b"b")
            .start_ts(9.into())
            .for_update_ts(15.into())
            .build_prewrite(LockType::Put, true);
        let entry_c_4 = EntryBuilder::default()
            .key(b"c")
            .value(b"c_4")
            .start_ts(4.into())
            .commit_ts(6.into())
            .build_commit(WriteType::Put, true);
        let entry_c_5 = EntryBuilder::default()
            .key(b"c")
            .value(b"c_5")
            .primary(b"c")
            .start_ts(5.into())
            .for_update_ts(15.into())
            .old_value(b"c_4")
            .build_prewrite(LockType::Put, true);

        let check = |after_ts: u64, expected: Vec<&TxnEntry>| {
            let snapshot = engine.snapshot(Default::default()).unwrap();
            let mut scanner = ScannerBuilder::new(snapshot, TimeStamp::max())
                .range(None, None)
                .build_delta_scanner(after_ts.into(), ExtraOp::ReadOldValue)
                .unwrap();
            for entry in expected {
                assert_eq!(scanner.next_entry().unwrap().as_ref(), Some(entry));
            }
            let last = scanner.next_entry().unwrap();
            assert!(last.is_none(), "{:?}", last);
        };

        // Scanning entries in (10, max] should get all prewrites
        check(10, vec![&entry_a_5, &entry_b_15, &entry_c_5]);
        // Scanning entries include delete in (7, max] should get a_5, b_10, b_15 and c_5
        check(7, vec![&entry_a_5, &entry_b_15, &entry_b_10, &entry_c_5]);
        // Scanning entries in (0, max] should get a_1, a_3, a_5, b_2, b_10, and b_15
        check(
            0,
            vec![
                &entry_a_5,
                &entry_a_3,
                &entry_a_1,
                &entry_b_15,
                &entry_b_10,
                &entry_b_2,
                &entry_c_5,
                &entry_c_4,
            ],
        );
    }

    #[test]
    fn test_old_value_check_gc_fence() {
        let engine = TestEngineBuilder::new().build().unwrap();
        prepare_test_data_for_check_gc_fence(&engine);

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, TimeStamp::max())
            .range(None, None)
            .build_delta_scanner(40.into(), ExtraOp::ReadOldValue)
            .unwrap();
        let entries: Vec<_> = std::iter::from_fn(|| scanner.next_entry().unwrap()).collect();
        let expected_entries_1 = vec![
            EntryBuilder::default()
                .key(b"k1")
                .value(b"v1x")
                .primary(b"k1")
                .start_ts(49.into())
                .commit_ts(50.into())
                .old_value(b"v1")
                .build_commit(WriteType::Put, true),
            EntryBuilder::default()
                .key(b"k6")
                .value(b"v6x")
                .primary(b"k6")
                .start_ts(49.into())
                .commit_ts(50.into())
                .old_value(b"v6")
                .build_commit(WriteType::Put, true),
        ];
        assert_eq!(entries, expected_entries_1);

        // Lock all the keys at 55 and check again.
        for i in b'1'..=b'8' {
            let key = &[b'k', i];
            let value = &[b'v', i, b'x', b'x'];
            must_prewrite_put(&engine, key, value, b"k1", 55);
        }
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, TimeStamp::max())
            .range(None, None)
            .build_delta_scanner(40.into(), ExtraOp::ReadOldValue)
            .unwrap();
        let entries: Vec<_> = std::iter::from_fn(|| scanner.next_entry().unwrap()).collect();

        // Shortcut for generating the expected result at current time.
        let build_entry = |k, v, old_value| {
            let mut b = EntryBuilder::default();
            b.key(k).value(v).primary(b"k1").start_ts(55.into());
            if let Some(ov) = old_value {
                b.old_value(ov);
            }
            b.build_prewrite(LockType::Put, true)
        };

        let expected_entries_2 = vec![
            build_entry(b"k1", b"v1xx", Some(b"v1x")),
            expected_entries_1[0].clone(),
            build_entry(b"k2", b"v2xx", None),
            build_entry(b"k3", b"v3xx", None),
            build_entry(b"k4", b"v4xx", None),
            build_entry(b"k5", b"v5xx", None),
            build_entry(b"k6", b"v6xx", Some(b"v6x")),
            expected_entries_1[1].clone(),
            build_entry(b"k7", b"v7xx", None),
            build_entry(b"k8", b"v8xx", Some(b"v8")),
        ];
        assert_eq!(entries, expected_entries_2);

        // Commit all the locks and check again.
        for i in b'1'..=b'8' {
            let key = &[b'k', i];
            must_commit(&engine, key, 55, 56);
        }
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, TimeStamp::max())
            .range(None, None)
            .build_delta_scanner(40.into(), ExtraOp::ReadOldValue)
            .unwrap();
        let entries: Vec<_> = std::iter::from_fn(|| scanner.next_entry().unwrap()).collect();

        // Shortcut for generating the expected result at current time.
        let build_entry = |k, v, old_value| {
            let mut b = EntryBuilder::default();
            b.key(k)
                .value(v)
                .primary(b"k1")
                .start_ts(55.into())
                .commit_ts(56.into());
            if let Some(ov) = old_value {
                b.old_value(ov);
            }
            b.build_commit(WriteType::Put, true)
        };

        let expected_entries_2 = vec![
            build_entry(b"k1", b"v1xx", Some(b"v1x")),
            expected_entries_1[0].clone(),
            build_entry(b"k2", b"v2xx", None),
            build_entry(b"k3", b"v3xx", None),
            build_entry(b"k4", b"v4xx", None),
            build_entry(b"k5", b"v5xx", None),
            build_entry(b"k6", b"v6xx", Some(b"v6x")),
            expected_entries_1[1].clone(),
            build_entry(b"k7", b"v7xx", None),
            build_entry(b"k8", b"v8xx", Some(b"v8")),
        ];
        assert_eq!(entries, expected_entries_2);
    }
}
