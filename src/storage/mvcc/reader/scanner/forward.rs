// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;

use engine::CF_DEFAULT;
use kvproto::kvrpcpb::IsolationLevel;
use txn_types::{Key, Lock, TimeStamp, Value, WriteRef, WriteType};

use super::ScannerConfig;
use crate::storage::kv::SEEK_BOUND;
use crate::storage::mvcc::Result;
use crate::storage::txn::{Result as TxnResult, TxnEntry, TxnEntryScanner};
use crate::storage::{Cursor, Snapshot, Statistics};

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
}

pub enum HandleRes<T> {
    Return(T),
    Skip(Key),
}

pub struct Cursors<S: Snapshot> {
    lock: Cursor<S::Iter>,
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
}

impl<S: Snapshot, P: ScanPolicy<S>> ForwardScanner<S, P> {
    pub fn new(
        cfg: ScannerConfig<S>,
        lock_cursor: Cursor<S::Iter>,
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
            cfg,
            cursors,
            statistics: Statistics::default(),
            is_started: false,
            scan_policy,
        }
    }

    /// Take out and reset the statistics collected so far.
    pub fn take_statistics(&mut self) -> Statistics {
        std::mem::replace(&mut self.statistics, Statistics::default())
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
                self.cursors.lock.seek(
                    self.cfg.lower_bound.as_ref().unwrap(),
                    &mut self.statistics.lock,
                )?;
            } else {
                self.cursors.write.seek_to_first(&mut self.statistics.write);
                self.cursors.lock.seek_to_first(&mut self.statistics.lock);
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
                let l_key = if self.cursors.lock.valid()? {
                    Some(self.cursors.lock.key(&mut self.statistics.lock))
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
                current_user_key = match self.scan_policy.handle_lock(
                    current_user_key,
                    &mut self.cfg,
                    &mut self.cursors,
                    &mut self.statistics,
                )? {
                    HandleRes::Return(output) => return Ok(Some(output)),
                    HandleRes::Skip(key) => key,
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
                if Key::decode_ts_from(current_key)? <= self.cfg.ts {
                    // Founded, don't need to seek again.
                    needs_seek = false;
                    break;
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
        scan_latest_handle_lock(current_user_key, cfg, cursors, statistics)
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
            statistics.write.processed += 1;

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
            statistics.write.processed += 1;

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
                    });
                }
                WriteType::Delete if self.output_delete => {
                    break Some(TxnEntry::Commit {
                        default: (Vec::new(), Vec::new()),
                        write: (write_key.to_vec(), write_value.to_vec()),
                    });
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
}

fn scan_latest_handle_lock<S: Snapshot, T>(
    current_user_key: Key,
    cfg: &mut ScannerConfig<S>,
    cursors: &mut Cursors<S>,
    statistics: &mut Statistics,
) -> Result<HandleRes<T>> {
    let result = match cfg.isolation_level {
        IsolationLevel::Si => {
            // Only needs to check lock in SI
            let lock = {
                let lock_value = cursors.lock.value(&mut statistics.lock);
                Lock::parse(lock_value)?
            };
            lock.check_ts_conflict(&current_user_key, cfg.ts, &cfg.bypass_locks)
                .map(|_| ())
        }
        IsolationLevel::Rc => Ok(()),
    };
    cursors.lock.next(&mut statistics.lock);
    // Even if there is a lock error, we still need to step the cursor for future
    // calls.
    if result.is_err() {
        cursors.move_write_cursor_to_next_user_key(&current_user_key, statistics)?;
    }
    result
        .map(|_| HandleRes::Skip(current_user_key))
        .map_err(Into::into)
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

impl<S: Snapshot> TxnEntryScanner for EntryScanner<S> {
    fn next_entry(&mut self) -> TxnResult<Option<TxnEntry>> {
        Ok(self.read_next()?)
    }
    fn take_statistics(&mut self) -> Statistics {
        std::mem::replace(&mut self.statistics, Statistics::default())
    }
}

#[cfg(test)]
mod latest_kv_tests {
    use super::super::ScannerBuilder;
    use super::*;
    use crate::storage::kv::{Engine, TestEngineBuilder};
    use crate::storage::mvcc::tests::*;
    use crate::storage::Scanner;

    use kvproto::kvrpcpb::Context;

    /// Check whether everything works as usual when `ForwardKvScanner::get()` goes out of bound.
    #[test]
    fn test_get_out_of_bound() {
        let engine = TestEngineBuilder::new().build().unwrap();

        // Generate 1 put for [a].
        must_prewrite_put(&engine, b"a", b"value", b"a", 7);
        must_commit(&engine, b"a", 7, 7);

        // Generate 5 rollback for [b].
        for ts in 0..5 {
            must_rollback(&engine, b"b", ts);
        }

        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, 10.into(), false)
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

        // Use 5 next and reach out of bound:
        //   a_7 b_4 b_3 b_2 b_1 b_0
        //                           ^cursor
        assert_eq!(scanner.next().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 5);

        // Cursor remains invalid, so nothing should happen.
        assert_eq!(scanner.next().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 0);
    }

    /// Check whether everything works as usual when
    /// `ForwardKvScanner::move_write_cursor_to_next_user_key()` goes out of bound.
    ///
    /// Case 1. next() out of bound
    #[test]
    fn test_move_next_user_key_out_of_bound_1() {
        let engine = TestEngineBuilder::new().build().unwrap();

        // Generate 1 put for [a].
        must_prewrite_put(&engine, b"a", b"a_value", b"a", SEEK_BOUND * 2);
        must_commit(&engine, b"a", SEEK_BOUND * 2, SEEK_BOUND * 2);

        // Generate SEEK_BOUND / 2 rollback and 1 put for [b] .
        for ts in 0..SEEK_BOUND / 2 {
            must_rollback(&engine, b"b", ts as u64);
        }
        must_prewrite_put(&engine, b"b", b"b_value", b"a", SEEK_BOUND / 2);
        must_commit(&engine, b"b", SEEK_BOUND / 2, SEEK_BOUND / 2);

        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, (SEEK_BOUND * 2).into(), false)
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

        // Next we should get nothing.
        assert_eq!(scanner.next().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 0);
    }

    /// Check whether everything works as usual when
    /// `ForwardKvScanner::move_write_cursor_to_next_user_key()` goes out of bound.
    ///
    /// Case 2. seek() out of bound
    #[test]
    fn test_move_next_user_key_out_of_bound_2() {
        let engine = TestEngineBuilder::new().build().unwrap();

        // Generate 1 put for [a].
        must_prewrite_put(&engine, b"a", b"a_value", b"a", SEEK_BOUND * 2);
        must_commit(&engine, b"a", SEEK_BOUND * 2, SEEK_BOUND * 2);

        // Generate SEEK_BOUND-1 rollback and 1 put for [b] .
        for ts in 1..SEEK_BOUND {
            must_rollback(&engine, b"b", ts as u64);
        }
        must_prewrite_put(&engine, b"b", b"b_value", b"a", SEEK_BOUND);
        must_commit(&engine, b"b", SEEK_BOUND, SEEK_BOUND);

        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, (SEEK_BOUND * 2).into(), false)
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

        // Next we should get nothing.
        assert_eq!(scanner.next().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 0);
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

        let snapshot = engine.snapshot(&Context::default()).unwrap();

        // Test both bound specified.
        let mut scanner = ScannerBuilder::new(snapshot.clone(), 10.into(), false)
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

        // Test left bound not specified.
        let mut scanner = ScannerBuilder::new(snapshot.clone(), 10.into(), false)
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

        // Test right bound not specified.
        let mut scanner = ScannerBuilder::new(snapshot.clone(), 10.into(), false)
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

        // Test both bound not specified.
        let mut scanner = ScannerBuilder::new(snapshot.clone(), 10.into(), false)
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
    }
}

#[cfg(test)]
mod latest_entry_tests {
    use super::super::ScannerBuilder;
    use super::*;
    use crate::storage::mvcc::tests::*;
    use crate::storage::mvcc::Write;
    use crate::storage::{Engine, TestEngineBuilder};

    use kvproto::kvrpcpb::Context;

    #[derive(Default)]
    struct EntryBuilder {
        key: Vec<u8>,
        value: Vec<u8>,
        start_ts: TimeStamp,
        commit_ts: TimeStamp,
    }

    impl EntryBuilder {
        fn key(&mut self, key: &[u8]) -> &mut Self {
            self.key = key.to_owned();
            self
        }
        fn value(&mut self, val: &[u8]) -> &mut Self {
            self.value = val.to_owned();
            self
        }
        fn start_ts(&mut self, start_ts: TimeStamp) -> &mut Self {
            self.start_ts = start_ts;
            self
        }
        fn commit_ts(&mut self, commit_ts: TimeStamp) -> &mut Self {
            self.commit_ts = commit_ts;
            self
        }
        fn build_commit(&self, wt: WriteType, is_short_value: bool) -> TxnEntry {
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
            }
        }
    }

    /// Check whether everything works as usual when `EntryScanner::get()` goes out of bound.
    #[test]
    fn test_get_out_of_bound() {
        let engine = TestEngineBuilder::new().build().unwrap();

        // Generate 1 put for [a].
        must_prewrite_put(&engine, b"a", b"value", b"a", 7);
        must_commit(&engine, b"a", 7, 7);

        // Generate 5 rollback for [b].
        for ts in 0..5 {
            must_rollback(&engine, b"b", ts);
        }

        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, 10.into(), false)
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
        assert_eq!(scanner.next_entry().unwrap(), Some(entry),);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, 1);

        // Use 5 next and reach out of bound:
        //   a_7 b_4 b_3 b_2 b_1 b_0
        //                           ^cursor
        assert_eq!(scanner.next_entry().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 5);

        // Cursor remains invalid, so nothing should happen.
        assert_eq!(scanner.next_entry().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 0);
    }

    /// Check whether everything works as usual when
    /// `EntryScanner::move_write_cursor_to_next_user_key()` goes out of bound.
    ///
    /// Case 1. next() out of bound
    #[test]
    fn test_move_next_user_key_out_of_bound_1() {
        let engine = TestEngineBuilder::new().build().unwrap();

        // Generate 1 put for [a].
        must_prewrite_put(&engine, b"a", b"a_value", b"a", SEEK_BOUND * 2);
        must_commit(&engine, b"a", SEEK_BOUND * 2, SEEK_BOUND * 2);

        // Generate SEEK_BOUND / 2 rollback and 1 put for [b] .
        for ts in 0..SEEK_BOUND / 2 {
            must_rollback(&engine, b"b", ts as u64);
        }
        must_prewrite_put(&engine, b"b", b"b_value", b"a", SEEK_BOUND / 2);
        must_commit(&engine, b"b", SEEK_BOUND / 2, SEEK_BOUND / 2);

        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, (SEEK_BOUND * 2).into(), false)
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
        assert_eq!(scanner.next_entry().unwrap(), Some(entry),);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, 1);

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
        assert_eq!(scanner.next_entry().unwrap(), Some(entry),);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, (SEEK_BOUND / 2 + 1) as usize);

        // Next we should get nothing.
        assert_eq!(scanner.next_entry().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 0);
    }

    /// Check whether everything works as usual when
    /// `EntryScanner::move_write_cursor_to_next_user_key()` goes out of bound.
    ///
    /// Case 2. seek() out of bound
    #[test]
    fn test_move_next_user_key_out_of_bound_2() {
        let engine = TestEngineBuilder::new().build().unwrap();

        // Generate 1 put for [a].
        must_prewrite_put(&engine, b"a", b"a_value", b"a", SEEK_BOUND * 2);
        must_commit(&engine, b"a", SEEK_BOUND * 2, SEEK_BOUND * 2);

        // Generate SEEK_BOUND-1 rollback and 1 put for [b] .
        for ts in 1..SEEK_BOUND {
            must_rollback(&engine, b"b", ts as u64);
        }
        must_prewrite_put(&engine, b"b", b"b_value", b"a", SEEK_BOUND);
        must_commit(&engine, b"b", SEEK_BOUND, SEEK_BOUND);

        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, (SEEK_BOUND * 2).into(), false)
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
        assert_eq!(scanner.next_entry().unwrap(), Some(entry));
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, 1);

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
        assert_eq!(scanner.next_entry().unwrap(), Some(entry),);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, (SEEK_BOUND - 1) as usize);

        // Next we should get nothing.
        assert_eq!(scanner.next_entry().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 0);
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

        let snapshot = engine.snapshot(&Context::default()).unwrap();

        // Test both bound specified.
        let mut scanner = ScannerBuilder::new(snapshot.clone(), 10.into(), false)
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
        let mut scanner = ScannerBuilder::new(snapshot.clone(), 10.into(), false)
            .range(None, Some(Key::from_raw(&[3u8])))
            .build_entry_scanner(0.into(), false)
            .unwrap();
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[1u8], 7.into())));
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[2u8], 7.into())));
        assert_eq!(scanner.next_entry().unwrap(), None);

        // Test right bound not specified.
        let mut scanner = ScannerBuilder::new(snapshot.clone(), 10.into(), false)
            .range(Some(Key::from_raw(&[5u8])), None)
            .build_entry_scanner(0.into(), false)
            .unwrap();
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[5u8], 7.into())));
        assert_eq!(scanner.next_entry().unwrap(), Some(entry(&[6u8], 7.into())));
        assert_eq!(scanner.next_entry().unwrap(), None);

        // Test both bound not specified.
        let mut scanner = ScannerBuilder::new(snapshot.clone(), 10.into(), false)
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
            must_rollback(&engine, b"b", ts);
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
            let snapshot = engine.snapshot(&Context::default()).unwrap();
            let mut scanner = ScannerBuilder::new(snapshot, ts.into(), false)
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
        // Scanning entries without delete in (7, 10]  should get None
        check(10, 7, false, vec![]);
        // Scanning entries include delete in (7, 10]  should get entry_b_10
        check(10, 7, true, vec![&entry_b_10]);
        // Scanning entries include delete in (3, 10] should get a_7 and b_10
        check(10, 3, true, vec![&entry_a_7, &entry_b_10]);
        // Scanning entries in (0, 5] should get a_3 and b_1
        check(5, 0, true, vec![&entry_a_3, &entry_b_1]);
    }
}
