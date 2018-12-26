// Copyright 2018 PingCAP, Inc.
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

use std::cmp::Ordering;

use kvproto::kvrpcpb::IsolationLevel;

use storage::engine::SEEK_BOUND;
use storage::mvcc::write::{Write, WriteType};
use storage::mvcc::Result;
use storage::{Cursor, CursorBuilder, Key, Lock, Snapshot, Statistics, Value};
use storage::{CF_DEFAULT, CF_LOCK, CF_WRITE};

use super::util::CheckLockResult;

/// `ForwardScanner` factory.
pub struct ForwardScannerBuilder<S: Snapshot> {
    snapshot: S,
    fill_cache: bool,
    omit_value: bool,
    isolation_level: IsolationLevel,
    lower_bound: Option<Key>,
    upper_bound: Option<Key>,
    ts: u64,
}

impl<S: Snapshot> ForwardScannerBuilder<S> {
    /// Initialize a new `ForwardScanner`
    pub fn new(snapshot: S, ts: u64) -> Self {
        Self {
            snapshot,
            fill_cache: true,
            omit_value: false,
            isolation_level: IsolationLevel::SI,
            lower_bound: None,
            upper_bound: None,
            ts,
        }
    }

    /// Set whether or not read operations should fill the cache.
    ///
    /// Defaults to `true`.
    #[inline]
    pub fn fill_cache(mut self, fill_cache: bool) -> Self {
        self.fill_cache = fill_cache;
        self
    }

    /// Set whether values of the user key should be omitted. When `omit_value` is `true`, the
    /// length of returned value will be 0.
    ///
    /// Previously this option is called `key_only`.
    ///
    /// Defaults to `false`.
    #[inline]
    pub fn omit_value(mut self, omit_value: bool) -> Self {
        self.omit_value = omit_value;
        self
    }

    /// Set the isolation level.
    ///
    /// Defaults to `IsolationLevel::SI`.
    #[inline]
    pub fn isolation_level(mut self, isolation_level: IsolationLevel) -> Self {
        self.isolation_level = isolation_level;
        self
    }

    /// Limit the range to `[lower_bound, upper_bound)` in which the `ForwardScanner` should scan.
    /// `None` means unbounded.
    ///
    /// Default is `(None, None)`.
    #[inline]
    pub fn range(mut self, lower_bound: Option<Key>, upper_bound: Option<Key>) -> Self {
        self.lower_bound = lower_bound;
        self.upper_bound = upper_bound;
        self
    }

    /// Build `ForwardScanner` from the current configuration.
    pub fn build(self) -> Result<ForwardScanner<S>> {
        let lock_cursor = CursorBuilder::new(&self.snapshot, CF_LOCK)
            .range(self.lower_bound.clone(), self.upper_bound.clone())
            .fill_cache(self.fill_cache)
            .build()?;

        let write_cursor = CursorBuilder::new(&self.snapshot, CF_WRITE)
            .range(self.lower_bound.clone(), self.upper_bound.clone())
            .fill_cache(self.fill_cache)
            .build()?;

        Ok(ForwardScanner {
            snapshot: self.snapshot,
            fill_cache: self.fill_cache,
            omit_value: self.omit_value,
            isolation_level: self.isolation_level,
            lower_bound: self.lower_bound,
            upper_bound: self.upper_bound,
            ts: self.ts,
            lock_cursor,
            write_cursor,
            default_cursor: None,
            is_started: false,
            statistics: Statistics::default(),
        })
    }
}

/// This struct can be used to scan keys starting from the given user key (greater than or equal).
///
/// Internally, for each key, rollbacks are ignored and smaller version will be tried. If the
/// isolation level is SI, locks will be checked first.
///
/// Use `ForwardScannerBuilder` to build `ForwardScanner`.
pub struct ForwardScanner<S: Snapshot> {
    snapshot: S,
    fill_cache: bool,
    omit_value: bool,
    isolation_level: IsolationLevel,

    /// `lower_bound` and `upper_bound` is used to create `default_cursor`. `lower_bound`
    /// is used in initial seek as well. They will be consumed after `default_cursor` is being
    /// created.
    lower_bound: Option<Key>,
    upper_bound: Option<Key>,

    ts: u64,

    lock_cursor: Cursor<S::Iter>,
    write_cursor: Cursor<S::Iter>,

    /// `default cursor` is lazy created only when it's needed.
    default_cursor: Option<Cursor<S::Iter>>,

    /// Is iteration started
    is_started: bool,

    statistics: Statistics,
}

impl<S: Snapshot> ForwardScanner<S> {
    /// Take out and reset the statistics collected so far.
    pub fn take_statistics(&mut self) -> Statistics {
        ::std::mem::replace(&mut self.statistics, Statistics::default())
    }

    /// Get the next key-value pair, in forward order.
    pub fn read_next(&mut self) -> Result<Option<(Key, Value)>> {
        if !self.is_started {
            if self.lower_bound.is_some() {
                // TODO: `seek_to_first` is better, however it has performance issues currently.
                self.write_cursor.seek(
                    self.lower_bound.as_ref().unwrap(),
                    &mut self.statistics.write,
                )?;
                self.lock_cursor.seek(
                    self.lower_bound.as_ref().unwrap(),
                    &mut self.statistics.lock,
                )?;
            } else {
                self.write_cursor.seek_to_first(&mut self.statistics.write);
                self.lock_cursor.seek_to_first(&mut self.statistics.lock);
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
            let (current_user_key, has_write, has_lock) = {
                let w_key = if self.write_cursor.valid() {
                    Some(self.write_cursor.key(&mut self.statistics.write))
                } else {
                    None
                };
                let l_key = if self.lock_cursor.valid() {
                    Some(self.lock_cursor.key(&mut self.statistics.lock))
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

            // `result` stores intermediate values, including KeyLocked errors (but not other kind
            // of errors). If there is KeyLocked errors, we should be able to continue scanning.
            let mut result = Ok(None);

            // `get_ts` is the real used timestamp. If user specifies `MaxInt64` as the timestamp,
            // we need to change it to a most recently available one.
            let mut get_ts = self.ts;

            // `met_next_user_key` stores whether the write cursor has been already pointing to
            // the next user key. If so, we don't need to compare it again when trying to step
            // to the next user key later.
            let mut met_next_user_key = false;

            if has_lock {
                match self.isolation_level {
                    IsolationLevel::SI => {
                        // Only needs to check lock in SI
                        let lock = {
                            let lock_value = self.lock_cursor.value(&mut self.statistics.lock);
                            Lock::parse(lock_value)?
                        };
                        match super::util::check_lock(&current_user_key, self.ts, &lock)? {
                            CheckLockResult::NotLocked => {}
                            CheckLockResult::Locked(e) => result = Err(e),
                            CheckLockResult::Ignored(ts) => get_ts = ts,
                        }
                    }
                    IsolationLevel::RC => {}
                }
                self.lock_cursor.next(&mut self.statistics.lock);
            }
            if has_write {
                // We don't need to read version if there is a lock error already.
                if result.is_ok() {
                    // Attempt to read specified version of the key. Note that we may get `None`
                    // indicating that no desired version is found, or a DELETE version is found
                    result = self.get(&current_user_key, get_ts, &mut met_next_user_key);
                }
                // Even if there is a lock error, we still need to step the cursor for future
                // calls. However if we are already pointing at next user key, we don't need to
                // move it any more. `met_next_user_key` eliminates a key compare.
                if !met_next_user_key {
                    self.move_write_cursor_to_next_user_key(&current_user_key)?;
                }
            }

            // If we got something, it can be just used as the return value. Otherwise, we need
            // to continue stepping the cursor.
            if let Some(v) = result? {
                return Ok(Some((current_user_key, v)));
            }
        }
    }

    /// Attempt to get the value of a key specified by `user_key` and `self.ts`. This function
    /// requires that the write cursor is currently pointing to the latest version of `user_key`.
    #[inline]
    fn get(
        &mut self,
        user_key: &Key,
        ts: u64,
        met_next_user_key: &mut bool,
    ) -> Result<Option<Value>> {
        assert!(self.write_cursor.valid());

        // The logic starting from here is similar to `PointGetter`.

        // Try to iterate to `${user_key}_${ts}`. We first `next()` for a few times,
        // and if we have not reached where we want, we use `seek()`.

        // Whether we have *not* reached where we want by `next()`.
        let mut needs_seek = true;

        for i in 0..SEEK_BOUND {
            if i > 0 {
                self.write_cursor.next(&mut self.statistics.write);
                if !self.write_cursor.valid() {
                    // Key space ended.
                    return Ok(None);
                }
            }
            {
                let current_key = self.write_cursor.key(&mut self.statistics.write);
                if !Key::is_user_key_eq(current_key, user_key.as_encoded().as_slice()) {
                    // Meet another key.
                    *met_next_user_key = true;
                    return Ok(None);
                }
                if Key::decode_ts_from(current_key)? <= ts {
                    // Founded, don't need to seek again.
                    needs_seek = false;
                    break;
                }
            }
        }
        // If we have not found `${user_key}_${ts}` in a few `next()`, directly `seek()`.
        if needs_seek {
            // `user_key` must have reserved space here, so its clone has reserved space too. So no
            // reallocation happends in `append_ts`.
            self.write_cursor
                .seek(&user_key.clone().append_ts(ts), &mut self.statistics.write)?;
            if !self.write_cursor.valid() {
                // Key space ended.
                return Ok(None);
            }
            let current_key = self.write_cursor.key(&mut self.statistics.write);
            if !Key::is_user_key_eq(current_key, user_key.as_encoded().as_slice()) {
                // Meet another key.
                *met_next_user_key = true;
                return Ok(None);
            }
        }

        // Now we must have reached the first key >= `${user_key}_${ts}`. However, we may
        // meet `Lock` or `Rollback`. In this case, more versions needs to be looked up.
        loop {
            let write = Write::parse(self.write_cursor.value(&mut self.statistics.write))?;
            self.statistics.write.processed += 1;

            match write.write_type {
                WriteType::Put => return Ok(Some(self.load_data_by_write(write, user_key)?)),
                WriteType::Delete => return Ok(None),
                WriteType::Lock | WriteType::Rollback => {
                    // Continue iterate next `write`.
                }
            }

            self.write_cursor.next(&mut self.statistics.write);

            if !self.write_cursor.valid() {
                // Key space ended.
                return Ok(None);
            }
            let current_key = self.write_cursor.key(&mut self.statistics.write);
            if !Key::is_user_key_eq(current_key, user_key.as_encoded().as_slice()) {
                // Meet another key.
                *met_next_user_key = true;
                return Ok(None);
            }
        }
    }

    /// Load the value by the given `write`. If value is carried in `write`, it will be returned
    /// directly. Otherwise there will be a default CF look up.
    ///
    /// The implementation is the same as `PointGetter::load_data_by_write`.
    #[inline]
    fn load_data_by_write(&mut self, write: Write, user_key: &Key) -> Result<Value> {
        if self.omit_value {
            return Ok(vec![]);
        }
        match write.short_value {
            Some(value) => {
                // Value is carried in `write`.
                Ok(value)
            }
            None => {
                // Value is in the default CF.
                self.ensure_default_cursor()?;
                let value = super::util::near_load_data_by_write(
                    &mut self.default_cursor.as_mut().unwrap(),
                    user_key,
                    write,
                    &mut self.statistics,
                )?;
                Ok(value)
            }
        }
    }

    /// After `self.get()`, our write cursor may be pointing to current user key (if we
    /// found a desired version), or next user key (if there is no desired version), or
    /// out of bound.
    ///
    /// If it is pointing to current user key, we need to step it until we meet a new
    /// key. We first try to `next()` a few times. If still not reaching another user
    /// key, we `seek()`.
    #[inline]
    fn move_write_cursor_to_next_user_key(&mut self, current_user_key: &Key) -> Result<()> {
        for i in 0..SEEK_BOUND {
            if i > 0 {
                self.write_cursor.next(&mut self.statistics.write);
            }
            if !self.write_cursor.valid() {
                // Key space ended. We are done here.
                return Ok(());
            }
            {
                let current_key = self.write_cursor.key(&mut self.statistics.write);
                if !Key::is_user_key_eq(current_key, current_user_key.as_encoded().as_slice()) {
                    // Found another user key. We are done here.
                    return Ok(());
                }
            }
        }

        // We have not found another user key for now, so we directly `seek()`.
        // After that, we must pointing to another key, or out of bound.
        // `current_user_key` must have reserved space here, so its clone has reserved space too.
        // So no reallocation happends in `append_ts`.
        self.write_cursor.internal_seek(
            &current_user_key.clone().append_ts(0),
            &mut self.statistics.write,
        )?;

        Ok(())
    }

    /// Create the default cursor if it doesn't exist.
    #[inline]
    fn ensure_default_cursor(&mut self) -> Result<()> {
        if self.default_cursor.is_some() {
            return Ok(());
        }
        let cursor = CursorBuilder::new(&self.snapshot, CF_DEFAULT)
            .range(self.lower_bound.take(), self.upper_bound.take())
            .fill_cache(self.fill_cache)
            .build()?;
        self.default_cursor = Some(cursor);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage::mvcc::tests::*;
    use storage::{Engine, Key, TestEngineBuilder};

    use kvproto::kvrpcpb::Context;

    /// Check whether everything works as usual when `ForwardScanner::get()` goes out of bound.
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

        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut scanner = ForwardScannerBuilder::new(snapshot, 10)
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
            scanner.read_next().unwrap(),
            Some((Key::from_raw(b"a"), b"value".to_vec())),
        );
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, 1);

        // Use 5 next and reach out of bound:
        //   a_7 b_4 b_3 b_2 b_1 b_0
        //                           ^cursor
        assert_eq!(scanner.read_next().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 5);

        // Cursor remains invalid, so nothing should happen.
        assert_eq!(scanner.read_next().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 0);
    }

    /// Check whether everything works as usual when
    /// `ForwardScanner::move_write_cursor_to_next_user_key()` goes out of bound.
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

        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut scanner = ForwardScannerBuilder::new(snapshot, SEEK_BOUND * 2)
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
            scanner.read_next().unwrap(),
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
            scanner.read_next().unwrap(),
            Some((Key::from_raw(b"b"), b"b_value".to_vec())),
        );
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, (SEEK_BOUND / 2 + 1) as usize);

        // Next we should get nothing.
        assert_eq!(scanner.read_next().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 0);
    }

    /// Check whether everything works as usual when
    /// `ForwardScanner::move_write_cursor_to_next_user_key()` goes out of bound.
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

        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut scanner = ForwardScannerBuilder::new(snapshot, SEEK_BOUND * 2)
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
            scanner.read_next().unwrap(),
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
            scanner.read_next().unwrap(),
            Some((Key::from_raw(b"b"), b"b_value".to_vec())),
        );
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, (SEEK_BOUND - 1) as usize);

        // Next we should get nothing.
        assert_eq!(scanner.read_next().unwrap(), None);
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

        let snapshot = engine.snapshot(&Context::new()).unwrap();

        // Test both bound specified.
        let mut scanner = ForwardScannerBuilder::new(snapshot.clone(), 10)
            .range(Some(Key::from_raw(&[3u8])), Some(Key::from_raw(&[5u8])))
            .build()
            .unwrap();
        assert_eq!(
            scanner.read_next().unwrap(),
            Some((Key::from_raw(&[3u8]), vec![3u8]))
        );
        assert_eq!(
            scanner.read_next().unwrap(),
            Some((Key::from_raw(&[4u8]), vec![4u8]))
        );
        assert_eq!(scanner.read_next().unwrap(), None);

        // Test left bound not specified.
        let mut scanner = ForwardScannerBuilder::new(snapshot.clone(), 10)
            .range(None, Some(Key::from_raw(&[3u8])))
            .build()
            .unwrap();
        assert_eq!(
            scanner.read_next().unwrap(),
            Some((Key::from_raw(&[1u8]), vec![1u8]))
        );
        assert_eq!(
            scanner.read_next().unwrap(),
            Some((Key::from_raw(&[2u8]), vec![2u8]))
        );
        assert_eq!(scanner.read_next().unwrap(), None);

        // Test right bound not specified.
        let mut scanner = ForwardScannerBuilder::new(snapshot.clone(), 10)
            .range(Some(Key::from_raw(&[5u8])), None)
            .build()
            .unwrap();
        assert_eq!(
            scanner.read_next().unwrap(),
            Some((Key::from_raw(&[5u8]), vec![5u8]))
        );
        assert_eq!(
            scanner.read_next().unwrap(),
            Some((Key::from_raw(&[6u8]), vec![6u8]))
        );
        assert_eq!(scanner.read_next().unwrap(), None);

        // Test both bound not specified.
        let mut scanner = ForwardScannerBuilder::new(snapshot.clone(), 10)
            .range(None, None)
            .build()
            .unwrap();
        assert_eq!(
            scanner.read_next().unwrap(),
            Some((Key::from_raw(&[1u8]), vec![1u8]))
        );
        assert_eq!(
            scanner.read_next().unwrap(),
            Some((Key::from_raw(&[2u8]), vec![2u8]))
        );
        assert_eq!(
            scanner.read_next().unwrap(),
            Some((Key::from_raw(&[3u8]), vec![3u8]))
        );
        assert_eq!(
            scanner.read_next().unwrap(),
            Some((Key::from_raw(&[4u8]), vec![4u8]))
        );
        assert_eq!(
            scanner.read_next().unwrap(),
            Some((Key::from_raw(&[5u8]), vec![5u8]))
        );
        assert_eq!(
            scanner.read_next().unwrap(),
            Some((Key::from_raw(&[6u8]), vec![6u8]))
        );
        assert_eq!(scanner.read_next().unwrap(), None);
    }
}
