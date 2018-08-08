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
use storage::mvcc::{Lock, Result};
use storage::{Cursor, CursorBuilder, Key, Snapshot, Statistics, Value};
use storage::{CF_DEFAULT, CF_LOCK, CF_WRITE};

pub struct ForwardScannerBuilder<S: Snapshot> {
    snapshot: S,
    fill_cache: bool,
    omit_value: bool,
    isolation_level: IsolationLevel,
    lower_bound: Option<Vec<u8>>,
    upper_bound: Option<Vec<u8>>,
    ts: u64,
}

/// `ForwardScanner` factory.
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
    pub fn range(mut self, lower_bound: Option<Vec<u8>>, upper_bound: Option<Vec<u8>>) -> Self {
        self.lower_bound = lower_bound;
        self.upper_bound = upper_bound;
        self
    }

    /// Build `ForwardScanner` from the current configuration.
    pub fn build(self) -> Result<ForwardScanner<S>> {
        let lock_cursor = CursorBuilder::new(&self.snapshot, CF_LOCK)
            .bound(self.lower_bound.clone(), self.upper_bound.clone())
            .fill_cache(self.fill_cache)
            .build()?;

        let write_cursor = CursorBuilder::new(&self.snapshot, CF_WRITE)
            .bound(self.lower_bound.clone(), self.upper_bound.clone())
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

    /// `lower_bound` and `upper_bound` is only used to create `default_cursor`. It will be consumed
    /// after default_cursor's being created.
    lower_bound: Option<Vec<u8>>,
    upper_bound: Option<Vec<u8>>,

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

    /// Get reference of the statics collected so far.
    // TODO: Remove this function once `BackwardScanner` is landed.
    pub fn get_statistics(&self) -> &Statistics {
        &self.statistics
    }

    /// Get the next key-value pair, in forward order.
    pub fn read_next(&mut self) -> Result<Option<(Key, Value)>> {
        if !self.is_started {
            self.write_cursor.seek_to_first(&mut self.statistics.write);
            self.lock_cursor.seek_to_first(&mut self.statistics.lock);
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
                match (w_key, l_key) {
                    (None, None) => {
                        // Both cursors yield `None`: we know that there is nothing remaining.
                        return Ok(None);
                    }
                    (None, Some(k)) => {
                        // Write cursor yields `None` but lock cursor yields something:
                        // In RC, it means we got nothing.
                        // In SI, we need to check if the lock will cause conflict.
                        (k.to_vec(), false, true)
                    }
                    (Some(k), None) => {
                        // Write cursor yields something but lock cursor yields `None`:
                        // We need to further step write cursor to our desired version
                        (Key::truncate_ts_for(k)?.to_vec(), true, false)
                    }
                    (Some(wk), Some(lk)) => match Key::truncate_ts_for(wk)?.cmp(lk) {
                        Ordering::Less => {
                            // Write cursor user key < lock cursor, it means the lock of the
                            // current key that write cursor is pointing to does not exist.
                            (Key::truncate_ts_for(wk)?.to_vec(), true, false)
                        }
                        Ordering::Greater => {
                            // Write cursor user key > lock cursor, it means we got a lock of a
                            // key that does not have a write. In SI, we need to check if the
                            // lock will cause conflict.
                            (lk.to_vec(), false, true)
                        }
                        Ordering::Equal => {
                            // Write cursor user key == lock cursor, it means the lock of the
                            // current key that write cursor is pointing to *exists*.
                            (lk.to_vec(), true, true)
                        }
                    },
                }
            };
            let current_user_key = Key::from_encoded(current_user_key);

            // Attempt to read specified version of the key. Note that we may get `None`
            // indicating that no desired version is found, or a DELETE version is found, or
            // when `has_write == false`. We may also get `Err` due to key lock. We need to
            // ensure that the scanner keeps working for future calls even when meeting key
            // lock errors (why??), so we don't apply `?` operator to the result here thus
            // iterators can be moved forward.
            let result = self.get(&current_user_key, has_write, has_lock);

            if has_write {
                self.move_write_cursor_to_next_user_key(&current_user_key)?;
            }
            if has_lock {
                self.lock_cursor.next(&mut self.statistics.lock);
            }

            // If we got something, it can be just used as the return value. Otherwise, we need
            // to continue stepping the cursor.
            if let Some(v) = result? {
                return Ok(Some((current_user_key, v)));
            }
        }
    }

    /// Attempt to get the value of a key specified by `user_key` and `self.ts`.
    #[inline]
    fn get(&mut self, user_key: &Key, has_write: bool, has_lock: bool) -> Result<Option<Value>> {
        let mut safe_ts = self.ts;
        match self.isolation_level {
            IsolationLevel::SI => {
                if has_lock {
                    let lock_value = self.lock_cursor.value(&mut self.statistics.lock);
                    let lock = Lock::parse(lock_value)?;
                    safe_ts = super::util::check_lock(user_key, safe_ts, &lock)?
                }
            }
            IsolationLevel::RC => {}
        }

        if !has_write {
            return Ok(None);
        }
        assert!(self.write_cursor.valid());

        // The logic starting from here is similar to `PointGetter`.

        // Try to iterate to `${user_key}_${safe_ts}`. We first `next()` for a few times,
        // and if we have not reached where we want, we use `seek()`.

        // Whether we have *not* reached where we want by `next()`.
        let mut needs_seek = true;

        for _ in 0..SEEK_BOUND {
            {
                let current_key = self.write_cursor.key(&mut self.statistics.write);
                if !Key::is_user_key_eq(current_key, user_key.encoded().as_slice()) {
                    // Meet another key.
                    return Ok(None);
                }
                if Key::decode_ts_from(current_key)? <= safe_ts {
                    // Founded, don't need to seek again.
                    needs_seek = false;
                    break;
                }
            }
            self.write_cursor.next(&mut self.statistics.write);
            if !self.write_cursor.valid() {
                // Key space ended.
                return Ok(None);
            }
        }
        // If we have not found `${user_key}_${safe_ts}` in a few `next()`, directly `seek()`.
        if needs_seek {
            self.write_cursor.seek(
                &user_key.clone().append_ts(safe_ts),
                &mut self.statistics.write,
            )?;
            if !self.write_cursor.valid() {
                // Key space ended.
                return Ok(None);
            }
            let current_key = self.write_cursor.key(&mut self.statistics.write);
            if !Key::is_user_key_eq(current_key, user_key.encoded().as_slice()) {
                // Meet another key.
                return Ok(None);
            }
        }

        // Now we must have reached the first key >= `${user_key}_${safe_ts}`. However, we may
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
            if !Key::is_user_key_eq(current_key, user_key.encoded().as_slice()) {
                // Meet another key.
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
        for _ in 0..SEEK_BOUND {
            if !self.write_cursor.valid() {
                // Key space ended. We are done here.
                return Ok(());
            }
            {
                let current_key = self.write_cursor.key(&mut self.statistics.write);
                if !Key::is_user_key_eq(current_key, current_user_key.encoded().as_slice()) {
                    // Found another user key. We are done here.
                    return Ok(());
                }
            }
            self.write_cursor.next(&mut self.statistics.write);
        }

        // We have not found another user key for now, so we directly `seek()`.
        // After that, we must pointing to another key, or out of bound.
        self.write_cursor.seek(
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
            .bound(self.lower_bound.take(), self.upper_bound.take())
            .fill_cache(self.fill_cache)
            .build()?;
        self.default_cursor = Some(cursor);
        Ok(())
    }
}
