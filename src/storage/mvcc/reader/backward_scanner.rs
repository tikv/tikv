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

use storage::mvcc::write::{Write, WriteType};
use storage::mvcc::{Lock, Result};
use storage::{Cursor, CursorBuilder, Key, ScanMode, Snapshot, Statistics, Value};
use storage::{CF_DEFAULT, CF_LOCK, CF_WRITE};

// When there are many versions for the user key, after several tries,
// we will use seek to locate the right position. But this will turn around
// the write cf's iterator's direction inside RocksDB, and the next user key
// need to turn back the direction to backward. As we have tested, turn around
// iterator's direction from forward to backward is as expensive as seek in
// RocksDB, so don't set REVERSE_SEEK_BOUND too small.
const REVERSE_SEEK_BOUND: u64 = 32;

/// `BackwardScanner` factory.
pub struct BackwardScannerBuilder<S: Snapshot> {
    snapshot: S,
    fill_cache: bool,
    omit_value: bool,
    isolation_level: IsolationLevel,
    lower_bound: Option<Vec<u8>>,
    upper_bound: Option<Vec<u8>>,
    ts: u64,
}

impl<S: Snapshot> BackwardScannerBuilder<S> {
    /// Initialize a new `BackwardScanner`
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

    /// Limit the range to `[lower_bound, upper_bound)` in which the `BackwardScanner` should scan.
    /// `None` means unbounded.
    ///
    /// Default is `(None, None)`.
    #[inline]
    pub fn range(mut self, lower_bound: Option<Vec<u8>>, upper_bound: Option<Vec<u8>>) -> Self {
        self.lower_bound = lower_bound;
        self.upper_bound = upper_bound;
        self
    }

    /// Build `BackwardScanner` from the current configuration.
    pub fn build(self) -> Result<BackwardScanner<S>> {
        let lock_cursor = CursorBuilder::new(&self.snapshot, CF_LOCK)
            .bound(self.lower_bound.clone(), self.upper_bound.clone())
            .fill_cache(self.fill_cache)
            .scan_mode(ScanMode::Backward)
            .build()?;

        let write_cursor = CursorBuilder::new(&self.snapshot, CF_WRITE)
            .bound(self.lower_bound.clone(), self.upper_bound.clone())
            .fill_cache(self.fill_cache)
            .scan_mode(ScanMode::Backward)
            .build()?;

        Ok(BackwardScanner {
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

/// This struct can be used to scan keys starting from the given user key in the reverse order
/// (less than).
///
/// Internally, for each key, rollbacks are ignored and smaller version will be tried. If the
/// isolation level is SI, locks will be checked first.
///
/// Use `BackwardScannerBuilder` to build `BackwardScanner`.
pub struct BackwardScanner<S: Snapshot> {
    snapshot: S,
    fill_cache: bool,
    omit_value: bool,
    isolation_level: IsolationLevel,

    /// `lower_bound` and `upper_bound` is only used to create `default_cursor`. It will be
    /// consumed after `default_cursor` is being created.
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

impl<S: Snapshot> BackwardScanner<S> {
    /// Take out and reset the statistics collected so far.
    pub fn take_statistics(&mut self) -> Statistics {
        ::std::mem::replace(&mut self.statistics, Statistics::default())
    }

    /// Get reference of the statics collected so far.
    pub fn get_statistics(&self) -> &Statistics {
        &self.statistics
    }

    /// Get the next key-value pair, in backward order.
    pub fn read_next(&mut self) -> Result<Option<(Key, Value)>> {
        if !self.is_started {
            self.write_cursor.seek_to_last(&mut self.statistics.write);
            self.lock_cursor.seek_to_last(&mut self.statistics.lock);
            self.is_started = true;
        }

        // Similar to forward scanner, the general idea is to simultaneously step write
        // cursor and lock cursor. Please refer to `ForwardScanner` for details.

        loop {
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
                    (None, None) => return Ok(None),
                    (None, Some(lk)) => (lk.to_vec(), false, true),
                    (Some(wk), None) => (Key::truncate_ts_for(wk)?.to_vec(), true, false),
                    (Some(wk), Some(lk)) => match Key::truncate_ts_for(wk)?.cmp(lk) {
                        Ordering::Less => {
                            // We are scanning from largest user key to smallest user key, so this
                            // indicate that we meet a lock first, thus its corresponding write
                            // does not exist.
                            (lk.to_vec(), false, true)
                        }
                        Ordering::Greater => {
                            // We meet write first, so the lock of the write key does not exist.
                            (Key::truncate_ts_for(wk)?.to_vec(), true, false)
                        }
                        Ordering::Equal => (Key::truncate_ts_for(wk)?.to_vec(), true, true),
                    },
                }
            };
            let current_user_key = Key::from_encoded(current_user_key);
            let result = self.reverse_get(&current_user_key, has_write, has_lock);
            if has_write {
                self.write_cursor.near_reverse_seek(
                    &current_user_key,
                    false,
                    &mut self.statistics.write,
                )?;
            }
            if has_lock {
                self.lock_cursor.prev(&mut self.statistics.lock);
            }
            if let Some(v) = result? {
                return Ok(Some((current_user_key, v)));
            }
        }
    }

    /// Attempt to get the value of a key specified by `user_key` and `self.ts` in reverse order.
    /// This function requires that the write cursor is currently pointing to the earliest version
    /// of `user_key`.
    #[inline]
    fn reverse_get(
        &mut self,
        user_key: &Key,
        has_write: bool,
        has_lock: bool,
    ) -> Result<Option<Value>> {
        let mut safe_ts = self.ts;
        match self.isolation_level {
            IsolationLevel::SI => {
                if has_lock {
                    assert!(self.lock_cursor.valid());
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

        // At first, we try to use several `prev()` to get the desired version.

        // We need to save last desired version, because when we may move to an unwanted version
        // at any time.
        let mut last_version = None;

        for _ in 0..REVERSE_SEEK_BOUND {
            let mut is_done = false;
            {
                let current_key = self.write_cursor.key(&mut self.statistics.write);
                if !Key::is_user_key_eq(current_key, user_key.encoded().as_slice())
                    || Key::decode_ts_from(current_key)? > safe_ts
                {
                    // Meet another key or meet an unwanted version. We use `last_version` as the return.
                    is_done = true;
                }
            }
            if is_done {
                return Ok(self.handle_last_write(last_version, user_key)?);
            }

            let write = Write::parse(self.write_cursor.value(&mut self.statistics.write))?;
            self.statistics.write.processed += 1;

            match write.write_type {
                WriteType::Put | WriteType::Delete => last_version = Some(write),
                WriteType::Lock | WriteType::Rollback => {}
            }

            self.write_cursor.prev(&mut self.statistics.write);

            if !self.write_cursor.valid() {
                // Key space ended. We use `last_version` as the return.
                return Ok(self.handle_last_write(last_version, user_key)?);
            }
        }

        // After several `prev()`, we still not get the latest version for the specified ts,
        // use seek to locate the latest version.
        let last_handled_key = self.write_cursor.key(&mut self.statistics.write).to_vec();

        let seek_key = user_key.clone().append_ts(safe_ts);
        // TODO: Replace by cast + seek().
        self.write_cursor
            .internal_seek(&seek_key, &mut self.statistics.write)?;
        assert!(self.write_cursor.valid());

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
            assert!(self.write_cursor.valid());

            let mut is_done = false;
            {
                let current_key = self.write_cursor.key(&mut self.statistics.write);
                if !Key::is_user_key_eq(current_key, user_key.encoded().as_slice()) {
                    // Meet another key.
                    return Ok(None);
                }
                if current_key >= last_handled_key.as_slice() {
                    // We reach the last handled key, it means we have checked all versions for
                    // this user key. We use `last_version` as return.
                    is_done = true;
                }
            }
            if is_done {
                return Ok(self.handle_last_write(last_version, user_key)?);
            }
        }
    }

    /// Handle last write. Last write may be PUT or DELETE. If it is a PUT, value should be
    /// load.
    #[inline]
    fn handle_last_write(
        &mut self,
        some_write: Option<Write>,
        user_key: &Key,
    ) -> Result<Option<Value>> {
        match some_write {
            None => Ok(None),
            Some(write) => match write.write_type {
                WriteType::Put => Ok(Some(self.load_data_by_write(write, user_key)?)),
                WriteType::Delete => Ok(None),
                _ => unreachable!(),
            },
        }
    }

    /// Load the value by the given `some_write`. If value is carried in `some_write`, it will be
    /// returned directly. Otherwise there will be a default CF look up.
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

    /// Create the default cursor if it doesn't exist.
    fn ensure_default_cursor(&mut self) -> Result<()> {
        if self.default_cursor.is_some() {
            return Ok(());
        }
        let cursor = CursorBuilder::new(&self.snapshot, CF_DEFAULT)
            .bound(self.lower_bound.take(), self.upper_bound.take())
            .fill_cache(self.fill_cache)
            .scan_mode(ScanMode::Backward)
            .build()?;
        self.default_cursor = Some(cursor);
        Ok(())
    }
}
