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
                    (Some(wk), Some(lk)) => {
                        let write_user_key = Key::truncate_ts_for(wk)?;
                        match write_user_key.cmp(lk) {
                            Ordering::Less => {
                                // We are scanning from largest user key to smallest user key, so this
                                // indicate that we meet a lock first, thus its corresponding write
                                // does not exist.
                                (lk.to_vec(), false, true)
                            }
                            Ordering::Greater => {
                                // We meet write first, so the lock of the write key does not exist.
                                (write_user_key.to_vec(), true, false)
                            }
                            Ordering::Equal => (write_user_key.to_vec(), true, true),
                        }
                    }
                }
            };
            let current_user_key = Key::from_encoded(current_user_key);

            let result = self.reverse_get(&current_user_key, has_write, has_lock);

            if has_write {
                // Skip rest later versions and point to previous user key.
                self.move_write_cursor_to_prev_user_key(&current_user_key)?;
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
        let mut ts = self.ts;

        if has_lock {
            match self.isolation_level {
                IsolationLevel::SI => {
                    assert!(self.lock_cursor.valid());
                    let lock_value = self.lock_cursor.value(&mut self.statistics.lock);
                    let lock = Lock::parse(lock_value)?;
                    ts = super::util::check_lock(user_key, ts, &lock)?
                }
                IsolationLevel::RC => {}
            }
        }

        if !has_write {
            return Ok(None);
        }
        assert!(self.write_cursor.valid());

        // At first, we try to use several `prev()` to get the desired version.

        // We need to save last desired version, because when we may move to an unwanted version
        // at any time.
        let mut last_version = None;
        let mut last_checked_commit_ts = 0;

        for i in 0..REVERSE_SEEK_BOUND {
            if i > 0 {
                // We are already pointing at the smallest version, so we don't need to prev()
                // for the first iteration. So we will totally call `prev()` function
                // `REVERSE_SEEK_BOUND - 1` times.
                self.write_cursor.prev(&mut self.statistics.write);
                if !self.write_cursor.valid() {
                    // Key space ended. We use `last_version` as the return.
                    return Ok(self.handle_last_version(last_version, user_key)?);
                }
            }

            let mut is_done = false;
            {
                let current_key = self.write_cursor.key(&mut self.statistics.write);
                last_checked_commit_ts = Key::decode_ts_from(current_key)?;

                if !Key::is_user_key_eq(current_key, user_key.encoded().as_slice())
                    || last_checked_commit_ts > ts
                {
                    // Meet another key or meet an unwanted version. We use `last_version` as the return.
                    // TODO: If we meet another user key here, we don't need to compare the key
                    // again when trying to move to prev user key in `read_next`.
                    is_done = true;
                }
            }
            if is_done {
                return Ok(self.handle_last_version(last_version, user_key)?);
            }

            let write = Write::parse(self.write_cursor.value(&mut self.statistics.write))?;
            self.statistics.write.processed += 1;

            match write.write_type {
                WriteType::Put | WriteType::Delete => last_version = Some(write),
                WriteType::Lock | WriteType::Rollback => {}
            }
        }

        // At this time, we must have current commit_ts <= ts. If commit_ts == ts,
        // we don't need to seek any more and we can just utilize `last_version`.
        if last_checked_commit_ts == ts {
            return Ok(self.handle_last_version(last_version, user_key)?);
        }

        // After several `prev()`, we still not get the latest version for the specified ts,
        // use seek to locate the latest version.
        let last_handled_key = self.write_cursor.key(&mut self.statistics.write).to_vec();

        let seek_key = user_key.clone().append_ts(ts);
        assert!(seek_key.encoded().as_slice() < last_handled_key.as_slice());

        // TODO: Replace by cast + seek().
        self.write_cursor
            .internal_seek(&seek_key, &mut self.statistics.write)?;
        assert!(self.write_cursor.valid());

        loop {
            // After seek, or after some `next()`, we may reach `last_handled_key` again. It
            // means we have checked all versions for this user key. We use `last_version` as
            // return.
            let mut is_done = false;
            {
                let current_key = self.write_cursor.key(&mut self.statistics.write);
                // We should never reach another user key.
                assert!(Key::is_user_key_eq(
                    current_key,
                    user_key.encoded().as_slice()
                ));
                if current_key >= last_handled_key.as_slice() {
                    // We reach the last handled key,
                    is_done = true;
                }
            }
            if is_done {
                return Ok(self.handle_last_version(last_version, user_key)?);
            }

            let write = Write::parse(self.write_cursor.value(&mut self.statistics.write))?;
            self.statistics.write.processed += 1;

            match write.write_type {
                WriteType::Put => {
                    return Ok(Some(self.reverse_load_data_by_write(write, user_key)?))
                }
                WriteType::Delete => return Ok(None),
                WriteType::Lock | WriteType::Rollback => {
                    // Continue iterate next `write`.
                    self.write_cursor.next(&mut self.statistics.write);
                    assert!(self.write_cursor.valid());
                }
            }
        }
    }

    /// Handle last version. Last version may be PUT or DELETE. If it is a PUT, value should be
    /// load.
    #[inline]
    fn handle_last_version(
        &mut self,
        some_write: Option<Write>,
        user_key: &Key,
    ) -> Result<Option<Value>> {
        match some_write {
            None => Ok(None),
            Some(write) => match write.write_type {
                WriteType::Put => Ok(Some(self.reverse_load_data_by_write(write, user_key)?)),
                WriteType::Delete => Ok(None),
                _ => unreachable!(),
            },
        }
    }

    /// Load the value by the given `some_write`. If value is carried in `some_write`, it will be
    /// returned directly. Otherwise there will be a default CF look up.
    ///
    /// The implementation is similar to `PointGetter::load_data_by_write`.
    #[inline]
    fn reverse_load_data_by_write(&mut self, write: Write, user_key: &Key) -> Result<Value> {
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
                let value = super::util::near_reverse_load_data_by_write(
                    &mut self.default_cursor.as_mut().unwrap(),
                    user_key,
                    write,
                    &mut self.statistics,
                )?;
                Ok(value)
            }
        }
    }

    /// After `self.reverse_get()`, our write cursor may be pointing to current user key (if we
    /// found a desired version), or previous user key (if there is no desired version), or
    /// out of bound.
    ///
    /// If it is pointing to current user key, we need to step it until we meet a new
    /// key. We first try to `prev()` a few times. If still not reaching another user
    /// key, we `seek_for_prev()`.
    #[inline]
    fn move_write_cursor_to_prev_user_key(&mut self, current_user_key: &Key) -> Result<()> {
        for i in 0..SEEK_BOUND {
            if i > 0 {
                self.write_cursor.prev(&mut self.statistics.write);
            }
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
        }

        // We have not found another user key for now, so we directly `seek_for_prev()`.
        // After that, we must pointing to another key, or out of bound.
        self.write_cursor
            .seek_for_prev(current_user_key, &mut self.statistics.write)?;

        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use storage::engine::{self, TEMP_DIR};
    use storage::mvcc::tests::*;
    use storage::ALL_CFS;
    use storage::{Engine, Key};

    use kvproto::kvrpcpb::Context;

    #[test]
    fn test_basic_1() {
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();

        // Generate REVERSE_SEEK_BOUND / 2 Put for key [10].
        let k = &[10 as u8];
        for ts in 0..REVERSE_SEEK_BOUND / 2 {
            must_prewrite_put(&engine, k, &[ts as u8], k, ts);
            must_commit(&engine, k, ts, ts);
        }

        // Generate REVERSE_SEEK_BOUND + 1 Put for key [9].
        let k = &[9 as u8];
        for ts in 0..REVERSE_SEEK_BOUND + 1 {
            must_prewrite_put(&engine, k, &[ts as u8], k, ts);
            must_commit(&engine, k, ts, ts);
        }

        // Generate REVERSE_SEEK_BOUND / 2 Put and REVERSE_SEEK_BOUND / 2 + 1 Rollback for key [8].
        let k = &[8 as u8];
        for ts in 0..REVERSE_SEEK_BOUND + 1 {
            must_prewrite_put(&engine, k, &[ts as u8], k, ts);
            if ts < REVERSE_SEEK_BOUND / 2 {
                must_commit(&engine, k, ts, ts);
            } else {
                must_rollback(&engine, k, ts);
            }
        }

        // Generate REVERSE_SEEK_BOUND / 2 Put, 1 Delete and REVERSE_SEEK_BOUND / 2 Rollback
        // for key [7].
        let k = &[7 as u8];
        for ts in 0..REVERSE_SEEK_BOUND / 2 {
            must_prewrite_put(&engine, k, &[ts as u8], k, ts);
            must_commit(&engine, k, ts, ts);
        }
        {
            let ts = REVERSE_SEEK_BOUND / 2;
            must_prewrite_delete(&engine, k, k, ts);
            must_commit(&engine, k, ts, ts);
        }
        for ts in REVERSE_SEEK_BOUND / 2 + 1..REVERSE_SEEK_BOUND + 1 {
            must_prewrite_put(&engine, k, &[ts as u8], k, ts);
            must_rollback(&engine, k, ts);
        }

        // Generate 1 PUT for key [6].
        let k = &[6 as u8];
        for ts in 0..1 {
            must_prewrite_put(&engine, k, &[ts as u8], k, ts);
            must_commit(&engine, k, ts, ts);
        }

        // Generate REVERSE_SEEK_BOUND + 1 Rollback for key [5].
        let k = &[5 as u8];
        for ts in 0..REVERSE_SEEK_BOUND + 1 {
            must_prewrite_put(&engine, k, &[ts as u8], k, ts);
            must_rollback(&engine, k, ts);
        }

        // Generate 1 PUT with ts = REVERSE_SEEK_BOUND and 1 PUT
        // with ts = REVERSE_SEEK_BOUND + 1 for key [4].
        let k = &[4 as u8];
        for ts in REVERSE_SEEK_BOUND..REVERSE_SEEK_BOUND + 2 {
            must_prewrite_put(&engine, k, &[ts as u8], k, ts);
            must_commit(&engine, k, ts, ts);
        }

        // Assume REVERSE_SEEK_BOUND == 4, we have keys:
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10

        let snapshot = engine.snapshot(&Context::new()).unwrap();

        let mut scanner = BackwardScannerBuilder::new(snapshot, REVERSE_SEEK_BOUND)
            .range(None, Some(vec![11 as u8]))
            .build()
            .unwrap();

        // Initial position: 1 seek_to_last
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //                                                  ^cursor
        // When get key [10]: REVERSE_SEEK_BOUND / 2 prev
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //                                             ^cursor
        //                                               ^last_version
        // After get key [10]: 0 prev
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //                                             ^
        assert_eq!(
            scanner.read_next().unwrap(),
            Some((
                Key::from_raw(&[10 as u8]),
                vec![(REVERSE_SEEK_BOUND / 2 - 1) as u8]
            ))
        );
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.prev, REVERSE_SEEK_BOUND as usize / 2);
        assert_eq!(statistics.write.seek, 1); // 1 seek_to_last
        assert_eq!(statistics.write.next, 0);
        assert_eq!(statistics.write.seek_for_prev, 0);

        // Before get key [9]:
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //                                             ^cursor
        // When get key [9]:
        // First, REVERSE_SEEK_BOUND - 1 prev
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //                                       ^cursor
        //                                       ^last_version
        //                                       ^last_handled_key
        // Bound reached, 1 extra seek.
        // After seek:
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //                                     ^cursor
        //                                       ^last_handled_key
        // Now we got key[9].
        // After get key [9]: 1 prev
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //                                   ^cursor
        assert_eq!(
            scanner.read_next().unwrap(),
            Some((Key::from_raw(&[9 as u8]), vec![REVERSE_SEEK_BOUND as u8]))
        );
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.prev, REVERSE_SEEK_BOUND as usize);
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, 0);
        assert_eq!(statistics.write.seek_for_prev, 0);

        // Before get key [8]:
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //                                   ^cursor
        // When get key [8]:
        // First, REVERSE_SEEK_BOUND - 1 prev
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //                             ^cursor
        //                                 ^last_version
        //                             ^last_handled_key
        // Bound reached, 1 extra seek.
        // After seek:
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //                           ^cursor
        //                                 ^last_version
        //                             ^last_handled_key
        // Got ROLLBACK, so 1 next:
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //                             ^cursor
        //                                 ^last_version
        //                             ^last_handled_key
        // Hit last_handled_key, so use last_version and we get key [8].
        // After get key [8]: 2 prev
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //                         ^cursor
        assert_eq!(
            scanner.read_next().unwrap(),
            Some((
                Key::from_raw(&[8 as u8]),
                vec![(REVERSE_SEEK_BOUND / 2 - 1) as u8]
            ))
        );
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.prev, REVERSE_SEEK_BOUND as usize + 1);
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, 1);
        assert_eq!(statistics.write.seek_for_prev, 0);

        // Before get key [7]:
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //                         ^cursor
        // First, REVERSE_SEEK_BOUND - 1 prev
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //                   ^cursor
        // Bound reached, 1 extra seek.
        // After seek:
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //                 ^cursor
        // Got ROLLBACK, so 1 next:
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //                   ^cursor
        // Hit last_handled_key, so use last_version and we get None.
        // Skip this key's versions and go to next key: 2 prev
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //               ^cursor
        // Current commit ts > ts is not satisfied, so 1 prev:
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //             ^cursor
        //               ^last_version
        // We reached another key, use last_version and we get [6].
        // After get key [6]: 0 prev
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //             ^cursor
        assert_eq!(
            scanner.read_next().unwrap(),
            Some((Key::from_raw(&[6 as u8]), vec![0 as u8]))
        );
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.prev, REVERSE_SEEK_BOUND as usize + 2);
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, 1);
        assert_eq!(statistics.write.seek_for_prev, 0);

        // Before get key [5]:
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //             ^cursor
        // First, REVERSE_SEEK_BOUND - 1 prev
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //       ^cursor (last_version == None)
        // Bound reached, 1 extra seek.
        // After seek:
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //     ^cursor (last_version == None)
        // Got ROLLBACK, so 1 next:
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //       ^cursor (last_version == None)
        // Hit last_handled_key, so use last_version and we get None.
        // Skip this key's versions and go to next key: 2 prev
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        //   ^cursor
        // Current commit ts > ts is not satisfied, so 1 prev:
        // 4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        // ^cursor
        //   ^last_version
        // Current commit ts > ts is satisfied, use last_version and we get [4].
        // After get key [4]: 1 prev
        //   4 4 5 5 5 5 5 6 7 7 7 7 7 8 8 8 8 8 9 9 9 9 9 10 10
        // ^cursor
        assert_eq!(
            scanner.read_next().unwrap(),
            Some((Key::from_raw(&[4 as u8]), vec![REVERSE_SEEK_BOUND as u8]))
        );
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.prev, REVERSE_SEEK_BOUND as usize + 3);
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, 1);
        assert_eq!(statistics.write.seek_for_prev, 0);

        // Scan end.
        assert_eq!(scanner.read_next().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.write.prev, 0);
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 0);
        assert_eq!(statistics.write.seek_for_prev, 0);
    }

    #[test]
    fn test_many_tombstones() {
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();

        // Generate RocksDB tombstones in write cf.
        let start_ts = 1;
        let safe_point = 2;
        for i in 0..256 {
            for y in 0..256 {
                let pk = &[i as u8, y as u8];
                must_prewrite_put(&engine, pk, b"", pk, start_ts);
                must_rollback(&engine, pk, start_ts);
                // Generate 65534 RocksDB tombstones between [0,0] and [255,255].
                if !((i == 0 && y == 0) || (i == 255 && y == 255)) {
                    must_gc(&engine, pk, safe_point);
                }
            }
        }

        // Generate 256 locks in lock cf.
        let start_ts = 3;
        for i in 0..256 {
            let pk = &[i as u8];
            must_prewrite_put(&engine, pk, b"", pk, start_ts);
        }

        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let row = &[255 as u8];
        let k = Key::from_raw(row);

        // Call reverse scan
        let ts = 2;
        let mut scanner = BackwardScannerBuilder::new(snapshot, ts)
            .range(None, Some(k.take_encoded()))
            .build()
            .unwrap();
        assert_eq!(scanner.read_next().unwrap(), None);
        let statistics = scanner.take_statistics();
        assert_eq!(statistics.lock.prev, 255);
        assert_eq!(statistics.write.prev, 1);
    }

}
