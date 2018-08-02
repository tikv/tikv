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

use kvproto::kvrpcpb::IsolationLevel;

use super::util::CursorBuilder;
use storage::mvcc::write::{Write, WriteType};
use storage::mvcc::{Lock, Result};
use storage::{Cursor, Key, Snapshot, Statistics, Value, CF_DEFAULT, CF_LOCK, CF_WRITE};
use util::codec::number;

pub struct ForwardSeekerBuilder<S: Snapshot> {
    snapshot: S,
    fill_cache: bool,
    omit_value: bool,
    isolation_level: IsolationLevel,
    lower_bound: Option<Vec<u8>>,
    upper_bound: Option<Vec<u8>>,
}

/// `ForwardSeeker` factory.
impl<S: Snapshot> ForwardSeekerBuilder<S> {
    /// Initialize a new `ForwardSeeker`
    pub fn new(snapshot: S) -> Self {
        Self {
            snapshot,
            fill_cache: true,
            omit_value: false,
            isolation_level: IsolationLevel::SI,
            lower_bound: None,
            upper_bound: None,
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

    /// Limit the range in which the `ForwardSeeker` should seek. `None` means unbounded.
    /// TODO: Is the range `[lower_bound, upper_bound)`?
    ///
    /// Default is `(None, None)`.
    #[inline]
    pub fn range(mut self, lower_bound: Option<Vec<u8>>, upper_bound: Option<Vec<u8>>) -> Self {
        self.lower_bound = lower_bound;
        self.upper_bound = upper_bound;
        self
    }

    /// Build `ForwardSeeker` from the current configuration.
    pub fn build(self) -> Result<ForwardSeeker<S>> {
        let lock_cursor = CursorBuilder::new(&self.snapshot, CF_LOCK)
            .bound(self.lower_bound.clone(), self.upper_bound.clone())
            .fill_cache(self.fill_cache)
            .build()?;

        let write_cursor = CursorBuilder::new(&self.snapshot, CF_WRITE)
            .bound(self.lower_bound.clone(), self.upper_bound.clone())
            .fill_cache(self.fill_cache)
            .build()?;

        Ok(ForwardSeeker {
            snapshot: self.snapshot,
            fill_cache: self.fill_cache,
            omit_value: self.omit_value,
            isolation_level: self.isolation_level,
            lower_bound: self.lower_bound,
            upper_bound: self.upper_bound,
            lock_cursor,
            write_cursor,
            default_cursor: None,
            statistics: Statistics::default(),
            last_read_key: Key::from_encoded(vec![]),
        })
    }
}

/// This struct can be used to find next key greater or equal to a given user key. Internally,
/// rollbacks are ignored and smaller version will be tried. If the isolation level is SI, locks
/// will be checked first.
///
/// This struct keeps the iterator moves forward. If you use this to perform seeking multiple times,
/// then you are not allowed to seek a key that is less than the previous one.
///
/// Use `ForwardSeekerBuilder` to build `ForwardSeeker`.
pub struct ForwardSeeker<S: Snapshot> {
    snapshot: S,
    fill_cache: bool,
    omit_value: bool,
    isolation_level: IsolationLevel,

    /// `lower_bound` and `upper_bound` is only used to create `default_cursor`. It will be consumed
    /// after default_cursor's being created.
    lower_bound: Option<Vec<u8>>,
    upper_bound: Option<Vec<u8>>,

    lock_cursor: Cursor<S::Iter>,
    write_cursor: Cursor<S::Iter>,

    /// `default cursor` is lazy created only when it's needed.
    default_cursor: Option<Cursor<S::Iter>>,

    statistics: Statistics,

    last_read_key: Key,
}

impl<S: Snapshot> ForwardSeeker<S> {
    /// Take out and reset the statistics collected so far.
    pub fn take_statistics(&mut self) -> Statistics {
        ::std::mem::replace(&mut self.statistics, Statistics::default())
    }

    /// Get reference of the statics collected so far.
    pub fn get_statistics(&self) -> &Statistics {
        &self.statistics
    }

    /// Get the next key-value pair, where the key is greater or equal to given `key`.
    pub fn read_next(&mut self, mut key: Key, ts: u64) -> Result<Option<(Key, Value)>> {
        if key < self.last_read_key {
            panic!(
                "ForwardSeeker: try to read from key {} which is less than previous read key {}",
                key, self.last_read_key
            );
        }
        // TODO: Can we avoid cloning?
        self.last_read_key = key.clone();

        let (mut write_valid, mut lock_valid) = (true, true);

        // TODO: Add more comments to explain the logic.
        loop {
            let mut has_lock = false;
            key = {
                let (mut w_key, mut l_key) = (None, None);
                // Try to advance both write_cursor and lock_valid
                if write_valid {
                    if self
                        .write_cursor
                        .near_seek(&key, &mut self.statistics.write)?
                    {
                        w_key = Some(self.write_cursor.key(&mut self.statistics.write));
                    } else {
                        w_key = None;
                        write_valid = false;
                    }
                }
                if lock_valid {
                    if self.lock_cursor.near_seek(&key, &mut self.statistics.lock)? {
                        l_key = Some(self.lock_cursor.key(&mut self.statistics.lock));
                    } else {
                        l_key = None;
                        lock_valid = false;
                    }
                }
                match (w_key, l_key) {
                    (None, None) => return Ok(None),
                    (None, Some(k)) => {
                        has_lock = true;
                        Key::from_encoded(k.to_vec())
                    }
                    (Some(k), None) => Key::from_encoded(k.to_vec()).truncate_ts()?,
                    (Some(wk), Some(lk)) => if wk < lk {
                        // Lock greater than `wk`, so `wk` must not have lock.
                        Key::from_encoded(wk.to_vec()).truncate_ts()?
                    } else {
                        has_lock = true;
                        Key::from_encoded(lk.to_vec())
                    },
                }
            };
            let lock = if has_lock {
                Some(self.lock_cursor.value(&mut self.statistics.lock).to_vec())
            } else {
                None
            };
            if let Some(v) = self.get(&key, ts, lock)? {
                return Ok(Some((key, v)));
            }
            key = key.append_ts(0);
        }
    }

    /// Try to get the value of a key. Returns empty value if `omit_value` is set. Returns `None` if
    /// No valid value on this key.
    fn get(&mut self, user_key: &Key, mut ts: u64, lock: Option<Vec<u8>>) -> Result<Option<Value>> {
        match self.isolation_level {
            IsolationLevel::SI => {
                if let Some(lock) = lock {
                    let lock = Lock::parse(&lock)?;
                    ts = super::util::check_lock(user_key, ts, &lock)?
                }
            }
            IsolationLevel::RC => {}
        }

        // TODO: following code is very similar with PointGetter::read_next but different
        let encoded_user_key = user_key.encoded();

        // First seek to `${user_key}_${ts}`.
        self.write_cursor
            .near_seek(&user_key.append_ts(ts), &mut self.statistics.write)?;

        loop {
            if !self.write_cursor.valid() {
                // Key space ended.
                return Ok(None);
            }
            // We may move forward / seek to another key. In this case, the scan ends.
            {
                let cursor_key = self.write_cursor.key(&mut self.statistics.write);
                if cursor_key.len() != encoded_user_key.len() + number::U64_SIZE
                    || !cursor_key.starts_with(encoded_user_key)
                {
                    // Meet another key.
                    return Ok(None);
                }
            }

            let write = Write::parse(self.write_cursor.value(&mut self.statistics.write))?;
            self.statistics.write.processed += 1;

            match write.write_type {
                WriteType::Put => {
                    if self.omit_value {
                        return Ok(Some(vec![]));
                    }
                    match write.short_value {
                        Some(value) => {
                            // Value is carried in `write`.
                            return Ok(Some(value));
                        }
                        None => {
                            // Value is in the default CF.
                            self.ensure_default_cursor()?;
                            let value = super::util::load_data_by_write(
                                &mut self.default_cursor.as_mut().unwrap(),
                                user_key,
                                write,
                                &mut self.statistics,
                            )?;
                            return Ok(Some(value));
                        }
                    }
                }
                WriteType::Delete => return Ok(None),
                WriteType::Lock | WriteType::Rollback => {
                    // Continue iterate next `write`.
                }
            }

            self.write_cursor.next(&mut self.statistics.write);
        }
    }

    /// Create the default cursor if it doesn't exist.
    fn ensure_default_cursor(&mut self) -> Result<()> {
        if self.default_cursor.is_some() {
            return Ok(());
        }
        let cursor = CursorBuilder::new(&self.snapshot, CF_DEFAULT)
            .bound(self.lower_bound.clone(), self.upper_bound.clone())
            .fill_cache(self.fill_cache)
            .build()?;
        self.default_cursor = Some(cursor);
        Ok(())
    }
}
