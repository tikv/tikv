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

use super::REVERSE_SEEK_BOUND;
use storage::mvcc::write::{Write, WriteType};
use storage::mvcc::{Lock, Result};
use storage::{
    Cursor, CursorBuilder, Key, ScanMode, Snapshot, Statistics, Value, CF_DEFAULT, CF_LOCK,
    CF_WRITE,
};

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

/// This struct can be used to find next key less or equal to a given user key. Internally,
/// rollbacks are ignored and smaller version will be tried. If the isolation level is SI, locks
/// will be checked first..
///
/// Use `BackwardScannerBuilder` to build `BackwardScanner`.
pub struct BackwardScanner<S: Snapshot> {
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

        loop {
            let (key, has_write, has_lock) = {
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
                        Ordering::Less => (lk.to_vec(), false, true),
                        Ordering::Greater => (Key::truncate_ts_for(wk)?.to_vec(), true, false),
                        Ordering::Equal => (Key::truncate_ts_for(wk)?.to_vec(), true, true),
                    },
                }
            };

            let key = Key::from_encoded(key);

            let lock = if has_lock {
                Some(self.lock_cursor.value(&mut self.statistics.lock).to_vec())
            } else {
                None
            };
            // Don't return error here. We need to seek to the next position then.
            let res = self.reverse_get(&key, lock);

            if has_write {
                self.write_cursor
                    .near_reverse_seek(&key, false, &mut self.statistics.write)?;
            }
            if has_lock {
                self.lock_cursor.prev(&mut self.statistics.lock);
            }

            if let Some(v) = res? {
                return Ok(Some((key, v)));
            }
        }
    }

    fn reverse_get(&mut self, user_key: &Key, lock: Option<Vec<u8>>) -> Result<Option<Value>> {
        let mut ts = self.ts;

        match self.isolation_level {
            IsolationLevel::SI => {
                // TODO: Ensure statistics.lock.processed is updated correctly
                if let Some(lock) = lock {
                    self.statistics.lock.processed += 1;
                    let lock = Lock::parse(&lock)?;
                    ts = super::util::check_lock(user_key, ts, &lock)?;
                }
            }
            IsolationLevel::RC => {}
        }

        // Get value for this user key.
        // At first, we use several prev to try to get the latest version.
        let mut lastest_version = (None /* start_ts */, None /* short value */);
        let mut last_handled_key: Option<Vec<u8>> = None;
        for _ in 0..REVERSE_SEEK_BOUND {
            if !self.write_cursor.valid() {
                return self.get_value(user_key, lastest_version.0, lastest_version.1);
            }

            let mut write = {
                let (commit_ts, key) = {
                    last_handled_key =
                        Some(self.write_cursor.key(&mut self.statistics.write).to_vec());
                    let w_key = Key::from_encoded(
                        self.write_cursor.key(&mut self.statistics.write).to_vec(),
                    );
                    (w_key.decode_ts()?, w_key.truncate_ts()?)
                };

                // reach neighbour user key or can't see this version.
                if ts < commit_ts || &key != user_key {
                    assert!(&key <= user_key);
                    return self.get_value(user_key, lastest_version.0, lastest_version.1);
                }
                self.statistics.write.processed += 1;
                Write::parse(self.write_cursor.value(&mut self.statistics.write))?
            };

            match write.write_type {
                WriteType::Put => {
                    if write.short_value.is_some() {
                        if self.omit_value {
                            lastest_version = (None, Some(vec![]));
                        } else {
                            lastest_version = (None, write.short_value.take());
                        }
                    } else {
                        lastest_version = (Some(write.start_ts), None);
                    }
                }
                WriteType::Delete => lastest_version = (None, None),
                WriteType::Lock | WriteType::Rollback => {}
            }
            self.write_cursor.prev(&mut self.statistics.write);
        }

        // After several prev, we still not get the latest version for the specified ts,
        // use seek to locate the latest version.
        let key = user_key.clone().append_ts(ts);
        let valid = self
            .write_cursor
            .internal_seek(&key, &mut self.statistics.write)?;
        assert!(valid);
        loop {
            let mut write = {
                // If we reach the last handled key, it means we have checked all versions
                // for this user key.
                if self.write_cursor.key(&mut self.statistics.write)
                    >= last_handled_key.as_ref().unwrap().as_slice()
                {
                    return self.get_value(user_key, lastest_version.0, lastest_version.1);
                }

                let w_key =
                    Key::from_encoded(self.write_cursor.key(&mut self.statistics.write).to_vec());
                let commit_ts = w_key.decode_ts()?;
                assert!(commit_ts <= ts);
                let key = w_key.truncate_ts()?;
                assert_eq!(&key, user_key);
                self.statistics.write.processed += 1;
                Write::parse(self.write_cursor.value(&mut self.statistics.write))?
            };

            match write.write_type {
                WriteType::Put => {
                    if write.short_value.is_some() {
                        if self.omit_value {
                            return Ok(Some(vec![]));
                        } else {
                            return Ok(write.short_value.take());
                        }
                    } else {
                        return Ok(Some(self.load_data(user_key, write.start_ts)?));
                    }
                }
                WriteType::Delete => return Ok(None),
                WriteType::Lock | WriteType::Rollback => {
                    let success = self.write_cursor.next(&mut self.statistics.write);
                    assert!(success);
                }
            }
        }
    }

    #[inline]
    fn get_value(
        &mut self,
        user_key: &Key,
        start_ts: Option<u64>,
        short_value: Option<Vec<u8>>,
    ) -> Result<Option<Value>> {
        if let Some(ts) = start_ts {
            Ok(Some(self.load_data(user_key, ts)?))
        } else {
            Ok(short_value)
        }
    }

    fn load_data(&mut self, key: &Key, ts: u64) -> Result<Value> {
        if self.omit_value {
            return Ok(vec![]);
        }
        self.ensure_default_cursor()?;

        let k = key.clone().append_ts(ts);
        let res = self
            .default_cursor
            .as_mut()
            .unwrap()
            .near_seek_get(&k, false, &mut self.statistics.data)?
            .unwrap_or_else(|| panic!("key {} not found, ts {}", key, ts))
            .to_vec();
        self.statistics.data.processed += 1;
        Ok(res)
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
