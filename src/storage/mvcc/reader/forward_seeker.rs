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

use raftstore::store::engine::IterOption;
use storage::mvcc::write::{Write, WriteType};
use storage::mvcc::Result;
use storage::{Cursor, Key, ScanMode, Snapshot, Statistics, Value, CF_DEFAULT, CF_LOCK, CF_WRITE};

pub struct ForwardSeekerBuilder<S: Snapshot> {
    snapshot: S,
    fill_cache: bool,
    omit_value: bool,
    isolation_level: IsolationLevel,
    lower_bound: Option<Vec<u8>>,
    upper_bound: Option<Vec<u8>>,
}

impl<S: Snapshot> ForwardSeekerBuilder<S> {
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

    #[inline]
    pub fn fill_cache(mut self, fill_cache: bool) -> Self {
        self.fill_cache = fill_cache;
        self
    }

    #[inline]
    pub fn omit_value(mut self, omit_value: bool) -> Self {
        self.omit_value = omit_value;
        self
    }

    #[inline]
    pub fn isolation_level(mut self, isolation_level: IsolationLevel) -> Self {
        self.isolation_level = isolation_level;
        self
    }

    #[inline]
    pub fn range(mut self, lower_bound: Option<Vec<u8>>, upper_bound: Option<Vec<u8>>) -> Self {
        self.lower_bound = lower_bound;
        self.upper_bound = upper_bound;
        self
    }

    pub fn build(self) -> Result<ForwardSeeker<S>> {
        let lock_cursor = self.snapshot.iter_cf(
            CF_LOCK,
            IterOption::new(
                self.lower_bound.clone(),
                self.upper_bound.clone(),
                self.fill_cache,
            ),
            ScanMode::Forward,
        )?;

        let write_cursor = self.snapshot.iter_cf(
            CF_WRITE,
            IterOption::new(
                self.lower_bound.clone(),
                self.upper_bound.clone(),
                self.fill_cache,
            ),
            ScanMode::Forward,
        )?;

        let default_cursor = if self.omit_value {
            None
        } else {
            Some(self.snapshot.iter_cf(
                CF_DEFAULT,
                IterOption::new(
                    self.lower_bound.clone(),
                    self.upper_bound.clone(),
                    self.fill_cache,
                ),
                ScanMode::Forward,
            )?)
        };

        Ok(ForwardSeeker {
            snapshot: self.snapshot,
            omit_value: self.omit_value,
            isolation_level: self.isolation_level,
            lock_cursor,
            write_cursor,
            default_cursor,
            statistics: Statistics::default(),
        })
    }
}

pub struct ForwardSeeker<S: Snapshot> {
    snapshot: S,
    omit_value: bool,
    isolation_level: IsolationLevel,

    lock_cursor: Cursor<S::Iter>,
    write_cursor: Cursor<S::Iter>,
    default_cursor: Option<Cursor<S::Iter>>,

    statistics: Statistics,
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

    pub fn read_next(&mut self, mut key: Key, ts: u64) -> Result<Option<(Key, Value)>> {
        //        assert!(*self.scan_mode.as_ref().unwrap() == ScanMode::Forward);
        //        self.ensure_write_cursor()?;
        //        self.ensure_lock_cursor()?;
        // TODO: Panic if given key is less than current key
        // (Maybe we can panic in cursor's code)

        let (mut write_valid, mut lock_valid) = (true, true);

        loop {
            key = {
                let (mut w_key, mut l_key) = (None, None);
                if write_valid {
                    if self
                        .write_cursor
                        .near_seek(&key, &mut self.statistics.write)?
                    {
                        w_key = Some(self.write_cursor.key());
                    } else {
                        w_key = None;
                        write_valid = false;
                    }
                }
                if lock_valid {
                    if self.lock_cursor.near_seek(&key, &mut self.statistics.lock)? {
                        l_key = Some(self.lock_cursor.key());
                    } else {
                        l_key = None;
                        lock_valid = false;
                    }
                }
                match (w_key, l_key) {
                    (None, None) => return Ok(None),
                    (None, Some(k)) => Key::from_encoded(k.to_vec()),
                    (Some(k), None) => Key::from_encoded(k.to_vec()).truncate_ts()?,
                    (Some(wk), Some(lk)) => if wk < lk {
                        Key::from_encoded(wk.to_vec()).truncate_ts()?
                    } else {
                        Key::from_encoded(lk.to_vec())
                    },
                }
            };
            if let Some(v) = self.get(&key, ts)? {
                return Ok(Some((key, v)));
            }
            key = key.append_ts(0);
        }
    }

    fn get(&mut self, key: &Key, mut ts: u64) -> Result<Option<Value>> {
        match self.isolation_level {
            IsolationLevel::SI => {
                // TODO: Use `self.lock_cursor` here
                ts =
                    super::util::load_and_check_lock(&self.snapshot, key, ts, &mut self.statistics)?
            }
            IsolationLevel::RC => {}
        }

        // TODO: following code is duplicated with PointGetter::read_next
        loop {
            // Near seek `${key}_${commit_ts}` in write CF.

            // TODO: each next in near_seek should count flow_stats as well.
            if !self
                .write_cursor
                .near_seek(&key.append_ts(ts), &mut self.statistics.write)?
            {
                // Cursor reaches range end and key is not found
                return Ok(None);
            }

            self.statistics.write.flow_stats.read_bytes +=
                self.write_cursor.key().len() + self.write_cursor.value().len();
            self.statistics.write.flow_stats.read_keys += 1;

            let write_key = Key::from_encoded(self.write_cursor.key().to_vec());
            let commit_ts = write_key.decode_ts()?;
            let write_user_key = write_key.truncate_ts()?;
            if write_user_key != *key {
                // Found another key, current key with commit_ts < ts must not exist.
                return Ok(None);
            }

            let write = Write::parse(self.write_cursor.value())?;
            self.statistics.write.processed += 1;

            match write.write_type {
                WriteType::Put => {
                    if self.omit_value {
                        return Ok(Some(vec![]));
                    } else {
                        let value = super::util::load_data_from_write(
                            self.default_cursor.as_mut().unwrap(),
                            key,
                            write,
                            &mut self.statistics,
                        )?;
                        return Ok(Some(value));
                    }
                }
                WriteType::Delete => return Ok(None),
                WriteType::Lock | WriteType::Rollback => ts = commit_ts - 1,
            }
        }
    }
}
