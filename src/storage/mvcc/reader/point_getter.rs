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

use storage::mvcc::write::{Write, WriteType};
use storage::mvcc::Result;
use storage::{Cursor, Key, Snapshot, Statistics, Value};

pub struct PointGetter<S: Snapshot> {
    snapshot: S,
    multi: bool,
    fill_cache: bool,
    key_only: bool,
    isolation_level: IsolationLevel,

    statistics: Statistics,

    /// Whether there is already a `read_next` call. When `multi == false`, we use this field
    /// to check that `read_next` is called only once.
    read_once: bool,

    write_cursor: Cursor<S::Iter>,
    default_cursor: Cursor<S::Iter>,
}

impl<S: Snapshot> PointGetter<S> {
    /// Take out and reset the statistics collected so far.
    pub fn take_statistics(&mut self) -> Statistics {
        let mut statistics = Statistics::default();
        ::std::mem::swap(&mut statistics, &mut self.statistics);
        statistics
    }

    /// Get the value of a user key. Internally, rollbacks are ignored and smaller version
    /// will be tried. If the isolation level is SI, locks will be checked first.
    ///
    /// If `multi` is `false`, prefix filter will be used so that you can only call `read_next`
    /// once, otherwise there will be incorrect results.
    ///
    /// If `multi` is `true`, the instance can be re-used to get multiple keys. However it will
    /// be optimal if these keys are get in ascending order and are relatively close to each other.
    pub fn read_next(&mut self, key: &Key, mut ts: u64) -> Result<Option<Value>> {
        if !self.multi && self.read_once {
            panic!("PointGetter(multi=false) must not call `read_next` multiple times.");
        }

        // Check for locks that signal concurrent writes.
        match self.isolation_level {
            IsolationLevel::SI => {
                ts =
                    super::util::load_and_check_lock(&self.snapshot, key, ts, &mut self.statistics)?
            }
            IsolationLevel::RC => {}
        }

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
                    if self.key_only {
                        return Ok(Some(vec![]));
                    } else {
                        let value = super::util::load_data_from_write(
                            &mut self.default_cursor,
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
