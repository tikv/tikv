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
use storage::{Cursor, Key, Snapshot, Statistics, Value, CF_DEFAULT, CF_WRITE};
use util::codec::number;

/// `PointGetter` factory.
pub struct PointGetterBuilder<S: Snapshot> {
    snapshot: S,
    multi: bool,
    fill_cache: bool,
    omit_value: bool,
    isolation_level: IsolationLevel,
}

impl<S: Snapshot> PointGetterBuilder<S> {
    /// Initialize a new `PointGetterBuilder`.
    pub fn new(snapshot: S) -> Self {
        Self {
            snapshot,
            multi: true,
            fill_cache: true,
            omit_value: false,
            isolation_level: IsolationLevel::SI,
        }
    }

    /// Set whether multiple values will be retrieved. If `multi` is `false`, only single value
    /// will be retrieved. Prefix filter will be used thus it will be faster.
    ///
    /// Defaults to `true`.
    #[inline]
    pub fn multi(mut self, multi: bool) -> Self {
        self.multi = multi;
        self
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

    /// Build `PointGetter` from the current configuration.
    pub fn build(self) -> Result<PointGetter<S>> {
        let write_cursor = super::util::CursorBuilder::new(&self.snapshot, CF_WRITE)
            .fill_cache(self.fill_cache)
            .prefix_seek(!self.multi)
            .build()?;

        Ok(PointGetter {
            snapshot: self.snapshot,
            multi: self.multi,
            fill_cache: self.fill_cache,
            omit_value: self.omit_value,
            isolation_level: self.isolation_level,

            statistics: Statistics::default(),

            read_once: false,

            write_cursor,
            default_cursor: None,
        })
    }
}

/// This struct can be used to get the value of a user key. Internally, rollbacks are ignored and
/// smaller version will be tried. If the isolation level is SI, locks will be checked first.
///
/// If `multi` is `false`, prefix filter will be used so that you can only call `read_next`
/// once, otherwise there will be incorrect results.
///
/// If `multi` is `true`, the instance can be re-used to get multiple keys. However it will
/// be optimal if these keys are get in ascending order and are relatively close to each other.
///
/// Use `PointGetterBuilder` to build `PointGetter`.
pub struct PointGetter<S: Snapshot> {
    snapshot: S,
    multi: bool,
    fill_cache: bool,
    omit_value: bool,
    isolation_level: IsolationLevel,

    statistics: Statistics,

    /// Whether there is already a `read_next` call. When `multi == false`, we use this field
    /// to check that `read_next` is called only once.
    read_once: bool,

    write_cursor: Cursor<S::Iter>,

    /// Default cursor is optional since when value is short we don't need to look up in
    /// the default CF.
    default_cursor: Option<Cursor<S::Iter>>,
}

impl<S: Snapshot> PointGetter<S> {
    /// Take out and reset the statistics collected so far.
    #[inline]
    pub fn take_statistics(&mut self) -> Statistics {
        ::std::mem::replace(&mut self.statistics, Statistics::default())
    }

    /// Get the value of a user key. See `PointGetter` for details.
    pub fn read_next(&mut self, user_key: &Key, mut ts: u64) -> Result<Option<Value>> {
        if !self.multi && self.read_once {
            panic!("PointGetter(multi=false) must not call `read_next` multiple times.");
        }

        self.read_once = true;

        if self.isolation_level == IsolationLevel::SI {
            // Check for locks that signal concurrent writes in SI.
            ts = super::util::load_and_check_lock(
                &self.snapshot,
                user_key,
                ts,
                &mut self.statistics,
            )?;
        }

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
        let cursor = super::util::CursorBuilder::new(&self.snapshot, CF_DEFAULT)
            .fill_cache(self.fill_cache)
            .prefix_seek(!self.multi)
            .build()?;
        self.default_cursor = Some(cursor);
        Ok(())
    }
}
