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
use storage::mvcc::write::WriteType;
use storage::mvcc::Result;
use storage::{Cursor, Key, ScanMode, Snapshot, Statistics, Value, CF_DEFAULT, CF_WRITE};

/// Build `IterOption` (which is later used to build `Cursor`) according to configurations.
fn build_iter_opt(fill_cache: bool, prefix_filter: bool) -> IterOption {
    let mut iter_opt = IterOption::new(None, None, fill_cache);
    if prefix_filter {
        // Use prefix bloom filter if we only want to get a single value.
        iter_opt = iter_opt.use_prefix_seek().set_prefix_same_as_start(true);
    }
    iter_opt
}

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
        Ok(PointGetter {
            snapshot: self.snapshot.clone(),
            multi: self.multi,
            fill_cache: self.fill_cache,
            omit_value: self.omit_value,
            isolation_level: self.isolation_level,

            statistics: Statistics::default(),

            read_once: false,

            write_cursor: Some(self.snapshot.iter_cf(
                CF_WRITE,
                build_iter_opt(self.fill_cache, !self.multi),
                ScanMode::Forward,
            )?),
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

    /// Write cursor will never be `None`. It is `Option` so that we can take it away.
    write_cursor: Option<Cursor<S::Iter>>,

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
    pub fn read_next(&mut self, key: &Key, mut ts: u64) -> Result<Option<Value>> {
        if !self.multi && self.read_once {
            panic!("PointGetter(multi=false) must not call `read_next` multiple times.");
        }

        self.read_once = true;

        if self.isolation_level == IsolationLevel::SI {
            // Check for locks that signal concurrent writes in SI.
            ts = super::util::load_and_check_lock(&self.snapshot, key, ts, &mut self.statistics)?;
        }

        let mut ret_value = None;

        // Iterate all versions <= max_ts for the key.
        // We take `write_cursor` to avoid keeping a mutable reference of `self`.
        let mut write_cursor = self.write_cursor.take().unwrap();
        {
            let mut writes_iter = super::ForwardWriteIter::new(&mut write_cursor, &key, ts)?;
            for result in &mut writes_iter {
                let (_commit_ts, write) = result?;
                match write.write_type {
                    WriteType::Put => {
                        if self.omit_value {
                            ret_value = Some(vec![]);
                        } else {
                            match write.short_value {
                                Some(value) => {
                                    // Value is carried in `write`.
                                    ret_value = Some(value);
                                }
                                None => {
                                    // Value is in the default CF.
                                    self.ensure_default_cursor()?;
                                    let value = super::util::load_data_by_write(
                                        &mut self.default_cursor.as_mut().unwrap(),
                                        key,
                                        write,
                                        &mut self.statistics,
                                    )?;
                                    ret_value = Some(value);
                                }
                            }
                        }
                        break;
                    }
                    WriteType::Delete => {
                        ret_value = None;
                        break;
                    }
                    WriteType::Lock | WriteType::Rollback => {
                        // Continue iterate next `write`.
                    }
                }
            }

            self.statistics.add(&writes_iter.take_statistics());
        }
        self.write_cursor = Some(write_cursor);
        Ok(ret_value)
    }

    /// Create the default cursor if it doesn't exist.
    fn ensure_default_cursor(&mut self) -> Result<()> {
        if self.default_cursor.is_some() {
            return Ok(());
        }
        let iter_opt = build_iter_opt(self.fill_cache, !self.multi);
        let iter = self
            .snapshot
            .iter_cf(CF_DEFAULT, iter_opt, ScanMode::Forward)?;
        self.default_cursor = Some(iter);
        Ok(())
    }
}
