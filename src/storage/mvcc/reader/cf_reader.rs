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

use raftstore::store::engine::IterOption;
use storage::mvcc::Lock;
use storage::mvcc::Result;
use storage::{Cursor, Key, ScanMode, Snapshot, Statistics, CF_LOCK, CF_WRITE};

/// `CFReader` factory.
pub struct CFReaderBuilder<S: Snapshot> {
    snapshot: S,
    fill_cache: bool,
}

impl<S: Snapshot> CFReaderBuilder<S> {
    /// Initialize a new `CFReaderBuilder`.
    pub fn new(snapshot: S) -> Self {
        Self {
            snapshot,
            fill_cache: true,
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

    /// Build `CFReader` from the current configuration.
    pub fn build(self) -> Result<CFReader<S>> {
        Ok(CFReader {
            snapshot: self.snapshot,
            fill_cache: self.fill_cache,
            statistics: Statistics::default(),

            lock_cursor: None,
            write_cursor: None,
        })
    }
}

/// A handy utility around functions in `mvcc::reader::util`. This struct does not provide
/// performance guarantee. Please carefully review each interface's requirement.
///
/// Use `CFReaderBuilder` to build `CFReader`.
pub struct CFReader<S: Snapshot> {
    snapshot: S,
    fill_cache: bool,

    statistics: Statistics,

    lock_cursor: Option<Cursor<S::Iter>>,
    write_cursor: Option<Cursor<S::Iter>>,
}

impl<S: Snapshot> CFReader<S> {
    /// Take out and reset the statistics collected so far.
    pub fn take_statistics(&mut self) -> Statistics {
        ::std::mem::replace(&mut self.statistics, Statistics::default())
    }

    /// Get the lock of a user key in the lock CF.
    ///
    /// Internally, a db get will be performed.
    #[inline]
    pub fn load_lock(&mut self, key: &Key) -> Result<Option<Lock>> {
        // TODO: `load_lock` should respect `fill_cache` options as well.
        super::util::load_lock(&self.snapshot, key, &mut self.statistics)
    }

    /// Get a lock of a user key in the lock CF. If lock exists, it will be checked to see whether
    /// it conflicts with the given `ts`. If there is no conflict or no lock, the safe `ts` will be
    /// returned.
    ///
    /// Internally, a db get will be performed.
    #[inline]
    pub fn load_and_check_lock(&mut self, key: &Key, ts: u64) -> Result<u64> {
        super::util::load_and_check_lock(&self.snapshot, key, ts, &mut self.statistics)
    }

    /// Iterate and get all locks in the lock CF that `predicate` returns `true` within the given
    /// key space (specified by `start_key` and `limit`). If `limit` is `0`, the key space only
    /// has left bound.
    #[inline]
    pub fn scan_locks<F>(
        &mut self,
        predicate: F,
        start_key: Option<&Key>,
        limit: usize,
    ) -> Result<Vec<(Key, Lock)>>
    where
        F: Fn(&Lock) -> bool,
    {
        self.ensure_lock_cursor()?;
        let lock_cursor = self.lock_cursor.as_mut().unwrap();
        super::util::scan_locks(
            lock_cursor,
            predicate,
            start_key,
            limit,
            &mut self.statistics,
        )
    }

    /// Iterate and get all user keys in the write CF within the given key space (specified by
    /// `start_key` and `limit`). If `limit` is `0`, no keys will be returned.
    ///
    /// The return type is `(keys, next_start_key)`. `next_start_key` is the `start_key` that
    /// can be used to continue scanning keys. If `next_start_key` is `None`, it means that
    /// there is no more keys.
    pub fn scan_keys(
        &mut self,
        start_key: Option<&Key>,
        limit: usize,
    ) -> Result<(Vec<Key>, Option<Key>)> {
        self.ensure_write_cursor()?;
        let write_cursor = self.write_cursor.as_mut().unwrap();
        super::util::scan_keys(write_cursor, start_key, limit, &mut self.statistics)
    }

    /// Create the lock cursor if it doesn't exist.
    fn ensure_lock_cursor(&mut self) -> Result<()> {
        if self.lock_cursor.is_some() {
            return Ok(());
        }
        let iter_opt = IterOption::new(None, None, self.fill_cache);
        let iter = self.snapshot.iter_cf(CF_LOCK, iter_opt, ScanMode::Forward)?;
        self.lock_cursor = Some(iter);
        Ok(())
    }

    /// Create the write cursor if it doesn't exist.
    fn ensure_write_cursor(&mut self) -> Result<()> {
        if self.write_cursor.is_some() {
            return Ok(());
        }
        let iter_opt = IterOption::new(None, None, self.fill_cache);
        let iter = self
            .snapshot
            .iter_cf(CF_WRITE, iter_opt, ScanMode::Forward)?;
        self.write_cursor = Some(iter);
        Ok(())
    }
}
