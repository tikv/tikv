// Copyright 2019 PingCAP, Inc.
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

mod backward;
mod forward;
mod util;

use kvproto::kvrpcpb::IsolationLevel;

use crate::storage::mvcc::Result;
use crate::storage::{CursorBuilder, Key, ScanMode, Snapshot, Statistics, Value};
use crate::storage::{CF_LOCK, CF_WRITE};

use self::backward::BackwardScanner;
use self::forward::ForwardScanner;

/// `Scanner` factory.
pub struct ScannerBuilder<S: Snapshot> {
    snapshot: S,
    fill_cache: bool,
    omit_value: bool,
    isolation_level: IsolationLevel,
    lower_bound: Option<Key>,
    upper_bound: Option<Key>,
    ts: u64,
    desc: bool,
}

impl<S: Snapshot> ScannerBuilder<S> {
    /// Initialize a new `Scanner`
    pub fn new(snapshot: S, ts: u64, desc: bool) -> Self {
        Self {
            snapshot,
            fill_cache: true,
            omit_value: false,
            isolation_level: IsolationLevel::SI,
            lower_bound: None,
            upper_bound: None,
            ts,
            desc,
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
    pub fn range(mut self, lower_bound: Option<Key>, upper_bound: Option<Key>) -> Self {
        self.lower_bound = lower_bound;
        self.upper_bound = upper_bound;
        self
    }

    /// Build `ForwardScanner` from the current configuration.
    pub fn build(self) -> Result<Scanner<S>> {
        let lock_cursor_builder = CursorBuilder::new(&self.snapshot, CF_LOCK)
            .range(self.lower_bound.clone(), self.upper_bound.clone())
            .fill_cache(self.fill_cache);

        let write_cursor_builder = CursorBuilder::new(&self.snapshot, CF_WRITE)
            .range(self.lower_bound.clone(), self.upper_bound.clone())
            .fill_cache(self.fill_cache);
        if self.desc {
            let lock_cursor = lock_cursor_builder.scan_mode(ScanMode::Backward).build()?;
            let write_cursor = write_cursor_builder.scan_mode(ScanMode::Backward).build()?;
            Ok(Scanner::Backward(BackwardScanner::new(
                self.snapshot,
                self.fill_cache,
                self.omit_value,
                self.isolation_level,
                self.lower_bound,
                self.upper_bound,
                self.ts,
                lock_cursor,
                write_cursor,
            )))
        } else {
            let write_cursor = write_cursor_builder.build()?;
            let lock_cursor = lock_cursor_builder.build()?;
            Ok(Scanner::Forward(ForwardScanner::new(
                self.snapshot,
                self.fill_cache,
                self.omit_value,
                self.isolation_level,
                self.lower_bound,
                self.upper_bound,
                self.ts,
                lock_cursor,
                write_cursor,
            )))
        }
    }
}

pub enum Scanner<S: Snapshot> {
    Forward(ForwardScanner<S>),
    Backward(BackwardScanner<S>),
}

impl<S: Snapshot> Scanner<S> {
    pub fn read_next(&mut self) -> Result<Option<(Key, Value)>> {
        match self {
            Scanner::Forward(scanner) => Ok(scanner.read_next()?),
            Scanner::Backward(scanner) => Ok(scanner.read_next()?),
        }
    }

    /// Take out and reset the statistics collected so far.
    pub fn take_statistics(&mut self) -> Statistics {
        match self {
            Scanner::Forward(scanner) => scanner.take_statistics(),
            Scanner::Backward(scanner) => scanner.take_statistics(),
        }
    }
}
