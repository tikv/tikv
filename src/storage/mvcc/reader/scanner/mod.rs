// Copyright 2019 TiKV Project Authors.
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
use crate::storage::txn::Result as TxnResult;
use crate::storage::{
    CfName, Cursor, CursorBuilder, Key, ScanMode, Scanner as StoreScanner, Snapshot, Statistics,
    Value, CF_DEFAULT, CF_LOCK, CF_WRITE,
};

use self::backward::BackwardScanner;
use self::forward::ForwardScanner;

/// `Scanner` factory.
pub struct ScannerBuilder<S: Snapshot>(ScannerConfig<S>);

impl<S: Snapshot> std::ops::Deref for ScannerBuilder<S> {
    type Target = ScannerConfig<S>;
    fn deref<'a>(&'a self) -> &'a ScannerConfig<S> {
        &self.0
    }
}

impl<S: Snapshot> std::ops::DerefMut for ScannerBuilder<S> {
    fn deref_mut<'a>(&'a mut self) -> &'a mut ScannerConfig<S> {
        &mut self.0
    }
}

impl<S: Snapshot> ScannerBuilder<S> {
    /// Initialize a new `ScannerBuilder`
    pub fn new(snapshot: S, ts: u64, desc: bool) -> Self {
        Self(ScannerConfig::new(snapshot, ts, desc))
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

    /// Build `Scanner` from the current configuration.
    pub fn build(mut self) -> Result<Scanner<S>> {
        let lock_cursor = self.create_cf_cursor(CF_LOCK)?;
        let write_cursor = self.create_cf_cursor(CF_WRITE)?;
        if self.desc {
            Ok(Scanner::Backward(BackwardScanner::new(
                self.0,
                lock_cursor,
                write_cursor,
            )))
        } else {
            Ok(Scanner::Forward(ForwardScanner::new(
                self.0,
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

impl<S: Snapshot> StoreScanner for Scanner<S> {
    fn next(&mut self) -> TxnResult<Option<(Key, Value)>> {
        match self {
            Scanner::Forward(scanner) => Ok(scanner.read_next()?),
            Scanner::Backward(scanner) => Ok(scanner.read_next()?),
        }
    }
    /// Take out and reset the statistics collected so far.
    fn take_statistics(&mut self) -> Statistics {
        match self {
            Scanner::Forward(scanner) => scanner.take_statistics(),
            Scanner::Backward(scanner) => scanner.take_statistics(),
        }
    }
}

pub struct ScannerConfig<S: Snapshot> {
    snapshot: S,
    fill_cache: bool,
    omit_value: bool,
    isolation_level: IsolationLevel,

    /// `lower_bound` and `upper_bound` is used to create `default_cursor`. `upper_bound`
    /// is used in initial seek(or `lower_bound` in initial backward seek) as well. They will be consumed after `default_cursor` is being
    /// created.
    lower_bound: Option<Key>,
    upper_bound: Option<Key>,

    ts: u64,
    desc: bool,
}

impl<S: Snapshot> ScannerConfig<S> {
    fn new(snapshot: S, ts: u64, desc: bool) -> Self {
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

    #[inline]
    fn scan_mode(&self) -> ScanMode {
        if self.desc {
            ScanMode::Backward
        } else {
            ScanMode::Forward
        }
    }

    /// Create the cursor.
    #[inline]
    fn create_cf_cursor(&mut self, cf: CfName) -> Result<Cursor<S::Iter>> {
        let (lower, upper) = if cf == CF_DEFAULT {
            (self.lower_bound.take(), self.upper_bound.take())
        } else {
            (self.lower_bound.clone(), self.upper_bound.clone())
        };
        let cursor = CursorBuilder::new(&self.snapshot, cf)
            .range(lower, upper)
            .fill_cache(self.fill_cache)
            .scan_mode(self.scan_mode())
            .build()?;
        Ok(cursor)
    }
}
