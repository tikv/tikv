// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! A dedicate scanner that outputs content in each CF.

use crate::storage::mvcc::Result;
use crate::storage::txn::{Result as TxnResult, TxnEntry, TxnEntryScanner};
use crate::storage::{Cursor, Key, Snapshot, Statistics};

use super::ScannerConfig;

/// A dedicate scanner that outputs content in each CF.
///
/// Use `ScannerBuilder` to build `EntryScanner`.
///
/// Note: The implementation is almost the same as `ForwardScanner`, made a few
///       adjustments to output content in each cf.
pub struct Scanner<S: Snapshot> {
    _cfg: ScannerConfig<S>,
    _lock_cursor: Cursor<S::Iter>,
    _latest_cursor: Cursor<S::Iter>,
    _history_cursor: Cursor<S::Iter>,
    _lower_bound: Option<Key>,
    /// Is iteration started
    _is_started: bool,
    statistics: Statistics,
}

impl<S: Snapshot> TxnEntryScanner for Scanner<S> {
    fn next_entry(&mut self) -> TxnResult<Option<TxnEntry>> {
        Ok(self.read_next()?)
    }
    fn take_statistics(&mut self) -> Statistics {
        std::mem::replace(&mut self.statistics, Statistics::default())
    }
}

impl<S: Snapshot> Scanner<S> {
    pub fn new(
        _cfg: ScannerConfig<S>,
        _lock_cursor: Cursor<S::Iter>,
        _latest_cursor: Cursor<S::Iter>,
        _history_cursor: Cursor<S::Iter>,
        _lower_bound: Option<Key>,
    ) -> Result<Scanner<S>> {
        Ok(Scanner {
            _cfg,
            _lock_cursor,
            _latest_cursor,
            _history_cursor,
            _lower_bound,
            statistics: Statistics::default(),
            _is_started: false,
        })
    }

    /// Get the next txn entry, in forward order.
    pub fn read_next(&mut self) -> Result<Option<TxnEntry>> {
        Ok(None)
    }
}
