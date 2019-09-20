// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! A dedicate scanner that outputs content in each CF.

use std::cmp::Ordering;

use kvproto::kvrpcpb::IsolationLevel;

use crate::storage::kv::SEEK_BOUND;
use crate::storage::mvcc::write::{Write, WriteType};
use crate::storage::mvcc::Result;
use crate::storage::txn::{Result as TxnResult, TxnEntry, TxnEntryScanner};
use crate::storage::{Cursor, Key, Lock, Snapshot, Statistics};

use super::super::util::CheckLockResult;
use super::ScannerConfig;

/// A dedicate scanner that outputs content in each CF.
///
/// Use `ScannerBuilder` to build `EntryScanner`.
///
/// Note: The implementation is almost the same as `ForwardScanner`, made a few
///       adjustments to output content in each cf.
pub struct Scanner<S: Snapshot> {
    cfg: ScannerConfig<S>,
    lock_cursor: Cursor<S::Iter>,
    latest_cursor: Cursor<S::Iter>,
    history_cursor: Cursor<S::Iter>,
    lower_bound: Option<Key>,
    /// Is iteration started
    is_started: bool,
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
        cfg: ScannerConfig<S>,
        lock_cursor: Cursor<S::Iter>,
        latest_cursor: Cursor<S::Iter>,
        history_cursor: Cursor<S::Iter>,
        lower_bound: Option<Key>,
    ) -> Result<Scanner<S>> {
        Ok(Scanner {
            cfg,
            lock_cursor,
            latest_cursor,
            history_cursor,
            lower_bound,
            statistics: Statistics::default(),
            is_started: false,
        })
    }

    /// Get the next txn entry, in forward order.
    pub fn read_next(&mut self) -> Result<Option<TxnEntry>> {
        Ok(None)
    }
}
