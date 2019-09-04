// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod backward;
mod forward;
mod txn_entry;
mod util;

use engine::{CfName, CF_DEFAULT, CF_LOCK, CF_WRITE};
use kvproto::kvrpcpb::IsolationLevel;

use crate::storage::mvcc::Result;
use crate::storage::txn::Result as TxnResult;
use crate::storage::{
    Cursor, CursorBuilder, Key, ScanMode, Scanner as StoreScanner, Snapshot, Statistics, Value,
};

use self::backward::BackwardScanner;
use self::forward::ForwardScanner;
pub use self::txn_entry::Scanner as EntryScanner;

/// `Scanner` factory.
pub struct ScannerBuilder<S: Snapshot>(ScannerConfig<S>);

impl<S: Snapshot> std::ops::Deref for ScannerBuilder<S> {
    type Target = ScannerConfig<S>;
    fn deref(&self) -> &ScannerConfig<S> {
        &self.0
    }
}

impl<S: Snapshot> std::ops::DerefMut for ScannerBuilder<S> {
    fn deref_mut(&mut self) -> &mut ScannerConfig<S> {
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
    /// Defaults to `IsolationLevel::Si`.
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

    pub fn build_entry_scanner(mut self) -> Result<EntryScanner<S>> {
        let lower_bound = self.lower_bound.clone();
        let lock_cursor = self.create_cf_cursor(CF_LOCK)?;
        let write_cursor = self.create_cf_cursor(CF_WRITE)?;
        // Note: Create a default cf cursor will take key range, so we need to
        //       ensure the default cursor is created after lock and write.
        let default_cursor = self.create_cf_cursor(CF_DEFAULT)?;
        Ok(EntryScanner::new(
            self.0,
            lock_cursor,
            write_cursor,
            default_cursor,
            lower_bound,
        )?)
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
            isolation_level: IsolationLevel::Si,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::kv::Engine;
    use crate::storage::mvcc::tests::*;
    use crate::storage::mvcc::Error as MvccError;
    use crate::storage::txn::Error as TxnError;
    use crate::storage::TestEngineBuilder;
    use kvproto::kvrpcpb::Context;

    // Collect data from the scanner and assert it equals to `expected`, which is a collection of
    // (raw_key, value).
    // `None` value in `expected` means the key is locked.
    fn check_scan_result<S: Snapshot>(
        mut scanner: Scanner<S>,
        expected: &[(Vec<u8>, Option<Vec<u8>>)],
    ) {
        let mut scan_result = Vec::new();
        loop {
            match scanner.next() {
                Ok(None) => break,
                Ok(Some((key, value))) => scan_result.push((key.to_raw().unwrap(), Some(value))),
                Err(TxnError::Mvcc(MvccError::KeyIsLocked(mut info))) => {
                    scan_result.push((info.take_key(), None))
                }
                e => panic!("got error while scanning: {:?}", e),
            }
        }

        assert_eq!(scan_result, expected);
    }

    fn test_scan_with_lock_impl(desc: bool) {
        let engine = TestEngineBuilder::new().build().unwrap();

        for i in 0..5 {
            must_prewrite_put(&engine, &[i], &[b'v', i], &[i], 1);
            must_commit(&engine, &[i], 1, 2);
            must_prewrite_put(&engine, &[i], &[b'v', i], &[i], 10);
            must_commit(&engine, &[i], 10, 100);
        }

        must_acquire_pessimistic_lock(&engine, &[1], &[1], 20, 110);
        must_acquire_pessimistic_lock(&engine, &[2], &[2], 50, 110);
        must_acquire_pessimistic_lock(&engine, &[3], &[3], 105, 110);
        must_prewrite_put(&engine, &[4], b"a", &[4], 105);

        let snapshot = engine.snapshot(&Context::default()).unwrap();

        let mut expected_result = vec![
            (vec![0], Some(vec![b'v', 0])),
            (vec![1], Some(vec![b'v', 1])),
            (vec![2], Some(vec![b'v', 2])),
            (vec![3], Some(vec![b'v', 3])),
            (vec![4], Some(vec![b'v', 4])),
        ];

        if desc {
            expected_result = expected_result.into_iter().rev().collect();
        }

        let scanner = ScannerBuilder::new(snapshot.clone(), 30, desc)
            .build()
            .unwrap();
        check_scan_result(scanner, &expected_result);

        let scanner = ScannerBuilder::new(snapshot.clone(), 70, desc)
            .build()
            .unwrap();
        check_scan_result(scanner, &expected_result);

        let scanner = ScannerBuilder::new(snapshot.clone(), 103, desc)
            .build()
            .unwrap();
        check_scan_result(scanner, &expected_result);

        // The value of key 4 is locked at 105 so that it can't be read at 106
        if desc {
            expected_result[0].1 = None;
        } else {
            expected_result[4].1 = None;
        }
        let scanner = ScannerBuilder::new(snapshot, 106, desc).build().unwrap();
        check_scan_result(scanner, &expected_result);
    }

    #[test]
    fn test_scan_with_lock() {
        test_scan_with_lock_impl(false);
        test_scan_with_lock_impl(true);
    }
}
