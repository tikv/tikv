// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod backward;
mod forward;

use engine::{CfName, IterOption, CF_DEFAULT, CF_LOCK, CF_WRITE};
use kvproto::kvrpcpb::IsolationLevel;
use txn_types::{Key, TimeStamp, TsSet, Value};

use self::backward::BackwardKvScanner;
use self::forward::{ForwardKvScanner, ForwardScanner, LatestEntryPolicy, LatestKvPolicy};
use crate::storage::kv::{
    CfStatistics, Cursor, CursorBuilder, Iterator, ScanMode, Snapshot, Statistics,
};
use crate::storage::mvcc::{default_not_found_error, Result};
use crate::storage::txn::{Result as TxnResult, Scanner as StoreScanner};

pub use self::forward::EntryScanner;

pub struct ScannerBuilder<S: Snapshot>(ScannerConfig<S>);

impl<S: Snapshot> ScannerBuilder<S> {
    /// Initialize a new `ScannerBuilder`
    pub fn new(snapshot: S, ts: TimeStamp, desc: bool) -> Self {
        Self(ScannerConfig::new(snapshot, ts, desc))
    }

    /// Set whether or not read operations should fill the cache.
    ///
    /// Defaults to `true`.
    #[inline]
    pub fn fill_cache(mut self, fill_cache: bool) -> Self {
        self.0.fill_cache = fill_cache;
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
        self.0.omit_value = omit_value;
        self
    }

    /// Set the isolation level.
    ///
    /// Defaults to `IsolationLevel::Si`.
    #[inline]
    pub fn isolation_level(mut self, isolation_level: IsolationLevel) -> Self {
        self.0.isolation_level = isolation_level;
        self
    }

    /// Limit the range to `[lower_bound, upper_bound)` in which the `ForwardKvScanner` should scan.
    /// `None` means unbounded.
    ///
    /// Default is `(None, None)`.
    #[inline]
    pub fn range(mut self, lower_bound: Option<Key>, upper_bound: Option<Key>) -> Self {
        self.0.lower_bound = lower_bound;
        self.0.upper_bound = upper_bound;
        self
    }

    /// Set locks that the scanner can bypass. Locks with start_ts in the specified set will be
    /// ignored during scanning.
    ///
    /// Default is empty.
    #[inline]
    pub fn bypass_locks(mut self, locks: TsSet) -> Self {
        self.0.bypass_locks = locks;
        self
    }

    /// Set the hint for the minimum commit ts we want to scan.
    ///
    /// Default is empty.
    #[inline]
    pub fn hint_min_ts(mut self, min_ts: Option<TimeStamp>) -> Self {
        self.0.hint_min_ts = min_ts;
        self
    }

    /// Set the hint for the maximum commit ts we want to scan.
    ///
    /// Default is empty.
    #[inline]
    pub fn hint_max_ts(mut self, max_ts: Option<TimeStamp>) -> Self {
        self.0.hint_max_ts = max_ts;
        self
    }

    /// Build `Scanner` from the current configuration.
    pub fn build(mut self) -> Result<Scanner<S>> {
        let lock_cursor = self.0.create_cf_cursor(CF_LOCK)?;
        let write_cursor = self.0.create_cf_cursor(CF_WRITE)?;
        if self.0.desc {
            Ok(Scanner::Backward(BackwardKvScanner::new(
                self.0,
                lock_cursor,
                write_cursor,
            )))
        } else {
            Ok(Scanner::Forward(ForwardScanner::new(
                self.0,
                lock_cursor,
                write_cursor,
                None,
                LatestKvPolicy,
            )))
        }
    }

    pub fn build_entry_scanner(
        mut self,
        after_ts: TimeStamp,
        output_delete: bool,
    ) -> Result<EntryScanner<S>> {
        let lock_cursor = self.0.create_cf_cursor(CF_LOCK)?;
        let write_cursor = self.0.create_cf_cursor(CF_WRITE)?;
        // Note: Create a default cf cursor will take key range, so we need to
        //       ensure the default cursor is created after lock and write.
        let default_cursor = self.0.create_cf_cursor(CF_DEFAULT)?;
        Ok(ForwardScanner::new(
            self.0,
            lock_cursor,
            write_cursor,
            Some(default_cursor),
            LatestEntryPolicy::new(after_ts, output_delete),
        ))
    }
}

pub enum Scanner<S: Snapshot> {
    Forward(ForwardKvScanner<S>),
    Backward(BackwardKvScanner<S>),
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
    // hint for we will only scan data with commit ts >= hint_min_ts
    hint_min_ts: Option<TimeStamp>,
    // hint for we will only scan data with commit ts <= hint_max_ts
    hint_max_ts: Option<TimeStamp>,

    ts: TimeStamp,
    desc: bool,

    bypass_locks: TsSet,
}

impl<S: Snapshot> ScannerConfig<S> {
    fn new(snapshot: S, ts: TimeStamp, desc: bool) -> Self {
        Self {
            snapshot,
            fill_cache: true,
            omit_value: false,
            isolation_level: IsolationLevel::Si,
            lower_bound: None,
            upper_bound: None,
            hint_min_ts: None,
            hint_max_ts: None,
            ts,
            desc,
            bypass_locks: Default::default(),
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
        // FIXME: Try to find out how to filter default CF SSTs by start ts
        let (hint_min_ts, hint_max_ts) = if cf == CF_WRITE {
            (self.hint_min_ts, self.hint_max_ts)
        } else {
            (None, None)
        };
        let cursor = CursorBuilder::new(&self.snapshot, cf)
            .range(lower, upper)
            .fill_cache(self.fill_cache)
            .scan_mode(self.scan_mode())
            .hint_min_ts(hint_min_ts)
            .hint_max_ts(hint_max_ts)
            .build()?;
        Ok(cursor)
    }
}

/// Reads user key's value in default CF according to the given write CF value
/// (`write`).
///
/// Internally, there will be a `near_seek` operation.
///
/// Notice that the value may be already carried in the `write` (short value). In this
/// case, you should not call this function.
///
/// # Panics
///
/// Panics if there is a short value carried in the given `write`.
///
/// Panics if key in default CF does not exist. This means there is a data corruption.
fn near_load_data_by_write<I>(
    default_cursor: &mut Cursor<I>, // TODO: make it `ForwardCursor`.
    user_key: &Key,
    write_start_ts: TimeStamp,
    statistics: &mut Statistics,
) -> Result<Value>
where
    I: Iterator,
{
    let seek_key = user_key.clone().append_ts(write_start_ts);
    default_cursor.near_seek(&seek_key, &mut statistics.data)?;
    if !default_cursor.valid()?
        || default_cursor.key(&mut statistics.data) != seek_key.as_encoded().as_slice()
    {
        return Err(default_not_found_error(
            user_key.to_raw()?,
            "near_load_data_by_write",
        ));
    }
    statistics.data.processed += 1;
    Ok(default_cursor.value(&mut statistics.data).to_vec())
}

/// Similar to `near_load_data_by_write`, but accepts a `BackwardCursor` and use
/// `near_seek_for_prev` internally.
fn near_reverse_load_data_by_write<I>(
    default_cursor: &mut Cursor<I>, // TODO: make it `BackwardCursor`.
    user_key: &Key,
    write_start_ts: TimeStamp,
    statistics: &mut Statistics,
) -> Result<Value>
where
    I: Iterator,
{
    let seek_key = user_key.clone().append_ts(write_start_ts);
    default_cursor.near_seek_for_prev(&seek_key, &mut statistics.data)?;
    if !default_cursor.valid()?
        || default_cursor.key(&mut statistics.data) != seek_key.as_encoded().as_slice()
    {
        return Err(default_not_found_error(
            user_key.to_raw()?,
            "near_reverse_load_data_by_write",
        ));
    }
    statistics.data.processed += 1;
    Ok(default_cursor.value(&mut statistics.data).to_vec())
}

pub fn has_data_in_range<S: Snapshot>(
    snapshot: S,
    cf: CfName,
    left: &Key,
    right: &Key,
    statistic: &mut CfStatistics,
) -> Result<bool> {
    let iter_opt = IterOption::new(None, None, true);
    let mut iter = snapshot.iter_cf(cf, iter_opt, ScanMode::Forward)?;
    if iter.seek(left, statistic)? {
        if iter.key(statistic) < right.as_encoded().as_slice() {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::kv::{Engine, RocksEngine, TestEngineBuilder};
    use crate::storage::mvcc::tests::*;
    use crate::storage::mvcc::{Error as MvccError, ErrorInner as MvccErrorInner};
    use crate::storage::txn::{Error as TxnError, ErrorInner as TxnErrorInner};
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
                Err(TxnError(box TxnErrorInner::Mvcc(MvccError(
                    box MvccErrorInner::KeyIsLocked(mut info),
                )))) => scan_result.push((info.take_key(), None)),
                e => panic!("got error while scanning: {:?}", e),
            }
        }

        assert_eq!(scan_result, expected);
    }

    fn test_scan_with_lock_and_write_impl(desc: bool) {
        const SCAN_TS: TimeStamp = TimeStamp::new(10);
        const PREV_TS: TimeStamp = TimeStamp::new(4);
        const POST_TS: TimeStamp = TimeStamp::new(5);

        let new_engine = || TestEngineBuilder::new().build().unwrap();
        let add_write_at_ts = |commit_ts, engine, key, value| {
            must_prewrite_put(engine, key, value, key, commit_ts);
            must_commit(engine, key, commit_ts, commit_ts);
        };

        let add_lock_at_ts = |lock_ts, engine, key| {
            must_prewrite_put(engine, key, b"lock", key, lock_ts);
            must_locked(engine, key, lock_ts);
        };

        let test_scanner_result =
            move |engine: &RocksEngine, expected_result: Vec<(Vec<u8>, Option<Vec<u8>>)>| {
                let snapshot = engine.snapshot(&Context::default()).unwrap();

                let scanner = ScannerBuilder::new(snapshot.clone(), SCAN_TS, desc)
                    .build()
                    .unwrap();
                check_scan_result(scanner, &expected_result);
            };

        let desc_map = move |result: Vec<(Vec<u8>, Option<Vec<u8>>)>| {
            if desc {
                result.into_iter().rev().collect()
            } else {
                result
            }
        };

        // Lock after write
        let engine = new_engine();

        add_write_at_ts(POST_TS, &engine, b"a", b"a_value");
        add_lock_at_ts(PREV_TS, &engine, b"b");

        let expected_result = desc_map(vec![
            (b"a".to_vec(), Some(b"a_value".to_vec())),
            (b"b".to_vec(), None),
        ]);

        test_scanner_result(&engine, expected_result);

        // Lock before write for same key
        let engine = new_engine();
        add_write_at_ts(PREV_TS, &engine, b"a", b"a_value");
        add_lock_at_ts(POST_TS, &engine, b"a");

        let expected_result = vec![(b"a".to_vec(), None)];

        test_scanner_result(&engine, expected_result);

        // Lock before write in different keys
        let engine = new_engine();
        add_lock_at_ts(POST_TS, &engine, b"a");
        add_write_at_ts(PREV_TS, &engine, b"b", b"b_value");

        let expected_result = desc_map(vec![
            (b"a".to_vec(), None),
            (b"b".to_vec(), Some(b"b_value".to_vec())),
        ]);
        test_scanner_result(&engine, expected_result);

        // Only a lock here
        let engine = new_engine();
        add_lock_at_ts(PREV_TS, &engine, b"a");

        let expected_result = desc_map(vec![(b"a".to_vec(), None)]);

        test_scanner_result(&engine, expected_result);

        // Write Only
        let engine = new_engine();
        add_write_at_ts(PREV_TS, &engine, b"a", b"a_value");

        let expected_result = desc_map(vec![(b"a".to_vec(), Some(b"a_value".to_vec()))]);
        test_scanner_result(&engine, expected_result);
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

        let scanner = ScannerBuilder::new(snapshot.clone(), 30.into(), desc)
            .build()
            .unwrap();
        check_scan_result(scanner, &expected_result);

        let scanner = ScannerBuilder::new(snapshot.clone(), 70.into(), desc)
            .build()
            .unwrap();
        check_scan_result(scanner, &expected_result);

        let scanner = ScannerBuilder::new(snapshot.clone(), 103.into(), desc)
            .build()
            .unwrap();
        check_scan_result(scanner, &expected_result);

        // The value of key 4 is locked at 105 so that it can't be read at 106
        if desc {
            expected_result[0].1 = None;
        } else {
            expected_result[4].1 = None;
        }
        let scanner = ScannerBuilder::new(snapshot, 106.into(), desc)
            .build()
            .unwrap();
        check_scan_result(scanner, &expected_result);
    }

    #[test]
    fn test_scan_with_lock_and_write() {
        test_scan_with_lock_and_write_impl(true);
        test_scan_with_lock_and_write_impl(false);
    }

    #[test]
    fn test_scan_with_lock() {
        test_scan_with_lock_impl(false);
        test_scan_with_lock_impl(true);
    }

    fn test_scan_bypass_locks_impl(desc: bool) {
        let engine = TestEngineBuilder::new().build().unwrap();

        for i in 0..5 {
            must_prewrite_put(&engine, &[i], &[b'v', i], &[i], 10);
            must_commit(&engine, &[i], 10, 20);
        }

        // Locks are: 30, 40, 50, 60, 70
        for i in 0..5 {
            must_prewrite_put(&engine, &[i], &[b'v', i], &[i], 30 + u64::from(i) * 10);
        }

        let bypass_locks = TsSet::from_u64s(vec![30, 41, 50]);

        // Scan at ts 65 will meet locks at 40 and 60.
        let mut expected_result = vec![
            (vec![0], Some(vec![b'v', 0])),
            (vec![1], None),
            (vec![2], Some(vec![b'v', 2])),
            (vec![3], None),
            (vec![4], Some(vec![b'v', 4])),
        ];

        if desc {
            expected_result = expected_result.into_iter().rev().collect();
        }

        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let scanner = ScannerBuilder::new(snapshot.clone(), 65.into(), desc)
            .bypass_locks(bypass_locks)
            .build()
            .unwrap();
        check_scan_result(scanner, &expected_result);
    }

    #[test]
    fn test_scan_bypass_locks() {
        test_scan_bypass_locks_impl(false);
        test_scan_bypass_locks_impl(true);
    }
}
