// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
mod backward;
mod forward;

use std::ops::Bound;

use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE, CfName};
use kvproto::kvrpcpb::{ExtraOp, IsolationLevel};
use txn_types::{
    Key, Lock, LockType, OldValue, TimeStamp, TsSet, Value, ValueEntry, Write, WriteRef, WriteType,
};

pub use self::forward::{DeltaScanner, EntryScanner, test_util};
use self::{
    backward::BackwardKvScanner,
    forward::{
        DeltaEntryPolicy, ForwardKvScanner, ForwardScanner, LatestEntryPolicy, LatestKvPolicy,
    },
};
use crate::storage::{
    kv::{
        CfStatistics, Cursor, CursorBuilder, Iterator, LoadDataHint, ScanMode, Snapshot, Statistics,
    },
    mvcc::{NewerTsCheckState, Result, default_not_found_error},
    need_check_locks,
    txn::{Result as TxnResult, Scanner as StoreScanner},
};

pub struct ScannerBuilder<S: Snapshot>(ScannerConfig<S>);

impl<S: Snapshot> ScannerBuilder<S> {
    /// Initialize a new `ScannerBuilder`
    pub fn new(snapshot: S, ts: TimeStamp) -> Self {
        Self(ScannerConfig::new(snapshot, ts))
    }

    /// Set whether or not read operations should fill the cache.
    ///
    /// Defaults to `true`.
    #[inline]
    #[must_use]
    pub fn fill_cache(mut self, fill_cache: bool) -> Self {
        self.0.fill_cache = fill_cache;
        self
    }

    /// Set whether values of the user key should be omitted. When `omit_value`
    /// is `true`, the length of returned value will be 0.
    ///
    /// Previously this option is called `key_only`.
    ///
    /// Defaults to `false`.
    #[inline]
    #[must_use]
    pub fn omit_value(mut self, omit_value: bool) -> Self {
        self.0.omit_value = omit_value;
        self
    }

    /// Set the isolation level.
    ///
    /// Defaults to `IsolationLevel::Si`.
    #[inline]
    #[must_use]
    pub fn isolation_level(mut self, isolation_level: IsolationLevel) -> Self {
        self.0.isolation_level = isolation_level;
        self
    }

    /// Set the desc.
    ///
    /// Default is 'false'.
    #[inline]
    #[must_use]
    pub fn desc(mut self, desc: bool) -> Self {
        self.0.desc = desc;
        self
    }

    /// Limit the range to `[lower_bound, upper_bound)` in which the
    /// `ForwardKvScanner` should scan. `None` means unbounded.
    ///
    /// Default is `(None, None)`.
    #[inline]
    #[must_use]
    pub fn range(mut self, lower_bound: Option<Key>, upper_bound: Option<Key>) -> Self {
        self.0.lower_bound = lower_bound;
        self.0.upper_bound = upper_bound;
        self
    }

    /// Set locks that the scanner can bypass. Locks with start_ts in the
    /// specified set will be ignored during scanning.
    ///
    /// Default is empty.
    #[inline]
    #[must_use]
    pub fn bypass_locks(mut self, locks: TsSet) -> Self {
        self.0.bypass_locks = locks;
        self
    }

    /// Set locks that the scanner can read through. Locks with start_ts in the
    /// specified set will be accessed during scanning.
    ///
    /// Default is empty.
    #[inline]
    #[must_use]
    pub fn access_locks(mut self, locks: TsSet) -> Self {
        self.0.access_locks = locks;
        self
    }

    /// Set the hint for the minimum commit ts we want to scan.
    ///
    /// Default is empty.
    ///
    /// NOTE: user should be careful to use it with `ExtraOp::ReadOldValue`.
    #[inline]
    #[must_use]
    pub fn hint_min_ts(mut self, min_ts: Option<TimeStamp>) -> Self {
        self.0.hint_min_ts = min_ts;
        self
    }

    /// Set the hint for the maximum commit ts we want to scan.
    ///
    /// Default is empty.
    ///
    /// NOTE: user should be careful to use it with `ExtraOp::ReadOldValue`.
    #[inline]
    #[must_use]
    pub fn hint_max_ts(mut self, max_ts: Option<TimeStamp>) -> Self {
        self.0.hint_max_ts = max_ts;
        self
    }

    /// Check whether there is data with newer ts. The result of
    /// `met_newer_ts_data` is Unknown if this option is not set.
    ///
    /// Default is false.
    #[inline]
    #[must_use]
    pub fn check_has_newer_ts_data(mut self, enabled: bool) -> Self {
        self.0.check_has_newer_ts_data = enabled;
        self
    }

    /// Set whether to load commit timestamp when scanning.
    ///
    /// Default is false.
    #[inline]
    #[must_use]
    pub fn set_load_commit_ts(mut self, enabled: bool) -> Self {
        self.0.load_commit_ts = enabled;
        self
    }

    /// Build `Scanner` from the current configuration.
    pub fn build(mut self) -> Result<Scanner<S>> {
        let lock_cursor = self.build_lock_cursor()?;
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
        let lock_cursor = self.build_lock_cursor()?;
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

    pub fn build_delta_scanner(
        mut self,
        from_ts: TimeStamp,
        extra_op: ExtraOp,
    ) -> Result<DeltaScanner<S>> {
        let lock_cursor = self.build_lock_cursor()?;
        let write_cursor = self.0.create_cf_cursor(CF_WRITE)?;
        // Note: Create a default cf cursor will take key range, so we need to
        //       ensure the default cursor is created after lock and write.
        let default_cursor = self
            .0
            .create_cf_cursor_with_scan_mode(CF_DEFAULT, ScanMode::Mixed)?;
        Ok(ForwardScanner::new(
            self.0,
            lock_cursor,
            write_cursor,
            Some(default_cursor),
            DeltaEntryPolicy::new(from_ts, extra_op),
        ))
    }

    fn build_lock_cursor(&mut self) -> Result<Option<Cursor<S::Iter>>> {
        Ok(if need_check_locks(self.0.isolation_level) {
            Some(self.0.create_cf_cursor(CF_LOCK)?)
        } else {
            None
        })
    }
}

pub enum Scanner<S: Snapshot> {
    Forward(ForwardKvScanner<S>),
    Backward(BackwardKvScanner<S>),
}

impl<S: Snapshot> StoreScanner for Scanner<S> {
    fn next_entry(&mut self) -> TxnResult<Option<(Key, ValueEntry)>> {
        fail_point!("scanner_next");

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

    /// Returns whether data with newer ts is found. The result is meaningful
    /// only when `check_has_newer_ts_data` is set to true.
    fn met_newer_ts_data(&self) -> NewerTsCheckState {
        match self {
            Scanner::Forward(scanner) => scanner.met_newer_ts_data(),
            Scanner::Backward(scanner) => scanner.met_newer_ts_data(),
        }
    }
}

pub struct ScannerConfig<S: Snapshot> {
    snapshot: S,
    fill_cache: bool,
    omit_value: bool,
    load_commit_ts: bool,
    isolation_level: IsolationLevel,

    /// `lower_bound` and `upper_bound` is used to create `default_cursor`.
    /// `upper_bound` is used in initial seek(or `lower_bound` in initial
    /// backward seek) as well. They will be consumed after `default_cursor` is
    /// being created.
    lower_bound: Option<Key>,
    upper_bound: Option<Key>,
    // hint for we will only scan data with commit ts >= hint_min_ts
    hint_min_ts: Option<TimeStamp>,
    // hint for we will only scan data with commit ts <= hint_max_ts
    hint_max_ts: Option<TimeStamp>,

    ts: TimeStamp,
    desc: bool,

    bypass_locks: TsSet,
    access_locks: TsSet,

    check_has_newer_ts_data: bool,
}

impl<S: Snapshot> ScannerConfig<S> {
    fn new(snapshot: S, ts: TimeStamp) -> Self {
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
            desc: false,
            bypass_locks: Default::default(),
            access_locks: Default::default(),
            check_has_newer_ts_data: false,
            load_commit_ts: false,
        }
    }

    #[inline]
    fn scan_mode(&self) -> ScanMode {
        if self.desc {
            ScanMode::Mixed
        } else {
            ScanMode::Forward
        }
    }

    /// Create the cursor.
    #[inline]
    fn create_cf_cursor(&mut self, cf: CfName) -> Result<Cursor<S::Iter>> {
        self.create_cf_cursor_with_scan_mode(cf, self.scan_mode())
    }

    /// Create the cursor with specified scan_mode, instead of inferring
    /// scan_mode from the config.
    #[inline]
    fn create_cf_cursor_with_scan_mode(
        &mut self,
        cf: CfName,
        scan_mode: ScanMode,
    ) -> Result<Cursor<S::Iter>> {
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
            .scan_mode(scan_mode)
            .hint_min_ts(hint_min_ts.map(Bound::Included))
            .hint_max_ts(hint_max_ts.map(Bound::Included))
            .build()?;
        Ok(cursor)
    }
}

/// Reads user key's value in default CF according to the given write CF value
/// (`write`).
///
/// Internally, there will be a `near_seek` or `seek` operation depending on
/// write CF stats.
///
/// Notice that the value may be already carried in the `write` (short value).
/// In this case, you should not call this function.
///
/// # Panics
///
/// Panics if there is a short value carried in the given `write`.
///
/// Panics if key in default CF does not exist. This means there is a data
/// corruption.
pub fn near_load_data_by_write<I>(
    default_cursor: &mut Cursor<I>, // TODO: make it `ForwardCursor`.
    user_key: &Key,
    write_start_ts: TimeStamp,
    statistics: &mut Statistics,
) -> Result<Value>
where
    I: Iterator,
{
    fail_point!("near_load_data_by_write_default_not_found", |_| Err(
        default_not_found_error(
            user_key.clone().append_ts(write_start_ts).into_encoded(),
            "near_load_data_by_write",
        )
    ));
    let seek_key = user_key.clone().append_ts(write_start_ts);
    match statistics.load_data_hint() {
        LoadDataHint::NearSeek => default_cursor.near_seek(&seek_key, &mut statistics.data)?,
        LoadDataHint::Seek => default_cursor.seek(&seek_key, &mut statistics.data)?,
    };

    if !default_cursor.valid()?
        || default_cursor.key(&mut statistics.data) != seek_key.as_encoded().as_slice()
    {
        return Err(default_not_found_error(
            user_key.clone().append_ts(write_start_ts).into_encoded(),
            "near_load_data_by_write",
        ));
    }
    statistics.data.processed_keys += 1;
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
    match statistics.load_data_hint() {
        LoadDataHint::NearSeek => {
            default_cursor.near_seek_for_prev(&seek_key, &mut statistics.data)?
        }
        LoadDataHint::Seek => default_cursor.seek_for_prev(&seek_key, &mut statistics.data)?,
    };
    if !default_cursor.valid()?
        || default_cursor.key(&mut statistics.data) != seek_key.as_encoded().as_slice()
    {
        return Err(default_not_found_error(
            user_key.clone().append_ts(write_start_ts).into_encoded(),
            "near_reverse_load_data_by_write",
        ));
    }
    statistics.data.processed_keys += 1;
    Ok(default_cursor.value(&mut statistics.data).to_vec())
}

pub fn has_data_in_range<S: Snapshot>(
    snapshot: S,
    cf: CfName,
    left: &Key,
    right: &Key,
    statistic: &mut CfStatistics,
) -> Result<bool> {
    let mut cursor = CursorBuilder::new(&snapshot, cf)
        .range(None, Some(right.clone()))
        .scan_mode(ScanMode::Forward)
        .fill_cache(true)
        .max_skippable_internal_keys(100)
        .build()?;
    match cursor.seek(left, statistic) {
        Ok(valid) => {
            if valid && cursor.key(statistic) < right.as_encoded().as_slice() {
                return Ok(true);
            }
        }
        Err(e)
            if e.to_string()
                .contains("Result incomplete: Too many internal keys skipped") =>
        {
            return Ok(true);
        }
        err @ Err(_) => {
            err?;
        }
    }
    Ok(false)
}

/// Seek for the next valid (write type == Put or Delete) write record.
/// The write cursor must indicate a data key of the user key of which ts <=
/// after_ts. Return None if cannot find any valid write record.
///
/// GC fence will be checked against the specified `gc_fence_limit`. If
/// `gc_fence_limit` is greater than the `commit_ts` of the current write record
/// pointed by the cursor, The caller must guarantee that there are no other
/// versions in range `(current_commit_ts, gc_fence_limit]`. Note that if a
/// record is determined as invalid by checking GC fence, the `write_cursor`'s
/// position will be left remain on it.
pub fn seek_for_valid_write<I>(
    write_cursor: &mut Cursor<I>,
    user_key: &Key,
    after_ts: TimeStamp,
    gc_fence_limit: TimeStamp,
    statistics: &mut Statistics,
) -> Result<Option<Write>>
where
    I: Iterator,
{
    let mut ret = None;
    while write_cursor.valid()?
        && Key::is_user_key_eq(
            write_cursor.key(&mut statistics.write),
            user_key.as_encoded(),
        )
    {
        let write_ref = WriteRef::parse(write_cursor.value(&mut statistics.write))?;
        if !write_ref.check_gc_fence_as_latest_version(gc_fence_limit) {
            break;
        }
        match write_ref.write_type {
            WriteType::Put | WriteType::Delete => {
                // After a pessimistic transaction is committed,
                // a non-pessimistic lock of the same transaction can be also prewritten
                // successfully. The lock TS of such a lock is smaller than the commit TS of the
                // latest write record.
                // See https://github.com/tikv/tikv/issues/11187
                let latest_write_commit_ts =
                    Key::decode_ts_from(write_cursor.key(&mut statistics.write))?;
                if after_ts < latest_write_commit_ts {
                    warn!("found the user key of which ts > after_ts. There may exist an unexpected stale non-pessimistic lock";
                        "user_key" => %user_key,
                        "after_ts" => after_ts,
                        "latest_write_commit_ts" => latest_write_commit_ts,
                        "latest_write_start_ts" => write_ref.start_ts,
                        "write_type" => ?write_ref.write_type,
                    );
                }
                ret = Some(write_ref.to_owned());
                break;
            }
            WriteType::Lock | WriteType::Rollback => {
                // Move to the next write record.
                write_cursor.next(&mut statistics.write);
            }
        }
    }
    Ok(ret)
}

/// Seek for the last written value.
/// The write cursor must indicate a data key of the user key of which ts <=
/// after_ts. Return None if cannot find any valid write record or found a
/// delete record.
///
/// GC fence will be checked against the specified `gc_fence_limit`. If
/// `gc_fence_limit` is greater than the `commit_ts` of the current write record
/// pointed by the cursor, The caller must guarantee that there are no other
/// versions in range `(current_commit_ts, gc_fence_limit]`. Note that if a
/// record is determined as invalid by checking GC fence, the `write_cursor`'s
/// position will be left remain on it.
///
/// `write_cursor` maybe created with an `TsFilter`, which can filter out some
/// key-value pairs with less `commit_ts` than `ts_filter`. So if the got value
/// has a less timestamp than `ts_filter`, it should be replaced by None because
/// the real wanted value can have been filtered.
pub fn seek_for_valid_value<I>(
    write_cursor: &mut Cursor<I>,
    default_cursor: &mut Cursor<I>,
    user_key: &Key,
    after_ts: TimeStamp,
    gc_fence_limit: TimeStamp,
    ts_filter: Option<TimeStamp>,
    statistics: &mut Statistics,
) -> Result<OldValue>
where
    I: Iterator,
{
    let seek_after = || {
        let seek_after = user_key.clone().append_ts(after_ts);
        OldValue::SeekWrite(seek_after)
    };

    if let Some(write) =
        seek_for_valid_write(write_cursor, user_key, after_ts, gc_fence_limit, statistics)?
    {
        if write.write_type == WriteType::Put {
            if let Some(ts_filter) = ts_filter {
                let k = write_cursor.key(&mut statistics.write);
                if Key::decode_ts_from(k).unwrap() < ts_filter {
                    return Ok(seek_after());
                }
            }
            let value = if let Some(v) = write.short_value {
                v
            } else {
                near_load_data_by_write(default_cursor, user_key, write.start_ts, statistics)?
            };
            return Ok(OldValue::Value { value });
        }
        Ok(OldValue::None)
    } else if ts_filter.is_some() {
        Ok(seek_after())
    } else {
        Ok(OldValue::None)
    }
}

pub(crate) fn load_data_by_lock<S: Snapshot, I: Iterator>(
    current_user_key: &Key,
    cfg: &ScannerConfig<S>,
    default_cursor: &mut Cursor<I>,
    lock: Lock,
    statistics: &mut Statistics,
) -> Result<Option<Value>> {
    match lock.lock_type {
        LockType::Put => {
            if cfg.omit_value {
                return Ok(Some(vec![]));
            }
            match lock.short_value {
                Some(value) => {
                    // Value is carried in `lock`.
                    Ok(Some(value.to_vec()))
                }
                None => {
                    let value = if cfg.desc {
                        near_reverse_load_data_by_write(
                            default_cursor,
                            current_user_key,
                            lock.ts,
                            statistics,
                        )
                    } else {
                        near_load_data_by_write(
                            default_cursor,
                            current_user_key,
                            lock.ts,
                            statistics,
                        )
                    }?;
                    Ok(Some(value))
                }
            }
        }
        LockType::Delete => Ok(None),
        LockType::Lock | LockType::Pessimistic | LockType::Shared => {
            // Only when fails to call `txn_types::check_ts_conflict()`, the function is
            // called, so it's unreachable here.
            unreachable!()
        }
    }
}

#[cfg(test)]
mod tests {
    use engine_rocks::ReadPerfInstant;
    use engine_traits::MiscExt;
    use txn_types::{LastChange, OldValue};

    use super::*;
    use crate::storage::{
        kv::{Engine, RocksEngine, SEEK_BOUND, TestEngineBuilder},
        mvcc::{Error as MvccError, ErrorInner as MvccErrorInner, tests::*},
        txn::{
            Error as TxnError, ErrorInner as TxnErrorInner, TxnEntry, TxnEntryScanner, tests::*,
        },
    };

    // Collect data from the scanner and assert it equals to `expected`, which is a
    // collection of (raw_key, value).
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
        let add_write_at_ts = |commit_ts, engine: &mut _, key, value| {
            must_prewrite_put(engine, key, value, key, commit_ts);
            must_commit(engine, key, commit_ts, commit_ts);
        };

        let add_lock_at_ts = |lock_ts, engine: &mut _, key| {
            must_prewrite_put(engine, key, b"lock", key, lock_ts);
            must_locked(engine, key, lock_ts);
        };

        let test_scanner_result =
            move |engine: &mut RocksEngine, expected_result: Vec<(Vec<u8>, Option<Vec<u8>>)>| {
                let snapshot = engine.snapshot(Default::default()).unwrap();

                let scanner = ScannerBuilder::new(snapshot, SCAN_TS)
                    .desc(desc)
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
        let mut engine = new_engine();

        add_write_at_ts(POST_TS, &mut engine, b"a", b"a_value");
        add_lock_at_ts(PREV_TS, &mut engine, b"b");

        let expected_result = desc_map(vec![
            (b"a".to_vec(), Some(b"a_value".to_vec())),
            (b"b".to_vec(), None),
        ]);

        test_scanner_result(&mut engine, expected_result);

        // Lock before write for same key
        let mut engine = new_engine();
        add_write_at_ts(PREV_TS, &mut engine, b"a", b"a_value");
        add_lock_at_ts(POST_TS, &mut engine, b"a");

        let expected_result = vec![(b"a".to_vec(), None)];

        test_scanner_result(&mut engine, expected_result);

        // Lock before write in different keys
        let mut engine = new_engine();
        add_lock_at_ts(POST_TS, &mut engine, b"a");
        add_write_at_ts(PREV_TS, &mut engine, b"b", b"b_value");

        let expected_result = desc_map(vec![
            (b"a".to_vec(), None),
            (b"b".to_vec(), Some(b"b_value".to_vec())),
        ]);
        test_scanner_result(&mut engine, expected_result);

        // Only a lock here
        let mut engine = new_engine();
        add_lock_at_ts(PREV_TS, &mut engine, b"a");

        let expected_result = desc_map(vec![(b"a".to_vec(), None)]);

        test_scanner_result(&mut engine, expected_result);

        // Write Only
        let mut engine = new_engine();
        add_write_at_ts(PREV_TS, &mut engine, b"a", b"a_value");

        let expected_result = desc_map(vec![(b"a".to_vec(), Some(b"a_value".to_vec()))]);
        test_scanner_result(&mut engine, expected_result);
    }

    fn test_scan_with_lock_impl(desc: bool) {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        for i in 0..5 {
            must_prewrite_put(&mut engine, &[i], &[b'v', i], &[i], 1);
            must_commit(&mut engine, &[i], 1, 2);
            must_prewrite_put(&mut engine, &[i], &[b'v', i], &[i], 10);
            must_commit(&mut engine, &[i], 10, 100);
        }

        must_acquire_pessimistic_lock(&mut engine, &[1], &[1], 20, 110);
        must_acquire_pessimistic_lock(&mut engine, &[2], &[2], 50, 110);
        must_acquire_pessimistic_lock(&mut engine, &[3], &[3], 105, 110);
        must_prewrite_put(&mut engine, &[4], b"a", &[4], 105);

        let snapshot = engine.snapshot(Default::default()).unwrap();

        let mut expected_result = vec![
            (vec![0], Some(vec![b'v', 0])),
            (vec![1], Some(vec![b'v', 1])),
            (vec![2], Some(vec![b'v', 2])),
            (vec![3], Some(vec![b'v', 3])),
            (vec![4], Some(vec![b'v', 4])),
        ];

        if desc {
            expected_result.reverse();
        }

        let scanner = ScannerBuilder::new(snapshot.clone(), 30.into())
            .desc(desc)
            .build()
            .unwrap();
        check_scan_result(scanner, &expected_result);

        let scanner = ScannerBuilder::new(snapshot.clone(), 70.into())
            .desc(desc)
            .build()
            .unwrap();
        check_scan_result(scanner, &expected_result);

        let scanner = ScannerBuilder::new(snapshot.clone(), 103.into())
            .desc(desc)
            .build()
            .unwrap();
        check_scan_result(scanner, &expected_result);

        // The value of key 4 is locked at 105 so that it can't be read at 106
        if desc {
            expected_result[0].1 = None;
        } else {
            expected_result[4].1 = None;
        }
        let scanner = ScannerBuilder::new(snapshot, 106.into())
            .desc(desc)
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
        let mut engine = TestEngineBuilder::new().build().unwrap();

        for i in 0..5 {
            must_prewrite_put(&mut engine, &[i], &[b'v', i], &[i], 10);
            must_commit(&mut engine, &[i], 10, 20);
        }

        // Locks are: 30, 40, 50, 60, 70
        for i in 0..5 {
            must_prewrite_put(&mut engine, &[i], &[b'v', i], &[i], 30 + u64::from(i) * 10);
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

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let scanner = ScannerBuilder::new(snapshot, 65.into())
            .desc(desc)
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

    fn test_scan_access_locks_impl(desc: bool, delete_bound: bool) {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        for i in 0..=8 {
            must_prewrite_put(&mut engine, &[i], &[b'v', i], &[i], 10);
            must_commit(&mut engine, &[i], 10, 20);
        }

        if delete_bound {
            must_prewrite_delete(&mut engine, &[0], &[0], 30); // access delete
        } else {
            must_prewrite_put(&mut engine, &[0], &[b'v', 0, 0], &[0], 30); // access put
        }
        must_prewrite_put(&mut engine, &[1], &[b'v', 1, 1], &[1], 40); // access put
        must_prewrite_delete(&mut engine, &[2], &[2], 50); // access delete
        must_prewrite_lock(&mut engine, &[3], &[3], 60); // access lock(actually ignored)
        must_prewrite_put(&mut engine, &[4], &[b'v', 4, 4], &[4], 70); // locked
        must_prewrite_put(&mut engine, &[5], &[b'v', 5, 5], &[5], 80); // bypass
        must_prewrite_put(&mut engine, &[6], &[b'v', 6, 6], &[6], 100); // locked with larger ts
        if delete_bound {
            must_prewrite_delete(&mut engine, &[8], &[8], 90); // access delete
        } else {
            must_prewrite_put(&mut engine, &[8], &[b'v', 8, 8], &[8], 90); // access put
        }

        let bypass_locks = TsSet::from_u64s(vec![80]);
        let access_locks = TsSet::from_u64s(vec![30, 40, 50, 60, 90]);

        let mut expected_result = vec![
            (vec![0], Some(vec![b'v', 0, 0])), // access put if not delete_bound
            (vec![1], Some(vec![b'v', 1, 1])), // access put
            // vec![2] access delete
            (vec![3], Some(vec![b'v', 3])),    // ignore LockType::Lock
            (vec![4], None),                   // locked
            (vec![5], Some(vec![b'v', 5])),    // bypass
            (vec![6], Some(vec![b'v', 6])),    // ignore lock with larger ts
            (vec![7], Some(vec![b'v', 7])),    // no lock
            (vec![8], Some(vec![b'v', 8, 8])), // access put if not delete_bound
        ];
        if desc {
            expected_result.reverse();
        }
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let scanner = ScannerBuilder::new(snapshot, 95.into())
            .desc(desc)
            .bypass_locks(bypass_locks)
            .access_locks(access_locks)
            .build()
            .unwrap();
        check_scan_result(
            scanner,
            if delete_bound {
                &expected_result[1..expected_result.len() - 1]
            } else {
                &expected_result
            },
        );
    }

    #[test]
    fn test_scan_access_locks() {
        for (desc, delete_bound) in [(false, false), (false, true), (true, false), (true, true)] {
            test_scan_access_locks_impl(desc, delete_bound);
        }
    }

    fn must_met_newer_ts_data<E: Engine>(
        engine: &mut E,
        scanner_ts: impl Into<TimeStamp>,
        key: &[u8],
        value: Option<&[u8]>,
        desc: bool,
        expected_met_newer_ts_data: bool,
    ) {
        let mut scanner = ScannerBuilder::new(
            engine.snapshot(Default::default()).unwrap(),
            scanner_ts.into(),
        )
        .desc(desc)
        .range(Some(Key::from_raw(key)), None)
        .check_has_newer_ts_data(true)
        .build()
        .unwrap();

        let result = scanner.next().unwrap();
        if let Some(value) = value {
            let (k, v) = result.unwrap();
            assert_eq!(k, Key::from_raw(key));
            assert_eq!(v, value);
        } else {
            assert!(result.is_none());
        }

        let expected = if expected_met_newer_ts_data {
            NewerTsCheckState::Met
        } else {
            NewerTsCheckState::NotMetYet
        };
        assert_eq!(expected, scanner.met_newer_ts_data());
    }

    fn test_met_newer_ts_data_impl(deep_write_seek: bool, desc: bool) {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let (key, val1) = (b"foo", b"bar1");

        if deep_write_seek {
            for i in 1..SEEK_BOUND {
                must_prewrite_put(&mut engine, key, val1, key, i);
                must_commit(&mut engine, key, i, i);
            }
        }

        must_prewrite_put(&mut engine, key, val1, key, 100);
        must_commit(&mut engine, key, 100, 200);
        let (key, val2) = (b"foo", b"bar2");
        must_prewrite_put(&mut engine, key, val2, key, 300);
        must_commit(&mut engine, key, 300, 400);

        must_met_newer_ts_data(
            &mut engine,
            100,
            key,
            if deep_write_seek { Some(val1) } else { None },
            desc,
            true,
        );
        must_met_newer_ts_data(&mut engine, 200, key, Some(val1), desc, true);
        must_met_newer_ts_data(&mut engine, 300, key, Some(val1), desc, true);
        must_met_newer_ts_data(&mut engine, 400, key, Some(val2), desc, false);
        must_met_newer_ts_data(&mut engine, 500, key, Some(val2), desc, false);

        must_prewrite_lock(&mut engine, key, key, 600);

        must_met_newer_ts_data(&mut engine, 500, key, Some(val2), desc, true);
        must_met_newer_ts_data(&mut engine, 600, key, Some(val2), desc, true);
    }

    #[test]
    fn test_met_newer_ts_data() {
        test_met_newer_ts_data_impl(false, false);
        test_met_newer_ts_data_impl(false, true);
        test_met_newer_ts_data_impl(true, false);
        test_met_newer_ts_data_impl(true, true);
    }

    #[test]
    fn test_old_value_with_hint_min_ts() {
        let mut engine = TestEngineBuilder::new().build_without_cache().unwrap();
        let mut engine_clone = engine.clone();
        let mut create_scanner = |from_ts: u64| {
            let snap = engine_clone.snapshot(Default::default()).unwrap();
            ScannerBuilder::new(snap, TimeStamp::max())
                .fill_cache(false)
                .hint_min_ts(Some(from_ts.into()))
                .build_delta_scanner(from_ts.into(), ExtraOp::ReadOldValue)
                .unwrap()
        };

        let mut value = Vec::with_capacity(1024);
        (0..128).for_each(|_| value.extend_from_slice(b"long-val"));

        // Create the initial data with CF_WRITE L0: |zkey_110, zkey1_160|
        must_prewrite_put(&mut engine, b"zkey", &value, b"zkey", 100);
        must_commit(&mut engine, b"zkey", 100, 110);
        must_prewrite_put(&mut engine, b"zkey1", &value, b"zkey1", 150);
        must_commit(&mut engine, b"zkey1", 150, 160);
        engine
            .kv_engine()
            .unwrap()
            .flush_cf(CF_WRITE, true)
            .unwrap();
        engine
            .kv_engine()
            .unwrap()
            .flush_cf(CF_DEFAULT, true)
            .unwrap();
        must_prewrite_delete(&mut engine, b"zkey", b"zkey", 200);

        let tests = vec![
            // `zkey_110` is filtered, so no old value and block reads is 0.
            (200, OldValue::seek_write(b"zkey", 200), 0),
            // Old value can be found as expected, read 2 blocks from CF_WRITE and CF_DEFAULT.
            (100, OldValue::value(value.clone()), 2),
            // `zkey_110` isn't filtered, so needs to read 1 block from CF_WRITE.
            // But we can't ensure whether it's the old value or not.
            (150, OldValue::seek_write(b"zkey", 200), 1),
        ];
        for (from_ts, expected_old_value, block_reads) in tests {
            let mut scanner = create_scanner(from_ts);
            let perf_instant = ReadPerfInstant::new();
            match scanner.next_entry().unwrap().unwrap() {
                TxnEntry::Prewrite { old_value, .. } => assert_eq!(old_value, expected_old_value),
                TxnEntry::Commit { .. } => unreachable!(),
            }
            let delta = perf_instant.delta();
            assert_eq!(delta.block_read_count, block_reads);
        }

        // CF_WRITE L0: |zkey_110, zkey1_160|, |zkey_210|
        must_commit(&mut engine, b"zkey", 200, 210);
        engine
            .kv_engine()
            .unwrap()
            .flush_cf(CF_WRITE, false)
            .unwrap();
        engine
            .kv_engine()
            .unwrap()
            .flush_cf(CF_DEFAULT, false)
            .unwrap();

        let tests = vec![
            // `zkey_110` is filtered, so no old value and block reads is 0.
            (200, OldValue::seek_write(b"zkey", 209), 0),
            // Old value can be found as expected, read 2 blocks from CF_WRITE and CF_DEFAULT.
            (100, OldValue::value(value), 2),
            // `zkey_110` isn't filtered, so needs to read 1 block from CF_WRITE.
            // But we can't ensure whether it's the old value or not.
            (150, OldValue::seek_write(b"zkey", 209), 1),
        ];
        for (from_ts, expected_old_value, block_reads) in tests {
            let mut scanner = create_scanner(from_ts);
            let perf_instant = ReadPerfInstant::new();
            match scanner.next_entry().unwrap().unwrap() {
                TxnEntry::Prewrite { .. } => unreachable!(),
                TxnEntry::Commit { old_value, .. } => assert_eq!(old_value, expected_old_value),
            }
            let delta = perf_instant.delta();
            assert_eq!(delta.block_read_count, block_reads);
        }
    }

    #[test]
    fn test_rc_scan_skip_lock() {
        test_rc_scan_skip_lock_impl(false);
        test_rc_scan_skip_lock_impl(true);
    }

    fn test_rc_scan_skip_lock_impl(desc: bool) {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let (key1, val1, val12) = (b"foo1", b"bar1", b"bar12");
        let (key2, val2) = (b"foo2", b"bar2");
        let mut expected = vec![(key1, val1), (key2, val2)];
        if desc {
            expected.reverse();
        }

        must_prewrite_put(&mut engine, key1, val1, key1, 10);
        must_commit(&mut engine, key1, 10, 20);

        must_prewrite_put(&mut engine, key2, val2, key2, 30);
        must_commit(&mut engine, key2, 30, 40);

        must_prewrite_put(&mut engine, key1, val12, key1, 50);

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, 60.into())
            .fill_cache(false)
            .range(Some(Key::from_raw(key1)), None)
            .desc(desc)
            .isolation_level(IsolationLevel::Rc)
            .build()
            .unwrap();

        for e in expected {
            let (k, v) = scanner.next().unwrap().unwrap();
            assert_eq!(k, Key::from_raw(e.0));
            assert_eq!(v, e.1);
        }

        assert!(scanner.next().unwrap().is_none());
        assert_eq!(scanner.take_statistics().lock.total_op_count(), 0);
    }

    #[test]
    fn test_scan_with_load_commit_ts() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let (key1, val1, val12, val13) = (b"foo1", b"bar1", b"bar4", b"bar5");
        let (key2, val2, val22) = (b"foo2", b"bar2", b"bar6");
        let (key3, val3) = (b"foo3", b"bar3");

        must_prewrite_put(&mut engine, key1, val1, key1, 10);
        must_commit(&mut engine, key1, 10, 20);

        must_prewrite_put(&mut engine, key2, val2, key2, 30);
        must_commit(&mut engine, key2, 30, 40);

        must_prewrite_put(&mut engine, key3, val3, key3, 50);
        must_commit(&mut engine, key3, 50, 60);

        must_prewrite_put(&mut engine, key1, val12, key1, 60);
        must_commit(&mut engine, key1, 60, 70);

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, 80.into())
            .fill_cache(false)
            .range(Some(Key::from_raw(key1)), None)
            .desc(false)
            .set_load_commit_ts(true)
            .build()
            .unwrap();

        let expected = vec![(key1, val12, 70), (key2, val2, 40), (key3, val3, 60)];
        for e in expected {
            let (k, v_ts) = scanner.next_entry().unwrap().unwrap();
            assert_eq!(k, Key::from_raw(e.0));
            assert_eq!(v_ts.value, e.1);
            assert_eq!(v_ts.commit_ts.unwrap().into_inner(), e.2);
        }
        assert!(scanner.next().unwrap().is_none());

        // test access_locks should be ignored
        must_prewrite_put(&mut engine, key1, val13, key1, 80);
        must_prewrite_put(&mut engine, key2, val22, key1, 80);
        must_commit(&mut engine, key1, 80, 90);
        let locks = TsSet::new(vec![TimeStamp::new(80)]);
        let snapshot = engine.snapshot(Default::default()).unwrap();
        scanner = ScannerBuilder::new(snapshot, 100.into())
            .fill_cache(false)
            .range(Some(Key::from_raw(key1)), None)
            .desc(false)
            .set_load_commit_ts(true)
            .access_locks(locks)
            .build()
            .unwrap();

        let (k, v_ts) = scanner.next_entry().unwrap().unwrap();
        assert_eq!(k, Key::from_raw(key1));
        assert_eq!(v_ts.value, val13);
        assert_eq!(v_ts.commit_ts.unwrap().into_inner(), 90);

        // meet lock, load_commit_ts is set, so even access_locks is set, it should be
        // ignored
        scanner.next_entry().unwrap_err();
    }

    #[test]
    fn test_scan_with_load_commit_ts_top_lock_versions_across_seek_bound_and_delete() {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        let lock_count_lt = SEEK_BOUND - 1;
        let lock_count_ge = SEEK_BOUND + 1;

        let keys_put = [b"k1".as_slice(), b"k2".as_slice()];
        let lock_counts_put = [lock_count_lt, lock_count_ge];
        for (&key, &lock_count) in keys_put.iter().zip(lock_counts_put.iter()) {
            must_prewrite_put(&mut engine, key, key, key, 1);
            must_commit(&mut engine, key, 1, 1);
            for ts in 2..=lock_count + 1 {
                must_prewrite_lock(&mut engine, key, key, ts);
                must_commit(&mut engine, key, ts, ts);
            }
        }

        let keys_delete = [b"k3".as_slice(), b"k4".as_slice()];
        let lock_counts_delete = [lock_count_lt, lock_count_ge];
        for (&key, &lock_count) in keys_delete.iter().zip(lock_counts_delete.iter()) {
            must_prewrite_put(&mut engine, key, key, key, 1);
            must_commit(&mut engine, key, 1, 1);
            must_prewrite_delete(&mut engine, key, key, 2);
            must_commit(&mut engine, key, 2, 2);
            for ts in 3..=lock_count + 2 {
                must_prewrite_lock(&mut engine, key, key, ts);
                must_commit(&mut engine, key, ts, ts);
            }
        }

        // Sanity check `last_change` and the seek-bound threshold for the top LOCK
        // records.
        let check_snapshot = engine.snapshot(Default::default()).unwrap();

        let check_top_lock = |key: &[u8], top_ts: u64, last_change_ts: u64, expect_ge: bool| {
            let write_value = check_snapshot
                .get_cf(
                    engine_traits::CF_WRITE,
                    &Key::from_raw(key).append_ts(top_ts.into()),
                )
                .unwrap()
                .unwrap();
            let write = WriteRef::parse(&write_value).unwrap();
            assert_eq!(write.write_type, WriteType::Lock);
            match write.last_change {
                LastChange::Exist {
                    last_change_ts: ts,
                    estimated_versions_to_last_change,
                } => {
                    assert_eq!(ts, last_change_ts.into());
                    if expect_ge {
                        assert!(estimated_versions_to_last_change >= SEEK_BOUND);
                    } else {
                        assert!(estimated_versions_to_last_change < SEEK_BOUND);
                    }
                }
                other => panic!("unexpected last_change: {:?}", other),
            }
        };

        check_top_lock(b"k1", lock_count_lt + 1, 1, false);
        check_top_lock(b"k2", lock_count_ge + 1, 1, true);
        check_top_lock(b"k3", lock_count_lt + 2, 2, false);
        check_top_lock(b"k4", lock_count_ge + 2, 2, true);

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, 100.into())
            .fill_cache(false)
            .range(None, None)
            .desc(false)
            .set_load_commit_ts(true)
            .build()
            .unwrap();

        // `k3` and `k4` are deleted, so they must be skipped.
        let (key, entry) = scanner.next_entry().unwrap().unwrap();
        assert_eq!(key, Key::from_raw(b"k1"));
        assert_eq!(entry.value, b"k1".to_vec());
        assert_eq!(entry.commit_ts.unwrap().into_inner(), 1);

        let (key, entry) = scanner.next_entry().unwrap().unwrap();
        assert_eq!(key, Key::from_raw(b"k2"));
        assert_eq!(entry.value, b"k2".to_vec());
        assert_eq!(entry.commit_ts.unwrap().into_inner(), 1);

        assert!(scanner.next_entry().unwrap().is_none());
    }
}
