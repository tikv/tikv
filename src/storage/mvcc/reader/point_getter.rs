// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath
use std::borrow::Cow;

use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE};
use kvproto::kvrpcpb::{IsolationLevel, WriteConflictReason};
use tikv_kv::SEEK_BOUND;
use txn_types::{
    Key, LastChange, Lock, LockType, TimeStamp, TsSet, Value, ValueEntry, WriteRef, WriteType,
};

use crate::storage::{
    kv::{Cursor, CursorBuilder, ScanMode, Snapshot, Statistics},
    mvcc::{ErrorInner::WriteConflict, NewerTsCheckState, Result, default_not_found_error},
    need_check_locks,
};

/// `PointGetter` factory.
pub struct PointGetterBuilder<S: Snapshot> {
    snapshot: S,
    fill_cache: bool,
    omit_value: bool,
    isolation_level: IsolationLevel,
    ts: TimeStamp,
    bypass_locks: TsSet,
    access_locks: TsSet,
    check_has_newer_ts_data: bool,
}

impl<S: Snapshot> PointGetterBuilder<S> {
    /// Initialize a new `PointGetterBuilder`.
    pub fn new(snapshot: S, ts: TimeStamp) -> Self {
        Self {
            snapshot,
            fill_cache: true,
            omit_value: false,
            isolation_level: IsolationLevel::Si,
            ts,
            bypass_locks: Default::default(),
            access_locks: Default::default(),
            check_has_newer_ts_data: false,
        }
    }

    /// Set whether or not read operations should fill the cache.
    ///
    /// Defaults to `true`.
    #[inline]
    #[must_use]
    pub fn fill_cache(mut self, fill_cache: bool) -> Self {
        self.fill_cache = fill_cache;
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
        self.omit_value = omit_value;
        self
    }

    /// Set the isolation level.
    ///
    /// Defaults to `IsolationLevel::Si`.
    #[inline]
    #[must_use]
    pub fn isolation_level(mut self, isolation_level: IsolationLevel) -> Self {
        self.isolation_level = isolation_level;
        self
    }

    /// Set a set to locks that the reading process can bypass.
    ///
    /// Defaults to none.
    #[inline]
    #[must_use]
    pub fn bypass_locks(mut self, locks: TsSet) -> Self {
        self.bypass_locks = locks;
        self
    }

    /// Set a set to locks that the reading process can access their values.
    ///
    /// Defaults to none.
    #[inline]
    #[must_use]
    pub fn access_locks(mut self, locks: TsSet) -> Self {
        self.access_locks = locks;
        self
    }

    /// Check whether there is data with newer ts. The result of
    /// `met_newer_ts_data` is Unknown if this option is not set.
    ///
    /// Default is false.
    #[inline]
    #[must_use]
    pub fn check_has_newer_ts_data(mut self, enabled: bool) -> Self {
        self.check_has_newer_ts_data = enabled;
        self
    }

    /// Build `PointGetter` from the current configuration.
    pub fn build(self) -> Result<PointGetter<S>> {
        let write_cursor = CursorBuilder::new(&self.snapshot, CF_WRITE)
            .fill_cache(self.fill_cache)
            .prefix_seek(true)
            .scan_mode(ScanMode::Mixed)
            .build()?;

        Ok(PointGetter {
            snapshot: self.snapshot,
            omit_value: self.omit_value,
            isolation_level: self.isolation_level,
            ts: self.ts,
            bypass_locks: self.bypass_locks,
            access_locks: self.access_locks,
            met_newer_ts_data: if self.check_has_newer_ts_data {
                NewerTsCheckState::NotMetYet
            } else {
                NewerTsCheckState::Unknown
            },

            statistics: Statistics::default(),

            write_cursor,
        })
    }
}

/// This struct can be used to get the value of user keys. Internally, rollbacks
/// are ignored and smaller version will be tried. If the isolation level is Si,
/// locks will be checked first.
///
/// Use `PointGetterBuilder` to build `PointGetter`.
pub struct PointGetter<S: Snapshot> {
    snapshot: S,
    omit_value: bool,
    isolation_level: IsolationLevel,
    ts: TimeStamp,
    bypass_locks: TsSet,
    access_locks: TsSet,
    met_newer_ts_data: NewerTsCheckState,

    statistics: Statistics,

    write_cursor: Cursor<S::Iter>,
}

impl<S: Snapshot> PointGetter<S> {
    /// Take out and reset the statistics collected so far.
    #[inline]
    pub fn take_statistics(&mut self) -> Statistics {
        std::mem::take(&mut self.statistics)
    }

    /// Whether we met newer ts data.
    /// The result is always `Unknown` if `check_has_newer_ts_data` is not set.
    #[inline]
    pub fn met_newer_ts_data(&self) -> NewerTsCheckState {
        self.met_newer_ts_data
    }

    /// Get the value of a user key.
    #[inline]
    pub fn get(&mut self, user_key: &Key) -> Result<Option<Value>> {
        match self.get_entry(user_key, false)? {
            Some(entry) => Ok(Some(entry.value)),
            None => Ok(None),
        }
    }

    /// Get the value entry of a user key.
    ///
    /// If `load_commit_ts` is true, the commit timestamp will be present in
    /// the return `ValueEntry`, otherwise `ValueEntry.CommitTS` will be `None`.
    /// The access_locks will be skipped if `load_commit_ts` is true to ensure a
    /// valid commit timestamp can be fetched, so, set it to false if you
    /// don't need commit_ts to reduce unnecessary performance overhead.
    #[inline]
    pub fn get_entry(
        &mut self,
        user_key: &Key,
        load_commit_ts: bool,
    ) -> Result<Option<ValueEntry>> {
        fail_point!("point_getter_get");
        if need_check_locks(self.isolation_level) {
            // Check locks that signal concurrent writes for `Si` or more recent writes for
            // `RcCheckTs`.
            if let Some(lock) = self.load_and_check_lock(user_key, !load_commit_ts)? {
                // When commit timestamp is required, we should not load data from lock.
                debug_assert!(!load_commit_ts);
                return self
                    .load_data_from_lock(user_key, lock)
                    .map(|o| o.map(ValueEntry::from_value));
            }
        }

        self.load_data(user_key, load_commit_ts)
    }

    /// Get a lock of a user key in the lock CF. If lock exists, it will be
    /// checked to see whether it conflicts with the given `ts` and return
    /// an error if so. If the lock is in access_locks, it will be returned
    /// and caller can read through it.
    ///
    /// In common cases we expect to get nothing in lock cf. Using a `get_cf`
    /// instead of `seek` is fast in such cases due to no need for RocksDB
    /// to continue move and skip deleted entries until find a user key.
    ///
    /// If `extract_access_lock` is true, this method will return the `Lock` if
    /// its start_ts is in the access_locks; otherwise, all locks should be
    /// regarded as a conflict ignoring the access_locks setting.
    /// Sometimes we need to set `extract_access_lock` to false because we need
    /// the commit timestamp to construct the `ValueEntry`, and the commit
    /// timestamp is not stored in the lock.
    /// For other cases, we can set `extract_access_lock` to true to avoid
    /// unnecessary performance overhead.
    fn load_and_check_lock(
        &mut self,
        user_key: &Key,
        extract_access_lock: bool,
    ) -> Result<Option<Lock>> {
        self.statistics.lock.get += 1;
        let lock_value = self.snapshot.get_cf(CF_LOCK, user_key)?;

        if let Some(ref lock_value) = lock_value {
            let lock_or_shared_locks = txn_types::parse_lock(lock_value)?;

            if self.met_newer_ts_data == NewerTsCheckState::NotMetYet {
                self.met_newer_ts_data = NewerTsCheckState::Met;
            }
            if let Err(e) = txn_types::check_ts_conflict(
                Cow::Borrowed(&lock_or_shared_locks),
                user_key,
                self.ts,
                &self.bypass_locks,
                self.isolation_level,
            ) {
                let lock = lock_or_shared_locks
                    .left()
                    .expect("Err result only for single lock");
                self.statistics.lock.processed_keys += 1;
                if extract_access_lock && self.access_locks.contains(lock.ts) {
                    return Ok(Some(lock));
                }
                Err(e.into())
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// Load the value.
    ///
    /// First, a correct version info in the Write CF will be sought. Then,
    /// value will be loaded from Default CF if necessary.
    ///
    /// If `load_commit_ts` is true, the commit timestamp will be present in
    /// the return `ValueEntry`, otherwise it will be `None`.
    /// The access_locks will be skipped if `load_commit_ts` is true to ensure a
    /// valid commit timestamp can be fetched, so, set it to false if you
    /// don't need commit_ts to reduce unnecessary performance overhead.
    fn load_data(&mut self, user_key: &Key, load_commit_ts: bool) -> Result<Option<ValueEntry>> {
        let mut use_near_seek = false;
        let mut seek_key = user_key.clone();

        if self.met_newer_ts_data == NewerTsCheckState::NotMetYet
            || self.isolation_level == IsolationLevel::RcCheckTs
        {
            seek_key = seek_key.append_ts(TimeStamp::max());
            if !self
                .write_cursor
                .seek(&seek_key, &mut self.statistics.write)?
            {
                return Ok(None);
            }
            seek_key = seek_key.truncate_ts()?;
            use_near_seek = true;

            let cursor_key = self.write_cursor.key(&mut self.statistics.write);
            // No need to compare user key because it uses prefix seek.
            let key_commit_ts = Key::decode_ts_from(cursor_key)?;
            if key_commit_ts > self.ts {
                if self.met_newer_ts_data == NewerTsCheckState::NotMetYet {
                    self.met_newer_ts_data = NewerTsCheckState::Met;
                }
                if self.isolation_level == IsolationLevel::RcCheckTs {
                    // TODO: the more write recent version with `LOCK` or `ROLLBACK` write type
                    //       could be skipped.
                    return Err(WriteConflict {
                        start_ts: self.ts,
                        conflict_start_ts: Default::default(),
                        conflict_commit_ts: key_commit_ts,
                        key: cursor_key.into(),
                        primary: vec![],
                        reason: WriteConflictReason::RcCheckTs,
                    }
                    .into());
                }
            }
        }

        seek_key = seek_key.append_ts(self.ts);
        let data_found = if use_near_seek {
            if self.write_cursor.key(&mut self.statistics.write) >= seek_key.as_encoded().as_slice()
            {
                // we call near_seek with ScanMode::Mixed set, if the key() > seek_key,
                // it will call prev() several times, whereas we just want to seek forward here
                // so cmp them in advance
                true
            } else {
                self.write_cursor
                    .near_seek(&seek_key, &mut self.statistics.write)?
            }
        } else {
            self.write_cursor
                .seek(&seek_key, &mut self.statistics.write)?
        };
        if !data_found {
            return Ok(None);
        }

        let mut write = WriteRef::parse(self.write_cursor.value(&mut self.statistics.write))?;
        // Commit ts of `write` when it is loaded by `last_change` shortcut via
        // `get_cf`. In that case write cursor still points to a newer version
        // and its ts must not be used.
        let mut loaded_write_commit_ts = None;
        let mut owned_value: Vec<u8>; // To work around lifetime problem
        loop {
            if !write.check_gc_fence_as_latest_version(self.ts) {
                return Ok(None);
            }

            match write.write_type {
                WriteType::Put => {
                    let key_commit_ts = if load_commit_ts {
                        if let Some(ts) = loaded_write_commit_ts {
                            Some(ts)
                        } else {
                            let cursor_key = self.write_cursor.key(&mut self.statistics.write);
                            Some(Key::decode_ts_from(cursor_key)?)
                        }
                    } else {
                        None
                    };
                    self.statistics.write.processed_keys += 1;
                    resource_metering::record_read_keys(1);

                    if self.omit_value {
                        return Ok(Some(ValueEntry::new(vec![], key_commit_ts)));
                    }
                    match write.short_value {
                        Some(value) => {
                            // Value is carried in `write`.
                            self.statistics.processed_size += user_key.len() + value.len();
                            return Ok(Some(ValueEntry::new(value.to_vec(), key_commit_ts)));
                        }
                        None => {
                            let start_ts = write.start_ts;
                            let value = self.load_data_from_default_cf(start_ts, user_key)?;
                            self.statistics.processed_size += user_key.len() + value.len();
                            return Ok(Some(ValueEntry::new(value, key_commit_ts)));
                        }
                    }
                }
                WriteType::Delete => {
                    return Ok(None);
                }
                WriteType::Lock | WriteType::Rollback => {
                    match write.last_change {
                        LastChange::NotExist => {
                            return Ok(None);
                        }
                        LastChange::Exist {
                            last_change_ts: commit_ts,
                            estimated_versions_to_last_change,
                        } if estimated_versions_to_last_change >= SEEK_BOUND => {
                            let key_with_ts = user_key.clone().append_ts(commit_ts);
                            match self.snapshot.get_cf(CF_WRITE, &key_with_ts)? {
                                Some(v) => owned_value = v,
                                None => return Ok(None),
                            }
                            loaded_write_commit_ts = Some(commit_ts);
                            self.statistics.write.get += 1;
                            write = WriteRef::parse(&owned_value)?;
                            assert!(
                                write.write_type == WriteType::Put
                                    || write.write_type == WriteType::Delete,
                                "Write record pointed by last_change_ts {} should be Put or Delete, but got {:?}",
                                commit_ts,
                                write.write_type,
                            );
                            continue;
                        }
                        _ => {
                            // Continue iterate next `write`.
                        }
                    }
                }
            }

            if !self.write_cursor.next(&mut self.statistics.write) {
                return Ok(None);
            }
            loaded_write_commit_ts = None;
            // No need to compare user key because it uses prefix seek.
            write = WriteRef::parse(self.write_cursor.value(&mut self.statistics.write))?;
        }
    }

    /// Load the value from default CF.
    ///
    /// We assume that mostly the keys given to batch get keys are not very
    /// close to each other. `near_seek` will likely fall back to `seek` in
    /// such scenario, which takes 2x time compared to `get_cf`. Thus we use
    /// `get_cf` directly here.
    fn load_data_from_default_cf(
        &mut self,
        write_start_ts: TimeStamp,
        user_key: &Key,
    ) -> Result<Value> {
        fail_point!("load_data_from_default_cf_default_not_found", |_| Err(
            default_not_found_error(
                user_key.clone().append_ts(write_start_ts).into_encoded(),
                "load_data_from_default_cf",
            )
        ));
        self.statistics.data.get += 1;
        // TODO: We can avoid this clone.
        let value = self
            .snapshot
            .get_cf(CF_DEFAULT, &user_key.clone().append_ts(write_start_ts))?;

        if let Some(value) = value {
            self.statistics.data.processed_keys += 1;
            Ok(value)
        } else {
            Err(default_not_found_error(
                user_key.clone().append_ts(write_start_ts).into_encoded(),
                "load_data_from_default_cf",
            ))
        }
    }

    /// Load the value from the lock.
    ///
    /// The lock belongs to a committed transaction and its commit_ts <= read's
    /// start_ts.
    fn load_data_from_lock(&mut self, user_key: &Key, lock: Lock) -> Result<Option<Value>> {
        debug_assert!(lock.ts < self.ts && lock.min_commit_ts <= self.ts);
        match lock.lock_type {
            LockType::Put => {
                if self.omit_value {
                    return Ok(Some(vec![]));
                }
                match lock.short_value {
                    Some(value) => {
                        // Value is carried in `lock`.
                        self.statistics.processed_size += user_key.len() + value.len();
                        Ok(Some(value.to_vec()))
                    }
                    None => {
                        let value = self.load_data_from_default_cf(lock.ts, user_key)?;
                        self.statistics.processed_size += user_key.len() + value.len();
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
}

#[cfg(test)]
mod tests {

    use engine_rocks::ReadPerfInstant;
    use kvproto::kvrpcpb::{Assertion, AssertionLevel, PrewriteRequestPessimisticAction::*};
    use tidb_query_datatype::{
        codec::row::v2::{
            RowSlice,
            encoder_for_test::{RowEncoder, prepare_cols_for_test},
        },
        expr::EvalContext,
    };
    use txn_types::SHORT_VALUE_MAX_LEN;

    use super::*;
    use crate::storage::{
        kv::{CfStatistics, Engine, RocksEngine, TestEngineBuilder},
        mvcc::{Error, ErrorInner},
        txn::tests::{
            must_acquire_pessimistic_lock, must_cleanup_with_gc_fence, must_commit, must_gc,
            must_pessimistic_prewrite_delete, must_prewrite_delete, must_prewrite_lock,
            must_prewrite_put, must_prewrite_put_impl, must_rollback,
        },
    };

    fn new_point_getter<E: Engine>(engine: &mut E, ts: TimeStamp) -> PointGetter<E::Snap> {
        new_point_getter_with_iso(engine, ts, IsolationLevel::Si)
    }

    fn new_point_getter_with_iso<E: Engine>(
        engine: &mut E,
        ts: TimeStamp,
        iso_level: IsolationLevel,
    ) -> PointGetter<E::Snap> {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        PointGetterBuilder::new(snapshot, ts)
            .isolation_level(iso_level)
            .build()
            .unwrap()
    }

    fn must_get_key<S: Snapshot>(point_getter: &mut PointGetter<S>, key: &[u8]) {
        assert!(point_getter.get(&Key::from_raw(key)).unwrap().is_some());
    }

    fn must_get_value<S: Snapshot>(point_getter: &mut PointGetter<S>, key: &[u8], prefix: &[u8]) {
        let val = point_getter.get(&Key::from_raw(key)).unwrap().unwrap();
        assert!(val.starts_with(prefix));
    }

    fn must_get_entry<S: Snapshot>(
        point_getter: &mut PointGetter<S>,
        key: &[u8],
        load_commit_ts: bool,
        prefix: &[u8],
        commit_ts_opt: Option<u64>,
    ) {
        let entry = point_getter
            .get_entry(&Key::from_raw(key), load_commit_ts)
            .unwrap()
            .unwrap();
        assert!(entry.value.starts_with(prefix));
        if load_commit_ts {
            let commit_ts = commit_ts_opt.unwrap().into();
            assert_eq!(entry.commit_ts, Some(commit_ts));
        } else {
            assert!(commit_ts_opt.is_none());
            assert!(entry.commit_ts.is_none());
        }
    }

    fn must_met_newer_ts_data<E: Engine>(
        engine: &mut E,
        getter_ts: impl Into<TimeStamp>,
        key: &[u8],
        value: &[u8],
        expected_met_newer_ts_data: bool,
    ) {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let ts = getter_ts.into();
        let mut point_getter = PointGetterBuilder::new(snapshot.clone(), ts)
            .isolation_level(IsolationLevel::Si)
            .check_has_newer_ts_data(true)
            .build()
            .unwrap();
        let val = point_getter.get(&Key::from_raw(key)).unwrap().unwrap();
        assert_eq!(val, value);
        let expected = if expected_met_newer_ts_data {
            NewerTsCheckState::Met
        } else {
            NewerTsCheckState::NotMetYet
        };
        assert_eq!(expected, point_getter.met_newer_ts_data());

        let mut point_getter = PointGetterBuilder::new(snapshot, ts)
            .isolation_level(IsolationLevel::Si)
            .check_has_newer_ts_data(false)
            .build()
            .unwrap();
        let val = point_getter.get(&Key::from_raw(key)).unwrap().unwrap();
        assert_eq!(val, value);
        assert_eq!(NewerTsCheckState::Unknown, point_getter.met_newer_ts_data());
    }

    fn must_get_none<S: Snapshot>(point_getter: &mut PointGetter<S>, key: &[u8]) {
        assert!(point_getter.get(&Key::from_raw(key)).unwrap().is_none());
    }

    fn must_get_err<S: Snapshot>(point_getter: &mut PointGetter<S>, key: &[u8]) {
        point_getter.get(&Key::from_raw(key)).unwrap_err();
    }

    fn must_get_entry_err<S: Snapshot>(
        point_getter: &mut PointGetter<S>,
        key: &[u8],
        load_commit_ts: bool,
    ) -> Error {
        point_getter
            .get_entry(&Key::from_raw(key), load_commit_ts)
            .unwrap_err()
    }

    fn assert_seek_next_prev(stat: &CfStatistics, seek: usize, next: usize, prev: usize) {
        assert_eq!(
            stat.seek, seek,
            "expect seek to be {}, got {}",
            seek, stat.seek
        );
        assert_eq!(
            stat.next, next,
            "expect next to be {}, got {}",
            next, stat.next
        );
        assert_eq!(
            stat.prev, prev,
            "expect prev to be {}, got {}",
            prev, stat.prev
        );
    }

    /// Builds a sample engine with the following data:
    /// LOCK    bar                     (commit at 11)
    /// PUT     bar     -> barvvv...    (commit at 5)
    /// PUT     box     -> boxvv....    (commit at 9)
    /// DELETE  foo1                    (commit at 9)
    /// PUT     foo1    -> foo1vv...    (commit at 3)
    /// LOCK    foo2                    (commit at 101)
    /// ...
    /// LOCK    foo2                    (commit at 23)
    /// LOCK    foo2                    (commit at 21)
    /// PUT     foo2    -> foo2vv...    (commit at 5)
    /// DELETE  xxx                     (commit at 7)
    /// PUT     zz       -> zvzv....    (commit at 103)
    fn new_sample_engine() -> RocksEngine {
        let suffix = "v".repeat(SHORT_VALUE_MAX_LEN + 1);
        let mut engine = TestEngineBuilder::new().build().unwrap();
        must_prewrite_put(
            &mut engine,
            b"foo1",
            &format!("foo1{}", suffix).into_bytes(),
            b"foo1",
            2,
        );
        must_commit(&mut engine, b"foo1", 2, 3);
        must_prewrite_put(
            &mut engine,
            b"foo2",
            &format!("foo2{}", suffix).into_bytes(),
            b"foo2",
            4,
        );
        must_prewrite_put(
            &mut engine,
            b"bar",
            &format!("bar{}", suffix).into_bytes(),
            b"foo2",
            4,
        );
        must_commit(&mut engine, b"foo2", 4, 5);
        must_commit(&mut engine, b"bar", 4, 5);
        must_prewrite_delete(&mut engine, b"xxx", b"xxx", 6);
        must_commit(&mut engine, b"xxx", 6, 7);
        must_prewrite_put(
            &mut engine,
            b"box",
            &format!("box{}", suffix).into_bytes(),
            b"box",
            8,
        );
        must_prewrite_delete(&mut engine, b"foo1", b"box", 8);
        must_commit(&mut engine, b"box", 8, 9);
        must_commit(&mut engine, b"foo1", 8, 9);
        must_prewrite_lock(&mut engine, b"bar", b"bar", 10);
        must_commit(&mut engine, b"bar", 10, 11);
        for i in 20..100 {
            if i % 2 == 0 {
                must_prewrite_lock(&mut engine, b"foo2", b"foo2", i);
                must_commit(&mut engine, b"foo2", i, i + 1);
            }
        }
        must_prewrite_put(
            &mut engine,
            b"zz",
            &format!("zz{}", suffix).into_bytes(),
            b"zz",
            102,
        );
        must_commit(&mut engine, b"zz", 102, 103);
        engine
    }

    /// Builds a sample engine that contains transactions on the way and some
    /// short values embedded in the write CF. The data is as follows:
    /// DELETE  bar                     (start at 4)
    /// PUT     bar     -> barval       (commit at 3)
    /// PUT     foo1    -> foo1vv...    (commit at 3)
    /// PUT     foo2    -> foo2vv...    (start at 4)
    fn new_sample_engine_2() -> RocksEngine {
        let suffix = "v".repeat(SHORT_VALUE_MAX_LEN + 1);
        let mut engine = TestEngineBuilder::new().build().unwrap();
        must_prewrite_put(
            &mut engine,
            b"foo1",
            &format!("foo1{}", suffix).into_bytes(),
            b"foo1",
            2,
        );
        must_prewrite_put(&mut engine, b"bar", b"barval", b"foo1", 2);
        must_commit(&mut engine, b"foo1", 2, 3);
        must_commit(&mut engine, b"bar", 2, 3);

        must_prewrite_put(
            &mut engine,
            b"foo2",
            &format!("foo2{}", suffix).into_bytes(),
            b"foo2",
            4,
        );
        must_prewrite_delete(&mut engine, b"bar", b"foo2", 4);
        engine
    }

    /// No ts larger than get ts
    #[test]
    fn test_basic_1() {
        let mut engine = new_sample_engine();

        let mut getter = new_point_getter(&mut engine, 200.into());

        // Get a deleted key
        must_get_none(&mut getter, b"foo1");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_eq!(s.processed_size, 0);
        // Get again
        must_get_none(&mut getter, b"foo1");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_eq!(s.processed_size, 0);

        // Get a key that exists
        must_get_value(&mut getter, b"foo2", b"foo2v");
        let s = getter.take_statistics();
        // We have to check every version
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_eq!(
            s.processed_size,
            Key::from_raw(b"foo2").len()
                + b"foo2".len()
                + "v".repeat(SHORT_VALUE_MAX_LEN + 1).len()
        );
        // Get again
        must_get_value(&mut getter, b"foo2", b"foo2v");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_eq!(s.write.get, 1);
        assert_eq!(
            s.processed_size,
            Key::from_raw(b"foo2").len()
                + b"foo2".len()
                + "v".repeat(SHORT_VALUE_MAX_LEN + 1).len()
        );

        // Get a smaller key
        must_get_none(&mut getter, b"foo1");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_eq!(s.processed_size, 0);

        // Get a key that does not exist
        must_get_none(&mut getter, b"z");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_eq!(s.processed_size, 0);

        // Get a key that exists
        must_get_value(&mut getter, b"zz", b"zzv");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_eq!(
            s.processed_size,
            Key::from_raw(b"zz").len() + b"zz".len() + "v".repeat(SHORT_VALUE_MAX_LEN + 1).len()
        );
        // Get again
        must_get_value(&mut getter, b"zz", b"zzv");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_eq!(
            s.processed_size,
            Key::from_raw(b"zz").len() + b"zz".len() + "v".repeat(SHORT_VALUE_MAX_LEN + 1).len()
        );
    }

    #[test]
    fn test_use_prefix_seek() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        must_prewrite_put(&mut engine, b"foo1", b"bar1", b"foo1", 10);
        must_commit(&mut engine, b"foo1", 10, 20);

        // Mustn't get the next user key even if point getter doesn't compare user key.
        let mut getter = new_point_getter(&mut engine, 30.into());
        must_get_none(&mut getter, b"foo0");

        let mut getter = new_point_getter(&mut engine, 30.into());
        must_get_none(&mut getter, b"foo");
        must_get_none(&mut getter, b"foo0");
    }

    #[test]
    fn test_tombstone() {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        must_prewrite_put(&mut engine, b"foo", b"bar", b"foo", 10);
        must_prewrite_put(&mut engine, b"foo1", b"bar1", b"foo", 10);
        must_prewrite_put(&mut engine, b"foo2", b"bar2", b"foo", 10);
        must_prewrite_put(&mut engine, b"foo3", b"bar3", b"foo", 10);
        must_commit(&mut engine, b"foo", 10, 20);
        must_commit(&mut engine, b"foo1", 10, 20);
        must_commit(&mut engine, b"foo2", 10, 20);
        must_commit(&mut engine, b"foo3", 10, 20);
        must_prewrite_delete(&mut engine, b"foo1", b"foo1", 30);
        must_prewrite_delete(&mut engine, b"foo2", b"foo1", 30);
        must_commit(&mut engine, b"foo1", 30, 40);
        must_commit(&mut engine, b"foo2", 30, 40);

        must_gc(&mut engine, b"foo", 50);
        must_gc(&mut engine, b"foo1", 50);
        must_gc(&mut engine, b"foo2", 50);
        must_gc(&mut engine, b"foo3", 50);

        let mut getter = new_point_getter(&mut engine, TimeStamp::max());
        let perf_statistics = ReadPerfInstant::new();
        must_get_value(&mut getter, b"foo", b"bar");
        assert_eq!(perf_statistics.delta().internal_delete_skipped_count, 0);

        let perf_statistics = ReadPerfInstant::new();
        must_get_none(&mut getter, b"foo1");
        assert_eq!(perf_statistics.delta().internal_delete_skipped_count, 2);

        let perf_statistics = ReadPerfInstant::new();
        must_get_none(&mut getter, b"foo2");
        assert_eq!(perf_statistics.delta().internal_delete_skipped_count, 2);

        let perf_statistics = ReadPerfInstant::new();
        must_get_value(&mut getter, b"foo3", b"bar3");
        assert_eq!(perf_statistics.delta().internal_delete_skipped_count, 0);
    }

    #[test]
    fn test_with_iter_lower_bound() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        must_prewrite_put(&mut engine, b"foo", b"bar", b"foo", 10);
        must_commit(&mut engine, b"foo", 10, 20);

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let write_cursor = CursorBuilder::new(&snapshot, CF_WRITE)
            .prefix_seek(true)
            .scan_mode(ScanMode::Mixed)
            .range(Some(Key::from_raw(b"a")), None)
            .build()
            .unwrap();
        let mut getter = PointGetter {
            snapshot,
            omit_value: false,
            isolation_level: IsolationLevel::Si,
            ts: TimeStamp::new(30),
            bypass_locks: Default::default(),
            access_locks: Default::default(),
            met_newer_ts_data: NewerTsCheckState::NotMetYet,
            statistics: Statistics::default(),
            write_cursor,
        };
        must_get_value(&mut getter, b"foo", b"bar");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_eq!(s.processed_size, Key::from_raw(b"foo").len() + b"bar".len());
    }

    /// Some ts larger than get ts
    #[test]
    fn test_basic_2() {
        let mut engine = new_sample_engine();

        let mut getter = new_point_getter(&mut engine, 5.into());

        must_get_value(&mut getter, b"bar", b"barv");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_eq!(
            s.processed_size,
            Key::from_raw(b"bar").len() + b"bar".len() + "v".repeat(SHORT_VALUE_MAX_LEN + 1).len()
        );

        must_get_value(&mut getter, b"bar", b"barv");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_eq!(
            s.processed_size,
            Key::from_raw(b"bar").len() + b"bar".len() + "v".repeat(SHORT_VALUE_MAX_LEN + 1).len()
        );

        must_get_none(&mut getter, b"bo");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_eq!(s.processed_size, 0);

        must_get_none(&mut getter, b"box");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_eq!(s.processed_size, 0);

        must_get_value(&mut getter, b"foo1", b"foo1");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_eq!(
            s.processed_size,
            Key::from_raw(b"foo1").len()
                + b"foo1".len()
                + "v".repeat(SHORT_VALUE_MAX_LEN + 1).len()
        );

        must_get_none(&mut getter, b"zz");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_eq!(s.processed_size, 0);

        must_get_value(&mut getter, b"foo1", b"foo1");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_eq!(
            s.processed_size,
            Key::from_raw(b"foo1").len()
                + b"foo1".len()
                + "v".repeat(SHORT_VALUE_MAX_LEN + 1).len()
        );

        must_get_value(&mut getter, b"bar", b"barv");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_eq!(
            s.processed_size,
            Key::from_raw(b"bar").len() + b"bar".len() + "v".repeat(SHORT_VALUE_MAX_LEN + 1).len()
        );
    }

    /// All ts larger than get ts
    #[test]
    fn test_basic_3() {
        let mut engine = new_sample_engine();

        let mut getter = new_point_getter(&mut engine, 2.into());

        must_get_none(&mut getter, b"foo1");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_eq!(s.processed_size, 0);

        must_get_none(&mut getter, b"non_exist");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_eq!(s.processed_size, 0);

        must_get_none(&mut getter, b"foo1");
        must_get_none(&mut getter, b"foo0");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 2, 0, 0);
        assert_eq!(s.processed_size, 0);
    }

    /// There are some locks in the Lock CF.
    #[test]
    fn test_locked() {
        let mut engine = new_sample_engine_2();

        let mut getter = new_point_getter(&mut engine, 1.into());
        must_get_none(&mut getter, b"a");
        must_get_none(&mut getter, b"bar");
        must_get_none(&mut getter, b"foo1");
        must_get_none(&mut getter, b"foo2");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 4, 0, 0);
        assert_eq!(s.processed_size, 0);

        let mut getter = new_point_getter(&mut engine, 3.into());
        must_get_none(&mut getter, b"a");
        must_get_value(&mut getter, b"bar", b"barv");
        must_get_value(&mut getter, b"bar", b"barv");
        must_get_value(&mut getter, b"foo1", b"foo1v");
        must_get_value(&mut getter, b"foo1", b"foo1v");
        must_get_none(&mut getter, b"foo2");
        must_get_none(&mut getter, b"foo2");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 7, 0, 0);
        assert_eq!(
            s.processed_size,
            (Key::from_raw(b"bar").len() + b"barval".len()) * 2
                + (Key::from_raw(b"foo1").len()
                    + b"foo1".len()
                    + "v".repeat(SHORT_VALUE_MAX_LEN + 1).len())
                    * 2
        );

        let mut getter = new_point_getter(&mut engine, 4.into());
        must_get_none(&mut getter, b"a");
        must_get_err(&mut getter, b"bar");
        must_get_err(&mut getter, b"bar");
        must_get_value(&mut getter, b"foo1", b"foo1v");
        must_get_err(&mut getter, b"foo2");
        must_get_none(&mut getter, b"zz");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 3, 0, 0);
        assert_eq!(
            s.processed_size,
            Key::from_raw(b"foo1").len()
                + b"foo1".len()
                + "v".repeat(SHORT_VALUE_MAX_LEN + 1).len()
        );
    }

    #[test]
    fn test_omit_value() {
        let mut engine = new_sample_engine_2();

        let snapshot = engine.snapshot(Default::default()).unwrap();

        let mut getter = PointGetterBuilder::new(snapshot, 4.into())
            .isolation_level(IsolationLevel::Si)
            .omit_value(true)
            .build()
            .unwrap();
        must_get_err(&mut getter, b"bar");
        must_get_key(&mut getter, b"foo1");
        must_get_err(&mut getter, b"foo2");
        must_get_none(&mut getter, b"foo3");
    }

    #[test]
    fn test_get_latest_value() {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        let (key, val) = (b"foo", b"bar");
        must_prewrite_put(&mut engine, key, val, key, 10);
        must_commit(&mut engine, key, 10, 20);

        let mut getter = new_point_getter(&mut engine, TimeStamp::max());
        must_get_value(&mut getter, key, val);

        // Ignore the primary lock if read with max ts.
        must_prewrite_delete(&mut engine, key, key, 30);
        let mut getter = new_point_getter(&mut engine, TimeStamp::max());
        must_get_value(&mut getter, key, val);
        must_rollback(&mut engine, key, 30, false);

        // Should not ignore the secondary lock even though reading the latest version
        must_prewrite_delete(&mut engine, key, b"bar", 40);
        let mut getter = new_point_getter(&mut engine, TimeStamp::max());
        must_get_err(&mut getter, key);
        must_rollback(&mut engine, key, 40, false);

        // Should get the latest committed value if there is a primary lock with a ts
        // less than the latest Write's commit_ts.
        //
        // write.start_ts(10) < primary_lock.start_ts(15) < write.commit_ts(20)
        must_acquire_pessimistic_lock(&mut engine, key, key, 15, 50);
        must_pessimistic_prewrite_delete(&mut engine, key, key, 15, 50, DoPessimisticCheck);
        let mut getter = new_point_getter(&mut engine, TimeStamp::max());
        must_get_value(&mut getter, key, val);
    }

    #[test]
    fn test_get_bypass_locks() {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        let (key, val) = (b"foo", b"bar");
        must_prewrite_put(&mut engine, key, val, key, 10);
        must_commit(&mut engine, key, 10, 20);

        must_prewrite_delete(&mut engine, key, key, 30);

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut getter = PointGetterBuilder::new(snapshot, 60.into())
            .isolation_level(IsolationLevel::Si)
            .bypass_locks(TsSet::from_u64s(vec![30, 40, 50]))
            .build()
            .unwrap();
        must_get_value(&mut getter, key, val);

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut getter = PointGetterBuilder::new(snapshot, 60.into())
            .isolation_level(IsolationLevel::Si)
            .bypass_locks(TsSet::from_u64s(vec![31, 29]))
            .build()
            .unwrap();
        must_get_err(&mut getter, key);
    }

    #[test]
    fn test_get_access_locks() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let mut engine_clone = engine.clone();
        let mut build_getter = |ts: u64, bypass_locks, access_locks| {
            let snapshot = engine_clone.snapshot(Default::default()).unwrap();
            PointGetterBuilder::new(snapshot, ts.into())
                .isolation_level(IsolationLevel::Si)
                .bypass_locks(TsSet::from_u64s(bypass_locks))
                .access_locks(TsSet::from_u64s(access_locks))
                .build()
                .unwrap()
        };

        // short value
        let (key, val) = (b"foo", b"bar");
        must_prewrite_put(&mut engine, key, val, key, 10);
        must_get_value(&mut build_getter(20, vec![], vec![10]), key, val);
        must_commit(&mut engine, key, 10, 15);
        must_get_value(&mut build_getter(20, vec![], vec![]), key, val);

        // load value from default cf.
        let val = "v".repeat(SHORT_VALUE_MAX_LEN + 1);
        let val = val.as_bytes();
        must_prewrite_put(&mut engine, key, val, key, 20);
        must_get_value(&mut build_getter(30, vec![], vec![20]), key, val);
        must_commit(&mut engine, key, 20, 25);
        must_get_value(&mut build_getter(30, vec![], vec![]), key, val);

        // delete
        must_prewrite_delete(&mut engine, key, key, 30);
        must_get_none(&mut build_getter(40, vec![], vec![30]), key);
        must_commit(&mut engine, key, 30, 35);
        must_get_none(&mut build_getter(40, vec![], vec![]), key);

        // ignore locks not blocking read
        let (key, val) = (b"foo", b"bar");
        // lock's ts > read's ts
        must_prewrite_put(&mut engine, key, val, key, 50);
        must_get_none(&mut build_getter(45, vec![], vec![50]), key);
        must_commit(&mut engine, key, 50, 55);
        // LockType::Lock
        must_prewrite_lock(&mut engine, key, key, 60);
        must_get_value(&mut build_getter(65, vec![], vec![60]), key, val);
        must_commit(&mut engine, key, 60, 65);
        // LockType::Pessimistic
        must_acquire_pessimistic_lock(&mut engine, key, key, 70, 70);
        must_get_value(&mut build_getter(75, vec![], vec![70]), key, val);
        must_rollback(&mut engine, key, 70, false);
        // lock's min_commit_ts > read's ts
        must_prewrite_put_impl(
            &mut engine,
            key,
            &val[..1],
            key,
            &None,
            80.into(),
            SkipPessimisticCheck,
            100,
            80.into(),
            1,
            100.into(), // min_commit_ts
            TimeStamp::default(),
            false,
            Assertion::None,
            AssertionLevel::Off,
        );
        must_get_value(&mut build_getter(85, vec![], vec![80]), key, val);
        must_rollback(&mut engine, key, 80, false);
        // read'ts == max && lock is a primary lock.
        must_prewrite_put(&mut engine, key, &val[..1], key, 90);
        must_get_value(
            &mut build_getter(TimeStamp::max().into_inner(), vec![], vec![90]),
            key,
            val,
        );
        must_rollback(&mut engine, key, 90, false);
        // lock in resolve_keys(it can't happen).
        must_prewrite_put(&mut engine, key, &val[..1], key, 100);
        must_get_value(&mut build_getter(105, vec![100], vec![100]), key, val);
        must_rollback(&mut engine, key, 100, false);
    }

    #[test]
    fn test_met_newer_ts_data() {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        let (key, val1) = (b"foo", b"bar1");
        must_prewrite_put(&mut engine, key, val1, key, 10);
        must_commit(&mut engine, key, 10, 20);

        let (key, val2) = (b"foo", b"bar2");
        must_prewrite_put(&mut engine, key, val2, key, 30);
        must_commit(&mut engine, key, 30, 40);

        must_met_newer_ts_data(&mut engine, 20, key, val1, true);
        must_met_newer_ts_data(&mut engine, 30, key, val1, true);
        must_met_newer_ts_data(&mut engine, 40, key, val2, false);
        must_met_newer_ts_data(&mut engine, 50, key, val2, false);

        must_prewrite_lock(&mut engine, key, key, 60);

        must_met_newer_ts_data(&mut engine, 50, key, val2, true);
        must_met_newer_ts_data(&mut engine, 60, key, val2, true);
    }

    #[test]
    fn test_point_get_check_gc_fence() {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        // PUT,      Read
        //  `--------------^
        must_prewrite_put(&mut engine, b"k1", b"v1", b"k1", 10);
        must_commit(&mut engine, b"k1", 10, 20);
        must_cleanup_with_gc_fence(&mut engine, b"k1", 20, 0, 50, true);

        // PUT,      Read
        //  `---------^
        must_prewrite_put(&mut engine, b"k2", b"v2", b"k2", 11);
        must_commit(&mut engine, b"k2", 11, 20);
        must_cleanup_with_gc_fence(&mut engine, b"k2", 20, 0, 40, true);

        // PUT,      Read
        //  `-----^
        must_prewrite_put(&mut engine, b"k3", b"v3", b"k3", 12);
        must_commit(&mut engine, b"k3", 12, 20);
        must_cleanup_with_gc_fence(&mut engine, b"k3", 20, 0, 30, true);

        // PUT,   PUT,       Read
        //  `-----^ `----^
        must_prewrite_put(&mut engine, b"k4", b"v4", b"k4", 13);
        must_commit(&mut engine, b"k4", 13, 14);
        must_prewrite_put(&mut engine, b"k4", b"v4x", b"k4", 15);
        must_commit(&mut engine, b"k4", 15, 20);
        must_cleanup_with_gc_fence(&mut engine, b"k4", 14, 0, 20, false);
        must_cleanup_with_gc_fence(&mut engine, b"k4", 20, 0, 30, true);

        // PUT,   DEL,       Read
        //  `-----^ `----^
        must_prewrite_put(&mut engine, b"k5", b"v5", b"k5", 13);
        must_commit(&mut engine, b"k5", 13, 14);
        must_prewrite_delete(&mut engine, b"k5", b"v5", 15);
        must_commit(&mut engine, b"k5", 15, 20);
        must_cleanup_with_gc_fence(&mut engine, b"k5", 14, 0, 20, false);
        must_cleanup_with_gc_fence(&mut engine, b"k5", 20, 0, 30, true);

        // PUT, LOCK, LOCK,   Read
        //  `------------------------^
        must_prewrite_put(&mut engine, b"k6", b"v6", b"k6", 16);
        must_commit(&mut engine, b"k6", 16, 20);
        must_prewrite_lock(&mut engine, b"k6", b"k6", 25);
        must_commit(&mut engine, b"k6", 25, 26);
        must_prewrite_lock(&mut engine, b"k6", b"k6", 28);
        must_commit(&mut engine, b"k6", 28, 29);
        must_cleanup_with_gc_fence(&mut engine, b"k6", 20, 0, 50, true);

        // PUT, LOCK,   LOCK,   Read
        //  `---------^
        must_prewrite_put(&mut engine, b"k7", b"v7", b"k7", 16);
        must_commit(&mut engine, b"k7", 16, 20);
        must_prewrite_lock(&mut engine, b"k7", b"k7", 25);
        must_commit(&mut engine, b"k7", 25, 26);
        must_cleanup_with_gc_fence(&mut engine, b"k7", 20, 0, 27, true);
        must_prewrite_lock(&mut engine, b"k7", b"k7", 28);
        must_commit(&mut engine, b"k7", 28, 29);

        // PUT,  Read
        //  * (GC fence ts is 0)
        must_prewrite_put(&mut engine, b"k8", b"v8", b"k8", 17);
        must_commit(&mut engine, b"k8", 17, 30);
        must_cleanup_with_gc_fence(&mut engine, b"k8", 30, 0, 0, true);

        // PUT, LOCK,     Read
        // `-----------^
        must_prewrite_put(&mut engine, b"k9", b"v9", b"k9", 18);
        must_commit(&mut engine, b"k9", 18, 20);
        must_prewrite_lock(&mut engine, b"k9", b"k9", 25);
        must_commit(&mut engine, b"k9", 25, 26);
        must_cleanup_with_gc_fence(&mut engine, b"k9", 20, 0, 27, true);

        let expected_results = vec![
            (b"k1", Some(b"v1")),
            (b"k2", None),
            (b"k3", None),
            (b"k4", None),
            (b"k5", None),
            (b"k6", Some(b"v6")),
            (b"k7", None),
            (b"k8", Some(b"v8")),
            (b"k9", None),
        ];

        for (k, v) in expected_results.iter().copied() {
            let mut single_getter = new_point_getter(&mut engine, 40.into());
            let value = single_getter.get(&Key::from_raw(k)).unwrap();
            assert_eq!(value, v.map(|v| v.to_vec()));
        }

        let mut getter = new_point_getter(&mut engine, 40.into());
        for (k, v) in expected_results {
            let value = getter.get(&Key::from_raw(k)).unwrap();
            assert_eq!(value, v.map(|v| v.to_vec()));
        }
    }

    #[test]
    fn test_point_get_check_rc_ts() {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        let (key0, val0) = (b"k0", b"v0");
        must_prewrite_put(&mut engine, key0, val0, key0, 1);
        must_commit(&mut engine, key0, 1, 5);

        let (key1, val1) = (b"k1", b"v1");
        must_prewrite_put(&mut engine, key1, val1, key1, 10);
        must_commit(&mut engine, key1, 10, 20);

        let (key2, val2, val22) = (b"k2", b"v2", b"v22");
        must_prewrite_put(&mut engine, key2, val2, key2, 30);
        must_commit(&mut engine, key2, 30, 40);
        must_prewrite_put(&mut engine, key2, val22, key2, 41);
        must_commit(&mut engine, key2, 41, 42);

        let (key3, val3) = (b"k3", b"v3");
        must_prewrite_put(&mut engine, key3, val3, key3, 50);

        let (key4, val4) = (b"k4", b"val4");
        must_prewrite_put(&mut engine, key4, val4, key4, 55);
        must_commit(&mut engine, key4, 55, 56);
        must_prewrite_lock(&mut engine, key4, key4, 60);

        let (key5, val5) = (b"k5", b"val5");
        must_prewrite_put(&mut engine, key5, val5, key5, 57);
        must_commit(&mut engine, key5, 57, 58);
        must_acquire_pessimistic_lock(&mut engine, key5, key5, 65, 65);

        // No more recent version.
        let mut getter_with_ts_ok =
            new_point_getter_with_iso(&mut engine, 25.into(), IsolationLevel::RcCheckTs);
        must_get_value(&mut getter_with_ts_ok, key1, val1);

        // The read_ts is stale error should be reported.
        let mut getter_not_ok =
            new_point_getter_with_iso(&mut engine, 35.into(), IsolationLevel::RcCheckTs);
        must_get_err(&mut getter_not_ok, key2);

        // Though lock.ts > read_ts error should still be reported.
        let mut getter_not_ok =
            new_point_getter_with_iso(&mut engine, 45.into(), IsolationLevel::RcCheckTs);
        must_get_err(&mut getter_not_ok, key3);

        // Error should not be reported if the lock type is rollback or lock.
        let mut getter_ok =
            new_point_getter_with_iso(&mut engine, 70.into(), IsolationLevel::RcCheckTs);
        must_get_value(&mut getter_ok, key4, val4);
        let mut getter_ok =
            new_point_getter_with_iso(&mut engine, 70.into(), IsolationLevel::RcCheckTs);
        must_get_value(&mut getter_ok, key5, val5);

        // Test batch point get. Report error if more recent version is met.
        let mut batch_getter =
            new_point_getter_with_iso(&mut engine, 35.into(), IsolationLevel::RcCheckTs);
        must_get_value(&mut batch_getter, key0, val0);
        must_get_value(&mut batch_getter, key1, val1);
        must_get_err(&mut batch_getter, key2);

        // Test batch point get. Report error if lock is met though lock.ts > read_ts.
        let mut batch_getter =
            new_point_getter_with_iso(&mut engine, 45.into(), IsolationLevel::RcCheckTs);
        must_get_value(&mut batch_getter, key0, val0);
        must_get_value(&mut batch_getter, key1, val1);
        must_get_value(&mut batch_getter, key2, val22);
        must_get_err(&mut batch_getter, key3);

        // Test batch point get. Error should not be reported if the lock type is
        // rollback or lock.
        let mut batch_getter_ok =
            new_point_getter_with_iso(&mut engine, 70.into(), IsolationLevel::RcCheckTs);
        must_get_value(&mut batch_getter_ok, key4, val4);
        must_get_value(&mut batch_getter_ok, key5, val5);
    }

    #[test]
    fn test_point_get_non_exist_skip_lock() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let k = b"k";

        // Write enough LOCK recrods
        for start_ts in (1..30).step_by(2) {
            must_prewrite_lock(&mut engine, k, k, start_ts);
            must_commit(&mut engine, k, start_ts, start_ts + 1);
        }

        let mut getter = new_point_getter(&mut engine, 40.into());
        must_get_none(&mut getter, k);
        let s = getter.take_statistics();
        // We can know the key doesn't exist without skipping all these locks according
        // to last_change_ts and estimated_versions_to_last_change.
        assert_eq!(s.write.seek, 1);
        assert_eq!(s.write.next, 0);
        assert_eq!(s.write.get, 0);
    }

    #[test]
    fn test_point_get_with_checksum() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let k = b"k";
        let mut val_buf = Vec::new();
        let columns = prepare_cols_for_test();
        val_buf
            .write_row_with_checksum(&mut EvalContext::default(), columns, Some(123))
            .unwrap();

        must_prewrite_put(&mut engine, k, val_buf.as_slice(), k, 1);
        must_commit(&mut engine, k, 1, 2);

        let mut getter = new_point_getter(&mut engine, 40.into());
        let val = getter.get(&Key::from_raw(k)).unwrap().unwrap();
        assert_eq!(val, val_buf.as_slice());
        let row_slice = RowSlice::from_bytes(val.as_slice()).unwrap();
        assert!(row_slice.get_checksum().unwrap().get_checksum_val() > 0);
    }

    #[test]
    fn test_point_get_load_commit_ts() {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        let (key, val) = (b"foo", b"bar");
        let val2 = b"val2";
        must_prewrite_put(&mut engine, key, val, key, 10);
        must_commit(&mut engine, key, 10, 20);
        must_prewrite_put(&mut engine, key, val2, key, 30);
        must_commit(&mut engine, key, 30, 40);

        let mut getter = new_point_getter(&mut engine, 39.into());
        // when load_commit_ts is false, we should get a None commit_ts.
        must_get_entry(&mut getter, key, false, val, None);
        // when load_commit_ts is true, the right commit_ts should be returned.
        must_get_entry(&mut getter, key, true, val, Some(20));

        // Test access_locks for get_entry
        let val = b"val3";
        must_prewrite_put(&mut engine, key, val, key, 50);
        let mut getter =
            PointGetterBuilder::new(engine.snapshot(Default::default()).unwrap(), 60.into())
                .isolation_level(IsolationLevel::Si)
                .access_locks(TsSet::from_u64s(vec![50]))
                .build()
                .unwrap();
        // When load_commit_ts is false, `access_locks` should be processed and
        // return an entry with None commit_ts.
        must_get_entry(&mut getter, key, false, val, None);
        // When load_commit_ts is true, `access_locks` should be ignored and
        // the lock should be seen as conflict
        let err = must_get_entry_err(&mut getter, key, true);
        assert!(matches!(err.0, box ErrorInner::KeyIsLocked { .. }));
    }

    #[test]
    fn test_point_get_load_commit_ts_with_top_lock_versions() {
        let mut engine = new_sample_engine();
        let mut getter = new_point_getter(&mut engine, 200.into());
        // `foo2` has many committed `LOCK` writes on top of one visible `PUT` at
        // commit_ts = 5.
        must_get_entry(&mut getter, b"foo2", true, b"foo2", Some(5));
    }

    #[test]
    fn test_point_get_load_commit_ts_with_lt_seek_bound_lock_versions_above_put() {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        must_prewrite_put(&mut engine, b"k", b"v", b"k", 1);
        must_commit(&mut engine, b"k", 1, 1);

        let lock_count = SEEK_BOUND - 1;
        for ts in 2..=lock_count + 1 {
            must_prewrite_lock(&mut engine, b"k", b"k", ts);
            must_commit(&mut engine, b"k", ts, ts);
        }

        // Sanity check it won't take the `last_change` seek shortcut.
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let top_lock_ts = lock_count + 1;
        let write_value = snapshot
            .get_cf(CF_WRITE, &Key::from_raw(b"k").append_ts(top_lock_ts.into()))
            .unwrap()
            .unwrap();
        let write = WriteRef::parse(&write_value).unwrap();
        assert_eq!(write.write_type, WriteType::Lock);
        match write.last_change {
            LastChange::Exist {
                last_change_ts,
                estimated_versions_to_last_change,
            } => {
                assert_eq!(last_change_ts, 1.into());
                assert!(estimated_versions_to_last_change < SEEK_BOUND);
            }
            other => panic!("unexpected last_change: {:?}", other),
        }

        let mut getter = new_point_getter(&mut engine, (top_lock_ts + 10).into());
        must_get_entry(&mut getter, b"k", true, b"v", Some(1));
    }

    #[test]
    fn test_point_get_load_commit_ts_with_ge_seek_bound_lock_versions_above_put() {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        must_prewrite_put(&mut engine, b"k", b"v", b"k", 1);
        must_commit(&mut engine, b"k", 1, 1);

        let lock_count = SEEK_BOUND + 1;
        for ts in 2..=lock_count + 1 {
            must_prewrite_lock(&mut engine, b"k", b"k", ts);
            must_commit(&mut engine, b"k", ts, ts);
        }

        // Sanity check it will take the `last_change` seek shortcut.
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let top_lock_ts = lock_count + 1;
        let write_value = snapshot
            .get_cf(CF_WRITE, &Key::from_raw(b"k").append_ts(top_lock_ts.into()))
            .unwrap()
            .unwrap();
        let write = WriteRef::parse(&write_value).unwrap();
        assert_eq!(write.write_type, WriteType::Lock);
        match write.last_change {
            LastChange::Exist {
                last_change_ts,
                estimated_versions_to_last_change,
            } => {
                assert_eq!(last_change_ts, 1.into());
                assert!(estimated_versions_to_last_change >= SEEK_BOUND);
            }
            other => panic!("unexpected last_change: {:?}", other),
        }

        let mut getter = new_point_getter(&mut engine, (top_lock_ts + 10).into());
        must_get_entry(&mut getter, b"k", true, b"v", Some(1));
    }

    #[test]
    fn test_point_get_load_commit_ts_with_lt_seek_bound_lock_versions_above_delete() {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        must_prewrite_put(&mut engine, b"k", b"v", b"k", 1);
        must_commit(&mut engine, b"k", 1, 1);
        must_prewrite_delete(&mut engine, b"k", b"k", 2);
        must_commit(&mut engine, b"k", 2, 2);

        let lock_count = SEEK_BOUND - 1;
        for ts in 3..=lock_count + 2 {
            must_prewrite_lock(&mut engine, b"k", b"k", ts);
            must_commit(&mut engine, b"k", ts, ts);
        }

        // Sanity check it won't take the `last_change` seek shortcut.
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let top_lock_ts = lock_count + 2;
        let write_value = snapshot
            .get_cf(CF_WRITE, &Key::from_raw(b"k").append_ts(top_lock_ts.into()))
            .unwrap()
            .unwrap();
        let write = WriteRef::parse(&write_value).unwrap();
        assert_eq!(write.write_type, WriteType::Lock);
        match write.last_change {
            LastChange::Exist {
                last_change_ts,
                estimated_versions_to_last_change,
            } => {
                assert_eq!(last_change_ts, 2.into());
                assert!(estimated_versions_to_last_change < SEEK_BOUND);
            }
            other => panic!("unexpected last_change: {:?}", other),
        }

        let mut getter = new_point_getter(&mut engine, (top_lock_ts + 10).into());
        assert!(
            getter
                .get_entry(&Key::from_raw(b"k"), true)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_point_get_load_commit_ts_with_ge_seek_bound_lock_versions_above_delete() {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        must_prewrite_put(&mut engine, b"k", b"v", b"k", 1);
        must_commit(&mut engine, b"k", 1, 1);
        must_prewrite_delete(&mut engine, b"k", b"k", 2);
        must_commit(&mut engine, b"k", 2, 2);

        let lock_count = SEEK_BOUND + 1;
        for ts in 3..=lock_count + 2 {
            must_prewrite_lock(&mut engine, b"k", b"k", ts);
            must_commit(&mut engine, b"k", ts, ts);
        }

        // Sanity check it will take the `last_change` seek shortcut.
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let top_lock_ts = lock_count + 2;
        let write_value = snapshot
            .get_cf(CF_WRITE, &Key::from_raw(b"k").append_ts(top_lock_ts.into()))
            .unwrap()
            .unwrap();
        let write = WriteRef::parse(&write_value).unwrap();
        assert_eq!(write.write_type, WriteType::Lock);
        match write.last_change {
            LastChange::Exist {
                last_change_ts,
                estimated_versions_to_last_change,
            } => {
                assert_eq!(last_change_ts, 2.into());
                assert!(estimated_versions_to_last_change >= SEEK_BOUND);
            }
            other => panic!("unexpected last_change: {:?}", other),
        }

        let mut getter = new_point_getter(&mut engine, (top_lock_ts + 10).into());
        assert!(
            getter
                .get_entry(&Key::from_raw(b"k"), true)
                .unwrap()
                .is_none()
        );
    }
}
