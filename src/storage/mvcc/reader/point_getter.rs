// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath
use std::borrow::Cow;

use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE};
use kvproto::kvrpcpb::IsolationLevel;
use txn_types::{Key, Lock, LockType, TimeStamp, TsSet, Value, WriteRef, WriteType};

use crate::storage::{
    kv::{Cursor, CursorBuilder, ScanMode, Snapshot, Statistics},
    mvcc::{default_not_found_error, ErrorInner::WriteConflict, NewerTsCheckState, Result},
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

    /// Set whether values of the user key should be omitted. When `omit_value` is `true`, the
    /// length of returned value will be 0.
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

    /// Check whether there is data with newer ts. The result of `met_newer_ts_data` is Unknown
    /// if this option is not set.
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

/// This struct can be used to get the value of user keys. Internally, rollbacks are ignored and
/// smaller version will be tried. If the isolation level is Si, locks will be checked first.
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
    pub fn get(&mut self, user_key: &Key) -> Result<Option<Value>> {
        fail_point!("point_getter_get");

        if need_check_locks(self.isolation_level) {
            // Check locks that signal concurrent writes for `Si` or more recent writes for `RcCheckTs`.
            if let Some(lock) = self.load_and_check_lock(user_key)? {
                return self.load_data_from_lock(user_key, lock);
            }
        }

        self.load_data(user_key)
    }

    /// Get a lock of a user key in the lock CF. If lock exists, it will be checked to
    /// see whether it conflicts with the given `ts` and return an error if so. If the
    /// lock is in access_locks, it will be returned and caller can read through it.
    ///
    /// In common cases we expect to get nothing in lock cf. Using a `get_cf` instead of `seek`
    /// is fast in such cases due to no need for RocksDB to continue move and skip deleted entries
    /// until find a user key.
    fn load_and_check_lock(&mut self, user_key: &Key) -> Result<Option<Lock>> {
        self.statistics.lock.get += 1;
        let lock_value = self.snapshot.get_cf(CF_LOCK, user_key)?;

        if let Some(ref lock_value) = lock_value {
            let lock = Lock::parse(lock_value)?;
            if self.met_newer_ts_data == NewerTsCheckState::NotMetYet {
                self.met_newer_ts_data = NewerTsCheckState::Met;
            }
            if let Err(e) = Lock::check_ts_conflict(
                Cow::Borrowed(&lock),
                user_key,
                self.ts,
                &self.bypass_locks,
                self.isolation_level,
            ) {
                self.statistics.lock.processed_keys += 1;
                if self.access_locks.contains(lock.ts) {
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
    /// First, a correct version info in the Write CF will be sought. Then, value will be loaded
    /// from Default CF if necessary.
    fn load_data(&mut self, user_key: &Key) -> Result<Option<Value>> {
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

        loop {
            // No need to compare user key because it uses prefix seek.
            let write = WriteRef::parse(self.write_cursor.value(&mut self.statistics.write))?;

            if !write.check_gc_fence_as_latest_version(self.ts) {
                return Ok(None);
            }

            match write.write_type {
                WriteType::Put => {
                    self.statistics.write.processed_keys += 1;
                    resource_metering::record_read_keys(1);

                    if self.omit_value {
                        return Ok(Some(vec![]));
                    }
                    match write.short_value {
                        Some(value) => {
                            // Value is carried in `write`.
                            self.statistics.processed_size += user_key.len() + value.len();
                            return Ok(Some(value.to_vec()));
                        }
                        None => {
                            let start_ts = write.start_ts;
                            let value = self.load_data_from_default_cf(start_ts, user_key)?;
                            self.statistics.processed_size += user_key.len() + value.len();
                            return Ok(Some(value));
                        }
                    }
                }
                WriteType::Delete => {
                    return Ok(None);
                }
                WriteType::Lock | WriteType::Rollback => {
                    // Continue iterate next `write`.
                }
            }

            if !self.write_cursor.next(&mut self.statistics.write) {
                return Ok(None);
            }
        }
    }

    /// Load the value from default CF.
    ///
    /// We assume that mostly the keys given to batch get keys are not very close to each other.
    /// `near_seek` will likely fall back to `seek` in such scenario, which takes 2x time
    /// compared to `get_cf`. Thus we use `get_cf` directly here.
    fn load_data_from_default_cf(
        &mut self,
        write_start_ts: TimeStamp,
        user_key: &Key,
    ) -> Result<Value> {
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
                user_key.to_raw()?,
                "load_data_from_default_cf",
            ))
        }
    }

    /// Load the value from the lock.
    ///
    /// The lock belongs to a committed transaction and its commit_ts <= read's start_ts.
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
            LockType::Lock | LockType::Pessimistic => {
                // Only when fails to call `Lock::check_ts_conflict()`, the function is called, so it's
                // unreachable here.
                unreachable!()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use engine_rocks::ReadPerfInstant;
    use kvproto::kvrpcpb::{Assertion, AssertionLevel};
    use txn_types::SHORT_VALUE_MAX_LEN;

    use super::*;
    use crate::storage::{
        kv::{CfStatistics, Engine, RocksEngine, TestEngineBuilder},
        txn::tests::{
            must_acquire_pessimistic_lock, must_cleanup_with_gc_fence, must_commit, must_gc,
            must_pessimistic_prewrite_delete, must_prewrite_delete, must_prewrite_lock,
            must_prewrite_put, must_prewrite_put_impl, must_rollback,
        },
    };

    fn new_point_getter<E: Engine>(engine: &E, ts: TimeStamp) -> PointGetter<E::Snap> {
        new_point_getter_with_iso(engine, ts, IsolationLevel::Si)
    }

    fn new_point_getter_with_iso<E: Engine>(
        engine: &E,
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

    fn must_met_newer_ts_data<E: Engine>(
        engine: &E,
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
        assert!(point_getter.get(&Key::from_raw(key)).is_err());
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
        let engine = TestEngineBuilder::new().build().unwrap();
        must_prewrite_put(
            &engine,
            b"foo1",
            &format!("foo1{}", suffix).into_bytes(),
            b"foo1",
            2,
        );
        must_commit(&engine, b"foo1", 2, 3);
        must_prewrite_put(
            &engine,
            b"foo2",
            &format!("foo2{}", suffix).into_bytes(),
            b"foo2",
            4,
        );
        must_prewrite_put(
            &engine,
            b"bar",
            &format!("bar{}", suffix).into_bytes(),
            b"foo2",
            4,
        );
        must_commit(&engine, b"foo2", 4, 5);
        must_commit(&engine, b"bar", 4, 5);
        must_prewrite_delete(&engine, b"xxx", b"xxx", 6);
        must_commit(&engine, b"xxx", 6, 7);
        must_prewrite_put(
            &engine,
            b"box",
            &format!("box{}", suffix).into_bytes(),
            b"box",
            8,
        );
        must_prewrite_delete(&engine, b"foo1", b"box", 8);
        must_commit(&engine, b"box", 8, 9);
        must_commit(&engine, b"foo1", 8, 9);
        must_prewrite_lock(&engine, b"bar", b"bar", 10);
        must_commit(&engine, b"bar", 10, 11);
        for i in 20..100 {
            if i % 2 == 0 {
                must_prewrite_lock(&engine, b"foo2", b"foo2", i);
                must_commit(&engine, b"foo2", i, i + 1);
            }
        }
        must_prewrite_put(
            &engine,
            b"zz",
            &format!("zz{}", suffix).into_bytes(),
            b"zz",
            102,
        );
        must_commit(&engine, b"zz", 102, 103);
        engine
    }

    /// Builds a sample engine that contains transactions on the way and some short
    /// values embedded in the write CF. The data is as follows:
    /// DELETE  bar                     (start at 4)
    /// PUT     bar     -> barval       (commit at 3)
    /// PUT     foo1    -> foo1vv...    (commit at 3)
    /// PUT     foo2    -> foo2vv...    (start at 4)
    fn new_sample_engine_2() -> RocksEngine {
        let suffix = "v".repeat(SHORT_VALUE_MAX_LEN + 1);
        let engine = TestEngineBuilder::new().build().unwrap();
        must_prewrite_put(
            &engine,
            b"foo1",
            &format!("foo1{}", suffix).into_bytes(),
            b"foo1",
            2,
        );
        must_prewrite_put(&engine, b"bar", b"barval", b"foo1", 2);
        must_commit(&engine, b"foo1", 2, 3);
        must_commit(&engine, b"bar", 2, 3);

        must_prewrite_put(
            &engine,
            b"foo2",
            &format!("foo2{}", suffix).into_bytes(),
            b"foo2",
            4,
        );
        must_prewrite_delete(&engine, b"bar", b"foo2", 4);
        engine
    }

    /// No ts larger than get ts
    #[test]
    fn test_basic_1() {
        let engine = new_sample_engine();

        let mut getter = new_point_getter(&engine, 200.into());

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
        assert_seek_next_prev(&s.write, 1, 40, 0);
        assert_eq!(
            s.processed_size,
            Key::from_raw(b"foo2").len()
                + b"foo2".len()
                + "v".repeat(SHORT_VALUE_MAX_LEN + 1).len()
        );
        // Get again
        must_get_value(&mut getter, b"foo2", b"foo2v");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 40, 0);
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
        let engine = TestEngineBuilder::new().build().unwrap();
        must_prewrite_put(&engine, b"foo1", b"bar1", b"foo1", 10);
        must_commit(&engine, b"foo1", 10, 20);

        // Mustn't get the next user key even if point getter doesn't compare user key.
        let mut getter = new_point_getter(&engine, 30.into());
        must_get_none(&mut getter, b"foo0");

        let mut getter = new_point_getter(&engine, 30.into());
        must_get_none(&mut getter, b"foo");
        must_get_none(&mut getter, b"foo0");
    }

    #[test]
    fn test_tombstone() {
        let engine = TestEngineBuilder::new().build().unwrap();

        must_prewrite_put(&engine, b"foo", b"bar", b"foo", 10);
        must_prewrite_put(&engine, b"foo1", b"bar1", b"foo", 10);
        must_prewrite_put(&engine, b"foo2", b"bar2", b"foo", 10);
        must_prewrite_put(&engine, b"foo3", b"bar3", b"foo", 10);
        must_commit(&engine, b"foo", 10, 20);
        must_commit(&engine, b"foo1", 10, 20);
        must_commit(&engine, b"foo2", 10, 20);
        must_commit(&engine, b"foo3", 10, 20);
        must_prewrite_delete(&engine, b"foo1", b"foo1", 30);
        must_prewrite_delete(&engine, b"foo2", b"foo1", 30);
        must_commit(&engine, b"foo1", 30, 40);
        must_commit(&engine, b"foo2", 30, 40);

        must_gc(&engine, b"foo", 50);
        must_gc(&engine, b"foo1", 50);
        must_gc(&engine, b"foo2", 50);
        must_gc(&engine, b"foo3", 50);

        let mut getter = new_point_getter(&engine, TimeStamp::max());
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
        let engine = TestEngineBuilder::new().build().unwrap();
        must_prewrite_put(&engine, b"foo", b"bar", b"foo", 10);
        must_commit(&engine, b"foo", 10, 20);

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
        let engine = new_sample_engine();

        let mut getter = new_point_getter(&engine, 5.into());

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
        let engine = new_sample_engine();

        let mut getter = new_point_getter(&engine, 2.into());

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
        let engine = new_sample_engine_2();

        let mut getter = new_point_getter(&engine, 1.into());
        must_get_none(&mut getter, b"a");
        must_get_none(&mut getter, b"bar");
        must_get_none(&mut getter, b"foo1");
        must_get_none(&mut getter, b"foo2");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 4, 0, 0);
        assert_eq!(s.processed_size, 0);

        let mut getter = new_point_getter(&engine, 3.into());
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

        let mut getter = new_point_getter(&engine, 4.into());
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
        let engine = new_sample_engine_2();

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
        let engine = TestEngineBuilder::new().build().unwrap();

        let (key, val) = (b"foo", b"bar");
        must_prewrite_put(&engine, key, val, key, 10);
        must_commit(&engine, key, 10, 20);

        let mut getter = new_point_getter(&engine, TimeStamp::max());
        must_get_value(&mut getter, key, val);

        // Ignore the primary lock if read with max ts.
        must_prewrite_delete(&engine, key, key, 30);
        let mut getter = new_point_getter(&engine, TimeStamp::max());
        must_get_value(&mut getter, key, val);
        must_rollback(&engine, key, 30, false);

        // Should not ignore the secondary lock even though reading the latest version
        must_prewrite_delete(&engine, key, b"bar", 40);
        let mut getter = new_point_getter(&engine, TimeStamp::max());
        must_get_err(&mut getter, key);
        must_rollback(&engine, key, 40, false);

        // Should get the latest committed value if there is a primary lock with a ts less than
        // the latest Write's commit_ts.
        //
        // write.start_ts(10) < primary_lock.start_ts(15) < write.commit_ts(20)
        must_acquire_pessimistic_lock(&engine, key, key, 15, 50);
        must_pessimistic_prewrite_delete(&engine, key, key, 15, 50, true);
        let mut getter = new_point_getter(&engine, TimeStamp::max());
        must_get_value(&mut getter, key, val);
    }

    #[test]
    fn test_get_bypass_locks() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let (key, val) = (b"foo", b"bar");
        must_prewrite_put(&engine, key, val, key, 10);
        must_commit(&engine, key, 10, 20);

        must_prewrite_delete(&engine, key, key, 30);

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
        let engine = TestEngineBuilder::new().build().unwrap();
        let build_getter = |ts: u64, bypass_locks, access_locks| {
            let snapshot = engine.snapshot(Default::default()).unwrap();
            PointGetterBuilder::new(snapshot, ts.into())
                .isolation_level(IsolationLevel::Si)
                .bypass_locks(TsSet::from_u64s(bypass_locks))
                .access_locks(TsSet::from_u64s(access_locks))
                .build()
                .unwrap()
        };

        // short value
        let (key, val) = (b"foo", b"bar");
        must_prewrite_put(&engine, key, val, key, 10);
        must_get_value(&mut build_getter(20, vec![], vec![10]), key, val);
        must_commit(&engine, key, 10, 15);
        must_get_value(&mut build_getter(20, vec![], vec![]), key, val);

        // load value from default cf.
        let val = "v".repeat(SHORT_VALUE_MAX_LEN + 1);
        let val = val.as_bytes();
        must_prewrite_put(&engine, key, val, key, 20);
        must_get_value(&mut build_getter(30, vec![], vec![20]), key, val);
        must_commit(&engine, key, 20, 25);
        must_get_value(&mut build_getter(30, vec![], vec![]), key, val);

        // delete
        must_prewrite_delete(&engine, key, key, 30);
        must_get_none(&mut build_getter(40, vec![], vec![30]), key);
        must_commit(&engine, key, 30, 35);
        must_get_none(&mut build_getter(40, vec![], vec![]), key);

        // ignore locks not blocking read
        let (key, val) = (b"foo", b"bar");
        // lock's ts > read's ts
        must_prewrite_put(&engine, key, val, key, 50);
        must_get_none(&mut build_getter(45, vec![], vec![50]), key);
        must_commit(&engine, key, 50, 55);
        // LockType::Lock
        must_prewrite_lock(&engine, key, key, 60);
        must_get_value(&mut build_getter(65, vec![], vec![60]), key, val);
        must_commit(&engine, key, 60, 65);
        // LockType::Pessimistic
        must_acquire_pessimistic_lock(&engine, key, key, 70, 70);
        must_get_value(&mut build_getter(75, vec![], vec![70]), key, val);
        must_rollback(&engine, key, 70, false);
        // lock's min_commit_ts > read's ts
        must_prewrite_put_impl(
            &engine,
            key,
            &val[..1],
            key,
            &None,
            80.into(),
            false,
            100,
            80.into(),
            1,
            100.into(), /* min_commit_ts */
            TimeStamp::default(),
            false,
            Assertion::None,
            AssertionLevel::Off,
        );
        must_get_value(&mut build_getter(85, vec![], vec![80]), key, val);
        must_rollback(&engine, key, 80, false);
        // read'ts == max && lock is a primary lock.
        must_prewrite_put(&engine, key, &val[..1], key, 90);
        must_get_value(
            &mut build_getter(TimeStamp::max().into_inner(), vec![], vec![90]),
            key,
            val,
        );
        must_rollback(&engine, key, 90, false);
        // lock in resolve_keys(it can't happen).
        must_prewrite_put(&engine, key, &val[..1], key, 100);
        must_get_value(&mut build_getter(105, vec![100], vec![100]), key, val);
        must_rollback(&engine, key, 100, false);
    }

    #[test]
    fn test_met_newer_ts_data() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let (key, val1) = (b"foo", b"bar1");
        must_prewrite_put(&engine, key, val1, key, 10);
        must_commit(&engine, key, 10, 20);

        let (key, val2) = (b"foo", b"bar2");
        must_prewrite_put(&engine, key, val2, key, 30);
        must_commit(&engine, key, 30, 40);

        must_met_newer_ts_data(&engine, 20, key, val1, true);
        must_met_newer_ts_data(&engine, 30, key, val1, true);
        must_met_newer_ts_data(&engine, 40, key, val2, false);
        must_met_newer_ts_data(&engine, 50, key, val2, false);

        must_prewrite_lock(&engine, key, key, 60);

        must_met_newer_ts_data(&engine, 50, key, val2, true);
        must_met_newer_ts_data(&engine, 60, key, val2, true);
    }

    #[test]
    fn test_point_get_check_gc_fence() {
        let engine = TestEngineBuilder::new().build().unwrap();

        // PUT,      Read
        //  `--------------^
        must_prewrite_put(&engine, b"k1", b"v1", b"k1", 10);
        must_commit(&engine, b"k1", 10, 20);
        must_cleanup_with_gc_fence(&engine, b"k1", 20, 0, 50, true);

        // PUT,      Read
        //  `---------^
        must_prewrite_put(&engine, b"k2", b"v2", b"k2", 11);
        must_commit(&engine, b"k2", 11, 20);
        must_cleanup_with_gc_fence(&engine, b"k2", 20, 0, 40, true);

        // PUT,      Read
        //  `-----^
        must_prewrite_put(&engine, b"k3", b"v3", b"k3", 12);
        must_commit(&engine, b"k3", 12, 20);
        must_cleanup_with_gc_fence(&engine, b"k3", 20, 0, 30, true);

        // PUT,   PUT,       Read
        //  `-----^ `----^
        must_prewrite_put(&engine, b"k4", b"v4", b"k4", 13);
        must_commit(&engine, b"k4", 13, 14);
        must_prewrite_put(&engine, b"k4", b"v4x", b"k4", 15);
        must_commit(&engine, b"k4", 15, 20);
        must_cleanup_with_gc_fence(&engine, b"k4", 14, 0, 20, false);
        must_cleanup_with_gc_fence(&engine, b"k4", 20, 0, 30, true);

        // PUT,   DEL,       Read
        //  `-----^ `----^
        must_prewrite_put(&engine, b"k5", b"v5", b"k5", 13);
        must_commit(&engine, b"k5", 13, 14);
        must_prewrite_delete(&engine, b"k5", b"v5", 15);
        must_commit(&engine, b"k5", 15, 20);
        must_cleanup_with_gc_fence(&engine, b"k5", 14, 0, 20, false);
        must_cleanup_with_gc_fence(&engine, b"k5", 20, 0, 30, true);

        // PUT, LOCK, LOCK,   Read
        //  `------------------------^
        must_prewrite_put(&engine, b"k6", b"v6", b"k6", 16);
        must_commit(&engine, b"k6", 16, 20);
        must_prewrite_lock(&engine, b"k6", b"k6", 25);
        must_commit(&engine, b"k6", 25, 26);
        must_prewrite_lock(&engine, b"k6", b"k6", 28);
        must_commit(&engine, b"k6", 28, 29);
        must_cleanup_with_gc_fence(&engine, b"k6", 20, 0, 50, true);

        // PUT, LOCK,   LOCK,   Read
        //  `---------^
        must_prewrite_put(&engine, b"k7", b"v7", b"k7", 16);
        must_commit(&engine, b"k7", 16, 20);
        must_prewrite_lock(&engine, b"k7", b"k7", 25);
        must_commit(&engine, b"k7", 25, 26);
        must_cleanup_with_gc_fence(&engine, b"k7", 20, 0, 27, true);
        must_prewrite_lock(&engine, b"k7", b"k7", 28);
        must_commit(&engine, b"k7", 28, 29);

        // PUT,  Read
        //  * (GC fence ts is 0)
        must_prewrite_put(&engine, b"k8", b"v8", b"k8", 17);
        must_commit(&engine, b"k8", 17, 30);
        must_cleanup_with_gc_fence(&engine, b"k8", 30, 0, 0, true);

        // PUT, LOCK,     Read
        // `-----------^
        must_prewrite_put(&engine, b"k9", b"v9", b"k9", 18);
        must_commit(&engine, b"k9", 18, 20);
        must_prewrite_lock(&engine, b"k9", b"k9", 25);
        must_commit(&engine, b"k9", 25, 26);
        must_cleanup_with_gc_fence(&engine, b"k9", 20, 0, 27, true);

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

        for (k, v) in &expected_results {
            let mut single_getter = new_point_getter(&engine, 40.into());
            let value = single_getter.get(&Key::from_raw(*k)).unwrap();
            assert_eq!(value, v.map(|v| v.to_vec()));
        }

        let mut getter = new_point_getter(&engine, 40.into());
        for (k, v) in &expected_results {
            let value = getter.get(&Key::from_raw(*k)).unwrap();
            assert_eq!(value, v.map(|v| v.to_vec()));
        }
    }

    #[test]
    fn test_point_get_check_rc_ts() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let (key0, val0) = (b"k0", b"v0");
        must_prewrite_put(&engine, key0, val0, key0, 1);
        must_commit(&engine, key0, 1, 5);

        let (key1, val1) = (b"k1", b"v1");
        must_prewrite_put(&engine, key1, val1, key1, 10);
        must_commit(&engine, key1, 10, 20);

        let (key2, val2, val22) = (b"k2", b"v2", b"v22");
        must_prewrite_put(&engine, key2, val2, key2, 30);
        must_commit(&engine, key2, 30, 40);
        must_prewrite_put(&engine, key2, val22, key2, 41);
        must_commit(&engine, key2, 41, 42);

        let (key3, val3) = (b"k3", b"v3");
        must_prewrite_put(&engine, key3, val3, key3, 50);

        let (key4, val4) = (b"k4", b"val4");
        must_prewrite_put(&engine, key4, val4, key4, 55);
        must_commit(&engine, key4, 55, 56);
        must_prewrite_lock(&engine, key4, key4, 60);

        let (key5, val5) = (b"k5", b"val5");
        must_prewrite_put(&engine, key5, val5, key5, 57);
        must_commit(&engine, key5, 57, 58);
        must_acquire_pessimistic_lock(&engine, key5, key5, 65, 65);

        // No more recent version.
        let mut getter_with_ts_ok =
            new_point_getter_with_iso(&engine, 25.into(), IsolationLevel::RcCheckTs);
        must_get_value(&mut getter_with_ts_ok, key1, val1);

        // The read_ts is stale error should be reported.
        let mut getter_not_ok =
            new_point_getter_with_iso(&engine, 35.into(), IsolationLevel::RcCheckTs);
        must_get_err(&mut getter_not_ok, key2);

        // Though lock.ts > read_ts error should still be reported.
        let mut getter_not_ok =
            new_point_getter_with_iso(&engine, 45.into(), IsolationLevel::RcCheckTs);
        must_get_err(&mut getter_not_ok, key3);

        // Error should not be reported if the lock type is rollback or lock.
        let mut getter_ok =
            new_point_getter_with_iso(&engine, 70.into(), IsolationLevel::RcCheckTs);
        must_get_value(&mut getter_ok, key4, val4);
        let mut getter_ok =
            new_point_getter_with_iso(&engine, 70.into(), IsolationLevel::RcCheckTs);
        must_get_value(&mut getter_ok, key5, val5);

        // Test batch point get. Report error if more recent version is met.
        let mut batch_getter =
            new_point_getter_with_iso(&engine, 35.into(), IsolationLevel::RcCheckTs);
        must_get_value(&mut batch_getter, key0, val0);
        must_get_value(&mut batch_getter, key1, val1);
        must_get_err(&mut batch_getter, key2);

        // Test batch point get. Report error if lock is met though lock.ts > read_ts.
        let mut batch_getter =
            new_point_getter_with_iso(&engine, 45.into(), IsolationLevel::RcCheckTs);
        must_get_value(&mut batch_getter, key0, val0);
        must_get_value(&mut batch_getter, key1, val1);
        must_get_value(&mut batch_getter, key2, val22);
        must_get_err(&mut batch_getter, key3);

        // Test batch point get. Error should not be reported if the lock type is rollback or lock.
        let mut batch_getter_ok =
            new_point_getter_with_iso(&engine, 70.into(), IsolationLevel::RcCheckTs);
        must_get_value(&mut batch_getter_ok, key4, val4);
        must_get_value(&mut batch_getter_ok, key5, val5);
    }
}
