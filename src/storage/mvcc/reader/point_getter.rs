// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::kvrpcpb::IsolationLevel;

use crate::storage::mvcc::write::{WriteRef, WriteType};
use crate::storage::mvcc::{default_not_found_error, Lock, Result, TimeStamp, TsSet};
use crate::storage::{Cursor, CursorBuilder, Key, ScanMode, Snapshot, Statistics, Value, CF_LOCK};
use crate::storage::{CF_DEFAULT, CF_WRITE};

/// `PointGetter` factory.
pub struct PointGetterBuilder<S: Snapshot> {
    snapshot: S,
    multi: bool,
    fill_cache: bool,
    omit_value: bool,
    isolation_level: IsolationLevel,
    ts: TimeStamp,
    bypass_locks: TsSet,
}

impl<S: Snapshot> PointGetterBuilder<S> {
    /// Initialize a new `PointGetterBuilder`.
    pub fn new(snapshot: S, ts: TimeStamp) -> Self {
        Self {
            snapshot,
            multi: true,
            fill_cache: true,
            omit_value: false,
            isolation_level: IsolationLevel::Si,
            ts,
            bypass_locks: Default::default(),
        }
    }

    /// Set whether or not to get multiple keys.
    ///
    /// Defaults to `true`.
    #[inline]
    pub fn multi(mut self, multi: bool) -> Self {
        self.multi = multi;
        self
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

    /// Set a set to locks that the reading process can bypass.
    ///
    /// Defaults to none.
    #[inline]
    pub fn bypass_locks(mut self, locks: TsSet) -> Self {
        self.bypass_locks = locks;
        self
    }

    /// Build `PointGetter` from the current configuration.
    pub fn build(self) -> Result<PointGetter<S>> {
        // If we only want to get single value, we can use prefix seek.
        let write_cursor = CursorBuilder::new(&self.snapshot, CF_WRITE)
            .fill_cache(self.fill_cache)
            .prefix_seek(!self.multi)
            .scan_mode(if self.multi {
                ScanMode::Mixed
            } else {
                ScanMode::Forward
            })
            .build()?;

        Ok(PointGetter {
            snapshot: self.snapshot,
            multi: self.multi,
            omit_value: self.omit_value,
            isolation_level: self.isolation_level,
            ts: self.ts,
            bypass_locks: self.bypass_locks,

            statistics: Statistics::default(),

            write_cursor,

            drained: false,
        })
    }
}

/// This struct can be used to get the value of user keys. Internally, rollbacks are ignored and
/// smaller version will be tried. If the isolation level is Si, locks will be checked first.
///
/// Use `PointGetterBuilder` to build `PointGetter`.
pub struct PointGetter<S: Snapshot> {
    snapshot: S,
    multi: bool,
    omit_value: bool,
    isolation_level: IsolationLevel,
    ts: TimeStamp,
    bypass_locks: TsSet,

    statistics: Statistics,

    write_cursor: Cursor<S::Iter>,

    /// Indicating whether or not this structure can serve more requests. It is meaningful only
    /// when `multi == false`, to protect from producing undefined values when trying to get
    /// multiple values under `multi == false`.
    drained: bool,
}

impl<S: Snapshot> PointGetter<S> {
    /// Take out and reset the statistics collected so far.
    #[inline]
    pub fn take_statistics(&mut self) -> Statistics {
        std::mem::replace(&mut self.statistics, Statistics::default())
    }

    /// Get the value of a user key.
    ///
    /// If `multi == false`, this function must be called only once. Future calls return nothing.
    pub fn get(&mut self, user_key: &Key) -> Result<Option<Value>> {
        if !self.multi {
            // Protect from calling `get()` multiple times when `multi == false`.
            if self.drained {
                return Ok(None);
            } else {
                self.drained = true;
            }
        }

        match self.isolation_level {
            IsolationLevel::Si => {
                // Check for locks that signal concurrent writes in Si.
                self.load_and_check_lock(user_key)?;
            }
            IsolationLevel::Rc => {}
        }

        self.load_data(user_key)
    }

    /// Get a lock of a user key in the lock CF. If lock exists, it will be checked to
    /// see whether it conflicts with the given `ts`.
    ///
    /// In common cases we expect to get nothing in lock cf. Using a `get_cf` instead of `seek`
    /// is fast in such cases due to no need for RocksDB to continue move and skip deleted entries
    /// until find a user key.
    fn load_and_check_lock(&mut self, user_key: &Key) -> Result<()> {
        self.statistics.lock.get += 1;
        let lock_value = self.snapshot.get_cf(CF_LOCK, user_key)?;

        if let Some(ref lock_value) = lock_value {
            self.statistics.lock.processed += 1;
            let lock = Lock::parse(lock_value)?;
            lock.check_ts_conflict(user_key, self.ts, &self.bypass_locks)
        } else {
            Ok(())
        }
    }

    /// Load the value.
    ///
    /// First, a correct version info in the Write CF will be sought. Then, value will be loaded
    /// from Default CF if necessary.
    fn load_data(&mut self, user_key: &Key) -> Result<Option<Value>> {
        if !self.write_cursor.seek(
            &user_key.clone().append_ts(self.ts),
            &mut self.statistics.write,
        )? {
            return Ok(None);
        }

        loop {
            // We may seek to another key. In this case, it means we cannot find the specified key.
            {
                let cursor_key = self.write_cursor.key(&mut self.statistics.write);
                if !Key::is_user_key_eq(cursor_key, user_key.as_encoded().as_slice()) {
                    return Ok(None);
                }
            }

            self.statistics.write.processed += 1;
            let write = WriteRef::parse(self.write_cursor.value(&mut self.statistics.write))?;

            match write.write_type {
                WriteType::Put => {
                    if self.omit_value {
                        return Ok(Some(vec![]));
                    }
                    match write.short_value {
                        Some(value) => {
                            // Value is carried in `write`.
                            return Ok(Some(value.to_vec()));
                        }
                        None => {
                            let start_ts = write.start_ts;
                            return Ok(Some(self.load_data_from_default_cf(start_ts, user_key)?));
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
        // TODO: Not necessary to receive a `Write`.
        self.statistics.data.get += 1;
        // TODO: We can avoid this clone.
        let value = self
            .snapshot
            .get_cf(CF_DEFAULT, &user_key.clone().append_ts(write_start_ts))?;

        if let Some(value) = value {
            self.statistics.data.processed += 1;
            Ok(value)
        } else {
            Err(default_not_found_error(
                user_key.to_raw()?,
                "load_data_from_default_cf",
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use engine_rocks::RocksSyncSnapshot;
    use kvproto::kvrpcpb::{Context, IsolationLevel};

    use crate::storage::mvcc::tests::*;
    use crate::storage::SHORT_VALUE_MAX_LEN;
    use crate::storage::{CFStatistics, Engine, Key, RocksEngine, TestEngineBuilder};

    fn new_multi_point_getter<E: Engine>(engine: &E, ts: TimeStamp) -> PointGetter<E::Snap> {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        PointGetterBuilder::new(snapshot, ts)
            .isolation_level(IsolationLevel::Si)
            .build()
            .unwrap()
    }

    fn new_single_point_getter<E: Engine>(engine: &E, ts: TimeStamp) -> PointGetter<E::Snap> {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        PointGetterBuilder::new(snapshot, ts)
            .isolation_level(IsolationLevel::Si)
            .multi(false)
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

    fn must_get_none<S: Snapshot>(point_getter: &mut PointGetter<S>, key: &[u8]) {
        assert!(point_getter.get(&Key::from_raw(key)).unwrap().is_none());
    }

    fn must_get_err<S: Snapshot>(point_getter: &mut PointGetter<S>, key: &[u8]) {
        assert!(point_getter.get(&Key::from_raw(key)).is_err());
    }

    fn assert_seek_next_prev(stat: &CFStatistics, seek: usize, next: usize, prev: usize) {
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
    fn test_multi_basic_1() {
        let engine = new_sample_engine();

        let mut getter = new_multi_point_getter(&engine, 200.into());

        // Get a deleted key
        must_get_none(&mut getter, b"foo1");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        // Get again
        must_get_none(&mut getter, b"foo1");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);

        // Get a key that exists
        must_get_value(&mut getter, b"foo2", b"foo2v");
        let s = getter.take_statistics();
        // We have to check every version
        assert_seek_next_prev(&s.write, 1, 40, 0);
        // Get again
        must_get_value(&mut getter, b"foo2", b"foo2v");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 40, 0);

        // Get a smaller key
        must_get_none(&mut getter, b"foo1");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);

        // Get a key that does not exist
        must_get_none(&mut getter, b"z");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);

        // Get a key that exists
        must_get_value(&mut getter, b"zz", b"zzv");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        // Get again
        must_get_value(&mut getter, b"zz", b"zzv");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
    }

    /// Some ts larger than get ts
    #[test]
    fn test_multi_basic_2() {
        let engine = new_sample_engine();

        let mut getter = new_multi_point_getter(&engine, 5.into());

        must_get_value(&mut getter, b"bar", b"barv");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);

        must_get_value(&mut getter, b"bar", b"barv");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);

        must_get_none(&mut getter, b"bo");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);

        must_get_none(&mut getter, b"box");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);

        must_get_value(&mut getter, b"foo1", b"foo1");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);

        must_get_none(&mut getter, b"zz");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);

        must_get_value(&mut getter, b"foo1", b"foo1");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);

        must_get_value(&mut getter, b"bar", b"barv");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
    }

    /// All ts larger than get ts
    #[test]
    fn test_multi_basic_3() {
        let engine = new_sample_engine();

        let mut getter = new_multi_point_getter(&engine, 2.into());

        must_get_none(&mut getter, b"foo1");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);

        must_get_none(&mut getter, b"non_exist");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);

        must_get_none(&mut getter, b"foo1");
        must_get_none(&mut getter, b"foo0");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 2, 0, 0);
    }

    /// There are some locks in the Lock CF.
    #[test]
    fn test_multi_locked() {
        let engine = new_sample_engine_2();

        let mut getter = new_multi_point_getter(&engine, 1.into());
        must_get_none(&mut getter, b"a");
        must_get_none(&mut getter, b"bar");
        must_get_none(&mut getter, b"foo1");
        must_get_none(&mut getter, b"foo2");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 3, 0, 0);

        let mut getter = new_multi_point_getter(&engine, 3.into());
        must_get_none(&mut getter, b"a");
        must_get_value(&mut getter, b"bar", b"barv");
        must_get_value(&mut getter, b"bar", b"barv");
        must_get_value(&mut getter, b"foo1", b"foo1v");
        must_get_value(&mut getter, b"foo1", b"foo1v");
        must_get_none(&mut getter, b"foo2");
        must_get_none(&mut getter, b"foo2");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 6, 0, 0);

        let mut getter = new_multi_point_getter(&engine, 4.into());
        must_get_none(&mut getter, b"a");
        must_get_err(&mut getter, b"bar");
        must_get_err(&mut getter, b"bar");
        must_get_value(&mut getter, b"foo1", b"foo1v");
        must_get_err(&mut getter, b"foo2");
        must_get_none(&mut getter, b"zz");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 3, 0, 0);
    }

    /// Single Point Getter can only get once.
    #[test]
    fn test_single_basic() {
        let engine = new_sample_engine_2();

        let mut getter = new_single_point_getter(&engine, 1.into());
        must_get_none(&mut getter, b"foo1");

        let mut getter = new_single_point_getter(&engine, 3.into());
        must_get_value(&mut getter, b"bar", b"barv");
        must_get_none(&mut getter, b"bar");
        must_get_none(&mut getter, b"foo1");

        let mut getter = new_single_point_getter(&engine, 3.into());
        must_get_value(&mut getter, b"foo1", b"foo1v");
        must_get_none(&mut getter, b"foo2");

        let mut getter = new_single_point_getter(&engine, 3.into());
        must_get_none(&mut getter, b"foo2");
        must_get_none(&mut getter, b"foo2");

        let mut getter = new_single_point_getter(&engine, 4.into());
        must_get_err(&mut getter, b"bar");
        must_get_none(&mut getter, b"bar");
        must_get_none(&mut getter, b"a");
        must_get_none(&mut getter, b"foo1");

        let mut getter = new_single_point_getter(&engine, 4.into());
        must_get_value(&mut getter, b"foo1", b"foo1v");
        must_get_none(&mut getter, b"foo1");
    }

    #[test]
    fn test_omit_value() {
        let engine = new_sample_engine_2();

        let snapshot = engine.snapshot(&Context::new()).unwrap();

        let mut getter = PointGetterBuilder::new(snapshot.clone(), 4.into())
            .isolation_level(IsolationLevel::Si)
            .omit_value(true)
            .build()
            .unwrap();
        must_get_err(&mut getter, b"bar");
        must_get_key(&mut getter, b"foo1");
        must_get_err(&mut getter, b"foo2");
        must_get_none(&mut getter, b"foo3");

        fn new_omit_value_single_point_getter(
            snapshot: RocksSyncSnapshot,
            ts: TimeStamp,
        ) -> PointGetter<RocksSyncSnapshot> {
            PointGetterBuilder::new(snapshot, ts)
                .isolation_level(IsolationLevel::Si)
                .omit_value(true)
                .multi(false)
                .build()
                .unwrap()
        }

        let mut getter = new_omit_value_single_point_getter(snapshot.clone(), 4.into());
        must_get_err(&mut getter, b"bar");
        must_get_none(&mut getter, b"bar");

        let mut getter = new_omit_value_single_point_getter(snapshot.clone(), 4.into());
        must_get_key(&mut getter, b"foo1");
        must_get_none(&mut getter, b"foo1");

        let mut getter = new_omit_value_single_point_getter(snapshot.clone(), 4.into());
        must_get_none(&mut getter, b"foo3");
        must_get_none(&mut getter, b"foo3");
    }

    #[test]
    fn test_get_latest_value() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let (key, val) = (b"foo", b"bar");
        must_prewrite_put(&engine, key, val, key, 10);
        must_commit(&engine, key, 10, 20);

        let mut getter = new_single_point_getter(&engine, TimeStamp::max());
        must_get_value(&mut getter, key, val);

        // Ignore the primary lock if read with max ts.
        must_prewrite_delete(&engine, key, key, 30);
        let mut getter = new_single_point_getter(&engine, TimeStamp::max());
        must_get_value(&mut getter, key, val);
        must_rollback(&engine, key, 30);

        // Should not ignore the secondary lock even though reading the latest version
        must_prewrite_delete(&engine, key, b"bar", 40);
        let mut getter = new_single_point_getter(&engine, TimeStamp::max());
        must_get_err(&mut getter, key);
        must_rollback(&engine, key, 40);

        // Should get the latest committed value if there is a primary lock with a ts less than
        // the latest Write's commit_ts.
        //
        // write.start_ts(10) < primary_lock.start_ts(15) < write.commit_ts(20)
        must_acquire_pessimistic_lock(&engine, key, key, 15, 50);
        must_pessimistic_prewrite_delete(&engine, key, key, 15, 50, true);
        let mut getter = new_single_point_getter(&engine, TimeStamp::max());
        must_get_value(&mut getter, key, val);
    }

    #[test]
    fn test_get_bypass_locks() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let (key, val) = (b"foo", b"bar");
        must_prewrite_put(&engine, key, val, key, 10);
        must_commit(&engine, key, 10, 20);

        must_prewrite_delete(&engine, key, key, 30);

        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut getter = PointGetterBuilder::new(snapshot, 60.into())
            .isolation_level(IsolationLevel::Si)
            .bypass_locks(TsSet::from_u64s(vec![30, 40, 50]))
            .build()
            .unwrap();
        must_get_value(&mut getter, key, val);

        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut getter = PointGetterBuilder::new(snapshot, 60.into())
            .isolation_level(IsolationLevel::Si)
            .bypass_locks(TsSet::from_u64s(vec![31, 29]))
            .build()
            .unwrap();
        must_get_err(&mut getter, key);
    }
}
