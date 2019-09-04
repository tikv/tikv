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

use kvproto::kvrpcpb::IsolationLevel;

use storage::mvcc::write::{Write, WriteType};
use storage::mvcc::{default_not_found_error, Lock, Result};
use storage::{Cursor, CursorBuilder, Key, ScanMode, Snapshot, Statistics, Value, CF_LOCK};
use storage::{CF_DEFAULT, CF_WRITE};

use super::util::CheckLockResult;

/// `PointGetter` factory.
pub struct PointGetterBuilder<S: Snapshot> {
    snapshot: S,
    multi: bool,
    fill_cache: bool,
    omit_value: bool,
    isolation_level: IsolationLevel,
    ts: u64,
}

impl<S: Snapshot> PointGetterBuilder<S> {
    /// Initialize a new `PointGetterBuilder`.
    pub fn new(snapshot: S, ts: u64) -> Self {
        Self {
            snapshot,
            multi: true,
            fill_cache: true,
            omit_value: false,
            isolation_level: IsolationLevel::SI,
            ts,
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
    /// Defaults to `IsolationLevel::SI`.
    #[inline]
    pub fn isolation_level(mut self, isolation_level: IsolationLevel) -> Self {
        self.isolation_level = isolation_level;
        self
    }

    /// Build `PointGetter` from the current configuration.
    pub fn build(self) -> Result<PointGetter<S>> {
        // If we only want to get single value, we can use prefix seek.
        let write_cursor = CursorBuilder::new(&self.snapshot, CF_WRITE)
            .fill_cache(self.fill_cache)
            .prefix_seek(!self.multi)
            .build()?;

        Ok(PointGetter {
            snapshot: self.snapshot,
            multi: self.multi,
            fill_cache: self.fill_cache,
            omit_value: self.omit_value,
            isolation_level: self.isolation_level,
            ts: self.ts,

            statistics: Statistics::default(),

            write_cursor,
            write_cursor_drained: true,
            lock_cursor: None,
            lock_cursor_drained: false,
            default_cursor: None,

            drained: false,
        })
    }
}

/// This struct can be used to get the value of user keys. Internally, rollbacks are ignored and
/// smaller version will be tried. If the isolation level is SI, locks will be checked first.
///
/// Use `PointGetterBuilder` to build `PointGetter`.
pub struct PointGetter<S: Snapshot> {
    snapshot: S,
    multi: bool,
    fill_cache: bool,
    omit_value: bool,
    isolation_level: IsolationLevel,
    ts: u64,

    statistics: Statistics,

    write_cursor: Cursor<S::Iter>,
    write_cursor_drained: bool,
    /// Lock cursor and default cursor will be built only when necessary.
    lock_cursor: Option<Cursor<S::Iter>>,
    lock_cursor_drained: bool,
    default_cursor: Option<Cursor<S::Iter>>,

    drained: bool,
}

impl<S: Snapshot> PointGetter<S> {
    /// Take out and reset the statistics collected so far.
    #[inline]
    pub fn take_statistics(&mut self) -> Statistics {
        ::std::mem::replace(&mut self.statistics, Statistics::default())
    }

    /// Get the value of a user key.
    ///
    /// If `multi == false`, this function must be called only once. Future calls return nothing.
    /// If `multi == true`, keys must be given in non-descending order. Calls with smaller keys
    /// return nothing.
    pub fn get(&mut self, user_key: &Key) -> Result<Option<Value>> {
        if !self.multi {
            // Protect from calling `get()` multiple times when `multi == false`.
            if self.drained {
                return Ok(None);
            } else {
                self.drained = true;
            }
        }

        println!("get() {:?}", user_key);
        let mut ts = self.ts;

        match self.isolation_level {
            IsolationLevel::SI => {
                // Check for locks that signal concurrent writes in SI.
                match self.load_and_check_lock(user_key, ts)? {
                    CheckLockResult::NotLocked => {}
                    CheckLockResult::Locked(e) => return Err(e),
                    CheckLockResult::Ignored(new_ts) => ts = new_ts,
                }
            }
            IsolationLevel::RC => {}
        }

        self.load_data(user_key, ts)
    }

    /// Get a lock of a user key in the lock CF. If lock exists, it will be checked to
    /// see whether it conflicts with the given `ts`. If there is no conflict or no lock,
    /// the safe `ts` will be returned.
    #[inline]
    fn load_and_check_lock(&mut self, user_key: &Key, ts: u64) -> Result<CheckLockResult> {
        println!("load_and_check_lock() {:?}, {}", user_key, ts);
        if self.multi {
            self.load_and_check_lock_multi_get(user_key, ts)
        } else {
            self.load_and_check_lock_single_get(user_key, ts)
        }
    }

    /// If only one `get()` will be called, we can use `snapshot.get_cf()` to get lock directly.
    fn load_and_check_lock_single_get(
        &mut self,
        user_key: &Key,
        ts: u64,
    ) -> Result<CheckLockResult> {
        self.statistics.lock.get += 1;
        let lock_value = self.snapshot.get_cf(CF_LOCK, user_key)?;

        if let Some(ref lock_value) = lock_value {
            self.statistics.lock.processed += 1;
            let lock = Lock::parse(lock_value)?;
            super::util::check_lock(user_key, ts, &lock)
        } else {
            Ok(CheckLockResult::NotLocked)
        }
    }

    /// If multiple `get()` will be called, we need to use cursor to read the lock.
    fn load_and_check_lock_multi_get(
        &mut self,
        user_key: &Key,
        ts: u64,
    ) -> Result<CheckLockResult> {
        self.ensure_lock_cursor()?;
        let lock_cursor = self.lock_cursor.as_mut().unwrap();
        if !self.lock_cursor_drained {
            println!("load_and_check_lock_multi_get() -- exit by lock_cursor_drained");
            return Ok(CheckLockResult::NotLocked);
        }
        if !lock_cursor.near_seek(user_key, &mut self.statistics.lock)? {
            // If we seek and get nothing, `lock_cursor` becomes invalid. So next time calling
            // `near_seek` will result in cursor jump back if the given key is smaller than the
            // current key. To keep cursor move in forward direction constantly, let's mark this
            // state. Additionally this protects us from https://github.com/tikv/tikv/issues/3378.
            self.lock_cursor_drained = false;
            return Ok(CheckLockResult::NotLocked);
        }
        if lock_cursor.key(&mut self.statistics.lock) == user_key.as_encoded().as_slice() {
            self.statistics.lock.processed += 1;
            let lock_value = lock_cursor.value(&mut self.statistics.lock);
            let lock = Lock::parse(lock_value)?;
            super::util::check_lock(user_key, ts, &lock)
        } else {
            Ok(CheckLockResult::NotLocked)
        }
    }

    /// Creates the lock cursor if not created. This function will only be called when
    /// `multi == true`.
    fn ensure_lock_cursor(&mut self) -> Result<()> {
        if self.lock_cursor.is_some() {
            return Ok(());
        }
        // Keys will be given in non-descending order, so forward mode cursor is fine.
        let cursor = CursorBuilder::new(&self.snapshot, CF_LOCK)
            .fill_cache(self.fill_cache)
            .build()?;
        self.lock_cursor = Some(cursor);
        self.lock_cursor_drained = true;
        Ok(())
    }

    /// Creates the default cursor if not created. This function will only be called when
    /// `multi == true`.
    fn ensure_default_cursor(&mut self) -> Result<()> {
        if self.default_cursor.is_some() {
            return Ok(());
        }
        let cursor = CursorBuilder::new(&self.snapshot, CF_DEFAULT)
            .fill_cache(self.fill_cache)
            .build()?;
        self.default_cursor = Some(cursor);
        Ok(())
    }

    /// Load the value.
    ///
    /// First, a correct version info in the Write CF will be sought. Then, value will be loaded
    /// from Default CF if necessary.
    fn load_data(&mut self, user_key: &Key, ts: u64) -> Result<Option<Value>> {
        println!("load_data() {:?}, {}", user_key, ts);

        if !self.write_cursor_drained {
            println!("load_data() -- exit by write_cursor_drained");
            return Ok(None);
        }

        // Seek to `${user_key}_${ts}`.
        if !self
            .write_cursor
            .near_seek(&user_key.clone().append_ts(ts), &mut self.statistics.write)?
        {
            // If we seek to nothing, it means no write `key >= ${user_key}_${ts}`.
            // - If later we want to get a key >= current key, due to the above conclusion we can
            //   quit directly.
            // - If later we want to get a key < current key, we should prohibit this call.
            //   Returning nothing directly is safer than some undefined behaviour.
            // So in all scenarios we should not provide results in future calls when we enter this
            // branch.
            self.write_cursor_drained = false;
        }

        loop {
            if !self.write_cursor.valid()? {
                // Key space ended.
                println!("load_data() -- exit by write_cursor not valid");
                return Ok(None);
            }
            // We may seek to another key. In this case, it means we cannot find the specified key.
            {
                let cursor_key = self.write_cursor.key(&mut self.statistics.write);
                if !Key::is_user_key_eq(cursor_key, user_key.as_encoded().as_slice()) {
                    println!("load_data() -- exit by !user_key_eq");
                    return Ok(None);
                }
            }

            self.statistics.write.processed += 1;
            let write = Write::parse(self.write_cursor.value(&mut self.statistics.write))?;

            match write.write_type {
                WriteType::Put => {
                    println!("load_data() -- exit by found PUT");
                    return Ok(Some(self.load_data_by_write(write, user_key)?));
                }
                WriteType::Delete => {
                    println!("load_data() -- exit by found DELETE");
                    return Ok(None);
                }
                WriteType::Lock | WriteType::Rollback => {
                    // Continue iterate next `write`.
                }
            }

            self.write_cursor.next(&mut self.statistics.write);
        }
    }

    /// Load the value by the given `write`. If value is carried in `write`, it will be returned
    /// directly. Otherwise there will be a default CF look up.
    fn load_data_by_write(&mut self, write: Write, user_key: &Key) -> Result<Value> {
        if self.omit_value {
            return Ok(vec![]);
        }
        match write.short_value {
            Some(value) => {
                // Value is carried in `write`.
                Ok(value)
            }
            None => {
                if self.multi {
                    self.load_data_from_default_cf_multi_get(write, user_key)
                } else {
                    self.load_data_from_default_cf_single_get(write, user_key)
                }
            }
        }
    }

    /// Load the value from default CF. Use `snapshot.get_cf()` directly.
    fn load_data_from_default_cf_single_get(
        &mut self,
        write: Write,
        user_key: &Key,
    ) -> Result<Value> {
        // TODO: Not necessary to receive a `Write`.
        self.statistics.data.get += 1;
        let value = self
            .snapshot
            .get_cf(CF_DEFAULT, &user_key.clone().append_ts(write.start_ts))?;

        if let Some(value) = value {
            self.statistics.data.processed += 1;
            Ok(value)
        } else {
            Err(default_not_found_error(
                user_key.to_raw()?,
                write,
                "load_data_from_default_cf",
            ))
        }
    }

    /// Load the value from default CF. Use cursor to read value.
    fn load_data_from_default_cf_multi_get(
        &mut self,
        write: Write,
        user_key: &Key,
    ) -> Result<Value> {
        self.ensure_default_cursor()?;
        let value = super::util::near_load_data_by_write(
            &mut self.default_cursor.as_mut().unwrap(),
            user_key,
            write,
            &mut self.statistics,
        )?;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    // use rand::{thread_rng, Rng};

    use super::*;
    use storage::engine::{self, SEEK_BOUND, TEMP_DIR};
    use storage::mvcc::tests::*;
    use storage::{Engine, Key, RocksEngine};
    use storage::{ALL_CFS, SHORT_VALUE_MAX_LEN};

    use kvproto::kvrpcpb::{Context, IsolationLevel};

    /// Build a sample engine with the following data:
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
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();
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

    fn new_multi_point_getter<E: Engine>(engine: &E, ts: u64) -> PointGetter<E::Snap> {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        PointGetterBuilder::new(snapshot, ts)
            .isolation_level(IsolationLevel::SI)
            .build()
            .unwrap()
    }

    #[test]
    fn test_basic_1() {
        let engine = new_sample_engine();

        let mut point_getter = new_multi_point_getter(&engine, 200);

        // Get a deleted key
        assert!(point_getter.get(&Key::from_raw(b"foo1")).unwrap().is_none());
        let statistics = point_getter.take_statistics();
        assert_eq!(statistics.lock.seek, 1);
        assert_eq!(statistics.lock.next, 0);
        assert_eq!(statistics.lock.prev, 0);
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, 0);
        assert_eq!(statistics.write.prev, 0);
        assert_eq!(statistics.data.seek, 0);
        assert_eq!(statistics.data.next, 0);
        assert_eq!(statistics.data.prev, 0);
        // Get again
        assert!(point_getter.get(&Key::from_raw(b"foo1")).unwrap().is_none());
        let statistics = point_getter.take_statistics();
        assert_eq!(statistics.lock.seek, 0);
        assert_eq!(statistics.lock.next, 0);
        assert_eq!(statistics.lock.prev, 0);
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 0);
        assert_eq!(statistics.write.prev, 0);
        assert_eq!(statistics.data.seek, 0);
        assert_eq!(statistics.data.next, 0);
        assert_eq!(statistics.data.prev, 0);

        // Get a key that exists
        assert!(
            point_getter
                .get(&Key::from_raw(b"foo2"))
                .unwrap()
                .unwrap()
                .starts_with(b"foo2")
        );
        let statistics = point_getter.take_statistics();
        assert_eq!(statistics.lock.seek, 0);
        assert_eq!(statistics.lock.next, 0);
        assert_eq!(statistics.lock.prev, 0);
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 42); // We have to check every version in this case
        assert_eq!(statistics.write.prev, 0);
        assert_eq!(statistics.data.seek, 1);
        assert_eq!(statistics.data.next, 0);
        assert_eq!(statistics.data.prev, 0);
        // Get again
        assert!(
            point_getter
                .get(&Key::from_raw(b"foo2"))
                .unwrap()
                .unwrap()
                .starts_with(b"foo2")
        );
        let statistics = point_getter.take_statistics();
        assert_eq!(statistics.lock.seek, 0);
        assert_eq!(statistics.lock.next, 0);
        assert_eq!(statistics.lock.prev, 0);
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 0);
        assert_eq!(statistics.write.prev, 0);
        assert_eq!(statistics.data.seek, 0);
        assert_eq!(statistics.data.next, 0);
        assert_eq!(statistics.data.prev, 0);

        // Get a smaller key
        assert!(point_getter.get(&Key::from_raw(b"foo1")).unwrap().is_none());
        let statistics = point_getter.take_statistics();
        assert_eq!(statistics.lock.seek, 0);
        assert_eq!(statistics.lock.next, 0);
        assert_eq!(statistics.lock.prev, 0);
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 0);
        assert_eq!(statistics.write.prev, 0);
        assert_eq!(statistics.data.seek, 0);
        assert_eq!(statistics.data.next, 0);
        assert_eq!(statistics.data.prev, 0);

        // Get a key that does not exist
        assert!(point_getter.get(&Key::from_raw(b"z")).unwrap().is_none());
        let statistics = point_getter.take_statistics();
        println!("{:?}", statistics);
        assert_eq!(statistics.lock.seek, 0);
        assert_eq!(statistics.lock.next, 0);
        assert_eq!(statistics.lock.prev, 0);
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 2);
        assert_eq!(statistics.write.prev, 0);
        assert_eq!(statistics.data.seek, 0);
        assert_eq!(statistics.data.next, 0);
        assert_eq!(statistics.data.prev, 0);

        // Get a key that exists
        assert!(
            point_getter
                .get(&Key::from_raw(b"zz"))
                .unwrap()
                .unwrap()
                .starts_with(b"zz")
        );
        let statistics = point_getter.take_statistics();
        println!("{:?}", statistics);
        assert_eq!(statistics.lock.seek, 0);
        assert_eq!(statistics.lock.next, 0);
        assert_eq!(statistics.lock.prev, 0);
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 0);
        assert_eq!(statistics.write.prev, 0);
        assert_eq!(statistics.data.seek, 1);
        assert_eq!(statistics.data.next, 0);
        assert_eq!(statistics.data.prev, 0);
    }

    /// All data has a larger ts than get ts.
    #[test]
    fn test_skip_larger_ts() {
        let engine = new_sample_engine();

        let mut point_getter = new_multi_point_getter(&engine, 2);

        assert!(point_getter.get(&Key::from_raw(b"foo1")).unwrap().is_none());
        let statistics = point_getter.take_statistics();
        assert_eq!(statistics.lock.seek, 1);
        assert_eq!(statistics.lock.next, 0);
        assert_eq!(statistics.lock.prev, 0);
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, 0);
        assert_eq!(statistics.write.prev, 0);
        assert_eq!(statistics.data.seek, 0);
        assert_eq!(statistics.data.next, 0);
        assert_eq!(statistics.data.prev, 0);

        assert!(
            point_getter
                .get(&Key::from_raw(b"non_exist"))
                .unwrap()
                .is_none()
        );
        let statistics = point_getter.take_statistics();
        assert_eq!(statistics.lock.seek, 0);
        assert_eq!(statistics.lock.next, 0);
        assert_eq!(statistics.lock.prev, 0);
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, SEEK_BOUND as usize);
        assert_eq!(statistics.write.prev, 0);
        assert_eq!(statistics.data.seek, 0);
        assert_eq!(statistics.data.next, 0);
        assert_eq!(statistics.data.prev, 0);

        // Cursor never move back.
        assert!(point_getter.get(&Key::from_raw(b"foo1")).unwrap().is_none());
        let statistics = point_getter.take_statistics();
        println!("{:?}", statistics);
        assert!(point_getter.get(&Key::from_raw(b"foo0")).unwrap().is_none());
        let statistics = point_getter.take_statistics();
        println!("{:?}", statistics);
        assert_eq!(statistics.lock.seek, 0);
        assert_eq!(statistics.lock.next, 0);
        assert_eq!(statistics.lock.prev, 0);
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 0);
        assert_eq!(statistics.write.prev, 0);
        assert_eq!(statistics.data.seek, 0);
        assert_eq!(statistics.data.next, 0);
        assert_eq!(statistics.data.prev, 0);
    }

}

/*


        {
            let mut point_getter = new_point_getter(&engine, 1);
            assert!(
                point_getter
                    .get(&Key::from_raw(b"foo1"))
                    .unwrap()
                    .is_none()
            );
            let statistics = point_getter.take_statistics();
            assert_eq!(statistics.write.seek, 1);
            assert_eq!(statistics.write.next, 0);
            assert_eq!(statistics.write.flow_stats.read_keys, 0);
            assert_eq!(statistics.data.seek, 0);
            assert_eq!(statistics.data.next, 0);
            assert_eq!(statistics.data.flow_stats.read_keys, 0);

            let mut point_getter = new_point_getter(&engine, 5);
            assert!(
                point_getter
                    .get(&Key::from_raw(b"foo1"))
                    .unwrap()
                    .is_none()
            );
            let statistics = point_getter.take_statistics();
            assert_eq!(statistics.write.seek, 1);
            assert_eq!(statistics.write.next, 0);
            assert_eq!(statistics.write.flow_stats.read_keys, 0);
            assert_eq!(statistics.data.seek, 0);
            assert_eq!(statistics.data.next, 0);
            assert_eq!(statistics.data.flow_stats.read_keys, 0);
        }

        {
            let mut point_getter = new_point_getter(&engine);
            assert!(
                point_getter
                    .get(&Key::from_raw(b"foo1"), 2)
                    .unwrap()
                    .is_none()
            );
            let statistics = point_getter.take_statistics();
            assert_eq!(statistics.write.seek, 1);
            assert_eq!(statistics.write.next, 0);
            assert_eq!(statistics.write.flow_stats.read_keys, 0);
            assert_eq!(statistics.data.seek, 0);
            assert_eq!(statistics.data.next, 0);
            assert_eq!(statistics.data.flow_stats.read_keys, 0);

            assert_eq!(
                point_getter
                    .get(&Key::from_raw(b"foo1"), 3)
                    .unwrap()
                    .unwrap(),
                b"bar1_1".to_vec()
            );
            let statistics = point_getter.take_statistics();
            assert_eq!(statistics.write.seek, 1);
            assert_eq!(statistics.write.next, 0);
            assert_eq!(statistics.write.flow_stats.read_keys, 1);
            assert_eq!(statistics.data.seek, 0); // short value, no data lookup
            assert_eq!(statistics.data.next, 0);
            assert_eq!(statistics.data.flow_stats.read_keys, 0);

            assert_eq!(
                point_getter
                    .get(&Key::from_raw(b"foo1"), 3)
                    .unwrap()
                    .unwrap(),
                b"bar1_1".to_vec()
            );
            let statistics = point_getter.take_statistics();
            // current is foo1_3, seek for foo1_3, so no seek will be actually performed
            assert_eq!(statistics.write.seek, 0);
            assert_eq!(statistics.write.next, 0);
            assert_eq!(statistics.write.flow_stats.read_keys, 0);
            assert_eq!(statistics.data.seek, 0);
            assert_eq!(statistics.data.next, 0);
            assert_eq!(statistics.data.flow_stats.read_keys, 0);

            assert_eq!(
                point_getter
                    .get(&Key::from_raw(b"foo1"), 5)
                    .unwrap()
                    .unwrap(),
                b"bar1_1".to_vec()
            );
            let statistics = point_getter.take_statistics();
            // current is foo1_3, seek for foo1_5, so there will be a re-seek
            assert_eq!(statistics.write.seek, 1);
            assert_eq!(statistics.write.next, 0);
            assert_eq!(statistics.write.flow_stats.read_keys, 1);
            assert_eq!(statistics.data.seek, 0);
            assert_eq!(statistics.data.next, 0);
            assert_eq!(statistics.data.flow_stats.read_keys, 0);
        }
        let version2_value = "v".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        must_prewrite_put(&engine, b"foo1", &version2_value, b"foo1", 4);
        {
            let mut point_getter = new_point_getter(&engine);
            assert!(
                point_getter
                    .get(&Key::from_raw(b"foo1"), 2)
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                point_getter
                    .get(&Key::from_raw(b"foo1"), 3)
                    .unwrap()
                    .unwrap(),
                b"bar1_1".to_vec()
            );

            point_getter.take_statistics();
            assert_eq!(
                point_getter
                    .get(&Key::from_raw(b"foo1"), 4)
                    .unwrap()
                    .unwrap(),
                b"bar1_1".to_vec()
            );
            let statistics = point_getter.take_statistics();
            assert_eq!(statistics.write.seek, 1);
            assert_eq!(statistics.write.next, 0);
            assert_eq!(statistics.write.flow_stats.read_keys, 1);
            assert_eq!(statistics.data.seek, 0);
            assert_eq!(statistics.data.next, 0);
            assert_eq!(statistics.data.flow_stats.read_keys, 0);

            assert_eq!(
                point_getter
                    .get(&Key::from_raw(b"foo1"), 5)
                    .unwrap()
                    .unwrap(),
                b"bar1_1".to_vec()
            );
            let statistics = point_getter.take_statistics();
            assert_eq!(statistics.write.seek, 1);
            assert_eq!(statistics.write.next, 0);
            assert_eq!(statistics.write.flow_stats.read_keys, 1);
            assert_eq!(statistics.data.seek, 0);
            assert_eq!(statistics.data.next, 0);
            assert_eq!(statistics.data.flow_stats.read_keys, 0);

            assert_eq!(
                point_getter
                    .get(&Key::from_raw(b"foo1"), 3)
                    .unwrap()
                    .unwrap(),
                b"bar1_1".to_vec()
            );
            let statistics = point_getter.take_statistics();
            // current is foo1_3, seek for foo1_3, so no seek will be actually performed
            assert_eq!(statistics.write.seek, 0);
            assert_eq!(statistics.write.next, 0);
            assert_eq!(statistics.write.flow_stats.read_keys, 0);
            assert_eq!(statistics.data.seek, 0);
            assert_eq!(statistics.data.next, 0);
            assert_eq!(statistics.data.flow_stats.read_keys, 0);
        }
        must_commit(&engine, b"foo1", 4, 5);
        {
            let mut point_getter = new_point_getter(&engine);
            assert!(
                point_getter
                    .get(&Key::from_raw(b"foo1"), 2)
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                point_getter
                    .get(&Key::from_raw(b"foo1"), 3)
                    .unwrap()
                    .unwrap(),
                b"bar1_1".to_vec()
            );

            point_getter.take_statistics();
            assert_eq!(
                point_getter
                    .get(&Key::from_raw(b"foo1"), 4)
                    .unwrap()
                    .unwrap(),
                b"bar1_1".to_vec()
            );
            let statistics = point_getter.take_statistics();
            assert_eq!(statistics.write.seek, 1);
            assert_eq!(statistics.write.next, 0);
            assert_eq!(statistics.write.flow_stats.read_keys, 1);
            assert_eq!(statistics.data.seek, 0);
            assert_eq!(statistics.data.next, 0);
            assert_eq!(statistics.data.flow_stats.read_keys, 0);

            assert_eq!(
                point_getter
                    .get(&Key::from_raw(b"foo1"), 5)
                    .unwrap()
                    .unwrap(),
                version2_value,
            );
            let statistics = point_getter.take_statistics();
            assert_eq!(statistics.write.seek, 1);
            assert_eq!(statistics.write.next, 0);
            assert_eq!(statistics.write.flow_stats.read_keys, 1);
            assert_eq!(statistics.data.seek, 1); // long value, 1 seek
            assert_eq!(statistics.data.next, 0);
            assert_eq!(statistics.data.flow_stats.read_keys, 1);

            assert_eq!(
                point_getter
                    .get(&Key::from_raw(b"foo1"), 7)
                    .unwrap()
                    .unwrap(),
                version2_value,
            );
            let statistics = point_getter.take_statistics();
            assert_eq!(statistics.write.seek, 1);
            assert_eq!(statistics.write.next, 0);
            assert_eq!(statistics.write.flow_stats.read_keys, 1);
            assert_eq!(statistics.data.seek, 0); // current value, no seek
            assert_eq!(statistics.data.next, 0);
            assert_eq!(statistics.data.flow_stats.read_keys, 0);

            assert!(
                point_getter
                    .get(&Key::from_raw(b"foo1"), 2)
                    .unwrap()
                    .is_none()
            );
            let statistics = point_getter.take_statistics();
            // hit upper bound, so no actual seek is performed
            assert_eq!(statistics.write.seek, 0);
            assert_eq!(statistics.write.next, 0);
            assert_eq!(statistics.write.flow_stats.read_keys, 0);
            assert_eq!(statistics.data.seek, 0);
            assert_eq!(statistics.data.next, 0);
            assert_eq!(statistics.data.flow_stats.read_keys, 0);

            assert_eq!(
                point_getter
                    .get(&Key::from_raw(b"foo1"), 3)
                    .unwrap()
                    .unwrap(),
                b"bar1_1".to_vec()
            );
            let statistics = point_getter.take_statistics();
            // current is foo1_5, near_seek foo1_3 will be next * 1
            assert_eq!(statistics.write.seek, 0);
            assert_eq!(statistics.write.next, 1);
            assert_eq!(statistics.write.flow_stats.read_keys, 1);
            assert_eq!(statistics.data.seek, 0);
            assert_eq!(statistics.data.next, 0);
            assert_eq!(statistics.data.flow_stats.read_keys, 0);

            assert!(
                point_getter
                    .get(&Key::from_raw(b"foo1"), 1)
                    .unwrap()
                    .is_none()
            );
            let statistics = point_getter.take_statistics();
            // hit upper bound, so no actual seek is performed
            assert_eq!(statistics.write.seek, 0);
            assert_eq!(statistics.write.next, 0);
            assert_eq!(statistics.write.flow_stats.read_keys, 0);
            assert_eq!(statistics.data.seek, 0);
            assert_eq!(statistics.data.next, 0);
            assert_eq!(statistics.data.flow_stats.read_keys, 0);
        }
        must_prewrite_delete(&engine, b"foo1", b"foo1", 6);
        {
            let mut point_getter = new_point_getter(&engine);
            assert!(
                point_getter
                    .get(&Key::from_raw(b"foo1"), 2)
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                point_getter
                    .get(&Key::from_raw(b"foo1"), 3)
                    .unwrap()
                    .unwrap(),
                b"bar1_1".to_vec()
            );
            assert_eq!(
                point_getter
                    .get(&Key::from_raw(b"foo1"), 4)
                    .unwrap()
                    .unwrap(),
                b"bar1_1".to_vec()
            );
            assert_eq!(
                point_getter
                    .get(&Key::from_raw(b"foo1"), 5)
                    .unwrap()
                    .unwrap(),
                version2_value,
            );
            assert_eq!(
                point_getter
                    .get(&Key::from_raw(b"foo1"), 7)
                    .unwrap()
                    .unwrap(),
                version2_value,
            );

            point_getter.take_statistics();
            assert_eq!(
                point_getter
                    .get(&Key::from_raw(b"foo1"), 3)
                    .unwrap()
                    .unwrap(),
                b"bar1_1".to_vec()
            );
            let statistics = point_getter.take_statistics();
            // current is foo1_5, near_seek foo1_3 will be next * 1
            assert_eq!(statistics.write.seek, 0);
            assert_eq!(statistics.write.next, 1);
            assert_eq!(statistics.write.flow_stats.read_keys, 1);
            assert_eq!(statistics.data.seek, 0);
            assert_eq!(statistics.data.next, 0);
            assert_eq!(statistics.data.flow_stats.read_keys, 0);
        }
        must_commit(&engine, b"foo1", 6, 7);
        {
            let mut point_getter = new_point_getter(&engine);
            assert!(
                point_getter
                    .get(&Key::from_raw(b"foo1"), 2)
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                point_getter
                    .get(&Key::from_raw(b"foo1"), 3)
                    .unwrap()
                    .unwrap(),
                b"bar1_1".to_vec()
            );
            assert_eq!(
                point_getter
                    .get(&Key::from_raw(b"foo1"), 4)
                    .unwrap()
                    .unwrap(),
                b"bar1_1".to_vec()
            );
            assert_eq!(
                point_getter
                    .get(&Key::from_raw(b"foo1"), 5)
                    .unwrap()
                    .unwrap(),
                version2_value,
            );
            assert_eq!(
                point_getter
                    .get(&Key::from_raw(b"foo1"), 6)
                    .unwrap()
                    .unwrap(),
                version2_value,
            );
            assert!(
                point_getter
                    .get(&Key::from_raw(b"foo1"), 7)
                    .unwrap()
                    .is_none()
            );

            point_getter.take_statistics();
            assert_eq!(
                point_getter
                    .get(&Key::from_raw(b"foo1"), 3)
                    .unwrap()
                    .unwrap(),
                b"bar1_1".to_vec()
            );
            let statistics = point_getter.take_statistics();
            // current is foo1_7, near_seek foo1_3 will be next * 2
            assert_eq!(statistics.write.seek, 0);
            assert_eq!(statistics.write.next, 2);
            assert_eq!(statistics.write.flow_stats.read_keys, 2);
            assert_eq!(statistics.data.seek, 0);
            assert_eq!(statistics.data.next, 0);
            assert_eq!(statistics.data.flow_stats.read_keys, 0);
        }
    }
//
//    /// Get key multiple times. Each time getting a different key unordered.
//    /// These keys only have 1 version.
//    #[test]
//    fn test_multi_true_multi_key_single_version() {
//        const UPPER_KEY: u64 = 100;
//        const LOWER_KEY: u64 = 10;
//
//        // case 1. commit ts == key + 1
//        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();
//        for i in LOWER_KEY..UPPER_KEY {
//            let ts = i + 1;
//            must_prewrite_put(&engine, &[i as u8], &[i as u8], &[i as u8], ts);
//            must_commit(&engine, &[i as u8], ts, ts);
//        }
//        let snapshot = engine.snapshot(&Context::new()).unwrap();
//        let mut point_getter = PointGetterBuilder::new(snapshot).build().unwrap();
//        let mut test_cases = vec![];
//        for user_key in 0..UPPER_KEY + 10 {
//            for ts in 0..UPPER_KEY + 10 {
//                test_cases.push((user_key, ts));
//            }
//        }
//        // shuffle test cases
//        thread_rng().shuffle(test_cases.as_mut_slice());
//        for (user_key, ts) in test_cases {
//            let val = point_getter
//                .get(&Key::from_raw(&[user_key as u8]), ts)
//                .unwrap();
//            if user_key >= UPPER_KEY || user_key < LOWER_KEY {
//                // key not exist
//                assert!(val.is_none());
//            } else if ts < user_key + 1 {
//                // commit ts == key + 1, so if specified ts < key + 1, we should get nothing.
//                assert!(val.is_none());
//            } else {
//                // in other cases, we should get something.
//                assert_eq!(val.unwrap(), vec![user_key as u8]);
//            }
//        }
//
//        // case 2. commit ts = UPPER_KEY - key
//        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();
//        for i in LOWER_KEY..UPPER_KEY {
//            let ts = UPPER_KEY - i;
//            must_prewrite_put(&engine, &[i as u8], &[i as u8], &[i as u8], ts);
//            must_commit(&engine, &[i as u8], ts, ts);
//        }
//        let snapshot = engine.snapshot(&Context::new()).unwrap();
//        let mut point_getter = PointGetterBuilder::new(snapshot).build().unwrap();
//        let mut test_cases = vec![];
//        for user_key in 0..UPPER_KEY + 10 {
//            for ts in 0..UPPER_KEY + 10 {
//                test_cases.push((user_key, ts));
//            }
//        }
//        thread_rng().shuffle(test_cases.as_mut_slice());
//        for (user_key, ts) in test_cases {
//            let val = point_getter
//                .get(&Key::from_raw(&[user_key as u8]), ts)
//                .unwrap();
//            if user_key >= UPPER_KEY || user_key < LOWER_KEY {
//                // key not exist
//                assert!(val.is_none());
//            } else if ts < UPPER_KEY - user_key {
//                // commit ts == UPPER_KEY - key, so if specified ts < UPPER_KEY - key, we should
//                // get nothing.
//                assert!(val.is_none());
//            } else {
//                // in other cases, we should get something.
//                assert_eq!(val.unwrap(), vec![user_key as u8]);
//            }
//        }
//
//        // case 3. commit ts = constant 55.
//        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();
//        for i in LOWER_KEY..UPPER_KEY {
//            let ts = LOWER_KEY + 10;
//            must_prewrite_put(&engine, &[i as u8], &[i as u8], &[i as u8], ts);
//            must_commit(&engine, &[i as u8], ts, ts);
//        }
//        let snapshot = engine.snapshot(&Context::new()).unwrap();
//        let mut point_getter = PointGetterBuilder::new(snapshot).build().unwrap();
//        let mut test_cases = vec![];
//        for user_key in 0..UPPER_KEY + 10 {
//            for ts in 0..UPPER_KEY + 10 {
//                test_cases.push((user_key, ts));
//            }
//        }
//        thread_rng().shuffle(test_cases.as_mut_slice());
//        for (user_key, ts) in test_cases {
//            let val = point_getter
//                .get(&Key::from_raw(&[user_key as u8]), ts)
//                .unwrap();
//            if user_key >= UPPER_KEY || user_key < LOWER_KEY {
//                // key not exist
//                assert!(val.is_none());
//            } else if ts < LOWER_KEY + 10 {
//                // if specified ts < commit ts LOWER_KEY + 10, we should get nothing.
//                assert!(val.is_none());
//            } else {
//                // in other cases, we should get something.
//                assert_eq!(val.unwrap(), vec![user_key as u8]);
//            }
//        }
//    }
//
//    /// Get key multiple times. Each time getting a different key unordered.
//    /// These keys have multiple versions.
//    #[test]
//    fn test_multi_true_multi_key_multi_version() {
//        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();
//
//        // Generate SEEK_BOUND + 1 Put for key [10], long value.
//        let k = &[10 as u8];
//        for ts in 0..SEEK_BOUND + 1 {
//            let mut value = "v".repeat(SHORT_VALUE_MAX_LEN).into_bytes();
//            value.push(ts as u8);
//            must_prewrite_put(&engine, k, &value, k, ts);
//            must_commit(&engine, k, ts, ts);
//        }
//
//        // Generate SEEK_BOUND + 2 Put for key [20].
//        let k = &[20 as u8];
//        for ts in 0..SEEK_BOUND + 2 {
//            must_prewrite_put(&engine, k, &[ts as u8], k, ts);
//            must_commit(&engine, k, ts, ts);
//        }
//
//        // Generate SEEK_BOUND / 2 Put for key [30].
//        let k = &[30 as u8];
//        for ts in 0..SEEK_BOUND / 2 {
//            must_prewrite_put(&engine, k, &[ts as u8], k, ts);
//            must_commit(&engine, k, ts, ts);
//        }
//
//        // Generate SEEK_BOUND / 2 + 1 Put for key [40].
//        let k = &[40 as u8];
//        for ts in 0..SEEK_BOUND / 2 + 1 {
//            must_prewrite_put(&engine, k, &[ts as u8], k, ts);
//            must_commit(&engine, k, ts, ts);
//        }
//
//        // Generate SEEK_BOUND / 2 + 1 Put + SEEK_BOUND / 2 Rollback for key [50], long value.
//        let k = &[50 as u8];
//        for ts in 0..SEEK_BOUND / 2 + 1 {
//            let mut value = "v".repeat(SHORT_VALUE_MAX_LEN).into_bytes();
//            value.push(ts as u8);
//            must_prewrite_put(&engine, k, &value, k, ts);
//            must_commit(&engine, k, ts, ts);
//        }
//        for ts in SEEK_BOUND / 2 + 1..SEEK_BOUND + 1 {
//            must_rollback(&engine, k, ts);
//        }
//
//        // Generate SEEK_BOUND / 2 Put + 1 Delete + SEEK_BOUND / 2 Rollback for key [60].
//        let k = &[60 as u8];
//        for ts in 0..SEEK_BOUND / 2 {
//            must_prewrite_put(&engine, k, &[ts as u8], k, ts);
//            must_commit(&engine, k, ts, ts);
//        }
//        must_prewrite_delete(&engine, k, k, SEEK_BOUND / 2);
//        must_commit(&engine, k, SEEK_BOUND / 2, SEEK_BOUND / 2);
//        for ts in SEEK_BOUND / 2 + 1..SEEK_BOUND + 1 {
//            must_rollback(&engine, k, ts);
//        }
//
//        // Generate 1 Delete for key [70].
//        let k = &[70 as u8];
//        must_prewrite_delete(&engine, k, k, 1);
//        must_commit(&engine, k, 1, 2);
//
//        // Generate SEEK_BOUND + 1 Put for key [80].
//        let k = &[80 as u8];
//        for ts in 0..SEEK_BOUND / 2 + 1 {
//            must_prewrite_put(&engine, k, &[ts as u8], k, ts);
//            must_commit(&engine, k, ts, ts);
//        }
//
//        let snapshot = engine.snapshot(&Context::new()).unwrap();
//        let mut point_getter = PointGetterBuilder::new(snapshot).build().unwrap();
//
//        // First operation, 1 seek.
//        assert_eq!(
//            point_getter
//                .get(&Key::from_raw(&[55u8]), SEEK_BOUND)
//                .unwrap(),
//            None
//        );
//        let statistics = point_getter.take_statistics();
//        assert_eq!(statistics.write.seek, 1);
//        assert_eq!(statistics.write.next, 0);
//        assert_eq!(statistics.data.seek, 0);
//        assert_eq!(statistics.data.next, 0);
//
//        // Currently pointing at key [60_N], getting key [55_N], use seek.
//        // N denotes to SEEK_BOUND.
//        assert_eq!(
//            point_getter
//                .get(&Key::from_raw(&[55u8]), SEEK_BOUND)
//                .unwrap(),
//            None
//        );
//        let statistics = point_getter.take_statistics();
//        assert_eq!(statistics.write.seek, 1);
//        assert_eq!(statistics.write.next, 0);
//        assert_eq!(statistics.data.seek, 0);
//        assert_eq!(statistics.data.next, 0);
//
//        // Currently pointing at key [60_N], getting key [60_N], use next to skip
//        // SEEK_BOUND / 2 rollbacks.
//        assert_eq!(
//            point_getter
//                .get(&Key::from_raw(&[60u8]), SEEK_BOUND)
//                .unwrap(),
//            None
//        );
//        let statistics = point_getter.take_statistics();
//        assert_eq!(statistics.write.seek, 0);
//        assert_eq!(statistics.write.next, (SEEK_BOUND / 2) as usize);
//        assert_eq!(statistics.data.seek, 0);
//        assert_eq!(statistics.data.next, 0);
//
//        // Currently pointing at key [60_N/2], getting key [60_N], use seek to reach key [60_N]
//        // and next to skip SEEK_BOUND / 2 rollbacks.
//        assert_eq!(
//            point_getter
//                .get(&Key::from_raw(&[60u8]), SEEK_BOUND)
//                .unwrap(),
//            None
//        );
//        let statistics = point_getter.take_statistics();
//        assert_eq!(statistics.write.seek, 1);
//        assert_eq!(statistics.write.next, (SEEK_BOUND / 2) as usize);
//        assert_eq!(statistics.data.seek, 0);
//        assert_eq!(statistics.data.next, 0);
//
//        // Currently pointing at key [60_N/2], getting [80_1].
//        // There will be SEEK_BOUND next() + 1 seek.
//        assert_eq!(
//            point_getter
//                .get(&Key::from_raw(&[80u8]), 1)
//                .unwrap()
//                .unwrap(),
//            vec![1]
//        );
//        let statistics = point_getter.take_statistics();
//        assert_eq!(statistics.write.seek, 1);
//        assert_eq!(statistics.write.next, SEEK_BOUND as usize);
//        assert_eq!(statistics.data.seek, 0);
//        assert_eq!(statistics.data.next, 0);
//
//        // Currently pointing at key [80_1], getting [70_3], use seek.
//        assert_eq!(point_getter.get(&Key::from_raw(&[70u8]), 3).unwrap(), None);
//        let statistics = point_getter.take_statistics();
//        assert_eq!(statistics.write.seek, 1);
//        assert_eq!(statistics.write.next, 0);
//        assert_eq!(statistics.data.seek, 0);
//        assert_eq!(statistics.data.next, 0);
//
//        // Currently pointing at key [70_2], getting [0_N], use seek.
//        assert_eq!(
//            point_getter
//                .get(&Key::from_raw(&[0u8]), SEEK_BOUND)
//                .unwrap(),
//            None
//        );
//        let statistics = point_getter.take_statistics();
//        assert_eq!(statistics.write.seek, 1);
//        assert_eq!(statistics.write.next, 0);
//        assert_eq!(statistics.data.seek, 0);
//        assert_eq!(statistics.data.next, 0);
//
//        // Currently pointing at key [10_N], getting [10_N], no op.
//        let mut value = "v".repeat(SHORT_VALUE_MAX_LEN).into_bytes();
//        value.push(SEEK_BOUND as u8);
//        assert_eq!(
//            point_getter
//                .get(&Key::from_raw(&[10u8]), SEEK_BOUND)
//                .unwrap()
//                .unwrap(),
//            value
//        );
//        let statistics = point_getter.take_statistics();
//        assert_eq!(statistics.write.seek, 0);
//        assert_eq!(statistics.write.next, 0);
//        assert_eq!(statistics.data.seek, 1); // long value, first seek
//        assert_eq!(statistics.data.next, 0);
//        assert_eq!(statistics.data.flow_stats.read_keys, 1);
//
//        // Currently pointing at key [10_N], getting [10_N/2], N/2 next.
//        let mut value = "v".repeat(SHORT_VALUE_MAX_LEN).into_bytes();
//        value.push((SEEK_BOUND / 2) as u8);
//        assert_eq!(
//            point_getter
//                .get(&Key::from_raw(&[10u8]), SEEK_BOUND / 2)
//                .unwrap()
//                .unwrap(),
//            value
//        );
//        let statistics = point_getter.take_statistics();
//        assert_eq!(statistics.write.seek, 0);
//        assert_eq!(statistics.write.next, (SEEK_BOUND / 2) as usize);
//        assert_eq!(statistics.data.seek, 0);
//        assert_eq!(statistics.data.next, (SEEK_BOUND / 2) as usize);
//        assert_eq!(
//            statistics.data.flow_stats.read_keys,
//            (SEEK_BOUND / 2) as usize
//        );
//
//        // Currently pointing at key [10_N/2], getting [20_N/2+3].
//        // Use (SEEK_BOUND / 2 + 1) next() to skip versions of key [10], reaches [20_N+1].
//        // Use (SEEK_BOUND / 2 - 1) next() to reach [20_N/2+2].
//        assert_eq!(
//            point_getter
//                .get(&Key::from_raw(&[20u8]), SEEK_BOUND / 2 + 2)
//                .unwrap()
//                .unwrap(),
//            vec![(SEEK_BOUND / 2 + 2) as u8]
//        );
//        let statistics = point_getter.take_statistics();
//        assert_eq!(statistics.write.seek, 0);
//        assert_eq!(statistics.write.next, SEEK_BOUND as usize);
//        assert_eq!(statistics.data.seek, 0);
//        assert_eq!(statistics.data.next, 0);
//
//        // Currently pointing at key [20_N/2+2]. Use (SEEK_BOUND / 2 + 2) next() to reach [20_0].
//        assert_eq!(
//            point_getter
//                .get(&Key::from_raw(&[20u8]), 0)
//                .unwrap()
//                .unwrap(),
//            vec![0u8]
//        );
//        let statistics = point_getter.take_statistics();
//        assert_eq!(statistics.write.seek, 0);
//        assert_eq!(statistics.write.next, (SEEK_BOUND / 2 + 2) as usize);
//        assert_eq!(statistics.data.seek, 0);
//        assert_eq!(statistics.data.next, 0);
//
//        // Currently pointing at key [20_0]. Use SEEK_BOUND next() + 1 seek to reach [50_N].
//        // Then, use SEEK_BOUND / 2 next() to skip rollbacks.
//        let mut value = "v".repeat(SHORT_VALUE_MAX_LEN).into_bytes();
//        value.push((SEEK_BOUND / 2) as u8);
//        assert_eq!(
//            point_getter
//                .get(&Key::from_raw(&[50u8]), SEEK_BOUND)
//                .unwrap()
//                .unwrap(),
//            value,
//        );
//        let statistics = point_getter.take_statistics();
//        assert_eq!(statistics.write.seek, 1);
//        assert_eq!(
//            statistics.write.next,
//            (SEEK_BOUND + SEEK_BOUND / 2) as usize
//        );
//        assert_eq!(statistics.data.seek, 0);
//        // default cursor pointing at [10_N/2], using N/2 next() to point to [10_0] and 1 next()
//        // to point to [50_N/2].
//        assert_eq!(statistics.data.next, (SEEK_BOUND / 2 + 1) as usize);
//        assert_eq!(
//            statistics.data.flow_stats.read_keys,
//            (SEEK_BOUND / 2 + 1) as usize
//        );
//    }
//
//    /// Get key single time. Should get `None` for future attempts.
//    #[test]
//    fn test_multi_false() {
//        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();
//
//        must_prewrite_put(&engine, &[10u8], &[10u8], &[10u8], 5);
//        must_commit(&engine, &[10u8], 5, 5);
//
//        must_prewrite_put(&engine, &[20u8], &[20u8], &[20u8], 5);
//        must_commit(&engine, &[20u8], 5, 5);
//
//        must_prewrite_put(&engine, &[30u8], &[30u8], &[30u8], 5);
//        must_commit(&engine, &[30u8], 5, 5);
//
//        let snapshot = engine.snapshot(&Context::new()).unwrap();
//        let mut point_getter = PointGetterBuilder::new(snapshot)
//            .multi(false)
//            .build()
//            .unwrap();
//
//        // First operation, 1 seek.
//        assert_eq!(
//            point_getter
//                .get(&Key::from_raw(&[20u8]), 5)
//                .unwrap()
//                .unwrap(),
//            vec![20u8]
//        );
//        let statistics = point_getter.take_statistics();
//        assert_eq!(statistics.write.seek, 1);
//        assert_eq!(statistics.write.next, 0);
//
//        // Next (same key): no operation
//        assert_eq!(point_getter.get(&Key::from_raw(&[20u8]), 5).unwrap(), None,);
//        let statistics = point_getter.take_statistics();
//        assert_eq!(statistics.write.seek, 0);
//        assert_eq!(statistics.write.next, 0);
//
//        // Next (different key): no operation
//        assert_eq!(point_getter.get(&Key::from_raw(&[10u8]), 5).unwrap(), None,);
//        let statistics = point_getter.take_statistics();
//        assert_eq!(statistics.write.seek, 0);
//        assert_eq!(statistics.write.next, 0);
//
//        // Next (different key): no operation
//        assert_eq!(point_getter.get(&Key::from_raw(&[30u8]), 5).unwrap(), None,);
//        let statistics = point_getter.take_statistics();
//        assert_eq!(statistics.write.seek, 0);
//        assert_eq!(statistics.write.next, 0);
//
//        // Next (no key): no operation
//        assert_eq!(point_getter.get(&Key::from_raw(&[55u8]), 5).unwrap(), None,);
//        let statistics = point_getter.take_statistics();
//        assert_eq!(statistics.write.seek, 0);
//        assert_eq!(statistics.write.next, 0);
//
//        // Create another point getter. First retrieve a key not exist, got `None`.
//        let snapshot = engine.snapshot(&Context::new()).unwrap();
//        let mut point_getter = PointGetterBuilder::new(snapshot)
//            .multi(false)
//            .build()
//            .unwrap();
//        assert_eq!(point_getter.get(&Key::from_raw(&[50u8]), 5).unwrap(), None,);
//        let statistics = point_getter.take_statistics();
//        assert_eq!(statistics.write.seek, 1);
//        assert_eq!(statistics.write.next, 0);
//
//        // Future requests are `None` as well, even if key exists.
//        assert_eq!(point_getter.get(&Key::from_raw(&[10u8]), 5).unwrap(), None,);
//        let statistics = point_getter.take_statistics();
//        assert_eq!(statistics.write.seek, 0);
//        assert_eq!(statistics.write.next, 0);
//
//        // Future requests are `None` as well, even if key exists.
//        assert_eq!(point_getter.get(&Key::from_raw(&[30u8]), 5).unwrap(), None,);
//        let statistics = point_getter.take_statistics();
//        assert_eq!(statistics.write.seek, 0);
//        assert_eq!(statistics.write.next, 0);
//    }
//
//    /// Omit value == true && value is short value.
//    #[test]
//    fn test_omit_short_value() {
//        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();
//
//        must_prewrite_put(&engine, &[10u8], b"1", &[10u8], 5);
//        must_commit(&engine, &[10u8], 5, 5);
//
//        must_prewrite_put(&engine, &[20u8], b"2", &[20u8], 5);
//        must_commit(&engine, &[20u8], 5, 5);
//
//        must_prewrite_put(&engine, &[30u8], b"3", &[30u8], 5);
//        must_commit(&engine, &[30u8], 5, 5);
//
//        let snapshot = engine.snapshot(&Context::new()).unwrap();
//        let mut point_getter = PointGetterBuilder::new(snapshot)
//            .omit_value(true)
//            .build()
//            .unwrap();
//
//        assert!(
//            point_getter
//                .get(&Key::from_raw(&[0u8]), 5)
//                .unwrap()
//                .is_none(),
//        );
//        assert!(
//            point_getter
//                .get(&Key::from_raw(&[20u8]), 5)
//                .unwrap()
//                .unwrap()
//                .is_empty(),
//        );
//        assert!(
//            point_getter
//                .get(&Key::from_raw(&[35u8]), 5)
//                .unwrap()
//                .is_none(),
//        );
//        assert!(
//            point_getter
//                .get(&Key::from_raw(&[30u8]), 5)
//                .unwrap()
//                .unwrap()
//                .is_empty(),
//        );
//        assert!(
//            point_getter
//                .get(&Key::from_raw(&[30u8]), 1)
//                .unwrap()
//                .is_none(),
//        );
//        assert!(
//            point_getter
//                .get(&Key::from_raw(&[10u8]), 5)
//                .unwrap()
//                .unwrap()
//                .is_empty(),
//        );
//    }
//
//    /// Omit value == true && value is long value stored in default CF.
//    #[test]
//    fn test_omit_long_value() {
//        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();
//
//        must_prewrite_put(
//            &engine,
//            &[10u8],
//            "1".repeat(SHORT_VALUE_MAX_LEN + 1).as_bytes(),
//            &[10u8],
//            5,
//        );
//        must_commit(&engine, &[10u8], 5, 5);
//
//        must_prewrite_put(&engine, &[20u8], b"2", &[20u8], 5);
//        must_commit(&engine, &[20u8], 5, 5);
//
//        must_prewrite_put(
//            &engine,
//            &[30u8],
//            "3".repeat(SHORT_VALUE_MAX_LEN + 1).as_bytes(),
//            &[30u8],
//            5,
//        );
//        must_commit(&engine, &[30u8], 5, 5);
//
//        let snapshot = engine.snapshot(&Context::new()).unwrap();
//        let mut point_getter = PointGetterBuilder::new(snapshot)
//            .omit_value(true)
//            .build()
//            .unwrap();
//
//        assert!(
//            point_getter
//                .get(&Key::from_raw(&[0u8]), 5)
//                .unwrap()
//                .is_none(),
//        );
//        assert!(
//            point_getter
//                .get(&Key::from_raw(&[20u8]), 5)
//                .unwrap()
//                .unwrap()
//                .is_empty(),
//        );
//        assert!(
//            point_getter
//                .get(&Key::from_raw(&[35u8]), 5)
//                .unwrap()
//                .is_none(),
//        );
//        assert!(
//            point_getter
//                .get(&Key::from_raw(&[30u8]), 5)
//                .unwrap()
//                .unwrap()
//                .is_empty(),
//        );
//        assert!(
//            point_getter
//                .get(&Key::from_raw(&[30u8]), 1)
//                .unwrap()
//                .is_none(),
//        );
//        assert!(
//            point_getter
//                .get(&Key::from_raw(&[10u8]), 5)
//                .unwrap()
//                .unwrap()
//                .is_empty(),
//        );
//    }
//
//    /// Locks are checked in SI.
//    #[test]
//    fn test_isolation_si() {
//        fn new_point_getter<E: Engine>(engine: &E) -> PointGetter<E::Snap> {
//            let snapshot = engine.snapshot(&Context::new()).unwrap();
//            PointGetterBuilder::new(snapshot)
//                .isolation_level(IsolationLevel::SI)
//                .build()
//                .unwrap()
//        }
//
//        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();
//
//        // key [30_5] is committed.
//        must_prewrite_put(&engine, &[30u8], &[30u8], &[30u8], 5);
//        must_commit(&engine, &[30u8], 5, 5);
//
//        // key [10_5] not prewritten.
//        assert!(
//            new_point_getter(&engine)
//                .get(&Key::from_raw(&[10u8]), 5)
//                .unwrap()
//                .is_none()
//        );
//        // key [10_5] prewritten but not committed.
//        must_prewrite_put(&engine, &[10u8], &[10u8], &[10u8], 5);
//        {
//            // we should get error for key [10_5] even if previously we get a value [30_5].
//            let mut point_getter = new_point_getter(&engine);
//            assert_eq!(
//                point_getter
//                    .get(&Key::from_raw(&[30u8]), 5)
//                    .unwrap()
//                    .unwrap(),
//                &[30u8]
//            );
//            assert!(point_getter.get(&Key::from_raw(&[10u8]), 5).is_err());
//            // if we get the value with a smaller ts, we should success.
//            assert!(
//                point_getter
//                    .get(&Key::from_raw(&[30u8]), 4)
//                    .unwrap()
//                    .is_none()
//            );
//            assert!(
//                point_getter
//                    .get(&Key::from_raw(&[10u8]), 3)
//                    .unwrap()
//                    .is_none()
//            );
//            // if we directly get the value, we should fail as well.
//            assert!(
//                new_point_getter(&engine)
//                    .get(&Key::from_raw(&[10u8]), 5)
//                    .is_err()
//            );
//            // if we directly get the value with a smaller ts, we should success.
//            assert!(
//                new_point_getter(&engine)
//                    .get(&Key::from_raw(&[10u8]), 4)
//                    .unwrap()
//                    .is_none()
//            );
//        }
//        // key [10_5] committed.
//        must_commit(&engine, &[10u8], 5, 5);
//        assert_eq!(
//            new_point_getter(&engine)
//                .get(&Key::from_raw(&[10u8]), 8)
//                .unwrap()
//                .unwrap(),
//            &[10u8]
//        );
//        // key [10_10] prewritten but not committed
//        must_prewrite_put(&engine, &[10u8], &[100u8], &[10u8], 10);
//        {
//            // we should be able to read key [10_1] ~ [10_9]
//            let mut point_getter = new_point_getter(&engine);
//            assert!(
//                point_getter
//                    .get(&Key::from_raw(&[10u8]), 1)
//                    .unwrap()
//                    .is_none()
//            );
//            assert_eq!(
//                point_getter
//                    .get(&Key::from_raw(&[10u8]), 5)
//                    .unwrap()
//                    .unwrap(),
//                &[10u8]
//            );
//            assert_eq!(
//                point_getter
//                    .get(&Key::from_raw(&[10u8]), 9)
//                    .unwrap()
//                    .unwrap(),
//                &[10u8]
//            );
//            // we should not be able to read key >= version 10
//            assert!(point_getter.get(&Key::from_raw(&[10u8]), 10).is_err());
//            assert!(point_getter.get(&Key::from_raw(&[10u8]), 11).is_err());
//        }
//        must_commit(&engine, &[10u8], 10, 20);
//        {
//            // we should be able to read key [10] for any version.
//            let mut point_getter = new_point_getter(&engine);
//            assert_eq!(
//                point_getter
//                    .get(&Key::from_raw(&[10u8]), 9)
//                    .unwrap()
//                    .unwrap(),
//                &[10u8]
//            );
//            assert_eq!(
//                point_getter
//                    .get(&Key::from_raw(&[10u8]), 15)
//                    .unwrap()
//                    .unwrap(),
//                &[10u8]
//            );
//            assert!(
//                point_getter
//                    .get(&Key::from_raw(&[10u8]), 1)
//                    .unwrap()
//                    .is_none()
//            );
//            assert_eq!(
//                point_getter
//                    .get(&Key::from_raw(&[10u8]), 5)
//                    .unwrap()
//                    .unwrap(),
//                &[10u8]
//            );
//            assert_eq!(
//                point_getter
//                    .get(&Key::from_raw(&[10u8]), 20)
//                    .unwrap()
//                    .unwrap(),
//                &[100u8]
//            );
//        }
//        // key [20_5] not prewritten
//        assert!(
//            new_point_getter(&engine)
//                .get(&Key::from_raw(&[20u8]), 5)
//                .unwrap()
//                .is_none()
//        );
//        // key [20_5] prewritten but not committed
//        must_prewrite_put(&engine, &[20u8], &[20u8], &[20u8], 5);
//        {
//            // even if there was an error previously, we are able to read another committed key.
//            let mut point_getter = new_point_getter(&engine);
//            assert!(point_getter.get(&Key::from_raw(&[20u8]), 5).is_err());
//            assert_eq!(
//                point_getter
//                    .get(&Key::from_raw(&[10u8]), 5)
//                    .unwrap()
//                    .unwrap(),
//                &[10u8]
//            );
//            // or course we should be able to directly read the committed key.
//            assert_eq!(
//                new_point_getter(&engine)
//                    .get(&Key::from_raw(&[10u8]), 5)
//                    .unwrap()
//                    .unwrap(),
//                &[10u8]
//            );
//        }
//        // key [40_5] prewritten but not committed
//        must_prewrite_put(&engine, &[40u8], &[40u8], &[40u8], 5);
//        {
//            let mut point_getter = new_point_getter(&engine);
//            assert!(point_getter.get(&Key::from_raw(&[20u8]), 5).is_err());
//            assert!(point_getter.get(&Key::from_raw(&[40u8]), 5).is_err());
//        }
//        // key [20_5] committed
//        must_commit(&engine, &[20u8], 5, 5);
//        assert_eq!(
//            new_point_getter(&engine)
//                .get(&Key::from_raw(&[20u8]), 5)
//                .unwrap()
//                .unwrap(),
//            &[20u8]
//        );
//    }
//
//    /// Locks are ignored in RC. Use a same pattern as `test_isolation_si`, however in these
//    /// error cases we should be able to read last value.
//    #[test]
//    fn test_isolation_rc() {
//        fn new_point_getter<E: Engine>(engine: &E) -> PointGetter<E::Snap> {
//            let snapshot = engine.snapshot(&Context::new()).unwrap();
//            PointGetterBuilder::new(snapshot)
//                .isolation_level(IsolationLevel::RC)
//                .build()
//                .unwrap()
//        }
//
//        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();
//
//        // key [30_5] is committed.
//        must_prewrite_put(&engine, &[30u8], &[30u8], &[30u8], 5);
//        must_commit(&engine, &[30u8], 5, 5);
//
//        // key [10_5] not prewritten.
//        assert!(
//            new_point_getter(&engine)
//                .get(&Key::from_raw(&[10u8]), 5)
//                .unwrap()
//                .is_none()
//        );
//        // key [10_5] prewritten but not committed.
//        must_prewrite_put(&engine, &[10u8], &[10u8], &[10u8], 5);
//        {
//            let mut point_getter = new_point_getter(&engine);
//            assert_eq!(
//                point_getter
//                    .get(&Key::from_raw(&[30u8]), 5)
//                    .unwrap()
//                    .unwrap(),
//                &[30u8]
//            );
//            assert!(
//                point_getter
//                    .get(&Key::from_raw(&[10u8]), 5)
//                    .unwrap()
//                    .is_none()
//            );
//            assert!(
//                point_getter
//                    .get(&Key::from_raw(&[30u8]), 4)
//                    .unwrap()
//                    .is_none()
//            );
//            assert!(
//                point_getter
//                    .get(&Key::from_raw(&[10u8]), 3)
//                    .unwrap()
//                    .is_none()
//            );
//            assert!(
//                new_point_getter(&engine)
//                    .get(&Key::from_raw(&[10u8]), 5)
//                    .unwrap()
//                    .is_none()
//            );
//            // if we directly get the value with a smaller ts, we should success.
//            assert!(
//                new_point_getter(&engine)
//                    .get(&Key::from_raw(&[10u8]), 4)
//                    .unwrap()
//                    .is_none()
//            );
//        }
//        // key [10_5] committed.
//        must_commit(&engine, &[10u8], 5, 5);
//        assert_eq!(
//            new_point_getter(&engine)
//                .get(&Key::from_raw(&[10u8]), 8)
//                .unwrap()
//                .unwrap(),
//            &[10u8]
//        );
//        // key [10_10] prewritten but not committed
//        must_prewrite_put(&engine, &[10u8], &[100u8], &[10u8], 10);
//        {
//            let mut point_getter = new_point_getter(&engine);
//            assert!(
//                point_getter
//                    .get(&Key::from_raw(&[10u8]), 1)
//                    .unwrap()
//                    .is_none()
//            );
//            assert_eq!(
//                point_getter
//                    .get(&Key::from_raw(&[10u8]), 5)
//                    .unwrap()
//                    .unwrap(),
//                &[10u8]
//            );
//            assert_eq!(
//                point_getter
//                    .get(&Key::from_raw(&[10u8]), 9)
//                    .unwrap()
//                    .unwrap(),
//                &[10u8]
//            );
//            assert_eq!(
//                point_getter
//                    .get(&Key::from_raw(&[10u8]), 10)
//                    .unwrap()
//                    .unwrap(),
//                &[10u8]
//            );
//            assert_eq!(
//                point_getter
//                    .get(&Key::from_raw(&[10u8]), 11)
//                    .unwrap()
//                    .unwrap(),
//                &[10u8]
//            );
//        }
//        must_commit(&engine, &[10u8], 10, 20);
//        {
//            let mut point_getter = new_point_getter(&engine);
//            assert_eq!(
//                point_getter
//                    .get(&Key::from_raw(&[10u8]), 9)
//                    .unwrap()
//                    .unwrap(),
//                &[10u8]
//            );
//            assert_eq!(
//                point_getter
//                    .get(&Key::from_raw(&[10u8]), 15)
//                    .unwrap()
//                    .unwrap(),
//                &[10u8]
//            );
//            assert!(
//                point_getter
//                    .get(&Key::from_raw(&[10u8]), 1)
//                    .unwrap()
//                    .is_none()
//            );
//            assert_eq!(
//                point_getter
//                    .get(&Key::from_raw(&[10u8]), 5)
//                    .unwrap()
//                    .unwrap(),
//                &[10u8]
//            );
//            assert_eq!(
//                point_getter
//                    .get(&Key::from_raw(&[10u8]), 20)
//                    .unwrap()
//                    .unwrap(),
//                &[100u8]
//            );
//        }
//        // key [20_5] not prewritten
//        assert!(
//            new_point_getter(&engine)
//                .get(&Key::from_raw(&[20u8]), 5)
//                .unwrap()
//                .is_none()
//        );
//        // key [20_5] prewritten but not committed
//        must_prewrite_put(&engine, &[20u8], &[20u8], &[20u8], 5);
//        {
//            let mut point_getter = new_point_getter(&engine);
//            assert!(
//                point_getter
//                    .get(&Key::from_raw(&[20u8]), 5)
//                    .unwrap()
//                    .is_none()
//            );
//            assert_eq!(
//                point_getter
//                    .get(&Key::from_raw(&[10u8]), 5)
//                    .unwrap()
//                    .unwrap(),
//                &[10u8]
//            );
//            assert_eq!(
//                new_point_getter(&engine)
//                    .get(&Key::from_raw(&[10u8]), 5)
//                    .unwrap()
//                    .unwrap(),
//                &[10u8]
//            );
//        }
//        // key [40_5] prewritten but not committed
//        must_prewrite_put(&engine, &[40u8], &[40u8], &[40u8], 5);
//        {
//            let mut point_getter = new_point_getter(&engine);
//            assert!(
//                point_getter
//                    .get(&Key::from_raw(&[20u8]), 5)
//                    .unwrap()
//                    .is_none()
//            );
//            assert!(
//                point_getter
//                    .get(&Key::from_raw(&[40u8]), 5)
//                    .unwrap()
//                    .is_none()
//            );
//        }
//        // key [20_5] committed
//        must_commit(&engine, &[20u8], 5, 5);
//        assert_eq!(
//            new_point_getter(&engine)
//                .get(&Key::from_raw(&[20u8]), 5)
//                .unwrap()
//                .unwrap(),
//            &[20u8]
//        );
//    }
}
*/
