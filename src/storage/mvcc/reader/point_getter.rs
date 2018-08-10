// Copyright 2018 PingCAP, Inc.
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
use storage::mvcc::Result;
use storage::{Cursor, CursorBuilder, Key, Snapshot, Statistics, Value};
use storage::{CF_DEFAULT, CF_WRITE};

/// `PointGetter` factory.
pub struct PointGetterBuilder<S: Snapshot> {
    snapshot: S,
    multi: bool,
    fill_cache: bool,
    omit_value: bool,
    isolation_level: IsolationLevel,
}

impl<S: Snapshot> PointGetterBuilder<S> {
    /// Initialize a new `PointGetterBuilder`.
    pub fn new(snapshot: S) -> Self {
        Self {
            snapshot,
            multi: true,
            fill_cache: true,
            omit_value: false,
            isolation_level: IsolationLevel::SI,
        }
    }

    /// Set whether multiple values will be retrieved. If `multi` is `false`, only single value
    /// will be retrieved. Prefix filter will be used thus it will be faster.
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

            statistics: Statistics::default(),

            read_once: false,

            write_cursor,
            default_cursor: None,
        })
    }
}

/// This struct can be used to get the value of a user key. Internally, rollbacks are ignored and
/// smaller version will be tried. If the isolation level is SI, locks will be checked first.
///
/// If `multi` is `false`, prefix filter will be used so that you can only call `read_next`
/// once. Subsequent calls will yield `None`.
///
/// If `multi` is `true`, the instance can be re-used to get multiple keys. However it will
/// be optimal if these keys are get in ascending order and are relatively close to each other.
///
/// Use `PointGetterBuilder` to build `PointGetter`.
pub struct PointGetter<S: Snapshot> {
    snapshot: S,
    multi: bool,
    fill_cache: bool,
    omit_value: bool,
    isolation_level: IsolationLevel,

    statistics: Statistics,

    /// Whether there is already a `read_next` call. When `multi == false`, we use this field
    /// to check that `read_next` is called only once.
    read_once: bool,

    write_cursor: Cursor<S::Iter>,

    /// Default cursor is optional since when value is short we don't need to look up in
    /// the default CF.
    default_cursor: Option<Cursor<S::Iter>>,
}

impl<S: Snapshot> PointGetter<S> {
    /// Take out and reset the statistics collected so far.
    #[inline]
    pub fn take_statistics(&mut self) -> Statistics {
        ::std::mem::replace(&mut self.statistics, Statistics::default())
    }

    /// Get the value of a user key. See `PointGetter` for details.
    pub fn read_next(&mut self, user_key: &Key, mut ts: u64) -> Result<Option<Value>> {
        // Attempting to read multiple values when `multi == false`, directly returns `None`.
        if !self.multi && self.read_once {
            return Ok(None);
        }

        self.read_once = true;

        if self.isolation_level == IsolationLevel::SI {
            // Check for locks that signal concurrent writes in SI.
            ts = super::util::load_and_check_lock(
                &self.snapshot,
                user_key,
                ts,
                &mut self.statistics,
            )?;
        }

        // First seek to `${user_key}_${ts}`. In multi-read mode, the keys may given out of
        // order, so we allow re-seek.
        self.write_cursor.near_seek(
            &user_key.clone().append_ts(ts),
            true,
            &mut self.statistics.write,
        )?;

        loop {
            if !self.write_cursor.valid() {
                // Key space ended.
                return Ok(None);
            }
            // We may move forward / seek to another key. In this case, the scan ends.
            {
                let cursor_key = self.write_cursor.key(&mut self.statistics.write);
                if !Key::is_user_key_eq(cursor_key, user_key.encoded().as_slice()) {
                    // Meet another key.
                    return Ok(None);
                }
            }

            let write = Write::parse(self.write_cursor.value(&mut self.statistics.write))?;
            self.statistics.write.processed += 1;

            match write.write_type {
                WriteType::Put => return Ok(Some(self.load_data_by_write(write, user_key)?)),
                WriteType::Delete => return Ok(None),
                WriteType::Lock | WriteType::Rollback => {
                    // Continue iterate next `write`.
                }
            }

            self.write_cursor.next(&mut self.statistics.write);
        }
    }

    /// Load the value by the given `write`. If value is carried in `write`, it will be returned
    /// directly. Otherwise there will be a default CF look up.
    #[inline]
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
                // Value is in the default CF.
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
    }

    /// Create the default cursor if it doesn't exist.
    #[inline]
    fn ensure_default_cursor(&mut self) -> Result<()> {
        if self.default_cursor.is_some() {
            return Ok(());
        }
        let cursor = CursorBuilder::new(&self.snapshot, CF_DEFAULT)
            .fill_cache(self.fill_cache)
            .prefix_seek(!self.multi)
            .build()?;
        self.default_cursor = Some(cursor);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rand::{thread_rng, Rng};

    use super::*;
    use storage::engine::{self, SEEK_BOUND, TEMP_DIR};
    use storage::mvcc::tests::*;
    use storage::ALL_CFS;
    use storage::{Engine, Key};

    use kvproto::kvrpcpb::{Context, IsolationLevel};

    /// Get key multiple times. Each time getting the same key.
    #[test]
    fn test_multi_true_one_key() {
        fn new_point_getter<E: Engine>(engine: &E) -> PointGetter<E::Snap> {
            let snapshot = engine.snapshot(&Context::new()).unwrap();
            PointGetterBuilder::new(snapshot)
                .isolation_level(IsolationLevel::RC)
                .build()
                .unwrap()
        }

        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();
        must_prewrite_put(&engine, b"foo1", b"bar1_1", b"foo1", 2);
        {
            let mut point_getter = new_point_getter(&engine);
            assert!(
                point_getter
                    .read_next(&Key::from_raw(b"foo1"), 2)
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

            assert!(
                point_getter
                    .read_next(&Key::from_raw(b"foo1"), 5)
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
        must_commit(&engine, b"foo1", 2, 3);
        {
            let mut point_getter = new_point_getter(&engine);
            assert!(
                point_getter
                    .read_next(&Key::from_raw(b"foo1"), 2)
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
                    .read_next(&Key::from_raw(b"foo1"), 3)
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
                    .read_next(&Key::from_raw(b"foo1"), 3)
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
                    .read_next(&Key::from_raw(b"foo1"), 5)
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
        must_prewrite_put(&engine, b"foo1", b"bar1_2", b"foo1", 4);
        {
            let mut point_getter = new_point_getter(&engine);
            assert!(
                point_getter
                    .read_next(&Key::from_raw(b"foo1"), 2)
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                point_getter
                    .read_next(&Key::from_raw(b"foo1"), 3)
                    .unwrap()
                    .unwrap(),
                b"bar1_1".to_vec()
            );

            point_getter.take_statistics();
            assert_eq!(
                point_getter
                    .read_next(&Key::from_raw(b"foo1"), 4)
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
                    .read_next(&Key::from_raw(b"foo1"), 5)
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
                    .read_next(&Key::from_raw(b"foo1"), 3)
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
                    .read_next(&Key::from_raw(b"foo1"), 2)
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                point_getter
                    .read_next(&Key::from_raw(b"foo1"), 3)
                    .unwrap()
                    .unwrap(),
                b"bar1_1".to_vec()
            );

            point_getter.take_statistics();
            assert_eq!(
                point_getter
                    .read_next(&Key::from_raw(b"foo1"), 4)
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
                    .read_next(&Key::from_raw(b"foo1"), 5)
                    .unwrap()
                    .unwrap(),
                b"bar1_2".to_vec()
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
                    .read_next(&Key::from_raw(b"foo1"), 7)
                    .unwrap()
                    .unwrap(),
                b"bar1_2".to_vec()
            );
            let statistics = point_getter.take_statistics();
            assert_eq!(statistics.write.seek, 1);
            assert_eq!(statistics.write.next, 0);
            assert_eq!(statistics.write.flow_stats.read_keys, 1);
            assert_eq!(statistics.data.seek, 0);
            assert_eq!(statistics.data.next, 0);
            assert_eq!(statistics.data.flow_stats.read_keys, 0);

            assert!(
                point_getter
                    .read_next(&Key::from_raw(b"foo1"), 2)
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
                    .read_next(&Key::from_raw(b"foo1"), 3)
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
                    .read_next(&Key::from_raw(b"foo1"), 1)
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
                    .read_next(&Key::from_raw(b"foo1"), 2)
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                point_getter
                    .read_next(&Key::from_raw(b"foo1"), 3)
                    .unwrap()
                    .unwrap(),
                b"bar1_1".to_vec()
            );
            assert_eq!(
                point_getter
                    .read_next(&Key::from_raw(b"foo1"), 4)
                    .unwrap()
                    .unwrap(),
                b"bar1_1".to_vec()
            );
            assert_eq!(
                point_getter
                    .read_next(&Key::from_raw(b"foo1"), 5)
                    .unwrap()
                    .unwrap(),
                b"bar1_2".to_vec()
            );
            assert_eq!(
                point_getter
                    .read_next(&Key::from_raw(b"foo1"), 7)
                    .unwrap()
                    .unwrap(),
                b"bar1_2".to_vec()
            );

            point_getter.take_statistics();
            assert_eq!(
                point_getter
                    .read_next(&Key::from_raw(b"foo1"), 3)
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
                    .read_next(&Key::from_raw(b"foo1"), 2)
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                point_getter
                    .read_next(&Key::from_raw(b"foo1"), 3)
                    .unwrap()
                    .unwrap(),
                b"bar1_1".to_vec()
            );
            assert_eq!(
                point_getter
                    .read_next(&Key::from_raw(b"foo1"), 4)
                    .unwrap()
                    .unwrap(),
                b"bar1_1".to_vec()
            );
            assert_eq!(
                point_getter
                    .read_next(&Key::from_raw(b"foo1"), 5)
                    .unwrap()
                    .unwrap(),
                b"bar1_2".to_vec()
            );
            assert_eq!(
                point_getter
                    .read_next(&Key::from_raw(b"foo1"), 6)
                    .unwrap()
                    .unwrap(),
                b"bar1_2".to_vec()
            );
            assert!(
                point_getter
                    .read_next(&Key::from_raw(b"foo1"), 7)
                    .unwrap()
                    .is_none()
            );

            point_getter.take_statistics();
            assert_eq!(
                point_getter
                    .read_next(&Key::from_raw(b"foo1"), 3)
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

    /// Get key multiple times. Each time getting a different key unordered.
    /// These keys only have 1 version.
    #[test]
    fn test_multi_true_multi_key_single_version() {
        const UPPER_KEY: u64 = 100;
        const LOWER_KEY: u64 = 10;

        // case 1. commit ts == key + 1
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();
        for i in LOWER_KEY..UPPER_KEY {
            let ts = i + 1;
            must_prewrite_put(&engine, &[i as u8], &[i as u8], &[i as u8], ts);
            must_commit(&engine, &[i as u8], ts, ts);
        }
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut point_getter = PointGetterBuilder::new(snapshot).build().unwrap();
        let mut test_cases = vec![];
        for user_key in 0..UPPER_KEY + 10 {
            for ts in 0..UPPER_KEY + 10 {
                test_cases.push((user_key, ts));
            }
        }
        // shuffle test cases
        thread_rng().shuffle(test_cases.as_mut_slice());
        for (user_key, ts) in test_cases {
            let val = point_getter
                .read_next(&Key::from_raw(&[user_key as u8]), ts)
                .unwrap();
            if user_key >= UPPER_KEY || user_key < LOWER_KEY {
                // key not exist
                assert!(val.is_none());
            } else if ts < user_key + 1 {
                // commit ts == key + 1, so if specified ts < key + 1, we should get nothing.
                assert!(val.is_none());
            } else {
                // in other cases, we should get something.
                assert_eq!(val.unwrap(), vec![user_key as u8]);
            }
        }

        // case 2. commit ts = UPPER_KEY - key
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();
        for i in LOWER_KEY..UPPER_KEY {
            let ts = UPPER_KEY - i;
            must_prewrite_put(&engine, &[i as u8], &[i as u8], &[i as u8], ts);
            must_commit(&engine, &[i as u8], ts, ts);
        }
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut point_getter = PointGetterBuilder::new(snapshot).build().unwrap();
        let mut test_cases = vec![];
        for user_key in 0..UPPER_KEY + 10 {
            for ts in 0..UPPER_KEY + 10 {
                test_cases.push((user_key, ts));
            }
        }
        thread_rng().shuffle(test_cases.as_mut_slice());
        for (user_key, ts) in test_cases {
            let val = point_getter
                .read_next(&Key::from_raw(&[user_key as u8]), ts)
                .unwrap();
            if user_key >= UPPER_KEY || user_key < LOWER_KEY {
                // key not exist
                assert!(val.is_none());
            } else if ts < UPPER_KEY - user_key {
                // commit ts == UPPER_KEY - key, so if specified ts < UPPER_KEY - key, we should
                // get nothing.
                assert!(val.is_none());
            } else {
                // in other cases, we should get something.
                assert_eq!(val.unwrap(), vec![user_key as u8]);
            }
        }

        // case 3. commit ts = constant 55.
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();
        for i in LOWER_KEY..UPPER_KEY {
            let ts = LOWER_KEY + 10;
            must_prewrite_put(&engine, &[i as u8], &[i as u8], &[i as u8], ts);
            must_commit(&engine, &[i as u8], ts, ts);
        }
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut point_getter = PointGetterBuilder::new(snapshot).build().unwrap();
        let mut test_cases = vec![];
        for user_key in 0..UPPER_KEY + 10 {
            for ts in 0..UPPER_KEY + 10 {
                test_cases.push((user_key, ts));
            }
        }
        thread_rng().shuffle(test_cases.as_mut_slice());
        for (user_key, ts) in test_cases {
            let val = point_getter
                .read_next(&Key::from_raw(&[user_key as u8]), ts)
                .unwrap();
            if user_key >= UPPER_KEY || user_key < LOWER_KEY {
                // key not exist
                assert!(val.is_none());
            } else if ts < LOWER_KEY + 10 {
                // if specified ts < commit ts LOWER_KEY + 10, we should get nothing.
                assert!(val.is_none());
            } else {
                // in other cases, we should get something.
                assert_eq!(val.unwrap(), vec![user_key as u8]);
            }
        }
    }

    /// Get key multiple times. Each time getting a different key unordered.
    /// These keys have multiple versions.
    #[test]
    fn test_multi_true_multi_key_multi_version() {
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();

        // Generate SEEK_BOUND + 1 Put for key [10].
        let k = &[10 as u8];
        for ts in 0..SEEK_BOUND + 1 {
            must_prewrite_put(&engine, k, &[ts as u8], k, ts);
            must_commit(&engine, k, ts, ts);
        }

        // Generate SEEK_BOUND + 2 Put for key [20].
        let k = &[20 as u8];
        for ts in 0..SEEK_BOUND + 2 {
            must_prewrite_put(&engine, k, &[ts as u8], k, ts);
            must_commit(&engine, k, ts, ts);
        }

        // Generate SEEK_BOUND / 2 Put for key [30].
        let k = &[30 as u8];
        for ts in 0..SEEK_BOUND / 2 {
            must_prewrite_put(&engine, k, &[ts as u8], k, ts);
            must_commit(&engine, k, ts, ts);
        }

        // Generate SEEK_BOUND / 2 + 1 Put for key [40].
        let k = &[40 as u8];
        for ts in 0..SEEK_BOUND / 2 + 1 {
            must_prewrite_put(&engine, k, &[ts as u8], k, ts);
            must_commit(&engine, k, ts, ts);
        }

        // Generate SEEK_BOUND / 2 + 1 Put + SEEK_BOUND / 2 Rollback for key [50].
        let k = &[50 as u8];
        for ts in 0..SEEK_BOUND / 2 + 1 {
            must_prewrite_put(&engine, k, &[ts as u8], k, ts);
            must_commit(&engine, k, ts, ts);
        }
        for ts in SEEK_BOUND / 2 + 1..SEEK_BOUND + 1 {
            must_rollback(&engine, k, ts);
        }

        // Generate SEEK_BOUND / 2 Put + 1 Delete + SEEK_BOUND / 2 Rollback for key [60].
        let k = &[60 as u8];
        for ts in 0..SEEK_BOUND / 2 {
            must_prewrite_put(&engine, k, &[ts as u8], k, ts);
            must_commit(&engine, k, ts, ts);
        }
        must_prewrite_delete(&engine, k, k, SEEK_BOUND / 2);
        must_commit(&engine, k, SEEK_BOUND / 2, SEEK_BOUND / 2);
        for ts in SEEK_BOUND / 2 + 1..SEEK_BOUND + 1 {
            must_rollback(&engine, k, ts);
        }

        // Generate 1 Delete for key [70].
        let k = &[70 as u8];
        must_prewrite_delete(&engine, k, k, 1);
        must_commit(&engine, k, 1, 2);

        // Generate SEEK_BOUND + 1 Put for key [80].
        let k = &[80 as u8];
        for ts in 0..SEEK_BOUND / 2 + 1 {
            must_prewrite_put(&engine, k, &[ts as u8], k, ts);
            must_commit(&engine, k, ts, ts);
        }

        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut point_getter = PointGetterBuilder::new(snapshot).build().unwrap();

        // First operation, 1 seek.
        assert_eq!(
            point_getter
                .read_next(&Key::from_raw(&[55u8]), SEEK_BOUND)
                .unwrap(),
            None
        );
        let statistics = point_getter.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, 0);

        // Currently pointing at key [60_N], getting key [55_N], use seek.
        // N denotes to SEEK_BOUND.
        assert_eq!(
            point_getter
                .read_next(&Key::from_raw(&[55u8]), SEEK_BOUND)
                .unwrap(),
            None
        );
        let statistics = point_getter.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, 0);

        // Currently pointing at key [60_N], getting key [60_N], use next to skip
        // SEEK_BOUND / 2 rollbacks.
        assert_eq!(
            point_getter
                .read_next(&Key::from_raw(&[60u8]), SEEK_BOUND)
                .unwrap(),
            None
        );
        let statistics = point_getter.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, (SEEK_BOUND / 2) as usize);

        // Currently pointing at key [60_N/2], getting key [60_N], use seek to reach key [60_N]
        // and next to skip SEEK_BOUND / 2 rollbacks.
        assert_eq!(
            point_getter
                .read_next(&Key::from_raw(&[60u8]), SEEK_BOUND)
                .unwrap(),
            None
        );
        let statistics = point_getter.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, (SEEK_BOUND / 2) as usize);

        // Currently pointing at key [60_N/2], getting [80_1].
        // There will be SEEK_BOUND next() + 1 seek.
        assert_eq!(
            point_getter
                .read_next(&Key::from_raw(&[80u8]), 1)
                .unwrap()
                .unwrap(),
            vec![1]
        );
        let statistics = point_getter.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, SEEK_BOUND as usize);

        // Currently pointing at key [80_1], getting [70_3], use seek.
        assert_eq!(
            point_getter.read_next(&Key::from_raw(&[70u8]), 3).unwrap(),
            None
        );
        let statistics = point_getter.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, 0);

        // Currently pointing at key [70_2], getting [0_N], use seek.
        assert_eq!(
            point_getter
                .read_next(&Key::from_raw(&[0u8]), SEEK_BOUND)
                .unwrap(),
            None
        );
        let statistics = point_getter.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(statistics.write.next, 0);

        // Currently pointing at key [10_N], getting [10_N], no op.
        assert_eq!(
            point_getter
                .read_next(&Key::from_raw(&[10u8]), SEEK_BOUND)
                .unwrap()
                .unwrap(),
            vec![SEEK_BOUND as u8]
        );
        let statistics = point_getter.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, 0);

        // Currently pointing at key [10_N], getting [10_N/2], N/2 next.
        assert_eq!(
            point_getter
                .read_next(&Key::from_raw(&[10u8]), SEEK_BOUND / 2)
                .unwrap()
                .unwrap(),
            vec![(SEEK_BOUND / 2) as u8]
        );
        let statistics = point_getter.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, (SEEK_BOUND / 2) as usize);

        // Currently pointing at key [10_N/2], getting [20_N/2+3].
        // Use (SEEK_BOUND / 2 + 1) next() to skip versions of key [10], reaches [20_N+1].
        // Use (SEEK_BOUND / 2 - 1) next() to reach [20_N/2+2].
        assert_eq!(
            point_getter
                .read_next(&Key::from_raw(&[20u8]), SEEK_BOUND / 2 + 2)
                .unwrap()
                .unwrap(),
            vec![(SEEK_BOUND / 2 + 2) as u8]
        );
        let statistics = point_getter.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, SEEK_BOUND as usize);

        // Currently pointing at key [20_N/2+2]. Use (SEEK_BOUND / 2 + 2) next() to reach [20_0].
        assert_eq!(
            point_getter
                .read_next(&Key::from_raw(&[20u8]), 0)
                .unwrap()
                .unwrap(),
            vec![0u8]
        );
        let statistics = point_getter.take_statistics();
        assert_eq!(statistics.write.seek, 0);
        assert_eq!(statistics.write.next, (SEEK_BOUND / 2 + 2) as usize);

        // Currently pointing at key [20_0]. Use SEEK_BOUND next() + 1 seek to reach [50_N].
        // Then, use SEEK_BOUND / 2 next() to skip rollbacks.
        assert_eq!(
            point_getter
                .read_next(&Key::from_raw(&[50u8]), SEEK_BOUND)
                .unwrap()
                .unwrap(),
            vec![(SEEK_BOUND / 2) as u8]
        );
        let statistics = point_getter.take_statistics();
        assert_eq!(statistics.write.seek, 1);
        assert_eq!(
            statistics.write.next,
            (SEEK_BOUND + SEEK_BOUND / 2) as usize
        );
    }

    /// Get key single time. Should get `None` for future attempts.
    #[test]
    fn test_multi_false() {}

    /// Omit value == true && value is short value.
    #[test]
    fn test_omit_short_value() {}

    /// Omit value == true && value is long value stored in default CF.
    #[test]
    fn test_omit_long_value() {}

    /// Locks are checked in SI.
    #[test]
    fn test_isolation_si() {}

    /// Get keys that have long values.
    #[test]
    fn test_long_value() {}

    /// Get corrupted data.
    #[test]
    fn test_default_data_missing() {}

}
