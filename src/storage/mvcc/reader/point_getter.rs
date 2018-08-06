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
                // TODO: if length is not equal, no need to truncate.
                let current_user_key = Key::truncate_ts_for(cursor_key)?;
                if user_key.encoded().as_slice() != current_user_key {
                    // Meet another key.
                    return Ok(None);
                }
            }

            let write = Write::parse(self.write_cursor.value(&mut self.statistics.write))?;
            self.statistics.write.processed += 1;

            match write.write_type {
                WriteType::Put => {
                    if self.omit_value {
                        return Ok(Some(vec![]));
                    }
                    match write.short_value {
                        Some(value) => {
                            // Value is carried in `write`.
                            return Ok(Some(value));
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
                            return Ok(Some(value));
                        }
                    }
                }
                WriteType::Delete => return Ok(None),
                WriteType::Lock | WriteType::Rollback => {
                    // Continue iterate next `write`.
                }
            }

            self.write_cursor.next(&mut self.statistics.write);
        }
    }

    /// Create the default cursor if it doesn't exist.
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
    use super::*;
    use storage::engine::{self, TEMP_DIR};
    use storage::mvcc::tests::*;
    use storage::ALL_CFS;
    use storage::{Engine, Key};

    use kvproto::kvrpcpb::{Context, IsolationLevel};

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

    #[test]
    fn test_multi_true_multi_key_single_version() {}

    #[test]
    fn test_multi_true_multi_key_multi_version() {}

    #[test]
    fn test_multi_false() {}

    #[test]
    fn test_omit_short_value() {}

    #[test]
    fn test_omit_long_value() {}

    #[test]
    fn test_isolation_si() {}

    #[test]
    fn test_long_value() {}

    #[test]
    fn test_get_rollback() {}

    #[test]
    fn test_get_delete() {}

    #[test]
    fn test_get_value() {}

    #[test]
    fn test_default_data_missing() {}

}
