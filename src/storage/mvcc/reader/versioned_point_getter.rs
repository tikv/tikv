// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath
use engine_traits::{CF_DEFAULT, CF_WRITE};
use txn_types::{Key, TimeStamp, Value, WriteRef, WriteType};

use crate::storage::{
    kv::{Snapshot, Statistics},
    mvcc::{Result, default_not_found_error},
};

/// `VersionedPointGetter` factory.
pub struct VersionedPointGetterBuilder<S: Snapshot> {
    snapshot: S,
    omit_value: bool,
}

impl<S: Snapshot> VersionedPointGetterBuilder<S> {
    /// Initialize a new `VersionedPointGetterBuilder`.
    pub fn new(snapshot: S) -> Self {
        Self {
            snapshot,
            omit_value: false,
        }
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

    /// Build `VersionedPointGetter` from the current configuration.
    pub fn build(self) -> Result<VersionedPointGetter<S>> {
        Ok(VersionedPointGetter {
            snapshot: self.snapshot,
            omit_value: self.omit_value,

            statistics: Statistics::default(),
        })
    }
}

/// This struct is used to get specifiec version of user key. The version is the
/// commit_ts. It is used by TiCI scan's double read. TiCI does not support
/// snapshot read yet, so in TiCI scan, it will return both handle and the
/// version(commit_ts) of the row, in the double read stage, TiDB will use
/// VersionedLookup to lookup the value with given version. Since TiCI's data is
/// always the commited data, in VersionedPointGetter LockCF is ignored,
/// it simply use userkey_commit_ts to lookup WriteCF, and use userkey_start_ts
/// to lookup the DataCF. Theory speaking, the type of Write value should always
/// be PUT, but we just return empty value instead of error if the Write value's
/// type is not PUT
///
/// Use `VersionedPointGetterBuilder` to build `VersionedPointGetter`.
pub struct VersionedPointGetter<S: Snapshot> {
    snapshot: S,
    omit_value: bool,

    statistics: Statistics,
}

impl<S: Snapshot> VersionedPointGetter<S> {
    /// Take out and reset the statistics collected so far.
    #[inline]
    pub fn take_statistics(&mut self) -> Statistics {
        std::mem::take(&mut self.statistics)
    }

    /// Get the value of a user key.
    pub fn get(&mut self, user_key: &Key, version: TimeStamp) -> Result<Option<Value>> {
        fail_point!("versioned_point_getter_get");
        self.statistics.write.get += 1;
        // don't need check lock here because all the target result is the committed
        // data
        // TODO: We can avoid this clone.
        let write_value = self
            .snapshot
            .get_cf(CF_WRITE, &user_key.clone().append_ts(version))?;

        if let Some(write_value) = write_value {
            let write = WriteRef::parse(&write_value)?;
            match write.write_type {
                WriteType::Put => {
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
                    // should not happen since all the target data is the committed data
                    // todo add a warning here
                    return Ok(None);
                }
                WriteType::Lock | WriteType::Rollback => {
                    // should not happen since all the target data is the committed data
                    // todo add a warning here
                    return Ok(None);
                }
            }
        } else {
            // maybe the target data is gc-ed
            // todo add a warning here
            Ok(None)
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
}

#[cfg(test)]
mod tests {
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
        kv::{Engine, RocksEngine, TestEngineBuilder},
        txn::tests::{
            must_commit, must_gc, must_prewrite_delete, must_prewrite_lock, must_prewrite_put,
        },
    };

    fn new_versioned_point_getter<E: Engine>(engine: &mut E) -> VersionedPointGetter<E::Snap> {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        VersionedPointGetterBuilder::new(snapshot).build().unwrap()
    }

    fn must_get_key<S: Snapshot>(
        point_getter: &mut VersionedPointGetter<S>,
        key: &[u8],
        ts: impl Into<TimeStamp>,
    ) {
        assert!(
            point_getter
                .get(&Key::from_raw(key), ts.into())
                .unwrap()
                .is_some()
        );
    }

    fn must_get_value<S: Snapshot>(
        point_getter: &mut VersionedPointGetter<S>,
        key: &[u8],
        ts: impl Into<TimeStamp>,
        prefix: &[u8],
    ) {
        let val = point_getter
            .get(&Key::from_raw(key), ts.into())
            .unwrap()
            .unwrap();
        assert!(val.starts_with(prefix));
    }

    fn must_get_none<S: Snapshot>(
        point_getter: &mut VersionedPointGetter<S>,
        key: &[u8],
        ts: impl Into<TimeStamp>,
    ) {
        assert!(
            point_getter
                .get(&Key::from_raw(key), ts.into())
                .unwrap()
                .is_none()
        );
    }

    /// Builds a sample engine with the following data:
    /// LOCK    bar                     (commit at 11)
    /// PUT     bar     -> barvvv...    (commit at 5)
    /// PUT     box     -> boxvv....    (commit at 9)
    /// DELETE  foo1                    (commit at 9)
    /// PUT     foo1    -> foo1vv...    (commit at 3)
    /// DELETE  xxx                     (commit at 7)
    /// PUT     zz       -> zzv....    (commit at 103)
    /// PUT     zz       -> yyv....    (commit at 105)
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
            b"bar",
            &format!("bar{}", suffix).into_bytes(),
            b"foo2",
            4,
        );
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
        must_prewrite_put(
            &mut engine,
            b"zz",
            &format!("zz{}", suffix).into_bytes(),
            b"zz",
            102,
        );
        must_commit(&mut engine, b"zz", 102, 103);
        must_prewrite_put(
            &mut engine,
            b"zz",
            &format!("yy{}", suffix).into_bytes(),
            b"zz",
            104,
        );
        must_commit(&mut engine, b"zz", 104, 105);
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

    #[test]
    fn test_basic() {
        let mut engine = new_sample_engine();

        let mut getter = new_versioned_point_getter(&mut engine);

        // Get a deleted key, just return empty
        must_get_none(&mut getter, b"foo1", 9);
        let s = getter.take_statistics();
        assert_eq!(s.write.get, 1);
        assert_eq!(s.processed_size, 0);
        // Get again, no cache in the getter
        must_get_none(&mut getter, b"foo1", 9);
        let s = getter.take_statistics();
        assert_eq!(s.write.get, 1);
        assert_eq!(s.processed_size, 0);
        // Get a key of prev version
        must_get_value(&mut getter, b"foo1", 3, b"foo1v");
        let s = getter.take_statistics();
        assert_eq!(s.write.get, 1);
        assert_eq!(s.data.get, 1);
        assert_eq!(
            s.processed_size,
            Key::from_raw(b"foo1").len()
                + b"foo1".len()
                + "v".repeat(SHORT_VALUE_MAX_LEN + 1).len()
        );
        // Get again
        must_get_value(&mut getter, b"foo1", 3, b"foo1v");
        let s = getter.take_statistics();
        assert_eq!(s.write.get, 1);
        assert_eq!(s.data.get, 1);
        assert_eq!(
            s.processed_size,
            Key::from_raw(b"foo1").len()
                + b"foo1".len()
                + "v".repeat(SHORT_VALUE_MAX_LEN + 1).len()
        );
        // Get a key with larger version, return none since
        // version is not exactly match
        must_get_none(&mut getter, b"foo1", 4);
        let s = getter.take_statistics();
        assert_eq!(s.write.get, 1);
        assert_eq!(s.processed_size, 0);

        // Get a key with different version
        must_get_value(&mut getter, b"zz", 103, b"zzv");
        let s = getter.take_statistics();
        assert_eq!(s.write.get, 1);
        assert_eq!(s.data.get, 1);
        assert_eq!(
            s.processed_size,
            Key::from_raw(b"zz").len() + b"zz".len() + "v".repeat(SHORT_VALUE_MAX_LEN + 1).len()
        );
        must_get_value(&mut getter, b"zz", 105, b"yyv");
        let s = getter.take_statistics();
        assert_eq!(s.write.get, 1);
        assert_eq!(s.data.get, 1);
        assert_eq!(
            s.processed_size,
            Key::from_raw(b"zz").len() + b"yy".len() + "v".repeat(SHORT_VALUE_MAX_LEN + 1).len()
        );

        // Get a key that does not exist
        must_get_none(&mut getter, b"z", 10);
        let s = getter.take_statistics();
        assert_eq!(s.write.get, 1);
        assert_eq!(s.processed_size, 0);

        // Get meet lock type, just return none
        must_get_none(&mut getter, b"bar", 11);
        let s = getter.take_statistics();
        assert_eq!(s.write.get, 1);
        assert_eq!(s.processed_size, 0);
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

        let mut getter_1 = new_versioned_point_getter(&mut engine);
        must_get_value(&mut getter_1, b"foo", 20, b"bar");
        must_get_value(&mut getter_1, b"foo1", 20, b"bar1");
        must_get_value(&mut getter_1, b"foo2", 20, b"bar2");
        must_get_value(&mut getter_1, b"foo3", 20, b"bar3");

        must_gc(&mut engine, b"foo", 50);
        must_gc(&mut engine, b"foo1", 50);
        must_gc(&mut engine, b"foo2", 50);
        must_gc(&mut engine, b"foo3", 50);

        let mut getter = new_versioned_point_getter(&mut engine);
        // can not get deleted value after gc
        must_get_value(&mut getter, b"foo", 20, b"bar");
        must_get_none(&mut getter, b"foo1", 20);
        must_get_none(&mut getter, b"foo2", 20);
        must_get_value(&mut getter, b"foo3", 20, b"bar3");
    }

    // There are some locks in the Lock CF.
    #[test]
    fn test_locked() {
        let mut engine = new_sample_engine_2();

        let mut getter = new_versioned_point_getter(&mut engine);
        // "a" does not exists
        must_get_none(&mut getter, b"a", 3);
        // "bar" is visible even there is a lock
        must_get_value(&mut getter, b"bar", 3, b"barval");
        // "foo1" is visible
        must_get_value(&mut getter, b"foo1", 3, b"foo1v");
        // "foo2" is not visible in WriteCF
        must_get_none(&mut getter, b"foo2", 3);
        // all of them are not visible if version is wrong
        must_get_none(&mut getter, b"a", 4);
        must_get_none(&mut getter, b"bar", 4);
        must_get_none(&mut getter, b"foo1", 4);
        must_get_none(&mut getter, b"foo2", 4);
    }
    #[test]
    fn test_omit_value() {
        let mut engine = new_sample_engine_2();

        let snapshot = engine.snapshot(Default::default()).unwrap();

        let mut getter = VersionedPointGetterBuilder::new(snapshot)
            .omit_value(true)
            .build()
            .unwrap();
        must_get_none(&mut getter, b"bar", 4);
        must_get_key(&mut getter, b"foo1", 3);
        must_get_none(&mut getter, b"foo2", 4);
        must_get_none(&mut getter, b"foo3", 4);
    }

    #[test]
    fn test_short_value() {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        // short value
        let (key, val) = (b"foo", b"bar");
        must_prewrite_put(&mut engine, key, val, key, 10);
        must_commit(&mut engine, key, 10, 15);

        // load value from default cf.
        let val = "v".repeat(SHORT_VALUE_MAX_LEN + 1);
        let val = val.as_bytes();
        must_prewrite_put(&mut engine, key, val, key, 20);
        must_commit(&mut engine, key, 20, 25);

        // delete
        must_prewrite_delete(&mut engine, key, key, 30);
        must_commit(&mut engine, key, 30, 35);

        let mut getter = new_versioned_point_getter(&mut engine);
        // get from write cf
        must_get_value(&mut getter, key, 15, b"bar");
        let s = getter.take_statistics();
        assert_eq!(s.write.get, 1);
        assert_eq!(s.data.get, 0);
        // get from default cf
        must_get_value(&mut getter, key, 25, b"vv");
        let s = getter.take_statistics();
        assert_eq!(s.write.get, 1);
        assert_eq!(s.data.get, 1);
        // val is deleted
        must_get_none(&mut getter, key, 35);
        let s = getter.take_statistics();
        assert_eq!(s.write.get, 1);
        assert_eq!(s.data.get, 0);
        must_get_none(&mut getter, key, 16);
        let s = getter.take_statistics();
        assert_eq!(s.write.get, 1);
        assert_eq!(s.data.get, 0);
        // wrong version
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

        let mut getter = new_versioned_point_getter(&mut engine);
        let val = getter.get(&Key::from_raw(k), 2.into()).unwrap().unwrap();
        assert_eq!(val, val_buf.as_slice());
        let row_slice = RowSlice::from_bytes(val.as_slice()).unwrap();
        assert!(row_slice.get_checksum().unwrap().get_checksum_val() > 0);
    }
}
