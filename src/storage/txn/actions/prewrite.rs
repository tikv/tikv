// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::mvcc::{
    metrics::{MVCC_CONFLICT_COUNTER, MVCC_DUPLICATE_CMD_COUNTER_VEC},
    ErrorInner, LockType, MvccTxn, Result as MvccResult,
};
use crate::storage::txn::actions::shared::prewrite_key_value;
use crate::storage::Snapshot;
use txn_types::{Key, Mutation, TimeStamp, WriteType};

pub fn prewrite<S: Snapshot>(
    txn: &mut MvccTxn<S>,
    mutation: Mutation,
    primary: &[u8],
    secondary_keys: &Option<Vec<Vec<u8>>>,
    skip_constraint_check: bool,
    lock_ttl: u64,
    txn_size: u64,
    min_commit_ts: TimeStamp,
    max_commit_ts: TimeStamp,
    try_one_pc: bool,
) -> MvccResult<TimeStamp> {
    let lock_type = LockType::from_mutation(&mutation);
    // For the insert/checkNotExists operation, the old key should not be in the system.
    let should_not_exist = mutation.should_not_exists();
    let should_not_write = mutation.should_not_write();
    let mutation_type = mutation.mutation_type();
    let mut fallback_from_async_commit = false;
    let (key, value) = mutation.into_key_value();

    fail_point!("prewrite", |err| Err(
        crate::storage::mvcc::txn::make_txn_error(err, &key, txn.start_ts,).into()
    ));

    // Check whether the current key is locked at any timestamp.
    if let Some(lock) = txn.reader.load_lock(&key)? {
        if lock.ts != txn.start_ts {
            return Err(ErrorInner::KeyIsLocked(lock.into_lock_info(key.into_raw()?)).into());
        }
        // TODO: remove it in future
        if lock.lock_type == LockType::Pessimistic {
            return Err(ErrorInner::LockTypeNotMatch {
                start_ts: txn.start_ts,
                key: key.into_raw()?,
                pessimistic: true,
            }
            .into());
        }
        // Allow to overwrite the primary lock to fallback from async commit.
        if lock.use_async_commit && secondary_keys.is_none() {
            info!("fallback from async commit"; "start_ts" => txn.start_ts);
            fallback_from_async_commit = true;
        } else {
            // Duplicated command. No need to overwrite the lock and data.
            MVCC_DUPLICATE_CMD_COUNTER_VEC.prewrite.inc();
            return Ok(lock.min_commit_ts);
        }
    }

    let mut prev_write = None;
    // Check whether there is a newer version.
    if !skip_constraint_check && !fallback_from_async_commit {
        if let Some((commit_ts, write)) = txn.reader.seek_write(&key, TimeStamp::max())? {
            // Abort on writes after our start timestamp ...
            // If exists a commit version whose commit timestamp is larger than current start
            // timestamp, we should abort current prewrite.
            if commit_ts > txn.start_ts {
                MVCC_CONFLICT_COUNTER.prewrite_write_conflict.inc();
                return Err(ErrorInner::WriteConflict {
                    start_ts: txn.start_ts,
                    conflict_start_ts: write.start_ts,
                    conflict_commit_ts: commit_ts,
                    key: key.into_raw()?,
                    primary: primary.to_vec(),
                }
                .into());
            }
            // If there's a write record whose commit_ts equals to our start ts, the current
            // transaction is ok to continue, unless the record means that the current
            // transaction has been rolled back.
            if commit_ts == txn.start_ts
                && (write.write_type == WriteType::Rollback || write.has_overlapped_rollback)
            {
                MVCC_CONFLICT_COUNTER.rolled_back.inc();
                // TODO: Maybe we need to add a new error for the rolled back case.
                return Err(ErrorInner::WriteConflict {
                    start_ts: txn.start_ts,
                    conflict_start_ts: write.start_ts,
                    conflict_commit_ts: commit_ts,
                    key: key.into_raw()?,
                    primary: primary.to_vec(),
                }
                .into());
            }
            // Should check it when no lock exists, otherwise it can report error when there is
            // a lock belonging to a committed transaction which deletes the key.
            txn.check_data_constraint(should_not_exist, &write, commit_ts, &key)?;
            prev_write = Some(write);
        }
    }
    if should_not_write {
        return Ok(TimeStamp::zero());
    }

    if !fallback_from_async_commit {
        txn.check_extra_op(&key, mutation_type, prev_write)?;
    }
    prewrite_key_value(
        txn,
        key,
        lock_type.unwrap(),
        primary,
        secondary_keys,
        value,
        lock_ttl,
        TimeStamp::zero(),
        txn_size,
        min_commit_ts,
        max_commit_ts,
        try_one_pc,
        false,
        fallback_from_async_commit,
    )
}

pub mod tests {
    use super::*;
    use crate::storage::{mvcc::tests::*, mvcc::MvccTxn, Engine};
    use concurrency_manager::ConcurrencyManager;
    use kvproto::kvrpcpb::Context;
    use txn_types::TimeStamp;

    // Insert has a constraint that key should not exist
    pub fn try_prewrite_insert<E: Engine>(
        engine: &E,
        key: &[u8],
        value: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
    ) -> MvccResult<()> {
        let ctx = Context::default();
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let ts = ts.into();
        let cm = ConcurrencyManager::new(ts);
        let mut txn = MvccTxn::new(snapshot, ts, true, cm);

        prewrite(
            &mut txn,
            Mutation::Insert((Key::from_raw(key), value.to_vec())),
            pk,
            &None,
            false,
            0,
            0,
            TimeStamp::default(),
            TimeStamp::default(),
            false,
        )?;
        write(engine, &ctx, txn.into_modifies());
        Ok(())
    }

    pub fn try_prewrite_check_not_exists<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
    ) -> MvccResult<()> {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let ts = ts.into();
        let cm = ConcurrencyManager::new(ts);
        let mut txn = MvccTxn::new(snapshot, ts, true, cm);

        prewrite(
            &mut txn,
            Mutation::CheckNotExists(Key::from_raw(key)),
            pk,
            &None,
            false,
            0,
            0,
            TimeStamp::default(),
            TimeStamp::default(),
            false,
        )?;
        Ok(())
    }

    #[test]
    fn test_async_commit_prewrite_check_max_commit_ts() {
        let engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new(42.into());

        let snapshot = engine.snapshot(Default::default()).unwrap();

        let mut txn = MvccTxn::new(snapshot, 10.into(), false, cm.clone());
        // calculated commit_ts = 43 ≤ 50, ok
        prewrite(
            &mut txn,
            Mutation::Put((Key::from_raw(b"k1"), b"v1".to_vec())),
            b"k1",
            &Some(vec![b"k2".to_vec()]),
            false,
            2000,
            2,
            10.into(),
            50.into(),
            false,
        )
        .unwrap();

        cm.update_max_ts(60.into());
        // calculated commit_ts = 61 > 50, ok
        prewrite(
            &mut txn,
            Mutation::Put((Key::from_raw(b"k2"), b"v2".to_vec())),
            b"k1",
            &Some(vec![]),
            false,
            2000,
            1,
            10.into(),
            50.into(),
            false,
        )
        .unwrap_err();
    }

    #[test]
    fn test_1pc_check_max_commit_ts() {
        let engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new(42.into());

        let snapshot = engine.snapshot(Default::default()).unwrap();

        let mut txn = MvccTxn::new(snapshot, 10.into(), false, cm.clone());
        // calculated commit_ts = 43 ≤ 50, ok
        prewrite(
            &mut txn,
            Mutation::Put((Key::from_raw(b"k1"), b"v1".to_vec())),
            b"k1",
            &None,
            false,
            2000,
            2,
            10.into(),
            50.into(),
            true,
        )
        .unwrap();

        cm.update_max_ts(60.into());
        // calculated commit_ts = 61 > 50, ok
        prewrite(
            &mut txn,
            Mutation::Put((Key::from_raw(b"k2"), b"v2".to_vec())),
            b"k1",
            &None,
            false,
            2000,
            1,
            10.into(),
            50.into(),
            true,
        )
        .unwrap_err();
    }

    #[test]
    fn test_fallback_from_async_commit() {
        use super::super::tests::*;
        use crate::storage::mvcc::MvccReader;
        use kvproto::kvrpcpb::IsolationLevel;

        let engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        must_prewrite_put_async_commit(&engine, b"k", b"v", b"k", &Some(vec![vec![1]]), 10, 20);
        must_prewrite_put(&engine, b"k", b"v", b"k", 10);
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, false, IsolationLevel::Si);
        let lock = reader.load_lock(&Key::from_raw(b"k")).unwrap().unwrap();
        assert!(!lock.use_async_commit);
        assert!(lock.secondaries.is_empty());

        // deny overwrites with async commit prewrit
        must_prewrite_put_async_commit(&engine, b"k", b"v", b"k", &Some(vec![vec![1]]), 10, 20);
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, false, IsolationLevel::Si);
        let lock = reader.load_lock(&Key::from_raw(b"k")).unwrap().unwrap();
        assert!(!lock.use_async_commit);
        assert!(lock.secondaries.is_empty());
    }
}
