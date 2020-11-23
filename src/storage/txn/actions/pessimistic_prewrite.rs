// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::shared::prewrite_key_value;
use crate::storage::mvcc::{
    metrics::MVCC_DUPLICATE_CMD_COUNTER_VEC, ErrorInner, LockType, Mutation, MvccTxn,
    Result as MvccResult, TimeStamp,
};
use crate::storage::Snapshot;

pub fn pessimistic_prewrite<S: Snapshot>(
    txn: &mut MvccTxn<S>,
    mutation: Mutation,
    primary: &[u8],
    secondary_keys: &Option<Vec<Vec<u8>>>,
    is_pessimistic_lock: bool,
    mut lock_ttl: u64,
    for_update_ts: TimeStamp,
    txn_size: u64,
    mut min_commit_ts: TimeStamp,
    max_commit_ts: TimeStamp,
    try_one_pc: bool,
) -> MvccResult<TimeStamp> {
    if mutation.should_not_write() {
        return Err(box_err!(
            "cannot handle checkNotExists in pessimistic prewrite"
        ));
    }
    let mutation_type = mutation.mutation_type();
    let lock_type = LockType::from_mutation(&mutation);
    let mut fallback_from_async_commit = false;
    let (key, value) = mutation.into_key_value();

    fail_point!("pessimistic_prewrite", |err| Err(
        crate::storage::mvcc::txn::make_txn_error(err, &key, txn.start_ts,).into()
    ));

    let has_pessimistic_lock = if let Some(lock) = txn.reader.load_lock(&key)? {
        if lock.ts != txn.start_ts {
            // Abort on lock belonging to other transaction if
            // prewrites a pessimistic lock.
            if is_pessimistic_lock {
                warn!(
                    "prewrite failed (pessimistic lock not found)";
                    "start_ts" => txn.start_ts,
                    "key" => %key,
                    "lock_ts" => lock.ts
                );
                return Err(ErrorInner::PessimisticLockNotFound {
                    start_ts: txn.start_ts,
                    key: key.into_raw()?,
                }
                .into());
            }
            return Err(txn
                .handle_non_pessimistic_lock_conflict(key, lock)
                .unwrap_err());
        } else if lock.lock_type != LockType::Pessimistic {
            // Allow to overwrite the primary lock to fallback from async commit.
            if lock.use_async_commit && secondary_keys.is_none() {
                info!("fallback from async commit"; "start_ts" => txn.start_ts);
                fallback_from_async_commit = true;
                // needn't clear pessimistic locks
                false
            } else {
                // Duplicated command. No need to overwrite the lock and data.
                MVCC_DUPLICATE_CMD_COUNTER_VEC.prewrite.inc();
                return Ok(lock.min_commit_ts);
            }
        } else {
            // The lock is pessimistic and owned by this txn, go through to overwrite it.
            // The ttl and min_commit_ts of the lock may have been pushed forward.
            lock_ttl = std::cmp::max(lock_ttl, lock.ttl);
            min_commit_ts = std::cmp::max(min_commit_ts, lock.min_commit_ts);
            true
        }
    } else if is_pessimistic_lock {
        txn.amend_pessimistic_lock(&key)?;
        false
    } else {
        false
    };

    txn.check_extra_op(&key, mutation_type, None)?;
    // No need to check data constraint, it's resolved by pessimistic locks.
    prewrite_key_value(
        txn,
        key,
        lock_type.unwrap(),
        primary,
        secondary_keys,
        value,
        lock_ttl,
        for_update_ts,
        txn_size,
        min_commit_ts,
        max_commit_ts,
        try_one_pc,
        has_pessimistic_lock,
        fallback_from_async_commit,
    )
}

pub mod tests {
    use super::*;
    use crate::storage::mvcc::{Key, MvccTxn, Result as MvccResult, TimeStamp};
    #[cfg(test)]
    use crate::storage::txn::tests::must_acquire_pessimistic_lock;
    use crate::storage::Engine;
    use concurrency_manager::ConcurrencyManager;

    pub fn try_pessimistic_prewrite_check_not_exists<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
    ) -> MvccResult<()> {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let ts = ts.into();
        let cm = ConcurrencyManager::new(ts);
        let mut txn = MvccTxn::new(snapshot, ts, true, cm);

        pessimistic_prewrite(
            &mut txn,
            Mutation::CheckNotExists(Key::from_raw(key)),
            pk,
            &None,
            false,
            0,
            TimeStamp::default(),
            0,
            TimeStamp::default(),
            TimeStamp::default(),
            false,
        )?;
        Ok(())
    }

    #[test]
    fn test_async_commit_pessimistic_prewrite_check_max_commit_ts() {
        let engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new(42.into());

        must_acquire_pessimistic_lock(&engine, b"k1", b"k1", 10, 10);
        must_acquire_pessimistic_lock(&engine, b"k2", b"k1", 10, 10);

        let snapshot = engine.snapshot(Default::default()).unwrap();

        let mut txn = MvccTxn::new(snapshot, 10.into(), false, cm.clone());
        // calculated commit_ts = 43 ≤ 50, ok
        pessimistic_prewrite(
            &mut txn,
            Mutation::Put((Key::from_raw(b"k1"), b"v1".to_vec())),
            b"k1",
            &Some(vec![b"k2".to_vec()]),
            true,
            2000,
            20.into(),
            2,
            10.into(),
            50.into(),
            false,
        )
        .unwrap();

        cm.update_max_ts(60.into());
        // calculated commit_ts = 61 > 50, ok
        pessimistic_prewrite(
            &mut txn,
            Mutation::Put((Key::from_raw(b"k2"), b"v2".to_vec())),
            b"k1",
            &Some(vec![]),
            true,
            2000,
            20.into(),
            2,
            10.into(),
            50.into(),
            false,
        )
        .unwrap_err();
    }

    #[test]
    fn test_1pc_pessimistic_prewrite_check_max_commit_ts() {
        let engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new(42.into());

        must_acquire_pessimistic_lock(&engine, b"k1", b"k1", 10, 10);
        must_acquire_pessimistic_lock(&engine, b"k2", b"k1", 10, 10);

        let snapshot = engine.snapshot(Default::default()).unwrap();

        let mut txn = MvccTxn::new(snapshot, 10.into(), false, cm.clone());
        // calculated commit_ts = 43 ≤ 50, ok
        pessimistic_prewrite(
            &mut txn,
            Mutation::Put((Key::from_raw(b"k1"), b"v1".to_vec())),
            b"k1",
            &None,
            true,
            2000,
            20.into(),
            2,
            10.into(),
            50.into(),
            true,
        )
        .unwrap();

        cm.update_max_ts(60.into());
        // calculated commit_ts = 61 > 50, ok
        pessimistic_prewrite(
            &mut txn,
            Mutation::Put((Key::from_raw(b"k2"), b"v2".to_vec())),
            b"k1",
            &None,
            true,
            2000,
            20.into(),
            2,
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
        must_acquire_pessimistic_lock(&engine, b"k", b"k", 10, 10);

        must_pessimistic_prewrite_put_async_commit(
            &engine,
            b"k",
            b"v",
            b"k",
            &Some(vec![vec![1]]),
            10,
            10,
            true,
            20,
        );
        must_pessimistic_prewrite_put(&engine, b"k", b"v", b"k", 10, 20, true);
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, false, IsolationLevel::Si);
        let lock = reader.load_lock(&Key::from_raw(b"k")).unwrap().unwrap();
        assert!(!lock.use_async_commit);
        assert!(lock.secondaries.is_empty());

        // deny overwrites with async commit prewrit
        must_pessimistic_prewrite_put_async_commit(
            &engine,
            b"k",
            b"v",
            b"k",
            &Some(vec![vec![1]]),
            10,
            10,
            true,
            20,
        );
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, false, IsolationLevel::Si);
        let lock = reader.load_lock(&Key::from_raw(b"k")).unwrap().unwrap();
        assert!(!lock.use_async_commit);
        assert!(lock.secondaries.is_empty());
    }
}
