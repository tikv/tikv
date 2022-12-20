// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::kvrpcpb::WriteConflictReason;
// #[PerformanceCriticalPath]
use txn_types::{Key, LockType, OldValue, PessimisticLock, TimeStamp, Value, Write, WriteType};

use crate::storage::{
    mvcc::{
        metrics::{MVCC_CONFLICT_COUNTER, MVCC_DUPLICATE_CMD_COUNTER_VEC},
        ErrorInner, MvccTxn, Result as MvccResult, SnapshotReader,
    },
    txn::{
        actions::check_data_constraint::check_data_constraint, sched_pool::tls_can_enable,
        scheduler::LAST_CHANGE_TS,
    },
    types::PessimisticLockKeyResult,
    Snapshot,
};

/// Acquires pessimistic lock on a single key. Optionally reads the previous
/// value by the way.
///
/// When `need_value` is set, the first return value will be
/// `PessimisticLockKeyResult::Value`. When `need_value` is not set but
/// `need_check_existence` is set, the first return value will be
/// `PessimisticLockKeyResult::Existence`. If neither `need_value` nor
/// `need_check_existence` is set, the first return value will be
/// `PessimisticLockKeyResult::Empty`.
///
/// If `allow_lock_with_conflict` is set, and the lock is acquired successfully
/// ignoring a write conflict, the first return value will be
/// `PessimisticLockKeyResult::LockedWithConflict` no matter how `need_value`
/// and `need_check_existence` are set, and the `for_update_ts` in
/// the actually-written lock will be equal to the `commit_ts` of the latest
/// Write record found on the key.
///
/// The second return value will also contains the previous value of the key if
/// `need_old_value` is set, or `OldValue::Unspecified` otherwise.
pub fn acquire_pessimistic_lock<S: Snapshot>(
    txn: &mut MvccTxn,
    reader: &mut SnapshotReader<S>,
    key: Key,
    primary: &[u8],
    should_not_exist: bool,
    lock_ttl: u64,
    mut for_update_ts: TimeStamp,
    need_value: bool,
    need_check_existence: bool,
    min_commit_ts: TimeStamp,
    need_old_value: bool,
    lock_only_if_exists: bool,
    allow_lock_with_conflict: bool,
) -> MvccResult<(PessimisticLockKeyResult, OldValue)> {
    fail_point!("acquire_pessimistic_lock", |err| Err(
        crate::storage::mvcc::txn::make_txn_error(err, &key, reader.start_ts).into()
    ));
    if lock_only_if_exists && !need_value {
        error!(
            "lock_only_if_exists of a pessimistic lock request is set to true, but return_value is not";
            "start_ts" => reader.start_ts,
            "key" => log_wrappers::Value::key(key.as_encoded()),
        );
        return Err(ErrorInner::LockIfExistsFailed {
            start_ts: reader.start_ts,
            key: key.into_raw()?,
        }
        .into());
    }
    // If any of `should_not_exist`, `need_value`, `need_check_existence` is set,
    // it infers a read to the value, in which case max_ts need to be updated to
    // guarantee the linearizability and snapshot isolation.
    if should_not_exist || need_value || need_check_existence {
        txn.concurrency_manager.update_max_ts(for_update_ts);
    }

    // When `need_value` is set, the value need to be loaded of course. If
    // `need_check_existence` and `need_old_value` are both set, we also load
    // the value even if `need_value` is false, so that it avoids
    // `load_old_value` doing repeated work.
    let mut need_load_value = need_value || (need_check_existence && need_old_value);

    fn load_old_value<S: Snapshot>(
        need_old_value: bool,
        value_loaded: bool,
        val: Option<&Value>,
        reader: &mut SnapshotReader<S>,
        key: &Key,
        for_update_ts: TimeStamp,
        prev_write_loaded: bool,
        prev_write: Option<Write>,
    ) -> MvccResult<OldValue> {
        if !need_old_value {
            return Ok(OldValue::Unspecified);
        }
        if value_loaded {
            // The old value must be loaded to `val` when `need_value` is set.
            Ok(match val {
                Some(val) => OldValue::Value { value: val.clone() },
                None => OldValue::None,
            })
        } else {
            reader.get_old_value(key, for_update_ts, prev_write_loaded, prev_write)
        }
    }

    let mut val = None;
    if let Some(lock) = reader.load_lock(&key)? {
        if lock.ts != reader.start_ts {
            return Err(ErrorInner::KeyIsLocked(lock.into_lock_info(key.into_raw()?)).into());
        }
        if lock.lock_type != LockType::Pessimistic {
            return Err(ErrorInner::LockTypeNotMatch {
                start_ts: reader.start_ts,
                key: key.into_raw()?,
                pessimistic: false,
            }
            .into());
        }

        let locked_with_conflict_ts =
            if allow_lock_with_conflict && for_update_ts < lock.for_update_ts {
                // If the key is already locked by the same transaction with larger
                // for_update_ts, and the current request has
                // `allow_lock_with_conflict` set, we must consider
                // these possibilities:
                // * If a previous request successfully locked the key with conflict, but the
                //   response is lost due to some errors such as RPC failures. In this case, we
                //   return like the current request's result is locked_with_conflict, for
                //   idempotency concern.
                // * The key is locked by a newer request with larger for_update_ts, and the
                //   current request is stale. We can't distinguish this case with the above
                //   one, but we don't need to handle this case since no one would need the
                //   current request's result anymore.

                // Load value if locked_with_conflict, so that when the client (TiDB) need to
                // read the value during statement retry, it will be possible to read the value
                // from cache instead of RPC.
                need_load_value = true;
                for_update_ts = lock.for_update_ts;
                Some(lock.for_update_ts)
            } else {
                None
            };

        if need_load_value {
            val = reader.get(&key, for_update_ts)?;
        } else if need_check_existence {
            val = reader.get_write(&key, for_update_ts)?.map(|_| vec![]);
        }
        // Pervious write is not loaded.
        let (prev_write_loaded, prev_write) = (false, None);
        let old_value = load_old_value(
            need_old_value,
            need_load_value,
            val.as_ref(),
            reader,
            &key,
            for_update_ts,
            prev_write_loaded,
            prev_write,
        )?;

        // Overwrite the lock with small for_update_ts
        if for_update_ts > lock.for_update_ts {
            let lock = PessimisticLock {
                primary: primary.into(),
                start_ts: reader.start_ts,
                ttl: lock_ttl,
                for_update_ts,
                min_commit_ts,
                last_change_ts: lock.last_change_ts,
                versions_to_last_change: lock.versions_to_last_change,
            };
            txn.put_pessimistic_lock(key, lock, false);
        } else {
            MVCC_DUPLICATE_CMD_COUNTER_VEC
                .acquire_pessimistic_lock
                .inc();
        }
        return Ok((
            PessimisticLockKeyResult::new_success(
                need_value,
                need_check_existence,
                locked_with_conflict_ts,
                val,
            ),
            old_value,
        ));
    }

    let mut locked_with_conflict_ts = None;

    // Following seek_write read the previous write.
    let (prev_write_loaded, mut prev_write) = (true, None);
    let (mut last_change_ts, mut versions_to_last_change);
    if let Some((commit_ts, write)) = reader.seek_write(&key, TimeStamp::max())? {
        // Find a previous write.
        if need_old_value {
            prev_write = Some(write.clone());
        }

        // The isolation level of pessimistic transactions is RC. `for_update_ts` is
        // the commit_ts of the data this transaction read. If exists a commit version
        // whose commit timestamp is larger than current `for_update_ts`, the
        // transaction should retry to get the latest data.
        if commit_ts > for_update_ts {
            MVCC_CONFLICT_COUNTER
                .acquire_pessimistic_lock_conflict
                .inc();
            if allow_lock_with_conflict {
                // TODO: New metrics.
                locked_with_conflict_ts = Some(commit_ts);
                for_update_ts = commit_ts;
                need_load_value = true;
            } else {
                return Err(ErrorInner::WriteConflict {
                    start_ts: reader.start_ts,
                    conflict_start_ts: write.start_ts,
                    conflict_commit_ts: commit_ts,
                    key: key.into_raw()?,
                    primary: primary.to_vec(),
                    reason: WriteConflictReason::PessimisticRetry,
                }
                .into());
            }
        }

        // Handle rollback.
        // The rollback information may come from either a Rollback record or a record
        // with `has_overlapped_rollback` flag.
        if commit_ts == reader.start_ts
            && (write.write_type == WriteType::Rollback || write.has_overlapped_rollback)
        {
            assert!(write.has_overlapped_rollback || write.start_ts == commit_ts);
            return Err(ErrorInner::PessimisticLockRolledBack {
                start_ts: reader.start_ts,
                key: key.into_raw()?,
            }
            .into());
        }
        // If `commit_ts` we seek is already before `start_ts`, the rollback must not
        // exist.
        if commit_ts > reader.start_ts {
            if let Some((older_commit_ts, older_write)) =
                reader.seek_write(&key, reader.start_ts)?
            {
                if older_commit_ts == reader.start_ts
                    && (older_write.write_type == WriteType::Rollback
                        || older_write.has_overlapped_rollback)
                {
                    return Err(ErrorInner::PessimisticLockRolledBack {
                        start_ts: reader.start_ts,
                        key: key.into_raw()?,
                    }
                    .into());
                }
            }
        }

        // Check data constraint when acquiring pessimistic lock. But in case we are
        // going to lock it with write conflict, we do not check it since the
        // statement will then retry.
        if locked_with_conflict_ts.is_none() {
            check_data_constraint(reader, should_not_exist, &write, commit_ts, &key)?;
        }

        (last_change_ts, versions_to_last_change) = write.next_last_change_info(commit_ts);

        // Load value if locked_with_conflict, so that when the client (TiDB) need to
        // read the value during statement retry, it will be possible to read the value
        // from cache instead of RPC.
        if need_value || need_check_existence || locked_with_conflict_ts.is_some() {
            val = match write.write_type {
                // If it's a valid Write, no need to read again.
                WriteType::Put
                    if write
                        .as_ref()
                        .check_gc_fence_as_latest_version(reader.start_ts) =>
                {
                    if need_load_value {
                        Some(reader.load_data(&key, write)?)
                    } else {
                        Some(vec![])
                    }
                }
                WriteType::Delete | WriteType::Put => None,
                WriteType::Lock | WriteType::Rollback => {
                    if need_load_value {
                        reader.get(&key, commit_ts.prev())?
                    } else {
                        reader.get_write(&key, commit_ts.prev())?.map(|_| vec![])
                    }
                }
            };
        }
    } else {
        // last_change_ts == 0 && versions_to_last_change > 0 means the key actually
        // does not exist.
        (last_change_ts, versions_to_last_change) = (TimeStamp::zero(), 1);
    }
    if !tls_can_enable(LAST_CHANGE_TS) {
        (last_change_ts, versions_to_last_change) = (TimeStamp::zero(), 0);
    }

    let old_value = load_old_value(
        need_old_value,
        need_load_value,
        val.as_ref(),
        reader,
        &key,
        for_update_ts,
        prev_write_loaded,
        prev_write,
    )?;
    let lock = PessimisticLock {
        primary: primary.into(),
        start_ts: reader.start_ts,
        ttl: lock_ttl,
        for_update_ts,
        min_commit_ts,
        last_change_ts,
        versions_to_last_change,
    };

    // When lock_only_if_exists is false, always acquire pessimistic lock, otherwise
    // do it when val exists
    if !lock_only_if_exists || val.is_some() {
        txn.put_pessimistic_lock(key, lock, true);
    }
    // TODO don't we need to commit the modifies in txn?

    Ok((
        PessimisticLockKeyResult::new_success(
            need_value,
            need_check_existence,
            locked_with_conflict_ts,
            val,
        ),
        old_value,
    ))
}

pub mod tests {
    use concurrency_manager::ConcurrencyManager;
    use kvproto::kvrpcpb::Context;
    #[cfg(test)]
    use kvproto::kvrpcpb::PrewriteRequestPessimisticAction::*;
    use txn_types::{Lock, TimeStamp};

    use super::*;
    use crate::storage::{
        kv::WriteData,
        mvcc::{Error as MvccError, MvccReader},
        Engine,
    };
    #[cfg(test)]
    use crate::storage::{
        mvcc::tests::*,
        txn::actions::prewrite::tests::{
            old_value_put_delete_lock_insert, old_value_random, OldValueRandomTest,
        },
        txn::commands::pessimistic_rollback,
        txn::tests::*,
        TestEngineBuilder,
    };

    #[cfg(test)]
    pub fn acquire_pessimistic_lock_allow_lock_with_conflict<E: Engine>(
        engine: &mut E,
        key: &[u8],
        pk: &[u8],
        start_ts: impl Into<TimeStamp>,
        for_update_ts: impl Into<TimeStamp>,
        need_value: bool,
        need_check_existence: bool,
    ) -> MvccResult<PessimisticLockKeyResult> {
        let ctx = Context::default();
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let cm = ConcurrencyManager::new(0.into());
        let start_ts = start_ts.into();
        let mut txn = MvccTxn::new(start_ts, cm);
        let mut reader = SnapshotReader::new(start_ts, snapshot, true);
        let res = acquire_pessimistic_lock(
            &mut txn,
            &mut reader,
            Key::from_raw(key),
            pk,
            false,
            1,
            for_update_ts.into(),
            need_value,
            need_check_existence,
            0.into(),
            false,
            false,
            true,
        );
        if res.is_ok() {
            let modifies = txn.into_modifies();
            if !modifies.is_empty() {
                engine
                    .write(&ctx, WriteData::from_modifies(modifies))
                    .unwrap();
            }
        }
        res.map(|r| r.0)
    }

    #[cfg(test)]
    pub fn must_succeed_allow_lock_with_conflict<E: Engine>(
        engine: &mut E,
        key: &[u8],
        pk: &[u8],
        start_ts: impl Into<TimeStamp>,
        for_update_ts: impl Into<TimeStamp>,
        need_value: bool,
        need_check_existence: bool,
    ) -> PessimisticLockKeyResult {
        acquire_pessimistic_lock_allow_lock_with_conflict(
            engine,
            key,
            pk,
            start_ts,
            for_update_ts,
            need_value,
            need_check_existence,
        )
        .unwrap()
    }

    pub fn must_succeed_impl<E: Engine>(
        engine: &mut E,
        key: &[u8],
        pk: &[u8],
        start_ts: impl Into<TimeStamp>,
        should_not_exist: bool,
        lock_ttl: u64,
        for_update_ts: impl Into<TimeStamp>,
        need_value: bool,
        need_check_existence: bool,
        min_commit_ts: impl Into<TimeStamp>,
        lock_only_if_exists: bool,
    ) -> Option<Value> {
        let ctx = Context::default();
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let min_commit_ts = min_commit_ts.into();
        let cm = ConcurrencyManager::new(min_commit_ts);
        let start_ts = start_ts.into();
        let mut txn = MvccTxn::new(start_ts, cm);
        let mut reader = SnapshotReader::new(start_ts, snapshot, true);
        let res = acquire_pessimistic_lock(
            &mut txn,
            &mut reader,
            Key::from_raw(key),
            pk,
            should_not_exist,
            lock_ttl,
            for_update_ts.into(),
            need_value,
            need_check_existence,
            min_commit_ts,
            false,
            lock_only_if_exists,
            false,
        )
        .unwrap();
        let modifies = txn.into_modifies();
        if !modifies.is_empty() {
            engine
                .write(&ctx, WriteData::from_modifies(modifies))
                .unwrap();
        }
        // TODO: Adapt to new interface
        match res.0 {
            PessimisticLockKeyResult::Value(v) => v,
            PessimisticLockKeyResult::Existence(e) => {
                if e {
                    Some(vec![])
                } else {
                    None
                }
            }
            PessimisticLockKeyResult::Empty => None,
            res => panic!("unexpected result: {:?}", res),
        }
    }

    pub fn must_succeed<E: Engine>(
        engine: &mut E,
        key: &[u8],
        pk: &[u8],
        start_ts: impl Into<TimeStamp>,
        for_update_ts: impl Into<TimeStamp>,
    ) {
        must_succeed_with_ttl(engine, key, pk, start_ts, for_update_ts, 0);
    }

    pub fn must_succeed_return_value<E: Engine>(
        engine: &mut E,
        key: &[u8],
        pk: &[u8],
        start_ts: impl Into<TimeStamp>,
        for_update_ts: impl Into<TimeStamp>,
        lock_only_if_exists: bool,
    ) -> Option<Value> {
        must_succeed_impl(
            engine,
            key,
            pk,
            start_ts,
            false,
            0,
            for_update_ts.into(),
            true,
            false,
            TimeStamp::zero(),
            lock_only_if_exists,
        )
    }

    pub fn must_succeed_with_ttl<E: Engine>(
        engine: &mut E,
        key: &[u8],
        pk: &[u8],
        start_ts: impl Into<TimeStamp>,
        for_update_ts: impl Into<TimeStamp>,
        ttl: u64,
    ) {
        assert!(
            must_succeed_impl(
                engine,
                key,
                pk,
                start_ts,
                false,
                ttl,
                for_update_ts.into(),
                false,
                false,
                TimeStamp::zero(),
                false,
            )
            .is_none()
        );
    }

    pub fn must_succeed_for_large_txn<E: Engine>(
        engine: &mut E,
        key: &[u8],
        pk: &[u8],
        start_ts: impl Into<TimeStamp>,
        for_update_ts: impl Into<TimeStamp>,
        lock_ttl: u64,
    ) {
        let for_update_ts = for_update_ts.into();
        let min_commit_ts = for_update_ts.next();
        must_succeed_impl(
            engine,
            key,
            pk,
            start_ts,
            false,
            lock_ttl,
            for_update_ts,
            false,
            false,
            min_commit_ts,
            false,
        );
    }

    pub fn must_err<E: Engine>(
        engine: &mut E,
        key: &[u8],
        pk: &[u8],
        start_ts: impl Into<TimeStamp>,
        for_update_ts: impl Into<TimeStamp>,
    ) -> MvccError {
        must_err_impl(
            engine,
            key,
            pk,
            start_ts,
            false,
            for_update_ts,
            false,
            false,
            TimeStamp::zero(),
            false,
        )
    }

    pub fn must_err_return_value<E: Engine>(
        engine: &mut E,
        key: &[u8],
        pk: &[u8],
        start_ts: impl Into<TimeStamp>,
        for_update_ts: impl Into<TimeStamp>,
        lock_only_if_exists: bool,
    ) -> MvccError {
        must_err_impl(
            engine,
            key,
            pk,
            start_ts,
            false,
            for_update_ts,
            true,
            false,
            TimeStamp::zero(),
            lock_only_if_exists,
        )
    }

    fn must_err_impl<E: Engine>(
        engine: &mut E,
        key: &[u8],
        pk: &[u8],
        start_ts: impl Into<TimeStamp>,
        should_not_exist: bool,
        for_update_ts: impl Into<TimeStamp>,
        need_value: bool,
        need_check_existence: bool,
        min_commit_ts: impl Into<TimeStamp>,
        lock_only_if_exists: bool,
    ) -> MvccError {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let min_commit_ts = min_commit_ts.into();
        let cm = ConcurrencyManager::new(min_commit_ts);
        let start_ts = start_ts.into();
        let mut txn = MvccTxn::new(start_ts, cm);
        let mut reader = SnapshotReader::new(start_ts, snapshot, true);
        acquire_pessimistic_lock(
            &mut txn,
            &mut reader,
            Key::from_raw(key),
            pk,
            should_not_exist,
            0,
            for_update_ts.into(),
            need_value,
            need_check_existence,
            min_commit_ts,
            false,
            lock_only_if_exists,
            false,
        )
        .unwrap_err()
    }

    pub fn must_pessimistic_locked<E: Engine>(
        engine: &mut E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        for_update_ts: impl Into<TimeStamp>,
    ) -> Lock {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true);
        let lock = reader.load_lock(&Key::from_raw(key)).unwrap().unwrap();
        assert_eq!(lock.ts, start_ts.into());
        assert_eq!(lock.for_update_ts, for_update_ts.into());
        assert_eq!(lock.lock_type, LockType::Pessimistic);
        lock
    }

    #[test]
    fn test_pessimistic_lock() {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        let k = b"k1";
        let v = b"v1";

        // TODO: Some corner cases don't give proper results. Although they are not
        // important, we should consider whether they are better to be fixed.

        // Normal
        must_succeed(&mut engine, k, k, 1, 1);
        must_pessimistic_locked(&mut engine, k, 1, 1);
        must_pessimistic_prewrite_put(&mut engine, k, v, k, 1, 1, DoPessimisticCheck);
        must_locked(&mut engine, k, 1);
        must_commit(&mut engine, k, 1, 2);
        must_unlocked(&mut engine, k);

        // Lock conflict
        must_prewrite_put(&mut engine, k, v, k, 3);
        must_err(&mut engine, k, k, 4, 4);
        must_cleanup(&mut engine, k, 3, 0);
        must_unlocked(&mut engine, k);
        must_succeed(&mut engine, k, k, 5, 5);
        must_prewrite_lock_err(&mut engine, k, k, 6);
        must_err(&mut engine, k, k, 6, 6);
        must_cleanup(&mut engine, k, 5, 0);
        must_unlocked(&mut engine, k);

        // Data conflict
        must_prewrite_put(&mut engine, k, v, k, 7);
        must_commit(&mut engine, k, 7, 9);
        must_unlocked(&mut engine, k);
        must_prewrite_lock_err(&mut engine, k, k, 8);
        must_err(&mut engine, k, k, 8, 8);
        must_succeed(&mut engine, k, k, 8, 9);
        must_pessimistic_prewrite_put(&mut engine, k, v, k, 8, 8, DoPessimisticCheck);
        must_commit(&mut engine, k, 8, 10);
        must_unlocked(&mut engine, k);

        // Rollback
        must_succeed(&mut engine, k, k, 11, 11);
        must_pessimistic_locked(&mut engine, k, 11, 11);
        must_cleanup(&mut engine, k, 11, 0);
        must_err(&mut engine, k, k, 11, 11);
        must_pessimistic_prewrite_put_err(&mut engine, k, v, k, 11, 11, DoPessimisticCheck);
        must_prewrite_lock_err(&mut engine, k, k, 11);
        must_unlocked(&mut engine, k);

        must_succeed(&mut engine, k, k, 12, 12);
        must_pessimistic_prewrite_put(&mut engine, k, v, k, 12, 12, DoPessimisticCheck);
        must_locked(&mut engine, k, 12);
        must_cleanup(&mut engine, k, 12, 0);
        must_err(&mut engine, k, k, 12, 12);
        must_pessimistic_prewrite_put_err(&mut engine, k, v, k, 12, 12, DoPessimisticCheck);
        must_prewrite_lock_err(&mut engine, k, k, 12);
        must_unlocked(&mut engine, k);

        // Duplicated
        must_succeed(&mut engine, k, k, 13, 13);
        must_pessimistic_locked(&mut engine, k, 13, 13);
        must_succeed(&mut engine, k, k, 13, 13);
        must_pessimistic_locked(&mut engine, k, 13, 13);
        must_pessimistic_prewrite_put(&mut engine, k, v, k, 13, 13, DoPessimisticCheck);
        must_locked(&mut engine, k, 13);
        must_pessimistic_prewrite_put(&mut engine, k, v, k, 13, 13, DoPessimisticCheck);
        must_locked(&mut engine, k, 13);
        must_commit(&mut engine, k, 13, 14);
        must_unlocked(&mut engine, k);
        must_commit(&mut engine, k, 13, 14);
        must_unlocked(&mut engine, k);

        // Pessimistic lock doesn't block reads.
        must_succeed(&mut engine, k, k, 15, 15);
        must_pessimistic_locked(&mut engine, k, 15, 15);
        must_get(&mut engine, k, 16, v);
        must_pessimistic_prewrite_delete(&mut engine, k, k, 15, 15, DoPessimisticCheck);
        must_get_err(&mut engine, k, 16);
        must_commit(&mut engine, k, 15, 17);

        // Rollback
        must_succeed(&mut engine, k, k, 18, 18);
        must_rollback(&mut engine, k, 18, false);
        must_unlocked(&mut engine, k);
        must_prewrite_put(&mut engine, k, v, k, 19);
        must_commit(&mut engine, k, 19, 20);
        must_err(&mut engine, k, k, 18, 21);
        must_unlocked(&mut engine, k);

        // LockTypeNotMatch
        must_prewrite_put(&mut engine, k, v, k, 23);
        must_locked(&mut engine, k, 23);
        must_err(&mut engine, k, k, 23, 23);
        must_cleanup(&mut engine, k, 23, 0);
        must_succeed(&mut engine, k, k, 24, 24);
        must_pessimistic_locked(&mut engine, k, 24, 24);
        must_prewrite_put_err(&mut engine, k, v, k, 24);
        must_rollback(&mut engine, k, 24, false);

        // Acquire lock on a prewritten key should fail.
        must_succeed(&mut engine, k, k, 26, 26);
        must_pessimistic_locked(&mut engine, k, 26, 26);
        must_pessimistic_prewrite_delete(&mut engine, k, k, 26, 26, DoPessimisticCheck);
        must_locked(&mut engine, k, 26);
        must_err(&mut engine, k, k, 26, 26);
        must_locked(&mut engine, k, 26);

        // Acquire lock on a committed key should fail.
        must_commit(&mut engine, k, 26, 27);
        must_unlocked(&mut engine, k);
        must_get_none(&mut engine, k, 28);
        must_err(&mut engine, k, k, 26, 26);
        must_unlocked(&mut engine, k);
        must_get_none(&mut engine, k, 28);
        // Pessimistic prewrite on a committed key should fail.
        must_pessimistic_prewrite_put_err(&mut engine, k, v, k, 26, 26, DoPessimisticCheck);
        must_unlocked(&mut engine, k);
        must_get_none(&mut engine, k, 28);
        // Currently we cannot avoid this.
        must_succeed(&mut engine, k, k, 26, 29);
        pessimistic_rollback::tests::must_success(&mut engine, k, 26, 29);
        must_unlocked(&mut engine, k);

        // Non pessimistic key in pessimistic transaction.
        must_pessimistic_prewrite_put(&mut engine, k, v, k, 30, 30, SkipPessimisticCheck);
        must_locked(&mut engine, k, 30);
        must_commit(&mut engine, k, 30, 31);
        must_unlocked(&mut engine, k);
        must_get_commit_ts(&mut engine, k, 30, 31);

        // Rollback collapsed.
        must_rollback(&mut engine, k, 32, false);
        must_rollback(&mut engine, k, 33, false);
        must_err(&mut engine, k, k, 32, 32);
        // Currently we cannot avoid this.
        must_succeed(&mut engine, k, k, 32, 34);
        pessimistic_rollback::tests::must_success(&mut engine, k, 32, 34);
        must_unlocked(&mut engine, k);

        // Acquire lock when there is lock with different for_update_ts.
        must_succeed(&mut engine, k, k, 35, 36);
        must_pessimistic_locked(&mut engine, k, 35, 36);
        must_succeed(&mut engine, k, k, 35, 35);
        must_pessimistic_locked(&mut engine, k, 35, 36);
        must_succeed(&mut engine, k, k, 35, 37);
        must_pessimistic_locked(&mut engine, k, 35, 37);

        // Cannot prewrite when there is another transaction's pessimistic lock.
        must_pessimistic_prewrite_put_err(&mut engine, k, v, k, 36, 36, DoPessimisticCheck);
        must_pessimistic_prewrite_put_err(&mut engine, k, v, k, 36, 38, DoPessimisticCheck);
        must_pessimistic_locked(&mut engine, k, 35, 37);
        // Cannot prewrite when there is another transaction's non-pessimistic lock.
        must_pessimistic_prewrite_put(&mut engine, k, v, k, 35, 37, DoPessimisticCheck);
        must_locked(&mut engine, k, 35);
        must_pessimistic_prewrite_put_err(&mut engine, k, v, k, 36, 38, DoPessimisticCheck);
        must_locked(&mut engine, k, 35);

        // Commit pessimistic transaction's key but with smaller commit_ts than
        // for_update_ts. Currently not checked, so in this case it will
        // actually be successfully committed.
        must_commit(&mut engine, k, 35, 36);
        must_unlocked(&mut engine, k);
        must_get_commit_ts(&mut engine, k, 35, 36);

        // Prewrite meets pessimistic lock on a non-pessimistic key.
        // Currently not checked, so prewrite will success.
        must_succeed(&mut engine, k, k, 40, 40);
        must_pessimistic_locked(&mut engine, k, 40, 40);
        must_pessimistic_prewrite_put(&mut engine, k, v, k, 40, 40, SkipPessimisticCheck);
        must_locked(&mut engine, k, 40);
        must_commit(&mut engine, k, 40, 41);
        must_unlocked(&mut engine, k);

        // Prewrite with different for_update_ts.
        // Currently not checked.
        must_succeed(&mut engine, k, k, 42, 45);
        must_pessimistic_locked(&mut engine, k, 42, 45);
        must_pessimistic_prewrite_put(&mut engine, k, v, k, 42, 43, DoPessimisticCheck);
        must_locked(&mut engine, k, 42);
        must_commit(&mut engine, k, 42, 45);
        must_unlocked(&mut engine, k);

        must_succeed(&mut engine, k, k, 46, 47);
        must_pessimistic_locked(&mut engine, k, 46, 47);
        must_pessimistic_prewrite_put(&mut engine, k, v, k, 46, 48, DoPessimisticCheck);
        must_locked(&mut engine, k, 46);
        must_commit(&mut engine, k, 46, 50);
        must_unlocked(&mut engine, k);

        // Prewrite on non-pessimistic key meets write with larger commit_ts than
        // current for_update_ts (non-pessimistic data conflict).
        // Normally non-pessimistic keys in pessimistic transactions are used when we
        // are sure that there won't be conflicts. So this case is also not checked, and
        // prewrite will succeeed.
        must_pessimistic_prewrite_put(&mut engine, k, v, k, 47, 48, SkipPessimisticCheck);
        must_locked(&mut engine, k, 47);
        must_cleanup(&mut engine, k, 47, 0);
        must_unlocked(&mut engine, k);

        // The rollback of the primary key in a pessimistic transaction should be
        // protected from being collapsed.
        must_succeed(&mut engine, k, k, 49, 60);
        must_pessimistic_prewrite_put(&mut engine, k, v, k, 49, 60, DoPessimisticCheck);
        must_locked(&mut engine, k, 49);
        must_cleanup(&mut engine, k, 49, 0);
        must_get_rollback_protected(&mut engine, k, 49, true);
        must_prewrite_put(&mut engine, k, v, k, 51);
        must_rollback(&mut engine, k, 51, false);
        must_err(&mut engine, k, k, 49, 60);

        // Overlapped rollback record will be written when the current start_ts equals
        // to another write records' commit ts. Now there is a commit record with
        // commit_ts = 50.
        must_succeed(&mut engine, k, k, 50, 61);
        must_pessimistic_prewrite_put(&mut engine, k, v, k, 50, 61, DoPessimisticCheck);
        must_locked(&mut engine, k, 50);
        must_cleanup(&mut engine, k, 50, 0);
        must_get_overlapped_rollback(&mut engine, k, 50, 46, WriteType::Put, Some(0));

        // start_ts and commit_ts interlacing
        for start_ts in &[140, 150, 160] {
            let for_update_ts = start_ts + 48;
            let commit_ts = start_ts + 50;
            must_succeed(&mut engine, k, k, *start_ts, for_update_ts);
            must_pessimistic_prewrite_put(
                &mut engine,
                k,
                v,
                k,
                *start_ts,
                for_update_ts,
                DoPessimisticCheck,
            );
            must_commit(&mut engine, k, *start_ts, commit_ts);
            must_get(&mut engine, k, commit_ts + 1, v);
        }

        must_rollback(&mut engine, k, 170, false);

        // Now the data should be like: (start_ts -> commit_ts)
        // 140 -> 190
        // 150 -> 200
        // 160 -> 210
        // 170 -> rollback
        must_get_commit_ts(&mut engine, k, 140, 190);
        must_get_commit_ts(&mut engine, k, 150, 200);
        must_get_commit_ts(&mut engine, k, 160, 210);
        must_get_rollback_ts(&mut engine, k, 170);
    }

    #[test]
    fn test_pessimistic_lock_return_value() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let (k, v) = (b"k", b"v");

        assert_eq!(
            must_succeed_return_value(&mut engine, k, k, 10, 10, false),
            None
        );
        must_pessimistic_locked(&mut engine, k, 10, 10);
        pessimistic_rollback::tests::must_success(&mut engine, k, 10, 10);

        // Put
        must_prewrite_put(&mut engine, k, v, k, 10);
        // KeyIsLocked
        match must_err_return_value(&mut engine, k, k, 20, 20, false) {
            MvccError(box ErrorInner::KeyIsLocked(_)) => (),
            e => panic!("unexpected error: {}", e),
        };
        must_commit(&mut engine, k, 10, 20);
        // WriteConflict
        match must_err_return_value(&mut engine, k, k, 15, 15, false) {
            MvccError(box ErrorInner::WriteConflict { .. }) => (),
            e => panic!("unexpected error: {}", e),
        };
        assert_eq!(
            must_succeed_return_value(&mut engine, k, k, 25, 25, false),
            Some(v.to_vec())
        );
        must_pessimistic_locked(&mut engine, k, 25, 25);
        pessimistic_rollback::tests::must_success(&mut engine, k, 25, 25);

        // Skip Write::Lock
        must_prewrite_lock(&mut engine, k, k, 30);
        must_commit(&mut engine, k, 30, 40);
        assert_eq!(
            must_succeed_return_value(&mut engine, k, k, 45, 45, false),
            Some(v.to_vec())
        );
        must_pessimistic_locked(&mut engine, k, 45, 45);
        pessimistic_rollback::tests::must_success(&mut engine, k, 45, 45);

        // Skip Write::Rollback
        must_rollback(&mut engine, k, 50, false);
        assert_eq!(
            must_succeed_return_value(&mut engine, k, k, 55, 55, false),
            Some(v.to_vec())
        );
        must_pessimistic_locked(&mut engine, k, 55, 55);
        pessimistic_rollback::tests::must_success(&mut engine, k, 55, 55);

        // Delete
        must_prewrite_delete(&mut engine, k, k, 60);
        must_commit(&mut engine, k, 60, 70);
        assert_eq!(
            must_succeed_return_value(&mut engine, k, k, 75, 75, false),
            None
        );
        // Duplicated command
        assert_eq!(
            must_succeed_return_value(&mut engine, k, k, 75, 75, false),
            None
        );
        assert_eq!(
            must_succeed_return_value(&mut engine, k, k, 75, 55, false),
            Some(v.to_vec())
        );
        must_pessimistic_locked(&mut engine, k, 75, 75);
        pessimistic_rollback::tests::must_success(&mut engine, k, 75, 75);
    }

    #[test]
    fn test_pessimistic_lock_only_if_exists() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let (k, v) = (b"k", b"v");

        // The key doesn't exist, no pessimistic lock is generated
        assert_eq!(
            must_succeed_return_value(&mut engine, k, k, 10, 10, true),
            None
        );
        must_unlocked(&mut engine, k);

        match must_err_impl(
            &mut engine,
            k,
            k,
            10,
            false,
            10,
            false,
            false,
            TimeStamp::zero(),
            true,
        ) {
            MvccError(box ErrorInner::LockIfExistsFailed {
                start_ts: _,
                key: _,
            }) => (),
            e => panic!("unexpected error: {}", e),
        };

        // Put the value, writecf: k_20_put_v
        must_prewrite_put(&mut engine, k, v, k, 10);
        must_commit(&mut engine, k, 10, 20);
        // Pessimistic lock generated
        assert_eq!(
            must_succeed_return_value(&mut engine, k, k, 25, 25, true),
            Some(v.to_vec())
        );
        must_pessimistic_locked(&mut engine, k, 25, 25);
        pessimistic_rollback::tests::must_success(&mut engine, k, 25, 25);

        // Skip Write::Lock, WriteRecord: k_20_put_v k_40_lock
        must_prewrite_lock(&mut engine, k, k, 30);
        must_commit(&mut engine, k, 30, 40);
        assert_eq!(
            must_succeed_return_value(&mut engine, k, k, 45, 45, true),
            Some(v.to_vec())
        );
        must_pessimistic_locked(&mut engine, k, 45, 45);
        pessimistic_rollback::tests::must_success(&mut engine, k, 45, 45);

        // Skip Write::Rollback WriteRecord: k_20_put_v k_40_lock k_50_R
        must_rollback(&mut engine, k, 50, false);
        assert_eq!(
            must_succeed_return_value(&mut engine, k, k, 55, 55, true),
            Some(v.to_vec())
        );
        must_pessimistic_locked(&mut engine, k, 55, 55);
        pessimistic_rollback::tests::must_success(&mut engine, k, 55, 55);

        // Delete WriteRecord: k_20_put_v k_40_lock k_50_R k_70_delete
        must_prewrite_delete(&mut engine, k, k, 60);
        must_commit(&mut engine, k, 60, 70);
        assert_eq!(
            must_succeed_return_value(&mut engine, k, k, 75, 75, true),
            None
        );
        must_unlocked(&mut engine, k);

        // Duplicated command
        assert_eq!(
            must_succeed_return_value(&mut engine, k, k, 75, 75, false),
            None
        );
        must_pessimistic_locked(&mut engine, k, 75, 75);
        assert_eq!(
            must_succeed_return_value(&mut engine, k, k, 75, 85, true),
            None
        );
        must_pessimistic_locked(&mut engine, k, 75, 85);
        pessimistic_rollback::tests::must_success(&mut engine, k, 75, 85);
        must_unlocked(&mut engine, k);
    }

    #[test]
    fn test_overwrite_pessimistic_lock() {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        let k = b"k1";

        must_succeed(&mut engine, k, k, 1, 2);
        must_pessimistic_locked(&mut engine, k, 1, 2);
        must_succeed(&mut engine, k, k, 1, 1);
        must_pessimistic_locked(&mut engine, k, 1, 2);
        must_succeed(&mut engine, k, k, 1, 3);
        must_pessimistic_locked(&mut engine, k, 1, 3);
    }

    #[test]
    fn test_pessimistic_lock_check_gc_fence() {
        use pessimistic_rollback::tests::must_success as must_pessimistic_rollback;

        let mut engine = TestEngineBuilder::new().build().unwrap();

        // PUT,           Read
        //  `------^
        must_prewrite_put(&mut engine, b"k1", b"v1", b"k1", 10);
        must_commit(&mut engine, b"k1", 10, 30);
        must_cleanup_with_gc_fence(&mut engine, b"k1", 30, 0, 40, true);

        // PUT,           Read
        //  * (GC fence ts = 0)
        must_prewrite_put(&mut engine, b"k2", b"v2", b"k2", 11);
        must_commit(&mut engine, b"k2", 11, 30);
        must_cleanup_with_gc_fence(&mut engine, b"k2", 30, 0, 0, true);

        // PUT, LOCK,   LOCK, Read
        //  `---------^
        must_prewrite_put(&mut engine, b"k3", b"v3", b"k3", 12);
        must_commit(&mut engine, b"k3", 12, 30);
        must_prewrite_lock(&mut engine, b"k3", b"k3", 37);
        must_commit(&mut engine, b"k3", 37, 38);
        must_cleanup_with_gc_fence(&mut engine, b"k3", 30, 0, 40, true);
        must_prewrite_lock(&mut engine, b"k3", b"k3", 42);
        must_commit(&mut engine, b"k3", 42, 43);

        // PUT, LOCK,   LOCK, Read
        //  *
        must_prewrite_put(&mut engine, b"k4", b"v4", b"k4", 13);
        must_commit(&mut engine, b"k4", 13, 30);
        must_prewrite_lock(&mut engine, b"k4", b"k4", 37);
        must_commit(&mut engine, b"k4", 37, 38);
        must_prewrite_lock(&mut engine, b"k4", b"k4", 42);
        must_commit(&mut engine, b"k4", 42, 43);
        must_cleanup_with_gc_fence(&mut engine, b"k4", 30, 0, 0, true);

        // PUT,   PUT,    READ
        //  `-----^ `------^
        must_prewrite_put(&mut engine, b"k5", b"v5", b"k5", 14);
        must_commit(&mut engine, b"k5", 14, 20);
        must_prewrite_put(&mut engine, b"k5", b"v5x", b"k5", 21);
        must_commit(&mut engine, b"k5", 21, 30);
        must_cleanup_with_gc_fence(&mut engine, b"k5", 20, 0, 30, false);
        must_cleanup_with_gc_fence(&mut engine, b"k5", 30, 0, 40, true);

        // PUT,   PUT,    READ
        //  `-----^ *
        must_prewrite_put(&mut engine, b"k6", b"v6", b"k6", 15);
        must_commit(&mut engine, b"k6", 15, 20);
        must_prewrite_put(&mut engine, b"k6", b"v6x", b"k6", 22);
        must_commit(&mut engine, b"k6", 22, 30);
        must_cleanup_with_gc_fence(&mut engine, b"k6", 20, 0, 30, false);
        must_cleanup_with_gc_fence(&mut engine, b"k6", 30, 0, 0, true);

        // PUT,  LOCK,    READ
        //  `----------^
        // Note that this case is special because usually the `LOCK` is the first write
        // already got during prewrite/acquire_pessimistic_lock and will continue
        // searching an older version from the `LOCK` record.
        must_prewrite_put(&mut engine, b"k7", b"v7", b"k7", 16);
        must_commit(&mut engine, b"k7", 16, 30);
        must_prewrite_lock(&mut engine, b"k7", b"k7", 37);
        must_commit(&mut engine, b"k7", 37, 38);
        must_cleanup_with_gc_fence(&mut engine, b"k7", 30, 0, 40, true);

        let cases = vec![
            (b"k1" as &[u8], None),
            (b"k2", Some(b"v2" as &[u8])),
            (b"k3", None),
            (b"k4", Some(b"v4")),
            (b"k5", None),
            (b"k6", Some(b"v6x")),
            (b"k7", None),
        ];

        for (key, expected_value) in cases {
            // Test constraint check with `should_not_exist`.
            if expected_value.is_none() {
                assert!(
                    must_succeed_impl(
                        &mut engine,
                        key,
                        key,
                        50,
                        true,
                        0,
                        50,
                        false,
                        false,
                        51,
                        false
                    )
                    .is_none()
                );
                must_pessimistic_rollback(&mut engine, key, 50, 51);
            } else {
                must_err_impl(&mut engine, key, key, 50, true, 50, false, false, 51, false);
            }
            must_unlocked(&mut engine, key);

            // Test getting value.
            let res = must_succeed_impl(
                &mut engine,
                key,
                key,
                50,
                false,
                0,
                50,
                true,
                false,
                51,
                false,
            );
            assert_eq!(res, expected_value.map(|v| v.to_vec()));
            must_pessimistic_rollback(&mut engine, key, 50, 51);

            // Test getting value when already locked.
            must_succeed(&mut engine, key, key, 50, 51);
            let res2 = must_succeed_impl(
                &mut engine,
                key,
                key,
                50,
                false,
                0,
                50,
                true,
                false,
                51,
                false,
            );
            assert_eq!(res2, expected_value.map(|v| v.to_vec()));
            must_pessimistic_rollback(&mut engine, key, 50, 51);
        }
    }

    #[test]
    fn test_old_value_put_delete_lock_insert() {
        let mut engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let start_ts = old_value_put_delete_lock_insert(&mut engine, b"k1");
        let key = Key::from_raw(b"k1");
        for should_not_exist in &[true, false] {
            for need_value in &[true, false] {
                for need_check_existence in &[true, false] {
                    let snapshot = engine.snapshot(Default::default()).unwrap();
                    let cm = ConcurrencyManager::new(start_ts);
                    let mut txn = MvccTxn::new(start_ts, cm);
                    let mut reader = SnapshotReader::new(start_ts, snapshot, true);
                    let need_old_value = true;
                    let lock_ttl = 0;
                    let for_update_ts = start_ts;
                    let min_commit_ts = 0.into();
                    let (_, old_value) = acquire_pessimistic_lock(
                        &mut txn,
                        &mut reader,
                        key.clone(),
                        key.as_encoded(),
                        *should_not_exist,
                        lock_ttl,
                        for_update_ts,
                        *need_value,
                        *need_check_existence,
                        min_commit_ts,
                        need_old_value,
                        false,
                        false,
                    )
                    .unwrap();
                    assert_eq!(old_value, OldValue::None);
                }
            }
        }
    }

    #[test]
    fn test_old_value_for_update_ts() {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        let k = b"k1";
        let v1 = b"v1";

        // Put v1 @ start ts 1, commit ts 2
        must_succeed(&mut engine, k, k, 1, 1);
        must_pessimistic_prewrite_put(&mut engine, k, v1, k, 1, 1, DoPessimisticCheck);
        must_commit(&mut engine, k, 1, 2);

        let v2 = b"v2";
        // Put v2 @ start ts 10, commit ts 11
        must_succeed(&mut engine, k, k, 10, 10);
        must_pessimistic_prewrite_put(&mut engine, k, v2, k, 10, 10, DoPessimisticCheck);
        must_commit(&mut engine, k, 10, 11);

        // Lock @ start ts 9, for update ts 12, commit ts 13
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let min_commit_ts = TimeStamp::zero();
        let cm = ConcurrencyManager::new(min_commit_ts);
        let start_ts = TimeStamp::new(9);
        let for_update_ts = TimeStamp::new(12);
        let need_old_value = true;
        // Force to read old via reader.
        let need_value = false;
        let need_check_existence = false;
        let mut txn = MvccTxn::new(start_ts, cm.clone());
        let mut reader = SnapshotReader::new(start_ts, snapshot, true);
        let res = acquire_pessimistic_lock(
            &mut txn,
            &mut reader,
            Key::from_raw(k),
            k,
            false,
            0,
            for_update_ts,
            need_value,
            need_check_existence,
            min_commit_ts,
            need_old_value,
            false,
            false,
        )
        .unwrap();
        assert_eq!(
            res.1,
            OldValue::Value {
                value: b"v2".to_vec()
            }
        );

        // Write the lock.
        let modifies = txn.into_modifies();
        if !modifies.is_empty() {
            engine
                .write(&Default::default(), WriteData::from_modifies(modifies))
                .unwrap();
        }

        // Lock again.
        let mut txn = MvccTxn::new(start_ts, cm);
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = SnapshotReader::new(start_ts, snapshot, true);
        let res = acquire_pessimistic_lock(
            &mut txn,
            &mut reader,
            Key::from_raw(k),
            k,
            false,
            0,
            for_update_ts,
            false,
            false,
            min_commit_ts,
            true,
            false,
            false,
        )
        .unwrap();
        assert_eq!(
            res.1,
            OldValue::Value {
                value: b"v2".to_vec()
            }
        );
    }

    #[test]
    fn test_old_value_random() {
        let key = b"k1";
        let mut tests: Vec<OldValueRandomTest> = vec![];
        let mut tests_require_old_value_none: Vec<OldValueRandomTest> = vec![];

        for should_not_exist in &[true, false] {
            for need_value in &[true, false] {
                for need_check_existence in &[true, false] {
                    let should_not_exist = *should_not_exist;
                    let need_value = *need_value;
                    let t = Box::new(move |snapshot, start_ts| {
                        let key = Key::from_raw(key);
                        let cm = ConcurrencyManager::new(start_ts);
                        let mut txn = MvccTxn::new(start_ts, cm);
                        let mut reader = SnapshotReader::new(start_ts, snapshot, true);
                        let need_old_value = true;
                        let lock_ttl = 0;
                        let for_update_ts = start_ts;
                        let min_commit_ts = 0.into();
                        let (_, old_value) = acquire_pessimistic_lock(
                            &mut txn,
                            &mut reader,
                            key.clone(),
                            key.as_encoded(),
                            should_not_exist,
                            lock_ttl,
                            for_update_ts,
                            need_value,
                            *need_check_existence,
                            min_commit_ts,
                            need_old_value,
                            false,
                            false,
                        )?;
                        Ok(old_value)
                    });
                    if should_not_exist {
                        tests_require_old_value_none.push(t);
                    } else {
                        tests.push(t);
                    }
                }
            }
        }
        let require_old_value_none = false;
        old_value_random(key, require_old_value_none, tests);
        let require_old_value_none = true;
        old_value_random(key, require_old_value_none, tests_require_old_value_none);
    }

    #[test]
    fn test_acquire_pessimistic_lock_should_not_exist() {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        let (key, value) = (b"k", b"val");

        // T1: start_ts = 3, commit_ts = 5, put key:value
        must_succeed(&mut engine, key, key, 3, 3);
        must_pessimistic_prewrite_put(&mut engine, key, value, key, 3, 3, DoPessimisticCheck);
        must_commit(&mut engine, key, 3, 5);

        // T2: start_ts = 15, acquire pessimistic lock on k, with should_not_exist flag
        // set.
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let min_commit_ts = TimeStamp::zero();
        let cm = ConcurrencyManager::new(min_commit_ts);
        let start_ts = TimeStamp::new(15);
        let for_update_ts = TimeStamp::new(15);
        let need_old_value = true;
        let need_value = false;
        let need_check_existence = false;
        let mut txn = MvccTxn::new(start_ts, cm.clone());
        let mut reader = SnapshotReader::new(start_ts, snapshot, true);
        let _res = acquire_pessimistic_lock(
            &mut txn,
            &mut reader,
            Key::from_raw(key),
            key,
            true,
            0,
            for_update_ts,
            need_value,
            need_check_existence,
            min_commit_ts,
            need_old_value,
            false,
            false,
        )
        .unwrap_err();

        assert_eq!(cm.max_ts().into_inner(), 15);

        // T3: start_ts = 8, commit_ts = max_ts + 1 = 16, prewrite a DELETE operation on
        // k
        must_succeed(&mut engine, key, key, 8, 8);
        must_pessimistic_prewrite_delete(&mut engine, key, key, 8, 8, DoPessimisticCheck);
        must_commit(&mut engine, key, 8, cm.max_ts().into_inner() + 1);

        // T1: start_ts = 10, repeatedly acquire pessimistic lock on k, with
        // should_not_exist flag set
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let start_ts = TimeStamp::new(10);
        let for_update_ts = TimeStamp::new(10);
        let need_old_value = true;
        let need_value = false;
        let check_existence = false;
        let mut txn = MvccTxn::new(start_ts, cm);
        let mut reader = SnapshotReader::new(start_ts, snapshot, true);
        let _res = acquire_pessimistic_lock(
            &mut txn,
            &mut reader,
            Key::from_raw(key),
            key,
            true,
            0,
            for_update_ts,
            need_value,
            check_existence,
            min_commit_ts,
            need_old_value,
            false,
            false,
        )
        .unwrap_err();
    }

    #[test]
    fn test_check_existence() {
        use pessimistic_rollback::tests::must_success as must_pessimistic_rollback;
        let mut engine = TestEngineBuilder::new().build().unwrap();

        // k1: Not exists

        // k2: Exists
        must_prewrite_put(&mut engine, b"k2", b"v2", b"k2", 5);
        must_commit(&mut engine, b"k2", 5, 20);

        // k3: Delete
        must_prewrite_put(&mut engine, b"k3", b"v3", b"k3", 5);
        must_commit(&mut engine, b"k3", 5, 6);
        must_prewrite_delete(&mut engine, b"k3", b"k3", 7);
        must_commit(&mut engine, b"k3", 7, 20);

        // k4: Exist + Lock + Rollback
        must_prewrite_put(&mut engine, b"k4", b"v4", b"k4", 5);
        must_commit(&mut engine, b"k4", 5, 15);
        must_prewrite_lock(&mut engine, b"k4", b"k4", 16);
        must_commit(&mut engine, b"k4", 16, 17);
        must_rollback(&mut engine, b"k4", 20, true);

        // k5: GC fence invalid
        must_prewrite_put(&mut engine, b"k5", b"v5", b"k5", 5);
        must_commit(&mut engine, b"k5", 5, 6);
        // A invalid gc fence is assumed never pointing to a ts greater than GC
        // safepoint, and a read operation's ts is assumed never less than the
        // GC safepoint. Therefore since we will read at ts=10 later, we can't
        // put a version greater than 10 in this case.
        must_cleanup_with_gc_fence(&mut engine, b"k5", 6, 0, 8, true);

        for &need_value in &[false, true] {
            for &need_check_existence in &[false, true] {
                for &start_ts in &[30u64, 10u64] {
                    for &repeated_request in &[false, true] {
                        println!(
                            "{} {} {} {}",
                            need_value, need_check_existence, start_ts, repeated_request
                        );
                        if repeated_request {
                            for &k in &[b"k1", b"k2", b"k3", b"k4", b"k5"] {
                                must_succeed(&mut engine, k, k, start_ts, 30);
                            }
                        }

                        let expected_value = |value: Option<&[u8]>| {
                            if need_value {
                                value.map(|v| v.to_vec())
                            } else if need_check_existence {
                                value.map(|_| vec![])
                            } else {
                                None
                            }
                        };

                        let value1 = must_succeed_impl(
                            &mut engine,
                            b"k1",
                            b"k1",
                            start_ts,
                            false,
                            1000,
                            30,
                            need_value,
                            need_check_existence,
                            0,
                            false,
                        );
                        assert_eq!(value1, None);
                        must_pessimistic_rollback(&mut engine, b"k1", start_ts, 30);

                        let value2 = must_succeed_impl(
                            &mut engine,
                            b"k2",
                            b"k2",
                            start_ts,
                            false,
                            1000,
                            30,
                            need_value,
                            need_check_existence,
                            0,
                            false,
                        );
                        assert_eq!(value2, expected_value(Some(b"v2")));
                        must_pessimistic_rollback(&mut engine, b"k2", start_ts, 30);

                        let value3 = must_succeed_impl(
                            &mut engine,
                            b"k3",
                            b"k3",
                            start_ts,
                            false,
                            1000,
                            30,
                            need_value,
                            need_check_existence,
                            0,
                            false,
                        );
                        assert_eq!(value3, None);
                        must_pessimistic_rollback(&mut engine, b"k3", start_ts, 30);

                        let value4 = must_succeed_impl(
                            &mut engine,
                            b"k4",
                            b"k4",
                            start_ts,
                            false,
                            1000,
                            30,
                            need_value,
                            need_check_existence,
                            0,
                            false,
                        );
                        assert_eq!(value4, expected_value(Some(b"v4")));
                        must_pessimistic_rollback(&mut engine, b"k4", start_ts, 30);

                        let value5 = must_succeed_impl(
                            &mut engine,
                            b"k5",
                            b"k5",
                            start_ts,
                            false,
                            1000,
                            30,
                            need_value,
                            need_check_existence,
                            0,
                            false,
                        );
                        assert_eq!(value5, None);
                        must_pessimistic_rollback(&mut engine, b"k5", start_ts, 30);
                    }
                }
            }
        }
    }

    #[test]
    fn test_calculate_last_change_ts() {
        use engine_traits::CF_WRITE;
        use pd_client::FeatureGate;

        use crate::storage::txn::sched_pool::set_tls_feature_gate;

        let mut engine = TestEngineBuilder::new().build().unwrap();
        let key = b"k";

        let feature_gate = FeatureGate::default();
        feature_gate.set_version("6.4.0").unwrap();
        set_tls_feature_gate(feature_gate.clone());

        // Latest version is a PUT, but last_change_ts is enabled with cluster version
        // higher than 6.5.0.
        let write = Write::new(WriteType::Put, 15.into(), Some(b"value".to_vec()));
        engine
            .put_cf(
                Default::default(),
                CF_WRITE,
                Key::from_raw(key).append_ts(20.into()),
                write.as_ref().to_bytes(),
            )
            .unwrap();
        must_succeed(&mut engine, key, key, 10, 30);
        let lock = must_pessimistic_locked(&mut engine, key, 10, 30);
        assert_eq!(lock.last_change_ts, TimeStamp::zero());
        assert_eq!(lock.versions_to_last_change, 0);
        pessimistic_rollback::tests::must_success(&mut engine, key, 10, 30);
        // Set cluster version to 6.5.0, last_change_ts should work now.
        feature_gate.set_version("6.5.0").unwrap();
        must_succeed(&mut engine, key, key, 10, 30);
        let lock = must_pessimistic_locked(&mut engine, key, 10, 30);
        assert_eq!(lock.last_change_ts, 20.into());
        assert_eq!(lock.versions_to_last_change, 1);
        pessimistic_rollback::tests::must_success(&mut engine, key, 10, 30);

        // Latest version is a DELETE
        let write = Write::new(WriteType::Delete, 40.into(), None);
        engine
            .put_cf(
                Default::default(),
                CF_WRITE,
                Key::from_raw(key).append_ts(50.into()),
                write.as_ref().to_bytes(),
            )
            .unwrap();
        must_succeed(&mut engine, key, key, 60, 70);
        let lock = must_pessimistic_locked(&mut engine, key, 60, 70);
        assert_eq!(lock.last_change_ts, 50.into());
        assert_eq!(lock.versions_to_last_change, 1);
        pessimistic_rollback::tests::must_success(&mut engine, key, 60, 70);

        // Latest version is a LOCK without last_change_ts
        let write = Write::new(WriteType::Lock, 70.into(), None);
        engine
            .put_cf(
                Default::default(),
                CF_WRITE,
                Key::from_raw(key).append_ts(75.into()),
                write.as_ref().to_bytes(),
            )
            .unwrap();
        must_succeed(&mut engine, key, key, 80, 80);
        let lock = must_pessimistic_locked(&mut engine, key, 80, 80);
        assert!(lock.last_change_ts.is_zero());
        assert_eq!(lock.versions_to_last_change, 0);
        pessimistic_rollback::tests::must_success(&mut engine, key, 80, 80);

        // Latest version is a ROLLBACK without last_change_ts
        let write = Write::new(WriteType::Lock, 90.into(), None);
        engine
            .put_cf(
                Default::default(),
                CF_WRITE,
                Key::from_raw(key).append_ts(90.into()),
                write.as_ref().to_bytes(),
            )
            .unwrap();
        must_succeed(&mut engine, key, key, 95, 95);
        let lock = must_pessimistic_locked(&mut engine, key, 95, 95);
        assert!(lock.last_change_ts.is_zero());
        assert_eq!(lock.versions_to_last_change, 0);
        pessimistic_rollback::tests::must_success(&mut engine, key, 95, 95);

        // Latest version is a LOCK with last_change_ts
        let write = Write::new(WriteType::Lock, 100.into(), None).set_last_change(40.into(), 4);
        engine
            .put_cf(
                Default::default(),
                CF_WRITE,
                Key::from_raw(key).append_ts(110.into()),
                write.as_ref().to_bytes(),
            )
            .unwrap();
        must_succeed(&mut engine, key, key, 120, 130);
        let lock = must_pessimistic_locked(&mut engine, key, 120, 130);
        assert_eq!(lock.last_change_ts, 40.into());
        assert_eq!(lock.versions_to_last_change, 5);
        pessimistic_rollback::tests::must_success(&mut engine, key, 120, 130);

        // Latest version is a ROLLBACK with last_change_ts
        let write = Write::new(WriteType::Rollback, 120.into(), None).set_last_change(40.into(), 5);
        engine
            .put_cf(
                Default::default(),
                CF_WRITE,
                Key::from_raw(key).append_ts(120.into()),
                write.as_ref().to_bytes(),
            )
            .unwrap();
        must_succeed(&mut engine, key, key, 140, 140);
        let lock = must_pessimistic_locked(&mut engine, key, 140, 140);
        assert_eq!(lock.last_change_ts, 40.into());
        assert_eq!(lock.versions_to_last_change, 6);
        pessimistic_rollback::tests::must_success(&mut engine, key, 140, 140);

        // Lock on a key with no write record
        must_succeed(&mut engine, b"k2", b"k2", 150, 150);
        let lock = must_pessimistic_locked(&mut engine, b"k2", 150, 150);
        assert!(lock.last_change_ts.is_zero());
        assert_eq!(lock.versions_to_last_change, 1);
    }

    #[test]
    fn test_lock_with_conflict() {
        use pessimistic_rollback::tests::must_success as must_pessimistic_rollback;

        let mut engine = TestEngineBuilder::new().build().unwrap();

        must_prewrite_put(&mut engine, b"k1", b"v1", b"k1", 10);
        must_commit(&mut engine, b"k1", 10, 20);

        // Normal cases.
        must_succeed_allow_lock_with_conflict(&mut engine, b"k1", b"k1", 10, 30, false, false)
            .assert_empty();
        must_pessimistic_rollback(&mut engine, b"k1", 10, 30);
        must_unlocked(&mut engine, b"k1");

        must_succeed_allow_lock_with_conflict(&mut engine, b"k1", b"k1", 10, 30, false, true)
            .assert_existence(true);
        must_pessimistic_rollback(&mut engine, b"k1", 10, 30);
        must_unlocked(&mut engine, b"k1");

        must_succeed_allow_lock_with_conflict(&mut engine, b"k1", b"k1", 10, 30, true, false)
            .assert_value(Some(b"v1"));
        must_pessimistic_rollback(&mut engine, b"k1", 10, 30);
        must_unlocked(&mut engine, b"k1");

        must_succeed_allow_lock_with_conflict(&mut engine, b"k1", b"k1", 10, 30, true, true)
            .assert_value(Some(b"v1"));
        must_pessimistic_rollback(&mut engine, b"k1", 10, 30);
        must_unlocked(&mut engine, b"k1");

        // Conflicting cases.
        for &(need_value, need_check_existence) in
            &[(false, false), (false, true), (true, false), (true, true)]
        {
            must_succeed_allow_lock_with_conflict(
                &mut engine,
                b"k1",
                b"k1",
                10,
                15,
                need_value,
                need_check_existence,
            )
            .assert_locked_with_conflict(Some(b"v1"), 20);
            must_pessimistic_locked(&mut engine, b"k1", 10, 20);
            must_pessimistic_rollback(&mut engine, b"k1", 10, 20);
            must_unlocked(&mut engine, b"k1");
        }

        // Idempotency
        must_succeed_allow_lock_with_conflict(&mut engine, b"k1", b"k1", 10, 50, false, false)
            .assert_empty();
        must_succeed_allow_lock_with_conflict(&mut engine, b"k1", b"k1", 10, 40, false, false)
            .assert_locked_with_conflict(Some(b"v1"), 50);
        must_succeed_allow_lock_with_conflict(&mut engine, b"k1", b"k1", 10, 15, false, false)
            .assert_locked_with_conflict(Some(b"v1"), 50);
        must_pessimistic_locked(&mut engine, b"k1", 10, 50);
        must_pessimistic_rollback(&mut engine, b"k1", 10, 50);
        must_unlocked(&mut engine, b"k1");

        // Lock waiting.
        must_succeed_allow_lock_with_conflict(&mut engine, b"k1", b"k1", 10, 50, false, false)
            .assert_empty();
        let err = acquire_pessimistic_lock_allow_lock_with_conflict(
            &mut engine,
            b"k1",
            b"k1",
            11,
            55,
            false,
            false,
        )
        .unwrap_err();
        assert!(matches!(err, MvccError(box ErrorInner::KeyIsLocked(_))));
        let err = acquire_pessimistic_lock_allow_lock_with_conflict(
            &mut engine,
            b"k1",
            b"k1",
            9,
            9,
            false,
            false,
        )
        .unwrap_err();
        assert!(matches!(err, MvccError(box ErrorInner::KeyIsLocked(_))));
        must_pessimistic_locked(&mut engine, b"k1", 10, 50);
        must_pessimistic_rollback(&mut engine, b"k1", 10, 50);
        must_unlocked(&mut engine, b"k1");
    }
}
