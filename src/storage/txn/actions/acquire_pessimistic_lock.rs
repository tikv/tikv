use crate::storage::mvcc::{
    metrics::{MVCC_CONFLICT_COUNTER, MVCC_DUPLICATE_CMD_COUNTER_VEC},
    ErrorInner, MvccTxn, Result as MvccResult,
};
use crate::storage::txn::actions::check_data_constraint::check_data_constraint;
use crate::storage::Snapshot;
use txn_types::{Key, Lock, LockType, TimeStamp, Value, WriteType};

pub fn acquire_pessimistic_lock<S: Snapshot>(
    txn: &mut MvccTxn<S>,
    key: Key,
    primary: &[u8],
    should_not_exist: bool,
    lock_ttl: u64,
    for_update_ts: TimeStamp,
    need_value: bool,
    min_commit_ts: TimeStamp,
) -> MvccResult<Option<Value>> {
    fail_point!("acquire_pessimistic_lock", |err| Err(
        crate::storage::mvcc::txn::make_txn_error(err, &key, txn.start_ts,).into()
    ));

    fn pessimistic_lock(
        primary: &[u8],
        start_ts: TimeStamp,
        lock_ttl: u64,
        for_update_ts: TimeStamp,
        min_commit_ts: TimeStamp,
    ) -> Lock {
        Lock::new(
            LockType::Pessimistic,
            primary.to_vec(),
            start_ts,
            lock_ttl,
            None,
            for_update_ts,
            0,
            min_commit_ts,
        )
    }

    let mut val = None;
    if let Some(lock) = txn.reader.load_lock(&key)? {
        if lock.ts != txn.start_ts {
            return Err(ErrorInner::KeyIsLocked(lock.into_lock_info(key.into_raw()?)).into());
        }
        if lock.lock_type != LockType::Pessimistic {
            return Err(ErrorInner::LockTypeNotMatch {
                start_ts: txn.start_ts,
                key: key.into_raw()?,
                pessimistic: false,
            }
            .into());
        }
        if need_value {
            val = txn
                .reader
                .get(&key, for_update_ts, Some(txn.start_ts), true)?;
        }
        // Overwrite the lock with small for_update_ts
        if for_update_ts > lock.for_update_ts {
            let lock = pessimistic_lock(
                primary,
                txn.start_ts,
                lock_ttl,
                for_update_ts,
                min_commit_ts,
            );
            txn.put_lock(key, &lock);
        } else {
            MVCC_DUPLICATE_CMD_COUNTER_VEC
                .acquire_pessimistic_lock
                .inc();
        }
        return Ok(val);
    }

    if let Some((commit_ts, write)) = txn.reader.seek_write(&key, TimeStamp::max())? {
        // The isolation level of pessimistic transactions is RC. `for_update_ts` is
        // the commit_ts of the data this transaction read. If exists a commit version
        // whose commit timestamp is larger than current `for_update_ts`, the
        // transaction should retry to get the latest data.
        if commit_ts > for_update_ts {
            MVCC_CONFLICT_COUNTER
                .acquire_pessimistic_lock_conflict
                .inc();
            return Err(ErrorInner::WriteConflict {
                start_ts: txn.start_ts,
                conflict_start_ts: write.start_ts,
                conflict_commit_ts: commit_ts,
                key: key.into_raw()?,
                primary: primary.to_vec(),
            }
            .into());
        }

        // Handle rollback.
        // The rollback information may come from either a Rollback record or a record with
        // `has_overlapped_rollback` flag.
        if commit_ts == txn.start_ts
            && (write.write_type == WriteType::Rollback || write.has_overlapped_rollback)
        {
            assert!(write.has_overlapped_rollback || write.start_ts == commit_ts);
            return Err(ErrorInner::PessimisticLockRolledBack {
                start_ts: txn.start_ts,
                key: key.into_raw()?,
            }
            .into());
        }
        // If `commit_ts` we seek is already before `start_ts`, the rollback must not exist.
        if commit_ts > txn.start_ts {
            if let Some((older_commit_ts, older_write)) =
                txn.reader.seek_write(&key, txn.start_ts)?
            {
                if older_commit_ts == txn.start_ts
                    && (older_write.write_type == WriteType::Rollback
                        || older_write.has_overlapped_rollback)
                {
                    return Err(ErrorInner::PessimisticLockRolledBack {
                        start_ts: txn.start_ts,
                        key: key.into_raw()?,
                    }
                    .into());
                }
            }
        }

        // Check data constraint when acquiring pessimistic lock.
        check_data_constraint(txn, should_not_exist, &write, commit_ts, &key)?;

        if need_value {
            val = match write.write_type {
                // If it's a valid Write, no need to read again.
                WriteType::Put
                    if write
                        .as_ref()
                        .check_gc_fence_as_latest_version(txn.start_ts) =>
                {
                    Some(txn.reader.load_data(&key, write)?)
                }
                WriteType::Delete | WriteType::Put => None,
                WriteType::Lock | WriteType::Rollback => {
                    txn.reader
                        .get(&key, commit_ts.prev(), Some(txn.start_ts), true)?
                }
            };
        }
    }

    let lock = pessimistic_lock(
        primary,
        txn.start_ts,
        lock_ttl,
        for_update_ts,
        min_commit_ts,
    );
    txn.put_lock(key, &lock);

    Ok(val)
}

pub mod tests {
    use super::*;
    use crate::storage::kv::WriteData;
    use crate::storage::mvcc::{Error as MvccError, MvccReader, MvccTxn};
    use crate::storage::Engine;
    use concurrency_manager::ConcurrencyManager;
    use kvproto::kvrpcpb::Context;
    use kvproto::kvrpcpb::IsolationLevel;
    use txn_types::TimeStamp;

    #[cfg(test)]
    use crate::storage::{
        mvcc::tests::*, txn::commands::pessimistic_rollback, txn::tests::*, TestEngineBuilder,
    };

    pub fn must_succeed_impl<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        start_ts: impl Into<TimeStamp>,
        should_not_exist: bool,
        lock_ttl: u64,
        for_update_ts: impl Into<TimeStamp>,
        need_value: bool,
        min_commit_ts: impl Into<TimeStamp>,
    ) -> Option<Value> {
        let ctx = Context::default();
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let min_commit_ts = min_commit_ts.into();
        let cm = ConcurrencyManager::new(min_commit_ts);
        let mut txn = MvccTxn::new(snapshot, start_ts.into(), true, cm);
        let res = acquire_pessimistic_lock(
            &mut txn,
            Key::from_raw(key),
            pk,
            should_not_exist,
            lock_ttl,
            for_update_ts.into(),
            need_value,
            min_commit_ts,
        )
        .unwrap();
        let modifies = txn.into_modifies();
        if !modifies.is_empty() {
            engine
                .write(&ctx, WriteData::from_modifies(modifies))
                .unwrap();
        }
        res
    }

    pub fn must_succeed<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        start_ts: impl Into<TimeStamp>,
        for_update_ts: impl Into<TimeStamp>,
    ) {
        must_succeed_with_ttl(engine, key, pk, start_ts, for_update_ts, 0);
    }

    pub fn must_succeed_return_value<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        start_ts: impl Into<TimeStamp>,
        for_update_ts: impl Into<TimeStamp>,
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
            TimeStamp::zero(),
        )
    }

    pub fn must_succeed_with_ttl<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        start_ts: impl Into<TimeStamp>,
        for_update_ts: impl Into<TimeStamp>,
        ttl: u64,
    ) {
        assert!(must_succeed_impl(
            engine,
            key,
            pk,
            start_ts,
            false,
            ttl,
            for_update_ts.into(),
            false,
            TimeStamp::zero(),
        )
        .is_none());
    }

    pub fn must_succeed_for_large_txn<E: Engine>(
        engine: &E,
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
            min_commit_ts,
        );
    }

    pub fn must_err<E: Engine>(
        engine: &E,
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
            TimeStamp::zero(),
        )
    }

    pub fn must_err_return_value<E: Engine>(
        engine: &E,
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
            true,
            TimeStamp::zero(),
        )
    }

    fn must_err_impl<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        start_ts: impl Into<TimeStamp>,
        should_not_exist: bool,
        for_update_ts: impl Into<TimeStamp>,
        need_value: bool,
        min_commit_ts: impl Into<TimeStamp>,
    ) -> MvccError {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let min_commit_ts = min_commit_ts.into();
        let cm = ConcurrencyManager::new(min_commit_ts);
        let mut txn = MvccTxn::new(snapshot, start_ts.into(), true, cm);
        acquire_pessimistic_lock(
            &mut txn,
            Key::from_raw(key),
            pk,
            should_not_exist,
            0,
            for_update_ts.into(),
            need_value,
            min_commit_ts,
        )
        .unwrap_err()
    }

    pub fn must_pessimistic_locked<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        for_update_ts: impl Into<TimeStamp>,
    ) {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, IsolationLevel::Si);
        let lock = reader.load_lock(&Key::from_raw(key)).unwrap().unwrap();
        assert_eq!(lock.ts, start_ts.into());
        assert_eq!(lock.for_update_ts, for_update_ts.into());
        assert_eq!(lock.lock_type, LockType::Pessimistic);
    }

    #[test]
    fn test_pessimistic_lock() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let k = b"k1";
        let v = b"v1";

        // TODO: Some corner cases don't give proper results. Although they are not important, we
        // should consider whether they are better to be fixed.

        // Normal
        must_succeed(&engine, k, k, 1, 1);
        must_pessimistic_locked(&engine, k, 1, 1);
        must_pessimistic_prewrite_put(&engine, k, v, k, 1, 1, true);
        must_locked(&engine, k, 1);
        must_commit(&engine, k, 1, 2);
        must_unlocked(&engine, k);

        // Lock conflict
        must_prewrite_put(&engine, k, v, k, 3);
        must_err(&engine, k, k, 4, 4);
        must_cleanup(&engine, k, 3, 0);
        must_unlocked(&engine, k);
        must_succeed(&engine, k, k, 5, 5);
        must_prewrite_lock_err(&engine, k, k, 6);
        must_err(&engine, k, k, 6, 6);
        must_cleanup(&engine, k, 5, 0);
        must_unlocked(&engine, k);

        // Data conflict
        must_prewrite_put(&engine, k, v, k, 7);
        must_commit(&engine, k, 7, 9);
        must_unlocked(&engine, k);
        must_prewrite_lock_err(&engine, k, k, 8);
        must_err(&engine, k, k, 8, 8);
        must_succeed(&engine, k, k, 8, 9);
        must_pessimistic_prewrite_put(&engine, k, v, k, 8, 8, true);
        must_commit(&engine, k, 8, 10);
        must_unlocked(&engine, k);

        // Rollback
        must_succeed(&engine, k, k, 11, 11);
        must_pessimistic_locked(&engine, k, 11, 11);
        must_cleanup(&engine, k, 11, 0);
        must_err(&engine, k, k, 11, 11);
        must_pessimistic_prewrite_put_err(&engine, k, v, k, 11, 11, true);
        must_prewrite_lock_err(&engine, k, k, 11);
        must_unlocked(&engine, k);

        must_succeed(&engine, k, k, 12, 12);
        must_pessimistic_prewrite_put(&engine, k, v, k, 12, 12, true);
        must_locked(&engine, k, 12);
        must_cleanup(&engine, k, 12, 0);
        must_err(&engine, k, k, 12, 12);
        must_pessimistic_prewrite_put_err(&engine, k, v, k, 12, 12, true);
        must_prewrite_lock_err(&engine, k, k, 12);
        must_unlocked(&engine, k);

        // Duplicated
        must_succeed(&engine, k, k, 13, 13);
        must_pessimistic_locked(&engine, k, 13, 13);
        must_succeed(&engine, k, k, 13, 13);
        must_pessimistic_locked(&engine, k, 13, 13);
        must_pessimistic_prewrite_put(&engine, k, v, k, 13, 13, true);
        must_locked(&engine, k, 13);
        must_pessimistic_prewrite_put(&engine, k, v, k, 13, 13, true);
        must_locked(&engine, k, 13);
        must_commit(&engine, k, 13, 14);
        must_unlocked(&engine, k);
        must_commit(&engine, k, 13, 14);
        must_unlocked(&engine, k);

        // Pessimistic lock doesn't block reads.
        must_succeed(&engine, k, k, 15, 15);
        must_pessimistic_locked(&engine, k, 15, 15);
        must_get(&engine, k, 16, v);
        must_pessimistic_prewrite_delete(&engine, k, k, 15, 15, true);
        must_get_err(&engine, k, 16);
        must_commit(&engine, k, 15, 17);

        // Rollback
        must_succeed(&engine, k, k, 18, 18);
        must_rollback(&engine, k, 18);
        must_unlocked(&engine, k);
        must_prewrite_put(&engine, k, v, k, 19);
        must_commit(&engine, k, 19, 20);
        must_err(&engine, k, k, 18, 21);
        must_unlocked(&engine, k);

        // LockTypeNotMatch
        must_prewrite_put(&engine, k, v, k, 23);
        must_locked(&engine, k, 23);
        must_err(&engine, k, k, 23, 23);
        must_cleanup(&engine, k, 23, 0);
        must_succeed(&engine, k, k, 24, 24);
        must_pessimistic_locked(&engine, k, 24, 24);
        must_prewrite_put_err(&engine, k, v, k, 24);
        must_rollback(&engine, k, 24);

        // Acquire lock on a prewritten key should fail.
        must_succeed(&engine, k, k, 26, 26);
        must_pessimistic_locked(&engine, k, 26, 26);
        must_pessimistic_prewrite_delete(&engine, k, k, 26, 26, true);
        must_locked(&engine, k, 26);
        must_err(&engine, k, k, 26, 26);
        must_locked(&engine, k, 26);

        // Acquire lock on a committed key should fail.
        must_commit(&engine, k, 26, 27);
        must_unlocked(&engine, k);
        must_get_none(&engine, k, 28);
        must_err(&engine, k, k, 26, 26);
        must_unlocked(&engine, k);
        must_get_none(&engine, k, 28);
        // Pessimistic prewrite on a committed key should fail.
        must_pessimistic_prewrite_put_err(&engine, k, v, k, 26, 26, true);
        must_unlocked(&engine, k);
        must_get_none(&engine, k, 28);
        // Currently we cannot avoid this.
        must_succeed(&engine, k, k, 26, 29);
        pessimistic_rollback::tests::must_success(&engine, k, 26, 29);
        must_unlocked(&engine, k);

        // Non pessimistic key in pessimistic transaction.
        must_pessimistic_prewrite_put(&engine, k, v, k, 30, 30, false);
        must_locked(&engine, k, 30);
        must_commit(&engine, k, 30, 31);
        must_unlocked(&engine, k);
        must_get_commit_ts(&engine, k, 30, 31);

        // Rollback collapsed.
        must_rollback_collapsed(&engine, k, 32);
        must_rollback_collapsed(&engine, k, 33);
        must_err(&engine, k, k, 32, 32);
        // Currently we cannot avoid this.
        must_succeed(&engine, k, k, 32, 34);
        pessimistic_rollback::tests::must_success(&engine, k, 32, 34);
        must_unlocked(&engine, k);

        // Acquire lock when there is lock with different for_update_ts.
        must_succeed(&engine, k, k, 35, 36);
        must_pessimistic_locked(&engine, k, 35, 36);
        must_succeed(&engine, k, k, 35, 35);
        must_pessimistic_locked(&engine, k, 35, 36);
        must_succeed(&engine, k, k, 35, 37);
        must_pessimistic_locked(&engine, k, 35, 37);

        // Cannot prewrite when there is another transaction's pessimistic lock.
        must_pessimistic_prewrite_put_err(&engine, k, v, k, 36, 36, true);
        must_pessimistic_prewrite_put_err(&engine, k, v, k, 36, 38, true);
        must_pessimistic_locked(&engine, k, 35, 37);
        // Cannot prewrite when there is another transaction's non-pessimistic lock.
        must_pessimistic_prewrite_put(&engine, k, v, k, 35, 37, true);
        must_locked(&engine, k, 35);
        must_pessimistic_prewrite_put_err(&engine, k, v, k, 36, 38, true);
        must_locked(&engine, k, 35);

        // Commit pessimistic transaction's key but with smaller commit_ts than for_update_ts.
        // Currently not checked, so in this case it will actually be successfully committed.
        must_commit(&engine, k, 35, 36);
        must_unlocked(&engine, k);
        must_get_commit_ts(&engine, k, 35, 36);

        // Prewrite meets pessimistic lock on a non-pessimistic key.
        // Currently not checked, so prewrite will success.
        must_succeed(&engine, k, k, 40, 40);
        must_pessimistic_locked(&engine, k, 40, 40);
        must_pessimistic_prewrite_put(&engine, k, v, k, 40, 40, false);
        must_locked(&engine, k, 40);
        must_commit(&engine, k, 40, 41);
        must_unlocked(&engine, k);

        // Prewrite with different for_update_ts.
        // Currently not checked.
        must_succeed(&engine, k, k, 42, 45);
        must_pessimistic_locked(&engine, k, 42, 45);
        must_pessimistic_prewrite_put(&engine, k, v, k, 42, 43, true);
        must_locked(&engine, k, 42);
        must_commit(&engine, k, 42, 45);
        must_unlocked(&engine, k);

        must_succeed(&engine, k, k, 46, 47);
        must_pessimistic_locked(&engine, k, 46, 47);
        must_pessimistic_prewrite_put(&engine, k, v, k, 46, 48, true);
        must_locked(&engine, k, 46);
        must_commit(&engine, k, 46, 50);
        must_unlocked(&engine, k);

        // Prewrite on non-pessimistic key meets write with larger commit_ts than current
        // for_update_ts (non-pessimistic data conflict).
        // Normally non-pessimistic keys in pessimistic transactions are used when we are sure that
        // there won't be conflicts. So this case is also not checked, and prewrite will succeeed.
        must_pessimistic_prewrite_put(&engine, k, v, k, 47, 48, false);
        must_locked(&engine, k, 47);
        must_cleanup(&engine, k, 47, 0);
        must_unlocked(&engine, k);

        // The rollback of the primary key in a pessimistic transaction should be protected from
        // being collapsed.
        must_succeed(&engine, k, k, 49, 60);
        must_pessimistic_prewrite_put(&engine, k, v, k, 49, 60, true);
        must_locked(&engine, k, 49);
        must_cleanup(&engine, k, 49, 0);
        must_get_rollback_protected(&engine, k, 49, true);
        must_prewrite_put(&engine, k, v, k, 51);
        must_rollback_collapsed(&engine, k, 51);
        must_err(&engine, k, k, 49, 60);

        // Overlapped rollback record will be written when the current start_ts equals to another write
        // records' commit ts. Now there is a commit record with commit_ts = 50.
        must_succeed(&engine, k, k, 50, 61);
        must_pessimistic_prewrite_put(&engine, k, v, k, 50, 61, true);
        must_locked(&engine, k, 50);
        must_cleanup(&engine, k, 50, 0);
        must_get_overlapped_rollback(&engine, k, 50, 46, WriteType::Put, Some(0));

        // start_ts and commit_ts interlacing
        for start_ts in &[140, 150, 160] {
            let for_update_ts = start_ts + 48;
            let commit_ts = start_ts + 50;
            must_succeed(&engine, k, k, *start_ts, for_update_ts);
            must_pessimistic_prewrite_put(&engine, k, v, k, *start_ts, for_update_ts, true);
            must_commit(&engine, k, *start_ts, commit_ts);
            must_get(&engine, k, commit_ts + 1, v);
        }

        must_rollback(&engine, k, 170);

        // Now the data should be like: (start_ts -> commit_ts)
        // 140 -> 190
        // 150 -> 200
        // 160 -> 210
        // 170 -> rollback
        must_get_commit_ts(&engine, k, 140, 190);
        must_get_commit_ts(&engine, k, 150, 200);
        must_get_commit_ts(&engine, k, 160, 210);
        must_get_rollback_ts(&engine, k, 170);
    }

    #[test]
    fn test_pessimistic_lock_return_value() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let (k, v) = (b"k", b"v");

        assert_eq!(must_succeed_return_value(&engine, k, k, 10, 10), None);
        must_pessimistic_locked(&engine, k, 10, 10);
        pessimistic_rollback::tests::must_success(&engine, k, 10, 10);

        // Put
        must_prewrite_put(&engine, k, v, k, 10);
        // KeyIsLocked
        match must_err_return_value(&engine, k, k, 20, 20) {
            MvccError(box ErrorInner::KeyIsLocked(_)) => (),
            e => panic!("unexpected error: {}", e),
        };
        must_commit(&engine, k, 10, 20);
        // WriteConflict
        match must_err_return_value(&engine, k, k, 15, 15) {
            MvccError(box ErrorInner::WriteConflict { .. }) => (),
            e => panic!("unexpected error: {}", e),
        };
        assert_eq!(
            must_succeed_return_value(&engine, k, k, 25, 25),
            Some(v.to_vec())
        );
        must_pessimistic_locked(&engine, k, 25, 25);
        pessimistic_rollback::tests::must_success(&engine, k, 25, 25);

        // Skip Write::Lock
        must_prewrite_lock(&engine, k, k, 30);
        must_commit(&engine, k, 30, 40);
        assert_eq!(
            must_succeed_return_value(&engine, k, k, 45, 45),
            Some(v.to_vec())
        );
        must_pessimistic_locked(&engine, k, 45, 45);
        pessimistic_rollback::tests::must_success(&engine, k, 45, 45);

        // Skip Write::Rollback
        must_rollback(&engine, k, 50);
        assert_eq!(
            must_succeed_return_value(&engine, k, k, 55, 55),
            Some(v.to_vec())
        );
        must_pessimistic_locked(&engine, k, 55, 55);
        pessimistic_rollback::tests::must_success(&engine, k, 55, 55);

        // Delete
        must_prewrite_delete(&engine, k, k, 60);
        must_commit(&engine, k, 60, 70);
        assert_eq!(must_succeed_return_value(&engine, k, k, 75, 75), None);
        // Duplicated command
        assert_eq!(must_succeed_return_value(&engine, k, k, 75, 75), None);
        assert_eq!(
            must_succeed_return_value(&engine, k, k, 75, 55),
            Some(v.to_vec())
        );
        must_pessimistic_locked(&engine, k, 75, 75);
        pessimistic_rollback::tests::must_success(&engine, k, 75, 75);
    }

    #[test]
    fn test_overwrite_pessimistic_lock() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let k = b"k1";

        must_succeed(&engine, k, k, 1, 2);
        must_pessimistic_locked(&engine, k, 1, 2);
        must_succeed(&engine, k, k, 1, 1);
        must_pessimistic_locked(&engine, k, 1, 2);
        must_succeed(&engine, k, k, 1, 3);
        must_pessimistic_locked(&engine, k, 1, 3);
    }

    #[test]
    fn test_pessimistic_lock_check_gc_fence() {
        use pessimistic_rollback::tests::must_success as must_pessimistic_rollback;

        let engine = TestEngineBuilder::new().build().unwrap();

        // PUT,           Read
        //  `------^
        must_prewrite_put(&engine, b"k1", b"v1", b"k1", 10);
        must_commit(&engine, b"k1", 10, 30);
        must_cleanup_with_gc_fence(&engine, b"k1", 30, 0, 40, true);

        // PUT,           Read
        //  * (GC fence ts = 0)
        must_prewrite_put(&engine, b"k2", b"v2", b"k2", 11);
        must_commit(&engine, b"k2", 11, 30);
        must_cleanup_with_gc_fence(&engine, b"k2", 30, 0, 0, true);

        // PUT, LOCK,   LOCK, Read
        //  `---------^
        must_prewrite_put(&engine, b"k3", b"v3", b"k3", 12);
        must_commit(&engine, b"k3", 12, 30);
        must_prewrite_lock(&engine, b"k3", b"k3", 37);
        must_commit(&engine, b"k3", 37, 38);
        must_cleanup_with_gc_fence(&engine, b"k3", 30, 0, 40, true);
        must_prewrite_lock(&engine, b"k3", b"k3", 42);
        must_commit(&engine, b"k3", 42, 43);

        // PUT, LOCK,   LOCK, Read
        //  *
        must_prewrite_put(&engine, b"k4", b"v4", b"k4", 13);
        must_commit(&engine, b"k4", 13, 30);
        must_prewrite_lock(&engine, b"k4", b"k4", 37);
        must_commit(&engine, b"k4", 37, 38);
        must_prewrite_lock(&engine, b"k4", b"k4", 42);
        must_commit(&engine, b"k4", 42, 43);
        must_cleanup_with_gc_fence(&engine, b"k4", 30, 0, 0, true);

        // PUT,   PUT,    READ
        //  `-----^ `------^
        must_prewrite_put(&engine, b"k5", b"v5", b"k5", 14);
        must_commit(&engine, b"k5", 14, 20);
        must_prewrite_put(&engine, b"k5", b"v5x", b"k5", 21);
        must_commit(&engine, b"k5", 21, 30);
        must_cleanup_with_gc_fence(&engine, b"k5", 20, 0, 30, false);
        must_cleanup_with_gc_fence(&engine, b"k5", 30, 0, 40, true);

        // PUT,   PUT,    READ
        //  `-----^ *
        must_prewrite_put(&engine, b"k6", b"v6", b"k6", 15);
        must_commit(&engine, b"k6", 15, 20);
        must_prewrite_put(&engine, b"k6", b"v6x", b"k6", 22);
        must_commit(&engine, b"k6", 22, 30);
        must_cleanup_with_gc_fence(&engine, b"k6", 20, 0, 30, false);
        must_cleanup_with_gc_fence(&engine, b"k6", 30, 0, 0, true);

        // PUT,  LOCK,    READ
        //  `----------^
        // Note that this case is special because usually the `LOCK` is the first write already got
        // during prewrite/acquire_pessimistic_lock and will continue searching an older version
        // from the `LOCK` record.
        must_prewrite_put(&engine, b"k7", b"v7", b"k7", 16);
        must_commit(&engine, b"k7", 16, 30);
        must_prewrite_lock(&engine, b"k7", b"k7", 37);
        must_commit(&engine, b"k7", 37, 38);
        must_cleanup_with_gc_fence(&engine, b"k7", 30, 0, 40, true);

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
                assert!(must_succeed_impl(&engine, key, key, 50, true, 0, 50, false, 51).is_none());
                must_pessimistic_rollback(&engine, key, 50, 51);
                must_unlocked(&engine, key);
            } else {
                must_err_impl(&engine, key, key, 50, true, 50, false, 51);
                must_unlocked(&engine, key);
            }

            // Test getting value.
            let res = must_succeed_impl(&engine, key, key, 50, false, 0, 50, true, 51);
            assert_eq!(res, expected_value.map(|v| v.to_vec()));
            must_pessimistic_rollback(&engine, key, 50, 51);

            // Test getting value when already locked.
            must_succeed(&engine, key, key, 50, 51);
            let res2 = must_succeed_impl(&engine, key, key, 50, false, 0, 50, true, 51);
            assert_eq!(res2, expected_value.map(|v| v.to_vec()));
            must_pessimistic_rollback(&engine, key, 50, 51);
        }
    }
}
