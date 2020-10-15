use crate::storage::mvcc::{
    metrics::{MVCC_CONFLICT_COUNTER, MVCC_DUPLICATE_CMD_COUNTER_VEC},
    ErrorInner, MvccTxn, Result as MvccResult,
};
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
            val = txn.reader.get(&key, for_update_ts, true)?;
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
        // The rollack informathin may come from either a Rollback record or a record with
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
            if let Some((commit_ts, write)) = txn.reader.seek_write(&key, txn.start_ts)? {
                if write.start_ts == txn.start_ts {
                    assert!(commit_ts == txn.start_ts && write.write_type == WriteType::Rollback);
                    return Err(ErrorInner::PessimisticLockRolledBack {
                        start_ts: txn.start_ts,
                        key: key.into_raw()?,
                    }
                    .into());
                }
            }
        }

        // Check data constraint when acquiring pessimistic lock.
        txn.check_data_constraint(should_not_exist, &write, commit_ts, &key)?;

        if need_value {
            val = match write.write_type {
                // If it's a valid Write, no need to read again.
                WriteType::Put => Some(txn.reader.load_data(&key, write)?),
                WriteType::Delete => None,
                WriteType::Lock | WriteType::Rollback => {
                    txn.reader.get(&key, commit_ts.prev(), true)?
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
    use crate::storage::mvcc::{Error as MvccError, MvccTxn};
    use crate::storage::Engine;
    use concurrency_manager::ConcurrencyManager;
    use kvproto::kvrpcpb::Context;
    use txn_types::TimeStamp;

    pub fn must_succeed_impl<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        start_ts: impl Into<TimeStamp>,
        lock_ttl: u64,
        for_update_ts: impl Into<TimeStamp>,
        need_value: bool,
        min_commit_ts: impl Into<TimeStamp>,
    ) -> Option<Value> {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let min_commit_ts = min_commit_ts.into();
        let cm = ConcurrencyManager::new(min_commit_ts);
        let mut txn = MvccTxn::new(snapshot, start_ts.into(), true, cm);
        let res = acquire_pessimistic_lock(
            &mut txn,
            Key::from_raw(key),
            pk,
            false,
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
        for_update_ts: impl Into<TimeStamp>,
        need_value: bool,
        min_commit_ts: impl Into<TimeStamp>,
    ) -> MvccError {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let min_commit_ts = min_commit_ts.into();
        let cm = ConcurrencyManager::new(min_commit_ts);
        let mut txn = MvccTxn::new(snapshot, start_ts.into(), true, cm);
        acquire_pessimistic_lock(
            &mut txn,
            Key::from_raw(key),
            pk,
            false,
            0,
            for_update_ts.into(),
            need_value,
            min_commit_ts,
        )
        .unwrap_err()
    }
}
