// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::{
    mvcc::{
        metrics::{
            CONCURRENCY_MANAGER_LOCK_DURATION_HISTOGRAM, MVCC_CONFLICT_COUNTER,
            MVCC_DUPLICATE_CMD_COUNTER_VEC,
        },
        ErrorInner, Lock, LockType, MvccTxn, Result as MvccResult,
    },
    txn::actions::check_data_constraint::check_data_constraint,
    Snapshot,
};
use fail::fail_point;
use std::cmp;
use txn_types::{is_short_value, Key, Mutation, TimeStamp, Value, Write, WriteType};

#[derive(Clone, Debug)]
pub enum TransactionKind {
    // bool is skip_constraint_check
    Optimistic(bool),
    // is_pessimistic_lock, for_update_ts
    Pessimistic(bool, TimeStamp),
}

impl TransactionKind {
    fn skip_constraint_check(&self) -> bool {
        match self {
            TransactionKind::Optimistic(s) => *s,
            TransactionKind::Pessimistic(..) => true,
        }
    }

    fn is_pessimistic(&self) -> bool {
        match self {
            TransactionKind::Optimistic(_) => false,
            TransactionKind::Pessimistic(..) => true,
        }
    }

    fn is_pessimistic_lock(&self) -> bool {
        match self {
            TransactionKind::Optimistic(_) => false,
            TransactionKind::Pessimistic(pl, _) => *pl,
        }
    }

    fn for_update_ts(&self) -> TimeStamp {
        match self {
            TransactionKind::Optimistic(_) => TimeStamp::zero(),
            TransactionKind::Pessimistic(_, ts) => *ts,
        }
    }
}

pub fn prewrite<S: Snapshot>(
    kind: TransactionKind,
    txn: &mut MvccTxn<S>,
    mutation: Mutation,
    primary: &[u8],
    secondary_keys: &Option<Vec<Vec<u8>>>,
    mut lock_ttl: u64,
    txn_size: u64,
    mut min_commit_ts: TimeStamp,
    max_commit_ts: TimeStamp,
    try_one_pc: bool,
) -> MvccResult<TimeStamp> {
    let should_not_write = mutation.should_not_write();

    if kind.is_pessimistic() && should_not_write {
        return Err(box_err!(
            "cannot handle checkNotExists in pessimistic prewrite"
        ));
    }

    let lock_type = LockType::from_mutation(&mutation);
    // For the insert/checkNotExists operation, the old key should not be in the system.
    let should_not_exist = mutation.should_not_exists();
    let mutation_type = mutation.mutation_type();
    let (key, mut value) = mutation.into_key_value();

    fail_point!("prewrite", |err| Err(
        crate::storage::mvcc::txn::make_txn_error(err, &key, txn.start_ts).into()
    ));

    let (has_pessimistic_lock, fallback_from_async_commit) = match check_lock(
        &key,
        secondary_keys.is_some(),
        kind.is_pessimistic_lock(),
        kind.is_pessimistic(),
        txn,
    )? {
        LockStatus::Locked(ts) => return Ok(ts),
        LockStatus::AsyncFallback => (false, true),
        LockStatus::Pessimistic(ttl, ts) => {
            lock_ttl = std::cmp::max(lock_ttl, ttl);
            min_commit_ts = std::cmp::max(min_commit_ts, ts);
            (true, false)
        }
        LockStatus::None => (false, false),
    };

    let prev_write = if !kind.skip_constraint_check() && !fallback_from_async_commit {
        check_for_newer_version(&key, primary, should_not_exist, txn)?
    } else {
        None
    };

    if should_not_write {
        return Ok(TimeStamp::zero());
    }

    if fallback_from_async_commit {
        value = None;
    } else {
        txn.check_extra_op(&key, mutation_type, prev_write)?;
    }

    let mut lock = make_lock(
        txn,
        &key,
        lock_type.unwrap(),
        primary,
        secondary_keys,
        value,
        lock_ttl,
        kind.for_update_ts(),
        txn_size,
        min_commit_ts,
    );

    // The final_min_commit_ts will be calculated if either async commit or 1PC is enabled.
    // It's allowed to enable 1PC without enabling async commit.
    let final_min_commit_ts = if secondary_keys.is_some() || try_one_pc {
        async_commit_timestamps(txn, &key, &mut lock, kind.for_update_ts(), max_commit_ts)?
    } else {
        TimeStamp::zero()
    };

    if try_one_pc {
        txn.put_locks_for_1pc(key, lock, has_pessimistic_lock);
    } else {
        txn.put_lock(key, &lock);
    }

    fail_point!("after_prewrite_one_key");

    Ok(final_min_commit_ts)
}

enum LockStatus {
    // Lock has already been locked; min_commit_ts of lock.
    Locked(TimeStamp),
    AsyncFallback,
    // The lock is pessimistic and owned by this txn, go through to overwrite it.
    // The ttl and min_commit_ts of the lock may have been pushed forward.
    Pessimistic(u64, TimeStamp),
    None,
}

/// Check whether the current key is locked at any timestamp.
fn check_lock<S: Snapshot>(
    key: &Key,
    has_secondary_keys: bool,
    is_pessimistic_lock: bool,
    pessimistic: bool,
    txn: &mut MvccTxn<S>,
) -> MvccResult<LockStatus> {
    Ok(match txn.reader.load_lock(key)? {
        Some(lock) => {
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
                        key: key.to_raw()?,
                    }
                    .into());
                }

                // Pessimistic transactions only acquire pessimistic locks on row keys and unique index keys.
                // The corresponding secondary index keys are not locked until pessimistic prewrite.
                // It's possible that lock conflict occurs on them, but the isolation is
                // guaranteed by pessimistic locks, so let TiDB resolves these locks immediately.
                let mut info = lock.into_lock_info(key.to_raw()?);
                if pessimistic {
                    info.set_lock_ttl(0);
                }
                return Err(ErrorInner::KeyIsLocked(info).into());
            }

            if lock.lock_type == LockType::Pessimistic {
                // TODO: remove it in future
                if !pessimistic {
                    return Err(ErrorInner::LockTypeNotMatch {
                        start_ts: txn.start_ts,
                        key: key.to_raw()?,
                        pessimistic: true,
                    }
                    .into());
                }
                LockStatus::Pessimistic(lock.ttl, lock.min_commit_ts)
            } else {
                // Allow to overwrite the primary lock to fallback from async commit.
                if lock.use_async_commit && !has_secondary_keys {
                    info!("fallback from async commit"; "start_ts" => txn.start_ts);
                    // needn't clear pessimistic locks
                    LockStatus::AsyncFallback
                } else {
                    // Duplicated command. No need to overwrite the lock and data.
                    MVCC_DUPLICATE_CMD_COUNTER_VEC.prewrite.inc();
                    LockStatus::Locked(lock.min_commit_ts)
                }
            }
        }
        None if is_pessimistic_lock => {
            amend_pessimistic_lock(key, txn)?;
            LockStatus::None
        }
        None => LockStatus::None,
    })
}

fn check_for_newer_version<S: Snapshot>(
    key: &Key,
    primary: &[u8],
    should_not_exist: bool,
    txn: &mut MvccTxn<S>,
) -> MvccResult<Option<Write>> {
    match txn.reader.seek_write(&key, TimeStamp::max())? {
        Some((commit_ts, write)) => {
            // Abort on writes after our start timestamp ...
            // If exists a commit version whose commit timestamp is larger than current start
            // timestamp, we should abort current prewrite.
            if commit_ts > txn.start_ts {
                MVCC_CONFLICT_COUNTER.prewrite_write_conflict.inc();
                return Err(ErrorInner::WriteConflict {
                    start_ts: txn.start_ts,
                    conflict_start_ts: write.start_ts,
                    conflict_commit_ts: commit_ts,
                    key: key.to_raw()?,
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
                    key: key.to_raw()?,
                    primary: primary.to_vec(),
                }
                .into());
            }
            // Should check it when no lock exists, otherwise it can report error when there is
            // a lock belonging to a committed transaction which deletes the key.
            check_data_constraint(txn, should_not_exist, &write, commit_ts, &key)?;

            Ok(Some(write))
        }
        None => Ok(None),
    }
}

fn make_lock<S: Snapshot>(
    txn: &mut MvccTxn<S>,
    key: &Key,
    lock_type: LockType,
    primary: &[u8],
    secondary_keys: &Option<Vec<Vec<u8>>>,
    value: Option<Value>,
    lock_ttl: u64,
    for_update_ts: TimeStamp,
    txn_size: u64,
    min_commit_ts: TimeStamp,
) -> Lock {
    let mut lock = Lock::new(
        lock_type,
        primary.to_vec(),
        txn.start_ts,
        lock_ttl,
        None,
        for_update_ts,
        txn_size,
        min_commit_ts,
    );

    if let Some(value) = value {
        if is_short_value(&value) {
            // If the value is short, embed it in Lock.
            lock.short_value = Some(value);
        } else {
            // value is long
            txn.put_value(key.clone(), txn.start_ts, value);
        }
    }

    if let Some(secondary_keys) = secondary_keys {
        lock.use_async_commit = true;
        lock.secondaries = secondary_keys.to_owned();
    }

    lock
}

fn async_commit_timestamps<S: Snapshot>(
    txn: &mut MvccTxn<S>,
    key: &Key,
    lock: &mut Lock,
    for_update_ts: TimeStamp,
    max_commit_ts: TimeStamp,
) -> MvccResult<TimeStamp> {
    // This operation should not block because the latch makes sure only one thread
    // is operating on this key.
    let key_guard = CONCURRENCY_MANAGER_LOCK_DURATION_HISTOGRAM.observe_closure_duration(|| {
        ::futures_executor::block_on(txn.concurrency_manager.lock_key(key))
    });

    let final_min_commit_ts = key_guard.with_lock(|l| {
        let max_ts = txn.concurrency_manager.max_ts();
        fail_point!("before-set-lock-in-memory");
        let min_commit_ts = cmp::max(cmp::max(max_ts, txn.start_ts), for_update_ts).next();
        lock.min_commit_ts = cmp::max(lock.min_commit_ts, min_commit_ts);

        if !max_commit_ts.is_zero() && lock.min_commit_ts > max_commit_ts {
            return Err(ErrorInner::CommitTsTooLarge {
                start_ts: txn.start_ts,
                min_commit_ts: lock.min_commit_ts,
                max_commit_ts,
            });
        }

        *l = Some(lock.clone());
        Ok(lock.min_commit_ts)
    })?;

    txn.guards.push(key_guard);

    Ok(final_min_commit_ts)
}

// TiKV may fails to write pessimistic locks due to pipelined process.
// If the data is not changed after acquiring the lock, we can still prewrite the key.
fn amend_pessimistic_lock<S: Snapshot>(key: &Key, txn: &mut MvccTxn<S>) -> MvccResult<()> {
    if let Some((commit_ts, _)) = txn.reader.seek_write(key, TimeStamp::max())? {
        // The invariants of pessimistic locks are:
        //   1. lock's for_update_ts >= key's latest commit_ts
        //   2. lock's for_update_ts >= txn's start_ts
        //   3. If the data is changed after acquiring the pessimistic lock, key's new commit_ts > lock's for_update_ts
        //
        // So, if the key's latest commit_ts is still less than or equal to lock's for_update_ts, the data is not changed.
        // However, we can't get lock's for_update_ts in current implementation (txn's for_update_ts is updated for each DML),
        // we can only use txn's start_ts to check -- If the key's commit_ts is less than txn's start_ts, it's less than
        // lock's for_update_ts too.
        if commit_ts >= txn.start_ts {
            warn!(
                "prewrite failed (pessimistic lock not found)";
                "start_ts" => txn.start_ts,
                "commit_ts" => commit_ts,
                "key" => %key
            );
            MVCC_CONFLICT_COUNTER
                .pipelined_acquire_pessimistic_lock_amend_fail
                .inc();
            return Err(ErrorInner::PessimisticLockNotFound {
                start_ts: txn.start_ts,
                key: key.clone().into_raw()?,
            }
            .into());
        }
    }
    // Used pipelined pessimistic lock acquiring in this txn but failed
    // Luckily no other txn modified this lock, amend it by treat it as optimistic txn.
    MVCC_CONFLICT_COUNTER
        .pipelined_acquire_pessimistic_lock_amend_success
        .inc();
    Ok(())
}

pub mod tests {
    use super::*;
    #[cfg(test)]
    use crate::storage::txn::tests::must_acquire_pessimistic_lock;
    use crate::storage::{mvcc::tests::*, Engine};
    use concurrency_manager::ConcurrencyManager;
    use kvproto::kvrpcpb::Context;

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
            TransactionKind::Optimistic(false),
            &mut txn,
            Mutation::Insert((Key::from_raw(key), value.to_vec())),
            pk,
            &None,
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
            TransactionKind::Optimistic(false),
            &mut txn,
            Mutation::CheckNotExists(Key::from_raw(key)),
            pk,
            &None,
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
            TransactionKind::Optimistic(false),
            &mut txn,
            Mutation::Put((Key::from_raw(b"k1"), b"v1".to_vec())),
            b"k1",
            &Some(vec![b"k2".to_vec()]),
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
            TransactionKind::Optimistic(false),
            &mut txn,
            Mutation::Put((Key::from_raw(b"k2"), b"v2".to_vec())),
            b"k1",
            &Some(vec![]),
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
            TransactionKind::Optimistic(false),
            &mut txn,
            Mutation::Put((Key::from_raw(b"k1"), b"v1".to_vec())),
            b"k1",
            &None,
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
            TransactionKind::Optimistic(false),
            &mut txn,
            Mutation::Put((Key::from_raw(b"k2"), b"v2".to_vec())),
            b"k1",
            &None,
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

        prewrite(
            TransactionKind::Pessimistic(false, TimeStamp::default()),
            &mut txn,
            Mutation::CheckNotExists(Key::from_raw(key)),
            pk,
            &None,
            0,
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
        prewrite(
            TransactionKind::Pessimistic(true, 20.into()),
            &mut txn,
            Mutation::Put((Key::from_raw(b"k1"), b"v1".to_vec())),
            b"k1",
            &Some(vec![b"k2".to_vec()]),
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
            TransactionKind::Pessimistic(true, 20.into()),
            &mut txn,
            Mutation::Put((Key::from_raw(b"k2"), b"v2".to_vec())),
            b"k1",
            &Some(vec![]),
            2000,
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
        prewrite(
            TransactionKind::Pessimistic(true, 20.into()),
            &mut txn,
            Mutation::Put((Key::from_raw(b"k1"), b"v1".to_vec())),
            b"k1",
            &None,
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
            TransactionKind::Pessimistic(true, 20.into()),
            &mut txn,
            Mutation::Put((Key::from_raw(b"k2"), b"v2".to_vec())),
            b"k1",
            &None,
            2000,
            2,
            10.into(),
            50.into(),
            true,
        )
        .unwrap_err();
    }

    #[test]
    fn test_fallback_from_async_commit_pessimistic() {
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
