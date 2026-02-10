// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use tikv_util::Either;

use crate::storage::{
    Snapshot, TxnStatus,
    mvcc::{
        ErrorInner, Key, MvccTxn, ReleasedLock, Result as MvccResult, SnapshotReader, TimeStamp,
        metrics::{MVCC_CONFLICT_COUNTER, MVCC_DUPLICATE_CMD_COUNTER_VEC},
    },
    txn::actions::check_txn_status::{
        MissingLockAction, check_txn_status_missing_lock, rollback_lock, rollback_shared_lock,
    },
};

/// Cleanup the lock if it's TTL has expired, comparing with `current_ts`. If
/// `current_ts` is 0, cleanup the lock without checking TTL. If the lock is the
/// primary lock of a pessimistic transaction, the rollback record is protected
/// from being collapsed.
///
/// Returns the released lock. Returns error if the key is locked or has already
/// been committed.
pub fn cleanup<S: Snapshot>(
    txn: &mut MvccTxn,
    reader: &mut SnapshotReader<S>,
    key: Key,
    current_ts: TimeStamp,
    protect_rollback: bool,
) -> MvccResult<Option<ReleasedLock>> {
    fail_point!("cleanup", |err| Err(
        crate::storage::mvcc::txn::make_txn_error(err, &key, reader.start_ts).into()
    ));

    // Check for pending lock modifications first. This is important when
    // processing multiple sub-locks of the same key in a single batch (e.g.,
    // in resolve_lock). Each operation needs to see the pending writes from
    // previous operations in the same batch.
    let lock_state = match txn.get_pending_lock_bytes(&key) {
        Some(None) => {
            // Lock was deleted by a previous operation in this batch
            None
        }
        Some(Some(bytes)) => {
            // Use pending lock state
            Some(txn_types::parse_lock(bytes)?)
        }
        None => {
            // No pending modification, read from snapshot
            reader.load_lock(&key)?
        }
    };

    match lock_state {
        Some(Either::Left(ref lock)) if lock.ts == reader.start_ts => {
            // If current_ts is not 0, check the Lock's TTL.
            // If the lock is not expired, do not rollback it but report key is locked.
            if !current_ts.is_zero() {
                let expire_time = lock.ts.physical() + lock.ttl;
                if expire_time >= current_ts.physical() {
                    return Err(ErrorInner::KeyIsLocked(
                        lock.clone().into_lock_info(key.into_raw()?),
                    )
                    .into());
                }
            }
            rollback_lock(
                txn,
                reader,
                key,
                lock,
                lock.is_pessimistic_txn(),
                !protect_rollback,
            )
        }
        Some(Either::Right(mut shared_locks))
            if shared_locks.contains_start_ts(reader.start_ts) =>
        {
            // If current_ts is not 0, check the Lock's TTL.
            // If the lock is not expired, do not rollback it but report key is locked.
            if !current_ts.is_zero() {
                let expire_time = match shared_locks.get_lock(&reader.start_ts)? {
                    Some(l) => l.ts.physical() + l.ttl,
                    _ => unreachable!(), // since contains_start_ts returned true
                };
                if expire_time >= current_ts.physical() {
                    return Err(ErrorInner::KeyIsLocked(
                        shared_locks.into_lock_info(key.into_raw()?),
                    )
                    .into());
                }
            }
            rollback_shared_lock(
                txn,
                reader,
                key,
                shared_locks,
                reader.start_ts,
                !protect_rollback,
            )
        }
        lock_or_shared_locks => {
            let lock =
                lock_or_shared_locks.and_then(|lock_or_shared_locks| match lock_or_shared_locks {
                    Either::Left(lock) => Some(lock),
                    Either::Right(_) => None,
                });
            match check_txn_status_missing_lock(
                txn,
                reader,
                key.clone(),
                lock,
                MissingLockAction::rollback_protect(protect_rollback),
                false,
            )? {
                TxnStatus::Committed { commit_ts } => {
                    MVCC_CONFLICT_COUNTER.rollback_committed.inc();
                    Err(ErrorInner::Committed {
                        start_ts: reader.start_ts,
                        commit_ts,
                        key: key.into_raw()?,
                    }
                    .into())
                }
                TxnStatus::RolledBack => {
                    // Return Ok on Rollback already exist.
                    MVCC_DUPLICATE_CMD_COUNTER_VEC.rollback.inc();
                    Ok(None)
                }
                TxnStatus::LockNotExist => Ok(None),
                _ => unreachable!(),
            }
        }
    }
}

pub mod tests {
    use concurrency_manager::ConcurrencyManager;
    use engine_traits::CF_WRITE;
    use kvproto::kvrpcpb::Context;
    #[cfg(test)]
    use kvproto::kvrpcpb::PrewriteRequestPessimisticAction::*;
    use txn_types::TimeStamp;

    use super::*;
    use crate::storage::{
        Engine,
        mvcc::{
            Error as MvccError, WriteType,
            tests::{must_have_write, must_not_have_write, write},
        },
        txn::tests::{must_commit, must_prewrite_put},
    };
    #[cfg(test)]
    use crate::storage::{
        TestEngineBuilder,
        mvcc::tests::{
            must_get_rollback_protected, must_get_rollback_ts, must_load_shared_lock, must_locked,
            must_unlocked, must_written,
        },
        txn::actions::tests::must_shared_prewrite_lock,
        txn::commands::txn_heart_beat,
        txn::tests::{
            must_acquire_pessimistic_lock, must_acquire_shared_pessimistic_lock,
            must_pessimistic_prewrite_put,
        },
    };

    pub fn must_succeed<E: Engine>(
        engine: &mut E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        current_ts: impl Into<TimeStamp>,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let current_ts = current_ts.into();
        let cm = ConcurrencyManager::new_for_test(current_ts);
        let start_ts = start_ts.into();
        let mut txn = MvccTxn::new(start_ts, cm);
        let mut reader = SnapshotReader::new(start_ts, snapshot, true);
        cleanup(&mut txn, &mut reader, Key::from_raw(key), current_ts, true).unwrap();
        write(engine, &ctx, txn.into_modifies());
    }

    pub fn must_err<E: Engine>(
        engine: &mut E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        current_ts: impl Into<TimeStamp>,
    ) -> MvccError {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let current_ts = current_ts.into();
        let cm = ConcurrencyManager::new_for_test(current_ts);
        let start_ts = start_ts.into();
        let mut txn = MvccTxn::new(start_ts, cm);
        let mut reader = SnapshotReader::new(start_ts, snapshot, true);
        cleanup(&mut txn, &mut reader, Key::from_raw(key), current_ts, true).unwrap_err()
    }

    pub fn must_cleanup_with_gc_fence<E: Engine>(
        engine: &mut E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        current_ts: impl Into<TimeStamp>,
        gc_fence: impl Into<TimeStamp>,
        without_target_write: bool,
    ) {
        let ctx = Context::default();
        let gc_fence = gc_fence.into();
        let start_ts = start_ts.into();
        let current_ts = current_ts.into();

        if !gc_fence.is_zero() && without_target_write {
            // Put a dummy record and remove it after doing cleanup.
            must_not_have_write(engine, key, gc_fence);
            must_prewrite_put(engine, key, b"dummy_value", key, gc_fence.prev());
            must_commit(engine, key, gc_fence.prev(), gc_fence);
        }

        let cm = ConcurrencyManager::new_for_test(current_ts);
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut txn = MvccTxn::new(start_ts, cm);
        let mut reader = SnapshotReader::new(start_ts, snapshot, true);
        cleanup(&mut txn, &mut reader, Key::from_raw(key), current_ts, true).unwrap();

        write(engine, &ctx, txn.into_modifies());

        let w = must_have_write(engine, key, start_ts);
        assert_ne!(w.start_ts, start_ts, "no overlapping write record");
        assert!(
            w.write_type != WriteType::Rollback && w.write_type != WriteType::Lock,
            "unexpected write type {:?}",
            w.write_type
        );

        if !gc_fence.is_zero() && without_target_write {
            engine
                .delete_cf(&ctx, CF_WRITE, Key::from_raw(key).append_ts(gc_fence))
                .unwrap();
            must_not_have_write(engine, key, gc_fence);
        }
    }

    #[test]
    fn test_must_cleanup_with_gc_fence() {
        // Tests the test util
        let mut engine = TestEngineBuilder::new().build().unwrap();
        must_prewrite_put(&mut engine, b"k", b"v", b"k", 10);
        must_commit(&mut engine, b"k", 10, 20);
        must_cleanup_with_gc_fence(&mut engine, b"k", 20, 0, 30, true);
        let w = must_written(&mut engine, b"k", 10, 20, WriteType::Put);
        assert!(w.has_overlapped_rollback);
        assert_eq!(w.gc_fence.unwrap(), 30.into());
    }

    #[test]
    fn test_cleanup() {
        // Cleanup's logic is mostly similar to rollback, except the TTL check. Tests
        // that not related to TTL check should be covered by other test cases.
        let mut engine = TestEngineBuilder::new().build().unwrap();

        // Shorthand for composing ts.
        let ts = TimeStamp::compose;

        let (k, v) = (b"k", b"v");

        must_prewrite_put(&mut engine, k, v, k, ts(10, 0));
        must_locked(&mut engine, k, ts(10, 0));
        txn_heart_beat::tests::must_success(&mut engine, k, ts(10, 0), 100, 100);
        // Check the last txn_heart_beat has set the lock's TTL to 100.
        txn_heart_beat::tests::must_success(&mut engine, k, ts(10, 0), 90, 100);

        // TTL not expired. Do nothing but returns an error.
        must_err(&mut engine, k, ts(10, 0), ts(20, 0));
        must_locked(&mut engine, k, ts(10, 0));

        // Try to cleanup another transaction's lock. Does nothing.
        must_succeed(&mut engine, k, ts(10, 1), ts(120, 0));
        // If there is no existing lock when cleanup, it may be a pessimistic
        // transaction, so the rollback should be protected.
        must_get_rollback_protected(&mut engine, k, ts(10, 1), true);
        must_locked(&mut engine, k, ts(10, 0));

        // TTL expired. The lock should be removed.
        must_succeed(&mut engine, k, ts(10, 0), ts(120, 0));
        must_unlocked(&mut engine, k);
        // Rollbacks of optimistic transactions need to be protected
        // See: https://github.com/tikv/tikv/issues/16620
        must_get_rollback_protected(&mut engine, k, ts(10, 0), true);
        must_get_rollback_ts(&mut engine, k, ts(10, 0));

        // Rollbacks of primary keys in pessimistic transactions should be protected
        must_acquire_pessimistic_lock(&mut engine, k, k, ts(11, 1), ts(12, 1));
        must_succeed(&mut engine, k, ts(11, 1), ts(120, 0));
        must_get_rollback_protected(&mut engine, k, ts(11, 1), true);

        must_acquire_pessimistic_lock(&mut engine, k, k, ts(13, 1), ts(14, 1));
        must_pessimistic_prewrite_put(
            &mut engine,
            k,
            v,
            k,
            ts(13, 1),
            ts(14, 1),
            DoPessimisticCheck,
        );
        must_succeed(&mut engine, k, ts(13, 1), ts(120, 0));
        must_get_rollback_protected(&mut engine, k, ts(13, 1), true);
    }

    #[test]
    fn test_cleanup_shared_lock() {
        let ts = TimeStamp::compose;
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let pk = b"pk";
        let shared_lock_key = b"shared";

        // Case 1: shared pessimistic lock cleaned up after TTL expiration.
        must_acquire_shared_pessimistic_lock(
            &mut engine,
            shared_lock_key,
            pk,
            ts(20, 0),
            ts(25, 0),
            10,
        );
        let shared_lock = must_load_shared_lock(&mut engine, shared_lock_key);
        assert_eq!(shared_lock.len(), 1);

        must_err(&mut engine, shared_lock_key, ts(20, 0), ts(30, 0));
        must_succeed(&mut engine, shared_lock_key, ts(20, 0), ts(40, 0));
        must_unlocked(&mut engine, shared_lock_key);
        must_get_rollback_protected(&mut engine, shared_lock_key, ts(20, 0), false);

        // Case 2: prewritten shared lock cleaned up after TTL expiration.
        must_acquire_shared_pessimistic_lock(
            &mut engine,
            shared_lock_key,
            pk,
            ts(30, 0),
            ts(35, 0),
            10,
        );
        must_shared_prewrite_lock(&mut engine, shared_lock_key, pk, ts(30, 0), ts(35, 0));

        let mut shared_lock = must_load_shared_lock(&mut engine, shared_lock_key);
        assert_eq!(shared_lock.len(), 1);
        let sub_lock = shared_lock.get_lock(&ts(30, 0)).unwrap().unwrap();
        assert_eq!(sub_lock.lock_type, txn_types::LockType::Lock);
        assert_eq!(sub_lock.ttl, 10);

        must_err(&mut engine, shared_lock_key, ts(30, 0), ts(40, 0));
        must_succeed(&mut engine, shared_lock_key, ts(30, 0), ts(45, 0));
        must_unlocked(&mut engine, shared_lock_key);
        must_get_rollback_protected(&mut engine, shared_lock_key, ts(30, 0), false);
    }

    #[test]
    fn test_cleanup_shared_lock_returns_released_lock() {
        let ts = TimeStamp::compose;
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let pk = b"pk";
        let shared_lock_key = b"shared-release";

        must_acquire_shared_pessimistic_lock(
            &mut engine,
            shared_lock_key,
            pk,
            ts(40, 0),
            ts(45, 0),
            10,
        );
        must_acquire_shared_pessimistic_lock(
            &mut engine,
            shared_lock_key,
            pk,
            ts(50, 0),
            ts(55, 0),
            10,
        );
        let shared_lock = must_load_shared_lock(&mut engine, shared_lock_key);
        assert_eq!(shared_lock.len(), 2);

        for start_ts in [40, 50] {
            let last_lock = start_ts == 50;
            let snapshot = engine.snapshot(Default::default()).unwrap();
            let current_ts = ts(start_ts + 20, 0);
            let start_ts = ts(start_ts, 0);
            let cm = ConcurrencyManager::new_for_test(current_ts);
            let mut txn = MvccTxn::new(start_ts, cm);
            let mut reader = SnapshotReader::new(start_ts, snapshot, true);
            let released = cleanup(
                &mut txn,
                &mut reader,
                Key::from_raw(shared_lock_key),
                current_ts,
                true,
            )
            .unwrap();
            write(&engine, &Context::default(), txn.into_modifies());
            must_get_rollback_protected(&mut engine, shared_lock_key, start_ts, false);

            if !last_lock {
                assert!(released.is_none());
                continue;
            }
            assert!(released.is_some());
            let released = released.unwrap();
            assert_eq!(released.start_ts, start_ts);
            assert!(released.commit_ts.is_zero());
            assert!(released.pessimistic);
        }
        must_unlocked(&mut engine, shared_lock_key);
    }
}
