use crate::storage::mvcc::txn::MissingLockAction;
use crate::storage::mvcc::{
    metrics::{MVCC_CONFLICT_COUNTER, MVCC_DUPLICATE_CMD_COUNTER_VEC},
    ErrorInner, Key, MvccTxn, ReleasedLock, Result as MvccResult, TimeStamp,
};
use crate::storage::{Snapshot, TxnStatus};

use super::check_txn_status::check_txn_status_missing_lock;

/// Cleanup the lock if it's TTL has expired, comparing with `current_ts`. If `current_ts` is 0,
/// cleanup the lock without checking TTL. If the lock is the primary lock of a pessimistic
/// transaction, the rollback record is protected from being collapsed.
///
/// Returns the released lock. Returns error if the key is locked or has already been
/// committed.
pub fn cleanup<S: Snapshot>(
    txn: &mut MvccTxn<S>,
    key: Key,
    current_ts: TimeStamp,
    protect_rollback: bool,
) -> MvccResult<Option<ReleasedLock>> {
    fail_point!("cleanup", |err| Err(
        crate::storage::mvcc::txn::make_txn_error(err, &key, txn.start_ts).into()
    ));

    match txn.reader.load_lock(&key)? {
        Some(ref lock) if lock.ts == txn.start_ts => {
            // If current_ts is not 0, check the Lock's TTL.
            // If the lock is not expired, do not rollback it but report key is locked.
            if !current_ts.is_zero() && lock.ts.physical() + lock.ttl >= current_ts.physical() {
                return Err(
                    ErrorInner::KeyIsLocked(lock.clone().into_lock_info(key.into_raw()?)).into(),
                );
            }

            let is_pessimistic_txn = !lock.for_update_ts.is_zero();
            txn.check_write_and_rollback_lock(key, lock, is_pessimistic_txn)
        }
        l => match check_txn_status_missing_lock(
            txn,
            key.clone(),
            l,
            MissingLockAction::rollback_protect(protect_rollback),
            false,
        )? {
            TxnStatus::Committed { commit_ts } => {
                MVCC_CONFLICT_COUNTER.rollback_committed.inc();
                Err(ErrorInner::Committed {
                    start_ts: txn.start_ts,
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
        },
    }
}

pub mod tests {
    use super::*;
    use crate::storage::mvcc::tests::{must_have_write, must_not_have_write, write};
    use crate::storage::mvcc::{Error as MvccError, MvccTxn, WriteType};
    use crate::storage::txn::tests::{must_commit, must_prewrite_put};
    use crate::storage::Engine;
    use concurrency_manager::ConcurrencyManager;
    use engine_traits::CF_WRITE;
    use kvproto::kvrpcpb::Context;
    use txn_types::TimeStamp;

    #[cfg(test)]
    use crate::storage::{
        mvcc::tests::{
            must_get_rollback_protected, must_get_rollback_ts, must_locked, must_unlocked,
            must_written,
        },
        txn::commands::txn_heart_beat,
        txn::tests::{must_acquire_pessimistic_lock, must_pessimistic_prewrite_put},
        TestEngineBuilder,
    };

    pub fn must_succeed<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        current_ts: impl Into<TimeStamp>,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let current_ts = current_ts.into();
        let cm = ConcurrencyManager::new(current_ts);
        let mut txn = MvccTxn::new(snapshot, start_ts.into(), true, cm);
        cleanup(&mut txn, Key::from_raw(key), current_ts, true).unwrap();
        write(engine, &ctx, txn.into_modifies());
    }

    pub fn must_err<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        current_ts: impl Into<TimeStamp>,
    ) -> MvccError {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let current_ts = current_ts.into();
        let cm = ConcurrencyManager::new(current_ts);
        let mut txn = MvccTxn::new(snapshot, start_ts.into(), true, cm);
        cleanup(&mut txn, Key::from_raw(key), current_ts, true).unwrap_err()
    }

    pub fn must_cleanup_with_gc_fence<E: Engine>(
        engine: &E,
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

        let cm = ConcurrencyManager::new(current_ts);
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut txn = MvccTxn::new(snapshot, start_ts, true, cm);
        cleanup(&mut txn, Key::from_raw(key), current_ts, true).unwrap();

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
        let engine = TestEngineBuilder::new().build().unwrap();
        must_prewrite_put(&engine, b"k", b"v", b"k", 10);
        must_commit(&engine, b"k", 10, 20);
        must_cleanup_with_gc_fence(&engine, b"k", 20, 0, 30, true);
        let w = must_written(&engine, b"k", 10, 20, WriteType::Put);
        assert!(w.has_overlapped_rollback);
        assert_eq!(w.gc_fence.unwrap(), 30.into());
    }

    #[test]
    fn test_cleanup() {
        // Cleanup's logic is mostly similar to rollback, except the TTL check. Tests that not
        // related to TTL check should be covered by other test cases.
        let engine = TestEngineBuilder::new().build().unwrap();

        // Shorthand for composing ts.
        let ts = TimeStamp::compose;

        let (k, v) = (b"k", b"v");

        must_prewrite_put(&engine, k, v, k, ts(10, 0));
        must_locked(&engine, k, ts(10, 0));
        txn_heart_beat::tests::must_success(&engine, k, ts(10, 0), 100, 100);
        // Check the last txn_heart_beat has set the lock's TTL to 100.
        txn_heart_beat::tests::must_success(&engine, k, ts(10, 0), 90, 100);

        // TTL not expired. Do nothing but returns an error.
        must_err(&engine, k, ts(10, 0), ts(20, 0));
        must_locked(&engine, k, ts(10, 0));

        // Try to cleanup another transaction's lock. Does nothing.
        must_succeed(&engine, k, ts(10, 1), ts(120, 0));
        // If there is no exisiting lock when cleanup, it may be a pessimistic transaction,
        // so the rollback should be protected.
        must_get_rollback_protected(&engine, k, ts(10, 1), true);
        must_locked(&engine, k, ts(10, 0));

        // TTL expired. The lock should be removed.
        must_succeed(&engine, k, ts(10, 0), ts(120, 0));
        must_unlocked(&engine, k);
        // Rollbacks of optimistic transactions needn't be protected
        must_get_rollback_protected(&engine, k, ts(10, 0), false);
        must_get_rollback_ts(&engine, k, ts(10, 0));

        // Rollbacks of primary keys in pessimistic transactions should be protected
        must_acquire_pessimistic_lock(&engine, k, k, ts(11, 1), ts(12, 1));
        must_succeed(&engine, k, ts(11, 1), ts(120, 0));
        must_get_rollback_protected(&engine, k, ts(11, 1), true);

        must_acquire_pessimistic_lock(&engine, k, k, ts(13, 1), ts(14, 1));
        must_pessimistic_prewrite_put(&engine, k, v, k, ts(13, 1), ts(14, 1), true);
        must_succeed(&engine, k, ts(13, 1), ts(120, 0));
        must_get_rollback_protected(&engine, k, ts(13, 1), true);
    }
}
