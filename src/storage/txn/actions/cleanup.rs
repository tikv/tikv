use crate::storage::mvcc::txn::MissingLockAction;
use crate::storage::mvcc::{
    metrics::{MVCC_CONFLICT_COUNTER, MVCC_DUPLICATE_CMD_COUNTER_VEC},
    ErrorInner, Key, MvccTxn, ReleasedLock, Result as MvccResult, TimeStamp,
};
use crate::storage::{Snapshot, TxnStatus};

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
        crate::storage::mvcc::txn::make_txn_error(err, &key, txn.start_ts,).into()
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
        l => match txn.check_txn_status_missing_lock(
            key,
            l,
            MissingLockAction::rollback_protect(protect_rollback),
        )? {
            TxnStatus::Committed { commit_ts } => {
                MVCC_CONFLICT_COUNTER.rollback_committed.inc();
                Err(ErrorInner::Committed { commit_ts }.into())
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
