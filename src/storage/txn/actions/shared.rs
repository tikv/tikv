use crate::storage::mvcc::metrics::CONCURRENCY_MANAGER_LOCK_DURATION_HISTOGRAM;
use crate::storage::mvcc::{ErrorInner, Lock, LockType, MvccTxn, Result as MvccResult, TimeStamp};
use crate::storage::txn::commands::ReleasedLocks;
use crate::storage::Snapshot;
use fail::fail_point;
use std::cmp;
use txn_types::{is_short_value, Key, Value, Write, WriteType};

pub(super) fn prewrite_key_value<S: Snapshot>(
    txn: &mut MvccTxn<S>,
    key: Key,
    lock_type: LockType,
    primary: &[u8],
    secondary_keys: &Option<Vec<Vec<u8>>>,
    value: Option<Value>,
    lock_ttl: u64,
    for_update_ts: TimeStamp,
    txn_size: u64,
    min_commit_ts: TimeStamp,
    max_commit_ts: TimeStamp,
    try_one_pc: bool,
    has_pessimistic_lock: bool,
) -> MvccResult<TimeStamp> {
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

    let mut final_min_commit_ts = TimeStamp::zero();
    // The final_min_commit_ts will be calculated if either async commit or 1PC is enabled.
    // It's allowed to enable 1PC without enabling async commit.
    if secondary_keys.is_some() || try_one_pc {
        if let Some(secondary_keys) = secondary_keys {
            lock.use_async_commit = true;
            lock.secondaries = secondary_keys.to_owned();
        }

        // This operation should not block because the latch makes sure only one thread
        // is operating on this key.
        let key_guard =
            CONCURRENCY_MANAGER_LOCK_DURATION_HISTOGRAM.observe_closure_duration(|| {
                ::futures_executor::block_on(txn.concurrency_manager.lock_key(&key))
            });

        final_min_commit_ts = key_guard.with_lock(|l| {
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
    }

    if try_one_pc {
        txn.put_locks_for_1pc(key, lock, has_pessimistic_lock);
    } else {
        txn.put_lock(key, &lock);
    }

    fail_point!("after_prewrite_one_key");

    Ok(final_min_commit_ts)
}

pub(in crate::storage::txn) fn handle_1pc<S: Snapshot>(
    txn: &mut MvccTxn<S>,
    commit_ts: TimeStamp,
) -> ReleasedLocks {
    let mut released_locks = ReleasedLocks::new(txn.start_ts, commit_ts);

    for (key, lock, delete_pessimistic_lock) in std::mem::take(&mut txn.locks_for_1pc) {
        let write = Write::new(
            WriteType::from_lock_type(lock.lock_type).unwrap(),
            txn.start_ts,
            lock.short_value,
        );
        // Transactions committed with 1PC should be impossible to overwrite rollback records.
        txn.put_write(key.clone(), commit_ts, write.as_ref().to_bytes());
        if delete_pessimistic_lock {
            released_locks.push(txn.unlock_key(key, true));
        }
    }

    released_locks
}
