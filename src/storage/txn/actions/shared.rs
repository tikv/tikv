use crate::storage::mvcc::metrics::CONCURRENCY_MANAGER_LOCK_DURATION_HISTOGRAM;
use crate::storage::mvcc::{Lock, LockType, MvccTxn, Result as MvccResult, TimeStamp};
use crate::storage::Snapshot;
use std::cmp;
use txn_types::{is_short_value, Key, Value};

pub fn prewrite_key_value<S: Snapshot>(
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

    let mut async_commit_ts = TimeStamp::zero();
    if let Some(secondary_keys) = secondary_keys {
        lock.use_async_commit = true;
        lock.secondaries = secondary_keys.to_owned();

        // This operation should not block because the latch makes sure only one thread
        // is operating on this key.
        let key_guard =
            CONCURRENCY_MANAGER_LOCK_DURATION_HISTOGRAM.observe_closure_duration(|| {
                ::futures_executor::block_on(txn.concurrency_manager.lock_key(&key))
            });

        async_commit_ts = key_guard.with_lock(|l| {
            let max_ts = txn.concurrency_manager.max_ts();
            fail_point!("before-set-lock-in-memory");
            let min_commit_ts = cmp::max(cmp::max(max_ts, txn.start_ts), for_update_ts).next();
            lock.min_commit_ts = cmp::max(lock.min_commit_ts, min_commit_ts);
            *l = Some(lock.clone());
            lock.min_commit_ts
        });

        txn.guards.push(key_guard);
    }

    txn.put_lock(key, &lock);
    Ok(async_commit_ts)
}
