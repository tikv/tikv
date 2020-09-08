// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::mvcc::{
    metrics::{MVCC_CONFLICT_COUNTER, MVCC_DUPLICATE_CMD_COUNTER_VEC},
    ErrorInner, LockType, MvccTxn, ReleasedLock, Result as MvccResult,
};
use crate::storage::Snapshot;
use txn_types::{Key, TimeStamp, Write, WriteType};

pub fn commit<S: Snapshot>(
    txn: &mut MvccTxn<S>,
    key: Key,
    commit_ts: TimeStamp,
) -> MvccResult<Option<ReleasedLock>> {
    fail_point!("commit", |err| Err(
        crate::storage::mvcc::txn::make_txn_error(err, &key, txn.start_ts,).into()
    ));

    let mut lock = match txn.reader.load_lock(&key)? {
        Some(mut lock) if lock.ts == txn.start_ts => {
            // A lock with larger min_commit_ts than current commit_ts can't be committed
            if commit_ts < lock.min_commit_ts {
                info!(
                    "trying to commit with smaller commit_ts than min_commit_ts";
                    "key" => %key,
                    "start_ts" => txn.start_ts,
                    "commit_ts" => commit_ts,
                    "min_commit_ts" => lock.min_commit_ts,
                );
                return Err(ErrorInner::CommitTsExpired {
                    start_ts: txn.start_ts,
                    commit_ts,
                    key: key.into_raw()?,
                    min_commit_ts: lock.min_commit_ts,
                }
                .into());
            }

            // It's an abnormal routine since pessimistic locks shouldn't be committed in our
            // transaction model. But a pessimistic lock will be left if the pessimistic
            // rollback request fails to send and the transaction need not to acquire
            // this lock again(due to WriteConflict). If the transaction is committed, we
            // should commit this pessimistic lock too.
            if lock.lock_type == LockType::Pessimistic {
                warn!(
                    "commit a pessimistic lock with Lock type";
                    "key" => %key,
                    "start_ts" => txn.start_ts,
                    "commit_ts" => commit_ts,
                );
                // Commit with WriteType::Lock.
                lock.lock_type = LockType::Lock;
            }
            lock
        }
        _ => {
            return match txn.reader.get_txn_commit_record(&key, txn.start_ts)?.info() {
                Some((_, WriteType::Rollback)) | None => {
                    MVCC_CONFLICT_COUNTER.commit_lock_not_found.inc();
                    // None: related Rollback has been collapsed.
                    // Rollback: rollback by concurrent transaction.
                    info!(
                        "txn conflict (lock not found)";
                        "key" => %key,
                        "start_ts" => txn.start_ts,
                        "commit_ts" => commit_ts,
                    );
                    Err(ErrorInner::TxnLockNotFound {
                        start_ts: txn.start_ts,
                        commit_ts,
                        key: key.into_raw()?,
                    }
                    .into())
                }
                // Committed by concurrent transaction.
                Some((_, WriteType::Put))
                | Some((_, WriteType::Delete))
                | Some((_, WriteType::Lock)) => {
                    MVCC_DUPLICATE_CMD_COUNTER_VEC.commit.inc();
                    Ok(None)
                }
            };
        }
    };
    let mut write = Write::new(
        WriteType::from_lock_type(lock.lock_type).unwrap(),
        txn.start_ts,
        lock.short_value.take(),
    );

    for ts in &lock.rollback_ts {
        if *ts == commit_ts {
            write = write.set_overlapped_rollback(true);
            break;
        }
    }

    txn.put_write(key.clone(), commit_ts, write.as_ref().to_bytes());
    Ok(txn.unlock_key(key, lock.is_pessimistic_txn()))
}

pub mod tests {
    use super::*;
    use crate::storage::mvcc::tests::write;
    use crate::storage::mvcc::MvccTxn;
    use crate::storage::Engine;
    use concurrency_manager::ConcurrencyManager;
    use kvproto::kvrpcpb::Context;
    use txn_types::TimeStamp;

    pub fn must_succeed<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let start_ts = start_ts.into();
        let cm = ConcurrencyManager::new(start_ts);
        let mut txn = MvccTxn::new(snapshot, start_ts, true, cm);
        commit(&mut txn, Key::from_raw(key), commit_ts.into()).unwrap();
        write(engine, &ctx, txn.into_modifies());
    }

    pub fn must_err<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let start_ts = start_ts.into();
        let cm = ConcurrencyManager::new(start_ts);
        let mut txn = MvccTxn::new(snapshot, start_ts, true, cm);
        assert!(commit(&mut txn, Key::from_raw(key), commit_ts.into()).is_err());
    }
}
