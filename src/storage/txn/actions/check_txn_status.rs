// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use txn_types::{Key, Lock, TimeStamp, Write, WriteType};

use crate::storage::{
    mvcc::{
        metrics::MVCC_CHECK_TXN_STATUS_COUNTER_VEC, reader::OverlappedWrite, ErrorInner, LockType,
        MvccTxn, ReleasedLock, Result, SnapshotReader, TxnCommitRecord,
    },
    Snapshot, TxnStatus,
};

// Check whether there's an overlapped write record, and then perform rollback. The actual behavior
// to do the rollback differs according to whether there's an overlapped write record.
pub fn check_txn_status_lock_exists(
    txn: &mut MvccTxn,
    reader: &mut SnapshotReader<impl Snapshot>,
    primary_key: Key,
    mut lock: Lock,
    current_ts: TimeStamp,
    caller_start_ts: TimeStamp,
    force_sync_commit: bool,
    resolving_pessimistic_lock: bool,
) -> Result<(TxnStatus, Option<ReleasedLock>)> {
    // Never rollback or push forward min_commit_ts in check_txn_status if it's using async commit.
    // Rollback of async-commit locks are done during ResolveLock.
    if lock.use_async_commit {
        if force_sync_commit {
            info!(
                "fallback is set, check_txn_status treats it as a non-async-commit txn";
                "start_ts" => reader.start_ts,
                "primary_key" => ?primary_key,
            );
        } else {
            return Ok((TxnStatus::uncommitted(lock, false), None));
        }
    }

    let is_pessimistic_txn = !lock.for_update_ts.is_zero();
    if lock.ts.physical() + lock.ttl < current_ts.physical() {
        // If the lock is expired, clean it up.
        // If the resolving and primary key lock are both pessimistic locks, just unlock the
        // primary pessimistic lock and do not write rollback records.
        return if resolving_pessimistic_lock && lock.lock_type == LockType::Pessimistic {
            let released = txn.unlock_key(primary_key, is_pessimistic_txn);
            MVCC_CHECK_TXN_STATUS_COUNTER_VEC.pessimistic_rollback.inc();
            Ok((TxnStatus::PessimisticRollBack, released))
        } else {
            let released =
                rollback_lock(txn, reader, primary_key, &lock, is_pessimistic_txn, true)?;
            MVCC_CHECK_TXN_STATUS_COUNTER_VEC.rollback.inc();
            Ok((TxnStatus::TtlExpire, released))
        };
    }

    // If lock.min_commit_ts is 0, it's not a large transaction and we can't push forward
    // its min_commit_ts otherwise the transaction can't be committed by old version TiDB
    // during rolling update.
    if !lock.min_commit_ts.is_zero()
        && !caller_start_ts.is_max()
        // Push forward the min_commit_ts so that reading won't be blocked by locks.
        && caller_start_ts >= lock.min_commit_ts
    {
        lock.min_commit_ts = caller_start_ts.next();

        if lock.min_commit_ts < current_ts {
            lock.min_commit_ts = current_ts;
        }

        txn.put_lock(primary_key, &lock);
        MVCC_CHECK_TXN_STATUS_COUNTER_VEC.update_ts.inc();
    }

    // As long as the primary lock's min_commit_ts > caller_start_ts, locks belong to the same transaction
    // can't block reading. Return MinCommitTsPushed result to the client to let it bypass locks.
    let min_commit_ts_pushed = (!caller_start_ts.is_zero() && lock.min_commit_ts > caller_start_ts)
        // If the caller_start_ts is max, it's a point get in the autocommit transaction.
        // We don't push forward lock's min_commit_ts and the point get can ignore the lock
        // next time because it's not committed yet.
        || caller_start_ts.is_max();

    Ok((TxnStatus::uncommitted(lock, min_commit_ts_pushed), None))
}

pub fn check_txn_status_missing_lock(
    txn: &mut MvccTxn,
    reader: &mut SnapshotReader<impl Snapshot>,
    primary_key: Key,
    mismatch_lock: Option<Lock>,
    action: MissingLockAction,
    resolving_pessimistic_lock: bool,
) -> Result<TxnStatus> {
    MVCC_CHECK_TXN_STATUS_COUNTER_VEC.get_commit_info.inc();

    match reader.get_txn_commit_record(&primary_key)? {
        TxnCommitRecord::SingleRecord { commit_ts, write } => {
            if write.write_type == WriteType::Rollback {
                Ok(TxnStatus::RolledBack)
            } else {
                Ok(TxnStatus::committed(commit_ts))
            }
        }
        TxnCommitRecord::OverlappedRollback { .. } => Ok(TxnStatus::RolledBack),
        TxnCommitRecord::None { overlapped_write } => {
            if MissingLockAction::ReturnError == action {
                return Err(ErrorInner::TxnNotFound {
                    start_ts: reader.start_ts,
                    key: primary_key.into_raw()?,
                }
                .into());
            }
            if resolving_pessimistic_lock {
                return Ok(TxnStatus::LockNotExistDoNothing);
            }

            let ts = reader.start_ts;

            // collapse previous rollback if exist.
            if action.collapse_rollback() {
                collapse_prev_rollback(txn, reader, &primary_key)?;
            }

            if let (Some(l), None) = (mismatch_lock, overlapped_write.as_ref()) {
                txn.mark_rollback_on_mismatching_lock(
                    &primary_key,
                    l,
                    action == MissingLockAction::ProtectedRollback,
                );
            }

            // Insert a Rollback to Write CF in case that a stale prewrite
            // command is received after a cleanup command.
            if let Some(write) = action.construct_write(ts, overlapped_write) {
                txn.put_write(primary_key, ts, write.as_ref().to_bytes());
            }
            MVCC_CHECK_TXN_STATUS_COUNTER_VEC.rollback.inc();

            Ok(TxnStatus::LockNotExist)
        }
    }
}

pub fn rollback_lock(
    txn: &mut MvccTxn,
    reader: &mut SnapshotReader<impl Snapshot>,
    key: Key,
    lock: &Lock,
    is_pessimistic_txn: bool,
    collapse_rollback: bool,
) -> Result<Option<ReleasedLock>> {
    let overlapped_write = match reader.get_txn_commit_record(&key)? {
        TxnCommitRecord::None { overlapped_write } => overlapped_write,
        TxnCommitRecord::SingleRecord { write, .. } if write.write_type != WriteType::Rollback => {
            panic!("txn record found but not expected: {:?}", txn)
        }
        _ => return Ok(txn.unlock_key(key, is_pessimistic_txn)),
    };

    // If prewrite type is DEL or LOCK or PESSIMISTIC, it is no need to delete value.
    if lock.short_value.is_none() && lock.lock_type == LockType::Put {
        txn.delete_value(key.clone(), lock.ts);
    }

    // Only the primary key of a pessimistic transaction needs to be protected.
    let protected: bool = is_pessimistic_txn && key.is_encoded_from(&lock.primary);
    if let Some(write) = make_rollback(reader.start_ts, protected, overlapped_write) {
        txn.put_write(key.clone(), reader.start_ts, write.as_ref().to_bytes());
    }

    if collapse_rollback {
        collapse_prev_rollback(txn, reader, &key)?;
    }

    Ok(txn.unlock_key(key, is_pessimistic_txn))
}

pub fn collapse_prev_rollback(
    txn: &mut MvccTxn,
    reader: &mut SnapshotReader<impl Snapshot>,
    key: &Key,
) -> Result<()> {
    if let Some((commit_ts, write)) = reader.seek_write(key, reader.start_ts)? {
        if write.write_type == WriteType::Rollback && !write.as_ref().is_protected() {
            txn.delete_write(key.clone(), commit_ts);
        }
    }
    Ok(())
}

/// Generate the Write record that should be written that means to perform a specified rollback
/// operation.
pub fn make_rollback(
    start_ts: TimeStamp,
    protected: bool,
    overlapped_write: Option<OverlappedWrite>,
) -> Option<Write> {
    match overlapped_write {
        Some(OverlappedWrite { write, gc_fence }) => {
            assert!(start_ts > write.start_ts);
            if protected {
                Some(write.set_overlapped_rollback(true, Some(gc_fence)))
            } else {
                // No need to update the original write.
                None
            }
        }
        None => Some(Write::new_rollback(start_ts, protected)),
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum MissingLockAction {
    Rollback,
    ProtectedRollback,
    ReturnError,
}

impl MissingLockAction {
    pub fn rollback_protect(protect_rollback: bool) -> MissingLockAction {
        if protect_rollback {
            MissingLockAction::ProtectedRollback
        } else {
            MissingLockAction::Rollback
        }
    }

    pub fn rollback(rollback_if_not_exist: bool) -> MissingLockAction {
        if rollback_if_not_exist {
            MissingLockAction::ProtectedRollback
        } else {
            MissingLockAction::ReturnError
        }
    }

    fn collapse_rollback(&self) -> bool {
        match self {
            MissingLockAction::Rollback => true,
            MissingLockAction::ProtectedRollback => false,
            _ => unreachable!(),
        }
    }

    pub fn construct_write(
        &self,
        ts: TimeStamp,
        overlapped_write: Option<OverlappedWrite>,
    ) -> Option<Write> {
        make_rollback(ts, !self.collapse_rollback(), overlapped_write)
    }
}
