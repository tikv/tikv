// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::ops::ControlFlow;

use tikv_kv::SnapshotExt;
// #[PerformanceCriticalPath]
use txn_types::{Key, Lock, SharedLocks, TimeStamp, Write, WriteType};

use crate::storage::{
    Snapshot, TxnStatus,
    mvcc::{
        ErrorInner, LockType, MvccTxn, ReleasedLock, Result, SnapshotReader, TxnCommitRecord,
        metrics::MVCC_CHECK_TXN_STATUS_COUNTER_VEC, reader::OverlappedWrite,
    },
};

// The returned `TxnStatus` is Some(..) if the transaction status is already
// determined.
fn check_txn_status_from_pessimistic_primary_lock(
    txn: &mut MvccTxn,
    reader: &mut SnapshotReader<impl Snapshot>,
    primary_key: Key,
    lock: &Lock,
    current_ts: TimeStamp,
    resolving_pessimistic_lock: bool,
) -> Result<(Option<TxnStatus>, Option<ReleasedLock>)> {
    assert!(lock.is_pessimistic_lock());
    // Check the storage information first in case the force lock could be stale.
    // See https://github.com/pingcap/tidb/issues/43540 for more details.
    if lock.is_pessimistic_lock_with_conflict() {
        // Use `check_txn_status_missing_lock` to check if there exists a commit or
        // rollback record in the write CF, if so the current primary
        // pessimistic lock is stale. Otherwise the primary pessimistic lock is
        // regarded as valid, and the transaction status is determined by it.
        if let Some(txn_status) = check_determined_txn_status(reader, &primary_key)? {
            info!("unlock stale pessimistic primary lock";
                "primary_key" => ?&primary_key,
                "lock" => ?&lock,
                "current_ts" => current_ts,
                "resolving_pessimistic_lock" => ?resolving_pessimistic_lock,
            );
            let released = txn.unlock_key(primary_key, true, TimeStamp::zero());
            MVCC_CHECK_TXN_STATUS_COUNTER_VEC.pessimistic_rollback.inc();
            return Ok((Some(txn_status), released));
        }
    }

    // The primary pessimistic lock has expired, and this lock is regarded as valid
    // primary lock. If `resolving_pessimistic_lock` is false, it means the
    // secondary lock is a prewrite lock and the transaction must already be in
    // commit phase, thus the primary key must NOT change any more. In this case
    // if primary lock expires, unlock it and put a rollback record.
    // If `resolving_pessimistic_lock` is true. The transaction may still be ongoing
    // and it's not in commit phase, the primary key could still change. If the
    // primary lock expires, just pessimistically rollback it but do NOT put an
    // rollback record.
    if lock.ts.physical() + lock.ttl < current_ts.physical() {
        return if resolving_pessimistic_lock {
            let released = txn.unlock_key(primary_key, true, TimeStamp::zero());
            MVCC_CHECK_TXN_STATUS_COUNTER_VEC.pessimistic_rollback.inc();
            Ok((Some(TxnStatus::PessimisticRollBack), released))
        } else {
            let released = rollback_lock(txn, reader, primary_key, lock, true, true)?;
            MVCC_CHECK_TXN_STATUS_COUNTER_VEC.rollback.inc();
            Ok((Some(TxnStatus::TtlExpire), released))
        };
    }

    Ok((None, None))
}

/// Evaluate transaction status if a lock exists with the anticipated
/// 'start_ts'.
///
/// 1. Validate whether the existing lock indeed corresponds to the
///    primary lock. The primary key may switch under certain circumstances. If
///    it's a stale lock, the transaction status should not be determined by it.
///    Refer to https://github.com/pingcap/tidb/issues/42937 for additional information.
///    Note that the primary key should remain unaltered if the transaction is
///    already in the commit or 2PC phase.
///
/// 2. Manage the check in accordance with the primary lock type: 2.1 For the
///    pessimistic type: 2.1.1 If it's a forced lock, validate the storage data
///    initially to ensure the forced lock isn't stale. 2.1.2 If it's a regular
///    lock, verify the lock's TTL and the current timestamp to determine the
///    status. If the `resolving_pessimistic` parameter is true, perform a
///    pessimistic rollback, else carry out a real rollback. 2.2 For the
///    prewrite type, verify the lock's TTL and the current timestamp to decide
///    the status.
/// 3. Perform required operations on the valid primary lock, such as
///    incrementing `min_commit_ts`. The actual procedure for executing the
///    rollback differs based on the presence or absence of an overlapping write
///    record.
pub fn check_txn_status_lock_exists(
    txn: &mut MvccTxn,
    reader: &mut SnapshotReader<impl Snapshot>,
    primary_key: Key,
    mut lock: Lock,
    current_ts: TimeStamp,
    caller_start_ts: TimeStamp,
    force_sync_commit: bool,
    resolving_pessimistic_lock: bool,
    verify_is_primary: bool,
    rollback_if_not_exist: bool,
) -> Result<(TxnStatus, Option<ReleasedLock>)> {
    if verify_is_primary && !primary_key.is_encoded_from(&lock.primary) {
        // If the resolving lock is a prewrite lock and the current lock is a
        // pessimistic lock, the primary key in the prewrite lock must be valid.
        // So if the current lock dose not match it must be invalid, unlock the
        // invalid lock and check the transaction status with the lock missing path.
        return match (resolving_pessimistic_lock, lock.is_pessimistic_lock()) {
            (false, true) => {
                info!("unlock invalid pessimistic primary lock";
                    "primary_key" => ?&primary_key,
                    "lock" => ?&lock,
                    "current_ts" => current_ts,
                    "resolving_pessimistic_lock" => ?resolving_pessimistic_lock,
                );
                let txn_status = check_txn_status_missing_lock(
                    txn,
                    reader,
                    primary_key.clone(),
                    None,
                    MissingLockAction::rollback(rollback_if_not_exist),
                    resolving_pessimistic_lock,
                )?;
                let released = txn.unlock_key(primary_key, true, TimeStamp::zero());
                MVCC_CHECK_TXN_STATUS_COUNTER_VEC.pessimistic_rollback.inc();
                Ok((txn_status, released))
            }
            _ => {
                warn!("mismatch primary key and lock";
                    "primary_key" => ?&primary_key,
                    "lock" => ?&lock,
                    "current_ts" => current_ts,
                    "resolving_pessimistic_lock" => ?resolving_pessimistic_lock,
                    "rollback_if_not_exist" => rollback_if_not_exist,
                );
                // Return the current lock info to tell the client what the actual primary is.
                Err(
                    ErrorInner::PrimaryMismatch(lock.into_lock_info(primary_key.into_raw()?))
                        .into(),
                )
            }
        };
    }

    // Never rollback or push forward min_commit_ts in check_txn_status if it's
    // using async commit. Rollback of async-commit locks are done during
    // ResolveLock.
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
    if lock.is_pessimistic_lock() {
        let check_result = check_txn_status_from_pessimistic_primary_lock(
            txn,
            reader,
            primary_key.clone(),
            &lock,
            current_ts,
            resolving_pessimistic_lock,
        )?;
        // Return if the primary lock is stale or the transaction status is decided.
        if let (Some(txn_status), Some(released_lock)) = check_result {
            return Ok((txn_status, Some(released_lock)));
        }
        assert!(check_result.0.is_none() && check_result.1.is_none());
    } else if lock.ts.physical() + lock.ttl < current_ts.physical() {
        if lock.generation > 0 {
            warn!("flushed lock has been rolled back";
                "lock" => ?&lock,
                "current_ts" => current_ts,
                "caller_start_ts" => caller_start_ts,
            );
        }
        let released = rollback_lock(txn, reader, primary_key, &lock, is_pessimistic_txn, true)?;
        MVCC_CHECK_TXN_STATUS_COUNTER_VEC.rollback.inc();
        return Ok((TxnStatus::TtlExpire, released));
    }

    // If lock.min_commit_ts is 0, it's not a large transaction and we can't push
    // forward its min_commit_ts otherwise the transaction can't be committed by
    // old version TiDB during rolling update.
    if !lock.min_commit_ts.is_zero()
        && !caller_start_ts.is_max()
        // Push forward the min_commit_ts so that reading won't be blocked by locks.
        && caller_start_ts >= lock.min_commit_ts
    {
        lock.min_commit_ts = caller_start_ts.next();

        if lock.min_commit_ts < current_ts {
            lock.min_commit_ts = current_ts;
        }

        txn.put_lock(primary_key, &lock, false);
        MVCC_CHECK_TXN_STATUS_COUNTER_VEC.update_ts.inc();
    }

    // As long as the primary lock's min_commit_ts > caller_start_ts, locks belong
    // to the same transaction can't block reading. Return MinCommitTsPushed
    // result to the client to let it bypass locks.
    let min_commit_ts_pushed = (!caller_start_ts.is_zero() && lock.min_commit_ts > caller_start_ts)
        // If the caller_start_ts is max, it's a point get in the autocommit transaction.
        // We don't push forward lock's min_commit_ts and the point get can ignore the lock
        // next time because it's not committed yet.
        || caller_start_ts.is_max();

    Ok((TxnStatus::uncommitted(lock, min_commit_ts_pushed), None))
}

/// Status/TTL operations on one shared primary never replace the wrapper or
/// modify the other transactions stored at the same physical key.
pub fn check_txn_status_shared_lock(
    txn: &mut MvccTxn,
    reader: &mut SnapshotReader<impl Snapshot>,
    primary_key: Key,
    mut shared_locks: SharedLocks,
    current_ts: TimeStamp,
    caller_start_ts: TimeStamp,
    resolving_pessimistic_lock: bool,
    verify_is_primary: bool,
    rollback_if_not_exist: bool,
) -> Result<(TxnStatus, Option<ReleasedLock>)> {
    let mut lock = shared_locks.get_lock(&reader.start_ts)?.unwrap().clone();
    let status = if verify_is_primary && !primary_key.is_encoded_from(&lock.primary) {
        if resolving_pessimistic_lock || !lock.is_pessimistic_lock() {
            return Err(
                ErrorInner::PrimaryMismatch(lock.into_lock_info(primary_key.into_raw()?)).into(),
            );
        }
        // A secondary that has prewritten already names the authoritative primary.
        // Resolve its outcome before discarding this stale pessimistic member.
        let status = check_txn_status_missing_lock(
            txn,
            reader,
            primary_key.clone(),
            None,
            MissingLockAction::rollback(rollback_if_not_exist),
            resolving_pessimistic_lock,
        )?;
        MVCC_CHECK_TXN_STATUS_COUNTER_VEC.pessimistic_rollback.inc();
        Some(status)
    } else {
        match read_txn_status(reader, &primary_key)? {
            ControlFlow::Break(status) => {
                if lock.is_pessimistic_lock() {
                    MVCC_CHECK_TXN_STATUS_COUNTER_VEC.pessimistic_rollback.inc();
                }
                Some(status)
            }
            ControlFlow::Continue(overlapped_write)
                if lock.ts.physical() + lock.ttl < current_ts.physical() =>
            {
                if lock.is_pessimistic_lock() && resolving_pessimistic_lock {
                    MVCC_CHECK_TXN_STATUS_COUNTER_VEC.pessimistic_rollback.inc();
                    Some(TxnStatus::PessimisticRollBack)
                } else {
                    write_shared_member_rollback_record(
                        txn,
                        reader,
                        &primary_key,
                        true,
                        overlapped_write,
                        true,
                    )?;
                    MVCC_CHECK_TXN_STATUS_COUNTER_VEC.rollback.inc();
                    Some(TxnStatus::TtlExpire)
                }
            }
            ControlFlow::Continue(_) => None,
        }
    };
    if let Some(status) = status {
        shared_locks.remove_lock(&reader.start_ts)?;
        let released = persist_remaining_shared_locks(txn, primary_key, shared_locks);
        return Ok((status, released));
    }
    if !lock.min_commit_ts.is_zero()
        && !caller_start_ts.is_max()
        && caller_start_ts >= lock.min_commit_ts
    {
        lock.min_commit_ts = std::cmp::max(caller_start_ts.next(), current_ts);
        shared_locks.update_lock(lock.clone())?;
        txn.put_shared_locks(primary_key, &shared_locks, false);
        MVCC_CHECK_TXN_STATUS_COUNTER_VEC.update_ts.inc();
    }
    let pushed = (!caller_start_ts.is_zero() && lock.min_commit_ts > caller_start_ts)
        || caller_start_ts.is_max();
    Ok((TxnStatus::uncommitted(lock, pushed), None))
}

// Check transaction status from storage for the primary key, this function
// would have no impact on the transaction status, it is read only and would not
// write anything. The returned `TxnStatus` is Some(..) if it's already
// determined.
pub fn check_determined_txn_status(
    reader: &mut SnapshotReader<impl Snapshot>,
    primary_key: &Key,
) -> Result<Option<TxnStatus>> {
    Ok(read_txn_status(reader, primary_key)?.break_value())
}

/// A terminal record ends status resolution. Otherwise keep the overlapping
/// write so rollback can protect it without scanning the version chain again.
fn read_txn_status(
    reader: &mut SnapshotReader<impl Snapshot>,
    primary_key: &Key,
) -> Result<ControlFlow<TxnStatus, Option<OverlappedWrite>>> {
    MVCC_CHECK_TXN_STATUS_COUNTER_VEC.get_commit_info.inc();
    Ok(match reader.get_txn_commit_record(primary_key)? {
        TxnCommitRecord::SingleRecord { commit_ts, write }
            if write.write_type != WriteType::Rollback =>
        {
            ControlFlow::Break(TxnStatus::committed(commit_ts))
        }
        TxnCommitRecord::SingleRecord { .. } | TxnCommitRecord::OverlappedRollback { .. } => {
            ControlFlow::Break(TxnStatus::RolledBack)
        }
        TxnCommitRecord::None { overlapped_write } => ControlFlow::Continue(overlapped_write),
    })
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
    // Lock is never shared in the current branch's architecture - shared locks use
    // SharedLocks type
    let overlapped_write = match reader.get_txn_commit_record(&key)? {
        TxnCommitRecord::None { overlapped_write } => overlapped_write,
        TxnCommitRecord::SingleRecord { write, commit_ts }
            if write.write_type != WriteType::Rollback =>
        {
            panic!(
                "txn record found but not expected: {:?} {} {:?} {:?} [region_id={}]",
                write,
                commit_ts,
                txn,
                lock,
                reader.reader.snapshot_ext().get_region_id().unwrap_or(0)
            )
        }
        _ => return Ok(txn.unlock_key(key, is_pessimistic_txn, TimeStamp::zero())),
    };

    // If prewrite type is DEL or LOCK or PESSIMISTIC, it is no need to delete
    // value.
    if lock.short_value.is_none() && lock.lock_type == LockType::Put {
        txn.delete_value(key.clone(), lock.ts);
    }

    // (1) The primary key of any transaction needs to be protected.
    //
    // (2) If the lock belongs to a pipelined-DML transaction, it must be protected.
    //
    // This is for avoiding false positive of assertion failures.
    // Consider the sequence of events happening on a same key:
    // 1. T0 commits at commit_ts=10
    // 2. T1(pipelined-DML) with start_ts=10 flushes, and assert exist. The
    //    assertion passes.
    // 3. T2 rolls back T1. The lock is removed, if this is not protected, there's
    //    no clue left that indicates T1 is rolled back.
    // 4. T1 flushes again, and assert not exist. It observes T0's commit and
    //    assertion failed.
    // If the lock is protected, the second flush will detect the conflict and
    // return a write conflict error.
    let protected: bool = key.is_encoded_from(&lock.primary) || (lock.generation > 0);
    if let Some(write) = make_rollback(reader.start_ts, protected, overlapped_write) {
        txn.put_write(key.clone(), reader.start_ts, write.as_ref().to_bytes());
    }

    if collapse_rollback {
        collapse_prev_rollback(txn, reader, &key)?;
    }

    Ok(txn.unlock_key(key, is_pessimistic_txn, TimeStamp::zero()))
}

/// Roll back one shared member and preserve every other holder on the key.
pub fn rollback_shared_lock(
    txn: &mut MvccTxn,
    reader: &mut SnapshotReader<impl Snapshot>,
    key: Key,
    mut shared_locks: SharedLocks,
    lock_ts: TimeStamp,
    collapse_rollback: bool,
) -> Result<Option<ReleasedLock>> {
    let lock = match shared_locks.remove_lock(&lock_ts)? {
        Some(l) => l,
        None => {
            // misuse of rollback_shared_lock? maybe print a warning with stack?
            return Ok(None);
        }
    };

    rollback_shared_member_record(txn, reader, &key, &lock, collapse_rollback)?;
    Ok(persist_remaining_shared_locks(txn, key, shared_locks))
}

fn persist_remaining_shared_locks(
    txn: &mut MvccTxn,
    key: Key,
    shared_locks: SharedLocks,
) -> Option<ReleasedLock> {
    if shared_locks.is_empty() {
        txn.unlock_key(key, true, TimeStamp::zero())
    } else {
        txn.put_remaining_shared_locks(key, shared_locks);
        None
    }
}

pub(super) fn rollback_shared_member_record(
    txn: &mut MvccTxn,
    reader: &mut SnapshotReader<impl Snapshot>,
    key: &Key,
    lock: &Lock,
    collapse_rollback: bool,
) -> Result<()> {
    let overlapped_write = match reader.get_txn_commit_record(key)? {
        TxnCommitRecord::None { overlapped_write } => overlapped_write,
        TxnCommitRecord::SingleRecord { write, commit_ts }
            if write.write_type != WriteType::Rollback =>
        {
            panic!(
                "txn record found but not expected: {:?} {} {:?} {:?} [region_id={}]",
                write,
                commit_ts,
                txn,
                lock,
                reader.reader.snapshot_ext().get_region_id().unwrap_or(0)
            )
        }
        _ => return Ok(()),
    };

    assert!(lock.generation == 0);
    write_shared_member_rollback_record(
        txn,
        reader,
        key,
        key.is_encoded_from(&lock.primary),
        overlapped_write,
        collapse_rollback,
    )
}

fn write_shared_member_rollback_record(
    txn: &mut MvccTxn,
    reader: &mut SnapshotReader<impl Snapshot>,
    key: &Key,
    protected: bool,
    overlapped_write: Option<OverlappedWrite>,
    collapse_rollback: bool,
) -> Result<()> {
    // A shared primary owns the transaction outcome just like an exclusive
    // primary. Its rollback must survive collapse while unrelated members remain.
    if let Some(write) = make_rollback(reader.start_ts, protected, overlapped_write) {
        txn.put_write(key.clone(), reader.start_ts, write.as_ref().to_bytes());
    }

    if collapse_rollback {
        collapse_prev_rollback(txn, reader, key)?;
    }

    Ok(())
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

/// Generate the Write record that should be written that means to perform a
/// specified rollback operation.
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

#[derive(Debug, Copy, Clone, PartialEq)]
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
