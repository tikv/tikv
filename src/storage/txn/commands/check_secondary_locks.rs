// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::kv::WriteData;
use crate::storage::lock_manager::LockManager;
use crate::storage::mvcc::txn::make_rollback;
use crate::storage::mvcc::{
    MvccTxn, ReleasedLock, SecondaryLockStatus, TimeStamp, TxnCommitRecord,
};
use crate::storage::txn::commands::{
    Command, CommandExt, ReleasedLocks, TypedCommand, WriteCommand, WriteContext, WriteResult,
};
use crate::storage::txn::Result;
use crate::storage::types::SecondaryLocksStatus;
use crate::storage::{ProcessResult, Snapshot};
use pd_client::PdClient;
use std::mem;
use txn_types::{Key, LockType, WriteType};

command! {
    /// Check secondary locks of an async commit transaction.
    ///
    /// If all prewritten locks exist, the lock information is returned.
    /// Otherwise, it returns the commit timestamp of the transaction.
    ///
    /// If the lock does not exist or is a pessimistic lock, to prevent the
    /// status being changed, a rollback may be written.
    CheckSecondaryLocks:
        cmd_ty => SecondaryLocksStatus,
        display => "kv::command::CheckSecondaryLocks {} keys@{} | {:?}", (keys.len, start_ts, ctx),
        content => {
            /// The keys of secondary locks.
            keys: Vec<Key>,
            /// The start timestamp of the transaction.
            start_ts: txn_types::TimeStamp,
        }
}

impl CommandExt for CheckSecondaryLocks {
    ctx!();
    tag!(check_secondary_locks);
    ts!(start_ts);
    write_bytes!(keys: multiple);
    gen_lock!(keys: multiple);
}

impl CheckSecondaryLocks {
    /// Check the status of a secondary (optimistic) lock.
    ///
    /// It checks whether the given secondary lock exists. If the lock exists,
    /// the lock information is returned. Otherwise, it searches the write CF
    /// for the commit record of the lock and returns the commit timestamp
    /// (0 if the lock is not committed).
    ///
    /// If the lock does not exist or is a pessimistic lock, to prevent the
    /// status being changed, a rollback may be written and this rollback
    /// needs to be protected.
    pub fn check_secondary_lock<S: Snapshot, P: PdClient + 'static>(
        &mut self,
        txn: &mut MvccTxn<S, P>,
        key: &Key,
    ) -> Result<(SecondaryLockStatus, Option<ReleasedLock>)> {
        let mut released_lock = None;
        let (status, need_rollback, rollback_overlapped_write) = match txn.reader.load_lock(&key)? {
            Some(lock) if lock.ts == self.start_ts => {
                if lock.lock_type == LockType::Pessimistic {
                    released_lock = txn.unlock_key(key.clone(), true);
                    let overlapped_write = txn
                        .reader
                        .get_txn_commit_record(&key, self.start_ts)?
                        .unwrap_none();
                    (SecondaryLockStatus::RolledBack, true, overlapped_write)
                } else {
                    (SecondaryLockStatus::Locked(lock), false, None)
                }
            }
            _ => match txn.reader.get_txn_commit_record(&key, self.start_ts)? {
                TxnCommitRecord::SingleRecord { commit_ts, write } => {
                    let status = if write.write_type != WriteType::Rollback {
                        SecondaryLockStatus::Committed(commit_ts)
                    } else {
                        SecondaryLockStatus::RolledBack
                    };
                    // We needn't write a rollback once there is a write record for it:
                    // If it's a committed record, it cannot be changed.
                    // If it's a rollback record, it either comes from another check_secondary_lock
                    // (thus protected) or the client stops commit actively. So we don't need
                    // to make it protected again.
                    (status, false, None)
                }
                TxnCommitRecord::OverlappedRollback { .. } => {
                    (SecondaryLockStatus::RolledBack, false, None)
                }
                TxnCommitRecord::None { overlapped_write } => {
                    (SecondaryLockStatus::RolledBack, true, overlapped_write)
                }
            },
        };
        if need_rollback {
            // We must protect this rollback in case this rollback is collapsed and a stale
            // acquire_pessimistic_lock and prewrite succeed again.
            if let Some(write) = make_rollback(self.start_ts, true, rollback_overlapped_write) {
                txn.put_write(key.clone(), self.start_ts, write.as_ref().to_bytes());
                if txn.collapse_rollback {
                    txn.collapse_prev_rollback(key.clone())?;
                }
            }
        }
        Ok((status, released_lock))
    }
}

impl<S: Snapshot, L: LockManager, P: PdClient + 'static> WriteCommand<S, L, P>
    for CheckSecondaryLocks
{
    fn process_write(
        mut self,
        snapshot: S,
        context: WriteContext<'_, L, P>,
    ) -> Result<WriteResult> {
        let mut txn = MvccTxn::new(
            snapshot,
            self.start_ts,
            !self.ctx.get_not_fill_cache(),
            context.pd_client,
        );
        let mut released_locks = ReleasedLocks::new(self.start_ts, TimeStamp::zero());
        let mut result = SecondaryLocksStatus::Locked(Vec::new());

        for key in mem::take(&mut self.keys) {
            let (status, released) = self.check_secondary_lock(&mut txn, &key)?;
            released_locks.push(released);
            match status {
                SecondaryLockStatus::Locked(lock) => {
                    result.push(lock.into_lock_info(key.to_raw()?));
                }
                SecondaryLockStatus::Committed(commit_ts) => {
                    result = SecondaryLocksStatus::Committed(commit_ts);
                    break;
                }
                SecondaryLockStatus::RolledBack => {
                    result = SecondaryLocksStatus::RolledBack;
                    break;
                }
            }
        }

        let mut rows = 0;
        if let SecondaryLocksStatus::RolledBack = &result {
            // Lock is only released when result is `RolledBack`.
            released_locks.wake_up(context.lock_mgr);
            // One row is mutated only when a secondary lock is rolled back.
            rows = 1;
        }
        context.statistics.add(&txn.take_statistics());
        let pr = ProcessResult::SecondaryLocksStatus { status: result };
        let write_data = WriteData::from_modifies(txn.into_modifies());
        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write: write_data,
            rows,
            pr,
            lock_info: None,
        })
    }
}
