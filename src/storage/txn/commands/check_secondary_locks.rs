// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::kv::WriteData;
use crate::storage::lock_manager::LockManager;
use crate::storage::mvcc::{MvccTxn, SecondaryLockStatus, TimeStamp};
use crate::storage::txn::commands::{
    Command, CommandExt, ReleasedLocks, TypedCommand, WriteCommand, WriteContext, WriteResult,
};
use crate::storage::txn::Result;
use crate::storage::types::SecondaryLocksStatus;
use crate::storage::{ProcessResult, Snapshot};
use txn_types::Key;

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

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for CheckSecondaryLocks {
    fn process_write(self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        let mut txn = MvccTxn::new(
            snapshot,
            self.start_ts,
            !self.ctx.get_not_fill_cache(),
            context.concurrency_manager,
        );
        let mut released_locks = ReleasedLocks::new(self.start_ts, TimeStamp::zero());
        let mut result = SecondaryLocksStatus::Locked(Vec::new());

        for key in self.keys {
            let (status, released) = txn.check_secondary_lock(&key, self.start_ts)?;
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
            lock_guards: vec![],
        })
    }
}
