// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use txn_types::Key;

use crate::storage::kv::WriteData;
use crate::storage::lock_manager::LockManager;
use crate::storage::mvcc::MvccTxn;
use crate::storage::txn::commands::{
    Command, CommandExt, ReleasedLocks, ResponsePolicy, TypedCommand, WriteCommand, WriteContext,
    WriteResult,
};
use crate::storage::txn::{commit, Error, ErrorInner, Result};
use crate::storage::{ProcessResult, Snapshot, TxnStatus};

command! {
    /// Commit the transaction that started at `lock_ts`.
    ///
    /// This should be following a [`Prewrite`](Command::Prewrite).
    Commit:
        cmd_ty => TxnStatus,
        display => "kv::command::commit {} {} -> {} | {:?}", (keys.len, lock_ts, commit_ts, ctx),
        content => {
            /// The keys affected.
            keys: Vec<Key>,
            /// The lock timestamp.
            lock_ts: txn_types::TimeStamp,
            /// The commit timestamp.
            commit_ts: txn_types::TimeStamp,
        }
}

impl CommandExt for Commit {
    ctx!();
    tag!(commit);
    ts!(commit_ts);
    write_bytes!(keys: multiple);
    gen_lock!(keys: multiple);
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for Commit {
    fn process_write(self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        if self.commit_ts <= self.lock_ts {
            return Err(Error::from(ErrorInner::InvalidTxnTso {
                start_ts: self.lock_ts,
                commit_ts: self.commit_ts,
            }));
        }
        let mut txn = MvccTxn::new(
            snapshot,
            self.lock_ts,
            !self.ctx.get_not_fill_cache(),
            context.concurrency_manager,
        );

        let rows = self.keys.len();
        // Pessimistic txn needs key_hashes to wake up waiters
        let mut released_locks = ReleasedLocks::new(self.lock_ts, self.commit_ts);
        for k in self.keys {
            released_locks.push(commit(&mut txn, k, self.commit_ts)?);
        }
        released_locks.wake_up(context.lock_mgr);

        context.statistics.add(&txn.take_statistics());
        let pr = ProcessResult::TxnStatus {
            txn_status: TxnStatus::committed(self.commit_ts),
        };
        let write_data = WriteData::from_modifies(txn.into_modifies());
        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write: write_data,
            rows,
            pr,
            lock_info: None,
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
        })
    }
}
