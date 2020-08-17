// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::kv::WriteData;
use crate::storage::lock_manager::LockManager;
use crate::storage::mvcc::MvccTxn;
use crate::storage::txn::commands::{
    Command, CommandExt, ReleasedLocks, TypedCommand, WriteCommand, WriteContext, WriteResult,
};
use crate::storage::txn::Result;
use crate::storage::{ProcessResult, Result as StorageResult, Snapshot};
use txn_types::{Key, TimeStamp};

command! {
    /// Rollback pessimistic locks identified by `start_ts` and `for_update_ts`.
    ///
    /// This can roll back an [`AcquirePessimisticLock`](Command::AcquirePessimisticLock) command.
    PessimisticRollback:
        cmd_ty => Vec<StorageResult<()>>,
        display => "kv::command::pessimistic_rollback keys({}) @ {} {} | {:?}", (keys.len, start_ts, for_update_ts, ctx),
        content => {
            /// The keys to be rolled back.
            keys: Vec<Key>,
            /// The transaction timestamp.
            start_ts: TimeStamp,
            for_update_ts: TimeStamp,
        }
}

impl CommandExt for PessimisticRollback {
    ctx!();
    tag!(pessimistic_rollback);
    ts!(start_ts);
    write_bytes!(keys: multiple);
    gen_lock!(keys: multiple);
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for PessimisticRollback {
    fn process_write(self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        let mut txn = MvccTxn::new(
            snapshot,
            self.start_ts,
            !self.ctx.get_not_fill_cache(),
            context.concurrency_manager,
        );

        let rows = self.keys.len();
        let mut released_locks = ReleasedLocks::new(self.start_ts, TimeStamp::zero());
        for k in self.keys {
            released_locks.push(txn.pessimistic_rollback(k, self.for_update_ts)?);
        }
        released_locks.wake_up(context.lock_mgr);

        context.statistics.add(&txn.take_statistics());
        let write_data = WriteData::from_modifies(txn.into_modifies());
        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write: write_data,
            rows,
            pr: ProcessResult::MultiRes { results: vec![] },
            lock_info: None,
            lock_guards: vec![],
        })
    }
}
