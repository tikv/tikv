// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use txn_types::{Key, TimeStamp};

use crate::storage::kv::WriteData;
use crate::storage::lock_manager::LockManager;
use crate::storage::mvcc::{MvccTxn, SnapshotReader};
use crate::storage::txn::commands::{
    Command, CommandExt, ReleasedLocks, ResponsePolicy, TypedCommand, WriteCommand, WriteContext,
    WriteResult,
};
use crate::storage::txn::{cleanup, Result};
use crate::storage::{ProcessResult, Snapshot};

command! {
    /// Rollback mutations on a single key.
    ///
    /// This should be following a [`Prewrite`](Command::Prewrite) on the given key.
    Cleanup:
        cmd_ty => (),
        display => "kv::command::cleanup {} @ {} | {:?}", (key, start_ts, ctx),
        content => {
            key: Key,
            /// The transaction timestamp.
            start_ts: TimeStamp,
            /// The approximate current ts when cleanup request is invoked, which is used to check the
            /// lock's TTL. 0 means do not check TTL.
            current_ts: TimeStamp,
        }
}

impl CommandExt for Cleanup {
    ctx!();
    tag!(cleanup);
    ts!(start_ts);
    write_bytes!(key);
    gen_lock!(key);
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for Cleanup {
    fn process_write(self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        // It is not allowed for commit to overwrite a protected rollback. So we update max_ts
        // to prevent this case from happening.
        context.concurrency_manager.update_max_ts(self.start_ts);

        let mut txn = MvccTxn::new(self.start_ts, context.concurrency_manager);
        let mut reader =
            SnapshotReader::new(self.start_ts, snapshot, !self.ctx.get_not_fill_cache());

        let mut released_locks = ReleasedLocks::new(self.start_ts, TimeStamp::zero());
        // The rollback must be protected, see more on
        // [issue #7364](https://github.com/tikv/tikv/issues/7364)
        released_locks.push(cleanup(
            &mut txn,
            &mut reader,
            self.key,
            self.current_ts,
            true,
        )?);
        released_locks.wake_up(context.lock_mgr);

        context.statistics.add(&reader.take_statistics());
        let write_data = WriteData::from_modifies(txn.into_modifies());
        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write: write_data,
            rows: 1,
            pr: ProcessResult::Res,
            lock_info: None,
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
        })
    }
}
