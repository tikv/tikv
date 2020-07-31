// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use pd_client::PdClient;
use txn_types::{Key, TimeStamp};

use crate::storage::kv::WriteData;
use crate::storage::lock_manager::LockManager;
use crate::storage::mvcc::MvccTxn;
use crate::storage::txn::commands::{
    Command, CommandExt, ReleasedLocks, TypedCommand, WriteCommand, WriteContext, WriteResult,
};
use crate::storage::txn::Result;
use crate::storage::{ProcessResult, Snapshot};
use std::sync::Arc;

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

impl<S: Snapshot, L: LockManager, P: PdClient + 'static> WriteCommand<S, L, P> for Cleanup {
    fn process_write<'a>(
        self,
        snapshot: S,
        lock_mgr: &'a L,
        pd_client: Arc<P>,
        context: WriteContext<'a>,
    ) -> Result<WriteResult> {
        let mut txn = MvccTxn::new(
            snapshot,
            self.start_ts,
            !self.ctx.get_not_fill_cache(),
            pd_client,
        );

        let mut released_locks = ReleasedLocks::new(self.start_ts, TimeStamp::zero());
        // The rollback must be protected, see more on
        // [issue #7364](https://github.com/tikv/tikv/issues/7364)
        released_locks.push(txn.cleanup(self.key, self.current_ts, true)?);
        released_locks.wake_up(lock_mgr);

        context.statistics.add(&txn.take_statistics());
        let write_data = WriteData::from_modifies(txn.into_modifies());
        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write: write_data,
            rows: 1,
            pr: ProcessResult::Res,
            lock_info: None,
        })
    }
}
