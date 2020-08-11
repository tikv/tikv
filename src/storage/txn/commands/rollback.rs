// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::kv::WriteData;
use crate::storage::lock_manager::LockManager;
use crate::storage::mvcc::txn::make_txn_error;
use crate::storage::mvcc::{MvccTxn, ReleasedLock, Result as MvccResult};
use crate::storage::txn::commands::cleanup::Cleanup;
use crate::storage::txn::commands::{
    Command, CommandExt, ReleasedLocks, TypedCommand, WriteCommand, WriteContext, WriteResult,
};
use crate::storage::txn::Result;
use crate::storage::{ProcessResult, Snapshot};
use pd_client::PdClient;
use std::mem;
use txn_types::{Key, TimeStamp};

command! {
    /// Rollback from the transaction that was started at `start_ts`.
    ///
    /// This should be following a [`Prewrite`](Command::Prewrite) on the given key.
    Rollback:
        cmd_ty => (),
        display => "kv::command::rollback keys({}) @ {} | {:?}", (keys.len, start_ts, ctx),
        content => {
            keys: Vec<Key>,
            /// The transaction timestamp.
            start_ts: TimeStamp,
        }
}

impl CommandExt for Rollback {
    ctx!();
    tag!(rollback);
    ts!(start_ts);
    write_bytes!(keys: multiple);
    gen_lock!(keys: multiple);
}

impl Rollback {
    pub fn rollback<S: Snapshot, P: PdClient + 'static>(
        &mut self,
        txn: &mut MvccTxn<S, P>,
        key: Key,
    ) -> MvccResult<Option<ReleasedLock>> {
        fail_point!("rollback", |err| Err(make_txn_error(
            err,
            &key,
            self.start_ts,
        )
        .into()));
        let cleanup_command = Cleanup {
            key,
            start_ts: self.start_ts,
            current_ts: TimeStamp::zero(),
            ctx: self.ctx.clone(),
        };

        // Rollback is called only if the transaction is known to fail. Under the circumstances,
        // the rollback record needn't be protected.
        cleanup_command.cleanup(txn, false)
    }
}

impl<S: Snapshot, L: LockManager, P: PdClient + 'static> WriteCommand<S, L, P> for Rollback {
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

        let rows = self.keys.len();
        let mut released_locks = ReleasedLocks::new(self.start_ts, TimeStamp::zero());
        let keys = mem::take(&mut self.keys);
        for k in keys {
            released_locks.push(self.rollback(&mut txn, k)?);
        }
        released_locks.wake_up(context.lock_mgr);

        context.statistics.add(&txn.take_statistics());
        let write_data = WriteData::from_modifies(txn.into_modifies());
        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write: write_data,
            rows,
            pr: ProcessResult::Res,
            lock_info: None,
        })
    }
}
