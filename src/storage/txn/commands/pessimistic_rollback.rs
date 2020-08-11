// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::kv::WriteData;
use crate::storage::lock_manager::LockManager;
use crate::storage::mvcc::txn::make_txn_error;
use crate::storage::mvcc::{MvccTxn, ReleasedLock, Result as MvccResult};
use crate::storage::txn::commands::{
    Command, CommandExt, ReleasedLocks, TypedCommand, WriteCommand, WriteContext, WriteResult,
};
use crate::storage::txn::Result;
use crate::storage::{ProcessResult, Result as StorageResult, Snapshot};
use pd_client::PdClient;
use std::mem;
use txn_types::{Key, LockType, TimeStamp};

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

impl PessimisticRollback {
    /// Delete any pessimistic lock with small for_update_ts belongs to this transaction.
    pub fn pessimistic_rollback<S: Snapshot, P: PdClient + 'static>(
        &mut self,
        txn: &mut MvccTxn<S, P>,
        key: Key,
    ) -> MvccResult<Option<ReleasedLock>> {
        fail_point!("pessimistic_rollback", |err| Err(make_txn_error(
            err,
            &key,
            txn.start_ts,
        )
        .into()));

        if let Some(lock) = txn.reader.load_lock(&key)? {
            if lock.lock_type == LockType::Pessimistic
                && lock.ts == txn.start_ts
                && lock.for_update_ts <= self.for_update_ts
            {
                return Ok(txn.unlock_key(key, true));
            }
        }
        Ok(None)
    }
}

impl<S: Snapshot, L: LockManager, P: PdClient + 'static> WriteCommand<S, L, P>
    for PessimisticRollback
{
    fn process_write(
        mut self,
        snapshot: S,
        context: WriteContext<'_, L, P>,
    ) -> Result<WriteResult> {
        let ctx = mem::take(&mut self.ctx);
        let keys = mem::take(&mut self.keys);

        let mut txn = MvccTxn::new(
            snapshot,
            self.start_ts,
            !self.ctx.get_not_fill_cache(),
            context.pd_client,
        );

        let rows = keys.len();
        let mut released_locks = ReleasedLocks::new(self.start_ts, TimeStamp::zero());
        for k in keys {
            released_locks.push(self.pessimistic_rollback(&mut txn, k)?);
        }
        released_locks.wake_up(context.lock_mgr);

        context.statistics.add(&txn.take_statistics());
        let write_data = WriteData::from_modifies(txn.into_modifies());
        Ok(WriteResult {
            ctx,
            to_be_write: write_data,
            rows,
            pr: ProcessResult::MultiRes { results: vec![] },
            lock_info: None,
        })
    }
}
