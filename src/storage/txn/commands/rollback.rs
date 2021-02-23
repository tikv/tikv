// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::kv::WriteData;
use crate::storage::lock_manager::LockManager;
use crate::storage::mvcc::MvccTxn;
use crate::storage::txn::commands::{
    Command, CommandExt, ReleasedLocks, ResponsePolicy, TypedCommand, WriteCommand, WriteContext,
    WriteResult,
};
use crate::storage::txn::{cleanup, Result};
use crate::storage::{ProcessResult, Snapshot};
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

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for Rollback {
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
            // Rollback is called only if the transaction is known to fail. Under the circumstances,
            // the rollback record needn't be protected.
            let released_lock = cleanup(&mut txn, k, TimeStamp::zero(), false)?;
            released_locks.push(released_lock);
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
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::txn::tests::*;
    use crate::storage::TestEngineBuilder;

    #[test]
    fn rollback_lock_with_existing_rollback() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let (k1, k2) = (b"k1", b"k2");
        let v = b"v";

        must_acquire_pessimistic_lock(&engine, k1, k1, 10, 10);
        must_rollback(&engine, k1, 10);
        must_rollback(&engine, k2, 10);

        must_pessimistic_prewrite_put(&engine, k2, v, k1, 10, 10, false);
        must_rollback(&engine, k2, 10);
    }
}
