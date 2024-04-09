// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use txn_types::{Key, TimeStamp};

use crate::storage::{
    kv::WriteData,
    lock_manager::LockManager,
    mvcc::{MvccTxn, SnapshotReader},
    txn::{
        cleanup,
        commands::{
            Command, CommandExt, ReaderWithStats, ReleasedLocks, ResponsePolicy, TypedCommand,
            WriteCommand, WriteContext, WriteResult,
        },
        Result,
    },
    ProcessResult, Snapshot,
};

command! {
    /// Rollback from the transaction that was started at `start_ts`.
    ///
    /// This should be following a [`Prewrite`](Command::Prewrite) on the given key.
    Rollback:
        cmd_ty => (),
        display => {
            "kv::command::rollback keys({:?}) @ {} | {:?}",
            (keys, start_ts, ctx),
        }
        content => {
            keys: Vec<Key>,
            /// The transaction timestamp.
            start_ts: TimeStamp,
        }
        in_heap => {
            keys,
        }
}

impl CommandExt for Rollback {
    ctx!();
    tag!(rollback);
    request_type!(KvRollback);
    ts!(start_ts);
    write_bytes!(keys: multiple);
    gen_lock!(keys: multiple);
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for Rollback {
    fn process_write(self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        let mut txn = MvccTxn::new(self.start_ts, context.concurrency_manager);
        let mut reader = ReaderWithStats::new(
            SnapshotReader::new_with_ctx(self.start_ts, snapshot, &self.ctx),
            context.statistics,
        );

        let rows = self.keys.len();
        let mut released_locks = ReleasedLocks::new();
        for k in self.keys {
            // Rollback is called only if the transaction is known to fail. Under the
            // circumstances, the rollback record needn't be protected.
            let released_lock = cleanup(&mut txn, &mut reader, k, TimeStamp::zero(), false)?;
            released_locks.push(released_lock);
        }

        let new_acquired_locks = txn.take_new_locks();
        let mut write_data = WriteData::from_modifies(txn.into_modifies());
        write_data.set_allowed_on_disk_almost_full();
        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write: write_data,
            rows,
            pr: ProcessResult::Res,
            lock_info: vec![],
            released_locks,
            new_acquired_locks,
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
            known_txn_status: vec![],
        })
    }
}

#[cfg(test)]
mod tests {
    use kvproto::kvrpcpb::PrewriteRequestPessimisticAction::*;

    use crate::storage::{txn::tests::*, TestEngineBuilder};

    #[test]
    fn rollback_lock_with_existing_rollback() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let (k1, k2) = (b"k1", b"k2");
        let v = b"v";

        must_acquire_pessimistic_lock(&mut engine, k1, k1, 10, 10);
        must_rollback(&mut engine, k1, 10, false);
        must_rollback(&mut engine, k2, 10, false);

        must_pessimistic_prewrite_put(&mut engine, k2, v, k1, 10, 10, SkipPessimisticCheck);
        must_rollback(&mut engine, k2, 10, false);
    }
}
