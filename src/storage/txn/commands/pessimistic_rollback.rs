// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::mem;

use txn_types::{Key, LockType, TimeStamp};

use crate::storage::{
    kv::WriteData,
    lock_manager::LockManager,
    mvcc::{MvccTxn, Result as MvccResult, SnapshotReader},
    txn::{
        commands::{
            Command, CommandExt, ReaderWithStats, ReleasedLocks, ResponsePolicy, TypedCommand,
            WriteCommand, WriteContext, WriteResult,
        },
        Result,
    },
    ProcessResult, Result as StorageResult, Snapshot,
};

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
    /// Delete any pessimistic lock with small for_update_ts belongs to this transaction.
    fn process_write(mut self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        let mut txn = MvccTxn::new(self.start_ts, context.concurrency_manager);
        let mut reader = ReaderWithStats::new(
            SnapshotReader::new_with_ctx(self.start_ts, snapshot, &self.ctx),
            context.statistics,
        );

        let ctx = mem::take(&mut self.ctx);
        let keys = mem::take(&mut self.keys);

        let rows = keys.len();
        let mut released_locks = ReleasedLocks::new(self.start_ts, TimeStamp::zero());
        for key in keys {
            fail_point!("pessimistic_rollback", |err| Err(
                crate::storage::mvcc::Error::from(crate::storage::mvcc::txn::make_txn_error(
                    err,
                    &key,
                    self.start_ts
                ))
                .into()
            ));
            let released_lock: MvccResult<_> = if let Some(lock) = reader.load_lock(&key)? {
                if lock.lock_type == LockType::Pessimistic
                    && lock.ts == self.start_ts
                    && lock.for_update_ts <= self.for_update_ts
                {
                    Ok(txn.unlock_key(key, true))
                } else {
                    Ok(None)
                }
            } else {
                Ok(None)
            };
            released_locks.push(released_lock?);
        }
        released_locks.wake_up(context.lock_mgr);

        let mut write_data = WriteData::from_modifies(txn.into_modifies());
        write_data.set_allowed_on_disk_almost_full();
        Ok(WriteResult {
            ctx,
            to_be_write: write_data,
            rows,
            pr: ProcessResult::MultiRes { results: vec![] },
            lock_info: None,
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
        })
    }
}

#[cfg(test)]
pub mod tests {
    use concurrency_manager::ConcurrencyManager;
    use kvproto::kvrpcpb::Context;
    use tikv_util::deadline::Deadline;
    use txn_types::Key;

    use super::*;
    use crate::storage::{
        kv::Engine,
        lock_manager::DummyLockManager,
        mvcc::tests::*,
        txn::{
            commands::{WriteCommand, WriteContext},
            scheduler::DEFAULT_EXECUTION_DURATION_LIMIT,
            tests::*,
        },
        TestEngineBuilder,
    };

    pub fn must_success<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        for_update_ts: impl Into<TimeStamp>,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let for_update_ts = for_update_ts.into();
        let cm = ConcurrencyManager::new(for_update_ts);
        let start_ts = start_ts.into();
        let command = crate::storage::txn::commands::PessimisticRollback {
            ctx: ctx.clone(),
            keys: vec![Key::from_raw(key)],
            start_ts,
            for_update_ts,
            deadline: Deadline::from_now(DEFAULT_EXECUTION_DURATION_LIMIT),
        };
        let lock_mgr = DummyLockManager;
        let write_context = WriteContext {
            lock_mgr: &lock_mgr,
            concurrency_manager: cm,
            extra_op: Default::default(),
            statistics: &mut Default::default(),
            async_apply_prewrite: false,
        };
        let result = command.process_write(snapshot, write_context).unwrap();
        write(engine, &ctx, result.to_be_write.modifies);
    }

    #[test]
    fn test_pessimistic_rollback() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let k = b"k1";
        let v = b"v1";

        // Normal
        must_acquire_pessimistic_lock(&engine, k, k, 1, 1);
        must_pessimistic_locked(&engine, k, 1, 1);
        must_success(&engine, k, 1, 1);
        must_unlocked(&engine, k);
        must_get_commit_ts_none(&engine, k, 1);
        // Pessimistic rollback is idempotent
        must_success(&engine, k, 1, 1);
        must_unlocked(&engine, k);
        must_get_commit_ts_none(&engine, k, 1);

        // Succeed if the lock doesn't exist.
        must_success(&engine, k, 2, 2);

        // Do nothing if meets other transaction's pessimistic lock
        must_acquire_pessimistic_lock(&engine, k, k, 2, 3);
        must_success(&engine, k, 1, 1);
        must_success(&engine, k, 1, 2);
        must_success(&engine, k, 1, 3);
        must_success(&engine, k, 1, 4);
        must_success(&engine, k, 3, 3);
        must_success(&engine, k, 4, 4);

        // Succeed if for_update_ts is larger; do nothing if for_update_ts is smaller.
        must_pessimistic_locked(&engine, k, 2, 3);
        must_success(&engine, k, 2, 2);
        must_pessimistic_locked(&engine, k, 2, 3);
        must_success(&engine, k, 2, 4);
        must_unlocked(&engine, k);

        // Do nothing if rollbacks a non-pessimistic lock.
        must_prewrite_put(&engine, k, v, k, 3);
        must_locked(&engine, k, 3);
        must_success(&engine, k, 3, 3);
        must_locked(&engine, k, 3);

        // Do nothing if meets other transaction's optimistic lock
        must_success(&engine, k, 2, 2);
        must_success(&engine, k, 2, 3);
        must_success(&engine, k, 2, 4);
        must_success(&engine, k, 4, 4);
        must_locked(&engine, k, 3);

        // Do nothing if committed
        must_commit(&engine, k, 3, 4);
        must_unlocked(&engine, k);
        must_get_commit_ts(&engine, k, 3, 4);
        must_success(&engine, k, 3, 3);
        must_success(&engine, k, 3, 4);
        must_success(&engine, k, 3, 5);
    }
}
