// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use txn_types::Key;

use crate::storage::{
    kv::WriteData,
    lock_manager::LockManager,
    mvcc::{MvccTxn, SnapshotReader},
    txn::{
        commands::{
            Command, CommandExt, ReaderWithStats, ReleasedLocks, ResponsePolicy, TypedCommand,
            WriteCommand, WriteContext, WriteResult,
        },
        commit, Error, ErrorInner, Result,
    },
    ProcessResult, Snapshot, TxnStatus,
};

command! {
    /// Commit the transaction that started at `lock_ts`.
    ///
    /// This should be following a [`Prewrite`](Command::Prewrite).
    Commit:
        cmd_ty => TxnStatus,
        display => "kv::command::commit {:?} {} -> {} | {:?}", (keys, lock_ts, commit_ts, ctx),
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
    request_type!(KvCommit);
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
        let mut txn = MvccTxn::new(self.lock_ts, context.concurrency_manager);
        let mut reader = ReaderWithStats::new(
            SnapshotReader::new_with_ctx(self.lock_ts, snapshot, &self.ctx),
            context.statistics,
        );

        let rows = self.keys.len();
        // Pessimistic txn needs key_hashes to wake up waiters
        let mut released_locks = ReleasedLocks::new(self.lock_ts, self.commit_ts);
        for k in self.keys {
            released_locks.push(commit(
                &mut txn,
                &mut reader,
                k,
                self.commit_ts,
                context.enable_mark_cf,
            )?);
        }
        released_locks.wake_up(context.lock_mgr);

        let pr = ProcessResult::TxnStatus {
            txn_status: TxnStatus::committed(self.commit_ts),
        };
        let mut write_data = WriteData::from_modifies(txn.into_modifies());
        write_data.set_allowed_on_disk_almost_full();
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

#[cfg(test)]
mod tests {
    use tikv_kv::Statistics;
    use txn_types::MarkType;

    use super::*;
    use crate::storage::{
        mvcc::tests::{must_get_mark, must_get_no_mark},
        txn::{actions::tests::must_prewrite_lock, commands::test_util::commit},
        TestEngineBuilder,
    };

    // Test from command to check whether enable_mark_cf is correctly passed from
    // the commit command to the commit action.
    #[test]
    fn test_commit_lock() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let mut statistics = Statistics::default();
        let key = b"k";

        // enable_mark_cf = true
        must_prewrite_lock(&mut engine, key, key, 10);
        commit(
            &mut engine,
            &mut statistics,
            vec![Key::from_raw(key)],
            10,
            20,
            true,
        )
        .unwrap();
        must_get_mark(&mut engine, key, 10, 20, MarkType::Lock);

        // enable_mark_cf = false
        must_prewrite_lock(&mut engine, key, key, 30);
        commit(
            &mut engine,
            &mut statistics,
            vec![Key::from_raw(key)],
            30,
            40,
            false,
        )
        .unwrap();
        must_get_no_mark(&mut engine, key, 30);
    }
}
