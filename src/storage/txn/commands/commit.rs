// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use txn_types::Key;

use crate::storage::{
    kv::WriteData,
    lock_manager::LockManager,
    mvcc::{MvccTxn, SnapshotReader},
    txn::{
        actions::commit::commit_with_primary,
        commands::{
            Command, CommandExt, ReaderWithStats, ReleasedLocks, ResponsePolicy, TypedCommand,
            WriteCommand, WriteContext, WriteResult,
        },
        commit,
        flight_recorder::TXN_FLIGHT_RECORDER,
        Error, ErrorInner, Result,
    },
    ProcessResult, Snapshot, TxnStatus,
};

command! {
    /// Commit the transaction that started at `lock_ts`.
    ///
    /// This should be following a [`Prewrite`](Command::Prewrite).
    Commit:
        cmd_ty => TxnStatus,
        display => { "kv::command::commit {:?} {} -> {} | {:?}", (keys, lock_ts, commit_ts, ctx), }
        content => {
            /// The keys affected.
            keys: Vec<Key>,
            /// The lock timestamp.
            lock_ts: txn_types::TimeStamp,
            /// The commit timestamp.
            commit_ts: txn_types::TimeStamp,
        }
        in_heap => {
            keys,
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
        let mut released_locks = ReleasedLocks::new();
        if TXN_FLIGHT_RECORDER.is_enabled() {
            for k in self.keys {
                let (released, primary_key_hash) =
                    commit_with_primary(&mut txn, &mut reader, k, self.commit_ts)?;
                released_locks.set_flight_primary_key_hash(primary_key_hash);
                released_locks.push(released);
            }
        } else {
            for k in self.keys {
                released_locks.push(commit(&mut txn, &mut reader, k, self.commit_ts)?);
            }
        }

        let pr = ProcessResult::TxnStatus {
            txn_status: TxnStatus::committed(self.commit_ts),
        };
        let new_acquired_locks = txn.take_new_locks();
        let mut write_data = WriteData::from_modifies(txn.into_modifies());
        write_data.set_allowed_on_disk_almost_full();
        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write: write_data,
            rows,
            pr,
            lock_info: vec![],
            released_locks,
            new_acquired_locks,
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
            known_txn_status: vec![(self.lock_ts, self.commit_ts)],
        })
    }
}
