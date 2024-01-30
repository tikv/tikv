// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::mem;

use kvproto::kvrpcpb::{AssertionLevel, ExtraOp, PrewriteRequestPessimisticAction};
// #[PerformanceCriticalPath]
use txn_types::{insert_old_value_if_resolved, Mutation, OldValues, TimeStamp, TxnExtra};

use crate::storage::{
    kv::WriteData,
    lock_manager::LockManager,
    mvcc::{MvccTxn, SnapshotReader},
    txn::{
        actions::common::check_committed_record_on_err,
        commands::{
            CommandExt, ReaderWithStats, ReleasedLocks, ResponsePolicy, WriteCommand, WriteContext,
            WriteResult,
        },
        prewrite, CommitKind, Error, Result, TransactionKind, TransactionProperties,
    },
    Command, ProcessResult, Result as StorageResult, Snapshot, TypedCommand,
};

command! {
    Flush:
        cmd_ty => Vec<StorageResult<()>>,
        display => "kv::command::flush keys({:?}) @ {} | {:?}", (mutations, start_ts, ctx),
        content => {
            start_ts: TimeStamp,
            primary: Vec<u8>,
            mutations: Vec<Mutation>,
            lock_ttl: u64,
            assertion_level: AssertionLevel,
        }
}

impl CommandExt for Flush {
    ctx!();
    tag!(flush);
    request_type!(KvFlush);
    ts!(start_ts);

    fn write_bytes(&self) -> usize {
        let mut bytes = 0;
        for m in &self.mutations {
            match *m {
                Mutation::Put((ref key, ref value), _)
                | Mutation::Insert((ref key, ref value), _) => {
                    bytes += key.as_encoded().len();
                    bytes += value.len();
                }
                Mutation::Delete(ref key, _) | Mutation::Lock(ref key, _) => {
                    bytes += key.as_encoded().len();
                }
                Mutation::CheckNotExists(..) => (),
            }
        }
        bytes
    }

    gen_lock!(mutations: multiple(|x| x.key()));
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for Flush {
    fn process_write(mut self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        let rows = self.mutations.len();
        let mut txn = MvccTxn::new(self.start_ts, context.concurrency_manager);
        let mut reader = ReaderWithStats::new(
            SnapshotReader::new_with_ctx(self.start_ts, snapshot, &self.ctx),
            context.statistics,
        );
        let mut old_values = Default::default();

        let res = self.flush(&mut txn, &mut reader, &mut old_values, context.extra_op);
        let locks = res?;
        let extra = TxnExtra {
            old_values,
            one_pc: false,
            allowed_in_flashback: false,
        };
        let new_locks = txn.take_new_locks();
        let guards = txn.take_guards();
        assert!(guards.is_empty());
        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write: WriteData::new(txn.into_modifies(), extra),
            rows,
            pr: ProcessResult::MultiRes { results: locks },
            lock_info: vec![],
            released_locks: ReleasedLocks::new(),
            new_acquired_locks: new_locks,
            lock_guards: guards,
            response_policy: ResponsePolicy::OnApplied,
            known_txn_status: vec![],
        })
    }
}

impl Flush {
    fn flush(
        &mut self,
        txn: &mut MvccTxn,
        reader: &mut SnapshotReader<impl Snapshot>,
        old_values: &mut OldValues,
        extra_op: ExtraOp,
    ) -> Result<Vec<std::result::Result<(), crate::storage::errors::Error>>> {
        let props = TransactionProperties {
            start_ts: self.start_ts,
            kind: TransactionKind::Optimistic(false),
            commit_kind: CommitKind::TwoPc,
            primary: &self.primary,
            txn_size: 0, // txn_size is unknown
            lock_ttl: self.lock_ttl,
            min_commit_ts: TimeStamp::zero(),
            need_old_value: extra_op == ExtraOp::ReadOldValue, // FIXME?
            is_retry_request: self.ctx.is_retry_request,
            assertion_level: self.assertion_level,
            txn_source: self.ctx.get_txn_source(),
        };
        let mut locks = Vec::new();
        // If there are other errors, return other error prior to `AssertionFailed`.
        let mut assertion_failure = None;

        for m in mem::take(&mut self.mutations) {
            let key = m.key().clone();
            let mutation_type = m.mutation_type();
            let prewrite_result = prewrite(
                txn,
                reader,
                &props,
                m,
                &None,
                PrewriteRequestPessimisticAction::SkipPessimisticCheck,
                None,
            );
            match prewrite_result {
                Ok((_ts, old_value)) => {
                    insert_old_value_if_resolved(
                        old_values,
                        key,
                        txn.start_ts,
                        old_value,
                        Some(mutation_type),
                    );
                }
                Err(crate::storage::mvcc::Error(
                    box crate::storage::mvcc::ErrorInner::WriteConflict {
                        start_ts,
                        conflict_commit_ts,
                        ..
                    },
                )) if conflict_commit_ts > start_ts => {
                    return check_committed_record_on_err(prewrite_result, txn, reader, &key)
                        .map(|(locks, _)| locks);
                }
                Err(crate::storage::mvcc::Error(
                    box crate::storage::mvcc::ErrorInner::PessimisticLockNotFound { .. },
                ))
                | Err(crate::storage::mvcc::Error(
                    box crate::storage::mvcc::ErrorInner::CommitTsTooLarge { .. },
                )) => {
                    unreachable!();
                }
                Err(crate::storage::mvcc::Error(
                    box crate::storage::mvcc::ErrorInner::KeyIsLocked { .. },
                )) => match check_committed_record_on_err(prewrite_result, txn, reader, &key) {
                    Ok(res) => return Ok(res.0),
                    Err(e) => locks.push(Err(e.into())),
                },
                Err(
                    e @ crate::storage::mvcc::Error(
                        box crate::storage::mvcc::ErrorInner::AssertionFailed { .. },
                    ),
                ) => {
                    if assertion_failure.is_none() {
                        assertion_failure = Some(e);
                    }
                }
                Err(e) => return Err(Error::from(e)),
            }
        }
        if let Some(e) = assertion_failure {
            return Err(Error::from(e));
        }
        Ok(locks)
    }
}
