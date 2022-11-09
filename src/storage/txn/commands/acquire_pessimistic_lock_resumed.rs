// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use kvproto::kvrpcpb::ExtraOp;
use txn_types::{Key, OldValues, TxnExtra};

use crate::storage::{
    kv::WriteData,
    lock_manager::{lock_waiting_queue::LockWaitEntry, LockManager, LockWaitToken},
    mvcc::{Error as MvccError, ErrorInner as MvccErrorInner, MvccTxn, SnapshotReader},
    txn::{
        acquire_pessimistic_lock,
        commands::{
            Command, CommandExt, ReleasedLocks, ResponsePolicy, TypedCommand, WriteCommand,
            WriteContext, WriteResult, WriteResultLockInfo,
        },
        Error, Result,
    },
    types::{PessimisticLockParameters, PessimisticLockResults},
    Error as StorageError, PessimisticLockKeyResult, ProcessResult, Result as StorageResult,
    Snapshot,
};

#[derive(Debug)]
pub struct ResumedPessimisticLockItem {
    pub key: Key,
    pub should_not_exist: bool,
    pub params: PessimisticLockParameters,
    pub lock_wait_token: LockWaitToken,
}

command! {
    /// Acquire a Pessimistic lock on the keys.
    ///
    /// This can be rolled back with a [`PessimisticRollback`](Command::PessimisticRollback) command.
    AcquirePessimisticLockResumed:
        cmd_ty => StorageResult<PessimisticLockResults>,
        display => "kv::command::acquirepessimisticlockresumed {:?}",
        (items),
        content => {
            items: Vec<ResumedPessimisticLockItem>,
        }
}

impl CommandExt for AcquirePessimisticLockResumed {
    ctx!();
    tag!(acquire_pessimistic_lock_resumed);
    request_type!(KvPessimisticLock);

    property!(can_be_pipelined);

    fn write_bytes(&self) -> usize {
        self.items
            .iter()
            .map(|item| item.key.as_encoded().len())
            .sum()
    }

    gen_lock!(items: multiple(|x| &x.key));
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for AcquirePessimisticLockResumed {
    fn process_write(self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        let mut modifies = vec![];
        let mut txn = None;
        let mut reader: Option<SnapshotReader<S>> = None;

        let mut written_rows = 0;
        let mut res = PessimisticLockResults::with_capacity(self.items.len());
        let mut encountered_locks = vec![];
        let need_old_value = context.extra_op == ExtraOp::ReadOldValue;
        let mut old_values = OldValues::default();

        let mut new_locked_keys = Vec::with_capacity(self.items.len());

        for item in self.items.into_iter() {
            let ResumedPessimisticLockItem {
                key,
                should_not_exist,
                params,
                lock_wait_token,
            } = item;

            // TODO: Refine the code for rebuilding txn state.
            if txn
                .as_ref()
                .map_or(true, |t: &MvccTxn| t.start_ts != params.start_ts)
            {
                if let Some(txn) = txn {
                    if !txn.is_empty() {
                        modifies.extend(txn.into_modifies());
                    }
                }
                txn = Some(MvccTxn::new(
                    params.start_ts,
                    context.concurrency_manager.clone(),
                ));
                // TODO: Is it possible to reuse the same reader but change the start_ts stored
                // in it?
                if let Some(mut reader) = reader {
                    context.statistics.add(&reader.take_statistics());
                }
                reader = Some(SnapshotReader::new_with_ctx(
                    params.start_ts,
                    snapshot.clone(),
                    &self.ctx,
                ));
            }
            let txn = txn.as_mut().unwrap();
            let reader = reader.as_mut().unwrap();

            match acquire_pessimistic_lock(
                txn,
                reader,
                key.clone(),
                &params.primary,
                should_not_exist,
                params.lock_ttl,
                params.for_update_ts,
                params.return_values,
                params.check_existence,
                params.min_commit_ts,
                need_old_value,
                params.lock_only_if_exists,
                true,
            ) {
                Ok((key_res, old_value)) => {
                    res.push(key_res);
                    new_locked_keys.push((params.start_ts, key.clone()));
                    if old_value.resolved() {
                        let key = key.append_ts(txn.start_ts);
                        // MutationType is unknown in AcquirePessimisticLock stage.
                        let mutation_type = None;
                        old_values.insert(key, (old_value, mutation_type));
                    }
                    written_rows += 1;
                }
                Err(MvccError(box MvccErrorInner::KeyIsLocked(lock_info))) => {
                    let mut lock_info =
                        WriteResultLockInfo::new(lock_info, params, key, should_not_exist);
                    lock_info.lock_wait_token = lock_wait_token;
                    res.push(PessimisticLockKeyResult::Waiting);
                    encountered_locks.push(lock_info);
                }
                Err(e) => {
                    res.push(PessimisticLockKeyResult::Failed(
                        StorageError::from(Error::from(e)).into(),
                    ));
                }
            };
        }

        if let Some(txn) = txn {
            if !txn.is_empty() {
                modifies.extend(txn.into_modifies());
            }
        }
        if let Some(mut reader) = reader {
            context.statistics.add(&reader.take_statistics());
        }

        let pr = ProcessResult::PessimisticLockRes { res: Ok(res) };
        let to_be_write = if written_rows > 0 {
            let extra = TxnExtra {
                old_values,
                // One pc status is unknown in AcquirePessimisticLock stage.
                one_pc: false,
                for_flashback: false,
            };
            WriteData::new(modifies, extra)
        } else {
            WriteData::default()
        };

        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write,
            rows: written_rows,
            pr,
            lock_info: encountered_locks,
            released_locks: ReleasedLocks::new(),
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnProposed,
        })
    }
}

impl AcquirePessimisticLockResumed {
    pub fn from_lock_wait_entries(
        lock_wait_entries: impl IntoIterator<Item = Box<LockWaitEntry>>,
    ) -> TypedCommand<StorageResult<PessimisticLockResults>> {
        let items: Vec<_> = lock_wait_entries
            .into_iter()
            .map(|item| {
                assert!(item.key_cb.is_none());
                ResumedPessimisticLockItem {
                    key: item.key,
                    should_not_exist: item.should_not_exist,
                    params: item.parameters,
                    lock_wait_token: item.lock_wait_token,
                }
            })
            .collect();

        assert!(!items.is_empty());
        let ctx = items[0].params.pb_ctx.clone();
        // TODO: May it cause problem by using the first one as the pb_ctx of the
        // Command?
        Self::new(items, ctx)
    }
}
