// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use kvproto::kvrpcpb::{Context, ExtraOp};
use txn_types::{Key, OldValues, TimeStamp, TxnExtra};

use crate::storage::kv::WriteData;
use crate::storage::lock_manager::{LockManager, WaitTimeout};
use crate::storage::mvcc::{
    Error as MvccError, ErrorInner as MvccErrorInner, MvccTxn, SnapshotReader,
};
use crate::storage::txn::commands::{
    Command, CommandExt, PessimisticLockKeyCallback, PessimisticLockParameters, ReaderWithStats,
    ResponsePolicy, TypedCommand, WriteCommand, WriteContext, WriteResult, WriteResultLockInfo,
};
use crate::storage::txn::scheduler::PartialPessimisticLockRequestSharedState;
use crate::storage::txn::{acquire_pessimistic_lock, Error, Result};
use crate::storage::types::PessimisticLockKeyResult;
use crate::storage::{PessimisticLockResults, ProcessResult, Result as StorageResult, Snapshot};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::fmt::Formatter;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tikv_kv::SnapshotExt;

pub struct PessimisticLockKeyContext {
    pub index_in_request: usize,
    pub lock_hash: u64,
    pub hash_for_latch: u64,
}

pub struct ResumedPessimisticLockItem {
    pub key: Key,
    pub should_not_exist: bool,
    pub params: PessimisticLockParameters,
    pub lock_key_ctx: PessimisticLockKeyContext,
    pub awakened_with_primary_index: Option<usize>,
    pub req_states: Arc<PartialPessimisticLockRequestSharedState>,
}

#[allow(clippy::large_enum_variant)]
pub enum PessimisticLockCmdInner {
    SingleRequest {
        params: PessimisticLockParameters,
        keys: Vec<(Key, bool)>,
        allow_lock_with_conflict: bool,
    },
    BatchResumedRequests {
        items: Vec<ResumedPessimisticLockItem>,
    },
}

impl std::fmt::Display for PessimisticLockCmdInner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PessimisticLockCmdInner::SingleRequest {
                params,
                keys,
                allow_lock_with_conflict,
            } => {
                write!(
                    f,
                    "keys({}) @ {} {} | {:?}{}",
                    keys.len(),
                    params.start_ts,
                    params.for_update_ts,
                    params.pb_ctx,
                    if !allow_lock_with_conflict {
                        " (legacy mode)"
                    } else {
                        ""
                    }
                )
            }
            PessimisticLockCmdInner::BatchResumedRequests { items } => {
                write!(f, "batch resumed {} keys", items.len(),)
            }
        }
    }
}

command! {
    /// Acquire a Pessimistic lock on the keys.
    ///
    /// This can be rolled back with a [`PessimisticRollback`](Command::PessimisticRollback) command.
    AcquirePessimisticLock:
        cmd_ty => StorageResult<PessimisticLockResults>,
        display => "kv::command::acquirepessimisticlock {}", (inner),
        content => {
            inner: PessimisticLockCmdInner,
            is_first_lock: bool,
        }
}

impl PessimisticLockKeyContext {
    fn from_key(index: usize, key: &Key) -> Self {
        let hash_for_lock = key.gen_hash();
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash_for_latch = hasher.finish();
        Self {
            index_in_request: index,
            lock_hash: hash_for_lock,
            hash_for_latch,
        }
    }
}

impl CommandExt for AcquirePessimisticLock {
    ctx!();

    fn tag(&self) -> crate::storage::metrics::CommandKind {
        match self.inner {
            PessimisticLockCmdInner::SingleRequest { .. } => {
                crate::storage::metrics::CommandKind::acquire_pessimistic_lock
            }
            PessimisticLockCmdInner::BatchResumedRequests { .. } => {
                crate::storage::metrics::CommandKind::acquire_pessimistic_lock_resumed
            }
        }
    }

    fn incr_cmd_metric(&self) {
        match self.inner {
            PessimisticLockCmdInner::SingleRequest { .. } => {
                crate::storage::metrics::KV_COMMAND_COUNTER_VEC_STATIC
                    .acquire_pessimistic_lock
                    .inc();
            }
            PessimisticLockCmdInner::BatchResumedRequests { .. } => {
                crate::storage::metrics::KV_COMMAND_COUNTER_VEC_STATIC
                    .acquire_pessimistic_lock_resumed
                    .inc();
            }
        }
    }

    property!(can_be_pipelined);

    fn ts(&self) -> TimeStamp {
        match &self.inner {
            PessimisticLockCmdInner::SingleRequest { params, .. } => params.start_ts,
            PessimisticLockCmdInner::BatchResumedRequests { .. } => TimeStamp::zero(),
        }
    }

    fn write_bytes(&self) -> usize {
        match &self.inner {
            PessimisticLockCmdInner::SingleRequest { keys, .. } => {
                keys.iter().map(|(key, _)| key.as_encoded().len()).sum()
            }
            PessimisticLockCmdInner::BatchResumedRequests { items, .. } => {
                items.iter().map(|item| item.key.as_encoded().len()).sum()
            }
        }
    }

    gen_lock!(inner: enum_match {
        PessimisticLockCmdInner::SingleRequest { keys, .. } => keys.iter().map(|k| &k.0),
        PessimisticLockCmdInner::BatchResumedRequests{items, ..} => items.iter().map(|item| &item.key)
    });
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for AcquirePessimisticLock {
    fn process_write(self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        match self.inner {
            PessimisticLockCmdInner::SingleRequest {
                params,
                keys,
                allow_lock_with_conflict,
            } => Self::process_write_for_single_request(
                snapshot,
                context,
                self.ctx,
                params,
                keys,
                allow_lock_with_conflict,
            ),
            PessimisticLockCmdInner::BatchResumedRequests { items, .. } => {
                Self::process_write_for_resumed(snapshot, context, self.ctx, items)
            }
        }
    }
}

impl AcquirePessimisticLock {
    fn process_write_for_single_request<S, L>(
        snapshot: S,
        context: WriteContext<'_, L>,
        pb_ctx: Context,
        params: PessimisticLockParameters,
        keys: Vec<(Key, bool)>,
        allow_lock_with_conflict: bool,
    ) -> Result<WriteResult>
    where
        S: Snapshot,
        L: LockManager,
    {
        let term = snapshot.ext().get_term();

        let mut txn = MvccTxn::new(params.start_ts, context.concurrency_manager);
        let mut reader = ReaderWithStats::new(
            SnapshotReader::new_with_ctx(params.start_ts, snapshot, &pb_ctx),
            context.statistics,
        );

        let mut written_rows = 0;
        let total_keys = keys.len();
        let mut res = PessimisticLockResults::with_capacity(total_keys);
        let need_old_value = context.extra_op == ExtraOp::ReadOldValue;
        let mut old_values = OldValues::default();
        for (index, (key, should_not_exist)) in keys.iter().enumerate() {
            let lock_key_ctx = PessimisticLockKeyContext::from_key(index, key);
            match acquire_pessimistic_lock(
                &mut txn,
                &mut reader,
                key.clone(),
                &params.primary,
                *should_not_exist,
                params.lock_ttl,
                params.for_update_ts,
                params.return_values,
                params.check_existence,
                params.min_commit_ts,
                need_old_value,
                allow_lock_with_conflict,
            ) {
                Ok((key_res, old_value)) => {
                    res.push(key_res);
                    if old_value.resolved() {
                        let key = key.clone().append_ts(txn.start_ts);
                        // MutationType is unknown in AcquirePessimisticLock stage.
                        let mutation_type = None;
                        old_values.insert(key, (old_value, mutation_type));
                    }
                    written_rows += 1;
                }
                Err(MvccError(box MvccErrorInner::KeyIsLocked(lock_info))) => {
                    let mut lock_info = WriteResultLockInfo::new(
                        lock_key_ctx.index_in_request,
                        key.clone(),
                        *should_not_exist,
                        lock_info,
                        term,
                        params.clone(),
                        lock_key_ctx.lock_hash,
                        lock_key_ctx.hash_for_latch,
                        None,
                    );
                    if !allow_lock_with_conflict {
                        // The request is in legacy behavior. Do not lock previously succeeded keys.
                        txn.clear();
                        res.0.clear();
                    }
                    res.push(PessimisticLockKeyResult::Waiting(Some(lock_info)));
                }
                Err(e) => return Err(Error::from(e)),
            }
        }

        let modifies = txn.into_modifies();

        let pr = ProcessResult::PessimisticLockRes { res: Ok(res) };
        let to_be_write = if written_rows > 0 {
            let extra = TxnExtra {
                old_values,
                // One pc status is unknown in AcquirePessimisticLock stage.
                one_pc: false,
            };
            WriteData::new(modifies, extra)
        } else {
            WriteData::default()
        };

        Ok(WriteResult {
            ctx: pb_ctx,
            to_be_write,
            rows: written_rows,
            pr,
            released_locks: None,
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnProposed,
        })
    }

    fn process_write_for_resumed<S, L>(
        snapshot: S,
        context: WriteContext<'_, L>,
        pb_ctx: Context,
        items: Vec<ResumedPessimisticLockItem>,
    ) -> Result<WriteResult>
    where
        S: Snapshot,
        L: LockManager,
    {
        let mut modifies = vec![];
        let mut txn = None; // MvccTxn::new(start_ts, context.concurrency_manager);
        let mut reader: Option<SnapshotReader<S>> = None;

        let mut written_rows = 0;
        let mut res = PessimisticLockResults::with_capacity(items.len());
        let need_old_value = context.extra_op == ExtraOp::ReadOldValue;
        let mut old_values = OldValues::default();

        for (index, item) in items.into_iter().enumerate() {
            let ResumedPessimisticLockItem {
                key,
                should_not_exist,
                params,
                lock_key_ctx,
                awakened_with_primary_index,
                req_states,
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
                // TODO: Is it possible to reuse the same reader but change the start_ts stored in it?
                if let Some(mut reader) = reader {
                    context.statistics.add(&reader.take_statistics());
                }
                reader = Some(SnapshotReader::new_with_ctx(
                    params.start_ts,
                    snapshot.clone(),
                    &pb_ctx,
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
                true,
            ) {
                Ok((key_res, old_value)) => {
                    res.push(key_res);
                    if old_value.resolved() {
                        let key = key.append_ts(txn.start_ts);
                        // MutationType is unknown in AcquirePessimisticLock stage.
                        let mutation_type = None;
                        old_values.insert(key, (old_value, mutation_type));
                    }
                    written_rows += 1;
                }
                Err(MvccError(box MvccErrorInner::KeyIsLocked(lock_info))) => {
                    let mut lock_info = WriteResultLockInfo::new(
                        lock_key_ctx.index_in_request,
                        key,
                        should_not_exist,
                        lock_info,
                        snapshot.ext().get_term(),
                        params,
                        lock_key_ctx.lock_hash,
                        lock_key_ctx.hash_for_latch,
                        None,
                    );
                    lock_info.req_states = Some(req_states);
                    res.push(PessimisticLockKeyResult::Waiting(Some(lock_info)));
                }
                Err(e) => {
                    res.push(PessimisticLockKeyResult::Failed(Arc::new(
                        Error::from(e).into(),
                    )));
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
            };
            WriteData::new(modifies, extra)
        } else {
            WriteData::default()
        };

        Ok(WriteResult {
            ctx: pb_ctx,
            to_be_write,
            rows: written_rows,
            pr,
            released_locks: None,
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnProposed,
        })
    }
}

impl AcquirePessimisticLock {
    pub fn new_normal(
        keys: Vec<(Key, bool)>,
        primary: Vec<u8>,
        start_ts: TimeStamp,
        lock_ttl: u64,
        is_first_lock: bool,
        for_update_ts: TimeStamp,
        wait_timeout: Option<WaitTimeout>,
        return_values: bool,
        min_commit_ts: TimeStamp,
        _old_values: OldValues, // TODO: Remove it
        check_existence: bool,
        ctx: kvproto::kvrpcpb::Context,
    ) -> TypedCommand<StorageResult<PessimisticLockResults>> {
        let params = PessimisticLockParameters {
            pb_ctx: ctx.clone(),
            primary,
            start_ts,
            lock_ttl,
            for_update_ts,
            wait_timeout,
            return_values,
            min_commit_ts,
            check_existence,
        };
        let inner = PessimisticLockCmdInner::SingleRequest {
            params,
            keys,
            allow_lock_with_conflict: true,
        };
        Self::new(inner, is_first_lock, ctx)
    }

    pub fn new_disallow_lock_with_conflict(
        keys: Vec<(Key, bool)>,
        primary: Vec<u8>,
        start_ts: TimeStamp,
        lock_ttl: u64,
        is_first_lock: bool,
        for_update_ts: TimeStamp,
        wait_timeout: Option<WaitTimeout>,
        return_values: bool,
        min_commit_ts: TimeStamp,
        _old_values: OldValues, // TODO: Remove it
        check_existence: bool,
        ctx: kvproto::kvrpcpb::Context,
    ) -> TypedCommand<StorageResult<PessimisticLockResults>> {
        let params = PessimisticLockParameters {
            pb_ctx: ctx.clone(),
            primary,
            start_ts,
            lock_ttl,
            for_update_ts,
            wait_timeout,
            return_values,
            min_commit_ts,
            check_existence,
        };
        let inner = PessimisticLockCmdInner::SingleRequest {
            params,
            keys,
            allow_lock_with_conflict: false,
        };
        Self::new(inner, is_first_lock, ctx)
    }

    pub fn new_resumed_from_lock_info(
        lock_info: Vec<WriteResultLockInfo>,
    ) -> TypedCommand<StorageResult<PessimisticLockResults>> {
        let mut items: Vec<_> = lock_info
            .into_iter()
            .map(|item| {
                assert!(item.key_cb.is_none());
                let lock_key_ctx = PessimisticLockKeyContext {
                    index_in_request: item.index_in_request,
                    lock_hash: item.lock_hash,
                    hash_for_latch: item.hash_for_latch,
                };
                ResumedPessimisticLockItem {
                    key: item.key,
                    should_not_exist: item.should_not_exist,
                    params: item.parameters,
                    lock_key_ctx,
                    awakened_with_primary_index: None,
                    req_states: item.req_states.unwrap(),
                }
            })
            .collect();
        Self::new_resumed(items)
    }

    pub fn new_resumed(
        mut items: Vec<ResumedPessimisticLockItem>,
    ) -> TypedCommand<StorageResult<PessimisticLockResults>> {
        assert!(!items.is_empty());
        let ctx = items[0].params.pb_ctx.clone();
        let inner = PessimisticLockCmdInner::BatchResumedRequests { items };
        Self::new(inner, false, ctx)
    }

    pub fn is_resumed_after_waiting(&self) -> bool {
        match &self.inner {
            PessimisticLockCmdInner::SingleRequest { .. } => false,
            PessimisticLockCmdInner::BatchResumedRequests { .. } => true,
        }
    }

    pub fn get_single_request_meta(&self) -> Option<SingleRequestPessimisticLockCommandMeta> {
        match &self.inner {
            PessimisticLockCmdInner::SingleRequest { params, keys, .. } => {
                Some(SingleRequestPessimisticLockCommandMeta {
                    start_ts: params.start_ts,
                    for_update_ts: params.for_update_ts,
                    keys_count: keys.len(),
                    is_first_lock: self.is_first_lock,
                })
            }
            PessimisticLockCmdInner::BatchResumedRequests { .. } => None,
        }
    }
}

pub struct SingleRequestPessimisticLockCommandMeta {
    pub start_ts: TimeStamp,
    pub for_update_ts: TimeStamp,
    pub keys_count: usize,
    pub is_first_lock: bool,
}

#[cfg(test)]
mod tests {
    // use super::*;

    // #[test]
    // fn test_gen_lock_info_from_result() {
    //     let raw_key = b"key".to_vec();
    //     let key = Key::from_raw(&raw_key);
    //     let ts = 100;
    //     let is_first_lock = true;
    //     let wait_timeout = WaitTimeout::from_encoded(200);
    //
    //     let mut info = LockInfo::default();
    //     info.set_key(raw_key.clone());
    //     info.set_lock_version(ts);
    //     info.set_lock_ttl(100);
    //     let case = StorageError::from(StorageErrorInner::Txn(Error::from(ErrorInner::Mvcc(
    //         MvccError::from(MvccErrorInner::KeyIsLocked(info)),
    //     ))));
    //     let lock_info = WriteResultLockInfo::new(
    //         extract_lock_info_from_result::<()>(&Err(case)),
    //         is_first_lock,
    //         wait_timeout,
    //     );
    //     assert_eq!(lock_info.locks.ts, ts.into());
    //     assert_eq!(lock_info.locks.hash, key.gen_hash());
    //     assert_eq!(lock_info.key, raw_key);
    //     assert_eq!(lock_info.is_first_lock, is_first_lock);
    //     assert_eq!(lock_info.wait_timeout, wait_timeout);
    // }
}
