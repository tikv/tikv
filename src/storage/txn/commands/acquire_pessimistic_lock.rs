// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::{fmt::Formatter, sync::Arc};

use kvproto::kvrpcpb::{Context, ExtraOp};
use txn_types::{Key, OldValues, TimeStamp, TxnExtra};

use crate::storage::{
    kv::WriteData,
    lock_manager::{lock_waiting_queue::LockWaitEntry, LockManager, WaitTimeout},
    mvcc::{Error as MvccError, ErrorInner as MvccErrorInner, MvccTxn, SnapshotReader},
    txn::{
        acquire_pessimistic_lock,
        commands::{
            CommandExt, ReaderWithStats, ReleasedLocks, ResponsePolicy, TypedCommand, WriteCommand,
            WriteContext, WriteResult, WriteResultLockInfo,
        },
        Error, ErrorInner, Result,
    },
    types::{PessimisticLockKeyResult, PessimisticLockParameters, PessimisticLockResults},
    ProcessResult, Result as StorageResult, Snapshot,
};

pub struct PessimisticLockKeyContext {
    pub lock_hash: u64,
}

pub struct ResumedPessimisticLockItem {
    pub key: Key,
    pub should_not_exist: bool,
    pub params: PessimisticLockParameters,
    pub lock_key_ctx: PessimisticLockKeyContext,
}

#[allow(clippy::large_enum_variant)]
pub enum PessimisticLockCmdInner {
    SingleRequest {
        params: PessimisticLockParameters,
        keys: Vec<(Key, bool)>,
    },
    BatchResumedRequests {
        items: Vec<ResumedPessimisticLockItem>,
    },
}

impl std::fmt::Display for PessimisticLockCmdInner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PessimisticLockCmdInner::SingleRequest { params, keys } => {
                write!(
                    f,
                    "keys({}) @ {} {} {} {:?} {} {} {} | {:?}{}",
                    keys.len(),
                    params.start_ts,
                    params.lock_ttl,
                    params.for_update_ts,
                    params.wait_timeout,
                    params.min_commit_ts,
                    params.check_existence,
                    params.lock_only_if_exists,
                    params.pb_ctx,
                    if !params.allow_lock_with_conflict {
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
        }
}

impl PessimisticLockKeyContext {
    fn from_key(index: usize, key: &Key) -> Self {
        let hash_for_lock = key.gen_hash();
        // let mut hasher = DefaultHasher::new();
        // key.hash(&mut hasher);
        // let hash_for_latch = hasher.finish();
        Self {
            lock_hash: hash_for_lock,
            // hash_for_latch,
        }
    }
}

impl CommandExt for AcquirePessimisticLock {
    ctx!();
    request_type!(KvPessimisticLock);

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
            PessimisticLockCmdInner::SingleRequest { params, keys } => {
                Self::process_write_for_single_request(snapshot, context, self.ctx, params, keys)
            }
            PessimisticLockCmdInner::BatchResumedRequests { items, .. } => {
                Self::process_write_for_resumed(snapshot, context, self.ctx, items)
            }
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
struct InvalidPessimisticLockRequestError {
    desc: String,
}

impl std::fmt::Display for InvalidPessimisticLockRequestError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl std::error::Error for InvalidPessimisticLockRequestError {}

impl AcquirePessimisticLock {
    fn process_write_for_single_request<S, L>(
        snapshot: S,
        context: WriteContext<'_, L>,
        pb_ctx: Context,
        params: PessimisticLockParameters,
        keys: Vec<(Key, bool)>,
    ) -> Result<WriteResult>
    where
        S: Snapshot,
        L: LockManager,
    {
        if params.allow_lock_with_conflict && keys.len() > 1 {
            return Err(Error::from(ErrorInner::Other(Box::new(
                InvalidPessimisticLockRequestError {
                    desc: "multiple keys in a single request with allowed_lock_with_conflict set is not allowed".into(),
                },
            ))));
        }

        let mut txn = MvccTxn::new(params.start_ts, context.concurrency_manager);
        let mut reader = ReaderWithStats::new(
            SnapshotReader::new_with_ctx(params.start_ts, snapshot, &pb_ctx),
            context.statistics,
        );

        let mut written_rows = 0;
        let total_keys = keys.len();
        let mut res = PessimisticLockResults::with_capacity(total_keys);
        let mut encountered_locks = vec![];
        let need_old_value = context.extra_op == ExtraOp::ReadOldValue;
        let mut old_values = OldValues::default();
        let mut new_locked_keys = Vec::with_capacity(keys.len());
        for (key, should_not_exist) in keys.iter() {
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
                params.lock_only_if_exists,
                params.allow_lock_with_conflict,
            ) {
                Ok((key_res, old_value)) => {
                    res.push(key_res);
                    new_locked_keys.push((params.start_ts, key.clone()));
                    if old_value.resolved() {
                        let key = key.clone().append_ts(txn.start_ts);
                        // MutationType is unknown in AcquirePessimisticLock stage.
                        let mutation_type = None;
                        old_values.insert(key, (old_value, mutation_type));
                    }
                    written_rows += 1;
                }
                Err(MvccError(box MvccErrorInner::KeyIsLocked(lock_info))) => {
                    let lock_info = WriteResultLockInfo::new(
                        lock_info,
                        params.clone(),
                        key.clone(),
                        *should_not_exist,
                    );
                    encountered_locks.push(lock_info);
                    // Do not lock previously succeeded keys.
                    // For multiple keys, is_first_lock should be unset if some of the keys
                    // can be successfully locked.
                    txn.clear();
                    res.0.clear();
                    new_locked_keys.clear();
                    res.push(PessimisticLockKeyResult::Waiting);
                    break;
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
                for_flashback: false,
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
            lock_info: encountered_locks,
            released_locks: ReleasedLocks::new(),
            new_acquired_locks: new_locked_keys,
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
        let mut encountered_locks = vec![];
        let need_old_value = context.extra_op == ExtraOp::ReadOldValue;
        let mut old_values = OldValues::default();

        let mut new_locked_keys = Vec::with_capacity(items.len());

        for item in items.into_iter() {
            let ResumedPessimisticLockItem {
                key,
                should_not_exist,
                params,
                lock_key_ctx,
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
                    res.push(PessimisticLockKeyResult::Waiting);
                    encountered_locks.push(lock_info);
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
                for_flashback: false,
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
            lock_info: encountered_locks,
            released_locks: ReleasedLocks::new(),
            new_acquired_locks: new_locked_keys,
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
        check_existence: bool,
        lock_only_if_exists: bool,
        allow_lock_with_conflict: bool,
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
            is_first_lock,
            lock_only_if_exists,
            allow_lock_with_conflict,
        };
        let inner = PessimisticLockCmdInner::SingleRequest { params, keys };
        Self::new(inner, ctx)
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
        lock_only_if_exists: bool,
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
            is_first_lock,
            lock_only_if_exists,
            allow_lock_with_conflict: false,
        };
        let inner = PessimisticLockCmdInner::SingleRequest { params, keys };
        Self::new(inner, ctx)
    }

    pub fn new_resumed_from_lock_wait_entries(
        lock_wait_entries: impl IntoIterator<Item = Box<LockWaitEntry>>,
    ) -> TypedCommand<StorageResult<PessimisticLockResults>> {
        let items: Vec<_> = lock_wait_entries
            .into_iter()
            .map(|item| {
                assert!(item.key_cb.is_none());
                let lock_key_ctx = PessimisticLockKeyContext {
                    lock_hash: item.lock_hash,
                };
                ResumedPessimisticLockItem {
                    key: item.key,
                    should_not_exist: item.should_not_exist,
                    params: item.parameters,
                    lock_key_ctx,
                }
            })
            .collect();
        Self::new_resumed(items)
    }

    pub fn new_resumed(
        items: Vec<ResumedPessimisticLockItem>,
    ) -> TypedCommand<StorageResult<PessimisticLockResults>> {
        assert!(!items.is_empty());
        let ctx = items[0].params.pb_ctx.clone();
        let inner = PessimisticLockCmdInner::BatchResumedRequests { items };
        Self::new(inner, ctx)
    }

    pub fn is_resumed_after_waiting(&self) -> bool {
        match &self.inner {
            PessimisticLockCmdInner::SingleRequest { .. } => false,
            PessimisticLockCmdInner::BatchResumedRequests { .. } => true,
        }
    }

    pub fn get_single_request_meta(&self) -> Option<SingleRequestPessimisticLockCommandMeta> {
        match &self.inner {
            PessimisticLockCmdInner::SingleRequest { params, keys } => {
                Some(SingleRequestPessimisticLockCommandMeta {
                    start_ts: params.start_ts,
                    for_update_ts: params.for_update_ts,
                    keys_count: keys.len(),
                    is_first_lock: params.is_first_lock,
                    allow_lock_with_conflict: params.allow_lock_with_conflict,
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
    pub allow_lock_with_conflict: bool,
}
