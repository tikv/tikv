// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
//! Commands used in the transaction system
#[macro_use]
mod macros;
pub(crate) mod acquire_pessimistic_lock;
pub(crate) mod acquire_pessimistic_lock_resumed;
pub(crate) mod atomic_store;
pub(crate) mod check_secondary_locks;
pub(crate) mod check_txn_status;
pub(crate) mod cleanup;
pub(crate) mod commit;
pub(crate) mod compare_and_swap;
pub(crate) mod flashback_to_version;
pub(crate) mod flashback_to_version_read_phase;
pub(crate) mod flush;
pub(crate) mod mvcc_by_key;
pub(crate) mod mvcc_by_start_ts;
pub(crate) mod pause;
pub(crate) mod pessimistic_rollback;
mod pessimistic_rollback_read_phase;
pub(crate) mod prewrite;
pub(crate) mod resolve_lock;
pub(crate) mod resolve_lock_lite;
pub(crate) mod resolve_lock_readphase;
pub(crate) mod rollback;
pub(crate) mod txn_heart_beat;

use std::{
    fmt::{self, Debug, Display, Formatter},
    iter,
    marker::PhantomData,
    ops::{Deref, DerefMut},
    sync::Arc,
};

pub use acquire_pessimistic_lock::AcquirePessimisticLock;
pub use acquire_pessimistic_lock_resumed::AcquirePessimisticLockResumed;
pub use atomic_store::RawAtomicStore;
pub use check_secondary_locks::CheckSecondaryLocks;
pub use check_txn_status::CheckTxnStatus;
pub use cleanup::Cleanup;
pub use commit::Commit;
pub use compare_and_swap::RawCompareAndSwap;
use concurrency_manager::{ConcurrencyManager, KeyHandleGuard};
pub use flashback_to_version::FlashbackToVersion;
pub use flashback_to_version_read_phase::{
    new_flashback_rollback_lock_cmd, new_flashback_write_cmd, FlashbackToVersionReadPhase,
    FlashbackToVersionState,
};
pub use flush::Flush;
use kvproto::kvrpcpb::*;
pub use mvcc_by_key::MvccByKey;
pub use mvcc_by_start_ts::MvccByStartTs;
pub use pause::Pause;
pub use pessimistic_rollback::PessimisticRollback;
pub use pessimistic_rollback_read_phase::PessimisticRollbackReadPhase;
pub use prewrite::{one_pc_commit, Prewrite, PrewritePessimistic};
pub use resolve_lock::{ResolveLock, RESOLVE_LOCK_BATCH_SIZE};
pub use resolve_lock_lite::ResolveLockLite;
pub use resolve_lock_readphase::ResolveLockReadPhase;
pub use rollback::Rollback;
use tikv_util::{deadline::Deadline, memory::HeapSize};
use tracker::RequestType;
pub use txn_heart_beat::TxnHeartBeat;
use txn_types::{Key, TimeStamp, Value, Write};

use crate::storage::{
    kv::WriteData,
    lock_manager::{
        self, lock_wait_context::LockWaitContextSharedState, LockManager, LockWaitToken,
        WaitTimeout,
    },
    metrics,
    mvcc::{Lock as MvccLock, MvccReader, ReleasedLock, SnapshotReader},
    txn::{latch, txn_status_cache::TxnStatusCache, ProcessResult, Result},
    types::{
        MvccInfo, PessimisticLockParameters, PessimisticLockResults, PrewriteResult,
        SecondaryLocksStatus, StorageCallbackType, TxnStatus,
    },
    Result as StorageResult, Snapshot, Statistics,
};

/// Store Transaction scheduler commands.
///
/// Learn more about our transaction system at
/// [Deep Dive TiKV: Distributed Transactions](https://tikv.org/docs/deep-dive/distributed-transaction/introduction/)
///
/// These are typically scheduled and used through the
/// [`Storage`](crate::storage::Storage) with functions like
/// [`prewrite`](prewrite::Prewrite) trait and are executed asynchronously.
pub enum Command {
    Prewrite(Prewrite),
    PrewritePessimistic(PrewritePessimistic),
    AcquirePessimisticLock(AcquirePessimisticLock),
    AcquirePessimisticLockResumed(AcquirePessimisticLockResumed),
    Commit(Commit),
    Cleanup(Cleanup),
    Rollback(Rollback),
    PessimisticRollback(PessimisticRollback),
    PessimisticRollbackReadPhase(PessimisticRollbackReadPhase),
    TxnHeartBeat(TxnHeartBeat),
    CheckTxnStatus(CheckTxnStatus),
    CheckSecondaryLocks(CheckSecondaryLocks),
    ResolveLockReadPhase(ResolveLockReadPhase),
    ResolveLock(ResolveLock),
    ResolveLockLite(ResolveLockLite),
    Pause(Pause),
    MvccByKey(MvccByKey),
    MvccByStartTs(MvccByStartTs),
    RawCompareAndSwap(RawCompareAndSwap),
    RawAtomicStore(RawAtomicStore),
    FlashbackToVersionReadPhase(FlashbackToVersionReadPhase),
    FlashbackToVersion(FlashbackToVersion),
    Flush(Flush),
}

/// A `Command` with its return type, reified as the generic parameter `T`.
///
/// Incoming grpc requests (like `CommitRequest`, `PrewriteRequest`) are
/// converted to this type via a series of transformations. That process is
/// described below using `CommitRequest` as an example:
/// 1. A `CommitRequest` is handled by the `future_commit` method in kv.rs,
/// where it needs to be transformed to a `TypedCommand` before being passed to
/// the `storage.sched_txn_command` method.
/// 2. The `From<CommitRequest>` impl for `TypedCommand` gets chosen, and its
/// generic parameter indicates that the result type for this instance of
/// `TypedCommand` is going to be `TxnStatus` - one of the variants of the
/// `StorageCallback` enum.
/// 3. In the above `from` method, the details of the
/// commit request are captured by creating an instance of the struct
/// `storage::txn::commands::commit::Command` via its `new` method.
/// 4. This struct is wrapped in a variant of the enum
/// `storage::txn::commands::Command`. This enum exists to facilitate generic
/// operations over different commands. 5. Finally, the `Command` enum variant
/// for `Commit` is converted to the `TypedCommand` using the `From<Command>`
/// impl for `TypedCommand`.
///
/// For other requests, see the corresponding `future_` method, the `From` trait
/// implementation and so on.
pub struct TypedCommand<T> {
    pub cmd: Command,

    /// Track the type of the command's return value.
    _pd: PhantomData<T>,
}

impl<T: StorageCallbackType> From<Command> for TypedCommand<T> {
    fn from(cmd: Command) -> TypedCommand<T> {
        TypedCommand {
            cmd,
            _pd: PhantomData,
        }
    }
}

impl<T> From<TypedCommand<T>> for Command {
    fn from(t: TypedCommand<T>) -> Command {
        t.cmd
    }
}

impl From<PrewriteRequest> for TypedCommand<PrewriteResult> {
    fn from(mut req: PrewriteRequest) -> Self {
        let for_update_ts = req.get_for_update_ts();
        let secondary_keys = if req.get_use_async_commit() {
            Some(req.get_secondaries().into())
        } else {
            None
        };
        if for_update_ts == 0 {
            Prewrite::new(
                req.take_mutations().into_iter().map(Into::into).collect(),
                req.take_primary_lock(),
                req.get_start_version().into(),
                req.get_lock_ttl(),
                req.get_skip_constraint_check(),
                req.get_txn_size(),
                req.get_min_commit_ts().into(),
                req.get_max_commit_ts().into(),
                secondary_keys,
                req.get_try_one_pc(),
                req.get_assertion_level(),
                req.take_context(),
            )
        } else {
            let pessimistic_actions = req.take_pessimistic_actions();
            let mutations = req
                .take_mutations()
                .into_iter()
                .map(Into::into)
                .zip(pessimistic_actions)
                .collect();
            PrewritePessimistic::new(
                mutations,
                req.take_primary_lock(),
                req.get_start_version().into(),
                req.get_lock_ttl(),
                for_update_ts.into(),
                req.get_txn_size(),
                req.get_min_commit_ts().into(),
                req.get_max_commit_ts().into(),
                secondary_keys,
                req.get_try_one_pc(),
                req.get_assertion_level(),
                req.take_for_update_ts_constraints().into(),
                req.take_context(),
            )
        }
    }
}

impl From<PessimisticLockRequest> for TypedCommand<StorageResult<PessimisticLockResults>> {
    fn from(mut req: PessimisticLockRequest) -> Self {
        let keys = req
            .take_mutations()
            .into_iter()
            .map(|x| match x.get_op() {
                Op::PessimisticLock => (
                    Key::from_raw(x.get_key()),
                    x.get_assertion() == Assertion::NotExist,
                ),
                _ => panic!("mismatch Op in pessimistic lock mutations"),
            })
            .collect();

        let allow_lock_with_conflict = match req.get_wake_up_mode() {
            PessimisticLockWakeUpMode::WakeUpModeNormal => false,
            PessimisticLockWakeUpMode::WakeUpModeForceLock => true,
        };

        AcquirePessimisticLock::new(
            keys,
            req.take_primary_lock(),
            req.get_start_version().into(),
            req.get_lock_ttl(),
            req.get_is_first_lock(),
            req.get_for_update_ts().into(),
            WaitTimeout::from_encoded(req.get_wait_timeout()),
            req.get_return_values(),
            req.get_min_commit_ts().into(),
            req.get_check_existence(),
            req.get_lock_only_if_exists(),
            allow_lock_with_conflict,
            req.take_context(),
        )
    }
}

impl From<CommitRequest> for TypedCommand<TxnStatus> {
    fn from(mut req: CommitRequest) -> Self {
        let keys = req.get_keys().iter().map(|x| Key::from_raw(x)).collect();

        Commit::new(
            keys,
            req.get_start_version().into(),
            req.get_commit_version().into(),
            req.take_context(),
        )
    }
}

impl From<CleanupRequest> for TypedCommand<()> {
    fn from(mut req: CleanupRequest) -> Self {
        Cleanup::new(
            Key::from_raw(req.get_key()),
            req.get_start_version().into(),
            req.get_current_ts().into(),
            req.take_context(),
        )
    }
}

impl From<BatchRollbackRequest> for TypedCommand<()> {
    fn from(mut req: BatchRollbackRequest) -> Self {
        let keys = req.get_keys().iter().map(|x| Key::from_raw(x)).collect();
        Rollback::new(keys, req.get_start_version().into(), req.take_context())
    }
}

impl From<PessimisticRollbackRequest> for TypedCommand<Vec<StorageResult<()>>> {
    fn from(mut req: PessimisticRollbackRequest) -> Self {
        // If the keys are empty, try to scan locks with specified `start_ts` and
        // `for_update_ts`, and then pass them to a new pessimitic rollback
        // command to clean up, just like resolve lock with read phase.
        if req.get_keys().is_empty() {
            PessimisticRollbackReadPhase::new(
                req.get_start_version().into(),
                req.get_for_update_ts().into(),
                None,
                req.take_context(),
            )
        } else {
            let keys = req.get_keys().iter().map(|x| Key::from_raw(x)).collect();
            PessimisticRollback::new(
                keys,
                req.get_start_version().into(),
                req.get_for_update_ts().into(),
                None,
                req.take_context(),
            )
        }
    }
}

impl From<TxnHeartBeatRequest> for TypedCommand<TxnStatus> {
    fn from(mut req: TxnHeartBeatRequest) -> Self {
        TxnHeartBeat::new(
            Key::from_raw(req.get_primary_lock()),
            req.get_start_version().into(),
            req.get_advise_lock_ttl(),
            req.take_context(),
        )
    }
}

impl From<CheckTxnStatusRequest> for TypedCommand<TxnStatus> {
    fn from(mut req: CheckTxnStatusRequest) -> Self {
        CheckTxnStatus::new(
            Key::from_raw(req.get_primary_key()),
            req.get_lock_ts().into(),
            req.get_caller_start_ts().into(),
            req.get_current_ts().into(),
            req.get_rollback_if_not_exist(),
            req.get_force_sync_commit(),
            req.get_resolving_pessimistic_lock(),
            req.get_verify_is_primary(),
            req.take_context(),
        )
    }
}

impl From<CheckSecondaryLocksRequest> for TypedCommand<SecondaryLocksStatus> {
    fn from(mut req: CheckSecondaryLocksRequest) -> Self {
        CheckSecondaryLocks::new(
            req.take_keys()
                .into_iter()
                .map(|k| Key::from_raw(&k))
                .collect(),
            req.get_start_version().into(),
            req.take_context(),
        )
    }
}

impl From<ResolveLockRequest> for TypedCommand<()> {
    fn from(mut req: ResolveLockRequest) -> Self {
        let resolve_keys: Vec<Key> = req
            .get_keys()
            .iter()
            .map(|key| Key::from_raw(key))
            .collect();
        let txn_status = if req.get_start_version() > 0 {
            iter::once((
                req.get_start_version().into(),
                req.get_commit_version().into(),
            ))
            .collect()
        } else {
            req.take_txn_infos()
                .into_iter()
                .map(|info| (info.txn.into(), info.status.into()))
                .collect()
        };

        if resolve_keys.is_empty() {
            ResolveLockReadPhase::new(txn_status, None, req.take_context())
        } else {
            let start_ts: TimeStamp = req.get_start_version().into();
            assert!(!start_ts.is_zero());
            let commit_ts = req.get_commit_version().into();
            ResolveLockLite::new(start_ts, commit_ts, resolve_keys, req.take_context())
        }
    }
}

impl From<MvccGetByKeyRequest> for TypedCommand<MvccInfo> {
    fn from(mut req: MvccGetByKeyRequest) -> Self {
        MvccByKey::new(Key::from_raw(req.get_key()), req.take_context())
    }
}

impl From<MvccGetByStartTsRequest> for TypedCommand<Option<(Key, MvccInfo)>> {
    fn from(mut req: MvccGetByStartTsRequest) -> Self {
        MvccByStartTs::new(req.get_start_ts().into(), req.take_context())
    }
}

impl From<PrepareFlashbackToVersionRequest> for TypedCommand<()> {
    fn from(mut req: PrepareFlashbackToVersionRequest) -> Self {
        new_flashback_rollback_lock_cmd(
            req.get_start_ts().into(),
            req.get_version().into(),
            Key::from_raw(req.get_start_key()),
            Key::from_raw_maybe_unbounded(req.get_end_key()),
            req.take_context(),
        )
    }
}

impl From<FlashbackToVersionRequest> for TypedCommand<()> {
    fn from(mut req: FlashbackToVersionRequest) -> Self {
        new_flashback_write_cmd(
            req.get_start_ts().into(),
            req.get_commit_ts().into(),
            req.get_version().into(),
            Key::from_raw(req.get_start_key()),
            Key::from_raw_maybe_unbounded(req.get_end_key()),
            req.take_context(),
        )
    }
}

impl From<FlushRequest> for TypedCommand<Vec<StorageResult<()>>> {
    fn from(mut req: FlushRequest) -> Self {
        Flush::new(
            req.get_start_ts().into(),
            req.take_primary_key(),
            req.take_mutations().into_iter().map(Into::into).collect(),
            req.get_generation(),
            req.get_lock_ttl(),
            req.get_assertion_level(),
            req.take_context(),
        )
    }
}

/// Represents for a scheduler command, when should the response sent to the
/// client. For most cases, the response should be sent after the result being
/// successfully applied to the storage (if needed). But in some special cases,
/// some optimizations allows the response to be returned at an earlier phase.
///
/// Note that this doesn't affect latch releasing. The latch and the memory lock
/// (if any) are always released after applying, regardless of when the response
/// is sent.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ResponsePolicy {
    /// Return the response to the client when the command has finished
    /// applying.
    OnApplied,
    /// Return the response after finishing Raft committing.
    OnCommitted,
    /// Return the response after finishing raft proposing.
    OnProposed,
}

pub struct WriteResult {
    pub ctx: Context,
    pub to_be_write: WriteData,
    pub rows: usize,
    pub pr: ProcessResult,
    pub lock_info: Vec<WriteResultLockInfo>,
    pub released_locks: ReleasedLocks,
    pub new_acquired_locks: Vec<LockInfo>,
    pub lock_guards: Vec<KeyHandleGuard>,
    pub response_policy: ResponsePolicy,
    /// The txn status that can be inferred by the successful writing. This will
    /// be used to update the cache.
    ///
    /// Currently only commit_ts of committed transactions will be collected.
    /// Rolled-back transactions may also be collected in the future.
    pub known_txn_status: Vec<(TimeStamp, TimeStamp)>,
}

pub struct WriteResultLockInfo {
    pub lock_digest: lock_manager::LockDigest,
    pub key: Key,
    pub should_not_exist: bool,
    pub lock_info_pb: LockInfo,
    pub parameters: PessimisticLockParameters,
    pub hash_for_latch: u64,
    /// If a request is woken up after waiting for some lock, and it encounters
    /// another lock again after resuming, this field will carry the token
    /// that was already allocated before.
    pub lock_wait_token: LockWaitToken,
    /// For resumed pessimistic lock requests, this is needed to check if it's
    /// canceled outside.
    pub req_states: Option<Arc<LockWaitContextSharedState>>,
}

impl WriteResultLockInfo {
    pub fn new(
        lock_info_pb: LockInfo,
        parameters: PessimisticLockParameters,
        key: Key,
        should_not_exist: bool,
    ) -> Self {
        let lock = lock_manager::LockDigest {
            ts: lock_info_pb.get_lock_version().into(),
            hash: key.gen_hash(),
        };
        let hash_for_latch = latch::Lock::hash(&key);
        Self {
            lock_digest: lock,
            key,
            should_not_exist,
            lock_info_pb,
            parameters,
            hash_for_latch,
            lock_wait_token: LockWaitToken(None),
            req_states: None,
        }
    }
}

#[derive(Default)]
pub struct ReleasedLocks(Vec<ReleasedLock>);

impl ReleasedLocks {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, lock: Option<ReleasedLock>) {
        if let Some(lock) = lock {
            self.0.push(lock);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn clear(&mut self) {
        self.0.clear()
    }

    pub fn into_iter(self) -> impl Iterator<Item = ReleasedLock> {
        self.0.into_iter()
    }
}

type LockWritesVals = (
    Option<MvccLock>,
    Vec<(TimeStamp, Write)>,
    Vec<(TimeStamp, Value)>,
);

fn find_mvcc_infos_by_key<S: Snapshot>(
    reader: &mut MvccReader<S>,
    key: &Key,
    mut ts: TimeStamp,
) -> Result<LockWritesVals> {
    let mut writes = vec![];
    let mut values = vec![];
    let lock = reader.load_lock(key)?;
    loop {
        let opt = reader.seek_write(key, ts)?;
        match opt {
            Some((commit_ts, write)) => {
                writes.push((commit_ts, write));
                if commit_ts.is_zero() {
                    break;
                }
                ts = commit_ts.prev();
            }
            None => break,
        };
    }
    for (ts, v) in reader.scan_values_in_default(key)? {
        values.push((ts, v));
    }
    Ok((lock, writes, values))
}

pub trait CommandExt: Display {
    fn tag(&self) -> metrics::CommandKind;

    fn request_type(&self) -> RequestType {
        RequestType::Unknown
    }

    fn get_ctx(&self) -> &Context;

    fn get_ctx_mut(&mut self) -> &mut Context;

    fn deadline(&self) -> Deadline;

    fn incr_cmd_metric(&self);

    fn ts(&self) -> TimeStamp {
        TimeStamp::zero()
    }

    fn readonly(&self) -> bool {
        false
    }

    fn is_sys_cmd(&self) -> bool {
        false
    }

    fn can_be_pipelined(&self) -> bool {
        false
    }

    fn write_bytes(&self) -> usize;

    fn gen_lock(&self) -> latch::Lock;
}

pub struct RawExt {
    pub ts: TimeStamp,
    pub key_guard: KeyHandleGuard,
}

pub struct WriteContext<'a, L: LockManager> {
    pub lock_mgr: &'a L,
    pub concurrency_manager: ConcurrencyManager,
    pub extra_op: ExtraOp,
    pub statistics: &'a mut Statistics,
    pub async_apply_prewrite: bool,
    pub raw_ext: Option<RawExt>,
    // use for apiv2
    pub txn_status_cache: &'a TxnStatusCache,
}

pub struct ReaderWithStats<'a, S: Snapshot> {
    reader: SnapshotReader<S>,
    statistics: &'a mut Statistics,
}

impl<'a, S: Snapshot> ReaderWithStats<'a, S> {
    fn new(reader: SnapshotReader<S>, statistics: &'a mut Statistics) -> Self {
        Self { reader, statistics }
    }
}

impl<'a, S: Snapshot> Deref for ReaderWithStats<'a, S> {
    type Target = SnapshotReader<S>;

    fn deref(&self) -> &Self::Target {
        &self.reader
    }
}

impl<'a, S: Snapshot> DerefMut for ReaderWithStats<'a, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.reader
    }
}

impl<'a, S: Snapshot> Drop for ReaderWithStats<'a, S> {
    fn drop(&mut self) {
        self.statistics.add(&self.reader.take_statistics())
    }
}

impl Command {
    // These two are for backward compatibility, after some other refactors are done
    // we can remove Command totally and use `&dyn CommandExt` instead
    fn command_ext(&self) -> &dyn CommandExt {
        match &self {
            Command::Prewrite(t) => t,
            Command::PrewritePessimistic(t) => t,
            Command::AcquirePessimisticLock(t) => t,
            Command::AcquirePessimisticLockResumed(t) => t,
            Command::Commit(t) => t,
            Command::Cleanup(t) => t,
            Command::Rollback(t) => t,
            Command::PessimisticRollback(t) => t,
            Command::PessimisticRollbackReadPhase(t) => t,
            Command::TxnHeartBeat(t) => t,
            Command::CheckTxnStatus(t) => t,
            Command::CheckSecondaryLocks(t) => t,
            Command::ResolveLockReadPhase(t) => t,
            Command::ResolveLock(t) => t,
            Command::ResolveLockLite(t) => t,
            Command::Pause(t) => t,
            Command::MvccByKey(t) => t,
            Command::MvccByStartTs(t) => t,
            Command::RawCompareAndSwap(t) => t,
            Command::RawAtomicStore(t) => t,
            Command::FlashbackToVersionReadPhase(t) => t,
            Command::FlashbackToVersion(t) => t,
            Command::Flush(t) => t,
        }
    }

    fn command_ext_mut(&mut self) -> &mut dyn CommandExt {
        match self {
            Command::Prewrite(t) => t,
            Command::PrewritePessimistic(t) => t,
            Command::AcquirePessimisticLock(t) => t,
            Command::AcquirePessimisticLockResumed(t) => t,
            Command::Commit(t) => t,
            Command::Cleanup(t) => t,
            Command::Rollback(t) => t,
            Command::PessimisticRollback(t) => t,
            Command::PessimisticRollbackReadPhase(t) => t,
            Command::TxnHeartBeat(t) => t,
            Command::CheckTxnStatus(t) => t,
            Command::CheckSecondaryLocks(t) => t,
            Command::ResolveLockReadPhase(t) => t,
            Command::ResolveLock(t) => t,
            Command::ResolveLockLite(t) => t,
            Command::Pause(t) => t,
            Command::MvccByKey(t) => t,
            Command::MvccByStartTs(t) => t,
            Command::RawCompareAndSwap(t) => t,
            Command::RawAtomicStore(t) => t,
            Command::FlashbackToVersionReadPhase(t) => t,
            Command::FlashbackToVersion(t) => t,
            Command::Flush(t) => t,
        }
    }

    pub(super) fn process_read<S: Snapshot>(
        self,
        snapshot: S,
        statistics: &mut Statistics,
    ) -> Result<ProcessResult> {
        match self {
            Command::ResolveLockReadPhase(t) => t.process_read(snapshot, statistics),
            Command::PessimisticRollbackReadPhase(t) => t.process_read(snapshot, statistics),
            Command::MvccByKey(t) => t.process_read(snapshot, statistics),
            Command::MvccByStartTs(t) => t.process_read(snapshot, statistics),
            Command::FlashbackToVersionReadPhase(t) => t.process_read(snapshot, statistics),
            _ => panic!("unsupported read command"),
        }
    }

    pub(crate) fn process_write<S: Snapshot, L: LockManager>(
        self,
        snapshot: S,
        context: WriteContext<'_, L>,
    ) -> Result<WriteResult> {
        match self {
            Command::Prewrite(t) => t.process_write(snapshot, context),
            Command::PrewritePessimistic(t) => t.process_write(snapshot, context),
            Command::AcquirePessimisticLock(t) => t.process_write(snapshot, context),
            Command::AcquirePessimisticLockResumed(t) => t.process_write(snapshot, context),
            Command::Commit(t) => t.process_write(snapshot, context),
            Command::Cleanup(t) => t.process_write(snapshot, context),
            Command::Rollback(t) => t.process_write(snapshot, context),
            Command::PessimisticRollback(t) => t.process_write(snapshot, context),
            Command::ResolveLock(t) => t.process_write(snapshot, context),
            Command::ResolveLockLite(t) => t.process_write(snapshot, context),
            Command::TxnHeartBeat(t) => t.process_write(snapshot, context),
            Command::CheckTxnStatus(t) => t.process_write(snapshot, context),
            Command::CheckSecondaryLocks(t) => t.process_write(snapshot, context),
            Command::Pause(t) => t.process_write(snapshot, context),
            Command::RawCompareAndSwap(t) => t.process_write(snapshot, context),
            Command::RawAtomicStore(t) => t.process_write(snapshot, context),
            Command::FlashbackToVersion(t) => t.process_write(snapshot, context),
            Command::Flush(t) => t.process_write(snapshot, context),
            _ => panic!("unsupported write command"),
        }
    }

    pub fn readonly(&self) -> bool {
        self.command_ext().readonly()
    }

    pub fn incr_cmd_metric(&self) {
        self.command_ext().incr_cmd_metric()
    }

    pub fn priority(&self) -> CommandPri {
        if self.command_ext().is_sys_cmd() {
            return CommandPri::High;
        }
        self.command_ext().get_ctx().get_priority()
    }

    pub fn resource_control_ctx(&self) -> &ResourceControlContext {
        self.command_ext().get_ctx().get_resource_control_context()
    }

    pub fn group_name(&self) -> String {
        self.resource_control_ctx()
            .get_resource_group_name()
            .to_owned()
    }

    pub fn need_flow_control(&self) -> bool {
        !self.readonly() && self.priority() != CommandPri::High
    }

    pub fn tag(&self) -> metrics::CommandKind {
        self.command_ext().tag()
    }

    pub fn request_type(&self) -> RequestType {
        self.command_ext().request_type()
    }

    pub fn ts(&self) -> TimeStamp {
        self.command_ext().ts()
    }

    pub fn write_bytes(&self) -> usize {
        self.command_ext().write_bytes()
    }

    pub fn gen_lock(&self) -> latch::Lock {
        self.command_ext().gen_lock()
    }

    pub fn can_be_pipelined(&self) -> bool {
        self.command_ext().can_be_pipelined()
    }

    pub fn ctx(&self) -> &Context {
        self.command_ext().get_ctx()
    }

    pub fn ctx_mut(&mut self) -> &mut Context {
        self.command_ext_mut().get_ctx_mut()
    }

    pub fn deadline(&self) -> Deadline {
        self.command_ext().deadline()
    }
}

impl Display for Command {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.command_ext().fmt(f)
    }
}

impl Debug for Command {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.command_ext().fmt(f)
    }
}

impl HeapSize for Command {
    fn approximate_heap_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + match self {
                Command::Prewrite(t) => t.approximate_heap_size(),
                Command::PrewritePessimistic(t) => t.approximate_heap_size(),
                Command::AcquirePessimisticLock(t) => t.approximate_heap_size(),
                Command::AcquirePessimisticLockResumed(t) => t.approximate_heap_size(),
                Command::Commit(t) => t.approximate_heap_size(),
                Command::Cleanup(t) => t.approximate_heap_size(),
                Command::Rollback(t) => t.approximate_heap_size(),
                Command::PessimisticRollback(t) => t.approximate_heap_size(),
                Command::PessimisticRollbackReadPhase(t) => t.approximate_heap_size(),
                Command::TxnHeartBeat(t) => t.approximate_heap_size(),
                Command::CheckTxnStatus(t) => t.approximate_heap_size(),
                Command::CheckSecondaryLocks(t) => t.approximate_heap_size(),
                Command::ResolveLockReadPhase(t) => t.approximate_heap_size(),
                Command::ResolveLock(t) => t.approximate_heap_size(),
                Command::ResolveLockLite(t) => t.approximate_heap_size(),
                Command::Pause(t) => t.approximate_heap_size(),
                Command::MvccByKey(t) => t.approximate_heap_size(),
                Command::MvccByStartTs(t) => t.approximate_heap_size(),
                Command::RawCompareAndSwap(t) => t.approximate_heap_size(),
                Command::RawAtomicStore(t) => t.approximate_heap_size(),
                Command::FlashbackToVersionReadPhase(t) => t.approximate_heap_size(),
                Command::FlashbackToVersion(t) => t.approximate_heap_size(),
                Command::Flush(t) => t.approximate_heap_size(),
            }
    }
}

/// Commands that do not need to modify the database during execution will
/// implement this trait.
pub trait ReadCommand<S: Snapshot>: CommandExt {
    fn process_read(self, snapshot: S, statistics: &mut Statistics) -> Result<ProcessResult>;
}

/// Commands that need to modify the database during execution will implement
/// this trait.
pub trait WriteCommand<S: Snapshot, L: LockManager>: CommandExt {
    fn process_write(self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult>;
}

#[cfg(test)]
pub mod test_util {
    use std::sync::Arc;

    use causal_ts::CausalTsProviderImpl;
    use kvproto::kvrpcpb::ApiVersion;
    use txn_types::Mutation;

    use super::*;
    use crate::storage::{
        mvcc::{Error as MvccError, ErrorInner as MvccErrorInner},
        txn::{Error, ErrorInner, Result},
        Engine, MockLockManager,
    };

    // Some utils for tests that may be used in multiple source code files.

    pub fn prewrite_command<E: Engine>(
        engine: &mut E,
        cm: ConcurrencyManager,
        statistics: &mut Statistics,
        cmd: TypedCommand<PrewriteResult>,
    ) -> Result<PrewriteResult> {
        let snap = engine.snapshot(Default::default())?;
        let context = WriteContext {
            lock_mgr: &MockLockManager::new(),
            concurrency_manager: cm,
            extra_op: ExtraOp::Noop,
            statistics,
            async_apply_prewrite: false,
            raw_ext: None,
            txn_status_cache: &TxnStatusCache::new_for_test(),
        };
        let ret = cmd.cmd.process_write(snap, context)?;
        let res = match ret.pr {
            ProcessResult::PrewriteResult {
                result: PrewriteResult { locks, .. },
            } if !locks.is_empty() => {
                let info = LockInfo::default();
                return Err(Error::from(ErrorInner::Mvcc(MvccError::from(
                    MvccErrorInner::KeyIsLocked(info),
                ))));
            }
            ProcessResult::PrewriteResult { result } => result,
            _ => unreachable!(),
        };
        let ctx = Context::default();
        if !ret.to_be_write.modifies.is_empty() {
            engine.write(&ctx, ret.to_be_write).unwrap();
        }
        Ok(res)
    }

    pub fn prewrite<E: Engine>(
        engine: &mut E,
        statistics: &mut Statistics,
        mutations: Vec<Mutation>,
        primary: Vec<u8>,
        start_ts: u64,
        one_pc_max_commit_ts: Option<u64>,
    ) -> Result<PrewriteResult> {
        let cm = ConcurrencyManager::new(start_ts.into());
        prewrite_with_cm(
            engine,
            cm,
            statistics,
            mutations,
            primary,
            start_ts,
            one_pc_max_commit_ts,
        )
    }

    pub fn prewrite_with_cm<E: Engine>(
        engine: &mut E,
        cm: ConcurrencyManager,
        statistics: &mut Statistics,
        mutations: Vec<Mutation>,
        primary: Vec<u8>,
        start_ts: u64,
        one_pc_max_commit_ts: Option<u64>,
    ) -> Result<PrewriteResult> {
        let cmd = if let Some(max_commit_ts) = one_pc_max_commit_ts {
            Prewrite::with_1pc(
                mutations,
                primary,
                TimeStamp::from(start_ts),
                max_commit_ts.into(),
            )
        } else {
            Prewrite::with_defaults(mutations, primary, TimeStamp::from(start_ts))
        };
        prewrite_command(engine, cm, statistics, cmd)
    }

    pub fn pessimistic_prewrite<E: Engine>(
        engine: &mut E,
        statistics: &mut Statistics,
        mutations: Vec<(Mutation, PrewriteRequestPessimisticAction)>,
        primary: Vec<u8>,
        start_ts: u64,
        for_update_ts: u64,
        one_pc_max_commit_ts: Option<u64>,
    ) -> Result<PrewriteResult> {
        let cm = ConcurrencyManager::new(start_ts.into());
        pessimistic_prewrite_with_cm(
            engine,
            cm,
            statistics,
            mutations,
            primary,
            start_ts,
            for_update_ts,
            one_pc_max_commit_ts,
        )
    }

    pub fn pessimistic_prewrite_with_cm<E: Engine>(
        engine: &mut E,
        cm: ConcurrencyManager,
        statistics: &mut Statistics,
        mutations: Vec<(Mutation, PrewriteRequestPessimisticAction)>,
        primary: Vec<u8>,
        start_ts: u64,
        for_update_ts: u64,
        one_pc_max_commit_ts: Option<u64>,
    ) -> Result<PrewriteResult> {
        let cmd = if let Some(max_commit_ts) = one_pc_max_commit_ts {
            PrewritePessimistic::with_1pc(
                mutations,
                primary,
                start_ts.into(),
                for_update_ts.into(),
                max_commit_ts.into(),
            )
        } else {
            PrewritePessimistic::with_defaults(
                mutations,
                primary,
                start_ts.into(),
                for_update_ts.into(),
            )
        };
        prewrite_command(engine, cm, statistics, cmd)
    }

    pub fn pessimistic_prewrite_check_for_update_ts<E: Engine>(
        engine: &mut E,
        statistics: &mut Statistics,
        mutations: Vec<(Mutation, PrewriteRequestPessimisticAction)>,
        primary: Vec<u8>,
        start_ts: u64,
        for_update_ts: u64,
        for_update_ts_constraints: impl IntoIterator<Item = (usize, u64)>,
    ) -> Result<PrewriteResult> {
        let cmd = PrewritePessimistic::with_for_update_ts_constraints(
            mutations,
            primary,
            start_ts.into(),
            for_update_ts.into(),
            for_update_ts_constraints
                .into_iter()
                .map(|(size, ts)| (size, TimeStamp::from(ts))),
        );
        let cm = ConcurrencyManager::new(start_ts.into());
        prewrite_command(engine, cm, statistics, cmd)
    }

    pub fn commit<E: Engine>(
        engine: &mut E,
        statistics: &mut Statistics,
        keys: Vec<Key>,
        lock_ts: u64,
        commit_ts: u64,
    ) -> Result<()> {
        let ctx = Context::default();
        let snap = engine.snapshot(Default::default())?;
        let concurrency_manager = ConcurrencyManager::new(lock_ts.into());
        let cmd = Commit::new(
            keys,
            TimeStamp::from(lock_ts),
            TimeStamp::from(commit_ts),
            ctx,
        );

        let context = WriteContext {
            lock_mgr: &MockLockManager::new(),
            concurrency_manager,
            extra_op: ExtraOp::Noop,
            statistics,
            async_apply_prewrite: false,
            raw_ext: None,
            txn_status_cache: &TxnStatusCache::new_for_test(),
        };

        let ret = cmd.cmd.process_write(snap, context)?;
        let ctx = Context::default();
        engine.write(&ctx, ret.to_be_write).unwrap();
        Ok(())
    }

    pub fn rollback<E: Engine>(
        engine: &mut E,
        statistics: &mut Statistics,
        keys: Vec<Key>,
        start_ts: u64,
    ) -> Result<()> {
        let ctx = Context::default();
        let snap = engine.snapshot(Default::default())?;
        let concurrency_manager = ConcurrencyManager::new(start_ts.into());
        let cmd = Rollback::new(keys, TimeStamp::from(start_ts), ctx);
        let context = WriteContext {
            lock_mgr: &MockLockManager::new(),
            concurrency_manager,
            extra_op: ExtraOp::Noop,
            statistics,
            async_apply_prewrite: false,
            raw_ext: None,
            txn_status_cache: &TxnStatusCache::new_for_test(),
        };

        let ret = cmd.cmd.process_write(snap, context)?;
        let ctx = Context::default();
        engine.write(&ctx, ret.to_be_write).unwrap();
        Ok(())
    }

    pub fn gen_ts_provider(api_version: ApiVersion) -> Option<Arc<CausalTsProviderImpl>> {
        if api_version == ApiVersion::V2 {
            let test_provider: causal_ts::CausalTsProviderImpl =
                causal_ts::tests::TestProvider::default().into();
            Some(Arc::new(test_provider))
        } else {
            None
        }
    }
}
