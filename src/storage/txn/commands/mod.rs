// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! Commands used in the transaction system
#[macro_use]
mod macros;
pub(crate) mod acquire_pessimistic_lock;
pub(crate) mod check_secondary_locks;
pub(crate) mod check_txn_status;
pub(crate) mod cleanup;
pub(crate) mod commit;
pub(crate) mod mvcc_by_key;
pub(crate) mod mvcc_by_start_ts;
pub(crate) mod pause;
pub(crate) mod pessimistic_rollback;
pub(crate) mod prewrite;
pub(crate) mod resolve_lock;
pub(crate) mod resolve_lock_lite;
pub(crate) mod resolve_lock_readphase;
pub(crate) mod rollback;
pub(crate) mod txn_heart_beat;

pub use acquire_pessimistic_lock::AcquirePessimisticLock;
pub use check_secondary_locks::CheckSecondaryLocks;
pub use check_txn_status::CheckTxnStatus;
pub use cleanup::Cleanup;
pub use commit::Commit;
pub use mvcc_by_key::MvccByKey;
pub use mvcc_by_start_ts::MvccByStartTs;
pub use pause::Pause;
pub use pessimistic_rollback::PessimisticRollback;
pub use prewrite::{one_pc_commit_ts, Prewrite, PrewritePessimistic};
pub use resolve_lock::ResolveLock;
pub use resolve_lock_lite::ResolveLockLite;
pub use resolve_lock_readphase::ResolveLockReadPhase;
pub use rollback::Rollback;
pub use txn_heart_beat::TxnHeartBeat;

pub use resolve_lock::RESOLVE_LOCK_BATCH_SIZE;

use std::fmt::{self, Debug, Display, Formatter};
use std::iter;
use std::marker::PhantomData;

use kvproto::kvrpcpb::*;
use txn_types::{Key, TimeStamp, Value, Write};

use crate::storage::kv::WriteData;
use crate::storage::lock_manager::{self, LockManager, WaitTimeout};
use crate::storage::mvcc::{Lock as MvccLock, MvccReader, ReleasedLock};
use crate::storage::txn::latch::{self, Latches};
use crate::storage::txn::{ProcessResult, Result};
use crate::storage::types::{
    MvccInfo, PessimisticLockRes, PrewriteResult, SecondaryLocksStatus, StorageCallbackType,
    TxnStatus,
};
use crate::storage::{metrics, Result as StorageResult, Snapshot, Statistics};
use concurrency_manager::{ConcurrencyManager, KeyHandleGuard};

/// Store Transaction scheduler commands.
///
/// Learn more about our transaction system at
/// [Deep Dive TiKV: Distributed Transactions](https://tikv.org/docs/deep-dive/distributed-transaction/introduction/)
///
/// These are typically scheduled and used through the [`Storage`](crate::storage::Storage) with functions like
/// [`prewrite`](prewrite::Prewrite) trait and are executed asynchronously.
pub enum Command {
    Prewrite(Prewrite),
    PrewritePessimistic(PrewritePessimistic),
    AcquirePessimisticLock(AcquirePessimisticLock),
    Commit(Commit),
    Cleanup(Cleanup),
    Rollback(Rollback),
    PessimisticRollback(PessimisticRollback),
    TxnHeartBeat(TxnHeartBeat),
    CheckTxnStatus(CheckTxnStatus),
    CheckSecondaryLocks(CheckSecondaryLocks),
    ResolveLockReadPhase(ResolveLockReadPhase),
    ResolveLock(ResolveLock),
    ResolveLockLite(ResolveLockLite),
    Pause(Pause),
    MvccByKey(MvccByKey),
    MvccByStartTs(MvccByStartTs),
}

/// A `Command` with its return type, reified as the generic parameter `T`.
///
/// Incoming grpc requests (like `CommitRequest`, `PrewriteRequest`) are converted to
/// this type via a series of transformations. That process is described below using
/// `CommitRequest` as an example:
/// 1. A `CommitRequest` is handled by the `future_commit` method in kv.rs, where it
/// needs to be transformed to a `TypedCommand` before being passed to the
/// `storage.sched_txn_command` method.
/// 2. The `From<CommitRequest>` impl for `TypedCommand` gets chosen, and its generic
/// parameter indicates that the result type for this instance of `TypedCommand` is
/// going to be `TxnStatus` - one of the variants of the `StorageCallback` enum.
/// 3. In the above `from` method, the details of the commit request are captured by
/// creating an instance of the struct `storage::txn::commands::commit::Command`
/// via its `new` method.
/// 4. This struct is wrapped in a variant of the enum `storage::txn::commands::Command`.
/// This enum exists to facilitate generic operations over different commands.
/// 5. Finally, the `Command` enum variant for `Commit` is converted to the `TypedCommand`
/// using the `From<Command>` impl for `TypedCommand`.
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
                req.take_context(),
            )
        } else {
            let is_pessimistic_lock = req.take_is_pessimistic_lock();
            let mutations = req
                .take_mutations()
                .into_iter()
                .map(Into::into)
                .zip(is_pessimistic_lock.into_iter())
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
                req.take_context(),
            )
        }
    }
}

impl From<PessimisticLockRequest> for TypedCommand<StorageResult<PessimisticLockRes>> {
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
        let keys = req.get_keys().iter().map(|x| Key::from_raw(x)).collect();

        PessimisticRollback::new(
            keys,
            req.get_start_version().into(),
            req.get_for_update_ts().into(),
            req.take_context(),
        )
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

#[derive(Default)]
pub(super) struct ReleasedLocks {
    start_ts: TimeStamp,
    commit_ts: TimeStamp,
    hashes: Vec<u64>,
    pessimistic: bool,
}

/// Represents for a scheduler command, when should the response sent to the client.
/// For most cases, the response should be sent after the result being successfully applied to
/// the storage (if needed). But in some special cases, some optimizations allows the response to be
/// returned at an earlier phase.
///
/// Note that this doesn't affect latch releasing. The latch and the memory lock (if any) are always
/// released after applying, regardless of when the response is sent.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ResponsePolicy {
    /// Return the response to the client when the command has finished applying.
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
    // (lock, is_first_lock, wait_timeout)
    pub lock_info: Option<(lock_manager::Lock, bool, Option<WaitTimeout>)>,
    pub lock_guards: Vec<KeyHandleGuard>,
    pub response_policy: ResponsePolicy,
}

impl ReleasedLocks {
    pub fn new(start_ts: TimeStamp, commit_ts: TimeStamp) -> Self {
        Self {
            start_ts,
            commit_ts,
            ..Default::default()
        }
    }

    pub fn push(&mut self, lock: Option<ReleasedLock>) {
        if let Some(lock) = lock {
            self.hashes.push(lock.hash);
            if !self.pessimistic {
                self.pessimistic = lock.pessimistic;
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.hashes.is_empty()
    }

    // Wake up pessimistic transactions that waiting for these locks.
    pub fn wake_up<L: LockManager>(self, lock_mgr: &L) {
        lock_mgr.wake_up(self.start_ts, self.hashes, self.commit_ts, self.pessimistic);
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
                ts = commit_ts.prev();
                writes.push((commit_ts, write));
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

    fn get_ctx(&self) -> &Context;

    fn get_ctx_mut(&mut self) -> &mut Context;

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

    fn gen_lock(&self, _latches: &Latches) -> latch::Lock;
}

pub struct WriteContext<'a, L: LockManager> {
    pub lock_mgr: &'a L,
    pub concurrency_manager: ConcurrencyManager,
    pub extra_op: ExtraOp,
    pub statistics: &'a mut Statistics,
    pub async_apply_prewrite: bool,
}

impl Command {
    // These two are for backward compatibility, after some other refactors are done
    // we can remove Command totally and use `&dyn CommandExt` instead
    fn command_ext(&self) -> &dyn CommandExt {
        match &self {
            Command::Prewrite(t) => t,
            Command::PrewritePessimistic(t) => t,
            Command::AcquirePessimisticLock(t) => t,
            Command::Commit(t) => t,
            Command::Cleanup(t) => t,
            Command::Rollback(t) => t,
            Command::PessimisticRollback(t) => t,
            Command::TxnHeartBeat(t) => t,
            Command::CheckTxnStatus(t) => t,
            Command::CheckSecondaryLocks(t) => t,
            Command::ResolveLockReadPhase(t) => t,
            Command::ResolveLock(t) => t,
            Command::ResolveLockLite(t) => t,
            Command::Pause(t) => t,
            Command::MvccByKey(t) => t,
            Command::MvccByStartTs(t) => t,
        }
    }

    fn command_ext_mut(&mut self) -> &mut dyn CommandExt {
        match self {
            Command::Prewrite(t) => t,
            Command::PrewritePessimistic(t) => t,
            Command::AcquirePessimisticLock(t) => t,
            Command::Commit(t) => t,
            Command::Cleanup(t) => t,
            Command::Rollback(t) => t,
            Command::PessimisticRollback(t) => t,
            Command::TxnHeartBeat(t) => t,
            Command::CheckTxnStatus(t) => t,
            Command::CheckSecondaryLocks(t) => t,
            Command::ResolveLockReadPhase(t) => t,
            Command::ResolveLock(t) => t,
            Command::ResolveLockLite(t) => t,
            Command::Pause(t) => t,
            Command::MvccByKey(t) => t,
            Command::MvccByStartTs(t) => t,
        }
    }

    pub(super) fn process_read<S: Snapshot>(
        self,
        snapshot: S,
        statistics: &mut Statistics,
    ) -> Result<ProcessResult> {
        match self {
            Command::ResolveLockReadPhase(t) => t.process_read(snapshot, statistics),
            Command::MvccByKey(t) => t.process_read(snapshot, statistics),
            Command::MvccByStartTs(t) => t.process_read(snapshot, statistics),
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

    pub fn need_flow_control(&self) -> bool {
        !self.readonly() && self.priority() != CommandPri::High
    }

    pub fn tag(&self) -> metrics::CommandKind {
        self.command_ext().tag()
    }

    pub fn ts(&self) -> TimeStamp {
        self.command_ext().ts()
    }

    pub fn write_bytes(&self) -> usize {
        self.command_ext().write_bytes()
    }

    pub fn gen_lock(&self, latches: &Latches) -> latch::Lock {
        self.command_ext().gen_lock(latches)
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

/// Commands that do not need to modify the database during execution will implement this trait.
pub trait ReadCommand<S: Snapshot>: CommandExt {
    fn process_read(self, snapshot: S, statistics: &mut Statistics) -> Result<ProcessResult>;
}

/// Commands that need to modify the database during execution will implement this trait.
pub trait WriteCommand<S: Snapshot, L: LockManager>: CommandExt {
    fn process_write(self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult>;
}

#[cfg(test)]
pub mod test_util {
    use super::*;

    use crate::storage::mvcc::{Error as MvccError, ErrorInner as MvccErrorInner};
    use crate::storage::txn::{Error, ErrorInner, Result};
    use crate::storage::DummyLockManager;
    use crate::storage::Engine;
    use txn_types::Mutation;

    // Some utils for tests that may be used in multiple source code files.

    pub fn prewrite_command<E: Engine>(
        engine: &E,
        cm: ConcurrencyManager,
        statistics: &mut Statistics,
        cmd: TypedCommand<PrewriteResult>,
    ) -> Result<PrewriteResult> {
        let snap = engine.snapshot(Default::default())?;
        let context = WriteContext {
            lock_mgr: &DummyLockManager {},
            concurrency_manager: cm,
            extra_op: ExtraOp::Noop,
            statistics,
            async_apply_prewrite: false,
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
        engine.write(&ctx, ret.to_be_write).unwrap();
        Ok(res)
    }

    pub fn prewrite<E: Engine>(
        engine: &E,
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
        engine: &E,
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

    pub fn pessimsitic_prewrite<E: Engine>(
        engine: &E,
        statistics: &mut Statistics,
        mutations: Vec<(Mutation, bool)>,
        primary: Vec<u8>,
        start_ts: u64,
        for_update_ts: u64,
        one_pc_max_commit_ts: Option<u64>,
    ) -> Result<PrewriteResult> {
        let cm = ConcurrencyManager::new(start_ts.into());
        pessimsitic_prewrite_with_cm(
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

    pub fn pessimsitic_prewrite_with_cm<E: Engine>(
        engine: &E,
        cm: ConcurrencyManager,
        statistics: &mut Statistics,
        mutations: Vec<(Mutation, bool)>,
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

    pub fn commit<E: Engine>(
        engine: &E,
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
            lock_mgr: &DummyLockManager {},
            concurrency_manager,
            extra_op: ExtraOp::Noop,
            statistics,
            async_apply_prewrite: false,
        };

        let ret = cmd.cmd.process_write(snap, context)?;
        let ctx = Context::default();
        engine.write(&ctx, ret.to_be_write).unwrap();
        Ok(())
    }

    pub fn rollback<E: Engine>(
        engine: &E,
        statistics: &mut Statistics,
        keys: Vec<Key>,
        start_ts: u64,
    ) -> Result<()> {
        let ctx = Context::default();
        let snap = engine.snapshot(Default::default())?;
        let concurrency_manager = ConcurrencyManager::new(start_ts.into());
        let cmd = Rollback::new(keys, TimeStamp::from(start_ts), ctx);
        let context = WriteContext {
            lock_mgr: &DummyLockManager {},
            concurrency_manager,
            extra_op: ExtraOp::Noop,
            statistics,
            async_apply_prewrite: false,
        };

        let ret = cmd.cmd.process_write(snap, context)?;
        let ctx = Context::default();
        engine.write(&ctx, ret.to_be_write).unwrap();
        Ok(())
    }
}
