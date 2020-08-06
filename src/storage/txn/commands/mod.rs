// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! Commands used in the transaction system
#[macro_use]
mod macros;
mod acquire_pessimistic_lock;
mod check_secondary_locks;
mod check_txn_status;
mod cleanup;
mod commit;
mod mvcc_by_key;
mod mvcc_by_start_ts;
mod pause;
mod pessimistic_rollback;
mod prewrite;
mod prewrite_pessimistic;
mod resolve_lock_lite;
mod resolve_lock_scan;
mod rollback;
mod scan_lock;
mod txn_heart_beat;

pub use acquire_pessimistic_lock::AcquirePessimisticLock;
pub use check_secondary_locks::CheckSecondaryLocks;
pub use check_txn_status::CheckTxnStatus;
pub use cleanup::Cleanup;
pub use commit::Commit;
pub use mvcc_by_key::MvccByKey;
pub use mvcc_by_start_ts::MvccByStartTs;
pub use pause::Pause;
pub use pessimistic_rollback::PessimisticRollback;
pub use prewrite::Prewrite;
pub use prewrite_pessimistic::PrewritePessimistic;
pub use resolve_lock_lite::ResolveLockLite;
pub use resolve_lock_scan::ResolveLockScan;
pub use rollback::Rollback;
pub use scan_lock::ScanLock;
pub use txn_heart_beat::TxnHeartBeat;

#[cfg(test)]
pub(crate) use prewrite::FORWARD_MIN_MUTATIONS_NUM;

use std::fmt::{self, Debug, Display, Formatter};
use std::iter::{self, FromIterator};
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
use pd_client::PdClient;
use std::sync::Arc;
use tikv_util::collections::HashMap;

/// Store Transaction scheduler commands.
///
/// Learn more about our transaction system at
/// [Deep Dive TiKV: Distributed Transactions](https://tikv.org/docs/deep-dive/distributed-transaction/introduction/)
///
/// These are typically scheduled and used through the [`Storage`](Storage) with functions like
/// [`Storage::prewrite`](Storage::prewrite) trait and are executed asynchronously.
// Logic related to these can be found in the `src/storage/txn/proccess.rs::process_write_impl` function.
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
    ScanLock(ScanLock),
    ResolveLockScan(ResolveLockScan),
    ResolveLockLite(ResolveLockLite),
    Pause(Pause),
    MvccByKey(MvccByKey),
    MvccByStartTs(MvccByStartTs),
}

pub struct TypedCommand<T> {
    pub cmd: Command,
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
        if for_update_ts == 0 {
            Prewrite::new(
                req.take_mutations().into_iter().map(Into::into).collect(),
                req.take_primary_lock(),
                req.get_start_version().into(),
                req.get_lock_ttl(),
                req.get_skip_constraint_check(),
                req.get_txn_size(),
                req.get_min_commit_ts().into(),
                if req.get_use_async_commit() {
                    Some(req.get_secondaries().into())
                } else {
                    None
                },
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

impl From<ScanLockRequest> for TypedCommand<Vec<LockInfo>> {
    fn from(mut req: ScanLockRequest) -> Self {
        let start_key = if req.get_start_key().is_empty() {
            None
        } else {
            Some(Key::from_raw(req.get_start_key()))
        };

        ScanLock::new(
            req.get_max_version().into(),
            start_key,
            req.get_limit() as usize,
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
            HashMap::from_iter(iter::once((
                req.get_start_version().into(),
                req.get_commit_version().into(),
            )))
        } else {
            HashMap::from_iter(
                req.take_txn_infos()
                    .into_iter()
                    .map(|info| (info.txn.into(), info.status.into())),
            )
        };

        if resolve_keys.is_empty() {
            ResolveLockScan::new(txn_status, None, req.take_context())
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
struct ReleasedLocks {
    start_ts: TimeStamp,
    commit_ts: TimeStamp,
    hashes: Vec<u64>,
    pessimistic: bool,
}

pub(super) struct WriteResult {
    pub ctx: Context,
    pub to_be_write: WriteData,
    pub rows: usize,
    pub pr: ProcessResult,
    // (lock, is_first_lock, wait_timeout)
    pub lock_info: Option<(lock_manager::Lock, bool, Option<WaitTimeout>)>,
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

pub struct WriteContext<'a, L: LockManager, P: PdClient + 'static> {
    pub cid: u64,
    pub latches: &'a Latches,
    pub lock_mgr: &'a L,
    pub pd_client: Arc<P>,
    pub extra_op: ExtraOp,
    pub statistics: &'a mut Statistics,
    pub pipelined_pessimistic_lock: bool,
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
            Command::ScanLock(t) => t,
            Command::ResolveLockScan(t) => t,
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
            Command::ScanLock(t) => t,
            Command::ResolveLockScan(t) => t,
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
            Command::ScanLock(t) => t.process_read(snapshot, statistics),
            Command::MvccByKey(t) => t.process_read(snapshot, statistics),
            Command::MvccByStartTs(t) => t.process_read(snapshot, statistics),
            _ => panic!("unsupported read command"),
        }
    }

    pub(super) fn process_write<S: Snapshot, L: LockManager, P: PdClient + 'static>(
        self,
        snapshot: S,
        context: WriteContext<'_, L, P>,
    ) -> Result<WriteResult> {
        match self {
            Command::Prewrite(t) => t.process_write(snapshot, context),
            Command::PrewritePessimistic(t) => t.process_write(snapshot, context),
            Command::AcquirePessimisticLock(t) => t.process_write(snapshot, context),
            Command::Commit(t) => t.process_write(snapshot, context),
            Command::Cleanup(t) => t.process_write(snapshot, context),
            Command::Rollback(t) => t.process_write(snapshot, context),
            Command::PessimisticRollback(t) => t.process_write(snapshot, context),
            Command::ResolveLockScan(t) => t.process_write(snapshot, context),
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

pub trait ReadCommand<S: Snapshot>: CommandExt {
    fn process_read(self, snapshot: S, statistics: &mut Statistics) -> Result<ProcessResult>;
}

pub(super) trait WriteCommand<S: Snapshot, L: LockManager, P: PdClient + 'static>:
    CommandExt
{
    fn process_write(self, snapshot: S, context: WriteContext<'_, L, P>) -> Result<WriteResult>;
}
