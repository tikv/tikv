// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Debug, Display, Formatter};
use std::iter::{self, FromIterator};
use std::marker::PhantomData;

use kvproto::kvrpcpb::*;
use tikv_util::collections::HashMap;
use txn_types::{Key, Lock, Mutation, TimeStamp};

use crate::storage::lock_manager::WaitTimeout;
use crate::storage::metrics::{self, KV_COMMAND_COUNTER_VEC_STATIC};
use crate::storage::txn::latch::{self, Latches};
use crate::storage::types::{MvccInfo, PessimisticLockRes, StorageCallbackType, TxnStatus};
use crate::storage::Result;

/// Store Transaction scheduler commands.
///
/// Learn more about our transaction system at
/// [Deep Dive TiKV: Distributed Transactions](https://tikv.org/docs/deep-dive/distributed-transaction/introduction/)
///
/// These are typically scheduled and used through the [`Storage`](Storage) with functions like
/// [`Storage::prewrite`](Storage::prewrite) trait and are executed asynchronously.
// Logic related to these can be found in the `src/storage/txn/proccess.rs::process_write_impl` function.
pub struct Command {
    pub ctx: Context,
    pub kind: CommandKind,
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

impl From<PrewriteRequest> for TypedCommand<Vec<Result<()>>> {
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
                req.take_context(),
            )
            .into()
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
            .into()
        }
    }
}

impl From<PessimisticLockRequest> for TypedCommand<Result<PessimisticLockRes>> {
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

impl From<PessimisticRollbackRequest> for TypedCommand<Vec<Result<()>>> {
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
            ResolveLock::new(txn_status, None, vec![], req.take_context()).into()
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

macro_rules! command {
    (
        $(#[$outer_doc: meta])*
        $cmd: ident -> $cmd_ty: ty {
            $($(#[$inner_doc:meta])* $arg: ident : $arg_ty: ty,)*
        }
    ) => {
        $(#[$outer_doc])*
        pub struct $cmd {
            $($(#[$inner_doc])* pub $arg: $arg_ty,)*
        }

        impl $cmd {
            pub fn new(
                $($arg: $arg_ty,)*
                ctx: Context,
            ) -> TypedCommand<$cmd_ty> {
                Command {
                    ctx,
                    kind: CommandKind::$cmd($cmd {
                        $($arg,)*
                    }),
                }
                .into()
            }
        }
    }
}

command! {
    /// The prewrite phase of a transaction. The first phase of 2PC.
    ///
    /// This prepares the system to commit the transaction. Later a [`Commit`](CommandKind::Commit)
    /// or a [`Rollback`](CommandKind::Rollback) should follow.
    Prewrite -> Vec<Result<()>> {
        /// The set of mutations to apply.
        mutations: Vec<Mutation>,
        /// The primary lock. Secondary locks (from `mutations`) will refer to the primary lock.
        primary: Vec<u8>,
        /// The transaction timestamp.
        start_ts: TimeStamp,
        lock_ttl: u64,
        skip_constraint_check: bool,
        /// How many keys this transaction involved.
        txn_size: u64,
        min_commit_ts: TimeStamp,
    }
}

impl Prewrite {
    #[cfg(test)]
    pub fn with_defaults(
        mutations: Vec<Mutation>,
        primary: Vec<u8>,
        start_ts: TimeStamp,
    ) -> TypedCommand<Vec<Result<()>>> {
        Prewrite::new(
            mutations,
            primary,
            start_ts,
            0,
            false,
            0,
            TimeStamp::default(),
            Context::default(),
        )
    }

    #[cfg(test)]
    pub fn with_lock_ttl(
        mutations: Vec<Mutation>,
        primary: Vec<u8>,
        start_ts: TimeStamp,
        lock_ttl: u64,
    ) -> TypedCommand<Vec<Result<()>>> {
        Prewrite::new(
            mutations,
            primary,
            start_ts,
            lock_ttl,
            false,
            0,
            TimeStamp::default(),
            Context::default(),
        )
    }

    pub fn with_context(
        mutations: Vec<Mutation>,
        primary: Vec<u8>,
        start_ts: TimeStamp,
        ctx: Context,
    ) -> TypedCommand<Vec<Result<()>>> {
        Prewrite::new(
            mutations,
            primary,
            start_ts,
            0,
            false,
            0,
            TimeStamp::default(),
            ctx,
        )
    }
}

command! {
    /// The prewrite phase of a transaction using pessimistic locking. The first phase of 2PC.
    ///
    /// This prepares the system to commit the transaction. Later a [`Commit`](CommandKind::Commit)
    /// or a [`Rollback`](CommandKind::Rollback) should follow.
    PrewritePessimistic -> Vec<Result<()>> {
        /// The set of mutations to apply; the bool = is pessimistic lock.
        mutations: Vec<(Mutation, bool)>,
        /// The primary lock. Secondary locks (from `mutations`) will refer to the primary lock.
        primary: Vec<u8>,
        /// The transaction timestamp.
        start_ts: TimeStamp,
        lock_ttl: u64,
        for_update_ts: TimeStamp,
        /// How many keys this transaction involved.
        txn_size: u64,
        min_commit_ts: TimeStamp,
    }
}

command! {
    /// Acquire a Pessimistic lock on the keys.
    ///
    /// This can be rolled back with a [`PessimisticRollback`](CommandKind::PessimisticRollback) command.
    AcquirePessimisticLock -> Result<PessimisticLockRes> {
        /// The set of keys to lock.
        keys: Vec<(Key, bool)>,
        /// The primary lock. Secondary locks (from `keys`) will refer to the primary lock.
        primary: Vec<u8>,
        /// The transaction timestamp.
        start_ts: TimeStamp,
        lock_ttl: u64,
        is_first_lock: bool,
        for_update_ts: TimeStamp,
        /// Time to wait for lock released in milliseconds when encountering locks.
        wait_timeout: Option<WaitTimeout>,
        /// If it is true, TiKV will return values of the keys if no error, so TiDB can cache the values for
        /// later read in the same transaction.
        return_values: bool,
        min_commit_ts: TimeStamp,
    }
}

command! {
    /// Commit the transaction that started at `lock_ts`.
    ///
    /// This should be following a [`Prewrite`](CommandKind::Prewrite).
    Commit -> TxnStatus {
        /// The keys affected.
        keys: Vec<Key>,
        /// The lock timestamp.
        lock_ts: TimeStamp,
        /// The commit timestamp.
        commit_ts: TimeStamp,
    }
}

command! {
    /// Rollback mutations on a single key.
    ///
    /// This should be following a [`Prewrite`](CommandKind::Prewrite) on the given key.
    Cleanup -> () {
        key: Key,
        /// The transaction timestamp.
        start_ts: TimeStamp,
        /// The approximate current ts when cleanup request is invoked, which is used to check the
        /// lock's TTL. 0 means do not check TTL.
        current_ts: TimeStamp,
    }
}

command! {
    /// Rollback from the transaction that was started at `start_ts`.
    ///
    /// This should be following a [`Prewrite`](CommandKind::Prewrite) on the given key.
    Rollback -> () {
        keys: Vec<Key>,
        /// The transaction timestamp.
        start_ts: TimeStamp,
    }
}

command! {
    /// Rollback pessimistic locks identified by `start_ts` and `for_update_ts`.
    ///
    /// This can roll back an [`AcquirePessimisticLock`](CommandKind::AcquirePessimisticLock) command.
    PessimisticRollback -> Vec<Result<()>> {
        /// The keys to be rolled back.
        keys: Vec<Key>,
        /// The transaction timestamp.
        start_ts: TimeStamp,
        for_update_ts: TimeStamp,
    }
}

command! {
    /// Heart beat of a transaction. It enlarges the primary lock's TTL.
    ///
    /// This is invoked on a transaction's primary lock. The lock may be generated by either
    /// [`AcquirePessimisticLock`](CommandKind::AcquirePessimisticLock) or
    /// [`Prewrite`](CommandKind::Prewrite).
    TxnHeartBeat -> TxnStatus {
        /// The primary key of the transaction.
        primary_key: Key,
        /// The transaction's start_ts.
        start_ts: TimeStamp,
        /// The new TTL that will be used to update the lock's TTL. If the lock's TTL is already
        /// greater than `advise_ttl`, nothing will happen.
        advise_ttl: u64,
    }
}

command! {
    /// Check the status of a transaction. This is usually invoked by a transaction that meets
    /// another transaction's lock. If the primary lock is expired, it will rollback the primary
    /// lock. If the primary lock exists but is not expired, it may update the transaction's
    /// `min_commit_ts`. Returns a [`TxnStatus`](TxnStatus) to represent the status.
    ///
    /// This is invoked on a transaction's primary lock. The lock may be generated by either
    /// [`AcquirePessimisticLock`](CommandKind::AcquirePessimisticLock) or
    /// [`Prewrite`](CommandKind::Prewrite).
    CheckTxnStatus -> TxnStatus {
        /// The primary key of the transaction.
        primary_key: Key,
        /// The lock's ts, namely the transaction's start_ts.
        lock_ts: TimeStamp,
        /// The start_ts of the transaction that invokes this command.
        caller_start_ts: TimeStamp,
        /// The approximate current_ts when the command is invoked.
        current_ts: TimeStamp,
        /// Specifies the behavior when neither commit/rollback record nor lock is found. If true,
        /// rollbacks that transaction; otherwise returns an error.
        rollback_if_not_exist: bool,
    }
}

/// Scan locks from `start_key`, and find all locks whose timestamp is before `max_ts`.
pub struct ScanLock {
    /// The maximum transaction timestamp to scan.
    pub max_ts: TimeStamp,
    /// The key to start from. (`None` means start from the very beginning.)
    pub start_key: Option<Key>,
    /// The result limit.
    pub limit: usize,
}

impl ScanLock {
    pub fn new(
        max_ts: TimeStamp,
        start_key: Option<Key>,
        limit: usize,
        ctx: Context,
    ) -> TypedCommand<Vec<LockInfo>> {
        Command {
            ctx,
            kind: CommandKind::ScanLock(ScanLock {
                max_ts,
                start_key,
                limit,
            }),
        }
        .into()
    }
}

command! {
    /// Resolve locks according to `txn_status`.
    ///
    /// During the GC operation, this should be called to clean up stale locks whose timestamp is
    /// before safe point.
    ResolveLock -> () {
        /// Maps lock_ts to commit_ts. If a transaction was rolled back, it is mapped to 0.
        ///
        /// For example, let `txn_status` be `{ 100: 101, 102: 0 }`, then it means that the transaction
        /// whose start_ts is 100 was committed with commit_ts `101`, and the transaction whose
        /// start_ts is 102 was rolled back. If there are these keys in the db:
        ///
        /// * "k1", lock_ts = 100
        /// * "k2", lock_ts = 102
        /// * "k3", lock_ts = 104
        /// * "k4", no lock
        ///
        /// Here `"k1"`, `"k2"` and `"k3"` each has a not-yet-committed version, because they have
        /// locks. After calling resolve_lock, `"k1"` will be committed with commit_ts = 101 and `"k2"`
        /// will be rolled back.  `"k3"` will not be affected, because its lock_ts is not contained in
        /// `txn_status`. `"k4"` will not be affected either, because it doesn't have a non-committed
        /// version.
        txn_status: HashMap<TimeStamp, TimeStamp>,
        scan_key: Option<Key>,
        key_locks: Vec<(Key, Lock)>,
    }
}

command! {
    /// Resolve locks on `resolve_keys` according to `start_ts` and `commit_ts`.
    ResolveLockLite -> () {
        /// The transaction timestamp.
        start_ts: TimeStamp,
        /// The transaction commit timestamp.
        commit_ts: TimeStamp,
        /// The keys to resolve.
        resolve_keys: Vec<Key>,
    }
}

command! {
    /// **Testing functionality:** Latch the given keys for given duration.
    ///
    /// This means other write operations that involve these keys will be blocked.
    Pause -> () {
        /// The keys to hold latches on.
        keys: Vec<Key>,
        /// The amount of time in milliseconds to latch for.
        duration: u64,
    }
}

command! {
    /// Retrieve MVCC information for the given key.
    MvccByKey -> MvccInfo {
        key: Key,
    }
}

command! {
    /// Retrieve MVCC info for the first committed key which `start_ts == ts`.
    MvccByStartTs -> Option<(Key, MvccInfo)> {
        start_ts: TimeStamp,
    }
}

pub enum CommandKind {
    Prewrite(Prewrite),
    PrewritePessimistic(PrewritePessimistic),
    AcquirePessimisticLock(AcquirePessimisticLock),
    Commit(Commit),
    Cleanup(Cleanup),
    Rollback(Rollback),
    PessimisticRollback(PessimisticRollback),
    TxnHeartBeat(TxnHeartBeat),
    CheckTxnStatus(CheckTxnStatus),
    ScanLock(ScanLock),
    ResolveLock(ResolveLock),
    ResolveLockLite(ResolveLockLite),
    Pause(Pause),
    MvccByKey(MvccByKey),
    MvccByStartTs(MvccByStartTs),
}

impl Command {
    pub fn readonly(&self) -> bool {
        match self.kind {
            CommandKind::ScanLock(_)
            | CommandKind::MvccByKey(_)
            | CommandKind::MvccByStartTs(_) => true,
            CommandKind::ResolveLock(ResolveLock { ref key_locks, .. }) => key_locks.is_empty(),
            _ => false,
        }
    }

    pub fn incr_cmd_metric(&self) {
        match &self.kind {
            CommandKind::Prewrite(_) => KV_COMMAND_COUNTER_VEC_STATIC.prewrite.inc(),
            CommandKind::PrewritePessimistic(_) => KV_COMMAND_COUNTER_VEC_STATIC.prewrite.inc(),
            CommandKind::AcquirePessimisticLock(_) => {
                KV_COMMAND_COUNTER_VEC_STATIC.acquire_pessimistic_lock.inc()
            }
            CommandKind::Commit(_) => KV_COMMAND_COUNTER_VEC_STATIC.commit.inc(),
            CommandKind::Cleanup(_) => KV_COMMAND_COUNTER_VEC_STATIC.cleanup.inc(),
            CommandKind::Rollback(_) => KV_COMMAND_COUNTER_VEC_STATIC.rollback.inc(),
            CommandKind::PessimisticRollback(_) => {
                KV_COMMAND_COUNTER_VEC_STATIC.pessimistic_rollback.inc()
            }
            CommandKind::TxnHeartBeat(_) => KV_COMMAND_COUNTER_VEC_STATIC.txn_heart_beat.inc(),
            CommandKind::CheckTxnStatus(_) => KV_COMMAND_COUNTER_VEC_STATIC.check_txn_status.inc(),
            CommandKind::ScanLock(_) => KV_COMMAND_COUNTER_VEC_STATIC.scan_lock.inc(),
            CommandKind::ResolveLock(_) => KV_COMMAND_COUNTER_VEC_STATIC.resolve_lock.inc(),
            CommandKind::ResolveLockLite(_) => {
                KV_COMMAND_COUNTER_VEC_STATIC.resolve_lock_lite.inc()
            }
            CommandKind::Pause(_) => KV_COMMAND_COUNTER_VEC_STATIC.pause.inc(),
            CommandKind::MvccByKey(_) => KV_COMMAND_COUNTER_VEC_STATIC.key_mvcc.inc(),
            CommandKind::MvccByStartTs(_) => KV_COMMAND_COUNTER_VEC_STATIC.start_ts_mvcc.inc(),
        }
    }

    pub fn priority(&self) -> CommandPri {
        self.ctx.get_priority()
    }

    pub fn is_sys_cmd(&self) -> bool {
        match self.kind {
            CommandKind::ScanLock(_)
            | CommandKind::ResolveLock(_)
            | CommandKind::ResolveLockLite(_) => true,
            _ => false,
        }
    }

    pub fn need_flow_control(&self) -> bool {
        !self.readonly() && self.priority() != CommandPri::High
    }

    pub fn tag(&self) -> metrics::CommandKind {
        match self.kind {
            CommandKind::Prewrite(_) | CommandKind::PrewritePessimistic(_) => {
                metrics::CommandKind::prewrite
            }
            CommandKind::AcquirePessimisticLock(_) => {
                metrics::CommandKind::acquire_pessimistic_lock
            }
            CommandKind::Commit(_) => metrics::CommandKind::commit,
            CommandKind::Cleanup(_) => metrics::CommandKind::cleanup,
            CommandKind::Rollback(_) => metrics::CommandKind::rollback,
            CommandKind::PessimisticRollback(_) => metrics::CommandKind::pessimistic_rollback,
            CommandKind::TxnHeartBeat(_) => metrics::CommandKind::txn_heart_beat,
            CommandKind::CheckTxnStatus(_) => metrics::CommandKind::check_txn_status,
            CommandKind::ScanLock(_) => metrics::CommandKind::scan_lock,
            CommandKind::ResolveLock(_) => metrics::CommandKind::resolve_lock,
            CommandKind::ResolveLockLite(_) => metrics::CommandKind::resolve_lock_lite,
            CommandKind::Pause(_) => metrics::CommandKind::pause,
            CommandKind::MvccByKey(_) => metrics::CommandKind::key_mvcc,
            CommandKind::MvccByStartTs(_) => metrics::CommandKind::start_ts_mvcc,
        }
    }

    pub fn ts(&self) -> TimeStamp {
        match self.kind {
            CommandKind::Prewrite(Prewrite { start_ts, .. })
            | CommandKind::PrewritePessimistic(PrewritePessimistic { start_ts, .. })
            | CommandKind::AcquirePessimisticLock(AcquirePessimisticLock { start_ts, .. })
            | CommandKind::Cleanup(Cleanup { start_ts, .. })
            | CommandKind::Rollback(Rollback { start_ts, .. })
            | CommandKind::PessimisticRollback(PessimisticRollback { start_ts, .. })
            | CommandKind::TxnHeartBeat(TxnHeartBeat { start_ts, .. })
            | CommandKind::MvccByStartTs(MvccByStartTs { start_ts }) => start_ts,
            CommandKind::Commit(Commit { lock_ts, .. })
            | CommandKind::CheckTxnStatus(CheckTxnStatus { lock_ts, .. }) => lock_ts,
            CommandKind::ScanLock(ScanLock { max_ts, .. }) => max_ts,
            CommandKind::ResolveLockLite(ResolveLockLite { start_ts, .. }) => start_ts,
            CommandKind::ResolveLock(_) | CommandKind::Pause(_) | CommandKind::MvccByKey(_) => {
                TimeStamp::zero()
            }
        }
    }

    pub fn write_bytes(&self) -> usize {
        let mut bytes = 0;
        match self.kind {
            CommandKind::Prewrite(Prewrite { ref mutations, .. }) => {
                for m in mutations {
                    match *m {
                        Mutation::Put((ref key, ref value))
                        | Mutation::Insert((ref key, ref value)) => {
                            bytes += key.as_encoded().len();
                            bytes += value.len();
                        }
                        Mutation::Delete(ref key) | Mutation::Lock(ref key) => {
                            bytes += key.as_encoded().len();
                        }
                        Mutation::CheckNotExists(_) => (),
                    }
                }
            }
            CommandKind::PrewritePessimistic(PrewritePessimistic { ref mutations, .. }) => {
                for (m, _) in mutations {
                    match *m {
                        Mutation::Put((ref key, ref value))
                        | Mutation::Insert((ref key, ref value)) => {
                            bytes += key.as_encoded().len();
                            bytes += value.len();
                        }
                        Mutation::Delete(ref key) | Mutation::Lock(ref key) => {
                            bytes += key.as_encoded().len();
                        }
                        Mutation::CheckNotExists(_) => (),
                    }
                }
            }
            CommandKind::AcquirePessimisticLock(AcquirePessimisticLock { ref keys, .. }) => {
                for (key, _) in keys {
                    bytes += key.as_encoded().len();
                }
            }
            CommandKind::Commit(Commit { ref keys, .. })
            | CommandKind::Rollback(Rollback { ref keys, .. })
            | CommandKind::PessimisticRollback(PessimisticRollback { ref keys, .. })
            | CommandKind::Pause(Pause { ref keys, .. }) => {
                for key in keys {
                    bytes += key.as_encoded().len();
                }
            }
            CommandKind::ResolveLock(ResolveLock { ref key_locks, .. }) => {
                for lock in key_locks {
                    bytes += lock.0.as_encoded().len();
                }
            }
            CommandKind::ResolveLockLite(ResolveLockLite {
                ref resolve_keys, ..
            }) => {
                for k in resolve_keys {
                    bytes += k.as_encoded().len();
                }
            }
            CommandKind::Cleanup(Cleanup { ref key, .. }) => {
                bytes += key.as_encoded().len();
            }
            CommandKind::TxnHeartBeat(TxnHeartBeat {
                ref primary_key, ..
            }) => {
                bytes += primary_key.as_encoded().len();
            }
            CommandKind::CheckTxnStatus(CheckTxnStatus {
                ref primary_key, ..
            }) => {
                bytes += primary_key.as_encoded().len();
            }
            _ => {}
        }
        bytes
    }

    pub fn gen_lock(&self, latches: &Latches) -> latch::Lock {
        match &self.kind {
            CommandKind::Prewrite(Prewrite { mutations, .. }) => {
                let keys: Vec<&Key> = mutations.iter().map(|x| x.key()).collect();
                latches.gen_lock(&keys)
            }
            CommandKind::PrewritePessimistic(PrewritePessimistic { mutations, .. }) => {
                let keys: Vec<&Key> = mutations.iter().map(|(x, _)| x.key()).collect();
                latches.gen_lock(&keys)
            }
            CommandKind::ResolveLock(ResolveLock { key_locks, .. }) => {
                let keys: Vec<&Key> = key_locks.iter().map(|x| &x.0).collect();
                latches.gen_lock(&keys)
            }
            CommandKind::AcquirePessimisticLock(AcquirePessimisticLock { keys, .. }) => {
                let keys: Vec<&Key> = keys.iter().map(|x| &x.0).collect();
                latches.gen_lock(&keys)
            }
            CommandKind::ResolveLockLite(ResolveLockLite { resolve_keys, .. }) => {
                latches.gen_lock(resolve_keys)
            }
            CommandKind::Commit(Commit { keys, .. })
            | CommandKind::Rollback(Rollback { keys, .. })
            | CommandKind::PessimisticRollback(PessimisticRollback { keys, .. }) => {
                latches.gen_lock(keys)
            }
            CommandKind::Cleanup(Cleanup { key, .. }) => latches.gen_lock(&[key]),
            CommandKind::Pause(Pause { keys, .. }) => latches.gen_lock(keys),
            CommandKind::TxnHeartBeat(TxnHeartBeat { primary_key, .. }) => {
                latches.gen_lock(&[primary_key])
            }
            CommandKind::CheckTxnStatus(CheckTxnStatus { primary_key, .. }) => {
                latches.gen_lock(&[primary_key])
            }

            // Avoid using wildcard _ here to avoid forgetting add new commands here.
            CommandKind::ScanLock(_)
            | CommandKind::MvccByKey(_)
            | CommandKind::MvccByStartTs(_) => latch::Lock::new(vec![]),
        }
    }

    pub fn requires_pessimistic_txn(&self) -> bool {
        match &self.kind {
            CommandKind::PrewritePessimistic(_)
            | CommandKind::AcquirePessimisticLock(_)
            | CommandKind::PessimisticRollback(_) => true,
            _ => false,
        }
    }

    pub fn can_pipelined(&self) -> bool {
        match &self.kind {
            CommandKind::AcquirePessimisticLock(_) => true,
            _ => false,
        }
    }
}

impl Display for Command {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.kind {
            CommandKind::Prewrite(Prewrite {
                ref mutations,
                start_ts,
                ..
            }) => write!(
                f,
                "kv::command::prewrite mutations({}) @ {} | {:?}",
                mutations.len(),
                start_ts,
                self.ctx,
            ),
            CommandKind::PrewritePessimistic(PrewritePessimistic {
                ref mutations,
                start_ts,
                ..
            }) => write!(
                f,
                "kv::command::prewrite_pessimistic mutations({}) @ {} | {:?}",
                mutations.len(),
                start_ts,
                self.ctx,
            ),
            CommandKind::AcquirePessimisticLock(AcquirePessimisticLock {
                ref keys,
                start_ts,
                for_update_ts,
                ..
            }) => write!(
                f,
                "kv::command::acquirepessimisticlock keys({}) @ {} {} | {:?}",
                keys.len(),
                start_ts,
                for_update_ts,
                self.ctx,
            ),
            CommandKind::Commit(Commit {
                ref keys,
                lock_ts,
                commit_ts,
                ..
            }) => write!(
                f,
                "kv::command::commit {} {} -> {} | {:?}",
                keys.len(),
                lock_ts,
                commit_ts,
                self.ctx,
            ),
            CommandKind::Cleanup(Cleanup {
                ref key, start_ts, ..
            }) => write!(
                f,
                "kv::command::cleanup {} @ {} | {:?}",
                key, start_ts, self.ctx
            ),
            CommandKind::Rollback(Rollback {
                ref keys, start_ts, ..
            }) => write!(
                f,
                "kv::command::rollback keys({}) @ {} | {:?}",
                keys.len(),
                start_ts,
                self.ctx,
            ),
            CommandKind::PessimisticRollback(PessimisticRollback {
                ref keys,
                start_ts,
                for_update_ts,
            }) => write!(
                f,
                "kv::command::pessimistic_rollback keys({}) @ {} {} | {:?}",
                keys.len(),
                start_ts,
                for_update_ts,
                self.ctx,
            ),
            CommandKind::TxnHeartBeat(TxnHeartBeat {
                ref primary_key,
                start_ts,
                advise_ttl,
            }) => write!(
                f,
                "kv::command::txn_heart_beat {} @ {} ttl {} | {:?}",
                primary_key, start_ts, advise_ttl, self.ctx,
            ),
            CommandKind::CheckTxnStatus(CheckTxnStatus {
                ref primary_key,
                lock_ts,
                caller_start_ts,
                current_ts,
                ..
            }) => write!(
                f,
                "kv::command::check_txn_status {} @ {} curr({}, {}) | {:?}",
                primary_key, lock_ts, caller_start_ts, current_ts, self.ctx,
            ),
            CommandKind::ScanLock(ScanLock {
                max_ts,
                ref start_key,
                limit,
                ..
            }) => write!(
                f,
                "kv::scan_lock {:?} {} @ {} | {:?}",
                start_key, limit, max_ts, self.ctx,
            ),
            CommandKind::ResolveLock(_) => write!(f, "kv::resolve_lock"),
            CommandKind::ResolveLockLite(_) => write!(f, "kv::resolve_lock_lite"),
            CommandKind::Pause(Pause { ref keys, duration }) => write!(
                f,
                "kv::command::pause keys:({}) {} ms | {:?}",
                keys.len(),
                duration,
                self.ctx,
            ),
            CommandKind::MvccByKey(MvccByKey { ref key }) => {
                write!(f, "kv::command::mvccbykey {:?} | {:?}", key, self.ctx)
            }
            CommandKind::MvccByStartTs(MvccByStartTs { ref start_ts }) => write!(
                f,
                "kv::command::mvccbystartts {:?} | {:?}",
                start_ts, self.ctx
            ),
        }
    }
}

impl Debug for Command {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}
