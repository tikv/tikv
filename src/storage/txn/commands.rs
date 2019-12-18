// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Debug, Display, Formatter};

use kvproto::kvrpcpb::{CommandPri, Context, GetRequest, RawGetRequest};
use tikv_util::collections::HashMap;
use txn_types::{Key, Lock, Mutation, TimeStamp};

use crate::storage::lock_manager::WaitTimeout;
use crate::storage::metrics::{self, CommandPriority};

/// Get a single value.
pub struct PointGetCommand {
    pub ctx: Context,
    pub key: Key,
    /// None if this is a raw get, Some if this is a transactional get.
    pub ts: Option<TimeStamp>,
}

impl PointGetCommand {
    pub fn from_get(request: &mut GetRequest) -> Self {
        PointGetCommand {
            ctx: request.take_context(),
            key: Key::from_raw(request.get_key()),
            ts: Some(request.get_version().into()),
        }
    }

    pub fn from_raw_get(request: &mut RawGetRequest) -> Self {
        PointGetCommand {
            ctx: request.take_context(),
            key: Key::from_raw(request.get_key()),
            ts: None,
        }
    }

    #[cfg(test)]
    pub fn from_key_ts(key: Key, ts: Option<TimeStamp>) -> Self {
        PointGetCommand {
            ctx: Context::default(),
            key,
            ts,
        }
    }
}

/// Store Transaction scheduler commands.
///
/// Learn more about our transaction system at
/// [Deep Dive TiKV: Distributed Transactions](https://tikv.org/deep-dive/distributed-transaction/)
///
/// These are typically scheduled and used through the [`Storage`](Storage) with functions like
/// [`Storage::async_prewrite`](Storage::async_prewrite) trait and are executed asyncronously.
// Logic related to these can be found in the `src/storage/txn/proccess.rs::process_write_impl` function.
pub struct Command {
    pub ctx: Context,
    pub kind: CommandKind,
}

/// The prewrite phase of a transaction. The first phase of 2PC.
///
/// This prepares the system to commit the transaction. Later a [`Commit`](CommandKind::Commit)
/// or a [`Rollback`](CommandKind::Rollback) should follow.
pub struct Prewrite {
    /// The set of mutations to apply.
    pub mutations: Vec<Mutation>,
    /// The primary lock. Secondary locks (from `mutations`) will refer to the primary lock.
    pub primary: Vec<u8>,
    /// The transaction timestamp.
    pub start_ts: TimeStamp,
    pub lock_ttl: u64,
    pub skip_constraint_check: bool,
    /// How many keys this transaction involved.
    pub txn_size: u64,
    pub min_commit_ts: TimeStamp,
}

impl Prewrite {
    pub fn new(
        mutations: Vec<Mutation>,
        primary: Vec<u8>,
        start_ts: TimeStamp,
        lock_ttl: u64,
        skip_constraint_check: bool,
        txn_size: u64,
        min_commit_ts: TimeStamp,
        ctx: Context,
    ) -> Command {
        Command {
            ctx,
            kind: CommandKind::Prewrite(Prewrite {
                mutations,
                primary,
                start_ts,
                lock_ttl,
                skip_constraint_check,
                txn_size,
                min_commit_ts,
            }),
        }
    }
}

/// The prewrite phase of a transaction using pessimistic locking. The first phase of 2PC.
///
/// This prepares the system to commit the transaction. Later a [`Commit`](CommandKind::Commit)
/// or a [`Rollback`](CommandKind::Rollback) should follow.
pub struct PrewritePessimistic {
    /// The set of mutations to apply; the bool = is pessimistic lock.
    pub mutations: Vec<(Mutation, bool)>,
    /// The primary lock. Secondary locks (from `mutations`) will refer to the primary lock.
    pub primary: Vec<u8>,
    /// The transaction timestamp.
    pub start_ts: TimeStamp,
    pub lock_ttl: u64,
    pub for_update_ts: TimeStamp,
    /// How many keys this transaction involved.
    pub txn_size: u64,
    pub min_commit_ts: TimeStamp,
}

impl PrewritePessimistic {
    pub fn new(
        mutations: Vec<(Mutation, bool)>,
        primary: Vec<u8>,
        start_ts: TimeStamp,
        lock_ttl: u64,
        for_update_ts: TimeStamp,
        txn_size: u64,
        min_commit_ts: TimeStamp,
        ctx: Context,
    ) -> Command {
        debug_assert!(for_update_ts > TimeStamp::zero());
        Command {
            ctx,
            kind: CommandKind::PrewritePessimistic(PrewritePessimistic {
                mutations,
                primary,
                start_ts,
                lock_ttl,
                for_update_ts,
                txn_size,
                min_commit_ts,
            }),
        }
    }
}

/// Acquire a Pessimistic lock on the keys.
///
/// This can be rolled back with a [`PessimisticRollback`](CommandKind::PessimisticRollback) command.
pub struct AcquirePessimisticLock {
    /// The set of keys to lock.
    pub keys: Vec<(Key, bool)>,
    /// The primary lock. Secondary locks (from `keys`) will refer to the primary lock.
    pub primary: Vec<u8>,
    /// The transaction timestamp.
    pub start_ts: TimeStamp,
    pub lock_ttl: u64,
    pub is_first_lock: bool,
    pub for_update_ts: TimeStamp,
    /// Time to wait for lock released in milliseconds when encountering locks.
    pub wait_timeout: Option<WaitTimeout>,
}

impl AcquirePessimisticLock {
    pub fn new(
        keys: Vec<(Key, bool)>,
        primary: Vec<u8>,
        start_ts: TimeStamp,
        lock_ttl: u64,
        is_first_lock: bool,
        for_update_ts: TimeStamp,
        wait_timeout: Option<WaitTimeout>,
        ctx: Context,
    ) -> Command {
        Command {
            ctx,
            kind: CommandKind::AcquirePessimisticLock(AcquirePessimisticLock {
                keys,
                primary,
                start_ts,
                lock_ttl,
                is_first_lock,
                for_update_ts,
                wait_timeout,
            }),
        }
    }
}

/// Commit the transaction that started at `lock_ts`.
///
/// This should be following a [`Prewrite`](CommandKind::Prewrite).
pub struct Commit {
    /// The keys affected.
    pub keys: Vec<Key>,
    /// The lock timestamp.
    pub lock_ts: TimeStamp,
    /// The commit timestamp.
    pub commit_ts: TimeStamp,
}

impl Commit {
    pub fn new(keys: Vec<Key>, lock_ts: TimeStamp, commit_ts: TimeStamp, ctx: Context) -> Command {
        Command {
            ctx,
            kind: CommandKind::Commit(Commit {
                keys,
                lock_ts,
                commit_ts,
            }),
        }
    }
}

/// Rollback mutations on a single key.
///
/// This should be following a [`Prewrite`](CommandKind::Prewrite) on the given key.
pub struct Cleanup {
    pub key: Key,
    /// The transaction timestamp.
    pub start_ts: TimeStamp,
    /// The approximate current ts when cleanup request is invoked, which is used to check the
    /// lock's TTL. 0 means do not check TTL.
    pub current_ts: TimeStamp,
}

impl Cleanup {
    pub fn new(key: Key, start_ts: TimeStamp, current_ts: TimeStamp, ctx: Context) -> Command {
        Command {
            ctx,
            kind: CommandKind::Cleanup(Cleanup {
                key,
                start_ts,
                current_ts,
            }),
        }
    }
}

/// Rollback from the transaction that was started at `start_ts`.
///
/// This should be following a [`Prewrite`](CommandKind::Prewrite) on the given key.
pub struct Rollback {
    pub keys: Vec<Key>,
    /// The transaction timestamp.
    pub start_ts: TimeStamp,
}

impl Rollback {
    pub fn new(keys: Vec<Key>, start_ts: TimeStamp, ctx: Context) -> Command {
        Command {
            ctx,
            kind: CommandKind::Rollback(Rollback { keys, start_ts }),
        }
    }
}

/// Rollback pessimistic locks identified by `start_ts` and `for_update_ts`.
///
/// This can roll back an [`AcquirePessimisticLock`](CommandKind::AcquirePessimisticLock) command.
pub struct PessimisticRollback {
    /// The keys to be rolled back.
    pub keys: Vec<Key>,
    /// The transaction timestamp.
    pub start_ts: TimeStamp,
    pub for_update_ts: TimeStamp,
}

impl PessimisticRollback {
    pub fn new(
        keys: Vec<Key>,
        start_ts: TimeStamp,
        for_update_ts: TimeStamp,
        ctx: Context,
    ) -> Command {
        Command {
            ctx,
            kind: CommandKind::PessimisticRollback(PessimisticRollback {
                keys,
                start_ts,
                for_update_ts,
            }),
        }
    }
}

/// Heart beat of a transaction. It enlarges the primary lock's TTL.
///
/// This is invoked on a transaction's primary lock. The lock may be generated by either
/// [`AcquirePessimisticLock`](CommandKind::AcquirePessimisticLock) or
/// [`Prewrite`](CommandKind::Prewrite).
pub struct TxnHeartBeat {
    /// The primary key of the transaction.
    pub primary_key: Key,
    /// The transaction's start_ts.
    pub start_ts: TimeStamp,
    /// The new TTL that will be used to update the lock's TTL. If the lock's TTL is already
    /// greater than `advise_ttl`, nothing will happen.
    pub advise_ttl: u64,
}

impl TxnHeartBeat {
    pub fn new(primary_key: Key, start_ts: TimeStamp, advise_ttl: u64, ctx: Context) -> Command {
        Command {
            ctx,
            kind: CommandKind::TxnHeartBeat(TxnHeartBeat {
                primary_key,
                start_ts,
                advise_ttl,
            }),
        }
    }
}

/// Check the status of a transaction. This is usually invoked by a transaction that meets
/// another transaction's lock. If the primary lock is expired, it will rollback the primary
/// lock. If the primary lock exists but is not expired, it may update the transaction's
/// `min_commit_ts`. Returns a [`TxnStatus`](TxnStatus) to represent the status.
///
/// This is invoked on a transaction's primary lock. The lock may be generated by either
/// [`AcquirePessimisticLock`](CommandKind::AcquirePessimisticLock) or
/// [`Prewrite`](CommandKind::Prewrite).
pub struct CheckTxnStatus {
    /// The primary key of the transaction.
    pub primary_key: Key,
    /// The lock's ts, namely the transaction's start_ts.
    pub lock_ts: TimeStamp,
    /// The start_ts of the transaction that invokes this command.
    pub caller_start_ts: TimeStamp,
    /// The approximate current_ts when the command is invoked.
    pub current_ts: TimeStamp,
    /// Specifies the behavior when neither commit/rollback record nor lock is found. If true,
    /// rollbacks that transaction; otherwise returns an error.
    pub rollback_if_not_exist: bool,
}

impl CheckTxnStatus {
    pub fn new(
        primary_key: Key,
        lock_ts: TimeStamp,
        caller_start_ts: TimeStamp,
        current_ts: TimeStamp,
        rollback_if_not_exist: bool,
        ctx: Context,
    ) -> Command {
        Command {
            ctx,
            kind: CommandKind::CheckTxnStatus(CheckTxnStatus {
                primary_key,
                lock_ts,
                caller_start_ts,
                current_ts,
                rollback_if_not_exist,
            }),
        }
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
    pub fn new(max_ts: TimeStamp, start_key: Option<Key>, limit: usize, ctx: Context) -> Command {
        Command {
            ctx,
            kind: CommandKind::ScanLock(ScanLock {
                max_ts,
                start_key,
                limit,
            }),
        }
    }
}

/// Resolve locks according to `txn_status`.
///
/// During the GC operation, this should be called to clean up stale locks whose timestamp is
/// before safe point.
pub struct ResolveLock {
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
    pub txn_status: HashMap<TimeStamp, TimeStamp>,
    pub scan_key: Option<Key>,
    pub key_locks: Vec<(Key, Lock)>,
}

impl ResolveLock {
    pub fn new(
        txn_status: HashMap<TimeStamp, TimeStamp>,
        scan_key: Option<Key>,
        key_locks: Vec<(Key, Lock)>,
        ctx: Context,
    ) -> Command {
        Command {
            ctx,
            kind: CommandKind::ResolveLock(ResolveLock {
                txn_status,
                scan_key,
                key_locks,
            }),
        }
    }
}

/// Resolve locks on `resolve_keys` according to `start_ts` and `commit_ts`.
pub struct ResolveLockLite {
    /// The transaction timestamp.
    pub start_ts: TimeStamp,
    /// The transaction commit timestamp.
    pub commit_ts: TimeStamp,
    /// The keys to resolve.
    pub resolve_keys: Vec<Key>,
}

impl ResolveLockLite {
    pub fn new(
        start_ts: TimeStamp,
        commit_ts: TimeStamp,
        resolve_keys: Vec<Key>,
        ctx: Context,
    ) -> Command {
        Command {
            ctx,
            kind: CommandKind::ResolveLockLite(ResolveLockLite {
                start_ts,
                commit_ts,
                resolve_keys,
            }),
        }
    }
}

/// Delete all keys in the range [`start_key`, `end_key`).
///
/// **This is an unsafe action.**
///
/// All keys in the range will be deleted permanently regardless of their timestamps.
/// This means that deleted keys will not be retrievable by specifying an older timestamp.
pub struct DeleteRange {
    /// The inclusive start key.
    pub start_key: Key,
    /// The exclusive end key.
    pub end_key: Key,
}

impl DeleteRange {
    pub fn new(start_key: Key, end_key: Key, ctx: Context) -> Command {
        Command {
            ctx,
            kind: CommandKind::DeleteRange(DeleteRange { start_key, end_key }),
        }
    }
}

/// **Testing functionality:** Latch the given keys for given duration.
///
/// This means other write operations that involve these keys will be blocked.
pub struct Pause {
    /// The keys to hold latches on.
    pub keys: Vec<Key>,
    /// The amount of time in milliseconds to latch for.
    pub duration: u64,
}

impl Pause {
    pub fn new(keys: Vec<Key>, duration: u64, ctx: Context) -> Command {
        Command {
            ctx,
            kind: CommandKind::Pause(Pause { keys, duration }),
        }
    }
}

/// Retrieve MVCC information for the given key.
pub struct MvccByKey {
    pub key: Key,
}

impl MvccByKey {
    pub fn new(key: Key, ctx: Context) -> Command {
        Command {
            ctx,
            kind: CommandKind::MvccByKey(MvccByKey { key }),
        }
    }
}

/// Retrieve MVCC info for the first committed key which `start_ts == ts`.
pub struct MvccByStartTs {
    pub start_ts: TimeStamp,
}

impl MvccByStartTs {
    pub fn new(start_ts: TimeStamp, ctx: Context) -> Command {
        Command {
            ctx,
            kind: CommandKind::MvccByStartTs(MvccByStartTs { start_ts }),
        }
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
    DeleteRange(DeleteRange),
    Pause(Pause),
    MvccByKey(MvccByKey),
    MvccByStartTs(MvccByStartTs),
}

impl Command {
    pub fn readonly(&self) -> bool {
        match self.kind {
            CommandKind::ScanLock(_) |
            // DeleteRange only called by DDL bg thread after table is dropped and
            // must guarantee that there is no other read or write on these keys, so
            // we can treat DeleteRange as readonly Command.
            CommandKind::DeleteRange(_) |
            CommandKind::MvccByKey(_) |
            CommandKind::MvccByStartTs(_) => true,
            CommandKind::ResolveLock(ResolveLock { ref key_locks, .. }) => key_locks.is_empty(),
            _ => false,
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

    pub fn priority_tag(&self) -> CommandPriority {
        get_priority_tag(self.ctx.get_priority())
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
            CommandKind::DeleteRange(_) => metrics::CommandKind::delete_range,
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
            CommandKind::ResolveLock(_)
            | CommandKind::DeleteRange(_)
            | CommandKind::Pause(_)
            | CommandKind::MvccByKey(_) => TimeStamp::zero(),
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
            CommandKind::DeleteRange(DeleteRange {
                ref start_key,
                ref end_key,
            }) => write!(
                f,
                "kv::command::delete range [{:?}, {:?}) | {:?}",
                start_key, end_key, self.ctx,
            ),
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

pub fn get_priority_tag(priority: CommandPri) -> CommandPriority {
    match priority {
        CommandPri::Low => CommandPriority::low,
        CommandPri::Normal => CommandPriority::normal,
        CommandPri::High => CommandPriority::high,
    }
}
