// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
pub mod acquire_pessimistic_lock;
pub mod check_txn_status;
pub mod cleanup;
pub mod commit;
pub mod mvcc_by_key;
pub mod mvcc_by_start_ts;
pub mod pause;
pub mod pessimistic_rollback;
pub mod prewrite;
pub mod prewrite_pessimistic;
pub mod resolve_lock;
pub mod resolve_lock_lite;
pub mod rollback;
pub mod scan_lock;
pub mod txn_heart_beat;

use std::fmt::{self, Debug, Display, Formatter};
use std::iter::{self, FromIterator};
use std::marker::PhantomData;

use kvproto::kvrpcpb::*;
use txn_types::{Key, TimeStamp};

use crate::storage::lock_manager::WaitTimeout;
use crate::storage::metrics;
use crate::storage::txn::latch::{self, Latches};
use crate::storage::types::{MvccInfo, PessimisticLockRes, StorageCallbackType, TxnStatus};
use crate::storage::Result;
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
    Prewrite(prewrite::Prewrite),
    PrewritePessimistic(prewrite_pessimistic::PrewritePessimistic),
    AcquirePessimisticLock(acquire_pessimistic_lock::AcquirePessimisticLock),
    Commit(commit::Commit),
    Cleanup(cleanup::Cleanup),
    Rollback(rollback::Rollback),
    PessimisticRollback(pessimistic_rollback::PessimisticRollback),
    TxnHeartBeat(txn_heart_beat::TxnHeartBeat),
    CheckTxnStatus(check_txn_status::CheckTxnStatus),
    ResolveLock(resolve_lock::ResolveLock),
    ResolveLockLite(resolve_lock_lite::ResolveLockLite),
    ScanLock(scan_lock::ScanLock),
    Pause(pause::Pause),
    MvccByKey(mvcc_by_key::MvccByKey),
    MvccByStartTs(mvcc_by_start_ts::MvccByStartTs),
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
            prewrite::Prewrite::new(
                req.take_mutations().into_iter().map(Into::into).collect(),
                req.take_primary_lock(),
                req.get_start_version().into(),
                req.get_lock_ttl(),
                req.get_skip_constraint_check(),
                req.get_txn_size(),
                req.get_min_commit_ts().into(),
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
            prewrite_pessimistic::PrewritePessimistic::new(
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

        acquire_pessimistic_lock::AcquirePessimisticLock::new(
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

        commit::Commit::new(
            keys,
            req.get_start_version().into(),
            req.get_commit_version().into(),
            req.take_context(),
        )
    }
}

impl From<CleanupRequest> for TypedCommand<()> {
    fn from(mut req: CleanupRequest) -> Self {
        cleanup::Cleanup::new(
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
        rollback::Rollback::new(keys, req.get_start_version().into(), req.take_context())
    }
}

impl From<PessimisticRollbackRequest> for TypedCommand<Vec<Result<()>>> {
    fn from(mut req: PessimisticRollbackRequest) -> Self {
        let keys = req.get_keys().iter().map(|x| Key::from_raw(x)).collect();

        pessimistic_rollback::PessimisticRollback::new(
            keys,
            req.get_start_version().into(),
            req.get_for_update_ts().into(),
            req.take_context(),
        )
    }
}

impl From<TxnHeartBeatRequest> for TypedCommand<TxnStatus> {
    fn from(mut req: TxnHeartBeatRequest) -> Self {
        txn_heart_beat::TxnHeartBeat::new(
            Key::from_raw(req.get_primary_lock()),
            req.get_start_version().into(),
            req.get_advise_lock_ttl(),
            req.take_context(),
        )
    }
}

impl From<CheckTxnStatusRequest> for TypedCommand<TxnStatus> {
    fn from(mut req: CheckTxnStatusRequest) -> Self {
        check_txn_status::CheckTxnStatus::new(
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

        scan_lock::ScanLock::new(
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
            resolve_lock::ResolveLock::new(txn_status, None, vec![], req.take_context())
        } else {
            let start_ts: TimeStamp = req.get_start_version().into();
            assert!(!start_ts.is_zero());
            let commit_ts = req.get_commit_version().into();
            resolve_lock_lite::ResolveLockLite::new(
                start_ts,
                commit_ts,
                resolve_keys,
                req.take_context(),
            )
        }
    }
}

impl From<MvccGetByKeyRequest> for TypedCommand<MvccInfo> {
    fn from(mut req: MvccGetByKeyRequest) -> Self {
        mvcc_by_key::MvccByKey::new(Key::from_raw(req.get_key()), req.take_context())
    }
}

impl From<MvccGetByStartTsRequest> for TypedCommand<Option<(Key, MvccInfo)>> {
    fn from(mut req: MvccGetByStartTsRequest) -> Self {
        mvcc_by_start_ts::MvccByStartTs::new(req.get_start_ts().into(), req.take_context())
    }
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

    fn requires_pessimistic_txn(&self) -> bool {
        false
    }

    fn can_be_pipelined(&self) -> bool {
        false
    }

    fn write_bytes(&self) -> usize;

    fn gen_lock(&self, _latches: &Latches) -> latch::Lock;
}

#[macro_export]
macro_rules! ctx {
    () => {
        fn get_ctx(&self) -> &Context {
            &self.ctx
        }
        fn get_ctx_mut(&mut self) -> &mut Context {
            &mut self.ctx
        }
    };
}

#[macro_export]
macro_rules! command {
    (
        $(#[$outer_doc: meta])*
        $cmd: ident:
            cmd_ty => $cmd_ty: ty,
            display => $format_str: expr, ($($fields: ident$(.$sub_field:ident)?),*),
            content => {
                $($(#[$inner_doc:meta])* $arg: ident : $arg_ty: ty,)*
            }
    ) => {
        $(#[$outer_doc])*
        pub struct $cmd {
            pub ctx: Context,
            $($(#[$inner_doc])* pub $arg: $arg_ty,)*
        }

        impl $cmd {
            pub fn new(
                $($arg: $arg_ty,)*
                ctx: Context,
            ) -> TypedCommand<$cmd_ty> {
                Command::$cmd($cmd {
                        ctx,
                        $($arg,)*
                }).into()
            }
        }

        impl Display for $cmd {
            fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
                write!(
                    f,
                    $format_str,
                    $(
                        self.$fields$(.$sub_field())?,
                    )*
                )
            }
        }

        impl Debug for $cmd {
            fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self)
            }
        }
    }
}

#[macro_export]
macro_rules! ts {
    ($ts:ident) => {
        fn ts(&self) -> TimeStamp {
            self.$ts
        }
    };
}

#[macro_export]
macro_rules! tag {
    ($tag:ident) => {
        fn tag(&self) -> metrics::CommandKind {
            metrics::CommandKind::$tag
        }

        fn incr_cmd_metric(&self) {
            KV_COMMAND_COUNTER_VEC_STATIC.$tag.inc();
        }
    };
}

#[macro_export]
macro_rules! write_bytes {
    ($field: ident) => {
        fn write_bytes(&self) -> usize {
            self.$field.as_encoded().len()
        }
    };
    ($field: ident: multiple) => {
        fn write_bytes(&self) -> usize {
            self.$field.iter().map(|x| x.as_encoded().len()).sum()
        }
    };
}

#[macro_export]
macro_rules! gen_lock {
    (empty) => {
        fn gen_lock(&self, _latches: &Latches) -> latch::Lock {
            latch::Lock::new(vec![])
        }
    };
    ($field: ident) => {
        fn gen_lock(&self, latches: &Latches) -> latch::Lock {
            latches.gen_lock(iter::once(&self.$field))
        }
    };
    ($field: ident: multiple) => {
        fn gen_lock(&self, latches: &Latches) -> latch::Lock {
            latches.gen_lock(&self.$field)
        }
    };
    ($field: ident: multiple$transform: tt) => {
        fn gen_lock(&self, latches: &Latches) -> latch::Lock {
            #![allow(unused_parens)]
            let keys = self.$field.iter().map($transform);
            latches.gen_lock(keys)
        }
    };
}

#[macro_export]
macro_rules! command_method {
    ($name:ident, $return_ty: ty, $value: expr) => {
        fn $name(&self) -> $return_ty {
            $value
        }
    };
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
            Command::ScanLock(t) => t,
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
            Command::ScanLock(t) => t,
            Command::ResolveLock(t) => t,
            Command::ResolveLockLite(t) => t,
            Command::Pause(t) => t,
            Command::MvccByKey(t) => t,
            Command::MvccByStartTs(t) => t,
        }
    }

    pub fn readonly(&self) -> bool {
        self.command_ext().readonly()
    }

    pub fn incr_cmd_metric(&self) {
        self.command_ext().incr_cmd_metric()
    }

    pub fn priority(&self) -> CommandPri {
        self.command_ext().get_ctx().get_priority()
    }

    pub fn is_sys_cmd(&self) -> bool {
        self.command_ext().is_sys_cmd()
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

    pub fn requires_pessimistic_txn(&self) -> bool {
        self.command_ext().requires_pessimistic_txn()
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
