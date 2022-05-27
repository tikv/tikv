// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

//! Core data types.

use crate::storage::{
    mvcc::{Lock, LockType, TimeStamp, Write, WriteType},
    txn::ProcessResult,
    Callback, Error, Result,
};
use kvproto::kvrpcpb;
use std::fmt::Debug;
use std::sync::Arc;
use txn_types::{Key, Value};

/// `MvccInfo` stores all mvcc information of given key.
/// Used by `MvccGetByKey` and `MvccGetByStartTs`.
#[derive(Debug, Default)]
pub struct MvccInfo {
    pub lock: Option<Lock>,
    /// commit_ts and write
    pub writes: Vec<(TimeStamp, Write)>,
    /// start_ts and value
    pub values: Vec<(TimeStamp, Value)>,
}

impl MvccInfo {
    pub fn into_proto(self) -> kvrpcpb::MvccInfo {
        fn extract_2pc_values(res: Vec<(TimeStamp, Value)>) -> Vec<kvrpcpb::MvccValue> {
            res.into_iter()
                .map(|(start_ts, value)| {
                    let mut value_info = kvrpcpb::MvccValue::default();
                    value_info.set_start_ts(start_ts.into_inner());
                    value_info.set_value(value);
                    value_info
                })
                .collect()
        }

        fn extract_2pc_writes(res: Vec<(TimeStamp, Write)>) -> Vec<kvrpcpb::MvccWrite> {
            res.into_iter()
                .map(|(commit_ts, write)| {
                    let mut write_info = kvrpcpb::MvccWrite::default();
                    let op = match write.write_type {
                        WriteType::Put => kvrpcpb::Op::Put,
                        WriteType::Delete => kvrpcpb::Op::Del,
                        WriteType::Lock => kvrpcpb::Op::Lock,
                        WriteType::Rollback => kvrpcpb::Op::Rollback,
                    };
                    write_info.set_type(op);
                    write_info.set_start_ts(write.start_ts.into_inner());
                    write_info.set_commit_ts(commit_ts.into_inner());
                    write_info.set_short_value(write.short_value.unwrap_or_default());
                    write_info
                })
                .collect()
        }

        let mut mvcc_info = kvrpcpb::MvccInfo::default();
        if let Some(lock) = self.lock {
            let mut lock_info = kvrpcpb::MvccLock::default();
            let op = match lock.lock_type {
                LockType::Put => kvrpcpb::Op::Put,
                LockType::Delete => kvrpcpb::Op::Del,
                LockType::Lock => kvrpcpb::Op::Lock,
                LockType::Pessimistic => kvrpcpb::Op::PessimisticLock,
            };
            lock_info.set_type(op);
            lock_info.set_start_ts(lock.ts.into_inner());
            lock_info.set_primary(lock.primary);
            lock_info.set_short_value(lock.short_value.unwrap_or_default());
            mvcc_info.set_lock(lock_info);
        }
        let vv = extract_2pc_values(self.values);
        let vw = extract_2pc_writes(self.writes);
        mvcc_info.set_writes(vw.into());
        mvcc_info.set_values(vv.into());
        mvcc_info
    }
}

/// Represents the status of a transaction.
#[derive(PartialEq, Debug)]
pub enum TxnStatus {
    /// The txn was already rolled back before.
    RolledBack,
    /// The txn is just rolled back due to expiration.
    TtlExpire,
    /// The txn is just rolled back due to lock not exist.
    LockNotExist,
    /// The txn haven't yet been committed.
    Uncommitted {
        lock: Lock,
        min_commit_ts_pushed: bool,
    },
    /// The txn was committed.
    Committed { commit_ts: TimeStamp },
    /// The primary key is pessimistically rolled back.
    PessimisticRollBack,
    /// The txn primary key is not found and nothing is done.
    LockNotExistDoNothing,
}

impl TxnStatus {
    pub fn uncommitted(lock: Lock, min_commit_ts_pushed: bool) -> Self {
        Self::Uncommitted {
            lock,
            min_commit_ts_pushed,
        }
    }

    pub fn committed(commit_ts: TimeStamp) -> Self {
        Self::Committed { commit_ts }
    }
}

#[derive(Debug)]
pub struct PrewriteResult {
    pub locks: Vec<Result<()>>,
    pub min_commit_ts: TimeStamp,
    pub one_pc_commit_ts: TimeStamp,
}

#[derive(Clone, Debug)]
/// Represents the result of pessimistic lock on a single key.
pub enum PessimisticLockKeyResult {
    Empty,
    Value(Option<Value>),
    Existence(bool),
    LockedWithConflict {
        value: Option<Value>,
        conflict_ts: TimeStamp,
    },
    Waiting,
    Failed(Arc<Error>),
}

impl PessimisticLockKeyResult {
    pub fn new_success(
        need_value: bool,
        need_check_existence: bool,
        locked_with_conflict_ts: Option<TimeStamp>,
        value: Option<Value>,
    ) -> Self {
        if let Some(conflict_ts) = locked_with_conflict_ts {
            Self::LockedWithConflict { value, conflict_ts }
        } else if need_value {
            Self::Value(value)
        } else if need_check_existence {
            Self::Existence(value.is_some())
        } else {
            Self::Empty
        }
    }
}

#[derive(Clone, Debug)]
pub struct PessimisticLockResults(pub Vec<PessimisticLockKeyResult>);

impl PessimisticLockResults {
    pub fn new() -> Self {
        Self(vec![])
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self(Vec::with_capacity(capacity))
    }

    pub fn push(&mut self, key_res: PessimisticLockKeyResult) {
        self.0.push(key_res);
    }

    pub fn into_pb(self) -> (Vec<kvrpcpb::PessimisticLockKeyResult>, Option<Arc<Error>>) {
        let mut error = None;
        let res = self
            .0
            .into_iter()
            .map(|res| {
                let mut res_pb = kvrpcpb::PessimisticLockKeyResult::default();
                match res {
                    PessimisticLockKeyResult::Empty => {
                        res_pb.set_type(kvrpcpb::PessimisticLockKeyResultType::Empty)
                    }
                    PessimisticLockKeyResult::Value(v) => {
                        res_pb.set_type(kvrpcpb::PessimisticLockKeyResultType::Value);
                        res_pb.set_existence(v.is_some());
                        res_pb.set_value(v.unwrap_or_default());
                    }
                    PessimisticLockKeyResult::Existence(e) => {
                        res_pb.set_type(kvrpcpb::PessimisticLockKeyResultType::Existence);
                        res_pb.set_existence(e);
                    }
                    PessimisticLockKeyResult::LockedWithConflict { value, conflict_ts } => {
                        res_pb.set_type(kvrpcpb::PessimisticLockKeyResultType::LockedWithConflict);
                        res_pb.set_existence(value.is_some());
                        res_pb.set_value(value.unwrap_or_default());
                        res_pb.set_locked_with_conflict_ts(conflict_ts.into_inner());
                    }
                    PessimisticLockKeyResult::Waiting => unreachable!(),
                    PessimisticLockKeyResult::Failed(e) => {
                        if error.is_none() {
                            error = Some(e)
                        }
                        res_pb.set_type(kvrpcpb::PessimisticLockKeyResultType::Failed);
                    }
                }
                res_pb
            })
            .collect();
        (res, error)
    }
}

#[derive(Debug, PartialEq)]
pub enum SecondaryLocksStatus {
    Locked(Vec<kvrpcpb::LockInfo>),
    Committed(TimeStamp),
    RolledBack,
}

impl SecondaryLocksStatus {
    pub fn push(&mut self, lock: kvrpcpb::LockInfo) {
        match self {
            SecondaryLocksStatus::Locked(v) => v.push(lock),
            _ => panic!("unexpected SecondaryLocksStatus"),
        }
    }
}

macro_rules! storage_callback {
    ($($variant: ident ( $cb_ty: ty ) $result_variant: pat => $result: expr,)*) => {
        pub enum StorageCallback {
            $($variant(Callback<$cb_ty>),)*
        }

        impl StorageCallback {
            /// Delivers the process result of a command to the storage callback.
            pub fn execute(self, pr: ProcessResult) {
                match self {
                    $(StorageCallback::$variant(cb) => match pr {
                        $result_variant => cb(Ok($result)),
                        ProcessResult::Failed { err } => cb(Err(err)),
                        _ => panic!("process result mismatch"),
                    },)*
                }
            }
        }

        $(impl StorageCallbackType for $cb_ty {
            fn callback(cb: Callback<Self>) -> StorageCallback {
                StorageCallback::$variant(cb)
            }
        })*
    }
}

storage_callback! {
    Boolean(()) ProcessResult::Res => (),
    Booleans(Vec<Result<()>>) ProcessResult::MultiRes { results } => results,
    MvccInfoByKey(MvccInfo) ProcessResult::MvccKey { mvcc } => mvcc,
    MvccInfoByStartTs(Option<(Key, MvccInfo)>) ProcessResult::MvccStartTs { mvcc } => mvcc,
    Locks(Vec<kvrpcpb::LockInfo>) ProcessResult::Locks { locks } => locks,
    TxnStatus(TxnStatus) ProcessResult::TxnStatus { txn_status } => txn_status,
    Prewrite(PrewriteResult) ProcessResult::PrewriteResult { result } => result,
    PessimisticLock(Result<PessimisticLockResults>) ProcessResult::PessimisticLockRes { res } => res,
    SecondaryLocksStatus(SecondaryLocksStatus) ProcessResult::SecondaryLocksStatus { status } => status,
    RawCompareAndSwap((Option<Value>, bool)) ProcessResult::RawCompareAndSwapRes { previous_value, succeed } => (previous_value, succeed),
}

pub trait StorageCallbackType: Sized {
    fn callback(cb: Callback<Self>) -> StorageCallback;
}
