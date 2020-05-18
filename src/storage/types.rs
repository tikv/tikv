// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

//! Core data types.

use crate::storage::{
    mvcc::{Lock, LockType, TimeStamp, Write, WriteType},
    txn::ProcessResult,
    Callback, Result,
};
use kvproto::kvrpcpb;
use std::fmt::Debug;
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
        lock_ttl: u64,
        min_commit_ts: TimeStamp,
    },
    /// The txn was committed.
    Committed { commit_ts: TimeStamp },
}

impl TxnStatus {
    pub fn uncommitted(lock_ttl: u64, min_commit_ts: TimeStamp) -> Self {
        Self::Uncommitted {
            lock_ttl,
            min_commit_ts,
        }
    }

    pub fn committed(commit_ts: TimeStamp) -> Self {
        Self::Committed { commit_ts }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum PessimisticLockRes {
    Values(Vec<Option<Value>>),
    Empty,
}

impl PessimisticLockRes {
    pub fn push(&mut self, value: Option<Value>) {
        match self {
            PessimisticLockRes::Values(v) => v.push(value),
            _ => panic!("unexpected PessimisticLockRes"),
        }
    }

    pub fn into_vec(self) -> Vec<Value> {
        match self {
            PessimisticLockRes::Values(v) => v.into_iter().map(Option::unwrap_or_default).collect(),
            PessimisticLockRes::Empty => vec![],
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
    PessimisticLock(Result<PessimisticLockRes>) ProcessResult::PessimisticLockRes { res } => res,
}

pub trait StorageCallbackType: Sized {
    fn callback(cb: Callback<Self>) -> StorageCallback;
}
