// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

//! Core data types.

use crate::storage::{
    mvcc::{Lock, TimeStamp, Write},
    txn::ProcessResult,
    Callback, Result,
};
use keys::{Key, Value};
use kvproto::kvrpcpb::LockInfo;
use std::fmt::Debug;

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

pub enum StorageCallback {
    Boolean(Callback<()>),
    Booleans(Callback<Vec<Result<()>>>),
    MvccInfoByKey(Callback<MvccInfo>),
    MvccInfoByStartTs(Callback<Option<(Key, MvccInfo)>>),
    Locks(Callback<Vec<LockInfo>>),
    TxnStatus(Callback<TxnStatus>),
}

impl StorageCallback {
    /// Delivers the process result of a command to the storage callback.
    pub fn execute(self, pr: ProcessResult) {
        match self {
            StorageCallback::Boolean(cb) => match pr {
                ProcessResult::Res => cb(Ok(())),
                ProcessResult::Failed { err } => cb(Err(err)),
                _ => panic!("process result mismatch"),
            },
            StorageCallback::Booleans(cb) => match pr {
                ProcessResult::MultiRes { results } => cb(Ok(results)),
                ProcessResult::Failed { err } => cb(Err(err)),
                _ => panic!("process result mismatch"),
            },
            StorageCallback::MvccInfoByKey(cb) => match pr {
                ProcessResult::MvccKey { mvcc } => cb(Ok(mvcc)),
                ProcessResult::Failed { err } => cb(Err(err)),
                _ => panic!("process result mismatch"),
            },
            StorageCallback::MvccInfoByStartTs(cb) => match pr {
                ProcessResult::MvccStartTs { mvcc } => cb(Ok(mvcc)),
                ProcessResult::Failed { err } => cb(Err(err)),
                _ => panic!("process result mismatch"),
            },
            StorageCallback::Locks(cb) => match pr {
                ProcessResult::Locks { locks } => cb(Ok(locks)),
                ProcessResult::Failed { err } => cb(Err(err)),
                _ => panic!("process result mismatch"),
            },
            StorageCallback::TxnStatus(cb) => match pr {
                ProcessResult::TxnStatus { txn_status } => cb(Ok(txn_status)),
                ProcessResult::Failed { err } => cb(Err(err)),
                _ => panic!("process result mismatch"),
            },
        }
    }
}
