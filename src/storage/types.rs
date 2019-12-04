// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

//! Core data types.

use crate::storage::{
    mvcc::{Lock, TimeStamp, Write},
    Callback, Command, Error as StorageError, Result,
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

/// A row mutation.
#[derive(Debug, Clone)]
pub enum Mutation {
    /// Put `Value` into `Key`, overwriting any existing value.
    Put((Key, Value)),
    /// Delete `Key`.
    Delete(Key),
    /// Set a lock on `Key`.
    Lock(Key),
    /// Put `Value` into `Key` if `Key` does not yet exist.
    ///
    /// Returns [`KeyError::AlreadyExists`](kvproto::kvrpcpb::KeyError::AlreadyExists) if the key already exists.
    Insert((Key, Value)),
}

impl Mutation {
    pub fn key(&self) -> &Key {
        match self {
            Mutation::Put((ref key, _)) => key,
            Mutation::Delete(ref key) => key,
            Mutation::Lock(ref key) => key,
            Mutation::Insert((ref key, _)) => key,
        }
    }

    pub fn into_key_value(self) -> (Key, Option<Value>) {
        match self {
            Mutation::Put((key, value)) => (key, Some(value)),
            Mutation::Delete(key) => (key, None),
            Mutation::Lock(key) => (key, None),
            Mutation::Insert((key, value)) => (key, Some(value)),
        }
    }

    pub fn is_insert(&self) -> bool {
        match self {
            Mutation::Insert(_) => true,
            _ => false,
        }
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

pub enum StorageCallback {
    Boolean(Callback<()>),
    Booleans(Callback<Vec<Result<()>>>),
    MvccInfoByKey(Callback<MvccInfo>),
    MvccInfoByStartTs(Callback<Option<(Key, MvccInfo)>>),
    Locks(Callback<Vec<LockInfo>>),
    TxnStatus(Callback<TxnStatus>),
}

/// Process result of a command.
pub enum ProcessResult {
    Res,
    MultiRes { results: Vec<Result<()>> },
    MvccKey { mvcc: MvccInfo },
    MvccStartTs { mvcc: Option<(Key, MvccInfo)> },
    Locks { locks: Vec<LockInfo> },
    TxnStatus { txn_status: TxnStatus },
    NextCommand { cmd: Command },
    Failed { err: StorageError },
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
