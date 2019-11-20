// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

//! Core data types.

use std::fmt::Debug;

use kvproto::kvrpcpb::LockInfo;

use crate::storage::{
    mvcc::{Lock, Write},
    Callback, Result,
};

pub use keys::{Key, KvPair, Value};

/// `MvccInfo` stores all mvcc information of given key.
/// Used by `MvccGetByKey` and `MvccGetByStartTs`.
#[derive(Debug, Default)]
pub struct MvccInfo {
    pub lock: Option<Lock>,
    /// commit_ts and write
    pub writes: Vec<(u64, Write)>,
    /// start_ts and value
    pub values: Vec<(u64, Value)>,
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
    Rollbacked,
    /// The txn is just rolled back due to expiration.
    TtlExpire,
    /// The txn is just rolled back due to lock not exist.
    LockNotExist,
    /// The txn haven't yet been committed.
    Uncommitted { lock_ttl: u64, min_commit_ts: u64 },
    /// The txn was committed.
    Committed { commit_ts: u64 },
}

impl TxnStatus {
    pub fn uncommitted(lock_ttl: u64, min_commit_ts: u64) -> Self {
        Self::Uncommitted {
            lock_ttl,
            min_commit_ts,
        }
    }

    pub fn committed(commit_ts: u64) -> Self {
        Self::Committed { commit_ts }
    }
}

pub enum StorageCb {
    Boolean(Callback<()>),
    Booleans(Callback<Vec<Result<()>>>),
    MvccInfoByKey(Callback<MvccInfo>),
    MvccInfoByStartTs(Callback<Option<(Key, MvccInfo)>>),
    Locks(Callback<Vec<LockInfo>>),
    TxnStatus(Callback<TxnStatus>),
}
