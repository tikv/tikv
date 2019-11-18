// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::{types::ProcessResult, StorageCallback, TimeStamp};

#[derive(Clone, Copy, PartialEq, Debug, Default)]
pub struct Lock {
    pub ts: TimeStamp,
    pub hash: u64,
}

/// `LockManager` manages transactions waiting for locks held by other transactions.
/// It has responsibility to handle deadlocks between transactions.
pub trait LockManager: Clone + Send + 'static {
    /// Transaction with `start_ts` waits for `lock` released.
    ///
    /// If the lock is released or waiting times out or deadlock occurs, the transaction
    /// should be waken up and call `cb` with `pr` to notify the caller.
    ///
    /// If the lock is the first lock the transaction waits for, it won't result in deadlock.
    fn wait_for(
        &self,
        start_ts: TimeStamp,
        cb: StorageCallback,
        pr: ProcessResult,
        lock: Lock,
        is_first_lock: bool,
        timeout: i64,
    );

    /// The locks with `lock_ts` and `hashes` are released, tries to wake up transactions.
    fn wake_up(
        &self,
        lock_ts: TimeStamp,
        hashes: Option<Vec<u64>>,
        commit_ts: TimeStamp,
        is_pessimistic_txn: bool,
    );

    /// Returns true if there are waiters in the `LockManager`.
    ///
    /// This function is used to avoid useless calculation and wake-up.
    fn has_waiter(&self) -> bool {
        true
    }
}

// For test
#[derive(Clone)]
pub struct DummyLockManager;

impl LockManager for DummyLockManager {
    fn wait_for(
        &self,
        _start_ts: TimeStamp,
        _cb: StorageCallback,
        _pr: ProcessResult,
        _lock: Lock,
        _is_first_lock: bool,
        _wait_timeout: i64,
    ) {
    }

    fn wake_up(
        &self,
        _lock_ts: TimeStamp,
        _hashes: Option<Vec<u64>>,
        _commit_ts: TimeStamp,
        _is_pessimistic_txn: bool,
    ) {
    }
}
