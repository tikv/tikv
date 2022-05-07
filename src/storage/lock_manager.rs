// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use txn_types::TimeStamp;

use crate::{
    server::lock_manager::{waiter_manager, waiter_manager::Callback},
    storage::{txn::ProcessResult, types::StorageCallback},
};

#[derive(Clone, Copy, PartialEq, Debug, Default)]
pub struct Lock {
    pub ts: TimeStamp,
    pub hash: u64,
}

/// DiagnosticContext is for diagnosing problems about locks
#[derive(Clone, Default)]
pub struct DiagnosticContext {
    /// The key we care about
    pub key: Vec<u8>,
    /// This tag is used for aggregate related kv requests (eg. generated from same statement)
    /// Currently it is the encoded SQL digest if the client is TiDB
    pub resource_group_tag: Vec<u8>,
}

/// Time to wait for lock released when encountering locks.
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum WaitTimeout {
    Default,
    Millis(u64),
}

impl WaitTimeout {
    pub fn into_duration_with_ceiling(self, ceiling: u64) -> Duration {
        match self {
            WaitTimeout::Default => Duration::from_millis(ceiling),
            WaitTimeout::Millis(ms) if ms > ceiling => Duration::from_millis(ceiling),
            WaitTimeout::Millis(ms) => Duration::from_millis(ms),
        }
    }

    /// Timeouts are encoded as i64s in protobufs where 0 means using default timeout.
    /// Negative means no wait.
    pub fn from_encoded(i: i64) -> Option<WaitTimeout> {
        use std::cmp::Ordering::*;

        match i.cmp(&0) {
            Equal => Some(WaitTimeout::Default),
            Less => None,
            Greater => Some(WaitTimeout::Millis(i as u64)),
        }
    }
}

impl From<u64> for WaitTimeout {
    fn from(i: u64) -> WaitTimeout {
        WaitTimeout::Millis(i)
    }
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
        timeout: Option<WaitTimeout>,
        diag_ctx: DiagnosticContext,
    );

    /// The locks with `lock_ts` and `hashes` are released, tries to wake up transactions.
    fn wake_up(
        &self,
        lock_ts: TimeStamp,
        hashes: Vec<u64>,
        commit_ts: TimeStamp,
        is_pessimistic_txn: bool,
    );

    /// Returns true if there are waiters in the `LockManager`.
    ///
    /// This function is used to avoid useless calculation and wake-up.
    fn has_waiter(&self) -> bool {
        true
    }

    fn dump_wait_for_entries(&self, cb: waiter_manager::Callback);
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
        _wait_timeout: Option<WaitTimeout>,
        _diag_ctx: DiagnosticContext,
    ) {
    }

    fn wake_up(
        &self,
        _lock_ts: TimeStamp,
        _hashes: Vec<u64>,
        _commit_ts: TimeStamp,
        _is_pessimistic_txn: bool,
    ) {
    }

    fn dump_wait_for_entries(&self, cb: Callback) {
        cb(vec![])
    }
}
