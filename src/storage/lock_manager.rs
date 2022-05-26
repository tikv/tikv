// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::server::lock_manager::waiter_manager;
use crate::server::lock_manager::waiter_manager::Callback;
use crate::storage::Error as StorageError;
use kvproto::kvrpcpb::LockInfo;
use kvproto::metapb::RegionEpoch;
use std::num::NonZeroU64;
use std::time::Duration;
use txn_types::{Key, TimeStamp};

#[derive(Clone, Copy, PartialEq, Debug, Default)]
pub struct LockDigest {
    pub ts: TimeStamp,
    pub hash: u64,
}

impl LockDigest {
    pub fn from_lock_info_pb(lock_info: &LockInfo) -> Self {
        Self {
            ts: lock_info.get_lock_version().into(),
            hash: Key::from_raw(lock_info.get_key()).gen_hash(),
        }
    }
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

#[derive(Debug, Clone)]
pub struct KeyLockWaitInfo {
    pub key: Key,
    pub lock_digest: LockDigest,
    pub lock_info: LockInfo,
}

#[derive(Clone, Copy, Hash, PartialEq, Eq, Debug)]
pub struct LockWaitToken(pub Option<u64>);

impl LockWaitToken {
    pub fn is_valid(&self) -> bool {
        self.0.is_some()
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
    #[must_use]
    fn wait_for(
        &self,
        region_id: u64,
        region_epoch: RegionEpoch,
        term: u64,
        start_ts: TimeStamp,
        wait_info: Vec<KeyLockWaitInfo>,
        is_first_lock: bool,
        timeout: Option<WaitTimeout>,
        cancel_callback: Box<dyn FnOnce(StorageError) + Send>,
        diag_ctx: DiagnosticContext,
    ) -> LockWaitToken;

    fn update_wait_for() {
        unimplemented!()
    }

    /// The locks with `lock_ts` and `hashes` are released, tries to wake up transactions.
    fn remove_lock_wait(&self, token: LockWaitToken);

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
        _region_id: u64,
        _region_epoch: RegionEpoch,
        _term: NonZeroU64,
        _start_ts: TimeStamp,
        _wait_info: Vec<KeyLockWaitInfo>,
        _is_first_lock: bool,
        _timeout: Option<WaitTimeout>,
        _cancel_callback: Box<dyn FnOnce(StorageError) + Send>,
        _diag_ctx: DiagnosticContext,
    ) -> LockWaitToken {
        LockWaitToken(None)
    }

    fn remove_lock_wait(&self, _token: LockWaitToken) {}

    fn dump_wait_for_entries(&self, cb: Callback) {
        cb(vec![])
    }
}
