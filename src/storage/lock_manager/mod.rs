// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{Debug, Formatter},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use collections::{HashMap, HashSet};
use kvproto::{kvrpcpb::LockInfo, metapb::RegionEpoch};
use parking_lot::Mutex;
use tracker::TrackerToken;
use txn_types::{Key, TimeStamp};

pub use crate::storage::lock_manager::lock_wait_context::CancellationCallback;
use crate::{
    server::lock_manager::{waiter_manager, waiter_manager::Callback},
    storage::{
        mvcc::{Error as MvccError, ErrorInner as MvccErrorInner},
        txn::Error as TxnError,
        Error as StorageError,
    },
};

pub mod lock_wait_context;
pub mod lock_waiting_queue;

#[derive(Clone, Copy, PartialEq, Debug, Default)]
pub struct LockDigest {
    pub ts: TimeStamp,
    pub hash: u64,
}

/// DiagnosticContext is for diagnosing problems about locks
#[derive(Clone, Default)]
pub struct DiagnosticContext {
    /// The key we care about
    pub key: Vec<u8>,
    /// This tag is used for aggregate related kv requests (eg. generated from
    /// same statement) Currently it is the encoded SQL digest if the client
    /// is TiDB
    pub resource_group_tag: Vec<u8>,
    /// The tracker is used to track and collect the lock wait details.
    pub tracker: TrackerToken,
}

impl Debug for DiagnosticContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiagnosticContext")
            .field("key", &log_wrappers::Value::key(&self.key))
            // TODO: Perhaps the resource group tag don't need to be a secret
            .field("resource_group_tag", &log_wrappers::Value::key(&self.resource_group_tag))
            .finish()
    }
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

    /// Timeouts are encoded as i64s in protobufs where 0 means using default
    /// timeout. Negative means no wait.
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
    pub allow_lock_with_conflict: bool,
}

/// Uniquely identifies a lock-waiting request in a `LockManager`.
#[derive(Clone, Copy, Hash, PartialEq, Eq, Debug)]
pub struct LockWaitToken(pub Option<u64>);

impl LockWaitToken {
    pub fn is_valid(&self) -> bool {
        self.0.is_some()
    }
}

#[derive(Debug)]
pub struct UpdateWaitForEvent {
    pub token: LockWaitToken,
    pub start_ts: TimeStamp,
    pub is_first_lock: bool,
    pub wait_info: KeyLockWaitInfo,
}

/// `LockManager` manages transactions waiting for locks held by other
/// transactions. It has responsibility to handle deadlocks between
/// transactions.
pub trait LockManager: Clone + Send + Sync + 'static {
    /// Allocates a token for identifying a specific lock-waiting relationship.
    /// Use this to allocate a token before invoking `wait_for`.
    ///
    /// Since some information required by `wait_for` need to be initialized by
    /// the token, allocating token is therefore separated to a single
    /// function instead of internally allocated in `wait_for`.
    fn allocate_token(&self) -> LockWaitToken;

    /// Transaction with `start_ts` waits for `lock` released.
    ///
    /// If the lock is released or waiting times out or deadlock occurs, the
    /// transaction should be waken up and call `cb` with `pr` to notify the
    /// caller.
    ///
    /// If the lock is the first lock the transaction waits for, it won't result
    /// in deadlock.
    fn wait_for(
        &self,
        token: LockWaitToken,
        region_id: u64,
        region_epoch: RegionEpoch,
        term: u64,
        start_ts: TimeStamp,
        wait_info: KeyLockWaitInfo,
        is_first_lock: bool,
        timeout: Option<WaitTimeout>,
        cancel_callback: CancellationCallback,
        diag_ctx: DiagnosticContext,
    );

    fn update_wait_for(&self, updated_items: Vec<UpdateWaitForEvent>);

    /// Remove a waiter specified by token.
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
pub struct MockLockManager {
    allocated_token: Arc<AtomicU64>,
    waiters: Arc<Mutex<HashMap<LockWaitToken, (KeyLockWaitInfo, CancellationCallback)>>>,
}

impl MockLockManager {
    pub fn new() -> Self {
        Self {
            allocated_token: Arc::new(AtomicU64::new(1)),
            waiters: Arc::new(Mutex::new(HashMap::default())),
        }
    }
}

// Make the linter happy.
impl Default for MockLockManager {
    fn default() -> Self {
        Self::new()
    }
}

impl LockManager for MockLockManager {
    fn allocate_token(&self) -> LockWaitToken {
        LockWaitToken(Some(self.allocated_token.fetch_add(1, Ordering::Relaxed)))
    }

    fn wait_for(
        &self,
        token: LockWaitToken,
        _region_id: u64,
        _region_epoch: RegionEpoch,
        _term: u64,
        _start_ts: TimeStamp,
        wait_info: KeyLockWaitInfo,
        _is_first_lock: bool,
        _timeout: Option<WaitTimeout>,
        cancel_callback: CancellationCallback,
        _diag_ctx: DiagnosticContext,
    ) {
        self.waiters
            .lock()
            .insert(token, (wait_info, cancel_callback));
    }

    fn update_wait_for(&self, _updated_items: Vec<UpdateWaitForEvent>) {}

    fn remove_lock_wait(&self, _token: LockWaitToken) {}

    fn dump_wait_for_entries(&self, cb: Callback) {
        cb(vec![])
    }
}

impl MockLockManager {
    pub fn simulate_timeout_all(&self) {
        let mut map = self.waiters.lock();
        for (_, (wait_info, cancel_callback)) in map.drain() {
            let error = MvccError::from(MvccErrorInner::KeyIsLocked(wait_info.lock_info));
            cancel_callback(StorageError::from(TxnError::from(error)));
        }
    }

    pub fn simulate_timeout(&self, token: LockWaitToken) {
        if let Some((wait_info, cancel_callback)) = self.waiters.lock().remove(&token) {
            let error = MvccError::from(MvccErrorInner::KeyIsLocked(wait_info.lock_info));
            cancel_callback(StorageError::from(TxnError::from(error)));
        }
    }

    pub fn get_all_tokens(&self) -> HashSet<LockWaitToken> {
        self.waiters
            .lock()
            .iter()
            .map(|(&token, _)| token)
            .collect()
    }
}
