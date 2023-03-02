// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{Debug, Formatter},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    thread::JoinHandle,
    time::Duration,
};

use collections::{HashMap, HashSet};
use engine_traits::KvEngine;
use kvproto::{
    kvrpcpb::{self, LockInfo},
    metapb::RegionEpoch,
};
use parking_lot::Mutex;
use pd_client::PdClient;
use raftstore::coprocessor::CoprocessorHost;
use security::SecurityManager;
use tikv_util::worker::FutureWorker;
use tracker::TrackerToken;
use txn_types::{Key, TimeStamp};

pub use self::{
    config::{Config, LockManagerConfigManager},
    deadlock::{Scheduler as DetectorScheduler, Service as DeadlockService},
    lock_wait_context::CancellationCallback,
    waiter_manager::Scheduler as WaiterMgrScheduler,
};
use self::{
    deadlock::{Detector, RoleChangeNotifier},
    lock_wait_context::PessimisticLockKeyCallback,
    lock_waiting_queue::{
        DelayedNotifyAllFuture, LockWaitEntry, LockWaitQueues, UpdateLockWaitResult,
    },
    // DiagnosticContext, KeyLockWaitInfo, LockWaitToken, UpdateWaitForEvent, WaitTimeout,
    waiter_manager::{Callback, Waiter, WaiterManager},
};
use super::txn::commands::WriteResultLockInfo;
use crate::{
    server::{resolve::StoreAddrResolver, Error, Result},
    storage::{
        mvcc::{Error as MvccError, ErrorInner as MvccErrorInner},
        txn::Error as TxnError,
        DynamicConfigs as StorageDynamicConfigs, Error as StorageError,
    },
};

mod client;
mod config;
pub mod deadlock;
pub mod lock_wait_context;
pub mod lock_waiting_queue;
mod metrics;
pub mod waiter_manager;

/// `LockManager` has two components working in two threads:
///   * One is the `WaiterManager` which manages transactions waiting for locks.
///   * The other one is the `Detector` which detects deadlocks between
///     transactions.
pub struct LockManager {
    waiter_mgr_worker: Option<FutureWorker<waiter_manager::Task>>,
    detector_worker: Option<FutureWorker<deadlock::Task>>,

    waiter_mgr_scheduler: WaiterMgrScheduler,
    detector_scheduler: DetectorScheduler,

    waiter_count: Arc<AtomicUsize>,

    token_allocator: Arc<AtomicU64>,

    pipelined: Arc<AtomicBool>,

    in_memory: Arc<AtomicBool>,

    wake_up_delay_duration_ms: Arc<AtomicU64>,

    pub(super) lock_wait_queues: LockWaitQueues,
}

impl Clone for LockManager {
    fn clone(&self) -> Self {
        Self {
            waiter_mgr_worker: None,
            detector_worker: None,
            waiter_mgr_scheduler: self.waiter_mgr_scheduler.clone(),
            detector_scheduler: self.detector_scheduler.clone(),
            waiter_count: self.waiter_count.clone(),
            token_allocator: self.token_allocator.clone(),
            pipelined: self.pipelined.clone(),
            in_memory: self.in_memory.clone(),
            wake_up_delay_duration_ms: self.wake_up_delay_duration_ms.clone(),
            lock_wait_queues: self.lock_wait_queues.clone(),
        }
    }
}

impl LockManager {
    pub fn new(cfg: &Config) -> Self {
        let waiter_mgr_worker = FutureWorker::new("waiter-manager");
        let detector_worker = FutureWorker::new("deadlock-detector");

        Self {
            waiter_mgr_scheduler: WaiterMgrScheduler::new(waiter_mgr_worker.scheduler()),
            waiter_mgr_worker: Some(waiter_mgr_worker),
            detector_scheduler: DetectorScheduler::new(detector_worker.scheduler()),
            detector_worker: Some(detector_worker),
            waiter_count: Arc::new(AtomicUsize::new(0)),
            token_allocator: Arc::new(AtomicU64::new(0)),
            pipelined: Arc::new(AtomicBool::new(cfg.pipelined)),
            in_memory: Arc::new(AtomicBool::new(cfg.in_memory)),
            wake_up_delay_duration_ms: Arc::new(AtomicU64::new(
                cfg.wake_up_delay_duration.as_millis(),
            )),
            lock_wait_queues: LockWaitQueues::new(),
        }
    }

    /// Starts `WaiterManager` and `Detector`.
    pub fn start<S, P>(
        &mut self,
        store_id: u64,
        pd_client: Arc<P>,
        resolver: S,
        security_mgr: Arc<SecurityManager>,
        cfg: &Config,
    ) -> Result<()>
    where
        S: StoreAddrResolver + 'static,
        P: PdClient + 'static,
    {
        self.start_waiter_manager(cfg)?;
        self.start_deadlock_detector(store_id, pd_client, resolver, security_mgr, cfg)?;
        Ok(())
    }

    /// Stops `WaiterManager` and `Detector`.
    pub fn stop(&mut self) {
        self.stop_waiter_manager();
        self.stop_deadlock_detector();
    }

    fn start_waiter_manager(&mut self, cfg: &Config) -> Result<()> {
        let waiter_mgr_runner = WaiterManager::new(
            Arc::clone(&self.waiter_count),
            self.detector_scheduler.clone(),
            cfg,
        );
        self.waiter_mgr_worker
            .as_mut()
            .expect("worker should be some")
            .start(waiter_mgr_runner)?;
        Ok(())
    }

    fn stop_waiter_manager(&mut self) {
        if let Some(Err(e)) = self
            .waiter_mgr_worker
            .take()
            .and_then(|mut w| w.stop())
            .map(JoinHandle::join)
        {
            info!(
                "ignore failure when stopping waiter manager worker";
                "err" => ?e
            );
        }
    }

    fn start_deadlock_detector<S, P>(
        &mut self,
        store_id: u64,
        pd_client: Arc<P>,
        resolver: S,
        security_mgr: Arc<SecurityManager>,
        cfg: &Config,
    ) -> Result<()>
    where
        S: StoreAddrResolver + 'static,
        P: PdClient + 'static,
    {
        let detector_runner = Detector::new(
            store_id,
            pd_client,
            resolver,
            security_mgr,
            self.waiter_mgr_scheduler.clone(),
            cfg,
        );
        self.detector_worker
            .as_mut()
            .expect("worker should be some")
            .start(detector_runner)?;
        Ok(())
    }

    fn stop_deadlock_detector(&mut self) {
        if let Some(Err(e)) = self
            .detector_worker
            .take()
            .and_then(|mut w| w.stop())
            .map(JoinHandle::join)
        {
            info!(
                "ignore failure when stopping deadlock detector worker";
                "err" => ?e
            );
        }
    }

    /// Creates a `RoleChangeNotifier` of the deadlock detector worker and
    /// registers it to the `CoprocessorHost` to observe the role change
    /// events of the leader region.
    pub fn register_detector_role_change_observer(
        &self,
        host: &mut CoprocessorHost<impl KvEngine>,
    ) {
        let role_change_notifier = RoleChangeNotifier::new(self.detector_scheduler.clone());
        role_change_notifier.register(host);
    }

    /// Creates a `DeadlockService` to handle deadlock detect requests from
    /// other nodes.
    pub fn deadlock_service(&self) -> DeadlockService {
        DeadlockService::new(
            self.waiter_mgr_scheduler.clone(),
            self.detector_scheduler.clone(),
        )
    }

    pub fn config_manager(&self) -> LockManagerConfigManager {
        LockManagerConfigManager::new(
            self.waiter_mgr_scheduler.clone(),
            self.detector_scheduler.clone(),
            self.pipelined.clone(),
            self.in_memory.clone(),
            self.wake_up_delay_duration_ms.clone(),
        )
    }

    pub fn get_storage_dynamic_configs(&self) -> StorageDynamicConfigs {
        StorageDynamicConfigs {
            pipelined_pessimistic_lock: self.pipelined.clone(),
            in_memory_pessimistic_lock: self.in_memory.clone(),
            wake_up_delay_duration_ms: self.wake_up_delay_duration_ms.clone(),
        }
    }
}

impl LockManagerTrait for LockManager {
    fn allocate_token(&self) -> LockWaitToken {
        LockWaitToken(Some(self.token_allocator.fetch_add(1, Ordering::Relaxed)))
    }

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
    ) {
        let timeout = match timeout {
            Some(t) => t,
            None => {
                Waiter::cancel_no_timeout(wait_info, cancel_callback);
                return;
            }
        };

        // Increase `waiter_count` here to prevent there is an on-the-fly WaitFor msg
        // but the waiter_mgr haven't processed it, subsequent WakeUp msgs may be lost.
        self.waiter_count.fetch_add(1, Ordering::SeqCst);

        self.waiter_mgr_scheduler.wait_for(
            token,
            region_id,
            region_epoch,
            term,
            start_ts,
            wait_info.clone(),
            timeout,
            cancel_callback,
            diag_ctx.clone(),
        );

        // If it is the first lock the transaction tries to lock, it won't cause
        // deadlock.
        if !is_first_lock {
            self.detector_scheduler
                .detect(start_ts, wait_info, diag_ctx);
        }
    }

    fn update_wait_for(&self, updated_items: Vec<UpdateWaitForEvent>) {
        self.waiter_mgr_scheduler.update_wait_for(updated_items);
    }

    fn remove_lock_wait(&self, token: LockWaitToken) {
        if self.has_waiter() {
            self.waiter_mgr_scheduler.remove_lock_wait(token);
        }
    }

    fn has_waiter(&self) -> bool {
        self.waiter_count.load(Ordering::SeqCst) > 0
    }

    fn dump_wait_for_entries(&self, cb: waiter_manager::Callback) {
        self.waiter_mgr_scheduler.dump_wait_table(cb);
    }

    fn lock_wait_queues(&self) -> LockWaitQueues {
        self.lock_wait_queues.clone()
    }

    fn push_lock_wait(&self, lock_wait_entry: Box<LockWaitEntry>, current_lock: kvrpcpb::LockInfo) {
        self.lock_wait_queues
            .push_lock_wait(lock_wait_entry, current_lock)
    }

    fn remove_by_token(
        &self,
        key: &Key,
        lock_wait_token: LockWaitToken,
    ) -> Option<Box<LockWaitEntry>> {
        self.lock_wait_queues.remove_by_token(key, lock_wait_token)
    }

    fn pop_for_waking_up(
        &self,
        key: &Key,
        conflicting_start_ts: TimeStamp,
        conflicting_commit_ts: TimeStamp,
        wake_up_delay_duration_ms: u64,
    ) -> Option<(Box<LockWaitEntry>, Option<DelayedNotifyAllFuture>)> {
        self.lock_wait_queues.pop_for_waking_up(
            key,
            conflicting_start_ts,
            conflicting_commit_ts,
            wake_up_delay_duration_ms,
        )
    }

    fn update_lock_wait(&self, lock_info: Vec<kvrpcpb::LockInfo>) -> UpdateLockWaitResult {
        self.lock_wait_queues.update_lock_wait(lock_info)
    }
}

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
pub trait LockManagerTrait: Clone + Send + Sync + 'static {
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

    // TODO: it's temporary during refactoring. Remove it
    fn lock_wait_queues(&self) -> LockWaitQueues;

    // following functions delegate to LockWaitQueues
    fn push_lock_wait(&self, lock_wait_entry: Box<LockWaitEntry>, current_lock: kvrpcpb::LockInfo);

    fn remove_by_token(
        &self,
        key: &Key,
        lock_wait_token: LockWaitToken,
    ) -> Option<Box<LockWaitEntry>>;

    fn pop_for_waking_up(
        &self,
        key: &Key,
        conflicting_start_ts: TimeStamp,
        conflicting_commit_ts: TimeStamp,
        wake_up_delay_duration_ms: u64,
    ) -> Option<(Box<LockWaitEntry>, Option<DelayedNotifyAllFuture>)>;

    fn update_lock_wait(&self, lock_info: Vec<kvrpcpb::LockInfo>) -> UpdateLockWaitResult;
}

// For test
#[derive(Clone)]
pub struct MockLockManager {
    allocated_token: Arc<AtomicU64>,
    waiters: Arc<Mutex<HashMap<LockWaitToken, (KeyLockWaitInfo, CancellationCallback)>>>,
    lock_wait_queues: LockWaitQueues,
}

impl MockLockManager {
    pub fn new() -> Self {
        Self {
            allocated_token: Arc::new(AtomicU64::new(1)),
            waiters: Arc::new(Mutex::new(HashMap::default())),
            lock_wait_queues: LockWaitQueues::new(),
        }
    }
}

// Make the linter happy.
impl Default for MockLockManager {
    fn default() -> Self {
        Self::new()
    }
}

impl LockManagerTrait for MockLockManager {
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

    fn lock_wait_queues(&self) -> LockWaitQueues {
        self.lock_wait_queues.clone()
    }

    fn push_lock_wait(&self, lock_wait_entry: Box<LockWaitEntry>, current_lock: kvrpcpb::LockInfo) {
        self.lock_wait_queues
            .push_lock_wait(lock_wait_entry, current_lock)
    }

    fn remove_by_token(
        &self,
        key: &Key,
        lock_wait_token: LockWaitToken,
    ) -> Option<Box<LockWaitEntry>> {
        self.lock_wait_queues.remove_by_token(key, lock_wait_token)
    }

    fn pop_for_waking_up(
        &self,
        key: &Key,
        conflicting_start_ts: TimeStamp,
        conflicting_commit_ts: TimeStamp,
        wake_up_delay_duration_ms: u64,
    ) -> Option<(Box<LockWaitEntry>, Option<DelayedNotifyAllFuture>)> {
        self.lock_wait_queues.pop_for_waking_up(
            key,
            conflicting_start_ts,
            conflicting_commit_ts,
            wake_up_delay_duration_ms,
        )
    }

    fn update_lock_wait(&self, lock_info: Vec<kvrpcpb::LockInfo>) -> UpdateLockWaitResult {
        self.lock_wait_queues.update_lock_wait(lock_info)
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

pub(super) fn make_lock_waiting_after_resuming(
    lock_info: WriteResultLockInfo,
    cb: PessimisticLockKeyCallback,
) -> Box<LockWaitEntry> {
    Box::new(LockWaitEntry {
        key: lock_info.key,
        lock_hash: lock_info.lock_digest.hash,
        parameters: lock_info.parameters,
        should_not_exist: lock_info.should_not_exist,
        lock_wait_token: lock_info.lock_wait_token,
        // This must be called after an execution fo AcquirePessimisticLockResumed, in which
        // case there must be a valid req_state.
        req_states: lock_info.req_states.unwrap(),
        legacy_wake_up_index: None,
        key_cb: Some(cb.into()),
    })
}

#[cfg(test)]
pub mod proxy_test {
    use std::sync::mpsc::Sender;

    use super::*;

    #[allow(clippy::large_enum_variant)]
    pub enum Msg {
        WaitFor {
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
        },
        RemoveLockWait {
            token: LockWaitToken,
        },
    }

    // `ProxyLockMgr` sends all msgs it received to `Sender`.
    // It's used to check whether we send right messages to lock manager.
    #[derive(Clone)]
    pub struct ProxyLockMgr {
        tx: Arc<Mutex<Sender<Msg>>>,
        has_waiter: Arc<AtomicBool>,
        lock_wait_queues: LockWaitQueues,
    }

    impl ProxyLockMgr {
        pub fn new(tx: Sender<Msg>) -> Self {
            Self {
                tx: Arc::new(Mutex::new(tx)),
                has_waiter: Arc::new(AtomicBool::new(false)),
                lock_wait_queues: LockWaitQueues::new(),
            }
        }
    }

    impl LockManagerTrait for ProxyLockMgr {
        fn allocate_token(&self) -> LockWaitToken {
            LockWaitToken(Some(1))
        }

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
        ) {
            self.tx
                .lock()
                .send(Msg::WaitFor {
                    token,
                    region_id,
                    region_epoch,
                    term,
                    start_ts,
                    wait_info,
                    is_first_lock,
                    timeout,
                    cancel_callback,
                    diag_ctx,
                })
                .unwrap();
        }

        fn update_wait_for(&self, _updated_items: Vec<UpdateWaitForEvent>) {}

        fn remove_lock_wait(&self, token: LockWaitToken) {
            self.tx.lock().send(Msg::RemoveLockWait { token }).unwrap();
        }

        fn has_waiter(&self) -> bool {
            self.has_waiter.load(Ordering::Relaxed)
        }

        fn dump_wait_for_entries(&self, _cb: waiter_manager::Callback) {
            unimplemented!()
        }

        fn lock_wait_queues(&self) -> LockWaitQueues {
            self.lock_wait_queues.clone()
        }

        fn push_lock_wait(
            &self,
            lock_wait_entry: Box<LockWaitEntry>,
            current_lock: kvrpcpb::LockInfo,
        ) {
            self.lock_wait_queues
                .push_lock_wait(lock_wait_entry, current_lock)
        }

        fn remove_by_token(
            &self,
            key: &Key,
            lock_wait_token: LockWaitToken,
        ) -> Option<Box<LockWaitEntry>> {
            self.lock_wait_queues.remove_by_token(key, lock_wait_token)
        }

        fn pop_for_waking_up(
            &self,
            key: &Key,
            conflicting_start_ts: TimeStamp,
            conflicting_commit_ts: TimeStamp,
            wake_up_delay_duration_ms: u64,
        ) -> Option<(Box<LockWaitEntry>, Option<DelayedNotifyAllFuture>)> {
            self.lock_wait_queues.pop_for_waking_up(
                key,
                conflicting_start_ts,
                conflicting_commit_ts,
                wake_up_delay_duration_ms,
            )
        }

        fn update_lock_wait(&self, lock_info: Vec<kvrpcpb::LockInfo>) -> UpdateLockWaitResult {
            self.lock_wait_queues.update_lock_wait(lock_info)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use engine_test::kv::KvTestEngine;
    use futures::executor::block_on;
    use kvproto::metapb::{Peer, Region};
    use raft::StateRole;
    use raftstore::coprocessor::RegionChangeEvent;
    use security::SecurityConfig;
    use tikv_util::config::ReadableDuration;
    use tracker::{TrackerToken, INVALID_TRACKER_TOKEN};
    use txn_types::Key;

    use self::{deadlock::tests::*, metrics::*, waiter_manager::tests::*};
    use super::*;
    use crate::storage::lock_manager::LockDigest;

    fn start_lock_manager() -> LockManager {
        let mut coprocessor_host = CoprocessorHost::<KvTestEngine>::default();

        let cfg = Config {
            wait_for_lock_timeout: ReadableDuration::millis(3000),
            wake_up_delay_duration: ReadableDuration::millis(100),
            pipelined: false,
            in_memory: false,
        };
        let mut lock_mgr = LockManager::new(&cfg);

        lock_mgr.register_detector_role_change_observer(&mut coprocessor_host);
        lock_mgr
            .start(
                1,
                Arc::new(MockPdClient {}),
                MockResolver {},
                Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap()),
                &cfg,
            )
            .unwrap();

        // Make sure the deadlock detector is the leader.
        let mut leader_region = Region::default();
        leader_region.set_start_key(b"".to_vec());
        leader_region.set_end_key(b"foo".to_vec());
        leader_region.set_peers(vec![Peer::default()].into());
        coprocessor_host.on_region_changed(
            &leader_region,
            RegionChangeEvent::Create,
            StateRole::Leader,
        );
        thread::sleep(Duration::from_millis(100));

        lock_mgr
    }

    fn diag_ctx(key: &[u8], resource_group_tag: &[u8], tracker: TrackerToken) -> DiagnosticContext {
        DiagnosticContext {
            key: key.to_owned(),
            resource_group_tag: resource_group_tag.to_owned(),
            tracker,
        }
    }

    #[test]
    fn test_single_lock_manager() {
        let lock_mgr = start_lock_manager();

        // Timeout
        assert!(!lock_mgr.has_waiter());
        let (waiter, lock_info, f) = new_test_waiter(10.into(), 20.into(), 20);
        lock_mgr.wait_for(
            lock_mgr.allocate_token(),
            1,
            RegionEpoch::default(),
            1,
            waiter.start_ts,
            waiter.wait_info,
            true,
            Some(WaitTimeout::Default),
            waiter.cancel_callback,
            DiagnosticContext::default(),
        );
        assert!(lock_mgr.has_waiter());
        assert_elapsed(
            || expect_key_is_locked(block_on(f).unwrap(), lock_info),
            2500,
            3500,
        );
        assert!(!lock_mgr.has_waiter());

        // Removal
        let (waiter_ts, lock) = (
            10.into(),
            LockDigest {
                ts: 20.into(),
                hash: 20,
            },
        );
        let (waiter, _lock_info, f) = new_test_waiter(waiter_ts, lock.ts, lock.hash);
        let token = lock_mgr.allocate_token();
        lock_mgr.wait_for(
            token,
            1,
            RegionEpoch::default(),
            1,
            waiter.start_ts,
            waiter.wait_info,
            true,
            Some(WaitTimeout::Default),
            waiter.cancel_callback,
            DiagnosticContext::default(),
        );
        assert!(lock_mgr.has_waiter());
        lock_mgr.remove_lock_wait(token);
        // The waiter will be directly dropped.
        // In normal cases, when `remove_lock_wait` is invoked, the request's callback
        // must be called somewhere else.
        assert_elapsed(
            || {
                block_on(f).unwrap_err();
            },
            0,
            500,
        );
        assert!(!lock_mgr.has_waiter());

        // Deadlock
        let (waiter1, _lock_info1, f1) = new_test_waiter_with_key(10.into(), 20.into(), b"k1");
        let token1 = lock_mgr.allocate_token();
        lock_mgr.wait_for(
            token1,
            1,
            RegionEpoch::default(),
            1,
            waiter1.start_ts,
            waiter1.wait_info,
            false,
            Some(WaitTimeout::Default),
            waiter1.cancel_callback,
            diag_ctx(b"k1", b"tag1", INVALID_TRACKER_TOKEN),
        );
        assert!(lock_mgr.has_waiter());
        let (waiter2, lock_info2, f2) = new_test_waiter_with_key(20.into(), 10.into(), b"k2");
        lock_mgr.wait_for(
            lock_mgr.allocate_token(),
            1,
            RegionEpoch::default(),
            1,
            waiter2.start_ts,
            waiter2.wait_info,
            false,
            Some(WaitTimeout::Default),
            waiter2.cancel_callback,
            diag_ctx(b"k2", b"tag2", INVALID_TRACKER_TOKEN),
        );
        assert!(lock_mgr.has_waiter());
        assert_elapsed(
            || {
                expect_deadlock(
                    block_on(f2).unwrap(),
                    20.into(),
                    lock_info2,
                    Key::from_raw(b"k1").gen_hash(),
                    &[(10, 20, b"k1", b"tag1"), (20, 10, b"k2", b"tag2")],
                )
            },
            0,
            500,
        );
        // Simulating waiter2 releases its lock so that waiter1 is removed
        lock_mgr.remove_lock_wait(token1);
        assert_elapsed(
            || {
                block_on(f1).unwrap_err();
            },
            0,
            500,
        );
        assert!(!lock_mgr.has_waiter());

        // If it's the first lock, no detect.
        // If it's not, detect deadlock.
        // Note that if txn 30 is writing its first lock, there should never be another
        // transaction waiting for txn 30's lock. We added this waiter (40
        // waiting for 30) just for checking whether the lock manager does the
        // detection internally.
        let (waiter1, _, f1) = new_test_waiter_with_key(40.into(), 30.into(), b"k1");
        let token1 = lock_mgr.allocate_token();
        lock_mgr.wait_for(
            token1,
            1,
            RegionEpoch::default(),
            1,
            waiter1.start_ts,
            waiter1.wait_info,
            false,
            Some(WaitTimeout::Default),
            waiter1.cancel_callback,
            diag_ctx(b"k1", b"tag1", INVALID_TRACKER_TOKEN),
        );
        for is_first_lock in &[true, false] {
            let (waiter, lock_info2, f2) = new_test_waiter_with_key(30.into(), 40.into(), b"k2");
            let token2 = lock_mgr.allocate_token();
            lock_mgr.wait_for(
                token2,
                1,
                RegionEpoch::default(),
                1,
                waiter.start_ts,
                waiter.wait_info,
                *is_first_lock,
                Some(WaitTimeout::Default),
                waiter.cancel_callback,
                diag_ctx(b"k2", b"tag2", INVALID_TRACKER_TOKEN),
            );
            assert!(lock_mgr.has_waiter());
            if *is_first_lock {
                lock_mgr.remove_lock_wait(token2);
                block_on(f2).unwrap_err();
            } else {
                assert_elapsed(
                    || {
                        expect_deadlock(
                            block_on(f2).unwrap(),
                            30.into(),
                            lock_info2,
                            Key::from_raw(b"k1").gen_hash(),
                            &[(40, 30, b"k1", b"tag1"), (30, 40, b"k2", b"tag2")],
                        )
                    },
                    0,
                    500,
                );
            }
        }
        lock_mgr.remove_lock_wait(token1);
        block_on(f1).unwrap_err();
        assert!(!lock_mgr.has_waiter());

        // If timeout is none, no wait for.
        let (waiter, lock_info, f) = new_test_waiter(10.into(), 20.into(), 20);
        let prev_wait_for = TASK_COUNTER_METRICS.wait_for.get();
        lock_mgr.wait_for(
            lock_mgr.allocate_token(),
            1,
            RegionEpoch::default(),
            1,
            waiter.start_ts,
            waiter.wait_info,
            false,
            None,
            waiter.cancel_callback,
            DiagnosticContext::default(),
        );
        assert_elapsed(
            || expect_key_is_locked(block_on(f).unwrap(), lock_info),
            0,
            500,
        );
        assert_eq!(TASK_COUNTER_METRICS.wait_for.get(), prev_wait_for,);
    }

    #[bench]
    fn bench_lock_mgr_clone(b: &mut test::Bencher) {
        let lock_mgr = LockManager::new(&Config {
            pipelined: false,
            in_memory: false,
            ..Default::default()
        });
        b.iter(|| {
            test::black_box(lock_mgr.clone());
        });
    }
}
