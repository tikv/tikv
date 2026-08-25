// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod client;
mod config;
pub mod deadlock;
mod metrics;
pub mod waiter_manager;

use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    thread::JoinHandle,
};

use engine_traits::KvEngine;
use kvproto::metapb::RegionEpoch;
use pd_client::PdClient;
use raftstore::coprocessor::CoprocessorHost;
use security::SecurityManager;
use tikv_util::worker::FutureWorker;
use txn_types::TimeStamp;

pub use self::{
    config::{Config, LockManagerConfigManager},
    deadlock::{Scheduler as DetectorScheduler, Service as DeadlockService},
    waiter_manager::Scheduler as WaiterMgrScheduler,
};
use self::{
    deadlock::Detector,
    waiter_manager::{Waiter, WaiterManager},
};
use crate::{
    server::{
        lock_manager::deadlock::RoleChangeNotifier, resolve::StoreAddrResolver, Error, Result,
    },
    storage::{
        lock_manager::{
            CancellationCallback, DiagnosticContext, KeyLockWaitInfo,
            LockManager as LockManagerTrait, LockWaitToken, UpdateWaitForEvent, WaitTimeout,
        },
        DynamicConfigs as StorageDynamicConfigs,
    },
};

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
            is_first_lock,
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
}

#[cfg(test)]
mod tests {
    use std::{
        pin::Pin,
        task::{Context, Poll},
        thread,
        time::Duration,
    };

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
    use crate::{
        server::resolve::MockStoreAddrResolver,
        storage::lock_manager::{KeyLockWaitInfo, LockDigest, UpdateWaitForEvent},
    };

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
                MockStoreAddrResolver::default(),
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

    fn assert_pending<F: std::future::Future + Unpin>(f: &mut F, msg: &str) {
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(Pin::new(f).poll(&mut cx), Poll::Pending), "{msg}");
    }

    fn fair_wait_info_for_owner(
        mut wait_info: KeyLockWaitInfo,
        new_owner: TimeStamp,
    ) -> KeyLockWaitInfo {
        wait_info.allow_lock_with_conflict = true;
        wait_info.lock_digest.ts = new_owner;
        wait_info.lock_info.lock_version = new_owner.into_inner();
        wait_info
    }

    /// After the owner change, W no longer has W→A. A still-live waiting for
    /// W must not be reported as deadlock.
    #[test]
    fn test_dropped_edge_does_not_false_deadlock_on_old_owner() {
        let lock_mgr = start_lock_manager();
        let (mut w, _, mut f_w) = new_test_waiter_with_key(10.into(), 20.into(), b"hot");
        w.wait_info.allow_lock_with_conflict = true;
        let token_w = lock_mgr.allocate_token();
        lock_mgr.wait_for(
            token_w,
            1,
            RegionEpoch::default(),
            1,
            w.start_ts,
            w.wait_info.clone(),
            false,
            Some(WaitTimeout::Millis(3000)),
            w.cancel_callback,
            diag_ctx(b"hot", b"tag-w", INVALID_TRACKER_TOKEN),
        );
        thread::sleep(Duration::from_millis(50));
        lock_mgr.update_wait_for(vec![UpdateWaitForEvent {
            token: token_w,
            start_ts: 10.into(),
            is_first_lock: false,
            wait_info: fair_wait_info_for_owner(w.wait_info.clone(), 30.into()),
        }]);
        thread::sleep(Duration::from_millis(50));

        let (a, _, mut f_a) = new_test_waiter_with_key(20.into(), 10.into(), b"wkey");
        lock_mgr.wait_for(
            lock_mgr.allocate_token(),
            1,
            RegionEpoch::default(),
            1,
            a.start_ts,
            a.wait_info,
            false,
            Some(WaitTimeout::Millis(3000)),
            a.cancel_callback,
            diag_ctx(b"wkey", b"tag-a", INVALID_TRACKER_TOKEN),
        );
        thread::sleep(Duration::from_millis(150));
        assert_pending(&mut f_a, "stale W→A would false-deadlock A waiting for W");
        assert_pending(&mut f_w, "W should still be waiting");

        lock_mgr.remove_lock_wait(token_w);
        block_on(f_w).unwrap_err();
    }

    /// Handoff then cycle: this wait must not report deadlock; a new Detect of
    /// the current owner (retry) must.
    #[test]
    fn test_owner_change_misses_cycle_until_retry_detect() {
        let lock_mgr = start_lock_manager();
        let (mut w, _, f_w1) = new_test_waiter_with_key(10.into(), 20.into(), b"hot");
        w.wait_info.allow_lock_with_conflict = true;
        let token_w = lock_mgr.allocate_token();
        lock_mgr.wait_for(
            token_w,
            1,
            RegionEpoch::default(),
            1,
            w.start_ts,
            w.wait_info.clone(),
            false,
            Some(WaitTimeout::Millis(3000)),
            w.cancel_callback,
            diag_ctx(b"hot", b"tag-w", INVALID_TRACKER_TOKEN),
        );
        thread::sleep(Duration::from_millis(50));
        lock_mgr.update_wait_for(vec![UpdateWaitForEvent {
            token: token_w,
            start_ts: 10.into(),
            is_first_lock: false,
            wait_info: fair_wait_info_for_owner(w.wait_info.clone(), 30.into()),
        }]);
        thread::sleep(Duration::from_millis(50));

        let (c, _lock_info_c, mut f_c) = new_test_waiter_with_key(30.into(), 10.into(), b"wkey");
        lock_mgr.wait_for(
            lock_mgr.allocate_token(),
            1,
            RegionEpoch::default(),
            1,
            c.start_ts,
            c.wait_info,
            false,
            Some(WaitTimeout::Millis(3000)),
            c.cancel_callback,
            diag_ctx(b"wkey", b"tag-c", INVALID_TRACKER_TOKEN),
        );
        thread::sleep(Duration::from_millis(150));
        assert_pending(
            &mut f_c,
            "W→C was never registered; cycle must not be visible yet",
        );

        // End this wait (timeout / retry) and Detect the current owner.
        lock_mgr.remove_lock_wait(token_w);
        block_on(f_w1).unwrap_err();

        let (w2, lock_info_w, f_w2) = new_test_waiter_with_key(10.into(), 30.into(), b"hot");
        lock_mgr.wait_for(
            lock_mgr.allocate_token(),
            1,
            RegionEpoch::default(),
            1,
            w2.start_ts,
            w2.wait_info,
            false,
            Some(WaitTimeout::Millis(3000)),
            w2.cancel_callback,
            diag_ctx(b"hot", b"tag-w2", INVALID_TRACKER_TOKEN),
        );
        assert_elapsed(
            || {
                expect_deadlock(
                    block_on(f_w2).unwrap(),
                    10.into(),
                    lock_info_w,
                    Key::from_raw(b"wkey").gen_hash(),
                    &[(30, 10, b"wkey", b"tag-c"), (10, 30, b"hot", b"tag-w2")],
                )
            },
            0,
            500,
        );
        let _ = f_c;
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
