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
    time::Instant,
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
    server::{resolve::StoreAddrResolver, Error, Result},
    storage::{
        lock_manager::{
            DiagnosticContext, KeyLockWaitInfo, KeyWakeUpEvent, LockManager as LockManagerTrait,
            LockWaitToken, UpdateWaitForEvent, WaitTimeout,
        },
        DynamicConfigs as StorageDynamicConfigs, Error as StorageError,
    },
};

// const DETECTED_SLOTS_NUM: usize = 128;
//
// #[inline]
// fn detected_slot_idx(token: LockWaitToken) -> usize {
//     let mut s = DefaultHasher::new();
//     token.hash(&mut s);
//     (s.finish() as usize) & (DETECTED_SLOTS_NUM - 1)
// }

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

    /// Record transactions which have sent requests to detect deadlock.
    // detected: Arc<[CachePadded<Mutex<HashSet<LockWaitToken>>>]>,
    token_allocator: Arc<AtomicU64>,

    pipelined: Arc<AtomicBool>,

    in_memory: Arc<AtomicBool>,
}

impl Clone for LockManager {
    fn clone(&self) -> Self {
        Self {
            waiter_mgr_worker: None,
            detector_worker: None,
            waiter_mgr_scheduler: self.waiter_mgr_scheduler.clone(),
            detector_scheduler: self.detector_scheduler.clone(),
            waiter_count: self.waiter_count.clone(),
            // detected: self.detected.clone(),
            token_allocator: self.token_allocator.clone(),
            pipelined: self.pipelined.clone(),
            in_memory: self.in_memory.clone(),
        }
    }
}

impl LockManager {
    pub fn new(cfg: &Config) -> Self {
        let waiter_mgr_worker = FutureWorker::new("waiter-manager");
        let detector_worker = FutureWorker::new("deadlock-detector");
        // let mut detected = Vec::with_capacity(DETECTED_SLOTS_NUM);
        // detected.resize_with(DETECTED_SLOTS_NUM, ||
        // Mutex::new(HashSet::default()).into());

        Self {
            waiter_mgr_scheduler: WaiterMgrScheduler::new(waiter_mgr_worker.scheduler()),
            waiter_mgr_worker: Some(waiter_mgr_worker),
            detector_scheduler: DetectorScheduler::new(detector_worker.scheduler()),
            detector_worker: Some(detector_worker),
            waiter_count: Arc::new(AtomicUsize::new(0)),
            // detected: detected.into(),
            token_allocator: Arc::new(AtomicU64::new(0)),
            pipelined: Arc::new(AtomicBool::new(cfg.pipelined)),
            in_memory: Arc::new(AtomicBool::new(cfg.in_memory)),
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
        let role_change_notifier =
            deadlock::RoleChangeNotifier::new(self.detector_scheduler.clone());
        role_change_notifier.register(host);
        let region_cancel_observer = waiter_manager::RegionLockWaitCancellationObserver::new(
            self.waiter_mgr_scheduler.clone(),
        );
        region_cancel_observer.register(host);
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
        )
    }

    pub fn get_storage_dynamic_configs(&self) -> StorageDynamicConfigs {
        StorageDynamicConfigs {
            pipelined_pessimistic_lock: self.pipelined.clone(),
            in_memory_pessimistic_lock: self.in_memory.clone(),
        }
    }

    // fn add_to_detected(&self, token: LockWaitToken) {
    //     let mut detected = self.detected[detected_slot_idx(token)].lock();
    //     detected.insert(token);
    // }
    //
    // fn remove_from_detected(&self, token: LockWaitToken) -> bool {
    //     let mut detected = self.detected[detected_slot_idx(token)].lock();
    //     detected.remove(&token)
    // }
}

impl LockManagerTrait for LockManager {
    fn set_key_wake_up_delay_callback(
        &self,
        cb: Box<dyn Fn(&txn_types::Key, TimeStamp, TimeStamp, Instant) + Send>,
    ) {
        self.waiter_mgr_scheduler.set_key_wake_up_delay_callback(cb);
    }

    fn allocate_token(&self) -> LockWaitToken {
        LockWaitToken(Some(self.token_allocator.fetch_add(1, Ordering::SeqCst)))
    }

    fn wait_for(
        &self,
        token: LockWaitToken,
        region_id: u64,
        region_epoch: RegionEpoch,
        term: u64,
        start_ts: TimeStamp,
        wait_info: Vec<KeyLockWaitInfo>,
        is_first_lock: bool,
        timeout: Option<WaitTimeout>,
        cancel_callback: Box<dyn FnOnce(StorageError) + Send>,
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

        // If it is the first lock the transaction tries to lock, it won't cause
        // deadlock.
        if !is_first_lock {
            // self.add_to_detected(token);
            self.detector_scheduler
                .detect(token, start_ts, wait_info.clone(), diag_ctx.clone()); // TODO: Try to avoid cloning.
        }
        self.waiter_mgr_scheduler.wait_for(
            token,
            region_id,
            region_epoch,
            term,
            start_ts,
            wait_info,
            timeout,
            cancel_callback,
            diag_ctx,
        );
    }

    fn update_wait_for(&self, updated_items: Vec<UpdateWaitForEvent>) {
        self.waiter_mgr_scheduler.update_wait_for(updated_items);
    }

    fn on_keys_wakeup(&self, wake_up_events: Vec<KeyWakeUpEvent>) {
        self.waiter_mgr_scheduler
            .record_legacy_waking_up_keys(wake_up_events);
    }

    fn remove_lock_wait(&self, token: LockWaitToken) {
        // If `hashes` is some, there may be some waiters waiting for these locks.
        // Try to wake up them.
        if self.has_waiter() {
            self.waiter_mgr_scheduler.remove_lock_wait(token);
        }
        // If a pessimistic transaction is committed or rolled back and it once
        // sent requests to detect deadlock, clean up its wait-for
        // entries in the deadlock detector.
        // if is_pessimistic_txn && self.remove_from_detected(token) {
        //     self.detector_scheduler.clean_up(token);
        // }
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
    use std::{thread, time::Duration};

    use engine_test::kv::KvTestEngine;
    use futures::executor::block_on;
    use kvproto::metapb::{Peer, Region};
    use raft::StateRole;
    use raftstore::coprocessor::RegionChangeEvent;
    use security::SecurityConfig;
    use tikv_util::config::ReadableDuration;

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

    fn diag_ctx(key: &[u8], resource_group_tag: &[u8]) -> DiagnosticContext {
        DiagnosticContext {
            key: key.to_owned(),
            resource_group_tag: resource_group_tag.to_owned(),
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

        // Wake up
        let (waiter_ts, lock) = (
            10.into(),
            LockDigest {
                ts: 20.into(),
                hash: 20,
            },
        );
        let (waiter, lock_info, f) = new_test_waiter(waiter_ts, lock.ts, lock.hash);
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
        // lock_mgr.wake_up(lock.ts, vec![lock.hash], 30.into(), false);
        lock_mgr.remove_lock_wait(token);
        assert_elapsed(
            || expect_write_conflict(block_on(f).unwrap(), waiter_ts, lock_info, 30.into()),
            0,
            500,
        );
        assert!(!lock_mgr.has_waiter());

        // Deadlock
        let (waiter1, lock_info1, f1) = new_test_waiter(10.into(), 20.into(), 20);
        lock_mgr.wait_for(
            lock_mgr.allocate_token(),
            1,
            RegionEpoch::default(),
            1,
            waiter1.start_ts,
            waiter1.wait_info,
            false,
            Some(WaitTimeout::Default),
            waiter1.cancel_callback,
            diag_ctx(b"k1", b"tag1"),
        );
        assert!(lock_mgr.has_waiter());
        let (waiter2, lock_info2, f2) = new_test_waiter(20.into(), 10.into(), 10);
        let token = lock_mgr.allocate_token();
        lock_mgr.wait_for(
            token,
            1,
            RegionEpoch::default(),
            1,
            waiter2.start_ts,
            waiter2.wait_info,
            false,
            Some(WaitTimeout::Default),
            waiter2.cancel_callback,
            diag_ctx(b"k2", b"tag2"),
        );
        assert!(lock_mgr.has_waiter());
        assert_elapsed(
            || {
                expect_deadlock(
                    block_on(f2).unwrap(),
                    20.into(),
                    lock_info2,
                    20,
                    &[(10, 20, b"k1", b"tag1"), (20, 10, b"k2", b"tag2")],
                )
            },
            0,
            500,
        );
        // Waiter2 releases its lock.
        lock_mgr.remove_lock_wait(token);
        assert_elapsed(
            || expect_write_conflict(block_on(f1).unwrap(), 10.into(), lock_info1, 20.into()),
            0,
            500,
        );
        assert!(!lock_mgr.has_waiter());

        // If it's the first lock, no detect.
        // If it's not, detect deadlock.
        for is_first_lock in &[true, false] {
            let (waiter, _, mut f) = new_test_waiter(30.into(), 40.into(), 40);
            let token = lock_mgr.allocate_token();
            lock_mgr.wait_for(
                token,
                1,
                RegionEpoch::default(),
                1,
                waiter.start_ts,
                waiter.wait_info,
                *is_first_lock,
                Some(WaitTimeout::Default),
                waiter.cancel_callback,
                DiagnosticContext::default(),
            );
            assert!(lock_mgr.has_waiter());
            // assert_eq!(lock_mgr.remove_from_detected(token), !is_first_lock);
            lock_mgr.remove_lock_wait(token);
            // block_on(f).unwrap();
            f.try_recv().unwrap_err();
        }
        assert!(!lock_mgr.has_waiter());

        // // If key_hashes is empty, no wake up.
        // let prev_wake_up = TASK_COUNTER_METRICS.wake_up.get();
        // lock_mgr.wake_up(10.into(), vec![], 10.into(), false);
        // assert_eq!(TASK_COUNTER_METRICS.wake_up.get(), prev_wake_up);
        //
        // // If it's non-pessimistic-txn, no clean up.
        // let prev_clean_up = TASK_COUNTER_METRICS.clean_up.get();
        // lock_mgr.wake_up(10.into(), vec![], 10.into(), false);
        // assert_eq!(TASK_COUNTER_METRICS.clean_up.get(), prev_clean_up);
        //
        // // If the txn doesn't wait for locks, no clean up.
        // let prev_clean_up = TASK_COUNTER_METRICS.clean_up.get();
        // lock_mgr.wake_up(10.into(), vec![], 10.into(), true);
        // assert_eq!(TASK_COUNTER_METRICS.clean_up.get(), prev_clean_up);

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
