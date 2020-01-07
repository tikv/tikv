// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod client;
mod config;
pub mod deadlock;
mod metrics;
pub mod waiter_manager;

pub use self::config::Config;
pub use self::deadlock::Service as DeadlockService;

use self::deadlock::{Detector, RoleChangeNotifier, Scheduler as DetectorScheduler};
use self::waiter_manager::{Scheduler as WaiterMgrScheduler, WaiterManager};

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use crate::pd::PdClient;
use crate::raftstore::coprocessor::CoprocessorHost;
use crate::server::resolve::StoreAddrResolver;
use crate::server::{Error, Result};
use crate::storage::txn::{execute_callback, ProcessResult};
use crate::storage::{lock_manager::Lock, LockMgr, StorageCb};

use tikv_util::collections::HashSet;
use tikv_util::security::SecurityManager;
use tikv_util::worker::FutureWorker;

const DETECTED_SLOTS_NUM: usize = 128;

#[inline]
fn detected_slot_idx(txn_ts: u64) -> usize {
    let mut s = DefaultHasher::new();
    txn_ts.hash(&mut s);
    (s.finish() as usize) & (DETECTED_SLOTS_NUM - 1)
}

/// `LockManager` has two components working in two threads:
///   * One is the `WaiterManager` which manages transactions waiting for locks.
///   * The other one is the `Detector` which detects deadlocks between transactions.
pub struct LockManager {
    waiter_mgr_worker: Option<FutureWorker<waiter_manager::Task>>,
    detector_worker: Option<FutureWorker<deadlock::Task>>,

    waiter_mgr_scheduler: WaiterMgrScheduler,
    detector_scheduler: DetectorScheduler,

    waiter_count: Arc<AtomicUsize>,

    /// Record transactions which have sent requests to detect deadlock.
    detected: Arc<Vec<Mutex<HashSet<u64>>>>,
}

impl Clone for LockManager {
    fn clone(&self) -> Self {
        Self {
            waiter_mgr_worker: None,
            detector_worker: None,
            waiter_mgr_scheduler: self.waiter_mgr_scheduler.clone(),
            detector_scheduler: self.detector_scheduler.clone(),
            waiter_count: self.waiter_count.clone(),
            detected: self.detected.clone(),
        }
    }
}

impl LockManager {
    pub fn new() -> Self {
        let waiter_mgr_worker = FutureWorker::new("waiter-manager");
        let detector_worker = FutureWorker::new("deadlock-detector");
        let mut detected = Vec::with_capacity(DETECTED_SLOTS_NUM);
        detected.resize_with(DETECTED_SLOTS_NUM, || Mutex::new(HashSet::default()));

        Self {
            waiter_mgr_scheduler: WaiterMgrScheduler::new(waiter_mgr_worker.scheduler()),
            waiter_mgr_worker: Some(waiter_mgr_worker),
            detector_scheduler: DetectorScheduler::new(detector_worker.scheduler()),
            detector_worker: Some(detector_worker),
            waiter_count: Arc::new(AtomicUsize::new(0)),
            detected: Arc::new(detected),
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

    /// Creates a `RoleChangeNotifier` of the deadlock detector worker and registers it to
    /// the `CoprocessorHost` to observe the role change events of the leader region.
    pub fn register_detector_role_change_observer(&self, host: &mut CoprocessorHost) {
        let role_change_notifier = RoleChangeNotifier::new(self.detector_scheduler.clone());
        role_change_notifier.register(host);
    }

    /// Creates a `DeadlockService` to handle deadlock detect requests from other nodes.
    pub fn deadlock_service(&self) -> DeadlockService {
        DeadlockService::new(
            self.waiter_mgr_scheduler.clone(),
            self.detector_scheduler.clone(),
        )
    }

    fn add_to_detected(&self, txn_ts: u64) {
        let mut detected = self.detected[detected_slot_idx(txn_ts)].lock().unwrap();
        detected.insert(txn_ts);
    }

    fn remove_from_detected(&self, txn_ts: u64) -> bool {
        let mut detected = self.detected[detected_slot_idx(txn_ts)].lock().unwrap();
        detected.remove(&txn_ts)
    }
}

impl LockMgr for LockManager {
    fn wait_for(
        &self,
        start_ts: u64,
        cb: StorageCb,
        pr: ProcessResult,
        lock: Lock,
        is_first_lock: bool,
        timeout: i64,
    ) {
        // Negative timeout means no wait.
        if timeout < 0 {
            execute_callback(cb, pr);
            return;
        }
        // Increase `waiter_count` here to prevent there is an on-the-fly WaitFor msg
        // but the waiter_mgr haven't processed it, subsequent WakeUp msgs may be lost.
        self.waiter_count.fetch_add(1, Ordering::SeqCst);
        self.waiter_mgr_scheduler
            .wait_for(start_ts, cb, pr, lock, timeout as u64);

        // If it is the first lock the transaction tries to lock, it won't cause deadlock.
        if !is_first_lock {
            self.add_to_detected(start_ts);
            self.detector_scheduler.detect(start_ts, lock);
        }
    }

    fn wake_up(
        &self,
        lock_ts: u64,
        hashes: Option<Vec<u64>>,
        commit_ts: u64,
        is_pessimistic_txn: bool,
    ) {
        // If `hashes` is some, there may be some waiters waiting for these locks.
        // Try to wake up them.
        if self.has_waiter() {
            if let Some(hashes) = hashes {
                self.waiter_mgr_scheduler
                    .wake_up(lock_ts, hashes, commit_ts);
            }
        }
        // If a pessimistic transaction is committed or rolled back and it once sent requests to
        // detect deadlock, clean up its wait-for entries in the deadlock detector.
        if is_pessimistic_txn && self.remove_from_detected(lock_ts) {
            self.detector_scheduler.clean_up(lock_ts);
        }
    }

    fn has_waiter(&self) -> bool {
        self.waiter_count.load(Ordering::SeqCst) > 0
    }
}

#[cfg(test)]
mod tests {
    use self::deadlock::tests::*;
    use self::metrics::*;
    use self::waiter_manager::tests::*;
    use super::*;
    use crate::raftstore::coprocessor::RegionChangeEvent;
    use tikv_util::security::SecurityConfig;

    use std::thread;
    use std::time::Duration;

    use futures::Future;
    use kvproto::metapb::{Peer, Region};
    use raft::StateRole;

    fn start_lock_manager() -> LockManager {
        let mut coprocessor_host = CoprocessorHost::default();
        let mut lock_mgr = LockManager::new();
        let mut cfg = Config::default();
        cfg.wait_for_lock_timeout = 3000;
        cfg.wake_up_delay_duration = 100;
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
        let mut leader_region = Region::new();
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

    #[test]
    fn test_single_lock_manager() {
        let lock_mgr = start_lock_manager();

        // Timeout
        assert!(!lock_mgr.has_waiter());
        let (waiter, lock_info, f) = new_test_waiter(10, 20, 20);
        lock_mgr.wait_for(waiter.start_ts, waiter.cb, waiter.pr, waiter.lock, true, 0);
        assert!(lock_mgr.has_waiter());
        assert_elapsed(
            || expect_key_is_locked(f.wait().unwrap().unwrap().pop().unwrap(), lock_info),
            2900,
            3200,
        );
        assert!(!lock_mgr.has_waiter());

        // Wake up
        let (waiter_ts, lock) = (10, Lock { ts: 20, hash: 20 });
        let (waiter, lock_info, f) = new_test_waiter(waiter_ts, lock.ts, lock.hash);
        lock_mgr.wait_for(waiter.start_ts, waiter.cb, waiter.pr, waiter.lock, true, 0);
        assert!(lock_mgr.has_waiter());
        lock_mgr.wake_up(lock.ts, Some(vec![lock.hash]), 30, false);
        assert_elapsed(
            || expect_write_conflict(f.wait().unwrap(), waiter_ts, lock_info, 30),
            0,
            200,
        );
        assert!(!lock_mgr.has_waiter());

        // Deadlock
        let (waiter1, lock_info1, f1) = new_test_waiter(10, 20, 20);
        lock_mgr.wait_for(
            waiter1.start_ts,
            waiter1.cb,
            waiter1.pr,
            waiter1.lock,
            false,
            0,
        );
        assert!(lock_mgr.has_waiter());
        let (waiter2, lock_info2, f2) = new_test_waiter(20, 10, 10);
        lock_mgr.wait_for(
            waiter2.start_ts,
            waiter2.cb,
            waiter2.pr,
            waiter2.lock,
            false,
            0,
        );
        assert!(lock_mgr.has_waiter());
        assert_elapsed(
            || expect_deadlock(f2.wait().unwrap(), 20, lock_info2, 20),
            0,
            200,
        );
        // Waiter2 releases its lock.
        lock_mgr.wake_up(20, Some(vec![20]), 20, true);
        assert_elapsed(
            || expect_write_conflict(f1.wait().unwrap(), 10, lock_info1, 20),
            0,
            200,
        );
        assert!(!lock_mgr.has_waiter());

        // If it's the first lock, no detect.
        // If it's not, detect deadlock.
        for is_first_lock in &[true, false] {
            let (waiter, _, f) = new_test_waiter(30, 40, 40);
            lock_mgr.wait_for(
                waiter.start_ts,
                waiter.cb,
                waiter.pr,
                waiter.lock,
                *is_first_lock,
                0,
            );
            assert!(lock_mgr.has_waiter());
            assert_eq!(lock_mgr.remove_from_detected(30), !is_first_lock);
            lock_mgr.wake_up(40, Some(vec![40]), 40, false);
            f.wait().unwrap().unwrap_err();
        }
        assert!(!lock_mgr.has_waiter());

        // If key_hashes is none, no wake up.
        let prev_wake_up = TASK_COUNTER_VEC.wake_up.get();
        lock_mgr.wake_up(10, None, 10, false);
        assert_eq!(TASK_COUNTER_VEC.wake_up.get(), prev_wake_up);

        // If it's non-pessimistic-txn, no clean up.
        let prev_clean_up = TASK_COUNTER_VEC.clean_up.get();
        lock_mgr.wake_up(10, None, 10, false);
        assert_eq!(TASK_COUNTER_VEC.clean_up.get(), prev_clean_up);

        // If the txn doesn't wait for locks, no clean up.
        let prev_clean_up = TASK_COUNTER_VEC.clean_up.get();
        lock_mgr.wake_up(10, None, 10, true);
        assert_eq!(TASK_COUNTER_VEC.clean_up.get(), prev_clean_up);

        // If timeout is negative, no wait for.
        let (waiter, lock_info, f) = new_test_waiter(10, 20, 20);
        let prev_wait_for = TASK_COUNTER_VEC.wait_for.get();
        lock_mgr.wait_for(
            waiter.start_ts,
            waiter.cb,
            waiter.pr,
            waiter.lock,
            false,
            -1,
        );
        assert_elapsed(
            || expect_key_is_locked(f.wait().unwrap().unwrap().pop().unwrap(), lock_info),
            0,
            200,
        );
        assert_eq!(TASK_COUNTER_VEC.wait_for.get(), prev_wait_for);
    }

    #[bench]
    fn bench_lock_mgr_clone(b: &mut test::Bencher) {
        let lock_mgr = LockManager::new();
        b.iter(|| {
            test::black_box(lock_mgr.clone());
        });
    }
}
