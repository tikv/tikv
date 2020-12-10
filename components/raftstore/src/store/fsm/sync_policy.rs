// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicI64, AtomicU64};
use std::sync::Arc;

use crossbeam::utils::CachePadded;
use engine_traits::KvEngine;
use engine_traits::RaftEngine;

use tikv_util::collections::HashMap;
use tikv_util::time::Instant as TiInstant;

use crate::store::fsm::RaftRouter;
use crate::store::local_metrics::SyncEventMetrics;
use crate::store::PeerMsg;

const UNSYNCED_REGIONS_SIZE_LIMIT: usize = 1024;

pub trait Action: Clone {
    fn current_ts(&self) -> i64;
    fn sync_raft_engine(&self);
    fn notify_synced(&self, region_id: u64, number: u64);
}

#[derive(Clone)]
pub struct SyncAction<EK: KvEngine, ER: RaftEngine> {
    raft_engine: ER,
    router: RaftRouter<EK, ER>,
}

impl<EK: KvEngine, ER: RaftEngine> SyncAction<EK, ER> {
    pub fn new(raft_engine: ER, router: RaftRouter<EK, ER>) -> SyncAction<EK, ER> {
        SyncAction {
            raft_engine,
            router,
        }
    }
}

impl<EK: KvEngine, ER: RaftEngine> Action for SyncAction<EK, ER> {
    fn current_ts(&self) -> i64 {
        TiInstant::now_coarse().to_microsec()
    }

    fn sync_raft_engine(&self) {
        self.raft_engine.sync().unwrap_or_else(|e| {
            panic!("failed to sync raft engine: {:?}", e);
        });
    }

    fn notify_synced(&self, region_id: u64, number: u64) {
        // TODO: we don't need to send a noop if this fsm still has many messages.
        if let Err(e) = self.router.force_send(region_id, PeerMsg::Noop) {
            debug!(
                "failed to send noop to trigger persisted ready";
                "region_id" => region_id,
                "ready_number" => number,
                "error" => ?e,
            );
        }
    }
}

#[derive(Default)]
pub struct UnsyncedReady {
    number: u64,
    region_id: u64,
    notifier: Arc<AtomicU64>,
    version: u64,
}

impl UnsyncedReady {
    fn new(number: u64, region_id: u64, notifier: Arc<AtomicU64>, version: u64) -> UnsyncedReady {
        UnsyncedReady {
            number,
            region_id,
            notifier,
            version,
        }
    }
}

/// Used for controlling the raft-engine wal 'sync' policy.
/// When regions receive data, the 'sync' will be holded until it reach
/// the deadline. After that, when 'sync' is called by certain thread later,
/// then the notifications will be sent to these unsynced regions.
pub struct SyncPolicy<A: Action> {
    pub metrics: SyncEventMetrics,
    sync_action: A,
    delay_sync_enabled: bool,
    delay_sync_us: i64,

    global_unsynced_version: Arc<CachePadded<AtomicU64>>,
    global_synced_version: Arc<CachePadded<AtomicU64>>,

    local_synced_version: u64,

    /// The global-variables are for cooperating with other store threads.
    /// Invariant:
    ///     1. last_ts = global_last_sync_ts.load(Acquire);
    ///     2. plan_ts = global_plan_sync_ts.load(Acquire);
    ///     3. assert!(plan_ts >= last_ts);
    /// It depends on the facts below:
    ///     1. plan_ts must be changed first.
    ///     2. If plan_ts is changed in one thread, the last_ts must be
    ///        changed in the same thread after calling sync function.
    ///     3. Between 1 and 2, other threads can not change the plan_ts
    ///        and last_ts.
    global_plan_sync_ts: Arc<CachePadded<AtomicI64>>,
    global_last_sync_ts: Arc<CachePadded<AtomicI64>>,

    /// Notify them when 'sync' is triggered and finished.
    unsynced_readies: VecDeque<UnsyncedReady>,
}

impl<A: Action> Clone for SyncPolicy<A> {
    fn clone(&self) -> Self {
        SyncPolicy {
            metrics: SyncEventMetrics::default(),
            sync_action: self.sync_action.clone(),
            delay_sync_enabled: self.delay_sync_enabled,
            delay_sync_us: self.delay_sync_us,
            global_unsynced_version: self.global_unsynced_version.clone(),
            global_synced_version: self.global_synced_version.clone(),
            local_synced_version: self.local_synced_version,
            global_plan_sync_ts: self.global_plan_sync_ts.clone(),
            global_last_sync_ts: self.global_last_sync_ts.clone(),
            unsynced_readies: VecDeque::default(),
        }
    }
}

impl<A: Action> SyncPolicy<A> {
    pub fn new(sync_action: A, delay_sync_enabled: bool, delay_sync_us: i64) -> SyncPolicy<A> {
        let current_ts = sync_action.current_ts();
        SyncPolicy {
            metrics: SyncEventMetrics::default(),
            sync_action,
            delay_sync_us,
            global_unsynced_version: Arc::new(CachePadded::new(AtomicU64::new(0))),
            global_synced_version: Arc::new(CachePadded::new(AtomicU64::new(0))),
            local_synced_version: 0,
            global_plan_sync_ts: Arc::new(CachePadded::new(AtomicI64::new(current_ts))),
            global_last_sync_ts: Arc::new(CachePadded::new(AtomicI64::new(current_ts))),
            delay_sync_enabled,
            unsynced_readies: VecDeque::default(),
        }
    }

    pub fn delay_sync_enabled(&self) -> bool {
        self.delay_sync_enabled
    }

    pub fn new_unsynced_version(&self) -> u64 {
        self.global_unsynced_version.fetch_add(1, Ordering::AcqRel)
    }

    /// Return if all unsynced readies are flushed.
    pub fn try_flush_readies(&mut self) -> bool {
        if !self.delay_sync_enabled || self.unsynced_readies.is_empty() {
            return true;
        }
        let version = self.global_synced_version.load(Ordering::Acquire);
        if self.local_synced_version < version {
            self.local_synced_version = version;
            self.flush_unsynced_readies(Some(version))
        } else {
            false
        }
    }

    /// Call sync if it's needed.
    /// Return whether sync is called or not.
    pub fn sync_if_needed(&mut self, for_ready: bool) -> bool {
        if !self.delay_sync_enabled {
            return false;
        }

        let current_ts = self.sync_action.current_ts();
        if !self.check_sync_internal(current_ts) {
            self.metrics.sync_events.sync_raftdb_skipped_count += 1;
            return false;
        }

        let version = self.global_unsynced_version.load(Ordering::Acquire);

        self.sync_action.sync_raft_engine();

        self.local_synced_version = version;
        self.global_synced_version.store(version, Ordering::Release);

        self.metrics.sync_events.sync_raftdb_count += 1;
        if !for_ready {
            self.metrics.sync_events.sync_raftdb_with_no_ready += 1;
        }

        self.update_ts_after_synced(current_ts);

        self.flush_unsynced_readies(None);

        true
    }

    /// Try to sync and flush unsynced readies, it's called when no 'ready' comes.
    /// Return if all unsynced readies are flushed.
    pub fn try_sync_and_flush(&mut self) -> bool {
        if !self.delay_sync_enabled {
            return true;
        }

        if self.unsynced_readies.is_empty() {
            return true;
        }

        if self.try_flush_readies() {
            return true;
        }

        self.sync_if_needed(false)
    }

    pub fn mark_ready_unsynced(
        &mut self,
        number: u64,
        region_id: u64,
        notifier: Arc<AtomicU64>,
        version: u64,
    ) {
        if !self.delay_sync_enabled {
            return;
        }
        self.unsynced_readies
            .push_back(UnsyncedReady::new(number, region_id, notifier, version));
    }

    /// Update the global timestamps(last_sync_ts, last_plan_ts).
    fn update_ts_after_synced(&mut self, before_sync_ts: i64) {
        let last_sync_ts = self.global_last_sync_ts.load(Ordering::Acquire);
        let plan_sync_ts = self.global_plan_sync_ts.load(Ordering::Acquire);
        assert_eq!(
            plan_sync_ts, before_sync_ts,
            "plan sync ts != before sync ts"
        );

        let pre_ts = self.global_last_sync_ts.compare_and_swap(
            last_sync_ts,
            before_sync_ts,
            Ordering::AcqRel,
        );
        assert_eq!(
            pre_ts, last_sync_ts,
            "failed to CAS last sync ts, pre ts != last sync ts"
        );
    }

    /// Check if this thread should call sync or not.
    /// If it's true, the global_plan_sync_ts will be updated to before_sync_ts.
    /// If current_ts is close to last_sync_ts, or other threads are planning
    /// to sync, then this thread should not sync.
    fn check_sync_internal(&mut self, before_sync_ts: i64) -> bool {
        let last_sync_ts = self.global_last_sync_ts.load(Ordering::Acquire);
        let plan_sync_ts = self.global_plan_sync_ts.load(Ordering::Acquire);
        if last_sync_ts != plan_sync_ts {
            // Another thread is planning to sync, so this thread should do nothing
            assert!(
                plan_sync_ts > last_sync_ts,
                "plan sync ts {} < last sync ts {}",
                plan_sync_ts,
                last_sync_ts
            );
            return false;
        }
        if before_sync_ts <= last_sync_ts {
            return false;
        }

        enum SyncReason {
            CacheFull,
            ReachDealine(i64),
        };

        let reason = if self.unsynced_readies.len() > UNSYNCED_REGIONS_SIZE_LIMIT {
            Some(SyncReason::CacheFull)
        } else {
            let elapsed = before_sync_ts - last_sync_ts;
            if elapsed >= self.delay_sync_us {
                Some(SyncReason::ReachDealine(elapsed))
            } else {
                None
            }
        };

        if let Some(r) = reason {
            // If it's false, it means another thread is planning to sync, so this thread should do nothing
            if plan_sync_ts
                == self.global_plan_sync_ts.compare_and_swap(
                    plan_sync_ts,
                    before_sync_ts,
                    Ordering::AcqRel,
                )
            {
                match r {
                    SyncReason::CacheFull => {
                        self.metrics.sync_events.sync_raftdb_delay_cache_is_full += 1;
                    }
                    SyncReason::ReachDealine(t) => {
                        self.metrics.sync_events.sync_raftdb_reach_deadline += 1;
                        self.metrics.thread_check_delay.observe(t as f64 / 1e9);
                    }
                }
                return true;
            }
        }

        false
    }

    /// Return if all unsynced readies are flushed.
    /// If synced_version is None, it means all unsynced readies should be flushed.
    fn flush_unsynced_readies(&mut self, synced_version: Option<u64>) -> bool {
        let mut need_notify_regions = HashMap::default();
        while let Some(r) = self.unsynced_readies.front() {
            if let Some(version) = synced_version {
                if version < r.version {
                    break;
                }
            }
            let ready = self.unsynced_readies.pop_front().unwrap();

            need_notify_regions.insert(ready.region_id, (ready.number, ready.notifier));
        }
        for (region_id, (number, notifier)) in need_notify_regions {
            loop {
                let pre_number = notifier.load(Ordering::Acquire);
                assert_ne!(pre_number, number);
                if pre_number > number {
                    break;
                }
                if pre_number == notifier.compare_and_swap(pre_number, number, Ordering::AcqRel) {
                    self.sync_action.notify_synced(region_id, number);
                    break;
                }
            }
        }

        if self.unsynced_readies.len() < UNSYNCED_REGIONS_SIZE_LIMIT
            && self.unsynced_readies.capacity() > 2 * UNSYNCED_REGIONS_SIZE_LIMIT
        {
            self.unsynced_readies.shrink_to_fit();
        }
        self.unsynced_readies.is_empty()
    }
}

pub fn new_sync_policy<EK: KvEngine, ER: RaftEngine>(
    raft_engine: ER,
    router: RaftRouter<EK, ER>,
    delay_sync_enabled: bool,
    delay_sync_us: i64,
) -> SyncPolicy<SyncAction<EK, ER>> {
    SyncPolicy::new(
        SyncAction::new(raft_engine, router),
        delay_sync_enabled,
        delay_sync_us,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use tikv_util::collections::HashSet;
    use tikv_util::mpsc::{self, Receiver, Sender};

    #[derive(Clone)]
    struct TestSyncAction {
        raft_engine: Sender<()>,
        router: Sender<(u64, u64)>,
        time: Arc<AtomicI64>,
    }

    impl Action for TestSyncAction {
        fn current_ts(&self) -> i64 {
            self.time.load(Ordering::Acquire)
        }

        fn sync_raft_engine(&self) {
            self.raft_engine.send(()).unwrap();
        }

        fn notify_synced(&self, region_id: u64, number: u64) {
            self.router.send((region_id, number)).unwrap();
        }
    }

    struct TestSyncPolicy {
        sync_policy: SyncPolicy<TestSyncAction>,
        raft_rx: Receiver<()>,
        router_rx: Receiver<(u64, u64)>,
        time: Arc<AtomicI64>,
    }

    fn new_test_sync_policy(delay_sync_enabled: bool, delay_sync_us: i64) -> TestSyncPolicy {
        let (raft_tx, raft_rx) = mpsc::unbounded();
        let (router_tx, router_rx) = mpsc::unbounded();
        let time = Arc::new(AtomicI64::new(0));
        let action = TestSyncAction {
            raft_engine: raft_tx,
            router: router_tx,
            time: time.clone(),
        };
        TestSyncPolicy {
            sync_policy: SyncPolicy::new(action, delay_sync_enabled, delay_sync_us),
            raft_rx,
            router_rx,
            time,
        }
    }

    fn must_sync_times(raft_rx: &Receiver<()>, times: usize) {
        assert_eq!(raft_rx.len(), times, "raft sync times != expected times");
    }

    fn must_same_router_msg(router_rx: &Receiver<(u64, u64)>, msg: Vec<(u64, u64)>) {
        assert_eq!(
            router_rx.len(),
            msg.len(),
            "router msg len != expected msg len"
        );
        let mut msg_set = HashSet::default();
        for v in msg {
            msg_set.insert(v);
        }
        while let Ok(v) = router_rx.try_recv() {
            if !msg_set.remove(&v) {
                panic!("router msg {:?} not in expected msg {:?}", v, msg_set);
            }
        }
        assert!(
            msg_set.is_empty(),
            "remaining expected msg {:?} not in router msg",
            msg_set
        );
    }

    #[test]
    fn test_disable_delay_sync() {
        let test_sync_policy = new_test_sync_policy(false, 10);
        let mut sync_policy = test_sync_policy.sync_policy.clone();
        assert!(!sync_policy.delay_sync_enabled());
        assert!(sync_policy.try_flush_readies());
        assert!(!sync_policy.sync_if_needed(false));
        assert!(!sync_policy.sync_if_needed(true));
        assert!(sync_policy.try_sync_and_flush());

        sync_policy.mark_ready_unsynced(1, 1, Arc::new(AtomicU64::new(1)), 1);
        assert!(sync_policy.unsynced_readies.is_empty());
    }

    #[test]
    fn test_try_flush_readies() {
        let test = new_test_sync_policy(true, 10);
        let mut sync_policy = test.sync_policy.clone();

        let notifier_1 = Arc::new(AtomicU64::new(0));
        let notifier_2 = Arc::new(AtomicU64::new(0));
        let notifier_3 = Arc::new(AtomicU64::new(2));
        sync_policy.mark_ready_unsynced(1, 1, notifier_1.clone(), 2);
        sync_policy.mark_ready_unsynced(3, 1, notifier_1.clone(), 5);
        sync_policy.mark_ready_unsynced(1, 2, notifier_2.clone(), 6);
        sync_policy.mark_ready_unsynced(4, 3, notifier_3.clone(), 6);
        sync_policy.mark_ready_unsynced(4, 2, notifier_2.clone(), 8);
        sync_policy.mark_ready_unsynced(5, 1, notifier_1.clone(), 11);
        sync_policy.mark_ready_unsynced(6, 3, notifier_3.clone(), 12);

        sync_policy
            .global_unsynced_version
            .store(12, Ordering::Release);
        sync_policy.local_synced_version = 1;
        sync_policy
            .global_synced_version
            .store(8, Ordering::Release);
        // So number = 4, region_id = 3, version = 6 should not be notified
        notifier_3.store(5, Ordering::Release);

        sync_policy.try_flush_readies();

        must_sync_times(&test.raft_rx, 0);
        must_same_router_msg(&test.router_rx, vec![(1, 3), (2, 4)]);
        assert_eq!(notifier_1.load(Ordering::Acquire), 3);
        assert_eq!(notifier_2.load(Ordering::Acquire), 4);
        assert_eq!(notifier_3.load(Ordering::Acquire), 5);
        assert_eq!(sync_policy.unsynced_readies.len(), 2);
    }

    #[test]
    fn test_check_sync_internal() {
        let test = new_test_sync_policy(true, 10);
        let mut sync_policy = test.sync_policy.clone();

        sync_policy.global_plan_sync_ts.store(15, Ordering::Release);
        sync_policy.global_last_sync_ts.store(15, Ordering::Release);

        // 12 < global_last_sync_ts(15)
        assert_eq!(sync_policy.check_sync_internal(12), false);

        // 25 - 15 >= delay_sync_us(10)
        assert_eq!(sync_policy.check_sync_internal(25), true);
        assert_eq!(sync_policy.global_plan_sync_ts.load(Ordering::Acquire), 25);
        // Should not change global_last_sync_ts
        assert_eq!(sync_policy.global_last_sync_ts.load(Ordering::Acquire), 15);
        // global_plan_sync_ts > global_last_sync_ts
        assert_eq!(sync_policy.check_sync_internal(30), false);

        sync_policy.global_last_sync_ts.store(25, Ordering::Release);
        // 40 - 25 >= delay_sync_us(10)
        assert_eq!(sync_policy.check_sync_internal(40), true);
        assert_eq!(sync_policy.global_plan_sync_ts.load(Ordering::Acquire), 40);
        // Should not change global_last_sync_ts
        assert_eq!(sync_policy.global_last_sync_ts.load(Ordering::Acquire), 25);
        // global_plan_sync_ts > global_last_sync_ts
        assert_eq!(sync_policy.check_sync_internal(50), false);

        sync_policy.global_last_sync_ts.store(40, Ordering::Release);
        let notify_1 = Arc::new(AtomicU64::new(0));
        for i in 0..(UNSYNCED_REGIONS_SIZE_LIMIT + 1) {
            sync_policy.mark_ready_unsynced(i as u64, 1, notify_1.clone(), 41);
        }
        // Reach limit of `UNSYNCED_REGIONS_SIZE_LIMIT`
        assert_eq!(sync_policy.check_sync_internal(41), true);
    }

    #[test]
    fn test_sync_if_needed() {
        let test = new_test_sync_policy(true, 10);
        let mut sync_policy = test.sync_policy.clone();

        let notifier_1 = Arc::new(AtomicU64::new(0));
        let notifier_2 = Arc::new(AtomicU64::new(0));
        let notifier_3 = Arc::new(AtomicU64::new(2));
        sync_policy.mark_ready_unsynced(1, 1, notifier_1.clone(), 2);
        sync_policy.mark_ready_unsynced(3, 1, notifier_1.clone(), 5);
        sync_policy.mark_ready_unsynced(1, 2, notifier_2.clone(), 6);
        sync_policy.mark_ready_unsynced(4, 3, notifier_3.clone(), 6);
        sync_policy.mark_ready_unsynced(4, 2, notifier_2.clone(), 8);
        sync_policy.mark_ready_unsynced(5, 1, notifier_1.clone(), 11);
        sync_policy.mark_ready_unsynced(6, 3, notifier_3.clone(), 12);

        sync_policy
            .global_unsynced_version
            .store(12, Ordering::Release);
        test.time.store(10, Ordering::Release);

        assert!(sync_policy.sync_if_needed(true));

        must_sync_times(&test.raft_rx, 1);
        must_same_router_msg(&test.router_rx, vec![(1, 5), (2, 4), (3, 6)]);
        assert_eq!(notifier_1.load(Ordering::Acquire), 5);
        assert_eq!(notifier_2.load(Ordering::Acquire), 4);
        assert_eq!(notifier_3.load(Ordering::Acquire), 6);
        assert_eq!(sync_policy.unsynced_readies.len(), 0);
        assert_eq!(
            sync_policy.global_synced_version.load(Ordering::Acquire),
            12
        );
        assert_eq!(sync_policy.global_plan_sync_ts.load(Ordering::Acquire), 10);
        assert_eq!(sync_policy.global_last_sync_ts.load(Ordering::Acquire), 10);
    }

    #[test]
    fn test_mark_ready_unsynced() {
        let test = new_test_sync_policy(true, 10);
        let mut sync_policy = test.sync_policy.clone();

        let notifier_1 = Arc::new(AtomicU64::new(0));
        let notifier_2 = Arc::new(AtomicU64::new(0));
        sync_policy.mark_ready_unsynced(1, 1, notifier_1.clone(), 2);
        sync_policy.mark_ready_unsynced(3, 1, notifier_1.clone(), 5);
        sync_policy.mark_ready_unsynced(1, 2, notifier_2, 6);
        assert_eq!(sync_policy.unsynced_readies.len(), 3);
    }
}
