// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicI64, AtomicU64};
use std::sync::Arc;

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

/// Used for controlling the raft-engine wal 'sync' policy.
/// When regions receive data, the 'sync' will be holded until it reach
/// the deadline. After that, when 'sync' is called by certain thread later,
/// then the notifications will be sent to these unsynced regions.
pub struct SyncPolicy<A: Action> {
    pub metrics: SyncEventMetrics,
    sync_action: A,
    delay_sync_enabled: bool,
    delay_sync_us: i64,

    /// The global-variables are for cooperate with other poll-worker threads.
    global_plan_sync_ts: Arc<AtomicI64>,
    global_last_sync_ts: Arc<AtomicI64>,

    /// Mark the last-sync-time of this thread, for checking other threads did 'sync' or not.
    local_last_sync_ts: i64,

    /// Store the unsynced regions, notify them when 'sync' is triggered and finished.
    /// Contains: (create_time, region_id, ready number, atomic notifier)
    unsynced_regions: VecDeque<(i64, u64, u64, Arc<AtomicU64>)>,
}

impl<A: Action> SyncPolicy<A> {
    pub fn new(sync_action: A, delay_sync_enabled: bool, delay_sync_us: i64) -> SyncPolicy<A> {
        let current_ts = sync_action.current_ts();
        SyncPolicy {
            metrics: SyncEventMetrics::default(),
            sync_action,
            delay_sync_us,
            global_plan_sync_ts: Arc::new(AtomicI64::new(current_ts)),
            global_last_sync_ts: Arc::new(AtomicI64::new(current_ts)),
            delay_sync_enabled,
            local_last_sync_ts: current_ts,
            unsynced_regions: VecDeque::default(),
        }
    }

    pub fn clone(&self) -> SyncPolicy<A> {
        SyncPolicy {
            metrics: SyncEventMetrics::default(),
            sync_action: self.sync_action.clone(),
            delay_sync_enabled: self.delay_sync_enabled,
            delay_sync_us: self.delay_sync_us,
            global_plan_sync_ts: self.global_plan_sync_ts.clone(),
            global_last_sync_ts: self.global_last_sync_ts.clone(),
            local_last_sync_ts: self.global_last_sync_ts.load(Ordering::Acquire),
            unsynced_regions: self.unsynced_regions.clone(),
        }
    }

    pub fn current_ts(&self) -> i64 {
        self.sync_action.current_ts()
    }

    pub fn delay_sync_enabled(&self) -> bool {
        self.delay_sync_enabled
    }

    /// Return if all unsynced regions are flushed.
    pub fn try_flush_regions(&mut self) -> bool {
        if !self.delay_sync_enabled || self.unsynced_regions.is_empty() {
            return true;
        }
        let last_sync_ts = self.global_last_sync_ts.load(Ordering::Acquire);
        if self.local_last_sync_ts < last_sync_ts {
            self.local_last_sync_ts = last_sync_ts;
            self.flush_unsynced_regions(last_sync_ts, false)
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

        let before_sync_ts = self.current_ts();
        if !self.check_sync_internal(before_sync_ts) {
            self.metrics.sync_events.sync_raftdb_skipped_count += 1;
            return false;
        }

        self.sync_action.sync_raft_engine();

        self.metrics.sync_events.sync_raftdb_count += 1;
        if !for_ready {
            self.metrics.sync_events.sync_raftdb_with_no_ready += 1;
        }

        self.update_status_after_synced(before_sync_ts);
        self.flush_unsynced_regions(before_sync_ts, true);

        true
    }

    /// Try to sync and flush unsynced regions, it's called when no 'ready' comes.
    /// Return if all unsynced regions are flushed.
    pub fn try_sync_and_flush(&mut self) -> bool {
        if !self.delay_sync_enabled {
            return true;
        }

        if self.unsynced_regions.is_empty() {
            return true;
        }

        if self.try_flush_regions() {
            return true;
        }

        self.sync_if_needed(false)
    }

    pub fn mark_region_unsynced(
        &mut self,
        region_id: u64,
        number: u64,
        notifier: Arc<AtomicU64>,
        current_ts: i64,
    ) {
        if !self.delay_sync_enabled {
            return;
        }
        if let Some(v) = self.unsynced_regions.back() {
            assert!(v.0 <= current_ts, "last ts > current ts, ts jump back!");
        }
        self.unsynced_regions
            .push_back((current_ts, region_id, number, notifier));
    }

    /// Update the global status(last_sync_ts, last_plan_ts),
    fn update_status_after_synced(&mut self, before_sync_ts: i64) {
        self.local_last_sync_ts = before_sync_ts;

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
            assert!(plan_sync_ts > last_sync_ts, "plan sync ts < last sync ts");
            return false;
        }
        if before_sync_ts <= last_sync_ts {
            return false;
        }

        let mut need_sync = if self.unsynced_regions.len() > UNSYNCED_REGIONS_SIZE_LIMIT {
            self.metrics.sync_events.sync_raftdb_delay_cache_is_full += 1;
            true
        } else {
            false
        };

        need_sync |= {
            let elapsed = before_sync_ts - last_sync_ts;
            self.metrics
                .thread_check_result
                .observe(elapsed as f64 / 1e9);

            if elapsed >= self.delay_sync_us {
                self.metrics.sync_events.sync_raftdb_reach_deadline += 1;
                true
            } else {
                false
            }
        };

        if !need_sync {
            return false;
        }

        // If it's false, it means another thread is planning to sync, so this thread should do nothing
        plan_sync_ts
            == self.global_plan_sync_ts.compare_and_swap(
                plan_sync_ts,
                before_sync_ts,
                Ordering::AcqRel,
            )
    }

    /// Return if all unsynced region are flushed
    fn flush_unsynced_regions(&mut self, synced_ts: i64, flush_all: bool) -> bool {
        let mut need_notify_regions = HashMap::default();
        while let Some(v) = self.unsynced_regions.front() {
            let delay_duration = synced_ts - (*v).0;
            if !flush_all && delay_duration <= 0 {
                break;
            }
            let (_, region_id, number, notifier) = self.unsynced_regions.pop_front().unwrap();
            self.metrics
                .sync_delay_duration
                .observe(delay_duration as f64 / 1e9);

            need_notify_regions.insert(region_id, (number, notifier));
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

        if self.unsynced_regions.len() < UNSYNCED_REGIONS_SIZE_LIMIT
            && self.unsynced_regions.capacity() > 2 * UNSYNCED_REGIONS_SIZE_LIMIT
        {
            self.unsynced_regions.shrink_to_fit();
        }
        self.unsynced_regions.is_empty()
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
        _time: Arc<AtomicI64>,
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
                panic!("router msg {:?} not in expected msg", v);
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
        assert!(sync_policy.try_flush_regions());
        assert!(!sync_policy.sync_if_needed(false));
        assert!(!sync_policy.sync_if_needed(true));
        assert!(sync_policy.try_sync_and_flush());

        sync_policy.mark_region_unsynced(1, 1, Arc::new(AtomicU64::new(1)), 1);
        assert!(sync_policy.unsynced_regions.is_empty());
    }

    #[test]
    fn test_try_flush_regions() {
        let test = new_test_sync_policy(true, 10);
        let mut sync_policy = test.sync_policy.clone();
        assert_eq!(sync_policy.local_last_sync_ts, 0);

        let notify_1 = Arc::new(AtomicU64::new(0));
        let notify_2 = Arc::new(AtomicU64::new(0));
        let notify_3 = Arc::new(AtomicU64::new(2));
        sync_policy.mark_region_unsynced(1, 1, notify_1.clone(), 2);
        sync_policy.mark_region_unsynced(1, 3, notify_1.clone(), 5);
        sync_policy.mark_region_unsynced(2, 1, notify_2.clone(), 6);
        sync_policy.mark_region_unsynced(2, 4, notify_2.clone(), 8);
        sync_policy.mark_region_unsynced(1, 5, notify_1.clone(), 11);
        sync_policy.mark_region_unsynced(3, 4, notify_3.clone(), 12);

        sync_policy.global_plan_sync_ts.store(10, Ordering::Release);
        sync_policy.global_last_sync_ts.store(10, Ordering::Release);

        sync_policy.try_flush_regions();

        assert_eq!(sync_policy.local_last_sync_ts, 10);
        must_sync_times(&test.raft_rx, 0);
        must_same_router_msg(&test.router_rx, vec![(1, 3), (2, 4)]);
    }
}
