// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::BTreeSet;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicI64, AtomicU64};
use std::sync::Arc;

use engine::Engines;
use engine_rocks::RocksEngine;

use tikv_util::collections::HashMap;
use tikv_util::time::Instant as TiInstant;

use crate::store::fsm::RaftRouter;
use crate::store::local_metrics::SyncEventMetrics;
use crate::store::PeerMsg;

const UNSYNCED_REGIONS_SIZE_LIMIT: usize = 512;

/// Used for controlling the raft-engine wal 'sync' policy.
/// When regions receive data, the 'sync' will be holded until it reach
/// the deadline. After that, when 'sync' is called by certain thread later,
/// then the notifications will be sent to these unsynced regions.
pub struct SyncPolicy {
    pub metrics: SyncEventMetrics,
    engine: Engines,
    router: RaftRouter<RocksEngine>,
    delay_sync_enabled: bool,
    delay_sync_us: i64,

    /// The global-variables are for cooperate with other poll-worker threads.
    global_plan_sync_ts: Arc<AtomicI64>,
    global_last_sync_ts: Arc<AtomicI64>,

    /// Mark the last-sync-time of this thread, for checking other threads did 'sync' or not.
    local_last_sync_ts: i64,

    /// Store the unsynced regions, notify them when 'sync' is triggered and finished.
    /// The map contains: <region_id, (ready number, atomic notifier, create_time)>
    unsynced_regions_map: HashMap<u64, (u64, Arc<AtomicU64>, i64)>,
    /// Used for quickly checking if timestamp reaches deadline.
    /// The set contains: <(create_time, region_id)>
    unsynced_regions_set: BTreeSet<(i64, u64)>,
}

impl SyncPolicy {
    pub fn new(
        engine: Engines,
        router: RaftRouter<RocksEngine>,
        delay_sync_enabled: bool,
        delay_sync_us: i64,
    ) -> SyncPolicy {
        let current_ts = TiInstant::now_coarse().to_microsec();
        SyncPolicy {
            metrics: SyncEventMetrics::default(),
            engine,
            router,
            delay_sync_us,
            global_plan_sync_ts: Arc::new(AtomicI64::new(current_ts)),
            global_last_sync_ts: Arc::new(AtomicI64::new(current_ts)),
            delay_sync_enabled,
            local_last_sync_ts: current_ts,
            unsynced_regions_map: HashMap::default(),
            unsynced_regions_set: BTreeSet::default(),
        }
    }

    pub fn clone(&self) -> SyncPolicy {
        SyncPolicy {
            metrics: SyncEventMetrics::default(),
            engine: self.engine.clone(),
            router: self.router.clone(),
            delay_sync_enabled: self.delay_sync_enabled,
            delay_sync_us: self.delay_sync_us,
            global_plan_sync_ts: self.global_plan_sync_ts.clone(),
            global_last_sync_ts: self.global_last_sync_ts.clone(),
            local_last_sync_ts: self.global_last_sync_ts.load(Ordering::Relaxed),
            unsynced_regions_map: HashMap::default(),
            unsynced_regions_set: BTreeSet::default(),
        }
    }

    pub fn current_ts(&self) -> i64 {
        TiInstant::now_coarse().to_microsec()
    }

    pub fn delay_sync_enabled(&self) -> bool {
        self.delay_sync_enabled
    }

    /// Return if all unsynced regions are flushed.
    pub fn try_flush_regions(&mut self) -> bool {
        if !self.delay_sync_enabled || self.unsynced_regions_map.is_empty() {
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

        self.engine.sync_raft().unwrap_or_else(|e| {
            panic!("failed to sync raft engine: {:?}", e);
        });

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

        if self.unsynced_regions_map.is_empty() {
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
        notify: Arc<AtomicU64>,
        current_ts: i64,
    ) {
        if !self.delay_sync_enabled {
            return;
        }
        if let Some((_, _, prev_ts)) = self
            .unsynced_regions_map
            .insert(region_id, (number, notify, current_ts))
        {
            if !self.unsynced_regions_set.remove(&(prev_ts, region_id)) {
                panic!(
                    "sync policy metadata corrupt, region_id {}, ts {} not in set",
                    region_id, prev_ts
                );
            }
        }
        self.unsynced_regions_set.insert((current_ts, region_id));
    }

    pub fn mark_region_synced(&mut self, region_id: u64) {
        if !self.delay_sync_enabled {
            return;
        }
        if let Some((_, _, prev_ts)) = self.unsynced_regions_map.remove(&region_id) {
            if !self.unsynced_regions_set.remove(&(prev_ts, region_id)) {
                panic!(
                    "sync policy metadata corrupt, region_id {}, ts {} not in set",
                    region_id, prev_ts
                );
            }
        }
        if self.unsynced_regions_map.len() < UNSYNCED_REGIONS_SIZE_LIMIT
            && self.unsynced_regions_map.capacity() > 2 * UNSYNCED_REGIONS_SIZE_LIMIT
        {
            self.unsynced_regions_map.shrink_to_fit();
        }
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

        let mut need_sync = if self.unsynced_regions_map.len() > UNSYNCED_REGIONS_SIZE_LIMIT {
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
        while let Some((ts, region_id)) = self.unsynced_regions_set.iter().next() {
            let delay_duration = synced_ts - *ts;
            if !flush_all && delay_duration <= 0 {
                break;
            }
            self.metrics
                .sync_delay_duration
                .observe(delay_duration as f64 / 1e9);

            let (ts, region_id) = (*ts, *region_id);
            assert_eq!(self.unsynced_regions_set.remove(&(ts, region_id)), true);
            let (number, notifier, ts2) = self.unsynced_regions_map.remove(&region_id).unwrap();
            assert_eq!(ts, ts2);

            loop {
                let pre_number = notifier.load(Ordering::Acquire);
                assert_ne!(pre_number, number);
                if pre_number > number {
                    break;
                }
                if pre_number == notifier.compare_and_swap(pre_number, number, Ordering::AcqRel) {
                    if let Err(e) = self.router.force_send(region_id, PeerMsg::Noop) {
                        debug!(
                            "failed to send noop to trigger persisted ready";
                            "region_id" => region_id,
                            "ready_number" => number,
                            "error" => ?e,
                        );
                    }
                    break;
                }
            }
        }
        if self.unsynced_regions_map.len() < UNSYNCED_REGIONS_SIZE_LIMIT
            && self.unsynced_regions_map.capacity() > 2 * UNSYNCED_REGIONS_SIZE_LIMIT
        {
            self.unsynced_regions_map.shrink_to_fit();
        }
        self.unsynced_regions_map.is_empty()
    }
}
