// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::min,
    collections::HashMap,
    fmt,
    marker::PhantomData,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use concurrency_manager::ConcurrencyManager;
use engine_traits::KvEngine;
use futures::channel::oneshot::{channel, Receiver, Sender};
use grpcio::Environment;
use kvproto::{kvrpcpb::LeaderInfo, metapb::Region, raft_cmdpb::AdminCmdType};
use online_config::{self, ConfigChange, ConfigManager, OnlineConfig};
use pd_client::PdClient;
use raftstore::{
    coprocessor::{CmdBatch, ObserveHandle, ObserveId},
    router::CdcHandle,
    store::{
        fsm::store::StoreRegionMeta,
        util::{
            self, ReadState, RegionReadProgress, RegionReadProgressCore, RegionReadProgressRegistry,
        },
    },
};
use security::SecurityManager;
use tikv::config::ResolvedTsConfig;
use tikv_util::{
    memory::{HeapSize, MemoryQuota},
    warn,
    worker::{Runnable, RunnableWithTimer, Scheduler},
};
use tokio::sync::{Notify, Semaphore};
use txn_types::{Key, TimeStamp};

use crate::{
    advance::{AdvanceTsWorker, LeadershipResolver, DEFAULT_CHECK_LEADER_TIMEOUT_DURATION},
    cmd::{ChangeLog, ChangeRow},
    metrics::*,
    resolver::{LastAttempt, Resolver},
    scanner::{ScanEntries, ScanTask, ScannerPool},
    Error, Result, TsSource, TxnLocks, ON_DROP_WARN_HEAP_SIZE,
};

/// grace period for identifying identifying slow resolved-ts and safe-ts.
const SLOW_LOG_GRACE_PERIOD_MS: u64 = 1000;
const MEMORY_QUOTA_EXCEEDED_BACKOFF: Duration = Duration::from_secs(30);

enum ResolverStatus {
    Pending {
        tracked_index: u64,
        locks: Vec<PendingLock>,
        cancelled: Option<Sender<()>>,
        memory_quota: Arc<MemoryQuota>,
    },
    Ready,
}

impl Drop for ResolverStatus {
    fn drop(&mut self) {
        let ResolverStatus::Pending {
            locks,
            memory_quota,
            ..
        } = self else {
            return;
        };
        if locks.is_empty() {
            return;
        }

        // Free memory quota used by pending locks and unlocks.
        let mut bytes = 0;
        let num_locks = locks.len();
        for lock in locks {
            bytes += lock.heap_size();
        }
        if bytes > ON_DROP_WARN_HEAP_SIZE {
            warn!("drop huge ResolverStatus";
                "bytes" => bytes,
                "num_locks" => num_locks,
                "memory_quota_in_use" => memory_quota.in_use(),
                "memory_quota_capacity" => memory_quota.capacity(),
            );
        }
        memory_quota.free(bytes);
    }
}

impl ResolverStatus {
    fn push_pending_lock(&mut self, lock: PendingLock, region_id: u64) -> Result<()> {
        let ResolverStatus::Pending {
            locks,
            memory_quota,
            ..
        } = self else {
            panic!("region {:?} resolver has ready", region_id)
        };
        // Check if adding a new lock or unlock will exceed the memory
        // quota.
        memory_quota.alloc(lock.heap_size()).map_err(|e| {
            fail::fail_point!("resolved_ts_on_pending_locks_memory_quota_exceeded");
            Error::MemoryQuotaExceeded(e)
        })?;
        locks.push(lock);
        Ok(())
    }

    fn update_tracked_index(&mut self, index: u64, region_id: u64) {
        let ResolverStatus::Pending {
            tracked_index,
            ..
        } = self else {
            panic!("region {:?} resolver has ready", region_id)
        };
        assert!(
            *tracked_index < index,
            "region {}, tracked_index: {}, incoming index: {}",
            region_id,
            *tracked_index,
            index
        );
        *tracked_index = index;
    }

    fn drain_pending_locks(
        &mut self,
        region_id: u64,
    ) -> (u64, impl Iterator<Item = PendingLock> + '_) {
        let ResolverStatus::Pending {
            locks,
            memory_quota,
            tracked_index,
            ..
        } = self else {
            panic!("region {:?} resolver has ready", region_id)
        };
        let memory_quota = memory_quota.clone();
        // Must take locks, otherwise it may double free memory quota on drop.
        let locks = std::mem::take(locks);
        (
            *tracked_index,
            locks.into_iter().map(move |lock| {
                memory_quota.free(lock.heap_size());
                lock
            }),
        )
    }
}

#[allow(dead_code)]
enum PendingLock {
    Track {
        key: Key,
        start_ts: TimeStamp,
    },
    Untrack {
        key: Key,
        start_ts: Option<TimeStamp>,
        commit_ts: Option<TimeStamp>,
    },
}

impl HeapSize for PendingLock {
    fn heap_size(&self) -> usize {
        match self {
            PendingLock::Track { key, .. } | PendingLock::Untrack { key, .. } => {
                key.as_encoded().heap_size()
            }
        }
    }
}

// Records information related to observed region.
// observe_id is used for avoiding ABA problems in incremental scan task,
// advance resolved ts task, and command observing.
struct ObserveRegion {
    meta: Region,
    handle: ObserveHandle,
    // TODO: Get lease from raftstore.
    // lease: Option<RemoteLease>,
    resolver: Resolver,
    resolver_status: ResolverStatus,
}

impl ObserveRegion {
    fn new(
        meta: Region,
        rrp: Arc<RegionReadProgress>,
        memory_quota: Arc<MemoryQuota>,
        cancelled: Sender<()>,
    ) -> Self {
        ObserveRegion {
            resolver: Resolver::with_read_progress(meta.id, Some(rrp), memory_quota.clone()),
            meta,
            handle: ObserveHandle::new(),
            resolver_status: ResolverStatus::Pending {
                tracked_index: 0,
                locks: vec![],
                cancelled: Some(cancelled),
                memory_quota,
            },
        }
    }

    fn read_progress(&self) -> &Arc<RegionReadProgress> {
        self.resolver.read_progress().unwrap()
    }

    fn track_change_log(&mut self, change_logs: &[ChangeLog]) -> Result<()> {
        if matches!(self.resolver_status, ResolverStatus::Pending { .. }) {
            for log in change_logs {
                match log {
                    ChangeLog::Error(e) => {
                        debug!(
                            "skip change log error";
                            "region" => self.meta.id,
                            "error" => ?e,
                        );
                        continue;
                    }
                    ChangeLog::Admin(req_type) => {
                        // TODO: for admin cmd that won't change the region meta like peer list
                        // and key range (i.e. `CompactLog`, `ComputeHash`) we may not need to
                        // return error
                        return Err(box_err!(
                            "region met admin command {:?} while initializing resolver",
                            req_type
                        ));
                    }
                    ChangeLog::Rows { rows, index } => {
                        for row in rows {
                            let lock = match row {
                                ChangeRow::Prewrite { key, start_ts, .. } => PendingLock::Track {
                                    key: key.clone(),
                                    start_ts: *start_ts,
                                },
                                ChangeRow::Commit {
                                    key,
                                    start_ts,
                                    commit_ts,
                                    ..
                                } => PendingLock::Untrack {
                                    key: key.clone(),
                                    start_ts: *start_ts,
                                    commit_ts: *commit_ts,
                                },
                                // One pc command do not contains any lock, so just skip it
                                ChangeRow::OnePc { .. } | ChangeRow::IngestSsT => continue,
                            };
                            self.resolver_status.push_pending_lock(lock, self.meta.id)?;
                        }
                        self.resolver_status
                            .update_tracked_index(*index, self.meta.id);
                    }
                }
            }
        } else {
            for log in change_logs {
                match log {
                    ChangeLog::Error(e) => {
                        debug!(
                            "skip change log error";
                            "region" => self.meta.id,
                            "error" => ?e,
                        );
                        continue;
                    }
                    ChangeLog::Admin(req_type) => match req_type {
                        AdminCmdType::Split
                        | AdminCmdType::BatchSplit
                        | AdminCmdType::PrepareMerge
                        | AdminCmdType::RollbackMerge
                        | AdminCmdType::CommitMerge => {
                            info!(
                                "region met split/merge command, stop tracking since key range changed, wait for re-register";
                                "req_type" => ?req_type,
                            );
                            // Stop tracking so that `tracked_index` larger than the split/merge
                            // command index won't be published until `RegionUpdate` event
                            // trigger the region re-register and re-scan the new key range
                            self.resolver.stop_tracking();
                        }
                        _ => {
                            debug!(
                                "skip change log admin";
                                "region" => self.meta.id,
                                "req_type" => ?req_type,
                            );
                        }
                    },
                    ChangeLog::Rows { rows, index } => {
                        for row in rows {
                            match row {
                                ChangeRow::Prewrite { key, start_ts, .. } => {
                                    self.resolver.track_lock(
                                        *start_ts,
                                        key.to_raw().unwrap(),
                                        Some(*index),
                                    )?;
                                }
                                ChangeRow::Commit { key, .. } => self
                                    .resolver
                                    .untrack_lock(&key.to_raw().unwrap(), Some(*index)),
                                // One pc command do not contains any lock, so just skip it
                                ChangeRow::OnePc { .. } => {
                                    self.resolver.update_tracked_index(*index);
                                }
                                ChangeRow::IngestSsT => {
                                    self.resolver.update_tracked_index(*index);
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Track locks in incoming scan entries.
    fn track_scan_locks(&mut self, entries: ScanEntries, apply_index: u64) -> Result<()> {
        match entries {
            ScanEntries::Lock(locks) => {
                if let ResolverStatus::Ready = self.resolver_status {
                    panic!("region {:?} resolver has ready", self.meta.id)
                }
                for (key, lock) in locks {
                    self.resolver
                        .track_lock(lock.ts, key.to_raw().unwrap(), Some(apply_index))?;
                }
            }
            ScanEntries::None => {
                // Update the `tracked_index` to the snapshot's `apply_index`
                self.resolver.update_tracked_index(apply_index);
                let mut resolver_status =
                    std::mem::replace(&mut self.resolver_status, ResolverStatus::Ready);
                let (pending_tracked_index, pending_locks) =
                    resolver_status.drain_pending_locks(self.meta.id);
                for lock in pending_locks {
                    match lock {
                        PendingLock::Track { key, start_ts } => {
                            self.resolver.track_lock(
                                start_ts,
                                key.to_raw().unwrap(),
                                Some(pending_tracked_index),
                            )?;
                        }
                        PendingLock::Untrack { key, .. } => self
                            .resolver
                            .untrack_lock(&key.to_raw().unwrap(), Some(pending_tracked_index)),
                    }
                }
                info!(
                    "Resolver initialized";
                    "region" => self.meta.id,
                    "observe_id" => ?self.handle.id,
                    "snapshot_index" => apply_index,
                    "pending_data_index" => pending_tracked_index,
                );
            }
        }
        Ok(())
    }
}

pub struct Endpoint<T, E: KvEngine, S> {
    store_id: Option<u64>,
    cfg: ResolvedTsConfig,
    memory_quota: Arc<MemoryQuota>,
    advance_notify: Arc<Notify>,
    store_meta: Arc<Mutex<S>>,
    region_read_progress: RegionReadProgressRegistry,
    regions: HashMap<u64, ObserveRegion>,
    scanner_pool: ScannerPool<T, E>,
    scan_concurrency_semaphore: Arc<Semaphore>,
    scheduler: Scheduler<Task>,
    advance_worker: AdvanceTsWorker,
    _phantom: PhantomData<(T, E)>,
}

// methods that are used for metrics and logging
impl<T, E, S> Endpoint<T, E, S>
where
    T: 'static + CdcHandle<E>,
    E: KvEngine,
    S: StoreRegionMeta,
{
    fn collect_stats(&mut self) -> Stats {
        fn is_leader(store_id: Option<u64>, leader_store_id: Option<u64>) -> bool {
            store_id.is_some() && store_id == leader_store_id
        }

        let store_id = self.get_or_init_store_id();
        let mut stats = Stats::default();
        let regions = &mut self.regions;
        self.region_read_progress.with(|registry| {
            for (region_id, read_progress) in registry {
                let (leader_info, leader_store_id) = read_progress.dump_leader_info();
                let core = read_progress.get_core();
                let resolved_ts = leader_info.get_read_state().get_safe_ts();
                let safe_ts = core.read_state().ts;

                if resolved_ts == 0 {
                    stats.zero_ts_count += 1;
                    continue;
                }

                if is_leader(store_id, leader_store_id) {
                    // leader resolved-ts
                    if resolved_ts < stats.min_leader_resolved_ts.resolved_ts {
                        let resolver = regions.get_mut(region_id).map(|x| &mut x.resolver);
                        stats
                            .min_leader_resolved_ts
                            .set(*region_id, resolver, &core, &leader_info);
                    }
                } else {
                    // follower safe-ts
                    if safe_ts > 0 && safe_ts < stats.min_follower_safe_ts.safe_ts {
                        stats.min_follower_safe_ts.set(*region_id, &core);
                    }

                    // follower resolved-ts
                    if resolved_ts < stats.min_follower_resolved_ts.resolved_ts {
                        stats.min_follower_resolved_ts.set(*region_id, &core);
                    }
                }
            }
        });

        stats.resolver = self.collect_resolver_stats();
        stats.cm_min_lock = self.advance_worker.concurrency_manager.global_min_lock();
        stats
    }

    fn collect_resolver_stats(&mut self) -> ResolverStats {
        let mut stats = ResolverStats::default();
        for observed_region in self.regions.values() {
            match &observed_region.resolver_status {
                ResolverStatus::Pending { locks, .. } => {
                    for l in locks {
                        stats.heap_size += l.heap_size() as i64;
                    }
                    stats.unresolved_count += 1;
                }
                ResolverStatus::Ready { .. } => {
                    stats.heap_size += observed_region.resolver.approximate_heap_bytes() as i64;
                    stats.resolved_count += 1;
                }
            }
        }
        stats
    }

    fn update_metrics(&self, stats: &Stats) {
        let now = self.approximate_now_tso();
        // general
        if stats.min_follower_resolved_ts.resolved_ts < stats.min_leader_resolved_ts.resolved_ts {
            RTS_MIN_RESOLVED_TS.set(stats.min_follower_resolved_ts.resolved_ts as i64);
            RTS_MIN_RESOLVED_TS_GAP.set(now.saturating_sub(
                TimeStamp::from(stats.min_follower_resolved_ts.resolved_ts).physical(),
            ) as i64);
            RTS_MIN_RESOLVED_TS_REGION.set(stats.min_follower_resolved_ts.region_id as i64);
        } else {
            RTS_MIN_RESOLVED_TS.set(stats.min_leader_resolved_ts.resolved_ts as i64);
            RTS_MIN_RESOLVED_TS_GAP.set(now.saturating_sub(
                TimeStamp::from(stats.min_leader_resolved_ts.resolved_ts).physical(),
            ) as i64);
            RTS_MIN_RESOLVED_TS_REGION.set(stats.min_leader_resolved_ts.region_id as i64);
        }
        RTS_ZERO_RESOLVED_TS.set(stats.zero_ts_count);

        RTS_LOCK_HEAP_BYTES_GAUGE.set(stats.resolver.heap_size);
        RTS_LOCK_QUOTA_IN_USE_BYTES_GAUGE.set(self.memory_quota.in_use() as i64);
        RTS_REGION_RESOLVE_STATUS_GAUGE_VEC
            .with_label_values(&["resolved"])
            .set(stats.resolver.resolved_count);
        RTS_REGION_RESOLVE_STATUS_GAUGE_VEC
            .with_label_values(&["unresolved"])
            .set(stats.resolver.unresolved_count);

        CONCURRENCY_MANAGER_MIN_LOCK_TS.set(
            stats
                .cm_min_lock
                .clone()
                .map(|(ts, _)| ts.into_inner())
                .unwrap_or_default() as i64,
        );

        // min follower safe ts
        RTS_MIN_FOLLOWER_SAFE_TS_REGION.set(stats.min_follower_safe_ts.region_id as i64);
        RTS_MIN_FOLLOWER_SAFE_TS.set(stats.min_follower_safe_ts.safe_ts as i64);
        RTS_MIN_FOLLOWER_SAFE_TS_GAP.set(
            now.saturating_sub(TimeStamp::from(stats.min_follower_safe_ts.safe_ts).physical())
                as i64,
        );
        RTS_MIN_FOLLOWER_SAFE_TS_DURATION_TO_LAST_CONSUME_LEADER.set(
            stats
                .min_follower_safe_ts
                .duration_to_last_consume_leader
                .map(|x| x as i64)
                .unwrap_or(-1),
        );

        // min leader resolved ts
        RTS_MIN_LEADER_RESOLVED_TS.set(stats.min_leader_resolved_ts.resolved_ts as i64);
        RTS_MIN_LEADER_RESOLVED_TS_REGION.set(stats.min_leader_resolved_ts.region_id as i64);
        RTS_MIN_LEADER_RESOLVED_TS_REGION_MIN_LOCK_TS.set(
            stats
                .min_leader_resolved_ts
                .min_lock
                .as_ref()
                .map(|(ts, _)| (*ts).into_inner() as i64)
                .unwrap_or(-1),
        );
        RTS_MIN_LEADER_RESOLVED_TS_GAP
            .set(now.saturating_sub(
                TimeStamp::from(stats.min_leader_resolved_ts.resolved_ts).physical(),
            ) as i64);
        RTS_MIN_LEADER_DUATION_TO_LAST_UPDATE_SAFE_TS.set(
            stats
                .min_leader_resolved_ts
                .duration_to_last_update_ms
                .map(|x| x as i64)
                .unwrap_or(-1),
        );

        // min follower resolved ts
        RTS_MIN_FOLLOWER_RESOLVED_TS.set(stats.min_follower_resolved_ts.resolved_ts as i64);
        RTS_MIN_FOLLOWER_RESOLVED_TS_REGION.set(stats.min_follower_resolved_ts.region_id as i64);
        RTS_MIN_FOLLOWER_RESOLVED_TS_GAP.set(
            now.saturating_sub(
                TimeStamp::from(stats.min_follower_resolved_ts.resolved_ts).physical(),
            ) as i64,
        );
        RTS_MIN_FOLLOWER_RESOLVED_TS_DURATION_TO_LAST_CONSUME_LEADER.set(
            stats
                .min_follower_resolved_ts
                .duration_to_last_consume_leader
                .map(|x| x as i64)
                .unwrap_or(-1),
        );
    }

    // Approximate a TSO from PD. It is better than local timestamp when clock skew
    // exists.
    // Returns the physical part.
    fn approximate_now_tso(&self) -> u64 {
        self.advance_worker
            .last_pd_tso
            .try_lock()
            .map(|opt| {
                opt.map(|(pd_ts, instant)| {
                    pd_ts.physical() + instant.saturating_elapsed().as_millis() as u64
                })
                .unwrap_or_else(|| TimeStamp::physical_now())
            })
            .unwrap_or_else(|_| TimeStamp::physical_now())
    }

    fn log_slow_regions(&self, stats: &Stats) {
        let expected_interval = min(
            self.cfg.advance_ts_interval.as_millis(),
            DEFAULT_CHECK_LEADER_TIMEOUT_DURATION.as_millis() as u64,
        ) + self.cfg.advance_ts_interval.as_millis();
        let leader_threshold = expected_interval + SLOW_LOG_GRACE_PERIOD_MS;
        let follower_threshold = 2 * expected_interval + SLOW_LOG_GRACE_PERIOD_MS;
        let now = self.approximate_now_tso();

        // min leader resolved ts
        let min_leader_resolved_ts_gap = now
            .saturating_sub(TimeStamp::from(stats.min_leader_resolved_ts.resolved_ts).physical());
        if min_leader_resolved_ts_gap > leader_threshold {
            info!(
                "the max gap of leader resolved-ts is large";
                "region_id" => stats.min_leader_resolved_ts.region_id,
                "gap" => format!("{}ms", min_leader_resolved_ts_gap),
                "read_state" => ?stats.min_leader_resolved_ts.read_state,
                "applied_index" => stats.min_leader_resolved_ts.applied_index,
                "min_lock" => ?stats.min_leader_resolved_ts.min_lock,
                "lock_num" => stats.min_leader_resolved_ts.lock_num,
                "txn_num" => stats.min_leader_resolved_ts.txn_num,
                "min_memory_lock" => ?stats.cm_min_lock,
                "duration_to_last_update_safe_ts" => match stats.min_leader_resolved_ts.duration_to_last_update_ms {
                    Some(d) => format!("{}ms", d),
                    None => "none".to_owned(),
                },
                "last_resolve_attempt" => &stats.min_leader_resolved_ts.last_resolve_attempt,
            );
        }

        // min follower safe ts
        let min_follower_safe_ts_gap =
            now.saturating_sub(TimeStamp::from(stats.min_follower_safe_ts.safe_ts).physical());
        if min_follower_safe_ts_gap > follower_threshold {
            info!(
                "the max gap of follower safe-ts is large";
                "region_id" => stats.min_follower_safe_ts.region_id,
                "gap" => format!("{}ms", min_follower_safe_ts_gap),
                "safe_ts" => stats.min_follower_safe_ts.safe_ts,
                "resolved_ts" => stats.min_follower_safe_ts.resolved_ts,
                "duration_to_last_consume_leader" => match stats.min_follower_safe_ts.duration_to_last_consume_leader {
                    Some(d) => format!("{}ms", d),
                    None => "none".to_owned(),
                },
                "applied_index" => stats.min_follower_safe_ts.applied_index,
                "latest_candidate" => ?stats.min_follower_safe_ts.latest_candidate,
                "oldest_candidate" => ?stats.min_follower_safe_ts.oldest_candidate,
            );
        }

        // min follower resolved ts
        let min_follower_resolved_ts_gap = now
            .saturating_sub(TimeStamp::from(stats.min_follower_resolved_ts.resolved_ts).physical());
        if min_follower_resolved_ts_gap > follower_threshold {
            if stats.min_follower_resolved_ts.region_id == stats.min_follower_safe_ts.region_id {
                info!(
                    "the max gap of follower resolved-ts is large; it's the same region that has the min safe-ts"
                );
            } else {
                info!(
                    "the max gap of follower resolved-ts is large";
                    "region_id" => stats.min_follower_resolved_ts.region_id,
                    "gap" => format!("{}ms", min_follower_resolved_ts_gap),
                    "safe_ts" => stats.min_follower_resolved_ts.safe_ts,
                    "resolved_ts" => stats.min_follower_resolved_ts.resolved_ts,
                    "duration_to_last_consume_leader" => match stats.min_follower_resolved_ts.duration_to_last_consume_leader {
                        Some(d) => format!("{}ms", d),
                        None => "none".to_owned(),
                    },
                    "applied_index" => stats.min_follower_resolved_ts.applied_index,
                    "latest_candidate" => ?stats.min_follower_resolved_ts.latest_candidate,
                    "oldest_candidate" => ?stats.min_follower_resolved_ts.oldest_candidate,
                );
            }
        }
    }
}

impl<T, E, S> Endpoint<T, E, S>
where
    T: 'static + CdcHandle<E>,
    E: KvEngine,
    S: StoreRegionMeta,
{
    pub fn new(
        cfg: &ResolvedTsConfig,
        scheduler: Scheduler<Task>,
        cdc_handle: T,
        store_meta: Arc<Mutex<S>>,
        pd_client: Arc<dyn PdClient>,
        concurrency_manager: ConcurrencyManager,
        env: Arc<Environment>,
        security_mgr: Arc<SecurityManager>,
    ) -> Self {
        let (region_read_progress, store_id) = {
            let meta = store_meta.lock().unwrap();
            (meta.region_read_progress().clone(), meta.store_id())
        };
        let advance_worker =
            AdvanceTsWorker::new(pd_client.clone(), scheduler.clone(), concurrency_manager);
        let scanner_pool = ScannerPool::new(cfg.scan_lock_pool_size, cdc_handle);
        let store_resolver_gc_interval = Duration::from_secs(60);
        let leader_resolver = LeadershipResolver::new(
            store_id,
            pd_client.clone(),
            env,
            security_mgr,
            region_read_progress.clone(),
            store_resolver_gc_interval,
        );
        let scan_concurrency_semaphore = Arc::new(Semaphore::new(cfg.incremental_scan_concurrency));
        let ep = Self {
            store_id: Some(store_id),
            cfg: cfg.clone(),
            memory_quota: Arc::new(MemoryQuota::new(cfg.memory_quota.0 as usize)),
            advance_notify: Arc::new(Notify::new()),
            scheduler,
            store_meta,
            region_read_progress,
            advance_worker,
            scanner_pool,
            scan_concurrency_semaphore,
            regions: HashMap::default(),
            _phantom: PhantomData::default(),
        };
        ep.handle_advance_resolved_ts(leader_resolver);
        ep
    }

    fn register_region(&mut self, region: Region, backoff: Option<Duration>) {
        let region_id = region.get_id();
        assert!(self.regions.get(&region_id).is_none());
        let Some(read_progress) = self.region_read_progress.get(&region_id) else {
            warn!("try register nonexistent region"; "region" => ?region);
            return;
        };
        info!("register observe region"; "region" => ?region);
        let (cancelled_tx, cancelled_rx) = channel();
        let observe_region = ObserveRegion::new(
            region.clone(),
            read_progress,
            self.memory_quota.clone(),
            cancelled_tx,
        );
        let observe_handle = observe_region.handle.clone();
        observe_region
            .read_progress()
            .update_advance_resolved_ts_notify(self.advance_notify.clone());
        self.regions.insert(region_id, observe_region);

        let scan_task = self.build_scan_task(region, observe_handle, cancelled_rx, backoff);
        let concurrency_semaphore = self.scan_concurrency_semaphore.clone();
        self.scanner_pool
            .spawn_task(scan_task, concurrency_semaphore);
        RTS_SCAN_TASKS.with_label_values(&["total"]).inc();
    }

    fn build_scan_task(
        &self,
        region: Region,
        observe_handle: ObserveHandle,
        cancelled: Receiver<()>,
        backoff: Option<Duration>,
    ) -> ScanTask {
        let scheduler = self.scheduler.clone();
        ScanTask {
            handle: observe_handle,
            region,
            checkpoint_ts: TimeStamp::zero(),
            backoff,
            cancelled,
            scheduler,
        }
    }

    fn deregister_region(&mut self, region_id: u64) {
        if let Some(observe_region) = self.regions.remove(&region_id) {
            let ObserveRegion {
                handle,
                mut resolver_status,
                ..
            } = observe_region;

            info!(
                "deregister observe region";
                "store_id" => ?self.get_or_init_store_id(),
                "region_id" => region_id,
                "observe_id" => ?handle.id
            );
            // Stop observing data
            handle.stop_observing();
            // Stop scanning data
            if let ResolverStatus::Pending {
                ref mut cancelled, ..
            } = resolver_status
            {
                let _ = cancelled.take();
            }
        } else {
            debug!("deregister unregister region"; "region_id" => region_id);
        }
    }

    fn region_updated(&mut self, incoming_region: Region) {
        let region_id = incoming_region.get_id();
        if let Some(obs_region) = self.regions.get_mut(&region_id) {
            if obs_region.meta.get_region_epoch().get_version()
                == incoming_region.get_region_epoch().get_version()
            {
                // only peer list change, no need to re-register region
                obs_region.meta = incoming_region;
                return;
            }
            // TODO: may not need to re-register region for some cases:
            // - `Split/BatchSplit`, which can be handled by remove out-of-range locks from
            //   the `Resolver`'s lock heap
            // - `PrepareMerge` and `RollbackMerge`, the key range is unchanged
            self.deregister_region(region_id);
            self.register_region(incoming_region, None);
        }
    }

    // This function is corresponding to RegionDestroyed event that can be only
    // scheduled by observer. To prevent destroying region for wrong peer, it
    // should check the region epoch at first.
    fn region_destroyed(&mut self, region: Region) {
        if let Some(observe_region) = self.regions.get(&region.id) {
            if util::compare_region_epoch(
                observe_region.meta.get_region_epoch(),
                &region,
                true,
                true,
                false,
            )
            .is_ok()
            {
                self.deregister_region(region.id);
            } else {
                warn!(
                    "resolved ts destroy region failed due to epoch not match";
                    "region_id" => region.id,
                    "current_epoch" => ?observe_region.meta.get_region_epoch(),
                    "request_epoch" => ?region.get_region_epoch(),
                )
            }
        }
    }

    // Deregister current observed region and try to register it again.
    fn re_register_region(
        &mut self,
        region_id: u64,
        observe_id: ObserveId,
        cause: Error,
        backoff: Option<Duration>,
    ) {
        if let Some(observe_region) = self.regions.get(&region_id) {
            if observe_region.handle.id != observe_id {
                warn!("resolved ts deregister region failed due to observe_id not match");
                return;
            }

            info!(
                "register region again";
                "region_id" => region_id,
                "observe_id" => ?observe_id,
                "cause" => ?cause
            );
            self.deregister_region(region_id);
            let region;
            {
                let meta = self.store_meta.lock().unwrap();
                match meta.reader(region_id) {
                    Some(r) => region = r.region.as_ref().clone(),
                    None => return,
                }
            }
            self.register_region(region, backoff);
        }
    }

    // Update advanced resolved ts.
    // Must ensure all regions are leaders at the point of ts.
    fn handle_resolved_ts_advanced(
        &mut self,
        regions: Vec<u64>,
        ts: TimeStamp,
        ts_source: TsSource,
    ) {
        if regions.is_empty() {
            return;
        }
        let now = tikv_util::time::Instant::now_coarse();
        for region_id in regions.iter() {
            if let Some(observe_region) = self.regions.get_mut(region_id) {
                if let ResolverStatus::Ready = observe_region.resolver_status {
                    let _ = observe_region
                        .resolver
                        .resolve(ts, Some(now), ts_source.clone());
                }
            }
        }
    }

    // Tracking or untracking locks with incoming commands that corresponding
    // observe id is valid.
    #[allow(clippy::drop_ref)]
    fn handle_change_log(&mut self, cmd_batch: Vec<CmdBatch>) {
        let size = cmd_batch.iter().map(|b| b.size()).sum::<usize>();
        RTS_CHANNEL_PENDING_CMD_BYTES.sub(size as i64);
        for batch in cmd_batch {
            if batch.is_empty() {
                continue;
            }
            if let Some(observe_region) = self.regions.get_mut(&batch.region_id) {
                let observe_id = batch.rts_id;
                let region_id = observe_region.meta.id;
                if observe_region.handle.id == observe_id {
                    let logs = ChangeLog::encode_change_log(region_id, batch);
                    if let Err(e) = observe_region.track_change_log(&logs) {
                        drop(observe_region);
                        let backoff = match e {
                            Error::MemoryQuotaExceeded(_) => Some(MEMORY_QUOTA_EXCEEDED_BACKOFF),
                            Error::Other(_) => None,
                        };
                        self.re_register_region(region_id, observe_id, e, backoff);
                    }
                } else {
                    debug!("resolved ts CmdBatch discarded";
                        "region_id" => batch.region_id,
                        "observe_id" => ?batch.rts_id,
                        "current" => ?observe_region.handle.id,
                    );
                }
            }
        }
    }

    fn handle_scan_locks(
        &mut self,
        region_id: u64,
        observe_id: ObserveId,
        entries: ScanEntries,
        apply_index: u64,
    ) {
        let mut memory_quota_exceeded = None;
        if let Some(observe_region) = self.regions.get_mut(&region_id) {
            if observe_region.handle.id == observe_id {
                if let Err(Error::MemoryQuotaExceeded(e)) =
                    observe_region.track_scan_locks(entries, apply_index)
                {
                    memory_quota_exceeded = Some(Error::MemoryQuotaExceeded(e));
                }
            }
        } else {
            debug!("scan locks region not exist";
                "region_id" => region_id,
                "observe_id" => ?observe_id);
        }
        if let Some(e) = memory_quota_exceeded {
            let backoff = Some(MEMORY_QUOTA_EXCEEDED_BACKOFF);
            self.re_register_region(region_id, observe_id, e, backoff);
        }
    }

    fn handle_advance_resolved_ts(&self, leader_resolver: LeadershipResolver) {
        let regions = self.regions.keys().into_iter().copied().collect();
        self.advance_worker.advance_ts_for_regions(
            regions,
            leader_resolver,
            self.cfg.advance_ts_interval.0,
            self.advance_notify.clone(),
        );
    }

    fn handle_change_config(&mut self, change: ConfigChange) {
        let prev = format!("{:?}", self.cfg);
        if let Err(e) = self.cfg.update(change) {
            warn!("resolved-ts config fails"; "error" => ?e);
        } else {
            self.advance_notify.notify_waiters();
            self.memory_quota
                .set_capacity(self.cfg.memory_quota.0 as usize);
            self.scan_concurrency_semaphore =
                Arc::new(Semaphore::new(self.cfg.incremental_scan_concurrency));
            info!(
                "resolved-ts config changed";
                "prev" => prev,
                "current" => ?self.cfg,
            );
        }
    }

    fn get_or_init_store_id(&mut self) -> Option<u64> {
        self.store_id.or_else(|| {
            let meta = self.store_meta.lock().unwrap();
            self.store_id = Some(meta.store_id());
            self.store_id
        })
    }

    fn handle_get_diagnosis_info(
        &self,
        region_id: u64,
        log_locks: bool,
        min_start_ts: u64,
        callback: tikv::server::service::ResolvedTsDiagnosisCallback,
    ) {
        if let Some(r) = self.regions.get(&region_id) {
            if log_locks {
                r.resolver.log_locks(min_start_ts);
            }
            callback(Some((
                r.resolver.stopped(),
                r.resolver.resolved_ts().into_inner(),
                r.resolver.tracked_index(),
                r.resolver.num_locks(),
                r.resolver.num_transactions(),
            )));
        } else {
            callback(None);
        }
    }
}

pub enum Task {
    RegionUpdated(Region),
    RegionDestroyed(Region),
    RegisterRegion {
        region: Region,
    },
    DeRegisterRegion {
        region_id: u64,
    },
    ReRegisterRegion {
        region_id: u64,
        observe_id: ObserveId,
        cause: Error,
    },
    AdvanceResolvedTs {
        leader_resolver: LeadershipResolver,
    },
    ResolvedTsAdvanced {
        regions: Vec<u64>,
        ts: TimeStamp,
        ts_source: TsSource,
    },
    ChangeLog {
        cmd_batch: Vec<CmdBatch>,
    },
    ScanLocks {
        region_id: u64,
        observe_id: ObserveId,
        entries: ScanEntries,
        apply_index: u64,
    },
    ChangeConfig {
        change: ConfigChange,
    },
    GetDiagnosisInfo {
        region_id: u64,
        log_locks: bool,
        min_start_ts: u64,
        callback: tikv::server::service::ResolvedTsDiagnosisCallback,
    },
}

impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut de = f.debug_struct("ResolvedTsTask");
        match self {
            Task::RegionDestroyed(ref region) => de
                .field("name", &"region_destroyed")
                .field("region", &region)
                .finish(),
            Task::RegionUpdated(ref region) => de
                .field("name", &"region_updated")
                .field("region", &region)
                .finish(),
            Task::RegisterRegion { ref region } => de
                .field("name", &"register_region")
                .field("region", &region)
                .finish(),
            Task::DeRegisterRegion { ref region_id } => de
                .field("name", &"deregister_region")
                .field("region_id", &region_id)
                .finish(),
            Task::ReRegisterRegion {
                ref region_id,
                ref observe_id,
                ref cause,
            } => de
                .field("name", &"re_register_region")
                .field("region_id", &region_id)
                .field("observe_id", &observe_id)
                .field("cause", &cause)
                .finish(),
            Task::ResolvedTsAdvanced {
                ref regions,
                ref ts,
                ref ts_source,
            } => de
                .field("name", &"advance_resolved_ts")
                .field("regions", &regions)
                .field("ts", &ts)
                .field("ts_source", &ts_source.label())
                .finish(),
            Task::ChangeLog { .. } => de.field("name", &"change_log").finish(),
            Task::ScanLocks {
                ref region_id,
                ref observe_id,
                ref apply_index,
                ..
            } => de
                .field("name", &"scan_locks")
                .field("region_id", &region_id)
                .field("observe_id", &observe_id)
                .field("apply_index", &apply_index)
                .finish(),
            Task::AdvanceResolvedTs { .. } => de.field("name", &"advance_resolved_ts").finish(),
            Task::ChangeConfig { ref change } => de
                .field("name", &"change_config")
                .field("change", &change)
                .finish(),
            Task::GetDiagnosisInfo { region_id, .. } => de
                .field("name", &"get_diagnosis_info")
                .field("region_id", &region_id)
                .field("callback", &"callback")
                .finish(),
        }
    }
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl<T, E, S> Runnable for Endpoint<T, E, S>
where
    T: 'static + CdcHandle<E>,
    E: KvEngine,
    S: StoreRegionMeta,
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        debug!("run resolved-ts task"; "task" => ?task);
        match task {
            Task::RegionDestroyed(region) => self.region_destroyed(region),
            Task::RegionUpdated(region) => self.region_updated(region),
            Task::RegisterRegion { region } => self.register_region(region, None),
            Task::DeRegisterRegion { region_id } => self.deregister_region(region_id),
            Task::ReRegisterRegion {
                region_id,
                observe_id,
                cause,
            } => self.re_register_region(region_id, observe_id, cause, None),
            Task::AdvanceResolvedTs { leader_resolver } => {
                self.handle_advance_resolved_ts(leader_resolver)
            }
            Task::ResolvedTsAdvanced {
                regions,
                ts,
                ts_source,
            } => self.handle_resolved_ts_advanced(regions, ts, ts_source),
            Task::ChangeLog { cmd_batch } => self.handle_change_log(cmd_batch),
            Task::ScanLocks {
                region_id,
                observe_id,
                entries,
                apply_index,
            } => self.handle_scan_locks(region_id, observe_id, entries, apply_index),
            Task::ChangeConfig { change } => self.handle_change_config(change),
            Task::GetDiagnosisInfo {
                region_id,
                log_locks,
                min_start_ts,
                callback,
            } => self.handle_get_diagnosis_info(region_id, log_locks, min_start_ts, callback),
        }
    }
}

pub struct ResolvedTsConfigManager(Scheduler<Task>);

impl ResolvedTsConfigManager {
    pub fn new(scheduler: Scheduler<Task>) -> ResolvedTsConfigManager {
        ResolvedTsConfigManager(scheduler)
    }
}

impl ConfigManager for ResolvedTsConfigManager {
    fn dispatch(&mut self, change: ConfigChange) -> online_config::Result<()> {
        if let Err(e) = self.0.schedule(Task::ChangeConfig { change }) {
            error!("failed to schedule ChangeConfig task"; "err" => ?e);
        }
        Ok(())
    }
}

#[derive(Default)]
struct Stats {
    // stats for metrics
    zero_ts_count: i64,
    min_leader_resolved_ts: LeaderStats,
    min_follower_safe_ts: FollowerStats,
    min_follower_resolved_ts: FollowerStats,
    resolver: ResolverStats,
    // we don't care about min_safe_ts_leader, because safe_ts should be equal to resolved_ts in
    // leaders
    // The min memory lock in concurrency manager.
    cm_min_lock: Option<(TimeStamp, Key)>,
}

struct LeaderStats {
    region_id: u64,
    resolved_ts: u64,
    read_state: ReadState,
    duration_to_last_update_ms: Option<u64>,
    last_resolve_attempt: Option<LastAttempt>,
    applied_index: u64,
    // min lock in LOCK CF
    min_lock: Option<(TimeStamp, TxnLocks)>,
    lock_num: Option<u64>,
    txn_num: Option<u64>,
}

impl Default for LeaderStats {
    fn default() -> Self {
        Self {
            region_id: 0,
            resolved_ts: u64::MAX,
            read_state: ReadState::default(),
            duration_to_last_update_ms: None,
            applied_index: 0,
            last_resolve_attempt: None,
            min_lock: None,
            lock_num: None,
            txn_num: None,
        }
    }
}

impl LeaderStats {
    fn set(
        &mut self,
        region_id: u64,
        mut resolver: Option<&mut Resolver>,
        region_read_progress: &MutexGuard<'_, RegionReadProgressCore>,
        leader_info: &LeaderInfo,
    ) {
        *self = LeaderStats {
            region_id,
            resolved_ts: leader_info.get_read_state().get_safe_ts(),
            read_state: region_read_progress.read_state().clone(),
            duration_to_last_update_ms: region_read_progress
                .last_instant_of_update_ts()
                .map(|i| i.saturating_elapsed().as_millis() as u64),
            last_resolve_attempt: resolver.as_mut().and_then(|r| r.take_last_attempt()),
            min_lock: resolver
                .as_ref()
                .and_then(|r| r.oldest_transaction().map(|(t, tk)| (*t, tk.clone()))),
            applied_index: region_read_progress.applied_index(),
            lock_num: resolver.as_ref().map(|r| r.num_locks()),
            txn_num: resolver.as_ref().map(|r| r.num_transactions()),
        };
    }
}

struct FollowerStats {
    region_id: u64,
    resolved_ts: u64,
    safe_ts: u64,
    latest_candidate: Option<ReadState>,
    oldest_candidate: Option<ReadState>,
    applied_index: u64,
    duration_to_last_consume_leader: Option<u64>,
}

impl Default for FollowerStats {
    fn default() -> Self {
        Self {
            region_id: 0,
            safe_ts: u64::MAX,
            resolved_ts: u64::MAX,
            latest_candidate: None,
            oldest_candidate: None,
            applied_index: 0,
            duration_to_last_consume_leader: None,
        }
    }
}

impl FollowerStats {
    fn set(
        &mut self,
        region_id: u64,
        region_read_progress: &MutexGuard<'_, RegionReadProgressCore>,
    ) {
        let read_state = region_read_progress.read_state();
        *self = FollowerStats {
            region_id,
            resolved_ts: region_read_progress
                .get_leader_info()
                .get_read_state()
                .get_safe_ts(),
            safe_ts: read_state.ts,
            applied_index: region_read_progress.applied_index(),
            latest_candidate: region_read_progress.pending_items().back().cloned(),
            oldest_candidate: region_read_progress.pending_items().front().cloned(),
            duration_to_last_consume_leader: region_read_progress
                .last_instant_of_consume_leader()
                .map(|i| i.saturating_elapsed().as_millis() as u64),
        };
    }
}

#[derive(Default)]
struct ResolverStats {
    resolved_count: i64,
    unresolved_count: i64,
    heap_size: i64,
}

const METRICS_FLUSH_INTERVAL: u64 = 10_000; // 10s

impl<T, E, S> RunnableWithTimer for Endpoint<T, E, S>
where
    T: 'static + CdcHandle<E>,
    E: KvEngine,
    S: StoreRegionMeta,
{
    fn on_timeout(&mut self) {
        let stats = self.collect_stats();
        self.update_metrics(&stats);
        self.log_slow_regions(&stats);
    }

    fn get_interval(&self) -> Duration {
        Duration::from_millis(METRICS_FLUSH_INTERVAL)
    }
}
