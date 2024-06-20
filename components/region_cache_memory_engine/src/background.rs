// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::BTreeSet, fmt::Display, sync::Arc, thread::JoinHandle, time::Duration};

use bytes::Bytes;
use crossbeam::{
    channel::{bounded, tick, Sender},
    epoch, select,
};
use engine_rocks::{RocksEngine, RocksSnapshot};
use engine_traits::{
    CacheRange, IterOptions, Iterable, Iterator, MiscExt, RangeHintService, SnapshotMiscExt,
    CF_DEFAULT, CF_WRITE, DATA_CFS,
};
use parking_lot::RwLock;
use pd_client::{PdClient, RpcClient};
use raftstore::coprocessor::RegionInfoProvider;
use slog_global::{error, info, warn};
use tikv_util::{
    config::ReadableSize,
    future::block_on_timeout,
    keybuilder::KeyBuilder,
    time::Instant,
    worker::{Builder, Runnable, RunnableWithTimer, ScheduleError, Scheduler, Worker},
};
use txn_types::{Key, TimeStamp, WriteRef, WriteType};
use yatp::Remote;

use crate::{
    engine::{RangeCacheMemoryEngineCore, SkiplistHandle},
    keys::{
        decode_key, encode_key, encode_key_for_boundary_with_mvcc, encoding_for_filter,
        InternalBytes, InternalKey, ValueType,
    },
    memory_controller::{MemoryController, MemoryUsage},
    metrics::{
        GC_FILTERED_STATIC, RANGE_CACHE_COUNT, RANGE_CACHE_MEMORY_USAGE, RANGE_GC_TIME_HISTOGRAM,
        RANGE_LOAD_TIME_HISTOGRAM,
    },
    range_manager::LoadFailedReason,
    range_stats::{RangeStatsManager, DEFAULT_EVICT_MIN_DURATION},
    region_label::{
        LabelRule, RegionLabelAddedCb, RegionLabelRulesManager, RegionLabelServiceBuilder,
    },
    write_batch::RangeCacheWriteBatchEntry,
    BackgroundTask::ManualLoad,
};

/// Try to extract the key and `u64` timestamp from `encoded_key`.
///
/// See also: [`txn_types::Key::split_on_ts_for`]
fn split_ts(key: &[u8]) -> Result<(&[u8], u64), String> {
    match Key::split_on_ts_for(key) {
        Ok((key, ts)) => Ok((key, ts.into_inner())),
        Err(_) => Err(format!(
            "invalid write cf key: {}",
            log_wrappers::Value(key)
        )),
    }
}

fn parse_write(value: &[u8]) -> Result<WriteRef<'_>, String> {
    match WriteRef::parse(value) {
        Ok(write) => Ok(write),
        Err(_) => Err(format!(
            "invalid write cf value: {}",
            log_wrappers::Value(value)
        )),
    }
}

#[derive(Debug)]
pub enum BackgroundTask {
    Gc(GcTask),
    LoadRange,
    MemoryCheckAndEvict,
    DeleteRange(Vec<CacheRange>),
    TopRegionsLoadEvict,
    CleanLockTombstone(u64),
    SetRocksEngine(RocksEngine),
    ManualLoad(CacheRange),
}

impl Display for BackgroundTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackgroundTask::Gc(ref t) => t.fmt(f),
            BackgroundTask::LoadRange => f.debug_struct("LoadTask").finish(),
            BackgroundTask::MemoryCheckAndEvict => f.debug_struct("MemoryCheckAndEvict").finish(),
            BackgroundTask::DeleteRange(ref r) => {
                f.debug_struct("DeleteRange").field("range", r).finish()
            }
            BackgroundTask::TopRegionsLoadEvict => f.debug_struct("CheckTopRegions").finish(),
            BackgroundTask::CleanLockTombstone(ref r) => f
                .debug_struct("CleanLockTombstone")
                .field("seqno", r)
                .finish(),
            BackgroundTask::SetRocksEngine(_) => f.debug_struct("SetDiskEngine").finish(),
            BackgroundTask::ManualLoad(ref r) => {
                f.debug_struct("ManualLoad").field("range", r).finish()
            }
        }
    }
}

#[derive(Debug)]
pub struct GcTask {
    pub safe_point: u64,
}

impl Display for GcTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcTask")
            .field("safe_point", &self.safe_point)
            .finish()
    }
}

// BgWorkManager managers the worker inits, stops, and task schedules. When
// created, it starts a worker which receives tasks such as gc task, range
// delete task, range snapshot load and so on, and starts a thread for
// periodically schedule gc tasks.
pub struct BgWorkManager {
    worker: Worker,
    scheduler: Scheduler<BackgroundTask>,
    delete_range_scheduler: Scheduler<BackgroundTask>,
    manual_load_scheduler: Scheduler<BackgroundTask>,
    tick_stopper: Option<(JoinHandle<()>, Sender<bool>)>,
    core: Arc<RwLock<RangeCacheMemoryEngineCore>>,
}

impl Drop for BgWorkManager {
    fn drop(&mut self) {
        let (h, tx) = self.tick_stopper.take().unwrap();
        let _ = tx.send(true);
        let _ = h.join();
        self.worker.stop();
    }
}

pub struct PdRangeHintService(Arc<RpcClient>);

impl RangeHintService for PdRangeHintService {}

impl From<Arc<RpcClient>> for PdRangeHintService {
    fn from(pd_client: Arc<RpcClient>) -> Self {
        PdRangeHintService(pd_client)
    }
}

const CACHE_LABEL_RULE_KEY: &str = "cache";
const CACHE_LABEL_RULE_ALWAYS: &str = "always";

/// This implementation starts a background task using to pull down region label
/// rules from PD.
impl PdRangeHintService {
    /// Spawn a background task on `remote` to continuosly watch for region
    /// label rules that contain the label `cache`; if a new added for which
    /// `cache` is set to `always`, request loading the label's keyranges using
    /// `range_manager_load_cb`.
    ///
    /// TODO (afeinberg): Add support for evicting key ranges when the `cache`
    /// label is removed or no longer set to always.
    pub fn start<F>(&self, remote: Remote<yatp::task::future::TaskCell>, range_manager_load_cb: F)
    where
        F: Fn(&CacheRange) -> Result<(), LoadFailedReason> + Send + Sync + 'static,
    {
        let pd_client = self.0.clone();
        let region_label_added_cb: RegionLabelAddedCb = Arc::new(move |label_rule: &LabelRule| {
            if !label_rule
                .labels
                .iter()
                .any(|e| e.key == CACHE_LABEL_RULE_KEY && e.value == CACHE_LABEL_RULE_ALWAYS)
            {
                // not related to caching, skip.
                return;
            }
            for key_range in &label_rule.data {
                match CacheRange::try_from(key_range) {
                    Ok(cache_range) => {
                        info!("Requested to cache range"; "cache_range" => ?&cache_range);
                        if let Err(reason) = range_manager_load_cb(&cache_range) {
                            error!("Cache range load failed"; "range" => ?&cache_range, "reason" => ?reason);
                        }
                    }
                    Err(e) => {
                        error!("Unable to convert key_range rule to cache range"; "err" => ?e);
                    }
                }
            }
        });
        let mut region_label_svc = RegionLabelServiceBuilder::new(
            Arc::new(RegionLabelRulesManager {
                region_label_added_cb: Some(region_label_added_cb),
                ..RegionLabelRulesManager::default()
            }),
            pd_client,
        )
        .rule_filter_fn(|label_rule| {
            label_rule
                .labels
                .iter()
                .any(|e| e.key == CACHE_LABEL_RULE_KEY)
        })
        .build()
        .unwrap();
        remote.spawn(async move { region_label_svc.watch_region_labels().await })
    }
}

impl BgWorkManager {
    pub fn new(
        core: Arc<RwLock<RangeCacheMemoryEngineCore>>,
        pd_client: Arc<dyn PdClient>,
        gc_interval: Duration,
        load_evict_interval: Duration,
        expected_region_size: usize,
        memory_controller: Arc<MemoryController>,
        region_info_provider: Option<Arc<dyn RegionInfoProvider>>,
    ) -> Self {
        let worker = Worker::new("range-cache-background-worker");
        let (runner, delete_range_scheduler, manual_load_scheduler) = BackgroundRunner::new(
            core.clone(),
            memory_controller,
            region_info_provider,
            expected_region_size,
        );
        let scheduler = worker.start_with_timer("range-cache-engine-background", runner);

        let (h, tx) = BgWorkManager::start_tick(
            scheduler.clone(),
            pd_client,
            gc_interval,
            load_evict_interval,
        );

        Self {
            worker,
            scheduler,
            delete_range_scheduler,
            manual_load_scheduler,
            tick_stopper: Some((h, tx)),
            core,
        }
    }

    pub fn schedule_task(&self, task: BackgroundTask) -> Result<(), ScheduleError<BackgroundTask>> {
        match task {
            task @ BackgroundTask::DeleteRange(_) => {
                self.delete_range_scheduler.schedule_force(task)
            }
            task => self.scheduler.schedule_force(task),
        }
    }

    pub fn start_bg_hint_service(&self, range_hint_service: PdRangeHintService) {
        let scheduler = self.manual_load_scheduler.clone();
        range_hint_service.start(self.worker.remote(), move |cache_range: &CacheRange| {
            if let Err(e) = scheduler.schedule(BackgroundTask::ManualLoad(cache_range.clone())) {
                error!(
                    "schedule manual load failed";
                    "err" => ?e,
                );
                assert!(tikv_util::thread_group::is_shutdown(!cfg!(test)));
            }
            // TODO (afeinberg): This does not actually load the range. The load happens
            // the apply thread begins to apply raft entries. To force this (for read-only
            // use-cases) we should propose a No-Op command.
            Ok(())
        });
    }

    fn start_tick(
        scheduler: Scheduler<BackgroundTask>,
        pd_client: Arc<dyn PdClient>,
        gc_interval: Duration,
        load_evict_interval: Duration,
    ) -> (JoinHandle<()>, Sender<bool>) {
        let (tx, rx) = bounded(0);
        // TODO: Instead of spawning a new thread, we should run this task
        //       in a shared background thread.
        let h = std::thread::spawn(move || {
            let gc_ticker = tick(gc_interval);
            let load_evict_ticker = tick(load_evict_interval); // TODO (afeinberg): Use a real value.
            // 5 seconds should be long enough for getting a TSO from PD.
            let tso_timeout = std::cmp::min(gc_interval, Duration::from_secs(5));
            'LOOP: loop {
                select! {
                    recv(gc_ticker) -> _ => {
                        let now = match block_on_timeout(pd_client.get_tso(), tso_timeout) {
                            Ok(Ok(ts)) => ts,
                            err => {
                                error!(
                                    "schedule range cache engine gc failed ";
                                    "timeout_duration" => ?tso_timeout,
                                    "error" => ?err,
                                );
                                continue 'LOOP;
                            }
                        };
                        let safe_point = now.physical() - gc_interval.as_millis() as u64;
                        let safe_point = TimeStamp::compose(safe_point, 0).into_inner();
                        if let Err(e) = scheduler.schedule(BackgroundTask::Gc(GcTask {safe_point})) {
                            error!(
                                "schedule range cache engine gc failed";
                                "err" => ?e,
                            );
                        }
                    },
                    recv(load_evict_ticker) -> _ => {
                        if let Err(e) = scheduler.schedule(BackgroundTask::TopRegionsLoadEvict) {
                            error!(
                                "schedule load evict failed";
                                "err" => ?e,
                            );
                        }
                    },
                    recv(rx) -> r => {
                        if let Err(e) = r {
                            error!(
                                "receive error in range cache engien gc ticker";
                                "err" => ?e,
                            );
                        }
                        return;
                    },
                }
            }
        });
        (h, tx)
    }
}

#[derive(Clone)]
struct BackgroundRunnerCore {
    engine: Arc<RwLock<RangeCacheMemoryEngineCore>>,
    memory_controller: Arc<MemoryController>,
    range_stats_manager: Option<RangeStatsManager>,
}

impl BackgroundRunnerCore {
    /// Returns the ranges that are eligible for garbage collection.
    ///
    /// Returns `None` if there are no ranges cached or the previous gc is not
    /// finished.
    fn ranges_for_gc(&self) -> Option<BTreeSet<CacheRange>> {
        let ranges: BTreeSet<CacheRange> = {
            let core = self.engine.read();
            if core.range_manager().has_ranges_in_gc() {
                return None;
            }
            core.range_manager().ranges().keys().cloned().collect()
        };
        let ranges_clone = ranges.clone();
        if ranges_clone.is_empty() {
            return None;
        }
        {
            let mut core = self.engine.write();
            core.mut_range_manager().set_ranges_in_gc(ranges_clone);
        }
        Some(ranges)
    }

    fn gc_range(&self, range: &CacheRange, safe_point: u64, oldest_seqno: u64) -> FilterMetrics {
        let (skiplist_engine, safe_ts) = {
            let mut core = self.engine.write();
            let Some(range_meta) = core.mut_range_manager().mut_range_meta(range) else {
                return FilterMetrics::default();
            };
            let min_snapshot = range_meta
                .range_snapshot_list()
                .min_snapshot_ts()
                .unwrap_or(u64::MAX);
            let safe_point = u64::min(safe_point, min_snapshot);

            if safe_point <= range_meta.safe_point() {
                info!(
                    "safe point not large enough";
                    "prev" => range_meta.safe_point(),
                    "current" => safe_point,
                );
                return FilterMetrics::default();
            }

            // todo: change it to debug!
            info!(
                "safe point update";
                "prev" => range_meta.safe_point(),
                "current" => safe_point,
                "range" => ?range,
            );
            range_meta.set_safe_point(safe_point);
            (core.engine(), safe_point)
        };

        let start = Instant::now();
        let write_cf_handle = skiplist_engine.cf_handle(CF_WRITE);
        let default_cf_handle = skiplist_engine.cf_handle(CF_DEFAULT);
        let mut filter = Filter::new(
            safe_ts,
            oldest_seqno,
            default_cf_handle,
            write_cf_handle.clone(),
        );

        let mut iter = write_cf_handle.iterator();
        let guard = &epoch::pin();
        let (start_key, end_key) = encode_key_for_boundary_with_mvcc(range);
        iter.seek(&start_key, guard);
        while iter.valid() && iter.key() < &end_key {
            let k = iter.key();
            let v = iter.value();
            if let Err(e) = filter.filter(k.as_bytes(), v.as_bytes()) {
                warn!(
                    "Something Wrong in memory engine GC";
                    "error" => ?e,
                );
            }
            iter.next(guard);
        }

        let duration = start.saturating_elapsed();
        RANGE_GC_TIME_HISTOGRAM.observe(duration.as_secs_f64());
        info!(
            "range gc complete";
            "range" => ?range,
            "gc_duration" => ?duration,
            "total_version" => filter.metrics.total,
            "filtered_version" => filter.metrics.filtered,
            "below_safe_point_unique_keys" => filter.metrics.unique_key,
            "below_safe_point_version" => filter.metrics.versions,
            "below_safe_point_delete_version" => filter.metrics.delete_versions,
            "current_safe_point" => safe_ts,
        );

        std::mem::take(&mut filter.metrics)
    }

    fn on_gc_finished(&mut self, ranges: BTreeSet<CacheRange>) {
        let mut core = self.engine.write();
        core.mut_range_manager().on_gc_finished(ranges);
    }

    /// Returns the first range to load with RocksDB snapshot. The `bool`
    /// returned indicates whether the task has been canceled due to memory
    /// issue.
    ///
    /// Returns `None` if there are no ranges to load.
    fn get_range_to_load(&self) -> Option<(CacheRange, Arc<RocksSnapshot>, bool)> {
        let core = self.engine.read();
        core.range_manager()
            .pending_ranges_loading_data
            .front()
            .cloned()
    }

    // if `false` is returned, the load is canceled
    fn on_snapshot_load_finished(
        &mut self,
        range: CacheRange,
        delete_range_scheduler: &Scheduler<BackgroundTask>,
    ) -> bool {
        fail::fail_point!("on_snapshot_load_finished");
        fail::fail_point!("on_snapshot_load_finished2");
        loop {
            // Consume the cached write batch after the snapshot is acquired.
            let mut core = self.engine.write();
            // We still need to check whether the snapshot is canceled during the load
            let canceled = core
                .range_manager()
                .pending_ranges_loading_data
                .front()
                .unwrap()
                .2;
            if canceled {
                let (r, ..) = core
                    .mut_range_manager()
                    .pending_ranges_loading_data
                    .pop_front()
                    .unwrap();
                assert_eq!(r, range);
                core.mut_range_manager()
                    .ranges_being_deleted
                    .insert(r.clone());
                core.remove_cached_write_batch(&range);
                drop(core);
                fail::fail_point!("in_memory_engine_snapshot_load_canceled");

                if let Err(e) =
                    delete_range_scheduler.schedule_force(BackgroundTask::DeleteRange(vec![r]))
                {
                    error!(
                        "schedule delete range failed";
                        "err" => ?e,
                    );
                    assert!(tikv_util::thread_group::is_shutdown(!cfg!(test)));
                }

                return false;
            }

            if core.has_cached_write_batch(&range) {
                let (cache_batch, skiplist_engine) = {
                    (
                        core.take_cached_write_batch_entries(&range),
                        core.engine().clone(),
                    )
                };
                drop(core);
                let guard = &epoch::pin();
                for (seq, entry) in cache_batch {
                    entry
                        .write_to_memory(
                            seq,
                            &skiplist_engine,
                            self.memory_controller.clone(),
                            guard,
                        )
                        .unwrap();
                }
                fail::fail_point!("on_cached_write_batch_consumed");
            } else {
                core.remove_cached_write_batch(&range);
                RangeCacheMemoryEngineCore::pending_range_completes_loading(&mut core, &range);
                drop(core);

                fail::fail_point!("pending_range_completes_loading");
                break;
            }
        }
        true
    }

    fn on_snapshot_load_canceled(
        &mut self,
        range: CacheRange,
        delete_range_scheduler: &Scheduler<BackgroundTask>,
    ) {
        let mut core = self.engine.write();
        let (r, ..) = core
            .mut_range_manager()
            .pending_ranges_loading_data
            .pop_front()
            .unwrap();
        assert_eq!(r, range);
        core.remove_cached_write_batch(&range);
        core.mut_range_manager()
            .ranges_being_deleted
            .insert(r.clone());

        if let Err(e) = delete_range_scheduler.schedule_force(BackgroundTask::DeleteRange(vec![r]))
        {
            error!(
                "schedule delete range failed";
                "err" => ?e,
            );
            assert!(tikv_util::thread_group::is_shutdown(!cfg!(test)));
        }
    }

    /// Eviction on soft limit reached:
    ///
    /// When soft limit is reached, collect the candidates for eviction, and
    /// keep evicting until either all candidates are evicted, or the total
    /// approximated size of evicted regions is equal to or greater than the
    /// excess memory usage.
    fn evict_on_soft_limit_reached(&self, delete_range_scheduler: &Scheduler<BackgroundTask>) {
        if self.range_stats_manager.is_none() {
            warn!("range stats manager is not initialized, cannot evict on soft limit reached");
            return;
        }
        let range_stats_manager = self.range_stats_manager.as_ref().unwrap();
        let to_shrink_by = self
            .memory_controller
            .mem_usage()
            .checked_sub(self.memory_controller.soft_limit_threshold());
        if to_shrink_by.is_none() {
            return;
        }
        let mut remaining = to_shrink_by.unwrap();
        let mut ranges_to_evict = Vec::<(CacheRange, u64)>::with_capacity(256);

        // TODO (afeinberg, low): consider returning just an iterator and using scan
        // below for cleaner code.
        range_stats_manager.collect_candidates_for_eviction(&mut ranges_to_evict, |range| {
            self.engine.read().range_manager().contains_range(range)
        });

        let mut ranges_to_delete = vec![];
        // TODO (afeinberg): approximate size may differ from size in in-memory cache,
        // consider taking the actual size into account.
        for (range, approx_size) in &ranges_to_evict {
            if remaining == 0 {
                break;
            }
            let evicted_range = {
                let mut engine_wr = self.engine.write();
                let mut ranges = engine_wr.mut_range_manager().evict_range(range);
                if !ranges.is_empty() {
                    info!(
                        "evict on soft limit reached";
                        "range_to_evict" => ?&range,
                        "ranges_evicted" => ?ranges,
                        "approx_size" => approx_size,
                        "remaining" => remaining
                    );
                    remaining = remaining
                        .checked_sub(*approx_size as usize)
                        .unwrap_or_default();
                    ranges_to_delete.append(&mut ranges);
                    true
                } else {
                    false
                }
            };
            if evicted_range {
                range_stats_manager.handle_range_evicted(range);
            }
        }

        if !ranges_to_delete.is_empty() {
            if let Err(e) =
                delete_range_scheduler.schedule_force(BackgroundTask::DeleteRange(ranges_to_delete))
            {
                error!(
                    "schedule deletet range failed";
                    "err" => ?e,
                );
                assert!(tikv_util::thread_group::is_shutdown(!cfg!(test)));
            }
        }
    }

    /// Periodically load top regions.
    ///
    /// If the soft limit is exceeded, evict (some) regions no longer considered
    /// top.
    ///
    /// See: [`RangeStatsManager::collect_changes_ranges`] for
    /// algorithm details.
    fn top_regions_load_evict(&self, delete_range_scheduler: &Scheduler<BackgroundTask>) {
        if self.range_stats_manager.is_none() {
            return;
        }
        let range_stats_manager: &RangeStatsManager = self.range_stats_manager.as_ref().unwrap();
        if range_stats_manager.checking_top_regions() {
            return;
        }
        range_stats_manager.set_checking_top_regions(true);

        let curr_memory_usage = self.memory_controller.mem_usage();
        let threshold = self.memory_controller.soft_limit_threshold();
        range_stats_manager.adjust_max_num_regions(curr_memory_usage, threshold);

        let mut ranges_to_add = Vec::<CacheRange>::with_capacity(256);
        let mut ranges_to_remove = Vec::<CacheRange>::with_capacity(256);
        range_stats_manager.collect_changed_ranges(&mut ranges_to_add, &mut ranges_to_remove);
        let mut ranges_to_delete = vec![];
        info!("load_evict"; "ranges_to_add" => ?&ranges_to_add, "may_evict" => ?&ranges_to_remove);
        for evict_range in ranges_to_remove {
            if self.memory_controller.reached_soft_limit() {
                let mut core = self.engine.write();
                let mut ranges = core.mut_range_manager().evict_range(&evict_range);
                info!(
                    "load_evict: soft limit reached";
                    "range_to_evict" => ?&evict_range,
                    "ranges_evicted" => ?ranges
                );
                ranges_to_delete.append(&mut ranges);
            }
        }
        if !ranges_to_delete.is_empty() {
            if let Err(e) =
                delete_range_scheduler.schedule_force(BackgroundTask::DeleteRange(ranges_to_delete))
            {
                error!(
                    "schedule deletet range failed";
                    "err" => ?e,
                );
                assert!(tikv_util::thread_group::is_shutdown(!cfg!(test)));
            }
        }
        for cache_range in ranges_to_add {
            let mut core = self.engine.write();
            if let Err(e) = core.mut_range_manager().load_range(cache_range.clone()) {
                error!("error loading range"; "cache_range" => ?&cache_range, "err" => ?e);
            }
        }
        range_stats_manager.set_checking_top_regions(false);
        info!("load_evict complete");
    }
}

// Flush epoch and pin enough times to make the delayed operations be executed
#[cfg(test)]
pub(crate) fn flush_epoch() {
    {
        let guard = &epoch::pin();
        guard.flush();
    }
    // Local epoch tries to advance the global epoch every 128 pins. When global
    // epoch advances, the operations(here, means delete) in the older epoch can be
    // executed.
    for _ in 0..128 {
        let _ = &epoch::pin();
    }
}

pub struct BackgroundRunner {
    core: BackgroundRunnerCore,

    // We have following four separate workers so that each type of task would not block each
    // others
    range_load_remote: Remote<yatp::task::future::TaskCell>,
    range_load_worker: Worker,

    delete_range_scheduler: Scheduler<BackgroundTask>,
    delete_range_worker: Worker,

    manual_load_worker: Worker,

    gc_range_remote: Remote<yatp::task::future::TaskCell>,
    gc_range_worker: Worker,

    // Region load and eviction worker.
    // TODO: this can be consolidated, possibly with the GC worker.
    load_evict_remote: Remote<yatp::task::future::TaskCell>,
    load_evict_worker: Worker,

    lock_cleanup_remote: Remote<yatp::task::future::TaskCell>,
    lock_cleanup_worker: Worker,

    // The last sequence number for the lock cf tombstone cleanup
    last_seqno: u64,
    // RocksEngine is used to get the oldest snapshot sequence number.
    rocks_engine: Option<RocksEngine>,
}

impl Drop for BackgroundRunner {
    fn drop(&mut self) {
        self.manual_load_worker.stop();
        self.range_load_worker.stop();
        self.delete_range_worker.stop();
        self.gc_range_worker.stop();
        self.load_evict_worker.stop();
        self.lock_cleanup_worker.stop();
    }
}

impl BackgroundRunner {
    pub fn new(
        engine: Arc<RwLock<RangeCacheMemoryEngineCore>>,
        memory_controller: Arc<MemoryController>,
        region_info_provider: Option<Arc<dyn RegionInfoProvider>>,
        expected_region_size: usize,
    ) -> (Self, Scheduler<BackgroundTask>, Scheduler<BackgroundTask>) {
        let range_load_worker = Builder::new("background-range-load-worker")
            // Range load now is implemented sequentially, so we must use exactly one thread to handle it.
            // todo(SpadeA): if the load speed is a bottleneck, we may consider to use multiple threads to load ranges.
            .thread_count(1)
            .create();
        let range_load_remote = range_load_worker.remote();

        let delete_range_worker = Worker::new("background-delete-range-worker");
        let delete_range_runner = DeleteRangeRunner::new(engine.clone());
        let delete_range_scheduler =
            delete_range_worker.start_with_timer("delete-range-runner", delete_range_runner);

        let manual_load_worker = Worker::new("manual-load-worker");
        let manual_load_runner = ManualLoadRunner::new(
            region_info_provider.as_ref().unwrap().clone(),
            engine.clone(),
        );
        let manual_load_scheduler =
            manual_load_worker.start_with_timer("manual-load-runner", manual_load_runner);

        let lock_cleanup_worker = Worker::new("lock-cleanup-worker");
        let lock_cleanup_remote = lock_cleanup_worker.remote();

        let gc_range_worker = Builder::new("background-range-load-worker")
            // Gc must also use exactly one thread to handle it.
            .thread_count(1)
            .create();
        let gc_range_remote = gc_range_worker.remote();

        let load_evict_worker = Worker::new("background-region-load-evict-worker");
        let load_evict_remote = load_evict_worker.remote();

        let num_regions_to_cache = memory_controller.soft_limit_threshold() / expected_region_size;
        let range_stats_manager = region_info_provider.map(|region_info_provider| {
            RangeStatsManager::new(
                num_regions_to_cache,
                DEFAULT_EVICT_MIN_DURATION,
                expected_region_size,
                region_info_provider,
            )
        });
        (
            Self {
                core: BackgroundRunnerCore {
                    engine,
                    memory_controller,
                    range_stats_manager,
                },
                range_load_worker,
                range_load_remote,
                delete_range_worker,
                delete_range_scheduler: delete_range_scheduler.clone(),
                manual_load_worker,
                gc_range_worker,
                gc_range_remote,
                load_evict_worker,
                load_evict_remote,
                lock_cleanup_remote,
                lock_cleanup_worker,
                last_seqno: 0,
                rocks_engine: None,
            },
            delete_range_scheduler,
            manual_load_scheduler,
        )
    }
}

impl Runnable for BackgroundRunner {
    type Task = BackgroundTask;

    fn run(&mut self, task: Self::Task) {
        match task {
            BackgroundTask::SetRocksEngine(rocks_engine) => {
                self.rocks_engine = Some(rocks_engine);
                fail::fail_point!("in_memory_engine_set_rocks_engine");
            }
            BackgroundTask::Gc(t) => {
                let seqno = (|| {
                    fail::fail_point!("in_memory_engine_gc_oldest_seqno", |t| {
                        Some(t.unwrap().parse::<u64>().unwrap())
                    });

                    let Some(ref rocks_engine) = self.rocks_engine else {
                        return None;
                    };
                    let latest_seqno = rocks_engine.get_latest_sequence_number();
                    Some(
                        rocks_engine
                            .get_oldest_snapshot_sequence_number()
                            .unwrap_or(latest_seqno),
                    )
                })();

                let Some(seqno) = seqno else {
                    return;
                };

                info!(
                    "start a new round of gc for range cache engine";
                    "safe_point" => t.safe_point,
                    "oldest_sequence" => seqno,
                );
                let mut core = self.core.clone();
                if let Some(ranges) = core.ranges_for_gc() {
                    let f = async move {
                        let mut metrics = FilterMetrics::default();
                        for range in &ranges {
                            let m = core.gc_range(range, t.safe_point, seqno);
                            metrics.merge(&m);
                        }
                        core.on_gc_finished(ranges);
                        metrics.flush();
                        fail::fail_point!("in_memory_engine_gc_finish");
                    };
                    self.gc_range_remote.spawn(f);
                }
            }
            BackgroundTask::LoadRange => {
                let mut core = self.core.clone();
                let delete_range_scheduler = self.delete_range_scheduler.clone();
                let f = async move {
                    let skiplist_engine = {
                        let core = core.engine.read();
                        core.engine().clone()
                    };
                    while let Some((range, snap, mut canceled)) = core.get_range_to_load() {
                        info!("Loading range"; "range" => ?&range);
                        let iter_opt = IterOptions::new(
                            Some(KeyBuilder::from_vec(range.start.clone(), 0, 0)),
                            Some(KeyBuilder::from_vec(range.end.clone(), 0, 0)),
                            false,
                        );
                        if core.memory_controller.reached_soft_limit() {
                            // We are running out of memory, so cancel the load.
                            canceled = true;
                        }

                        if canceled {
                            info!(
                                "snapshot load canceled due to memory reaching soft limit";
                                "range" => ?range,
                            );
                            core.on_snapshot_load_canceled(range, &delete_range_scheduler);
                            continue;
                        }

                        let snapshot_load = || -> bool {
                            for &cf in DATA_CFS {
                                let handle = skiplist_engine.cf_handle(cf);
                                let seq = snap.sequence_number();
                                let guard = &epoch::pin();
                                match snap.iterator_opt(cf, iter_opt.clone()) {
                                    Ok(mut iter) => {
                                        iter.seek_to_first().unwrap();
                                        while iter.valid().unwrap() {
                                            // use the sequence number from RocksDB snapshot here as
                                            // the kv is clearly visible
                                            let mut encoded_key =
                                                encode_key(iter.key(), seq, ValueType::Value);
                                            let mut val =
                                                InternalBytes::from_vec(iter.value().to_vec());

                                            let mem_size =
                                                RangeCacheWriteBatchEntry::calc_put_entry_size(
                                                    iter.key(),
                                                    val.as_bytes(),
                                                );

                                            // todo(SpadeA): we can batch acquire the memory size
                                            // here.
                                            if let MemoryUsage::HardLimitReached(n) =
                                                core.memory_controller.acquire(mem_size)
                                            {
                                                warn!(
                                                    "stop loading snapshot due to memory reaching hard limit";
                                                    "range" => ?range,
                                                    "memory_usage(MB)" => ReadableSize(n as u64).as_mb_f64(),
                                                );
                                                return false;
                                            }

                                            encoded_key.set_memory_controller(
                                                core.memory_controller.clone(),
                                            );
                                            val.set_memory_controller(
                                                core.memory_controller.clone(),
                                            );
                                            handle.insert(encoded_key, val, guard);
                                            iter.next().unwrap();
                                        }
                                    }
                                    Err(e) => {
                                        error!("creating rocksdb iterator failed"; "cf" => cf, "err" => %e);
                                        return false;
                                    }
                                }
                            }
                            true
                        };

                        let start = Instant::now();
                        if !snapshot_load() {
                            info!(
                                "snapshot load failed";
                                "range" => ?range,
                            );
                            core.on_snapshot_load_canceled(range, &delete_range_scheduler);
                            continue;
                        }

                        if core.on_snapshot_load_finished(range.clone(), &delete_range_scheduler) {
                            let duration = start.saturating_elapsed();
                            RANGE_LOAD_TIME_HISTOGRAM.observe(duration.as_secs_f64());
                            info!(
                                "Loading range finished";
                                "range" => ?range,
                                "duration(sec)" => ?duration,
                            );
                        } else {
                            info!("Loading range canceled";"range" => ?range);
                        }
                    }
                };
                self.range_load_remote.spawn(f);
            }
            BackgroundTask::MemoryCheckAndEvict => {
                let mem_usage = self.core.memory_controller.mem_usage();
                info!(
                    "start memory usage check and evict";
                    "mem_usage(MB)" => ReadableSize(mem_usage as u64).as_mb()
                );
                if mem_usage > self.core.memory_controller.soft_limit_threshold() {
                    let delete_range_scheduler = self.delete_range_scheduler.clone();
                    let core = self.core.clone();
                    let task = async move {
                        core.evict_on_soft_limit_reached(&delete_range_scheduler);
                        core.memory_controller.set_memory_checking(false);
                    };
                    self.load_evict_remote.spawn(task);
                } else {
                    self.core.memory_controller.set_memory_checking(false);
                }
            }
            // DeleteRange task is executed by `DeleteRangeRunner` with a different scheduler so
            // that the task will not be scheduled to here.
            BackgroundTask::DeleteRange(_) => unreachable!(),
            BackgroundTask::TopRegionsLoadEvict => {
                let delete_range_scheduler = self.delete_range_scheduler.clone();
                let core = self.core.clone();
                let task = async move { core.top_regions_load_evict(&delete_range_scheduler) };
                self.load_evict_remote.spawn(task);
            }
            BackgroundTask::CleanLockTombstone(snapshot_seqno) => {
                if snapshot_seqno < self.last_seqno {
                    return;
                }
                self.last_seqno = snapshot_seqno;
                let core = self.core.clone();

                let f = async move {
                    info!(
                        "begin to cleanup tombstones in lock cf";
                        "seqno" => snapshot_seqno,
                    );

                    let mut last_user_key = vec![];
                    let mut remove_rest = false;
                    let mut cached_to_remove: Option<Vec<u8>> = None;

                    let mut removed = 0;
                    let mut total = 0;
                    let now = Instant::now();
                    let lock_handle = core.engine.read().engine().cf_handle("lock");
                    let guard = &epoch::pin();
                    let mut iter = lock_handle.iterator();
                    iter.seek_to_first(guard);
                    while iter.valid() {
                        total += 1;
                        let InternalKey {
                            user_key,
                            v_type,
                            sequence,
                        } = decode_key(iter.key().as_bytes());
                        if user_key != last_user_key {
                            if let Some(remove) = cached_to_remove.take() {
                                removed += 1;
                                lock_handle.remove(&InternalBytes::from_vec(remove), guard);
                            }
                            last_user_key = user_key.to_vec();
                            if sequence >= snapshot_seqno {
                                remove_rest = false;
                            } else {
                                remove_rest = true;
                                if v_type == ValueType::Deletion {
                                    cached_to_remove = Some(iter.key().as_bytes().to_vec());
                                }
                            }
                        } else if remove_rest {
                            assert!(sequence < snapshot_seqno);
                            removed += 1;
                            lock_handle.remove(iter.key(), guard);
                        } else if sequence < snapshot_seqno {
                            remove_rest = true;
                            if v_type == ValueType::Deletion {
                                assert!(cached_to_remove.is_none());
                                cached_to_remove = Some(iter.key().as_bytes().to_vec());
                            }
                        }

                        iter.next(guard);
                    }
                    if let Some(remove) = cached_to_remove.take() {
                        removed += 1;
                        lock_handle.remove(&InternalBytes::from_vec(remove), guard);
                    }

                    info!(
                        "cleanup tombstones in lock cf";
                        "seqno" => snapshot_seqno,
                        "total" => total,
                        "removed" => removed,
                        "duration" => ?now.saturating_elapsed(),
                        "current_count" => lock_handle.len(),
                    );

                    fail::fail_point!("clean_lock_tombstone_done");
                };

                self.lock_cleanup_remote.spawn(f);
            }
            BackgroundTask::ManualLoad(_) => unreachable!(),
        }
    }
}

impl RunnableWithTimer for BackgroundRunner {
    fn on_timeout(&mut self) {
        let mem_usage = self.core.memory_controller.mem_usage();
        RANGE_CACHE_MEMORY_USAGE.set(mem_usage as i64);

        let core = self.core.engine.read();
        let pending = core.range_manager.pending_ranges.len();
        let cached = core.range_manager.ranges().len();
        let loading = core.range_manager.pending_ranges_loading_data.len();
        let evictions = core.range_manager.get_and_reset_range_evictions();
        drop(core);
        RANGE_CACHE_COUNT
            .with_label_values(&["pending_range"])
            .set(pending as i64);
        RANGE_CACHE_COUNT
            .with_label_values(&["cached_range"])
            .set(cached as i64);
        RANGE_CACHE_COUNT
            .with_label_values(&["loading_range"])
            .set(loading as i64);
        RANGE_CACHE_COUNT
            .with_label_values(&["range_evictions"])
            .set(evictions as i64);
    }

    fn get_interval(&self) -> Duration {
        Duration::from_secs(1)
    }
}

pub struct DeleteRangeRunner {
    engine: Arc<RwLock<RangeCacheMemoryEngineCore>>,
    // It is possible that when `DeleteRangeRunner` begins to delete a range, the range is being
    // written by apply threads. In that case, we have to delay the delete range task to avoid race
    // condition between them. Periodically, these delayed ranges will be checked to see if it is
    // ready to be deleted.
    delay_ranges: Vec<CacheRange>,
}

impl DeleteRangeRunner {
    fn new(engine: Arc<RwLock<RangeCacheMemoryEngineCore>>) -> Self {
        Self {
            engine,
            delay_ranges: vec![],
        }
    }

    fn delete_ranges(&mut self, ranges: &[CacheRange]) {
        let skiplist_engine = self.engine.read().engine();
        for r in ranges {
            skiplist_engine.delete_range(r);
        }
        self.engine
            .write()
            .mut_range_manager()
            .on_delete_ranges(ranges);

        fail::fail_point!("in_memory_engine_delete_range_done");

        #[cfg(test)]
        flush_epoch();
    }
}

impl Runnable for DeleteRangeRunner {
    type Task = BackgroundTask;
    fn run(&mut self, task: Self::Task) {
        match task {
            BackgroundTask::DeleteRange(ranges) => {
                let (mut ranges_to_delay, ranges_to_delete) = {
                    let core = self.engine.read();
                    let mut ranges_to_delay = vec![];
                    let mut ranges_to_delete = vec![];
                    for r in ranges {
                        // If the range is overlapped with ranges in `ranges_being_written`, the
                        // range has to be delayed to delete. See comment on `delay_ranges`.
                        if core
                            .range_manager
                            .is_overlapped_with_ranges_being_written(&r)
                        {
                            ranges_to_delay.push(r);
                        } else {
                            ranges_to_delete.push(r);
                        }
                    }
                    (ranges_to_delay, ranges_to_delete)
                };
                self.delay_ranges.append(&mut ranges_to_delay);
                if !ranges_to_delete.is_empty() {
                    self.delete_ranges(&ranges_to_delete);
                }
            }
            _ => unreachable!(),
        }
    }
}

impl RunnableWithTimer for DeleteRangeRunner {
    fn on_timeout(&mut self) {
        if self.delay_ranges.is_empty() {
            return;
        }
        let ranges = std::mem::take(&mut self.delay_ranges);
        self.run(BackgroundTask::DeleteRange(ranges));
    }

    fn get_interval(&self) -> Duration {
        Duration::from_millis(500)
    }
}

pub struct ManualLoadRunner {
    ranges: BTreeSet<CacheRange>,
    region_info_provider: Arc<dyn RegionInfoProvider>,
    engine: Arc<RwLock<RangeCacheMemoryEngineCore>>,
}

impl ManualLoadRunner {
    fn new(
        region_info_provider: Arc<dyn RegionInfoProvider>,
        engine: Arc<RwLock<RangeCacheMemoryEngineCore>>,
    ) -> Self {
        Self {
            ranges: BTreeSet::default(),
            region_info_provider,
            engine,
        }
    }

    fn load_regions_in_range(&self, range: &CacheRange) {
        match self.region_info_provider.get_regions_in_range(
            &range.start[1..],
            &range.end[1..],
            true,
        ) {
            Ok(regions) => {
                let mut core = self.engine.write();
                let range_manager = core.mut_range_manager();
                for r in &regions {
                    let range_to_load = CacheRange::from_region(r);
                    if let Err(e) = range_manager.load_range(range_to_load.clone()) {
                        info!("manual load range failed"; "cache_range" => ?&range_to_load, "err" => ?e);
                    } else {
                        info!("manual load range succeed"; "cache_range" => ?&range_to_load);
                    }
                }
            }
            Err(e) => {
                error!(
                    "get regions in range failed";
                    "error" => ?e,
                );
            }
        }
    }
}

impl Runnable for ManualLoadRunner {
    type Task = BackgroundTask;

    fn run(&mut self, task: Self::Task) {
        match task {
            BackgroundTask::ManualLoad(r) => {
                self.load_regions_in_range(&r);
                self.ranges.insert(r);
            }
            _ => unreachable!(),
        }
    }
}

impl RunnableWithTimer for ManualLoadRunner {
    fn on_timeout(&mut self) {
        for r in &self.ranges {
            self.load_regions_in_range(r);
        }
    }

    fn get_interval(&self) -> Duration {
        Duration::from_secs(20)
    }
}

#[derive(Default)]
struct FilterMetrics {
    total: usize,
    versions: usize,
    delete_versions: usize,
    filtered: usize,
    unique_key: usize,
    mvcc_rollback_and_locks: usize,
}

impl FilterMetrics {
    fn merge(&mut self, other: &FilterMetrics) {
        self.total += other.total;
        self.versions += other.versions;
        self.delete_versions += other.delete_versions;
        self.filtered += other.filtered;
        self.unique_key += other.unique_key;
        self.mvcc_rollback_and_locks += other.mvcc_rollback_and_locks;
    }

    fn flush(&self) {
        GC_FILTERED_STATIC.total.inc_by(self.total as u64);
        GC_FILTERED_STATIC
            .below_safe_point_total
            .inc_by(self.versions as u64);
        GC_FILTERED_STATIC.filtered.inc_by(self.filtered as u64);
        GC_FILTERED_STATIC
            .below_safe_point_unique
            .inc_by(self.unique_key as u64);
    }
}

struct Filter {
    safe_point: u64,
    oldest_seqno: u64,
    mvcc_key_prefix: Vec<u8>,
    remove_older: bool,

    default_cf_handle: SkiplistHandle,
    write_cf_handle: SkiplistHandle,

    // When deleting some keys, the latest one should be deleted at last to avoid the older
    // version appears.
    cached_mvcc_delete_key: Option<Vec<u8>>,
    cached_skiplist_delete_key: Option<Vec<u8>>,

    metrics: FilterMetrics,

    last_user_key: Vec<u8>,
}

impl Drop for Filter {
    fn drop(&mut self) {
        if let Some(cached_delete_key) = self.cached_mvcc_delete_key.take() {
            let guard = &epoch::pin();
            self.write_cf_handle
                .remove(&InternalBytes::from_vec(cached_delete_key), guard);
        }
        if let Some(cached_delete_key) = self.cached_skiplist_delete_key.take() {
            let guard = &epoch::pin();
            self.write_cf_handle
                .remove(&InternalBytes::from_vec(cached_delete_key), guard);
        }
    }
}

impl Filter {
    fn new(
        safe_point: u64,
        oldest_seqno: u64,
        default_cf_handle: SkiplistHandle,
        write_cf_handle: SkiplistHandle,
    ) -> Self {
        Self {
            safe_point,
            oldest_seqno,
            default_cf_handle,
            write_cf_handle,
            mvcc_key_prefix: vec![],
            cached_mvcc_delete_key: None,
            cached_skiplist_delete_key: None,
            remove_older: false,
            metrics: FilterMetrics::default(),
            last_user_key: vec![],
        }
    }

    fn filter(&mut self, key: &Bytes, value: &Bytes) -> Result<(), String> {
        self.metrics.total += 1;
        let InternalKey {
            user_key,
            v_type,
            sequence,
        } = decode_key(key);

        if sequence > self.oldest_seqno {
            // skip those under read by some snapshots
            return Ok(());
        }

        let (mvcc_key_prefix, commit_ts) = split_ts(user_key)?;
        if commit_ts > self.safe_point {
            return Ok(());
        }

        // Just like what rocksdb compaction filter does, we do not handle internal
        // keys (representing different MVCC versions of the same user key) that have
        // been marked as tombstones. However, these keys need to be deleted. Since they
        // are below the safe point, we can safely delete them directly now.
        // For each user key, we cache the first ValueType::Deletion and delete all the
        // older internal keys of the same user keys. The cached ValueType::Delete is
        // deleted at last to avoid these older keys visible.
        if v_type == ValueType::Deletion {
            if let Some(cache_skiplist_delete_key) = self.cached_skiplist_delete_key.take() {
                // Reaching here in two cases:
                // 1. There are two ValueType::Deletion in the same user key.
                // 2. Two consecutive ValueType::Deletion of different user keys.
                // In either cases, we can delete the previous one directly.
                let guard = &epoch::pin();
                self.write_cf_handle
                    .remove(&InternalBytes::from_vec(cache_skiplist_delete_key), guard)
            }
            self.cached_skiplist_delete_key = Some(key.to_vec());
            return Ok(());
        } else if let Some(ref cache_skiplist_delete_key) = self.cached_skiplist_delete_key {
            let InternalKey {
                user_key: cache_skiplist_delete_user_key,
                ..
            } = decode_key(cache_skiplist_delete_key);
            let guard = &epoch::pin();
            if cache_skiplist_delete_user_key == user_key {
                self.write_cf_handle
                    .remove(&InternalBytes::from_bytes(key.clone()), guard);
                return Ok(());
            } else {
                self.write_cf_handle.remove(
                    &InternalBytes::from_vec(self.cached_skiplist_delete_key.take().unwrap()),
                    guard,
                )
            }
        }

        let guard = &epoch::pin();
        // Also, we only handle the same user_key once (user_key here refers to the key
        // with MVCC version but without sequence number).
        if user_key != self.last_user_key {
            self.last_user_key = user_key.to_vec();
        } else {
            self.write_cf_handle
                .remove(&InternalBytes::from_bytes(key.clone()), guard);
            return Ok(());
        }

        self.metrics.versions += 1;
        if self.mvcc_key_prefix != mvcc_key_prefix {
            self.metrics.unique_key += 1;
            self.mvcc_key_prefix.clear();
            self.mvcc_key_prefix.extend_from_slice(mvcc_key_prefix);
            self.remove_older = false;
            if let Some(cached_delete_key) = self.cached_mvcc_delete_key.take() {
                self.write_cf_handle
                    .remove(&InternalBytes::from_vec(cached_delete_key), guard);
            }
        }

        let mut filtered = self.remove_older;
        let write = parse_write(value)?;
        if !self.remove_older {
            match write.write_type {
                WriteType::Rollback | WriteType::Lock => {
                    self.metrics.mvcc_rollback_and_locks += 1;
                    filtered = true;
                }
                WriteType::Put => self.remove_older = true,
                WriteType::Delete => {
                    self.metrics.delete_versions += 1;
                    self.remove_older = true;

                    // The first mvcc type below safe point is the mvcc delete. We should delay to
                    // remove it until all the followings with the same user key have been deleted
                    // to avoid older version apper.
                    self.cached_mvcc_delete_key = Some(key.to_vec());
                }
            }
        }

        if !filtered {
            return Ok(());
        }
        self.metrics.filtered += 1;
        self.write_cf_handle
            .remove(&InternalBytes::from_bytes(key.clone()), guard);
        self.handle_filtered_write(write, guard)?;

        Ok(())
    }

    fn handle_filtered_write(
        &mut self,
        write: WriteRef<'_>,
        guard: &epoch::Guard,
    ) -> std::result::Result<(), String> {
        if write.short_value.is_none() && write.write_type == WriteType::Put {
            // todo(SpadeA): We don't know the sequence number of the key in the skiplist so
            // we cannot delete it directly. So we encoding a key with MAX sequence number
            // so we can find the mvcc key with sequence number in the skiplist by using
            // get_with_key and delete it with the result key. It involes more than one
            // seek(both get and remove invovle seek). Maybe we can provide the API to
            // delete the mvcc keys with all sequence numbers.
            let default_key = encoding_for_filter(&self.mvcc_key_prefix, write.start_ts);
            let mut iter = self.default_cf_handle.iterator();
            iter.seek(&default_key, guard);
            while iter.valid() && iter.key().same_user_key_with(&default_key) {
                self.default_cf_handle.remove(iter.key(), guard);
                iter.next(guard);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use std::{
        sync::{
            mpsc::{channel, Sender},
            Arc, Mutex,
        },
        time::Duration,
    };

    use crossbeam::epoch;
    use engine_rocks::util::new_engine;
    use engine_traits::{
        CacheRange, IterOptions, Iterable, Iterator, RangeCacheEngine, SyncMutable, CF_DEFAULT,
        CF_LOCK, CF_WRITE, DATA_CFS,
    };
    use futures::future::ready;
    use keys::{data_key, DATA_MAX_KEY, DATA_MIN_KEY};
    use online_config::{ConfigChange, ConfigManager, ConfigValue};
    use pd_client::PdClient;
    use tempfile::Builder;
    use tikv_util::{
        config::{ReadableDuration, ReadableSize, VersionTrack},
        worker::dummy_scheduler,
    };
    use txn_types::{Key, TimeStamp, Write, WriteType};

    use super::*;
    use crate::{
        background::BackgroundRunner,
        config::RangeCacheConfigManager,
        engine::{SkiplistEngine, SkiplistHandle},
        keys::{
            construct_key, construct_user_key, construct_value, encode_key, encode_seek_key,
            encoding_for_filter, InternalBytes, ValueType,
        },
        memory_controller::MemoryController,
        region_label::{
            region_label_meta_client,
            tests::{add_region_label_rule, new_region_label_rule, new_test_server_and_client},
        },
        test_util::{put_data, put_data_with_overwrite},
        write_batch::RangeCacheWriteBatchEntry,
        RangeCacheEngineConfig, RangeCacheEngineContext, RangeCacheMemoryEngine,
    };

    fn delete_data(
        key: &[u8],
        ts: u64,
        seq_num: u64,
        write_cf: &SkiplistHandle,
        mem_controller: Arc<MemoryController>,
    ) {
        let raw_write_k = Key::from_raw(key)
            .append_ts(TimeStamp::new(ts))
            .into_encoded();
        let mut write_k = encode_key(&raw_write_k, seq_num, ValueType::Value);
        write_k.set_memory_controller(mem_controller.clone());
        let write_v = Write::new(WriteType::Delete, TimeStamp::new(ts), None);
        let mut val = InternalBytes::from_vec(write_v.as_ref().to_bytes());
        val.set_memory_controller(mem_controller.clone());
        let guard = &epoch::pin();
        let _ = mem_controller.acquire(RangeCacheWriteBatchEntry::calc_put_entry_size(
            &raw_write_k,
            val.as_bytes(),
        ));
        write_cf.insert(write_k, val, guard);
    }

    fn rollback_data(
        key: &[u8],
        ts: u64,
        seq_num: u64,
        write_cf: &SkiplistHandle,
        mem_controller: Arc<MemoryController>,
    ) {
        let raw_write_k = Key::from_raw(key)
            .append_ts(TimeStamp::new(ts))
            .into_encoded();
        let mut write_k = encode_key(&raw_write_k, seq_num, ValueType::Value);
        write_k.set_memory_controller(mem_controller.clone());
        let write_v = Write::new(WriteType::Rollback, TimeStamp::new(ts), None);
        let mut val = InternalBytes::from_vec(write_v.as_ref().to_bytes());
        val.set_memory_controller(mem_controller.clone());
        let guard = &epoch::pin();
        let _ = mem_controller.acquire(RangeCacheWriteBatchEntry::calc_put_entry_size(
            &raw_write_k,
            val.as_bytes(),
        ));
        write_cf.insert(write_k, val, guard);
    }

    fn element_count(sklist: &SkiplistHandle) -> u64 {
        let guard = &epoch::pin();
        let mut count = 0;
        let mut iter = sklist.iterator();
        iter.seek_to_first(guard);
        while iter.valid() {
            count += 1;
            iter.next(guard);
        }
        count
    }

    // We should not use skiplist.get directly as we only cares keys without
    // sequence number suffix
    fn key_exist(sl: &SkiplistHandle, key: &InternalBytes, guard: &epoch::Guard) -> bool {
        let mut iter = sl.iterator();
        iter.seek(key, guard);
        if iter.valid() && iter.key().same_user_key_with(key) {
            return true;
        }
        false
    }

    // We should not use skiplist.get directly as we only cares keys without
    // sequence number suffix
    fn get_value(
        sl: &SkiplistHandle,
        key: &InternalBytes,
        guard: &epoch::Guard,
    ) -> Option<Vec<u8>> {
        let mut iter = sl.iterator();
        iter.seek(key, guard);
        if iter.valid() && iter.key().same_user_key_with(key) {
            return Some(iter.value().as_slice().to_vec());
        }
        None
    }

    fn dummy_controller(skip_engine: SkiplistEngine) -> Arc<MemoryController> {
        let mut config = RangeCacheEngineConfig::config_for_test();
        config.soft_limit_threshold = Some(ReadableSize(u64::MAX));
        config.hard_limit_threshold = Some(ReadableSize(u64::MAX));
        let config = Arc::new(VersionTrack::new(config));
        Arc::new(MemoryController::new(config, skip_engine))
    }

    fn encode_raw_key_for_filter(key: &[u8], ts: TimeStamp) -> InternalBytes {
        let key = Key::from_raw(key);
        encoding_for_filter(key.as_encoded(), ts)
    }

    #[test]
    fn test_filter() {
        let skiplist_engine = SkiplistEngine::new();
        let write = skiplist_engine.cf_handle(CF_WRITE);
        let default = skiplist_engine.cf_handle(CF_DEFAULT);

        let memory_controller = dummy_controller(skiplist_engine.clone());

        put_data(
            b"key1",
            b"value1",
            10,
            15,
            10,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );
        put_data(
            b"key2",
            b"value21",
            10,
            15,
            12,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );
        put_data(
            b"key2",
            b"value22",
            20,
            25,
            14,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );
        // mock repeate apply
        put_data(
            b"key2",
            b"value22",
            20,
            25,
            15,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );
        put_data(
            b"key2",
            b"value23",
            30,
            35,
            16,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );
        put_data(
            b"key3",
            b"value31",
            20,
            25,
            18,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );
        put_data(
            b"key3",
            b"value32",
            30,
            35,
            20,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );
        delete_data(b"key3", 40, 22, &write, memory_controller.clone());
        assert_eq!(7, element_count(&default));
        assert_eq!(8, element_count(&write));

        let mut filter = Filter::new(50, 100, default.clone(), write.clone());
        let mut count = 0;
        let mut iter = write.iterator();
        let guard = &epoch::pin();
        iter.seek_to_first(guard);
        while iter.valid() {
            let k = iter.key();
            let v = iter.value();
            filter.filter(k.as_bytes(), v.as_bytes()).unwrap();
            count += 1;
            iter.next(guard);
        }
        assert_eq!(count, 8);
        drop(filter);

        assert_eq!(2, element_count(&write));
        assert_eq!(2, element_count(&default));

        let key = encode_raw_key_for_filter(b"key1", TimeStamp::new(15));
        assert!(key_exist(&write, &key, guard));

        let key = encode_raw_key_for_filter(b"key2", TimeStamp::new(35));
        assert!(key_exist(&write, &key, guard));

        let key = encode_raw_key_for_filter(b"key3", TimeStamp::new(35));
        assert!(!key_exist(&write, &key, guard));

        let key = encode_raw_key_for_filter(b"key1", TimeStamp::new(10));
        assert!(key_exist(&default, &key, guard));

        let key = encode_raw_key_for_filter(b"key2", TimeStamp::new(30));
        assert!(key_exist(&default, &key, guard));

        let key = encode_raw_key_for_filter(b"key3", TimeStamp::new(30));
        assert!(!key_exist(&default, &key, guard));
    }

    #[test]
    fn test_filter_with_delete() {
        let engine = RangeCacheMemoryEngine::new(RangeCacheEngineContext::new_for_tests(Arc::new(
            VersionTrack::new(RangeCacheEngineConfig::config_for_test()),
        )));
        let memory_controller = engine.memory_controller();
        let range = CacheRange::new(b"".to_vec(), b"z".to_vec());
        engine.new_range(range.clone());
        let (write, default) = {
            let skiplist_engine = engine.core().write().engine();
            (
                skiplist_engine.cf_handle(CF_WRITE),
                skiplist_engine.cf_handle(CF_DEFAULT),
            )
        };

        put_data(
            b"key1",
            b"value11",
            10,
            15,
            10,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );

        // Delete the above key
        let guard = &epoch::pin();
        let raw_write_k = Key::from_raw(b"key1")
            .append_ts(TimeStamp::new(15))
            .into_encoded();
        let mut write_k = encode_key(&raw_write_k, 15, ValueType::Deletion);
        write_k.set_memory_controller(memory_controller.clone());
        let mut val = InternalBytes::from_vec(b"".to_vec());
        val.set_memory_controller(memory_controller.clone());
        write.insert(write_k, val, guard);

        put_data(
            b"key2",
            b"value22",
            20,
            25,
            14,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );

        // Delete the above key
        let raw_write_k = Key::from_raw(b"key2")
            .append_ts(TimeStamp::new(25))
            .into_encoded();
        let mut write_k = encode_key(&raw_write_k, 15, ValueType::Deletion);
        write_k.set_memory_controller(memory_controller.clone());
        let mut val = InternalBytes::from_vec(b"".to_vec());
        val.set_memory_controller(memory_controller.clone());
        write.insert(write_k, val, guard);

        put_data(
            b"key2",
            b"value23",
            30,
            35,
            16,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );
        delete_data(b"key2", 40, 18, &write, memory_controller.clone());

        let snap = engine.snapshot(range.clone(), u64::MAX, u64::MAX).unwrap();
        let mut iter_opts = IterOptions::default();
        iter_opts.set_lower_bound(&range.start, 0);
        iter_opts.set_upper_bound(&range.end, 0);

        let (worker, _) = BackgroundRunner::new(
            engine.core.clone(),
            memory_controller.clone(),
            None,
            engine.expected_region_size(),
        );
        worker.core.gc_range(&range, 40, 100);

        let mut iter = snap.iterator_opt("write", iter_opts).unwrap();
        iter.seek_to_first().unwrap();
        assert!(!iter.valid().unwrap());

        let mut iter = write.iterator();
        iter.seek_to_first(guard);
        assert!(!iter.valid());
    }

    #[test]
    fn test_gc() {
        let engine = RangeCacheMemoryEngine::new(RangeCacheEngineContext::new_for_tests(Arc::new(
            VersionTrack::new(RangeCacheEngineConfig::config_for_test()),
        )));
        let memory_controller = engine.memory_controller();
        let range = CacheRange::new(b"".to_vec(), b"z".to_vec());
        engine.new_range(range.clone());
        let (write, default) = {
            let skiplist_engine = engine.core().write().engine();
            (
                skiplist_engine.cf_handle(CF_WRITE),
                skiplist_engine.cf_handle(CF_DEFAULT),
            )
        };

        let encode_key = |key, ts| {
            let key = Key::from_raw(key);
            encoding_for_filter(key.as_encoded(), ts)
        };

        put_data(
            b"key1",
            b"value1",
            10,
            11,
            10,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );
        put_data(
            b"key1",
            b"value2",
            12,
            13,
            12,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );
        put_data(
            b"key1",
            b"value3",
            14,
            15,
            14,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );
        assert_eq!(3, element_count(&default));
        assert_eq!(3, element_count(&write));

        let (worker, _) = BackgroundRunner::new(
            engine.core.clone(),
            memory_controller.clone(),
            None,
            engine.expected_region_size(),
        );

        // gc should not hanlde keys with larger seqno than oldest seqno
        worker.core.gc_range(&range, 13, 10);
        assert_eq!(3, element_count(&default));
        assert_eq!(3, element_count(&write));

        // gc will not remove the latest mvcc put below safe point
        worker.core.gc_range(&range, 14, 100);
        assert_eq!(2, element_count(&default));
        assert_eq!(2, element_count(&write));

        worker.core.gc_range(&range, 16, 100);
        assert_eq!(1, element_count(&default));
        assert_eq!(1, element_count(&write));

        // rollback will not make the first older version be filtered
        rollback_data(b"key1", 17, 16, &write, memory_controller.clone());
        worker.core.gc_range(&range, 17, 100);
        assert_eq!(1, element_count(&default));
        assert_eq!(1, element_count(&write));
        let key = encode_key(b"key1", TimeStamp::new(15));
        let guard = &epoch::pin();
        assert!(key_exist(&write, &key, guard));
        let key = encode_key(b"key1", TimeStamp::new(14));
        assert!(key_exist(&default, &key, guard));

        // unlike in WriteCompactionFilter, the latest mvcc delete below safe point will
        // be filtered
        delete_data(b"key1", 19, 18, &write, memory_controller.clone());
        worker.core.gc_range(&range, 19, 100);
        assert_eq!(0, element_count(&write));
        assert_eq!(0, element_count(&default));
    }

    // The GC of one range should not impact other ranges
    #[test]
    fn test_gc_one_range() {
        let config = RangeCacheEngineConfig::config_for_test();
        let engine = RangeCacheMemoryEngine::new(RangeCacheEngineContext::new_for_tests(Arc::new(
            VersionTrack::new(config),
        )));
        let memory_controller = engine.memory_controller();
        let (write, default, range1, range2) = {
            let mut core = engine.core().write();

            let start1 = Key::from_raw(b"k00").into_encoded();
            let end1 = Key::from_raw(b"k10").into_encoded();
            let range1 = CacheRange::new(start1, end1);
            core.mut_range_manager().new_range(range1.clone());

            let start2 = Key::from_raw(b"k30").into_encoded();
            let end2 = Key::from_raw(b"k40").into_encoded();
            let range2 = CacheRange::new(start2, end2);
            core.mut_range_manager().new_range(range2.clone());

            let engine = core.engine();
            (
                engine.cf_handle(CF_WRITE),
                engine.cf_handle(CF_DEFAULT),
                range1,
                range2,
            )
        };

        put_data(
            b"k05",
            b"val1",
            10,
            11,
            10,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );

        put_data(
            b"k05",
            b"val2",
            12,
            13,
            14,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );

        put_data(
            b"k05",
            b"val1",
            14,
            15,
            18,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );

        put_data(
            b"k35",
            b"val1",
            10,
            11,
            12,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );

        put_data(
            b"k35",
            b"val2",
            12,
            13,
            16,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );

        put_data(
            b"k35",
            b"val1",
            14,
            15,
            20,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );

        let encode_key = |key, commit_ts, seq_num| -> InternalBytes {
            let raw_write_k = Key::from_raw(key)
                .append_ts(TimeStamp::new(commit_ts))
                .into_encoded();
            encode_key(&raw_write_k, seq_num, ValueType::Value)
        };

        let verify = |key, mvcc, seq, handle: &SkiplistHandle| {
            let guard = &epoch::pin();
            let key = encode_key(key, mvcc, seq);
            let mut iter = handle.iterator();
            iter.seek(&key, guard);
            assert_eq!(iter.key(), &key);
            iter.next(guard);
            assert!(!iter.valid() || !iter.key().same_user_key_with(&key));
        };

        assert_eq!(6, element_count(&default));
        assert_eq!(6, element_count(&write));

        let (worker, _) = BackgroundRunner::new(
            engine.core.clone(),
            memory_controller.clone(),
            None,
            engine.expected_region_size(),
        );
        worker.core.gc_range(&range1, 100, 100);

        verify(b"k05", 15, 18, &write);
        verify(b"k05", 14, 19, &default);

        assert_eq!(4, element_count(&default));
        assert_eq!(4, element_count(&write));

        let (worker, _) = BackgroundRunner::new(
            engine.core.clone(),
            memory_controller.clone(),
            None,
            engine.expected_region_size(),
        );
        worker.core.gc_range(&range2, 100, 100);

        verify(b"k35", 15, 20, &write);
        verify(b"k35", 14, 21, &default);

        assert_eq!(2, element_count(&default));
        assert_eq!(2, element_count(&write));
    }

    #[test]
    fn test_gc_for_overwrite_write() {
        let engine = RangeCacheMemoryEngine::new(RangeCacheEngineContext::new_for_tests(Arc::new(
            VersionTrack::new(RangeCacheEngineConfig::config_for_test()),
        )));
        let memory_controller = engine.memory_controller();
        let range = CacheRange::new(b"".to_vec(), b"z".to_vec());
        engine.new_range(range.clone());
        let (write, default) = {
            let skiplist_engine = engine.core().write().engine();
            (
                skiplist_engine.cf_handle(CF_WRITE),
                skiplist_engine.cf_handle(CF_DEFAULT),
            )
        };

        put_data_with_overwrite(
            b"key1",
            b"value1",
            10,
            11,
            100,
            101,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );

        assert_eq!(1, element_count(&default));
        assert_eq!(2, element_count(&write));

        let (worker, _) = BackgroundRunner::new(
            engine.core.clone(),
            memory_controller.clone(),
            None,
            engine.expected_region_size(),
        );

        worker.core.gc_range(&range, 20, 200);
        assert_eq!(1, element_count(&default));
        assert_eq!(1, element_count(&write));
    }

    #[test]
    fn test_snapshot_block_gc() {
        let engine = RangeCacheMemoryEngine::new(RangeCacheEngineContext::new_for_tests(Arc::new(
            VersionTrack::new(RangeCacheEngineConfig::config_for_test()),
        )));
        let memory_controller = engine.memory_controller();
        let range = CacheRange::new(b"".to_vec(), b"z".to_vec());
        engine.new_range(range.clone());
        let (write, default) = {
            let skiplist_engine = engine.core().write().engine();
            (
                skiplist_engine.cf_handle(CF_WRITE),
                skiplist_engine.cf_handle(CF_DEFAULT),
            )
        };

        put_data(
            b"key1",
            b"value1",
            10,
            11,
            10,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );
        put_data(
            b"key2",
            b"value21",
            10,
            11,
            12,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );
        put_data(
            b"key2",
            b"value22",
            15,
            16,
            14,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );
        put_data(
            b"key2",
            b"value23",
            20,
            21,
            16,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );
        put_data(
            b"key3",
            b"value31",
            5,
            6,
            18,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );
        put_data(
            b"key3",
            b"value32",
            10,
            11,
            20,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );
        assert_eq!(6, element_count(&default));
        assert_eq!(6, element_count(&write));

        let (worker, _) = BackgroundRunner::new(
            engine.core.clone(),
            memory_controller,
            None,
            engine.expected_region_size(),
        );
        let s1 = engine.snapshot(range.clone(), 10, u64::MAX);
        let s2 = engine.snapshot(range.clone(), 11, u64::MAX);
        let s3 = engine.snapshot(range.clone(), 20, u64::MAX);

        // nothing will be removed due to snapshot 5
        worker.core.gc_range(&range, 30, 100);
        assert_eq!(6, element_count(&default));
        assert_eq!(6, element_count(&write));

        drop(s1);
        worker.core.gc_range(&range, 30, 100);
        assert_eq!(5, element_count(&default));
        assert_eq!(5, element_count(&write));

        drop(s2);
        worker.core.gc_range(&range, 30, 100);
        assert_eq!(4, element_count(&default));
        assert_eq!(4, element_count(&write));

        drop(s3);
        worker.core.gc_range(&range, 30, 100);
        assert_eq!(3, element_count(&default));
        assert_eq!(3, element_count(&write));
    }

    #[test]
    fn test_background_worker_load() {
        let mut engine = RangeCacheMemoryEngine::new(RangeCacheEngineContext::new_for_tests(
            Arc::new(VersionTrack::new(RangeCacheEngineConfig::config_for_test())),
        ));
        let path = Builder::new().prefix("test_load").tempdir().unwrap();
        let path_str = path.path().to_str().unwrap();
        let rocks_engine = new_engine(path_str, DATA_CFS).unwrap();
        engine.set_disk_engine(rocks_engine.clone());

        for i in 10..20 {
            let key = construct_key(i, 1);
            let key = data_key(&key);
            let value = construct_value(i, i);
            rocks_engine
                .put_cf(CF_DEFAULT, &key, value.as_bytes())
                .unwrap();
            rocks_engine
                .put_cf(CF_WRITE, &key, value.as_bytes())
                .unwrap();
        }

        let k = format!("zk{:08}", 15).into_bytes();
        let r1 = CacheRange::new(DATA_MIN_KEY.to_vec(), k.clone());
        let r2 = CacheRange::new(k, DATA_MAX_KEY.to_vec());
        {
            let mut core = engine.core.write();
            core.mut_range_manager().pending_ranges.push(r1.clone());
            core.mut_range_manager().pending_ranges.push(r2.clone());
        }
        engine.prepare_for_apply(1, &r1);
        engine.prepare_for_apply(1, &r2);

        // concurrent write to rocksdb, but the key will not be loaded in the memory
        // engine
        let key = construct_key(20, 1);
        let key20 = data_key(&key);
        let value = construct_value(20, 20);
        rocks_engine
            .put_cf(CF_DEFAULT, &key20, value.as_bytes())
            .unwrap();
        rocks_engine
            .put_cf(CF_WRITE, &key20, value.as_bytes())
            .unwrap();

        let (write, default) = {
            let core = engine.core().write();
            let skiplist_engine = core.engine();
            (
                skiplist_engine.cf_handle(CF_WRITE),
                skiplist_engine.cf_handle(CF_DEFAULT),
            )
        };

        // wait for background load
        std::thread::sleep(Duration::from_secs(1));

        let _ = engine.snapshot(r1, u64::MAX, u64::MAX).unwrap();
        let _ = engine.snapshot(r2, u64::MAX, u64::MAX).unwrap();

        let guard = &epoch::pin();
        for i in 10..20 {
            let key = construct_key(i, 1);
            let key = data_key(&key);
            let value = construct_value(i, i);
            let key = encode_seek_key(&key, u64::MAX);
            assert_eq!(
                get_value(&write, &key, guard).unwrap().as_slice(),
                value.as_bytes()
            );
            assert_eq!(
                get_value(&default, &key, guard).unwrap().as_slice(),
                value.as_bytes()
            );
        }

        let key20 = encode_seek_key(&key20, u64::MAX);
        assert!(!key_exist(&write, &key20, guard));
        assert!(!key_exist(&default, &key20, guard));
    }

    #[test]
    fn test_ranges_for_gc() {
        let engine = RangeCacheMemoryEngine::new(RangeCacheEngineContext::new_for_tests(Arc::new(
            VersionTrack::new(RangeCacheEngineConfig::config_for_test()),
        )));
        let memory_controller = engine.memory_controller();
        let r1 = CacheRange::new(b"a".to_vec(), b"b".to_vec());
        let r2 = CacheRange::new(b"b".to_vec(), b"c".to_vec());
        engine.new_range(r1);
        engine.new_range(r2);

        let (mut runner, _) = BackgroundRunner::new(
            engine.core.clone(),
            memory_controller,
            None,
            engine.expected_region_size(),
        );
        let ranges = runner.core.ranges_for_gc().unwrap();
        assert_eq!(2, ranges.len());

        // until the previous gc finished, node ranges will be returned
        assert!(runner.core.ranges_for_gc().is_none());
        runner.core.on_gc_finished(ranges);

        let ranges = runner.core.ranges_for_gc().unwrap();
        assert_eq!(2, ranges.len());
    }

    // Test creating and loading cache hint using a region label rule:
    // 1. Insert some data into rocks engine, which is set as disk engine for the
    //    memory engine.
    // 2. Use test pd client server to create a label rule for portion of the data.
    // 3. Wait until data is loaded.
    // 4. Verify that only the labeled key range has been loaded.
    #[test]
    fn test_load_from_pd_hint_service() {
        let mut engine = RangeCacheMemoryEngine::new(RangeCacheEngineContext::new_for_tests(
            Arc::new(VersionTrack::new(RangeCacheEngineConfig::config_for_test())),
        ));
        let path = Builder::new()
            .prefix("test_load_from_pd_hint_service")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();
        let rocks_engine = new_engine(path_str, DATA_CFS).unwrap();
        engine.set_disk_engine(rocks_engine.clone());

        for i in 10..20 {
            let key = construct_key(i, 1);
            let key = data_key(&key);
            let value = construct_value(i, i);
            rocks_engine
                .put_cf(CF_DEFAULT, &key, value.as_bytes())
                .unwrap();
            rocks_engine
                .put_cf(CF_WRITE, &key, value.as_bytes())
                .unwrap();
        }

        let (mut pd_server, pd_client) = new_test_server_and_client(ReadableDuration::millis(100));
        let cluster_id = pd_client.get_cluster_id().unwrap();
        let pd_client = Arc::new(pd_client);
        engine.start_hint_service(PdRangeHintService::from(pd_client.clone()));
        let meta_client = region_label_meta_client(pd_client.clone());
        let label_rule = new_region_label_rule(
            "cache/0",
            &hex::encode(format!("k{:08}", 10).into_bytes()),
            &hex::encode(format!("k{:08}", 15).into_bytes()),
        );
        add_region_label_rule(meta_client, cluster_id, &label_rule);

        // Wait for the watch to fire.
        std::thread::sleep(Duration::from_millis(200));
        let r1 = CacheRange::try_from(&label_rule.data[0]).unwrap();
        engine.prepare_for_apply(1, &r1);

        // Wait for the range to be loaded.
        std::thread::sleep(Duration::from_secs(1));
        let _ = engine.snapshot(r1, u64::MAX, u64::MAX).unwrap();

        let (write, default) = {
            let core = engine.core().write();
            let skiplist_engine = core.engine();
            (
                skiplist_engine.cf_handle(CF_WRITE),
                skiplist_engine.cf_handle(CF_DEFAULT),
            )
        };

        let guard = &epoch::pin();
        for i in 10..15 {
            let key = construct_key(i, 1);
            let key = data_key(&key);
            let value = construct_value(i, i);
            let key = encode_seek_key(&key, u64::MAX);
            assert_eq!(
                get_value(&write, &key, guard).unwrap().as_slice(),
                value.as_bytes()
            );
            assert_eq!(
                get_value(&default, &key, guard).unwrap().as_slice(),
                value.as_bytes()
            );
        }
        for i in 15..=20 {
            let key = construct_key(i, 1);
            let key = data_key(&key);
            let key = encode_seek_key(&key, u64::MAX);
            assert!(!key_exist(&write, &key, guard));
            assert!(!key_exist(&default, &key, guard));
        }

        pd_server.stop();
    }

    #[test]
    fn test_snapshot_load_reaching_limit() {
        let mut config = RangeCacheEngineConfig::config_for_test();
        config.soft_limit_threshold = Some(ReadableSize(1000));
        config.hard_limit_threshold = Some(ReadableSize(1500));
        let config = Arc::new(VersionTrack::new(config));
        let mut engine =
            RangeCacheMemoryEngine::new(RangeCacheEngineContext::new_for_tests(config));
        let path = Builder::new()
            .prefix("test_snapshot_load_reaching_limit")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();
        let rocks_engine = new_engine(path_str, DATA_CFS).unwrap();
        engine.set_disk_engine(rocks_engine.clone());
        let mem_controller = engine.memory_controller();

        let range1 = CacheRange::new(construct_user_key(1), construct_user_key(3));
        // Memory for one put is 17(key) + 3(val) + 8(Seqno) + 16(Memory controller in
        // key and val) + 96(Node overhead) = 140
        let key = construct_key(1, 10);
        rocks_engine.put_cf(CF_DEFAULT, &key, b"val").unwrap();
        rocks_engine.put_cf(CF_LOCK, &key, b"val").unwrap();
        rocks_engine.put_cf(CF_WRITE, &key, b"val").unwrap();

        let key = construct_key(2, 10);
        rocks_engine.put_cf(CF_DEFAULT, &key, b"val").unwrap();
        rocks_engine.put_cf(CF_LOCK, &key, b"val").unwrap();
        rocks_engine.put_cf(CF_WRITE, &key, b"val").unwrap();
        // After loading range1, the memory usage should be 140*6=840

        let range2 = CacheRange::new(construct_user_key(3), construct_user_key(5));
        let key = construct_key(3, 10);
        rocks_engine.put_cf(CF_DEFAULT, &key, b"val").unwrap();
        rocks_engine.put_cf(CF_LOCK, &key, b"val").unwrap();
        rocks_engine.put_cf(CF_WRITE, &key, b"val").unwrap();

        let key = construct_key(4, 10);
        rocks_engine.put_cf(CF_DEFAULT, &key, b"val").unwrap();
        rocks_engine.put_cf(CF_LOCK, &key, b"val").unwrap();
        rocks_engine.put_cf(CF_WRITE, &key, b"val").unwrap();
        // 840*2 > hard limit 1500, so the load will fail and the loaded keys should be
        // removed

        let range3 = CacheRange::new(construct_user_key(5), construct_user_key(6));
        let key = construct_key(5, 10);
        rocks_engine.put_cf(CF_DEFAULT, &key, b"val").unwrap();
        rocks_engine.put_cf(CF_LOCK, &key, b"val").unwrap();
        rocks_engine.put_cf(CF_WRITE, &key, b"val").unwrap();
        let key = construct_key(6, 10);
        rocks_engine.put_cf(CF_DEFAULT, &key, b"val").unwrap();
        rocks_engine.put_cf(CF_LOCK, &key, b"val").unwrap();
        rocks_engine.put_cf(CF_WRITE, &key, b"val").unwrap();

        for r in [&range1, &range2, &range3] {
            engine.load_range(r.clone()).unwrap();
            engine.prepare_for_apply(1, r);
        }

        // ensure all ranges are finshed
        {
            let mut count = 0;
            while count < 20 {
                {
                    let core = engine.core.read();
                    let range_manager = core.range_manager();
                    if range_manager.pending_ranges.is_empty()
                        && range_manager.pending_ranges_loading_data.is_empty()
                    {
                        break;
                    }
                }
                std::thread::sleep(Duration::from_millis(100));
                count += 1;
            }
        }

        let verify = |range: CacheRange, exist, expect_count| {
            if exist {
                let snap = engine.snapshot(range.clone(), 10, u64::MAX).unwrap();
                let mut count = 0;
                for cf in DATA_CFS {
                    let mut iter = IterOptions::default();
                    iter.set_lower_bound(&range.start, 0);
                    iter.set_upper_bound(&range.end, 0);
                    let mut iter = snap.iterator_opt(cf, iter).unwrap();
                    let _ = iter.seek_to_first();
                    while iter.valid().unwrap() {
                        let _ = iter.next();
                        count += 1;
                    }
                }
                assert_eq!(count, expect_count);
            } else {
                engine.snapshot(range, 10, 10).unwrap_err();
            }
        };
        verify(range1, true, 6);
        verify(range2, false, 0);
        verify(range3, false, 3);
        assert_eq!(mem_controller.mem_usage(), 1540);
    }

    #[test]
    fn test_soft_hard_limit_change() {
        let mut config = RangeCacheEngineConfig::config_for_test();
        config.soft_limit_threshold = Some(ReadableSize(1000));
        config.hard_limit_threshold = Some(ReadableSize(1500));
        let config = Arc::new(VersionTrack::new(config));
        let mut engine =
            RangeCacheMemoryEngine::new(RangeCacheEngineContext::new_for_tests(config.clone()));
        let path = Builder::new()
            .prefix("test_snapshot_load_reaching_limit")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();
        let rocks_engine = new_engine(path_str, DATA_CFS).unwrap();
        engine.set_disk_engine(rocks_engine.clone());
        let mem_controller = engine.memory_controller();

        let range1 = CacheRange::new(construct_user_key(1), construct_user_key(3));
        // Memory for one put is 17(key) + 3(val) + 8(Seqno) + 16(Memory controller in
        // key and val) + 96(Node overhead) = 140
        let key = construct_key(1, 10);
        rocks_engine.put_cf(CF_DEFAULT, &key, b"val").unwrap();
        rocks_engine.put_cf(CF_LOCK, &key, b"val").unwrap();
        rocks_engine.put_cf(CF_WRITE, &key, b"val").unwrap();

        let key = construct_key(2, 10);
        rocks_engine.put_cf(CF_DEFAULT, &key, b"val").unwrap();
        rocks_engine.put_cf(CF_LOCK, &key, b"val").unwrap();
        rocks_engine.put_cf(CF_WRITE, &key, b"val").unwrap();
        // After loading range1, the memory usage should be 140*6=840
        engine.load_range(range1.clone()).unwrap();
        engine.prepare_for_apply(1, &range1);

        let range2 = CacheRange::new(construct_user_key(3), construct_user_key(5));
        let key = construct_key(3, 10);
        rocks_engine.put_cf(CF_DEFAULT, &key, b"val").unwrap();
        rocks_engine.put_cf(CF_LOCK, &key, b"val").unwrap();
        rocks_engine.put_cf(CF_WRITE, &key, b"val").unwrap();

        let key = construct_key(4, 10);
        rocks_engine.put_cf(CF_DEFAULT, &key, b"val").unwrap();
        rocks_engine.put_cf(CF_LOCK, &key, b"val").unwrap();
        rocks_engine.put_cf(CF_WRITE, &key, b"val").unwrap();
        // 840*2 > hard limit 1500, so the load will fail and the loaded keys should be
        // removed. However now we change the memory quota to 2000, so the range2 can be
        // cached.
        let mut config_manager = RangeCacheConfigManager(config.clone());
        let mut config_change = ConfigChange::new();
        config_change.insert(
            String::from("hard_limit_threshold"),
            ConfigValue::Size(2000),
        );
        config_manager.dispatch(config_change).unwrap();
        assert_eq!(config.value().hard_limit_threshold(), 2000);

        engine.load_range(range2.clone()).unwrap();
        engine.prepare_for_apply(1, &range2);

        // ensure all ranges are finshed
        {
            let mut count = 0;
            while count < 20 {
                {
                    let core = engine.core.read();
                    let range_manager = core.range_manager();
                    if range_manager.pending_ranges.is_empty()
                        && range_manager.pending_ranges_loading_data.is_empty()
                    {
                        break;
                    }
                }
                std::thread::sleep(Duration::from_millis(100));
                count += 1;
            }
        }

        let verify = |range: CacheRange, exist, expect_count| {
            if exist {
                let snap = engine.snapshot(range.clone(), 10, u64::MAX).unwrap();
                let mut count = 0;
                for cf in DATA_CFS {
                    let mut iter = IterOptions::default();
                    iter.set_lower_bound(&range.start, 0);
                    iter.set_upper_bound(&range.end, 0);
                    let mut iter = snap.iterator_opt(cf, iter).unwrap();
                    let _ = iter.seek_to_first();
                    while iter.valid().unwrap() {
                        let _ = iter.next();
                        count += 1;
                    }
                }
                assert_eq!(count, expect_count);
            } else {
                engine.snapshot(range, 10, 10).unwrap_err();
            }
        };
        verify(range1, true, 6);
        verify(range2, true, 6);
        assert_eq!(mem_controller.mem_usage(), 1680);
    }

    #[test]
    fn test_gc_use_pd_tso() {
        struct MockPdClient {
            tx: Mutex<Sender<()>>,
        }
        impl PdClient for MockPdClient {
            fn get_tso(&self) -> pd_client::PdFuture<txn_types::TimeStamp> {
                self.tx.lock().unwrap().send(()).unwrap();
                Box::pin(ready(Ok(TimeStamp::compose(TimeStamp::physical_now(), 0))))
            }
        }

        let start_time = TimeStamp::compose(TimeStamp::physical_now(), 0);
        let (tx, pd_client_rx) = channel();
        let pd_client = Arc::new(MockPdClient { tx: Mutex::new(tx) });
        let gc_interval = Duration::from_millis(100);
        let load_evict_interval = Duration::from_millis(200);
        let (scheduler, mut rx) = dummy_scheduler();
        let (handle, stop) =
            BgWorkManager::start_tick(scheduler, pd_client, gc_interval, load_evict_interval);

        let Some(BackgroundTask::Gc(GcTask { safe_point })) =
            rx.recv_timeout(10 * gc_interval).unwrap()
        else {
            panic!("must be a GcTask");
        };
        let safe_point = TimeStamp::from(safe_point);
        // Make sure it is a reasonable timestamp.
        assert!(safe_point > start_time, "{safe_point}, {start_time}");
        let now = TimeStamp::compose(TimeStamp::physical_now(), 0);
        assert!(safe_point < now, "{safe_point}, {now}");
        // Must get ts from PD.
        pd_client_rx.try_recv().unwrap();

        stop.send(true).unwrap();
        handle.join().unwrap();
    }
}
