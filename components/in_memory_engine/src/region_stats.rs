// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex as StdMutex,
    },
    time::{Duration, Instant},
};

use collections::HashMap;
use crossbeam::sync::ShardedLock;
use engine_traits::{CacheRegion, EvictReason, OnEvictFinishedCallback};
use kvproto::metapb::Region;
use parking_lot::Mutex;
use pd_client::RegionStat;
use raftstore::coprocessor::RegionInfoProvider;
use slog_global::error;
use tikv_util::{config::VersionTrack, info, worker::Scheduler};
use tokio::sync::mpsc;

use crate::{
    memory_controller::MemoryController, region_manager::CopRequestsSma, BackgroundTask,
    InMemoryEngineConfig,
};

/// Do not evict a region if has been cached for less than this duration.
pub const DEFAULT_EVICT_MIN_DURATION: Duration = Duration::from_secs(60 * 5);
const MIN_REGION_COUNT_TO_EVICT: usize = 5;
// TODO(SpadeA): this 10 and 20 may be adjusted by observing more workloads.
const ITERATED_COUNT_FILTER_FACTOR: usize = 20;
const SMA_COP_REQUEST_AVG_FILTER_FACTOR: usize = 20;
const SMA_COP_REQUEST_COUNT_FILTER: usize = 2;

#[derive(Clone)]
pub(crate) struct RegionStatsManager {
    config: Arc<VersionTrack<InMemoryEngineConfig>>,
    info_provider: Arc<dyn RegionInfoProvider>,
    checking_top_regions: Arc<AtomicBool>,
    region_loaded_at: Arc<ShardedLock<BTreeMap<u64, Instant>>>,
    evict_min_duration: Duration,
    last_load_evict_time: Arc<Mutex<Instant>>,
}

impl RegionStatsManager {
    /// Creates a new RangeStatsManager that retrieves state from
    /// `info_provider`.
    ///
    /// * `num_regions` Initial number of top regions to track and cache. This
    ///   may change, see `adjust_max_num_regions` below.
    /// * `evict_min_duration` - do not evict regions that have been loaded for
    ///   less than this duration.
    pub fn new(
        config: Arc<VersionTrack<InMemoryEngineConfig>>,
        evict_min_duration: Duration,
        info_provider: Arc<dyn RegionInfoProvider>,
    ) -> Self {
        RegionStatsManager {
            config,
            info_provider,
            checking_top_regions: Arc::new(AtomicBool::new(false)),
            region_loaded_at: Arc::new(ShardedLock::new(BTreeMap::new())),
            evict_min_duration,
            last_load_evict_time: Arc::new(Mutex::new(Instant::now())),
        }
    }

    pub(crate) fn expected_region_size(&self) -> usize {
        self.config.value().expected_region_size.0 as usize
    }

    /// If false is returned, it is not ready to check.
    pub fn ready_for_auto_load_and_evict(&self) -> bool {
        // The auto load and evict process can block for some time (mainly waiting for
        // eviction). To avoid too check after check immediately, we check the elapsed
        // time after the last check.
        // Region stats update duration is one minute by default, to avoid two checks
        // using the same region stats as much as possible, we set a min check interval
        // of 1.5 minutes(considers there are not just one region for stat collection).
        self.last_load_evict_time.lock().elapsed()
            > Duration::max(
                self.config.value().load_evict_interval.0 / 2,
                Duration::from_secs(90),
            )
            && !self.set_checking_top_regions(true)
    }

    pub fn complete_auto_load_and_evict(&self) {
        *self.last_load_evict_time.lock() = Instant::now();
        self.set_checking_top_regions(false);
    }

    /// Prevents two instances of this from running concurrently.
    /// Return the previous checking status.
    fn set_checking_top_regions(&self, v: bool) -> bool {
        self.checking_top_regions.swap(v, Ordering::Relaxed)
    }

    /// This method should be called when `evicted_range` is succesfully evicted
    /// to remove any internal `RegionStatsManager` that corresponds to the
    /// range.
    ///
    /// Calls [raftstore::coprocessor::region_info_accessor::RegionInfoProvider::find_region_by_key] to
    /// find the region corresponding to the range.
    ///
    /// TODO (afeinberg): This is inefficient, either make this method bulk, or
    /// find another way to avoid calling `find_region_by_key` in a loop.
    pub fn handle_region_evicted(&self, region: &Region) {
        self.region_loaded_at.write().unwrap().remove(&region.id);
    }

    /// Get regions' stat of the cached region and sort them by coprocessor
    /// requests and next + prev in descending order.
    fn get_cached_region_stats(
        &self,
        cached_regions: &HashMap<u64, Arc<StdMutex<CopRequestsSma>>>,
    ) -> Vec<CachedRegionStat> {
        match self
            .info_provider
            .get_regions_stat(cached_regions.iter().map(|(id, _)| *id).collect())
        {
            Ok(regions_stat) => sort_cached_region_stats(regions_stat, cached_regions),
            Err(e) => {
                error!(
                    "ime get regions stat failed";
                    "err" => ?e,
                );
                assert!(tikv_util::thread_group::is_shutdown(!cfg!(test)));
                vec![]
            }
        }
    }

    /// Collects regions to load and evict based on region stat and memory
    /// constraints.
    ///
    /// If memory usage is below the stop load threshold, regions will not be
    /// evicted.
    ///
    /// If memory usage reaches stop load threshold, whether to evict region is
    /// determined by comparison between the new top regions' activity and the
    /// current cached regions.
    ///
    /// # Returns
    /// (Regions to load, Regions to evict)
    pub fn collect_regions_to_load_and_evict(
        &self,
        current_region_count: usize,
        cached_regions: HashMap<u64, Arc<StdMutex<CopRequestsSma>>>,
        memory_controller: &MemoryController,
    ) -> (Vec<Region>, Vec<Region>) {
        let cached_region_stats = self.get_cached_region_stats(&cached_regions);

        // Use evict-threshold rather than stop-load-threshold as there might
        // be some regions to be evicted.
        let expected_new_count = memory_controller
            .evict_threshold()
            .saturating_sub(memory_controller.mem_usage())
            / self.expected_region_size();
        let expected_num_regions = usize::max(1, current_region_count + expected_new_count);
        info!("ime collect_changed_ranges";
            "current_region_count" => current_region_count,
            "expected_num_regions" => expected_num_regions,
        );
        let curr_top_regions = match self
            .info_provider
            .get_top_regions(NonZeroUsize::try_from(expected_num_regions).unwrap())
        {
            Ok(top_regions) => top_regions,
            Err(e) => {
                error!(
                    "ime get top regions failed";
                    "err" => ?e,
                );
                assert!(tikv_util::thread_group::is_shutdown(!cfg!(test)));
                return (vec![], vec![]);
            }
        };

        let regions_to_load = {
            let mut regions_to_load = Vec::with_capacity(curr_top_regions.len());
            let mut region_loaded_map = self.region_loaded_at.write().unwrap();
            for region in &curr_top_regions {
                let _ = region_loaded_map.insert(region.0.id, Instant::now());
                if !cached_regions.contains_key(&region.0.id) {
                    regions_to_load.push(region.0.clone());
                }
            }
            regions_to_load
        };

        {
            // TODO(SpadeA): remove it after it's stable
            let debug: Vec<_> = cached_region_stats
                .iter()
                .map(|crs| {
                    format!(
                        "region_id={}, cop={}, avg_cop={}, cop_detail={:?}, mvcc_amplification={}",
                        crs.region.id,
                        crs.stat.query_stats.coprocessor,
                        crs.sma_cop_requests_avg,
                        crs.stat.cop_detail,
                        crs.stat.cop_detail.mvcc_amplification(),
                    )
                })
                .collect();
            info!(
                "ime collect regions activities";
                "regions" => ?debug,
            );
        }

        let has_reached_stop_load = memory_controller.reached_stop_load_threshold();
        let mut regions_loaded = self.region_loaded_at.write().unwrap();
        // Evict at most 1/10 of the regions.
        let max_count_to_evict = usize::max(1, cached_region_stats.len() / 10);
        // Use top MIN_REGION_COUNT_TO_EVICT regions next and prev to filter regions
        // with very few next and prev. If there's less than or equal to
        // MIN_REGION_COUNT_TO_EVICT regions, do not evict any one.
        if cached_region_stats.len() <= MIN_REGION_COUNT_TO_EVICT {
            return (regions_to_load, vec![]);
        }

        let mut total_next_prev = 0;
        let mut total_cop_requests = 0;
        for crs in &cached_region_stats {
            total_next_prev += crs.stat.cop_detail.iterated_count();
            total_cop_requests += crs.sma_cop_requests_avg;
        }
        let avg_next_prev = total_next_prev / cached_region_stats.len();
        let avg_cop_requests = total_cop_requests / cached_region_stats.len();

        let now = Instant::now();
        let mut region_to_evict = Vec::with_capacity(max_count_to_evict);
        for crs in cached_region_stats.into_iter().take(max_count_to_evict) {
            let need_evict = {
                // In case, memory usage is relatively low, we evict those that
                // should not be cached.
                //
                // NB: When a region is cached, its RegionWriteCfCopDetail only
                // reflects the MVCC amplification in IME not in RocksDB. So
                // low MVCC amplification is not a good indicator for eviction.
                //
                // Instead, we assume workload patterns remain consistent after
                // caching and evict based on request frequency.
                let is_cop_requests_low = crs.sma_cop_requests_avg
                    <= avg_cop_requests / SMA_COP_REQUEST_AVG_FILTER_FACTOR;

                // Reliable means the region has been loaded for a while (
                // by default 30 minutes), and the SMA coprocessor requests is
                // stable.
                let is_cop_requests_reliable =
                    crs.sma_cop_requests_count > SMA_COP_REQUEST_COUNT_FILTER;

                let is_iterated_count_low = crs.stat.cop_detail.iterated_count()
                    <= avg_next_prev / ITERATED_COUNT_FILTER_FACTOR;

                has_reached_stop_load
                    && is_cop_requests_reliable
                    && is_cop_requests_low
                    && is_iterated_count_low
            };
            if !need_evict {
                continue;
            }

            // Do not evict regions that were loaded less than `EVICT_MIN_DURATION` ago.
            // If it has no time recorded, it should be loaded
            // be pre-load or something, record the time and
            // does not evict it this time.
            let load_time = regions_loaded.entry(crs.region.id).or_insert(now);
            let can_evict = now
                .checked_duration_since(*load_time)
                .map_or(false, |d| d > self.evict_min_duration);
            if !can_evict {
                continue;
            }
            region_to_evict.push(crs);
        }

        // TODO(SpadeA): remove it after it's stable
        let debug_evict: Vec<_> = region_to_evict
            .iter()
            .map(|crs| {
                format!(
                    "region_id={}, cop={}, avg_cop={}, cop_detail={:?}, mvcc_amplification={}",
                    crs.region.id,
                    crs.stat.query_stats.coprocessor,
                    crs.sma_cop_requests_avg,
                    crs.stat.cop_detail,
                    crs.stat.cop_detail.mvcc_amplification(),
                )
            })
            .collect();
        let debug_load: Vec<_> = regions_to_load
            .iter()
            .map(|r| format!("region_id={}", r.id,))
            .collect();
        info!(
            "ime collect regions to load and evict";
            "reached_stop_limit" => has_reached_stop_load,
            "load" => ?debug_load,
            "evicts" => ?debug_evict,
            "avg_next_prev" => avg_next_prev,
            "avg_cop_requests" => avg_cop_requests,
        );
        let regions_to_evict = region_to_evict.into_iter().map(|crs| crs.region).collect();
        (regions_to_load, regions_to_evict)
    }

    // Evict regions with less read flow until the memory usage is below the evict
    // threshold.
    pub(crate) async fn evict_on_evict_threshold_reached<F>(
        &self,
        mut evict_region: F,
        delete_range_scheduler: &Scheduler<BackgroundTask>,
        cached_regions: HashMap<u64, Arc<StdMutex<CopRequestsSma>>>,
        memory_controller: &MemoryController,
    ) where
        F: FnMut(&CacheRegion, EvictReason, Option<OnEvictFinishedCallback>) -> Vec<CacheRegion>,
    {
        let regions_activity = self.get_cached_region_stats(&cached_regions);
        if regions_activity.is_empty() {
            return;
        }

        // Use the average coprocessor requests to filter the regions with low
        // read flow.
        let avg_cop_requests = regions_activity
            .iter()
            .map(|crs| crs.sma_cop_requests_avg)
            .sum::<usize>()
            / regions_activity.len();

        let evict_candidates: Vec<_> = {
            let mut evict_candidates = regions_activity;
            info!(
                "ime evict candidate count before filter";
                "count" => evict_candidates.len(),
            );
            let mut filter_by_avg_cop_requests = 0;
            evict_candidates.retain(|crs| {
                // Do not evict regions with high mvcc amplification
                if crs.sma_cop_requests_avg > avg_cop_requests {
                    filter_by_avg_cop_requests += 1;
                    return false;
                }
                true
            });

            info!(
                "ime evict candidate count after filter";
                "count" => evict_candidates.len(),
                "filter_by_avg_cop_requests" => filter_by_avg_cop_requests,
            );

            evict_candidates.into_iter().map(|crs| crs.region).collect()
        };
        // Evict two regions each time to reduce the probability that an un-dropped
        // ongoing snapshot blocks the process
        for regions in evict_candidates.chunks(2) {
            let mut deletable_regions = vec![];

            let (tx, mut rx) = mpsc::channel(3);
            for r in regions {
                info!(
                    "ime evict on evict threshold reached";
                    "region_to_evict" => ?r,
                );

                let tx_clone = tx.clone();
                deletable_regions.extend(evict_region(
                    &CacheRegion::from_region(r),
                    EvictReason::MemoryLimitReached,
                    // This callback will be executed when eviction finishes at `on_delete_regions`
                    // and when the rx.recv() returns, we know some memory are freed.
                    Some(Box::new(move || {
                        Box::pin(async move {
                            let _ = tx_clone.send(()).await;
                        })
                    })),
                ));
                self.handle_region_evicted(r);
            }
            if !deletable_regions.is_empty() {
                if let Err(e) = delete_range_scheduler
                    .schedule_force(BackgroundTask::DeleteRegions(deletable_regions))
                {
                    error!(
                        "ime schedule delete regions failed";
                        "err" => ?e,
                    );
                    assert!(tikv_util::thread_group::is_shutdown(!cfg!(test)));
                }
            }
            for _ in 0..regions.len() {
                // It's better to use `timeout(Duration, rx.recv())` but which needs to use
                // tokio runtime with timer enabled while yatp does not.
                if rx.recv().await.is_none() {
                    break;
                }
            }
            if !memory_controller.reached_stop_load_threshold() {
                return;
            }
        }
    }
}

struct CachedRegionStat {
    region: Region,
    stat: RegionStat,
    sma_cop_requests_count: usize,
    sma_cop_requests_avg: usize,
    iterated_count: usize,
}

/// Sorter for `RegionStat` for cached regions.
///
/// Prioritizes cop requests over iterated count, assuming consistent workload
/// patterns after caching, implying cached regions have configured MVCC
/// amplification at least.
fn sort_cached_region_stats(
    region_stats: Vec<(Region, RegionStat)>,
    cached_regions: &HashMap<u64, Arc<StdMutex<CopRequestsSma>>>,
) -> Vec<CachedRegionStat> {
    // update the moving average of the coprocessor requests.
    let mut crss = Vec::with_capacity(region_stats.len());
    for (region, stat) in region_stats {
        let mut cop_requests_avg = stat.query_stats.coprocessor as usize;
        let mut cop_requests_count = 0;
        let cop_requests = stat.query_stats.coprocessor as usize;
        if let Some(cached_region) = cached_regions.get(&region.id)
            && let Ok(mut avg) = cached_region.lock()
        {
            avg.observe(cop_requests as f64);
            cop_requests_avg = avg.get_avg().ceil() as usize;
            cop_requests_count = avg.get_count();
        }
        crss.push(CachedRegionStat {
            iterated_count: stat.cop_detail.iterated_count(),
            region,
            stat,
            sma_cop_requests_avg: cop_requests_avg,
            sma_cop_requests_count: cop_requests_count,
        });
    }

    crss.sort_by(|a, b| {
        if a.sma_cop_requests_avg != b.sma_cop_requests_avg {
            return a.sma_cop_requests_avg.cmp(&b.sma_cop_requests_avg);
        }
        if a.iterated_count != b.iterated_count {
            return a.iterated_count.cmp(&b.iterated_count);
        }
        std::cmp::Ordering::Equal
    });
    crss
}

#[cfg(test)]
pub mod tests {
    use futures::executor::block_on;
    use pd_client::{RegionStat, RegionWriteCfCopDetail};
    use raftstore::coprocessor::{self, region_info_accessor::TopRegions, RegionInfoProvider};
    use tikv_util::{
        box_err,
        config::{ReadableDuration, ReadableSize, VersionTrack},
        worker::dummy_scheduler,
    };

    use super::*;
    use crate::{
        engine::SkiplistEngine, region_manager::COP_REQUEST_SMA_RECORD_COUNT,
        test_util::new_region, InMemoryEngineConfig,
    };

    struct RegionInfoSimulator {
        regions: Mutex<TopRegions>,
        region_stats: Mutex<HashMap<u64, (Region, RegionStat)>>,
    }

    impl RegionInfoSimulator {
        fn set_top_regions(&self, top_regions: &TopRegions) {
            *self.regions.lock() = top_regions.clone()
        }

        fn set_region_stats(&self, region_stats: &[(Region, RegionStat)]) {
            *self.region_stats.lock() = region_stats
                .iter()
                .map(|(r, s)| (r.id, (r.clone(), s.clone())))
                .collect::<HashMap<u64, (Region, RegionStat)>>();
        }
    }

    impl RegionInfoProvider for RegionInfoSimulator {
        fn find_region_by_key(&self, key: &[u8]) -> coprocessor::Result<Region> {
            self.regions
                .lock()
                .iter()
                .find(|(region, _)| region.start_key == key)
                .cloned()
                .map_or_else(
                    || Err(box_err!(format!("key {:?} not found", key))),
                    |(region, _)| Ok(region),
                )
        }

        fn get_top_regions(&self, count: NonZeroUsize) -> coprocessor::Result<TopRegions> {
            Ok(self
                .regions
                .lock()
                .iter()
                .take(count.get())
                .cloned()
                .collect::<Vec<_>>())
        }

        fn get_regions_stat(
            &self,
            ids: Vec<u64>,
        ) -> coprocessor::Result<Vec<(Region, RegionStat)>> {
            let g = self.region_stats.lock().clone();
            Ok(ids
                .into_iter()
                .filter_map(|id| g.get(&id).cloned())
                .collect())
        }
    }

    fn new_region_stat(cop_requests: usize, next: usize, processed_keys: usize) -> RegionStat {
        let mut stat = RegionStat::default();
        stat.query_stats.coprocessor = cop_requests as u64;
        stat.cop_detail = RegionWriteCfCopDetail::new(next, 0, processed_keys);
        stat
    }

    fn new_cached_regions(
        cached_region_ids: Vec<u64>,
    ) -> HashMap<u64, Arc<StdMutex<CopRequestsSma>>> {
        let mut cached_regions = HashMap::default();
        for id in cached_region_ids {
            cached_regions.insert(id, Arc::new(StdMutex::new(CopRequestsSma::default())));
        }
        cached_regions
    }

    #[test]
    fn test_collect_changed_regions() {
        let skiplist_engine = SkiplistEngine::new();
        let mut config = InMemoryEngineConfig::config_for_test();
        config.stop_load_threshold = Some(ReadableSize::kb(1));
        config.mvcc_amplification_threshold = 10;
        let config = Arc::new(VersionTrack::new(config));
        // Make sure stop load threshold is reached.
        let mc = MemoryController::new(config.clone(), skiplist_engine);
        mc.acquire(config.value().stop_load_threshold() * 2);

        let regions = vec![
            new_region(1, b"k01", b"k02"),
            new_region(2, b"k03", b"k04"),
            new_region(3, b"k05", b"k06"),
            new_region(4, b"k07", b"k08"),
            new_region(5, b"k09", b"k10"),
            new_region(6, b"k11", b"k12"),
            new_region(7, b"k13", b"k14"),
        ];
        let sim = Arc::new(RegionInfoSimulator {
            regions: Mutex::default(),
            region_stats: Mutex::default(),
        });
        // 10 ms min duration eviction for testing purposes.
        let mut rsm =
            RegionStatsManager::new(config.clone(), Duration::from_millis(10), sim.clone());

        // All regions are busy, should be cached.
        let empty_cached_regions = new_cached_regions(vec![]);
        let all_busy = regions
            .iter()
            .map(|r| (r.clone(), new_region_stat(1000, 1000, 10)))
            .collect();
        sim.set_top_regions(&all_busy);
        let (added, removed) = rsm.collect_regions_to_load_and_evict(0, empty_cached_regions, &mc);
        assert_eq!(added.len(), regions.len());
        assert!(removed.is_empty());

        // Within SMA unreliable period, any idle region should not be evicted.
        std::thread::sleep(Duration::from_millis(50));
        let cached_regions = new_cached_regions(regions.iter().map(|r| r.id).collect());
        for _ in 0..SMA_COP_REQUEST_COUNT_FILTER {
            let mut region1_idle = all_busy.clone();
            region1_idle[0].1 = new_region_stat(0, 0, 0);
            sim.set_region_stats(&region1_idle);
            region1_idle.remove(0);
            sim.set_top_regions(&region1_idle);

            let (added, removed) =
                rsm.collect_regions_to_load_and_evict(0, cached_regions.clone(), &mc);
            assert!(removed.is_empty(), "{:?}", removed);
            assert!(added.is_empty(), "{:?}", added);
        }

        // All regions are busy.
        let sma_cap = COP_REQUEST_SMA_RECORD_COUNT;
        sim.set_top_regions(&all_busy);
        sim.set_region_stats(&all_busy);
        for _ in 0..sma_cap {
            let (added, removed) =
                rsm.collect_regions_to_load_and_evict(0, cached_regions.clone(), &mc);
            assert!(added.is_empty());
            assert!(removed.is_empty());
        }

        // Region 1 is idle for a short period, it should not be evicted.
        let mut region1_idle = all_busy.clone();
        region1_idle[0].1 = new_region_stat(0, 0, 0);
        sim.set_region_stats(&region1_idle);
        region1_idle.remove(0);
        sim.set_top_regions(&region1_idle);
        let (added, removed) =
            rsm.collect_regions_to_load_and_evict(0, cached_regions.clone(), &mc);
        assert!(removed.is_empty(), "{:?}", removed);
        assert!(added.is_empty(), "{:?}", added);

        // Region 1 is in idle for a long period, should be evicted.
        std::thread::sleep(Duration::from_millis(50));
        let mut has_removed = false;
        for _ in 0..2 * sma_cap {
            let (added, removed) =
                rsm.collect_regions_to_load_and_evict(0, cached_regions.clone(), &mc);
            assert!(added.is_empty());
            if !removed.is_empty() {
                assert_eq!(removed, vec![regions[0].clone()]);
                has_removed = true;
                break;
            }
        }
        assert!(has_removed);

        // Make sure stop load threshold is not reached.
        mc.release(config.value().stop_load_threshold() * 2);
        rsm.collect_regions_to_load_and_evict(0, cached_regions.clone(), &mc);
        assert!(removed.is_empty(), "{:?}", removed);

        // Region can only be evicted after `evict_min_duration`.
        rsm.evict_min_duration = Duration::MAX;
        mc.acquire(config.value().stop_load_threshold() * 2);
        rsm.collect_regions_to_load_and_evict(0, cached_regions.clone(), &mc);
        assert!(removed.is_empty(), "{:?}", removed);
    }

    #[test]
    fn test_collect_candidates_for_eviction() {
        let skiplist_engine = SkiplistEngine::new();
        let mut config = InMemoryEngineConfig::config_for_test();
        config.stop_load_threshold = Some(ReadableSize::kb(1));
        config.mvcc_amplification_threshold = 10;
        let config = Arc::new(VersionTrack::new(config));
        let mc = MemoryController::new(config.clone(), skiplist_engine);
        let (scheduler, _rx) = dummy_scheduler();

        fn make_region_vec(rs: &[Region], stats: &[RegionStat]) -> TopRegions {
            rs.iter()
                .zip(stats.iter())
                .map(|(r, stat)| (r.clone(), stat.clone()))
                .collect::<Vec<_>>()
        }

        let region_1 = new_region(1, b"k01", b"k02");
        let region_2 = new_region(2, b"k03", b"k04");
        let region_3 = new_region(3, b"k05", b"k06");
        let region_4 = new_region(4, b"k07", b"k08");
        let region_5 = new_region(5, b"k09", b"k10");
        let region_6 = new_region(6, b"k11", b"k12");
        let regions = vec![region_1, region_2, region_3, region_4, region_5, region_6];

        let region_stats = vec![
            new_region_stat(1, 100, 6),
            new_region_stat(1, 10000, 1000),
            new_region_stat(1, 100000, 100000),
            new_region_stat(1, 100, 50), // will be evicted
            new_region_stat(1, 1000, 120),
            new_region_stat(1, 20, 2), // will be evicted
        ];

        let all_regions = make_region_vec(&regions, &region_stats);
        let region_stats: HashMap<u64, (Region, RegionStat)> = all_regions
            .iter()
            .map(|top_region| {
                (
                    top_region.0.id,
                    (top_region.0.clone(), top_region.1.clone()),
                )
            })
            .collect();

        let sim = Arc::new(RegionInfoSimulator {
            regions: Mutex::new(all_regions),
            region_stats: Mutex::new(region_stats),
        });
        // 10 ms min duration eviction for testing purposes.
        let rsm = RegionStatsManager::new(config.clone(), Duration::from_millis(10), sim.clone());
        rsm.collect_regions_to_load_and_evict(0, new_cached_regions(vec![]), &mc);
        std::thread::sleep(Duration::from_millis(100));

        let evicted_regions = Arc::new(Mutex::new(vec![]));
        let cbs = Arc::new(Mutex::new(vec![]));

        let evicted_regions2 = evicted_regions.clone();
        let cbs2 = cbs.clone();
        let evict_fn = move |evict_region: &CacheRegion,
                             _: EvictReason,
                             cb: Option<OnEvictFinishedCallback>|
              -> Vec<CacheRegion> {
            evicted_regions2.lock().push(evict_region.id);
            cbs2.lock().push(cb.unwrap());
            vec![]
        };

        let handle = std::thread::spawn(move || {
            block_on(async {
                rsm.evict_on_evict_threshold_reached(
                    evict_fn,
                    &scheduler,
                    new_cached_regions(vec![1, 2, 3, 4, 5, 6]),
                    &mc,
                )
                .await
            });
        });

        let t = Instant::now();
        while t.elapsed() < Duration::from_secs(2) {
            std::thread::sleep(Duration::from_millis(100));
            let mut cb_lock = cbs.lock();
            if cb_lock.len() == 2 {
                let evicted_regions_lock = evicted_regions.lock();
                assert_eq!(*evicted_regions_lock, vec![6, 4]);
                block_on(async {
                    cb_lock.pop().unwrap()().await;
                    cb_lock.pop().unwrap()().await;
                });
                break;
            }
        }

        let _ = handle.join();
    }

    #[test]
    fn test_check_after_check() {
        let sim = Arc::new(RegionInfoSimulator {
            regions: Mutex::new(vec![]),
            region_stats: Mutex::new(HashMap::default()),
        });
        let mut config = InMemoryEngineConfig::config_for_test();
        config.load_evict_interval = ReadableDuration(Duration::from_secs(120));
        let config = Arc::new(VersionTrack::new(config));
        let mgr = RegionStatsManager::new(config, Duration::from_secs(120), sim);

        // Interval is not enough
        assert!(!mgr.ready_for_auto_load_and_evict());
        *mgr.last_load_evict_time.lock() = Instant::now() - Duration::from_secs(120);
        assert!(mgr.ready_for_auto_load_and_evict());
        // Checking
        assert!(!mgr.ready_for_auto_load_and_evict());

        mgr.complete_auto_load_and_evict();
        // Interval is not enough
        assert!(!mgr.ready_for_auto_load_and_evict());
        *mgr.last_load_evict_time.lock() = Instant::now() - Duration::from_secs(120);
        assert!(mgr.ready_for_auto_load_and_evict());
    }

    #[test]
    fn test_sort_cached_region_stats() {
        let cases = [
            (
                vec![
                    (new_region(1, b"", b""), new_region_stat(1, 3, 0)),
                    (new_region(2, b"", b""), new_region_stat(1, 2, 0)),
                    (new_region(3, b"", b""), new_region_stat(1, 1, 0)),
                    (new_region(4, b"", b""), new_region_stat(1, 4, 0)),
                ],
                vec![3, 2, 1, 4],
            ),
            (
                vec![
                    (new_region(1, b"", b""), new_region_stat(3, 1, 0)),
                    (new_region(2, b"", b""), new_region_stat(2, 1, 0)),
                    (new_region(3, b"", b""), new_region_stat(1, 1, 0)),
                    (new_region(4, b"", b""), new_region_stat(4, 1, 0)),
                ],
                vec![3, 2, 1, 4],
            ),
            (
                vec![
                    (new_region(1, b"", b""), new_region_stat(3, 1, 0)),
                    (new_region(2, b"", b""), new_region_stat(2, 2, 0)),
                    (new_region(3, b"", b""), new_region_stat(1, 3, 0)),
                    (new_region(4, b"", b""), new_region_stat(4, 4, 0)),
                ],
                vec![3, 2, 1, 4],
            ),
            (
                vec![
                    (new_region(1, b"", b""), new_region_stat(3, 3, 0)),
                    (new_region(2, b"", b""), new_region_stat(3, 2, 0)),
                    (new_region(3, b"", b""), new_region_stat(3, 1, 0)),
                    (new_region(4, b"", b""), new_region_stat(4, 1, 0)),
                ],
                vec![3, 2, 1, 4],
            ),
        ];
        for (regions_stat, expected_order) in cases {
            let mut cached_regions = HashMap::default();
            for (r, _) in &regions_stat {
                cached_regions.insert(r.id, Arc::new(StdMutex::new(CopRequestsSma::default())));
            }
            let sorted = sort_cached_region_stats(regions_stat, &cached_regions);
            let order: Vec<_> = sorted.iter().map(|crs| crs.region.id).collect();
            assert_eq!(order, *expected_order);
        }
    }
}
