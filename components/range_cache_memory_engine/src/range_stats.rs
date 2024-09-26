// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    collections::{BTreeMap, HashSet},
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use collections::HashMap;
use crossbeam::sync::ShardedLock;
use engine_traits::{CacheRegion, EvictReason};
use kvproto::metapb::Region;
use parking_lot::Mutex;
use raftstore::coprocessor::RegionInfoProvider;
use slog_global::error;
use tikv_util::{config::VersionTrack, info, smoother::Smoother, worker::Scheduler};
use tokio::sync::mpsc;

use crate::{
    memory_controller::MemoryController, range_manager::AsyncFnOnce, BackgroundTask,
    RangeCacheEngineConfig,
};

/// Do not evict a region if has been cached for less than this duration.
pub const DEFAULT_EVICT_MIN_DURATION: Duration = Duration::from_secs(60 * 5);
const MIN_REGION_COUNT_TO_EVICT: usize = 5;
// TODO(SpadeA): this 10 and 20 may be adjusted by observing more workloads.
const MVCC_AMPLIFICATION_FILTER_FACTOR: f64 = 10.0;
const ITERATED_COUNT_FILTER_FACTOR: usize = 20;

#[derive(Clone)]
pub(crate) struct RangeStatsManager {
    config: Arc<VersionTrack<RangeCacheEngineConfig>>,
    info_provider: Arc<dyn RegionInfoProvider>,
    checking_top_regions: Arc<AtomicBool>,
    region_loaded_at: Arc<ShardedLock<BTreeMap<u64, Instant>>>,
    evict_min_duration: Duration,
    // Moving average of amplification reduction. Amplification reduction is the reduced
    // multiple before and after the cache. When a new top region (of course, not cached) comes in,
    // this moving average number is used to estimate the mvcc amplification after cache so we can
    // use it to determine whether to evict some regions if memory usage is relative high.
    ma_mvcc_amplification_reduction: Arc<Mutex<Smoother<f64, 10, 0, 0>>>,
    mvcc_amplification_record: Arc<Mutex<HashMap<u64, f64>>>,

    last_load_evict_time: Arc<Mutex<Instant>>,
}

impl RangeStatsManager {
    /// Creates a new RangeStatsManager that retrieves state from
    /// `info_provider`.
    ///
    /// * `num_regions` Initial number of top regions to track and cache. This
    ///   may change, see `adjust_max_num_regions` below.
    /// * `evict_min_duration` - do not evict regions that have been loaded for
    ///   less than this duration.
    pub fn new(
        config: Arc<VersionTrack<RangeCacheEngineConfig>>,
        evict_min_duration: Duration,
        info_provider: Arc<dyn RegionInfoProvider>,
    ) -> Self {
        RangeStatsManager {
            config,
            info_provider,
            checking_top_regions: Arc::new(AtomicBool::new(false)),
            region_loaded_at: Arc::new(ShardedLock::new(BTreeMap::new())),
            ma_mvcc_amplification_reduction: Arc::new(Mutex::new(Smoother::default())),
            mvcc_amplification_record: Arc::default(),
            evict_min_duration,
            last_load_evict_time: Arc::new(Mutex::new(Instant::now())),
        }
    }

    pub(crate) fn expected_region_size(&self) -> usize {
        self.config.value().expected_region_size()
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

    /// Collects regions to load and evict based on region stat, mvcc
    /// amplification, and memory constraints. New top regions will be
    /// collected in `regions_added_out` to be loaded.
    ///
    /// If memory usage is below the stop load limit, regions with low read flow
    /// or low mvcc amplification are considered for eviction.
    ///
    /// If memory usage reaches stop load limit, whether to evict region is
    /// determined by comparison between the new top regions' activity and the
    /// current cached regions.
    ///
    /// # Returns
    /// (Regions to load, Regions to evict)
    pub fn collect_regions_to_load_and_evict(
        &self,
        current_region_count: usize,
        cached_region_ids: Vec<u64>,
        memory_controller: &MemoryController,
    ) -> (Vec<Region>, Vec<Region>) {
        // Get regions' stat of the cached region and sort them by next + prev in
        // descending order.
        let mut regions_stat = self
            .info_provider
            .get_regions_stat(cached_region_ids.clone())
            .unwrap();
        regions_stat.sort_by(|a, b| {
            let next_prev_a = a.1.cop_detail.iterated_count();
            let next_prev_b = b.1.cop_detail.iterated_count();
            next_prev_b.cmp(&next_prev_a)
        });

        let ma_mvcc_amplification_reduction = {
            let mut ma_mvcc_amplification_reduction = self.ma_mvcc_amplification_reduction.lock();
            let mut record = self.mvcc_amplification_record.lock();
            // update the moving average of the mvcc amplification reduction(the reduced
            // multiple before and after the cache).
            regions_stat.iter().for_each(|(r, a)| {
                if let Some(&amplification) = record.get(&r.id) {
                    let amp_after_cache = a.cop_detail.mvcc_amplification();
                    if amp_after_cache != 0.0 && amp_after_cache != amplification {
                        ma_mvcc_amplification_reduction
                            .observe(amplification / a.cop_detail.mvcc_amplification());
                    }
                }
            });
            record.clear();
            // this reduction should not be less than 1
            ma_mvcc_amplification_reduction.get_avg()
        };
        info!(
            "IME moving average mvcc amplification reduction update";
            "ma_mvcc_amplification_reduction" => ma_mvcc_amplification_reduction,
        );

        // Use soft-limit-threshold rather than stop-load-limit-threshold as there might
        // be some regions to be evicted.
        let expected_new_count = (memory_controller
            .soft_limit_threshold()
            .saturating_sub(memory_controller.mem_usage()))
            / self.expected_region_size();
        let expected_num_regions = current_region_count + expected_new_count;
        info!("ime collect_changed_ranges"; "num_regions" => expected_num_regions);
        let curr_top_regions = self
            .info_provider
            .get_top_regions(Some(NonZeroUsize::try_from(expected_num_regions).unwrap()))
            .unwrap() // TODO (afeinberg): Potentially custom error handling here.
            .iter()
            .map(|(r, region_stats)| (r.id, (r.clone(), region_stats.clone())))
            .collect::<BTreeMap<_, _>>();
        {
            let mut region_loaded_map = self.region_loaded_at.write().unwrap();
            for &region_id in curr_top_regions.keys() {
                let _ = region_loaded_map.insert(region_id, Instant::now());
            }
        }

        let cached_region_ids = cached_region_ids.into_iter().collect::<HashSet<u64>>();
        let (mvcc_amplification_to_filter, regions_to_load) = {
            let mut max_mvcc_amplification: f64 = 0.0;
            let mut record = self.mvcc_amplification_record.lock();
            let regions_to_load = curr_top_regions
                .iter()
                .filter_map(|(id, (r, region_stats))| {
                    if !cached_region_ids.contains(id) {
                        max_mvcc_amplification = max_mvcc_amplification
                            .max(region_stats.cop_detail.mvcc_amplification());
                        record.insert(*id, region_stats.cop_detail.mvcc_amplification());
                        Some(r.clone())
                    } else {
                        None
                    }
                })
                .collect();
            // `max_mvcc_amplification / ma_mvcc_amplification_reduction` is the
            // expected mvcc amplification factor after cache. Make the half of it to filter
            // the cached regions.
            (
                max_mvcc_amplification / f64::max(1.0, ma_mvcc_amplification_reduction / 2.0),
                regions_to_load,
            )
        };

        info!(
            "IME mvcc amplification reduction filter";
            "mvcc_amplification_to_filter" => mvcc_amplification_to_filter,
        );

        {
            // TODO(SpadeA): remove it after it's stable
            let debug: Vec<_> = regions_stat
                .iter()
                .map(|(r, s)| {
                    format!(
                        "region_id={}, cop={}, cop_detail={:?}, mvcc_amplification={}",
                        r.id,
                        s.query_stats.coprocessor,
                        s.cop_detail,
                        s.cop_detail.mvcc_amplification(),
                    )
                })
                .collect();
            info!(
                "ime collect regions activities";
                "regions" => ?debug,
            );
        }

        let reach_stop_load = memory_controller.reached_stop_load_limit();
        let mut regions_loaded = self.region_loaded_at.write().unwrap();
        // Evict at most 1/10 of the regions.
        let max_count_to_evict = usize::max(1, regions_stat.len() / 10);
        // Use top MIN_REGION_COUNT_TO_EVICT regions next and prev to filter regions
        // with very few next and prev. If there's less than or equal to
        // MIN_REGION_COUNT_TO_EVICT regions, do not evict any one.
        let regions_to_evict = if regions_stat.len() > MIN_REGION_COUNT_TO_EVICT {
            let avg_top_next_prev = if reach_stop_load {
                regions_stat
                    .iter()
                    .map(|r| r.1.cop_detail.iterated_count())
                    .take(MIN_REGION_COUNT_TO_EVICT)
                    .sum::<usize>()
                    / MIN_REGION_COUNT_TO_EVICT
            } else {
                0
            };
            let region_to_evict: Vec<_> = regions_stat
                    .into_iter()
                    .filter(|(_, r)| {
                        if reach_stop_load {
                            r.cop_detail.mvcc_amplification()
                                < mvcc_amplification_to_filter
                        } else {
                            // In this case, memory usage is relarively low, we only evict those that should not be cached apparently.
                            r.cop_detail.mvcc_amplification()
                                <= self.config.value().mvcc_amplification_threshold as f64 / MVCC_AMPLIFICATION_FILTER_FACTOR
                                || r.cop_detail.iterated_count()  < avg_top_next_prev / ITERATED_COUNT_FILTER_FACTOR
                        }
                    })
                    .filter_map(|(r, s)| {
                        // Do not evict regions that were loaded less than `EVICT_MIN_DURATION` ago.
                        // If it has no time recorded, it should be loaded
                        // be pre-load or something, record the time and
                        // does not evict it this time.
                        let time_loaded = regions_loaded.entry(r.id).or_insert(Instant::now());
                        if Instant::now() - *time_loaded < self.evict_min_duration {
                            None
                        } else {
                            Some((r, s))
                        }
                    })
                    .rev() // evict the regions with least next + prev after the filter
                    .take(max_count_to_evict)
                    .collect();

            // TODO(SpadeA): remove it after it's stable
            let debug: Vec<_> = region_to_evict
                .iter()
                .map(|(r, s)| {
                    format!(
                        "region_id={}, cop={}, cop_detail={:?}, mvcc_amplification={}",
                        r.id,
                        s.query_stats.coprocessor,
                        s.cop_detail,
                        s.cop_detail.mvcc_amplification(),
                    )
                })
                .collect();
            info!(
                "ime collect regions to evict";
                "reached_stop_limit" => reach_stop_load,
                "regions" => ?debug,
            );
            region_to_evict.into_iter().map(|(r, _)| r).collect()
        } else {
            vec![]
        };
        (regions_to_load, regions_to_evict)
    }

    // Evict regions with less read flow until the memory usage is below the soft
    // limit threshold.
    pub(crate) async fn evict_on_soft_limit_reached<F>(
        &self,
        mut evict_region: F,
        delete_range_scheduler: &Scheduler<BackgroundTask>,
        cached_region_ids: Vec<u64>,
        memory_controller: &MemoryController,
    ) where
        F: FnMut(
            &CacheRegion,
            EvictReason,
            Option<Box<dyn AsyncFnOnce + Send + Sync>>,
        ) -> Vec<CacheRegion>,
    {
        // Get regions' stat of the cached region and sort them by next + prev in
        // descending order.
        let regions_activity = self
            .info_provider
            .get_regions_stat(cached_region_ids.clone())
            .unwrap();
        if regions_activity.is_empty() {
            return;
        }

        // Use the average mvcc amplification to filter the regions with high mvcc
        // amplification
        let avg_mvcc_amplification = regions_activity
            .iter()
            .map(|(_, ra)| ra.cop_detail.mvcc_amplification())
            .sum::<f64>()
            / regions_activity.len() as f64;

        let evict_candidates: Vec<_> = {
            let mut evict_candidates = regions_activity;
            info!(
                "ime evict candidate count before filter";
                "count" => evict_candidates.len(),
            );
            let mut filter_by_mvcc_amplification = 0;
            evict_candidates.retain(|(_, ra)| {
                // Do not evict regions with high mvcc amplification
                if ra.cop_detail.mvcc_amplification() > avg_mvcc_amplification {
                    filter_by_mvcc_amplification += 1;
                    return false;
                }
                true
            });
            evict_candidates.sort_by(|a, b| {
                let next_prev_a = a.1.cop_detail.iterated_count();
                let next_prev_b = b.1.cop_detail.iterated_count();
                next_prev_a.cmp(&next_prev_b)
            });

            info!(
                "ime evict candidate count after filter";
                "count" => evict_candidates.len(),
                "filter_by_mvcc_amplification" => filter_by_mvcc_amplification,
            );

            evict_candidates.into_iter().map(|(r, _)| r).collect()
        };
        // Evict two regions each time to reduce the probability that an un-dropped
        // ongoing snapshot blocks the process
        for regions in evict_candidates.chunks(2) {
            let mut deleteable_regions = vec![];

            let (tx, mut rx) = mpsc::channel(3);
            for r in regions {
                info!(
                    "ime evict on soft limit reached";
                    "region_to_evict" => ?r,
                );

                let tx_clone = tx.clone();
                deleteable_regions.extend(evict_region(
                    &CacheRegion::from_region(r),
                    EvictReason::MemoryLimitReached,
                    // This callback will be executed when eviction finishes at `on_delete_regions`
                    // and when the reletive rx.recv() returns, we know some memory are freed.
                    Some(Box::new(move || {
                        Box::pin(async move {
                            let _ = tx_clone.send(()).await;
                        })
                    })),
                ));
                self.handle_region_evicted(r);
            }
            if !deleteable_regions.is_empty() {
                if let Err(e) = delete_range_scheduler
                    .schedule_force(BackgroundTask::DeleteRegions(deleteable_regions))
                {
                    error!(
                        "ime schedule deletet range failed";
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
            if !memory_controller.reached_stop_load_limit() {
                return;
            }
        }
    }
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
    use crate::{engine::SkiplistEngine, test_util::new_region, RangeCacheEngineConfig};

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

        fn get_top_regions(&self, count: Option<NonZeroUsize>) -> coprocessor::Result<TopRegions> {
            Ok(count.map_or_else(
                || self.regions.lock().clone(),
                |count| {
                    self.regions
                        .lock()
                        .iter()
                        .take(count.get())
                        .cloned()
                        .collect::<Vec<_>>()
                },
            ))
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

    fn new_region_stat(next: usize, processed_keys: usize) -> RegionStat {
        let mut stat = RegionStat::default();
        stat.cop_detail = RegionWriteCfCopDetail::new(next, 0, processed_keys);
        stat
    }

    #[test]
    fn test_collect_changed_regions() {
        let skiplist_engine = SkiplistEngine::new();
        let mut config = RangeCacheEngineConfig::config_for_test();
        config.stop_load_limit_threshold = Some(ReadableSize::kb(1));
        config.mvcc_amplification_threshold = 10;
        let config = Arc::new(VersionTrack::new(config));
        let mc = MemoryController::new(config.clone(), skiplist_engine);
        let region_1 = new_region(1, b"k01", b"k02");

        let region_2 = new_region(2, b"k03", b"k04");
        let sim = Arc::new(RegionInfoSimulator {
            regions: Mutex::new(vec![(region_1.clone(), new_region_stat(1000, 10))]),
            region_stats: Mutex::default(),
        });
        // 10 ms min duration eviction for testing purposes.
        let rsm = RangeStatsManager::new(config, Duration::from_millis(10), sim.clone());
        let (added, removed) = rsm.collect_regions_to_load_and_evict(0, vec![], &mc);
        assert_eq!(&added, &[region_1.clone()]);
        assert!(removed.is_empty());
        let top_regions = vec![(region_2.clone(), new_region_stat(1000, 8))];
        let region_stats = vec![(region_1.clone(), new_region_stat(20, 10))];
        sim.set_top_regions(&top_regions);
        sim.set_region_stats(&region_stats);
        let (added, removed) = rsm.collect_regions_to_load_and_evict(0, vec![1], &mc);
        assert_eq!(&added, &[region_2.clone()]);
        assert!(removed.is_empty());
        let region_3 = new_region(3, b"k05", b"k06");
        let region_4 = new_region(4, b"k07", b"k08");
        let region_5 = new_region(5, b"k09", b"k10");
        let region_6 = new_region(6, b"k11", b"k12");
        let region_7 = new_region(7, b"k13", b"k14");
        let region_8 = new_region(8, b"k15", b"k16");
        let top_regions = vec![
            (region_6.clone(), new_region_stat(1000, 10)),
            (region_3.clone(), new_region_stat(1000, 10)),
            (region_4.clone(), new_region_stat(1000, 10)),
            (region_5.clone(), new_region_stat(1000, 10)),
            (region_7.clone(), new_region_stat(2000, 10)),
        ];
        let region_stats = vec![
            (region_1.clone(), new_region_stat(20, 10)),
            (region_2.clone(), new_region_stat(20, 8)),
        ];
        sim.set_top_regions(&top_regions);
        sim.set_region_stats(&region_stats);
        let (added, removed) = rsm.collect_regions_to_load_and_evict(0, vec![1, 2], &mc);
        assert!(removed.is_empty());
        assert_eq!(
            &added,
            &[
                region_3.clone(),
                region_4.clone(),
                region_5.clone(),
                region_6.clone(),
                region_7.clone()
            ]
        );

        let region_stats = vec![
            (region_1.clone(), new_region_stat(2, 10)),
            (region_6.clone(), new_region_stat(30, 10)),
            (region_2.clone(), new_region_stat(20, 8)),
            (region_3.clone(), new_region_stat(15, 10)),
            (region_4.clone(), new_region_stat(20, 10)),
            (region_5.clone(), new_region_stat(25, 10)),
            (region_7.clone(), new_region_stat(40, 10)),
        ];
        sim.set_top_regions(&vec![]);
        sim.set_region_stats(&region_stats);
        let (_, removed) = rsm.collect_regions_to_load_and_evict(0, vec![1, 2, 3, 4, 5, 6, 7], &mc);
        // `region_1` is no longer needed to cached, but since it was loaded less
        // than 10 ms ago, it should not be included in the removed ranges.
        assert!(removed.is_empty());
        std::thread::sleep(Duration::from_millis(100));
        // After 100 ms passed, check again, and verify `region_1` is evictable.

        sim.set_top_regions(&vec![]);
        sim.set_region_stats(&region_stats);
        let (_, removed) = rsm.collect_regions_to_load_and_evict(0, vec![1, 2, 3, 4, 5, 6, 7], &mc);
        assert_eq!(&removed, &[region_1.clone()]);

        let top_regions = vec![(region_8.clone(), new_region_stat(4000, 10))];
        sim.set_top_regions(&top_regions);
        sim.set_region_stats(&region_stats);
        mc.acquire(2000);
        let (_, removed) = rsm.collect_regions_to_load_and_evict(0, vec![2, 3, 4, 5, 6, 7], &mc);
        assert_eq!(&removed, &[region_3.clone()]);
    }

    #[test]
    fn test_collect_candidates_for_eviction() {
        let skiplist_engine = SkiplistEngine::new();
        let mut config = RangeCacheEngineConfig::config_for_test();
        config.stop_load_limit_threshold = Some(ReadableSize::kb(1));
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
            new_region_stat(100, 6),
            new_region_stat(10000, 1000),
            new_region_stat(100000, 100000),
            new_region_stat(100, 50), // will be evicted
            new_region_stat(1000, 120),
            new_region_stat(200, 120), // will be evicted
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
        let rsm = RangeStatsManager::new(config.clone(), Duration::from_millis(10), sim.clone());
        rsm.collect_regions_to_load_and_evict(0, vec![], &mc);
        std::thread::sleep(Duration::from_millis(100));

        let evicted_regions = Arc::new(Mutex::new(vec![]));
        let cbs = Arc::new(Mutex::new(vec![]));

        let evicted_regions2 = evicted_regions.clone();
        let cbs2 = cbs.clone();
        let evict_fn = move |evict_region: &CacheRegion,
                             _: EvictReason,
                             cb: Option<Box<dyn AsyncFnOnce + Send + Sync>>|
              -> Vec<CacheRegion> {
            evicted_regions2.lock().push(evict_region.id);
            cbs2.lock().push(cb.unwrap());
            vec![]
        };

        let handle = std::thread::spawn(move || {
            block_on(async {
                rsm.evict_on_soft_limit_reached(evict_fn, &scheduler, vec![1, 2, 3, 4, 5, 6], &mc)
                    .await
            });
        });

        let t = Instant::now();
        while t.elapsed() < Duration::from_secs(2) {
            std::thread::sleep(Duration::from_millis(100));
            let mut cb_lock = cbs.lock();
            if cb_lock.len() == 2 {
                let evicted_regions_lock = evicted_regions.lock();
                assert_eq!(*evicted_regions_lock, vec![4, 6]);
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
        let mut config = RangeCacheEngineConfig::config_for_test();
        config.load_evict_interval = ReadableDuration(Duration::from_secs(120));
        let config = Arc::new(VersionTrack::new(config));
        let mgr = RangeStatsManager::new(config, Duration::from_secs(120), sim);

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
}
