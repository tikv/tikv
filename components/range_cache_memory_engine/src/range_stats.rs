// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    cmp,
    collections::{BTreeMap, HashSet},
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use collections::HashMap;
use crossbeam::sync::ShardedLock;
use kvproto::metapb::Region;
use parking_lot::Mutex;
use raftstore::coprocessor::RegionInfoProvider;
use simple_moving_average::{SumTreeSMA, SMA};
use tikv_util::info;

use crate::memory_controller::MemoryController;

#[derive(Clone)]
pub(crate) struct RangeStatsManager {
    num_regions: Arc<AtomicUsize>,
    info_provider: Arc<dyn RegionInfoProvider>,
    checking_top_regions: Arc<AtomicBool>,
    region_loaded_at: Arc<ShardedLock<BTreeMap<u64, Instant>>>,
    evict_min_duration: Duration,
    expected_region_size: usize,
    // Moving average of amplification reduction. Amplification reduction is the reduced
    // multiple before and after the cache. When a new top region (of course, not cached) comes in,
    // this moving average number is used to estimate the mvcc amplification after cache so we can
    // use it to determine whether to evict some regions if memory usage is relative high.
    ma_mvcc_amplification_reduction: Arc<Mutex<SumTreeSMA<f64, f64, 10>>>,
    mvcc_amplification_threshold: usize,

    mvcc_amplification_record: Arc<Mutex<HashMap<u64, f64>>>,
}

/// Do not evict a region if has been cached for less than this duration.
pub const DEFAULT_EVICT_MIN_DURATION: Duration = Duration::from_secs(60 * 5);
pub const MIN_REGION_COUNT_TO_EVICT: usize = 5;

impl RangeStatsManager {
    /// Creates a new RangeStatsManager that retrieves state from
    /// `info_provider`.
    ///
    /// * `num_regions` Initial number of top regions to track and cache. This
    ///   may change, see `adjust_max_num_regions` below.
    /// * `evict_min_duration` - do not evict regions that have been loaded for
    ///   less than this duration.
    pub fn new(
        num_regions: usize,
        evict_min_duration: Duration,
        expected_region_size: usize,
        mvcc_amplification_threshold: usize,
        info_provider: Arc<dyn RegionInfoProvider>,
    ) -> Self {
        RangeStatsManager {
            num_regions: Arc::new(AtomicUsize::new(num_regions)),
            info_provider,
            checking_top_regions: Arc::new(AtomicBool::new(false)),
            region_loaded_at: Arc::new(ShardedLock::new(BTreeMap::new())),
            ma_mvcc_amplification_reduction: Arc::new(Mutex::new(SumTreeSMA::new())),
            mvcc_amplification_record: Arc::default(),
            evict_min_duration,
            expected_region_size,
            mvcc_amplification_threshold,
        }
    }

    /// Prevents two instances of this from running concurrently.
    /// Return the previous checking status.
    pub fn set_checking_top_regions(&self, v: bool) -> bool {
        self.checking_top_regions.swap(v, Ordering::Relaxed)
    }

    fn set_max_num_regions(&self, v: usize) {
        self.num_regions.store(v, Ordering::Relaxed);
    }

    /// Returns the maximum number of regions that can be cached.
    ///
    /// See also `adjust_max_num_regions` below.
    pub fn max_num_regions(&self) -> usize {
        self.num_regions.load(Ordering::Relaxed)
    }

    /// Collect candidates for eviction sorted by activity in creasing order:
    ///
    /// 1. Get all the regions sorted (decreasing) by region activity using
    ///    [raftstore::coprocessor::RegionCollector::handle_get_top_regions].
    /// 2. Remove all regions where `is_cached_pred` returns false when passed
    ///    the region's range or those that have been loaded for less than
    ///    `self.evict_min_duration`.
    /// 3. Reverse the list so that it is now sorted in the order of increasing
    ///    activity.
    /// 4. Store the results in `ranges_out` using [Vec::extend].
    pub fn collect_candidates_for_eviction<F>(
        &self,
        regions_out: &mut Vec<(Region, u64)>,
        is_cached_pred: F,
    ) where
        F: Fn(&Region) -> bool,
    {
        // Gets all of the regions, sorted by activity.
        let all_regions = self.info_provider.get_top_regions(None).unwrap();
        let regions_loaded = self.region_loaded_at.read().unwrap();
        regions_out.extend(
            all_regions
                .iter()
                .filter_map(|(region, approx_size)| {
                    is_cached_pred(region)
                        .then(|| {
                            match regions_loaded.get(&region.get_id()) {
                                // Do not evict ranges that were loaded less than
                                // `EVICT_MIN_DURATION` ago.
                                Some(&time_loaded)
                                    if Instant::now() - time_loaded < self.evict_min_duration =>
                                {
                                    None
                                }
                                Some(_) | None =>
                                // None indicates range loaded from a hint, not by this manager.
                                {
                                    Some((region.clone(), approx_size.approximate_size))
                                }
                            }
                        })
                        .flatten()
                })
                .rev(),
        );
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

    /// Attempt to adjust the maximum number of cached regions based on memory
    /// usage:
    ///
    /// If `curr_memory_usage` is LESS THAN `threshold` by 3 *
    /// self.expected_region_size bytes, *increase* the maximum
    /// by `threshold - curr_memory_usage / 3 * self.expected_region_size`.
    ///
    /// If `curr_memory_usage` is GREATER THAN `threshold`, *decrease* the
    /// maximum by `(curr_memory_usage - threshold) /
    /// self.expected_region_size`.
    pub fn adjust_max_num_regions(&self, curr_memory_usage: usize, threshold: usize) {
        match curr_memory_usage.cmp(&threshold) {
            cmp::Ordering::Less => {
                let room_to_grow = threshold - curr_memory_usage;
                if room_to_grow > self.expected_region_size * 3 {
                    let curr_num_regions = self.max_num_regions();
                    let next_num_regions =
                        curr_num_regions + room_to_grow / (self.expected_region_size * 3);
                    info!("ime increasing number of top regions to cache";
                        "from" => curr_num_regions,
                        "to" => next_num_regions,
                    );
                    self.set_max_num_regions(next_num_regions);
                }
            }
            cmp::Ordering::Greater => {
                let to_shrink_by = curr_memory_usage - threshold;
                let curr_num_regions = self.max_num_regions();
                let next_num_regions = curr_num_regions
                    .checked_sub(1.max(to_shrink_by / self.expected_region_size))
                    .unwrap_or(1)
                    .max(1);
                info!("ime decreasing number of top regions to cache";
                    "from" => curr_num_regions,
                    "to" => next_num_regions,
                );
                self.set_max_num_regions(next_num_regions);
            }
            _ => (),
        };
    }

    /// Collects regions to load and evict based on region activity, mvcc
    /// amplification, and memory constraints.
    /// New top regions will be collected in `regions_added_out` to be loaded.
    ///
    /// If memory usage is below the stop load limit, regions with low activity
    /// or low mvcc amplification are considered for eviction.
    ///
    /// If memory usage reaches stop load limit, whether to evict region is
    /// determined by comparison between the new top regions' activity and the
    /// current cached regions.
    ///
    /// # Parameters
    ///
    /// - `regions_added_out`: A mutable vector that will be populated with
    ///   regions that are loaded into memory.
    /// - `cached_region_ids`: A vector of region IDs that are currently cached.
    /// - `memory_controller`: A reference to the memory controller to determine
    ///   memory usage and constraints.
    ///
    /// # Returns
    ///
    /// A vector of regions that should be evicted from memory.
    pub fn collect_regions_to_load_and_evict(
        &self,
        regions_added_out: &mut Vec<Region>,
        cached_region_ids: Vec<u64>,
        memory_controller: &MemoryController,
    ) -> Vec<Region> {
        // Get regions' activity of the cached region and sort them by next + prev in
        // descending order
        let mut regions_activity = self
            .info_provider
            .get_regions_activity(cached_region_ids.clone())
            .unwrap();
        regions_activity.sort_by(|a, b| {
            let next_prev_a = a.1.cop_detail.next + a.1.cop_detail.prev;
            let next_prev_b = b.1.cop_detail.next + b.1.cop_detail.prev;
            next_prev_b.cmp(&next_prev_a)
        });

        let ma_mvcc_amplification_reduction = {
            let mut ma_mvcc_amplification_reduction = self.ma_mvcc_amplification_reduction.lock();
            let mut record = self.mvcc_amplification_record.lock();
            // update the moving average of the mvcc amplification reduction(the reduced
            // multiple before and after the cache).
            regions_activity.iter().for_each(|(r, a)| {
                if let Some(&amplification) = record.get(&r.id) {
                    let amp_after_cache = a.cop_detail.mvcc_amplification();
                    if amp_after_cache != 0.0 && amp_after_cache != amplification {
                        ma_mvcc_amplification_reduction
                            .add_sample(amplification / a.cop_detail.mvcc_amplification());
                    }
                }
            });
            record.clear();
            // this reduction should not be less than 1
            ma_mvcc_amplification_reduction.get_average()
        };
        info!(
            "IME moving average mvcc amplification reduction update";
            "ma_mvcc_amplification_reduction" => ma_mvcc_amplification_reduction,
        );

        info!("ime collect_changed_ranges"; "num_regions" => self.max_num_regions());
        let curr_top_regions = self
            .info_provider
            .get_top_regions(Some(NonZeroUsize::try_from(self.max_num_regions()).unwrap()))
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
        let mvcc_amplification_to_filter = {
            let mut max_mvcc_amplification: f64 = 0.0;
            let mut record = self.mvcc_amplification_record.lock();
            let added_ranges = curr_top_regions
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
                });
            regions_added_out.extend(added_ranges);
            // `max_mvcc_amplification / ma_mvcc_amplification_reduction` is the
            // expected mvcc amplification factor after cache. Make the half of it to filter
            // the cached regions.
            max_mvcc_amplification / f64::max(1.0, ma_mvcc_amplification_reduction / 2.0)
        };

        info!(
            "IME mvcc amplification reduction filter";
            "mvcc_amplification_to_filter" => mvcc_amplification_to_filter,
        );

        {
            // TODO(SpadeA): remove it after it's stable
            let debug: Vec<_> = regions_activity
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
        let max_count_to_evict = usize::max(1, regions_activity.len() / 10);
        // Use top MIN_REGION_COUNT_TO_EVICT regions next and prev to filter regions
        // with very few next and prev. If there's less than or equal to
        // MIN_REGION_COUNT_TO_EVICT regions, do not evict any one.
        if regions_activity.len() > MIN_REGION_COUNT_TO_EVICT {
            let avg_top_next_prev = if reach_stop_load {
                regions_activity
                    .iter()
                    .map(|r| r.1.cop_detail.next + r.1.cop_detail.prev)
                    .take(MIN_REGION_COUNT_TO_EVICT)
                    .sum::<usize>()
                    / MIN_REGION_COUNT_TO_EVICT
            } else {
                0
            };
            let region_to_evict: Vec<_> = regions_activity
                    .into_iter()
                    .filter(|(_, r)| {
                        if reach_stop_load {
                            r.cop_detail.mvcc_amplification()
                                < mvcc_amplification_to_filter
                        } else {
                            // In this case, memory usage is relarively low, we only evict those that should not be cached apparently.
                            r.cop_detail.mvcc_amplification()
                                // TODO(SpadeA): this hard coded 10 and 20 may be adjusted by observing more workloads.
                                <= self.mvcc_amplification_threshold as f64 / 10.0
                                || (r.cop_detail.next + r.cop_detail.prev)  < avg_top_next_prev / 20
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
        }
    }
}

#[cfg(test)]
pub mod tests {
    use pd_client::{RegionStat, RegionWriteCfCopDetail};
    use raftstore::coprocessor::{self, region_info_accessor::TopRegions, RegionInfoProvider};
    use tikv_util::{
        box_err,
        config::{ReadableSize, VersionTrack},
    };

    use super::*;
    use crate::{engine::SkiplistEngine, test_util::new_region, RangeCacheEngineConfig};

    struct RegionInfoSimulator {
        regions: Mutex<TopRegions>,
        region_activities: Mutex<HashMap<u64, (Region, RegionStat)>>,
    }

    impl RegionInfoSimulator {
        fn set_top_regions(&self, top_regions: &TopRegions) {
            *self.regions.lock() = top_regions.clone()
        }

        fn set_region_activities(&self, region_activities: &Vec<(Region, RegionStat)>) {
            *self.region_activities.lock() = region_activities
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

        fn get_regions_activity(
            &self,
            ids: Vec<u64>,
        ) -> coprocessor::Result<Vec<(Region, RegionStat)>> {
            let g = self.region_activities.lock().clone();
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
        let config = Arc::new(VersionTrack::new(config));
        let mc = MemoryController::new(config, skiplist_engine);
        let region_1 = new_region(1, b"k01", b"k02");

        let region_2 = new_region(2, b"k03", b"k04");
        let sim = Arc::new(RegionInfoSimulator {
            regions: Mutex::new(vec![(region_1.clone(), new_region_stat(1000, 10))]),
            region_activities: Mutex::default(),
        });
        // 10 ms min duration eviction for testing purposes.
        let rsm = RangeStatsManager::new(
            10,
            Duration::from_millis(10),
            RangeCacheEngineConfig::config_for_test().expected_region_size(),
            10,
            sim.clone(),
        );
        let mut added = Vec::new();
        let removed = rsm.collect_regions_to_load_and_evict(&mut added, vec![], &mc);
        assert_eq!(&added, &[region_1.clone()]);
        assert!(removed.is_empty());
        let top_regions = vec![(region_2.clone(), new_region_stat(1000, 8))];
        let region_activities = vec![(region_1.clone(), new_region_stat(20, 10))];
        sim.set_top_regions(&top_regions);
        sim.set_region_activities(&region_activities);
        added.clear();
        let removed = rsm.collect_regions_to_load_and_evict(&mut added, vec![1], &mc);
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
        let region_activities = vec![
            (region_1.clone(), new_region_stat(20, 10)),
            (region_2.clone(), new_region_stat(20, 8)),
        ];
        sim.set_top_regions(&top_regions);
        sim.set_region_activities(&region_activities);
        added.clear();
        let removed = rsm.collect_regions_to_load_and_evict(&mut added, vec![1, 2], &mc);
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

        let region_activities = vec![
            (region_1.clone(), new_region_stat(2, 10)),
            (region_6.clone(), new_region_stat(30, 10)),
            (region_2.clone(), new_region_stat(20, 8)),
            (region_3.clone(), new_region_stat(15, 10)),
            (region_4.clone(), new_region_stat(20, 10)),
            (region_5.clone(), new_region_stat(25, 10)),
            (region_7.clone(), new_region_stat(40, 10)),
        ];
        sim.set_top_regions(&vec![]);
        sim.set_region_activities(&region_activities);
        let removed =
            rsm.collect_regions_to_load_and_evict(&mut added, vec![1, 2, 3, 4, 5, 6, 7], &mc);
        // `region_1` is no longer needed to cached, but since it was loaded less
        // than 10 ms ago, it should not be included in the removed ranges.
        assert!(removed.is_empty());
        std::thread::sleep(Duration::from_millis(100));
        // After 100 ms passed, check again, and verify `region_1` is evictable.

        sim.set_top_regions(&vec![]);
        sim.set_region_activities(&region_activities);
        let removed =
            rsm.collect_regions_to_load_and_evict(&mut added, vec![1, 2, 3, 4, 5, 6, 7], &mc);
        assert_eq!(&removed, &[region_1.clone()]);

        let top_regions = vec![(region_8.clone(), new_region_stat(4000, 10))];
        sim.set_top_regions(&top_regions);
        sim.set_region_activities(&region_activities);
        mc.acquire(2000);
        let removed =
            rsm.collect_regions_to_load_and_evict(&mut added, vec![2, 3, 4, 5, 6, 7], &mc);
        assert_eq!(&removed, &[region_3.clone()]);
    }

    // todo(SpadeA): fix this PR in the next PR (optimize eviction when soft
    // limit reached)
    // #[test]
    // fn test_collect_candidates_for_eviction() {
    //     let skiplist_engine = SkiplistEngine::new();
    //     let mut config = RangeCacheEngineConfig::config_for_test();
    //     config.stop_load_limit_threshold = Some(ReadableSize::kb(1));
    //     let config = Arc::new(VersionTrack::new(config));
    //     let mc = MemoryController::new(config, skiplist_engine);

    //     fn make_region_vec(rs: &[&Region]) -> TopRegions {
    //         let mut stat = RegionStat::default();
    //         stat.approximate_size = 42;
    //         rs.iter().map(|&r| (r.clone(), stat)).collect::<Vec<_>>()
    //     }

    //     let region_1 = new_region(1, b"k1", b"k2");
    //     let region_2 = new_region(2, b"k3", b"k4");
    //     let region_3 = new_region(3, b"k5", b"k6");
    //     let region_4 = new_region(4, b"k7", b"k8");
    //     let region_5 = new_region(5, b"k9", b"k10");
    //     let region_6 = new_region(6, b"k11", b"k12");

    //     let all_regions = make_region_vec(&[
    //         &region_1, &region_2, &region_3, &region_4, &region_5, &region_6,
    //     ]);

    //     let sim = Arc::new(RegionInfoSimulator {
    //         regions: Mutex::new(all_regions.clone()),
    //         region_activities: Mutex::default(),
    //     });
    //     // 10 ms min duration eviction for testing purposes.
    //     let rsm = RangeStatsManager::new(
    //         5,
    //         Duration::from_millis(10),
    //         RangeCacheEngineConfig::config_for_test().expected_region_size(),
    //         10,
    //         sim.clone(),
    //     );
    //     let r_i_p: Arc<dyn RegionInfoProvider> = sim.clone();
    //     let check_is_cached = move |r: &Region| -> bool {
    //         r_i_p.find_region_by_key(&r.start_key).unwrap().get_id() <= 5
    //     };
    //     let mut _added = Vec::new();
    //     let mut _removed = Vec::new();
    //     rsm.collect_regions_to_load_and_evict(&mut _added, vec![], &mc);
    //     let mut candidates_for_eviction = Vec::new();
    //     rsm.collect_candidates_for_eviction(&mut candidates_for_eviction,
    // &check_is_cached);     assert!(candidates_for_eviction.is_empty());
    //     std::thread::sleep(Duration::from_millis(100));
    //     rsm.collect_candidates_for_eviction(&mut candidates_for_eviction,
    // &check_is_cached);     let expected_candidates_for_eviction =
    // all_regions         .iter()
    //         .rev()
    //         .filter_map(|(r, s)| {
    //             if r.get_id() <= 5 {
    //                 Some((r.clone(), *s))
    //             } else {
    //                 None
    //             }
    //         })
    //         .collect::<Vec<_>>();
    //     assert_eq!(expected_candidates_for_eviction,
    // candidates_for_eviction); }
}
