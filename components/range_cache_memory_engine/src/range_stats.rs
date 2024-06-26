// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    cmp,
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use crossbeam::sync::ShardedLock;
use engine_traits::CacheRange;
use kvproto::metapb::Region;
use parking_lot::Mutex;
use raftstore::coprocessor::RegionInfoProvider;
use tikv_util::info;

#[derive(Clone)]
pub(crate) struct RangeStatsManager {
    num_regions: Arc<AtomicUsize>,
    info_provider: Arc<dyn RegionInfoProvider>,
    prev_top_regions: Arc<Mutex<BTreeMap<u64, Region>>>,
    checking_top_regions: Arc<AtomicBool>,
    region_loaded_at: Arc<ShardedLock<BTreeMap<u64, Instant>>>,
    evict_min_duration: Duration,
    expected_region_size: usize,
}

/// Do not evict a region if has been cached for less than this duration.
pub const DEFAULT_EVICT_MIN_DURATION: Duration = Duration::from_secs(60 * 3);

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
        info_provider: Arc<dyn RegionInfoProvider>,
    ) -> Self {
        RangeStatsManager {
            num_regions: Arc::new(AtomicUsize::new(num_regions)),
            info_provider,
            prev_top_regions: Arc::new(Mutex::new(BTreeMap::new())),
            checking_top_regions: Arc::new(AtomicBool::new(false)),
            region_loaded_at: Arc::new(ShardedLock::new(BTreeMap::new())),
            evict_min_duration,
            expected_region_size,
        }
    }

    /// Prevents two instances of this from running concurrently.
    pub fn set_checking_top_regions(&self, v: bool) {
        self.checking_top_regions.store(v, Ordering::Relaxed);
    }

    /// Returns true if another thread is checking top regions.
    pub fn checking_top_regions(&self) -> bool {
        self.checking_top_regions.load(Ordering::Relaxed)
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
        ranges_out: &mut Vec<(CacheRange, u64)>,
        is_cached_pred: F,
    ) where
        F: Fn(&CacheRange) -> bool,
    {
        // Gets all of the regions, sorted by activity.
        let all_regions = self.info_provider.get_top_regions(None).unwrap();
        let regions_loaded = self.region_loaded_at.read().unwrap();
        ranges_out.extend(
            all_regions
                .iter()
                .filter_map(|(region, approx_size)| {
                    let r = CacheRange::from_region(region);
                    is_cached_pred(&r)
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
                                    Some((r, *approx_size))
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
    pub fn handle_range_evicted(&self, evicted_range: &CacheRange) {
        // TODO (afeinberg): This is inefficient.
        let _ = self
            .info_provider
            .find_region_by_key(&evicted_range.start)
            .map(|region| {
                let id = region.get_id();
                let _ = self.prev_top_regions.lock().remove(&id);
                let _ = {
                    let mut regions_loaded = self.region_loaded_at.write().unwrap();
                    regions_loaded.remove(&id)
                };
            });
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
                    info!("increasing number of top regions to cache";
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
                info!("decreasing number of top regions to cache";
                    "from" => curr_num_regions,
                    "to" => next_num_regions,
                );
                self.set_max_num_regions(next_num_regions);
            }
            _ => (),
        };
    }

    /// Collects changes to top regions since the previous time this method was
    /// called. This method should be called by background tasks responsing
    /// for algorithmic loading and eviction.
    ///
    /// 1. Calls [raftstore::coprocessor::RegionCollector::handle_get_top_regions] to
    ///    request the top `self.max_num_regions()` regions.
    ///
    /// 2. If this is the first time this method has been called on this
    ///    instance, stores the results of previous step in `ranges_added_out`
    ///    and returns.
    ///
    /// 3. If this method has been called before, compare results of step 1 with
    ///    previous results:
    ///   - Newly added ranges (regions missing from previous results) are
    ///     stored in `ranges_added_out`. This can happen when
    ///     `max_num_regions()` increases, or when `max_num_regions()` is
    ///     unchanged but the activity order changed.
    ///   - Removed regions - regions included in previous results - but not the
    ///     current ones are stored in `ranges_removed_out`.
    pub fn collect_changed_ranges(
        &self,
        ranges_added_out: &mut Vec<CacheRange>,
        ranges_removed_out: &mut Vec<CacheRange>,
    ) {
        info!("collect_changed_ranges"; "num_regions" => self.max_num_regions());
        let curr_top_regions = self
            .info_provider
            .get_top_regions(Some(NonZeroUsize::try_from(self.max_num_regions()).unwrap()))
            .unwrap() // TODO (afeinberg): Potentially custom error handling here.
            .iter()
            .map(|(r, _)| (r.id, r.clone()))
            .collect::<BTreeMap<_, _>>();
        {
            let mut region_loaded_map = self.region_loaded_at.write().unwrap();
            for &region_id in curr_top_regions.keys() {
                let _ = region_loaded_map.insert(region_id, Instant::now());
            }
        }
        let prev_top_regions = {
            let mut mut_prev_top_regions = self.prev_top_regions.lock();
            let ret = mut_prev_top_regions.clone();
            *mut_prev_top_regions = curr_top_regions.clone();
            ret
        };
        if prev_top_regions.is_empty() {
            ranges_added_out.extend(curr_top_regions.values().map(CacheRange::from_region));
            return;
        }
        let added_ranges = curr_top_regions
            .iter()
            .filter(|(id, _)| !prev_top_regions.contains_key(id))
            .map(|(_, region)| CacheRange::from_region(region));
        let regions_loaded = self.region_loaded_at.read().unwrap();
        let removed_ranges = prev_top_regions.iter().filter_map(|(&id, region)| {
            if !curr_top_regions.contains_key(&id) {
                match regions_loaded.get(&id) {
                    // Do not evict ranges that were loaded less than `EVICT_MIN_DURATION` ago.
                    Some(&time_loaded)
                        if Instant::now() - time_loaded < self.evict_min_duration =>
                    {
                        let mut mut_prev_top_regions = self.prev_top_regions.lock();
                        let _ = mut_prev_top_regions.insert(id, region.clone());
                        None
                    }
                    _ => Some(CacheRange::from_region(region)),
                }
            } else {
                None
            }
        });
        ranges_added_out.extend(added_ranges);
        ranges_removed_out.extend(removed_ranges);
    }
}

#[cfg(test)]
pub mod tests {
    use kvproto::metapb::Peer;
    use raftstore::coprocessor::{self, region_info_accessor::TopRegions, RegionInfoProvider};
    use tikv_util::box_err;

    use super::*;
    use crate::RangeCacheEngineConfig;

    struct RegionInfoSimulator {
        regions: Mutex<TopRegions>,
    }

    impl RegionInfoSimulator {
        fn set_top_regions(&self, top_regions: &TopRegions) {
            *self.regions.lock() = top_regions.clone()
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
    }

    fn new_region(id: u64, start_key: &[u8], end_key: &[u8], version: u64) -> Region {
        let mut region = Region::default();
        region.set_id(id);
        region.set_start_key(start_key.to_vec());
        region.set_end_key(end_key.to_vec());
        region.mut_region_epoch().set_version(version);
        region.mut_peers().push(Peer::default());
        region
    }

    #[test]
    fn test_collect_changed_regions() {
        let region_1 = new_region(1, b"k1", b"k2", 0);

        let region_2 = new_region(2, b"k3", b"k4", 0);
        let sim = Arc::new(RegionInfoSimulator {
            regions: Mutex::new(vec![(region_1.clone(), 42)]),
        });
        // 10 ms min duration eviction for testing purposes.
        let rsm = RangeStatsManager::new(
            5,
            Duration::from_millis(10),
            RangeCacheEngineConfig::config_for_test().expected_region_size(),
            sim.clone(),
        );
        let mut added = Vec::<CacheRange>::new();
        let mut removed = Vec::<CacheRange>::new();
        rsm.collect_changed_ranges(&mut added, &mut removed);
        assert_eq!(&added, &[CacheRange::from_region(&region_1)]);
        assert!(removed.is_empty());
        let top_regions = vec![(region_1.clone(), 42), (region_2.clone(), 7)];
        sim.set_top_regions(&top_regions);
        added.clear();
        removed.clear();
        rsm.collect_changed_ranges(&mut added, &mut removed);
        assert_eq!(&added, &[CacheRange::from_region(&region_2)]);
        assert!(removed.is_empty());
        let region_3 = new_region(3, b"k5", b"k6", 0);
        let region_4 = new_region(4, b"k7", b"k8", 0);
        let region_5 = new_region(5, b"k9", b"k10", 0);
        let region_6 = new_region(6, b"k11", b"k12", 0);
        let top_regions = vec![
            (region_6.clone(), 42),
            (region_2.clone(), 7),
            (region_3.clone(), 8),
            (region_4.clone(), 9),
            (region_5.clone(), 2),
        ];
        sim.set_top_regions(&top_regions);
        added.clear();
        removed.clear();
        rsm.collect_changed_ranges(&mut added, &mut removed);
        assert_eq!(
            &added,
            &[
                CacheRange::from_region(&region_3),
                CacheRange::from_region(&region_4),
                CacheRange::from_region(&region_5),
                CacheRange::from_region(&region_6)
            ]
        );
        // `region_1` is no longer in the top regions list, but since it was loaded less
        // than 10 ms ago, it should not be included in the removed ranges.
        assert!(removed.is_empty());
        std::thread::sleep(Duration::from_millis(100));
        // After 100 ms passed, check again, and verify `region_1` is evictable.
        rsm.collect_changed_ranges(&mut added, &mut removed);
        assert_eq!(&removed, &[CacheRange::from_region(&region_1)]);
    }

    #[test]
    fn test_collect_candidates_for_eviction() {
        fn make_region_vec(rs: &[&Region]) -> TopRegions {
            rs.iter().map(|&r| (r.clone(), 42)).collect::<Vec<_>>()
        }

        let region_1 = new_region(1, b"k1", b"k2", 0);
        let region_2 = new_region(2, b"k3", b"k4", 0);
        let region_3 = new_region(3, b"k5", b"k6", 0);
        let region_4 = new_region(4, b"k7", b"k8", 0);
        let region_5 = new_region(5, b"k9", b"k10", 0);
        let region_6 = new_region(6, b"k11", b"k12", 0);

        let all_regions = make_region_vec(&[
            &region_1, &region_2, &region_3, &region_4, &region_5, &region_6,
        ]);

        let sim = Arc::new(RegionInfoSimulator {
            regions: Mutex::new(all_regions.clone()),
        });
        // 10 ms min duration eviction for testing purposes.
        let rsm = RangeStatsManager::new(
            5,
            Duration::from_millis(10),
            RangeCacheEngineConfig::config_for_test().expected_region_size(),
            sim.clone(),
        );
        let r_i_p: Arc<dyn RegionInfoProvider> = sim.clone();
        let check_is_cached = move |range: &CacheRange| -> bool {
            r_i_p
                .find_region_by_key(&range.start[1..])
                .unwrap()
                .get_id()
                <= 5
        };
        let mut _added = Vec::<CacheRange>::new();
        let mut _removed = Vec::<CacheRange>::new();
        rsm.collect_changed_ranges(&mut _added, &mut _removed);
        let mut candidates_for_eviction = Vec::<(CacheRange, u64)>::new();
        rsm.collect_candidates_for_eviction(&mut candidates_for_eviction, &check_is_cached);
        assert!(candidates_for_eviction.is_empty());
        std::thread::sleep(Duration::from_millis(100));
        rsm.collect_candidates_for_eviction(&mut candidates_for_eviction, &check_is_cached);
        let expected_candidates_for_eviction = all_regions
            .iter()
            .rev()
            .filter_map(|(r, s)| {
                if r.get_id() <= 5 {
                    Some((CacheRange::from_region(r), *s))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(expected_candidates_for_eviction, candidates_for_eviction);
    }
}
