// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{BTreeMap, VecDeque},
    result,
    sync::Arc,
};

use collections::HashMap;
use engine_rocks::RocksSnapshot;
use engine_traits::{CacheRange, FailedReason};
use kvproto::metapb::Region;
use tikv_util::{info, time::Instant};

use crate::{metrics::RANGE_EVICTION_DURATION_HISTOGRAM, read::RangeCacheSnapshotMeta};

// read_ts -> ref_count
#[derive(Default, Debug)]
pub(crate) struct SnapshotList(pub(crate) BTreeMap<u64, u64>);

impl SnapshotList {
    pub(crate) fn new_snapshot(&mut self, read_ts: u64) {
        // snapshot with this ts may be granted before
        let count = self.0.get(&read_ts).unwrap_or(&0) + 1;
        self.0.insert(read_ts, count);
    }

    pub(crate) fn remove_snapshot(&mut self, read_ts: u64) {
        let count = self.0.get_mut(&read_ts).unwrap();
        assert!(*count >= 1);
        if *count == 1 {
            self.0.remove(&read_ts).unwrap();
        } else {
            *count -= 1;
        }
    }

    // returns the min snapshot_ts (read_ts) if there's any
    pub fn min_snapshot_ts(&self) -> Option<u64> {
        self.0.first_key_value().map(|(ts, _)| *ts)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[derive(Debug, Default)]
pub struct RangeMeta {
    // start_key and end_key cannot uniquely identify a range as range can split and merge, so we
    // need a range id.
    id: u64,
    region: Region,
    region_snapshot_list: SnapshotList,
    safe_point: u64,
}

impl RangeMeta {
    fn new(id: u64, region: Region) -> Self {
        Self {
            id,
            region,
            region_snapshot_list: SnapshotList::default(),
            safe_point: 0,
        }
    }

    #[inline]
    pub(crate) fn region(&self) -> &Region {
        &self.region
    }

    pub(crate) fn safe_point(&self) -> u64 {
        self.safe_point
    }

    pub(crate) fn set_safe_point(&mut self, safe_point: u64) {
        assert!(self.safe_point <= safe_point);
        self.safe_point = safe_point;
    }

    pub(crate) fn region_snapshot_list(&self) -> &SnapshotList {
        &self.region_snapshot_list
    }
}

#[derive(Default)]
struct IdAllocator(u64);

impl IdAllocator {
    fn allocate_id(&mut self) -> u64 {
        self.0 += 1;
        self.0
    }
}

// RangeManger manges the ranges for RangeCacheMemoryEngine. Every new ranges
// (whether created by new_range or by splitted due to eviction) has an unique
// id so that range + id can exactly identify which range it is.
// When an eviction occured, say we now have k1-k10 in self.ranges and the
// eviction range is k3-k5. k1-k10 will be splitted to three ranges: k1-k3,
// k3-k5, and k5-k10.
// k1-k3 and k5-k10 will be new ranges inserted in self.ranges with meta dervied
// from meta of k1-k10 (only safe_ts will be derived). k1-k10 will be removed
// from self.ranges and inserted to self.historical_regions. Then, k3-k5 will be
// in the self.evicted_ranges. Now, we cannot remove the data of k3-k5 as there
// may be some snapshot of k1-k10. After these snapshot are dropped, k3-k5 can
// be acutally removed.
#[derive(Default)]
pub struct RangeManager {
    // Each new range will increment it by one.
    id_allocator: IdAllocator,
    // Region before an eviction. It is recorded due to some undropped snapshot, which block the
    // evicted range deleting the relevant data.
    historical_regions: BTreeMap<CacheRange, RangeMeta>,
    // `ranges_being_deleted` contains ranges that are evicted but not finished the delete (or even
    // not start to delete due to ongoing snapshot)
    // `bool` means whether the range has been scheduled to the delete range worker
    pub(crate) regions_being_deleted: BTreeMap<CacheRange, (Region, bool, Instant)>,
    // ranges that are cached now
    regions_by_range: BTreeMap<CacheRange, u64>,
    regions: HashMap<u64, RangeMeta>,

    // `pending_ranges` contains ranges that will be loaded into the memory engine. To guarantee
    // the completeness of the data, we also need to write the data that is applied after the
    // snapshot is acquired. And to ensure the data is written by order, we should cache the data
    // that is applied after the snapshot acquired and only consume them when snapshot load
    // finishes.
    // So, at sometime in the apply thread, the pending ranges, coupled with rocksdb
    // snapshot, will be poped and pushed into `pending_ranges_loading_data` (data here means the
    // data in snapshot and in further applied write). Then the data in the snapshot of the
    // given ranges will be loaded in the memory engine in the background worker. When the
    // snapshot load is finished, we begin to consume the write batch that is cached after the
    // snapshot is acquired.
    //
    // Note: as we will release lock during the consuming of the cached write batch, there could be
    // further write batch being cached. We must ensure the cached write batch is empty at the time
    // the range becoming accessable range.
    //
    // Note: the region with range equaling to the range in the `pending_range` may have been
    // split. This is fine, we just let the first child region that calls the prepare_for_apply
    // to schedule it. We should cache writes for all child regions, and the load task
    // completes as long as the snapshot has been loaded and the cached write batches for this
    // super range have all been consumed.
    pub(crate) pending_regions: Vec<Region>,
    // The bool indicates the loading is canceled due to memory capcity issue
    pub(crate) pending_regions_loading_data: VecDeque<(Region, Arc<RocksSnapshot>, bool)>,

    regions_in_gc: BTreeMap<CacheRange, u64>,
    // Record the ranges that are being written.
    //
    // It is used to avoid the conccurency issue between delete range and write to memory: after
    // the range is evicted or failed to load, the range is recorded in `ranges_being_deleted`
    // which means no further write of it is allowed, and a DeleteRange task of the range will be
    // scheduled to cleanup the dirty data. However, it is possible that the apply thread is
    // writting data for this range. Therefore, we have to delay the DeleteRange task until the
    // range leaves the `ranges_being_written`.
    //
    // The key in this map is the id of the write batch, and the value is a collection
    // the ranges of this batch. So, when the write batch is consumed by the in-memory engine,
    // all ranges of it are cleared from `ranges_being_written`.
    regions_being_written: HashMap<u64, Vec<(u64, CacheRange)>>,
}

impl RangeManager {
    pub(crate) fn regions(&self) -> &HashMap<u64, RangeMeta> {
        &self.regions
    }

    pub(crate) fn historical_regions(&self) -> &BTreeMap<CacheRange, RangeMeta> {
        &self.historical_regions
    }

    pub fn new_region(&mut self, region: Region) {
        assert!(!self.overlap_with_region(&region));
        let id = region.id;
        let region_range = CacheRange::new(region.start_key.clone(), region.end_key.clone());
        let range_meta = RangeMeta::new(self.id_allocator.allocate_id(), region);
        self.regions.insert(id, range_meta);
        self.regions_by_range.insert(region_range, id);
    }

    pub fn mut_region_meta(&mut self, id: u64) -> Option<&mut RangeMeta> {
        self.regions.get_mut(&id)
    }

    pub fn set_safe_point(&mut self, region_id: u64, safe_ts: u64) -> bool {
        if let Some(meta) = self.regions.get_mut(&region_id) {
            if meta.safe_point > safe_ts {
                return false;
            }
            meta.safe_point = safe_ts;
            true
        } else {
            false
        }
    }

    pub fn get_region_for_key(&self, key: &[u8]) -> Option<Region> {
        let id = self.regions_by_range.iter().find_map(|r| {
            if r.0.contains_key(key) {
                Some(*r.1)
            } else {
                None
            }
        });
        id.and_then(|id| self.regions.get(&id).map(|m| m.region.clone()))
    }

    pub fn contains_region(&self, region_id: u64) -> bool {
        self.regions.contains_key(&region_id)
    }

    pub fn pending_regions_in_loading_contains(&self, region_id: u64) -> bool {
        self.pending_regions_loading_data
            .iter()
            .any(|r| r.0.id == region_id)
        // self.pending_ranges_loading_data
        //     .iter()
        //     .any(|(r, ..)| r.contains_region(range))
    }

    fn overlap_with_region(&self, region: &Region) -> bool {
        self.regions_by_range
            .keys()
            .any(|r| r.overlaps_with_region(region))
    }

    // Acquire a snapshot of the `range` with `read_ts`. If the range is not
    // accessable, None will be returned. Otherwise, the range id will be returned.
    pub(crate) fn region_snapshot(
        &mut self,
        region_id: u64,
        region_epoch: u64,
        read_ts: u64,
    ) -> result::Result<u64, FailedReason> {
        let Some(meta) = self.regions.get_mut(&region_id) else {
            return Err(FailedReason::NotCached);
        };

        if meta.region.get_region_epoch().version != region_epoch {
            return Err(FailedReason::EpochNotMatch);
        }

        if read_ts <= meta.safe_point {
            return Err(FailedReason::TooOldRead);
        }

        meta.region_snapshot_list.new_snapshot(read_ts);
        Ok(meta.id)
    }

    // If the snapshot is the last one in the snapshot list of one cache range in
    // historical_regions, it means one or some evicted_ranges may be ready to be
    // removed physically.
    // So, we return a vector of ranges to denote the ranges that are ready to be
    // removed.range_key
    pub(crate) fn remove_region_snapshot(
        &mut self,
        snapshot_meta: &RangeCacheSnapshotMeta,
    ) -> Vec<Region> {
        if let Some(meta) = self.historical_regions.get_mut(&snapshot_meta.range) {
            meta.region_snapshot_list
                .remove_snapshot(snapshot_meta.snapshot_ts);
            if meta.region_snapshot_list.is_empty() {
                self.historical_regions.remove(&snapshot_meta.range);
            }

            let mut deletable_regions = vec![];
            for (range, (region, scheduled, _)) in &mut self.regions_being_deleted {
                if !self.historical_regions.keys().any(|r| r.overlaps(range)) {
                    *scheduled = true;
                    deletable_regions.push(region.clone());
                }
            }
            return deletable_regions;
        }

        // It must belong to the `self.regions` if not found in
        // `self.historical_regions`
        let region_meta = self.regions.get_mut(&snapshot_meta.region_id).unwrap();
        region_meta
            .region_snapshot_list
            .remove_snapshot(snapshot_meta.snapshot_ts);

        vec![]
    }

    /// Return ranges that can be deleted now (no ongoing snapshot).
    // There are two cases based on the relationship between `evict_range` and
    // cached ranges:
    // 1. `evict_range` is contained(including equals) by a cached range (at most
    //    one due to non-overlapping in cached ranges)
    // 2. `evict_range` is overlapped with (including contains but not be contained)
    //    one or more cached ranges
    //
    // For 1, if the `evict_range` is a proper subset of the cached_range, we will
    // split the cached_range so that only the `evict_range` part will be evicted
    // and deleted.
    //
    // For 2, this is caused by some special operations such as merge and delete
    // range. So, conservatively, we evict all ranges overlap with it.
    pub(crate) fn evict_region(&mut self, evict_region: &Region) -> Vec<Region> {
        info!(
            "try to evict region";
            "evict_region" => ?evict_region,
        );

        // cancel loading ranges overlapped with `evict_range`
        self.pending_regions_loading_data
            .iter_mut()
            .for_each(|(r, _, canceled)| {
                if is_region_overlap(evict_region, r) {
                    // TODO: handle epoch changes.
                    info!("evict overlapped region"; "evicted_region" => ?evict_region, "overlapped_region" => ?r,);
                    *canceled = true;
                }
            });

        let mut deleteable_regions = vec![];
        let mut need_scan = true;
        if let Some(meta) = self.regions.get(&evict_region.id) {
            // if epoch not changed, no need to do range scan.
            if meta.region.get_region_epoch().version == evict_region.get_region_epoch().version {
                need_scan = false;
            }
            if !need_scan || is_region_overlap(&meta.region, evict_region) {
                if let Some(region) = self.do_evict_region(evict_region.id, evict_region) {
                    deleteable_regions.push(region);
                }
            }
        }

        if need_scan {
            let evict_ids: Vec<_> = self
                .regions_by_range
                .iter()
                .filter_map(|(r, id)| {
                    if r.overlaps_with_region(&evict_region) {
                        Some(*id)
                    } else {
                        None
                    }
                })
                .collect();
            for rid in evict_ids {
                if let Some(region) = self.do_evict_region(rid, evict_region) {
                    deleteable_regions.push(region);
                }
            }
        }

        deleteable_regions
    }

    // return the region if it can be directly deleted.
    fn do_evict_region(&mut self, id: u64, evict_region: &Region) -> Option<Region> {
        let meta = self.remove_region(id).unwrap();
        assert!(is_region_overlap(&meta.region, evict_region));
        info!(
            "evict overlap region in cache range engine";
            "target_region" => ?evict_region,
            "overlap_region" => ?meta.region,
        );
        let range = CacheRange::from_region(&meta.region);
        self.regions_being_deleted
            .insert(range.clone(), (meta.region.clone(), false, Instant::now()));
        if !meta.region_snapshot_list.is_empty() {
            self.historical_regions.insert(range, meta);
        } else if !self
            .historical_regions
            .keys()
            .any(|r| r.overlaps_with_region(&meta.region))
        {
            return Some(meta.region);
        }
        None
    }

    fn remove_region(&mut self, id: u64) -> Option<RangeMeta> {
        self.regions.remove(&id).map(|m| {
            self.regions_by_range
                .remove(&CacheRange::from_region(&m.region))
                .unwrap();
            m
        })
    }

    pub fn has_regions_in_gc(&self) -> bool {
        !self.regions_in_gc.is_empty()
    }

    pub fn on_delete_ranges(&mut self, ranges: &[CacheRange]) {
        for r in ranges {
            let (_, _, t) = self.regions_being_deleted.remove(r).unwrap();
            RANGE_EVICTION_DURATION_HISTOGRAM.observe(t.saturating_elapsed_secs());
            info!(
                "range eviction done";
                "range" => ?r,
            );
        }
    }

    pub fn set_regions_in_gc(&mut self, regions_in_gc: BTreeMap<CacheRange, u64>) {
        self.regions_in_gc = regions_in_gc;
    }

    pub(crate) fn is_overlapped_with_regions_being_written(&self, r: &Region) -> bool {
        self.regions_being_written.iter().any(|(_, ranges)| {
            ranges
                .iter()
                .any(|range_being_written| range_being_written.1.overlaps_with_region(r))
        })
    }

    pub(crate) fn record_in_region_being_written(
        &mut self,
        write_batch_id: u64,
        range: CacheRange,
        region_id: u64,
    ) {
        self.regions_being_written
            .entry(write_batch_id)
            .or_default()
            .push((region_id, range))
    }

    pub(crate) fn clear_regions_in_being_written(
        &mut self,
        write_batch_id: u64,
        has_entry_applied: bool,
    ) {
        let regions = self.regions_being_written.remove(&write_batch_id);
        if has_entry_applied {
            assert!(!regions.unwrap().is_empty());
        }
    }

    pub fn on_gc_finished(&mut self, range: BTreeMap<CacheRange, u64>) {
        assert_eq!(range, std::mem::take(&mut self.regions_in_gc));
    }

    pub fn add_pinned_range(&mut self, _cache_range: CacheRange) {
        // TODO: handle pinned range later
    }

    pub fn load_region(&mut self, region: Region) -> Result<(), LoadFailedReason> {
        if self.overlap_with_region(&region) {
            return Err(LoadFailedReason::Overlapped);
        }
        if self
            .pending_regions
            .iter()
            .any(|r| is_region_overlap(r, &region))
            || self
                .pending_regions_loading_data
                .iter()
                .any(|(r, ..)| is_region_overlap(r, &region))
        {
            return Err(LoadFailedReason::PendingRange);
        }
        if self
            .regions_in_gc
            .keys()
            .any(|r| r.overlaps_with_region(&region))
        {
            return Err(LoadFailedReason::InGc);
        }
        if self
            .regions_being_deleted
            .values()
            .any(|r| is_region_overlap(&r.0, &region))
        {
            return Err(LoadFailedReason::Evicting);
        }
        self.pending_regions.push(region);
        Ok(())
    }

    // Only ranges that have not been scheduled will be retained in `ranges`
    pub fn mark_delete_regions_scheduled(&mut self, regions: &mut Vec<Region>) {
        regions.retain(|r| {
            let range = CacheRange::new(r.start_key.clone(), r.end_key.clone());
            let (_, scheduled, _) = self.regions_being_deleted.get_mut(&range).unwrap();
            !std::mem::replace(scheduled, true)
        });
    }

    // return `true` is the region is evicted.
    pub(crate) fn split_region(
        &mut self,
        source_region: &Region,
        new_regions: Vec<kvproto::metapb::Region>,
    ) -> bool {
        if !self.regions.contains_key(&source_region.id) {
            info!("split source region not cached"; "region_id" => source_region.id);
            return false;
        }

        if let Some(source_meta) = self.regions.remove(&source_region.id) {
            assert_eq!(
                source_meta.region.get_region_epoch().version,
                source_region.get_region_epoch().version
            );
            let region_range = CacheRange::new(
                source_meta.region.start_key.clone(),
                source_meta.region.end_key.clone(),
            );
            assert!(self.regions_by_range.remove(&region_range).is_some());
            for region in new_regions {
                let id = self.id_allocator.allocate_id();
                let region_id = region.id;
                let range = CacheRange::new(region.start_key.clone(), region.end_key.clone());
                let mut meta = RangeMeta::new(id, region);
                meta.set_safe_point(source_meta.safe_point());
                self.regions_by_range.insert(range, region_id);
                self.regions.insert(region_id, meta);
            }
            self.historical_regions.insert(region_range, source_meta);
            // TODO: handle regions_in_gc
            return false;
        }

        // TODO: to make the temp change easier, we just evict corresponding region if
        // it is not in the active state.
        self.pending_regions.retain(|r| r.id == source_region.id);
        for (r, _, canceled) in &mut self.pending_regions_loading_data {
            if r.id == source_region.id {
                *canceled = true;
                return true;
            }
        }
        false
    }
}

pub fn is_region_overlap(rg1: &Region, rg2: &Region) -> bool {
    rg1.start_key < rg2.end_key && rg1.end_key > rg2.start_key
}

#[derive(Debug, PartialEq)]
pub enum LoadFailedReason {
    Overlapped,
    PendingRange,
    InGc,
    Evicting,
}

pub enum RangeCacheStatus {
    NotInCache,
    Cached,
    Loading,
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use engine_traits::{CacheRange, FailedReason};

    use super::RangeManager;
    use crate::{engine::tests::new_region, range_manager::LoadFailedReason};

    #[test]
    fn test_range_manager() {
        let mut range_mgr = RangeManager::default();
        let r1 = new_region(1, "k00", b"k10");

        range_mgr.new_region(r1.clone());
        range_mgr.set_safe_point(r1.id, 5);
        assert_eq!(
            range_mgr.region_snapshot(r1.id, 0, 5).unwrap_err(),
            FailedReason::TooOldRead
        );
        range_mgr.region_snapshot(r1.id, 0, 8).unwrap();
        range_mgr.region_snapshot(r1.id, 0, 10).unwrap();
        assert_eq!(
            range_mgr.region_snapshot(2, 0, 8).unwrap_err(),
            FailedReason::NotCached
        );

        let mut r_evict = new_region(2, b"k03", b"k06");
        let mut r_left = new_region(1, b"k00", b"k03");
        let mut r_right = new_region(3, b"k06", b"k10");
        for r in [&mut r_evict, &mut r_left, &mut r_right] {
            r.mut_region_epoch().version = 2;
        }
        range_mgr.split_region(&r1, vec![r_left, r_evict.clone(), r_right]);
        range_mgr.evict_region(&r_evict);
        let meta1 = range_mgr
            .historical_regions
            .get(&CacheRange::from_region(&r1))
            .unwrap();
        assert!(
            range_mgr
                .regions_being_deleted
                .get(&CacheRange::from_region(&r_evict))
                .is_some()
        );
        assert!(
            range_mgr
                .regions_by_range
                .get(&CacheRange::from_region(&r1))
                .is_none()
        );
        let meta2 = range_mgr.regions.get(&r_left.id).unwrap();
        let meta3 = range_mgr.regions.get(&r_right.id).unwrap();
        assert!(meta1.safe_point == meta2.safe_point && meta1.safe_point == meta3.safe_point);

        // evict a range with accurate match
        let _ = range_mgr.region_snapshot(r_left.id, 1, 10);
        range_mgr.evict_region(&r_left);
        assert!(
            range_mgr
                .historical_regions
                .get(&CacheRange::from_region(&r_left))
                .is_some()
        );
        assert!(
            range_mgr
                .regions_being_deleted
                .get(&CacheRange::from_region(&r_left))
                .is_some()
        );
        assert!(range_mgr.regions.get(&r_left.id).is_none());

        assert!(range_mgr.evict_region(&r_right).is_empty());
        assert!(
            range_mgr
                .historical_regions
                .get(&CacheRange::from_region(&r_right))
                .is_none()
        );
    }

    #[test]
    fn test_range_load() {
        let mut range_mgr = RangeManager::default();
        let r1 = new_region(1, b"k00", b"k10");
        let r2 = new_region(2, b"k10", b"k20");
        let r3 = new_region(3, b"k20", b"k30");
        let r4 = new_region(4, b"k25", b"k35");

        range_mgr.new_region(r1.clone());
        range_mgr.new_region(r3.clone());
        range_mgr.evict_region(&r1);

        let mut gced = BTreeMap::default();
        gced.insert(CacheRange::from_region(&r2), r2.id);
        range_mgr.set_regions_in_gc(gced);

        assert_eq!(
            range_mgr.load_region(r1).unwrap_err(),
            LoadFailedReason::Evicting
        );

        assert_eq!(
            range_mgr.load_region(r2).unwrap_err(),
            LoadFailedReason::InGc
        );

        assert_eq!(
            range_mgr.load_region(r4).unwrap_err(),
            LoadFailedReason::Overlapped
        );
    }

    #[test]
    fn test_range_load_overlapped() {
        let mut range_mgr = RangeManager::default();
        let r1 = new_region(1, b"k00", b"k10");
        let r2 = new_region(2, b"k20", b"k30");
        let r3 = new_region(3, b"k40", b"k50");
        range_mgr.new_region(r1.clone());
        range_mgr.evict_region(&r1);

        let mut gced = BTreeMap::default();
        gced.insert(CacheRange::from_region(&r2), r2.id);
        range_mgr.set_regions_in_gc(gced);

        range_mgr.load_region(r3).unwrap();

        let r = new_region(4, b"k00", b"k05");
        assert_eq!(
            range_mgr.load_region(r).unwrap_err(),
            LoadFailedReason::Evicting
        );
        let r = new_region(4, b"k05", b"k15");
        assert_eq!(
            range_mgr.load_region(r).unwrap_err(),
            LoadFailedReason::Evicting
        );

        let r = new_region(4, b"k15", b"k25");
        assert_eq!(
            range_mgr.load_region(r).unwrap_err(),
            LoadFailedReason::InGc
        );
        let r = new_region(4, b"k25", b"k35");
        assert_eq!(
            range_mgr.load_region(r).unwrap_err(),
            LoadFailedReason::InGc
        );

        let r = new_region(4, b"k35", b"k45");
        assert_eq!(
            range_mgr.load_region(r).unwrap_err(),
            LoadFailedReason::PendingRange
        );
        let r = new_region(4, b"k45", b"k55");
        assert_eq!(
            range_mgr.load_region(r).unwrap_err(),
            LoadFailedReason::PendingRange
        );
    }

    #[test]
    fn test_evict_regions() {
        {
            let mut range_mgr = RangeManager::default();
            let r1 = new_region(1, b"k00", b"k10");
            let r2 = new_region(2, b"k20", b"k30");
            let r3 = new_region(3, b"k40", b"k50");
            range_mgr.new_region(r1.clone());
            range_mgr.new_region(r2.clone());
            range_mgr.new_region(r3.clone());
            range_mgr.contains_region(r1.id);
            range_mgr.contains_region(r2.id);
            range_mgr.contains_region(r3.id);

            let mut r4 = new_region(4, b"00", b"05");
            r4.mut_region_epoch().version = 2;
            assert_eq!(range_mgr.evict_region(&r4), vec![r1]);
        }

        {
            let mut range_mgr = RangeManager::default();
            let r1 = new_region(1, b"k00", b"k10");
            let r2 = new_region(2, b"k20", b"k30");
            let r3 = new_region(3, b"k40", b"k50");
            range_mgr.new_region(r1.clone());
            range_mgr.new_region(r2.clone());
            range_mgr.new_region(r3.clone());
            assert!(range_mgr.contains_region(r1.id));
            assert!(range_mgr.contains_region(r2.id));
            assert!(range_mgr.contains_region(r3.id));

            let r4 = new_region(1, b"k", b"k51");
            assert_eq!(range_mgr.evict_region(&r4), vec![r1, r2, r3]);
            assert!(range_mgr.regions().is_empty());
        }

        {
            let mut range_mgr = RangeManager::default();
            let r1 = new_region(1, b"k00", b"k10");
            let r2 = new_region(2, b"k20", b"k30");
            let r3 = new_region(3, b"k40", b"k50");
            range_mgr.new_region(r1.clone());
            range_mgr.new_region(r2.clone());
            range_mgr.new_region(r3.clone());

            let r4 = new_region(4, b"k25", b"k55");
            assert_eq!(range_mgr.evict_region(&r4), vec![r2, r3]);
            assert_eq!(range_mgr.regions().len(), 1);
        }

        {
            let mut range_mgr = RangeManager::default();
            let r1 = new_region(1, b"k00", b"k10");
            let r2 = new_region(2, b"k30", b"k40");
            let r3 = new_region(3, b"k50", b"k60");
            range_mgr.new_region(r1.clone());
            range_mgr.new_region(r2.clone());
            range_mgr.new_region(r3.clone());

            let r4 = new_region(1, b"k25", b"k75");
            assert_eq!(range_mgr.evict_region(&r4), vec![r2, r3]);
            assert_eq!(range_mgr.regions().len(), 1);
        }
    }
}
