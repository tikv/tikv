// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    result,
    sync::Arc,
};

use engine_rocks::RocksSnapshot;
use engine_traits::{CacheRange, FailedReason};

use crate::engine::{RagneCacheSnapshotMeta, SnapshotList};

#[derive(Debug, Default)]
pub struct RangeMeta {
    id: u64,
    range_snapshot_list: SnapshotList,
    safe_point: u64,
}

impl RangeMeta {
    fn new(id: u64) -> Self {
        Self {
            id,
            range_snapshot_list: SnapshotList::default(),
            safe_point: 0,
        }
    }

    pub(crate) fn safe_point(&self) -> u64 {
        self.safe_point
    }

    pub(crate) fn set_safe_point(&mut self, safe_point: u64) {
        assert!(self.safe_point <= safe_point);
        self.safe_point = safe_point;
    }

    fn derive_from(id: u64, r: &RangeMeta) -> Self {
        Self {
            id,
            range_snapshot_list: SnapshotList::default(),
            safe_point: r.safe_point,
        }
    }

    pub(crate) fn range_snapshot_list(&self) -> &SnapshotList {
        &self.range_snapshot_list
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
// id so that range + id can exactly locate the position.
// When an eviction occured, say we now have k1-k10 in self.ranges and the
// eviction range is k3-k5. k1-k10 will be splitted to three ranges: k1-k3,
// k3-k5, and k5-k10.
// k1-k3 and k5-k10 will be new ranges inserted in self.ranges with meta dervied
// from meta of k1-k10 (only safe_ts and can_read will be derived). k1-k10 will
// be removed from self.ranges and inserted to self.historical_ranges. Then,
// k3-k5 will be in the self.evicted_ranges. Now, we cannot remove the data of
// k3-k5 as there may be some snapshot of k1-k10. After these snapshot are
// dropped, k3-k5 can be acutally removed.
#[derive(Default)]
pub struct RangeManager {
    // Each new range will increment it by one.
    id_allocator: IdAllocator,
    // Range before an eviction. It is recorded due to some undropped snapshot, which block the
    // evicted range deleting the relevant data.
    historical_ranges: BTreeMap<CacheRange, RangeMeta>,
    // `ranges_being_deleted` contains two types of ranges: 1. the range is evicted and not finish
    // the delete, 2. the range is loading data but memory acquirement is rejected.
    pub(crate) ranges_being_deleted: BTreeSet<CacheRange>,
    // ranges that are cached now
    ranges: BTreeMap<CacheRange, RangeMeta>,

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
    pub(crate) pending_ranges: Vec<CacheRange>,
    // The bool indicates the loading is canceled due to memory capcity issue
    pub(crate) pending_ranges_loading_data: VecDeque<(CacheRange, Arc<RocksSnapshot>, bool)>,

    ranges_in_gc: BTreeSet<CacheRange>,
}

impl RangeManager {
    pub(crate) fn ranges(&self) -> &BTreeMap<CacheRange, RangeMeta> {
        &self.ranges
    }

    pub fn new_range(&mut self, range: CacheRange) {
        assert!(!self.overlap_with_range(&range));
        let range_meta = RangeMeta::new(self.id_allocator.allocate_id());
        self.ranges.insert(range, range_meta);
    }

    pub fn mut_range_meta(&mut self, range: &CacheRange) -> Option<&mut RangeMeta> {
        self.ranges.get_mut(range)
    }

    pub fn set_safe_point(&mut self, range: &CacheRange, safe_ts: u64) -> bool {
        if let Some(meta) = self.ranges.get_mut(range) {
            if meta.safe_point > safe_ts {
                return false;
            }
            meta.safe_point = safe_ts;
            true
        } else {
            false
        }
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        self.ranges.keys().any(|r| r.contains_key(key))
    }

    pub fn get_range_for_key(&self, key: &[u8]) -> Option<CacheRange> {
        self.ranges.keys().find_map(|r| {
            if r.contains_key(key) {
                Some(r.clone())
            } else {
                None
            }
        })
    }

    pub fn contains_range(&self, range: &CacheRange) -> bool {
        self.ranges.keys().any(|r| r.contains_range(range))
    }

    pub fn pending_ranges_in_loading_contains(&self, range: &CacheRange) -> bool {
        self.pending_ranges_loading_data
            .iter()
            .any(|(r, ..)| r.contains_range(range))
    }

    pub(crate) fn overlap_with_range(&self, range: &CacheRange) -> bool {
        self.ranges.keys().any(|r| r.overlaps(range))
    }

    // Acquire a snapshot of the `range` with `read_ts`. If the range is not
    // accessable, None will be returned. Otherwise, the range id will be returned.
    pub(crate) fn range_snapshot(
        &mut self,
        range: &CacheRange,
        read_ts: u64,
    ) -> result::Result<u64, FailedReason> {
        let Some(range_key) = self
            .ranges
            .keys()
            .find(|&r| r.contains_range(range))
            .cloned()
        else {
            return Err(FailedReason::NotCached);
        };
        let meta = self.ranges.get_mut(&range_key).unwrap();

        if read_ts <= meta.safe_point {
            return Err(FailedReason::TooOldRead);
        }

        meta.range_snapshot_list.new_snapshot(read_ts);
        Ok(meta.id)
    }

    // If the snapshot is the last one in the snapshot list of one cache range in
    // historical_ranges, it means one or some evicted_ranges may be ready to be
    // removed physically.
    // So, here, we return a vector of ranges to denote the ranges that are ready to
    // be removed.
    pub(crate) fn remove_range_snapshot(
        &mut self,
        snapshot_meta: &RagneCacheSnapshotMeta,
    ) -> Vec<CacheRange> {
        if let Some(range_key) = self
            .historical_ranges
            .iter()
            .find(|&(range, meta)| {
                range.contains_range(&snapshot_meta.range) && meta.id == snapshot_meta.range_id
            })
            .map(|(r, _)| r.clone())
        {
            let meta = self.historical_ranges.get_mut(&range_key).unwrap();
            meta.range_snapshot_list
                .remove_snapshot(snapshot_meta.snapshot_ts);
            if meta.range_snapshot_list.is_empty() {
                self.historical_ranges.remove(&range_key);
            }

            return self
                .ranges_being_deleted
                .iter()
                .filter(|evicted_range| {
                    !self
                        .historical_ranges
                        .keys()
                        .any(|r| r.overlaps(evicted_range))
                })
                .cloned()
                .collect::<Vec<_>>();
        }

        // It must belong to the `self.ranges` if not found in `self.historical_ranges`
        let range_key = self
            .ranges
            .iter()
            .find(|&(range, meta)| {
                range.contains_range(&snapshot_meta.range) && meta.id == snapshot_meta.range_id
            })
            .map(|(r, _)| r.clone())
            .unwrap();
        let meta = self.ranges.get_mut(&range_key).unwrap();
        meta.range_snapshot_list
            .remove_snapshot(snapshot_meta.snapshot_ts);
        vec![]
    }

    // return whether the range can be already removed
    pub(crate) fn evict_range(&mut self, evict_range: &CacheRange) -> bool {
        let range_key = self
            .ranges
            .keys()
            .find(|&r| r.contains_range(evict_range))
            .unwrap_or_else(|| panic!("evict a range that does not contain: {:?}", evict_range))
            .clone();
        let meta = self.ranges.remove(&range_key).unwrap();
        let (left_range, right_range) = range_key.split_off(evict_range);
        assert!((left_range.is_some() || right_range.is_some()) || &range_key == evict_range);

        if let Some(left_range) = left_range {
            let left_meta = RangeMeta::derive_from(self.id_allocator.allocate_id(), &meta);
            self.ranges.insert(left_range, left_meta);
        }

        if let Some(right_range) = right_range {
            let right_meta = RangeMeta::derive_from(self.id_allocator.allocate_id(), &meta);
            self.ranges.insert(right_range, right_meta);
        }

        self.ranges_being_deleted.insert(evict_range.clone());

        if !meta.range_snapshot_list.is_empty() {
            self.historical_ranges.insert(range_key, meta);
            return false;
        }

        // we also need to check with previous historical_ranges
        !self
            .historical_ranges
            .keys()
            .any(|r| r.overlaps(evict_range))
    }

    pub fn has_ranges_in_gc(&self) -> bool {
        !self.ranges_in_gc.is_empty()
    }

    pub fn on_delete_ranges(&mut self, ranges: &[CacheRange]) {
        for r in ranges {
            self.ranges_being_deleted.remove(r);
        }
    }

    pub fn set_ranges_in_gc(&mut self, ranges_in_gc: BTreeSet<CacheRange>) {
        self.ranges_in_gc = ranges_in_gc;
    }

    pub fn on_gc_finished(&mut self, range: BTreeSet<CacheRange>) {
        assert_eq!(range, std::mem::take(&mut self.ranges_in_gc));
    }

    pub fn load_range(&mut self, cache_range: CacheRange) -> Result<(), LoadFailedReason> {
        if self.overlap_with_range(&cache_range) {
            return Err(LoadFailedReason::Overlapped);
        };
        if self.ranges_in_gc.contains(&cache_range) {
            return Err(LoadFailedReason::InGc);
        }
        if self.ranges_being_deleted.contains(&cache_range) {
            return Err(LoadFailedReason::Evicted);
        }
        self.pending_ranges.push(cache_range);
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
pub enum LoadFailedReason {
    Overlapped,
    InGc,
    Evicted,
}

pub enum RangeCacheStatus {
    NotInCache,
    Cached,
    Loading,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use engine_traits::{CacheRange, FailedReason};

    use super::RangeManager;
    use crate::range_manager::LoadFailedReason;

    #[test]
    fn test_range_manager() {
        let mut range_mgr = RangeManager::default();
        let r1 = CacheRange::new(b"k00".to_vec(), b"k10".to_vec());

        range_mgr.new_range(r1.clone());
        range_mgr.set_safe_point(&r1, 5);
        assert_eq!(
            range_mgr.range_snapshot(&r1, 5).unwrap_err(),
            FailedReason::TooOldRead
        );
        range_mgr.range_snapshot(&r1, 8).unwrap();
        range_mgr.range_snapshot(&r1, 10).unwrap();
        let tmp_r = CacheRange::new(b"k08".to_vec(), b"k15".to_vec());
        assert_eq!(
            range_mgr.range_snapshot(&tmp_r, 8).unwrap_err(),
            FailedReason::NotCached
        );
        let tmp_r = CacheRange::new(b"k10".to_vec(), b"k11".to_vec());
        assert_eq!(
            range_mgr.range_snapshot(&tmp_r, 8).unwrap_err(),
            FailedReason::NotCached
        );

        let r_evict = CacheRange::new(b"k03".to_vec(), b"k06".to_vec());
        let r_left = CacheRange::new(b"k00".to_vec(), b"k03".to_vec());
        let r_right = CacheRange::new(b"k06".to_vec(), b"k10".to_vec());
        range_mgr.evict_range(&r_evict);
        let meta1 = range_mgr.historical_ranges.get(&r1).unwrap();
        assert!(range_mgr.ranges_being_deleted.contains(&r_evict));
        assert!(range_mgr.ranges.get(&r1).is_none());
        let meta2 = range_mgr.ranges.get(&r_left).unwrap();
        let meta3 = range_mgr.ranges.get(&r_right).unwrap();
        assert!(meta1.safe_point == meta2.safe_point && meta1.safe_point == meta3.safe_point);

        // evict a range with accurate match
        let _ = range_mgr.range_snapshot(&r_left, 10);
        range_mgr.evict_range(&r_left);
        assert!(range_mgr.historical_ranges.get(&r_left).is_some());
        assert!(range_mgr.ranges_being_deleted.contains(&r_left));
        assert!(range_mgr.ranges.get(&r_left).is_none());

        assert!(!range_mgr.evict_range(&r_right));
        assert!(range_mgr.historical_ranges.get(&r_right).is_none());
    }

    #[test]
    fn test_range_load() {
        let mut range_mgr = RangeManager::default();
        let r1 = CacheRange::new(b"k00".to_vec(), b"k10".to_vec());
        let r2 = CacheRange::new(b"k10".to_vec(), b"k20".to_vec());
        let r3 = CacheRange::new(b"k20".to_vec(), b"k30".to_vec());
        let r4 = CacheRange::new(b"k25".to_vec(), b"k35".to_vec());
        range_mgr.new_range(r1.clone());
        range_mgr.new_range(r3.clone());
        range_mgr.evict_range(&r1);

        let mut gced = BTreeSet::default();
        gced.insert(r2.clone());
        range_mgr.set_ranges_in_gc(gced);

        assert_eq!(
            range_mgr.load_range(r1).unwrap_err(),
            LoadFailedReason::Evicted
        );

        assert_eq!(
            range_mgr.load_range(r2).unwrap_err(),
            LoadFailedReason::InGc
        );

        assert_eq!(
            range_mgr.load_range(r4).unwrap_err(),
            LoadFailedReason::Overlapped
        );
    }
}
