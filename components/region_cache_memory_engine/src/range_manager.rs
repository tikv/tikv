// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    result,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use collections::HashMap;
use engine_rocks::RocksSnapshot;
use engine_traits::{CacheRange, FailedReason};
use kvproto::metapb;
use tikv_util::info;

use crate::read::RangeCacheSnapshotMeta;

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
// id so that range + id can exactly identify which range it is.
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
    // `ranges_being_deleted` contains ranges that are evicted but not finished the delete (or even
    // not start to delete due to ongoing snapshot)
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
    pub(crate) ranges_being_written: HashMap<u64, Vec<CacheRange>>,
    range_evictions: AtomicU64,
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

    fn overlap_with_range(&self, range: &CacheRange) -> bool {
        self.ranges.keys().any(|r| r.overlaps(range))
    }

    pub fn overlap_or_contained_by_range(&self, range: &CacheRange) -> Option<CacheRange> {
        self.ranges
            .keys()
            .find(|r| r.overlaps(range) || range.contains_range(r))
            .cloned()
    }

    fn overlap_with_evicting_range(&self, range: &CacheRange) -> bool {
        self.ranges_being_deleted.iter().any(|r| r.overlaps(range))
    }

    fn overlap_with_range_in_gc(&self, range: &CacheRange) -> bool {
        self.ranges_in_gc.iter().any(|r| r.overlaps(range))
    }

    fn overlap_with_pending_range(&self, range: &CacheRange) -> bool {
        self.pending_ranges.iter().any(|r| r.overlaps(range))
            || self
                .pending_ranges_loading_data
                .iter()
                .any(|(r, ..)| r.overlaps(range))
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

        if read_ts < meta.safe_point {
            return Err(FailedReason::TooOldRead);
        }

        meta.range_snapshot_list.new_snapshot(read_ts);
        Ok(meta.id)
    }

    // If the snapshot is the last one in the snapshot list of one cache range in
    // historical_ranges, it means one or some evicted_ranges may be ready to be
    // removed physically.
    // So, we return a vector of ranges to denote the ranges that are ready to be
    // removed.
    pub(crate) fn remove_range_snapshot(
        &mut self,
        snapshot_meta: &RangeCacheSnapshotMeta,
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
    pub(crate) fn evict_range(&mut self, evict_range: &CacheRange) -> Option<CacheRange> {
        let Some(range_key) = self
            .ranges
            .keys()
            .find(|&r| r.contains_range(evict_range))
            .cloned()
        else {
            self.pending_ranges.retain(|r| {
                if r.overlaps(evict_range)
                    || evict_range.contains_range(r)
                    || r.contains_range(evict_range)
                {
                    info!(
                        "evict range that overlaps with pending range";
                        "evicted_range" => ?evict_range,
                        "overlapped_range" => ?r,
                    );
                    false
                } else {
                    true
                }
            });
            if let Some((range_key, _, canceled)) =
                self.pending_ranges_loading_data.iter_mut().find(|(r, ..)| {
                    evict_range.overlaps(r)
                        || evict_range.contains_range(r)
                        || r.contains_range(evict_range)
                })
            {
                // The range may be loading now
                info!(
                    "evict range that overlaps with loading range";
                    "evicted_range" => ?evict_range,
                    "overlapped_range" => ?range_key,
                );
                *canceled = true;
            }

            if let Some(range_key) = self
                .ranges
                .keys()
                .find(|&r| r.overlaps(evict_range) || evict_range.contains_range(r))
                .cloned()
            {
                info!(
                    "evict range that overlaps";
                    "evicted_range" => ?evict_range,
                    "overlapped_range" => ?range_key,
                );
                if range_key.overlaps(evict_range) {
                    unreachable!();
                }
                let meta = self.ranges.remove(&range_key).unwrap();
                self.ranges_being_deleted.insert(evict_range.clone());
                if !meta.range_snapshot_list.is_empty() {
                    assert!(self.historical_ranges.insert(range_key, meta).is_none());
                    return None;
                }
                return Some(range_key);
            }

            info!(
                "evict a range that is not cached";
                "range" => ?evict_range,
            );
            return None;
        };

        info!(
            "evict range in cache range engine";
            "range_start" => log_wrappers::Value(&evict_range.start),
            "range_end" => log_wrappers::Value(&evict_range.end),
        );
        self.range_evictions.fetch_add(1, Ordering::Relaxed);
        let meta = self.ranges.remove(&range_key).unwrap();
        let (left_range, right_range) = range_key.split_off(evict_range);
        assert!((left_range.is_some() || right_range.is_some()) || &range_key == evict_range);

        if let Some(left_range) = left_range {
            let left_meta = RangeMeta::derive_from(self.id_allocator.allocate_id(), &meta);
            assert!(self.ranges.insert(left_range, left_meta).is_none());
        }

        if let Some(right_range) = right_range {
            let right_meta = RangeMeta::derive_from(self.id_allocator.allocate_id(), &meta);
            assert!(self.ranges.insert(right_range, right_meta).is_none());
        }

        self.ranges_being_deleted.insert(evict_range.clone());

        if !meta.range_snapshot_list.is_empty() {
            assert!(self.historical_ranges.insert(range_key, meta).is_none());
            return None;
        }

        // we also need to check with previous historical_ranges
        if !self
            .historical_ranges
            .keys()
            .any(|r| r.overlaps(evict_range))
        {
            Some(evict_range.clone())
        } else {
            None
        }
    }

    pub fn has_ranges_in_gc(&self) -> bool {
        !self.ranges_in_gc.is_empty()
    }

    pub fn on_delete_ranges(&mut self, ranges: &[CacheRange]) {
        for r in ranges {
            self.ranges_being_deleted.remove(&r);
            info!(
                "range eviction done";
                "range" => ?r,
            );
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
        if self.overlap_with_pending_range(&cache_range) {
            return Err(LoadFailedReason::PendingRange);
        }
        if self.overlap_with_range_in_gc(&cache_range) {
            return Err(LoadFailedReason::InGc);
        }
        if self.overlap_with_evicting_range(&cache_range) {
            return Err(LoadFailedReason::Evicting);
        }
        self.pending_ranges.push(cache_range);
        Ok(())
    }

    pub fn get_and_reset_range_evictions(&self) -> u64 {
        self.range_evictions.swap(0, Ordering::Relaxed)
    }

    pub fn get_range_by_id(&self, region_id: u64) -> Option<CacheRange> {
        let tag = CacheRange::new_tag(region_id);

        self.ranges
            .iter()
            .find(|(range, _)| tag == range.tag)
            .map(|(r, _)| r.clone())
    }

    pub fn dump_cache(&self, region_id: u64) -> String {
        let tag = CacheRange::new_tag(region_id);

        // historical_ranges
        let mut buffer = "historical_ranges:\n".to_owned();
        for (range, meta) in self
            .historical_ranges
            .iter()
            .filter(|(range, _)| tag == range.tag)
        {
            buffer.push_str(&format!("  range: {:?}, meta: {:?}\n", range, meta));
        }

        // ranges_being_deleted
        buffer.push_str("ranges_being_deleted:\n");
        for range in self
            .ranges_being_deleted
            .iter()
            .filter(|range| tag == range.tag)
        {
            buffer.push_str(&format!("  range: {:?}\n", range));
        }

        // ranges
        buffer.push_str("ranges:\n");
        for (range, meta) in self.ranges.iter().filter(|(range, _)| tag == range.tag) {
            buffer.push_str(&format!("  range: {:?}, meta: {:?}\n", range, meta));
        }

        // pending_ranges
        buffer.push_str("pending_ranges:\n");
        for range in self.pending_ranges.iter().filter(|range| tag == range.tag) {
            buffer.push_str(&format!("  range: {:?}\n", range));
        }

        // pending_ranges_loading_data
        buffer.push_str("pending_ranges_loading_data:\n");
        for range in self
            .pending_ranges_loading_data
            .iter()
            .filter(|range| tag == range.0.tag)
        {
            buffer.push_str(&format!("  range: {:?}\n", range));
        }

        // ranges_in_gc
        buffer.push_str("ranges_in_gc:\n");
        for range in self.ranges_in_gc.iter().filter(|range| tag == range.tag) {
            buffer.push_str(&format!("  range: {:?}\n", range));
        }

        buffer
    }
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
            LoadFailedReason::Evicting
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

    #[test]
    fn test_range_load_overlapped() {
        let mut range_mgr = RangeManager::default();
        let r1 = CacheRange::new(b"k00".to_vec(), b"k10".to_vec());
        let r2 = CacheRange::new(b"k20".to_vec(), b"k30".to_vec());
        let r3 = CacheRange::new(b"k40".to_vec(), b"k50".to_vec());
        range_mgr.new_range(r1.clone());
        range_mgr.evict_range(&r1);

        let mut gced = BTreeSet::default();
        gced.insert(r2);
        range_mgr.set_ranges_in_gc(gced);

        range_mgr.load_range(r3).unwrap();

        let r = CacheRange::new(b"".to_vec(), b"k05".to_vec());
        assert_eq!(
            range_mgr.load_range(r).unwrap_err(),
            LoadFailedReason::Evicting
        );
        let r = CacheRange::new(b"k05".to_vec(), b"k15".to_vec());
        assert_eq!(
            range_mgr.load_range(r).unwrap_err(),
            LoadFailedReason::Evicting
        );

        let r = CacheRange::new(b"k15".to_vec(), b"k25".to_vec());
        assert_eq!(range_mgr.load_range(r).unwrap_err(), LoadFailedReason::InGc);
        let r = CacheRange::new(b"k25".to_vec(), b"k35".to_vec());
        assert_eq!(range_mgr.load_range(r).unwrap_err(), LoadFailedReason::InGc);

        let r = CacheRange::new(b"k35".to_vec(), b"k45".to_vec());
        assert_eq!(
            range_mgr.load_range(r).unwrap_err(),
            LoadFailedReason::PendingRange
        );
        let r = CacheRange::new(b"k45".to_vec(), b"k55".to_vec());
        assert_eq!(
            range_mgr.load_range(r).unwrap_err(),
            LoadFailedReason::PendingRange
        );
    }
}
