// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::{BTreeMap, BTreeSet};

use engine_traits::CacheRange;

use crate::engine::{RagneCacheSnapshotMeta, SnapshotList};

#[derive(Debug, Default)]
pub struct RangeMeta {
    id: u64,
    range_snapshot_list: SnapshotList,
    evicted: bool,
    can_read: bool,
    safe_ts: u64,
}

impl RangeMeta {
    fn new(id: u64) -> Self {
        Self {
            id,
            range_snapshot_list: SnapshotList::default(),
            evicted: false,
            can_read: false,
            safe_ts: 0,
        }
    }

    fn derive_from(id: u64, r: &RangeMeta) -> Self {
        Self {
            id,
            range_snapshot_list: SnapshotList::default(),
            evicted: false,
            can_read: r.can_read,
            safe_ts: r.safe_ts,
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

/// RangeManger manges the ranges for RangeCacheMemoryEngine.
#[derive(Default)]
pub struct RangeManager {
    // Each new range will increment it by one.
    id_allocator: IdAllocator,
    historical_ranges: BTreeMap<CacheRange, RangeMeta>,
    evicted_ranges: BTreeSet<CacheRange>,
    ranges: BTreeMap<CacheRange, RangeMeta>,
}

impl RangeManager {
    pub(crate) fn ranges(&self) -> &BTreeMap<CacheRange, RangeMeta> {
        &self.ranges
    }

    pub(crate) fn new_range(&mut self, range: CacheRange) {
        let range_meta = RangeMeta::new(self.id_allocator.allocate_id());
        self.ranges.insert(range, range_meta);
    }

    pub fn set_range_readable(&mut self, range: &CacheRange, set_readable: bool) {
        if let Some(meta) = self.ranges.get_mut(&range) {
            meta.can_read = true;
        }
    }

    pub fn set_safe_ts(&mut self, range: &CacheRange, safe_ts: u64) -> bool {
        if let Some(meta) = self.ranges.get_mut(&range) {
            if meta.safe_ts > safe_ts {
                return false;
            }
            meta.safe_ts = safe_ts;
            true
        } else {
            false
        }
    }

    pub(crate) fn overlap_with_range(&self, range: &CacheRange) -> bool {
        self.ranges.keys().any(|r| r.overlaps(range))
    }

    // Acquire a snapshot of the `range` with `reaed_ts`. If the range is not
    // accessable, None will be returned. Otherwise, the range id will be returned.
    pub(crate) fn range_snapshot(&mut self, range: &CacheRange, read_ts: u64) -> Option<u64> {
        let Some(range_key) = self.ranges.keys().find(|&r| r.contains(range)).cloned() else {
            return None;
        };
        let meta = self.ranges.get_mut(&range_key).unwrap();

        if read_ts <= meta.safe_ts || meta.evicted || !meta.can_read {
            // todo(SpadeA): add metrics for it
            return None;
        }

        meta.range_snapshot_list.new_snapshot(read_ts);
        Some(meta.id)
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
                range.contains(&snapshot_meta.range) && meta.id == snapshot_meta.range_id
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
                .evicted_ranges
                .iter()
                .filter(|evicted_range| {
                    !self
                        .historical_ranges
                        .keys()
                        .any(|r| r.overlaps(&evicted_range))
                })
                .map(|r| r.clone())
                .collect();
        }

        // It must belong to the `self.ranges` if not found in `self.historical_ranges`
        let range_key = self
            .ranges
            .iter()
            .find(|&(range, meta)| {
                range.contains(&snapshot_meta.range) && meta.id == snapshot_meta.range_id
            })
            .map(|(r, _)| r.clone())
            .unwrap();
        let meta = self.ranges.get_mut(&range_key).unwrap();
        meta.range_snapshot_list
            .remove_snapshot(snapshot_meta.snapshot_ts);
        vec![]
    }

    pub(crate) fn range_evictable(&self, range: &CacheRange) -> bool {
        unimplemented!()
    }

    pub(crate) fn evict_range(&mut self, range: &CacheRange) {
        let range_key = self
            .ranges
            .keys()
            .find(|&r| r.contains(range))
            .unwrap()
            .clone();
        let meta = self.ranges.remove(&range_key).unwrap();
        let (left_range, right_range) = range_key.split_off(&range);
        assert!((left_range.is_some() || right_range.is_some()) || &range_key == range);

        if let Some(left_range) = left_range {
            let left_meta = RangeMeta::derive_from(self.id_allocator.allocate_id(), &meta);
            self.ranges.insert(left_range, left_meta);
        }

        if let Some(right_range) = right_range {
            let right_meta = RangeMeta::derive_from(self.id_allocator.allocate_id(), &meta);
            self.ranges.insert(right_range, right_meta);
        }

        self.evicted_ranges.insert(range.clone());
        self.historical_ranges.insert(range_key, meta);
    }
}

#[cfg(test)]
mod tests {
    use engine_traits::CacheRange;

    use super::RangeManager;

    #[test]
    fn test_range_manager() {
        let mut range_mgr = RangeManager::default();
        let r1 = CacheRange::new(b"k00".to_vec(), b"k10".to_vec());

        range_mgr.new_range(r1.clone());
        range_mgr.set_range_readable(&r1, true);
        range_mgr.set_safe_ts(&r1, 5);
        assert!(range_mgr.range_snapshot(&r1, 5).is_none());
        assert!(range_mgr.range_snapshot(&r1, 8).is_some());
        assert!(range_mgr.range_snapshot(&r1, 10).is_some());
        let tmp_r = CacheRange::new(b"k08".to_vec(), b"k15".to_vec());
        assert!(range_mgr.range_snapshot(&tmp_r, 8).is_none());
        let tmp_r = CacheRange::new(b"k10".to_vec(), b"k11".to_vec());
        assert!(range_mgr.range_snapshot(&tmp_r, 8).is_none());

        let r_evict = CacheRange::new(b"k03".to_vec(), b"k06".to_vec());
        let r_left = CacheRange::new(b"k00".to_vec(), b"k03".to_vec());
        let r_right = CacheRange::new(b"k06".to_vec(), b"k10".to_vec());
        range_mgr.evict_range(&r_evict);
        let meta1 = range_mgr.historical_ranges.get(&r1).unwrap();
        assert!(range_mgr.evicted_ranges.contains(&r_evict));
        assert!(range_mgr.ranges.get(&r1).is_none());
        let meta2 = range_mgr.ranges.get(&r_left).unwrap();
        let meta3 = range_mgr.ranges.get(&r_right).unwrap();
        assert!(meta1.safe_ts == meta2.safe_ts && meta1.safe_ts == meta3.safe_ts);
        assert!(meta2.can_read && meta3.can_read);

        // evict a range with accurate match
        range_mgr.evict_range(&r_left);
        assert!(range_mgr.historical_ranges.get(&r_left).is_some());
        assert!(range_mgr.evicted_ranges.contains(&r_left));
        assert!(range_mgr.ranges.get(&r_left).is_none());
    }
}
