// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::{BTreeMap, BTreeSet};

use engine_traits::CacheRange;

use crate::engine::SnapshotList;

//  RangeManager: range [k1-k10]
//  range_snapshot_list: [k1-k10]
//
//  batch split k1-k5 k5-k10
// 
//  RangeManager: range [k1-k10]
//  range_snapshot_list: [k1-k10, k1-k5]
//  
// 
//  RangeManager: range [ k1-k5,        k5-k10 -- evict ]
//  k1-k5 range_snapshot_list: [k1-k10, k1-k5]
// 
// 
// range_snapshot_list: [k1-k10, k1-k3, k3-k5]
// ranges_evicted: [k3-k5]
// 
//  k1-k3  
#[derive(Debug, Default)]
pub struct RangeMeta {
    range_snapshot_list: BTreeMap<CacheRange, SnapshotList>,
    ranges_evicted: BTreeSet<CacheRange>,
    ranges_unreadable: BTreeSet<CacheRange>,
    safe_ts: u64,
}

impl RangeMeta {
    pub(crate) fn range_snapshot_list(&self) -> &BTreeMap<CacheRange, SnapshotList> {
        &self.range_snapshot_list
    }

    pub(crate) fn merge_meta(&mut self, mut other: RangeMeta) {
        self.range_snapshot_list
            .append(&mut other.range_snapshot_list);
        self.ranges_evicted.append(&mut other.ranges_evicted);
        self.ranges_unreadable.append(&mut other.ranges_unreadable);

        // merge safe_ts to the min of them if not 0
        if self.safe_ts != 0 && other.safe_ts != 0 {
            self.safe_ts = u64::min(self.safe_ts, other.safe_ts);
        } else if other.safe_ts != 0 {
            self.safe_ts = other.safe_ts;
        }
    }

    pub(crate) fn set_range_readable(&mut self, range: &CacheRange, set_readable: bool) {
        if set_readable {
            assert!(self.ranges_unreadable.remove(range));
        } else {
            self.ranges_unreadable.insert(range.clone());
        }
    }
}

#[derive(Default)]
pub struct RangeManager {
    // the range reflects the range of data that is accessable.
    ranges: BTreeMap<CacheRange, RangeMeta>,
}

impl RangeManager {
    pub(crate) fn ranges(&self) -> &BTreeMap<CacheRange, RangeMeta> {
        &self.ranges
    }

    pub(crate) fn new_range(&mut self, range: CacheRange) {
        if let Some(sibling) = self.ranges.keys().find(|r| r.is_sibling(&range)).cloned() {
            let left_sib = sibling < range;
            let mut sib_meta = self.ranges.remove(&sibling).unwrap();
            sib_meta.ranges_unreadable.insert(range.clone());
            let mut range = CacheRange::merge(sibling, range);

            // if sibling found above is the left sibling of the range, there could be a
            // right sibling
            if left_sib {
                if let Some(right_sibling) =
                    self.ranges.keys().find(|r| r.is_sibling(&range)).cloned()
                {
                    let right_sib_meta = self.ranges.remove(&right_sibling).unwrap();
                    sib_meta.merge_meta(right_sib_meta);
                    range = CacheRange::merge(range, right_sibling);
                }
            }
            self.ranges.insert(range, sib_meta);
        } else {
            let mut range_meta = RangeMeta::default();
            range_meta.ranges_unreadable.insert(range.clone());
            self.ranges.insert(range, range_meta);
        }
    }

    pub fn set_range_readable(&mut self, range: &CacheRange, set_readable: bool) {
        let range_key = self
            .ranges
            .keys()
            .find(|&r| r.contains(range))
            .unwrap()
            .clone();
        let meta = self.ranges.get_mut(&range_key).unwrap();
        meta.set_range_readable(range, set_readable);
    }

    pub fn set_safe_ts(&mut self, range: &CacheRange, safe_ts: u64) -> bool {
        if let Some(meta) = self.ranges.get_mut(range) {
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

    pub(crate) fn range_snapshot(&mut self, range: &CacheRange, read_ts: u64) -> bool {
        let Some(range_key) = self.ranges.keys().find(|&r| r.contains(range)) else {
            return false;
        };
        let meta = self.ranges.get(range_key).unwrap();

        if read_ts <= meta.safe_ts {
            // todo(SpadeA): add metrics for it
            return false;
        }

        if meta.ranges_evicted.iter().any(|r| r.overlaps(range)) {
            return false;
        }

        if meta.ranges_unreadable.iter().any(|r| r.overlaps(range)) {
            return false;
        }

        let meta = self.ranges.get_mut(&range_key.clone()).unwrap();
        meta.range_snapshot_list
            .entry(range.clone())
            .or_default()
            .new_snapshot(read_ts);
        true
    }

    pub(crate) fn remove_range_snapshot(&mut self, range: &CacheRange, read_ts: u64) {
        let range_key = self
            .ranges
            .keys()
            .find(|&r| r.contains(range))
            .unwrap()
            .clone();
        let meta = self.ranges.get_mut(&range_key).unwrap();
        let snap_list = meta.range_snapshot_list.get_mut(range).unwrap();
        snap_list.remove_snapshot(read_ts);
        if snap_list.is_empty() {
            meta.range_snapshot_list.remove(range).unwrap();
        }
    }

    pub(crate) fn range_evictable(&self, range: &CacheRange) -> bool {
        unimplemented!()
    }

    // Evict a range which results in a split of the range containing it. Meta
    // should also be splitted.
    pub(crate) fn evict_range(&mut self, range: &CacheRange) {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use engine_traits::CacheRange;

    use super::RangeManager;

    #[test]
    fn test_range_manager() {
        let mut range_mgr = RangeManager::default();
        let r1 = CacheRange::new(b"k00".to_vec(), b"k03".to_vec());
        let r1_1 = CacheRange::new(b"k00".to_vec(), b"k01".to_vec());
        let r1_2 = CacheRange::new(b"k01".to_vec(), b"k02".to_vec());

        range_mgr.new_range(r1.clone());
        range_mgr.set_range_readable(&r1, true);
        range_mgr.set_safe_ts(&r1, 5);
        assert!(!range_mgr.range_snapshot(&r1, 5));
        assert!(range_mgr.range_snapshot(&r1, 8));
        let tmp_r = CacheRange::new(b"k00".to_vec(), b"k04".to_vec());
        assert!(!range_mgr.range_snapshot(&tmp_r, 8));
        assert!(range_mgr.range_snapshot(&r1, 10));
        range_mgr.set_range_readable(&r1_1, false);
        assert!(!range_mgr.range_snapshot(&r1_1, 10));
        range_mgr.set_range_readable(&r1_2, false);
        assert!(!range_mgr.range_snapshot(&r1_2, 10));

        let r2 = CacheRange::new(b"k05".to_vec(), b"k10".to_vec());
        let r2_1 = CacheRange::new(b"k05".to_vec(), b"k07".to_vec());
        let r2_2 = CacheRange::new(b"k07".to_vec(), b"k08".to_vec());
        range_mgr.new_range(r2.clone());
        range_mgr.set_range_readable(&r2, true);
        range_mgr.set_safe_ts(&r2, 3);
        assert!(range_mgr.range_snapshot(&r2, 10));
        range_mgr.set_range_readable(&r2_1, false);
        range_mgr.set_range_readable(&r2_2, false);

        let r3 = CacheRange::new(b"k03".to_vec(), b"k05".to_vec());
        // it makes all ranges merged
        range_mgr.new_range(r3.clone());
        range_mgr.set_range_readable(&r3, true);
        let r = CacheRange::new(b"k00".to_vec(), b"k10".to_vec());
        assert_eq!(range_mgr.ranges.len(), 1);
        let meta = range_mgr.ranges.get(&r).unwrap();
        assert_eq!(meta.safe_ts, 3);
        assert_eq!(meta.ranges_unreadable.len(), 4);
        assert_eq!(meta.range_snapshot_list.len(), 2);
        assert_eq!(meta.range_snapshot_list.get(&r1).unwrap().len(), 2);
        assert_eq!(meta.range_snapshot_list.get(&r2).unwrap().len(), 1);
    }
}
