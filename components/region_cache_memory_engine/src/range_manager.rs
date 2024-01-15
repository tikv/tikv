// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::{BTreeMap, BTreeSet};

use engine_traits::CacheRange;

use crate::engine::SnapshotList;

#[derive(Debug, Default)]
pub struct RangeMeta {
    range_snapshot_list: BTreeMap<CacheRange, SnapshotList>,
    ranges_evcited: BTreeSet<CacheRange>,
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
        self.ranges_evcited.append(&mut other.ranges_evcited);
        self.ranges_unreadable.append(&mut other.ranges_unreadable);
    }

    pub(crate) fn set_range_readable(&mut self, range: &CacheRange, set_readable: bool) {
        if set_readable {
            assert!(self.ranges_unreadable.remove(&range));
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
            let range = CacheRange::merge(sibling, range);

            // if sibling found above is the left sibling of the range, there could be a
            // right sibling
            if left_sib {
                if let Some(right_sibling) =
                    self.ranges.keys().find(|r| r.is_sibling(&range)).cloned()
                {
                    let right_sib_meta = self.ranges.remove(&right_sibling).unwrap();
                    sib_meta.merge_meta(right_sib_meta);
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
            meta.safe_ts = safe_ts;
            true
        } else {
            false
        }
    }

    pub(crate) fn overlap_with_range(&self, range: &CacheRange) -> bool {
        false
    }

    pub(crate) fn range_readable(&self, range: &CacheRange, read_ts: u64) -> bool {
        let Some(range_key) = self.ranges.keys().find(|&r| r.contains(range)) else {
            return false;
        };
        let meta = self.ranges.get(range_key).unwrap();

        if read_ts <= meta.safe_ts {
            // todo(SpadeA): add metrics for it
            return false;
        }

        if meta.ranges_evcited.iter().any(|r| r.overlaps(range)) {
            return false;
        }

        if meta.ranges_unreadable.iter().any(|r| r.overlaps(range)) {
            return false;
        }

        true
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

        if meta.ranges_evcited.iter().any(|r| r.overlaps(range)) {
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

    // A range is evictable if:
    // 1. it is marked as evicted (so it's in the ranges_evicted)
    // 2. there's no snapshot whose range overlap with it
    pub(crate) fn range_evictable(&self, range: &CacheRange) -> bool {
        let range_key = self.ranges.keys().find(|&r| r.contains(range)).unwrap();
        let meta = self.ranges.get(range_key).unwrap();
        if meta.ranges_evcited.get(range).is_none() {
            return false;
        }

        return !meta.range_snapshot_list.keys().any(|r| r.overlaps(range));
    }

    // Evict a range which results in a split of the range containing it. Meta
    // should also be splitted.
    pub(crate) fn evict_range(&mut self, range: &CacheRange) {
        let range_key = self
            .ranges
            .keys()
            .find(|&r| r.contains(range))
            .unwrap()
            .clone();
        let mut meta = self.ranges.remove(&range_key).unwrap();

        let range_snapshot_list2 = meta.range_snapshot_list.split_off(&range_key);
        // range overlap assertion check
        range_snapshot_list2
            .keys()
            .into_iter()
            .next()
            .map(|r| assert!(!r.overlaps(range)));
        meta.range_snapshot_list
            .keys()
            .last()
            .map(|r| assert!(!r.overlaps(range)));
        let evicted2 = meta.ranges_evcited.split_off(&range_key);
        // range overlap assertion check
        evicted2.iter().next().map(|r| assert!(!r.overlaps(&range)));
        meta.ranges_evcited
            .iter()
            .last()
            .map(|r| assert!(!r.overlaps(range)));
        let unreadable2 = meta.ranges_unreadable.split_off(&range_key);
        // range overlap assertion check
        unreadable2
            .iter()
            .next()
            .map(|r| assert!(!r.overlaps(&range)));
        meta.ranges_unreadable
            .iter()
            .last()
            .map(|r| assert!(!r.overlaps(range)));

        let (r1, r2) = range_key.split_off(range);
        let safe_ts = meta.safe_ts;
        self.ranges.insert(r1, meta);
        self.ranges.insert(
            r2,
            RangeMeta {
                range_snapshot_list: range_snapshot_list2,
                ranges_evcited: evicted2,
                ranges_unreadable: unreadable2,
                safe_ts,
            },
        );
    }
}

#[cfg(test)]
mod tests {}
