// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Debug},
    result,
};

use keys::{enc_end_key, enc_start_key};
use kvproto::metapb::Region;

use crate::{Iterable, KvEngine, Snapshot, WriteBatchExt};

#[derive(Debug, PartialEq)]
pub enum FailedReason {
    NotCached,
    TooOldRead,
    EpochNotMatch,
}

#[derive(Debug, PartialEq)]
pub enum RegionEvent {
    Split {
        source: Region,
        new_regions: Vec<Region>,
    },
    Eviction {
        region: Region,
        reason: EvictReason,
    },
    // range eviction triggered by delete_range
    // we should evict all cache regions that overlaps with this range
    EvictByRange {
        range: CacheRange,
        reason: EvictReason,
    },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EvictReason {
    LoadFailed,
    LoadFailedWithoutStart,
    MemoryLimitReached,
    BecomeFollower,
    AutoEvict,
    DeleteRange,
    Merge,
    InMemoryEngineDisabled,
}

/// RangeCacheEngine works as a range cache caching some ranges (in Memory or
/// NVME for instance) to improve the read performance.
pub trait RangeCacheEngine:
    WriteBatchExt + Iterable + Debug + Clone + Unpin + Send + Sync + 'static
{
    type Snapshot: Snapshot;

    // If None is returned, the RangeCacheEngine is currently not readable for this
    // region or read_ts.
    // Sequence number is shared between RangeCacheEngine and disk KvEnigne to
    // provide atomic write
    fn snapshot(
        &self,
        reigon_id: u64,
        region_epoch: u64,
        range: CacheRange,
        read_ts: u64,
        seq_num: u64,
    ) -> result::Result<Self::Snapshot, FailedReason>;

    type DiskEngine: KvEngine;
    fn set_disk_engine(&mut self, disk_engine: Self::DiskEngine);

    // return the region containing the key
    fn get_region_for_key(&self, key: &[u8]) -> Option<Region>;

    type RangeHintService: RangeHintService;
    fn start_hint_service(&self, range_hint_service: Self::RangeHintService);

    fn enabled(&self) -> bool {
        false
    }

    fn on_region_event(&self, event: RegionEvent);
}

pub trait RangeCacheEngineExt {
    fn range_cache_engine_enabled(&self) -> bool;

    // TODO(SpadeA): try to find a better way to reduce coupling degree of range
    // cache engine and kv engine
    fn on_region_event(&self, event: RegionEvent);
}

/// A service that should run in the background to retrieve and apply cache
/// hints.
///
/// TODO (afeinberg): Presently, this is only a marker trait with a single
/// implementation. Methods and/or associated types will be added to this trait
/// as it continues to evolve to handle eviction, using stats.
pub trait RangeHintService: Send + Sync {}

#[derive(Clone, Eq, PartialEq, PartialOrd, Ord)]
pub struct CacheRange {
    pub start: Vec<u8>,
    pub end: Vec<u8>,
}

impl Debug for CacheRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheRange")
            .field("range_start", &log_wrappers::Value(&self.start))
            .field("range_end", &log_wrappers::Value(&self.end))
            .finish()
    }
}

impl CacheRange {
    pub fn new(start: Vec<u8>, end: Vec<u8>) -> Self {
        Self { start, end }
    }

    pub fn from_region(region: &Region) -> Self {
        Self {
            start: enc_start_key(region),
            end: enc_end_key(region),
        }
    }
}

impl CacheRange {
    // todo: need to consider ""?
    pub fn contains_range(&self, other: &CacheRange) -> bool {
        self.start <= other.start && self.end >= other.end
    }

    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.start.as_slice() <= key && key < self.end.as_slice()
    }

    // Note: overlaps also includes "contains"
    pub fn overlaps(&self, other: &CacheRange) -> bool {
        self.start < other.end && other.start < self.end
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use super::CacheRange;
    #[test]
    fn test_cache_range_partial_cmp() {
        let r1 = CacheRange::new(b"k1".to_vec(), b"k2".to_vec());
        let r2 = CacheRange::new(b"k2".to_vec(), b"k3".to_vec());
        let r3 = CacheRange::new(b"k2".to_vec(), b"k4".to_vec());
        assert_eq!(r1.partial_cmp(&r2).unwrap(), Ordering::Less);
        assert_eq!(r2.partial_cmp(&r1).unwrap(), Ordering::Greater);
        assert!(r2.partial_cmp(&r3).is_none());
    }

    #[test]
    fn test_overlap() {
        let r1 = CacheRange::new(b"k1".to_vec(), b"k6".to_vec());
        let r2 = CacheRange::new(b"k2".to_vec(), b"k4".to_vec());
        assert!(r1.overlaps(&r2));
        assert!(r2.overlaps(&r1));

        let r1 = CacheRange::new(b"k1".to_vec(), b"k6".to_vec());
        let r2 = CacheRange::new(b"k2".to_vec(), b"k7".to_vec());
        assert!(r1.overlaps(&r2));
        assert!(r2.overlaps(&r1));

        let r1 = CacheRange::new(b"k1".to_vec(), b"k6".to_vec());
        let r2 = CacheRange::new(b"k1".to_vec(), b"k4".to_vec());
        assert!(r1.overlaps(&r2));
        assert!(r2.overlaps(&r1));

        let r1 = CacheRange::new(b"k1".to_vec(), b"k6".to_vec());
        let r2 = CacheRange::new(b"k2".to_vec(), b"k6".to_vec());
        assert!(r1.overlaps(&r2));
        assert!(r2.overlaps(&r1));

        let r1 = CacheRange::new(b"k1".to_vec(), b"k6".to_vec());
        let r2 = CacheRange::new(b"k1".to_vec(), b"k6".to_vec());
        assert!(r1.overlaps(&r2));
        assert!(r2.overlaps(&r1));

        let r1 = CacheRange::new(b"k1".to_vec(), b"k2".to_vec());
        let r2 = CacheRange::new(b"k2".to_vec(), b"k3".to_vec());
        assert!(!r1.overlaps(&r2));
        assert!(!r2.overlaps(&r1));

        let r1 = CacheRange::new(b"k1".to_vec(), b"k2".to_vec());
        let r2 = CacheRange::new(b"k3".to_vec(), b"k4".to_vec());
        assert!(!r1.overlaps(&r2));
        assert!(!r2.overlaps(&r1));
    }
}
