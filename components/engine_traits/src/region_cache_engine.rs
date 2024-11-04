// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Debug},
    result,
};

use keys::{enc_end_key, enc_start_key};
use kvproto::metapb::Region;

use crate::{KvEngine, Snapshot, WriteBatchExt};

#[derive(Debug, PartialEq)]
pub enum FailedReason {
    NotCached,
    TooOldRead,
    // while we always get rocksdb's snapshot first and then get IME's snapshot.
    // epoch is first checked in get rocksdb's snaphsot by raftstore.
    // But because we update IME's epoch in apply batch, and update raft local reader's
    // epoch after ApplyRes is returned, so it's possible that IME's region epoch is
    // newer than raftstore's, so we still need to check epoch again in IME snapshot.
    EpochNotMatch,
}

#[derive(Debug, PartialEq)]
pub enum RegionEvent {
    Split {
        source: CacheRegion,
        new_regions: Vec<CacheRegion>,
    },
    TryLoad {
        region: CacheRegion,
        for_manual_range: bool,
    },
    Eviction {
        region: CacheRegion,
        reason: EvictReason,
    },
    // range eviction triggered by delete_range
    // we should evict all cache regions that overlaps with this range
    EvictByRange {
        range: CacheRegion,
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
    Disabled,
    ApplySnapshot,
    Flashback,
    Manual,
    PeerDestroy,
}

/// RegionCacheEngine works as a region cache caching some regions (in Memory or
/// NVME for instance) to improve the read performance.
pub trait RegionCacheEngine:
    RegionCacheEngineExt + WriteBatchExt + Debug + Clone + Unpin + Send + Sync + 'static
{
    type Snapshot: Snapshot;

    // If None is returned, the RegionCacheEngine is currently not readable for this
    // region or read_ts.
    // Sequence number is shared between RegionCacheEngine and disk KvEnigne to
    // provide atomic write
    fn snapshot(
        &self,
        region: CacheRegion,
        read_ts: u64,
        seq_num: u64,
    ) -> result::Result<Self::Snapshot, FailedReason>;

    type DiskEngine: KvEngine;
    fn set_disk_engine(&mut self, disk_engine: Self::DiskEngine);

    type RangeHintService: RangeHintService;
    fn start_hint_service(&self, range_hint_service: Self::RangeHintService);

    fn enabled(&self) -> bool {
        false
    }
}

pub trait RegionCacheEngineExt {
    // TODO(SpadeA): try to find a better way to reduce coupling degree of
    // region cache engine and kv engine
    fn on_region_event(&self, event: RegionEvent);

    fn region_cached(&self, region: &Region) -> bool;

    fn load_region(&self, region: &Region);
}

/// A service that should run in the background to retrieve and apply cache
/// hints.
///
/// TODO (afeinberg): Presently, this is only a marker trait with a single
/// implementation. Methods and/or associated types will be added to this trait
/// as it continues to evolve to handle eviction, using stats.
pub trait RangeHintService: Send + Sync {}

#[derive(Clone, Eq, PartialEq, Hash)]
pub struct CacheRegion {
    // target region id
    pub id: u64,
    // the version of target region epoch. we only track version but not
    // conf_version because conf_version does not change the applied data.
    pub epoch_version: u64,
    // data start key of the region range,  equals to data_key(region.start_key).
    pub start: Vec<u8>,
    // data end key of the region range, equals to data_end_key(region.end_key).
    pub end: Vec<u8>,
}

impl Debug for CacheRegion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheRegion")
            .field("id", &self.id)
            .field("epoch", &self.epoch_version)
            .field("range_start", &log_wrappers::Value(&self.start))
            .field("range_end", &log_wrappers::Value(&self.end))
            .finish()
    }
}

impl CacheRegion {
    pub fn new<T1: Into<Vec<u8>>, T2: Into<Vec<u8>>>(
        id: u64,
        epoch_version: u64,
        start: T1,
        end: T2,
    ) -> Self {
        Self {
            id,
            epoch_version,
            start: start.into(),
            end: end.into(),
        }
    }

    pub fn from_region(region: &Region) -> Self {
        Self {
            start: enc_start_key(region),
            end: enc_end_key(region),
            id: region.id,
            epoch_version: region.get_region_epoch().version,
        }
    }
}

impl CacheRegion {
    pub fn contains_range(&self, other: &CacheRegion) -> bool {
        self.start <= other.start && self.end >= other.end
    }

    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.start.as_slice() <= key && key < self.end.as_slice()
    }

    // Note: overlaps also includes "contains"
    pub fn overlaps(&self, other: &CacheRegion) -> bool {
        self.start < other.end && other.start < self.end
    }

    pub fn union(&self, other: &CacheRegion) -> Option<CacheRegion> {
        if self.overlaps(other) {
            Some(CacheRegion {
                id: 0,
                epoch_version: 0,
                start: std::cmp::min(&self.start, &other.start).clone(),
                end: std::cmp::max(&self.end, &other.end).clone(),
            })
        } else {
            None
        }
    }

    pub fn difference(&self, other: &CacheRegion) -> (Option<CacheRegion>, Option<CacheRegion>) {
        if !self.overlaps(other) {
            return (None, None);
        }
        let left = if self.start < other.start {
            Some(CacheRegion {
                id: 0,
                epoch_version: 0,
                start: self.start.clone(),
                end: other.start.clone(),
            })
        } else {
            None
        };
        let right = if self.end > other.end {
            Some(CacheRegion {
                id: 0,
                epoch_version: 0,
                start: other.end.clone(),
                end: self.end.clone(),
            })
        } else {
            None
        };
        (left, right)
    }
}

#[cfg(test)]
mod tests {
    use super::CacheRegion;

    #[test]
    fn test_overlap() {
        let r1 = CacheRegion::new(1, 0, b"k1".to_vec(), b"k6".to_vec());
        let r2 = CacheRegion::new(2, 0, b"k2".to_vec(), b"k4".to_vec());
        assert!(r1.overlaps(&r2));
        assert!(r2.overlaps(&r1));

        let r1 = CacheRegion::new(1, 0, b"k1".to_vec(), b"k6".to_vec());
        let r2 = CacheRegion::new(2, 0, b"k2".to_vec(), b"k7".to_vec());
        assert!(r1.overlaps(&r2));
        assert!(r2.overlaps(&r1));

        let r1 = CacheRegion::new(1, 0, b"k1".to_vec(), b"k6".to_vec());
        let r2 = CacheRegion::new(2, 0, b"k1".to_vec(), b"k4".to_vec());
        assert!(r1.overlaps(&r2));
        assert!(r2.overlaps(&r1));

        let r1 = CacheRegion::new(1, 0, b"k1".to_vec(), b"k6".to_vec());
        let r2 = CacheRegion::new(2, 0, b"k2".to_vec(), b"k6".to_vec());
        assert!(r1.overlaps(&r2));
        assert!(r2.overlaps(&r1));

        let r1 = CacheRegion::new(1, 0, b"k1".to_vec(), b"k6".to_vec());
        let r2 = CacheRegion::new(2, 0, b"k1".to_vec(), b"k6".to_vec());
        assert!(r1.overlaps(&r2));
        assert!(r2.overlaps(&r1));

        let r1 = CacheRegion::new(1, 0, b"k1".to_vec(), b"k2".to_vec());
        let r2 = CacheRegion::new(2, 0, b"k2".to_vec(), b"k3".to_vec());
        assert!(!r1.overlaps(&r2));
        assert!(!r2.overlaps(&r1));

        let r1 = CacheRegion::new(1, 0, b"k1".to_vec(), b"k2".to_vec());
        let r2 = CacheRegion::new(2, 0, b"k3".to_vec(), b"k4".to_vec());
        assert!(!r1.overlaps(&r2));
        assert!(!r2.overlaps(&r1));
    }
}
