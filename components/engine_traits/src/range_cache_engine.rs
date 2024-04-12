// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cmp, fmt::Debug, result};

use keys::{enc_end_key, enc_start_key};
use kvproto::metapb;

use crate::{Iterable, KvEngine, Snapshot, WriteBatchExt};

#[derive(Debug, PartialEq)]
pub enum FailedReason {
    NotCached,
    TooOldRead,
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
        range: CacheRange,
        read_ts: u64,
        seq_num: u64,
    ) -> result::Result<Self::Snapshot, FailedReason>;

    type DiskEngine: KvEngine;
    fn set_disk_engine(&mut self, disk_engine: Self::DiskEngine);

    // return the range containing the key
    fn get_range_for_key(&self, key: &[u8]) -> Option<CacheRange>;

    type RangeHintService: RangeHintService;
    fn start_hint_service(&self, range_hint_service: Self::RangeHintService);
}

/// A service that should run in the background to retrieve and apply cache
/// hints.
///
/// TODO (afeinberg): Presently, this is only a marker trait with a single
/// implementation. Methods and/or associated types will be added to this trait
/// as it continues to evolve to handle eviction, using stats.
pub trait RangeHintService: Send + Sync {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheRange {
    pub start: Vec<u8>,
    pub end: Vec<u8>,
}

impl CacheRange {
    pub fn new(start: Vec<u8>, end: Vec<u8>) -> Self {
        Self { start, end }
    }

    pub fn from_region(region: &metapb::Region) -> Self {
        Self {
            start: enc_start_key(region),
            end: enc_end_key(region),
        }
    }
}

impl PartialOrd for CacheRange {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        if self.end <= other.start {
            return Some(cmp::Ordering::Less);
        }

        if other.end <= self.start {
            return Some(cmp::Ordering::Greater);
        }

        if self == other {
            return Some(cmp::Ordering::Equal);
        }

        None
    }
}

impl Ord for CacheRange {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        let c = self.start.cmp(&other.start);
        if !c.is_eq() {
            return c;
        }
        self.end.cmp(&other.end)
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

    pub fn overlaps(&self, other: &CacheRange) -> bool {
        self.start < other.end && other.start < self.end
    }

    pub fn split_off(&self, range: &CacheRange) -> (Option<CacheRange>, Option<CacheRange>) {
        assert!(self.contains_range(range));
        let left = if self.start != range.start {
            Some(CacheRange {
                start: self.start.clone(),
                end: range.start.clone(),
            })
        } else {
            None
        };
        let right = if self.end != range.end {
            Some(CacheRange {
                start: range.end.clone(),
                end: self.end.clone(),
            })
        } else {
            None
        };

        (left, right)
    }
}
