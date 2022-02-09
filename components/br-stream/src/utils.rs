// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    borrow::Borrow,
    collections::{hash_map::RandomState, BTreeMap, HashMap},
    ops::{Bound, RangeBounds},
    time::Duration,
};

use crate::{
    annotate,
    errors::{Error, Result},
};

use engine_traits::CF_DEFAULT;
use futures::{channel::mpsc, executor::block_on, StreamExt};
use kvproto::raft_cmdpb::{CmdType, Request};
use raft::StateRole;
use raftstore::{coprocessor::RegionInfoProvider, RegionInfo};

use tikv_util::{box_err, time::Instant, warn, Either};
use tokio::sync::{Mutex, RwLock};
use txn_types::{Key, TimeStamp};

/// wrap a user key with encoded data key.
pub fn wrap_key(v: Vec<u8>) -> Vec<u8> {
    // TODO: encode in place.
    let key = Key::from_raw(v.as_slice()).into_encoded();
    key
}

/// decode ts from a key, and transform the error to the crate error.
pub fn get_ts(key: &Key) -> Result<TimeStamp> {
    key.decode_ts().map_err(|err| {
        annotate!(
            err,
            "failed to get ts from key '{}'",
            redact(&key.as_encoded())
        )
    })
}

pub fn redact(key: &impl AsRef<[u8]>) -> log_wrappers::Value<'_> {
    log_wrappers::Value::key(key.as_ref())
}

/// RegionPager seeks regions with leader role in the range.
pub struct RegionPager<P> {
    regions: P,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    reach_last_region: bool,
}

impl<P: RegionInfoProvider> RegionPager<P> {
    pub fn scan_from(regions: P, start_key: Vec<u8>, end_key: Vec<u8>) -> Self {
        Self {
            regions,
            start_key,
            end_key,
            reach_last_region: false,
        }
    }

    pub fn next_page(&mut self, size: usize) -> Result<Vec<RegionInfo>> {
        if self.start_key >= self.end_key || self.reach_last_region {
            return Ok(vec![]);
        }

        let (mut tx, rx) = mpsc::channel(size);
        let end_key = self.end_key.clone();
        self.regions
            .seek_region(
                &self.start_key,
                Box::new(move |i| {
                    let r = i
                        .filter(|r| r.role == StateRole::Leader)
                        .take(size)
                        .take_while(|r| r.region.start_key < end_key)
                        .try_for_each(|r| tx.try_send(r.clone()));
                    if let Err(_err) = r {
                        warn!("failed to scan region and send to initlizer")
                    }
                }),
            )
            .map_err(|err| {
                Error::Other(box_err!(
                    "failed to seek region for start key {}: {}",
                    redact(&self.start_key),
                    err
                ))
            })?;
        let collected_regions = block_on(rx.collect::<Vec<_>>());
        self.start_key = collected_regions
            .last()
            .map(|region| region.region.end_key.to_owned())
            // no leader region found.
            .unwrap_or_default();
        if self.start_key.is_empty() {
            self.reach_last_region = true;
        }
        Ok(collected_regions)
    }
}

/// StopWatch is a utility for record time cost in multi-stage tasks.
/// NOTE: Maybe it should be generic over somewhat Clock type?
pub struct StopWatch(Instant);

impl StopWatch {
    /// Create a new stopwatch via current time.
    pub fn new() -> Self {
        Self(Instant::now_coarse())
    }

    /// Get time elapsed since last lap (or creation if the first time).
    pub fn lap(&mut self) -> Duration {
        let elapsed = self.0.saturating_elapsed();
        self.0 = Instant::now_coarse();
        elapsed
    }
}

/// Slot is a shareable slot in the slot map.
pub type Slot<T> = Mutex<T>;

/// SlotMap is a trivial concurrent map which sharding over each key.
/// NOTE: Maybe we can use dashmap for replacing the RwLock.
pub type SlotMap<K, V, S = RandomState> = RwLock<HashMap<K, Slot<V>, S>>;

/// Like `..=val`(a.k.a. `RangeToInclusive`), but allows `val` being a reference to DSTs.
struct RangeToInclusiveRef<'a, T: ?Sized>(&'a T);

impl<'a, T: ?Sized> RangeBounds<T> for RangeToInclusiveRef<'a, T> {
    fn start_bound(&self) -> Bound<&T> {
        Bound::Unbounded
    }

    fn end_bound(&self) -> Bound<&T> {
        Bound::Included(self.0)
    }
}

/// A container for holding ranges without overlapping.
/// supports fast(`O(log(n))`) query of overlapping and points in segments.
///
/// Maybe replace it with extended binary search tree or the real segment tree?
/// So it can contains overlapping segments.
#[derive(Default)]
pub struct SegmentTree<T: Ord>(BTreeMap<T, T>);

impl<T: Ord + std::fmt::Debug> SegmentTree<T> {
    /// Try to add a element into the segment tree.
    ///
    /// - If no overlapping, insert the range into the tree and returns `true`.
    /// - If overlapping detected, do nothing and return `false`.
    pub fn add(&mut self, (start, end): (T, T)) -> bool {
        if self.is_overlapping((&start, &end)) {
            return false;
        }
        self.0.insert(start, end);
        true
    }

    pub fn get_interval_by_point<R>(&self, point: &R) -> Option<(&T, &T)>
    where
        T: Borrow<R>,
        R: Ord + ?Sized,
    {
        self.0
            .range(RangeToInclusiveRef(point))
            .next_back()
            .filter(|(_, end)| <T as Borrow<R>>::borrow(end) > point)
    }

    pub fn is_overlapping<R>(&self, range: (&R, &R)) -> bool
    where
        T: Borrow<R>,
        R: Ord + ?Sized,
    {
        self.get_interval_by_point(range.0).is_some()
            || self
                .get_interval_by_point(range.1)
                .map(|rng| <T as Borrow<R>>::borrow(rng.0) != range.1)
                .unwrap_or(false)
    }
}

/// transform a [`RaftCmdRequest`] to `(key, value, cf)` triple.
/// once it contains a write request, extract it, and return `Left((key, value, cf))`,
/// otherwise return the request itself via `Right`.
pub fn request_to_triple(mut req: Request) -> Either<(Vec<u8>, Vec<u8>, String), Request> {
    let (key, value, mut cf) = match req.get_cmd_type() {
        CmdType::Put => {
            let mut put = req.take_put();
            (put.take_key(), put.take_value(), put.cf)
        }
        CmdType::Delete => {
            let mut del = req.take_delete();
            (del.take_key(), Vec::new(), del.cf)
        }
        _ => return Either::Right(req),
    };
    // Sometimes `cf` of some request would be empty, which means write them to `default` CF.
    if cf.is_empty() {
        cf = CF_DEFAULT.to_owned();
    }
    Either::Left((key, value, cf))
}

/// `try_send!(s: Scheduler<T>, task: T)` tries to send a task to the scheduler,
/// once meet an error, would report it, with the current file and line (so it is made as a macro).    
/// returns whether it success.
#[macro_export(crate)]
macro_rules! try_send {
    ($s: expr, $task: expr) => {
        match $s.schedule($task) {
            Err(err) => {
                $crate::errors::Error::from(err).report(concat!(
                    "[",
                    file!(),
                    ":",
                    line!(),
                    "]",
                    "failed to schedule task"
                ));
                false
            }
            Ok(_) => true,
        }
    };
}

#[cfg(test)]
mod test {
    use super::SegmentTree;

    #[test]
    fn test_segment_tree() {
        let mut tree = SegmentTree::default();
        assert!(tree.add((1, 4)));
        assert!(tree.add((4, 8)));
        assert!(tree.add((42, 46)));
        assert!(!tree.add((3, 8)));
        assert_eq!(tree.get_interval_by_point(&3), Some((&1, &4)));
        assert_eq!(tree.get_interval_by_point(&7), Some((&4, &8)));
        assert_eq!(tree.get_interval_by_point(&90), None);
        assert!(tree.is_overlapping((&1, &3)));
        assert!(tree.is_overlapping((&7, &9)));
        assert!(!tree.is_overlapping((&8, &42)));
        assert!(!tree.is_overlapping((&9, &10)));
        assert!(tree.is_overlapping((&2, &10)));
    }
}
