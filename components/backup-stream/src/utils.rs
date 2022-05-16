// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    borrow::Borrow,
    collections::{hash_map::RandomState, BTreeMap, HashMap},
    ops::{Bound, RangeBounds},
    time::Duration,
};

use engine_traits::{CfName, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
use futures::{channel::mpsc, executor::block_on, StreamExt};
use kvproto::raft_cmdpb::{CmdType, Request};
use raft::StateRole;
use raftstore::{coprocessor::RegionInfoProvider, RegionInfo};
use tikv::storage::CfStatistics;
use tikv_util::{box_err, time::Instant, warn, worker::Scheduler, Either};
use tokio::sync::{Mutex, RwLock};
use txn_types::{Key, Lock, LockType};

use crate::{
    errors::{Error, Result},
    Task,
};

/// wrap a user key with encoded data key.
pub fn wrap_key(v: Vec<u8>) -> Vec<u8> {
    // TODO: encode in place.
    let key = Key::from_raw(v.as_slice()).into_encoded();
    key
}

/// Transform a str to a [`engine_traits::CfName`]\(`&'static str`).
/// If the argument isn't one of `""`, `"DEFAULT"`, `"default"`, `"WRITE"`, `"write"`, `"LOCK"`, `"lock"`...
/// returns "ERR_CF". (Which would be ignored then.)
pub fn cf_name(s: &str) -> CfName {
    match s {
        "" | "DEFAULT" | "default" => CF_DEFAULT,
        "WRITE" | "write" => CF_WRITE,
        "LOCK" | "lock" => CF_LOCK,
        "RAFT" | "raft" => CF_RAFT,
        _ => {
            Error::Other(box_err!("unknown cf name {}", s)).report("");
            "ERR_CF"
        }
    }
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

struct RangeToExclusiveRef<'a, T: ?Sized>(&'a T);

impl<'a, T: ?Sized> RangeBounds<T> for RangeToExclusiveRef<'a, T> {
    fn start_bound(&self) -> Bound<&T> {
        Bound::Unbounded
    }

    fn end_bound(&self) -> Bound<&T> {
        Bound::Excluded(self.0)
    }
}
#[derive(Default, Debug, Clone)]
pub struct SegmentMap<K: Ord, V>(BTreeMap<K, SegmentValue<K, V>>);

#[derive(Clone, Debug)]
pub struct SegmentValue<R, T> {
    pub range_end: R,
    pub item: T,
}

/// A container for holding ranges without overlapping.
/// supports fast(`O(log(n))`) query of overlapping and points in segments.
///
/// Maybe replace it with extended binary search tree or the real segment tree?
/// So it can contains overlapping segments.
pub type SegmentSet<T> = SegmentMap<T, ()>;

impl<K: Ord, V: Default> SegmentMap<K, V> {
    /// Try to add a element into the segment tree, with default value.
    /// (This is useful when using the segment tree as a `Set`, i.e. `SegmentMap<T, ()>`)
    ///
    /// - If no overlapping, insert the range into the tree and returns `true`.
    /// - If overlapping detected, do nothing and return `false`.
    pub fn add(&mut self, (start, end): (K, K)) -> bool {
        self.insert((start, end), V::default())
    }
}

impl<K: Ord, V> SegmentMap<K, V> {
    /// Remove all records in the map.
    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// Like `add`, but insert a value associated to the key.
    pub fn insert(&mut self, (start, end): (K, K), value: V) -> bool {
        if self.is_overlapping((&start, &end)) {
            return false;
        }
        self.0.insert(
            start,
            SegmentValue {
                range_end: end,
                item: value,
            },
        );
        true
    }

    /// Find a segment with its associated value by the point.
    pub fn get_by_point<R>(&self, point: &R) -> Option<(&K, &K, &V)>
    where
        K: Borrow<R>,
        R: Ord + ?Sized,
    {
        self.0
            .range(RangeToInclusiveRef(point))
            .next_back()
            .filter(|(_, end)| <K as Borrow<R>>::borrow(&end.range_end) > point)
            .map(|(k, v)| (k, &v.range_end, &v.item))
    }

    /// Like `get_by_point`, but omit the segment.
    pub fn get_value_by_point<R>(&self, point: &R) -> Option<&V>
    where
        K: Borrow<R>,
        R: Ord + ?Sized,
    {
        self.get_by_point(point).map(|(_, _, v)| v)
    }

    /// Like `get_by_point`, but omit the segment.
    pub fn get_interval_by_point<R>(&self, point: &R) -> Option<(&K, &K)>
    where
        K: Borrow<R>,
        R: Ord + ?Sized,
    {
        self.get_by_point(point).map(|(k, v, _)| (k, v))
    }

    pub fn find_overlapping<R>(&self, range: (&R, &R)) -> Option<(&K, &K, &V)>
    where
        K: Borrow<R>,
        R: Ord + ?Sized,
    {
        // o: The Start Key.
        // e: The End Key.
        // +: The Boundary of Candidate Range.
        // |------+-s----+----e----|
        // Firstly, we check whether the start point is in some range.
        // if true, it must be overlapping.
        if let Some(overlap_with_start) = self.get_by_point(range.0) {
            return Some(overlap_with_start);
        }
        // |--s----+-----+----e----|
        // Otherwise, the possibility of being overlapping would be there are some sub range
        // of the queried range...
        // |--s----+----e----+-----|
        // ...Or the end key is contained by some Range.
        // For faster query, we merged the two cases together.
        let covered_by_the_range = self
             .0
             // When querying possibility of overlapping by end key,
             // we don't want the range [end key, ...) become a candidate.
             // (which is impossible to overlapping with the range)
             .range(RangeToExclusiveRef(range.1))
             .next_back()
             .filter(|(start, end)| {
                 <K as Borrow<R>>::borrow(&end.range_end) > range.1
                     || <K as Borrow<R>>::borrow(start) > range.0
             });
        covered_by_the_range.map(|(k, v)| (k, &v.range_end, &v.item))
    }

    /// Check whether the range is overlapping with any range in the segment tree.
    pub fn is_overlapping<R>(&self, range: (&R, &R)) -> bool
    where
        K: Borrow<R>,
        R: Ord + ?Sized,
    {
        self.find_overlapping(range).is_some()
    }

    pub fn get_inner(&mut self) -> &mut BTreeMap<K, SegmentValue<K, V>> {
        &mut self.0
    }
}

/// transform a [`RaftCmdRequest`] to `(key, value, cf)` triple.
/// once it contains a write request, extract it, and return `Left((key, value, cf))`,
/// otherwise return the request itself via `Right`.
pub fn request_to_triple(mut req: Request) -> Either<(Vec<u8>, Vec<u8>, CfName), Request> {
    let (key, value, cf) = match req.get_cmd_type() {
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
    Either::Left((key, value, cf_name(cf.as_str())))
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

/// a hacky macro which allow us enable all debug log via the feature `backup_stream_debug`.
/// because once we enable debug log for all crates, it would soon get too verbose to read.
/// using this macro now we can enable debug log level for the crate only (even compile time...).
#[macro_export(crate)]
macro_rules! debug {
    ($($t: tt)+) => {
        if cfg!(feature = "backup-stream-debug") {
            tikv_util::info!(#"backup-stream", $($t)+)
        } else {
            tikv_util::debug!(#"backup-stream", $($t)+)
        }
    };
}

macro_rules! record_fields {
    ($m:expr,$cf:expr,$stat:expr, [ $(.$s:ident),+ ]) => {
        {
            let m = &$m;
            let cf = &$cf;
            let stat = &$stat;
            $( m.with_label_values(&[cf, stringify!($s)]).inc_by(stat.$s as _) );+
        }
    };
}

pub fn record_cf_stat(cf_name: &str, stat: &CfStatistics) {
    let m = &crate::metrics::INITIAL_SCAN_STAT;
    m.with_label_values(&[cf_name, "read_bytes"])
        .inc_by(stat.flow_stats.read_bytes as _);
    m.with_label_values(&[cf_name, "read_keys"])
        .inc_by(stat.flow_stats.read_keys as _);
    record_fields!(
        m,
        cf_name,
        stat,
        [
            .get,
            .next,
            .prev,
            .seek,
            .seek_for_prev,
            .over_seek_bound,
            .next_tombstone,
            .prev_tombstone,
            .seek_tombstone,
            .seek_for_prev_tombstone
        ]
    );
}

/// a shortcut for handing the result return from `Router::on_events`, when any faliure, send a fatal error to the `doom_messenger`.
pub fn handle_on_event_result(doom_messenger: &Scheduler<Task>, result: Vec<(String, Result<()>)>) {
    for (task, res) in result.into_iter() {
        if let Err(err) = res {
            try_send!(
                doom_messenger,
                Task::FatalError(
                    task,
                    Box::new(err.context("failed to record event to local temporary files"))
                )
            );
        }
    }
}

/// tests whether the lock should be tracked or skipped.
pub fn should_track_lock(l: &Lock) -> bool {
    match l.lock_type {
        LockType::Put | LockType::Delete => true,
        // Lock or Pessimistic lock won't commit more data,
        // (i.e. won't break the integration of data between [Lock.start_ts, get_ts()))
        // it is safe for ignoring them and advancing resolved_ts.
        LockType::Lock | LockType::Pessimistic => false,
    }
}

#[cfg(test)]
mod test {
    use crate::utils::SegmentMap;

    #[test]
    fn test_segment_tree() {
        let mut tree = SegmentMap::default();
        assert!(tree.add((1, 4)));
        assert!(tree.add((4, 8)));
        assert!(tree.add((42, 46)));
        assert!(!tree.add((3, 8)));
        assert!(tree.insert((47, 88), "hello".to_owned()));
        assert_eq!(
            tree.get_value_by_point(&49).map(String::as_str),
            Some("hello")
        );
        assert_eq!(tree.get_interval_by_point(&3), Some((&1, &4)));
        assert_eq!(tree.get_interval_by_point(&7), Some((&4, &8)));
        assert_eq!(tree.get_interval_by_point(&90), None);
        assert!(tree.is_overlapping((&1, &3)));
        assert!(tree.is_overlapping((&7, &9)));
        assert!(!tree.is_overlapping((&8, &42)));
        assert!(!tree.is_overlapping((&9, &10)));
        assert!(tree.is_overlapping((&2, &10)));
        assert!(tree.is_overlapping((&0, &9999999)));
    }
}
