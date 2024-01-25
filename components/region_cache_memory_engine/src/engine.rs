// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use core::slice::SlicePattern;
use std::{
    collections::BTreeMap,
    fmt::{self, Debug},
    ops::Deref,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use engine_rocks::{raw::SliceTransform, util::FixedSuffixSliceTransform};
use engine_traits::{
    CacheRange, CfNamesExt, DbVector, Error, IterOptions, Iterable, Iterator, Peekable,
    RangeCacheEngine, ReadOptions, Result, Snapshot, SnapshotMiscExt, CF_DEFAULT, CF_LOCK,
    CF_WRITE,
};
use skiplist_rs::{IterRef, Skiplist, MIB};

use crate::{
    keys::{
        decode_key, encode_key_for_eviction, encode_seek_key, InternalKey, InternalKeyComparator,
        ValueType, VALUE_TYPE_FOR_SEEK, VALUE_TYPE_FOR_SEEK_FOR_PREV,
    },
    memory_limiter::GlobalMemoryLimiter,
    range_manager::RangeManager,
};

pub(crate) const EVICTION_KEY_BUFFER_LIMIT: usize = 5 * MIB as usize;

pub(crate) fn cf_to_id(cf: &str) -> usize {
    match cf {
        CF_DEFAULT => 0,
        CF_LOCK => 1,
        CF_WRITE => 2,
        _ => panic!("unrecognized cf {}", cf),
    }
}

/// A single global set of skiplists shared by all cached ranges
#[derive(Clone)]
pub struct SkiplistEngine {
    pub(crate) data: [Arc<Skiplist<InternalKeyComparator, GlobalMemoryLimiter>>; 3],
}

impl SkiplistEngine {
    pub fn new(global_limiter: Arc<GlobalMemoryLimiter>) -> Self {
        SkiplistEngine {
            data: [
                Arc::new(Skiplist::new(
                    InternalKeyComparator::default(),
                    global_limiter.clone(),
                )),
                Arc::new(Skiplist::new(
                    InternalKeyComparator::default(),
                    global_limiter.clone(),
                )),
                Arc::new(Skiplist::new(
                    InternalKeyComparator::default(),
                    global_limiter.clone(),
                )),
            ],
        }
    }

    pub fn cf_handle(&self, cf: &str) -> Arc<Skiplist<InternalKeyComparator, GlobalMemoryLimiter>> {
        self.data[cf_to_id(cf)].clone()
    }

    fn delete_range(&self, range: &CacheRange) {
        self.data.iter().for_each(|d| {
            let mut key_buffer: Vec<Bytes> = vec![];
            let mut key_buffer_size = 0;
            let (start, end) = encode_key_for_eviction(range);

            let mut iter = d.iter();
            iter.seek(&start);
            while iter.valid() && iter.key() < &end {
                if key_buffer_size + iter.key().len() >= EVICTION_KEY_BUFFER_LIMIT {
                    for key in key_buffer.drain(..) {
                        d.remove(key.as_slice());
                    }
                    iter = d.iter();
                    iter.seek(&start);
                    continue;
                }

                key_buffer_size += iter.key().len();
                key_buffer.push(iter.key().clone());
                iter.next();
            }

            for key in key_buffer {
                d.remove(key.as_slice());
            }
        });
    }
}

impl Debug for SkiplistEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Range Memory Engine")
    }
}

// read_ts -> ref_count
#[derive(Default, Debug)]
pub(crate) struct SnapshotList(BTreeMap<u64, u64>);

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

    pub(crate) fn len(&self) -> usize {
        self.0.keys().len()
    }
}

pub struct RangeCacheMemoryEngineCore {
    engine: SkiplistEngine,
    range_manager: RangeManager,
}

impl RangeCacheMemoryEngineCore {
    pub fn new(limiter: Arc<GlobalMemoryLimiter>) -> RangeCacheMemoryEngineCore {
        RangeCacheMemoryEngineCore {
            engine: SkiplistEngine::new(limiter),
            range_manager: RangeManager::default(),
        }
    }

    pub fn engine(&self) -> SkiplistEngine {
        self.engine.clone()
    }

    pub fn range_manager(&self) -> &RangeManager {
        &self.range_manager
    }

    pub fn mut_range_manager(&mut self) -> &mut RangeManager {
        &mut self.range_manager
    }
}

/// The RangeCacheMemoryEngine serves as a range cache, storing hot ranges in
/// the leaders' store. Incoming writes that are written to disk engine (now,
/// RocksDB) are also written to the RangeCacheMemoryEngine, leading to a
/// mirrored data set in the cached ranges with the disk engine.
///
/// A load/evict unit manages the memory, deciding which ranges should be
/// evicted when the memory used by the RangeCacheMemoryEngine reaches a
/// certain limit, and determining which ranges should be loaded when there is
/// spare memory capacity.
///
/// The safe point lifetime differs between RangeCacheMemoryEngine and the disk
/// engine, often being much shorter in RangeCacheMemoryEngine. This means that
/// RangeCacheMemoryEngine may filter out some keys that still exist in the
/// disk engine, thereby improving read performance as fewer duplicated keys
/// will be read. If there's a need to read keys that may have been filtered by
/// RangeCacheMemoryEngine (as indicated by read_ts and safe_point of the
/// cached region), we resort to using a the disk engine's snapshot instead.
#[derive(Clone)]
pub struct RangeCacheMemoryEngine {
    pub(crate) core: Arc<Mutex<RangeCacheMemoryEngineCore>>,
    memory_limiter: Arc<GlobalMemoryLimiter>,
}

impl RangeCacheMemoryEngine {
    pub fn new(limiter: Arc<GlobalMemoryLimiter>) -> Self {
        let engine = RangeCacheMemoryEngineCore::new(limiter.clone());
        Self {
            core: Arc::new(Mutex::new(engine)),
            memory_limiter: limiter,
        }
    }

    pub fn new_range(&self, range: CacheRange) {
        let mut core = self.core.lock().unwrap();
        core.range_manager.new_range(range);
    }

    pub fn evict_range(&mut self, range: &CacheRange) {
        let mut core = self.core.lock().unwrap();
        if core.range_manager.evict_range(range) {
            core.engine.delete_range(range);
        }
    }
}

impl RangeCacheMemoryEngine {
    pub fn core(&self) -> &Arc<Mutex<RangeCacheMemoryEngineCore>> {
        &self.core
    }
}

impl Debug for RangeCacheMemoryEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Range Cache Memory Engine")
    }
}

impl RangeCacheEngine for RangeCacheMemoryEngine {
    type Snapshot = RangeCacheSnapshot;

    fn snapshot(&self, range: CacheRange, read_ts: u64, seq_num: u64) -> Option<Self::Snapshot> {
        RangeCacheSnapshot::new(self.clone(), range, read_ts, seq_num)
    }
}

#[derive(PartialEq)]
enum Direction {
    Uninit,
    Forward,
    Backward,
}

pub struct RangeCacheIterator {
    cf: String,
    valid: bool,
    iter: IterRef<
        Skiplist<InternalKeyComparator, GlobalMemoryLimiter>,
        InternalKeyComparator,
        GlobalMemoryLimiter,
    >,
    // The lower bound is inclusive while the upper bound is exclusive if set
    // Note: bounds (region boundaries) have no mvcc versions
    lower_bound: Vec<u8>,
    upper_bound: Vec<u8>,
    // A snapshot sequence number passed from RocksEngine Snapshot to guarantee suitable
    // visibility.
    sequence_number: u64,

    saved_user_key: Vec<u8>,
    // This is only used by backwawrd iteration where the value we want may not be pointed by the
    // `iter`
    saved_value: Option<Bytes>,

    // Not None means we are performing prefix seek
    // Note: prefix_seek doesn't support seek_to_first and seek_to_last.
    prefix_extractor: Option<FixedSuffixSliceTransform>,
    prefix: Option<Vec<u8>>,

    direction: Direction,
}

impl Iterable for RangeCacheMemoryEngine {
    type Iterator = RangeCacheIterator;

    fn iterator_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        unimplemented!()
    }
}

impl RangeCacheIterator {
    // If `skipping_saved_key` is true, the function will keep iterating until it
    // finds a user key that is larger than `saved_user_key`.
    // If `prefix` is not None, the iterator needs to stop when all keys for the
    // prefix are exhausted and the iterator is set to invalid.
    fn find_next_visible_key(&mut self, mut skip_saved_key: bool) {
        while self.iter.valid() {
            let InternalKey {
                user_key,
                sequence,
                v_type,
            } = decode_key(self.iter.key().as_slice());

            if user_key >= self.upper_bound.as_slice() {
                break;
            }

            if let Some(ref prefix) = self.prefix {
                if prefix != self.prefix_extractor.as_mut().unwrap().transform(user_key) {
                    // stop iterating due to unmatched prefix
                    break;
                }
            }

            if self.is_visible(sequence) {
                if skip_saved_key && user_key == self.saved_user_key.as_slice() {
                    // the user key has been met before, skip it.
                    // todo(SpadeA): add metrics if neede
                } else {
                    self.saved_user_key.clear();
                    self.saved_user_key.extend_from_slice(user_key);

                    match v_type {
                        ValueType::Deletion => {
                            skip_saved_key = true;
                        }
                        ValueType::Value => {
                            self.valid = true;
                            return;
                        }
                    }
                }
            } else if skip_saved_key && user_key > self.saved_user_key.as_slice() {
                // user key changed, so no need to skip it
                skip_saved_key = false;
            }

            self.iter.next();
        }

        self.valid = false;
    }

    fn is_visible(&self, seq: u64) -> bool {
        seq <= self.sequence_number
    }

    fn seek_internal(&mut self, key: &[u8]) -> Result<bool> {
        self.iter.seek(key);
        if self.iter.valid() {
            self.find_next_visible_key(false);
        }
        Ok(self.valid)
    }

    fn seek_for_prev_internal(&mut self, key: &[u8]) -> Result<bool> {
        self.iter.seek_for_prev(key);
        self.prev_internal();

        Ok(self.valid)
    }

    fn prev_internal(&mut self) {
        while self.iter.valid() {
            let InternalKey { user_key, .. } = decode_key(self.iter.key());
            self.saved_user_key.clear();
            self.saved_user_key.extend_from_slice(user_key);

            if user_key < self.lower_bound.as_slice() {
                break;
            }

            if let Some(ref prefix) = self.prefix {
                if prefix != self.prefix_extractor.as_mut().unwrap().transform(user_key) {
                    // stop iterating due to unmatched prefix
                    break;
                }
            }

            if !self.find_value_for_current_key() {
                return;
            }

            self.find_user_key_before_saved();

            if self.valid {
                return;
            }
        }

        // We have not found any key
        self.valid = false;
    }

    // Used for backwards iteration.
    // Looks at the entries with user key `saved_user_key` and finds the most
    // up-to-date value for it. Sets `valid`` to true if the value is found and is
    // ready to be presented to the user through value().
    fn find_value_for_current_key(&mut self) -> bool {
        assert!(self.iter.valid());
        let mut last_key_entry_type = ValueType::Deletion;
        while self.iter.valid() {
            let InternalKey {
                user_key,
                sequence,
                v_type,
            } = decode_key(self.iter.key());

            if !self.is_visible(sequence) || self.saved_user_key != user_key {
                // no further version is visible or the user key changed
                break;
            }

            last_key_entry_type = v_type;
            match v_type {
                ValueType::Value => {
                    self.saved_value = Some(self.iter.value().clone());
                }
                ValueType::Deletion => {
                    self.saved_value.take();
                }
            }

            self.iter.prev();
        }

        self.valid = last_key_entry_type == ValueType::Value;
        self.iter.valid()
    }

    // Move backwards until the key smaller than `saved_user_key`.
    // Changes valid only if return value is false.
    fn find_user_key_before_saved(&mut self) {
        while self.iter.valid() {
            let InternalKey { user_key, .. } = decode_key(self.iter.key());

            if user_key < self.saved_user_key.as_slice() {
                return;
            }

            self.iter.prev();
        }
    }
}

impl Iterator for RangeCacheIterator {
    fn key(&self) -> &[u8] {
        assert!(self.valid);
        &self.saved_user_key
    }

    fn value(&self) -> &[u8] {
        assert!(self.valid);
        if let Some(saved_value) = self.saved_value.as_ref() {
            saved_value.as_slice()
        } else {
            self.iter.value().as_slice()
        }
    }

    fn next(&mut self) -> Result<bool> {
        assert!(self.valid);
        assert!(self.direction == Direction::Forward);
        self.iter.next();
        self.valid = self.iter.valid();
        if self.valid {
            self.find_next_visible_key(true);
        }
        Ok(self.valid)
    }

    fn prev(&mut self) -> Result<bool> {
        assert!(self.valid);
        assert!(self.direction == Direction::Backward);
        self.prev_internal();
        Ok(self.valid)
    }

    fn seek(&mut self, key: &[u8]) -> Result<bool> {
        self.direction = Direction::Forward;
        if let Some(ref mut extractor) = self.prefix_extractor {
            assert!(key.len() >= 8);
            self.prefix = Some(extractor.transform(key).to_vec())
        }

        let seek_key = if key < self.lower_bound.as_slice() {
            self.lower_bound.as_slice()
        } else {
            key
        };

        let seek_key = encode_seek_key(seek_key, self.sequence_number, VALUE_TYPE_FOR_SEEK);
        self.seek_internal(&seek_key)
    }

    fn seek_for_prev(&mut self, key: &[u8]) -> Result<bool> {
        self.direction = Direction::Backward;
        if let Some(ref mut extractor) = self.prefix_extractor {
            assert!(key.len() >= 8);
            self.prefix = Some(extractor.transform(key).to_vec())
        }

        let seek_key = if key > self.upper_bound.as_slice() {
            encode_seek_key(
                self.upper_bound.as_slice(),
                u64::MAX,
                VALUE_TYPE_FOR_SEEK_FOR_PREV,
            )
        } else {
            encode_seek_key(key, 0, VALUE_TYPE_FOR_SEEK_FOR_PREV)
        };

        self.seek_for_prev_internal(&seek_key)
    }

    fn seek_to_first(&mut self) -> Result<bool> {
        assert!(self.prefix_extractor.is_none());
        self.direction = Direction::Forward;
        let seek_key =
            encode_seek_key(&self.lower_bound, self.sequence_number, VALUE_TYPE_FOR_SEEK);
        self.seek_internal(&seek_key)
    }

    fn seek_to_last(&mut self) -> Result<bool> {
        assert!(self.prefix_extractor.is_none());
        self.direction = Direction::Backward;
        let seek_key = encode_seek_key(&self.upper_bound, u64::MAX, VALUE_TYPE_FOR_SEEK_FOR_PREV);
        self.seek_for_prev_internal(&seek_key)
    }

    fn valid(&self) -> Result<bool> {
        Ok(self.valid)
    }
}

#[derive(Clone, Debug)]
pub struct RagneCacheSnapshotMeta {
    pub(crate) range_id: u64,
    pub(crate) range: CacheRange,
    pub(crate) snapshot_ts: u64,
    // Sequence number is shared between RangeCacheEngine and disk KvEnigne to
    // provide atomic write
    pub(crate) sequence_number: u64,
}

impl RagneCacheSnapshotMeta {
    fn new(range_id: u64, range: CacheRange, snapshot_ts: u64, sequence_number: u64) -> Self {
        Self {
            range_id,
            range,
            snapshot_ts,
            sequence_number,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RangeCacheSnapshot {
    snapshot_meta: RagneCacheSnapshotMeta,
    skiplist_engine: SkiplistEngine,
    engine: RangeCacheMemoryEngine,
}

impl RangeCacheSnapshot {
    pub fn new(
        engine: RangeCacheMemoryEngine,
        range: CacheRange,
        read_ts: u64,
        seq_num: u64,
    ) -> Option<Self> {
        let mut core = engine.core.lock().unwrap();
        if let Some(range_id) = core.range_manager.range_snapshot(&range, read_ts) {
            return Some(RangeCacheSnapshot {
                snapshot_meta: RagneCacheSnapshotMeta::new(range_id, range, read_ts, seq_num),
                skiplist_engine: core.engine.clone(),
                engine: engine.clone(),
            });
        }

        None
    }
}

impl Drop for RangeCacheSnapshot {
    fn drop(&mut self) {
        let mut core = self.engine.core.lock().unwrap();
        for range_removable in core
            .range_manager
            .remove_range_snapshot(&self.snapshot_meta)
        {
            // todo: schedule it to a separate thread
            core.engine.delete_range(&self.snapshot_meta.range);
        }
    }
}

impl Snapshot for RangeCacheSnapshot {}

impl Iterable for RangeCacheSnapshot {
    type Iterator = RangeCacheIterator;

    fn iterator_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        let iter = self.skiplist_engine.data[cf_to_id(cf)].iter();
        let prefix_extractor = if opts.prefix_same_as_start() {
            Some(FixedSuffixSliceTransform::new(8))
        } else {
            None
        };

        let (lower_bound, upper_bound) = opts.build_bounds();
        // only support with lower/upper bound set
        if lower_bound.is_none() || upper_bound.is_none() {
            return Err(Error::BoundaryNotSet);
        }

        Ok(RangeCacheIterator {
            cf: String::from(cf),
            valid: false,
            prefix: None,
            lower_bound: lower_bound.unwrap(),
            upper_bound: upper_bound.unwrap(),
            iter,
            sequence_number: self.sequence_number(),
            saved_user_key: vec![],
            saved_value: None,
            direction: Direction::Uninit,
            prefix_extractor,
        })
    }
}

impl Peekable for RangeCacheSnapshot {
    type DbVector = RangeCacheDbVector;

    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Self::DbVector>> {
        self.get_value_cf_opt(opts, CF_DEFAULT, key)
    }

    fn get_value_cf_opt(
        &self,
        _: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<Self::DbVector>> {
        let seq = self.sequence_number();
        let mut iter = self.skiplist_engine.data[cf_to_id(cf)].iter();
        let seek_key = encode_seek_key(key, self.sequence_number(), VALUE_TYPE_FOR_SEEK);

        iter.seek(&seek_key);
        if !iter.valid() {
            return Ok(None);
        }

        match decode_key(iter.key()) {
            InternalKey {
                user_key,
                v_type: ValueType::Value,
                ..
            } if user_key == key => Ok(Some(RangeCacheDbVector(iter.value().clone()))),
            _ => Ok(None),
        }
    }
}

impl CfNamesExt for RangeCacheSnapshot {
    fn cf_names(&self) -> Vec<&str> {
        unimplemented!()
    }
}

impl SnapshotMiscExt for RangeCacheSnapshot {
    fn sequence_number(&self) -> u64 {
        self.snapshot_meta.sequence_number
    }
}

#[derive(Debug)]
pub struct RangeCacheDbVector(Bytes);

impl Deref for RangeCacheDbVector {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl DbVector for RangeCacheDbVector {}

impl<'a> PartialEq<&'a [u8]> for RangeCacheDbVector {
    fn eq(&self, rhs: &&[u8]) -> bool {
        self.0.as_slice() == *rhs
    }
}

#[cfg(test)]
mod tests {
    use core::{ops::Range, slice::SlicePattern};
    use std::{iter, iter::StepBy, ops::Deref, sync::Arc};

    use bytes::{BufMut, Bytes};
    use engine_traits::{
        CacheRange, IterOptions, Iterable, Iterator, Peekable, RangeCacheEngine, ReadOptions,
    };
    use skiplist_rs::Skiplist;

    use super::{cf_to_id, GlobalMemoryLimiter, RangeCacheIterator, SkiplistEngine};
    use crate::{
        keys::{decode_key, encode_key, InternalKeyComparator, ValueType},
        RangeCacheMemoryEngine,
    };

    #[test]
    fn test_snapshot() {
        let engine = RangeCacheMemoryEngine::new(Arc::new(GlobalMemoryLimiter::default()));
        let range = CacheRange::new(b"k00".to_vec(), b"k10".to_vec());
        engine.new_range(range.clone());

        let verify_snapshot_count = |snapshot_ts, count| {
            let core = engine.core.lock().unwrap();
            if count > 0 {
                assert_eq!(
                    *core
                        .range_manager
                        .ranges()
                        .get(&range)
                        .unwrap()
                        .range_snapshot_list()
                        .0
                        .get(&snapshot_ts)
                        .unwrap(),
                    count
                );
            } else {
                assert!(
                    core.range_manager
                        .ranges()
                        .get(&range)
                        .unwrap()
                        .range_snapshot_list()
                        .0
                        .get(&snapshot_ts)
                        .is_none()
                )
            }
        };

        assert!(engine.snapshot(range.clone(), 5, u64::MAX).is_none());

        {
            let mut core = engine.core.lock().unwrap();
            core.range_manager.set_range_readable(&range, true);
        }
        let s1 = engine.snapshot(range.clone(), 5, u64::MAX).unwrap();

        {
            let mut core = engine.core.lock().unwrap();
            let t_range = CacheRange::new(b"k00".to_vec(), b"k02".to_vec());
            assert!(!core.range_manager.set_safe_ts(&t_range, 5));
            assert!(core.range_manager.set_safe_ts(&range, 5));
        }
        assert!(engine.snapshot(range.clone(), 5, u64::MAX).is_none());
        let s2 = engine.snapshot(range.clone(), 10, u64::MAX).unwrap();

        verify_snapshot_count(5, 1);
        verify_snapshot_count(10, 1);
        let s3 = engine.snapshot(range.clone(), 10, u64::MAX).unwrap();
        verify_snapshot_count(10, 2);

        drop(s1);
        verify_snapshot_count(5, 0);
        drop(s2);
        verify_snapshot_count(10, 1);
        let s4 = engine.snapshot(range.clone(), 10, u64::MAX).unwrap();
        verify_snapshot_count(10, 2);
        drop(s4);
        verify_snapshot_count(10, 1);
        drop(s3);
        {
            let core = engine.core.lock().unwrap();
            assert!(
                core.range_manager
                    .ranges()
                    .get(&range)
                    .unwrap()
                    .range_snapshot_list()
                    .is_empty()
            );
        }
    }

    fn construct_user_key(i: u64) -> Vec<u8> {
        let k = format!("k{:08}", i);
        k.as_bytes().to_owned()
    }

    fn construct_key(i: u64, mvcc: u64) -> Vec<u8> {
        let k = format!("k{:08}", i);
        let mut key = k.as_bytes().to_vec();
        // mvcc version should be make bit-wise reverse so that k-100 is less than k-99
        key.put_u64(!mvcc);
        key
    }

    fn construct_value(i: u64, j: u64) -> String {
        format!("value-{:04}-{:04}", i, j)
    }

    fn fill_data_in_skiplist(
        sl: Arc<Skiplist<InternalKeyComparator, GlobalMemoryLimiter>>,
        key_range: StepBy<Range<u64>>,
        mvcc_range: Range<u64>,
        mut start_seq: u64,
    ) {
        for mvcc in mvcc_range {
            for i in key_range.clone() {
                let key = construct_key(i, mvcc);
                let val = construct_value(i, mvcc);
                let key = encode_key(&key, start_seq, ValueType::Value);
                sl.put(key, Bytes::from(val));
            }
            start_seq += 1;
        }
    }

    fn delete_data_in_skiplist(
        sl: Arc<Skiplist<InternalKeyComparator, GlobalMemoryLimiter>>,
        key_range: StepBy<Range<u64>>,
        mvcc_range: Range<u64>,
        mut seq: u64,
    ) {
        for i in key_range {
            for mvcc in mvcc_range.clone() {
                let key = construct_key(i, mvcc);
                let key = encode_key(&key, seq, ValueType::Deletion);
                sl.put(key, Bytes::default());
            }
            seq += 1;
        }
    }

    fn construct_mvcc_key(key: &str, mvcc: u64) -> Vec<u8> {
        let mut k = vec![];
        k.extend_from_slice(key.as_bytes());
        k.put_u64(!mvcc);
        k
    }

    fn put_key_val(
        sl: &Arc<Skiplist<InternalKeyComparator, GlobalMemoryLimiter>>,
        key: &str,
        val: &str,
        mvcc: u64,
        seq: u64,
    ) {
        let key = construct_mvcc_key(key, mvcc);
        let key = encode_key(&key, seq, ValueType::Value);
        sl.put(key, Bytes::from(val.to_owned()));
    }

    fn delete_key(
        sl: &Arc<Skiplist<InternalKeyComparator, GlobalMemoryLimiter>>,
        key: &str,
        mvcc: u64,
        seq: u64,
    ) {
        let key = construct_mvcc_key(key, mvcc);
        let key = encode_key(&key, seq, ValueType::Deletion);
        sl.put(key, Bytes::default());
    }

    fn verify_key_value(k: &[u8], v: &[u8], i: u64, mvcc: u64) {
        let key = construct_key(i, mvcc);
        let val = construct_value(i, mvcc);
        assert_eq!(k, &key);
        assert_eq!(v, val.as_bytes());
    }

    fn verify_key_not_equal(k: &[u8], i: u64, mvcc: u64) {
        let key = construct_key(i, mvcc);
        assert_ne!(k, &key);
    }

    fn verify_key_values<I: iter::Iterator<Item = u32>, J: iter::Iterator<Item = u32> + Clone>(
        iter: &mut RangeCacheIterator,
        key_range: I,
        mvcc_range: J,
        foward: bool,
        ended: bool,
    ) {
        for i in key_range {
            for mvcc in mvcc_range.clone() {
                let k = iter.key();
                let val = iter.value();
                verify_key_value(k, val, i as u64, mvcc as u64);
                if foward {
                    iter.next().unwrap();
                } else {
                    iter.prev().unwrap();
                }
            }
        }

        if ended {
            assert!(!iter.valid().unwrap());
        }
    }

    #[test]
    fn test_get_value() {
        let engine = RangeCacheMemoryEngine::new(Arc::default());
        let range = CacheRange::new(b"k000".to_vec(), b"k100".to_vec());
        engine.new_range(range.clone());

        {
            let mut core = engine.core.lock().unwrap();
            core.range_manager.set_range_readable(&range, true);
            core.range_manager.set_safe_ts(&range, 5);
            let sl = core.engine.data[cf_to_id("write")].clone();
            fill_data_in_skiplist(sl.clone(), (1..10).step_by(1), 1..50, 1);
            // k1 is deleted at seq_num 150 while k49 is deleted at seq num 101
            delete_data_in_skiplist(sl, (1..10).step_by(1), 1..50, 100);
        }

        let opts = ReadOptions::default();
        {
            let snapshot = engine.snapshot(range.clone(), 10, 60).unwrap();
            for i in 1..10 {
                for mvcc in 1..50 {
                    let k = construct_key(i, mvcc);
                    let v = snapshot
                        .get_value_cf_opt(&opts, "write", &k)
                        .unwrap()
                        .unwrap();
                    verify_key_value(&k, &v, i, mvcc);
                }
                let k = construct_key(i, 50);
                assert!(
                    snapshot
                        .get_value_cf_opt(&opts, "write", &k)
                        .unwrap()
                        .is_none()
                );
            }
        }

        // all deletions
        {
            let snapshot = engine.snapshot(range.clone(), 10, u64::MAX).unwrap();
            for i in 1..10 {
                for mvcc in 1..50 {
                    let k = construct_key(i, mvcc);
                    assert!(
                        snapshot
                            .get_value_cf_opt(&opts, "write", &k)
                            .unwrap()
                            .is_none()
                    );
                }
            }
        }

        // some deletions
        {
            let snapshot = engine.snapshot(range.clone(), 10, 105).unwrap();
            for mvcc in 1..50 {
                for i in 1..7 {
                    let k = construct_key(i, mvcc);
                    assert!(
                        snapshot
                            .get_value_cf_opt(&opts, "write", &k)
                            .unwrap()
                            .is_none()
                    );
                }
                for i in 7..10 {
                    let k = construct_key(i, mvcc);
                    let v = snapshot
                        .get_value_cf_opt(&opts, "write", &k)
                        .unwrap()
                        .unwrap();
                    verify_key_value(&k, &v, i, mvcc);
                }
            }
        }
    }

    #[test]
    fn test_iterator_forawrd() {
        let engine = RangeCacheMemoryEngine::new(Arc::default());
        let range = CacheRange::new(b"k000".to_vec(), b"k100".to_vec());
        engine.new_range(range.clone());
        let step: i32 = 2;

        {
            let mut core = engine.core.lock().unwrap();
            core.range_manager.set_range_readable(&range, true);
            core.range_manager.set_safe_ts(&range, 5);
            let sl = core.engine.data[cf_to_id("write")].clone();
            fill_data_in_skiplist(sl.clone(), (1..100).step_by(step as usize), 1..10, 1);
            delete_data_in_skiplist(sl, (1..100).step_by(step as usize), 1..10, 200);
        }

        let mut iter_opt = IterOptions::default();
        let snapshot = engine.snapshot(range.clone(), 10, u64::MAX).unwrap();
        // boundaries are not set
        assert!(snapshot.iterator_opt("lock", iter_opt.clone()).is_err());

        let lower_bound = construct_user_key(1);
        let upper_bound = construct_user_key(100);
        iter_opt.set_upper_bound(&upper_bound, 0);
        iter_opt.set_lower_bound(&lower_bound, 0);

        let mut iter = snapshot.iterator_opt("lock", iter_opt.clone()).unwrap();
        assert!(!iter.seek_to_first().unwrap());

        let mut iter = snapshot.iterator_opt("default", iter_opt.clone()).unwrap();
        assert!(!iter.seek_to_first().unwrap());

        // Not restricted by bounds, no deletion (seq_num 150)
        {
            let snapshot = engine.snapshot(range.clone(), 100, 150).unwrap();
            let mut iter = snapshot.iterator_opt("write", iter_opt.clone()).unwrap();
            iter.seek_to_first().unwrap();
            verify_key_values(
                &mut iter,
                (1..100).step_by(step as usize),
                (1..10).rev(),
                true,
                true,
            );

            // seek key that is in the skiplist
            let seek_key = construct_key(11, u64::MAX);
            iter.seek(&seek_key).unwrap();
            verify_key_values(
                &mut iter,
                (11..100).step_by(step as usize),
                (1..10).rev(),
                true,
                true,
            );

            // seek key that is not in the skiplist
            let seek_key = construct_key(12, u64::MAX);
            iter.seek(&seek_key).unwrap();
            verify_key_values(
                &mut iter,
                (13..100).step_by(step as usize),
                (1..10).rev(),
                true,
                true,
            );
        }

        // Not restricted by bounds, some deletions (seq_num 230)
        {
            let snapshot = engine.snapshot(range.clone(), 10, 230).unwrap();
            let mut iter = snapshot.iterator_opt("write", iter_opt.clone()).unwrap();
            iter.seek_to_first().unwrap();
            verify_key_values(
                &mut iter,
                (63..100).step_by(step as usize),
                (1..10).rev(),
                true,
                true,
            );

            // sequence can see the deletion
            {
                // seek key that is in the skiplist
                let seek_key = construct_key(21, u64::MAX);
                assert!(iter.seek(&seek_key).unwrap());
                verify_key_not_equal(iter.key(), 21, 9);

                // seek key that is not in the skiplist
                let seek_key = construct_key(22, u64::MAX);
                assert!(iter.seek(&seek_key).unwrap());
                verify_key_not_equal(iter.key(), 23, 9);
            }

            // sequence cannot see the deletion
            {
                // seek key that is in the skiplist
                let seek_key = construct_key(65, u64::MAX);
                iter.seek(&seek_key).unwrap();
                verify_key_value(iter.key(), iter.value(), 65, 9);

                // seek key that is not in the skiplist
                let seek_key = construct_key(66, u64::MAX);
                iter.seek(&seek_key).unwrap();
                verify_key_value(iter.key(), iter.value(), 67, 9);
            }
        }

        // with bounds, no deletion (seq_num 150)
        let lower_bound = construct_user_key(20);
        let upper_bound = construct_user_key(40);
        iter_opt.set_upper_bound(&upper_bound, 0);
        iter_opt.set_lower_bound(&lower_bound, 0);
        {
            let snapshot = engine.snapshot(range.clone(), 10, 150).unwrap();
            let mut iter = snapshot.iterator_opt("write", iter_opt.clone()).unwrap();

            assert!(iter.seek_to_first().unwrap());
            verify_key_values(
                &mut iter,
                (21..40).step_by(step as usize),
                (1..10).rev(),
                true,
                true,
            );

            // seek a key that is below the lower bound is the same with seek_to_first
            let seek_key = construct_key(19, u64::MAX);
            assert!(iter.seek(&seek_key).unwrap());
            verify_key_values(
                &mut iter,
                (21..40).step_by(step as usize),
                (1..10).rev(),
                true,
                true,
            );

            // seek a key that is larger or equal to upper bound won't get any key
            let seek_key = construct_key(41, u64::MAX);
            assert!(!iter.seek(&seek_key).unwrap());
            assert!(!iter.valid().unwrap());

            let seek_key = construct_key(32, u64::MAX);
            assert!(iter.seek(&seek_key).unwrap());
            verify_key_values(
                &mut iter,
                (33..40).step_by(step as usize),
                (1..10).rev(),
                true,
                true,
            );
        }

        // with bounds, some deletions (seq_num 215)
        {
            let snapshot = engine.snapshot(range.clone(), 10, 215).unwrap();
            let mut iter = snapshot.iterator_opt("write", iter_opt).unwrap();

            // sequence can see the deletion
            {
                // seek key that is in the skiplist
                let seek_key = construct_key(21, u64::MAX);
                assert!(iter.seek(&seek_key).unwrap());
                verify_key_not_equal(iter.key(), 21, 9);

                // seek key that is not in the skiplist
                let seek_key = construct_key(20, u64::MAX);
                assert!(iter.seek(&seek_key).unwrap());
                verify_key_not_equal(iter.key(), 21, 9);
            }

            // sequence cannot see the deletion
            {
                // seek key that is in the skiplist
                let seek_key = construct_key(33, u64::MAX);
                iter.seek(&seek_key).unwrap();
                verify_key_value(iter.key(), iter.value(), 33, 9);

                // seek key that is not in the skiplist
                let seek_key = construct_key(32, u64::MAX);
                iter.seek(&seek_key).unwrap();
                verify_key_value(iter.key(), iter.value(), 33, 9);
            }
        }
    }

    #[test]
    fn test_iterator_backward() {
        let engine = RangeCacheMemoryEngine::new(Arc::default());
        let range = CacheRange::new(b"k000".to_vec(), b"k100".to_vec());
        engine.new_range(range.clone());
        let step: i32 = 2;

        {
            let mut core = engine.core.lock().unwrap();
            core.range_manager.set_range_readable(&range, true);
            core.range_manager.set_safe_ts(&range, 5);
            let sl = core.engine.data[cf_to_id("write")].clone();
            fill_data_in_skiplist(sl.clone(), (1..100).step_by(step as usize), 1..10, 1);
            delete_data_in_skiplist(sl, (1..100).step_by(step as usize), 1..10, 200);
        }

        let mut iter_opt = IterOptions::default();
        let lower_bound = construct_user_key(1);
        let upper_bound = construct_user_key(100);
        iter_opt.set_upper_bound(&upper_bound, 0);
        iter_opt.set_lower_bound(&lower_bound, 0);

        // Not restricted by bounds, no deletion (seq_num 150)
        {
            let snapshot = engine.snapshot(range.clone(), 10, 150).unwrap();
            let mut iter = snapshot.iterator_opt("write", iter_opt.clone()).unwrap();
            assert!(iter.seek_to_last().unwrap());
            verify_key_values(
                &mut iter,
                (1..100).step_by(step as usize).rev(),
                1..10,
                false,
                true,
            );

            // seek key that is in the skiplist
            let seek_key = construct_key(81, 0);
            assert!(iter.seek_for_prev(&seek_key).unwrap());
            verify_key_values(
                &mut iter,
                (1..82).step_by(step as usize).rev(),
                1..10,
                false,
                true,
            );

            // seek key that is in the skiplist
            let seek_key = construct_key(80, 0);
            assert!(iter.seek_for_prev(&seek_key).unwrap());
            verify_key_values(
                &mut iter,
                (1..80).step_by(step as usize).rev(),
                1..10,
                false,
                true,
            );
        }

        let lower_bound = construct_user_key(21);
        let upper_bound = construct_user_key(39);
        iter_opt.set_upper_bound(&upper_bound, 0);
        iter_opt.set_lower_bound(&lower_bound, 0);
        {
            let snapshot = engine.snapshot(range.clone(), 10, 150).unwrap();
            let mut iter = snapshot.iterator_opt("write", iter_opt).unwrap();

            assert!(iter.seek_to_last().unwrap());
            verify_key_values(
                &mut iter,
                (21..38).step_by(step as usize).rev(),
                1..10,
                false,
                true,
            );

            // seek a key that is above the upper bound is the same with seek_to_last
            let seek_key = construct_key(40, 0);
            assert!(iter.seek_for_prev(&seek_key).unwrap());
            verify_key_values(
                &mut iter,
                (21..38).step_by(step as usize).rev(),
                1..10,
                false,
                true,
            );

            // seek a key that is less than the lower bound won't get any key
            let seek_key = construct_key(20, u64::MAX);
            assert!(!iter.seek_for_prev(&seek_key).unwrap());
            assert!(!iter.valid().unwrap());

            let seek_key = construct_key(26, 0);
            assert!(iter.seek_for_prev(&seek_key).unwrap());
            verify_key_values(
                &mut iter,
                (21..26).step_by(step as usize).rev(),
                1..10,
                false,
                true,
            );
        }
    }

    #[test]
    fn test_seq_visibility() {
        let engine = RangeCacheMemoryEngine::new(Arc::default());
        let range = CacheRange::new(b"k000".to_vec(), b"k100".to_vec());
        engine.new_range(range.clone());
        let step: i32 = 2;

        {
            let mut core = engine.core.lock().unwrap();
            core.range_manager.set_range_readable(&range, true);
            core.range_manager.set_safe_ts(&range, 5);
            let sl = core.engine.data[cf_to_id("write")].clone();

            put_key_val(&sl, "aaa", "va1", 10, 1);
            put_key_val(&sl, "aaa", "va2", 10, 3);
            delete_key(&sl, "aaa", 10, 4);
            put_key_val(&sl, "aaa", "va4", 10, 6);

            put_key_val(&sl, "bbb", "vb1", 10, 2);
            put_key_val(&sl, "bbb", "vb2", 10, 4);

            put_key_val(&sl, "ccc", "vc1", 10, 2);
            put_key_val(&sl, "ccc", "vc2", 10, 4);
            put_key_val(&sl, "ccc", "vc3", 10, 5);
            delete_key(&sl, "ccc", 10, 6);
        }

        let mut iter_opt = IterOptions::default();
        let lower_bound = b"";
        let upper_bound = b"z";
        iter_opt.set_upper_bound(upper_bound, 0);
        iter_opt.set_lower_bound(lower_bound, 0);

        // seq num 1
        {
            let snapshot = engine.snapshot(range.clone(), u64::MAX, 1).unwrap();
            let mut iter = snapshot.iterator_opt("write", iter_opt.clone()).unwrap();
            iter.seek_to_first().unwrap();
            assert_eq!(iter.value(), b"va1");
            assert!(!iter.next().unwrap());
            let key = construct_mvcc_key("aaa", 10);
            assert_eq!(
                snapshot
                    .get_value_cf("write", &key)
                    .unwrap()
                    .unwrap()
                    .deref(),
                "va1".as_bytes()
            );
            assert!(iter.seek(&key).unwrap());
            assert_eq!(iter.value(), "va1".as_bytes());

            let key = construct_mvcc_key("bbb", 10);
            assert!(snapshot.get_value_cf("write", &key).unwrap().is_none());
            assert!(!iter.seek(&key).unwrap());

            let key = construct_mvcc_key("ccc", 10);
            assert!(snapshot.get_value_cf("write", &key).unwrap().is_none());
            assert!(!iter.seek(&key).unwrap());
        }

        // seq num 2
        {
            let snapshot = engine.snapshot(range.clone(), u64::MAX, 2).unwrap();
            let mut iter = snapshot.iterator_opt("write", iter_opt.clone()).unwrap();
            iter.seek_to_first().unwrap();
            assert_eq!(iter.value(), b"va1");
            iter.next().unwrap();
            assert_eq!(iter.value(), b"vb1");
            iter.next().unwrap();
            assert_eq!(iter.value(), b"vc1");
            assert!(!iter.next().unwrap());
        }

        // seq num 5
        {
            let snapshot = engine.snapshot(range.clone(), u64::MAX, 5).unwrap();
            let mut iter = snapshot.iterator_opt("write", iter_opt.clone()).unwrap();
            iter.seek_to_first().unwrap();
            assert_eq!(iter.value(), b"vb2");
            iter.next().unwrap();
            assert_eq!(iter.value(), b"vc3");
            assert!(!iter.next().unwrap());
        }

        // seq num 6
        {
            let snapshot = engine.snapshot(range.clone(), u64::MAX, 6).unwrap();
            let mut iter = snapshot.iterator_opt("write", iter_opt.clone()).unwrap();
            iter.seek_to_first().unwrap();
            assert_eq!(iter.value(), b"va4");
            iter.next().unwrap();
            assert_eq!(iter.value(), b"vb2");
            assert!(!iter.next().unwrap());

            let key = construct_mvcc_key("aaa", 10);
            assert_eq!(
                snapshot
                    .get_value_cf("write", &key)
                    .unwrap()
                    .unwrap()
                    .deref(),
                "va4".as_bytes()
            );
            assert!(iter.seek(&key).unwrap());
            assert_eq!(iter.value(), "va4".as_bytes());

            let key = construct_mvcc_key("bbb", 10);
            assert_eq!(
                snapshot
                    .get_value_cf("write", &key)
                    .unwrap()
                    .unwrap()
                    .deref(),
                "vb2".as_bytes()
            );
            assert!(iter.seek(&key).unwrap());
            assert_eq!(iter.value(), "vb2".as_bytes());

            let key = construct_mvcc_key("ccc", 10);
            assert!(snapshot.get_value_cf("write", &key).unwrap().is_none());
            assert!(!iter.seek(&key).unwrap());
        }
    }

    #[test]
    fn test_seq_visibility_backward() {
        let engine = RangeCacheMemoryEngine::new(Arc::default());
        let range = CacheRange::new(b"k000".to_vec(), b"k100".to_vec());
        engine.new_range(range.clone());

        {
            let mut core = engine.core.lock().unwrap();
            core.range_manager.set_range_readable(&range, true);
            core.range_manager.set_safe_ts(&range, 5);
            let sl = core.engine.data[cf_to_id("write")].clone();

            put_key_val(&sl, "aaa", "va1", 10, 2);
            put_key_val(&sl, "aaa", "va2", 10, 4);
            put_key_val(&sl, "aaa", "va3", 10, 5);
            delete_key(&sl, "aaa", 10, 6);

            put_key_val(&sl, "bbb", "vb1", 10, 2);
            put_key_val(&sl, "bbb", "vb2", 10, 4);

            put_key_val(&sl, "ccc", "vc1", 10, 1);
            put_key_val(&sl, "ccc", "vc2", 10, 3);
            delete_key(&sl, "ccc", 10, 4);
            put_key_val(&sl, "ccc", "vc4", 10, 6);
        }

        let mut iter_opt = IterOptions::default();
        let lower_bound = b"";
        let upper_bound = b"z";
        iter_opt.set_upper_bound(upper_bound, 0);
        iter_opt.set_lower_bound(lower_bound, 0);

        // seq num 1
        {
            let snapshot = engine.snapshot(range.clone(), u64::MAX, 1).unwrap();
            let mut iter = snapshot.iterator_opt("write", iter_opt.clone()).unwrap();
            iter.seek_to_last().unwrap();
            assert_eq!(iter.value(), b"vc1");
            assert!(!iter.prev().unwrap());
            let key = construct_mvcc_key("aaa", 10);
            assert!(!iter.seek_for_prev(&key).unwrap());

            let key = construct_mvcc_key("bbb", 10);
            assert!(!iter.seek_for_prev(&key).unwrap());

            let key = construct_mvcc_key("ccc", 10);
            assert!(iter.seek_for_prev(&key).unwrap());
            assert_eq!(iter.value(), "vc1".as_bytes());
        }

        // seq num 2
        {
            let snapshot = engine.snapshot(range.clone(), u64::MAX, 2).unwrap();
            let mut iter = snapshot.iterator_opt("write", iter_opt.clone()).unwrap();
            iter.seek_to_last().unwrap();
            assert_eq!(iter.value(), b"vc1");
            iter.prev().unwrap();
            assert_eq!(iter.value(), b"vb1");
            iter.prev().unwrap();
            assert_eq!(iter.value(), b"va1");
            assert!(!iter.prev().unwrap());
        }

        // seq num 5
        {
            let snapshot = engine.snapshot(range.clone(), u64::MAX, 5).unwrap();
            let mut iter = snapshot.iterator_opt("write", iter_opt.clone()).unwrap();
            iter.seek_to_last().unwrap();
            assert_eq!(iter.value(), b"vb2");
            iter.prev().unwrap();
            assert_eq!(iter.value(), b"va3");
            assert!(!iter.prev().unwrap());
        }

        // seq num 6
        {
            let snapshot = engine.snapshot(range.clone(), u64::MAX, 6).unwrap();
            let mut iter = snapshot.iterator_opt("write", iter_opt.clone()).unwrap();
            iter.seek_to_last().unwrap();
            assert_eq!(iter.value(), b"vc4");
            iter.prev().unwrap();
            assert_eq!(iter.value(), b"vb2");
            assert!(!iter.prev().unwrap());

            let key = construct_mvcc_key("ccc", 10);
            assert!(iter.seek_for_prev(&key).unwrap());
            assert_eq!(iter.value(), "vc4".as_bytes());

            let key = construct_mvcc_key("bbb", 10);
            assert!(iter.seek_for_prev(&key).unwrap());
            assert_eq!(iter.value(), "vb2".as_bytes());

            let key = construct_mvcc_key("aaa", 10);
            assert!(!iter.seek_for_prev(&key).unwrap());
        }
    }

    #[test]
    fn test_iter_use_skip() {
        let mut iter_opt = IterOptions::default();
        let lower_bound = b"";
        let upper_bound = b"z";
        iter_opt.set_upper_bound(upper_bound, 0);
        iter_opt.set_lower_bound(lower_bound, 0);
        let range = CacheRange::new(b"k000".to_vec(), b"k100".to_vec());

        // backward, all put
        {
            let engine = RangeCacheMemoryEngine::new(Arc::default());
            engine.new_range(range.clone());
            let sl = {
                let mut core = engine.core.lock().unwrap();
                core.range_manager.set_range_readable(&range, true);
                core.range_manager.set_safe_ts(&range, 5);
                core.engine.data[cf_to_id("write")].clone()
            };

            let mut s = 1;
            for seq in 2..50 {
                put_key_val(&sl, "a", "val", 10, s + 1);
                for i in 2..50 {
                    let v = construct_value(i, i);
                    put_key_val(&sl, "b", v.as_str(), 10, s + i);
                }

                let snapshot = engine.snapshot(range.clone(), 10, s + seq).unwrap();
                let mut iter = snapshot.iterator_opt("write", iter_opt.clone()).unwrap();
                assert!(iter.seek_to_last().unwrap());
                let k = construct_mvcc_key("b", 10);
                let v = construct_value(seq, seq);
                assert_eq!(iter.key(), &k);
                assert_eq!(iter.value(), v.as_bytes());

                assert!(iter.prev().unwrap());
                let k = construct_mvcc_key("a", 10);
                assert_eq!(iter.key(), &k);
                assert_eq!(iter.value(), b"val");
                assert!(!iter.prev().unwrap());
                assert!(!iter.valid().unwrap());
                s += 100;
            }
        }

        // backward, all deletes
        {
            let engine = RangeCacheMemoryEngine::new(Arc::default());
            engine.new_range(range.clone());
            let sl = {
                let mut core = engine.core.lock().unwrap();
                core.range_manager.set_range_readable(&range, true);
                core.range_manager.set_safe_ts(&range, 5);
                core.engine.data[cf_to_id("write")].clone()
            };

            let mut s = 1;
            for seq in 2..50 {
                put_key_val(&sl, "a", "val", 10, s + 1);
                for i in 2..50 {
                    delete_key(&sl, "b", 10, s + i);
                }

                let snapshot = engine.snapshot(range.clone(), 10, s + seq).unwrap();
                let mut iter = snapshot.iterator_opt("write", iter_opt.clone()).unwrap();
                assert!(iter.seek_to_last().unwrap());
                let k = construct_mvcc_key("a", 10);
                assert_eq!(iter.key(), &k);
                assert_eq!(iter.value(), b"val");
                assert!(!iter.prev().unwrap());
                assert!(!iter.valid().unwrap());
                s += 100;
            }
        }

        // backward, all deletes except for last put, last put's seq
        {
            let engine = RangeCacheMemoryEngine::new(Arc::default());
            engine.new_range(range.clone());
            let sl = {
                let mut core = engine.core.lock().unwrap();
                core.range_manager.set_range_readable(&range, true);
                core.range_manager.set_safe_ts(&range, 5);
                core.engine.data[cf_to_id("write")].clone()
            };
            put_key_val(&sl, "a", "val", 10, 1);
            for i in 2..50 {
                delete_key(&sl, "b", 10, i);
            }
            let v = construct_value(50, 50);
            put_key_val(&sl, "b", v.as_str(), 10, 50);
            let snapshot = engine.snapshot(range.clone(), 10, 50).unwrap();
            let mut iter = snapshot.iterator_opt("write", iter_opt.clone()).unwrap();
            assert!(iter.seek_to_last().unwrap());
            let k = construct_mvcc_key("b", 10);
            let v = construct_value(50, 50);
            assert_eq!(iter.key(), &k);
            assert_eq!(iter.value(), v.as_bytes());

            assert!(iter.prev().unwrap());
            let k = construct_mvcc_key("a", 10);
            assert_eq!(iter.key(), &k);
            assert_eq!(iter.value(), b"val");
            assert!(!iter.prev().unwrap());
            assert!(!iter.valid().unwrap());
        }

        // all deletes except for last put, deletions' seq
        {
            let engine = RangeCacheMemoryEngine::new(Arc::default());
            engine.new_range(range.clone());
            let sl = {
                let mut core = engine.core.lock().unwrap();
                core.range_manager.set_range_readable(&range, true);
                core.range_manager.set_safe_ts(&range, 5);
                core.engine.data[cf_to_id("write")].clone()
            };
            let mut s = 1;
            for seq in 2..50 {
                for i in 2..50 {
                    delete_key(&sl, "b", 10, s + i);
                }
                let v = construct_value(50, 50);
                put_key_val(&sl, "b", v.as_str(), 10, s + 50);

                let snapshot = engine.snapshot(range.clone(), 10, s + seq).unwrap();
                let mut iter = snapshot.iterator_opt("write", iter_opt.clone()).unwrap();
                assert!(!iter.seek_to_first().unwrap());
                assert!(!iter.valid().unwrap());

                assert!(!iter.seek_to_last().unwrap());
                assert!(!iter.valid().unwrap());

                s += 100;
            }
        }
    }

    #[test]
    fn test_prefix_seek() {
        let engine = RangeCacheMemoryEngine::new(Arc::default());
        let range = CacheRange::new(b"k000".to_vec(), b"k100".to_vec());
        engine.new_range(range.clone());

        {
            let mut core = engine.core.lock().unwrap();
            core.range_manager.set_range_readable(&range, true);
            core.range_manager.set_safe_ts(&range, 5);
            let sl = core.engine.data[cf_to_id("write")].clone();

            for i in 1..5 {
                for mvcc in 10..20 {
                    let user_key = construct_key(i, mvcc);
                    let internal_key = encode_key(&user_key, 10, ValueType::Value);
                    let v = format!("v{:02}{:02}", i, mvcc);
                    sl.put(internal_key, v);
                }
            }
        }

        let mut iter_opt = IterOptions::default();
        let lower_bound = construct_user_key(1);
        let upper_bound = construct_user_key(5);
        iter_opt.set_upper_bound(&upper_bound, 0);
        iter_opt.set_lower_bound(&lower_bound, 0);
        iter_opt.set_prefix_same_as_start(true);
        let snapshot = engine.snapshot(range.clone(), u64::MAX, u64::MAX).unwrap();
        let mut iter = snapshot.iterator_opt("write", iter_opt.clone()).unwrap();

        // prefix seek, forward
        for i in 1..5 {
            let seek_key = construct_key(i, 100);
            assert!(iter.seek(&seek_key).unwrap());
            let mut start = 19;
            while iter.valid().unwrap() {
                let user_key = iter.key();
                let mvcc = !u64::from_be_bytes(user_key[user_key.len() - 8..].try_into().unwrap());
                assert_eq!(mvcc, start);
                let v = format!("v{:02}{:02}", i, start);
                assert_eq!(v.as_bytes(), iter.value());
                start -= 1;
                iter.next().unwrap();
            }
            assert_eq!(start, 9);
        }

        // prefix seek, backward
        for i in 1..5 {
            let seek_key = construct_key(i, 0);
            assert!(iter.seek_for_prev(&seek_key).unwrap());
            let mut start = 10;
            while iter.valid().unwrap() {
                let user_key = iter.key();
                let mvcc = !u64::from_be_bytes(user_key[user_key.len() - 8..].try_into().unwrap());
                assert_eq!(mvcc, start);
                let v = format!("v{:02}{:02}", i, start);
                assert_eq!(v.as_bytes(), iter.value());
                start += 1;
                iter.prev().unwrap();
            }
            assert_eq!(start, 20);
        }
    }

    #[test]
    fn test_skiplist_engine_evict_range() {
        let sl_engine = SkiplistEngine::new(Arc::default());
        sl_engine.data.iter().for_each(|sl| {
            fill_data_in_skiplist(sl.clone(), (1..60).step_by(1), 1..2, 1);
        });

        let evict_range = CacheRange::new(construct_user_key(20), construct_user_key(40));
        sl_engine.delete_range(&evict_range);
        sl_engine.data.iter().for_each(|sl| {
            let mut iter = sl.iter();
            iter.seek_to_first();
            for i in 1..20 {
                let internal_key = decode_key(iter.key());
                let expected_key = construct_key(i, 1);
                assert_eq!(internal_key.user_key, &expected_key);
                iter.next();
            }

            for i in 40..60 {
                let internal_key = decode_key(iter.key());
                let expected_key = construct_key(i, 1);
                assert_eq!(internal_key.user_key, &expected_key);
                iter.next();
            }
            assert!(!iter.valid());
        });
    }

    #[test]
    fn test_evict_range_without_snapshot() {
        let mut engine = RangeCacheMemoryEngine::new(Arc::default());
        let range = CacheRange::new(construct_user_key(0), construct_user_key(30));
        let evict_range = CacheRange::new(construct_user_key(10), construct_user_key(20));
        engine.new_range(range.clone());

        {
            let mut core = engine.core.lock().unwrap();
            core.range_manager.set_range_readable(&range, true);
            core.range_manager.set_safe_ts(&range, 5);
            let sl = core.engine.data[cf_to_id("write")].clone();
            for i in 0..30 {
                let user_key = construct_key(i, 10);
                let internal_key = encode_key(&user_key, 10, ValueType::Value);
                let v = construct_value(i, 10);
                sl.put(internal_key.clone(), v.clone());
            }
        }

        engine.evict_range(&evict_range);
        assert!(engine.snapshot(range.clone(), 10, 200).is_none());
        assert!(engine.snapshot(evict_range, 10, 200).is_none());

        {
            let removed = engine.memory_limiter.removed.lock().unwrap();
            for i in 10..20 {
                let user_key = construct_key(i, 10);
                let internal_key = encode_key(&user_key, 10, ValueType::Value);
                assert!(removed.contains(internal_key.as_slice()));
            }
        }

        let r_left = CacheRange::new(construct_user_key(0), construct_user_key(10));
        let r_right = CacheRange::new(construct_user_key(20), construct_user_key(30));
        let snap_left = engine.snapshot(r_left, 10, 200).unwrap();

        let mut iter_opt = IterOptions::default();
        let lower_bound = construct_user_key(0);
        let upper_bound = construct_user_key(10);
        iter_opt.set_upper_bound(&upper_bound, 0);
        iter_opt.set_lower_bound(&lower_bound, 0);
        let mut iter = snap_left.iterator_opt("write", iter_opt.clone()).unwrap();
        iter.seek_to_first().unwrap();
        verify_key_values(&mut iter, (0..10).step_by(1), 10..11, true, true);

        let lower_bound = construct_user_key(20);
        let upper_bound = construct_user_key(30);
        iter_opt.set_upper_bound(&upper_bound, 0);
        iter_opt.set_lower_bound(&lower_bound, 0);
        let mut iter = snap_left.iterator_opt("write", iter_opt).unwrap();
        iter.seek_to_first().unwrap();
        verify_key_values(&mut iter, (20..30).step_by(1), 10..11, true, true);
    }

    #[test]
    fn test_evict_range_with_snapshot() {
        let mut engine = RangeCacheMemoryEngine::new(Arc::default());
        let range = CacheRange::new(construct_user_key(0), construct_user_key(30));
        let evict_range = CacheRange::new(construct_user_key(10), construct_user_key(20));
        engine.new_range(range.clone());
        {
            let mut core = engine.core.lock().unwrap();
            core.range_manager.set_range_readable(&range, true);
            core.range_manager.set_safe_ts(&range, 5);
            let sl = core.engine.data[cf_to_id("write")].clone();
            for i in 0..30 {
                let user_key = construct_key(i, 10);
                let internal_key = encode_key(&user_key, 10, ValueType::Value);
                let v = construct_value(i, 10);
                sl.put(internal_key.clone(), v.clone());
            }
        }

        let s1 = engine.snapshot(range.clone(), 10, 10);
        let s2 = engine.snapshot(range, 20, 20);
        engine.evict_range(&evict_range);
        let range_left = CacheRange::new(construct_user_key(0), construct_user_key(10));
        let s3 = engine.snapshot(range_left, 20, 20).unwrap();
        let range_right = CacheRange::new(construct_user_key(20), construct_user_key(30));
        let s4 = engine.snapshot(range_right, 20, 20).unwrap();

        drop(s3);
        let range_left_eviction = CacheRange::new(construct_user_key(0), construct_user_key(5));
        engine.evict_range(&range_left_eviction);

        {
            let removed = engine.memory_limiter.removed.lock().unwrap();
            assert!(removed.is_empty());
        }

        drop(s1);
        {
            let removed = engine.memory_limiter.removed.lock().unwrap();
            for i in 10..20 {
                let user_key = construct_key(i, 10);
                let internal_key = encode_key(&user_key, 10, ValueType::Value);
                assert!(!removed.contains(internal_key.as_slice()));
            }
        }

        drop(s2);
        // s2 is dropped, so the range of `evict_range` is removed. The snapshot of s3
        // and s4 does not prevent it as they are not overlapped.
        {
            let removed = engine.memory_limiter.removed.lock().unwrap();
            for i in 10..20 {
                let user_key = construct_key(i, 10);
                let internal_key = encode_key(&user_key, 10, ValueType::Value);
                assert!(removed.contains(internal_key.as_slice()));
            }
        }
    }
}
