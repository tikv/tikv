// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use core::slice::SlicePattern;
use std::{
    cmp,
    collections::BTreeMap,
    fmt::{self, Debug},
    ops::Deref,
    sync::{Arc, Mutex},
};

use bytes::{BufMut, Bytes, BytesMut};
use collections::HashMap;
use engine_traits::{
    CfNamesExt, DbVector, Error, IterOptions, Iterable, Iterator, Mutable, Peekable, ReadOptions,
    RegionCacheEngine, Result, Snapshot, SnapshotMiscExt, WriteBatch, WriteBatchExt, WriteOptions,
    CF_DEFAULT, CF_LOCK, CF_WRITE,
};
use skiplist_rs::{IterRef, KeyComparator, Skiplist};
use tikv_util::config::ReadableSize;

fn cf_to_id(cf: &str) -> usize {
    match cf {
        CF_DEFAULT => 0,
        CF_LOCK => 1,
        CF_WRITE => 2,
        _ => panic!("unrecognized cf {}", cf),
    }
}

const ENC_KEY_SEQ_LENGTH: usize = std::mem::size_of::<u64>();

#[inline]
fn decode_key(encoded_key: &[u8]) -> (&[u8], u64) {
    debug_assert!(encoded_key.len() >= ENC_KEY_SEQ_LENGTH);
    let seq_offset = encoded_key.len() - ENC_KEY_SEQ_LENGTH;
    (
        &encoded_key[..seq_offset],
        u64::from_be_bytes(
            encoded_key[seq_offset..seq_offset + ENC_KEY_SEQ_LENGTH]
                .try_into()
                .unwrap(),
        ),
    )
}

#[inline]
fn encode_key_internal<T: BufMut>(key: &[u8], seq: u64, f: impl FnOnce(usize) -> T) -> T {
    let mut e = f(key.len() + ENC_KEY_SEQ_LENGTH);
    e.put(key);
    e.put_u64(seq);
    e
}

#[inline]
fn encode_key(key: &[u8], seq: u64) -> Bytes {
    let e = encode_key_internal::<BytesMut>(key, seq, BytesMut::with_capacity);
    e.freeze()
}

#[inline]
fn encode_seek_key(key: &[u8], seq: u64) -> Vec<u8> {
    encode_key_internal::<Vec<_>>(key, seq, Vec::with_capacity)
}

#[derive(Default, Debug, Clone, Copy)]
pub struct InternalKeyComparator {}

impl InternalKeyComparator {
    fn same_key(lhs: &[u8], rhs: &[u8]) -> bool {
        let (user_k_1, _) = decode_key(lhs);
        let (user_k_2, _) = decode_key(rhs);
        user_k_1 == user_k_2
    }
}

impl KeyComparator for InternalKeyComparator {
    fn compare_key(&self, lhs: &[u8], rhs: &[u8]) -> cmp::Ordering {
        let (user_k_1, seq1) = decode_key(lhs);
        let (user_k_2, seq2) = decode_key(rhs);
        let r = user_k_1.cmp(user_k_2);
        if r.is_eq() {
            if seq1 > seq2 {
                return cmp::Ordering::Less;
            } else if seq1 < seq2 {
                return cmp::Ordering::Greater;
            } else {
                return cmp::Ordering::Equal;
            }
        }
        r
    }

    fn same_key(&self, lhs: &[u8], rhs: &[u8]) -> bool {
        InternalKeyComparator::same_key(lhs, rhs)
    }
}

/// RegionMemoryEngine stores data for a specific cached region
///
/// todo: The skiplist used here currently is for test purpose. Replace it
/// with a formal implementation.
#[derive(Clone)]
pub struct RegionMemoryEngine {
    data: [Arc<Skiplist<InternalKeyComparator>>; 3],
}

impl RegionMemoryEngine {
    pub fn with_capacity(arena_size: usize) -> Self {
        RegionMemoryEngine {
            data: [
                Arc::new(Skiplist::with_capacity(
                    InternalKeyComparator::default(),
                    arena_size,
                    true,
                )),
                Arc::new(Skiplist::with_capacity(
                    InternalKeyComparator::default(),
                    arena_size,
                    true,
                )),
                Arc::new(Skiplist::with_capacity(
                    InternalKeyComparator::default(),
                    arena_size,
                    true,
                )),
            ],
        }
    }
}

impl Default for RegionMemoryEngine {
    fn default() -> Self {
        RegionMemoryEngine::with_capacity(ReadableSize::mb(1).0 as usize)
    }
}

impl Debug for RegionMemoryEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Region Memory Engine")
    }
}

// read_ts -> ref_count
#[derive(Default)]
struct SnapshotList(BTreeMap<u64, u64>);

impl SnapshotList {
    fn new_snapshot(&mut self, read_ts: u64) {
        // snapshot with this ts may be granted before
        let count = self.0.get(&read_ts).unwrap_or(&0) + 1;
        self.0.insert(read_ts, count);
    }

    fn remove_snapshot(&mut self, read_ts: u64) {
        let count = self.0.get_mut(&read_ts).unwrap();
        assert!(*count >= 1);
        if *count == 1 {
            self.0.remove(&read_ts).unwrap();
        } else {
            *count -= 1;
        }
    }
}

#[derive(Default)]
pub struct RegionMemoryMeta {
    // It records the snapshots that have been granted previsously with specific snapshot_ts. We
    // should guarantee that the data visible to any one of the snapshot in it will not be removed.
    snapshot_list: SnapshotList,
    // It indicates whether the region is readable. False means integrity of the data in this
    // cached region is not satisfied due to being evicted for instance.
    can_read: bool,
    // Request with read_ts below it is not eligible for granting snapshot.
    // Note: different region can have different safe_ts.
    safe_ts: u64,
}

impl RegionMemoryMeta {
    pub fn set_can_read(&mut self, can_read: bool) {
        self.can_read = can_read;
    }

    pub fn set_safe_ts(&mut self, safe_ts: u64) {
        self.safe_ts = safe_ts;
    }
}

#[derive(Default)]
pub struct RegionCacheMemoryEngineCore {
    engine: HashMap<u64, RegionMemoryEngine>,
    region_metas: HashMap<u64, RegionMemoryMeta>,
}

impl RegionCacheMemoryEngineCore {
    pub fn mut_region_meta(&mut self, region_id: u64) -> Option<&mut RegionMemoryMeta> {
        self.region_metas.get_mut(&region_id)
    }
}

/// The RegionCacheMemoryEngine serves as a region cache, storing hot regions in
/// the leaders' store. Incoming writes that are written to disk engine (now,
/// RocksDB) are also written to the RegionCacheMemoryEngine, leading to a
/// mirrored data set in the cached regions with the disk engine.
///
/// A load/evict unit manages the memory, deciding which regions should be
/// evicted when the memory used by the RegionCacheMemoryEngine reaches a
/// certain limit, and determining which regions should be loaded when there is
/// spare memory capacity.
///
/// The safe point lifetime differs between RegionCacheMemoryEngine and the disk
/// engine, often being much shorter in RegionCacheMemoryEngine. This means that
/// RegionCacheMemoryEngine may filter out some keys that still exist in the
/// disk engine, thereby improving read performance as fewer duplicated keys
/// will be read. If there's a need to read keys that may have been filtered by
/// RegionCacheMemoryEngine (as indicated by read_ts and safe_point of the
/// cached region), we resort to using a the disk engine's snapshot instead.
#[derive(Clone, Default)]
pub struct RegionCacheMemoryEngine {
    core: Arc<Mutex<RegionCacheMemoryEngineCore>>,
}

impl RegionCacheMemoryEngine {
    pub fn core(&self) -> &Arc<Mutex<RegionCacheMemoryEngineCore>> {
        &self.core
    }
}

impl Debug for RegionCacheMemoryEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Region Cache Memory Engine")
    }
}

impl RegionCacheMemoryEngine {
    pub fn new_region(&self, region_id: u64) {
        let mut core = self.core.lock().unwrap();

        assert!(core.engine.get(&region_id).is_none());
        assert!(core.region_metas.get(&region_id).is_none());
        core.engine.insert(region_id, RegionMemoryEngine::default());
        core.region_metas
            .insert(region_id, RegionMemoryMeta::default());
    }
}

impl RegionCacheEngine for RegionCacheMemoryEngine {
    type Snapshot = RegionCacheSnapshot;

    // todo(SpadeA): add sequence number logic
    fn snapshot(&self, region_id: u64, read_ts: u64, seq_num: u64) -> Option<Self::Snapshot> {
        RegionCacheSnapshot::new(self.clone(), region_id, read_ts, seq_num)
    }
}

// todo: fill fields needed
pub struct RegionCacheWriteBatch;

impl WriteBatchExt for RegionCacheMemoryEngine {
    type WriteBatch = RegionCacheWriteBatch;
    // todo: adjust it
    const WRITE_BATCH_MAX_KEYS: usize = 256;

    fn write_batch(&self) -> Self::WriteBatch {
        RegionCacheWriteBatch {}
    }

    fn write_batch_with_cap(&self, _: usize) -> Self::WriteBatch {
        RegionCacheWriteBatch {}
    }
}

#[derive(PartialEq)]
enum Direction {
    Uninit,
    Forward,
    Backward,
}

pub struct RegionCacheIterator {
    cf: String,
    valid: bool,
    prefix_same_as_start: bool,
    prefix: Option<Vec<u8>>,
    iter: IterRef<Skiplist<InternalKeyComparator>, InternalKeyComparator>,
    // The lower bound is inclusive while the upper bound is exclusive if set
    lower_bound: Vec<u8>,
    upper_bound: Vec<u8>,
    // A snapshot sequence number passed from RocksEngine Snapshot to guarantee suitable
    // visibility.
    sequence_number: u64,

    saved_user_key: Vec<u8>,
    // This is only used by backwawrd iteration where the value we want may not be pointed by the
    // `iter`
    pinned_value: Option<Bytes>,

    direction: Direction,
}

impl Iterable for RegionCacheMemoryEngine {
    type Iterator = RegionCacheIterator;

    fn iterator_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        unimplemented!()
    }
}

impl RegionCacheIterator {
    // If `skipping_saved_key` is true, the function will keep iterating until it
    // finds a user key that is larger than `saved_user_key`.
    // If `prefix` is not None, the iterator needs to stop when all keys for the
    // prefix are exhausted and the iterator is set to invalid.
    fn find_next_visible_key(&mut self, mut skip_saved_key: bool) {
        while self.iter.valid() {
            let (user_key, seq_num) = decode_key(self.iter.key().as_slice());

            if user_key >= self.upper_bound.as_slice() {
                break;
            }

            if self.prefix_same_as_start {
                // todo(SpadeA): support prefix seek
                unimplemented!()
            }

            if self.is_visible(seq_num) {
                if skip_saved_key && user_key == self.saved_user_key.as_slice() {
                    // the user key has been met before, skip it.
                } else {
                    self.saved_user_key.clear();
                    self.saved_user_key.extend_from_slice(user_key);
                    self.valid = true;
                    return;
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
            let (user_key, seq_num) = decode_key(self.iter.key());
            self.saved_user_key.clear();
            self.saved_user_key.extend_from_slice(user_key);

            if user_key < self.lower_bound.as_slice() {
                break;
            }

            if self.prefix_same_as_start {
                // todo(SpadeA): support prefix seek
                unimplemented!()
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
        let mut find_valid_value = false;
        while self.iter.valid() {
            let (user_key, seq_num) = decode_key(self.iter.key());

            if !self.is_visible(seq_num) || self.saved_user_key != user_key {
                // no further version is visible or the user key changed
                break;
            }

            find_valid_value = true;
            self.pinned_value = Some(self.iter.value().clone());

            self.iter.prev();
        }

        self.valid = find_valid_value;
        self.iter.valid()
    }

    // Move backwards until the key smaller than `saved_user_key`.
    // Changes valid only if return value is false.
    fn find_user_key_before_saved(&mut self) {
        while self.iter.valid() {
            let (user_key, seq_num) = decode_key(self.iter.key());

            if user_key < self.saved_user_key.as_slice() {
                return;
            }

            self.iter.prev();
        }
    }
}

impl Iterator for RegionCacheIterator {
    fn key(&self) -> &[u8] {
        assert!(self.valid);
        &self.saved_user_key
    }

    fn value(&self) -> &[u8] {
        assert!(self.valid);
        if self.pinned_value.is_some() {
            return self.pinned_value.as_ref().unwrap().as_slice();
        }
        self.iter.value().as_slice()
    }

    fn next(&mut self) -> Result<bool> {
        assert!(self.valid);
        if self.direction == Direction::Uninit {
            self.direction = Direction::Forward;
        }
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
        if self.direction == Direction::Uninit {
            self.direction = Direction::Backward;
        }
        assert!(self.direction == Direction::Backward);
        self.prev_internal();
        Ok(self.valid)
    }

    fn seek(&mut self, key: &[u8]) -> Result<bool> {
        let seek_key = if key < self.lower_bound.as_slice() {
            self.lower_bound.as_slice()
        } else {
            key
        };

        let seek_key = encode_seek_key(seek_key, self.sequence_number);
        self.seek_internal(&seek_key)
    }

    fn seek_for_prev(&mut self, key: &[u8]) -> Result<bool> {
        let seek_key = if key > self.upper_bound.as_slice() {
            encode_seek_key(self.upper_bound.as_slice(), u64::MAX)
        } else {
            encode_seek_key(key, 0)
        };

        self.seek_for_prev_internal(&seek_key)
    }

    fn seek_to_first(&mut self) -> Result<bool> {
        let seek_key = encode_seek_key(&self.lower_bound, self.sequence_number);
        self.seek_internal(&seek_key)
    }

    fn seek_to_last(&mut self) -> Result<bool> {
        let seek_key = encode_seek_key(&self.upper_bound, u64::MAX);
        self.seek_for_prev_internal(&seek_key)
    }

    fn valid(&self) -> Result<bool> {
        Ok(self.valid)
    }
}

impl WriteBatch for RegionCacheWriteBatch {
    fn write_opt(&mut self, _: &WriteOptions) -> Result<u64> {
        unimplemented!()
    }

    fn data_size(&self) -> usize {
        unimplemented!()
    }

    fn count(&self) -> usize {
        unimplemented!()
    }

    fn is_empty(&self) -> bool {
        unimplemented!()
    }

    fn should_write_to_engine(&self) -> bool {
        unimplemented!()
    }

    fn clear(&mut self) {
        unimplemented!()
    }

    fn set_save_point(&mut self) {
        unimplemented!()
    }

    fn pop_save_point(&mut self) -> Result<()> {
        unimplemented!()
    }

    fn rollback_to_save_point(&mut self) -> Result<()> {
        unimplemented!()
    }

    fn merge(&mut self, _: Self) -> Result<()> {
        unimplemented!()
    }
}

impl Mutable for RegionCacheWriteBatch {
    fn put(&mut self, _: &[u8], _: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn put_cf(&mut self, _: &str, _: &[u8], _: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn delete(&mut self, _: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn delete_cf(&mut self, _: &str, _: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn delete_range(&mut self, _: &[u8], _: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn delete_range_cf(&mut self, _: &str, _: &[u8], _: &[u8]) -> Result<()> {
        unimplemented!()
    }
}

#[derive(Clone, Debug)]
pub struct RegionCacheSnapshot {
    region_id: u64,
    snapshot_ts: u64,
    // Sequence number is shared between RegionCacheEngine and disk KvEnigne to
    // provide atomic write
    sequence_number: u64,
    region_memory_engine: RegionMemoryEngine,
    engine: RegionCacheMemoryEngine,
}

impl RegionCacheSnapshot {
    pub fn new(
        engine: RegionCacheMemoryEngine,
        region_id: u64,
        read_ts: u64,
        seq_num: u64,
    ) -> Option<Self> {
        let mut core = engine.core.lock().unwrap();
        let region_meta = core.region_metas.get_mut(&region_id)?;
        if !region_meta.can_read {
            return None;
        }

        if read_ts <= region_meta.safe_ts {
            // todo(SpadeA): add metrics for it
            return None;
        }

        region_meta.snapshot_list.new_snapshot(read_ts);

        Some(RegionCacheSnapshot {
            region_id,
            snapshot_ts: read_ts,
            sequence_number: seq_num,
            region_memory_engine: core.engine.get(&region_id).unwrap().clone(),
            engine: engine.clone(),
        })
    }
}

impl Drop for RegionCacheSnapshot {
    fn drop(&mut self) {
        let mut core = self.engine.core.lock().unwrap();
        let meta = core.region_metas.get_mut(&self.region_id).unwrap();
        meta.snapshot_list.remove_snapshot(self.snapshot_ts);
    }
}

impl Snapshot for RegionCacheSnapshot {}

impl Iterable for RegionCacheSnapshot {
    type Iterator = RegionCacheIterator;

    fn iterator_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        let iter = self.region_memory_engine.data[cf_to_id(cf)].iter();
        let prefix_same_as_start = opts.prefix_same_as_start();
        let (lower_bound, upper_bound) = opts.build_bounds();
        // only support with lower/upper bound set
        if lower_bound.is_none() || upper_bound.is_none() {
            return Err(Error::BoundaryNotSet);
        }
        Ok(RegionCacheIterator {
            cf: String::from(cf),
            valid: false,
            prefix_same_as_start,
            prefix: None,
            lower_bound: lower_bound.unwrap(),
            upper_bound: upper_bound.unwrap(),
            iter,
            sequence_number: self.sequence_number,
            saved_user_key: vec![],
            pinned_value: None,
            direction: Direction::Uninit,
        })
    }
}

impl Peekable for RegionCacheSnapshot {
    type DbVector = RegionCacheDbVector;

    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Self::DbVector>> {
        self.get_value_cf_opt(opts, CF_DEFAULT, key)
    }

    fn get_value_cf_opt(
        &self,
        _: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<Self::DbVector>> {
        let seq = self.sequence_number;
        let mut iter = self.region_memory_engine.data[cf_to_id(cf)].iter();
        let seek_key = encode_seek_key(key, self.sequence_number);

        iter.seek(&seek_key);
        if iter.valid() && InternalKeyComparator::same_key(iter.key(), &seek_key) {
            return Ok(Some(RegionCacheDbVector(iter.value().clone())));
        }

        Ok(None)
    }
}

impl CfNamesExt for RegionCacheSnapshot {
    fn cf_names(&self) -> Vec<&str> {
        unimplemented!()
    }
}

impl SnapshotMiscExt for RegionCacheSnapshot {
    fn sequence_number(&self) -> u64 {
        self.sequence_number
    }
}

#[derive(Debug)]
pub struct RegionCacheDbVector(Bytes);

impl Deref for RegionCacheDbVector {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl DbVector for RegionCacheDbVector {}

impl<'a> PartialEq<&'a [u8]> for RegionCacheDbVector {
    fn eq(&self, rhs: &&[u8]) -> bool {
        self.0.as_slice() == *rhs
    }
}

#[cfg(test)]
mod tests {
    use core::ops::Range;
    use std::{iter::StepBy, ops::Deref, sync::Arc};

    use bytes::Bytes;
    use engine_traits::{
        IterOptions, Iterable, Iterator, Peekable, ReadOptions, RegionCacheEngine,
    };
    use skiplist_rs::Skiplist;

    use super::{cf_to_id, encode_key, InternalKeyComparator, RegionCacheIterator};
    use crate::RegionCacheMemoryEngine;

    #[test]
    fn test_snapshot() {
        let engine = RegionCacheMemoryEngine::default();
        engine.new_region(1);

        let verify_snapshot_count = |snapshot_ts, count| {
            let core = engine.core.lock().unwrap();
            if count > 0 {
                assert_eq!(
                    *core
                        .region_metas
                        .get(&1)
                        .unwrap()
                        .snapshot_list
                        .0
                        .get(&snapshot_ts)
                        .unwrap(),
                    count
                );
            } else {
                assert!(
                    core.region_metas
                        .get(&1)
                        .unwrap()
                        .snapshot_list
                        .0
                        .get(&snapshot_ts)
                        .is_none()
                )
            }
        };

        assert!(engine.snapshot(1, 5, u64::MAX).is_none());

        {
            let mut core = engine.core.lock().unwrap();
            core.region_metas.get_mut(&1).unwrap().can_read = true;
        }
        let s1 = engine.snapshot(1, 5, u64::MAX).unwrap();

        {
            let mut core = engine.core.lock().unwrap();
            core.region_metas.get_mut(&1).unwrap().safe_ts = 5;
        }
        assert!(engine.snapshot(1, 5, u64::MAX).is_none());
        let s2 = engine.snapshot(1, 10, u64::MAX).unwrap();

        verify_snapshot_count(5, 1);
        verify_snapshot_count(10, 1);
        let s3 = engine.snapshot(1, 10, u64::MAX).unwrap();
        verify_snapshot_count(10, 2);

        drop(s1);
        verify_snapshot_count(5, 0);
        drop(s2);
        verify_snapshot_count(10, 1);
        let s4 = engine.snapshot(1, 10, u64::MAX).unwrap();
        verify_snapshot_count(10, 2);
        drop(s4);
        verify_snapshot_count(10, 1);
        drop(s3);
        verify_snapshot_count(10, 0);
    }

    fn construct_key(i: i32) -> String {
        format!("key-{:08}", !i)
    }

    fn construct_value(i: i32) -> String {
        format!("value-{:08}", i)
    }

    fn fill_data_in_skiplist(sl: Arc<Skiplist<InternalKeyComparator>>, range: StepBy<Range<i32>>) {
        let mut seq = 1;
        for i in range {
            let key = construct_key(i);
            let val = construct_value(i);
            let key = encode_key(key.as_bytes(), seq);
            sl.put(key, Bytes::from(val));
            seq += 1;
        }
    }

    fn construct_mvcc_key(key: &str, mvcc: u64) -> String {
        format!("{}-{}", key, !mvcc)
    }

    fn put_key_val(
        sl: &Arc<Skiplist<InternalKeyComparator>>,
        key: &str,
        val: &'static str,
        mvcc: u64,
        seq: u64,
    ) {
        let key = construct_mvcc_key(key, mvcc);
        let key = encode_key(key.as_bytes(), seq);
        sl.put(key, Bytes::from(val));
    }

    fn verify_key_value(k: &[u8], v: &[u8], i: i32) {
        let key = construct_key(i);
        let val = construct_value(i);
        assert_eq!(k, key.as_bytes());
        assert_eq!(v, val.as_bytes());
    }

    fn verify_key_values(
        iter: &mut RegionCacheIterator,
        step: i32,
        mut start_idx: i32,
        end_idx: i32,
    ) {
        let forward = step > 0;
        while iter.valid().unwrap() {
            let k = iter.key();
            let val = iter.value();
            verify_key_value(k, val, start_idx);
            if forward {
                iter.next().unwrap();
            } else {
                iter.prev().unwrap();
            }
            start_idx += step;
        }

        if forward {
            assert!(start_idx - step < end_idx);
        } else {
            assert!(start_idx - step > end_idx);
        }
    }

    #[test]
    fn test_get_value() {
        let engine = RegionCacheMemoryEngine::default();
        engine.new_region(1);

        {
            let mut core = engine.core.lock().unwrap();
            core.region_metas.get_mut(&1).unwrap().can_read = true;
            core.region_metas.get_mut(&1).unwrap().safe_ts = 5;
            let sl = core.engine.get_mut(&1).unwrap().data[cf_to_id("write")].clone();
            fill_data_in_skiplist(sl, (1..100).step_by(1));
        }

        let snapshot = engine.snapshot(1, 10, u64::MAX).unwrap();
        let opts = ReadOptions::default();
        for i in 1..100 {
            let k = construct_key(i);
            let v = snapshot
                .get_value_cf_opt(&opts, "write", k.as_bytes())
                .unwrap()
                .unwrap();
            verify_key_value(k.as_bytes(), &v, i);
        }

        let k = construct_key(100);
        assert!(
            snapshot
                .get_value_cf_opt(&opts, "write", k.as_bytes())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_iterator_forawrd() {
        let engine = RegionCacheMemoryEngine::default();
        engine.new_region(1);
        let step: i32 = 2;

        {
            let mut core = engine.core.lock().unwrap();
            core.region_metas.get_mut(&1).unwrap().can_read = true;
            core.region_metas.get_mut(&1).unwrap().safe_ts = 5;
            let sl = core.engine.get_mut(&1).unwrap().data[cf_to_id("write")].clone();
            fill_data_in_skiplist(sl, (1..100).step_by(step as usize));
        }

        let mut iter_opt = IterOptions::default();
        let snapshot = engine.snapshot(1, 10, u64::MAX).unwrap();
        // boundaries are not set
        assert!(snapshot.iterator_opt("lock", iter_opt.clone()).is_err());

        let lower_bound = construct_key(1);
        let upper_bound = construct_key(100);
        iter_opt.set_upper_bound(upper_bound.as_bytes(), 0);
        iter_opt.set_lower_bound(lower_bound.as_bytes(), 0);

        let mut iter = snapshot.iterator_opt("lock", iter_opt.clone()).unwrap();
        assert!(!iter.seek_to_first().unwrap());

        let mut iter = snapshot.iterator_opt("default", iter_opt.clone()).unwrap();
        assert!(!iter.seek_to_first().unwrap());

        let mut iter = snapshot.iterator_opt("write", iter_opt.clone()).unwrap();
        iter.seek_to_first().unwrap();
        verify_key_values(&mut iter, step, 1, i32::MAX);

        // seek key that is in the skiplist
        let seek_key = construct_key(11);
        iter.seek(seek_key.as_bytes()).unwrap();
        verify_key_values(&mut iter, step, 11, i32::MAX);

        // seek key that is not in the skiplist
        let seek_key = construct_key(12);
        iter.seek(seek_key.as_bytes()).unwrap();
        verify_key_values(&mut iter, step, 13, i32::MAX);

        // with bounds
        let lower_bound = construct_key(20);
        let upper_bound = construct_key(39);
        iter_opt.set_upper_bound(upper_bound.as_bytes(), 0);
        iter_opt.set_lower_bound(lower_bound.as_bytes(), 0);
        let mut iter = snapshot.iterator_opt("write", iter_opt).unwrap();

        assert!(iter.seek_to_first().unwrap());
        verify_key_values(&mut iter, step, 21, 39);

        // seek a key that is below the lower bound is the same with seek_to_first
        let seek_key = construct_key(11);
        assert!(iter.seek(seek_key.as_bytes()).unwrap());
        verify_key_values(&mut iter, step, 21, 39);

        // seek a key that is larger or equal to upper bound won't get any key
        let seek_key = construct_key(39);
        assert!(!iter.seek(seek_key.as_bytes()).unwrap());
        assert!(!iter.valid().unwrap());

        let seek_key = construct_key(22);
        assert!(iter.seek(seek_key.as_bytes()).unwrap());
        verify_key_values(&mut iter, step, 23, 39);
    }

    #[test]
    fn test_iterator_backward() {
        let engine = RegionCacheMemoryEngine::default();
        engine.new_region(1);
        let mut step: i32 = 2;

        {
            let mut core = engine.core.lock().unwrap();
            core.region_metas.get_mut(&1).unwrap().can_read = true;
            core.region_metas.get_mut(&1).unwrap().safe_ts = 5;
            let sl = core.engine.get_mut(&1).unwrap().data[cf_to_id("write")].clone();
            fill_data_in_skiplist(sl, (1..100).step_by(step as usize));
        }
        step = -step;

        let mut iter_opt = IterOptions::default();
        let lower_bound = construct_key(1);
        let upper_bound = construct_key(100);
        iter_opt.set_upper_bound(upper_bound.as_bytes(), 0);
        iter_opt.set_lower_bound(lower_bound.as_bytes(), 0);

        let snapshot = engine.snapshot(1, 10, u64::MAX).unwrap();
        let mut iter = snapshot.iterator_opt("write", iter_opt.clone()).unwrap();
        assert!(iter.seek_to_last().unwrap());
        verify_key_values(&mut iter, step, 99, i32::MIN);

        // seek key that is in the skiplist
        let seek_key = construct_key(81);
        assert!(iter.seek_for_prev(seek_key.as_bytes()).unwrap());
        verify_key_values(&mut iter, step, 81, i32::MIN);

        // seek key that is in the skiplist
        let seek_key = construct_key(80);
        assert!(iter.seek_for_prev(seek_key.as_bytes()).unwrap());
        verify_key_values(&mut iter, step, 79, i32::MIN);

        let lower_bound = construct_key(20);
        let upper_bound = construct_key(39);
        iter_opt.set_upper_bound(upper_bound.as_bytes(), 0);
        iter_opt.set_lower_bound(lower_bound.as_bytes(), 0);
        let mut iter = snapshot.iterator_opt("write", iter_opt).unwrap();

        assert!(iter.seek_to_last().unwrap());
        verify_key_values(&mut iter, step, 37, 20);

        // seek a key that is above the upper bound is the same with seek_to_last
        let seek_key = construct_key(45);
        assert!(iter.seek_for_prev(seek_key.as_bytes()).unwrap());
        verify_key_values(&mut iter, step, 37, 20);

        // seek a key that is less than the lower bound won't get any key
        let seek_key = construct_key(19);
        assert!(!iter.seek_for_prev(seek_key.as_bytes()).unwrap());
        assert!(!iter.valid().unwrap());

        let seek_key = construct_key(36);
        assert!(iter.seek_for_prev(seek_key.as_bytes()).unwrap());
        verify_key_values(&mut iter, step, 35, 20);
    }

    #[test]
    fn test_seq_visibility() {
        let engine = RegionCacheMemoryEngine::default();
        engine.new_region(1);
        let step: i32 = 2;

        {
            let mut core = engine.core.lock().unwrap();
            core.region_metas.get_mut(&1).unwrap().can_read = true;
            core.region_metas.get_mut(&1).unwrap().safe_ts = 5;
            let sl = core.engine.get_mut(&1).unwrap().data[cf_to_id("write")].clone();

            put_key_val(&sl, "aaa", "va1", 10, 1);
            put_key_val(&sl, "aaa", "va2", 10, 3);
            put_key_val(&sl, "aaa", "va3", 10, 4);

            put_key_val(&sl, "bbb", "vb1", 10, 2);
            put_key_val(&sl, "bbb", "vb2", 10, 4);

            put_key_val(&sl, "ccc", "vc1", 10, 2);
            put_key_val(&sl, "ccc", "vc2", 10, 4);
            put_key_val(&sl, "ccc", "vc3", 10, 5);
            put_key_val(&sl, "ccc", "vc4", 10, 6);
        }

        let mut iter_opt = IterOptions::default();
        let lower_bound = "";
        let upper_bound = "z";
        iter_opt.set_upper_bound(upper_bound.as_bytes(), 0);
        iter_opt.set_lower_bound(lower_bound.as_bytes(), 0);

        // seq num 1
        {
            let snapshot = engine.snapshot(1, u64::MAX, 1).unwrap();
            let mut iter = snapshot.iterator_opt("write", iter_opt.clone()).unwrap();
            iter.seek_to_first().unwrap();
            assert_eq!(iter.value(), b"va1");
            assert!(!iter.next().unwrap());
            let key = construct_mvcc_key("aaa", 10);
            assert_eq!(
                snapshot
                    .get_value_cf("write", key.as_bytes())
                    .unwrap()
                    .unwrap()
                    .deref(),
                "va1".as_bytes()
            );
            assert!(iter.seek(key.as_bytes()).unwrap());
            assert_eq!(iter.value(), "va1".as_bytes());

            let key = construct_mvcc_key("bbb", 10);
            assert!(
                snapshot
                    .get_value_cf("write", key.as_bytes())
                    .unwrap()
                    .is_none()
            );
            assert!(!iter.seek(key.as_bytes()).unwrap());

            let key = construct_mvcc_key("ccc", 10);
            assert!(
                snapshot
                    .get_value_cf("write", key.as_bytes())
                    .unwrap()
                    .is_none()
            );
            assert!(!iter.seek(key.as_bytes()).unwrap());
        }

        // seq num 2
        {
            let snapshot = engine.snapshot(1, u64::MAX, 2).unwrap();
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
            let snapshot = engine.snapshot(1, u64::MAX, 5).unwrap();
            let mut iter = snapshot.iterator_opt("write", iter_opt.clone()).unwrap();
            iter.seek_to_first().unwrap();
            assert_eq!(iter.value(), b"va3");
            iter.next().unwrap();
            assert_eq!(iter.value(), b"vb2");
            iter.next().unwrap();
            assert_eq!(iter.value(), b"vc3");
            assert!(!iter.next().unwrap());
        }

        // seq num 6
        {
            let snapshot = engine.snapshot(1, u64::MAX, 6).unwrap();
            let mut iter = snapshot.iterator_opt("write", iter_opt.clone()).unwrap();
            iter.seek_to_first().unwrap();
            assert_eq!(iter.value(), b"va3");
            iter.next().unwrap();
            assert_eq!(iter.value(), b"vb2");
            iter.next().unwrap();
            assert_eq!(iter.value(), b"vc4");
            assert!(!iter.next().unwrap());

            let key = construct_mvcc_key("aaa", 10);
            assert_eq!(
                snapshot
                    .get_value_cf("write", key.as_bytes())
                    .unwrap()
                    .unwrap()
                    .deref(),
                "va3".as_bytes()
            );
            assert!(iter.seek(key.as_bytes()).unwrap());
            assert_eq!(iter.value(), "va3".as_bytes());

            let key = construct_mvcc_key("bbb", 10);
            assert_eq!(
                snapshot
                    .get_value_cf("write", key.as_bytes())
                    .unwrap()
                    .unwrap()
                    .deref(),
                "vb2".as_bytes()
            );
            assert!(iter.seek(key.as_bytes()).unwrap());
            assert_eq!(iter.value(), "vb2".as_bytes());

            let key = construct_mvcc_key("ccc", 10);
            assert_eq!(
                snapshot
                    .get_value_cf("write", key.as_bytes())
                    .unwrap()
                    .unwrap()
                    .deref(),
                "vc4".as_bytes()
            );
            assert!(iter.seek(key.as_bytes()).unwrap());
            assert_eq!(iter.value(), "vc4".as_bytes());
        }
    }

    #[test]
    fn test_seq_visibility_backward() {
        let engine = RegionCacheMemoryEngine::default();
        engine.new_region(1);
        let step: i32 = 2;

        {
            let mut core = engine.core.lock().unwrap();
            core.region_metas.get_mut(&1).unwrap().can_read = true;
            core.region_metas.get_mut(&1).unwrap().safe_ts = 5;
            let sl = core.engine.get_mut(&1).unwrap().data[cf_to_id("write")].clone();

            put_key_val(&sl, "aaa", "va1", 10, 2);
            put_key_val(&sl, "aaa", "va2", 10, 4);
            put_key_val(&sl, "aaa", "va3", 10, 5);
            put_key_val(&sl, "aaa", "va4", 10, 6);

            put_key_val(&sl, "bbb", "vb1", 10, 2);
            put_key_val(&sl, "bbb", "vb2", 10, 4);

            put_key_val(&sl, "ccc", "vc1", 10, 1);
            put_key_val(&sl, "ccc", "vc2", 10, 3);
            put_key_val(&sl, "ccc", "vc3", 10, 4);
        }

        let mut iter_opt = IterOptions::default();
        let lower_bound = "";
        let upper_bound = "z";
        iter_opt.set_upper_bound(upper_bound.as_bytes(), 0);
        iter_opt.set_lower_bound(lower_bound.as_bytes(), 0);

        // seq num 1
        {
            let snapshot = engine.snapshot(1, u64::MAX, 1).unwrap();
            let mut iter = snapshot.iterator_opt("write", iter_opt.clone()).unwrap();
            iter.seek_to_last().unwrap();
            assert_eq!(iter.value(), b"vc1");
            assert!(!iter.prev().unwrap());
            let key = construct_mvcc_key("aaa", 10);
            assert!(!iter.seek_for_prev(key.as_bytes()).unwrap());

            let key = construct_mvcc_key("bbb", 10);
            assert!(!iter.seek_for_prev(key.as_bytes()).unwrap());

            let key = construct_mvcc_key("ccc", 10);
            assert!(iter.seek_for_prev(key.as_bytes()).unwrap());
            assert_eq!(iter.value(), "vc1".as_bytes());
        }

        // seq num 2
        {
            let snapshot = engine.snapshot(1, u64::MAX, 2).unwrap();
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
            let snapshot = engine.snapshot(1, u64::MAX, 5).unwrap();
            let mut iter = snapshot.iterator_opt("write", iter_opt.clone()).unwrap();
            iter.seek_to_last().unwrap();
            assert_eq!(iter.value(), b"vc3");
            iter.prev().unwrap();
            assert_eq!(iter.value(), b"vb2");
            iter.prev().unwrap();
            assert_eq!(iter.value(), b"va3");
            assert!(!iter.prev().unwrap());
        }

        // seq num 6
        {
            let snapshot = engine.snapshot(1, u64::MAX, 6).unwrap();
            let mut iter = snapshot.iterator_opt("write", iter_opt.clone()).unwrap();
            iter.seek_to_last().unwrap();
            assert_eq!(iter.value(), b"vc3");
            iter.prev().unwrap();
            assert_eq!(iter.value(), b"vb2");
            iter.prev().unwrap();
            assert_eq!(iter.value(), b"va4");
            assert!(!iter.prev().unwrap());

            let key = construct_mvcc_key("ccc", 10);
            assert!(iter.seek_for_prev(key.as_bytes()).unwrap());
            assert_eq!(iter.value(), "vc3".as_bytes());

            let key = construct_mvcc_key("bbb", 10);
            assert!(iter.seek_for_prev(key.as_bytes()).unwrap());
            assert_eq!(iter.value(), "vb2".as_bytes());

            let key = construct_mvcc_key("aaa", 10);
            assert!(iter.seek_for_prev(key.as_bytes()).unwrap());
            assert_eq!(iter.value(), "va4".as_bytes());
        }
    }
}
