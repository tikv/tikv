// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use core::slice::SlicePattern;
use std::{
    collections::BTreeMap,
    fmt::{self, Debug},
    ops::Deref,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use collections::{HashMap, HashSet};
use engine_rocks::{raw::SliceTransform, util::FixedSuffixSliceTransform};
use engine_traits::{
    BatchSplit, CfNamesExt, DbVector, Error, IterOptions, Iterable, Iterator, Mutable, Peekable,
    ReadOptions, RegionCacheEngine, Result, Snapshot, SnapshotMiscExt, WriteBatch, WriteBatchExt,
    WriteOptions, CF_DEFAULT, CF_LOCK, CF_WRITE,
};
use skiplist_rs::{AllocationRecorder, IterRef, MemoryLimiter, Skiplist};
use uuid::Uuid;

use crate::keys::{
    decode_key, encode_seek_key, encode_seek_key_in_place, InternalKey, InternalKeyComparator,
    ValueType, VALUE_TYPE_FOR_SEEK, VALUE_TYPE_FOR_SEEK_FOR_PREV,
};

fn cf_to_id(cf: &str) -> usize {
    match cf {
        CF_DEFAULT => 0,
        CF_LOCK => 1,
        CF_WRITE => 2,
        _ => panic!("unrecognized cf {}", cf),
    }
}

// todo: implement memory limiter
#[derive(Clone, Default)]
pub struct GlobalMemoryLimiter {
    recorder: Arc<Mutex<HashMap<usize, usize>>>,
}

impl MemoryLimiter for GlobalMemoryLimiter {
    fn acquire(&self, n: usize) -> bool {
        true
    }

    fn mem_usage(&self) -> usize {
        0
    }

    fn reclaim(&self, n: usize) {}
}

impl AllocationRecorder for GlobalMemoryLimiter {
    fn alloc(&self, addr: usize, size: usize) {
        let mut recorder = self.recorder.lock().unwrap();
        assert!(!recorder.contains_key(&addr));
        recorder.insert(addr, size);
    }

    fn free(&self, addr: usize, size: usize) {
        let mut recorder = self.recorder.lock().unwrap();
        assert_eq!(recorder.remove(&addr).unwrap(), size);
    }
}

/// RegionMemoryEngine stores data for a specific cached region
///
/// todo: The skiplist used here currently is for test purpose. Replace it
/// with a formal implementation.
#[derive(Clone)]
pub struct RegionMemoryEngine {
    data: [Arc<Skiplist<InternalKeyComparator, GlobalMemoryLimiter>>; 3],
    global_limiter: GlobalMemoryLimiter,
}

impl RegionMemoryEngine {
    pub fn new(global_limiter: GlobalMemoryLimiter) -> Self {
        RegionMemoryEngine {
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
            global_limiter,
        }
    }

    fn batch_split(&self, keys: &Vec<Vec<u8>>) -> Vec<RegionMemoryEngine> {
        let skiplists: Vec<_> = self
            .data
            .iter()
            .map(|s| s.new_headers_to_list(keys))
            .collect();
        assert_eq!(skiplists.len(), 3);
        let mut memory_engines = vec![];
        for i in 0..keys.len() {
            memory_engines.push(RegionMemoryEngine {
                data: [
                    Arc::new(skiplists[0][i].clone()),
                    Arc::new(skiplists[1][i].clone()),
                    Arc::new(skiplists[2][i].clone()),
                ],
                global_limiter: self.global_limiter.clone(),
            });
        }

        memory_engines
    }

    fn trim(&self, end_key: &[u8]) {
        self.data.iter().map(|s| s.cut(end_key));
    }
}

impl Debug for RegionMemoryEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Region Memory Engine")
    }
}

// read_ts -> ref_count
#[derive(Default, Clone)]
struct SnapshotList(BTreeMap<(u64, Uuid), u64>);

impl SnapshotList {
    fn new_snapshot(&mut self, read_ts: u64, uuid: Uuid) {
        // snapshot with this ts may be granted before
        let count = self.0.get(&(read_ts, uuid)).unwrap_or(&0) + 1;
        self.0.insert((read_ts, uuid), count);
    }

    fn remove_snapshot(&mut self, read_ts: u64, uuid: Uuid) -> bool {
        let Some(count) = self.0.get_mut(&(read_ts, uuid)) else {
            return false;
        };
        assert!(*count >= 1);
        if *count == 1 {
            self.0.remove(&(read_ts, uuid)).unwrap();
        } else {
            *count -= 1;
        }
        return true;
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
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
    evicted: bool,
    juniority: u64,
    uuid: Uuid,
}

impl RegionMemoryMeta {
    pub fn set_can_read(&mut self, can_read: bool) {
        self.can_read = can_read;
    }

    pub fn set_safe_ts(&mut self, safe_ts: u64) {
        self.safe_ts = safe_ts;
    }
}

// SnapshotsOnSplit records the snapshots at the time of batch split and the
// splitted RegionMemoryEngine.
//
// Say A1 is splitted into A2, B, and C.
// After the split, A2, B, and C is responsible for dropping elements that may
// still be accessed by A1 due to the snapshots acquired before split. So we
// records these snapshots here and hold clone of A1, B, and C to avoid they
// dropping elements. After the snapshots are all freed, we free the clones.
pub struct SnapshotRecordsOnSplit {
    uuid: Uuid,
    juniority: u64,
    snapshots: SnapshotList,
    child_uuids: Vec<(u64, Uuid)>,
    region_memory_engines: Vec<(u64, RegionMemoryEngine)>,
    split_keys: Vec<Vec<u8>>,
}

#[derive(Default)]
pub struct RegionsSnapshotRecordsOnSplit(HashMap<u64, HashMap<Uuid, SnapshotRecordsOnSplit>>);

impl RegionsSnapshotRecordsOnSplit {
    fn drop_recoded_snapshot(&mut self, snapshot: &RegionCacheSnapshot) {
        let child_uuids = {
            let snapshot_records = self.0.get_mut(&snapshot.region_id).unwrap();
            let record = snapshot_records.get_mut(&snapshot.uuid).unwrap();
            assert!(
                record
                    .snapshots
                    .remove_snapshot(snapshot.snapshot_ts, snapshot.uuid)
            );

            if !record.snapshots.is_empty() {
                return;
            }

            // all recorded snapshots have been dropped, so we can do cut
            assert_eq!(Arc::strong_count(&snapshot.region_memory_engine.data[0]), 1);
            snapshot.region_memory_engine.trim(&record.split_keys[0]);
            for i in 0..record.region_memory_engines.len() - 1 {
                record.region_memory_engines[i]
                    .1
                    .trim(&record.split_keys[i + 1]);
            }

            if record.juniority == 0 {
                record.child_uuids.to_owned()
            } else {
                return;
            }
        };

        for (id, uuid) in child_uuids {
            self.may_clean_up_child(id, uuid);
        }

        self.0
            .get_mut(&snapshot.region_id)
            .unwrap()
            .remove(&snapshot.uuid)
            .unwrap();
    }

    fn may_clean_up_child(&mut self, region_id: u64, child_uuid: Uuid) {
        let Some(snapshot_records) = self.0.get_mut(&region_id) else { return };
        let Some(record) = snapshot_records.get_mut(&child_uuid) else { return };
        assert_eq!(record.juniority, 1);
        record.juniority -= 1;

        let child_uuids = {
            if record.snapshots.is_empty() {
                record.child_uuids.to_owned()
            } else {
                return;
            }
        };

        for (id, uuid) in child_uuids {
            self.may_clean_up_child(id, uuid);
        }

        self.0
            .get_mut(&region_id)
            .unwrap()
            .remove(&child_uuid)
            .unwrap();
    }
}

#[derive(Default)]
pub struct RegionCacheMemoryEngineCore {
    engine: HashMap<u64, RegionMemoryEngine>,
    region_metas: HashMap<u64, RegionMemoryMeta>,
    // The snapshot records of the skiplist when the batch split is executed. The records avoid us
    // freeing the elements that the snapshots may read.
    // There's possible that before the previous recorded snapshots are freed, a new batch split
    // command is executed. So we use a uuid to distinguish them.
    snapshot_records_on_split: RegionsSnapshotRecordsOnSplit,
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
    memory_limiter: GlobalMemoryLimiter,
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
        core.engine.insert(
            region_id,
            RegionMemoryEngine::new(self.memory_limiter.clone()),
        );
        core.region_metas
            .insert(region_id, RegionMemoryMeta::default());
    }

    pub fn evict(&self, region_id: u64) {
        let mut core = self.core.lock().unwrap();
        assert!(core.engine.get(&region_id).is_some());
        assert!(core.region_metas.get(&region_id).is_some());
        let mut meta = core.region_metas.get_mut(&region_id).unwrap();
        meta.can_read = false;
        meta.evicted = true;

        let eviction = if meta.snapshot_list.is_empty() {
            core.region_metas.remove(&region_id);
            Some(core.engine.remove(&region_id).unwrap())
        } else {
            None
        };

        // unlock before drop the skipplist
        drop(core);
        drop(eviction);
    }
}

impl RegionCacheEngine for RegionCacheMemoryEngine {
    type Snapshot = RegionCacheSnapshot;

    // todo(SpadeA): add sequence number logic
    fn snapshot(&self, region_id: u64, read_ts: u64, seq_num: u64) -> Option<Self::Snapshot> {
        RegionCacheSnapshot::new(self.clone(), region_id, read_ts, seq_num)
    }
}

#[derive(Debug)]
pub struct MemorySplitResult {
    split_memory_engines: Vec<(u64, RegionMemoryEngine)>,
    split_keys: Vec<Vec<u8>>,
}

impl BatchSplit for RegionCacheMemoryEngine {
    type SplitResult = MemorySplitResult;

    // "splitted_region_ids" include all regions' id including the dervied where
    // the derived region id cloud be at front and end depending on whether
    // right_derive is enabled.
    //
    // "keys" should be [region_start_key, split keys...]
    fn batch_split(
        &self,
        region_id: u64,
        splitted_region_id: Vec<u64>,
        keys: Vec<Vec<u8>>,
    ) -> Self::SplitResult {
        let core = self.core.lock().unwrap();
        let meta = core.region_metas.get(&region_id).unwrap();
        let memory_engine = core.engine.get(&region_id).unwrap();

        // split key does not have mvcc suffix, we need to add it for key comparison
        let keys = keys
            .into_iter()
            .map(|k| encode_seek_key_in_place(k, u64::MAX, VALUE_TYPE_FOR_SEEK))
            .collect();
        let split_memory_engines: Vec<_> = splitted_region_id
            .into_iter()
            .zip(memory_engine.batch_split(&keys).into_iter())
            .collect();
        MemorySplitResult {
            split_memory_engines,
            split_keys: keys,
        }
    }

    fn on_batch_split(&self, region_id: u64, split_result: Self::SplitResult) {
        let mut core = self.core.lock().unwrap();
        let mut meta = core.region_metas.get_mut(&region_id).unwrap();
        let record_snapshots = meta.snapshot_list.clone();
        let parent_juniority = meta.juniority;
        let parent_uuid = meta.uuid;
        meta.juniority += 1;

        let mut old_engine = None;
        let mut child_uuids = vec![];
        for (id, e) in &split_result.split_memory_engines {
            let uuid = Uuid::new_v4();
            assert!(core.engine.get(id).is_none());
            assert!(core.region_metas.get(id).is_none());
            core.engine.insert(*id, e.clone());
            let mut meta = RegionMemoryMeta::default();
            meta.can_read = true;
            meta.juniority = parent_juniority + 1;
            meta.uuid = uuid;
            if let Some(e) = core.region_metas.insert(*id, meta) {
                assert!(old_engine.is_none());
                old_engine = Some(e);
            }
            child_uuids.push((*id, uuid));
        }

        if record_snapshots.is_empty() && parent_juniority == 0 {
            for (id, _) in &split_result.split_memory_engines {
                core.region_metas.get_mut(id).unwrap().juniority -= 1;
            }
            return;
        }

        let records = SnapshotRecordsOnSplit {
            uuid: parent_uuid,
            child_uuids,
            juniority: parent_juniority,
            snapshots: record_snapshots,
            split_keys: split_result.split_keys,
            region_memory_engines: split_result.split_memory_engines,
        };

        let region_records = core
            .snapshot_records_on_split
            .0
            .entry(region_id)
            .or_default();
        region_records.insert(parent_uuid, records);
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

impl Iterator for RegionCacheIterator {
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
    uuid: Uuid,
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

        region_meta
            .snapshot_list
            .new_snapshot(read_ts, region_meta.uuid);

        Some(RegionCacheSnapshot {
            region_id,
            uuid: region_meta.uuid,
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

        // if uuid is not equal, it means the current region is a derived region in the
        // split and the snapshot is granted before the split
        if self.uuid == meta.uuid {
            assert!(
                meta.snapshot_list
                    .remove_snapshot(self.snapshot_ts, self.uuid)
            );
            return;
        }

        core.snapshot_records_on_split.drop_recoded_snapshot(&self);

        let meta = core.region_metas.get(&self.region_id).unwrap();
        let eviction = if meta.snapshot_list.is_empty() && meta.evicted {
            core.region_metas.remove(&self.region_id);
            Some(core.engine.remove(&self.region_id).unwrap())
        } else {
            None
        };

        // unlock before drop the skipplist
        drop(core);
    }
}

impl Snapshot for RegionCacheSnapshot {}

impl Iterable for RegionCacheSnapshot {
    type Iterator = RegionCacheIterator;

    fn iterator_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        let iter = self.region_memory_engine.data[cf_to_id(cf)].iter();
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

        Ok(RegionCacheIterator {
            cf: String::from(cf),
            valid: false,
            prefix: None,
            lower_bound: lower_bound.unwrap(),
            upper_bound: upper_bound.unwrap(),
            iter,
            sequence_number: self.sequence_number,
            saved_user_key: vec![],
            saved_value: None,
            direction: Direction::Uninit,
            prefix_extractor,
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
        let seek_key = encode_seek_key(key, self.sequence_number, VALUE_TYPE_FOR_SEEK);

        iter.seek(&seek_key);
        if !iter.valid() {
            return Ok(None);
        }

        match decode_key(iter.key()) {
            InternalKey {
                user_key,
                v_type: ValueType::Value,
                ..
            } if user_key == key => Ok(Some(RegionCacheDbVector(iter.value().clone()))),
            _ => Ok(None),
        }
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
    use std::{iter, iter::StepBy, ops::Deref, sync::Arc};

    use bytes::{BufMut, Bytes};
    use engine_traits::{
        BatchSplit, IterOptions, Iterable, Iterator, Peekable, ReadOptions, RegionCacheEngine,
    };
    use skiplist_rs::Skiplist;

    use super::{cf_to_id, GlobalMemoryLimiter, RegionCacheIterator};
    use crate::{
        keys::{encode_key, InternalKeyComparator, ValueType},
        RegionCacheMemoryEngine,
    };

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
        let uuid = s2.uuid;

        verify_snapshot_count((5, uuid), 1);
        verify_snapshot_count((10, uuid), 1);
        let s3 = engine.snapshot(1, 10, u64::MAX).unwrap();
        verify_snapshot_count((10, uuid), 2);

        drop(s1);
        verify_snapshot_count((5, uuid), 0);
        drop(s2);
        verify_snapshot_count((10, uuid), 1);
        let s4 = engine.snapshot(1, 10, u64::MAX).unwrap();
        verify_snapshot_count((10, uuid), 2);
        drop(s4);
        verify_snapshot_count((10, uuid), 1);
        drop(s3);
        verify_snapshot_count((10, uuid), 0);
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
        iter: &mut RegionCacheIterator,
        key_range: I,
        mvcc_range: J,
        foward: bool,
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
        assert!(!iter.valid().unwrap());
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
            fill_data_in_skiplist(sl.clone(), (1..10).step_by(1), 1..50, 1);
            // k1 is deleted at seq_num 150 while k49 is deleted at seq num 101
            delete_data_in_skiplist(sl, (1..10).step_by(1), 1..50, 100);
        }

        let opts = ReadOptions::default();
        {
            let snapshot = engine.snapshot(1, 10, 60).unwrap();
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
            let snapshot = engine.snapshot(1, 10, u64::MAX).unwrap();
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
            let snapshot = engine.snapshot(1, 10, 105).unwrap();
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
        let engine = RegionCacheMemoryEngine::default();
        engine.new_region(1);
        let step: i32 = 2;

        {
            let mut core = engine.core.lock().unwrap();
            core.region_metas.get_mut(&1).unwrap().can_read = true;
            core.region_metas.get_mut(&1).unwrap().safe_ts = 5;
            let sl = core.engine.get_mut(&1).unwrap().data[cf_to_id("write")].clone();
            fill_data_in_skiplist(sl.clone(), (1..100).step_by(step as usize), 1..10, 1);
            delete_data_in_skiplist(sl, (1..100).step_by(step as usize), 1..10, 200);
        }

        let mut iter_opt = IterOptions::default();
        let snapshot = engine.snapshot(1, 10, u64::MAX).unwrap();
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
            let snapshot = engine.snapshot(1, 100, 150).unwrap();
            let mut iter = snapshot.iterator_opt("write", iter_opt.clone()).unwrap();
            iter.seek_to_first().unwrap();
            verify_key_values(
                &mut iter,
                (1..100).step_by(step as usize),
                (1..10).rev(),
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
            );

            // seek key that is not in the skiplist
            let seek_key = construct_key(12, u64::MAX);
            iter.seek(&seek_key).unwrap();
            verify_key_values(
                &mut iter,
                (13..100).step_by(step as usize),
                (1..10).rev(),
                true,
            );
        }

        // Not restricted by bounds, some deletions (seq_num 230)
        {
            let snapshot = engine.snapshot(1, 10, 230).unwrap();
            let mut iter = snapshot.iterator_opt("write", iter_opt.clone()).unwrap();
            iter.seek_to_first().unwrap();
            verify_key_values(
                &mut iter,
                (63..100).step_by(step as usize),
                (1..10).rev(),
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
            let snapshot = engine.snapshot(1, 10, 150).unwrap();
            let mut iter = snapshot.iterator_opt("write", iter_opt.clone()).unwrap();

            assert!(iter.seek_to_first().unwrap());
            verify_key_values(
                &mut iter,
                (21..40).step_by(step as usize),
                (1..10).rev(),
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
            );
        }

        // with bounds, some deletions (seq_num 215)
        {
            let snapshot = engine.snapshot(1, 10, 215).unwrap();
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
        let engine = RegionCacheMemoryEngine::default();
        engine.new_region(1);
        let step: i32 = 2;

        {
            let mut core = engine.core.lock().unwrap();
            core.region_metas.get_mut(&1).unwrap().can_read = true;
            core.region_metas.get_mut(&1).unwrap().safe_ts = 5;
            let sl = core.engine.get_mut(&1).unwrap().data[cf_to_id("write")].clone();
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
            let snapshot = engine.snapshot(1, 10, 150).unwrap();
            let mut iter = snapshot.iterator_opt("write", iter_opt.clone()).unwrap();
            assert!(iter.seek_to_last().unwrap());
            verify_key_values(
                &mut iter,
                (1..100).step_by(step as usize).rev(),
                1..10,
                false,
            );

            // seek key that is in the skiplist
            let seek_key = construct_key(81, 0);
            assert!(iter.seek_for_prev(&seek_key).unwrap());
            verify_key_values(
                &mut iter,
                (1..82).step_by(step as usize).rev(),
                1..10,
                false,
            );

            // seek key that is in the skiplist
            let seek_key = construct_key(80, 0);
            assert!(iter.seek_for_prev(&seek_key).unwrap());
            verify_key_values(
                &mut iter,
                (1..80).step_by(step as usize).rev(),
                1..10,
                false,
            );
        }

        let lower_bound = construct_user_key(21);
        let upper_bound = construct_user_key(39);
        iter_opt.set_upper_bound(&upper_bound, 0);
        iter_opt.set_lower_bound(&lower_bound, 0);
        {
            let snapshot = engine.snapshot(1, 10, 150).unwrap();
            let mut iter = snapshot.iterator_opt("write", iter_opt).unwrap();

            assert!(iter.seek_to_last().unwrap());
            verify_key_values(
                &mut iter,
                (21..38).step_by(step as usize).rev(),
                1..10,
                false,
            );

            // seek a key that is above the upper bound is the same with seek_to_last
            let seek_key = construct_key(40, 0);
            assert!(iter.seek_for_prev(&seek_key).unwrap());
            verify_key_values(
                &mut iter,
                (21..38).step_by(step as usize).rev(),
                1..10,
                false,
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
            );
        }
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
            let snapshot = engine.snapshot(1, u64::MAX, 1).unwrap();
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
        let engine = RegionCacheMemoryEngine::default();
        engine.new_region(1);

        {
            let mut core = engine.core.lock().unwrap();
            core.region_metas.get_mut(&1).unwrap().can_read = true;
            core.region_metas.get_mut(&1).unwrap().safe_ts = 5;
            let sl = core.engine.get_mut(&1).unwrap().data[cf_to_id("write")].clone();

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
            let snapshot = engine.snapshot(1, u64::MAX, 1).unwrap();
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

        // backward, all put
        {
            let engine = RegionCacheMemoryEngine::default();
            engine.new_region(1);
            let sl = {
                let mut core = engine.core.lock().unwrap();
                core.region_metas.get_mut(&1).unwrap().can_read = true;
                core.region_metas.get_mut(&1).unwrap().safe_ts = 5;
                core.engine.get_mut(&1).unwrap().data[cf_to_id("write")].clone()
            };

            let mut s = 1;
            for seq in 2..50 {
                put_key_val(&sl, "a", "val", 10, s + 1);
                for i in 2..50 {
                    let v = construct_value(i, i);
                    put_key_val(&sl, "b", v.as_str(), 10, s + i);
                }

                let snapshot = engine.snapshot(1, 10, s + seq).unwrap();
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
            let engine = RegionCacheMemoryEngine::default();
            engine.new_region(1);
            let sl = {
                let mut core = engine.core.lock().unwrap();
                core.region_metas.get_mut(&1).unwrap().can_read = true;
                core.region_metas.get_mut(&1).unwrap().safe_ts = 5;
                core.engine.get_mut(&1).unwrap().data[cf_to_id("write")].clone()
            };

            let mut s = 1;
            for seq in 2..50 {
                put_key_val(&sl, "a", "val", 10, s + 1);
                for i in 2..50 {
                    delete_key(&sl, "b", 10, s + i);
                }

                let snapshot = engine.snapshot(1, 10, s + seq).unwrap();
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
            let engine = RegionCacheMemoryEngine::default();
            engine.new_region(1);
            let sl = {
                let mut core = engine.core.lock().unwrap();
                core.region_metas.get_mut(&1).unwrap().can_read = true;
                core.region_metas.get_mut(&1).unwrap().safe_ts = 5;
                core.engine.get_mut(&1).unwrap().data[cf_to_id("write")].clone()
            };
            put_key_val(&sl, "a", "val", 10, 1);
            for i in 2..50 {
                delete_key(&sl, "b", 10, i);
            }
            let v = construct_value(50, 50);
            put_key_val(&sl, "b", v.as_str(), 10, 50);
            let snapshot = engine.snapshot(1, 10, 50).unwrap();
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
            let engine = RegionCacheMemoryEngine::default();
            engine.new_region(1);
            let sl = {
                let mut core = engine.core.lock().unwrap();
                core.region_metas.get_mut(&1).unwrap().can_read = true;
                core.region_metas.get_mut(&1).unwrap().safe_ts = 5;
                core.engine.get_mut(&1).unwrap().data[cf_to_id("write")].clone()
            };
            let mut s = 1;
            for seq in 2..50 {
                for i in 2..50 {
                    delete_key(&sl, "b", 10, s + i);
                }
                let v = construct_value(50, 50);
                put_key_val(&sl, "b", v.as_str(), 10, s + 50);

                let snapshot = engine.snapshot(1, 10, s + seq).unwrap();
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
        let engine = RegionCacheMemoryEngine::default();
        engine.new_region(1);

        {
            let mut core = engine.core.lock().unwrap();
            core.region_metas.get_mut(&1).unwrap().can_read = true;
            core.region_metas.get_mut(&1).unwrap().safe_ts = 5;
            let sl = core.engine.get_mut(&1).unwrap().data[cf_to_id("write")].clone();

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
        let snapshot = engine.snapshot(1, u64::MAX, u64::MAX).unwrap();
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
    fn test_batch_split() {
        for _ in 0..2000 {
            let engine = RegionCacheMemoryEngine::default();
            engine.new_region(1);

            {
                let mut core = engine.core.lock().unwrap();
                core.region_metas.get_mut(&1).unwrap().can_read = true;
                core.region_metas.get_mut(&1).unwrap().safe_ts = 5;
                let s1 = core.engine.get_mut(&1).unwrap().data[cf_to_id("write")].clone();

                for i in 0..90 {
                    let user_key = construct_key(i, 10);
                    let internal_key = encode_key(&user_key, 10, ValueType::Value);
                    let v = format!("v{:04}{:04}", i, 10);
                    s1.put(internal_key.clone(), v.clone());
                }
            }

            let s1 = engine.snapshot(1, 10, 10).unwrap();

            let split_keys = vec![
                construct_user_key(0),
                construct_user_key(30),
                construct_user_key(60),
            ];
            let sr = engine.batch_split(1, vec![1, 2, 3], split_keys);
            engine.on_batch_split(1, sr);

            // s1 should be the last clone of the original region 1
            assert_eq!(Arc::strong_count(&s1.region_memory_engine.data[0]), 1);
            drop(s1);

            let mut iter_opt = IterOptions::default();
            let lower = construct_user_key(30);
            let upper = construct_user_key(60);
            iter_opt.set_lower_bound(&lower, 0);
            iter_opt.set_upper_bound(&upper, 0);
            {
                let s = engine.snapshot(2, 10, 10).unwrap();
                let mut iter = s.iterator_opt("write", iter_opt.clone()).unwrap();
                iter.seek_to_first().unwrap();
                for i in 30..60 {
                    let user_key = construct_key(i, 10);
                    let k = iter.key();
                    assert_eq!(k, &user_key);
                    iter.next().unwrap();
                }
                assert!(!iter.valid().unwrap());

                assert_eq!(Arc::strong_count(&s.region_memory_engine.data[0]), 2);
                engine.evict(2);
            }
            assert!(engine.snapshot(2, 10, 10).is_none());

            let lower = construct_user_key(0);
            let upper = construct_user_key(30);
            iter_opt.set_lower_bound(&lower, 0);
            iter_opt.set_upper_bound(&upper, 0);
            {
                let s = engine.snapshot(1, 10, 10).unwrap();
                let mut iter = s.iterator_opt("write", iter_opt.clone()).unwrap();
                if !iter.seek_to_first().unwrap() {
                    assert!(iter.seek_to_first().unwrap());
                }
                for i in 0..30 {
                    let user_key = construct_key(i, 10);
                    let k = iter.key();
                    assert_eq!(k, &user_key);
                    iter.next().unwrap();
                }
                assert!(!iter.valid().unwrap());
                engine.evict(1);
            }

            let lower = construct_user_key(60);
            let upper = construct_user_key(90);
            iter_opt.set_lower_bound(&lower, 0);
            iter_opt.set_upper_bound(&upper, 0);
            {
                let s = engine.snapshot(3, 10, 10).unwrap();
                let mut iter = s.iterator_opt("write", iter_opt.clone()).unwrap();
                iter.seek_to_first().unwrap();
                for i in 60..90 {
                    let user_key = construct_key(i, 10);
                    let k = iter.key();
                    assert_eq!(k, &user_key);
                    iter.next().unwrap();
                }
                assert!(!iter.valid().unwrap());
            }

            // split again
            let split_keys = vec![construct_user_key(60), construct_user_key(80)];
            let split_res = engine.batch_split(3, vec![4, 3], split_keys);
            engine.on_batch_split(3, split_res);
            let lower = construct_user_key(80);
            let upper = construct_user_key(90);
            iter_opt.set_lower_bound(&lower, 0);
            iter_opt.set_upper_bound(&upper, 0);
            {
                let s = engine.snapshot(3, 10, 10).unwrap();
                let mut iter = s.iterator_opt("write", iter_opt.clone()).unwrap();
                iter.seek_to_first().unwrap();
                for i in 80..90 {
                    let user_key = construct_key(i, 10);
                    let k = iter.key();
                    assert_eq!(k, &user_key);
                    iter.next().unwrap();
                }
                assert!(!iter.valid().unwrap());
            }

            let lower = construct_user_key(60);
            let upper = construct_user_key(80);
            iter_opt.set_lower_bound(&lower, 0);
            iter_opt.set_upper_bound(&upper, 0);
            {
                let s3 = engine.snapshot(4, 10, 10).unwrap();
                let mut iter = s3.iterator_opt("write", iter_opt.clone()).unwrap();
                iter.seek_to_first().unwrap();
                for i in 60..80 {
                    let user_key = construct_key(i, 10);
                    let k = iter.key();
                    assert_eq!(k, &user_key);
                    iter.next().unwrap();
                }
                assert!(!iter.valid().unwrap());
            }
        }
    }
}
