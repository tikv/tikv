// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use core::slice::SlicePattern;
use std::{
    collections::BTreeMap,
    fmt::{self, Debug},
    ops::Deref,
    result,
    sync::Arc,
};

use bytes::Bytes;
use crossbeam::epoch::{self, default_collector, Guard};
use engine_rocks::{raw::SliceTransform, util::FixedSuffixSliceTransform, RocksEngine};
use engine_traits::{
    CacheRange, CfNamesExt, DbVector, Error, FailedReason, IterOptions, Iterable, Iterator,
    KvEngine, Peekable, RangeCacheEngine, ReadOptions, Result, Snapshot, SnapshotMiscExt,
    CF_DEFAULT, CF_LOCK, CF_WRITE, DATA_CFS,
};
use parking_lot::{lock_api::RwLockUpgradableReadGuard, RwLock, RwLockWriteGuard};
use skiplist_rs::{base::OwnedIter, SkipList};
use slog_global::error;
use tikv_util::box_err;

use crate::{
    background::{BackgroundTask, BgWorkManager, PdRangeHintService},
    keys::{
        decode_key, encode_key_for_eviction, encode_seek_for_prev_key, encode_seek_key,
        InternalBytes, InternalKey, ValueType,
    },
    memory_controller::MemoryController,
    range_manager::{LoadFailedReason, RangeCacheStatus, RangeManager},
    write_batch::{group_write_batch_entries, RangeCacheWriteBatchEntry},
    EngineConfig,
};

pub(crate) fn cf_to_id(cf: &str) -> usize {
    match cf {
        CF_DEFAULT => 0,
        CF_LOCK => 1,
        CF_WRITE => 2,
        _ => panic!("unrecognized cf {}", cf),
    }
}

// A wrapper for skiplist to provide some check and clean up worker
#[derive(Clone)]
pub struct SkiplistHandle(Arc<SkipList<InternalBytes, InternalBytes>>);

impl SkiplistHandle {
    pub fn insert(&self, key: InternalBytes, value: InternalBytes, guard: &Guard) {
        assert!(key.memory_controller_set() && value.memory_controller_set());
        self.0.insert(key, value, guard).release(guard);
    }

    pub fn remove(&self, key: &InternalBytes, guard: &Guard) {
        if let Some(entry) = self.0.remove(key, guard) {
            entry.release(guard);
        }
    }

    pub fn iterator(
        &self,
    ) -> OwnedIter<Arc<SkipList<InternalBytes, InternalBytes>>, InternalBytes, InternalBytes> {
        self.0.owned_iter()
    }
}

/// A single global set of skiplists shared by all cached ranges
#[derive(Clone)]
pub struct SkiplistEngine {
    pub(crate) data: [Arc<SkipList<InternalBytes, InternalBytes>>; 3],
}

impl Default for SkiplistEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl SkiplistEngine {
    pub fn new() -> Self {
        let collector = default_collector().clone();
        SkiplistEngine {
            data: [
                Arc::new(SkipList::new(collector.clone())),
                Arc::new(SkipList::new(collector.clone())),
                Arc::new(SkipList::new(collector)),
            ],
        }
    }

    pub fn cf_handle(&self, cf: &str) -> SkiplistHandle {
        SkiplistHandle(self.data[cf_to_id(cf)].clone())
    }

    pub fn node_count(&self) -> usize {
        let mut count = 0;
        self.data.iter().for_each(|s| count += s.len());
        count
    }

    pub(crate) fn delete_range(&self, range: &CacheRange) {
        DATA_CFS.iter().for_each(|&cf| {
            let (start, end) = encode_key_for_eviction(range);
            let handle = self.cf_handle(cf);
            let mut iter = handle.iterator();
            let guard = &epoch::pin();
            iter.seek(&start, guard);
            while iter.valid() && iter.key() < &end {
                handle.remove(iter.key(), guard);
                iter.next(guard);
            }
            // guard will buffer 8 drop methods, flush here to clear the buffer.
            guard.flush();
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
}

pub struct RangeCacheMemoryEngineCore {
    engine: SkiplistEngine,
    range_manager: RangeManager,
    pub(crate) cached_write_batch: BTreeMap<CacheRange, Vec<(u64, RangeCacheWriteBatchEntry)>>,
}

impl Default for RangeCacheMemoryEngineCore {
    fn default() -> Self {
        Self::new()
    }
}

impl RangeCacheMemoryEngineCore {
    pub fn new() -> RangeCacheMemoryEngineCore {
        RangeCacheMemoryEngineCore {
            engine: SkiplistEngine::new(),
            range_manager: RangeManager::default(),
            cached_write_batch: BTreeMap::default(),
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

    pub(crate) fn has_cached_write_batch(&self, cache_range: &CacheRange) -> bool {
        self.cached_write_batch.contains_key(cache_range)
    }

    pub(crate) fn take_cache_write_batch(
        &mut self,
        cache_range: &CacheRange,
    ) -> Option<Vec<(u64, RangeCacheWriteBatchEntry)>> {
        self.cached_write_batch.remove(cache_range)
    }

    // ensure that the transfer from `pending_ranges_loading_data` to
    // `range` is atomic with cached_write_batch empty
    pub(crate) fn pending_range_completes_loading(
        core: &mut RwLockWriteGuard<'_, Self>,
        range: &CacheRange,
    ) {
        fail::fail_point!("on_pending_range_completes_loading");
        assert!(!core.has_cached_write_batch(range));
        let range_manager = core.mut_range_manager();
        let (r, _, canceled) = range_manager
            .pending_ranges_loading_data
            .pop_front()
            .unwrap();
        assert_eq!(&r, range);
        assert!(!canceled);
        range_manager.new_range(r);
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
    pub(crate) core: Arc<RwLock<RangeCacheMemoryEngineCore>>,
    pub(crate) rocks_engine: Option<RocksEngine>,
    bg_work_manager: Arc<BgWorkManager>,
    memory_controller: Arc<MemoryController>,
}

impl RangeCacheMemoryEngine {
    pub fn new(config: EngineConfig) -> Self {
        let core = Arc::new(RwLock::new(RangeCacheMemoryEngineCore::new()));
        let skiplist_engine = { core.read().engine().clone() };

        let memory_controller = Arc::new(MemoryController::new(
            config.soft_limit_threshold,
            config.hard_limit_threshold,
            skiplist_engine,
        ));

        let bg_work_manager = Arc::new(BgWorkManager::new(
            core.clone(),
            config.gc_interval,
            memory_controller.clone(),
        ));

        Self {
            core,
            rocks_engine: None,
            bg_work_manager,
            memory_controller,
        }
    }

    pub fn new_range(&self, range: CacheRange) {
        let mut core = self.core.write();
        core.range_manager.new_range(range);
    }

    /// Load the range in the in-memory engine.
    // This method only push the range in the `pending_range` where sometime
    // later in `prepare_for_apply`, the range will be scheduled to load snapshot
    // data into engine.
    pub fn load_range(&self, range: CacheRange) -> result::Result<(), LoadFailedReason> {
        let mut core = self.core.write();
        core.mut_range_manager().load_range(range)
    }

    /// Evict a range from the in-memory engine. After this call, the range will
    /// not be readable, but the data of the range may not be deleted
    /// immediately due to some ongoing snapshots.
    pub fn evict_range(&self, range: &CacheRange) {
        let mut core = self.core.write();
        if core.range_manager.evict_range(range) {
            drop(core);
            // The range can be deleted directly.
            if let Err(e) = self
                .bg_worker_manager()
                .schedule_task(BackgroundTask::DeleteRange(vec![range.clone()]))
            {
                error!(
                    "schedule delete range failed";
                    "err" => ?e,
                );
                assert!(tikv_util::thread_group::is_shutdown(!cfg!(test)));
            }
        }
    }

    // It handles the pending range and check whether to buffer write for this
    // range.
    pub(crate) fn prepare_for_apply(&self, range: &CacheRange) -> RangeCacheStatus {
        let core = self.core.upgradable_read();
        let range_manager = core.range_manager();
        if range_manager.pending_ranges_in_loading_contains(range) {
            return RangeCacheStatus::Loading;
        }
        if range_manager.contains_range(range) {
            return RangeCacheStatus::Cached;
        }

        // check whether the range is in pending_range and we can schedule load task if
        // it is
        if let Some((idx, (left_range, right_range))) = range_manager
            .pending_ranges
            .iter()
            .enumerate()
            .find_map(|(idx, r)| {
                if r.contains_range(range) {
                    Some((idx, r.split_off(range)))
                } else if range.contains_range(r) {
                    // todo(SpadeA): merge occurs
                    unimplemented!()
                } else {
                    None
                }
            })
        {
            let mut core = RwLockUpgradableReadGuard::upgrade(core);

            let range_manager = core.mut_range_manager();
            if let Some(left_range) = left_range {
                range_manager.pending_ranges.push(left_range);
            }

            if let Some(right_range) = right_range {
                range_manager.pending_ranges.push(right_range);
            }

            let range_manager = core.mut_range_manager();
            range_manager.pending_ranges.swap_remove(idx);
            let rocks_snap = Arc::new(self.rocks_engine.as_ref().unwrap().snapshot(None));
            // Here, we use the range in `pending_ranges` rather than the parameter range as
            // the region may be splitted.
            range_manager
                .pending_ranges_loading_data
                .push_back((range.clone(), rocks_snap, false));
            if let Err(e) = self
                .bg_worker_manager()
                .schedule_task(BackgroundTask::LoadRange)
            {
                error!(
                    "schedule range load failed";
                    "err" => ?e,
                );
                assert!(tikv_util::thread_group::is_shutdown(!cfg!(test)));
            }
            // We have scheduled the range to loading data, so the writes of the range
            // should be buffered
            return RangeCacheStatus::Loading;
        }

        RangeCacheStatus::NotInCache
    }

    // The writes in `handle_pending_range_in_loading_buffer` indicating the ranges
    // of the writes are pending_ranges that are still loading data at the time of
    // `prepare_for_apply`. But some of them may have been finished the load and
    // become a normal range so that the writes should be written to the engine
    // directly rather than cached. This method decides which writes should be
    // cached and which writes should be written directly.
    pub(crate) fn handle_pending_range_in_loading_buffer(
        &self,
        seq: u64,
        pending_range_in_loading_buffer: Vec<RangeCacheWriteBatchEntry>,
    ) -> (Vec<RangeCacheWriteBatchEntry>, SkiplistEngine) {
        if !pending_range_in_loading_buffer.is_empty() {
            let core = self.core.upgradable_read();
            let (group_entries_to_cache, entries_to_write) =
                group_write_batch_entries(pending_range_in_loading_buffer, core.range_manager());
            let engine = core.engine().clone();
            if !group_entries_to_cache.is_empty() {
                let mut core = RwLockUpgradableReadGuard::upgrade(core);
                for (range, write_batches) in group_entries_to_cache {
                    core.cached_write_batch
                        .entry(range)
                        .or_default()
                        .extend(write_batches.into_iter().map(|e| (seq, e)));
                }
            }
            (entries_to_write, engine)
        } else {
            let core = self.core.read();
            (vec![], core.engine().clone())
        }
    }

    pub fn bg_worker_manager(&self) -> &BgWorkManager {
        &self.bg_work_manager
    }

    pub(crate) fn memory_controller(&self) -> Arc<MemoryController> {
        self.memory_controller.clone()
    }
}

impl RangeCacheMemoryEngine {
    pub fn core(&self) -> &Arc<RwLock<RangeCacheMemoryEngineCore>> {
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

    fn snapshot(
        &self,
        range: CacheRange,
        read_ts: u64,
        seq_num: u64,
    ) -> result::Result<Self::Snapshot, FailedReason> {
        RangeCacheSnapshot::new(self.clone(), range, read_ts, seq_num)
    }

    type DiskEngine = RocksEngine;
    fn set_disk_engine(&mut self, disk_engine: Self::DiskEngine) {
        self.rocks_engine = Some(disk_engine);
    }

    type RangeHintService = PdRangeHintService;
    fn start_hint_service(&self, range_hint_service: Self::RangeHintService) {
        self.bg_worker_manager()
            .start_bg_hint_service(range_hint_service)
    }

    fn get_range_for_key(&self, key: &[u8]) -> Option<CacheRange> {
        let core = self.core.read();
        core.range_manager().get_range_for_key(key)
    }
}

#[derive(PartialEq)]
enum Direction {
    Uninit,
    Forward,
    Backward,
}

pub struct RangeCacheIterator {
    valid: bool,
    iter: OwnedIter<Arc<SkipList<InternalBytes, InternalBytes>>, InternalBytes, InternalBytes>,
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

    fn iterator_opt(&self, _: &str, _: IterOptions) -> Result<Self::Iterator> {
        // This engine does not support creating iterators directly by the engine.
        unimplemented!()
    }
}

impl RangeCacheIterator {
    // If `skipping_saved_key` is true, the function will keep iterating until it
    // finds a user key that is larger than `saved_user_key`.
    // If `prefix` is not None, the iterator needs to stop when all keys for the
    // prefix are exhausted and the iterator is set to invalid.
    fn find_next_visible_key(&mut self, mut skip_saved_key: bool, guard: &epoch::Guard) {
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
                    // self.saved_user_key =
                    // Key::from_encoded(user_key.to_vec()).into_raw().unwrap();

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

            self.iter.next(guard);
        }

        self.valid = false;
    }

    fn is_visible(&self, seq: u64) -> bool {
        seq <= self.sequence_number
    }

    fn seek_internal(&mut self, key: &InternalBytes) -> Result<bool> {
        let guard = &epoch::pin();
        self.iter.seek(key, guard);
        if self.iter.valid() {
            self.find_next_visible_key(false, guard);
        }
        Ok(self.valid)
    }

    fn seek_for_prev_internal(&mut self, key: &InternalBytes) -> Result<bool> {
        let guard = &epoch::pin();
        self.iter.seek_for_prev(key, guard);
        self.prev_internal(guard);

        Ok(self.valid)
    }

    fn prev_internal(&mut self, guard: &epoch::Guard) {
        while self.iter.valid() {
            let InternalKey { user_key, .. } = decode_key(self.iter.key().as_slice());
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

            if !self.find_value_for_current_key(guard) {
                return;
            }

            self.find_user_key_before_saved(guard);

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
    fn find_value_for_current_key(&mut self, guard: &epoch::Guard) -> bool {
        assert!(self.iter.valid());
        let mut last_key_entry_type = ValueType::Deletion;
        while self.iter.valid() {
            let InternalKey {
                user_key,
                sequence,
                v_type,
            } = decode_key(self.iter.key().as_slice());

            if !self.is_visible(sequence) || self.saved_user_key != user_key {
                // no further version is visible or the user key changed
                break;
            }

            last_key_entry_type = v_type;
            match v_type {
                ValueType::Value => {
                    self.saved_value = Some(self.iter.value().clone_bytes());
                }
                ValueType::Deletion => {
                    self.saved_value.take();
                }
            }

            self.iter.prev(guard);
        }

        self.valid = last_key_entry_type == ValueType::Value;
        self.iter.valid()
    }

    // Move backwards until the key smaller than `saved_user_key`.
    // Changes valid only if return value is false.
    fn find_user_key_before_saved(&mut self, guard: &epoch::Guard) {
        while self.iter.valid() {
            let InternalKey { user_key, .. } = decode_key(self.iter.key().as_slice());

            if user_key < self.saved_user_key.as_slice() {
                return;
            }

            self.iter.prev(guard);
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
        let guard = &epoch::pin();
        self.iter.next(guard);
        self.valid = self.iter.valid();
        if self.valid {
            self.find_next_visible_key(true, guard);
        }
        Ok(self.valid)
    }

    fn prev(&mut self) -> Result<bool> {
        assert!(self.valid);
        assert!(self.direction == Direction::Backward);
        let guard = &epoch::pin();
        self.prev_internal(guard);
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

        let seek_key = encode_seek_key(seek_key, self.sequence_number);
        self.seek_internal(&seek_key)
    }

    fn seek_for_prev(&mut self, key: &[u8]) -> Result<bool> {
        self.direction = Direction::Backward;
        if let Some(ref mut extractor) = self.prefix_extractor {
            assert!(key.len() >= 8);
            self.prefix = Some(extractor.transform(key).to_vec())
        }

        let seek_key = if key > self.upper_bound.as_slice() {
            encode_seek_for_prev_key(self.upper_bound.as_slice(), u64::MAX)
        } else {
            encode_seek_for_prev_key(key, 0)
        };

        self.seek_for_prev_internal(&seek_key)
    }

    fn seek_to_first(&mut self) -> Result<bool> {
        assert!(self.prefix_extractor.is_none());
        self.direction = Direction::Forward;
        let seek_key = encode_seek_key(&self.lower_bound, self.sequence_number);
        self.seek_internal(&seek_key)
    }

    fn seek_to_last(&mut self) -> Result<bool> {
        assert!(self.prefix_extractor.is_none());
        self.direction = Direction::Backward;
        let seek_key = encode_seek_for_prev_key(&self.upper_bound, u64::MAX);
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
    ) -> result::Result<Self, FailedReason> {
        let mut core = engine.core.write();
        let range_id = core.range_manager.range_snapshot(&range, read_ts)?;
        Ok(RangeCacheSnapshot {
            snapshot_meta: RagneCacheSnapshotMeta::new(range_id, range, read_ts, seq_num),
            skiplist_engine: core.engine.clone(),
            engine: engine.clone(),
        })
    }
}

impl Drop for RangeCacheSnapshot {
    fn drop(&mut self) {
        let mut core = self.engine.core.write();
        let ranges_removable = core
            .range_manager
            .remove_range_snapshot(&self.snapshot_meta);
        if !ranges_removable.is_empty() {
            drop(core);
            if let Err(e) = self
                .engine
                .bg_worker_manager()
                .schedule_task(BackgroundTask::DeleteRange(ranges_removable))
            {
                error!(
                    "schedule delete range failed";
                    "err" => ?e,
                );
                assert!(tikv_util::thread_group::is_shutdown(!cfg!(test)));
            }
        }
    }
}

impl Snapshot for RangeCacheSnapshot {}

impl Iterable for RangeCacheSnapshot {
    type Iterator = RangeCacheIterator;

    fn iterator_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        let iter = self.skiplist_engine.data[cf_to_id(cf)].owned_iter();
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

        let (lower_bound, upper_bound) = (lower_bound.unwrap(), upper_bound.unwrap());
        if lower_bound < self.snapshot_meta.range.start
            || upper_bound > self.snapshot_meta.range.end
        {
            return Err(Error::Other(box_err!(
                "the bounderies required [{}, {}] exceeds the range of the snapshot [{}, {}]",
                log_wrappers::Value(&lower_bound),
                log_wrappers::Value(&upper_bound),
                log_wrappers::Value(&self.snapshot_meta.range.start),
                log_wrappers::Value(&self.snapshot_meta.range.end)
            )));
        }

        Ok(RangeCacheIterator {
            valid: false,
            prefix: None,
            lower_bound,
            upper_bound,
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
        fail::fail_point!("on_range_cache_get_value");
        if !self.snapshot_meta.range.contains_key(key) {
            return Err(Error::Other(box_err!(
                "key {} not in range[{}, {}]",
                log_wrappers::Value(key),
                log_wrappers::Value(&self.snapshot_meta.range.start),
                log_wrappers::Value(&self.snapshot_meta.range.end)
            )));
        }
        let mut iter = self.skiplist_engine.data[cf_to_id(cf)].owned_iter();
        let seek_key = encode_seek_key(key, self.sequence_number());

        let guard = &epoch::pin();
        iter.seek(&seek_key, guard);
        if !iter.valid() {
            return Ok(None);
        }

        match decode_key(iter.key().as_slice()) {
            InternalKey {
                user_key,
                v_type: ValueType::Value,
                ..
            } if user_key == key => Ok(Some(RangeCacheDbVector(iter.value().clone_bytes()))),
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
    use core::ops::Range;
    use std::{
        iter::{self, StepBy},
        ops::Deref,
        sync::Arc,
        time::Duration,
    };

    use bytes::{BufMut, Bytes};
    use crossbeam::epoch;
    use engine_traits::{
        CacheRange, FailedReason, IterOptions, Iterable, Iterator, Peekable, RangeCacheEngine,
        ReadOptions,
    };
    use skiplist_rs::SkipList;

    use super::{cf_to_id, RangeCacheIterator, SkiplistEngine};
    use crate::{
        keys::{
            construct_key, construct_user_key, construct_value, decode_key, encode_key,
            encode_seek_key, InternalBytes, ValueType,
        },
        EngineConfig, RangeCacheMemoryEngine,
    };

    #[test]
    fn test_snapshot() {
        let engine = RangeCacheMemoryEngine::new(EngineConfig::config_for_test());
        let range = CacheRange::new(b"k00".to_vec(), b"k10".to_vec());
        engine.new_range(range.clone());

        let verify_snapshot_count = |snapshot_ts, count| {
            let core = engine.core.read();
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

        let s1 = engine.snapshot(range.clone(), 5, u64::MAX).unwrap();

        {
            let mut core = engine.core.write();
            let t_range = CacheRange::new(b"k00".to_vec(), b"k02".to_vec());
            assert!(!core.range_manager.set_safe_point(&t_range, 5));
            assert!(core.range_manager.set_safe_point(&range, 5));
        }
        assert_eq!(
            engine.snapshot(range.clone(), 5, u64::MAX).unwrap_err(),
            FailedReason::TooOldRead
        );
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
            let core = engine.core.write();
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

    fn fill_data_in_skiplist(
        sl: Arc<SkipList<InternalBytes, InternalBytes>>,
        key_range: StepBy<Range<u64>>,
        mvcc_range: Range<u64>,
        mut start_seq: u64,
    ) {
        let guard = &epoch::pin();
        for mvcc in mvcc_range {
            for i in key_range.clone() {
                let key = construct_key(i, mvcc);
                let val = construct_value(i, mvcc);
                let key = encode_key(&key, start_seq, ValueType::Value);
                sl.insert(key, InternalBytes::from_vec(val.into_bytes()), guard)
                    .release(guard);
            }
            start_seq += 1;
        }
    }

    fn delete_data_in_skiplist(
        sl: Arc<SkipList<InternalBytes, InternalBytes>>,
        key_range: StepBy<Range<u64>>,
        mvcc_range: Range<u64>,
        mut seq: u64,
    ) {
        let guard = &epoch::pin();
        for i in key_range {
            for mvcc in mvcc_range.clone() {
                let key = construct_key(i, mvcc);
                let key = encode_key(&key, seq, ValueType::Deletion);
                sl.insert(key, InternalBytes::from_bytes(Bytes::default()), guard)
                    .release(guard);
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
        sl: &Arc<SkipList<InternalBytes, InternalBytes>>,
        key: &str,
        val: &str,
        mvcc: u64,
        seq: u64,
    ) {
        let key = construct_mvcc_key(key, mvcc);
        let key = encode_key(&key, seq, ValueType::Value);
        let guard = &epoch::pin();
        sl.insert(
            key,
            InternalBytes::from_vec(val.to_owned().into_bytes()),
            guard,
        )
        .release(guard);
    }

    fn delete_key(
        sl: &Arc<SkipList<InternalBytes, InternalBytes>>,
        key: &str,
        mvcc: u64,
        seq: u64,
    ) {
        let key = construct_mvcc_key(key, mvcc);
        let key = encode_key(&key, seq, ValueType::Deletion);
        let guard = &epoch::pin();
        sl.insert(key, InternalBytes::from_vec(b"".to_vec()), guard)
            .release(guard);
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
        let engine = RangeCacheMemoryEngine::new(EngineConfig::config_for_test());
        let range = CacheRange::new(b"".to_vec(), b"z".to_vec());
        engine.new_range(range.clone());

        {
            let mut core = engine.core.write();
            core.range_manager.set_safe_point(&range, 5);
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
        let engine = RangeCacheMemoryEngine::new(EngineConfig::config_for_test());
        let range = CacheRange::new(b"".to_vec(), b"z".to_vec());
        engine.new_range(range.clone());
        let step: i32 = 2;

        {
            let mut core = engine.core.write();
            core.range_manager.set_safe_point(&range, 5);
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
        let engine = RangeCacheMemoryEngine::new(EngineConfig::config_for_test());
        let range = CacheRange::new(b"".to_vec(), b"z".to_vec());
        engine.new_range(range.clone());
        let step: i32 = 2;

        {
            let mut core = engine.core.write();
            core.range_manager.set_safe_point(&range, 5);
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
        let engine = RangeCacheMemoryEngine::new(EngineConfig::config_for_test());
        let range = CacheRange::new(b"".to_vec(), b"z".to_vec());
        engine.new_range(range.clone());

        {
            let mut core = engine.core.write();
            core.range_manager.set_safe_point(&range, 5);
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
        iter_opt.set_upper_bound(&range.end, 0);
        iter_opt.set_lower_bound(&range.start, 0);

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
        let engine = RangeCacheMemoryEngine::new(EngineConfig::config_for_test());
        let range = CacheRange::new(b"".to_vec(), b"z".to_vec());
        engine.new_range(range.clone());

        {
            let mut core = engine.core.write();
            core.range_manager.set_safe_point(&range, 5);
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
        iter_opt.set_upper_bound(&range.end, 0);
        iter_opt.set_lower_bound(&range.start, 0);

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
    fn test_iter_user_skip() {
        let mut iter_opt = IterOptions::default();
        let range = CacheRange::new(b"".to_vec(), b"z".to_vec());
        iter_opt.set_upper_bound(&range.end, 0);
        iter_opt.set_lower_bound(&range.start, 0);

        // backward, all put
        {
            let engine = RangeCacheMemoryEngine::new(EngineConfig::config_for_test());
            engine.new_range(range.clone());
            let sl = {
                let mut core = engine.core.write();
                core.range_manager.set_safe_point(&range, 5);
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
            let engine = RangeCacheMemoryEngine::new(EngineConfig::config_for_test());
            engine.new_range(range.clone());
            let sl = {
                let mut core = engine.core.write();
                core.range_manager.set_safe_point(&range, 5);
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
            let engine = RangeCacheMemoryEngine::new(EngineConfig::config_for_test());
            engine.new_range(range.clone());
            let sl = {
                let mut core = engine.core.write();
                core.range_manager.set_safe_point(&range, 5);
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
            let engine = RangeCacheMemoryEngine::new(EngineConfig::config_for_test());
            engine.new_range(range.clone());
            let sl = {
                let mut core = engine.core.write();
                core.range_manager.set_safe_point(&range, 5);
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
        let engine = RangeCacheMemoryEngine::new(EngineConfig::config_for_test());
        let range = CacheRange::new(b"k000".to_vec(), b"k100".to_vec());
        engine.new_range(range.clone());

        {
            let mut core = engine.core.write();
            core.range_manager.set_safe_point(&range, 5);
            let sl = core.engine.data[cf_to_id("write")].clone();

            let guard = &epoch::pin();
            for i in 1..5 {
                for mvcc in 10..20 {
                    let user_key = construct_key(i, mvcc);
                    let internal_key = encode_key(&user_key, 10, ValueType::Value);
                    let v = format!("v{:02}{:02}", i, mvcc);
                    sl.insert(internal_key, InternalBytes::from_vec(v.into_bytes()), guard)
                        .release(guard);
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
        let sl_engine = SkiplistEngine::new();
        sl_engine.data.iter().for_each(|sl| {
            fill_data_in_skiplist(sl.clone(), (1..60).step_by(1), 1..2, 1);
        });

        let evict_range = CacheRange::new(construct_user_key(20), construct_user_key(40));
        sl_engine.delete_range(&evict_range);
        sl_engine.data.iter().for_each(|sl| {
            let mut iter = sl.owned_iter();
            let guard = &epoch::pin();
            iter.seek_to_first(guard);
            for i in 1..20 {
                let internal_key = decode_key(iter.key().as_slice());
                let expected_key = construct_key(i, 1);
                assert_eq!(internal_key.user_key, &expected_key);
                iter.next(guard);
            }

            for i in 40..60 {
                let internal_key = decode_key(iter.key().as_slice());
                let expected_key = construct_key(i, 1);
                assert_eq!(internal_key.user_key, &expected_key);
                iter.next(guard);
            }
            assert!(!iter.valid());
        });
    }

    fn verify_evict_range_deleted(engine: &RangeCacheMemoryEngine, range: &CacheRange) {
        let mut wait = 0;
        while wait < 10 {
            wait += 1;
            if !engine
                .core
                .read()
                .range_manager()
                .ranges_being_deleted
                .is_empty()
            {
                std::thread::sleep(Duration::from_millis(200));
            } else {
                break;
            }
        }
        let write_handle = engine.core.read().engine.cf_handle("write");
        let start_key = encode_seek_key(&range.start, u64::MAX);
        let mut iter = write_handle.iterator();

        let guard = &epoch::pin();
        iter.seek(&start_key, guard);
        let end = encode_seek_key(&range.end, u64::MAX);
        assert!(iter.key() > &end || !iter.valid());
    }

    #[test]
    fn test_evict_range_without_snapshot() {
        let engine = RangeCacheMemoryEngine::new(EngineConfig::config_for_test());
        let range = CacheRange::new(construct_user_key(0), construct_user_key(30));
        let evict_range = CacheRange::new(construct_user_key(10), construct_user_key(20));
        engine.new_range(range.clone());

        let guard = &epoch::pin();
        {
            let mut core = engine.core.write();
            core.range_manager.set_safe_point(&range, 5);
            let sl = core.engine.data[cf_to_id("write")].clone();
            for i in 0..30 {
                let user_key = construct_key(i, 10);
                let internal_key = encode_key(&user_key, 10, ValueType::Value);
                let v = construct_value(i, 10);
                sl.insert(internal_key, InternalBytes::from_vec(v.into_bytes()), guard)
                    .release(guard);
            }
        }

        engine.evict_range(&evict_range);
        assert_eq!(
            engine.snapshot(range.clone(), 10, 200).unwrap_err(),
            FailedReason::NotCached
        );
        assert_eq!(
            engine.snapshot(evict_range.clone(), 10, 200).unwrap_err(),
            FailedReason::NotCached
        );

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

        let snap_right = engine.snapshot(r_right, 10, 200).unwrap();
        let lower_bound = construct_user_key(20);
        let upper_bound = construct_user_key(30);
        iter_opt.set_upper_bound(&upper_bound, 0);
        iter_opt.set_lower_bound(&lower_bound, 0);
        let mut iter = snap_right.iterator_opt("write", iter_opt).unwrap();
        iter.seek_to_first().unwrap();
        verify_key_values(&mut iter, (20..30).step_by(1), 10..11, true, true);

        // verify the key, values are delete
        verify_evict_range_deleted(&engine, &evict_range);
    }

    #[test]
    fn test_evict_range_with_snapshot() {
        let engine = RangeCacheMemoryEngine::new(EngineConfig::config_for_test());
        let range = CacheRange::new(construct_user_key(0), construct_user_key(30));
        let evict_range = CacheRange::new(construct_user_key(10), construct_user_key(20));
        engine.new_range(range.clone());

        let guard = &epoch::pin();
        {
            let mut core = engine.core.write();
            core.range_manager.set_safe_point(&range, 5);
            let sl = core.engine.data[cf_to_id("write")].clone();
            for i in 0..30 {
                let user_key = construct_key(i, 10);
                let internal_key = encode_key(&user_key, 10, ValueType::Value);
                let v = construct_value(i, 10);
                sl.insert(
                    internal_key,
                    InternalBytes::from_vec(v.clone().into_bytes()),
                    guard,
                )
                .release(guard);
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

        // todo(SpadeA): memory limiter
        {
            // evict_range is not eligible for delete
            assert!(
                engine
                    .core
                    .read()
                    .range_manager()
                    .ranges_being_deleted
                    .contains(&evict_range)
            );
        }

        drop(s1);
        {
            // evict_range is still not eligible for delete
            assert!(
                engine
                    .core
                    .read()
                    .range_manager()
                    .ranges_being_deleted
                    .contains(&evict_range)
            );
        }
        drop(s2);
        // Now, all snapshots before evicting `evict_range` are released
        verify_evict_range_deleted(&engine, &evict_range);

        drop(s4);
        verify_evict_range_deleted(&engine, &range_left_eviction);
    }
}
