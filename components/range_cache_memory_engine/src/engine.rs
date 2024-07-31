// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    fmt::{self, Debug},
    ops::Bound,
    result,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use crossbeam::epoch::{self, default_collector, Guard};
use crossbeam_skiplist::{
    base::{Entry, OwnedIter},
    SkipList,
};
use engine_rocks::RocksEngine;
use engine_traits::{
    CacheRange, FailedReason, IterOptions, Iterable, KvEngine, RangeCacheEngine, RegionEvent,
    Result, CF_DEFAULT, CF_LOCK, CF_WRITE, DATA_CFS,
};
use kvproto::metapb::Region;
use parking_lot::{lock_api::RwLockUpgradableReadGuard, RwLock};
use raftstore::coprocessor::RegionInfoProvider;
use slog_global::error;
use tikv_util::{config::VersionTrack, info};

use crate::{
    background::{BackgroundTask, BgWorkManager, PdRangeHintService},
    keys::{
        encode_key_for_boundary_with_mvcc, encode_key_for_boundary_without_mvcc, InternalBytes,
    },
    memory_controller::MemoryController,
    range_manager::{RangeCacheStatus, RangeManager},
    read::{RangeCacheIterator, RangeCacheSnapshot},
    statistics::Statistics,
    write_batch::{group_write_batch_entries, RangeCacheWriteBatchEntry},
    RangeCacheEngineConfig, RangeCacheEngineContext,
};

pub(crate) const CF_DEFAULT_USIZE: usize = 0;
pub(crate) const CF_LOCK_USIZE: usize = 1;
pub(crate) const CF_WRITE_USIZE: usize = 2;

pub(crate) fn cf_to_id(cf: &str) -> usize {
    match cf {
        CF_DEFAULT => CF_DEFAULT_USIZE,
        CF_LOCK => CF_LOCK_USIZE,
        CF_WRITE => CF_WRITE_USIZE,
        _ => panic!("unrecognized cf {}", cf),
    }
}

pub(crate) fn id_to_cf(id: usize) -> &'static str {
    match id {
        CF_DEFAULT_USIZE => CF_DEFAULT,
        CF_LOCK_USIZE => CF_LOCK,
        CF_WRITE_USIZE => CF_WRITE,
        _ => panic!("unrecognized id {}", id),
    }
}

#[inline]
pub(crate) fn is_lock_cf(cf: usize) -> bool {
    cf == CF_LOCK_USIZE
}

// A wrapper for skiplist to provide some check and clean up worker
#[derive(Clone)]
pub struct SkiplistHandle(Arc<SkipList<InternalBytes, InternalBytes>>);

impl SkiplistHandle {
    pub fn get<'a: 'g, 'g>(
        &'a self,
        key: &InternalBytes,
        guard: &'g Guard,
    ) -> Option<Entry<'a, 'g, InternalBytes, InternalBytes>> {
        self.0.get(key, guard)
    }

    pub fn get_with_user_key<'a: 'g, 'g>(
        &'a self,
        key: &InternalBytes,
        guard: &'g Guard,
    ) -> Option<Entry<'a, 'g, InternalBytes, InternalBytes>> {
        let n = self.0.lower_bound(Bound::Included(key), guard)?;
        if n.key().same_user_key_with(key) {
            Some(n)
        } else {
            None
        }
    }

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

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
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

    pub(crate) fn delete_range_cf(&self, cf: &str, range: &CacheRange) {
        let (start, end) = if cf == CF_LOCK {
            encode_key_for_boundary_without_mvcc(range)
        } else {
            encode_key_for_boundary_with_mvcc(range)
        };

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
    }

    pub(crate) fn delete_range(&self, range: &CacheRange) {
        DATA_CFS.iter().for_each(|&cf| {
            self.delete_range_cf(cf, range);
        });
    }
}

impl Debug for SkiplistEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Range Memory Engine")
    }
}

pub struct RangeCacheMemoryEngineCore {
    pub(crate) engine: SkiplistEngine,
    pub(crate) range_manager: RangeManager,
    pub(crate) cached_write_batch: HashMap<u64, Vec<(u64, RangeCacheWriteBatchEntry)>>,
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
            cached_write_batch: HashMap::default(),
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

    // `cached_range` must not exist in the `cached_write_batch`
    pub fn init_cached_write_batch(&mut self, region_id: u64) {
        assert!(self.cached_write_batch.insert(region_id, vec![]).is_none());
    }

    pub fn has_cached_write_batch(&self, id: u64) -> bool {
        self.cached_write_batch
            .get(&id)
            .map_or(false, |entries| !entries.is_empty())
    }

    pub(crate) fn take_cached_write_batch_entries(
        &mut self,
        id: u64,
    ) -> Vec<(u64, RangeCacheWriteBatchEntry)> {
        std::mem::take(self.cached_write_batch.get_mut(&id).unwrap())
    }

    pub fn remove_cached_write_batch(&mut self, id: u64) {
        self.cached_write_batch.remove(&id).unwrap_or_else(|| {
            panic!("region {} cannot be found in cached_write_batch", id);
        });
    }

    // ensure that the transfer from `pending_regions_loading_data` to
    // `region` is atomic with cached_write_batch empty
    pub(crate) fn pending_region_completes_loading(&mut self, region: &Region) {
        assert!(!self.has_cached_write_batch(region.id));
        let (r, _, canceled) = self
            .range_manager
            .pending_regions_loading_data
            .pop_front()
            .unwrap();
        assert_eq!(&r, region);
        assert!(!canceled);
        self.range_manager.new_region(region.clone());
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
    bg_work_manager: Arc<BgWorkManager>,
    pub(crate) core: Arc<RwLock<RangeCacheMemoryEngineCore>>,
    pub(crate) rocks_engine: Option<RocksEngine>,
    memory_controller: Arc<MemoryController>,
    statistics: Arc<Statistics>,
    config: Arc<VersionTrack<RangeCacheEngineConfig>>,

    // The increment amount of tombstones in the lock cf.
    // When reaching to the threshold, a CleanLockTombstone task will be scheduled to clean lock cf
    // tombstones.
    pub(crate) lock_modification_bytes: Arc<AtomicU64>,

    // `write_batch_id_allocator` is used to allocate id for each write batch
    write_batch_id_allocator: Arc<AtomicU64>,
    region_info_provider: Option<Arc<dyn RegionInfoProvider>>,
}

impl RangeCacheMemoryEngine {
    pub fn new(range_cache_engine_context: RangeCacheEngineContext) -> Self {
        RangeCacheMemoryEngine::with_region_info_provider(range_cache_engine_context, None)
    }

    pub fn with_region_info_provider(
        range_cache_engine_context: RangeCacheEngineContext,
        region_info_provider: Option<Arc<dyn RegionInfoProvider>>,
    ) -> Self {
        info!("init range cache memory engine";);
        let core = Arc::new(RwLock::new(RangeCacheMemoryEngineCore::new()));
        let skiplist_engine = { core.read().engine().clone() };

        let RangeCacheEngineContext {
            config,
            statistics,
            pd_client,
        } = range_cache_engine_context;
        assert!(config.value().enabled);
        let memory_controller = Arc::new(MemoryController::new(config.clone(), skiplist_engine));

        let bg_work_manager = Arc::new(BgWorkManager::new(
            core.clone(),
            pd_client,
            config.value().gc_interval.0,
            config.value().load_evict_interval.0,
            config.value().expected_region_size(),
            memory_controller.clone(),
            region_info_provider.clone(),
        ));

        Self {
            core,
            rocks_engine: None,
            bg_work_manager,
            memory_controller,
            statistics,
            config,
            lock_modification_bytes: Arc::default(),
            write_batch_id_allocator: Arc::default(),
            region_info_provider,
        }
    }

    pub fn expected_region_size(&self) -> usize {
        self.config.value().expected_region_size()
    }

    /// Evict a region from the in-memory engine. After this call, the region
    /// will not be readable, but the data of the region may not be deleted
    /// immediately due to some ongoing snapshots.
    pub fn evict_region(&self, region: Region) {
        let mut core = self.core.write();
        let should_evict = core.range_manager.evict_region(&region);
        if should_evict {
            let mut regions = vec![region];
            core.mut_range_manager()
                .mark_delete_regions_scheduled(&mut regions);
            if !regions.is_empty() {
                drop(core);
                // The range can be deleted directly.
                if let Err(e) = self
                    .bg_worker_manager()
                    .schedule_task(BackgroundTask::DeleteRegions(regions))
                {
                    error!(
                        "schedule delete region failed";
                        "err" => ?e,
                    );
                    assert!(tikv_util::thread_group::is_shutdown(!cfg!(test)));
                }
            }
        }
    }

    // It handles the pending range and check whether to buffer write for this
    // range.
    pub(crate) fn prepare_for_apply(
        &self,
        write_batch_id: u64,
        range: CacheRange,
        region: &Region,
    ) -> RangeCacheStatus {
        let mut core = self.core.write();
        let range_manager: &mut RangeManager = core.mut_range_manager();
        if range_manager.pending_regions_in_loading_contains(region.id) {
            range_manager.record_in_region_being_written(write_batch_id, range, region.id);
            return RangeCacheStatus::Loading;
        }
        if range_manager.contains_region(region.id) {
            range_manager.record_in_region_being_written(write_batch_id, range, region.id);
            return RangeCacheStatus::Cached;
        }

        let idx = match range_manager
            .pending_regions
            .iter()
            .enumerate()
            .find_map(|(idx, r)| if r.id == region.id { Some(idx) } else { None })
        {
            Some(idx) => idx,
            None => {
                return RangeCacheStatus::NotInCache;
            }
        };

        let cached_region = range_manager.pending_regions.swap_remove(idx);
        // region range changes, discard this range.
        if cached_region.get_region_epoch().version != region.get_region_epoch().version {
            return RangeCacheStatus::NotInCache;
        }

        let rocks_snap = Arc::new(self.rocks_engine.as_ref().unwrap().snapshot(None));
        range_manager
            .pending_regions_loading_data
            .push_back((region.clone(), rocks_snap, false));

        let tag = range.tag.clone();
        range_manager.record_in_region_being_written(write_batch_id, range, region.id);
        info!(
            "range to load";
            "tag" => &tag,
            "cached" => range_manager.regions().len(),
            "pending" => range_manager.pending_regions_loading_data.len(),
        );

        // init cached write batch to cache the writes before loading complete
        core.init_cached_write_batch(region.id);

        if let Err(e) = self
            .bg_worker_manager()
            .schedule_task(BackgroundTask::LoadRange)
        {
            error!(
                "schedule range load failed";
                "err" => ?e,
                "tag" => &tag,
            );
            assert!(tikv_util::thread_group::is_shutdown(!cfg!(test)));
        }
        // We have scheduled the range to loading data, so the writes of the range
        // should be buffered
        RangeCacheStatus::Loading
    }

    // The writes in `handle_pending_range_in_loading_buffer` indicating the ranges
    // of the writes are pending_ranges that are still loading data at the time of
    // `prepare_for_apply`. But some of them may have been finished the load and
    // become a normal range so that the writes should be written to the engine
    // directly rather than cached. This method decides which writes should be
    // cached and which writes should be written directly.
    pub(crate) fn handle_pending_range_in_loading_buffer(
        &self,
        seq: &mut u64,
        pending_range_in_loading_buffer: Vec<RangeCacheWriteBatchEntry>,
    ) -> (Vec<RangeCacheWriteBatchEntry>, SkiplistEngine) {
        if !pending_range_in_loading_buffer.is_empty() {
            let core = self.core.upgradable_read();
            let (group_entries_to_cache, entries_to_write) =
                group_write_batch_entries(pending_range_in_loading_buffer, core.range_manager());
            let engine = core.engine().clone();
            if !group_entries_to_cache.is_empty() {
                let mut core = RwLockUpgradableReadGuard::upgrade(core);
                for (region_id, write_batches) in group_entries_to_cache {
                    core.cached_write_batch
                        .entry(region_id)
                        .or_default()
                        .extend(write_batches.into_iter().map(|e| {
                            // We should confirm the sequence number for cached entries, and
                            // also increased for each of them.
                            *seq += 1;
                            (*seq - 1, e)
                        }));
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

    pub fn memory_controller(&self) -> Arc<MemoryController> {
        self.memory_controller.clone()
    }

    pub fn statistics(&self) -> Arc<Statistics> {
        self.statistics.clone()
    }

    pub fn alloc_write_batch_id(&self) -> u64 {
        self.write_batch_id_allocator
            .fetch_add(1, Ordering::Relaxed)
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
        region_id: u64,
        region_epoch: u64,
        range: CacheRange,
        read_ts: u64,
        seq_num: u64,
    ) -> result::Result<Self::Snapshot, FailedReason> {
        RangeCacheSnapshot::new(
            self.clone(),
            region_id,
            region_epoch,
            range,
            read_ts,
            seq_num,
        )
    }

    type DiskEngine = RocksEngine;
    fn set_disk_engine(&mut self, disk_engine: Self::DiskEngine) {
        self.rocks_engine = Some(disk_engine.clone());
        if let Err(e) = self
            .bg_worker_manager()
            .schedule_task(BackgroundTask::SetRocksEngine(disk_engine))
        {
            error!(
                "schedule set rocks_engine failed";
                "err" => ?e,
            );
            assert!(tikv_util::thread_group::is_shutdown(!cfg!(test)));
        }
    }

    type RangeHintService = PdRangeHintService;
    fn start_hint_service(&self, range_hint_service: Self::RangeHintService) {
        self.bg_worker_manager()
            .start_bg_hint_service(range_hint_service)
    }

    fn get_region_for_key(&self, key: &[u8]) -> Option<Region> {
        self.core.read().range_manager().get_region_for_key(key)
    }

    fn enabled(&self) -> bool {
        self.config.value().enabled
    }

    fn on_region_event(&self, event: RegionEvent) {
        match event {
            RegionEvent::Eviction { region } => {
                self.evict_region(region);
            }
            RegionEvent::Split {
                source,
                new_regions,
            } => {
                let mut core = self.core.write();
                let is_evicted = core.range_manager.split_region(&source, new_regions);
                if is_evicted {
                    core.cached_write_batch.remove(&source.id);
                }
            }
            RegionEvent::EvictByRange { range } => {
                // TOOD: handle overlapped region locally after we change to state machine.
                if let Some(provider) = &self.region_info_provider {
                    let regions = provider
                        .get_regions_in_range(&range.start, &range.end)
                        .unwrap();
                    for r in regions {
                        self.evict_region(r);
                    }
                } else {
                    unimplemented!("Does not support currently");
                }
            }
        }
    }
}

impl Iterable for RangeCacheMemoryEngine {
    type Iterator = RangeCacheIterator;

    fn iterator_opt(&self, _: &str, _: IterOptions) -> Result<Self::Iterator> {
        // This engine does not support creating iterators directly by the engine.
        panic!("iterator_opt is not supported on creating by RangeCacheMemoryEngine directly")
    }
}

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use crossbeam::epoch;
    use engine_traits::{CacheRange, CF_DEFAULT, CF_LOCK, CF_WRITE};
    use tikv_util::config::{ReadableSize, VersionTrack};

    use super::SkiplistEngine;
    use crate::{
        keys::{construct_key, construct_user_key, encode_key},
        memory_controller::MemoryController,
        InternalBytes, RangeCacheEngineConfig, RangeCacheEngineContext, RangeCacheMemoryEngine,
        ValueType,
    };

    #[test]
    fn test_overlap_with_pending() {
        let engine = RangeCacheMemoryEngine::new(RangeCacheEngineContext::new_for_tests(Arc::new(
            VersionTrack::new(RangeCacheEngineConfig::config_for_test()),
        )));
        let range1 = CacheRange::new(b"k1".to_vec(), b"k3".to_vec());
        engine.load_range(range1).unwrap();

        let range2 = CacheRange::new(b"k1".to_vec(), b"k5".to_vec());
        engine.prepare_for_apply(1, &range2);
        assert!(
            engine.core.read().range_manager().pending_ranges.is_empty()
                && engine
                    .core
                    .read()
                    .range_manager()
                    .pending_ranges_loading_data
                    .is_empty()
        );

        let range1 = CacheRange::new(b"k1".to_vec(), b"k3".to_vec());
        engine.load_range(range1).unwrap();

        let range2 = CacheRange::new(b"k2".to_vec(), b"k5".to_vec());
        engine.prepare_for_apply(1, &range2);
        assert!(
            engine.core.read().range_manager().pending_ranges.is_empty()
                && engine
                    .core
                    .read()
                    .range_manager()
                    .pending_ranges_loading_data
                    .is_empty()
        );
    }

    #[test]
    fn test_delete_range() {
        let delete_range_cf = |cf| {
            let skiplist = SkiplistEngine::default();
            let handle = skiplist.cf_handle(cf);

            let config = Arc::new(VersionTrack::new(RangeCacheEngineConfig {
                enabled: true,
                gc_interval: Default::default(),
                load_evict_interval: Default::default(),
                soft_limit_threshold: Some(ReadableSize(300)),
                hard_limit_threshold: Some(ReadableSize(500)),
                expected_region_size: Some(ReadableSize::mb(20)),
            }));
            let mem_controller = Arc::new(MemoryController::new(config.clone(), skiplist.clone()));

            let guard = &epoch::pin();

            let insert_kv = |k, mvcc, v: &[u8], seq| {
                let user_key = construct_key(k, mvcc);
                let mut key = encode_key(&user_key, seq, ValueType::Value);
                let mut val = InternalBytes::from_vec(v.to_vec());
                key.set_memory_controller(mem_controller.clone());
                val.set_memory_controller(mem_controller.clone());
                handle.insert(key, val, guard);
            };

            insert_kv(0, 1, b"val", 100);
            insert_kv(1, 2, b"val", 101);
            insert_kv(1, 3, b"val", 102);
            insert_kv(2, 2, b"val", 103);
            insert_kv(9, 2, b"val", 104);
            insert_kv(10, 2, b"val", 105);

            let start = construct_user_key(1);
            let end = construct_user_key(10);
            let range = CacheRange::new(start, end);
            skiplist.delete_range(&range);

            let mut iter = handle.iterator();
            iter.seek_to_first(guard);
            let expect = construct_key(0, 1);
            let expect = encode_key(&expect, 100, ValueType::Value);
            assert_eq!(iter.key(), &expect);
            iter.next(guard);

            let expect = construct_key(10, 2);
            let expect = encode_key(&expect, 105, ValueType::Value);
            assert_eq!(iter.key(), &expect);
            iter.next(guard);
            assert!(!iter.valid());
        };
        delete_range_cf(CF_DEFAULT);
        delete_range_cf(CF_WRITE);
    }

    #[test]
    fn test_delete_range_for_lock_cf() {
        let skiplist = SkiplistEngine::default();
        let lock_handle = skiplist.cf_handle(CF_LOCK);

        let config = Arc::new(VersionTrack::new(RangeCacheEngineConfig {
            enabled: true,
            gc_interval: Default::default(),
            load_evict_interval: Default::default(),
            soft_limit_threshold: Some(ReadableSize(300)),
            hard_limit_threshold: Some(ReadableSize(500)),
            expected_region_size: Some(ReadableSize::mb(20)),
        }));
        let mem_controller = Arc::new(MemoryController::new(config.clone(), skiplist.clone()));

        let guard = &epoch::pin();

        let insert_kv = |k, v: &[u8], seq| {
            let mut key = encode_key(k, seq, ValueType::Value);
            let mut val = InternalBytes::from_vec(v.to_vec());
            key.set_memory_controller(mem_controller.clone());
            val.set_memory_controller(mem_controller.clone());
            lock_handle.insert(key, val, guard);
        };

        insert_kv(b"k", b"val", 100);
        insert_kv(b"k1", b"val1", 101);
        insert_kv(b"k2", b"val2", 102);
        insert_kv(b"k3", b"val3", 103);
        insert_kv(b"k4", b"val4", 104);

        let range = CacheRange::new(b"k1".to_vec(), b"k4".to_vec());
        skiplist.delete_range(&range);

        let mut iter = lock_handle.iterator();
        iter.seek_to_first(guard);
        let expect = encode_key(b"k", 100, ValueType::Value);
        assert_eq!(iter.key(), &expect);

        iter.next(guard);
        let expect = encode_key(b"k4", 104, ValueType::Value);
        assert_eq!(iter.key(), &expect);

        iter.next(guard);
        assert!(!iter.valid());
    }
}
