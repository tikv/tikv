// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
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
    CacheRegion, EvictReason, FailedReason, IterOptions, Iterable, KvEngine, RangeCacheEngine,
    RegionEvent, Result, CF_DEFAULT, CF_LOCK, CF_WRITE, DATA_CFS,
};
use kvproto::metapb::Region;
use parking_lot::RwLock;
use raftstore::coprocessor::RegionInfoProvider;
use slog_global::error;
use tikv_util::{config::VersionTrack, info};

use crate::{
    background::{BackgroundTask, BgWorkManager, PdRangeHintService},
    keys::{
        encode_key_for_boundary_with_mvcc, encode_key_for_boundary_without_mvcc, InternalBytes,
    },
    memory_controller::MemoryController,
    range_manager::{LoadFailedReason, RangeCacheStatus, RegionManager, RegionState},
    read::{RangeCacheIterator, RangeCacheSnapshot},
    statistics::Statistics,
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

    pub(crate) fn delete_range_cf(&self, cf: &str, region: &CacheRegion) {
        let (start, end) = if cf == CF_LOCK {
            encode_key_for_boundary_without_mvcc(region)
        } else {
            encode_key_for_boundary_with_mvcc(region)
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

    pub(crate) fn delete_range(&self, region: &CacheRegion) {
        DATA_CFS.iter().for_each(|&cf| {
            self.delete_range_cf(cf, region);
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
    pub(crate) range_manager: RegionManager,
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
            range_manager: RegionManager::default(),
        }
    }

    pub fn engine(&self) -> SkiplistEngine {
        self.engine.clone()
    }

    pub fn range_manager(&self) -> &RegionManager {
        &self.range_manager
    }

    pub fn mut_range_manager(&mut self) -> &mut RegionManager {
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
}

impl RangeCacheMemoryEngine {
    pub fn new(range_cache_engine_context: RangeCacheEngineContext) -> Self {
        RangeCacheMemoryEngine::with_region_info_provider(range_cache_engine_context, None)
    }

    pub fn with_region_info_provider(
        range_cache_engine_context: RangeCacheEngineContext,
        region_info_provider: Option<Arc<dyn RegionInfoProvider>>,
    ) -> Self {
        info!("ime init range cache memory engine");
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
            config.value().mvcc_amplification_threshold,
            memory_controller.clone(),
            region_info_provider,
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
        }
    }

    pub fn expected_region_size(&self) -> usize {
        self.config.value().expected_region_size()
    }

    pub fn new_region(&self, region: Region) {
        let cache_region = CacheRegion::from_region(&region);
        self.core.write().range_manager.new_region(cache_region);
    }

    pub fn load_region(&self, region: Region) -> result::Result<(), LoadFailedReason> {
        let cache_region = CacheRegion::from_region(&region);
        self.core
            .write()
            .mut_range_manager()
            .load_region(cache_region)
    }

    /// Evict a region from the in-memory engine. After this call, the region
    /// will not be readable, but the data of the region may not be deleted
    /// immediately due to some ongoing snapshots.
    pub fn evict_region(&self, region: &CacheRegion, evict_reason: EvictReason) {
        let deleteable_regions = self
            .core
            .write()
            .range_manager
            .evict_region(region, evict_reason);
        if !deleteable_regions.is_empty() {
            // The range can be deleted directly.
            if let Err(e) = self
                .bg_worker_manager()
                .schedule_task(BackgroundTask::DeleteRegions(deleteable_regions))
            {
                error!(
                    "ime schedule delete region failed";
                    "err" => ?e,
                );
                assert!(tikv_util::thread_group::is_shutdown(!cfg!(test)));
            }
        }
    }

    // It handles the pending range and check whether to buffer write for this
    // range.
    pub(crate) fn prepare_for_apply(
        &self,
        write_batch_id: u64,
        region: &CacheRegion,
    ) -> RangeCacheStatus {
        let mut core = self.core.write();
        let range_manager = core.mut_range_manager();
        let Some(mut region_state) = range_manager.check_region_state(region) else {
            return RangeCacheStatus::NotInCache;
        };

        let schedule_load = region_state == RegionState::Pending;
        if schedule_load {
            range_manager.update_region_state(region.id, RegionState::Loading);
            info!(
                "ime region to load";
                "region" => ?region,
                "cached" => range_manager.regions().len(),
            );
            region_state = RegionState::Loading;
        }

        let mut result = RangeCacheStatus::NotInCache;
        if region_state == RegionState::Loading || region_state == RegionState::Active {
            range_manager.record_in_region_being_written(write_batch_id, region.clone());
            if region_state == RegionState::Active {
                result = RangeCacheStatus::Cached;
            } else {
                result = RangeCacheStatus::Loading;
            }
        }
        drop(core);

        // get snapshot and schedule loading task at last to avoid locking IME for too
        // long.
        if schedule_load {
            let rocks_snap = Arc::new(self.rocks_engine.as_ref().unwrap().snapshot(None));
            if let Err(e) = self
                .bg_work_manager
                .schedule_task(BackgroundTask::LoadRegion(region.clone(), rocks_snap))
            {
                error!(
                    "ime schedule region load failed";
                    "err" => ?e,
                    "region" => ?region,
                );
                assert!(tikv_util::thread_group::is_shutdown(!cfg!(test)));
            }
        }

        result
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
        region: CacheRegion,
        read_ts: u64,
        seq_num: u64,
    ) -> result::Result<Self::Snapshot, FailedReason> {
        RangeCacheSnapshot::new(self.clone(), region, read_ts, seq_num)
    }

    type DiskEngine = RocksEngine;
    fn set_disk_engine(&mut self, disk_engine: Self::DiskEngine) {
        self.rocks_engine = Some(disk_engine.clone());
        if let Err(e) = self
            .bg_worker_manager()
            .schedule_task(BackgroundTask::SetRocksEngine(disk_engine))
        {
            error!(
                "ime schedule set rocks_engine failed";
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

    fn get_region_for_key(&self, key: &[u8]) -> Option<CacheRegion> {
        self.core.read().range_manager().get_region_for_key(key)
    }

    fn enabled(&self) -> bool {
        self.config.value().enabled
    }

    fn on_region_event(&self, event: RegionEvent) {
        match event {
            RegionEvent::Eviction { region, reason } => {
                self.evict_region(&region, reason);
            }
            RegionEvent::Split {
                source,
                new_regions,
            } => {
                self.core
                    .write()
                    .range_manager
                    .split_region(&source, new_regions);
            }
            RegionEvent::EvictByRange { range, reason } => {
                let mut regions = vec![];
                self.core()
                    .read()
                    .range_manager()
                    .iter_overlapped_regions(&range, |meta| {
                        assert!(meta.get_region().overlaps(&range));
                        regions.push(meta.get_region().clone());
                        true
                    });
                for r in regions {
                    self.evict_region(&r, reason);
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
    use engine_traits::{CacheRegion, CF_DEFAULT, CF_LOCK, CF_WRITE};
    use tikv_util::config::{ReadableSize, VersionTrack};

    use super::SkiplistEngine;
    use crate::{
        keys::{construct_key, construct_user_key, encode_key},
        memory_controller::MemoryController,
        range_manager::{CacheRegionMeta, RegionManager, RegionState::*},
        test_util::new_region,
        InternalBytes, RangeCacheEngineConfig, RangeCacheEngineContext, RangeCacheMemoryEngine,
        ValueType,
    };

    fn count_region(mgr: &RegionManager, mut f: impl FnMut(&CacheRegionMeta) -> bool) -> usize {
        mgr.regions().values().filter(|m| f(m)).count()
    }
    #[test]
    fn test_region_overlap_with_outdated_epoch() {
        let engine = RangeCacheMemoryEngine::new(RangeCacheEngineContext::new_for_tests(Arc::new(
            VersionTrack::new(RangeCacheEngineConfig::config_for_test()),
        )));
        let region1 = new_region(1, b"k1", b"k3");
        engine.load_region(region1).unwrap();

        let mut region2 = new_region(1, b"k1", b"k5");
        region2.mut_region_epoch().version = 2;
        engine.prepare_for_apply(1, &CacheRegion::from_region(&region2));
        assert_eq!(
            count_region(engine.core.read().range_manager(), |m| {
                matches!(m.get_state(), Pending | Loading)
            }),
            0
        );

        let region1 = new_region(1, b"k1", b"k3");
        engine.load_region(region1).unwrap();

        let mut region2 = new_region(1, b"k2", b"k5");
        region2.mut_region_epoch().version = 2;
        engine.prepare_for_apply(1, &CacheRegion::from_region(&region2));
        assert_eq!(
            count_region(engine.core.read().range_manager(), |m| {
                matches!(m.get_state(), Pending | Loading)
            }),
            0
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
                stop_load_limit_threshold: Some(ReadableSize(300)),
                soft_limit_threshold: Some(ReadableSize(300)),
                hard_limit_threshold: Some(ReadableSize(500)),
                expected_region_size: Some(ReadableSize::mb(20)),
                mvcc_amplification_threshold: 10,
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
            let range = CacheRegion::new(1, 0, start, end);
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
            stop_load_limit_threshold: Some(ReadableSize(300)),
            soft_limit_threshold: Some(ReadableSize(300)),
            hard_limit_threshold: Some(ReadableSize(500)),
            expected_region_size: Some(ReadableSize::mb(20)),
            mvcc_amplification_threshold: 10,
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

        let range = CacheRegion::new(1, 0, b"k1".to_vec(), b"k4".to_vec());
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
