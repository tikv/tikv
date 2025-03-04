// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Debug},
    ops::Bound,
    result,
    sync::{atomic::AtomicU64, Arc},
};

use crossbeam::epoch::{self, default_collector, Guard};
use crossbeam_skiplist::{
    base::{Entry, OwnedIter},
    SkipList,
};
use engine_rocks::RocksEngine;
use engine_traits::{
    CacheRegion, EvictReason, FailedReason, KvEngine, OnEvictFinishedCallback, RegionCacheEngine,
    RegionCacheEngineExt, RegionEvent, CF_DEFAULT, CF_LOCK, CF_WRITE, DATA_CFS,
};
use fail::fail_point;
use kvproto::metapb::Region;
use pd_client::PdClient;
use raftstore::{coprocessor::RegionInfoProvider, store::CasualRouter};
use slog_global::error;
use tikv_util::{config::VersionTrack, info, warn, worker::Scheduler};

use crate::{
    background::{BackgroundTask, BgWorkManager, PdRangeHintService},
    keys::{
        encode_key_for_boundary_with_mvcc, encode_key_for_boundary_without_mvcc, InternalBytes,
    },
    memory_controller::MemoryController,
    read::RegionCacheSnapshot,
    region_manager::{LoadFailedReason, RegionCacheStatus, RegionManager, RegionState},
    statistics::Statistics,
    InMemoryEngineConfig, InMemoryEngineContext,
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

/// A single global set of skiplists shared by all cached regions
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
        write!(f, "Region Memory Engine")
    }
}

pub struct RegionCacheMemoryEngineCore {
    pub(crate) engine: SkiplistEngine,
    pub(crate) region_manager: RegionManager,
}

impl Default for RegionCacheMemoryEngineCore {
    fn default() -> Self {
        Self::new()
    }
}

impl RegionCacheMemoryEngineCore {
    pub fn new() -> RegionCacheMemoryEngineCore {
        RegionCacheMemoryEngineCore {
            engine: SkiplistEngine::new(),
            region_manager: RegionManager::default(),
        }
    }

    pub fn engine(&self) -> SkiplistEngine {
        self.engine.clone()
    }

    pub fn region_manager(&self) -> &RegionManager {
        &self.region_manager
    }

    // It handles the pending range and check whether to buffer write for this
    // range.
    pub(crate) fn prepare_for_apply(
        &self,
        region: &CacheRegion,
        rocks_engine: Option<&RocksEngine>,
        scheduler: &Scheduler<BackgroundTask>,
        should_set_in_written: bool,
        in_flashback: bool,
    ) -> RegionCacheStatus {
        if !self.region_manager.is_active() {
            return RegionCacheStatus::NotInCache;
        }

        // fast path, only need to hold the read lock.
        {
            let regions_map = self.region_manager.regions_map.read();
            let Some(region_meta) = regions_map.region_meta(region.id) else {
                return RegionCacheStatus::NotInCache;
            };
            let state = region_meta.get_state();
            if state == RegionState::Active {
                if should_set_in_written {
                    region_meta.set_being_written();
                }
                return RegionCacheStatus::Cached;
            } else if state.is_evict() {
                return RegionCacheStatus::NotInCache;
            } else if state == RegionState::Loading {
                if should_set_in_written {
                    region_meta.set_being_written();
                }
                return RegionCacheStatus::Loading;
            }
        }

        // slow path, handle pending region
        let mut regions_map = self.region_manager.regions_map.write();
        let cached_count = regions_map.regions().len();
        let Some(mut region_meta) = regions_map.mut_region_meta(region.id) else {
            return RegionCacheStatus::NotInCache;
        };

        if region_meta.get_region().epoch_version < region.epoch_version || in_flashback {
            let meta = regions_map.remove_region(region.id);
            assert_eq!(meta.get_state(), RegionState::Pending);
            // try update outdated region.
            if !in_flashback && meta.can_be_updated_to(region) {
                info!("ime update outdated pending region";
                    "current_meta" => ?meta,
                    "new_region" => ?region);
                // the new region's range is smaller than removed region, so it is impossible to
                // be overlapped with other existing regions.
                regions_map.load_region(region.clone()).unwrap();
                region_meta = regions_map.mut_region_meta(region.id).unwrap();
            } else {
                fail_point!("ime_fail_to_schedule_load");
                info!("ime remove outdated pending region";
                    "pending_region" => ?meta.get_region(),
                    "new_region" => ?region);
                return RegionCacheStatus::NotInCache;
            }
        }

        let mut region_state = region_meta.get_state();
        let schedule_load = region_state == RegionState::Pending;
        if schedule_load {
            region_meta.set_state(RegionState::Loading);
            info!(
                "ime region to load";
                "region" => ?region,
                "cached" => cached_count,
            );
            region_state = RegionState::Loading;
        }

        let mut result = RegionCacheStatus::NotInCache;
        if region_state == RegionState::Loading || region_state == RegionState::Active {
            if should_set_in_written {
                region_meta.set_being_written();
            }
            if region_state == RegionState::Active {
                result = RegionCacheStatus::Cached;
            } else {
                result = RegionCacheStatus::Loading;
            }
        }
        drop(regions_map);

        // get snapshot and schedule loading task at last to avoid locking IME for too
        // long.
        if schedule_load {
            let rocks_snap = Arc::new(rocks_engine.unwrap().snapshot());
            if let Err(e) =
                scheduler.schedule(BackgroundTask::LoadRegion(region.clone(), rocks_snap))
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
#[derive(Clone)]
pub struct RegionCacheMemoryEngine {
    bg_work_manager: Arc<BgWorkManager>,
    pub(crate) core: Arc<RegionCacheMemoryEngineCore>,
    pub(crate) rocks_engine: Option<RocksEngine>,
    memory_controller: Arc<MemoryController>,
    statistics: Arc<Statistics>,
    config: Arc<VersionTrack<InMemoryEngineConfig>>,

    // The increment amount of tombstones in the lock cf.
    // When reaching to the threshold, a CleanLockTombstone task will be scheduled to clean lock cf
    // tombstones.
    pub(crate) lock_modification_bytes: Arc<AtomicU64>,
}

impl RegionCacheMemoryEngine {
    pub fn new(in_memory_engine_context: InMemoryEngineContext) -> Self {
        RegionCacheMemoryEngine::with_region_info_provider(in_memory_engine_context, None, None)
    }

    pub fn with_region_info_provider(
        in_memory_engine_context: InMemoryEngineContext,
        region_info_provider: Option<Arc<dyn RegionInfoProvider>>,
        raft_casual_router: Option<Box<dyn CasualRouter<RocksEngine>>>,
    ) -> Self {
        let core = Arc::new(RegionCacheMemoryEngineCore::new());
        let skiplist_engine = core.engine().clone();

        let InMemoryEngineContext {
            config,
            statistics,
            pd_client,
        } = in_memory_engine_context;
        assert!(config.value().enable);
        let memory_controller = Arc::new(MemoryController::new(config.clone(), skiplist_engine));

        let bg_work_manager = Arc::new(BgWorkManager::new(
            core.clone(),
            pd_client,
            config.clone(),
            memory_controller.clone(),
            region_info_provider,
            raft_casual_router,
        ));

        Self {
            core,
            rocks_engine: None,
            bg_work_manager,
            memory_controller,
            statistics,
            config,
            lock_modification_bytes: Arc::default(),
        }
    }

    pub fn new_region(&self, region: Region) {
        let cache_region = CacheRegion::from_region(&region);
        self.core.region_manager.new_region(cache_region);
    }

    pub fn load_region(&self, cache_region: CacheRegion) -> result::Result<(), LoadFailedReason> {
        fail_point!("ime_on_load_region", |_| { Ok(()) });
        self.core.region_manager().load_region(cache_region)
    }

    // Used for benchmark.
    pub fn must_set_region_state(&self, id: u64, state: RegionState) {
        let mut regions_map = self.core.region_manager().regions_map().write();
        let meta = regions_map.mut_region_meta(id).unwrap();
        meta.set_state(state);
    }

    /// Evict a region from the in-memory engine. After this call, the region
    /// will not be readable, but the data of the region may not be deleted
    /// immediately due to some ongoing snapshots.
    pub fn evict_region(
        &self,
        region: &CacheRegion,
        evict_reason: EvictReason,
        on_evict_finished: Option<OnEvictFinishedCallback>,
    ) {
        let deletable_regions =
            self.core
                .region_manager
                .evict_region(region, evict_reason, on_evict_finished);
        if !deletable_regions.is_empty() {
            // The region can be deleted directly.
            if let Err(e) = self
                .bg_worker_manager()
                .schedule_task(BackgroundTask::DeleteRegions(deletable_regions))
            {
                error!(
                    "ime schedule delete region failed";
                    "err" => ?e,
                );
                assert!(tikv_util::thread_group::is_shutdown(!cfg!(test)));
            }
        }
    }

    // It handles the pending region and check whether to buffer write for this
    // region.
    pub(crate) fn prepare_for_apply(
        &self,
        region: &CacheRegion,
        in_flashback: bool,
    ) -> RegionCacheStatus {
        self.core.prepare_for_apply(
            region,
            self.rocks_engine.as_ref(),
            self.bg_work_manager.background_scheduler(),
            true,
            in_flashback,
        )
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

    pub fn start_cross_check(
        &self,
        rocks_engine: RocksEngine,
        pd_client: Arc<dyn PdClient>,
        get_tikv_safe_point: Box<dyn Fn() -> Option<u64> + Send>,
    ) {
        let cross_check_interval = self.config.value().cross_check_interval;
        if !cross_check_interval.is_zero() {
            if let Err(e) =
                self.bg_worker_manager()
                    .schedule_task(BackgroundTask::TurnOnCrossCheck((
                        self.clone(),
                        rocks_engine,
                        pd_client,
                        cross_check_interval.0,
                        get_tikv_safe_point,
                    )))
            {
                error!(
                    "schedule TurnOnCrossCheck failed";
                    "err" => ?e,
                );
                assert!(tikv_util::thread_group::is_shutdown(!cfg!(test)));
            }
        }
    }
}

impl RegionCacheMemoryEngine {
    pub fn core(&self) -> &Arc<RegionCacheMemoryEngineCore> {
        &self.core
    }
}

impl Debug for RegionCacheMemoryEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Region Cache Memory Engine")
    }
}

impl RegionCacheEngine for RegionCacheMemoryEngine {
    type Snapshot = RegionCacheSnapshot;

    fn snapshot(
        &self,
        region: CacheRegion,
        read_ts: u64,
        seq_num: u64,
    ) -> result::Result<Self::Snapshot, FailedReason> {
        RegionCacheSnapshot::new(self.clone(), region, read_ts, seq_num)
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

    fn enabled(&self) -> bool {
        self.config.value().enable
    }
}

impl RegionCacheEngineExt for RegionCacheMemoryEngine {
    fn on_region_event(&self, event: RegionEvent) {
        match event {
            RegionEvent::Eviction {
                region,
                reason,
                on_evict_finished,
            } => {
                self.evict_region(&region, reason, on_evict_finished);
            }
            RegionEvent::TryLoad {
                region,
                for_manual_range,
            } => {
                if for_manual_range {
                    if self
                        .core
                        .region_manager()
                        .regions_map()
                        .read()
                        .overlap_with_manual_load_range(&region)
                    {
                        info!(
                            "ime try to load region in manual load range";
                            "region" => ?region,
                        );
                        if let Err(e) = self.load_region(region.clone()) {
                            warn!(
                                "ime load region failed";
                                "err" => ?e,
                                "region" => ?region,
                            );
                        }
                    }
                } else if let Err(e) = self.load_region(region.clone()) {
                    warn!(
                        "ime load region failed";
                        "error" => ?e,
                        "region" => ?region,
                    );
                }
            }
            RegionEvent::Split {
                source,
                new_regions,
            } => {
                self.core.region_manager.split_region(&source, new_regions);
            }
            RegionEvent::EvictByRange { range, reason } => {
                let mut regions = vec![];
                {
                    let regions_map = self.core.region_manager.regions_map.read();
                    regions_map.iter_overlapped_regions(&range, |meta| {
                        assert!(meta.get_region().overlaps(&range));
                        regions.push(meta.get_region().clone());
                        true
                    });
                }

                for r in regions {
                    self.evict_region(&r, reason, None);
                }
            }
        }
    }

    fn region_cached(&self, region: &Region) -> bool {
        let regions_map = self.core.region_manager().regions_map().read();
        if let Some(meta) = regions_map.region_meta(region.get_id()) {
            matches!(meta.get_state(), RegionState::Active | RegionState::Loading)
        } else {
            false
        }
    }

    fn load_region(&self, region: &Region) {
        self.on_region_event(RegionEvent::TryLoad {
            region: CacheRegion::from_region(region),
            for_manual_range: false,
        });
    }
}

#[cfg(test)]
pub mod tests {
    use std::{sync::Arc, time::Duration};

    use crossbeam::epoch;
    use engine_rocks::util::new_engine;
    use engine_traits::{
        CacheRegion, EvictReason, Mutable, RegionCacheEngine, RegionCacheEngineExt, RegionEvent,
        WriteBatch, WriteBatchExt, CF_DEFAULT, CF_LOCK, CF_WRITE, DATA_CFS,
    };
    use tikv_util::config::{ReadableDuration, VersionTrack};
    use tokio::{
        runtime::Builder,
        sync::{mpsc, Mutex},
        time::timeout,
    };

    use super::SkiplistEngine;
    use crate::{
        keys::{construct_key, construct_user_key, encode_key},
        memory_controller::MemoryController,
        region_manager::{CacheRegionMeta, RegionManager, RegionState::*},
        test_util::new_region,
        InMemoryEngineConfig, InMemoryEngineContext, InternalBytes, RegionCacheMemoryEngine,
        ValueType,
    };

    fn count_region(mgr: &RegionManager, mut f: impl FnMut(&CacheRegionMeta) -> bool) -> usize {
        let regions_map = mgr.regions_map.read();
        regions_map.regions().values().filter(|m| f(m)).count()
    }
    #[test]
    fn test_region_overlap_with_outdated_epoch() {
        let engine = RegionCacheMemoryEngine::new(InMemoryEngineContext::new_for_tests(Arc::new(
            VersionTrack::new(InMemoryEngineConfig::config_for_test()),
        )));
        let region1 = new_region(1, b"k1", b"k3");
        let cache_region1 = CacheRegion::from_region(&region1);
        engine.load_region(cache_region1).unwrap();

        let mut region2 = new_region(1, b"k1", b"k5");
        region2.mut_region_epoch().version = 2;
        engine.prepare_for_apply(&CacheRegion::from_region(&region2), false);
        assert_eq!(
            count_region(engine.core.region_manager(), |m| {
                matches!(m.get_state(), Pending | Loading)
            }),
            0
        );

        let region1 = new_region(1, b"k1", b"k3");
        let cache_region1 = CacheRegion::from_region(&region1);
        engine.load_region(cache_region1).unwrap();

        let mut region2 = new_region(1, b"k2", b"k5");
        region2.mut_region_epoch().version = 2;
        engine.prepare_for_apply(&CacheRegion::from_region(&region2), false);
        assert_eq!(
            count_region(engine.core.region_manager(), |m| {
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

            let config = Arc::new(VersionTrack::new(InMemoryEngineConfig::config_for_test()));
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

        let config = Arc::new(VersionTrack::new(InMemoryEngineConfig::config_for_test()));
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

    #[test]
    fn test_is_active() {
        let mut engine = RegionCacheMemoryEngine::new(InMemoryEngineContext::new_for_tests(
            Arc::new(VersionTrack::new(InMemoryEngineConfig::config_for_test())),
        ));
        let path = tempfile::Builder::new()
            .prefix("test_is_active")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();
        let rocks_engine = new_engine(path_str, DATA_CFS).unwrap();
        engine.set_disk_engine(rocks_engine.clone());

        let region = new_region(1, b"k00", b"k30");
        let cache_region = CacheRegion::from_region(&region);
        engine.load_region(cache_region.clone()).unwrap();
        assert!(engine.core.region_manager.is_active());

        let mut wb = engine.write_batch();
        wb.prepare_for_region(&region);
        wb.put(b"zk00", b"v1").unwrap();
        wb.put(b"zk10", b"v1").unwrap();
        wb.put(b"zk20", b"v1").unwrap();
        wb.set_sequence_number(1).unwrap();
        wb.write().unwrap();

        test_util::eventually(
            Duration::from_millis(10),
            Duration::from_millis(1000),
            || {
                let regions_map = engine.core.region_manager().regions_map().read();
                regions_map.region_meta(1).unwrap().get_state() == Active
            },
        );

        let mut wb = engine.write_batch();
        wb.prepare_for_region(&region);
        wb.put(b"zk10", b"v2").unwrap();
        wb.set_sequence_number(10).unwrap();

        // trigger split and eviction during write.
        let new_regions = vec![
            CacheRegion::new(1, 1, "zk00", "zk10"),
            CacheRegion::new(2, 1, "zk10", "zk20"),
            CacheRegion::new(3, 1, "zk20", "zk30"),
        ];
        engine.on_region_event(RegionEvent::Split {
            source: cache_region.clone(),
            new_regions: new_regions.clone(),
        });

        engine.on_region_event(RegionEvent::Eviction {
            region: new_regions[0].clone(),
            reason: EvictReason::AutoEvict,
            on_evict_finished: None,
        });

        // trigger split again
        let split_regions = vec![
            CacheRegion::new(2, 2, "zk10", "zk13"),
            CacheRegion::new(4, 2, "zk13", "zk16"),
            CacheRegion::new(5, 2, "zk16", "zk20"),
        ];
        engine.on_region_event(RegionEvent::Split {
            source: new_regions[1].clone(),
            new_regions: split_regions.clone(),
        });

        {
            let regions_map = engine.core.region_manager.regions_map.read();
            assert!(regions_map.regions().values().all(|m| m.is_written()));
        }
        wb.write().unwrap();
        {
            let regions_map = engine.core.region_manager.regions_map.read();
            assert!(regions_map.regions().values().all(|m| !m.is_written()));
        }

        engine.on_region_event(RegionEvent::Eviction {
            region: cache_region,
            reason: EvictReason::AutoEvict,
            on_evict_finished: None,
        });

        // engine should become inactive after all regions are evicted.
        test_util::eventually(
            Duration::from_millis(10),
            Duration::from_millis(1000),
            || !engine.core.region_manager.is_active(),
        );
    }

    #[test]
    fn test_cb_on_eviction_with_on_going_snapshot() {
        let mut config = InMemoryEngineConfig::config_for_test();
        config.gc_run_interval = ReadableDuration(Duration::from_secs(1));
        let engine = RegionCacheMemoryEngine::new(InMemoryEngineContext::new_for_tests(Arc::new(
            VersionTrack::new(config),
        )));

        let region = new_region(1, b"", b"z");
        let cache_region = CacheRegion::from_region(&region);
        engine.new_region(region.clone());

        let mut wb = engine.write_batch();
        wb.prepare_for_region(&region);
        wb.set_sequence_number(10).unwrap();
        wb.put(b"a", b"val1").unwrap();
        wb.put(b"b", b"val2").unwrap();
        wb.put(b"c", b"val3").unwrap();
        wb.write().unwrap();

        let snap = engine.snapshot(cache_region.clone(), 100, 100).unwrap();

        let (tx, rx) = mpsc::channel(1);
        engine.evict_region(
            &cache_region,
            EvictReason::BecomeFollower,
            Some(Box::new(move || {
                Box::pin(async move {
                    let _ = tx.send(()).await;
                })
            })),
        );

        let rt = Builder::new_current_thread().enable_all().build().unwrap();
        let rx = Arc::new(Mutex::new(rx));
        let rx_clone = rx.clone();
        rt.block_on(async move {
            timeout(Duration::from_secs(1), rx_clone.lock().await.recv())
                .await
                .unwrap_err()
        });
        drop(snap);
        rt.block_on(async move { rx.lock().await.recv().await.unwrap() });

        {
            let regions_map = engine.core().region_manager().regions_map().read();
            assert!(regions_map.region_meta(1).is_none());
        }
    }
}
