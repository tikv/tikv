// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::BTreeMap,
    fmt::{self, Debug},
    ops::Bound,
    result,
    sync::Arc,
};

use crossbeam::epoch::{self, default_collector, Guard};
use engine_rocks::RocksEngine;
use engine_traits::{
    CacheRange, FailedReason, IterOptions, Iterable, KvEngine, RangeCacheEngine, Result,
    CF_DEFAULT, CF_LOCK, CF_WRITE, DATA_CFS,
};
use parking_lot::{lock_api::RwLockUpgradableReadGuard, RwLock, RwLockWriteGuard};
use raftstore::coprocessor::RegionInfoProvider;
use skiplist_rs::{
    base::{Entry, OwnedIter},
    SkipList,
};
use slog_global::error;
use tikv_util::{config::VersionTrack, info};

use crate::{
    background::{BackgroundTask, BgWorkManager, PdRangeHintService},
    keys::{encode_key_for_eviction, InternalBytes},
    memory_controller::MemoryController,
    range_manager::{LoadFailedReason, RangeCacheStatus, RangeManager},
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

pub struct RangeCacheMemoryEngineCore {
    pub(crate) engine: SkiplistEngine,
    pub(crate) range_manager: RangeManager,
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
    statistics: Arc<Statistics>,
    config: Arc<VersionTrack<RangeCacheEngineConfig>>,
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

        let RangeCacheEngineContext { config, statistics } = range_cache_engine_context;
        assert!(config.value().enabled);
        let memory_controller = Arc::new(MemoryController::new(config.clone(), skiplist_engine));

        let bg_work_manager = Arc::new(BgWorkManager::new(
            core.clone(),
            config.value().gc_interval.0,
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

        let mut overlapped = false;
        // check whether the range is in pending_range and we can schedule load task if
        // it is
        if let Some((idx, (left_range, right_range))) = range_manager
            .pending_ranges
            .iter()
            .enumerate()
            .find_map(|(idx, pending_range)| {
                if pending_range.contains_range(range) {
                    // The `range` may be a proper subset of `r` and we should split it in this case
                    // and push the rest back to `pending_range` so that each range only schedules
                    // load task of its own.
                    Some((idx, pending_range.split_off(range)))
                } else if range.overlaps(pending_range) {
                    // Pending range `range` does not contains the applying range `r` but overlap
                    // with it, which means the pending range is out dated, we remove it directly.
                    info!(
                        "out of date pending ranges";
                        "applying_range" => ?range,
                        "pending_range" => ?pending_range,
                    );
                    overlapped = true;
                    Some((idx, (None, None)))
                } else {
                    None
                }
            })
        {
            let mut core = RwLockUpgradableReadGuard::upgrade(core);

            if overlapped {
                core.mut_range_manager().pending_ranges.swap_remove(idx);
                return RangeCacheStatus::NotInCache;
            }

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
            info!(
                "Range to load";
                "Tag" => &range.tag,
                "Cached" => range_manager.ranges().len(),
                "Pending" => range_manager.pending_ranges_loading_data.len(),
            );
            if let Err(e) = self
                .bg_worker_manager()
                .schedule_task(BackgroundTask::LoadRange)
            {
                error!(
                    "schedule range load failed";
                    "err" => ?e,
                    "tag" => &range.tag,
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

    pub(crate) fn statistics(&self) -> Arc<Statistics> {
        self.statistics.clone()
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

    fn enabled(&self) -> bool {
        self.config.value().enabled
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

    use engine_traits::CacheRange;
    use tikv_util::config::VersionTrack;

    use crate::{RangeCacheEngineConfig, RangeCacheEngineContext, RangeCacheMemoryEngine};

    #[test]
    fn test_overlap_with_pending() {
        let engine = RangeCacheMemoryEngine::new(RangeCacheEngineContext::new(Arc::new(
            VersionTrack::new(RangeCacheEngineConfig::config_for_test()),
        )));
        let range1 = CacheRange::new(b"k1".to_vec(), b"k3".to_vec());
        engine.load_range(range1).unwrap();

        let range2 = CacheRange::new(b"k1".to_vec(), b"k5".to_vec());
        engine.prepare_for_apply(&range2);
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
        engine.prepare_for_apply(&range2);
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
}
