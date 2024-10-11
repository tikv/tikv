// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use bytes::Bytes;
use crossbeam::epoch;
use engine_traits::{
    CacheRegion, EvictReason, MiscExt, Mutable, RegionCacheEngine, Result, WriteBatch,
    WriteBatchExt, WriteOptions, CF_DEFAULT,
};
use raftstore::store::fsm::apply::PRINTF_LOG;
use tikv_util::{box_err, config::ReadableSize, error, info, time::Instant, warn};

use crate::{
    background::BackgroundTask,
    engine::{cf_to_id, id_to_cf, is_lock_cf, SkiplistEngine},
    keys::{encode_key, InternalBytes, ValueType, ENC_KEY_SEQ_LENGTH},
    memory_controller::{MemoryController, MemoryUsage},
    metrics::{
        IN_MEMORY_ENGINE_PREPARE_FOR_WRITE_DURATION_HISTOGRAM,
        IN_MEMORY_ENGINE_WRITE_DURATION_HISTOGRAM,
    },
    region_manager::RegionCacheStatus,
    RegionCacheMemoryEngine,
};

// This is a bit of a hack. It's the overhead of a node in the skiplist with
// height 3, which is sufficiently conservative for estimating the node overhead
// size.
pub(crate) const NODE_OVERHEAD_SIZE_EXPECTATION: usize = 96;
// As every key/value holds a Arc<MemoryController>, this overhead should be
// taken into consideration.
pub(crate) const MEM_CONTROLLER_OVERHEAD: usize = 8;
// A threshold that when the lock cf increment bytes exceed it, a
// CleanLockTombstone will be scheduled to cleanup the lock tombstones.
// It's somewhat like RocksDB flush memtables when the memtable reaches to a
// certain bytes so that the compactions may cleanup some tombstones. By
// default, the memtable size for lock cf is 32MB. As not all ranges will be
// cached in the memory, just use half of it here.
const AMOUNT_TO_CLEAN_TOMBSTONE: u64 = ReadableSize::mb(16).0;
// The value of the delete entry in the in-memory engine. It's just a emptry
// slice.
const DELETE_ENTRY_VAL: &[u8] = b"";

// `prepare_for_region` should be called before raft command apply for each peer
// delegate. It sets `region_cache_status` which is used to determine whether
// the writes of this peer should be buffered.
pub struct RegionCacheWriteBatch {
    // `region_cache_status` indicates whether the range is cached, loading data, or not cached. If
    // it is cached, we should buffer the write in `buffer` which is consumed during the write
    // is written in the kv engine. If it is loading data, we should buffer the write in
    // `pending_range_in_loading_buffer` which is cached in the memory engine and will be consumed
    // after the snapshot has been loaded.
    region_cache_status: RegionCacheStatus,
    buffer: Vec<RegionCacheWriteBatchEntry>,
    engine: RegionCacheMemoryEngine,
    save_points: Vec<usize>,
    sequence_number: Option<u64>,
    memory_controller: Arc<MemoryController>,
    memory_usage_reach_hard_limit: bool,
    current_region_evicted: bool,
    current_region: Option<CacheRegion>,
    // all the regions this write batch is written.
    written_regions: Vec<CacheRegion>,

    // record the total durations of the prepare work for write in the write batch
    prepare_for_write_duration: Duration,
}

impl std::fmt::Debug for RegionCacheWriteBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegionCacheWriteBatch")
            .field("buffer", &self.buffer)
            .field("save_points", &self.save_points)
            .field("sequence_number", &self.sequence_number)
            .finish()
    }
}

impl From<&RegionCacheMemoryEngine> for RegionCacheWriteBatch {
    fn from(engine: &RegionCacheMemoryEngine) -> Self {
        Self {
            region_cache_status: RegionCacheStatus::NotInCache,
            buffer: Vec::new(),
            engine: engine.clone(),
            save_points: Vec::new(),
            sequence_number: None,
            memory_controller: engine.memory_controller(),
            memory_usage_reach_hard_limit: false,
            current_region_evicted: false,
            prepare_for_write_duration: Duration::default(),
            current_region: None,
            written_regions: vec![],
        }
    }
}

impl RegionCacheWriteBatch {
    pub fn with_capacity(engine: &RegionCacheMemoryEngine, cap: usize) -> Self {
        Self {
            region_cache_status: RegionCacheStatus::NotInCache,
            buffer: Vec::with_capacity(cap),
            // cache_buffer should need small capacity
            engine: engine.clone(),
            save_points: Vec::new(),
            sequence_number: None,
            memory_controller: engine.memory_controller(),
            memory_usage_reach_hard_limit: false,
            current_region_evicted: false,
            prepare_for_write_duration: Duration::default(),
            current_region: None,
            written_regions: vec![],
        }
    }

    /// Trigger a CleanLockTombstone task if the accumulated lock cf
    /// modification exceeds the threshold (16MB).
    ///
    /// NB: Acquiring the RocksDB mutex is necessary to get the oldest snapshot,
    ///     so avoid calling this in any RocksDB callback (e.g., write batch
    ///     callback) to prevent potential deadlocks.
    pub fn maybe_compact_lock_cf(&self) {
        if self.engine.lock_modification_bytes.load(Ordering::Relaxed) > AMOUNT_TO_CLEAN_TOMBSTONE {
            // Use `swap` to only allow one schedule when multiple writers reaches the limit
            // concurrently.
            if self
                .engine
                .lock_modification_bytes
                .swap(0, Ordering::Relaxed)
                > AMOUNT_TO_CLEAN_TOMBSTONE
            {
                let rocks_engine = self.engine.rocks_engine.as_ref().unwrap();
                let last_seqno = rocks_engine.get_latest_sequence_number();
                let snapshot_seqno = self
                    .engine
                    .rocks_engine
                    .as_ref()
                    .unwrap()
                    .get_oldest_snapshot_sequence_number()
                    .unwrap_or(last_seqno);

                if let Err(e) = self
                    .engine
                    .bg_worker_manager()
                    .schedule_task(BackgroundTask::CleanLockTombstone(snapshot_seqno))
                {
                    error!(
                        "ime schedule lock tombstone cleanup failed";
                        "err" => ?e,
                    );
                    assert!(tikv_util::thread_group::is_shutdown(!cfg!(test)));
                }
            }
        }
    }

    /// Sets the sequence number for this batch. This should only be called
    /// prior to writing the batch.
    pub fn set_sequence_number(&mut self, seq: u64) -> Result<()> {
        if let Some(seqno) = self.sequence_number {
            return Err(box_err!("Sequence number {} already set", seqno));
        };
        self.sequence_number = Some(seq);
        Ok(())
    }

    // Note: `seq` is the sequence number of the first key in this write batch in
    // the RocksDB, which will be incremented automatically for each key, so
    // that all keys have unique sequence numbers.
    fn write_impl(&mut self, mut seq: u64) -> Result<()> {
        // record last region before flush.
        self.record_last_written_region();

        if PRINTF_LOG.load(Ordering::Relaxed) {
            info!(
                "write impl";
                "seq" => seq,
            );
        }

        fail::fail_point!("on_region_cache_write_batch_write_impl");
        let guard = &epoch::pin();
        let start = Instant::now();
        let mut lock_modification: u64 = 0;
        let engine = self.engine.core.engine();
        // Some entries whose ranges may be marked as evicted above, but it does not
        // matter, they will be deleted later.
        std::mem::take(&mut self.buffer).into_iter().for_each(|e| {
            if is_lock_cf(e.cf) {
                lock_modification += e.data_size() as u64;
            }
            e.write_to_memory(seq, &engine, self.memory_controller.clone(), guard);
            seq += 1;
        });
        let duration = start.saturating_elapsed_secs();
        IN_MEMORY_ENGINE_WRITE_DURATION_HISTOGRAM.observe(duration);

        fail::fail_point!("in_memory_engine_write_batch_consumed");
        fail::fail_point!("before_clear_ranges_in_being_written");

        if !self.written_regions.is_empty() {
            self.engine
                .core
                .region_manager()
                .clear_regions_in_being_written(&self.written_regions);
        }

        self.engine
            .lock_modification_bytes
            .fetch_add(lock_modification, Ordering::Relaxed);

        IN_MEMORY_ENGINE_PREPARE_FOR_WRITE_DURATION_HISTOGRAM
            .observe(self.prepare_for_write_duration.as_secs_f64());

        Ok(())
    }

    #[inline]
    pub fn set_region_cache_status(&mut self, region_cache_status: RegionCacheStatus) {
        self.region_cache_status = region_cache_status;
    }

    fn evict_current_region(&mut self, reason: EvictReason) {
        if self.current_region_evicted {
            return;
        }
        self.engine
            .evict_region(self.current_region.as_ref().unwrap(), reason, None);
        self.current_region_evicted = true;
    }

    fn process_cf_operation<F1, F2>(&mut self, entry_size: F1, entry: F2)
    where
        F1: FnOnce() -> usize,
        F2: FnOnce() -> RegionCacheWriteBatchEntry,
    {
        if self.region_cache_status == RegionCacheStatus::NotInCache || self.current_region_evicted
        {
            return;
        }

        if !self.engine.enabled() {
            let region = self.current_region.as_ref().unwrap();
            info!("ime range cache is disabled, evict the range"; "region" => ?region);
            self.evict_current_region(EvictReason::Disabled);
            return;
        }
        let memory_expect = entry_size();
        if !self.memory_acquire(memory_expect) {
            let region = self.current_region.as_ref().unwrap();
            info!("ime memory acquire failed due to reaching hard limit"; "region" => ?region);
            self.evict_current_region(EvictReason::MemoryLimitReached);
            return;
        }

        self.buffer.push(entry());
    }

    fn schedule_memory_check(&self) {
        if self.memory_controller.memory_checking() {
            return;
        }
        if !self.memory_controller.set_memory_checking(true) {
            if let Err(e) = self
                .engine
                .bg_worker_manager()
                .schedule_task(BackgroundTask::MemoryCheckAndEvict)
            {
                error!(
                    "ime schedule memory check failed";
                    "err" => ?e,
                );
                assert!(tikv_util::thread_group::is_shutdown(!cfg!(test)));
            }
        }
    }

    // return false means the memory usage reaches to hard limit and we have no
    // quota to write to the engine
    fn memory_acquire(&mut self, mem_required: usize) -> bool {
        match self.memory_controller.acquire(mem_required) {
            MemoryUsage::HardLimitReached(n) => {
                self.memory_usage_reach_hard_limit = true;
                warn!(
                    "ime the memory usage of in-memory engine reaches to hard limit";
                    "region" => ?self.current_region.as_ref().unwrap(),
                    "memory_usage(MB)" => ReadableSize(n as u64).as_mb_f64(),
                );
                self.schedule_memory_check();
                return false;
            }
            MemoryUsage::SoftLimitReached(_) => {
                self.schedule_memory_check();
            }
            _ => {}
        }
        true
    }

    #[inline]
    fn record_last_written_region(&mut self) {
        // NOTE: event if the region is evcited due to memory limit, we still
        // need to track it because its "in written" flag has been set.
        if self.region_cache_status != RegionCacheStatus::NotInCache {
            let last_region = self.current_region.take().unwrap();
            self.written_regions.push(last_region);
        }
    }
}

#[derive(Clone, Debug)]
enum WriteBatchEntryInternal {
    PutValue(Bytes),
    Deletion,
}

impl WriteBatchEntryInternal {
    fn encode(&self, key: &[u8], seq: u64) -> (InternalBytes, InternalBytes) {
        match self {
            WriteBatchEntryInternal::PutValue(value) => (
                encode_key(key, seq, ValueType::Value),
                InternalBytes::from_bytes(value.clone()),
            ),
            WriteBatchEntryInternal::Deletion => (
                encode_key(key, seq, ValueType::Deletion),
                InternalBytes::from_bytes(Bytes::from_static(DELETE_ENTRY_VAL)),
            ),
        }
    }

    fn value(&self) -> &[u8] {
        match self {
            WriteBatchEntryInternal::PutValue(value) => value,
            WriteBatchEntryInternal::Deletion => &[],
        }
    }

    fn data_size(&self) -> usize {
        match self {
            WriteBatchEntryInternal::PutValue(value) => value.len(),
            WriteBatchEntryInternal::Deletion => 0,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct RegionCacheWriteBatchEntry {
    cf: usize,
    key: Bytes,
    inner: WriteBatchEntryInternal,
}

impl RegionCacheWriteBatchEntry {
    pub fn put_value(cf: &str, key: &[u8], value: &[u8]) -> Self {
        Self {
            cf: cf_to_id(cf),
            key: Bytes::copy_from_slice(key),
            inner: WriteBatchEntryInternal::PutValue(Bytes::copy_from_slice(value)),
        }
    }

    pub fn deletion(cf: &str, key: &[u8]) -> Self {
        Self {
            cf: cf_to_id(cf),
            key: Bytes::copy_from_slice(key),
            inner: WriteBatchEntryInternal::Deletion,
        }
    }

    #[inline]
    pub fn encode(&self, seq: u64) -> (InternalBytes, InternalBytes) {
        self.inner.encode(&self.key, seq)
    }

    pub fn calc_put_entry_size(key: &[u8], value: &[u8]) -> usize {
        RegionCacheWriteBatchEntry::memory_size_required_for_key_value(key, value)
    }

    pub fn calc_delete_entry_size(key: &[u8]) -> usize {
        // delete also has value which is an empty bytes
        RegionCacheWriteBatchEntry::memory_size_required_for_key_value(key, DELETE_ENTRY_VAL)
    }

    fn memory_size_required_for_key_value(key: &[u8], value: &[u8]) -> usize {
        // The key will be encoded with sequence number when it is written to in-memory
        // engine, so we have to acquire the sequence number suffix memory usage.
        InternalBytes::memory_size_required(key.len() + ENC_KEY_SEQ_LENGTH)
            + InternalBytes::memory_size_required(value.len())
    }

    pub fn data_size(&self) -> usize {
        self.key.len() + ENC_KEY_SEQ_LENGTH + self.inner.data_size()
    }

    fn memory_size_required(&self) -> usize {
        Self::memory_size_required_for_key_value(&self.key, self.inner.value())
    }

    #[inline]
    pub fn write_to_memory(
        &self,
        seq: u64,
        skiplist_engine: &SkiplistEngine,
        memory_controller: Arc<MemoryController>,
        guard: &epoch::Guard,
    ) {
        let handle = skiplist_engine.cf_handle(id_to_cf(self.cf));

        let (mut key, mut value) = self.encode(seq);
        key.set_memory_controller(memory_controller.clone());
        value.set_memory_controller(memory_controller);
        handle.insert(key, value, guard);

        if PRINTF_LOG.load(Ordering::Relaxed) {
            info!(
                "write to memory";
                "entry" => ?self,
                "seqno" => seq,
            );
        }
    }
}

impl WriteBatchExt for RegionCacheMemoryEngine {
    type WriteBatch = RegionCacheWriteBatch;
    // todo: adjust it
    const WRITE_BATCH_MAX_KEYS: usize = 256;

    fn write_batch(&self) -> Self::WriteBatch {
        RegionCacheWriteBatch::from(self)
    }

    fn write_batch_with_cap(&self, cap: usize) -> Self::WriteBatch {
        RegionCacheWriteBatch::with_capacity(self, cap)
    }
}

impl WriteBatch for RegionCacheWriteBatch {
    fn write_opt(&mut self, _: &WriteOptions) -> Result<u64> {
        self.sequence_number
            .map(|seq| self.write_impl(seq).map(|()| seq))
            .transpose()
            .map(|o| o.ok_or_else(|| box_err!("sequence_number must be set!")))?
    }

    fn data_size(&self) -> usize {
        self.buffer
            .iter()
            .map(RegionCacheWriteBatchEntry::data_size)
            .sum()
    }

    fn count(&self) -> usize {
        self.buffer.len()
    }

    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    fn should_write_to_engine(&self) -> bool {
        unimplemented!()
    }

    fn clear(&mut self) {
        self.region_cache_status = RegionCacheStatus::NotInCache;
        self.buffer.clear();
        self.save_points.clear();
        self.sequence_number = None;
        self.memory_usage_reach_hard_limit = false;
        self.current_region_evicted = false;
        self.current_region = None;
        self.written_regions.clear();
        self.prepare_for_write_duration = Duration::ZERO;
    }

    fn set_save_point(&mut self) {
        self.save_points.push(self.buffer.len())
    }

    fn pop_save_point(&mut self) -> Result<()> {
        self.save_points
            .pop()
            .map(|_| ())
            .ok_or_else(|| box_err!("no save points available"))
    }

    fn rollback_to_save_point(&mut self) -> Result<()> {
        self.save_points
            .pop()
            .map(|sp| {
                self.buffer.truncate(sp);
            })
            .ok_or_else(|| box_err!("no save point available!"))
    }

    fn merge(&mut self, mut other: Self) -> Result<()> {
        self.buffer.append(&mut other.buffer);
        Ok(())
    }

    fn prepare_for_region(&mut self, region: CacheRegion) {
        let time = Instant::now();
        // record last region for clearing region in written flags.
        self.record_last_written_region();

        // TODO: remote range.
        self.set_region_cache_status(self.engine.prepare_for_apply(&region));
        self.current_region = Some(region);
        self.memory_usage_reach_hard_limit = false;
        self.current_region_evicted = false;
        self.prepare_for_write_duration += time.saturating_elapsed();
    }
}

impl Mutable for RegionCacheWriteBatch {
    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        self.put_cf(CF_DEFAULT, key, val)
    }

    fn put_cf(&mut self, cf: &str, key: &[u8], val: &[u8]) -> Result<()> {
        self.process_cf_operation(
            || RegionCacheWriteBatchEntry::calc_put_entry_size(key, val),
            || RegionCacheWriteBatchEntry::put_value(cf, key, val),
        );
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.delete_cf(CF_DEFAULT, key)
    }

    fn delete_cf(&mut self, cf: &str, key: &[u8]) -> Result<()> {
        self.process_cf_operation(
            || RegionCacheWriteBatchEntry::calc_delete_entry_size(key),
            || RegionCacheWriteBatchEntry::deletion(cf, key),
        );
        Ok(())
    }

    // rather than delete the keys in the range, we evict ranges that overlap with
    // them directly
    fn delete_range(&mut self, _begin_key: &[u8], _end_key: &[u8]) -> Result<()> {
        self.evict_current_region(EvictReason::DeleteRange);
        Ok(())
    }

    fn delete_range_cf(&mut self, _: &str, _begin_key: &[u8], _end_key: &[u8]) -> Result<()> {
        self.evict_current_region(EvictReason::DeleteRange);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use crossbeam_skiplist::SkipList;
    use engine_rocks::util::new_engine;
    use engine_traits::{
        CacheRegion, FailedReason, Peekable, RegionCacheEngine, WriteBatch, DATA_CFS,
    };
    use online_config::{ConfigChange, ConfigManager, ConfigValue};
    use tempfile::Builder;
    use tikv_util::config::VersionTrack;

    use super::*;
    use crate::{
        background::flush_epoch, config::RegionCacheConfigManager, region_manager::RegionState,
        test_util::new_region, InMemoryEngineConfig, InMemoryEngineContext,
    };

    // We should not use skiplist.get directly as we only cares keys without
    // sequence number suffix
    fn get_value(
        sl: &Arc<SkipList<InternalBytes, InternalBytes>>,
        key: &InternalBytes,
        guard: &epoch::Guard,
    ) -> Option<Vec<u8>> {
        let mut iter = sl.owned_iter();
        iter.seek(key, guard);
        if iter.valid() && iter.key().same_user_key_with(key) {
            return Some(iter.value().as_slice().to_vec());
        }
        None
    }

    #[test]
    fn test_write_to_skiplist() {
        let engine = RegionCacheMemoryEngine::new(InMemoryEngineContext::new_for_tests(Arc::new(
            VersionTrack::new(InMemoryEngineConfig::config_for_test()),
        )));
        let r = new_region(1, b"", b"z");
        engine.new_region(r.clone());
        engine.core.region_manager().set_safe_point(r.id, 10);

        let mut wb = RegionCacheWriteBatch::from(&engine);
        wb.prepare_for_region(CacheRegion::from_region(&r));
        wb.put(b"aaa", b"bbb").unwrap();
        wb.set_sequence_number(1).unwrap();
        assert_eq!(wb.write().unwrap(), 1);
        let sl = engine.core.engine().data[cf_to_id(CF_DEFAULT)].clone();
        let guard = &crossbeam::epoch::pin();
        let val = get_value(&sl, &encode_key(b"aaa", 2, ValueType::Value), guard).unwrap();
        assert_eq!(&b"bbb"[..], val.as_slice());
    }

    #[test]
    fn test_savepoints() {
        let engine = RegionCacheMemoryEngine::new(InMemoryEngineContext::new_for_tests(Arc::new(
            VersionTrack::new(InMemoryEngineConfig::config_for_test()),
        )));
        let r = new_region(1, b"", b"z");
        engine.new_region(r.clone());
        engine.core.region_manager().set_safe_point(r.id, 10);

        let mut wb = RegionCacheWriteBatch::from(&engine);
        wb.prepare_for_region(CacheRegion::from_region(&r));
        wb.put(b"aaa", b"bbb").unwrap();
        wb.set_save_point();
        wb.put(b"aaa", b"ccc").unwrap();
        wb.put(b"ccc", b"ddd").unwrap();
        wb.rollback_to_save_point().unwrap();
        wb.set_sequence_number(1).unwrap();
        assert_eq!(wb.write().unwrap(), 1);
        let sl = engine.core.engine().data[cf_to_id(CF_DEFAULT)].clone();
        let guard = &crossbeam::epoch::pin();
        let val = get_value(&sl, &encode_key(b"aaa", 1, ValueType::Value), guard).unwrap();
        assert_eq!(&b"bbb"[..], val.as_slice());
        assert!(get_value(&sl, &encode_key(b"ccc", 1, ValueType::Value), guard).is_none())
    }

    #[test]
    fn test_put_write_clear_delete_put_write() {
        let engine = RegionCacheMemoryEngine::new(InMemoryEngineContext::new_for_tests(Arc::new(
            VersionTrack::new(InMemoryEngineConfig::config_for_test()),
        )));
        let r = new_region(1, b"", b"z");
        engine
            .core
            .region_manager()
            .new_region(CacheRegion::from_region(&r));
        engine.core.region_manager().set_safe_point(r.id, 10);

        let mut wb = RegionCacheWriteBatch::from(&engine);
        wb.prepare_for_region(CacheRegion::from_region(&r));
        wb.put(b"zaaa", b"bbb").unwrap();
        wb.set_sequence_number(1).unwrap();
        _ = wb.write();
        wb.clear();
        wb.prepare_for_region(CacheRegion::from_region(&r));
        wb.put(b"zbbb", b"ccc").unwrap();
        wb.delete(b"zaaa").unwrap();
        wb.set_sequence_number(2).unwrap();
        _ = wb.write();
        let snapshot = engine
            .snapshot(CacheRegion::from_region(&r), u64::MAX, 3)
            .unwrap();
        assert_eq!(
            snapshot.get_value(&b"zbbb"[..]).unwrap().unwrap(),
            &b"ccc"[..]
        );
        assert!(snapshot.get_value(&b"zaaa"[..]).unwrap().is_none())
    }

    #[test]
    fn test_prepare_for_apply() {
        let path = Builder::new()
            .prefix("test_prepare_for_apply")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();
        let rocks_engine = new_engine(path_str, DATA_CFS).unwrap();

        let mut engine = RegionCacheMemoryEngine::new(InMemoryEngineContext::new_for_tests(
            Arc::new(VersionTrack::new(InMemoryEngineConfig::config_for_test())),
        ));
        engine.set_disk_engine(rocks_engine.clone());

        let r1 = new_region(1, b"k01".to_vec(), b"k05".to_vec());
        {
            // load region with epoch and range change, will remove the pending region.
            let cache_r1 = CacheRegion::from_region(&r1);
            engine.load_region(cache_r1).unwrap();
            let mut r1_new = new_region(1, b"k01".to_vec(), b"k06".to_vec());
            r1_new.mut_region_epoch().version = 2;
            let mut wb = RegionCacheWriteBatch::from(&engine);
            wb.prepare_for_region(CacheRegion::from_region(&r1_new));
            assert!(
                engine
                    .core
                    .region_manager()
                    .regions_map
                    .read()
                    .regions()
                    .is_empty()
            );
            wb.put(b"zk01", b"val1").unwrap();
            wb.put(b"zk03", b"val1").unwrap();
            wb.put(b"zk05", b"val1").unwrap();
            wb.set_sequence_number(2).unwrap();
            wb.write().unwrap();
        }
        // pending region is removed, no data should write to skiplist.
        let skip_engine = engine.core.engine();
        assert_eq!(skip_engine.node_count(), 0);

        // epoch changes but new range is contained by the pending region, will update
        // the region.
        let cache_r1 = CacheRegion::from_region(&r1);
        engine.load_region(cache_r1).unwrap();
        let mut r1_new = new_region(1, b"k01".to_vec(), b"k05".to_vec());
        r1_new.mut_region_epoch().version = 2;
        let cache_r1_new = CacheRegion::from_region(&r1_new);
        let mut wb = RegionCacheWriteBatch::from(&engine);
        wb.prepare_for_region(cache_r1_new.clone());
        {
            let regions_map = engine.core.region_manager().regions_map.read();
            let region_meta = regions_map.region_meta(1).unwrap();
            assert_eq!(region_meta.get_region(), &cache_r1_new);
            assert_eq!(region_meta.get_state(), RegionState::Loading);
        }
        wb.put(b"zk02", b"val1").unwrap();
        wb.put(b"zk04", b"val1").unwrap();
        wb.set_sequence_number(5).unwrap();
        wb.write().unwrap();

        test_util::eventually(
            Duration::from_millis(50),
            Duration::from_millis(1000),
            || {
                let regions_map = engine.core.region_manager().regions_map.read();
                regions_map.region_meta(1).unwrap().get_state() == RegionState::Active
            },
        );
        let snapshot = engine.snapshot(cache_r1_new.clone(), u64::MAX, 6).unwrap();
        for i in 1..5 {
            let res = snapshot.get_value(format!("zk0{}", i).as_bytes()).unwrap();
            if i % 2 == 0 {
                assert_eq!(res.unwrap(), b"val1".as_slice());
            } else {
                assert!(res.is_none());
            }
        }
        assert_eq!(skip_engine.node_count(), 2);

        let mut wb = RegionCacheWriteBatch::from(&engine);
        wb.prepare_for_region(cache_r1_new.clone());
        wb.put(b"zk01", b"val2").unwrap();
        wb.set_sequence_number(6).unwrap();
        wb.write().unwrap();

        assert_eq!(skip_engine.node_count(), 3);

        // evict region, data should not be updated.
        {
            let mut regions_map = engine.core.region_manager().regions_map.write();
            regions_map
                .mut_region_meta(1)
                .unwrap()
                .set_state(RegionState::PendingEvict);
        }
        let mut wb = RegionCacheWriteBatch::from(&engine);
        wb.prepare_for_region(cache_r1_new.clone());
        wb.put(b"zk02", b"val2").unwrap();
        wb.set_sequence_number(7).unwrap();
        wb.write().unwrap();
        // node count should not change.
        assert_eq!(skip_engine.node_count(), 3);
    }

    fn wait_evict_done(engine: &RegionCacheMemoryEngine) {
        test_util::eventually(
            Duration::from_millis(100),
            Duration::from_millis(2000),
            || {
                let regions_map = engine.core.region_manager.regions_map().read();
                !regions_map
                    .regions()
                    .values()
                    .any(|meta| meta.get_state().is_evict())
            },
        );
    }

    #[test]
    fn test_write_batch_with_memory_controller() {
        let mut config = InMemoryEngineConfig::default();
        config.soft_limit_threshold = Some(ReadableSize(500));
        config.hard_limit_threshold = Some(ReadableSize(1000));
        config.enabled = true;
        let engine = RegionCacheMemoryEngine::new(InMemoryEngineContext::new_for_tests(Arc::new(
            VersionTrack::new(config),
        )));
        let regions = [
            new_region(1, b"k00", b"k10"),
            new_region(2, b"k10", b"k20"),
            new_region(3, b"k20", b"k30"),
            new_region(4, b"k30", b"k40"),
            new_region(5, b"k40", b"k50"),
        ];
        for r in &regions {
            engine.new_region(r.clone());
            engine.core.region_manager().set_safe_point(r.id, 10);
            let _ = engine
                .snapshot(CacheRegion::from_region(r), 1000, 1000)
                .unwrap();
        }
        let memory_controller = engine.memory_controller();

        let val1: Vec<u8> = vec![0; 150];
        let mut wb = RegionCacheWriteBatch::from(&engine);
        wb.prepare_for_region(CacheRegion::from_region(&regions[0]));
        // memory required:
        // 4(key) + 8(sequencen number) + 150(value) + 16(2 Arc<MemoryController) = 178
        wb.put(b"zk01", &val1).unwrap();
        wb.prepare_for_region(CacheRegion::from_region(&regions[1]));
        // Now, 356
        wb.put(b"zk11", &val1).unwrap();
        assert_eq!(356, memory_controller.mem_usage());
        assert_eq!(wb.count(), 2);
        wb.prepare_for_region(CacheRegion::from_region(&regions[2]));

        // Now, 534
        wb.put(b"zk21", &val1).unwrap();
        // memory required:
        // 4(key) + 8(sequence number) + 16(2 Arc<MemoryController>) = 28
        // Now, 562
        wb.delete(b"zk21").unwrap();
        assert_eq!(562, memory_controller.mem_usage());
        assert_eq!(wb.count(), 4);

        let val2: Vec<u8> = vec![2; 500];
        // The memory will fail to acquire
        wb.put(b"zk22", &val2).unwrap();

        wb.prepare_for_region(CacheRegion::from_region(&regions[3]));
        // region 2 is evicted due to memory insufficient,
        // after preprare, all region 2's entries are removed.
        assert_eq!(356, memory_controller.mem_usage());
        assert_eq!(wb.count(), 2);

        // The memory capacity is enough for the following two inserts
        // Now, 534
        let val3: Vec<u8> = vec![3; 150];
        wb.put(b"zk32", &val3).unwrap();
        assert_eq!(534, memory_controller.mem_usage());

        // Now, 862
        let val4: Vec<u8> = vec![3; 300];
        wb.prepare_for_region(CacheRegion::from_region(&regions[4]));
        wb.put(b"zk41", &val4).unwrap();

        // We should have allocated 740 as calculated above
        assert_eq!(862, memory_controller.mem_usage());
        wb.write_impl(1000).unwrap();
        // We dont count the node overhead (96 bytes for each node) in write batch, so
        // after they are written into the engine, the mem usage can even exceed
        // the hard limit. But this should be fine as this amount should be at
        // most MB level.
        assert_eq!(1246, memory_controller.mem_usage());

        let snap1 = engine
            .snapshot(CacheRegion::from_region(&regions[0]), 1000, 1010)
            .unwrap();
        assert_eq!(snap1.get_value(b"zk01").unwrap().unwrap(), &val1);
        let snap2 = engine
            .snapshot(CacheRegion::from_region(&regions[1]), 1000, 1010)
            .unwrap();
        assert_eq!(snap2.get_value(b"zk11").unwrap().unwrap(), &val1);

        assert_eq!(
            engine
                .snapshot(CacheRegion::from_region(&regions[2]), 1000, 1000)
                .unwrap_err(),
            FailedReason::NotCached
        );

        let snap4 = engine
            .snapshot(CacheRegion::from_region(&regions[3]), 1000, 1010)
            .unwrap();
        assert_eq!(snap4.get_value(b"zk32").unwrap().unwrap(), &val3);

        let _snap5 = engine
            .snapshot(CacheRegion::from_region(&regions[4]), 1000, 1010)
            .unwrap();

        drop(snap1);
        engine.evict_region(
            &CacheRegion::from_region(&regions[0]),
            EvictReason::AutoEvict,
            None,
        );
        wait_evict_done(&engine);
        flush_epoch();
        assert_eq!(972, memory_controller.mem_usage());
    }

    #[test]
    fn test_write_batch_with_config_change() {
        let mut config = InMemoryEngineConfig::default();
        config.soft_limit_threshold = Some(ReadableSize(u64::MAX));
        config.hard_limit_threshold = Some(ReadableSize(u64::MAX));
        config.enabled = true;
        let config = Arc::new(VersionTrack::new(config));
        let engine =
            RegionCacheMemoryEngine::new(InMemoryEngineContext::new_for_tests(config.clone()));
        let r1 = new_region(1, b"kk00".to_vec(), b"kk10".to_vec());
        let r2 = new_region(2, b"kk10".to_vec(), b"kk20".to_vec());
        for r in [&r1, &r2] {
            engine.new_region(r.clone());
            engine.core.region_manager().set_safe_point(r.id, 10);
            let _ = engine
                .snapshot(CacheRegion::from_region(r), 1000, 1000)
                .unwrap();
        }

        let val1: Vec<u8> = (0..150).map(|_| 0).collect();
        let mut wb = RegionCacheWriteBatch::from(&engine);
        wb.prepare_for_region(CacheRegion::from_region(&r2));
        wb.put(b"zkk11", &val1).unwrap();
        let snap1 = engine
            .snapshot(CacheRegion::from_region(&r1), 1000, 1000)
            .unwrap();

        // disable the range cache
        let mut config_manager = RegionCacheConfigManager(config.clone());
        let mut config_change = ConfigChange::new();
        config_change.insert(String::from("enabled"), ConfigValue::Bool(false));
        config_manager.dispatch(config_change).unwrap();

        wb.write_impl(1000).unwrap();
        // existing snapshot can still work after the range cache is disabled, but new
        // snapshot will fail to create
        assert!(snap1.get_value(b"zkk00").unwrap().is_none());

        let mut wb = RegionCacheWriteBatch::from(&engine);
        wb.prepare_for_region(CacheRegion::from_region(&r1));
        // put should trigger the evict and it won't write into range cache
        wb.put(b"zkk01", &val1).unwrap();
        wb.write_impl(1000).unwrap();

        // new snapshot will fail to create as it's evicted already
        let snap1 = engine.snapshot(CacheRegion::from_region(&r1), 1000, 1000);
        assert_eq!(snap1.unwrap_err(), FailedReason::NotCached);
        let snap2 = engine
            .snapshot(CacheRegion::from_region(&r2), 1000, 1000)
            .unwrap();
        // if no new write, the range cache can still be used.
        assert_eq!(snap2.get_value(b"zkk11").unwrap().unwrap(), &val1);

        // enable the range cache again
        let mut config_manager = RegionCacheConfigManager(config.clone());
        let mut config_change = ConfigChange::new();
        config_change.insert(String::from("enabled"), ConfigValue::Bool(true));
        config_manager.dispatch(config_change).unwrap();

        let snap1 = engine.snapshot(CacheRegion::from_region(&r1), 1000, 1000);
        assert_eq!(snap1.unwrap_err(), FailedReason::NotCached);
        let snap2 = engine
            .snapshot(CacheRegion::from_region(&r2), 1000, 1000)
            .unwrap();
        assert_eq!(snap2.get_value(b"zkk11").unwrap().unwrap(), &val1);
    }

    #[test]
    fn test_write_batch_update_outdated_pending_region() {
        let path = Builder::new()
            .prefix("test_write_batch_update_outdated_pending_region")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();
        let rocks_engine = new_engine(path_str, DATA_CFS).unwrap();

        let mut engine = RegionCacheMemoryEngine::new(InMemoryEngineContext::new_for_tests(
            Arc::new(VersionTrack::new(InMemoryEngineConfig::config_for_test())),
        ));
        engine.set_disk_engine(rocks_engine.clone());

        let r1 = CacheRegion::new(1, 0, b"k00", b"k10");

        engine.core().region_manager().load_region(r1).unwrap();

        // load a region with a newer epoch and small range, should trigger replace.
        let r_new = CacheRegion::new(1, 1, b"k00", b"k05");
        let mut wb = RegionCacheWriteBatch::from(&engine);
        wb.prepare_for_region(r_new.clone());

        {
            let regions_map = engine.core.region_manager.regions_map().read();
            let cache_meta = regions_map.region_meta(1).unwrap();
            assert_eq!(cache_meta.get_region(), &r_new);
            let meta_by_range = regions_map.region_meta_by_end_key(&r_new.end).unwrap();
            assert_eq!(meta_by_range.get_region(), &r_new);
        }
    }
}
