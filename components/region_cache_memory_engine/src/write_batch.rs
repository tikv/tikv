use std::{
    collections::BTreeSet,
    sync::{atomic::Ordering, Arc},
};

use bytes::Bytes;
use crossbeam::epoch;
use engine_traits::{
    CacheRange, MiscExt, Mutable, RangeCacheEngine, Result, WriteBatch, WriteBatchExt,
    WriteOptions, CF_DEFAULT,
};
use tikv_util::{box_err, config::ReadableSize, error, info, time::Instant, warn};

use crate::{
    background::BackgroundTask,
    engine::{cf_to_id, id_to_cf, is_lock_cf, SkiplistEngine},
    keys::{encode_key, InternalBytes, ValueType, ENC_KEY_SEQ_LENGTH},
    memory_controller::{MemoryController, MemoryUsage},
    metrics::WRITE_DURATION_HISTOGRAM,
    range_manager::{RangeCacheStatus, RangeManager},
    RangeCacheMemoryEngine,
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

// `prepare_for_range` should be called before raft command apply for each peer
// delegate. It sets `range_cache_status` which is used to determine whether the
// writes of this peer should be buffered.
pub struct RangeCacheWriteBatch {
    // `range_cache_status` indicates whether the range is cached, loading data, or not cached. If
    // it is cached, we should buffer the write in `buffer` which is consumed during the write
    // is written in the kv engine. If it is loading data, we should buffer the write in
    // `pending_range_in_loading_buffer` which is cached in the memory engine and will be consumed
    // after the snapshot has been loaded.
    range_cache_status: RangeCacheStatus,
    buffer: Vec<RangeCacheWriteBatchEntry>,
    pending_range_in_loading_buffer: Vec<RangeCacheWriteBatchEntry>,
    engine: RangeCacheMemoryEngine,
    save_points: Vec<usize>,
    sequence_number: Option<u64>,
    memory_controller: Arc<MemoryController>,
    memory_usage_reach_hard_limit: bool,

    current_range: Option<CacheRange>,
    // the ranges that reaches the hard limit and need to be evicted
    ranges_to_evict: BTreeSet<CacheRange>,
}

impl std::fmt::Debug for RangeCacheWriteBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RangeCacheWriteBatch")
            .field("buffer", &self.buffer)
            .field("save_points", &self.save_points)
            .field("sequence_number", &self.sequence_number)
            .finish()
    }
}

impl From<&RangeCacheMemoryEngine> for RangeCacheWriteBatch {
    fn from(engine: &RangeCacheMemoryEngine) -> Self {
        Self {
            range_cache_status: RangeCacheStatus::NotInCache,
            buffer: Vec::new(),
            pending_range_in_loading_buffer: Vec::new(),
            engine: engine.clone(),
            save_points: Vec::new(),
            sequence_number: None,
            memory_controller: engine.memory_controller(),
            memory_usage_reach_hard_limit: false,
            current_range: None,
            ranges_to_evict: BTreeSet::default(),
        }
    }
}

impl RangeCacheWriteBatch {
    pub fn with_capacity(engine: &RangeCacheMemoryEngine, cap: usize) -> Self {
        Self {
            range_cache_status: RangeCacheStatus::NotInCache,
            buffer: Vec::with_capacity(cap),
            // cache_buffer should need small capacity
            pending_range_in_loading_buffer: Vec::new(),
            engine: engine.clone(),
            save_points: Vec::new(),
            sequence_number: None,
            memory_controller: engine.memory_controller(),
            memory_usage_reach_hard_limit: false,
            current_range: None,
            ranges_to_evict: BTreeSet::default(),
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
                        "schedule lock tombstone cleanup failed";
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
        fail::fail_point!("on_write_impl");
        let ranges_to_delete = self.handle_ranges_to_evict();
        let (entries_to_write, engine) = self.engine.handle_pending_range_in_loading_buffer(
            &mut seq,
            std::mem::take(&mut self.pending_range_in_loading_buffer),
        );
        let guard = &epoch::pin();
        let start = Instant::now();
        let mut lock_modification: u64 = 0;
        // Some entries whose ranges may be marked as evicted above, but it does not
        // matter, they will be deleted later.
        let res = entries_to_write
            .into_iter()
            .chain(std::mem::take(&mut self.buffer))
            .try_for_each(|e| {
                if is_lock_cf(e.cf) {
                    lock_modification += e.data_size() as u64;
                }
                seq += 1;
                e.write_to_memory(seq - 1, &engine, self.memory_controller.clone(), guard)
            });
        let duration = start.saturating_elapsed_secs();
        WRITE_DURATION_HISTOGRAM.observe(duration);

        self.engine
            .lock_modification_bytes
            .fetch_add(lock_modification, Ordering::Relaxed);

        if !ranges_to_delete.is_empty() {
            if let Err(e) = self
                .engine
                .bg_worker_manager()
                .schedule_task(BackgroundTask::DeleteRange(ranges_to_delete))
            {
                error!(
                    "schedule delete range failed";
                    "err" => ?e,
                );
                assert!(tikv_util::thread_group::is_shutdown(!cfg!(test)));
            }
        }

        res
    }

    // return ranges that can be deleted from engine now
    fn handle_ranges_to_evict(&mut self) -> Vec<CacheRange> {
        if self.ranges_to_evict.is_empty() {
            return vec![];
        }
        let mut core = self.engine.core.write();
        let mut ranges = vec![];
        let range_manager = core.mut_range_manager();
        for r in std::mem::take(&mut self.ranges_to_evict) {
            if range_manager.contains_range(&r) && range_manager.evict_range(&r) {
                ranges.push(r);
                continue;
            }

            if let Some((.., canceled)) = range_manager
                .pending_ranges_loading_data
                .iter_mut()
                .find(|(range, ..)| range.contains_range(&r))
            {
                *canceled = true;
            }
        }
        ranges
    }

    #[inline]
    pub fn set_range_cache_status(&mut self, range_cache_status: RangeCacheStatus) {
        self.range_cache_status = range_cache_status;
    }

    fn process_cf_operation<F1, F2>(&mut self, entry_size: F1, entry: F2)
    where
        F1: FnOnce() -> usize,
        F2: FnOnce() -> RangeCacheWriteBatchEntry,
    {
        if !matches!(
            self.range_cache_status,
            RangeCacheStatus::Cached | RangeCacheStatus::Loading
        ) || self.memory_usage_reach_hard_limit
        {
            return;
        }

        if !self.engine.enabled() {
            let range = self.current_range.clone().unwrap();
            info!(
                "range cache is disabled, evict the range";
                "range_start" => log_wrappers::Value(&range.start),
                "range_end" => log_wrappers::Value(&range.end),
            );
            self.ranges_to_evict.insert(range);
            return;
        }
        let memory_expect = entry_size();
        if !self.memory_acquire(memory_expect) {
            let range = self.current_range.clone().unwrap();
            info!(
                "memory acquire failed due to reaching hard limit";
                "range_start" => log_wrappers::Value(&range.start),
                "range_end" => log_wrappers::Value(&range.end),
            );
            self.ranges_to_evict.insert(range);
            return;
        }

        match self.range_cache_status {
            RangeCacheStatus::Cached => {
                self.buffer.push(entry());
            }
            RangeCacheStatus::Loading => {
                self.pending_range_in_loading_buffer.push(entry());
            }
            RangeCacheStatus::NotInCache => {}
        }
    }

    fn schedule_memory_check(&self) {
        if self.memory_controller.memory_checking() {
            return;
        }
        self.memory_controller.set_memory_checking(true);
        if let Err(e) = self
            .engine
            .bg_worker_manager()
            .schedule_task(BackgroundTask::MemoryCheckAndEvict)
        {
            error!(
                "schedule memory check failed";
                "err" => ?e,
            );
            assert!(tikv_util::thread_group::is_shutdown(!cfg!(test)));
        }
    }

    // return false means the memory usage reaches to hard limit and we have no
    // quota to write to the engine
    fn memory_acquire(&mut self, mem_required: usize) -> bool {
        match self.memory_controller.acquire(mem_required) {
            MemoryUsage::HardLimitReached(n) => {
                self.memory_usage_reach_hard_limit = true;
                warn!(
                    "the memory usage of in-memory engine reaches to hard limit";
                    "range" => ?self.current_range.as_ref().unwrap(),
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
                InternalBytes::from_bytes(Bytes::new()),
            ),
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
pub(crate) struct RangeCacheWriteBatchEntry {
    cf: usize,
    key: Bytes,
    inner: WriteBatchEntryInternal,
}

impl RangeCacheWriteBatchEntry {
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
        key.len() + value.len() + ENC_KEY_SEQ_LENGTH + 2 * MEM_CONTROLLER_OVERHEAD /* one for key and one for value */
    }

    pub fn cal_delete_entry_size(key: &[u8]) -> usize {
        key.len() + ENC_KEY_SEQ_LENGTH
    }

    pub fn data_size(&self) -> usize {
        self.key.len() + ENC_KEY_SEQ_LENGTH + self.inner.data_size()
    }

    #[inline]
    pub fn write_to_memory(
        &self,
        seq: u64,
        skiplist_engine: &SkiplistEngine,
        memory_controller: Arc<MemoryController>,
        guard: &epoch::Guard,
    ) -> Result<()> {
        let handle = skiplist_engine.cf_handle(id_to_cf(self.cf));

        let (mut key, mut value) = self.encode(seq);
        key.set_memory_controller(memory_controller.clone());
        value.set_memory_controller(memory_controller);
        handle.insert(key, value, guard);

        Ok(())
    }
}

// group_write_batch_entries classifies the entries to two categories according
// to the infomation in range manager:
// 1. entreis that can be written to memory engine directly
// 2. entreis that need to be cached
// For 2, we group the entries according to the range. The method uses the
// property that entries in the same range are neighbors. Though that the method
// still handles corretly even they are randomly positioned.
//
// Note: Some entries may not found a range in both
// `pending_ranges_loading_data` and `ranges`, it means the range has been
// evicted.
pub fn group_write_batch_entries(
    mut entries: Vec<RangeCacheWriteBatchEntry>,
    range_manager: &RangeManager,
) -> (
    Vec<(CacheRange, Vec<RangeCacheWriteBatchEntry>)>,
    Vec<RangeCacheWriteBatchEntry>,
) {
    let mut group_entries_to_cache: Vec<(CacheRange, Vec<RangeCacheWriteBatchEntry>)> = vec![];
    let mut entries_to_write: Vec<RangeCacheWriteBatchEntry> = vec![];
    let mut drain = entries.drain(..).peekable();
    while let Some(mut e) = drain.next() {
        if let Some((range_loading, ..)) = range_manager
            .pending_ranges_loading_data
            .iter()
            .find(|r| r.0.contains_key(&e.key))
        {
            // The range of this write batch entry is still in loading status
            let mut current_group = vec![];
            loop {
                current_group.push(e);
                if let Some(next_e) = drain.peek()
                    && range_loading.contains_key(&next_e.key)
                {
                    e = drain.next().unwrap();
                } else {
                    break;
                }
            }
            group_entries_to_cache.push((range_loading.clone(), current_group));
        } else if let Some(range) = range_manager
            .ranges()
            .keys()
            .find(|r| r.contains_key(&e.key))
        {
            // The range has finished loading and became a normal cache range
            loop {
                entries_to_write.push(e);
                if let Some(next_e) = drain.peek()
                    && range.contains_key(&next_e.key)
                {
                    e = drain.next().unwrap();
                } else {
                    break;
                }
            }
        } else {
            // The range of the entry is not found, it means the ranges has been
            // evicted
        }
    }
    (group_entries_to_cache, entries_to_write)
}

impl WriteBatchExt for RangeCacheMemoryEngine {
    type WriteBatch = RangeCacheWriteBatch;
    // todo: adjust it
    const WRITE_BATCH_MAX_KEYS: usize = 256;

    fn write_batch(&self) -> Self::WriteBatch {
        RangeCacheWriteBatch::from(self)
    }

    fn write_batch_with_cap(&self, cap: usize) -> Self::WriteBatch {
        RangeCacheWriteBatch::with_capacity(self, cap)
    }
}

impl WriteBatch for RangeCacheWriteBatch {
    fn write_opt(&mut self, _: &WriteOptions) -> Result<u64> {
        self.sequence_number
            .map(|seq| self.write_impl(seq).map(|()| seq))
            .transpose()
            .map(|o| o.ok_or_else(|| box_err!("sequence_number must be set!")))?
    }

    fn data_size(&self) -> usize {
        self.buffer
            .iter()
            .map(RangeCacheWriteBatchEntry::data_size)
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
        self.buffer.clear();
        self.save_points.clear();
        _ = self.sequence_number.take();
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

    fn prepare_for_range(&mut self, range: CacheRange) {
        self.set_range_cache_status(self.engine.prepare_for_apply(&range));
        self.memory_usage_reach_hard_limit = false;
        self.current_range = Some(range);
    }
}

impl Mutable for RangeCacheWriteBatch {
    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        self.put_cf(CF_DEFAULT, key, val)
    }

    fn put_cf(&mut self, cf: &str, key: &[u8], val: &[u8]) -> Result<()> {
        self.process_cf_operation(
            || RangeCacheWriteBatchEntry::calc_put_entry_size(key, val),
            || RangeCacheWriteBatchEntry::put_value(cf, key, val),
        );
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.delete_cf(CF_DEFAULT, key)
    }

    fn delete_cf(&mut self, cf: &str, key: &[u8]) -> Result<()> {
        self.process_cf_operation(
            || RangeCacheWriteBatchEntry::cal_delete_entry_size(key),
            || RangeCacheWriteBatchEntry::deletion(cf, key),
        );
        Ok(())
    }

    fn delete_range(&mut self, _: &[u8], _: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn delete_range_cf(&mut self, _: &str, _: &[u8], _: &[u8]) -> Result<()> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use engine_rocks::util::new_engine;
    use engine_traits::{
        CacheRange, FailedReason, KvEngine, Peekable, RangeCacheEngine, WriteBatch, CF_WRITE,
        DATA_CFS,
    };
    use online_config::{ConfigChange, ConfigManager, ConfigValue};
    use skiplist_rs::SkipList;
    use tempfile::Builder;
    use tikv_util::config::VersionTrack;

    use super::*;
    use crate::{
        background::flush_epoch, config::RangeCacheConfigManager, RangeCacheEngineConfig,
        RangeCacheEngineContext,
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
        let engine = RangeCacheMemoryEngine::new(RangeCacheEngineContext::new(Arc::new(
            VersionTrack::new(RangeCacheEngineConfig::config_for_test()),
        )));
        let r = CacheRange::new(b"".to_vec(), b"z".to_vec());
        engine.new_range(r.clone());
        {
            let mut core = engine.core.write();
            core.mut_range_manager().set_safe_point(&r, 10);
        }
        let mut wb = RangeCacheWriteBatch::from(&engine);
        wb.range_cache_status = RangeCacheStatus::Cached;
        wb.put(b"aaa", b"bbb").unwrap();
        wb.set_sequence_number(1).unwrap();
        assert_eq!(wb.write().unwrap(), 1);
        let sl = engine.core.read().engine().data[cf_to_id(CF_DEFAULT)].clone();
        let guard = &crossbeam::epoch::pin();
        let val = get_value(&sl, &encode_key(b"aaa", 2, ValueType::Value), guard).unwrap();
        assert_eq!(&b"bbb"[..], val.as_slice());
    }

    #[test]
    fn test_savepoints() {
        let engine = RangeCacheMemoryEngine::new(RangeCacheEngineContext::new(Arc::new(
            VersionTrack::new(RangeCacheEngineConfig::config_for_test()),
        )));
        let r = CacheRange::new(b"".to_vec(), b"z".to_vec());
        engine.new_range(r.clone());
        {
            let mut core = engine.core.write();
            core.mut_range_manager().set_safe_point(&r, 10);
        }
        let mut wb = RangeCacheWriteBatch::from(&engine);
        wb.range_cache_status = RangeCacheStatus::Cached;
        wb.put(b"aaa", b"bbb").unwrap();
        wb.set_save_point();
        wb.put(b"aaa", b"ccc").unwrap();
        wb.put(b"ccc", b"ddd").unwrap();
        wb.rollback_to_save_point().unwrap();
        wb.set_sequence_number(1).unwrap();
        assert_eq!(wb.write().unwrap(), 1);
        let sl = engine.core.read().engine().data[cf_to_id(CF_DEFAULT)].clone();
        let guard = &crossbeam::epoch::pin();
        let val = get_value(&sl, &encode_key(b"aaa", 1, ValueType::Value), guard).unwrap();
        assert_eq!(&b"bbb"[..], val.as_slice());
        assert!(get_value(&sl, &encode_key(b"ccc", 1, ValueType::Value), guard).is_none())
    }

    #[test]
    fn test_put_write_clear_delete_put_write() {
        let engine = RangeCacheMemoryEngine::new(RangeCacheEngineContext::new(Arc::new(
            VersionTrack::new(RangeCacheEngineConfig::config_for_test()),
        )));
        let r = CacheRange::new(b"".to_vec(), b"z".to_vec());
        engine.new_range(r.clone());
        {
            let mut core = engine.core.write();
            core.mut_range_manager().set_safe_point(&r, 10);
        }
        let mut wb = RangeCacheWriteBatch::from(&engine);
        wb.range_cache_status = RangeCacheStatus::Cached;
        wb.put(b"aaa", b"bbb").unwrap();
        wb.set_sequence_number(1).unwrap();
        _ = wb.write();
        wb.clear();
        wb.put(b"bbb", b"ccc").unwrap();
        wb.delete(b"aaa").unwrap();
        wb.set_sequence_number(2).unwrap();
        _ = wb.write();
        let snapshot = engine.snapshot(r, u64::MAX, 3).unwrap();
        assert_eq!(
            snapshot.get_value(&b"bbb"[..]).unwrap().unwrap(),
            &b"ccc"[..]
        );
        assert!(snapshot.get_value(&b"aaa"[..]).unwrap().is_none())
    }

    #[test]
    fn test_prepare_for_apply() {
        let path = Builder::new()
            .prefix("test_prepare_for_apply")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();
        let rocks_engine = new_engine(path_str, DATA_CFS).unwrap();

        let engine = RangeCacheMemoryEngine::new(RangeCacheEngineContext::new(Arc::new(
            VersionTrack::new(RangeCacheEngineConfig::config_for_test()),
        )));
        let r1 = CacheRange::new(b"k01".to_vec(), b"k05".to_vec());
        let r2 = CacheRange::new(b"k05".to_vec(), b"k10".to_vec());
        let r3 = CacheRange::new(b"k10".to_vec(), b"k15".to_vec());
        {
            engine.new_range(r1.clone());
            let mut core = engine.core.write();
            core.mut_range_manager().set_safe_point(&r1, 10);

            let snap = Arc::new(rocks_engine.snapshot(None));
            core.mut_range_manager()
                .pending_ranges_loading_data
                .push_back((r2.clone(), snap, false));
        }
        let mut wb = RangeCacheWriteBatch::from(&engine);
        wb.prepare_for_range(r1.clone());
        wb.put(b"k01", b"val1").unwrap();
        wb.prepare_for_range(r2.clone());
        wb.put(b"k05", b"val5").unwrap();
        wb.prepare_for_range(r3);
        wb.put(b"k10", b"val10").unwrap();
        wb.set_sequence_number(2).unwrap();
        let _ = wb.write();
        let snapshot = engine.snapshot(r1.clone(), u64::MAX, 5).unwrap();
        assert_eq!(
            snapshot.get_value(&b"k01"[..]).unwrap().unwrap(),
            &b"val1"[..]
        );
        {
            let core = engine.core.read();
            assert_eq!(core.cached_write_batch.get(&r2).unwrap().len(), 1);
        }

        let mut wb = RangeCacheWriteBatch::from(&engine);
        wb.prepare_for_range(r1.clone());
        wb.delete(b"k01").unwrap();
        wb.set_sequence_number(5).unwrap();
        let _ = wb.write();
        let snapshot = engine.snapshot(r1, u64::MAX, 6).unwrap();
        assert!(snapshot.get_value(&b"k01"[..]).unwrap().is_none(),);
    }

    #[test]
    fn test_group_entries() {
        let path = Builder::new().prefix("test_group").tempdir().unwrap();
        let path_str = path.path().to_str().unwrap();
        let rocks_engine = new_engine(path_str, DATA_CFS).unwrap();
        let snap = rocks_engine.snapshot(None);

        let mut range_manager = RangeManager::default();
        let r1 = CacheRange::new(b"k00".to_vec(), b"k10".to_vec());
        let r2 = CacheRange::new(b"k10".to_vec(), b"k20".to_vec());
        let r3 = CacheRange::new(b"k20".to_vec(), b"k30".to_vec());
        range_manager.new_range(r1.clone());
        let snap = Arc::new(snap);
        range_manager
            .pending_ranges_loading_data
            .push_back((r2.clone(), snap.clone(), false));
        range_manager
            .pending_ranges_loading_data
            .push_back((r3.clone(), snap, false));

        let entries = vec![
            RangeCacheWriteBatchEntry::put_value(CF_DEFAULT, b"k22", b"val"),
            RangeCacheWriteBatchEntry::put_value(CF_DEFAULT, b"k21", b"val"),
            RangeCacheWriteBatchEntry::deletion(CF_DEFAULT, b"k25"),
            RangeCacheWriteBatchEntry::put_value(CF_DEFAULT, b"k28", b"val"),
            RangeCacheWriteBatchEntry::put_value(CF_WRITE, b"k03", b"val"),
            RangeCacheWriteBatchEntry::put_value(CF_WRITE, b"k05", b"val"),
            RangeCacheWriteBatchEntry::put_value(CF_WRITE, b"k09", b"val"),
            RangeCacheWriteBatchEntry::put_value(CF_WRITE, b"k10", b"val"),
            RangeCacheWriteBatchEntry::put_value(CF_WRITE, b"k19", b"val"),
            // Mock the range is evicted
            RangeCacheWriteBatchEntry::put_value(CF_WRITE, b"k32", b"val"),
            RangeCacheWriteBatchEntry::put_value(CF_WRITE, b"k45", b"val"),
        ];

        let (group_entries_to_cache, entries_to_write) =
            group_write_batch_entries(entries, &range_manager);
        assert_eq!(group_entries_to_cache.len(), 2);
        assert_eq!(entries_to_write.len(), 3);
        entries_to_write
            .iter()
            .for_each(|e| assert!(r1.contains_key(&e.key)));
        group_entries_to_cache.iter().for_each(|(range, entries)| {
            if *range == r2 {
                assert_eq!(entries.len(), 2);
            } else if *range == r3 {
                assert_eq!(entries.len(), 4);
            } else {
                unreachable!();
            }
            entries
                .iter()
                .for_each(|e| assert!(range.contains_key(&e.key)))
        });
    }

    fn wait_evict_done(engine: &RangeCacheMemoryEngine) {
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
    }

    #[test]
    fn test_write_batch_with_memory_controller() {
        let mut config = RangeCacheEngineConfig::default();
        config.soft_limit_threshold = Some(ReadableSize(500));
        config.hard_limit_threshold = Some(ReadableSize(1000));
        config.enabled = true;
        let engine = RangeCacheMemoryEngine::new(RangeCacheEngineContext::new(Arc::new(
            VersionTrack::new(config),
        )));
        let r1 = CacheRange::new(b"kk00".to_vec(), b"kk10".to_vec());
        let r2 = CacheRange::new(b"kk10".to_vec(), b"kk20".to_vec());
        let r3 = CacheRange::new(b"kk20".to_vec(), b"kk30".to_vec());
        let r4 = CacheRange::new(b"kk30".to_vec(), b"kk40".to_vec());
        let r5 = CacheRange::new(b"kk40".to_vec(), b"kk50".to_vec());
        for r in [&r1, &r2, &r3, &r4, &r5] {
            engine.new_range(r.clone());
            {
                let mut core = engine.core.write();
                core.mut_range_manager().set_safe_point(r, 10);
            }
            let _ = engine.snapshot(r.clone(), 1000, 1000).unwrap();
        }

        let val1: Vec<u8> = (0..150).map(|_| 0).collect();
        let mut wb = RangeCacheWriteBatch::from(&engine);
        wb.prepare_for_range(r1.clone());
        // memory required:
        // 4(key) + 8(sequencen number) + 150(value) + 16(2 Arc<MemoryController) = 178
        wb.put(b"kk01", &val1).unwrap();
        wb.prepare_for_range(r2.clone());
        // Now, 356
        wb.put(b"kk11", &val1).unwrap();
        wb.prepare_for_range(r3.clone());
        // Now, 534
        wb.put(b"kk21", &val1).unwrap();
        let val2: Vec<u8> = (0..500).map(|_| 2).collect();
        // The memory will fail to acquire
        wb.put(b"kk22", &val2).unwrap();

        // The memory capacity is enough the the following two inserts
        let val3: Vec<u8> = (0..150).map(|_| 3).collect();
        wb.prepare_for_range(r4.clone());
        // Now, 712
        wb.put(b"kk32", &val3).unwrap();

        let val4: Vec<u8> = (0..300).map(|_| 3).collect();
        wb.prepare_for_range(r5.clone());
        wb.put(b"kk41", &val4).unwrap();

        let memory_controller = engine.memory_controller();
        // We should have allocated 896 as calculated above
        assert_eq!(712, memory_controller.mem_usage());
        wb.write_impl(1000).unwrap();
        // We dont count the node overhead in write batch, so after they are written
        // into the engine, the mem usage can even exceed the hard limit. But this
        // should be fine as this amount should be at most MB level.
        assert_eq!(1096, memory_controller.mem_usage());

        let snap1 = engine.snapshot(r1.clone(), 1000, 1010).unwrap();
        assert_eq!(snap1.get_value(b"kk01").unwrap().unwrap(), &val1);
        let snap2 = engine.snapshot(r2.clone(), 1000, 1010).unwrap();
        assert_eq!(snap2.get_value(b"kk11").unwrap().unwrap(), &val1);

        assert_eq!(
            engine.snapshot(r3.clone(), 1000, 1000).unwrap_err(),
            FailedReason::NotCached
        );

        let snap4 = engine.snapshot(r4.clone(), 1000, 1010).unwrap();
        assert_eq!(snap4.get_value(b"kk32").unwrap().unwrap(), &val3);

        assert_eq!(
            engine.snapshot(r5.clone(), 1000, 1010).unwrap_err(),
            FailedReason::NotCached
        );

        // For range 3, one write is buffered but the other is rejected, so the range 3
        // is evicted and the keys of it are deleted. After flush the epoch, we should
        // get 1096-178(kv)-96(node overhead) = 822 memory usage.
        flush_epoch();
        wait_evict_done(&engine);
        assert_eq!(822, memory_controller.mem_usage());

        drop(snap1);
        engine.evict_range(&r1);
        flush_epoch();
        wait_evict_done(&engine);
        assert_eq!(548, memory_controller.mem_usage());
    }

    #[test]
    fn test_write_batch_with_config_change() {
        let mut config = RangeCacheEngineConfig::default();
        config.soft_limit_threshold = Some(ReadableSize(u64::MAX));
        config.hard_limit_threshold = Some(ReadableSize(u64::MAX));
        config.enabled = true;
        let config = Arc::new(VersionTrack::new(config));
        let engine = RangeCacheMemoryEngine::new(RangeCacheEngineContext::new(config.clone()));
        let r1 = CacheRange::new(b"kk00".to_vec(), b"kk10".to_vec());
        let r2 = CacheRange::new(b"kk10".to_vec(), b"kk20".to_vec());
        for r in [&r1, &r2] {
            engine.new_range(r.clone());
            {
                let mut core = engine.core.write();
                core.mut_range_manager().set_safe_point(r, 10);
            }
            let _ = engine.snapshot(r.clone(), 1000, 1000).unwrap();
        }

        let val1: Vec<u8> = (0..150).map(|_| 0).collect();
        let mut wb = RangeCacheWriteBatch::from(&engine);
        wb.prepare_for_range(r2.clone());
        wb.put(b"kk11", &val1).unwrap();
        let snap1 = engine.snapshot(r1.clone(), 1000, 1000).unwrap();

        // disable the range cache
        let mut config_manager = RangeCacheConfigManager(config.clone());
        let mut config_change = ConfigChange::new();
        config_change.insert(String::from("enabled"), ConfigValue::Bool(false));
        config_manager.dispatch(config_change).unwrap();

        wb.write_impl(1000).unwrap();
        // existing snapshot can still work after the range cache is disabled, but new
        // snapshot will fail to create
        assert!(snap1.get_value(b"kk00").unwrap().is_none());

        let mut wb = RangeCacheWriteBatch::from(&engine);
        wb.prepare_for_range(r1.clone());
        // put should trigger the evict and it won't write into range cache
        wb.put(b"kk01", &val1).unwrap();
        wb.write_impl(1000).unwrap();

        // new snapshot will fail to create as it's evicted already
        let snap1 = engine.snapshot(r1.clone(), 1000, 1000);
        assert_eq!(snap1.unwrap_err(), FailedReason::NotCached);
        let snap2 = engine.snapshot(r2.clone(), 1000, 1000).unwrap();
        // if no new write, the range cache can still be used.
        assert_eq!(snap2.get_value(b"kk11").unwrap().unwrap(), &val1);

        // enable the range cache again
        let mut config_manager = RangeCacheConfigManager(config.clone());
        let mut config_change = ConfigChange::new();
        config_change.insert(String::from("enabled"), ConfigValue::Bool(true));
        config_manager.dispatch(config_change).unwrap();

        let snap1 = engine.snapshot(r1.clone(), 1000, 1000);
        assert_eq!(snap1.unwrap_err(), FailedReason::NotCached);
        let snap2 = engine.snapshot(r2.clone(), 1000, 1000).unwrap();
        assert_eq!(snap2.get_value(b"kk11").unwrap().unwrap(), &val1);
    }
}
