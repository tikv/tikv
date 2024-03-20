use std::{collections::BTreeSet, sync::Arc};

use bytes::Bytes;
use crossbeam::epoch;
use engine_traits::{
    CacheRange, Mutable, Result, WriteBatch, WriteBatchExt, WriteOptions, CF_DEFAULT,
};
use tikv_util::{box_err, config::ReadableSize, error, warn};

use crate::{
    background::BackgroundTask,
    engine::{cf_to_id, SkiplistEngine},
    keys::{encode_key, InternalBytes, ValueType, ENC_KEY_SEQ_LENGTH},
    memory_limiter::{MemoryController, MemoryUsage},
    range_manager::RangeManager,
    RangeCacheMemoryEngine,
};

// This is a bit of a hack. It's the overhead of a node in the skiplist with
// height 3, which is sufficiently conservative for estimating the node overhead
// size.
pub(crate) const NODE_OVERHEAD_SIZE_EXPECTATION: usize = 96;

// `prepare_for_range` should be called before raft command apply for each peer
// delegate. It sets `range_in_cache` and `pending_range_in_loading` which are
// used to determine whether the writes of this peer should be buffered.
pub struct RangeCacheWriteBatch {
    // `range_in_cache` indicates that the range is cached in the memory engine and we should
    // buffer the write in `buffer` which is consumed during the write is written in the kv engine.
    range_in_cache: bool,
    buffer: Vec<RangeCacheWriteBatchEntry>,
    // `pending_range_in_loading` indicates that the range is pending and loading snapshot in the
    // background and we should buffer the further write for it in
    // `pending_range_in_loading_buffer` which is cached in the memory engine and will be
    // consumed after the snapshot has been loaded.
    pending_range_in_loading: bool,
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
            range_in_cache: false,
            pending_range_in_loading: false,
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
            range_in_cache: false,
            pending_range_in_loading: false,
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

    /// Sets the sequence number for this batch. This should only be called
    /// prior to writing the batch.
    pub fn set_sequence_number(&mut self, seq: u64) -> Result<()> {
        if let Some(seqno) = self.sequence_number {
            return Err(box_err!("Sequence number {} already set", seqno));
        };
        self.sequence_number = Some(seq);
        Ok(())
    }

    fn write_impl(&mut self, seq: u64) -> Result<()> {
        let ranges_to_delete = self.handle_ranges_to_evict();
        let (entries_to_write, engine) = self.engine.handle_pending_range_in_loading_buffer(
            seq,
            std::mem::take(&mut self.pending_range_in_loading_buffer),
        );
        let guard = &epoch::pin();
        // Some entries whose ranges may be marked as evicted above, but it does not
        // matter, they will be deleted at later.
        let res = entries_to_write
            .into_iter()
            .chain(std::mem::take(&mut self.buffer))
            .try_for_each(|e| {
                e.write_to_memory(seq, &engine, self.memory_controller.clone(), guard)
            });

        // todo: schedule it to background worker
        ranges_to_delete.iter().for_each(|r| engine.delete_range(r));
        if !ranges_to_delete.is_empty() {
            self.engine
                .core
                .write()
                .mut_range_manager()
                .on_delete_ranges(&ranges_to_delete);
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
        for r in std::mem::take(&mut self.ranges_to_evict) {
            if core.mut_range_manager().evict_range(&r) {
                ranges.push(r);
            }
        }
        ranges
    }

    pub fn set_range_in_cache(&mut self, v: bool) {
        self.range_in_cache = v;
    }

    pub fn set_pending_range_in_loading(&mut self, v: bool) {
        self.pending_range_in_loading = v;
    }

    fn process_cf_operation<F1, F2>(&mut self, entry_size: F1, entry: F2)
    where
        F1: FnOnce() -> usize,
        F2: FnOnce() -> RangeCacheWriteBatchEntry,
    {
        if self.memory_usage_reach_hard_limit {
            self.ranges_to_evict
                .insert(self.current_range.clone().unwrap());
            return;
        }

        if !(self.range_in_cache || self.pending_range_in_loading) {
            return;
        }

        let memory_expect = NODE_OVERHEAD_SIZE_EXPECTATION + entry_size();
        if !self.memory_acquire(memory_expect) {
            self.ranges_to_evict
                .insert(self.current_range.clone().unwrap());
            return;
        }

        if self.range_in_cache {
            self.buffer.push(entry());
        } else if self.pending_range_in_loading {
            self.pending_range_in_loading_buffer.push(entry());
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
            .schedule_task(BackgroundTask::MemoryCheck)
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
                    "memory_usage(MB)" => ReadableSize(n as u64).as_mb_f64(),
                    "memory_acquire(MB)" => ReadableSize(mem_required as u64).as_mb_f64(),
                );
                self.schedule_memory_check();
                return false;
            }
            MemoryUsage::SoftLimitReached(n) => {
                warn!(
                    "the memory usage of in-memory engine reaches to soft limit";
                    "memory_usage(MB)" => ReadableSize(n as u64).as_mb_f64(),
                    "memory_acquire(MB)" => ReadableSize(mem_required as u64).as_mb_f64(),
                );
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
        key.len() + value.len() + ENC_KEY_SEQ_LENGTH
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
        let handle = &skiplist_engine.data[self.cf];
        let (mut key, mut value) = self.encode(seq);
        key.set_memory_controller(memory_controller.clone());
        value.set_memory_controller(memory_controller);
        handle.insert(key, value, guard).release(guard);
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
        let mut cache_range = None;
        for r in &range_manager.pending_ranges_loading_data {
            if r.0.contains_key(&e.key) {
                cache_range = Some(r.0.clone());
                break;
            }
        }
        if let Some(cache_range) = cache_range {
            let mut current_group = vec![];
            // This range of this write batch entry is still in loading status
            loop {
                current_group.push(e);
                if let Some(next_e) = drain.peek()
                    && cache_range.contains_key(&next_e.key)
                {
                    e = drain.next().unwrap();
                } else {
                    break;
                }
            }
            group_entries_to_cache.push((cache_range, current_group));
        } else {
            // cache_range is None, it means the range has finished loading and
            // became a normal cache range
            for r in range_manager.ranges().keys() {
                if r.contains_key(&e.key) {
                    cache_range = Some(r.clone());
                }
            }
            let cache_range = cache_range.unwrap();
            loop {
                entries_to_write.push(e);
                if let Some(next_e) = drain.peek()
                    && cache_range.contains_key(&next_e.key)
                {
                    e = drain.next().unwrap();
                } else {
                    break;
                }
            }
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
        let (range_in_cache, range_in_loading) = self.engine.prepare_for_apply(&range);
        self.set_range_in_cache(range_in_cache);
        self.set_pending_range_in_loading(range_in_loading);
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
        CacheRange, KvEngine, Peekable, RangeCacheEngine, WriteBatch, CF_WRITE, DATA_CFS,
    };
    use skiplist_rs::SkipList;
    use tempfile::Builder;

    use super::*;
    use crate::EngineConfig;

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
        let engine = RangeCacheMemoryEngine::new(EngineConfig::config_for_test());
        let r = CacheRange::new(b"".to_vec(), b"z".to_vec());
        engine.new_range(r.clone());
        {
            let mut core = engine.core.write();
            core.mut_range_manager().set_range_readable(&r, true);
            core.mut_range_manager().set_safe_point(&r, 10);
        }
        let mut wb = RangeCacheWriteBatch::from(&engine);
        wb.range_in_cache = true;
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
        let engine = RangeCacheMemoryEngine::new(EngineConfig::config_for_test());
        let r = CacheRange::new(b"".to_vec(), b"z".to_vec());
        engine.new_range(r.clone());
        {
            let mut core = engine.core.write();
            core.mut_range_manager().set_range_readable(&r, true);
            core.mut_range_manager().set_safe_point(&r, 10);
        }
        let mut wb = RangeCacheWriteBatch::from(&engine);
        wb.range_in_cache = true;
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
        let engine = RangeCacheMemoryEngine::new(EngineConfig::config_for_test());
        let r = CacheRange::new(b"".to_vec(), b"z".to_vec());
        engine.new_range(r.clone());
        {
            let mut core = engine.core.write();
            core.mut_range_manager().set_range_readable(&r, true);
            core.mut_range_manager().set_safe_point(&r, 10);
        }
        let mut wb = RangeCacheWriteBatch::from(&engine);
        wb.range_in_cache = true;
        wb.put(b"aaa", b"bbb").unwrap();
        wb.set_sequence_number(1).unwrap();
        _ = wb.write();
        wb.clear();
        wb.put(b"bbb", b"ccc").unwrap();
        wb.delete(b"aaa").unwrap();
        wb.set_sequence_number(2).unwrap();
        _ = wb.write();
        let snapshot = engine.snapshot(r, u64::MAX, 2).unwrap();
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

        let engine = RangeCacheMemoryEngine::new(EngineConfig::config_for_test());
        let r1 = CacheRange::new(b"k01".to_vec(), b"k05".to_vec());
        let r2 = CacheRange::new(b"k05".to_vec(), b"k10".to_vec());
        let r3 = CacheRange::new(b"k10".to_vec(), b"k15".to_vec());
        {
            engine.new_range(r1.clone());
            let mut core = engine.core.write();
            core.mut_range_manager().set_range_readable(&r1, true);
            core.mut_range_manager().set_safe_point(&r1, 10);

            let snap = Arc::new(rocks_engine.snapshot(None));
            core.mut_range_manager()
                .pending_ranges_loading_data
                .push_back((r2.clone(), snap));
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
        let snapshot = engine.snapshot(r1.clone(), u64::MAX, 2).unwrap();
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
        wb.set_sequence_number(3).unwrap();
        let _ = wb.write();
        let snapshot = engine.snapshot(r1, u64::MAX, 3).unwrap();
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
            .push_back((r2.clone(), snap.clone()));
        range_manager
            .pending_ranges_loading_data
            .push_back((r3.clone(), snap));

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

    #[test]
    fn test_write_batch_with_memory_controller() {
        let config = EngineConfig::new(Duration::from_secs(600), 500, 1000, 1);
        let engine = RangeCacheMemoryEngine::new(config);
        let r1 = CacheRange::new(b"zk00".to_vec(), b"zk10".to_vec());
        let r2 = CacheRange::new(b"zk10".to_vec(), b"zk20".to_vec());
        let r3 = CacheRange::new(b"zk20".to_vec(), b"zk30".to_vec());
        let r4 = CacheRange::new(b"zk30".to_vec(), b"zk40".to_vec());
        engine.new_range(r1.clone());
        engine.new_range(r2.clone());
        engine.new_range(r3.clone());
        engine.new_range(r4.clone());
        {
            let mut core = engine.core.write();
            core.mut_range_manager().set_range_readable(&r1, true);
            core.mut_range_manager().set_range_readable(&r2, true);
            core.mut_range_manager().set_range_readable(&r3, true);
            core.mut_range_manager().set_range_readable(&r4, true);
            core.mut_range_manager().set_safe_point(&r1, 10);
            core.mut_range_manager().set_safe_point(&r2, 10);
            core.mut_range_manager().set_safe_point(&r3, 10);
            core.mut_range_manager().set_safe_point(&r4, 10);
        }

        let _ = engine.snapshot(r1.clone(), 1000, 1000).unwrap();
        let _ = engine.snapshot(r2.clone(), 1000, 1000).unwrap();
        let _ = engine.snapshot(r3.clone(), 1000, 1000).unwrap();
        let _ = engine.snapshot(r4.clone(), 1000, 1000).unwrap();

        let val1: Vec<u8> = (0..100).map(|_| 0).collect();
        let mut wb = RangeCacheWriteBatch::from(&engine);
        wb.prepare_for_range(r1.clone());
        // memory required:
        // 4(key) + 8(sequencen number) + 100(value) + 96(node overhead) = 208
        wb.put(b"zk01", &val1).unwrap();
        wb.prepare_for_range(r2.clone());
        // Now, 416
        wb.put(b"zk11", &val1).unwrap();
        wb.prepare_for_range(r3.clone());
        // Now, 624
        wb.put(b"zk21", &val1).unwrap();
        let val2: Vec<u8> = (0..400).map(|_| 0).collect();
        // The memory will fail to acquire
        wb.put(b"zk22", &val2).unwrap();

        // Although the memory is enough for this key/value, it's still will not be
        // buffered as the previous write want to acquire memory which reaches to the
        // hard limit.
        let val3: Vec<u8> = (0..100).map(|_| 0).collect();
        wb.prepare_for_range(r4.clone());
        wb.put(b"zk32", &val3).unwrap();

        wb.write_impl(1000).unwrap();
        let snap1 = engine.snapshot(r1.clone(), 1000, 1000).unwrap();
        assert_eq!(snap1.get_value(b"k01").unwrap().unwrap(), &val1);
        let snap2 = engine.snapshot(r2.clone(), 1000, 1000).unwrap();
        // FIXME: snap1 will success
        assert_eq!(snap2.get_value(b"k11").unwrap().unwrap(), &val1);
        assert!(engine.snapshot(r3.clone(), 1000, 1000).is_none());
        assert!(engine.snapshot(r4.clone(), 1000, 1000).is_none());
    }
}
