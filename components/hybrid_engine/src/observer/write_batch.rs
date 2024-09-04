// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{is_data_cf, CacheRegion, KvEngine, Mutable, Result, WriteBatch, WriteOptions};
use raftstore::coprocessor::{
    dispatcher::BoxWriteBatchObserver, Coprocessor, CoprocessorHost, ObservableWriteBatch,
    WriteBatchObserver,
};
use range_cache_memory_engine::{RangeCacheMemoryEngine, RangeCacheWriteBatch};

#[derive(Clone)]
pub struct HybridWriteBatchObserver {
    cache_engine: RangeCacheMemoryEngine,
}

impl HybridWriteBatchObserver {
    pub fn new(cache_engine: RangeCacheMemoryEngine) -> Self {
        HybridWriteBatchObserver { cache_engine }
    }

    pub fn register_to(&self, coprocessor_host: &mut CoprocessorHost<impl KvEngine>) {
        coprocessor_host
            .registry
            .register_write_batch_observer(BoxWriteBatchObserver::new(self.clone()));
    }
}

impl Coprocessor for HybridWriteBatchObserver {}

impl WriteBatchObserver for HybridWriteBatchObserver {
    fn create_observable_write_batch(&self) -> Box<dyn ObservableWriteBatch> {
        Box::new(HybridObservableWriteBatch {
            cache_write_batch: RangeCacheWriteBatch::from(&self.cache_engine),
        })
    }
}

struct HybridObservableWriteBatch {
    cache_write_batch: RangeCacheWriteBatch,
}

impl ObservableWriteBatch for HybridObservableWriteBatch {
    fn write_opt_seq(&mut self, opts: &WriteOptions, seq_num: u64) {
        self.cache_write_batch.set_sequence_number(seq_num).unwrap();
        self.cache_write_batch.write_opt(opts).unwrap();
    }
    fn post_write(&mut self) {
        self.cache_write_batch.maybe_compact_lock_cf();
    }
}

/// Implements the `WriteBatch` trait for `HybridObservableWriteBatch`.
///
/// The following methods are not implemented because they are not used
/// through the interface `Box<dyn ObservableWriteBatch>`.
///
/// - `write`, `write_opt`, `write_callback_opt`, `merge`
///
/// Implements the remaining methods of the `WriteBatch` trait by delegating
/// the calls to the `cache_write_batch` field.
impl WriteBatch for HybridObservableWriteBatch {
    fn write(&mut self) -> Result<u64> {
        unimplemented!("write")
    }
    fn write_opt(&mut self, opts: &WriteOptions) -> Result<u64> {
        unimplemented!("write_opt")
    }
    fn write_callback_opt(&mut self, opts: &WriteOptions, cb: impl FnMut(u64)) -> Result<u64>
    where
        Self: Sized,
    {
        unimplemented!("write_callback_opt")
    }
    fn merge(&mut self, other: Self) -> Result<()>
    where
        Self: Sized,
    {
        unimplemented!("merge")
    }

    fn data_size(&self) -> usize {
        self.cache_write_batch.data_size()
    }
    fn count(&self) -> usize {
        self.cache_write_batch.count()
    }
    fn is_empty(&self) -> bool {
        self.cache_write_batch.is_empty()
    }
    fn should_write_to_engine(&self) -> bool {
        self.cache_write_batch.should_write_to_engine()
    }
    fn clear(&mut self) {
        self.cache_write_batch.clear()
    }
    fn set_save_point(&mut self) {
        self.cache_write_batch.set_save_point()
    }
    fn pop_save_point(&mut self) -> Result<()> {
        self.cache_write_batch.pop_save_point()
    }
    fn rollback_to_save_point(&mut self) -> Result<()> {
        self.cache_write_batch.rollback_to_save_point()
    }
    fn prepare_for_region(&mut self, region: CacheRegion) {
        self.cache_write_batch.prepare_for_region(region);
    }
}

impl Mutable for HybridObservableWriteBatch {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.cache_write_batch.put(key, value)
    }
    fn put_cf(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        if is_data_cf(cf) {
            self.cache_write_batch.put_cf(cf, key, value)?;
        }
        Ok(())
    }
    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.cache_write_batch.delete(key)
    }
    fn delete_cf(&mut self, cf: &str, key: &[u8]) -> Result<()> {
        self.cache_write_batch.delete_cf(cf, key)
    }
    fn delete_range(&mut self, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        // delete_range in range cache engine means eviction -- all ranges overlapped
        // with [begin_key, end_key] will be evicted.
        self.cache_write_batch.delete_range(begin_key, end_key)
    }
    fn delete_range_cf(&mut self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        // delete_range in range cache engine means eviction -- all ranges overlapped
        // with [begin_key, end_key] will be evicted.
        self.cache_write_batch
            .delete_range_cf(cf, begin_key, end_key)
    }
}
