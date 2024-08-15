// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{is_data_cf, CacheRange, KvEngine, Mutable, WriteBatch as _, WriteOptions};
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
    fn put_cf(&mut self, cf: &str, key: &[u8], value: &[u8]) {
        if is_data_cf(cf) {
            self.cache_write_batch.put_cf(cf, key, value).unwrap();
        }
    }
    fn delete_cf(&mut self, cf: &str, key: &[u8]) {
        self.cache_write_batch.delete_cf(cf, key).unwrap();
    }
    fn delete_range_cf(&mut self, cf: &str, begin_key: &[u8], end_key: &[u8]) {
        // delete_range in range cache engine means eviction -- all ranges overlapped
        // with [begin_key, end_key] will be evicted.
        self.cache_write_batch
            .delete_range_cf(cf, begin_key, end_key)
            .unwrap();
    }
    fn set_save_point(&mut self) {
        self.cache_write_batch.set_save_point();
    }
    fn pop_save_point(&mut self) {
        self.cache_write_batch.pop_save_point().unwrap();
    }
    fn rollback_to_save_point(&mut self) {
        self.cache_write_batch.rollback_to_save_point().unwrap();
    }
    fn clear(&mut self) {
        self.cache_write_batch.clear();
    }
    fn write_opt(&mut self, opts: &WriteOptions, seq_num: u64) {
        self.cache_write_batch.set_sequence_number(seq_num).unwrap();
        self.cache_write_batch.write_opt(opts).unwrap();
    }
    fn prepare_for_range(&mut self, range: CacheRange) {
        self.cache_write_batch.prepare_for_range(range);
    }
}
