// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{
    KvEngine, Mutable, RegionCacheEngine, Result, WriteBatch, WriteBatchExt, WriteOptions,
};
use region_cache_memory_engine::RegionCacheMemoryEngine;

use crate::engine::HybridEngine;

pub struct HybridEngineWriteBatch<EK: KvEngine> {
    disk_write_batch: EK::WriteBatch,
    cache_write_batch: <RegionCacheMemoryEngine as WriteBatchExt>::WriteBatch,
}

impl<EK, EC> WriteBatchExt for HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
{
    type WriteBatch = HybridEngineWriteBatch<EK>;
    const WRITE_BATCH_MAX_KEYS: usize = EK::WRITE_BATCH_MAX_KEYS;

    fn write_batch(&self) -> Self::WriteBatch {
        unimplemented!()
    }

    fn write_batch_with_cap(&self, _: usize) -> Self::WriteBatch {
        unimplemented!()
    }
}

impl<EK: KvEngine> WriteBatch for HybridEngineWriteBatch<EK> {
    fn write_opt(&mut self, opts: &WriteOptions) -> Result<u64> {
        self.write_callback_opt(opts, |_| ())
    }

    fn write_callback_opt(&mut self, opts: &WriteOptions, mut cb: impl FnMut(u64)) -> Result<u64> {
        self.disk_write_batch
            .write_callback_opt(opts, |s| {
                self.cache_write_batch.set_sequence_number(s).unwrap();
                self.cache_write_batch.write_opt(opts).unwrap();
            })
            .map(|s| {
                cb(s);
                s
            })
    }

    fn data_size(&self) -> usize {
        unimplemented!()
    }

    fn count(&self) -> usize {
        unimplemented!()
    }

    fn is_empty(&self) -> bool {
        unimplemented!()
    }

    fn should_write_to_engine(&self) -> bool {
        unimplemented!()
    }

    fn clear(&mut self) {
        unimplemented!()
    }

    fn set_save_point(&mut self) {
        unimplemented!()
    }

    fn pop_save_point(&mut self) -> Result<()> {
        unimplemented!()
    }

    fn rollback_to_save_point(&mut self) -> Result<()> {
        unimplemented!()
    }

    fn merge(&mut self, _other: Self) -> Result<()> {
        unimplemented!()
    }
}

impl<EK: KvEngine> Mutable for HybridEngineWriteBatch<EK> {
    fn put(&mut self, _key: &[u8], _value: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn put_cf(&mut self, _cf: &str, _key: &[u8], _value: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn delete(&mut self, _key: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn delete_cf(&mut self, _cf: &str, _key: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn delete_range(&mut self, _begin_key: &[u8], _end_key: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn delete_range_cf(&mut self, _cf: &str, _begin_key: &[u8], _end_key: &[u8]) -> Result<()> {
        unimplemented!()
    }
}
