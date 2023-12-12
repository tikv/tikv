// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{
    KvEngine, Mutable, RegionCacheEngine, Result, WriteBatch, WriteBatchExt, WriteOptions,
};

use crate::engine::HybridEngine;

pub struct HybridEngineWriteBatch<EK: KvEngine> {
    _disk_write_batch: EK::WriteBatch,
    // todo: region_cache_engine write batch
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
    fn write_opt(&mut self, _: &WriteOptions) -> Result<u64> {
        unimplemented!()
    }

    fn write_callback_opt(&mut self, _opts: &WriteOptions, _cb: impl FnMut()) -> Result<u64> {
        unimplemented!()
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
