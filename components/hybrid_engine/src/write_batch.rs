// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{
    KvEngine, Mutable, RegionCacheEngine, Result, WriteBatch, WriteBatchExt, WriteOptions,
};

use crate::engine::HybridEngine;

pub struct HybridEngineWriteBatch<EK: KvEngine, EC: RegionCacheEngine> {
    disk_write_batch: EK::WriteBatch,
    cache_write_batch: EC::WriteBatch,
}

impl<EK, EC> WriteBatchExt for HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
{
    type WriteBatch = HybridEngineWriteBatch<EK, EC>;
    const WRITE_BATCH_MAX_KEYS: usize = EK::WRITE_BATCH_MAX_KEYS;

    fn write_batch(&self) -> Self::WriteBatch {
        HybridEngineWriteBatch {
            disk_write_batch: self.disk_engine().write_batch(),
            cache_write_batch: self.region_cache_engine().write_batch(),
        }
    }

    fn write_batch_with_cap(&self, cap: usize) -> Self::WriteBatch {
        HybridEngineWriteBatch {
            disk_write_batch: self.disk_engine().write_batch_with_cap(cap),
            cache_write_batch: self.region_cache_engine().write_batch_with_cap(cap),
        }
    }
}

impl<EK: KvEngine, EC: RegionCacheEngine> WriteBatch for HybridEngineWriteBatch<EK, EC> {
    fn write_opt(&mut self, opts: &WriteOptions) -> Result<u64> {
        self.write_callback_opt(opts, |_| ())
    }

    fn write_callback_opt(&mut self, opts: &WriteOptions, mut cb: impl FnMut(u64)) -> Result<u64> {
        self.disk_write_batch
            .write_callback_opt(opts, |s| {
                self.cache_write_batch.set_sequence_number(s).unwrap();
                let seq = self.cache_write_batch.write_opt(opts).unwrap();
                debug_assert_eq!(seq, s);
            })
            .map(|s| {
                cb(s);
                s
            })
    }

    fn data_size(&self) -> usize {
        self.disk_write_batch.data_size()
    }

    fn count(&self) -> usize {
        self.disk_write_batch.count()
    }

    fn is_empty(&self) -> bool {
        self.disk_write_batch.is_empty()
    }

    fn should_write_to_engine(&self) -> bool {
        self.disk_write_batch.should_write_to_engine()
    }

    fn clear(&mut self) {
        self.disk_write_batch.clear();
        self.cache_write_batch.clear()
    }

    fn set_save_point(&mut self) {
        self.disk_write_batch.set_save_point()
    }

    fn pop_save_point(&mut self) -> Result<()> {
        self.disk_write_batch.pop_save_point()
    }

    fn rollback_to_save_point(&mut self) -> Result<()> {
        self.disk_write_batch.rollback_to_save_point()
    }

    fn merge(&mut self, other: Self) -> Result<()> {
        self.disk_write_batch.merge(other.disk_write_batch)?;
        self.cache_write_batch.merge(other.cache_write_batch)
    }

    fn set_sequence_number(&mut self, _seq: u64) -> Result<()> {
        unimplemented!()
    }
}

impl<EK: KvEngine, EC: RegionCacheEngine> Mutable for HybridEngineWriteBatch<EK, EC> {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_region(1, key, value)
    }

    fn put_cf(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_region_cf(1, cf, key, value)
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.delete_region(1, key)
    }

    fn delete_cf(&mut self, cf: &str, key: &[u8]) -> Result<()> {
        self.delete_region_cf(1, cf, key)
    }

    fn delete_range(&mut self, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        self.disk_write_batch.delete_range(begin_key, end_key)
    }

    fn delete_range_cf(&mut self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        self.disk_write_batch
            .delete_range_cf(cf, begin_key, end_key)
    }

    fn put_region(&mut self, region_id: u64, key: &[u8], value: &[u8]) -> Result<()> {
        self.disk_write_batch.put_region(region_id, key, value)?;
        self.cache_write_batch.put_region(region_id, key, value)
    }

    fn put_region_cf(&mut self, region_id: u64, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.disk_write_batch
            .put_region_cf(region_id, cf, key, value)?;
        self.cache_write_batch
            .put_region_cf(region_id, cf, key, value)
    }

    fn delete_region(&mut self, region_id: u64, key: &[u8]) -> Result<()> {
        self.disk_write_batch.delete_region(region_id, key)?;
        self.cache_write_batch.delete_region(region_id, key)
    }

    fn delete_region_cf(&mut self, region_id: u64, cf: &str, key: &[u8]) -> Result<()> {
        self.disk_write_batch.delete_region_cf(region_id, cf, key)?;
        self.cache_write_batch.delete_region_cf(region_id, cf, key)
    }
}
