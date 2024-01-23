// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, Mutable, Result, WriteBatch, WriteBatchExt, WriteOptions};
use region_cache_memory_engine::{RangeCacheMemoryEngine, RangeCacheWriteBatch};

use crate::engine::HybridEngine;

pub struct HybridEngineWriteBatch<EK: KvEngine> {
    disk_write_batch: EK::WriteBatch,
    cache_write_batch: RangeCacheWriteBatch,
}

impl<EK> WriteBatchExt for HybridEngine<EK, RangeCacheMemoryEngine>
where
    EK: KvEngine,
{
    type WriteBatch = HybridEngineWriteBatch<EK>;
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
        self.disk_write_batch.set_save_point();
        self.cache_write_batch.set_save_point()
    }

    fn pop_save_point(&mut self) -> Result<()> {
        self.disk_write_batch.pop_save_point()?;
        self.cache_write_batch.pop_save_point()
    }

    fn rollback_to_save_point(&mut self) -> Result<()> {
        self.disk_write_batch.rollback_to_save_point()?;
        self.cache_write_batch.rollback_to_save_point()
    }

    fn merge(&mut self, other: Self) -> Result<()> {
        self.disk_write_batch.merge(other.disk_write_batch)?;
        self.cache_write_batch.merge(other.cache_write_batch)
    }
}

impl<EK: KvEngine> Mutable for HybridEngineWriteBatch<EK> {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.disk_write_batch.put(key, value)?;
        self.cache_write_batch.put(key, value)
    }

    fn put_cf(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.disk_write_batch.put_cf(cf, key, value)?;
        self.cache_write_batch.put_cf(cf, key, value)
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.disk_write_batch.delete(key)?;
        self.cache_write_batch.delete(key)
    }

    fn delete_cf(&mut self, cf: &str, key: &[u8]) -> Result<()> {
        self.disk_write_batch.delete_cf(cf, key)?;
        self.cache_write_batch.delete_cf(cf, key)
    }

    fn delete_range(&mut self, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        self.disk_write_batch.delete_range(begin_key, end_key)
    }

    fn delete_range_cf(&mut self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        self.disk_write_batch
            .delete_range_cf(cf, begin_key, end_key)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use engine_rocks::util::new_engine;
    use engine_traits::{CacheRange, WriteBatchExt, CF_DEFAULT, CF_LOCK, CF_WRITE};
    use region_cache_memory_engine::RangeCacheMemoryEngine;
    use tempfile::Builder;

    use crate::HybridEngine;

    #[test]
    fn test_region_cache_memory_engine() {
        let path = Builder::new().prefix("temp").tempdir().unwrap();
        let disk_engine = new_engine(
            path.path().to_str().unwrap(),
            &[CF_DEFAULT, CF_LOCK, CF_WRITE],
        )
        .unwrap();
        let memory_engine = RangeCacheMemoryEngine::new(Arc::default());
        let range = CacheRange::new(b"k00".to_vec(), b"k10".to_vec());
        memory_engine.new_range(range.clone());
        {
            let mut core = memory_engine.core().lock().unwrap();
            core.mut_range_manager().set_range_readable(&range, true);
            core.mut_range_manager().set_safe_ts(&range, 10);
        }

        let hybrid_engine =
            HybridEngine::<_, RangeCacheMemoryEngine>::new(disk_engine, memory_engine.clone());
        let mut write_batch = hybrid_engine.write_batch();
        write_batch
            .cache_write_batch
            .set_sequence_number(0)
            .unwrap(); // First call ok.
        assert!(
            write_batch
                .cache_write_batch
                .set_sequence_number(0)
                .is_err()
        ); // Second call err.
    }
}
