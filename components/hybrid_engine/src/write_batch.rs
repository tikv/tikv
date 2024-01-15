// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, Mutable, Result, WriteBatch, WriteBatchExt, WriteOptions};
use region_cache_memory_engine::{RangeCacheMemoryEngine, RegionCacheWriteBatch};

use crate::engine::HybridEngine;

pub struct HybridEngineWriteBatch<EK: KvEngine> {
    disk_write_batch: EK::WriteBatch,
    cache_write_batch: RegionCacheWriteBatch,
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
