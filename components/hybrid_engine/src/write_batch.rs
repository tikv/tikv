// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicBool, Ordering};

use engine_traits::{
    is_data_cf, CacheRegion, KvEngine, Mutable, Result, WriteBatch, WriteBatchExt, WriteOptions,
};
use in_memory_engine::{RegionCacheMemoryEngine, RegionCacheWriteBatch};

use crate::engine::HybridEngine;

pub struct HybridEngineWriteBatch<EK: KvEngine> {
    disk_write_batch: EK::WriteBatch,
    pub(crate) cache_write_batch: RegionCacheWriteBatch,
}

impl<EK> WriteBatchExt for HybridEngine<EK, RegionCacheMemoryEngine>
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
        let called = AtomicBool::new(false);
        let res = self
            .disk_write_batch
            .write_callback_opt(opts, |s| {
                if !called.fetch_or(true, Ordering::SeqCst) {
                    self.cache_write_batch.set_sequence_number(s).unwrap();
                    self.cache_write_batch.write_opt(opts).unwrap();
                }
            })
            .map(|s| {
                cb(s);
                s
            });
        self.cache_write_batch.maybe_compact_lock_cf();
        res
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

    fn prepare_for_region(&mut self, r: CacheRegion) {
        self.cache_write_batch.prepare_for_region(r);
    }
}

impl<EK: KvEngine> Mutable for HybridEngineWriteBatch<EK> {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.disk_write_batch.put(key, value)?;
        self.cache_write_batch.put(key, value)
    }

    fn put_cf(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.disk_write_batch.put_cf(cf, key, value)?;
        if is_data_cf(cf) {
            self.cache_write_batch.put_cf(cf, key, value)?;
        }
        Ok(())
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
        self.disk_write_batch.delete_range(begin_key, end_key)?;
        // delete_range in range cache engine means eviction -- all ranges overlapped
        // with [begin_key, end_key] will be evicted.
        self.cache_write_batch.delete_range(begin_key, end_key)
    }

    fn delete_range_cf(&mut self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        self.disk_write_batch
            .delete_range_cf(cf, begin_key, end_key)?;
        // delete_range in range cache engine means eviction -- all ranges overlapped
        // with [begin_key, end_key] will be evicted.
        self.cache_write_batch
            .delete_range_cf(cf, begin_key, end_key)
    }
}

#[cfg(test)]
mod tests {

    use std::time::Duration;

    use engine_traits::{
        CacheRegion, Mutable, Peekable, RegionCacheEngine, SnapshotContext, WriteBatch,
        WriteBatchExt,
    };
    use in_memory_engine::{test_util::new_region, InMemoryEngineConfig, RegionCacheStatus};

    use crate::util::hybrid_engine_for_tests;

    #[test]
    fn test_write_to_both_engines() {
        let region = new_region(1, b"", b"z");
        let region_clone = region.clone();
        let (_path, hybrid_engine) = hybrid_engine_for_tests(
            "temp",
            InMemoryEngineConfig::config_for_test(),
            move |memory_engine| {
                let id = region_clone.id;
                memory_engine.new_region(region_clone);
                memory_engine.core().region_manager().set_safe_point(id, 5);
            },
        )
        .unwrap();
        let cache_region = CacheRegion::from_region(&region);
        let mut write_batch = hybrid_engine.write_batch();
        write_batch.prepare_for_region(cache_region.clone());
        write_batch
            .cache_write_batch
            .set_region_cache_status(RegionCacheStatus::Cached);
        write_batch.put(b"zhello", b"world").unwrap();
        let seq = write_batch.write().unwrap();
        assert!(seq > 0);
        let actual: &[u8] = &hybrid_engine.get_value(b"zhello").unwrap().unwrap();
        assert_eq!(b"world", &actual);
        let ctx = SnapshotContext {
            region: Some(cache_region.clone()),
            read_ts: 10,
        };
        let snap = hybrid_engine.new_snapshot(Some(ctx));
        let actual: &[u8] = &snap.get_value(b"zhello").unwrap().unwrap();
        assert_eq!(b"world", &actual);
        let actual: &[u8] = &snap.disk_snap().get_value(b"zhello").unwrap().unwrap();
        assert_eq!(b"world", &actual);
        let actual: &[u8] = &snap
            .region_cache_snap()
            .unwrap()
            .get_value(b"zhello")
            .unwrap()
            .unwrap();
        assert_eq!(b"world", &actual);
    }

    #[test]
    fn test_in_memory_engine() {
        let (_path, hybrid_engine) = hybrid_engine_for_tests(
            "temp",
            InMemoryEngineConfig::config_for_test(),
            |memory_engine| {
                let region = new_region(1, b"k00", b"k10");
                memory_engine.new_region(region);
                memory_engine.core().region_manager().set_safe_point(1, 10);
            },
        )
        .unwrap();

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

    #[test]
    fn test_delete_range() {
        let region1 = new_region(1, b"k00", b"k10");
        let region2 = new_region(2, b"k20", b"k30");
        let cache_region1 = CacheRegion::from_region(&region1);
        let cache_region2 = CacheRegion::from_region(&region2);

        let region1_clone = region1.clone();
        let region2_clone = region2.clone();
        let (_path, hybrid_engine) = hybrid_engine_for_tests(
            "temp",
            InMemoryEngineConfig::config_for_test(),
            move |memory_engine| {
                memory_engine.new_region(region1_clone);
                memory_engine.new_region(region2_clone);
            },
        )
        .unwrap();

        let mut wb = hybrid_engine.write_batch();
        wb.prepare_for_region(cache_region1.clone());
        wb.put(b"zk05", b"val").unwrap();
        wb.put(b"zk08", b"val2").unwrap();
        wb.prepare_for_region(cache_region2.clone());
        wb.put(b"zk25", b"val3").unwrap();
        wb.put(b"zk27", b"val4").unwrap();
        wb.write().unwrap();

        hybrid_engine
            .region_cache_engine()
            .snapshot(cache_region1.clone(), 1000, 1000)
            .unwrap();
        hybrid_engine
            .region_cache_engine()
            .snapshot(cache_region2.clone(), 1000, 1000)
            .unwrap();
        assert_eq!(
            4,
            hybrid_engine
                .region_cache_engine()
                .core()
                .engine()
                .cf_handle("default")
                .len()
        );

        let mut wb = hybrid_engine.write_batch();
        // all ranges overlapped with it will be evicted
        wb.prepare_for_region(cache_region1.clone());
        wb.delete_range(b"zk05", b"zk08").unwrap();
        wb.prepare_for_region(cache_region2.clone());
        wb.delete_range(b"zk20", b"zk21").unwrap();
        wb.write().unwrap();

        hybrid_engine
            .region_cache_engine()
            .snapshot(cache_region1.clone(), 1000, 1000)
            .unwrap_err();
        hybrid_engine
            .region_cache_engine()
            .snapshot(cache_region2.clone(), 1000, 1000)
            .unwrap_err();
        let m_engine = hybrid_engine.region_cache_engine();

        test_util::eventually(
            Duration::from_millis(100),
            Duration::from_millis(2000),
            || m_engine.core().engine().cf_handle("default").is_empty(),
        );
    }
}
