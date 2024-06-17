// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicBool, Ordering};

use engine_traits::{
    is_data_cf, CacheRange, KvEngine, Mutable, Result, WriteBatch, WriteBatchExt, WriteOptions,
};
use region_cache_memory_engine::{RangeCacheMemoryEngine, RangeCacheWriteBatch};

use crate::engine::HybridEngine;

pub struct HybridEngineWriteBatch<EK: KvEngine> {
    disk_write_batch: EK::WriteBatch,
    pub(crate) cache_write_batch: RangeCacheWriteBatch,
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

    fn prepare_for_range(&mut self, range: CacheRange) {
        self.cache_write_batch.prepare_for_range(range);
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
        CacheRange, KvEngine, Mutable, Peekable, RangeCacheEngine, SnapshotContext, WriteBatch,
        WriteBatchExt,
    };
    use region_cache_memory_engine::{RangeCacheEngineConfig, RangeCacheStatus};

    use crate::util::hybrid_engine_for_tests;

    #[test]
    fn test_write_to_both_engines() {
        let range = CacheRange::new(b"".to_vec(), b"z".to_vec());
        let range_clone = range.clone();
        let (_path, hybrid_engine) = hybrid_engine_for_tests(
            "temp",
            RangeCacheEngineConfig::config_for_test(),
            move |memory_engine| {
                memory_engine.new_range(range_clone.clone());
                {
                    let mut core = memory_engine.core().write();
                    core.mut_range_manager().set_safe_point(&range_clone, 5);
                }
            },
        )
        .unwrap();
        let mut write_batch = hybrid_engine.write_batch();
        write_batch
            .cache_write_batch
            .set_range_cache_status(RangeCacheStatus::Cached);
        write_batch.put(b"hello", b"world").unwrap();
        let seq = write_batch.write().unwrap();
        assert!(seq > 0);
        let actual: &[u8] = &hybrid_engine.get_value(b"hello").unwrap().unwrap();
        assert_eq!(b"world", &actual);
        let ctx = SnapshotContext {
            range: Some(range.clone()),
            read_ts: 10,
        };
        let snap = hybrid_engine.snapshot(Some(ctx));
        let actual: &[u8] = &snap.get_value(b"hello").unwrap().unwrap();
        assert_eq!(b"world", &actual);
        let actual: &[u8] = &snap.disk_snap().get_value(b"hello").unwrap().unwrap();
        assert_eq!(b"world", &actual);
        let actual: &[u8] = &snap
            .region_cache_snap()
            .unwrap()
            .get_value(b"hello")
            .unwrap()
            .unwrap();
        assert_eq!(b"world", &actual);
    }

    #[test]
    fn test_range_cache_memory_engine() {
        let (_path, hybrid_engine) = hybrid_engine_for_tests(
            "temp",
            RangeCacheEngineConfig::config_for_test(),
            |memory_engine| {
                let range = CacheRange::new(b"k00".to_vec(), b"k10".to_vec());
                memory_engine.new_range(range.clone());
                {
                    let mut core = memory_engine.core().write();
                    core.mut_range_manager().set_safe_point(&range, 10);
                }
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
        let range1 = CacheRange::new(b"k00".to_vec(), b"k10".to_vec());
        let range2 = CacheRange::new(b"k20".to_vec(), b"k30".to_vec());

        let range1_clone = range1.clone();
        let range2_clone = range2.clone();
        let (_path, hybrid_engine) = hybrid_engine_for_tests(
            "temp",
            RangeCacheEngineConfig::config_for_test(),
            move |memory_engine| {
                memory_engine.new_range(range1_clone);
                memory_engine.new_range(range2_clone);
            },
        )
        .unwrap();

        let mut wb = hybrid_engine.write_batch();
        wb.prepare_for_range(range1.clone());
        wb.put(b"k05", b"val").unwrap();
        wb.put(b"k08", b"val2").unwrap();
        wb.prepare_for_range(range2.clone());
        wb.put(b"k25", b"val3").unwrap();
        wb.put(b"k27", b"val4").unwrap();
        wb.write().unwrap();

        hybrid_engine
            .region_cache_engine()
            .snapshot(range1.clone(), 1000, 1000)
            .unwrap();
        hybrid_engine
            .region_cache_engine()
            .snapshot(range2.clone(), 1000, 1000)
            .unwrap();
        assert_eq!(
            4,
            hybrid_engine
                .region_cache_engine()
                .core()
                .read()
                .engine()
                .cf_handle("default")
                .len()
        );

        let mut wb = hybrid_engine.write_batch();
        // all ranges overlapped with it will be evicted
        wb.delete_range(b"k05", b"k21").unwrap();
        wb.write().unwrap();

        hybrid_engine
            .region_cache_engine()
            .snapshot(range1, 1000, 1000)
            .unwrap();
        hybrid_engine
            .region_cache_engine()
            .snapshot(range2, 1000, 1000)
            .unwrap();
        let m_engine = hybrid_engine.region_cache_engine();

        let mut times = 0;
        while times < 10 {
            if m_engine
                .core()
                .read()
                .engine()
                .cf_handle("default")
                .is_empty()
            {
                return;
            }
            times += 1;
            std::thread::sleep(Duration::from_millis(200));
        }
        panic!("data is not empty");
    }
}
