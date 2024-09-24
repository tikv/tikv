// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{
    CacheRegion, EvictReason, KvEngine, MiscExt, RegionCacheEngine, RegionEvent, Result,
    WriteBatchExt,
};

use crate::{engine::HybridEngine, hybrid_metrics::HybridEngineStatisticsReporter};

impl<EK, EC> MiscExt for HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
    HybridEngine<EK, EC>: WriteBatchExt,
{
    type StatisticsReporter = HybridEngineStatisticsReporter;

    fn flush_cf(&self, cf: &str, wait: bool) -> Result<()> {
        self.disk_engine().flush_cf(cf, wait)
    }

    fn flush_cfs(&self, cfs: &[&str], wait: bool) -> Result<()> {
        self.disk_engine().flush_cfs(cfs, wait)
    }

    fn flush_oldest_cf(
        &self,
        wait: bool,
        threshold: Option<std::time::SystemTime>,
    ) -> Result<bool> {
        self.disk_engine().flush_oldest_cf(wait, threshold)
    }

    fn delete_ranges_cf(
        &self,
        wopts: &engine_traits::WriteOptions,
        cf: &str,
        strategy: engine_traits::DeleteStrategy,
        ranges: &[engine_traits::Range<'_>],
    ) -> Result<bool> {
        for r in ranges {
            self.region_cache_engine()
                .on_region_event(RegionEvent::EvictByRange {
                    range: CacheRegion::new(0, 0, r.start_key.to_vec(), r.end_key.to_vec()),
                    reason: EvictReason::DeleteRange,
                });
        }
        self.disk_engine()
            .delete_ranges_cf(wopts, cf, strategy, ranges)
    }

    fn get_approximate_memtable_stats_cf(
        &self,
        cf: &str,
        range: &engine_traits::Range<'_>,
    ) -> Result<(u64, u64)> {
        self.disk_engine()
            .get_approximate_memtable_stats_cf(cf, range)
    }

    fn ingest_maybe_slowdown_writes(&self, cf: &str) -> Result<bool> {
        self.disk_engine().ingest_maybe_slowdown_writes(cf)
    }

    fn get_sst_key_ranges(&self, cf: &str, level: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.disk_engine().get_sst_key_ranges(cf, level)
    }

    fn get_engine_used_size(&self) -> Result<u64> {
        self.disk_engine().get_engine_used_size()
    }

    fn path(&self) -> &str {
        self.disk_engine().path()
    }

    fn sync_wal(&self) -> Result<()> {
        self.disk_engine().sync_wal()
    }

    fn disable_manual_compaction(&self) -> Result<()> {
        self.disk_engine().disable_manual_compaction()
    }

    fn enable_manual_compaction(&self) -> Result<()> {
        self.disk_engine().enable_manual_compaction()
    }

    fn pause_background_work(&self) -> Result<()> {
        self.disk_engine().pause_background_work()
    }

    fn continue_background_work(&self) -> Result<()> {
        self.disk_engine().continue_background_work()
    }

    fn exists(path: &str) -> bool {
        EK::exists(path)
    }

    fn locked(path: &str) -> Result<bool> {
        EK::locked(path)
    }

    fn dump_stats(&self) -> Result<String> {
        self.disk_engine().dump_stats()
    }

    fn get_latest_sequence_number(&self) -> u64 {
        self.disk_engine().get_latest_sequence_number()
    }

    fn get_oldest_snapshot_sequence_number(&self) -> Option<u64> {
        self.disk_engine().get_oldest_snapshot_sequence_number()
    }

    fn get_total_sst_files_size_cf(&self, cf: &str) -> Result<Option<u64>> {
        self.disk_engine().get_total_sst_files_size_cf(cf)
    }

    fn get_num_keys(&self) -> Result<u64> {
        self.disk_engine().get_num_keys()
    }

    fn get_range_stats(
        &self,
        cf: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<Option<engine_traits::RangeStats>> {
        self.disk_engine().get_range_stats(cf, start, end)
    }

    fn is_stalled_or_stopped(&self) -> bool {
        self.disk_engine().is_stalled_or_stopped()
    }

    fn get_active_memtable_stats_cf(
        &self,
        cf: &str,
    ) -> Result<Option<(u64, std::time::SystemTime)>> {
        self.disk_engine().get_active_memtable_stats_cf(cf)
    }

    fn get_accumulated_flush_count_cf(cf: &str) -> Result<u64> {
        EK::get_accumulated_flush_count_cf(cf)
    }

    type DiskEngine = EK::DiskEngine;
    fn get_disk_engine(&self) -> &Self::DiskEngine {
        self.disk_engine().get_disk_engine()
    }
}

#[cfg(test)]
pub mod tests {
    use engine_traits::{
        CacheRegion, DeleteStrategy, MiscExt, Mutable, Range, RegionCacheEngine, WriteBatch,
        WriteBatchExt, WriteOptions, CF_DEFAULT,
    };
    use in_memory_engine::{test_util::new_region, RegionCacheEngineConfig};

    use crate::util::hybrid_engine_for_tests;

    #[test]
    fn test_delete_range() {
        let r1 = new_region(1, b"k00", b"k10");
        let r2 = new_region(2, b"k20", b"k30");
        let r3 = new_region(3, b"k40", b"k50");
        let r1_clone = r1.clone();
        let r2_clone = r2.clone();
        let r3_clone = r3.clone();
        let (_path, hybrid_engine) = hybrid_engine_for_tests(
            "temp",
            RegionCacheEngineConfig::config_for_test(),
            move |memory_engine| {
                memory_engine.new_region(r1_clone);
                memory_engine.new_region(r2_clone);
                memory_engine.new_region(r3_clone);
            },
        )
        .unwrap();

        let cache_r1 = CacheRegion::from_region(&r1);
        let cache_r2 = CacheRegion::from_region(&r2);
        let cache_r3 = CacheRegion::from_region(&r3);

        let mut write_batch = hybrid_engine.write_batch();
        write_batch.prepare_for_region(cache_r1.clone());
        write_batch.put(b"zk02", b"val").unwrap();
        write_batch.put(b"zk03", b"val").unwrap();
        write_batch.prepare_for_region(cache_r2.clone());
        write_batch.put(b"zk22", b"val").unwrap();
        write_batch.put(b"zk23", b"val").unwrap();
        write_batch.prepare_for_region(cache_r3.clone());
        write_batch.put(b"zk42", b"val").unwrap();
        write_batch.put(b"zk43", b"val").unwrap();
        write_batch.write().unwrap();

        hybrid_engine
            .delete_ranges_cf(
                &WriteOptions::default(),
                CF_DEFAULT,
                DeleteStrategy::DeleteByRange,
                &[Range::new(b"zk00", b"zk15"), Range::new(b"zk22", b"zk27")],
            )
            .unwrap();

        hybrid_engine
            .region_cache_engine()
            .snapshot(cache_r1.clone(), 1000, 1000)
            .unwrap_err();
        hybrid_engine
            .region_cache_engine()
            .snapshot(cache_r2.clone(), 1000, 1000)
            .unwrap_err();
        hybrid_engine
            .region_cache_engine()
            .snapshot(cache_r3.clone(), 1000, 1000)
            .unwrap();
    }
}
