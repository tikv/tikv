// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{CacheRange, KvEngine, MiscExt, RangeCacheEngine, Result, WriteBatchExt};

use crate::{engine::HybridEngine, hybrid_metrics::HybridEngineStatisticsReporter};

impl<EK, EC> MiscExt for HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
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
                .evict_range(CacheRange::new(r.start_key.to_vec(), r.end_key.to_vec()));
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
