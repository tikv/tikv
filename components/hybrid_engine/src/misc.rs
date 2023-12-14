// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, MiscExt, RegionCacheEngine, Result};

use crate::{engine::HybridEngine, hybrid_metrics::HybridEngineStatisticsReporter};

impl<EK, EC> MiscExt for HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
{
    type StatisticsReporter = HybridEngineStatisticsReporter;

    fn flush_cf(&self, cf: &str, wait: bool) -> Result<()> {
        unimplemented!()
    }

    fn flush_cfs(&self, cfs: &[&str], wait: bool) -> Result<()> {
        unimplemented!()
    }

    fn flush_oldest_cf(
        &self,
        wait: bool,
        threshold: Option<std::time::SystemTime>,
    ) -> Result<bool> {
        unimplemented!()
    }

    fn delete_ranges_cf(
        &self,
        wopts: &engine_traits::WriteOptions,
        cf: &str,
        strategy: engine_traits::DeleteStrategy,
        ranges: &[engine_traits::Range<'_>],
    ) -> Result<bool> {
        unimplemented!()
    }

    fn get_approximate_memtable_stats_cf(
        &self,
        cf: &str,
        range: &engine_traits::Range<'_>,
    ) -> Result<(u64, u64)> {
        unimplemented!()
    }

    fn ingest_maybe_slowdown_writes(&self, cf: &str) -> Result<bool> {
        unimplemented!()
    }

    fn get_sst_key_ranges(&self, cf: &str, level: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        unimplemented!()
    }

    fn get_engine_used_size(&self) -> Result<u64> {
        unimplemented!()
    }

    fn path(&self) -> &str {
        unimplemented!()
    }

    fn sync_wal(&self) -> Result<()> {
        unimplemented!()
    }

    fn pause_background_work(&self) -> Result<()> {
        unimplemented!()
    }

    fn continue_background_work(&self) -> Result<()> {
        unimplemented!()
    }

    fn exists(path: &str) -> bool {
        unimplemented!()
    }

    fn locked(path: &str) -> Result<bool> {
        unimplemented!()
    }

    fn dump_stats(&self) -> Result<String> {
        unimplemented!()
    }

    fn get_latest_sequence_number(&self) -> u64 {
        unimplemented!()
    }

    fn get_oldest_snapshot_sequence_number(&self) -> Option<u64> {
        unimplemented!()
    }

    fn get_total_sst_files_size_cf(&self, cf: &str) -> Result<Option<u64>> {
        unimplemented!()
    }

    fn get_num_keys(&self) -> Result<u64> {
        unimplemented!()
    }

    fn get_range_stats(
        &self,
        cf: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<Option<engine_traits::RangeStats>> {
        unimplemented!()
    }

    fn is_stalled_or_stopped(&self) -> bool {
        unimplemented!()
    }

    fn get_active_memtable_stats_cf(
        &self,
        cf: &str,
    ) -> Result<Option<(u64, std::time::SystemTime)>> {
        unimplemented!()
    }

    fn get_accumulated_flush_count_cf(cf: &str) -> Result<u64> {
        unimplemented!()
    }

    type DiskEngine = EK::DiskEngine;
    fn get_disk_engine(&self) -> &Self::DiskEngine {
        self.disk_engine().get_disk_engine()
    }
}
