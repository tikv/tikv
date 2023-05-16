// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{DeleteStrategy, MiscExt, Range, RangeStats, Result, StatisticsReporter};

use crate::engine::PanicEngine;

pub struct PanicReporter;

impl StatisticsReporter<PanicEngine> for PanicReporter {
    fn new(name: &str) -> Self {
        panic!()
    }

    fn collect(&mut self, engine: &PanicEngine) {
        panic!()
    }

    fn flush(&mut self) {
        panic!()
    }
}

impl MiscExt for PanicEngine {
    type StatisticsReporter = PanicReporter;

    fn flush_cfs(&self, cfs: &[&str], wait: bool) -> Result<()> {
        panic!()
    }

    fn flush_cf(&self, cf: &str, wait: bool) -> Result<()> {
        panic!()
    }

    fn delete_ranges_cf(
        &self,
        cf: &str,
        strategy: DeleteStrategy,
        ranges: &[Range<'_>],
    ) -> Result<()> {
        panic!()
    }

    fn get_approximate_memtable_stats_cf(&self, cf: &str, range: &Range<'_>) -> Result<(u64, u64)> {
        panic!()
    }

    fn ingest_maybe_slowdown_writes(&self, cf: &str) -> Result<bool> {
        panic!()
    }

    fn get_sst_key_ranges(&self, cf: &str, level: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        panic!()
    }

    fn get_engine_used_size(&self) -> Result<u64> {
        panic!()
    }

    fn path(&self) -> &str {
        panic!()
    }

    fn sync_wal(&self) -> Result<()> {
        panic!()
    }

    fn pause_background_work(&self) -> Result<()> {
        panic!()
    }

    fn continue_background_work(&self) -> Result<()> {
        panic!()
    }

    fn exists(path: &str) -> bool {
        panic!()
    }

    fn locked(path: &str) -> Result<bool> {
        panic!()
    }

    fn dump_stats(&self) -> Result<String> {
        panic!()
    }

    fn get_latest_sequence_number(&self) -> u64 {
        panic!()
    }

    fn get_oldest_snapshot_sequence_number(&self) -> Option<u64> {
        panic!()
    }

    fn get_total_sst_files_size_cf(&self, cf: &str) -> Result<Option<u64>> {
        panic!()
    }

    fn get_num_keys(&self) -> Result<u64> {
        panic!()
    }

    fn get_range_stats(&self, cf: &str, start: &[u8], end: &[u8]) -> Result<Option<RangeStats>> {
        panic!()
    }

    fn is_stalled_or_stopped(&self) -> bool {
        panic!()
    }
}
