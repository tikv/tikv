// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::SkiplistEngine;
use engine_traits::{MiscExt, Range, Result, SyncMutable};
use std::sync::atomic::Ordering;

impl MiscExt for SkiplistEngine {
    fn flush(&self, sync: bool) -> Result<()> {
        Ok(())
    }

    fn flush_cf(&self, cf: &str, sync: bool) -> Result<()> {
        Ok(())
    }

    fn delete_files_in_range_cf(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
        include_end: bool,
    ) -> Result<()> {
        self.delete_range_cf(cf, start_key, end_key)
    }

    fn get_approximate_memtable_stats_cf(&self, cf: &str, range: &Range) -> Result<(u64, u64)> {
        Ok((0, 0))
    }

    fn ingest_maybe_slowdown_writes(&self, cf: &str) -> Result<bool> {
        Ok(false)
    }

    fn get_engine_used_size(&self) -> Result<u64> {
        Ok(self.total_bytes.load(Ordering::Relaxed) as u64)
    }

    fn roughly_cleanup_ranges(&self, ranges: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        for range in ranges {
            self.delete_all_in_range(range.0.as_slice(), range.1.as_slice(), false)?;
        }
        Ok(())
    }

    fn path(&self) -> &str {
        ""
    }

    fn sync_wal(&self) -> Result<()> {
        Ok(())
    }

    fn exists(path: &str) -> bool {
        true
    }

    fn dump_stats(&self) -> Result<String> {
        Ok("".to_owned())
    }

    fn get_latest_sequence_number(&self) -> u64 {
        std::u64::MIN
    }

    fn get_oldest_snapshot_sequence_number(&self) -> Option<u64> {
        Some(self.get_latest_sequence_number() + 1)
    }

    fn delete_blob_files_in_range_cf(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
        include_end: bool,
    ) -> Result<()> {
        self.delete_range_cf(cf, start_key, end_key)
    }

    fn delete_all_in_range_cf(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
        use_delete_range: bool,
    ) -> Result<()> {
        self.delete_range_cf(cf, start_key, end_key)
    }
}
