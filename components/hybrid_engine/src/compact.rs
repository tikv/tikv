// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{CompactExt, KvEngine, ManualCompactionOptions, RangeCacheEngine, Result};

use crate::engine::HybridEngine;

impl<EK, EC> CompactExt for HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    type CompactedEvent = EK::CompactedEvent;

    fn auto_compactions_is_disabled(&self) -> Result<bool> {
        self.disk_engine().auto_compactions_is_disabled()
    }

    fn compact_range_cf(
        &self,
        cf: &str,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
        compaction_option: ManualCompactionOptions,
    ) -> Result<()> {
        self.disk_engine()
            .compact_range_cf(cf, start_key, end_key, compaction_option)
    }

    fn compact_files_in_range_cf(
        &self,
        cf: &str,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        output_level: Option<i32>,
    ) -> Result<()> {
        self.disk_engine()
            .compact_files_in_range_cf(cf, start, end, output_level)
    }

    fn compact_files_in_range(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        output_level: Option<i32>,
    ) -> Result<()> {
        self.disk_engine()
            .compact_files_in_range(start, end, output_level)
    }

    fn compact_files_cf(
        &self,
        cf: &str,
        files: Vec<String>,
        output_level: Option<i32>,
        max_subcompactions: u32,
        exclude_l0: bool,
    ) -> Result<()> {
        self.disk_engine()
            .compact_files_cf(cf, files, output_level, max_subcompactions, exclude_l0)
    }

    fn check_in_range(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Result<()> {
        self.disk_engine().check_in_range(start, end)
    }
}
