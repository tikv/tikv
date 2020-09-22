// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::SkiplistEngine;
use engine_traits::{CompactExt, CompactedEvent, Result, CF_DEFAULT};

pub struct SkiplistCompactedEvent;

impl CompactedEvent for SkiplistCompactedEvent {
    fn total_bytes_declined(&self) -> u64 {
        0
    }

    fn is_size_declining_trivial(&self, split_check_diff: u64) -> bool {
        false
    }

    fn output_level_label(&self) -> String {
        "".to_string()
    }

    fn calc_ranges_declined_bytes(
        self,
        ranges: &std::collections::BTreeMap<Vec<u8>, u64>,
        bytes_threshold: u64,
    ) -> Vec<(u64, u64)> {
        vec![]
    }

    fn cf(&self) -> &str {
        CF_DEFAULT
    }
}

impl CompactExt for SkiplistEngine {
    type CompactedEvent = SkiplistCompactedEvent;

    fn auto_compactions_is_disabled(&self) -> Result<bool> {
        Ok(true)
    }

    fn compact_range(
        &self,
        cf: &str,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
        exclusive_manual: bool,
        max_subcompactions: u32,
    ) -> Result<()> {
        Ok(())
    }

    fn compact_files_in_range(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        output_level: Option<i32>,
    ) -> Result<()> {
        Ok(())
    }

    fn compact_files_in_range_cf(
        &self,
        cf_name: &str,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        output_level: Option<i32>,
    ) -> Result<()> {
        Ok(())
    }
}
