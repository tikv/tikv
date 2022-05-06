// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::BTreeMap;

use engine_traits::{CompactExt, CompactedEvent, Result};

use crate::engine::PanicEngine;

impl CompactExt for PanicEngine {
    type CompactedEvent = PanicCompactedEvent;

    fn auto_compactions_is_disabled(&self) -> Result<bool> {
        panic!()
    }

    fn compact_range(
        &self,
        cf: &str,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
        exclusive_manual: bool,
        max_subcompactions: u32,
    ) -> Result<()> {
        panic!()
    }

    fn compact_files_in_range(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        output_level: Option<i32>,
    ) -> Result<()> {
        panic!()
    }

    fn compact_files_in_range_cf(
        &self,
        cf: &str,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        output_level: Option<i32>,
    ) -> Result<()> {
        panic!()
    }

    fn compact_files_cf(
        &self,
        cf: &str,
        files: Vec<String>,
        output_level: Option<i32>,
        max_subcompactions: u32,
        exclude_l0: bool,
    ) -> Result<()> {
        panic!()
    }
}

pub struct PanicCompactedEvent;

impl CompactedEvent for PanicCompactedEvent {
    fn total_bytes_declined(&self) -> u64 {
        panic!()
    }

    fn is_size_declining_trivial(&self, split_check_diff: u64) -> bool {
        panic!()
    }

    fn output_level_label(&self) -> String {
        panic!()
    }

    fn calc_ranges_declined_bytes(
        self,
        ranges: &BTreeMap<Vec<u8>, u64>,
        bytes_threshold: u64,
    ) -> Vec<(u64, u64)> {
        panic!()
    }

    fn cf(&self) -> &str {
        panic!()
    }
}
