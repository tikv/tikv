// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::PanicEngine;
use engine_traits::{CompactExt, Result};

impl CompactExt for PanicEngine {
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
        cf_name: &str,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        output_level: Option<i32>,
    ) -> Result<()> {
        panic!()
    }
}
