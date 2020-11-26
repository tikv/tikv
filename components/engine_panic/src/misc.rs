// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::PanicEngine;
use engine_traits::{DeleteStrategy, MiscExt, Range, Result};

impl MiscExt for PanicEngine {
    fn flush_cf(&self, cf: &str, sync: bool) -> Result<()> {
        panic!()
    }

    fn delete_ranges_cf(&self, cf: &str, strategy: DeleteStrategy, ranges: &[Range]) -> Result<()> {
        panic!()
    }

    fn get_approximate_memtable_stats_cf(&self, cf: &str, range: &Range) -> Result<(u64, u64)> {
        panic!()
    }
}
