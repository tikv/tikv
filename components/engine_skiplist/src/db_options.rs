// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::SkiplistEngine;
use engine_traits::Result;
use engine_traits::{DBOptions, DBOptionsExt, TitanDBOptions};

impl DBOptionsExt for SkiplistEngine {
    type DBOptions = SkiplistDBOptions;

    fn get_db_options(&self) -> Self::DBOptions {
        SkiplistDBOptions
    }
    fn set_db_options(&self, options: &[(&str, &str)]) -> Result<()> {
        Ok(())
    }
}

pub struct SkiplistDBOptions;

impl DBOptions for SkiplistDBOptions {
    type TitanDBOptions = SkiplistTitanDBOptions;

    fn new() -> Self {
        Self
    }

    fn get_max_background_jobs(&self) -> i32 {
        std::i32::MIN
    }

    fn get_rate_bytes_per_sec(&self) -> Option<i64> {
        None
    }

    fn set_rate_bytes_per_sec(&mut self, rate_bytes_per_sec: i64) -> Result<()> {
        Ok(())
    }

    fn set_titandb_options(&mut self, opts: &Self::TitanDBOptions) {}
}

pub struct SkiplistTitanDBOptions;

impl TitanDBOptions for SkiplistTitanDBOptions {
    fn new() -> Self {
        Self
    }
    fn set_min_blob_size(&mut self, size: u64) {}
}
