// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{DbOptions, DbOptionsExt, Result, TitanCfOptions};

use crate::engine::PanicEngine;

impl DbOptionsExt for PanicEngine {
    type DbOptions = PanicDbOptions;

    fn get_db_options(&self) -> Self::DbOptions {
        panic!()
    }
    fn set_db_options(&self, options: &[(&str, &str)]) -> Result<()> {
        panic!()
    }
}

pub struct PanicDbOptions;

impl DbOptions for PanicDbOptions {
    type TitanDbOptions = PanicTitanDbOptions;

    fn new() -> Self {
        panic!()
    }

    fn get_max_background_jobs(&self) -> i32 {
        panic!()
    }

    fn get_rate_bytes_per_sec(&self) -> Option<i64> {
        panic!()
    }

    fn set_rate_bytes_per_sec(&mut self, rate_bytes_per_sec: i64) -> Result<()> {
        panic!()
    }

    fn get_rate_limiter_auto_tuned(&self) -> Option<bool> {
        panic!()
    }

    fn set_rate_limiter_auto_tuned(&mut self, rate_limiter_auto_tuned: bool) -> Result<()> {
        panic!()
    }

    fn set_titandb_options(&mut self, opts: &Self::TitanDbOptions) {
        panic!()
    }

    fn set_track_and_verify_wals_in_manifest(&mut self, v: bool) {
        panic!()
    }
}

pub struct PanicTitanDbOptions;

impl TitanCfOptions for PanicTitanDbOptions {
    fn new() -> Self {
        panic!()
    }
    fn set_min_blob_size(&mut self, size: u64) {
        panic!()
    }
}
