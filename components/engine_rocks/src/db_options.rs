// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{DBOptions, DBOptionsExt, Result, TitanDBOptions};
use rocksdb::{DBOptions as RawDBOptions, TitanDBOptions as RawTitanDBOptions};
use tikv_util::box_err;

use crate::engine::RocksEngine;

impl DBOptionsExt for RocksEngine {
    type DBOptions = RocksDBOptions;

    fn get_db_options(&self) -> Self::DBOptions {
        RocksDBOptions::from_raw(self.as_inner().get_db_options())
    }
    fn set_db_options(&self, options: &[(&str, &str)]) -> Result<()> {
        self.as_inner()
            .set_db_options(options)
            .map_err(|e| box_err!(e))
    }
}

pub struct RocksDBOptions(RawDBOptions);

impl RocksDBOptions {
    pub fn from_raw(raw: RawDBOptions) -> RocksDBOptions {
        RocksDBOptions(raw)
    }

    pub fn into_raw(self) -> RawDBOptions {
        self.0
    }

    pub fn get_max_background_flushes(&self) -> i32 {
        self.0.get_max_background_flushes()
    }
}

impl DBOptions for RocksDBOptions {
    type TitanDBOptions = RocksTitanDBOptions;

    fn new() -> Self {
        RocksDBOptions::from_raw(RawDBOptions::new())
    }

    fn get_max_background_jobs(&self) -> i32 {
        self.0.get_max_background_jobs()
    }

    fn get_rate_bytes_per_sec(&self) -> Option<i64> {
        self.0.get_rate_bytes_per_sec()
    }

    fn set_rate_bytes_per_sec(&mut self, rate_bytes_per_sec: i64) -> Result<()> {
        self.0
            .set_rate_bytes_per_sec(rate_bytes_per_sec)
            .map_err(|e| box_err!(e))
    }

    fn get_rate_limiter_auto_tuned(&self) -> Option<bool> {
        self.0.get_auto_tuned()
    }

    fn set_rate_limiter_auto_tuned(&mut self, rate_limiter_auto_tuned: bool) -> Result<()> {
        self.0
            .set_auto_tuned(rate_limiter_auto_tuned)
            .map_err(|e| box_err!(e))
    }

    fn set_titandb_options(&mut self, opts: &Self::TitanDBOptions) {
        self.0.set_titandb_options(opts.as_raw())
    }
}

pub struct RocksTitanDBOptions(RawTitanDBOptions);

impl RocksTitanDBOptions {
    pub fn from_raw(raw: RawTitanDBOptions) -> RocksTitanDBOptions {
        RocksTitanDBOptions(raw)
    }

    pub fn as_raw(&self) -> &RawTitanDBOptions {
        &self.0
    }
}

impl TitanDBOptions for RocksTitanDBOptions {
    fn new() -> Self {
        RocksTitanDBOptions::from_raw(RawTitanDBOptions::new())
    }

    fn set_min_blob_size(&mut self, size: u64) {
        self.0.set_min_blob_size(size)
    }
}
