// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::ops::{Deref, DerefMut};

use engine_traits::{DbOptions, DbOptionsExt, Result, TitanCfOptions};
use rocksdb::{DBOptions as RawDBOptions, TitanDBOptions as RawTitanDBOptions};
use tikv_util::box_err;

use crate::engine::RocksEngine;

impl DbOptionsExt for RocksEngine {
    type DbOptions = RocksDbOptions;

    fn get_db_options(&self) -> Self::DbOptions {
        RocksDbOptions::from_raw(self.as_inner().get_db_options())
    }
    fn set_db_options(&self, options: &[(&str, &str)]) -> Result<()> {
        self.as_inner()
            .set_db_options(options)
            .map_err(|e| box_err!(e))
    }
}

#[derive(Default)]
pub struct RocksDbOptions(RawDBOptions);

impl RocksDbOptions {
    pub fn from_raw(raw: RawDBOptions) -> RocksDbOptions {
        RocksDbOptions(raw)
    }

    pub fn into_raw(self) -> RawDBOptions {
        self.0
    }

    pub fn get_max_background_flushes(&self) -> i32 {
        self.0.get_max_background_flushes()
    }
}

impl Deref for RocksDbOptions {
    type Target = RawDBOptions;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RocksDbOptions {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl DbOptions for RocksDbOptions {
    type TitanDbOptions = RocksTitanDbOptions;

    fn new() -> Self {
        RocksDbOptions::from_raw(RawDBOptions::new())
    }

    fn get_max_background_jobs(&self) -> i32 {
        self.0.get_max_background_jobs()
    }

    fn get_rate_bytes_per_sec(&self) -> Option<i64> {
        self.0.get_rate_limiter().map(|r| r.get_bytes_per_second())
    }

    fn set_rate_bytes_per_sec(&mut self, rate_bytes_per_sec: i64) -> Result<()> {
        if let Some(r) = self.0.get_rate_limiter() {
            r.set_bytes_per_second(rate_bytes_per_sec);
        } else {
            return Err(box_err!("rate limiter not found"));
        }
        Ok(())
    }

    fn get_rate_limiter_auto_tuned(&self) -> Option<bool> {
        self.0.get_rate_limiter().map(|r| r.get_auto_tuned())
    }

    fn set_rate_limiter_auto_tuned(&mut self, rate_limiter_auto_tuned: bool) -> Result<()> {
        if let Some(r) = self.0.get_rate_limiter() {
            r.set_auto_tuned(rate_limiter_auto_tuned);
        } else {
            return Err(box_err!("rate limiter not found"));
        }
        Ok(())
    }

    fn set_titandb_options(&mut self, opts: &Self::TitanDbOptions) {
        self.0.set_titandb_options(opts.as_raw())
    }
}

pub struct RocksTitanDbOptions(RawTitanDBOptions);

impl RocksTitanDbOptions {
    pub fn from_raw(raw: RawTitanDBOptions) -> RocksTitanDbOptions {
        RocksTitanDbOptions(raw)
    }

    pub fn as_raw(&self) -> &RawTitanDBOptions {
        &self.0
    }
}

impl Deref for RocksTitanDbOptions {
    type Target = RawTitanDBOptions;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RocksTitanDbOptions {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl TitanCfOptions for RocksTitanDbOptions {
    fn new() -> Self {
        RocksTitanDbOptions::from_raw(RawTitanDBOptions::new())
    }

    fn set_min_blob_size(&mut self, size: u64) {
        self.0.set_min_blob_size(size)
    }
}
