// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::errors::Result;

/// A trait for engines that support setting global options
pub trait DbOptionsExt {
    type DbOptions: DbOptions;

    fn get_db_options(&self) -> Self::DbOptions;
    fn set_db_options(&self, options: &[(&str, &str)]) -> Result<()>;
}

/// A handle to a database's options
pub trait DbOptions {
    type TitanDbOptions: TitanCfOptions;

    fn new() -> Self;
    fn get_max_background_jobs(&self) -> i32;
    fn get_rate_bytes_per_sec(&self) -> Option<i64>;
    fn set_rate_bytes_per_sec(&mut self, rate_bytes_per_sec: i64) -> Result<()>;
    fn get_rate_limiter_auto_tuned(&self) -> Option<bool>;
    fn set_rate_limiter_auto_tuned(&mut self, rate_limiter_auto_tuned: bool) -> Result<()>;
    fn set_flush_size(&mut self, f: usize) -> Result<()>;
    fn get_flush_size(&self) -> Result<u64>;
    fn set_flush_oldest_first(&mut self, f: bool) -> Result<()>;
    fn set_titandb_options(&mut self, opts: &Self::TitanDbOptions);
}

/// Titan-specefic options
pub trait TitanCfOptions {
    fn new() -> Self;
    fn set_min_blob_size(&mut self, size: u64);
}
