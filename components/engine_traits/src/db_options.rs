// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::errors::Result;

/// A trait for engines that support setting global options
pub trait DBOptionsExt {
    type DBOptions: DBOptions;

    fn get_db_options(&self) -> Self::DBOptions;
    fn set_db_options(&self, options: &[(&str, &str)]) -> Result<()>;
}

/// A handle to a database's options
pub trait DBOptions {
    type TitanDBOptions: TitanDBOptions;

    fn new() -> Self;
    fn get_max_background_jobs(&self) -> i32;
    fn set_titandb_options(&mut self, opts: &Self::TitanDBOptions);
}

/// Titan-specefic options
pub trait TitanDBOptions {
    fn new() -> Self;
    fn set_min_blob_size(&mut self, size: u64);
}
