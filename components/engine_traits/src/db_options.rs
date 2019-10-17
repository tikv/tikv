// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::result::Result as StdResult;

/// A trait for engines that support setting global options
pub trait DBOptionsExt {
    type DBOptions: DBOptions;

    fn get_db_options(&self) -> Self::DBOptions;
    // FIXME: return type
    fn set_db_options(&self, options: &[(&str, &str)]) -> StdResult<(), String>;
}

/// A handle to a database's options
pub trait DBOptions {
    fn get_max_background_jobs(&self) -> i32;
}
