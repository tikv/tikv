// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::RocksEngine;
use engine_traits::{MiscExt, Result};
use crate::util;

impl MiscExt for RocksEngine {
    fn is_titan(&self) -> bool {
        self.as_inner().is_titan()
    }

    fn flush_cf(&self, cf: &str, sync: bool) -> Result<()> {
        let handle = util::get_cf_handle(self.as_inner(), cf)?;
        Ok(self.as_inner().flush_cf(handle, sync)?)
    }

    fn delete_files_in_range_cf(&self, cf: &str, start_key: &[u8], end_key: &[u8], include_end: bool) -> Result<()> {
        let handle = util::get_cf_handle(self.as_inner(), cf)?;
        Ok(self.as_inner().delete_files_in_range_cf(handle, start_key, end_key, include_end)?)
    }
}
