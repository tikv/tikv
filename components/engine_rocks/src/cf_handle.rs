// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::cf_options::RocksColumnFamilyOptions;
use crate::engine::RocksEngine;
use engine_traits::CFHandleExt;
use engine_traits::{Result};
use crate::util;

impl CFHandleExt for RocksEngine {
    type ColumnFamilyOptions = RocksColumnFamilyOptions;

    fn get_options_cf(&self, cf: &str) -> Result<Self::ColumnFamilyOptions> {
        let handle = util::get_cf_handle(self.as_inner(), cf)?;
        Ok(RocksColumnFamilyOptions::from_raw(self.as_inner().get_options_cf(handle)))
    }

    fn set_options_cf(&self, cf: &str, options: &[(&str, &str)]) -> Result<()> {
        let handle = util::get_cf_handle(self.as_inner(), cf)?;
        self.as_inner()
            .set_options_cf(handle, options)
            .map_err(|e| box_err!(e))
    }
}
