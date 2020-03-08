// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{CompactExt, Result, CFNamesExt};
use crate::engine::RocksEngine;
use crate::util;

impl CompactExt for RocksEngine {
    fn auto_compactions_is_disabled(&self) -> Result<bool> {
        for cf_name in self.cf_names() {
            let cf = util::get_cf_handle(self.as_inner(), cf_name)?;
            if self.as_inner().get_options_cf(cf).get_disable_auto_compactions() {
                return Ok(true);
            }
        }
        Ok(false)
    }
}
