// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::cf_options::PanicColumnFamilyOptions;
use crate::engine::PanicEngine;
use engine_traits::{CFHandleExt, Result};

impl CFHandleExt for PanicEngine {
    type ColumnFamilyOptions = PanicColumnFamilyOptions;

    fn get_options_cf(&self, cf: &str) -> Result<Self::ColumnFamilyOptions> {
        panic!()
    }
    fn set_options_cf(&self, cf: &str, options: &[(&str, &str)]) -> Result<()> {
        panic!()
    }
}
