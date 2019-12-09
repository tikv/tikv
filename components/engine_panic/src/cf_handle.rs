// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{CFHandleExt, CFHandle, Result};
use crate::engine::PanicEngine;
use crate::cf_options::PanicColumnFamilyOptions;

impl CFHandleExt for PanicEngine {
    type CFHandle = PanicCFHandle;
    type ColumnFamilyOptions = PanicColumnFamilyOptions;

    fn cf_handle(&self, name: &str) -> Result<&Self::CFHandle> { panic!() }
    fn get_options_cf(&self, cf: &Self::CFHandle) -> Self::ColumnFamilyOptions { panic!() }
    fn set_options_cf(&self, cf: &Self::CFHandle, options: &[(&str, &str)]) -> Result<()> { panic!() }
}

pub struct PanicCFHandle;

impl CFHandle for PanicCFHandle { }
