// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::cf_options::PanicColumnFamilyOptions;
use crate::engine::PanicEngine;
use engine_traits::{CFHandle, CFHandleExt, Result};

impl CFHandleExt for PanicEngine {
    type CFHandle = PanicCFHandle;
    type ColumnFamilyOptions = PanicColumnFamilyOptions;

    fn cf_handle(&self, name: &str) -> Result<&Self::CFHandle> {
        panic!()
    }
    fn get_options_cf(&self, cf: &Self::CFHandle) -> Self::ColumnFamilyOptions {
        panic!()
    }
    fn set_options_cf(&self, cf: &Self::CFHandle, options: &[(&str, &str)]) -> Result<()> {
        panic!()
    }
}

pub struct PanicCFHandle;

impl CFHandle for PanicCFHandle {}
