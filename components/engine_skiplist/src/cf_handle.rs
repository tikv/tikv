// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::cf_options::SkiplistColumnFamilyOptions;
use crate::engine::SkiplistEngine;
use engine_traits::{CFHandle, CFHandleExt, Error, Result};

impl CFHandleExt for SkiplistEngine {
    type CFHandle = SkiplistCFHandle;
    type ColumnFamilyOptions = SkiplistColumnFamilyOptions;

    fn cf_handle(&self, name: &str) -> Result<&Self::CFHandle> {
        for handle in &self.cf_handles {
            if handle.cf_name == name {
                return Ok(handle);
            }
        }
        Err(Error::CFName(name.to_owned()))
    }
    fn get_options_cf(&self, cf: &Self::CFHandle) -> Self::ColumnFamilyOptions {
        SkiplistColumnFamilyOptions
    }
    fn set_options_cf(&self, cf: &Self::CFHandle, options: &[(&str, &str)]) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct SkiplistCFHandle {
    cf_name: &'static str,
}

impl CFHandle for SkiplistCFHandle {}
