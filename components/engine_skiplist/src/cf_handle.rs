// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{CFHandle, CFHandleExt, CfName, Error, Result};
use std::hash::{Hash, Hasher};

use crate::cf_options::SkiplistColumnFamilyOptions;
use crate::engine::SkiplistEngine;

impl CFHandleExt for SkiplistEngine {
    type CFHandle = SkiplistCFHandle;
    type ColumnFamilyOptions = SkiplistColumnFamilyOptions;

    fn cf_handle(&self, name: &str) -> Result<&Self::CFHandle> {
        self.cf_handles
            .get(name)
            .ok_or_else(|| Error::CFName(name.to_owned()))
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
    pub seq_no: usize,
    pub cf_name: CfName,
}

impl CFHandle for SkiplistCFHandle {}

impl Hash for SkiplistCFHandle {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.seq_no.hash(state);
    }
}

impl PartialEq for SkiplistCFHandle {
    fn eq(&self, other: &SkiplistCFHandle) -> bool {
        self.seq_no == other.seq_no
    }
}

impl Eq for SkiplistCFHandle {}
