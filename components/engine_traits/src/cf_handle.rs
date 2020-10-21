// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::cf_options::ColumnFamilyOptions;
use crate::errors::Result;

/// Trait for engines with column family handles.
pub trait CFHandleExt {
    type ColumnFamilyOptions: ColumnFamilyOptions;

    fn get_options_cf(&self, cf: &str) -> Result<Self::ColumnFamilyOptions>;
    fn set_options_cf(&self, cf: &str, options: &[(&str, &str)]) -> Result<()>;
}
