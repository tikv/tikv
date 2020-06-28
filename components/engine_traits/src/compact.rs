// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! Functionality related to compaction

use crate::errors::Result;

pub trait CompactExt {
    /// Checks whether any column family sets `disable_auto_compactions` to `True` or not.
    fn auto_compactions_is_disabled(&self) -> Result<bool>;

    /// Compacts the column families in the specified range by manual or not.
    fn compact_range(
        &self,
        cf: &str,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
        exclusive_manual: bool,
        max_subcompactions: u32,
    ) -> Result<()>;

    /// Compacts files in the range and above the output level.
    /// Compacts all files if the range is not specified.
    /// Compacts all files to the bottommost level if the output level is not specified.
    fn compact_files_in_range(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        output_level: Option<i32>,
    ) -> Result<()>;

    /// Compacts files in the range and above the output level of the given column family.
    /// Compacts all files to the bottommost level if the output level is not specified.
    fn compact_files_in_range_cf(
        &self,
        cf_name: &str,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        output_level: Option<i32>,
    ) -> Result<()>;
}
