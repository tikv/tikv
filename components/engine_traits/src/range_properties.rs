// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! Various metrics related to key ranges
//!
//! In RocksDB these are typically implemented with user collected properties,
//! which might require the database to be constructed with specific options.

use crate::Range;
use crate::errors::Result;

pub trait RangePropertiesExt {
    /// Gets the number of keys in a range.
    ///
    /// The region_id is used only for logging and means nothing internally.
    fn get_range_approximate_keys(&self, range: Range, region_id: u64, large_threshold: u64) -> Result<u64>;

    fn get_range_approximate_keys_cf(&self, cfname: &str, range: Range, region_id: u64, large_threshold: u64) -> Result<u64>;

    /// Get the approximate size of the range
    ///
    /// The region_id is used only for logging and means nothing internally.
    fn get_range_approximate_size(&self, range: Range, region_id: u64, large_threshold: u64) -> Result<u64>;

    fn get_range_approximate_size_cf(&self, cfname: &str, range: Range, region_id: u64, large_threshold: u64) -> Result<u64>;
}

