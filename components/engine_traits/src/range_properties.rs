// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! Various metrics related to key ranges
//!
//! In RocksDB these are typically implemented with user collected properties,
//! which might require the database to be constructed with specific options.

use crate::errors::Result;
use crate::{Range, CfName};

pub trait RangePropertiesExt {
    /// Gets the number of keys in a range.
    ///
    /// The region_id is used only for logging and means nothing internally.
    fn get_range_approximate_keys(
        &self,
        range: Range,
        region_id: u64,
        large_threshold: u64,
    ) -> Result<(CfName, u64)>;

    fn get_range_approximate_keys_cf(
        &self,
        cfname: &str,
        range: Range,
        region_id: u64,
        large_threshold: u64,
    ) -> Result<u64>;

    /// Get the approximate size of the range
    ///
    /// The region_id is used only for logging and means nothing internally.
    fn get_range_approximate_size(
        &self,
        range: Range,
        region_id: u64,
        large_threshold: u64,
    ) -> Result<u64>;

    fn get_range_approximate_size_cf(
        &self,
        cfname: &str,
        range: Range,
        region_id: u64,
        large_threshold: u64,
    ) -> Result<u64>;

    /// Get range approximate split keys based on default, write and lock cf.
    ///
    /// The region_id is used only for logging and means nothing internally.
    fn get_range_approximate_split_keys(
        &self,
        range: Range,
        region_id: u64,
        split_size: u64,
        max_size: u64,
        batch_split_limit: u64,
    ) -> Result<Vec<Vec<u8>>>;

    fn get_range_approximate_split_keys_cf(
        &self,
        cfname: &str,
        range: Range,
        region_id: u64,
        split_size: u64,
        max_size: u64,
        batch_split_limit: u64,
    ) -> Result<Vec<Vec<u8>>>;

    /// Get range approximate middle key based on default and write cf size.
    ///
    /// The region_id is used only for logging and means nothing internally.
    fn get_range_approximate_middle(&self, range: Range, region_id: u64)
        -> Result<Option<Vec<u8>>>;

    /// Get the approximate middle key of the region. If we suppose the region
    /// is stored on disk as a plain file, "middle key" means the key whose
    /// position is in the middle of the file.
    ///
    /// The returned key maybe is timestamped if transaction KV is used,
    /// and must start with "z".
    fn get_range_approximate_middle_cf(
        &self,
        cfname: &str,
        range: Range,
        region_id: u64,
    ) -> Result<Option<Vec<u8>>>;

    fn divide_range(&self, range: Range, region_id: u64, parts: usize) -> Result<Vec<Vec<u8>>>;

    fn divide_range_cf(
        &self,
        cf: &str,
        range: Range,
        region_id: u64,
        parts: usize,
    ) -> Result<Vec<Vec<u8>>>;
}
