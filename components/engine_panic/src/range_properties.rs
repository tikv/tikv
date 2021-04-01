// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::PanicEngine;
use engine_traits::{Range, RangePropertiesExt, Result};

impl RangePropertiesExt for PanicEngine {
    fn get_range_approximate_keys(&self, range: Range, large_threshold: u64) -> Result<u64> {
        panic!()
    }

    fn get_range_approximate_keys_cf(
        &self,
        cfname: &str,
        range: Range,
        large_threshold: u64,
    ) -> Result<u64> {
        panic!()
    }

    fn get_range_approximate_size(&self, range: Range, large_threshold: u64) -> Result<u64> {
        panic!()
    }

    fn get_range_approximate_size_cf(
        &self,
        cfname: &str,
        range: Range,
        large_threshold: u64,
    ) -> Result<u64> {
        panic!()
    }

    fn get_range_approximate_split_keys(
        &self,
        range: Range,
        key_count: usize,
    ) -> Result<Vec<Vec<u8>>> {
        panic!()
    }

    fn get_range_approximate_split_keys_cf(
        &self,
        cfname: &str,
        range: Range,
        key_count: usize,
    ) -> Result<Vec<Vec<u8>>> {
        panic!()
    }
}
