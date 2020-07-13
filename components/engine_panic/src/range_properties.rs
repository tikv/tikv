// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{RangePropertiesExt, Result, Range};
use crate::engine::PanicEngine;

impl RangePropertiesExt for PanicEngine {
    fn get_range_approximate_keys(&self, range: Range, region_id: u64, large_threshold: u64) -> Result<u64> {
        panic!()
    }

    fn get_range_approximate_keys_cf(&self, cfname: &str, range: Range, region_id: u64, large_threshold: u64) -> Result<u64> {
        panic!()
    }

    fn get_range_approximate_size(&self, range: Range, region_id: u64, large_threshold: u64) -> Result<u64> {
        panic!()
    }

    fn get_range_approximate_size_cf(&self, cfname: &str, range: Range, region_id: u64, large_threshold: u64) -> Result<u64> {
        panic!()
    }
}
