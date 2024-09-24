// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, Range, RegionCacheEngine, RangePropertiesExt, Result};

use crate::engine::HybridEngine;

impl<EK, EC> RangePropertiesExt for HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
{
    fn get_range_approximate_keys(&self, range: Range<'_>, large_threshold: u64) -> Result<u64> {
        self.disk_engine()
            .get_range_approximate_keys(range, large_threshold)
    }

    fn get_range_approximate_keys_cf(
        &self,
        cfname: &str,
        range: Range<'_>,
        large_threshold: u64,
    ) -> Result<u64> {
        self.disk_engine()
            .get_range_approximate_keys_cf(cfname, range, large_threshold)
    }

    fn get_range_approximate_size(&self, range: Range<'_>, large_threshold: u64) -> Result<u64> {
        self.disk_engine()
            .get_range_approximate_size(range, large_threshold)
    }

    fn get_range_approximate_size_cf(
        &self,
        cfname: &str,
        range: Range<'_>,
        large_threshold: u64,
    ) -> Result<u64> {
        self.disk_engine()
            .get_range_approximate_size_cf(cfname, range, large_threshold)
    }

    fn get_range_approximate_split_keys(
        &self,
        range: Range<'_>,
        key_count: usize,
    ) -> Result<Vec<Vec<u8>>> {
        self.disk_engine()
            .get_range_approximate_split_keys(range, key_count)
    }

    fn get_range_approximate_split_keys_cf(
        &self,
        cfname: &str,
        range: Range<'_>,
        key_count: usize,
    ) -> Result<Vec<Vec<u8>>> {
        self.disk_engine()
            .get_range_approximate_split_keys_cf(cfname, range, key_count)
    }
}
