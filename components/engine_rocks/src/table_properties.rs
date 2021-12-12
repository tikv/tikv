// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::RocksEngine;
use crate::util;
use engine_traits::Range;
use engine_traits::{Error, Result};

impl RocksEngine {
    pub(crate) fn get_properties_of_tables_in_range(
        &self,
        cf: &str,
        ranges: &[Range],
    ) -> Result<rocksdb::TablePropertiesCollection> {
        let cf = util::get_cf_handle(self.as_inner(), cf)?;
        // FIXME: extra allocation
        let ranges: Vec<_> = ranges.iter().map(util::range_to_rocks_range).collect();
        let raw = self
            .as_inner()
            .get_properties_of_tables_in_range(cf, &ranges);
        let raw = raw.map_err(Error::Engine)?;
        Ok(raw)
    }

    pub fn get_range_properties_cf(
        &self,
        cfname: &str,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<rocksdb::TablePropertiesCollection> {
        let range = Range::new(start_key, end_key);
        self.get_properties_of_tables_in_range(cfname, &[range])
    }
}
