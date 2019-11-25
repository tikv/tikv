// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{TablePropertiesExt, TablePropertiesCollection};
use crate::engine::RocksEngine;
use rocksdb::TablePropertiesCollection as RawTablePropertiesCollection;
use engine_traits::Range;
use engine_traits::{Result, Error};
use crate::util;

impl TablePropertiesExt for RocksEngine {
    type TablePropertiesCollection = RocksTablePropertiesCollection;

    fn get_properties_of_tables_in_range(
        &self,
        cf: &Self::CFHandle,
        ranges: &[Range],
    ) -> Result<Self::TablePropertiesCollection> {
        // FIXME: extra allocation
        let ranges: Vec<_> = ranges.iter().map(util::range_to_rocks_range).collect();
        let raw = self.as_inner().get_properties_of_tables_in_range(cf.as_inner(), &ranges);
        let raw = raw.map_err(Error::Engine)?;
        Ok(RocksTablePropertiesCollection::from_raw(raw))
    }
}

pub struct RocksTablePropertiesCollection(RawTablePropertiesCollection);

impl RocksTablePropertiesCollection {
    pub fn from_raw(raw: RawTablePropertiesCollection) -> RocksTablePropertiesCollection {
        RocksTablePropertiesCollection(raw)
    }
}

impl TablePropertiesCollection for RocksTablePropertiesCollection {
}
