// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::RocksEngine;
use crate::properties_types::DecodeProperties;
use crate::util;
use engine_traits::Range;
use engine_traits::{Error, Result};
use rocksdb::table_properties_rc as rc;

impl RocksEngine {
    pub(crate) fn get_properties_of_tables_in_range(
        &self,
        cf: &str,
        ranges: &[Range],
    ) -> Result<RocksTablePropertiesCollection> {
        let cf = util::get_cf_handle(self.as_inner(), cf)?;
        // FIXME: extra allocation
        let ranges: Vec<_> = ranges.iter().map(util::range_to_rocks_range).collect();
        let raw = self
            .as_inner()
            .get_properties_of_tables_in_range_rc(cf, &ranges);
        let raw = raw.map_err(Error::Engine)?;
        Ok(RocksTablePropertiesCollection::from_raw(raw))
    }

    pub fn get_range_properties_cf(
        &self,
        cfname: &str,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<RocksTablePropertiesCollection> {
        let range = Range::new(start_key, end_key);
        self.get_properties_of_tables_in_range(cfname, &[range])
    }
}

pub struct RocksTablePropertiesCollection(pub rc::TablePropertiesCollection);

impl RocksTablePropertiesCollection {
    fn from_raw(raw: rc::TablePropertiesCollection) -> RocksTablePropertiesCollection {
        RocksTablePropertiesCollection(raw)
    }
}

impl DecodeProperties for rc::UserCollectedProperties {
    fn decode(&self, k: &str) -> tikv_util::codec::Result<&[u8]> {
        self.get(k.as_bytes())
            .ok_or(tikv_util::codec::Error::KeyNotFound)
    }
}

#[repr(transparent)]
pub struct RocksUserCollectedPropertiesNoRc(rocksdb::UserCollectedProperties);
impl DecodeProperties for RocksUserCollectedPropertiesNoRc {
    fn decode(&self, k: &str) -> tikv_util::codec::Result<&[u8]> {
        self.0
            .get(k.as_bytes())
            .ok_or(tikv_util::codec::Error::KeyNotFound)
    }
}
