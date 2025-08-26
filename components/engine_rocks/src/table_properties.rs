// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{MvccProperties, Range, Result};

use crate::{RangeProperties, RocksEngine, mvcc_properties::RocksMvccProperties, r2e, util};

#[repr(transparent)]
pub struct UserCollectedProperties(rocksdb::UserCollectedProperties);
impl engine_traits::UserCollectedProperties for UserCollectedProperties {
    fn get(&self, index: &[u8]) -> Option<&[u8]> {
        self.0.get(index)
    }

    fn approximate_size_and_keys(&self, start: &[u8], end: &[u8]) -> Option<(usize, usize)> {
        let rp = RangeProperties::decode(&self.0).ok()?;
        let x = rp.get_approximate_distance_in_range(start, end);
        Some((x.0 as usize, x.1 as usize))
    }

    fn get_mvcc_properties(&self) -> Option<MvccProperties> {
        let mvcc = RocksMvccProperties::decode(&self.0).ok()?;
        Some(mvcc)
    }
}

#[repr(transparent)]
pub struct TableProperties(rocksdb::TableProperties);
impl engine_traits::TableProperties for TableProperties {
    type UserCollectedProperties = UserCollectedProperties;
    fn get_user_collected_properties(&self) -> &Self::UserCollectedProperties {
        unsafe {
            std::mem::transmute::<_, &UserCollectedProperties>(self.0.user_collected_properties())
        }
    }

    fn get_num_entries(&self) -> u64 {
        self.0.num_entries()
    }
}
#[repr(transparent)]
pub struct TablePropertiesCollection(rocksdb::TablePropertiesCollection);
impl engine_traits::TablePropertiesCollection for TablePropertiesCollection {
    type TableProperties = TableProperties;
    fn iter_table_properties<F>(&self, mut f: F)
    where
        F: FnMut(&Self::TableProperties) -> bool,
    {
        for (_, props) in self.0.into_iter() {
            let props = unsafe {
                std::mem::transmute::<&rocksdb::TableProperties, &TableProperties>(props)
            };
            if !f(props) {
                break;
            }
        }
    }
}

impl engine_traits::TablePropertiesExt for RocksEngine {
    type TablePropertiesCollection = TablePropertiesCollection;

    fn table_properties_collection(
        &self,
        cf: &str,
        ranges: &[Range<'_>],
    ) -> Result<Self::TablePropertiesCollection> {
        let collection = self.get_properties_of_tables_in_range(cf, ranges)?;
        Ok(TablePropertiesCollection(collection))
    }
}

impl RocksEngine {
    pub(crate) fn get_properties_of_tables_in_range(
        &self,
        cf: &str,
        ranges: &[Range<'_>],
    ) -> Result<rocksdb::TablePropertiesCollection> {
        let cf = util::get_cf_handle(self.as_inner(), cf)?;
        // FIXME: extra allocation
        let ranges: Vec<_> = ranges.iter().map(util::range_to_rocks_range).collect();
        self.as_inner()
            .get_properties_of_tables_in_range(cf, &ranges)
            .map_err(r2e)
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
