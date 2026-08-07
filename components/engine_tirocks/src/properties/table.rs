// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::mem;

use engine_traits::{Range, Result};
use tirocks::properties::table::{
    builtin::{OwnedTablePropertiesCollection, TableProperties},
    user::UserCollectedProperties,
};

use super::{mvcc::decode_mvcc, range::RangeProperties};
use crate::{RocksEngine, r2e};

#[repr(transparent)]
pub struct RocksUserCollectedProperties(UserCollectedProperties);

impl RocksUserCollectedProperties {
    #[inline]
    fn from_rocks(v: &UserCollectedProperties) -> &Self {
        unsafe { mem::transmute(v) }
    }
}

impl engine_traits::UserCollectedProperties for RocksUserCollectedProperties {
    #[inline]
    fn get(&self, index: &[u8]) -> Option<&[u8]> {
        self.0.get(index)
    }

    #[inline]
    fn approximate_size_and_keys(&self, start: &[u8], end: &[u8]) -> Option<(usize, usize)> {
        let rp = RangeProperties::decode(&self.0).ok()?;
        let x = rp.get_approximate_distance_in_range(start, end);
        Some((x.0 as usize, x.1 as usize))
    }

    fn get_mvcc_properties(&self) -> Option<engine_traits::MvccProperties> {
        decode_mvcc(&self.0).ok()
    }
}

#[repr(transparent)]
pub struct RocksTableProperties(TableProperties);

impl RocksTableProperties {
    #[inline]
    fn from_rocks(v: &TableProperties) -> &Self {
        unsafe { mem::transmute(v) }
    }
}

impl engine_traits::TableProperties for RocksTableProperties {
    type UserCollectedProperties = RocksUserCollectedProperties;

    fn get_user_collected_properties(&self) -> &Self::UserCollectedProperties {
        RocksUserCollectedProperties::from_rocks(self.0.user_collected_properties())
    }

    fn get_num_entries(&self) -> u64 {
        self.0.num_entries()
    }
}

#[repr(transparent)]
pub struct RocksTablePropertiesCollection(OwnedTablePropertiesCollection);

impl engine_traits::TablePropertiesCollection for RocksTablePropertiesCollection {
    type TableProperties = RocksTableProperties;

    #[inline]
    fn iter_table_properties<F>(&self, mut f: F)
    where
        F: FnMut(&Self::TableProperties) -> bool,
    {
        for (_, props) in &*self.0 {
            if !f(RocksTableProperties::from_rocks(props)) {
                break;
            }
        }
    }
}

impl engine_traits::TablePropertiesExt for RocksEngine {
    type TablePropertiesCollection = RocksTablePropertiesCollection;

    fn table_properties_collection(
        &self,
        cf: &str,
        ranges: &[Range<'_>],
    ) -> Result<Self::TablePropertiesCollection> {
        // FIXME: extra allocation
        let ranges: Vec<_> = ranges.iter().map(|r| (r.start_key, r.end_key)).collect();
        let collection = self.properties_of_tables_in_range(cf, &ranges)?;
        Ok(RocksTablePropertiesCollection(collection))
    }
}

impl RocksEngine {
    #[inline]
    pub(crate) fn properties_of_tables_in_range(
        &self,
        cf: &str,
        ranges: &[(&[u8], &[u8])],
    ) -> Result<OwnedTablePropertiesCollection> {
        let handle = self.cf(cf)?;
        let mut c = OwnedTablePropertiesCollection::default();
        self.as_inner()
            .properties_of_tables_in_range(handle, ranges, &mut c)
            .map_err(r2e)?;
        Ok(c)
    }

    #[inline]
    pub fn range_properties(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<OwnedTablePropertiesCollection> {
        self.properties_of_tables_in_range(cf, &[(start_key, end_key)])
    }
}
