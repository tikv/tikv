// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::errors::Result;
use crate::properties::DecodeProperties;
use crate::range::Range;
use crate::CFHandleExt;
use std::ops::Deref;

pub trait TablePropertiesExt: CFHandleExt {
    type TablePropertiesCollection: TablePropertiesCollection<
        Self::TablePropertiesCollectionIter,
        Self::TablePropertiesKey,
        Self::TableProperties,
        Self::UserCollectedProperties,
    >;
    type TablePropertiesCollectionIter: TablePropertiesCollectionIter<
        Self::TablePropertiesKey,
        Self::TableProperties,
        Self::UserCollectedProperties,
    >;
    type TablePropertiesKey: TablePropertiesKey;
    type TableProperties: TableProperties<Self::UserCollectedProperties>;
    type UserCollectedProperties: UserCollectedProperties;

    fn get_properties_of_tables_in_range(
        &self,
        cf: &Self::CFHandle,
        ranges: &[Range],
    ) -> Result<Self::TablePropertiesCollection>;

    fn get_range_properties_cf(
        &self,
        cfname: &str,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<Self::TablePropertiesCollection> {
        let cf = self.cf_handle(cfname)?;
        let range = Range::new(start_key, end_key);
        Ok(self.get_properties_of_tables_in_range(cf, &[range])?)
    }
}

pub trait TablePropertiesCollection<I, PKey, P, UCP>
where
    I: TablePropertiesCollectionIter<PKey, P, UCP>,
    PKey: TablePropertiesKey,
    P: TableProperties<UCP>,
    UCP: UserCollectedProperties,
{
    fn iter(&self) -> I;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub trait TablePropertiesCollectionIter<PKey, P, UCP>: Iterator<Item = (PKey, P)>
where
    PKey: TablePropertiesKey,
    P: TableProperties<UCP>,
    UCP: UserCollectedProperties,
{
}

pub trait TablePropertiesKey: Deref<Target = str> {}

pub trait TableProperties<UCP>
where
    UCP: UserCollectedProperties,
{
    fn num_entries(&self) -> u64;

    fn user_collected_properties(&self) -> UCP;
}

pub trait UserCollectedProperties: DecodeProperties {
    fn get(&self, index: &[u8]) -> Option<&[u8]>;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
