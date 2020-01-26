// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::errors::Result;
use crate::range::Range;
use crate::CFHandleExt;
use std::ops::Deref;

pub trait TablePropertiesExt: CFHandleExt {
    type TablePropertiesCollection: TablePropertiesCollection<
        Self::TablePropertiesCollectionIter,
        Self::TablePropertiesKey,
        Self::TableProperties,
        Self::UserCollectedProperties,
        Self::UserCollectedPropertiesIter,
    >;
    type TablePropertiesCollectionIter: TablePropertiesCollectionIter<
        Self::TablePropertiesKey,
        Self::TableProperties,
        Self::UserCollectedProperties,
        Self::UserCollectedPropertiesIter,
    >;
    type TablePropertiesKey: TablePropertiesKey;
    type TableProperties: TableProperties<
        Self::UserCollectedProperties,
        Self::UserCollectedPropertiesIter,
    >;
    type UserCollectedProperties: UserCollectedProperties<Self::UserCollectedPropertiesIter>;
    type UserCollectedPropertiesIter: UserCollectedPropertiesIter;

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

pub trait TablePropertiesCollection<I, PKey, P, UCP, UCPI>
where
    I: TablePropertiesCollectionIter<PKey, P, UCP, UCPI>,
    PKey: TablePropertiesKey,
    P: TableProperties<UCP, UCPI>,
    UCP: UserCollectedProperties<UCPI>,
    UCPI: UserCollectedPropertiesIter,
{
    fn iter(&self) -> I;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub trait TablePropertiesCollectionIter<PKey, P, UCP, UCPI>
where
    Self: Iterator<Item = (PKey, P)>,
    PKey: TablePropertiesKey,
    P: TableProperties<UCP, UCPI>,
    UCP: UserCollectedProperties<UCPI>,
    UCPI: UserCollectedPropertiesIter,
{
}

pub trait TablePropertiesKey
where
    Self: Deref<Target = str>,
{
}

pub trait TableProperties<UCP, UCPI>
where
    UCP: UserCollectedProperties<UCPI>,
    UCPI: UserCollectedPropertiesIter,
{
    fn num_entries(&self) -> u64;

    fn user_collected_properties(&self) -> UCP;
}

pub trait UserCollectedProperties<UCPI>
where
    UCPI: UserCollectedPropertiesIter,
{
    fn iter(&self) -> UCPI;

    fn get<Q: AsRef<[u8]>>(&self, index: Q) -> Option<&[u8]>;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub trait UserCollectedPropertiesIter {}
