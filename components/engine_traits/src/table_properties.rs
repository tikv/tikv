// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::ops::Deref;
use crate::errors::Result;
use crate::range::Range;
use crate::CFHandleExt;

pub trait TablePropertiesExt: CFHandleExt
{
    
    type TablePropertiesCollection: TablePropertiesCollection<Self::TableProperties, Self::TablePropertiesCollectionIter, Self::TablePropertiesStringRef, Self::TablePropertiesRef, Self::UserCollectedProperties, Self::UserCollectedPropertiesIter>;
    type TablePropertiesCollectionIter: TablePropertiesCollectionIter<Self::TableProperties, Self::TablePropertiesStringRef, Self::TablePropertiesRef, Self::UserCollectedProperties, Self::UserCollectedPropertiesIter>;
    type TableProperties: TableProperties<Self::UserCollectedProperties, Self::UserCollectedPropertiesIter>;
    type TablePropertiesStringRef: TablePropertiesStringRef;
    type TablePropertiesRef: TablePropertiesRef<Self::TableProperties, Self::UserCollectedProperties, Self::UserCollectedPropertiesIter>;
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

pub trait TablePropertiesCollection<P, I, SRef, PRef, UCP, UCPI,>
where P: TableProperties<UCP, UCPI>,
      I: TablePropertiesCollectionIter<P, SRef, PRef, UCP, UCPI>,
      SRef: TablePropertiesStringRef,
      PRef: TablePropertiesRef<P, UCP, UCPI>,
      UCP: UserCollectedProperties<UCPI>,
      UCPI: UserCollectedPropertiesIter,
{
    fn iter(&self) -> I;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub trait TablePropertiesCollectionIter<P, SRef, PRef, UCP, UCPI>
where Self: Iterator<Item = (SRef, PRef)>,
      P: TableProperties<UCP, UCPI>,
      SRef: TablePropertiesStringRef,
      PRef: TablePropertiesRef<P, UCP, UCPI>,
      UCP: UserCollectedProperties<UCPI>,
      UCPI: UserCollectedPropertiesIter,
{
}

pub trait TableProperties<UCP, UCPI>
where UCP: UserCollectedProperties<UCPI>,
      UCPI: UserCollectedPropertiesIter
{
    fn user_collected_properties(&self) -> UCP;
}

pub trait TablePropertiesStringRef
where Self: Deref<Target = str>
{}

pub trait TablePropertiesRef<P, UCP, UCPI>
where Self: Deref<Target = P>,
      P: TableProperties<UCP, UCPI>,
      UCP: UserCollectedProperties<UCPI>,
      UCPI: UserCollectedPropertiesIter,
{}

pub trait UserCollectedProperties<UCPI>
where UCPI: UserCollectedPropertiesIter
{
    fn iter(&self) -> UCPI;
}

pub trait UserCollectedPropertiesIter
{}
