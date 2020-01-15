// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::ops::Deref;
use std::iter::IntoIterator;
use crate::errors::Result;
use crate::range::Range;
use crate::CFHandleExt;

pub trait TablePropertiesExt: CFHandleExt
{
    
    type TablePropertiesCollection: TablePropertiesCollection<Self::TablePropertiesCollectionView, Self::TableProperties, Self::TablePropertiesCollectionIter, Self::TablePropertiesStringRef, Self::TablePropertiesRef>;
    type TablePropertiesCollectionView: TablePropertiesCollectionView<Self::TableProperties, Self::TablePropertiesCollectionIter, Self::TablePropertiesStringRef, Self::TablePropertiesRef>;
    type TablePropertiesCollectionIter: TablePropertiesCollectionIter<Self::TableProperties, Self::TablePropertiesStringRef, Self::TablePropertiesRef>;
    type TableProperties: TableProperties;
    type TablePropertiesStringRef: TablePropertiesStringRef;
    type TablePropertiesRef: TablePropertiesRef<Self::TableProperties>;

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

pub trait TablePropertiesCollection<V, P, I, SRef, PRef>
where Self: Deref<Target = V>,
      V: TablePropertiesCollectionView<P, I, SRef, PRef>,
      P: TableProperties,
      I: TablePropertiesCollectionIter<P, SRef, PRef>,
      SRef: TablePropertiesStringRef,
      PRef: TablePropertiesRef<P>,
{
}

pub trait TablePropertiesCollectionView<P, I, SRef, PRef>
where P: TableProperties,
      Self: IntoIterator<Item = (SRef, PRef), IntoIter = I>,
      I: TablePropertiesCollectionIter<P, SRef, PRef>,
      SRef: TablePropertiesStringRef,
      PRef: TablePropertiesRef<P>,
{
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub trait TablePropertiesCollectionIter<P, SRef, PRef>
where Self: Iterator<Item = (SRef, PRef)>,
      P: TableProperties,
      SRef: TablePropertiesStringRef,
      PRef: TablePropertiesRef<P>,
{
}

pub trait TableProperties {}

pub trait TablePropertiesStringRef
where Self: Deref<Target = str>
{}

pub trait TablePropertiesRef<P>
where Self: Deref<Target = P>,
      P: TableProperties
{}
