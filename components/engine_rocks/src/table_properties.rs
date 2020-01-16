// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::ops::Deref;
use crate::engine::RocksEngine;
use crate::util;
use engine_traits::Range;
use engine_traits::{Error, Result};
use engine_traits::{TablePropertiesCollection, TablePropertiesExt};
use engine_traits::{
    TablePropertiesCollectionIter,
    TableProperties,
    TablePropertiesStringRef,
    TablePropertiesRef
};
use rocksdb::TablePropertiesCollection as RawTablePropertiesCollection;

impl TablePropertiesExt for RocksEngine {
    type TablePropertiesCollection = RocksTablePropertiesCollection;
    type TablePropertiesCollectionIter = RocksTablePropertiesCollectionIter;
    type TableProperties = RocksTableProperties;
    type TablePropertiesStringRef = RocksTablePropertiesStringRef;
    type TablePropertiesRef = RocksTablePropertiesRef;

    fn get_properties_of_tables_in_range(
        &self,
        cf: &Self::CFHandle,
        ranges: &[Range],
    ) -> Result<Self::TablePropertiesCollection> {
        // FIXME: extra allocation
        let ranges: Vec<_> = ranges.iter().map(util::range_to_rocks_range).collect();
        let raw = self
            .as_inner()
            .get_properties_of_tables_in_range(cf.as_inner(), &ranges);
        let raw = raw.map_err(Error::Engine)?;
        Ok(RocksTablePropertiesCollection::from_raw(raw))
    }
}

pub struct RocksTablePropertiesCollection(RawTablePropertiesCollection);

impl RocksTablePropertiesCollection {
    pub fn from_raw(raw: RawTablePropertiesCollection) -> RocksTablePropertiesCollection {
        RocksTablePropertiesCollection(raw)
    }

    // for test
    pub fn get_raw(&self) -> &RawTablePropertiesCollection {
        &self.0
    }
}

type PA = RocksTableProperties;
type IA = RocksTablePropertiesCollectionIter;
type SRefA = RocksTablePropertiesStringRef;
type PRefA = RocksTablePropertiesRef;

impl TablePropertiesCollection<PA, IA, SRefA, PRefA> for RocksTablePropertiesCollection
{
    fn iter(&self) -> RocksTablePropertiesCollectionIter {
        panic!()
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}

pub struct RocksTablePropertiesCollectionIter;

impl TablePropertiesCollectionIter<PA, SRefA, PRefA> for RocksTablePropertiesCollectionIter
{}

impl Iterator for RocksTablePropertiesCollectionIter {
    type Item = (RocksTablePropertiesStringRef, RocksTablePropertiesRef);

    fn next(&mut self) -> Option<Self::Item> {
        panic!()
    }
}

pub struct RocksTableProperties;

impl TableProperties for RocksTableProperties { }

pub struct RocksTablePropertiesStringRef;

impl TablePropertiesStringRef for RocksTablePropertiesStringRef { }

impl Deref for RocksTablePropertiesStringRef {
    type Target = str;

    fn deref(&self) -> &str {
        panic!()
    }
}

pub struct RocksTablePropertiesRef;

impl TablePropertiesRef<PA> for RocksTablePropertiesRef
{}

impl Deref for RocksTablePropertiesRef {
    type Target = RocksTableProperties;

    fn deref(&self) -> &RocksTableProperties {
        panic!()
    }
}
