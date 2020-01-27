// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#![allow(unused)]

use crate::engine::RocksEngine;
use crate::util;
use engine_traits::Range;
use engine_traits::{Error, Result};
use engine_traits::{
    TableProperties, TablePropertiesCollectionIter, TablePropertiesKey, UserCollectedProperties,
};
use engine_traits::{TablePropertiesCollection, TablePropertiesExt};
use rocksdb::table_properties_rc as raw;
use std::ops::Deref;

impl TablePropertiesExt for RocksEngine {
    type TablePropertiesCollection = RocksTablePropertiesCollection;
    type TablePropertiesCollectionIter = RocksTablePropertiesCollectionIter;
    type TablePropertiesKey = RocksTablePropertiesKey;
    type TableProperties = RocksTableProperties;
    type UserCollectedProperties = RocksUserCollectedProperties;

    fn get_properties_of_tables_in_range(
        &self,
        cf: &Self::CFHandle,
        ranges: &[Range],
    ) -> Result<Self::TablePropertiesCollection> {
        // FIXME: extra allocation
        let ranges: Vec<_> = ranges.iter().map(util::range_to_rocks_range).collect();
        let raw = self
            .as_inner()
            .get_properties_of_tables_in_range_rc(cf.as_inner(), &ranges);
        let raw = raw.map_err(Error::Engine)?;
        Ok(RocksTablePropertiesCollection::from_raw(raw))
    }
}

pub struct RocksTablePropertiesCollection(raw::TablePropertiesCollection);

impl RocksTablePropertiesCollection {
    fn from_raw(raw: raw::TablePropertiesCollection) -> RocksTablePropertiesCollection {
        RocksTablePropertiesCollection(raw)
    }
}

type IA = RocksTablePropertiesCollectionIter;
type PKeyA = RocksTablePropertiesKey;
type PA = RocksTableProperties;
type UCPA = RocksUserCollectedProperties;

impl TablePropertiesCollection<IA, PKeyA, PA, UCPA> for RocksTablePropertiesCollection {
    fn iter(&self) -> RocksTablePropertiesCollectionIter {
        RocksTablePropertiesCollectionIter(self.0.iter())
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}

pub struct RocksTablePropertiesCollectionIter(raw::TablePropertiesCollectionIter);

impl TablePropertiesCollectionIter<PKeyA, PA, UCPA> for RocksTablePropertiesCollectionIter {}

impl Iterator for RocksTablePropertiesCollectionIter {
    type Item = (RocksTablePropertiesKey, RocksTableProperties);

    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .next()
            .map(|(key, props)| (RocksTablePropertiesKey(key), RocksTableProperties(props)))
    }
}

pub struct RocksTablePropertiesKey(raw::TablePropertiesKey);

impl TablePropertiesKey for RocksTablePropertiesKey {}

impl Deref for RocksTablePropertiesKey {
    type Target = str;

    fn deref(&self) -> &str {
        self.0.deref()
    }
}

pub struct RocksTableProperties(raw::TableProperties);

impl TableProperties<UCPA> for RocksTableProperties {
    fn num_entries(&self) -> u64 {
        self.0.num_entries()
    }

    fn user_collected_properties(&self) -> RocksUserCollectedProperties {
        RocksUserCollectedProperties(self.0.user_collected_properties())
    }
}

pub struct RocksUserCollectedProperties(raw::UserCollectedProperties);

impl UserCollectedProperties for RocksUserCollectedProperties {
    fn get<Q: AsRef<[u8]>>(&self, index: Q) -> Option<&[u8]> {
        self.0.get(index)
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}
