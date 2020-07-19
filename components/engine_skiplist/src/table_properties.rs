// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::SkiplistEngine;
use engine_traits::{
    DecodeProperties, Range, Result, TableProperties, TablePropertiesCollection,
    TablePropertiesCollectionIter, TablePropertiesExt, TablePropertiesKey, UserCollectedProperties,
};
use std::ops::Deref;

impl TablePropertiesExt for SkiplistEngine {
    type TablePropertiesCollection = SkiplistTablePropertiesCollection;
    type TablePropertiesCollectionIter = SkiplistTablePropertiesCollectionIter;
    type TablePropertiesKey = SkiplistTablePropertiesKey;
    type TableProperties = SkiplistTableProperties;
    type UserCollectedProperties = SkiplistUserCollectedProperties;

    fn get_properties_of_tables_in_range(
        &self,
        cf: &Self::CFHandle,
        ranges: &[Range],
    ) -> Result<Self::TablePropertiesCollection> {
        panic!()
    }
}

pub struct SkiplistTablePropertiesCollection;

impl
    TablePropertiesCollection<
        SkiplistTablePropertiesCollectionIter,
        SkiplistTablePropertiesKey,
        SkiplistTableProperties,
        SkiplistUserCollectedProperties,
    > for SkiplistTablePropertiesCollection
{
    fn iter(&self) -> SkiplistTablePropertiesCollectionIter {
        panic!()
    }

    fn len(&self) -> usize {
        panic!()
    }
}

pub struct SkiplistTablePropertiesCollectionIter;

impl
    TablePropertiesCollectionIter<
        SkiplistTablePropertiesKey,
        SkiplistTableProperties,
        SkiplistUserCollectedProperties,
    > for SkiplistTablePropertiesCollectionIter
{
}

impl Iterator for SkiplistTablePropertiesCollectionIter {
    type Item = (SkiplistTablePropertiesKey, SkiplistTableProperties);

    fn next(&mut self) -> Option<Self::Item> {
        panic!()
    }
}

pub struct SkiplistTablePropertiesKey;

impl TablePropertiesKey for SkiplistTablePropertiesKey {}

impl Deref for SkiplistTablePropertiesKey {
    type Target = str;

    fn deref(&self) -> &str {
        panic!()
    }
}

pub struct SkiplistTableProperties;

impl TableProperties<SkiplistUserCollectedProperties> for SkiplistTableProperties {
    fn num_entries(&self) -> u64 {
        panic!()
    }

    fn user_collected_properties(&self) -> SkiplistUserCollectedProperties {
        panic!()
    }
}

pub struct SkiplistUserCollectedProperties;

impl UserCollectedProperties for SkiplistUserCollectedProperties {
    fn get(&self, index: &[u8]) -> Option<&[u8]> {
        panic!()
    }

    fn len(&self) -> usize {
        panic!()
    }
}

impl DecodeProperties for SkiplistUserCollectedProperties {
    fn decode(&self, k: &str) -> tikv_util::codec::Result<&[u8]> {
        panic!()
    }
}
