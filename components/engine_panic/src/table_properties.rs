// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::PanicEngine;
use engine_traits::{
    DecodeProperties, Range, Result, TableProperties, TablePropertiesCollection,
    TablePropertiesCollectionIter, TablePropertiesKey, UserCollectedProperties,
};
use std::ops::Deref;

pub struct PanicTablePropertiesCollection;

impl
    TablePropertiesCollection<
        PanicTablePropertiesCollectionIter,
        PanicTablePropertiesKey,
        PanicTableProperties,
        PanicUserCollectedProperties,
    > for PanicTablePropertiesCollection
{
    fn iter(&self) -> PanicTablePropertiesCollectionIter {
        panic!()
    }

    fn len(&self) -> usize {
        panic!()
    }
}

pub struct PanicTablePropertiesCollectionIter;

impl
    TablePropertiesCollectionIter<
        PanicTablePropertiesKey,
        PanicTableProperties,
        PanicUserCollectedProperties,
    > for PanicTablePropertiesCollectionIter
{
}

impl Iterator for PanicTablePropertiesCollectionIter {
    type Item = (PanicTablePropertiesKey, PanicTableProperties);

    fn next(&mut self) -> Option<Self::Item> {
        panic!()
    }
}

pub struct PanicTablePropertiesKey;

impl TablePropertiesKey for PanicTablePropertiesKey {}

impl Deref for PanicTablePropertiesKey {
    type Target = str;

    fn deref(&self) -> &str {
        panic!()
    }
}

pub struct PanicTableProperties;

impl TableProperties<PanicUserCollectedProperties> for PanicTableProperties {
    fn num_entries(&self) -> u64 {
        panic!()
    }

    fn user_collected_properties(&self) -> PanicUserCollectedProperties {
        panic!()
    }
}

pub struct PanicUserCollectedProperties;

impl UserCollectedProperties for PanicUserCollectedProperties {
    fn get(&self, index: &[u8]) -> Option<&[u8]> {
        panic!()
    }

    fn len(&self) -> usize {
        panic!()
    }
}

impl DecodeProperties for PanicUserCollectedProperties {
    fn decode(&self, k: &str) -> tikv_util::codec::Result<&[u8]> {
        panic!()
    }
}
