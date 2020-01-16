// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#![allow(unused)]

use std::ops::Deref;
use crate::engine::RocksEngine;
use engine_traits::Range;
use engine_traits::{Error, Result};
use engine_traits::{TablePropertiesCollection, TablePropertiesExt};
use engine_traits::{
    TablePropertiesCollectionIter,
    TableProperties,
    TablePropertiesStringRef,
    TablePropertiesRef,
    UserCollectedProperties,
    UserCollectedPropertiesIter,
};

impl TablePropertiesExt for RocksEngine {
    type TablePropertiesCollection = RocksTablePropertiesCollection;
    type TablePropertiesCollectionIter = RocksTablePropertiesCollectionIter;
    type TableProperties = RocksTableProperties;
    type TablePropertiesStringRef = RocksTablePropertiesStringRef;
    type TablePropertiesRef = RocksTablePropertiesRef;
    type UserCollectedProperties = RocksUserCollectedProperties;
    type UserCollectedPropertiesIter = RocksUserCollectedPropertiesIter;

    fn get_properties_of_tables_in_range(
        &self,
        cf: &Self::CFHandle,
        ranges: &[Range],
    ) -> Result<Self::TablePropertiesCollection> {
        panic!()
    }
}

pub struct RocksTablePropertiesCollection;

impl RocksTablePropertiesCollection {
}

type PA = RocksTableProperties;
type IA = RocksTablePropertiesCollectionIter;
type SRefA = RocksTablePropertiesStringRef;
type PRefA = RocksTablePropertiesRef;
type UCPA = RocksUserCollectedProperties;
type UCPIA = RocksUserCollectedPropertiesIter;

impl TablePropertiesCollection<PA, IA, SRefA, PRefA, UCPA, UCPIA> for RocksTablePropertiesCollection
{
    fn iter(&self) -> RocksTablePropertiesCollectionIter {
        panic!()
    }

    fn len(&self) -> usize {
        panic!()
    }
}

pub struct RocksTablePropertiesCollectionIter;

impl TablePropertiesCollectionIter<PA, SRefA, PRefA, UCPA, UCPIA> for RocksTablePropertiesCollectionIter
{}

impl Iterator for RocksTablePropertiesCollectionIter {
    type Item = (RocksTablePropertiesStringRef, RocksTablePropertiesRef);

    fn next(&mut self) -> Option<Self::Item> {
        panic!()
    }
}

pub struct RocksTableProperties;

impl TableProperties<UCPA, UCPIA> for RocksTableProperties {
    fn user_collected_properties(&self) -> RocksUserCollectedProperties {
        panic!()
    }
}

pub struct RocksTablePropertiesStringRef;

impl TablePropertiesStringRef for RocksTablePropertiesStringRef { }

impl Deref for RocksTablePropertiesStringRef {
    type Target = str;

    fn deref(&self) -> &str {
        panic!()
    }
}

pub struct RocksTablePropertiesRef;

impl TablePropertiesRef<PA, UCPA, UCPIA> for RocksTablePropertiesRef
{}

impl Deref for RocksTablePropertiesRef {
    type Target = RocksTableProperties;

    fn deref(&self) -> &RocksTableProperties {
        panic!()
    }
}

pub struct RocksUserCollectedProperties;

impl UserCollectedProperties<UCPIA> for RocksUserCollectedProperties {
    fn iter(&self) -> RocksUserCollectedPropertiesIter {
        panic!()
    }

    fn get<Q: AsRef<[u8]>>(&self, index: Q) -> Option<&[u8]> {
        let _ = index;
        panic!()
    }
}

pub struct RocksUserCollectedPropertiesIter;

impl UserCollectedPropertiesIter for RocksUserCollectedPropertiesIter { }

