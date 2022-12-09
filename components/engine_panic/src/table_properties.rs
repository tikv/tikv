// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{Range, Result};

use crate::engine::PanicEngine;

pub struct UserCollectedProperties;
impl engine_traits::UserCollectedProperties for UserCollectedProperties {
    fn get(&self, _: &[u8]) -> Option<&[u8]> {
        None
    }
    fn approximate_size_and_keys(&self, _: &[u8], _: &[u8]) -> Option<(usize, usize)> {
        None
    }
}

pub struct TablePropertiesCollection;
impl engine_traits::TablePropertiesCollection for TablePropertiesCollection {
    type UserCollectedProperties = UserCollectedProperties;
    fn iter_user_collected_properties<F>(&self, _: F)
    where
        F: FnMut(&Self::UserCollectedProperties) -> bool,
    {
    }
}

impl engine_traits::TablePropertiesExt for PanicEngine {
    type TablePropertiesCollection = TablePropertiesCollection;
    fn table_properties_collection(
        &self,
        cf: &str,
        ranges: &[Range<'_>],
    ) -> Result<Self::TablePropertiesCollection> {
        panic!()
    }
}
