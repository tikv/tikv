// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{Range, Result};

pub trait UserCollectedProperties {
    fn get(&self, index: &[u8]) -> Option<&[u8]>;
}

pub trait TablePropertiesCollection {
    type UserCollectedProperties: UserCollectedProperties;

    /// Iterator all `UserCollectedProperties`, break if `f` returns false.
    fn iter_user_collected_properties<F>(&self, f: F)
    where
        F: FnMut(&Self::UserCollectedProperties) -> bool;
}

pub trait TablePropertiesExt {
    type TablePropertiesCollection: TablePropertiesCollection;

    fn table_properties_collection(
        &self,
        cf: &str,
        ranges: &[Range],
    ) -> Result<Self::TablePropertiesCollection>;
}
