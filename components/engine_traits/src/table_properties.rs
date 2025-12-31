// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{MvccProperties, Range, Result};

pub trait UserCollectedProperties {
    fn get(&self, index: &[u8]) -> Option<&[u8]>;
    fn approximate_size_and_keys(&self, start: &[u8], end: &[u8]) -> Option<(usize, usize)>;
    fn get_mvcc_properties(&self) -> Option<MvccProperties>;
}

pub trait TableProperties {
    type UserCollectedProperties: UserCollectedProperties;

    fn get_user_collected_properties(&self) -> &Self::UserCollectedProperties;
    fn get_num_entries(&self) -> u64;
}

pub trait TablePropertiesCollection {
    type TableProperties: TableProperties;

    /// Iterator all `TableProperties`, break if `f` returns false.
    fn iter_table_properties<F>(&self, f: F)
    where
        F: FnMut(&Self::TableProperties) -> bool;
}

pub trait TablePropertiesExt {
    type TablePropertiesCollection: TablePropertiesCollection;

    /// Collection of tables covering the given range.
    fn table_properties_collection(
        &self,
        cf: &str,
        ranges: &[Range<'_>],
    ) -> Result<Self::TablePropertiesCollection>;
}
