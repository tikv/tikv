// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::properties::DecodeProperties;
use std::ops::Deref;

pub trait TablePropertiesCollectionIter<PKey, P, UCP>: Iterator<Item = (PKey, P)>
where
    PKey: TablePropertiesKey,
    P: TableProperties<UCP>,
    UCP: UserCollectedProperties,
{
}

pub trait TablePropertiesKey: Deref<Target = str> {}

pub trait TableProperties<UCP>
where
    UCP: UserCollectedProperties,
{
    fn num_entries(&self) -> u64;

    fn user_collected_properties(&self) -> UCP;
}

pub trait UserCollectedProperties: DecodeProperties {
    fn get(&self, index: &[u8]) -> Option<&[u8]>;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
