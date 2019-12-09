// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{TablePropertiesExt, TablePropertiesCollection, Result, Range};
use crate::engine::PanicEngine;

impl TablePropertiesExt for PanicEngine {
    type TablePropertiesCollection = PanicTablePropertiesCollection;

    fn get_properties_of_tables_in_range(
        &self,
        cf: &Self::CFHandle,
        ranges: &[Range],
    ) -> Result<Self::TablePropertiesCollection> { panic!() }
}

pub struct PanicTablePropertiesCollection;

impl TablePropertiesCollection for PanicTablePropertiesCollection { }
