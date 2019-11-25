// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::CFHandleExt;
use crate::errors::Result;
use crate::range::Range;

pub trait TablePropertiesExt: CFHandleExt {
    type TablePropertiesCollection: TablePropertiesCollection;

    fn get_properties_of_tables_in_range(
        &self,
        cf: &Self::CFHandle,
        ranges: &[Range],
    ) -> Result<Self::TablePropertiesCollection>;
}

pub trait TablePropertiesCollection {
}
