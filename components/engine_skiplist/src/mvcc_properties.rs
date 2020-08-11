// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::SkiplistEngine;
use engine_traits::{
    DecodeProperties, MvccProperties, MvccPropertiesExt, Result, TableProperties,
    TablePropertiesCollection, TablePropertiesExt,
};
use txn_types::TimeStamp;

impl MvccPropertiesExt for SkiplistEngine {
    fn get_mvcc_properties_cf(
        &self,
        cf: &str,
        safe_point: TimeStamp,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<MvccProperties> {
        let mut props = MvccProperties::new();
        Ok(props)
    }
}
