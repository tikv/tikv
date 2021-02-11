// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::PanicEngine;
use engine_traits::{Result, TTLProperties, TTLPropertiesExt};

impl TTLPropertiesExt for PanicEngine {
    fn get_range_ttl_properties_cf(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<Vec<(String, TTLProperties)>> {
        panic!()
    }
}
