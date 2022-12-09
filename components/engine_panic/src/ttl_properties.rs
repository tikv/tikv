// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{Result, TtlProperties, TtlPropertiesExt};

use crate::engine::PanicEngine;

impl TtlPropertiesExt for PanicEngine {
    fn get_range_ttl_properties_cf(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<Vec<(String, TtlProperties)>> {
        panic!()
    }
}
