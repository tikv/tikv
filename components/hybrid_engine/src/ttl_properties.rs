// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, RangeCacheEngine, Result, TtlProperties, TtlPropertiesExt};

use crate::engine::HybridEngine;

impl<EK, EC> TtlPropertiesExt for HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    fn get_range_ttl_properties_cf(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<Vec<(String, TtlProperties)>> {
        self.disk_engine()
            .get_range_ttl_properties_cf(cf, start_key, end_key)
    }
}
