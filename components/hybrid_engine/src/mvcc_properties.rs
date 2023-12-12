// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, MvccProperties, MvccPropertiesExt, RegionCacheEngine};
use txn_types::TimeStamp;

use crate::engine::HybridEngine;

impl<EK, EC> MvccPropertiesExt for HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
{
    fn get_mvcc_properties_cf(
        &self,
        cf: &str,
        safe_point: TimeStamp,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Option<MvccProperties> {
        self.disk_engine()
            .get_mvcc_properties_cf(cf, safe_point, start_key, end_key)
    }
}
