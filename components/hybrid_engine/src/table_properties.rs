// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, Range, RangeCacheEngine, Result, TablePropertiesExt};

use crate::engine::HybridEngine;

impl<EK, EC> TablePropertiesExt for HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    type TablePropertiesCollection = EK::TablePropertiesCollection;

    fn table_properties_collection(
        &self,
        cf: &str,
        ranges: &[Range<'_>],
    ) -> Result<Self::TablePropertiesCollection> {
        self.disk_engine().table_properties_collection(cf, ranges)
    }
}
