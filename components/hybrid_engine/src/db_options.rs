// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{DbOptionsExt, KvEngine, RangeCacheEngine, Result};

use crate::engine::HybridEngine;

impl<EK, EC> DbOptionsExt for HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    type DbOptions = EK::DbOptions;

    fn get_db_options(&self) -> Self::DbOptions {
        self.disk_engine().get_db_options()
    }

    fn set_db_options(&self, options: &[(&str, &str)]) -> Result<()> {
        self.disk_engine().set_db_options(options)
    }
}
