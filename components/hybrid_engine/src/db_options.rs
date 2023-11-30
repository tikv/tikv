// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{DbOptionsExt, KvEngine, MemoryEngine, Result};

use crate::engine::HybridEngine;

impl<EK, EM> DbOptionsExt for HybridEngine<EK, EM>
where
    EK: KvEngine,
    EM: MemoryEngine,
{
    type DbOptions = EK::DbOptions;

    fn get_db_options(&self) -> Self::DbOptions {
        self.disk_engine.get_db_options()
    }

    fn set_db_options(&self, options: &[(&str, &str)]) -> Result<()> {
        self.disk_engine.set_db_options(options)
    }
}
