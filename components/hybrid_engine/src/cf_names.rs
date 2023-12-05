// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{CfNamesExt, KvEngine, MemoryEngine};

use crate::engine::HybridEngine;

impl<EK, EM> CfNamesExt for HybridEngine<EK, EM>
where
    EK: KvEngine,
    EM: MemoryEngine,
{
    fn cf_names(&self) -> Vec<&str> {
        self.disk_engine().cf_names()
    }
}
