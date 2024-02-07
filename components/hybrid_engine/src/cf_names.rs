// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{CfNamesExt, KvEngine, RangeCacheEngine};

use crate::engine::HybridEngine;

impl<EK, EC> CfNamesExt for HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    fn cf_names(&self) -> Vec<&str> {
        self.disk_engine().cf_names()
    }
}
