// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{CfOptionsExt, KvEngine, RangeCacheEngine, Result};

use crate::engine::HybridEngine;

impl<EK, EC> CfOptionsExt for HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    type CfOptions = EK::CfOptions;

    fn get_options_cf(&self, cf: &str) -> Result<Self::CfOptions> {
        self.disk_engine().get_options_cf(cf)
    }

    fn set_options_cf(&self, cf: &str, options: &[(&str, &str)]) -> Result<()> {
        self.disk_engine().set_options_cf(cf, options)
    }
}
