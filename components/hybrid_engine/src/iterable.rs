// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{IterOptions, Iterable, KvEngine, RegionCacheEngine, Result};

use crate::engine::HybridEngine;

impl<EK, EC> Iterable for HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
{
    type Iterator = EK::Iterator;

    fn iterator_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        // Iterator of region cache engine should only be created from the
        // snapshot of it
        self.disk_engine().iterator_opt(cf, opts)
    }
}
