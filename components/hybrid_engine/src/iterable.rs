// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{IterOptions, Iterable, KvEngine, RegionCacheEngine, Result};

use crate::{engine::HybridEngine, engine_iterator::HybridEngineIterator};

impl<EK, EC> Iterable for HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
{
    type Iterator = HybridEngineIterator<EK, EC>;

    fn iterator_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        unimplemented!()
    }
}
