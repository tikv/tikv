// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{IterOptions, Iterable, KvEngine, MemoryEngine, Result};

use crate::{engine::HybridEngine, engine_iterator::HybridEngineIterator};

impl<EK, EM> Iterable for HybridEngine<EK, EM>
where
    EK: KvEngine,
    EM: MemoryEngine,
{
    type Iterator = HybridEngineIterator<EK, EM>;

    fn iterator_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        unimplemented!()
    }
}
