// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{Checkpointable, KvEngine, MemoryEngine, Result};

use crate::engine::HybridEngine;

impl<EK, EM> Checkpointable for HybridEngine<EK, EM>
where
    EK: KvEngine,
    EM: MemoryEngine,
{
    type Checkpointer = EK::Checkpointer;

    fn new_checkpointer(&self) -> Result<Self::Checkpointer> {
        unimplemented!()
    }

    fn merge(&self, dbs: &[&Self]) -> Result<()> {
        unimplemented!()
    }
}
