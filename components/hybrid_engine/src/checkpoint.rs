// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{Checkpointable, KvEngine, MemoryEngine, Result};

use crate::engine::HybridEngine;

impl<EK, EM> Checkpointable for HybridEngine<EK, EM>
where
    EK: KvEngine,
    EM: MemoryEngine,
{
    type Checkpointer = EK::Checkpointer;

    fn new_checkpointer(&self) -> Result<Self::Checkpointer> {
        self.disk_engine().new_checkpointer()
    }

    fn merge(&self, dbs: &[&Self]) -> Result<()> {
        let disk_dbs: Vec<_> = dbs.iter().map(|&db| db.disk_engine()).collect();
        self.disk_engine().merge(&disk_dbs)
    }
}
