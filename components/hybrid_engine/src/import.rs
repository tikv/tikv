// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{ImportExt, KvEngine, MemoryEngine};

use crate::engine::HybridEngine;

impl<EK, EM> ImportExt for HybridEngine<EK, EM>
where
    EK: KvEngine,
    EM: MemoryEngine,
{
    type IngestExternalFileOptions = EK::IngestExternalFileOptions;

    fn ingest_external_file_cf(&self, cf: &str, files: &[&str]) -> engine_traits::Result<()> {
        // todo: except for ingesting file into underlying disk engine, the data should
        // also feed into in-memory_engine (within the peirod of ingesting, the
        // in-memory engine should invalidate read)
        unimplemented!()
    }
}
