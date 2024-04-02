// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{ImportExt, KvEngine, RangeCacheEngine};

use crate::engine::HybridEngine;

impl<EK, EC> ImportExt for HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    type IngestExternalFileOptions = EK::IngestExternalFileOptions;

    fn ingest_external_file_cf(&self, cf: &str, files: &[&str]) -> engine_traits::Result<()> {
        self.disk_engine().ingest_external_file_cf(cf, files)
    }
}
