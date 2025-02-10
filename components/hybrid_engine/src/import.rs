// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{ImportExt, KvEngine, Range, RangeCacheEngine};
use tikv_util::range_latch::RangeLatchGuard;

use crate::engine::HybridEngine;

impl<EK, EC> ImportExt for HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    type IngestExternalFileOptions = EK::IngestExternalFileOptions;

    fn ingest_external_file_cf(
        &self,
        cf: &str,
        files: &[&str],
        range: Option<Range<'_>>,
    ) -> engine_traits::Result<()> {
        self.disk_engine().ingest_external_file_cf(cf, files, range)
    }

    fn acquire_ingest_latch(&self, range: Range<'_>) -> RangeLatchGuard<'_> {
        self.disk_engine().acquire_ingest_latch(range)
    }
}
