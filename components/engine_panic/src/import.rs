// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{ImportExt, IngestExternalFileOptions, Range, Result};
use tikv_util::range_latch::RangeLatchGuard;

use crate::engine::PanicEngine;

impl ImportExt for PanicEngine {
    type IngestExternalFileOptions = PanicIngestExternalFileOptions;

    fn ingest_external_file_cf(
        &self,
        cf: &str,
        files: &[&str],
        range: Option<Range<'_>>,
    ) -> Result<()> {
        panic!()
    }

    fn acquire_ingest_latch(&self, range: Range<'_>) -> RangeLatchGuard<'_> {
        panic!()
    }
}

pub struct PanicIngestExternalFileOptions;

impl IngestExternalFileOptions for PanicIngestExternalFileOptions {
    fn new() -> Self {
        panic!()
    }

    fn move_files(&mut self, f: bool) {
        panic!()
    }

    fn allow_write(&mut self, f: bool) {
        panic!()
    }
}
