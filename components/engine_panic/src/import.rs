// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::Path;

use engine_traits::{ImportExt, IngestExternalFileOptions, Result};

use crate::engine::PanicEngine;

impl ImportExt for PanicEngine {
    type IngestExternalFileOptions = PanicIngestExternalFileOptions;

<<<<<<< HEAD
    fn ingest_external_file_cf(&self, cf: &str, files: &[&str]) -> Result<()> {
=======
    fn ingest_external_file_cf(
        &self,
        cf: &str,
        files: &[&str],
        range: Option<Range<'_>>,
        force_allow_write: bool,
    ) -> Result<()> {
        panic!()
    }

    fn acquire_ingest_latch(&self, range: Range<'_>) -> RangeLatchGuard<'_> {
>>>>>>> c7429059b2 (sst_importer: allow write during ingesting sst (#18514))
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

    fn get_write_global_seqno(&self) -> bool {
        panic!()
    }

    fn set_write_global_seqno(&mut self, f: bool) {
        panic!()
    }
}
