// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::SkiplistEngine;
use engine_traits::{ImportExt, IngestExternalFileOptions, Result};
use std::path::Path;

impl ImportExt for SkiplistEngine {
    type IngestExternalFileOptions = SkiplistIngestExternalFileOptions;

    fn ingest_external_file_cf(
        &self,
        cf: &Self::CFHandle,
        opts: &Self::IngestExternalFileOptions,
        files: &[&str],
    ) -> Result<()> {
        panic!()
    }

    fn validate_sst_for_ingestion<P: AsRef<Path>>(
        &self,
        cf: &Self::CFHandle,
        path: P,
        expected_size: u64,
        expected_checksum: u32,
    ) -> Result<()> {
        panic!()
    }
}

pub struct SkiplistIngestExternalFileOptions;

impl IngestExternalFileOptions for SkiplistIngestExternalFileOptions {
    fn new() -> Self {
        panic!()
    }
    fn move_files(&mut self, f: bool) {
        panic!()
    }
}
