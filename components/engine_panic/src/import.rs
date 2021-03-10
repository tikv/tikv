// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::PanicEngine;
use engine_traits::{ImportExt, IngestExternalFileOptions, Result};
use std::path::Path;

impl ImportExt for PanicEngine {
    type IngestExternalFileOptions = PanicIngestExternalFileOptions;

    fn ingest_external_file_cf(
        &self,
        cf: &Self::CFHandle,
        opts: &Self::IngestExternalFileOptions,
        files: &[&str],
    ) -> Result<()> {
        panic!()
    }

<<<<<<< HEAD
    fn validate_sst_for_ingestion<P: AsRef<Path>>(
        &self,
        cf: &Self::CFHandle,
        path: P,
        expected_size: u64,
        expected_checksum: u32,
    ) -> Result<()> {
=======
    fn reset_global_seq<P: AsRef<Path>>(&self, cf: &str, path: P) -> Result<()> {
>>>>>>> 3d3dd779d... sst_importer: make ingest reentrant (#9624)
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
}
