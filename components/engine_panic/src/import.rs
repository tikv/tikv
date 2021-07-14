// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::PanicEngine;
use engine_traits::{ImportExt, IngestExternalFileOptions, Result};
use std::path::Path;

impl ImportExt for PanicEngine {
    type IngestExternalFileOptions = PanicIngestExternalFileOptions;

    fn ingest_external_file_cf(&self, cf: &str, files: &[&str]) -> Result<()> {
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
