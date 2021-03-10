// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::cf_handle::CFHandleExt;
use crate::errors::Result;
use std::path::Path;

pub trait ImportExt: CFHandleExt {
    type IngestExternalFileOptions: IngestExternalFileOptions;

    fn ingest_external_file_cf(
        &self,
        cf: &Self::CFHandle,
        opt: &Self::IngestExternalFileOptions,
        files: &[&str],
    ) -> Result<()>;

<<<<<<< HEAD
    fn validate_sst_for_ingestion<P: AsRef<Path>>(
        &self,
        cf: &Self::CFHandle,
        path: P,
        expected_size: u64,
        expected_checksum: u32,
    ) -> Result<()>;
=======
    fn reset_global_seq<P: AsRef<Path>>(&self, cf: &str, path: P) -> Result<()>;
>>>>>>> 3d3dd779d... sst_importer: make ingest reentrant (#9624)
}

pub trait IngestExternalFileOptions {
    fn new() -> Self;

    fn move_files(&mut self, f: bool);
}
