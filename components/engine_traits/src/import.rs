// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::cf_handle::CFHandleExt;
use crate::errors::Result;
use std::path::Path;

pub trait IngestExternalFileOptions {
    fn move_files(&mut self, f: bool);
}

pub trait Import: CFHandleExt {
    type IngestExternalFileOptions: IngestExternalFileOptions;

    fn new_ingest_external_file_options() -> Self::IngestExternalFileOptions;

    fn prepare_sst_for_ingestion<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        path: P,
        clone: Q,
    ) -> Result<()>;

    fn ingest_external_file_cf(
        &self,
        cf: &Self::CFHandle,
        opts: &Self::IngestExternalFileOptions,
        files: &[&str],
    ) -> Result<()>;

    fn validate_sst_for_ingestion<P: AsRef<Path>>(
        &self,
        cf: &Self::CFHandle,
        path: P,
        expected_size: u64,
        expected_checksum: u32,
    ) -> Result<()>;
}
