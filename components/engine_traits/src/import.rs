// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::Path;
use crate::errors::Result;

pub struct IngestExternalFileOptions {
    pub move_files: bool,
}

pub trait Import {
    fn prepare_sst_for_ingestion<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        path: P,
        clone: Q,
    ) -> Result<()>;
    fn ingest_external_file_cf(
        &self,
        cf: &str,
        opts: &IngestExternalFileOptions,
        files: &[&str],
    ) -> Result<()>;
    fn validate_file_for_ingestion<P: AsRef<Path>>(
        &self,
        cf: &str,
        path: P,
        expected_size: u64,
        expected_checksum: u32,
    ) -> Result<()>;
}
