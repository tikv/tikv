// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::errors::Result;
use std::path::Path;

pub trait ImportExt {
    type IngestExternalFileOptions: IngestExternalFileOptions;

    fn ingest_external_file_cf(
        &self,
        cf: &str,
        opt: &Self::IngestExternalFileOptions,
        files: &[&str],
    ) -> Result<()>;

    fn reset_global_seq<P: AsRef<Path>>(&self, cf: &str, path: P) -> Result<()>;
}

pub trait IngestExternalFileOptions {
    fn new() -> Self;

    fn move_files(&mut self, f: bool);
}
