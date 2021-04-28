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

    fn reset_global_seq<P: AsRef<Path>>(&self, cf: &Self::CFHandle, path: P) -> Result<()>;
}

pub trait IngestExternalFileOptions {
    fn new() -> Self;

    fn move_files(&mut self, f: bool);
}
