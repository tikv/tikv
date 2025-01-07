// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::errors::Result;

pub trait ImportExt {
    type IngestExternalFileOptions: IngestExternalFileOptions;

    fn ingest_external_file_cf(
        &self,
        cf: &str,
        files: &[&str],
        allow_write_during_ingestion: bool,
    ) -> Result<()>;
}

pub trait IngestExternalFileOptions {
    fn new() -> Self;

    fn move_files(&mut self, f: bool);

    fn allow_write(&mut self, f: bool);
}
