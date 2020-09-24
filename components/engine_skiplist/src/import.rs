// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::SkiplistEngine;
use crate::write_batch::SkiplistWriteBatch;
use engine_rocks::RocksSstReader;
use engine_traits::{
    ImportExt, IngestExternalFileOptions, Iterable, Iterator, Mutable, Result, SstReader,
    WriteBatchExt,
};
use std::path::Path;

impl ImportExt for SkiplistEngine {
    type IngestExternalFileOptions = SkiplistIngestExternalFileOptions;

    fn ingest_external_file_cf(
        &self,
        cf: &Self::CFHandle,
        opts: &Self::IngestExternalFileOptions,
        files: &[&str],
    ) -> Result<()> {
        for path in files {
            let mut batch = SkiplistWriteBatch::new(self);
            let reader = RocksSstReader::open(path)?;
            reader.verify_checksum()?;
            let mut iter = reader.iter();
            while iter.valid()? {
                batch.put_cf(cf.cf_name, iter.key(), iter.value())?;
                iter.next()?;
            }
            self.write(&batch)?;
        }
        Ok(())
    }

    fn validate_sst_for_ingestion<P: AsRef<Path>>(
        &self,
        cf: &Self::CFHandle,
        path: P,
        expected_size: u64,
        expected_checksum: u32,
    ) -> Result<()> {
        Ok(())
    }
}

pub struct SkiplistIngestExternalFileOptions;

impl IngestExternalFileOptions for SkiplistIngestExternalFileOptions {
    fn new() -> Self {
        Self
    }
    fn move_files(&mut self, f: bool) {}
}
