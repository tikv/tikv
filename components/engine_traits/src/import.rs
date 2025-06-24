// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
use std::sync::Arc;

use encryption::DataKeyManager;
use tikv_util::range_latch::RangeLatchGuard;

use crate::{Range, errors::Result};

pub trait ImportExt {
    type IngestExternalFileOptions: IngestExternalFileOptions;

    /// Ingests external files into the specified column family.
    ///
    /// If the range is specified, it enables `RocksDB
    /// IngestExternalFileOptions.allow_write` and locks the
    /// specified range.
    fn ingest_external_file_cf(
        &self,
        cf: &str,
        files: &[&str],
        range: Option<Range<'_>>,
        force_allow_write: bool,
    ) -> Result<()>;

    /// Imports the contents of external SST files into the given column family
    /// by reading each key-value pair and writing them via write batches.
    ///
    /// This method is typically used as a mitigation when native RocksDB
    /// ingestion is not appropriate, such as when file format or encryption
    /// prevents direct ingestion or the source SST files are too trivial to
    /// be ingested. It iterates over all key-value pairs in the provided SST
    /// files and writes them into the target column family using batched
    /// writes.
    ///
    /// # Arguments
    /// * `cf` - The name of the column family to write into.
    /// * `files` - Paths to the external SST files to import.
    /// * `key_manager` - Optional key manager for decrypting encrypted SST
    ///   files.
    ///
    /// # Returns
    /// * `Result<()>` - Returns Ok if all files are imported successfully, or
    ///   an error otherwise.
    ///
    /// # Caveats
    /// - Write batches are flushed every 1024 entries for memory efficiency.
    fn import_external_file_cf_without_ingest(
        &self,
        cf: &str,
        files: &[&str],
        key_manager: Option<Arc<DataKeyManager>>,
    ) -> Result<()>;

    fn acquire_ingest_latch(&self, range: Range<'_>) -> RangeLatchGuard<'_>;
}

pub trait IngestExternalFileOptions {
    fn new() -> Self;

    fn move_files(&mut self, f: bool);

    fn allow_write(&mut self, f: bool);
}
