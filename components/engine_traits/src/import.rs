// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tikv_util::range_latch::RangeLatchGuard;

use crate::{errors::Result, Range};

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

    fn acquire_ingest_latch(&self, range: Range<'_>) -> RangeLatchGuard<'_>;
}

pub trait IngestExternalFileOptions {
    fn new() -> Self;

    fn move_files(&mut self, f: bool);

    fn allow_write(&mut self, f: bool);

    fn get_write_global_seqno(&self) -> bool;

    fn set_write_global_seqno(&mut self, f: bool);
}
