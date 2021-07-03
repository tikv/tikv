// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::RocksEngine;
use crate::util;
use engine_traits::ImportExt;
use engine_traits::IngestExternalFileOptions;
use engine_traits::Result;
use rocksdb::set_external_sst_file_global_seq_no;
use rocksdb::IngestExternalFileOptions as RawIngestExternalFileOptions;
use std::fs::File;
use std::path::Path;

impl ImportExt for RocksEngine {
    type IngestExternalFileOptions = RocksIngestExternalFileOptions;

    fn ingest_external_file_cf(&self, cf: &str, files: &[&str]) -> Result<()> {
        let cf = util::get_cf_handle(self.as_inner(), cf)?;
        let mut opts = RocksIngestExternalFileOptions::new();
        opts.move_files(true);
        opts.set_write_global_seqno(self.get_sst_ingestion_write_global_seqno());
        // This is calling a specially optimized version of
        // ingest_external_file_cf. In cases where the memtable needs to be
        // flushed it avoids blocking writers while doing the flush. The unused
        // return value here just indicates whether the fallback path requiring
        // the manual memtable flush was taken.
        let _did_nonblocking_memtable_flush = self
            .as_inner()
            .ingest_external_file_optimized(&cf, &opts.0, files)?;
        Ok(())
    }

    fn reset_global_seq<P: AsRef<Path>>(&self, cf: &str, path: P) -> Result<()> {
        let path = path.as_ref().to_str().unwrap();
        let f = File::open(path)?;

        // RocksDB may have modified the global seqno.
        let cf = util::get_cf_handle(self.as_inner(), cf)?;
        set_external_sst_file_global_seq_no(&self.as_inner(), cf, path, 0)?;
        f.sync_all()
            .map_err(|e| format!("sync {}: {:?}", path, e))?;

        Ok(())
    }
}

pub struct RocksIngestExternalFileOptions(RawIngestExternalFileOptions);

impl IngestExternalFileOptions for RocksIngestExternalFileOptions {
    fn new() -> RocksIngestExternalFileOptions {
        RocksIngestExternalFileOptions(RawIngestExternalFileOptions::new())
    }

    fn move_files(&mut self, f: bool) {
        self.0.move_files(f);
    }

    fn get_write_global_seqno(&self) -> bool {
        self.0.get_write_global_seqno()
    }

    fn set_write_global_seqno(&mut self, f: bool) {
        self.0.set_write_global_seqno(f);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ingest_external_file_options() {
        let mut opts = RocksIngestExternalFileOptions::new();
        opts.set_write_global_seqno(true);
        assert_eq!(true, opts.get_write_global_seqno());
        opts.set_write_global_seqno(false);
        assert_eq!(false, opts.get_write_global_seqno());
    }
}
