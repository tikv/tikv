// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::util::get_cf_handle;
use crate::Rocks;
use engine::rocks::util::prepare_sst_for_ingestion;
use engine_traits::Import;
use engine_traits::IngestExternalFileOptions;
use engine_traits::{Error, Result};
use rocksdb::set_external_sst_file_global_seq_no;
use std::fs::File;
use std::path::Path;
use tikv_util::file::calc_crc32;

impl Import for Rocks {
    fn prepare_sst_for_ingestion<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        path: P,
        clone: Q,
    ) -> Result<()> {
        Ok(prepare_sst_for_ingestion(path, clone).map_err(|e| Error::Other(box_err!(e)))?)
    }

    fn ingest_external_file_cf(
        &self,
        cf: &str,
        opts: &IngestExternalFileOptions,
        files: &[&str],
    ) -> Result<()> {
        let mut rocks_opts = rocksdb::IngestExternalFileOptions::new();
        rocks_opts.move_files(opts.move_files);
        let handle = get_cf_handle(&self.as_inner(), cf)?;
        self.as_inner()
            .ingest_external_file_cf(&handle, &rocks_opts, files)?;
        Ok(())
    }

    fn validate_file_for_ingestion<P: AsRef<Path>>(
        &self,
        cf: &str,
        path: P,
        expected_size: u64,
        expected_checksum: u32,
    ) -> Result<()> {
        let path = path.as_ref().to_str().unwrap();
        let f = File::open(path)?;

        let meta = f.metadata()?;
        if meta.len() != expected_size {
            return Err(Error::Engine(format!(
                "invalid size {} for {}, expected {}",
                meta.len(),
                path,
                expected_size
            )));
        }

        let checksum = calc_crc32(path)?;
        if checksum == expected_checksum {
            return Ok(());
        }

        // RocksDB may have modified the global seqno.
        let cf_handle = get_cf_handle(&self.as_inner(), cf)?;
        set_external_sst_file_global_seq_no(&self.as_inner(), cf_handle, path, 0)?;
        f.sync_all()
            .map_err(|e| format!("sync {}: {:?}", path, e))?;

        let checksum = calc_crc32(path)?;
        if checksum != expected_checksum {
            return Err(Error::Engine(format!(
                "invalid checksum {} for {}, expected {}",
                checksum, path, expected_checksum
            )));
        }

        Ok(())
    }
}
