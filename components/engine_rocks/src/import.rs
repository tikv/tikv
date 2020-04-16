// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::RocksEngine;
use engine_traits::ImportExt;
use engine_traits::IngestExternalFileOptions;
use engine_traits::Result;
use rocksdb::set_external_sst_file_global_seq_no;
use rocksdb::IngestExternalFileOptions as RawIngestExternalFileOptions;
use std::fs::File;
use std::path::Path;

impl ImportExt for RocksEngine {
    type IngestExternalFileOptions = RocksIngestExternalFileOptions;

    fn ingest_external_file_cf(
        &self,
        cf: &Self::CFHandle,
        opts: &Self::IngestExternalFileOptions,
        files: &[&str],
    ) -> Result<()> {
        let cf = cf.as_inner();
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

    // TODO: rename it to `reset_global_seq`.
    fn validate_sst_for_ingestion<P: AsRef<Path>>(
        &self,
        cf: &Self::CFHandle,
        path: P,
        _expected_size: u64,
        _expected_checksum: u32,
    ) -> Result<()> {
        let path = path.as_ref().to_str().unwrap();
        let f = File::open(path)?;

        // RocksDB may have modified the global seqno.
        let cf = cf.as_inner();
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
}

#[cfg(test)]
mod tests {
    use crate::cf_options::RocksColumnFamilyOptions;
    use crate::db_options::{RocksDBOptions, RocksTitanDBOptions};
    use crate::engine::RocksEngine;
    use crate::import::RocksIngestExternalFileOptions;
    use crate::sst::RocksSstWriterBuilder;
    use crate::util::{new_engine, RocksCFOptions};
    use engine_traits::CFHandleExt;
    use engine_traits::CfName;
    use engine_traits::ColumnFamilyOptions;
    use engine_traits::ImportExt;
    use engine_traits::IngestExternalFileOptions;
    use engine_traits::Peekable;
    use engine_traits::{DBOptions, TitanDBOptions};
    use engine_traits::{SstWriter, SstWriterBuilder};
    use std::fs;
    use std::path::Path;
    use tempfile::Builder;
    use tikv_util::file::calc_crc32;

    #[cfg(target_os = "linux")]
    fn check_hard_link<P: AsRef<Path>>(path: P, nlink: u64) {
        use std::os::linux::fs::MetadataExt;
        assert_eq!(fs::metadata(path).unwrap().st_nlink(), nlink);
    }

    #[cfg(not(target_os = "linux"))]
    fn check_hard_link<P: AsRef<Path>>(_: P, _: u64) {
        // Just do nothing
    }

    fn check_db_with_kvs(db: &RocksEngine, cf: &str, kvs: &[(&str, &str)]) {
        for &(k, v) in kvs {
            assert_eq!(
                db.get_value_cf(cf, k.as_bytes()).unwrap().unwrap(),
                v.as_bytes()
            );
        }
    }

    fn gen_sst_with_kvs(db: &RocksEngine, cf: CfName, path: &str, kvs: &[(&str, &str)]) {
        let mut writer = RocksSstWriterBuilder::new()
            .set_db(db)
            .set_cf(cf)
            .build(path)
            .unwrap();
        for &(k, v) in kvs {
            writer.put(k.as_bytes(), v.as_bytes()).unwrap();
        }
        writer.finish().unwrap();
    }
}
