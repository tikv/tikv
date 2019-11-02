// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::RocksEngine;
use engine::rocks::util::prepare_sst_for_ingestion;
use engine_traits::ImportExt;
use engine_traits::IngestExternalFileOptions;
use engine_traits::{Error, Result};
use rocksdb::set_external_sst_file_global_seq_no;
use rocksdb::IngestExternalFileOptions as RawIngestExternalFileOptions;
use std::fs::File;
use std::path::Path;
use tikv_util::file::calc_crc32;

impl ImportExt for RocksEngine {
    type IngestExternalFileOptions = RocksIngestExternalFileOptions;

    fn prepare_sst_for_ingestion<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        path: P,
        clone: Q,
    ) -> Result<()> {
        prepare_sst_for_ingestion(path, clone).map_err(|e| Error::Other(box_err!(e)))
    }

    fn ingest_external_file_cf(
        &self,
        cf: &Self::CFHandle,
        opts: &Self::IngestExternalFileOptions,
        files: &[&str],
    ) -> Result<()> {
        let cf = cf.as_inner();
        self.as_inner()
            .ingest_external_file_cf(&cf, &opts.0, files)?;
        Ok(())
    }

    fn validate_sst_for_ingestion<P: AsRef<Path>>(
        &self,
        cf: &Self::CFHandle,
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
        let cf = cf.as_inner();
        set_external_sst_file_global_seq_no(&self.as_inner(), cf, path, 0)?;
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
    use engine_traits::CfName;
    use engine_traits::{SstWriterBuilder, SstWriter};
    use engine_traits::IngestExternalFileOptions;
    use engine_traits::Peekable;
    use engine_traits::ImportExt;
    use engine_traits::CFHandleExt;
    use crate::sst::RocksSstWriterBuilder;
    use crate::db_options::RocksDBOptions;
    use crate::engine::RocksEngine;
    use crate::import::RocksIngestExternalFileOptions;
    use crate::util::{RocksCFOptions, new_engine};
    use tempfile::Builder;
    use std::fs;
    use std::path::Path;
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
            assert_eq!(db.get_cf(cf, k.as_bytes()).unwrap().unwrap(), v.as_bytes());
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

    fn check_prepare_sst_for_ingestion(
        db_opts: Option<RocksDBOptions>,
        cf_opts: Option<Vec<RocksCFOptions>>,
    ) {
        let path = Builder::new()
            .prefix("_util_rocksdb_test_prepare_sst_for_ingestion")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();

        let sst_dir = Builder::new()
            .prefix("_util_rocksdb_test_prepare_sst_for_ingestion_sst")
            .tempdir()
            .unwrap();
        let sst_path = sst_dir.path().join("abc.sst");
        let sst_clone = sst_dir.path().join("abc.sst.clone");

        let kvs = [("k1", "v1"), ("k2", "v2"), ("k3", "v3")];

        let cf_name = "default";
        let db = new_engine(path_str, db_opts, &[cf_name], cf_opts)
            .unwrap();
        let cf = db.cf_handle(cf_name).unwrap();
        let mut ingest_opts = RocksIngestExternalFileOptions::new();
        ingest_opts.move_files(true);

        gen_sst_with_kvs(&db, cf_name, sst_path.to_str().unwrap(), &kvs);
        let size = fs::metadata(&sst_path).unwrap().len();
        let checksum = calc_crc32(&sst_path).unwrap();

        // The first ingestion will hard link sst_path to sst_clone.
        check_hard_link(&sst_path, 1);
        db.prepare_sst_for_ingestion(&sst_path, &sst_clone).unwrap();
        db.validate_sst_for_ingestion(cf, &sst_clone, size, checksum).unwrap();
        check_hard_link(&sst_path, 2);
        check_hard_link(&sst_clone, 2);
        // If we prepare again, it will use hard link too.
        db.prepare_sst_for_ingestion(&sst_path, &sst_clone).unwrap();
        db.validate_sst_for_ingestion(cf, &sst_clone, size, checksum).unwrap();
        check_hard_link(&sst_path, 2);
        check_hard_link(&sst_clone, 2);
        db.ingest_external_file_cf(cf, &ingest_opts, &[sst_clone.to_str().unwrap()])
            .unwrap();
        check_db_with_kvs(&db, cf_name, &kvs);
        assert!(!sst_clone.exists());

        // The second ingestion will copy sst_path to sst_clone.
        check_hard_link(&sst_path, 2);
        db.prepare_sst_for_ingestion(&sst_path, &sst_clone).unwrap();
        db.validate_sst_for_ingestion(cf, &sst_clone, size, checksum).unwrap();
        check_hard_link(&sst_path, 2);
        check_hard_link(&sst_clone, 1);
        db.ingest_external_file_cf(cf, &ingest_opts, &[sst_clone.to_str().unwrap()])
            .unwrap();
        check_db_with_kvs(&db, cf_name, &kvs);
        assert!(!sst_clone.exists());
    }

    #[test]
    fn test_prepare_sst_for_ingestion() {
        check_prepare_sst_for_ingestion(None, None);
    }

    /*#[test]
    fn test_prepare_sst_for_ingestion_titan() {
        let mut db_opts = RocksDBOptions::new();
        let mut titan_opts = TitanDBOptions::new();
        // Force all values write out to blob files.
        titan_opts.set_min_blob_size(0);
        db_opts.set_titandb_options(&titan_opts);
        let mut cf_opts = RocksColumnFamilyOptions::new();
        cf_opts.set_titandb_options(&titan_opts);
        check_prepare_sst_for_ingestion(
            Some(db_opts),
            Some(vec![RocksCFOptions::new("default", cf_opts)]),
        );
    }*/
}
