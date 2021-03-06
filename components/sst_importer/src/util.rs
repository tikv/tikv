// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::Path;

use encryption::DataKeyManager;
use engine_traits::EncryptionKeyManager;
use file_system::File;

use super::Result;

/// Prepares the SST file for ingestion.
/// The purpose is to make the ingestion retryable when using the `move_files` option.
/// Things we need to consider here:
/// 1. We need to access the original file on retry, so we should make a clone
///    before ingestion.
/// 2. `RocksDB` will modified the global seqno of the ingested file, so we need
///    to modified the global seqno back to 0 so that we can pass the checksum
///    validation.
/// 3. If the file has been ingested to `RocksDB`, we should not modified the
///    global seqno directly, because that may corrupt RocksDB's data.
pub fn prepare_sst_for_ingestion<P: AsRef<Path>, Q: AsRef<Path>>(
    path: P,
    clone: Q,
    encryption_key_manager: Option<&DataKeyManager>,
) -> Result<()> {
    #[cfg(unix)]
    use std::os::unix::fs::MetadataExt;

    let path = path.as_ref().to_str().unwrap();
    let clone = clone.as_ref().to_str().unwrap();

    if Path::new(clone).exists() {
        file_system::remove_file(clone).map_err(|e| format!("remove {}: {:?}", clone, e))?;
    }
    // always try to remove the file from key manager because the clean up in rocksdb is not atomic,
    // thus the file may be deleted but key in key manager is not.
    if let Some(key_manager) = encryption_key_manager {
        key_manager.delete_file(clone)?;
    }

    #[cfg(unix)]
    let nlink = file_system::metadata(path)
        .map_err(|e| format!("read metadata from {}: {:?}", path, e))?
        .nlink();
    #[cfg(not(unix))]
    let nlink = 0;

    if nlink == 1 {
        // RocksDB must not have this file, we can make a hard link.
        file_system::hard_link(path, clone)
            .map_err(|e| format!("link from {} to {}: {:?}", path, clone, e))?;
        File::open(clone)?.sync_all()?;
    } else {
        // RocksDB may have this file, we should make a copy.
        file_system::copy_and_sync(path, clone)
            .map_err(|e| format!("copy from {} to {}: {:?}", path, clone, e))?;
    }
    // sync clone dir
    File::open(Path::new(clone).parent().unwrap())?.sync_all()?;
    if let Some(key_manager) = encryption_key_manager {
        key_manager.link_file(path, clone)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::prepare_sst_for_ingestion;

    use encryption::DataKeyManager;
    use engine_rocks::{
        util::{new_engine, RocksCFOptions},
        RocksColumnFamilyOptions, RocksDBOptions, RocksEngine, RocksIngestExternalFileOptions,
        RocksSstWriterBuilder, RocksTitanDBOptions,
    };
    use engine_traits::{
        CfName, ColumnFamilyOptions, DBOptions, EncryptionKeyManager, ImportExt,
        IngestExternalFileOptions, Peekable, SstWriter, SstWriterBuilder, TitanDBOptions,
    };
    use file_system::calc_crc32;
    use std::{path::Path, sync::Arc};
    use tempfile::Builder;
    use test_util::encryption::new_test_key_manager;

    #[cfg(unix)]
    fn check_hard_link<P: AsRef<Path>>(path: P, nlink: u64) {
        use std::os::unix::fs::MetadataExt;
        assert_eq!(file_system::metadata(path).unwrap().nlink(), nlink);
    }

    #[cfg(not(unix))]
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

    fn check_prepare_sst_for_ingestion(
        db_opts: Option<RocksDBOptions>,
        cf_opts: Option<Vec<RocksCFOptions>>,
        key_manager: Option<&DataKeyManager>,
        was_encrypted: bool,
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
        let db = new_engine(path_str, db_opts, &[cf_name], cf_opts).unwrap();
        let mut ingest_opts = RocksIngestExternalFileOptions::new();
        ingest_opts.move_files(true);

        gen_sst_with_kvs(&db, cf_name, sst_path.to_str().unwrap(), &kvs);
        let size = file_system::metadata(&sst_path).unwrap().len();
        let checksum = calc_crc32(&sst_path).unwrap();

        if was_encrypted {
            // Add the file to key_manager to simulate an encrypted file.
            if let Some(manager) = key_manager {
                manager.new_file(sst_path.to_str().unwrap()).unwrap();
            }
        }

        // The first ingestion will hard link sst_path to sst_clone.
        check_hard_link(&sst_path, 1);
        prepare_sst_for_ingestion(&sst_path, &sst_clone, key_manager).unwrap();
        db.validate_sst_for_ingestion(cf_name, &sst_clone, size, checksum)
            .unwrap();
        check_hard_link(&sst_path, 2);
        check_hard_link(&sst_clone, 2);
        // If we prepare again, it will use hard link too.
        prepare_sst_for_ingestion(&sst_path, &sst_clone, key_manager).unwrap();
        db.validate_sst_for_ingestion(cf_name, &sst_clone, size, checksum)
            .unwrap();
        check_hard_link(&sst_path, 2);
        check_hard_link(&sst_clone, 2);
        db.ingest_external_file_cf(cf_name, &ingest_opts, &[sst_clone.to_str().unwrap()])
            .unwrap();
        check_db_with_kvs(&db, cf_name, &kvs);
        assert!(!sst_clone.exists());
        // Since we are not using key_manager in db, simulate the db deleting the file from
        // key_manager.
        if let Some(manager) = key_manager {
            manager.delete_file(sst_clone.to_str().unwrap()).unwrap();
        }

        // The second ingestion will copy sst_path to sst_clone.
        check_hard_link(&sst_path, 2);
        prepare_sst_for_ingestion(&sst_path, &sst_clone, key_manager).unwrap();
        db.validate_sst_for_ingestion(cf_name, &sst_clone, size, checksum)
            .unwrap();
        check_hard_link(&sst_path, 2);
        check_hard_link(&sst_clone, 1);
        db.ingest_external_file_cf(cf_name, &ingest_opts, &[sst_clone.to_str().unwrap()])
            .unwrap();
        check_db_with_kvs(&db, cf_name, &kvs);
        assert!(!sst_clone.exists());
    }

    #[test]
    fn test_prepare_sst_for_ingestion() {
        check_prepare_sst_for_ingestion(
            None, None, None,  /*key_manager*/
            false, /* was encrypted*/
        );
    }

    #[test]
    fn test_prepare_sst_for_ingestion_titan() {
        let mut db_opts = RocksDBOptions::new();
        let mut titan_opts = RocksTitanDBOptions::new();
        // Force all values write out to blob files.
        titan_opts.set_min_blob_size(0);
        db_opts.set_titandb_options(&titan_opts);
        let mut cf_opts = RocksColumnFamilyOptions::new();
        cf_opts.set_titandb_options(&titan_opts);
        check_prepare_sst_for_ingestion(
            Some(db_opts),
            Some(vec![RocksCFOptions::new("default", cf_opts)]),
            None,  /*key_manager*/
            false, /*was_encrypted*/
        );
    }

    #[test]
    fn test_prepare_sst_for_ingestion_with_key_manager_plaintext() {
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let key_manager = new_test_key_manager(&tmp_dir, None, None, None);
        let manager = Arc::new(key_manager.unwrap().unwrap());
        check_prepare_sst_for_ingestion(None, None, Some(&manager), false /*was_encrypted*/);
    }

    #[test]
    fn test_prepare_sst_for_ingestion_with_key_manager_encrypted() {
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let key_manager = new_test_key_manager(&tmp_dir, None, None, None);
        let manager = Arc::new(key_manager.unwrap().unwrap());
        check_prepare_sst_for_ingestion(None, None, Some(&manager), true /*was_encrypted*/);
    }
}
