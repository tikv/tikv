// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    fs,
    fs::{File, OpenOptions},
    io::{self, BufReader, Read, Write},
    sync::Arc,
    usize,
};

use encryption::{
    encryption_method_from_db_encryption_method, DataKeyManager, DecrypterReader, EncrypterWriter,
    Iv,
};
use engine_traits::{
    CfName, EncryptionKeyManager, Error as EngineError, Iterable, KvEngine, Mutable,
    SstCompressionType, SstWriter, SstWriterBuilder, WriteBatch,
};
use kvproto::encryptionpb::EncryptionMethod;
use tikv_util::{
    box_try,
    codec::bytes::{BytesEncoder, CompactBytesFromFileDecoder},
    debug,
    time::Limiter,
};

use super::{Error, IO_LIMITER_CHUNK_SIZE};

/// Used to check a procedure is stale or not.
pub trait StaleDetector {
    fn is_stale(&self) -> bool;
}

#[derive(Clone, Copy, Default)]
pub struct BuildStatistics {
    pub key_count: usize,
    pub total_size: usize,
}

/// Build a snapshot file for the given column family in plain format.
/// If there are no key-value pairs fetched, no files will be created at `path`,
/// otherwise the file will be created and synchronized.
pub fn build_plain_cf_file<E>(
    path: &str,
    key_mgr: Option<&Arc<DataKeyManager>>,
    snap: &E::Snapshot,
    cf: &str,
    start_key: &[u8],
    end_key: &[u8],
) -> Result<BuildStatistics, Error>
where
    E: KvEngine,
{
    let mut file = Some(box_try!(
        OpenOptions::new().write(true).create_new(true).open(path)
    ));
    let mut encrypted_file: Option<EncrypterWriter<File>> = None;
    let mut should_encrypt = false;

    if let Some(key_mgr) = key_mgr {
        let enc_info = box_try!(key_mgr.new_file(path));
        let mthd = encryption_method_from_db_encryption_method(enc_info.method);
        if mthd != EncryptionMethod::Plaintext {
            let writer = box_try!(EncrypterWriter::new(
                file.take().unwrap(),
                mthd,
                &enc_info.key,
                box_try!(Iv::from_slice(&enc_info.iv)),
            ));
            encrypted_file = Some(writer);
            should_encrypt = true;
        }
    }

    let mut writer = if !should_encrypt {
        file.as_mut().unwrap() as &mut dyn Write
    } else {
        encrypted_file.as_mut().unwrap() as &mut dyn Write
    };

    let mut stats = BuildStatistics::default();
    box_try!(snap.scan_cf(cf, start_key, end_key, false, |key, value| {
        stats.key_count += 1;
        stats.total_size += key.len() + value.len();
        box_try!(BytesEncoder::encode_compact_bytes(&mut writer, key));
        box_try!(BytesEncoder::encode_compact_bytes(&mut writer, value));
        Ok(true)
    }));

    if stats.key_count > 0 {
        box_try!(BytesEncoder::encode_compact_bytes(&mut writer, b""));
        let file = if !should_encrypt {
            file.unwrap()
        } else {
            encrypted_file.unwrap().finalize().unwrap()
        };
        box_try!(file.sync_all());
    } else {
        drop(file);
        box_try!(fs::remove_file(path));
    }

    Ok(stats)
}

/// Build a snapshot file for the given column family in sst format.
/// If there are no key-value pairs fetched, no files will be created at `path`,
/// otherwise the file will be created and synchronized.
pub fn build_sst_cf_file<E>(
    path: &str,
    engine: &E,
    snap: &E::Snapshot,
    cf: CfName,
    start_key: &[u8],
    end_key: &[u8],
    io_limiter: &Limiter,
) -> Result<BuildStatistics, Error>
where
    E: KvEngine,
{
    let mut sst_writer = create_sst_file_writer::<E>(engine, cf, path)?;
    let mut stats = BuildStatistics::default();
    let mut remained_quota = 0;
    box_try!(snap.scan_cf(cf, start_key, end_key, false, |key, value| {
        let entry_len = key.len() + value.len();
        while entry_len > remained_quota {
            // It's possible to acquire more than necessary, but let it be.
            io_limiter.blocking_consume(IO_LIMITER_CHUNK_SIZE);
            remained_quota += IO_LIMITER_CHUNK_SIZE;
        }
        remained_quota -= entry_len;

        stats.key_count += 1;
        stats.total_size += entry_len;
        if let Err(e) = sst_writer.put(key, value) {
            let io_error = io::Error::new(io::ErrorKind::Other, e);
            return Err(io_error.into());
        }
        Ok(true)
    }));
    if stats.key_count > 0 {
        box_try!(sst_writer.finish());
        box_try!(File::open(path).and_then(|f| f.sync_all()));
    } else {
        box_try!(fs::remove_file(path));
    }
    Ok(stats)
}

/// Apply the given snapshot file into a column family. `callback` will be invoked after each batch of
/// key value pairs written to db.
pub fn apply_plain_cf_file<E, F>(
    path: &str,
    key_mgr: Option<&Arc<DataKeyManager>>,
    stale_detector: &impl StaleDetector,
    db: &E,
    cf: &str,
    batch_size: usize,
    mut callback: F,
) -> Result<(), Error>
where
    E: KvEngine,
    F: for<'r> FnMut(&'r [(Vec<u8>, Vec<u8>)]),
{
    let file = box_try!(File::open(path));
    let mut decoder = if let Some(key_mgr) = key_mgr {
        let reader = get_decrypter_reader(path, key_mgr)?;
        BufReader::new(reader)
    } else {
        BufReader::new(Box::new(file) as Box<dyn Read + Send>)
    };

    let mut wb = db.write_batch();
    let mut write_to_db = |batch: &mut Vec<(Vec<u8>, Vec<u8>)>| -> Result<(), EngineError> {
        batch.iter().try_for_each(|(k, v)| wb.put_cf(cf, k, v))?;
        wb.write()?;
        wb.clear();
        callback(batch);
        batch.clear();
        Ok(())
    };

    // Collect keys to a vec rather than wb so that we can invoke the callback less times.
    let mut batch = Vec::with_capacity(1024);
    let mut batch_data_size = 0;

    loop {
        if stale_detector.is_stale() {
            return Err(Error::Abort);
        }
        let key = box_try!(decoder.decode_compact_bytes());
        if key.is_empty() {
            if !batch.is_empty() {
                box_try!(write_to_db(&mut batch));
            }
            return Ok(());
        }
        let value = box_try!(decoder.decode_compact_bytes());
        batch_data_size += key.len() + value.len();
        batch.push((key, value));
        if batch_data_size >= batch_size {
            box_try!(write_to_db(&mut batch));
            batch_data_size = 0;
        }
    }
}

pub fn apply_sst_cf_file<E>(path: &str, db: &E, cf: &str) -> Result<(), Error>
where
    E: KvEngine,
{
    box_try!(db.ingest_external_file_cf(cf, &[path]));
    Ok(())
}

fn create_sst_file_writer<E>(engine: &E, cf: CfName, path: &str) -> Result<E::SstWriter, Error>
where
    E: KvEngine,
{
    let builder = E::SstWriterBuilder::new()
        .set_db(engine)
        .set_cf(cf)
        .set_compression_type(Some(SstCompressionType::Zstd));
    let writer = box_try!(builder.build(path));
    Ok(writer)
}

// TODO: Use DataKeyManager::open_file_for_read() instead.
pub fn get_decrypter_reader(
    file: &str,
    encryption_key_manager: &DataKeyManager,
) -> Result<Box<dyn Read + Send>, Error> {
    let enc_info = box_try!(encryption_key_manager.get_file(file));
    let mthd = encryption_method_from_db_encryption_method(enc_info.method);
    debug!(
        "get_decrypter_reader gets enc_info for {:?}, method: {:?}",
        file, mthd
    );
    if mthd == EncryptionMethod::Plaintext {
        let f = box_try!(File::open(file));
        return Ok(Box::new(f) as Box<dyn Read + Send>);
    }
    let iv = box_try!(Iv::from_slice(&enc_info.iv));
    let f = box_try!(File::open(file));
    let r = box_try!(DecrypterReader::new(f, mthd, &enc_info.key, iv));
    Ok(Box::new(r) as Box<dyn Read + Send>)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use engine_test::kv::KvTestEngine;
    use engine_traits::CF_DEFAULT;
    use tempfile::Builder;
    use tikv_util::time::Limiter;

    use super::*;
    use crate::store::snap::{tests::*, SNAPSHOT_CFS};

    struct TestStaleDetector;
    impl StaleDetector for TestStaleDetector {
        fn is_stale(&self) -> bool {
            false
        }
    }

    #[test]
    fn test_cf_build_and_apply_plain_files() {
        let db_creaters = &[open_test_empty_db, open_test_db];
        for db_creater in db_creaters {
            let (_enc_dir, enc_opts) =
                gen_db_options_with_encryption("test_cf_build_and_apply_plain_files_enc");
            for db_opt in vec![None, Some(enc_opts)] {
                let dir = Builder::new().prefix("test-snap-cf-db").tempdir().unwrap();
                let db: KvTestEngine = db_creater(dir.path(), db_opt.clone(), None).unwrap();
                // Collect keys via the key_callback into a collection.
                let mut applied_keys: HashMap<_, Vec<_>> = HashMap::new();
                let dir1 = Builder::new()
                    .prefix("test-snap-cf-db-apply")
                    .tempdir()
                    .unwrap();
                let db1: KvTestEngine = open_test_empty_db(dir1.path(), db_opt, None).unwrap();

                let snap = db.snapshot();
                for cf in SNAPSHOT_CFS {
                    let snap_cf_dir = Builder::new().prefix("test-snap-cf").tempdir().unwrap();
                    let plain_file_path = snap_cf_dir.path().join("plain");
                    let stats = build_plain_cf_file::<KvTestEngine>(
                        plain_file_path.to_str().unwrap(),
                        None,
                        &snap,
                        cf,
                        &keys::data_key(b"a"),
                        &keys::data_end_key(b"z"),
                    )
                    .unwrap();
                    if stats.key_count == 0 {
                        assert_eq!(
                            fs::metadata(&plain_file_path).unwrap_err().kind(),
                            io::ErrorKind::NotFound
                        );
                        continue;
                    }

                    let detector = TestStaleDetector {};
                    apply_plain_cf_file(
                        plain_file_path.to_str().unwrap(),
                        None,
                        &detector,
                        &db1,
                        cf,
                        16,
                        |v| {
                            v.iter()
                                .cloned()
                                .for_each(|pair| applied_keys.entry(cf).or_default().push(pair))
                        },
                    )
                    .unwrap();
                }

                assert_eq_db(&db, &db1);

                // Scan keys from db
                let mut keys_in_db: HashMap<_, Vec<_>> = HashMap::new();
                for cf in SNAPSHOT_CFS {
                    snap.scan_cf(
                        cf,
                        &keys::data_key(b"a"),
                        &keys::data_end_key(b"z"),
                        true,
                        |k, v| {
                            keys_in_db
                                .entry(cf)
                                .or_default()
                                .push((k.to_owned(), v.to_owned()));
                            Ok(true)
                        },
                    )
                    .unwrap();
                }
                assert_eq!(applied_keys, keys_in_db);
            }
        }
    }

    #[test]
    fn test_cf_build_and_apply_sst_files() {
        let db_creaters = &[open_test_empty_db, open_test_db];
        let limiter = Limiter::new(f64::INFINITY);
        for db_creater in db_creaters {
            let (_enc_dir, enc_opts) =
                gen_db_options_with_encryption("test_cf_build_and_apply_sst_files_enc");
            for db_opt in vec![None, Some(enc_opts)] {
                let dir = Builder::new().prefix("test-snap-cf-db").tempdir().unwrap();
                let db = db_creater(dir.path(), db_opt.clone(), None).unwrap();

                let snap_cf_dir = Builder::new().prefix("test-snap-cf").tempdir().unwrap();
                let sst_file_path = snap_cf_dir.path().join("sst");
                let stats = build_sst_cf_file::<KvTestEngine>(
                    sst_file_path.to_str().unwrap(),
                    &db,
                    &db.snapshot(),
                    CF_DEFAULT,
                    b"a",
                    b"z",
                    &limiter,
                )
                .unwrap();
                if stats.key_count == 0 {
                    assert_eq!(
                        fs::metadata(&sst_file_path).unwrap_err().kind(),
                        io::ErrorKind::NotFound
                    );
                    continue;
                }

                let dir1 = Builder::new()
                    .prefix("test-snap-cf-db-apply")
                    .tempdir()
                    .unwrap();
                let db1: KvTestEngine = open_test_empty_db(dir1.path(), db_opt, None).unwrap();
                apply_sst_cf_file(sst_file_path.to_str().unwrap(), &db1, CF_DEFAULT).unwrap();
                assert_eq_db(&db, &db1);
            }
        }
    }
}
