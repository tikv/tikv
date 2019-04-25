// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader};
use std::{fs, usize};

use engine::rocks::util::io_limiter::IOLimiter;
use engine::rocks::util::{get_cf_handle, get_fastest_supported_compression_type};
use engine::rocks::{
    DBCompressionType, EnvOptions, IngestExternalFileOptions, Snapshot as DbSnapshot,
    SstFileWriter, Writable, WriteBatch, DB,
};
use engine::Iterable;
use tikv_util::codec::bytes::{BytesEncoder, CompactBytesFromFileDecoder};

use super::Error;

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
pub fn build_plain_cf_file(
    path: &str,
    snap: &DbSnapshot,
    cf: &str,
    start_key: &[u8],
    end_key: &[u8],
) -> Result<BuildStatistics, Error> {
    let mut file = box_try!(OpenOptions::new().write(true).create_new(true).open(path));
    let mut stats = BuildStatistics::default();
    box_try!(snap.scan_cf(cf, start_key, end_key, false, |key, value| {
        stats.key_count += 1;
        stats.total_size += key.len() + value.len();
        box_try!(file.encode_compact_bytes(key));
        box_try!(file.encode_compact_bytes(value));
        Ok(true)
    }));
    if stats.key_count > 0 {
        // use an empty byte array to indicate that cf reaches an end.
        box_try!(file.encode_compact_bytes(b""));
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
pub fn build_sst_cf_file(
    path: &str,
    snap: &DbSnapshot,
    cf: &str,
    start_key: &[u8],
    end_key: &[u8],
    io_limiter: Option<&IOLimiter>,
) -> Result<BuildStatistics, Error> {
    let mut sst_writer = create_sst_file_writer(snap, cf, path)?;
    let mut stats = BuildStatistics::default();
    let mut base = if io_limiter.is_some() { 0 } else { usize::MAX };
    box_try!(snap.scan_cf(cf, start_key, end_key, false, |key, value| {
        if let Some(ref io_limiter) = io_limiter {
            if stats.total_size >= base {
                let next_request = io_limiter.get_max_bytes_per_time();
                io_limiter.request(next_request);
                base += next_request as usize;
            }
        }
        stats.key_count += 1;
        stats.total_size += key.len() + value.len();
        if let Err(e) = sst_writer.put(key, value) {
            let io_error = io::Error::new(io::ErrorKind::Other, e);
            return Err(io_error.into());
        }
        Ok(true)
    }));
    if stats.key_count > 0 {
        box_try!(sst_writer.finish());
        drop(sst_writer);
        box_try!(File::open(path).and_then(|f| f.sync_all()));
    } else {
        drop(sst_writer);
        box_try!(fs::remove_file(path));
    }
    Ok(stats)
}

/// Apply the given snapshot file into a column family.
pub fn apply_plain_cf_file(
    path: &str,
    stale_detector: &impl StaleDetector,
    db: &DB,
    cf: &str,
    batch_size: usize,
) -> Result<(), Error> {
    let mut decoder = BufReader::new(box_try!(File::open(path)));
    let cf_handle = box_try!(get_cf_handle(&db, cf));
    let wb = WriteBatch::new();
    loop {
        if stale_detector.is_stale() {
            return Err(Error::Abort);
        }
        let key = box_try!(decoder.decode_compact_bytes());
        if key.is_empty() {
            box_try!(db.write(&wb));
            return Ok(());
        }
        let value = box_try!(decoder.decode_compact_bytes());
        box_try!(wb.put_cf(cf_handle, &key, &value));
        if wb.data_size() >= batch_size {
            box_try!(db.write(&wb));
            wb.clear();
        }
    }
}

pub fn apply_sst_cf_file(path: &str, db: &DB, cf: &str) -> Result<(), Error> {
    let cf_handle = box_try!(get_cf_handle(&db, cf));
    let mut ingest_opt = IngestExternalFileOptions::new();
    ingest_opt.move_files(true);
    box_try!(db.ingest_external_file_optimized(cf_handle, &ingest_opt, &[path]));
    Ok(())
}

fn create_sst_file_writer(snap: &DbSnapshot, cf: &str, path: &str) -> Result<SstFileWriter, Error> {
    let handle = box_try!(snap.cf_handle(cf));
    let mut io_options = snap.get_db().get_options_cf(handle).clone();
    io_options.compression(get_fastest_supported_compression_type());
    // in rocksdb 5.5.1, SstFileWriter will try to use bottommost_compression and
    // compression_per_level first, so to make sure our specified compression type
    // being used, we must set them empty or disabled.
    io_options.compression_per_level(&[]);
    io_options.bottommost_compression(DBCompressionType::Disable);
    if let Some(env) = snap.get_db().env() {
        io_options.set_env(env);
    }
    let mut writer = SstFileWriter::new(EnvOptions::new(), io_options);
    box_try!(writer.open(path));
    Ok(writer)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::raftstore::store::snap::tests::*;
    use engine::CF_DEFAULT;
    use tempdir::TempDir;

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
            for db_opt in vec![None, Some(gen_db_options_with_encryption())] {
                let dir = TempDir::new("test-snap-cf-db").unwrap();
                let db = db_creater(&dir, db_opt.clone(), None).unwrap();

                let snap_cf_dir = TempDir::new("test-snap-cf").unwrap();
                let plain_file_path = snap_cf_dir.path().join("plain");
                let snap = DbSnapshot::new(Arc::clone(&db));
                let stats = build_plain_cf_file(
                    &plain_file_path.to_str().unwrap(),
                    &snap,
                    CF_DEFAULT,
                    b"a",
                    b"z",
                )
                .unwrap();
                if stats.key_count == 0 {
                    assert_eq!(
                        fs::metadata(&plain_file_path).unwrap_err().kind(),
                        io::ErrorKind::NotFound
                    );
                    continue;
                }

                let dir1 = TempDir::new("test-snap-cf-db-apply").unwrap();
                let db1 = open_test_empty_db(&dir1, db_opt, None).unwrap();
                let detector = TestStaleDetector {};
                apply_plain_cf_file(
                    &plain_file_path.to_str().unwrap(),
                    &detector,
                    &db1,
                    CF_DEFAULT,
                    16,
                )
                .unwrap();
                assert_eq_db(&db, &db1);
            }
        }
    }

    #[test]
    fn test_cf_build_and_apply_sst_files() {
        let db_creaters = &[open_test_empty_db, open_test_db];
        for db_creater in db_creaters {
            for db_opt in vec![None, Some(gen_db_options_with_encryption())] {
                let dir = TempDir::new("test-snap-cf-db").unwrap();
                let db = db_creater(&dir, db_opt.clone(), None).unwrap();

                let snap_cf_dir = TempDir::new("test-snap-cf").unwrap();
                let sst_file_path = snap_cf_dir.path().join("sst");
                let stats = build_sst_cf_file(
                    &sst_file_path.to_str().unwrap(),
                    &DbSnapshot::new(Arc::clone(&db)),
                    CF_DEFAULT,
                    b"a",
                    b"z",
                    None,
                )
                .unwrap();
                if stats.key_count == 0 {
                    assert_eq!(
                        fs::metadata(&sst_file_path).unwrap_err().kind(),
                        io::ErrorKind::NotFound
                    );
                    continue;
                }

                let dir1 = TempDir::new("test-snap-cf-db-apply").unwrap();
                let db1 = open_test_empty_db(&dir1, db_opt, None).unwrap();
                apply_sst_cf_file(&sst_file_path.to_str().unwrap(), &db1, CF_DEFAULT).unwrap();
                assert_eq_db(&db, &db1);
            }
        }
    }
}
