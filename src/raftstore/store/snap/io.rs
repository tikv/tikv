// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader};
use std::{fs, usize};

use engine::rocks::util::get_cf_handle;
use engine::rocks::{IngestExternalFileOptions, Snapshot as DbSnapshot, Writable, WriteBatch, DB};
use engine::CfName;
use engine_rocks::{Compat, RocksEngine, RocksSstWriter, RocksSstWriterBuilder};
use engine_traits::IOLimiter;
use engine_traits::{SstWriter, SstWriterBuilder, Iterable};
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
    box_try!(snap.c().scan_cf(cf, start_key, end_key, false, |key, value| {
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
pub fn build_sst_cf_file<L: IOLimiter>(
    path: &str,
    snap: &DbSnapshot,
    cf: CfName,
    start_key: &[u8],
    end_key: &[u8],
    io_limiter: Option<&L>,
) -> Result<BuildStatistics, Error> {
    let mut sst_writer = create_sst_file_writer(snap, cf, path)?;
    let mut stats = BuildStatistics::default();
    let base = io_limiter
        .as_ref()
        .map_or(0 as i64, |l| l.get_max_bytes_per_time());
    let mut bytes: i64 = 0;
    box_try!(snap.c().scan_cf(cf, start_key, end_key, false, |key, value| {
        let entry_len = key.len() + value.len();
        if let Some(ref io_limiter) = io_limiter {
            if bytes >= base {
                bytes = 0;
                io_limiter.request(base);
            }
            bytes += entry_len as i64
        }
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
    let wb = WriteBatch::default();
    loop {
        if stale_detector.is_stale() {
            return Err(Error::Abort);
        }
        let key = box_try!(decoder.decode_compact_bytes());
        if key.is_empty() {
            if !wb.is_empty() {
                box_try!(db.write(&wb));
            }
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

fn create_sst_file_writer(
    snap: &DbSnapshot,
    cf: CfName,
    path: &str,
) -> Result<RocksSstWriter, Error> {
    let db = snap.get_db();
    let engine = RocksEngine::from_ref(&db);
    let builder = RocksSstWriterBuilder::new().set_db(&engine).set_cf(cf);
    let writer = box_try!(builder.build(path));
    Ok(writer)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::raftstore::store::snap::tests::*;
    use engine::CF_DEFAULT;
    use engine_rocks::RocksIOLimiter;
    use tempfile::Builder;

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
                let dir = Builder::new().prefix("test-snap-cf-db").tempdir().unwrap();
                let db = db_creater(&dir.path(), db_opt.clone(), None).unwrap();

                let snap_cf_dir = Builder::new().prefix("test-snap-cf").tempdir().unwrap();
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

                let dir1 = Builder::new()
                    .prefix("test-snap-cf-db-apply")
                    .tempdir()
                    .unwrap();
                let db1 = open_test_empty_db(&dir1.path(), db_opt, None).unwrap();
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
                let dir = Builder::new().prefix("test-snap-cf-db").tempdir().unwrap();
                let db = db_creater(&dir.path(), db_opt.clone(), None).unwrap();

                let snap_cf_dir = Builder::new().prefix("test-snap-cf").tempdir().unwrap();
                let sst_file_path = snap_cf_dir.path().join("sst");
                let stats = build_sst_cf_file::<RocksIOLimiter>(
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

                let dir1 = Builder::new()
                    .prefix("test-snap-cf-db-apply")
                    .tempdir()
                    .unwrap();
                let db1 = open_test_empty_db(&dir1.path(), db_opt, None).unwrap();
                apply_sst_cf_file(&sst_file_path.to_str().unwrap(), &db1, CF_DEFAULT).unwrap();
                assert_eq_db(&db, &db1);
            }
        }
    }
}
