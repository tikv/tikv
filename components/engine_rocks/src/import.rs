// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#[cfg(feature = "testexport")]
use std::sync::Mutex;

#[cfg(feature = "testexport")]
use engine_traits::{CF_DEFAULT, SstWriter, SstWriterBuilder, SyncMutable};
use engine_traits::{ImportExt, IngestExternalFileOptions, Range, Result};
use fail::fail_point;
#[cfg(feature = "testexport")]
use lazy_static::lazy_static;
use rocksdb::IngestExternalFileOptions as RawIngestExternalFileOptions;
#[cfg(feature = "testexport")]
use rocksdb::{DB, Writable};
#[cfg(feature = "testexport")]
use tempfile::Builder;
use tikv_util::{range_latch::RangeLatchGuard, time::Instant};
#[cfg(feature = "testexport")]
use txn_types::{Key, TimeStamp, Write, WriteType};

#[cfg(feature = "testexport")]
use crate::RocksSstWriterBuilder;
use crate::{
    engine::RocksEngine,
    perf_context_metrics::{
        INGEST_EXTERNAL_FILE_ALLOW_WRITE_COUNTER, INGEST_EXTERNAL_FILE_TIME_HISTOGRAM,
    },
    r2e, util,
};

#[cfg(feature = "testexport")]
lazy_static! {
    static ref TEST_CURRENT_INGEST_DB: Mutex<Option<std::sync::Arc<DB>>> = Mutex::new(None);
    static ref TEST_ASYNC_INGEST_RESULT: Mutex<Option<std::result::Result<String, String>>> =
        Mutex::new(None);
}

#[cfg(feature = "testexport")]
struct TestCurrentIngestDbGuard;

#[cfg(feature = "testexport")]
impl TestCurrentIngestDbGuard {
    fn install(db: std::sync::Arc<DB>) -> Self {
        *TEST_CURRENT_INGEST_DB.lock().unwrap() = Some(db);
        Self
    }
}

#[cfg(feature = "testexport")]
impl Drop for TestCurrentIngestDbGuard {
    fn drop(&mut self) {
        *TEST_CURRENT_INGEST_DB.lock().unwrap() = None;
    }
}

impl ImportExt for RocksEngine {
    type IngestExternalFileOptions = RocksIngestExternalFileOptions;

    fn ingest_external_file_cf(
        &self,
        cf_name: &str,
        files: &[&str],
        range: Option<Range<'_>>,
        force_allow_write: bool,
    ) -> Result<()> {
        // Acquire latch to prevent concurrency with compaction-filter operations
        // when using RocksDB IngestExternalFileOptions.allow_write = true.
        let _region_inject_latch_guard = range.as_ref().map(|r| {
            self.ingest_latch
                .acquire(r.start_key.to_vec(), r.end_key.to_vec())
        });
        fail_point!("after_apply_snapshot_ingest_latch_acquired");

        let cf = util::get_cf_handle(self.as_inner(), cf_name)?;
        let mut opts = RocksIngestExternalFileOptions::new();
        opts.move_files(true);
        let allow_write = range.is_some() || force_allow_write;
        opts.allow_write(allow_write);
        if allow_write {
            INGEST_EXTERNAL_FILE_ALLOW_WRITE_COUNTER
                .with_label_values(&["
            allow_write"])
                .inc();
        } else {
            INGEST_EXTERNAL_FILE_ALLOW_WRITE_COUNTER
                .with_label_values(&["
            not_allow_write"])
                .inc();
        }

        // Note: no need reset the global seqno to 0 for compatibility as #16992
        // enable the TiKV to handle the case on applying abnormal snapshot.
        let now = Instant::now_coarse();
        #[cfg(feature = "testexport")]
        let _test_current_ingest_db_guard =
            allow_write.then(|| TestCurrentIngestDbGuard::install(self.as_inner().clone()));
        // This is calling a specially optimized version of
        // ingest_external_file_cf. In cases where the memtable needs to be
        // flushed it avoids blocking writers while doing the flush. The
        // return value here just indicates whether the fallback path requiring
        // the manual memtable flush was taken.
        let did_memtable_flush = self
            .as_inner()
            .ingest_external_file_optimized(cf, &opts.0, files)
            .map_err(r2e)?;
        let time_cost = now.saturating_elapsed_secs();
        if did_memtable_flush {
            INGEST_EXTERNAL_FILE_TIME_HISTOGRAM
                .get(cf_name.into())
                .block
                .observe(time_cost);
        } else {
            INGEST_EXTERNAL_FILE_TIME_HISTOGRAM
                .get(cf_name.into())
                .non_block
                .observe(time_cost);
        }
        Ok(())
    }

    fn acquire_ingest_latch(&self, range: Range<'_>) -> RangeLatchGuard<'_> {
        self.ingest_latch
            .acquire(range.start_key.to_vec(), range.end_key.to_vec())
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

    fn allow_write(&mut self, f: bool) {
        self.0.set_allow_write(f);
    }
}

#[cfg(feature = "testexport")]
pub fn test_enable_pause_before_ingest_set_last_sequence() {
    unsafe {
        rocksdb::crocksdb_ffi::crocksdb_test_pause_before_set_last_sequence_enable();
    }
}

#[cfg(feature = "testexport")]
pub fn test_wait_for_pause_before_ingest_set_last_sequence(timeout_ms: u64) -> bool {
    unsafe {
        rocksdb::crocksdb_ffi::crocksdb_test_pause_before_set_last_sequence_wait(timeout_ms) != 0
    }
}

#[cfg(feature = "testexport")]
pub fn test_resume_pause_before_ingest_set_last_sequence() {
    unsafe {
        rocksdb::crocksdb_ffi::crocksdb_test_pause_before_set_last_sequence_resume();
    }
}

#[cfg(feature = "testexport")]
pub fn test_disable_pause_before_ingest_set_last_sequence() {
    unsafe {
        rocksdb::crocksdb_ffi::crocksdb_test_pause_before_set_last_sequence_disable();
    }
}

#[cfg(feature = "testexport")]
pub fn test_inject_commit_write_on_current_ingest_db(
    key: Vec<u8>,
    start_ts: u64,
    commit_ts: u64,
) -> std::result::Result<(), String> {
    let db = TEST_CURRENT_INGEST_DB
        .lock()
        .unwrap()
        .clone()
        .ok_or_else(|| "no active allow_write ingest is paused".to_owned())?;
    let write_cf = util::get_cf_handle(&db, engine_traits::CF_WRITE).map_err(|e| e.to_string())?;

    let start_ts = TimeStamp::new(start_ts);
    let commit_ts = TimeStamp::new(commit_ts);
    let write_key = keys::data_key(Key::from_raw(&key).append_ts(commit_ts).as_encoded());
    let write_value = Write::new(WriteType::Put, start_ts, Some(b"v".to_vec()))
        .as_ref()
        .to_bytes();
    db.put_cf(write_cf, &write_key, &write_value)
        .map_err(|e| e.to_string())?;

    let dummy_key = keys::data_key(
        Key::from_raw(b"repro-dummy")
            .append_ts(commit_ts.next())
            .as_encoded(),
    );
    let dummy_value = Write::new(WriteType::Put, commit_ts, Some(b"dummy".to_vec()))
        .as_ref()
        .to_bytes();
    db.put_cf(write_cf, &dummy_key, &dummy_value)
        .map_err(|e| e.to_string())
}

#[cfg(feature = "testexport")]
pub fn test_start_ingest_overlapping_default_cf_entries(
    entry_count: usize,
) -> std::result::Result<(), String> {
    if entry_count == 0 || entry_count > 128 {
        return Err("entry_count must be in 1..=128".to_owned());
    }
    *TEST_ASYNC_INGEST_RESULT.lock().unwrap() = None;
    std::thread::spawn(move || {
        let result = test_ingest_overlapping_default_cf_entries(entry_count);
        *TEST_ASYNC_INGEST_RESULT.lock().unwrap() = Some(result);
    });
    Ok(())
}

#[cfg(feature = "testexport")]
pub fn test_async_ingest_result() -> Option<std::result::Result<String, String>> {
    TEST_ASYNC_INGEST_RESULT.lock().unwrap().clone()
}

#[cfg(feature = "testexport")]
pub fn test_registered_engine_latest_sequence() -> std::result::Result<u64, String> {
    let engine =
        crate::engine::test_registered_engine().ok_or_else(|| "no registered engine".to_owned())?;
    Ok(engine.as_inner().get_latest_sequence_number())
}

#[cfg(feature = "testexport")]
fn test_ingest_overlapping_default_cf_entries(
    entry_count: usize,
) -> std::result::Result<String, String> {
    let engine =
        crate::engine::test_registered_engine().ok_or_else(|| "no registered engine".to_owned())?;
    let mut keys = Vec::with_capacity(entry_count);
    for i in 0..entry_count {
        let key = format!("__test_ingest_overlap_{:04}", i).into_bytes();
        engine
            .put_cf(CF_DEFAULT, &key, b"base")
            .map_err(|e| e.to_string())?;
        keys.push(key);
    }
    let before_ingest_seq = engine.as_inner().get_latest_sequence_number();

    let sst_dir = Builder::new()
        .prefix("test_ingest_overlapping_default_cf_entries")
        .tempdir()
        .map_err(|e| e.to_string())?;
    let sst_path = sst_dir.path().join("overlap.sst");
    let sst_path_str = sst_path
        .to_str()
        .ok_or_else(|| format!("bad sst path {}", sst_path.display()))?;
    let mut writer = RocksSstWriterBuilder::new()
        .set_db(&engine)
        .set_cf(CF_DEFAULT)
        .build(sst_path_str)
        .map_err(|e| e.to_string())?;
    for key in keys {
        writer.put(&key, b"ingested").map_err(|e| e.to_string())?;
    }
    writer.finish().map_err(|e| e.to_string())?;

    engine
        .ingest_external_file_cf(CF_DEFAULT, &[sst_path_str], None, true)
        .map_err(|e| e.to_string())?;
    let after_ingest_seq = engine.as_inner().get_latest_sequence_number();
    Ok(format!(
        "before_ingest_seq={}, after_ingest_seq={}",
        before_ingest_seq, after_ingest_seq
    ))
}

#[cfg(test)]
mod tests {
    use engine_traits::{
        ALL_CFS, CF_DEFAULT, FlowControlFactorsExt, MiscExt, Mutable, Peekable, SstWriter,
        SstWriterBuilder, WriteBatch, WriteBatchExt,
    };
    use tempfile::Builder;

    use super::*;
    use crate::{RocksCfOptions, RocksDbOptions, RocksSstWriterBuilder, util::new_engine_opt};

    #[test]
    fn test_ingest_multiple_file() {
        let path_dir = Builder::new()
            .prefix("test_ingest_multiple_file")
            .tempdir()
            .unwrap();
        let root_path = path_dir.path();
        let db_path = root_path.join("db");
        let path_str = db_path.to_str().unwrap();

        let cfs_opts = ALL_CFS
            .iter()
            .map(|cf| {
                let mut opt = RocksCfOptions::default();
                opt.set_force_consistency_checks(true);
                (*cf, opt)
            })
            .collect();
        let db = new_engine_opt(path_str, RocksDbOptions::default(), cfs_opts).unwrap();
        let mut wb = db.write_batch();
        for i in 1000..5000 {
            let v = i.to_string();
            wb.put(v.as_bytes(), v.as_bytes()).unwrap();
            if i % 1000 == 100 {
                wb.write().unwrap();
                wb.clear();
            }
        }
        // Flush one memtable to L0 to make sure that the next sst files to be ingested
        //  must locate in L0.
        db.flush_cf(CF_DEFAULT, true).unwrap();
        assert_eq!(
            1,
            db.get_cf_num_files_at_level(CF_DEFAULT, 0)
                .unwrap()
                .unwrap()
        );

        let p1 = root_path.join("sst1");
        let p2 = root_path.join("sst2");
        let mut sst1 = RocksSstWriterBuilder::new()
            .set_db(&db)
            .set_cf(CF_DEFAULT)
            .build(p1.to_str().unwrap())
            .unwrap();
        let mut sst2 = RocksSstWriterBuilder::new()
            .set_db(&db)
            .set_cf(CF_DEFAULT)
            .build(p2.to_str().unwrap())
            .unwrap();
        for i in 1001..2000 {
            let v = i.to_string();
            sst1.put(v.as_bytes(), v.as_bytes()).unwrap();
        }
        sst1.finish().unwrap();
        for i in 2001..3000 {
            let v = i.to_string();
            sst2.put(v.as_bytes(), v.as_bytes()).unwrap();
        }
        sst2.finish().unwrap();
        db.ingest_external_file_cf(
            CF_DEFAULT,
            &[p1.to_str().unwrap(), p2.to_str().unwrap()],
            None,
            false, // force_allow_write
        )
        .unwrap();
    }

    #[cfg(feature = "testexport")]
    #[test]
    fn test_inject_commit_write_on_current_ingest_db_helper() {
        let path_dir = Builder::new()
            .prefix("test_inject_commit_write_on_current_ingest_db")
            .tempdir()
            .unwrap();
        let db_path = path_dir.path().join("db");
        let path_str = db_path.to_str().unwrap();
        let cfs_opts = ALL_CFS
            .iter()
            .map(|cf| (*cf, crate::RocksCfOptions::default()))
            .collect();
        let db = new_engine_opt(path_str, RocksDbOptions::default(), cfs_opts).unwrap();
        let _guard = TestCurrentIngestDbGuard::install(db.as_inner().clone());

        super::test_inject_commit_write_on_current_ingest_db(b"z".to_vec(), 100, 101).unwrap();

        let write_key = keys::data_key(
            txn_types::Key::from_raw(b"z")
                .append_ts(101.into())
                .as_encoded(),
        );
        assert!(
            db.get_value_cf(engine_traits::CF_WRITE, &write_key)
                .unwrap()
                .is_some()
        );
    }
}
