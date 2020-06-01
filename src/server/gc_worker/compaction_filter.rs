// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::ffi::CString;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use super::GcWorkerConfigManager;
use crate::storage::mvcc::{GC_DELETE_VERSIONS_HISTOGRAM, MVCC_VERSIONS_HISTOGRAM};
use engine::rocks::{
    new_compaction_filter_raw, CompactionFilter, CompactionFilterContext, CompactionFilterFactory,
    DBCompactionFilter, DBIterator, ReadOptions, SeekKey, WriteOptions, DB,
};
use engine_rocks::{util::get_cf_handle, RocksEngine, RocksWriteBatch};
use engine_traits::{Mutable, WriteBatch, CF_WRITE};
use txn_types::{Key, TimeStamp, WriteRef, WriteType};

const DEFAULT_DELETE_BATCH_SIZE: usize = 256 * 1024;

struct GcContext {
    db: Arc<DB>,
    safe_point: Arc<AtomicU64>,
    cfg_tracker: GcWorkerConfigManager,
}

lazy_static! {
    static ref GC_CONTEXT: Mutex<Option<GcContext>> = Mutex::new(None);
}

pub fn init_compaction_filter(
    db: RocksEngine,
    safe_point: Arc<AtomicU64>,
    cfg_tracker: GcWorkerConfigManager,
) {
    info!("initialize GC context for compaction filter");
    let mut gc_context = GC_CONTEXT.lock().unwrap();
    *gc_context = Some(GcContext {
        db: db.as_inner().clone(),
        safe_point,
        cfg_tracker,
    });
}

pub struct WriteCompactionFilterFactory;

impl CompactionFilterFactory for WriteCompactionFilterFactory {
    fn create_compaction_filter(
        &self,
        context: &CompactionFilterContext,
    ) -> *mut DBCompactionFilter {
        let gc_context_option = GC_CONTEXT.lock().unwrap();
        let gc_context = match *gc_context_option {
            Some(ref ctx) => ctx,
            None => return std::ptr::null_mut(),
        };
        if !gc_context.cfg_tracker.value().enable_compaction_filter {
            return std::ptr::null_mut();
        }
        if gc_context.safe_point.load(Ordering::Relaxed) == 0 {
            // Safe point has not been initialized yet.
            return std::ptr::null_mut();
        }

        let name = CString::new("write_compaction_filter").unwrap();
        let db = Arc::clone(&gc_context.db);
        let safe_point = Arc::clone(&gc_context.safe_point);

        let filter = Box::new(WriteCompactionFilter::new(db, safe_point, context));
        unsafe { new_compaction_filter_raw(name, filter) }
    }
}

struct WriteCompactionFilter {
    bottommost_level: bool,
    safe_point: Arc<AtomicU64>,
    db: Arc<DB>,

    write_batch: RocksWriteBatch,
    delete_key_prefix: Vec<u8>,
    delete_key_ts: u64,
    key_prefix: Vec<u8>,
    remove_older: bool,

    versions: usize,
    deleted: usize,
}

impl WriteCompactionFilter {
    fn new(db: Arc<DB>, safe_point: Arc<AtomicU64>, context: &CompactionFilterContext) -> Self {
        // Safe point must have been initialized.
        assert!(safe_point.load(Ordering::Relaxed) > 0);

        let wb = RocksWriteBatch::with_capacity(Arc::clone(&db), DEFAULT_DELETE_BATCH_SIZE);
        WriteCompactionFilter {
            bottommost_level: context.bottommost_level(),
            safe_point,
            db,
            write_batch: wb,
            delete_key_prefix: vec![],
            delete_key_ts: 0,
            key_prefix: vec![],
            remove_older: false,

            versions: 0,
            deleted: 0,
        }
    }

    fn delete_default_key(&mut self, key: &[u8], write_batch: Option<&mut RocksWriteBatch>) {
        if let Some(write_batch) = write_batch {
            write_batch.delete(key).unwrap();
            return;
        }
        self.write_batch.delete(key).unwrap();
        self.flush_pending_writes_if_need();
    }

    fn delete_write_key(&mut self, key: &[u8], write_batch: Option<&mut RocksWriteBatch>) {
        if let Some(write_batch) = write_batch {
            write_batch.delete_cf(CF_WRITE, key).unwrap();
            return;
        }
        self.write_batch.delete_cf(CF_WRITE, key).unwrap();
        self.flush_pending_writes_if_need();
    }

    fn flush_pending_writes_if_need(&mut self) {
        if self.write_batch.data_size() > DEFAULT_DELETE_BATCH_SIZE {
            let mut opts = WriteOptions::new();
            opts.set_sync(false);
            let raw_batch = self.write_batch.as_inner();
            self.db.write_opt(raw_batch, &opts).unwrap();
            self.write_batch.clear();
        }
    }

    fn reset_statistics(&mut self) {
        if self.versions != 0 {
            MVCC_VERSIONS_HISTOGRAM.observe(self.versions as f64);
            self.versions = 0;
        }
        if self.deleted != 0 {
            GC_DELETE_VERSIONS_HISTOGRAM.observe(self.deleted as f64);
            self.deleted = 0;
        }
    }
}

impl Drop for WriteCompactionFilter {
    fn drop(&mut self) {
        if !self.delete_key_prefix.is_empty() && self.delete_key_prefix == self.key_prefix {
            let mut write_batch =
                RocksWriteBatch::with_capacity(Arc::clone(&self.db), DEFAULT_DELETE_BATCH_SIZE);

            // In this compaction, the last MVCC version is deleted. However there could be
            // still some versions for the key which are not included in this compaction.
            let cf_handle = get_cf_handle(&self.db, CF_WRITE).unwrap();
            let mut iter = DBIterator::new_cf(self.db.clone(), cf_handle, ReadOptions::new());
            let seek_key = Key::from_encoded_slice(&self.delete_key_prefix)
                .append_ts(TimeStamp::from(self.delete_key_ts))
                .into_encoded();
            let mut valid = iter.seek(SeekKey::Key(&seek_key)).unwrap();
            while valid {
                let (key, value) = (iter.key(), iter.value());
                let key_prefix = match Key::split_on_ts_for(key) {
                    Ok((prefix, _)) if prefix != self.delete_key_prefix.as_slice() => break,
                    Ok((prefix, _)) => prefix,
                    Err(_) => break,
                };

                let write = WriteRef::parse(value).unwrap();
                let (start_ts, short_value) = (write.start_ts, write.short_value);
                if short_value.is_none() {
                    let key = Key::from_encoded_slice(key_prefix).append_ts(start_ts);
                    self.delete_default_key(key.as_encoded(), Some(&mut write_batch));
                }

                self.delete_write_key(key, Some(&mut write_batch));
                self.deleted += 1;
                valid = iter.next().unwrap();
            }

            let mut opts = WriteOptions::new();
            opts.set_sync(true);
            self.db.write_opt(write_batch.as_inner(), &opts).unwrap();
        }
        if !self.write_batch.is_empty() {
            let mut opts = WriteOptions::new();
            opts.set_sync(true);
            let raw_batch = self.write_batch.as_inner();
            self.db.write_opt(raw_batch, &opts).unwrap();
        } else {
            self.db.flush(true).unwrap();
        }
    }
}

impl CompactionFilter for WriteCompactionFilter {
    fn filter(
        &mut self,
        _level: usize,
        key: &[u8],
        value: &[u8],
        _: &mut Vec<u8>,
        _: &mut bool,
    ) -> bool {
        let safe_point = self.safe_point.load(Ordering::Relaxed);
        let (key_prefix, commit_ts) = match Key::split_on_ts_for(key) {
            Ok((key, ts)) => (key, ts.into_inner()),
            // Invalid MVCC keys, don't touch them.
            Err(_) => return false,
        };

        if self.key_prefix != key_prefix {
            self.key_prefix.clear();
            self.key_prefix.extend_from_slice(key_prefix);
            self.remove_older = false;
            self.reset_statistics();
        }

        self.versions += 1;
        if commit_ts > safe_point {
            return false;
        }

        let mut filtered = self.remove_older;
        let WriteRef {
            write_type,
            start_ts,
            short_value,
        } = WriteRef::parse(value).unwrap();
        if !self.remove_older {
            // here `filtered` must be false.
            match write_type {
                WriteType::Rollback | WriteType::Lock => filtered = true,
                WriteType::Delete => {
                    // Currently `WriteType::Delete` will always be kept.
                    self.remove_older = true;
                    if self.bottommost_level {
                        // Handle delete marks if the bottommost level is reached.
                        filtered = true;
                        self.delete_key_prefix.clear();
                        self.delete_key_prefix.extend_from_slice(key_prefix);
                        self.delete_key_ts = commit_ts;
                    }
                }
                WriteType::Put => self.remove_older = true,
            }
        }

        if filtered {
            if short_value.is_none() {
                let key = Key::from_encoded_slice(key_prefix).append_ts(start_ts);
                self.delete_default_key(key.as_encoded(), None);
            }
            self.deleted += 1;
        }

        filtered
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::storage::kv::{RocksEngine as StorageRocksEngine, TestEngineBuilder};
    use crate::storage::mvcc::tests::{must_commit, must_prewrite_delete, must_prewrite_put};
    use engine_rocks::RocksEngine;
    use engine_traits::{CompactExt, Peekable, SyncMutable};

    pub fn gc_by_compact(engine: &StorageRocksEngine, _: &[u8], safe_point: u64) {
        let engine = RocksEngine::from_db(engine.get_rocksdb());
        // Put a new key-value pair to ensure compaction can be triggered correctly.
        engine.put_cf("write", b"k1", b"v1").unwrap();
        do_gc_by_compact(&engine, None, None, safe_point);
    }

    fn do_gc_by_compact(
        engine: &RocksEngine,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        safe_point: u64,
    ) {
        let safe_point = Arc::new(AtomicU64::new(safe_point));
        let cfg = GcWorkerConfigManager(Arc::new(Default::default()));
        cfg.0.update(|v| v.enable_compaction_filter = true);
        init_compaction_filter(engine.clone(), safe_point, cfg);
        engine.compact_range("write", start, end, false, 1).unwrap();
    }

    fn rocksdb_level_file_counts(engine: &RocksEngine, cf: &str) -> Vec<usize> {
        let cf_handle = get_cf_handle(engine.as_inner(), cf).unwrap();
        let metadata = engine.as_inner().get_column_family_meta_data(cf_handle);
        let mut res = Vec::with_capacity(7);
        for level_meta in metadata.get_levels() {
            res.push(level_meta.get_files().len());
        }
        res
    }

    // There could be some versions which are not included in one compaction. This case tests that
    // those data can be cleared correctly by WriteCompactionFilter.
    #[test]
    fn test_compaction_filter_tail_data() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let raw_engine = RocksEngine::from_db(engine.get_rocksdb());

        let split_key = Key::from_raw(b"key")
            .append_ts(TimeStamp::from(135))
            .into_encoded();

        must_prewrite_put(&engine, b"key", b"value", b"key", 100);
        must_commit(&engine, b"key", 100, 110);
        do_gc_by_compact(&raw_engine, None, None, 50);
        assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[6], 1);

        must_prewrite_put(&engine, b"key", b"value", b"key", 120);
        must_commit(&engine, b"key", 120, 130);
        must_prewrite_delete(&engine, b"key", b"key", 140);
        must_commit(&engine, b"key", 140, 140);
        do_gc_by_compact(&raw_engine, None, Some(&split_key), 50);
        assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[6], 2);

        // Put more key/value pairs so that 1 file in L0 and 1 file in L6 can be merged.
        must_prewrite_put(&engine, b"kex", b"value", b"kex", 100);
        must_commit(&engine, b"kex", 100, 110);

        do_gc_by_compact(&raw_engine, None, Some(&split_key), 200);

        // There are still 2 files in L6 because the SST contains key_110 is not touched.
        assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[6], 2);

        // Although the SST files is not involved in the last compaction,
        // all versions of "key" should be cleared.
        let key = Key::from_raw(b"key")
            .append_ts(TimeStamp::from(110))
            .into_encoded();
        let x = raw_engine.get_value_cf(CF_WRITE, &key).unwrap();
        assert!(x.is_none());
    }
}
