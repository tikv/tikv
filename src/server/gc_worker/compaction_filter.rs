// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::ffi::CString;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use super::{GcConfig, GcWorkerConfigManager};
use crate::storage::mvcc::{GC_DELETE_VERSIONS_HISTOGRAM, MVCC_VERSIONS_HISTOGRAM};
use engine_rocks::raw::{
    new_compaction_filter_raw, CompactionFilter, CompactionFilterContext, CompactionFilterFactory,
    DBCompactionFilter, DB,
};
use engine_rocks::{RocksEngine, RocksEngineIterator, RocksWriteBatch};
use engine_traits::{
    IterOptions, Iterable, Iterator, MiscExt, Mutable, Peekable, SeekKey, WriteBatch,
    WriteBatchExt, WriteOptions, CF_LOCK, CF_WRITE,
};
use pd_client::ClusterVersion;
use txn_types::{Key, WriteRef, WriteType};

const DEFAULT_DELETE_BATCH_SIZE: usize = 256 * 1024;
const DEFAULT_DELETE_BATCH_COUNT: usize = 128;

// The default version that can enable compaction filter for GC. This is necessary because after
// compaction filter is enabled, it's impossible to fallback to ealier version which modifications
// of GC are distributed to other replicas by Raft.
const COMPACTION_FILTER_MINIMAL_VERSION: &str = "5.0.0";

struct GcContext {
    db: Arc<DB>,
    safe_point: Arc<AtomicU64>,
    cfg_tracker: GcWorkerConfigManager,
    cluster_version: ClusterVersion,
}

lazy_static! {
    static ref GC_CONTEXT: Mutex<Option<GcContext>> = Mutex::new(None);
}

pub fn init_compaction_filter(
    db: RocksEngine,
    safe_point: Arc<AtomicU64>,
    cfg_tracker: GcWorkerConfigManager,
    cluster_version: ClusterVersion,
) {
    info!("initialize GC context for compaction filter");
    let mut gc_context = GC_CONTEXT.lock().unwrap();
    *gc_context = Some(GcContext {
        db: db.as_inner().clone(),
        safe_point,
        cfg_tracker,
        cluster_version,
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

        let safe_point = gc_context.safe_point.load(Ordering::Relaxed);
        if safe_point == 0 {
            // Safe point has not been initialized yet.
            return std::ptr::null_mut();
        }

        if !is_compaction_filter_allowd(
            &*gc_context.cfg_tracker.value(),
            &gc_context.cluster_version,
        ) {
            return std::ptr::null_mut();
        }

        let name = CString::new("write_compaction_filter").unwrap();
        let db = Arc::clone(&gc_context.db);
        let filter = Box::new(WriteCompactionFilter::new(db, safe_point, context));
        unsafe { new_compaction_filter_raw(name, filter) }
    }
}

struct WriteCompactionFilter {
    safe_point: u64,
    engine: RocksEngine,

    write_batch: RocksWriteBatch,
    key_prefix: Vec<u8>,
    remove_older: bool,

    // Indicates whether the target is the bottommost level or not.
    bottommost_level: bool,
    // To handle delete marks at the bottommost level.
    write_iter: Option<RocksEngineIterator>,

    // For metrics about (versions, deleted_versions) for every MVCC key.
    versions: usize,
    deleted: usize,
    // Total versions and deleted versions in the compaction.
    total_versions: usize,
    total_deleted: usize,
}

impl WriteCompactionFilter {
    fn new(db: Arc<DB>, safe_point: u64, context: &CompactionFilterContext) -> Self {
        // Safe point must have been initialized.
        assert!(safe_point > 0);

        let engine = RocksEngine::from_db(db.clone());
        let write_batch = RocksWriteBatch::with_capacity(db, DEFAULT_DELETE_BATCH_SIZE);
        let bottommost_level = context.is_bottommost_level();
        let write_iter = if context.is_bottommost_level() {
            // TODO: give lower bound and upper bound to the iterator.
            let opts = IterOptions::default();
            Some(engine.iterator_cf_opt(CF_WRITE, opts).unwrap())
        } else {
            None
        };

        WriteCompactionFilter {
            safe_point,
            engine,
            write_batch,
            key_prefix: vec![],
            remove_older: false,
            bottommost_level,
            write_iter,
            versions: 0,
            deleted: 0,
            total_versions: 0,
            total_deleted: 0,
        }
    }

    // Do gc before a delete mark.
    fn gc_before_delete_mark(&mut self, delete: &[u8], prefix: &[u8]) {
        let mut iter = self.write_iter.take().unwrap();
        let mut valid = iter.seek(SeekKey::Key(delete)).unwrap()
            && (iter.key() != delete || iter.next().unwrap());
        while valid {
            let key = iter.key();
            let key_prefix = match Key::split_on_ts_for(key) {
                Ok((key, _)) => key,
                Err(_) => continue,
            };
            if key_prefix != prefix {
                break;
            }
            self.delete_write_key(key);
            valid = iter.next().unwrap();
        }
        self.write_iter = Some(iter);
    }

    fn delete_write_key(&mut self, key: &[u8]) {
        self.write_batch.delete_cf(CF_WRITE, key).unwrap();
        self.flush_pending_writes_if_need();
    }

    fn flush_pending_writes_if_need(&mut self) {
        if self.write_batch.count() > DEFAULT_DELETE_BATCH_COUNT {
            let mut opts = WriteOptions::new();
            opts.set_sync(false);
            self.engine.write_opt(&self.write_batch, &opts).unwrap();
            self.write_batch.clear();
        }
    }

    fn switch_key_metrics(&mut self) {
        if self.versions != 0 {
            MVCC_VERSIONS_HISTOGRAM.observe(self.versions as f64);
            self.total_versions += self.versions;
            self.versions = 0;
        }
        if self.deleted != 0 {
            GC_DELETE_VERSIONS_HISTOGRAM.observe(self.deleted as f64);
            self.total_deleted += self.deleted;
            self.deleted = 0;
        }
    }
}

impl Drop for WriteCompactionFilter {
    fn drop(&mut self) {
        if !self.write_batch.is_empty() {
            let mut opts = WriteOptions::new();
            opts.set_sync(true);
            self.engine.write_opt(&self.write_batch, &opts).unwrap();
            self.write_batch.clear();
        } else {
            self.engine.sync_wal().unwrap();
        }

        self.switch_key_metrics();
        debug!(
            "WriteCompactionFilter has filtered all key/value pairs";
            "bottommost_level" => self.bottommost_level,
            "versions" => self.total_versions,
            "deleted" => self.total_deleted,
        );
    }
}

impl CompactionFilter for WriteCompactionFilter {
    fn filter(
        &mut self,
        _start_level: usize,
        key: &[u8],
        value: &[u8],
        _: &mut Vec<u8>,
        _: &mut bool,
    ) -> bool {
        let (key_prefix, commit_ts) = match Key::split_on_ts_for(key) {
            Ok((key, ts)) => (key, ts.into_inner()),
            // Invalid MVCC keys, don't touch them.
            Err(_) => return false,
        };

        if self.key_prefix != key_prefix {
            self.key_prefix.clear();
            self.key_prefix.extend_from_slice(key_prefix);
            self.remove_older = false;
            self.switch_key_metrics();
        }

        if commit_ts > self.safe_point {
            return false;
        }

        self.versions += 1;
        let mut filtered = self.remove_older;
        let write = WriteRef::parse(value).unwrap();

        if !self.remove_older {
            // here `filtered` must be false.
            match write.write_type {
                WriteType::Rollback | WriteType::Lock => filtered = true,
                WriteType::Put => self.remove_older = true,
                WriteType::Delete => {
                    self.remove_older = true;
                    if self.bottommost_level {
                        filtered = true;
                        self.gc_before_delete_mark(key, key_prefix);
                    }
                }
            }
        }

        if filtered {
            self.deleted += 1;
        }
        filtered
    }
}

pub struct DefaultCompactionFilterFactory;

impl CompactionFilterFactory for DefaultCompactionFilterFactory {
    fn create_compaction_filter(
        &self,
        _context: &CompactionFilterContext,
    ) -> *mut DBCompactionFilter {
        let gc_context_option = GC_CONTEXT.lock().unwrap();
        let gc_context = match *gc_context_option {
            Some(ref ctx) => ctx,
            None => return std::ptr::null_mut(),
        };

        let safe_point = gc_context.safe_point.load(Ordering::Relaxed);
        if safe_point == 0 {
            // Safe point has not been initialized yet.
            return std::ptr::null_mut();
        }

        let name = CString::new("default_compaction_filter").unwrap();
        let db = Arc::clone(&gc_context.db);
        let filter = Box::new(DefaultCompactionFilter::new(db, safe_point));
        unsafe { new_compaction_filter_raw(name, filter) }
    }
}

pub struct DefaultCompactionFilter {
    safe_point: u64,
    engine: RocksEngine,

    key_prefix: Vec<u8>,
    // Valid transactions for `key_prefix`.
    valid_transactions: Vec<u64>,
    is_locked: bool,

    write_iter: RocksEngineIterator,

    // For metrics about (versions, deleted_versions) for every MVCC key.
    versions: usize,
    deleted: usize,
    // Total versions and deleted versions in the compaction.
    total_versions: usize,
    total_deleted: usize,
}

impl DefaultCompactionFilter {
    fn new(db: Arc<DB>, safe_point: u64) -> Self {
        // Safe point must have been initialized.
        assert!(safe_point > 0);
        let engine = RocksEngine::from_db(db);
        let write_iter = {
            // TODO: give lower bound and upper bound to the iterator.
            let opts = IterOptions::default();
            engine.iterator_cf_opt(CF_WRITE, opts).unwrap()
        };

        DefaultCompactionFilter {
            safe_point,
            engine,
            key_prefix: vec![],
            valid_transactions: vec![],
            is_locked: false,
            write_iter,
            versions: 0,
            deleted: 0,
            total_versions: 0,
            total_deleted: 0,
        }
    }

    fn switch_key_metrics(&mut self) {
        if self.versions != 0 {
            self.total_versions += self.versions;
            self.versions = 0;
        }
        if self.deleted != 0 {
            self.total_deleted += self.deleted;
            self.deleted = 0;
        }
    }

    fn key_is_locked(&self, key_prefix: &[u8]) -> bool {
        self.engine
            .get_value_cf(CF_LOCK, key_prefix)
            .unwrap()
            .is_some()
    }
}

impl CompactionFilter for DefaultCompactionFilter {
    fn filter(
        &mut self,
        _start_level: usize,
        key: &[u8],
        _value: &[u8],
        _: &mut Vec<u8>,
        _: &mut bool,
    ) -> bool {
        if key.starts_with(b"zm") {
            // TODO: some metadata is raw key/value pairs instead of MVCC data.
            return false;
        }

        let (key_prefix, start_ts) = match Key::split_on_ts_for(key) {
            Ok((key, ts)) => (key, ts.into_inner()),
            // Invalid MVCC keys, don't touch them.
            Err(_) => return false,
        };

        if self.key_prefix != key_prefix {
            self.key_prefix.clear();
            self.key_prefix.extend_from_slice(key_prefix);
            self.valid_transactions.clear();
            self.is_locked = false;
            self.switch_key_metrics();

            if self.key_is_locked(key_prefix) {
                self.is_locked = true;
            } else {
                let mut valid = self.write_iter.seek(SeekKey::Key(key_prefix)).unwrap();
                while valid {
                    let (key, value) = (self.write_iter.key(), self.write_iter.value());
                    if !key.starts_with(key_prefix) {
                        // All versions in write cf are scaned.
                        break;
                    }
                    let write = WriteRef::parse(value).unwrap();
                    self.valid_transactions.push(write.start_ts.into_inner());
                    valid = self.write_iter.next().unwrap();
                }
                self.valid_transactions.sort();
            }
        }

        self.versions += 1;
        if start_ts > self.safe_point || self.is_locked {
            return false;
        }

        if self.valid_transactions.binary_search(&start_ts).is_err() {
            // The version can be filtered if it's not in valid transactions.
            self.deleted += 1;
            return true;
        }
        false
    }
}

impl Drop for DefaultCompactionFilter {
    fn drop(&mut self) {
        self.switch_key_metrics();
        info!(
            "DefaultCompactionFilter has filtered all key/value pairs";
            "versions" => self.total_versions,
            "deleted" => self.total_deleted,
        );
    }
}

pub fn is_compaction_filter_allowd(cfg_value: &GcConfig, cluster_version: &ClusterVersion) -> bool {
    cfg_value.enable_compaction_filter
        && (cfg_value.compaction_filter_skip_version_check || {
            cluster_version.get().map_or(false, |cluster_version| {
                let minimal = semver::Version::parse(COMPACTION_FILTER_MINIMAL_VERSION).unwrap();
                cluster_version >= minimal
            })
        })
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::config::DbConfig;
    use crate::storage::kv::{RocksEngine as StorageRocksEngine, TestEngineBuilder};
    use crate::storage::mvcc::tests::{
        must_commit, must_get, must_get_none, must_prewrite_delete, must_prewrite_put,
    };
    use engine_rocks::raw::CompactOptions;
    use engine_rocks::util::get_cf_handle;
    use engine_rocks::RocksEngine;
    use engine_traits::{MiscExt, Peekable, SyncMutable, CF_DEFAULT};
    use txn_types::TimeStamp;

    // Use a lock to protect concurrent compactions.
    lazy_static! {
        static ref LOCK: Mutex<()> = std::sync::Mutex::new(());
    }

    fn do_gc_by_compact(
        engine: &RocksEngine,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        safe_point: u64,
        target_level: Option<usize>,
    ) {
        let _guard = LOCK.lock().unwrap();
        let safe_point = Arc::new(AtomicU64::new(safe_point));
        let cfg = GcWorkerConfigManager(Arc::new(Default::default()));
        cfg.0.update(|v| v.enable_compaction_filter = true);
        let cluster_version = ClusterVersion::new(semver::Version::new(5, 0, 0));
        init_compaction_filter(engine.clone(), safe_point, cfg, cluster_version);

        let db = engine.as_inner();
        let handle = get_cf_handle(db, CF_WRITE).unwrap();

        let mut compact_opts = CompactOptions::new();
        compact_opts.set_exclusive_manual_compaction(false);
        compact_opts.set_max_subcompactions(1);
        if let Some(target_level) = target_level {
            compact_opts.set_change_level(true);
            compact_opts.set_target_level(target_level as i32);
        }
        db.compact_range_cf_opt(handle, &compact_opts, start, end);
    }

    fn gc_default_cf(engine: &RocksEngine, safe_point: u64) {
        let _guard = LOCK.lock().unwrap();
        let safe_point = Arc::new(AtomicU64::new(safe_point));
        let cfg = GcWorkerConfigManager(Arc::new(Default::default()));
        cfg.0.update(|v| v.enable_compaction_filter = true);
        let cluster_version = ClusterVersion::new(semver::Version::new(5, 0, 0));
        init_compaction_filter(engine.clone(), safe_point, cfg, cluster_version);

        let db = engine.as_inner();
        let handle = get_cf_handle(db, CF_DEFAULT).unwrap();

        let mut compact_opts = CompactOptions::new();
        compact_opts.set_exclusive_manual_compaction(false);
        compact_opts.set_max_subcompactions(1);
        db.compact_range_cf_opt(handle, &compact_opts, None, None);
    }

    pub fn gc_by_compact(engine: &StorageRocksEngine, _: &[u8], safe_point: u64) {
        let engine = engine.get_rocksdb();
        // Put a new key-value pair to ensure compaction can be triggered correctly.
        engine.put_cf("write", b"k1", b"v1").unwrap();
        do_gc_by_compact(&engine, None, None, safe_point, None);
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

    #[test]
    fn test_is_compaction_filter_allowed() {
        let cluster_version = ClusterVersion::new(semver::Version::new(4, 1, 0));
        let mut cfg_value = GcConfig::default();
        assert!(!is_compaction_filter_allowd(&cfg_value, &cluster_version));

        cfg_value.enable_compaction_filter = true;
        assert!(!is_compaction_filter_allowd(&cfg_value, &cluster_version));

        cfg_value.compaction_filter_skip_version_check = true;
        assert!(is_compaction_filter_allowd(&cfg_value, &cluster_version));

        let cluster_version = ClusterVersion::new(semver::Version::new(5, 0, 0));
        cfg_value.compaction_filter_skip_version_check = false;
        assert!(is_compaction_filter_allowd(&cfg_value, &cluster_version));
    }

    #[test]
    fn test_compaction_filter_on_default() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let raw_engine = engine.get_rocksdb();
        let large_value = vec![b'x'; 1024];
        must_prewrite_put(&engine, b"key", &large_value, b"key", 100);
        must_commit(&engine, b"key", 100, 110);
        must_prewrite_put(&engine, b"key", &large_value, b"key", 120);
        must_commit(&engine, b"key", 120, 130);
        must_prewrite_put(&engine, b"key", &large_value, b"key", 140);
        gc_by_compact(&engine, b"", 200);
        gc_default_cf(&raw_engine, 200);
        must_commit(&engine, b"key", 140, 150);
        must_get_none(&engine, b"key", 110);
        must_get(&engine, b"key", 130, &large_value);
        must_get(&engine, b"key", 150, &large_value);
    }

    // Test a key can be GCed correctly if its MVCC versions cover multiple SST files.
    mod mvcc_versions_cover_multiple_ssts {
        use super::*;

        #[test]
        fn at_bottommost_level() {
            let engine = TestEngineBuilder::new().build().unwrap();
            let raw_engine = engine.get_rocksdb();

            let split_key = Key::from_raw(b"key")
                .append_ts(TimeStamp::from(135))
                .into_encoded();

            // So the construction of SST files will be:
            // L6: |key_110|
            must_prewrite_put(&engine, b"key", b"value", b"key", 100);
            must_commit(&engine, b"key", 100, 110);
            do_gc_by_compact(&raw_engine, None, None, 50, None);
            assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[6], 1);

            // So the construction of SST files will be:
            // L6: |key_140, key_130|, |key_110|
            must_prewrite_put(&engine, b"key", b"value", b"key", 120);
            must_commit(&engine, b"key", 120, 130);
            must_prewrite_delete(&engine, b"key", b"key", 140);
            must_commit(&engine, b"key", 140, 140);
            do_gc_by_compact(&raw_engine, None, Some(&split_key), 50, None);
            assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[6], 2);

            // Put more key/value pairs so that 1 file in L0 and 1 file in L6 can be merged.
            must_prewrite_put(&engine, b"kex", b"value", b"kex", 100);
            must_commit(&engine, b"kex", 100, 110);

            do_gc_by_compact(&raw_engine, None, Some(&split_key), 200, None);

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

        #[test]
        fn at_no_bottommost_level() {
            let mut cfg = DbConfig::default();
            cfg.writecf.dynamic_level_bytes = false;
            let engine = TestEngineBuilder::new().build_with_cfg(&cfg).unwrap();
            let raw_engine = engine.get_rocksdb();

            // So the construction of SST files will be:
            // L6: |AAAAA_101, CCCCC_111|
            must_prewrite_put(&engine, b"AAAAA", b"value", b"key", 100);
            must_commit(&engine, b"AAAAA", 100, 101);
            must_prewrite_put(&engine, b"CCCCC", b"value", b"key", 110);
            must_commit(&engine, b"CCCCC", 110, 111);
            do_gc_by_compact(&raw_engine, None, None, 50, Some(6));
            assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[6], 1);

            // So the construction of SST files will be:
            // L0: |BBBB_101, DDDDD_101|
            // L6: |AAAAA_101, CCCCC_111|
            must_prewrite_put(&engine, b"BBBBB", b"value", b"key", 100);
            must_commit(&engine, b"BBBBB", 100, 101);
            must_prewrite_put(&engine, b"DDDDD", b"value", b"key", 100);
            must_commit(&engine, b"DDDDD", 100, 101);
            raw_engine.flush_cf(CF_WRITE, true).unwrap();
            assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[0], 1);

            // So the construction of SST files will be:
            // L0: |AAAAA_111, BBBBB_111|, |BBBB_101, DDDDD_101|
            // L6: |AAAAA_101, CCCCC_111|
            must_prewrite_put(&engine, b"AAAAA", b"value", b"key", 110);
            must_commit(&engine, b"AAAAA", 110, 111);
            must_prewrite_delete(&engine, b"BBBBB", b"BBBBB", 110);
            must_commit(&engine, b"BBBBB", 110, 111);
            raw_engine.flush_cf(CF_WRITE, true).unwrap();
            assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[0], 2);

            // Compact |AAAAA_111, BBBBB_111| at L0 and |AAAA_101, CCCCC_111| at L6.
            let start = Key::from_raw(b"AAAAA").into_encoded();
            let end = Key::from_raw(b"AAAAAA").into_encoded();
            do_gc_by_compact(&raw_engine, Some(&start), Some(&end), 200, Some(6));

            must_get_none(&engine, b"BBBBB", 101);
        }
    }
}
