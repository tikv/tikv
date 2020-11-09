// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::Cell;
use std::cmp::Ordering as CmpOrdering;
use std::ffi::CString;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use super::{GcConfig, GcWorkerConfigManager};
use crate::storage::mvcc::{check_need_gc, GC_DELETE_VERSIONS_HISTOGRAM, MVCC_VERSIONS_HISTOGRAM};
use engine_rocks::raw::{
    new_compaction_filter_raw, CompactionFilter, CompactionFilterContext, CompactionFilterFactory,
    DBCompactionFilter, DB,
};
use engine_rocks::{
    RocksEngine, RocksEngineIterator, RocksMvccProperties, RocksUserCollectedPropertiesNoRc,
    RocksWriteBatch,
};
use engine_traits::{
    IterOptions, Iterable, Iterator, MiscExt, Mutable, MvccProperties, SeekKey, WriteBatchExt,
    WriteOptions, CF_WRITE,
};
use pd_client::ClusterVersion;
use txn_types::{Key, WriteRef, WriteType};

const DEFAULT_DELETE_BATCH_SIZE: usize = 256 * 1024;
const DEFAULT_DELETE_BATCH_COUNT: usize = 128;
const NEAR_SEEK_LIMIT: usize = 16;

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

pub trait CompactionFilterInitializer {
    fn init_compaction_filter(
        &self,
        safe_point: Arc<AtomicU64>,
        cfg_tracker: GcWorkerConfigManager,
        cluster_version: ClusterVersion,
    );
}

impl<T> CompactionFilterInitializer for T {
    default fn init_compaction_filter(
        &self,
        _safe_point: Arc<AtomicU64>,
        _cfg_tracker: GcWorkerConfigManager,
        _cluster_version: ClusterVersion,
    ) {
    }
}

impl CompactionFilterInitializer for RocksEngine {
    fn init_compaction_filter(
        &self,
        safe_point: Arc<AtomicU64>,
        cfg_tracker: GcWorkerConfigManager,
        cluster_version: ClusterVersion,
    ) {
        info!("initialize GC context for compaction filter");
        let mut gc_context = GC_CONTEXT.lock().unwrap();
        *gc_context = Some(GcContext {
            db: self.as_inner().clone(),
            safe_point,
            cfg_tracker,
            cluster_version,
        });
    }
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
            debug!("skip gc in compaction filter because of no safe point");
            // Safe point has not been initialized yet.
            return std::ptr::null_mut();
        }

        if !is_compaction_filter_allowd(
            &*gc_context.cfg_tracker.value(),
            &gc_context.cluster_version,
        ) {
            debug!("skip gc in compaction filter because it's not allowed");
            return std::ptr::null_mut();
        }

        let mut mvcc_props = MvccProperties::new();
        for i in 0..context.file_numbers().len() {
            let table_props = context.table_properties(i);
            let user_props = unsafe {
                &*(table_props.user_collected_properties() as *const _
                    as *const RocksUserCollectedPropertiesNoRc)
            };
            if let Ok(props) = RocksMvccProperties::decode(user_props) {
                mvcc_props.add(&props);
            }
        }

        let ratio_threshold = gc_context.cfg_tracker.value().ratio_threshold;
        if !check_need_gc(safe_point.into(), ratio_threshold, mvcc_props) {
            debug!("skip gc in compaction filter because it's not necessary");
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
    deleting: Option<Vec<Vec<u8>>>,

    // To handle delete marks.
    write_iter: Option<RocksEngineIterator>,
    near_seek_distance: usize,

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
        debug!("gc in compaction filter"; "files" => ?context.file_numbers());

        let engine = RocksEngine::from_db(db.clone());
        let write_batch = RocksWriteBatch::with_capacity(db, DEFAULT_DELETE_BATCH_SIZE);
        let write_iter = {
            // TODO: give lower bound and upper bound to the iterator.
            let opts = IterOptions::default();
            Some(engine.iterator_cf_opt(CF_WRITE, opts).unwrap())
        };

        WriteCompactionFilter {
            safe_point,
            engine,
            write_batch,
            key_prefix: vec![],
            remove_older: false,
            deleting: None,
            write_iter,
            near_seek_distance: NEAR_SEEK_LIMIT,
            versions: 0,
            deleted: 0,
            total_versions: 0,
            total_deleted: 0,
        }
    }

    // Do gc before a delete mark. Must be called before switching `key_prefix`.
    fn handle_deleting(&mut self) {
        let mut deleting = match self.deleting.take() {
            Some(v) => v,
            None => return,
        };

        let mut valid = self.write_iter_seek_to(deleting[0].as_slice());
        let mut write_iter = self.write_iter.take().unwrap();
        while valid {
            let (key, value) = (write_iter.key(), write_iter.value());
            if truncate_ts(key) != self.key_prefix.as_slice() {
                break;
            }

            let (mut drain_pos, mut found) = (0, false);
            while drain_pos < deleting.len() {
                match write_iter.key().cmp(deleting[drain_pos].as_slice()) {
                    CmpOrdering::Equal => found = true,
                    CmpOrdering::Less => break,
                    _ => {}
                }
                drain_pos += 1;
            }
            deleting.drain(0..drain_pos);
            if !found {
                self.handle_filtered_write(parse_write(value));
                self.delete_write_key(key);
            }

            valid = write_iter.next().unwrap();
        }
        self.write_iter = Some(write_iter);
    }

    fn write_iter_seek_to(&mut self, mark: &[u8]) -> bool {
        let write_iter = self.write_iter.as_mut().unwrap();
        if self.near_seek_distance >= NEAR_SEEK_LIMIT {
            self.near_seek_distance = 0;
            return write_iter.seek(SeekKey::Key(mark)).unwrap();
        }

        let mut valid = write_iter.valid().unwrap();
        let (mut next_count, mut found) = (0, false);
        while valid && next_count <= NEAR_SEEK_LIMIT {
            if write_iter.key() >= mark {
                found = true;
                break;
            }
            valid = write_iter.next().unwrap();
            next_count += 1;
        }

        if valid && !found {
            // Fallback to `seek` after some `next`s.
            valid = write_iter.seek(SeekKey::Key(mark)).unwrap();
        }
        self.near_seek_distance = 0;
        valid
    }

    fn handle_filtered_write(&mut self, write: WriteRef) {
        if write.short_value.is_none() && write.write_type == WriteType::Put {
            let prefix = Key::from_encoded_slice(&self.key_prefix);
            let def_key = prefix.append_ts(write.start_ts).into_encoded();
            self.write_batch.delete(&def_key).unwrap();
            self.flush_pending_writes_if_need();
        }
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

thread_local! {
    static VERSIONS_AND_DELETES: Cell<(usize, usize)> = Cell::new((0, 0));
}

impl Drop for WriteCompactionFilter {
    fn drop(&mut self) {
        self.handle_deleting();

        if !self.write_batch.is_empty() {
            let mut opts = WriteOptions::new();
            opts.set_sync(true);
            self.engine.write_opt(&self.write_batch, &opts).unwrap();
            self.write_batch.clear();
        } else {
            self.engine.sync_wal().unwrap();
        }

        self.switch_key_metrics();
        VERSIONS_AND_DELETES.with(|x| {
            x.update(|(mut versions, mut deletes)| {
                versions += self.total_versions;
                deletes += self.total_deleted;
                if versions >= 1024 * 1024 {
                    info!("Compaction filter reports"; "total" => versions, "gc" => deletes);
                    return (0, 0);
                }
                (versions, deletes)
            })
        });
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
        self.near_seek_distance += 1;
        let (key_prefix, commit_ts) = split_ts(key);
        if commit_ts > self.safe_point {
            return false;
        }

        self.versions += 1;
        if self.key_prefix != key_prefix {
            self.handle_deleting();
            self.key_prefix.clear();
            self.key_prefix.extend_from_slice(key_prefix);
            self.remove_older = false;
            self.switch_key_metrics();
        }

        let mut filtered = self.remove_older;
        let write = parse_write(value);
        if !self.remove_older {
            match write.write_type {
                WriteType::Rollback | WriteType::Lock => filtered = true,
                WriteType::Put => self.remove_older = true,
                WriteType::Delete => {
                    self.remove_older = true;
                    filtered = true;
                    self.deleting = Some(Vec::with_capacity(16));
                }
            }
        }

        if filtered {
            self.handle_filtered_write(write);
            if let Some(deleting) = self.deleting.as_mut() {
                deleting.push(key.to_vec());
            }
            self.deleted += 1;
        }

        filtered
    }
}

fn split_ts(key: &[u8]) -> (&[u8], u64) {
    match Key::split_on_ts_for(key) {
        Ok((key, ts)) => (key, ts.into_inner()),
        Err(_) => panic!("invalid write cf key: {}", hex::encode_upper(key)),
    }
}

fn truncate_ts(key: &[u8]) -> &[u8] {
    match Key::truncate_ts_for(key) {
        Ok(prefix) => prefix,
        Err(_) => panic!("invalid write cf key: {}", hex::encode_upper(key)),
    }
}

fn parse_write(value: &[u8]) -> WriteRef {
    match WriteRef::parse(value) {
        Ok(write) => write,
        Err(_) => panic!("invalid write cf value: {}", hex::encode_upper(value)),
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
    use crate::storage::mvcc::tests::{must_get, must_get_none};
    use crate::storage::txn::tests::{must_commit, must_prewrite_delete, must_prewrite_put};
    use engine_rocks::raw::CompactOptions;
    use engine_rocks::util::get_cf_handle;
    use engine_rocks::RocksEngine;
    use engine_traits::{MiscExt, Peekable, SyncMutable};
    use txn_types::TimeStamp;

    // Use a lock to protect concurrent compactions.
    lazy_static! {
        static ref LOCK: Mutex<()> = std::sync::Mutex::new(());
    }

    fn compact_options() -> CompactOptions {
        let mut compact_opts = CompactOptions::new();
        compact_opts.set_exclusive_manual_compaction(false);
        compact_opts.set_max_subcompactions(1);
        compact_opts
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
        engine.init_compaction_filter(safe_point, cfg, cluster_version);

        let db = engine.as_inner();
        let handle = get_cf_handle(db, CF_WRITE).unwrap();
        let mut compact_opts = compact_options();
        if let Some(target_level) = target_level {
            compact_opts.set_change_level(true);
            compact_opts.set_target_level(target_level as i32);
        }
        db.compact_range_cf_opt(handle, &compact_opts, start, end);
    }

    fn do_gc_by_compact_with_ratio_threshold(
        engine: &RocksEngine,
        safe_point: u64,
        ratio_threshold: f64,
    ) {
        let _guard = LOCK.lock().unwrap();

        let safe_point = Arc::new(AtomicU64::new(safe_point));
        let cfg = GcWorkerConfigManager(Arc::new(Default::default()));
        cfg.0.update(|v| {
            v.enable_compaction_filter = true;
            v.ratio_threshold = ratio_threshold;
        });
        let cluster_version = ClusterVersion::new(semver::Version::new(5, 0, 0));
        engine.init_compaction_filter(safe_point, cfg, cluster_version);

        let db = engine.as_inner();
        let handle = get_cf_handle(db, CF_WRITE).unwrap();
        db.compact_range_cf_opt(handle, &compact_options(), None, None);
    }

    pub fn gc_by_compact(engine: &StorageRocksEngine, _: &[u8], safe_point: u64) {
        let engine = engine.get_rocksdb();
        // Put a new key-value pair to ensure compaction can be triggered correctly.
        engine.delete_cf("write", b"not-exists-key").unwrap();
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
    fn test_compaction_filter_basic() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let raw_engine = engine.get_rocksdb();
        let value = vec![b'v'; 512];

        // GC can't delete keys after the given safe point.
        must_prewrite_put(&engine, b"key", &value, b"key", 100);
        must_commit(&engine, b"key", 100, 110);
        do_gc_by_compact(&raw_engine, None, None, 50, None);
        must_get(&engine, b"key", 110, &value);

        // GC can't delete keys before the safe ponit if they are latest versions.
        do_gc_by_compact(&raw_engine, None, None, 200, None);
        must_get(&engine, b"key", 110, &value);

        must_prewrite_put(&engine, b"key", &value, b"key", 120);
        must_commit(&engine, b"key", 120, 130);

        // GC can't delete the latest version before the safe ponit.
        do_gc_by_compact(&raw_engine, None, None, 115, None);
        must_get(&engine, b"key", 110, &value);

        // GC a version will also delete the key on default CF.
        do_gc_by_compact(&raw_engine, None, None, 200, None);
        must_get_none(&engine, b"key", 110);
        let default_key = Key::from_encoded_slice(b"key").append_ts(100.into());
        let default_key = default_key.into_encoded();
        assert!(raw_engine.get_value(&default_key).unwrap().is_none());
    }

    #[test]
    fn test_compaction_filter_handle_deleting() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let raw_engine = engine.get_rocksdb();
        let value = vec![b'v'; 512];

        // Delete mark and masked versions can be handled in `drop`.
        must_prewrite_put(&engine, b"key", &value, b"key", 100);
        must_commit(&engine, b"key", 100, 110);
        must_prewrite_delete(&engine, b"key", b"key", 120);
        must_commit(&engine, b"key", 120, 130);
        do_gc_by_compact(&raw_engine, None, None, 200, None);
        must_get_none(&engine, b"key", 110);

        must_prewrite_put(&engine, b"key", &value, b"key", 100);
        must_commit(&engine, b"key", 100, 110);
        must_prewrite_delete(&engine, b"key", b"key", 120);
        must_commit(&engine, b"key", 120, 130);
        must_prewrite_put(&engine, b"key1", &value, b"key1", 120);
        must_commit(&engine, b"key1", 120, 130);
        do_gc_by_compact(&raw_engine, None, None, 200, None);
        must_get_none(&engine, b"key", 110);
    }

    #[test]
    fn test_mvcc_properties() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let raw_engine = engine.get_rocksdb();
        let value = vec![b'v'; 512];

        for start_ts in &[100, 110, 120, 130] {
            must_prewrite_put(&engine, b"key", &value, b"key", *start_ts);
            must_commit(&engine, b"key", *start_ts, *start_ts + 5);
        }
        must_prewrite_delete(&engine, b"key", b"key", 140);
        must_commit(&engine, b"key", 140, 145);

        // Can't GC stale versions because of the threshold.
        do_gc_by_compact_with_ratio_threshold(&raw_engine, 200, 10.0);
        for commit_ts in &[105, 115, 125, 135] {
            must_get(&engine, b"key", commit_ts, &value);
        }
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
