// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::Cell;
use std::cmp::Ordering as CmpOrdering;
use std::ffi::CString;
use std::result::Result;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use engine_rocks::raw::{
    new_compaction_filter_raw, CompactionFilter, CompactionFilterContext, CompactionFilterDecision,
    CompactionFilterFactory, CompactionFilterValueType, DBCompactionFilter, DBIterator,
    ReadOptions, DB,
};
use engine_rocks::{
    RocksEngine, RocksEngineIterator, RocksMvccProperties, RocksUserCollectedPropertiesNoRc,
    RocksWriteBatch,
};
use engine_traits::{Iterator, MiscExt, Mutable, MvccProperties, SeekKey, WriteBatchExt, CF_WRITE};
use pd_client::ClusterVersion;
use prometheus::{local::*, *};
use txn_types::{Key, TimeStamp, WriteRef, WriteType};

use super::{GcConfig, GcWorkerConfigManager};
use crate::storage::mvcc::{GC_DELETE_VERSIONS_HISTOGRAM, MVCC_VERSIONS_HISTOGRAM};

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
    static ref GC_COMPACTION_FILTERED: IntCounter = register_int_counter!(
        "tikv_gc_compaction_filtered",
        "Filtered versions by compaction"
    )
    .unwrap();
    static ref GC_COMPACTION_DELETED: IntCounter = register_int_counter!(
        "tikv_gc_compaction_deleted",
        "Deleted versions by compaction"
    )
    .unwrap();
    static ref GC_COMPACTION_FAILURE: IntCounter = register_int_counter!(
        "tikv_gc_compaction_failure",
        "Compaction filter meets failure"
    )
    .unwrap();
    static ref GC_COMPACTION_FILTER_SKIP: IntCounter = register_int_counter!(
        "tikv_gc_compaction_filter_skip",
        "Skip to create compaction filter for GC because of table properties"
    )
    .unwrap();

    // It's relative to a key logic for handling mvcc delete marks.
    static ref GC_COMPACTION_MVCC_DELETE_SKIP_OLDER: IntCounter = register_int_counter!(
        "tikv_gc_compaction_mvcc_delete_skip_older",
        "Counter of skiped versions for bottommost deletes"
    )
    .unwrap();

    // `WriteType::Rollback` and `WriteType::Lock` are handled in different ways.
    static ref GC_COMPACTION_MVCC_ROLLBACK: IntCounter = register_int_counter!(
        "tikv_gc_compaction_mvcc_rollback",
        "Compaction of mvcc rollbacks"
    )
    .unwrap();
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

        let (enable, skip_vcheck, ratio_threshold) = {
            let value = &*gc_context.cfg_tracker.value();
            (
                value.enable_compaction_filter,
                value.compaction_filter_skip_version_check,
                value.ratio_threshold,
            )
        };
        debug!(
            "creating compaction filter"; "feature_enable" => enable,
            "skip_version_check" => skip_vcheck,
            "ratio_threshold" => ratio_threshold,
        );

        if !do_check_allowed(enable, skip_vcheck, &gc_context.cluster_version) {
            debug!("skip gc in compaction filter because it's not allowed");
            return std::ptr::null_mut();
        }

        if !check_need_gc(safe_point.into(), ratio_threshold, context) {
            debug!("skip gc in compaction filter because it's not necessary");
            GC_COMPACTION_FILTER_SKIP.inc();
            return std::ptr::null_mut();
        }

        let name = CString::new("write_compaction_filter").unwrap();
        let db = Arc::clone(&gc_context.db);
        debug!(
            "gc in compaction filter"; "safe_point" => safe_point,
            "files" => ?context.file_numbers(),
            "bottommost" => context.is_bottommost_level(),
            "manual" => context.is_manual_compaction(),
        );
        let filter = Box::new(WriteCompactionFilter::new(db, safe_point, context));
        unsafe { new_compaction_filter_raw(name, filter) }
    }
}

struct WriteCompactionFilter {
    safe_point: u64,
    engine: RocksEngine,
    write_batch: RocksWriteBatch,
    encountered_errors: bool,

    key_prefix: Vec<u8>,
    remove_older: bool,

    // For handling MVCC delete marks at the bottommost level.
    is_bottommost_level: bool,
    // Layout: (delete_mark, vec![(ts, sequence), ...]).
    #[allow(clippy::type_complexity)]
    deleting_filtered: Option<(Vec<u8>, Vec<(u64, u64)>)>,
    near_seek_distance: usize,
    write_iter: Option<RocksEngineIterator>,

    versions: usize,
    filtered: usize,
    deleted: usize,
    total_versions: usize,
    total_filtered: usize,
    total_deleted: usize,
    versions_hist: LocalHistogram,
    filtered_hist: LocalHistogram,

    // Some metrics about implementation detail.
    mvcc_delete_skip_older: usize,
    mvcc_rollback_and_locks: usize,
}

impl WriteCompactionFilter {
    fn new(db: Arc<DB>, safe_point: u64, context: &CompactionFilterContext) -> Self {
        // Safe point must have been initialized.
        assert!(safe_point > 0);
        debug!("gc in compaction filter"; "safe_point" => safe_point);

        let mut filter = WriteCompactionFilter {
            safe_point,
            engine: RocksEngine::from_db(db.clone()),
            write_batch: RocksWriteBatch::with_capacity(db, DEFAULT_DELETE_BATCH_SIZE),
            encountered_errors: false,

            key_prefix: vec![],
            remove_older: false,

            is_bottommost_level: context.is_bottommost_level(),
            deleting_filtered: None,
            near_seek_distance: NEAR_SEEK_LIMIT,
            write_iter: None,

            versions: 0,
            filtered: 0,
            deleted: 0,
            total_versions: 0,
            total_filtered: 0,
            total_deleted: 0,
            versions_hist: MVCC_VERSIONS_HISTOGRAM.local(),
            filtered_hist: GC_DELETE_VERSIONS_HISTOGRAM.local(),
            mvcc_delete_skip_older: 0,
            mvcc_rollback_and_locks: 0,
        };

        if filter.is_bottommost_level {
            let db = filter.engine.as_inner();
            let cf = db.cf_handle(CF_WRITE).unwrap();
            let iter = DBIterator::new_cf(db.clone(), cf, ReadOptions::new());
            filter.write_iter = Some(RocksEngineIterator::from_raw(iter));
        }

        filter
    }

    fn do_filter(
        &mut self,
        _start_level: usize,
        key: &[u8],
        sequence: u64,
        value: &[u8],
        value_type: CompactionFilterValueType,
    ) -> Result<CompactionFilterDecision, String> {
        self.meet_a_key_in_filter();

        let (key_prefix, commit_ts) = split_ts(key)?;
        if commit_ts > self.safe_point || value_type != CompactionFilterValueType::Value {
            return Ok(CompactionFilterDecision::Keep);
        }

        self.versions += 1;
        if self.key_prefix != key_prefix {
            self.handle_delete_mark()?;
            self.switch_key_metrics();
            self.key_prefix.clear();
            self.key_prefix.extend_from_slice(key_prefix);
            self.remove_older = false;
        }

        let mut filtered = self.remove_older;
        let write = parse_write(value)?;
        if !self.remove_older {
            match write.write_type {
                WriteType::Rollback | WriteType::Lock => {
                    self.mvcc_rollback_and_locks += 1;
                    filtered = true;
                }
                WriteType::Put => self.remove_older = true,
                WriteType::Delete => {
                    self.remove_older = true;
                    if self.is_bottommost_level {
                        filtered = true;
                        self.deleting_filtered = Some((key.to_vec(), Vec::new()));
                    }
                }
            }
        } else if let Some(deleting_filtered) = self.deleting_filtered.as_mut() {
            deleting_filtered.1.push((commit_ts, sequence));
        }

        let decision = match (filtered, self.remove_older) {
            (true, true) => {
                self.handle_filtered_write(write)?;
                self.filtered += 1;
                let prefix = Key::from_encoded_slice(key_prefix);
                let skip_until = prefix.append_ts(0.into()).into_encoded();
                CompactionFilterDecision::RemoveAndSkipUntil(skip_until)
            }
            (true, false) => {
                self.filtered += 1;
                CompactionFilterDecision::Remove
            }
            (false, _) => CompactionFilterDecision::Keep,
        };
        Ok(decision)
    }

    fn meet_a_key_in_filter(&mut self) {
        if self.is_bottommost_level && self.deleting_filtered.is_none() {
            // The compaction hasn't met the next MVCC delete mark.
            self.near_seek_distance += 1;
        }
    }

    // Do gc before a delete mark.
    fn handle_delete_mark(&mut self) -> Result<(), String> {
        let (mark, mut filtered) = match self.deleting_filtered.take() {
            Some((x, y)) => (x, y.into_iter().peekable()),
            None => return Ok(()),
        };
        debug_assert!(self.is_bottommost_level);

        let mut valid = self.write_iter_seek_to(&mark)?;
        let mut write_iter = self.write_iter.take().unwrap();
        if valid && write_iter.key() == mark {
            // The delete mark itself is handled in `filter`.
            valid = write_iter.next()?;
        }

        while valid {
            let (key, value) = (write_iter.key(), write_iter.value());
            if truncate_ts(key)? != self.key_prefix.as_slice() {
                break;
            }
            let (_, commit_ts) = split_ts(key)?;
            let mut need_delete = true;
            while let Some((ts, s)) = filtered.peek() {
                match commit_ts.cmp(ts) {
                    CmpOrdering::Less => break,
                    CmpOrdering::Equal => {
                        let seq = write_iter.as_raw().sequence().unwrap();
                        assert!(seq >= *s);
                        // NOTE: in the bottommost level sequence could be 0. So it's required that
                        // an ingested file's sequence is not 0 if it overlaps with the DB.
                        need_delete = seq > *s;
                        if !need_delete {
                            self.mvcc_delete_skip_older += 1;
                        }
                        let _ = filtered.next().unwrap();
                        break;
                    }
                    CmpOrdering::Greater => {
                        let _ = filtered.next().unwrap();
                    }
                }
            }
            if need_delete {
                self.delete_write_key(key)?;
                self.handle_filtered_write(parse_write(value)?)?;
            }
            valid = write_iter.next()?;
        }
        self.write_iter = Some(write_iter);
        Ok(())
    }

    fn write_iter_seek_to(&mut self, mark: &[u8]) -> Result<bool, String> {
        let write_iter = self.write_iter.as_mut().unwrap();
        if self.near_seek_distance >= NEAR_SEEK_LIMIT {
            self.near_seek_distance = 0;
            let valid = write_iter.seek(SeekKey::Key(mark))?;
            return Ok(valid);
        }

        let mut valid = write_iter.valid()?;
        let (mut next_count, mut found) = (0, false);
        while valid && next_count <= NEAR_SEEK_LIMIT {
            if write_iter.key() >= mark {
                found = true;
                break;
            }
            valid = write_iter.next()?;
            next_count += 1;
        }

        if valid && !found {
            // Fallback to `seek` after some `next`s.
            valid = write_iter.seek(SeekKey::Key(mark))?;
        }
        self.near_seek_distance = 0;
        Ok(valid)
    }

    fn handle_filtered_write(&mut self, write: WriteRef) -> Result<(), String> {
        if write.short_value.is_none() && write.write_type == WriteType::Put {
            let prefix = Key::from_encoded_slice(&self.key_prefix);
            let def_key = prefix.append_ts(write.start_ts).into_encoded();
            self.write_batch.delete(&def_key)?;
            self.flush_pending_writes_if_need()?;
        }
        Ok(())
    }

    fn delete_write_key(&mut self, key: &[u8]) -> Result<(), String> {
        self.write_batch.delete_cf(CF_WRITE, key)?;
        self.flush_pending_writes_if_need()?;
        self.deleted += 1;
        Ok(())
    }

    fn flush_pending_writes_if_need(&mut self) -> Result<(), String> {
        if self.write_batch.count() > DEFAULT_DELETE_BATCH_COUNT {
            self.engine.write(&self.write_batch)?;
            self.write_batch.clear();
        }
        Ok(())
    }

    fn switch_key_metrics(&mut self) {
        if self.versions != 0 {
            self.versions_hist.observe(self.versions as f64);
            self.total_versions += self.versions;
            self.versions = 0;
        }
        if self.filtered != 0 {
            self.filtered_hist.observe(self.filtered as f64);
            self.total_filtered += self.filtered;
            self.filtered = 0;
        }
        self.total_deleted += self.deleted;
        self.deleted = 0;
    }
}

struct CompactionFilterStats {
    versions: Cell<usize>, // Total stale versions meet by compaction filters.
    filtered: Cell<usize>, // Filtered versions by compaction filters.
    deleted: Cell<usize>,  // Deleted versions from RocksDB.
    last_report: Cell<Instant>,
}
impl CompactionFilterStats {
    fn need_report(&self) -> bool {
        self.versions.get() >= 1024 * 1024 // 1M versions.
            || self.last_report.get().elapsed() >= Duration::from_secs(60)
    }

    fn prepare_report(&self) -> (usize, usize, usize) {
        let versions = self.versions.replace(0);
        let filtered = self.filtered.replace(0);
        let deleted = self.deleted.replace(0);
        self.last_report.set(Instant::now());
        (versions, filtered, deleted)
    }
}
impl Default for CompactionFilterStats {
    fn default() -> Self {
        CompactionFilterStats {
            versions: Cell::new(0),
            filtered: Cell::new(0),
            deleted: Cell::new(0),
            last_report: Cell::new(Instant::now()),
        }
    }
}

thread_local! {
    static STATS: CompactionFilterStats = CompactionFilterStats::default();
}

impl Drop for WriteCompactionFilter {
    fn drop(&mut self) {
        self.handle_delete_mark().unwrap();
        self.switch_key_metrics();
        if !self.write_batch.is_empty() {
            self.engine.write(&self.write_batch).unwrap();
            self.write_batch.clear();
        }
        self.engine.sync_wal().unwrap();

        GC_COMPACTION_FILTERED.inc_by(self.total_filtered as i64);
        GC_COMPACTION_DELETED.inc_by(self.total_deleted as i64);
        GC_COMPACTION_MVCC_DELETE_SKIP_OLDER.inc_by(self.mvcc_delete_skip_older as i64);
        GC_COMPACTION_MVCC_ROLLBACK.inc_by(self.mvcc_rollback_and_locks as i64);
        if let Some((versions, filtered, deleted)) = STATS.with(|stats| {
            stats.versions.update(|x| x + self.total_versions);
            stats.filtered.update(|x| x + self.total_filtered);
            stats.deleted.update(|x| x + self.total_deleted);
            if stats.need_report() {
                return Some(stats.prepare_report());
            }
            None
        }) {
            info!(
                "Compaction filter reports"; "total" => versions,
                "filtered" => filtered, "deleted" => deleted,
            );
        }
    }
}

impl CompactionFilter for WriteCompactionFilter {
    fn filter_v2(
        &mut self,
        level: usize,
        key: &[u8],
        sequence: u64,
        value: &[u8],
        value_type: CompactionFilterValueType,
    ) -> CompactionFilterDecision {
        if self.encountered_errors {
            // If there are already some errors, do nothing.
            return CompactionFilterDecision::Keep;
        }

        match self.do_filter(level, key, sequence, value, value_type) {
            Ok(decision) => decision,
            Err(e) => {
                warn!("compaction filter meet error: {}", e);
                GC_COMPACTION_FAILURE.inc();
                self.encountered_errors = true;
                CompactionFilterDecision::Keep
            }
        }
    }
}

fn split_ts(key: &[u8]) -> Result<(&[u8], u64), String> {
    match Key::split_on_ts_for(key) {
        Ok((key, ts)) => Ok((key, ts.into_inner())),
        Err(_) => Err(format!("invalid write cf key: {}", hex::encode_upper(key))),
    }
}

fn truncate_ts(key: &[u8]) -> Result<&[u8], String> {
    match Key::truncate_ts_for(key) {
        Ok(prefix) => Ok(prefix),
        Err(_) => Err(format!("invalid write cf key: {}", hex::encode_upper(key))),
    }
}

fn parse_write(value: &[u8]) -> Result<WriteRef, String> {
    match WriteRef::parse(value) {
        Ok(write) => Ok(write),
        Err(_) => Err(format!(
            "invalid write cf value: {}",
            hex::encode_upper(value)
        )),
    }
}

pub fn is_compaction_filter_allowd(cfg_value: &GcConfig, cluster_version: &ClusterVersion) -> bool {
    do_check_allowed(
        cfg_value.enable_compaction_filter,
        cfg_value.compaction_filter_skip_version_check,
        cluster_version,
    )
}

fn do_check_allowed(enable: bool, skip_vcheck: bool, cluster_version: &ClusterVersion) -> bool {
    enable
        && (skip_vcheck || {
            cluster_version.get().map_or(false, |cluster_version| {
                let minimal = semver::Version::parse(COMPACTION_FILTER_MINIMAL_VERSION).unwrap();
                cluster_version >= minimal
            })
        })
}

fn check_need_gc(
    safe_point: TimeStamp,
    mut ratio_threshold: f64,
    context: &CompactionFilterContext,
) -> bool {
    if ratio_threshold < 1.0 {
        return true;
    }

    let is_bottommost = context.is_bottommost_level();
    let mut check_props = |props: &MvccProperties| {
        if props.min_ts > safe_point {
            return false;
        }
        let num_versions = if is_bottommost {
            props.num_versions as f64
        } else {
            if ratio_threshold < 1.5 {
                ratio_threshold = 1.5;
            }
            (props.num_versions - props.num_deletes) as f64
        };
        if num_versions > props.num_rows as f64 * ratio_threshold {
            return true;
        }
        if num_versions > props.num_puts as f64 * ratio_threshold {
            return true;
        }
        props.max_row_versions > 1024
    };

    let (mut sum_props, mut needs_gc) = (MvccProperties::new(), 0);
    for i in 0..context.file_numbers().len() {
        let table_props = context.table_properties(i);
        let user_props = unsafe {
            &*(table_props.user_collected_properties() as *const _
                as *const RocksUserCollectedPropertiesNoRc)
        };
        if let Ok(props) = RocksMvccProperties::decode(user_props) {
            sum_props.add(&props);
            if check_props(&props) {
                needs_gc += 1;
            }
        }
    }

    needs_gc >= (context.file_numbers().len() + 1) >> 1 || check_props(&sum_props)
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::config::DbConfig;
    use crate::storage::kv::{RocksEngine as StorageRocksEngine, TestEngineBuilder};
    use crate::storage::mvcc::tests::{must_get, must_get_none};
    use crate::storage::txn::tests::{must_commit, must_prewrite_delete, must_prewrite_put};
    use engine_rocks::raw::{CompactOptions, CompactionOptions};
    use engine_rocks::util::get_cf_handle;
    use engine_rocks::{RocksEngine, RocksIngestExternalFileOptions, RocksSstWriterBuilder};
    use engine_traits::{
        ImportExt, IngestExternalFileOptions, IterOptions, Iterable, MiscExt, Peekable, SstWriter,
        SstWriterBuilder, SyncMutable,
    };

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
        engine.delete_cf("write", b"znot-exists-key").unwrap();
        do_gc_by_compact(&engine, None, None, safe_point, None);
    }

    fn compact_files(engine: &RocksEngine, cf: &str, input_files: &[String], output_level: i32) {
        let db = engine.as_inner();
        let handle = db.cf_handle(cf).unwrap();
        db.compact_files_cf(handle, &CompactionOptions::new(), input_files, output_level)
            .unwrap();
    }

    fn rocksdb_level_files(engine: &RocksEngine, cf: &str) -> Vec<Vec<String>> {
        let cf_handle = get_cf_handle(engine.as_inner(), cf).unwrap();
        let metadata = engine.as_inner().get_column_family_meta_data(cf_handle);
        let mut res = Vec::with_capacity(7);
        for level_meta in metadata.get_levels() {
            let mut level = Vec::new();
            for meta in level_meta.get_files() {
                level.push(meta.get_name());
            }
            res.push(level);
        }
        res
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

    // Test compaction filter won't break basic GC rules.
    #[test]
    fn test_compaction_filter_basic() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let raw_engine = engine.get_rocksdb();
        let value = vec![b'v'; 512];

        // GC can't delete keys after the given safe point.
        must_prewrite_put(&engine, b"zkey", &value, b"zkey", 100);
        must_commit(&engine, b"zkey", 100, 110);
        do_gc_by_compact(&raw_engine, None, None, 50, None);
        must_get(&engine, b"zkey", 110, &value);

        // GC can't delete keys before the safe ponit if they are latest versions.
        do_gc_by_compact(&raw_engine, None, None, 200, None);
        must_get(&engine, b"zkey", 110, &value);

        must_prewrite_put(&engine, b"zkey", &value, b"zkey", 120);
        must_commit(&engine, b"zkey", 120, 130);

        // GC can't delete the latest version before the safe ponit.
        do_gc_by_compact(&raw_engine, None, None, 115, None);
        must_get(&engine, b"zkey", 110, &value);

        // GC a version will also delete the key on default CF.
        do_gc_by_compact(&raw_engine, None, None, 200, None);
        must_get_none(&engine, b"zkey", 110);
        let default_key = Key::from_encoded_slice(b"zkey").append_ts(100.into());
        let default_key = default_key.into_encoded();
        assert!(raw_engine.get_value(&default_key).unwrap().is_none());
    }

    // Test a delete mark can be handled with all older dirty versions correctly.
    #[test]
    fn test_compaction_filter_handle_deleting() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let raw_engine = engine.get_rocksdb();
        let value = vec![b'v'; 512];

        // Delete mark and masked versions can be handled in `drop`.
        must_prewrite_put(&engine, b"zkey", &value, b"zkey", 100);
        must_commit(&engine, b"zkey", 100, 110);
        must_prewrite_delete(&engine, b"zkey", b"zkey", 120);
        must_commit(&engine, b"zkey", 120, 130);
        do_gc_by_compact(&raw_engine, None, None, 200, None);
        must_get_none(&engine, b"zkey", 110);

        must_prewrite_put(&engine, b"zkey", &value, b"zkey", 100);
        must_commit(&engine, b"zkey", 100, 110);
        must_prewrite_delete(&engine, b"zkey", b"zkey", 120);
        must_commit(&engine, b"zkey", 120, 130);
        must_prewrite_put(&engine, b"zkey1", &value, b"zkey1", 120);
        must_commit(&engine, b"zkey1", 120, 130);
        do_gc_by_compact(&raw_engine, None, None, 200, None);
        must_get_none(&engine, b"zkey", 110);
    }

    // Test if there are not enought garbage in SST files involved by a compaction, no compaction
    // filter will be created.
    #[test]
    fn test_mvcc_properties() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let raw_engine = engine.get_rocksdb();
        let value = vec![b'v'; 512];

        for start_ts in &[100, 110, 120, 130] {
            must_prewrite_put(&engine, b"zkey", &value, b"zkey", *start_ts);
            must_commit(&engine, b"zkey", *start_ts, *start_ts + 5);
        }
        must_prewrite_delete(&engine, b"zkey", b"zkey", 140);
        must_commit(&engine, b"zkey", 140, 145);

        // Can't GC stale versions because of the threshold.
        do_gc_by_compact_with_ratio_threshold(&raw_engine, 200, 10.0);
        for commit_ts in &[105, 115, 125, 135] {
            must_get(&engine, b"zkey", commit_ts, &value);
        }
    }

    // If we use `CompactionFilterDecision::RemoveAndSkipUntil` in compaction filters,
    // delete marks can only be handled in the bottommost level. Otherwise dirty
    // versions could be exposed incorrectly.
    //
    // This case tests that delete marks won't be handled at internal levels, and at
    // the bottommost levels, dirty versions still can't be exposed.
    #[test]
    fn test_remove_and_skip_until() {
        let mut cfg = DbConfig::default();
        cfg.writecf.disable_auto_compactions = true;
        cfg.writecf.dynamic_level_bytes = false;
        let dir = tempfile::TempDir::new().unwrap();
        let engine = TestEngineBuilder::new()
            .path(dir.path())
            .build_with_cfg(&cfg)
            .unwrap();
        let raw_engine = engine.get_rocksdb();

        // So the construction of SST files will be:
        // L6: |key_110|
        must_prewrite_put(&engine, b"zkey", b"zvalue", b"zkey", 100);
        must_commit(&engine, b"zkey", 100, 110);
        do_gc_by_compact(&raw_engine, None, None, 50, Some(6));
        assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[6], 1);

        // So the construction of SST files will be:
        // L0: |key_130, key_110|
        // L6: |key_110|
        must_prewrite_delete(&engine, b"zkey", b"zkey", 120);
        must_commit(&engine, b"zkey", 120, 130);
        let k_110 = Key::from_raw(b"zkey").append_ts(110.into()).into_encoded();
        raw_engine.delete_cf(CF_WRITE, &k_110).unwrap();
        raw_engine.flush_cf(CF_WRITE, true).unwrap();
        assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[0], 1);
        assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[6], 1);

        // Compact the mvcc delete mark to L5, the stale version shouldn't be exposed.
        let level_files = rocksdb_level_files(&raw_engine, CF_WRITE);
        let l0_file = dir.path().join(&level_files[0][0]);
        let files = &[l0_file.to_str().unwrap().to_owned()];
        compact_files(&raw_engine, CF_WRITE, files, 5);
        assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[5], 1);
        assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[6], 1);
        must_get_none(&engine, b"zkey", 200);

        // Compact the mvcc delete mark to L6, the stale version shouldn't be exposed.
        do_gc_by_compact(&raw_engine, None, None, 200, Some(6));
        must_get_none(&engine, b"zkey", 200);
    }

    // Compaction filter uses sequence number to determine 2 same user keys is exactly
    // one or not. It's required that sequence number of ingested snapshot files can't
    // be 0 if those files are overlapped with any level in the current DB. It's ensured
    // by the current RocksDB implementation.
    // PTAL at `Status ExternalSstFileIngestionJob::AssignLevelAndSeqnoForIngestedFile(...)`.
    #[test]
    fn test_ingested_ssts_seqno() {
        for level in 0..7 {
            let mut cfg = DbConfig::default();
            cfg.writecf.disable_auto_compactions = true;
            cfg.writecf.dynamic_level_bytes = false;
            let dir = tempfile::TempDir::new().unwrap();
            let engine = TestEngineBuilder::new()
                .path(dir.path())
                .build_with_cfg(&cfg)
                .unwrap();
            let raw_engine = engine.get_rocksdb();
            must_prewrite_put(&engine, b"zkey", b"zvalue", b"zkey", 100);
            must_commit(&engine, b"zkey", 100, 110);

            if level == 0 {
                raw_engine.flush_cf(CF_WRITE, true).unwrap();
            } else {
                let db = raw_engine.as_inner();
                let handle = get_cf_handle(db, CF_WRITE).unwrap();
                let mut compact_opts = compact_options();
                compact_opts.set_change_level(true);
                compact_opts.set_target_level(level as i32);
                db.compact_range_cf_opt(handle, &compact_opts, None, None);
            }
            assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[level], 1);

            let sst = tempfile::NamedTempFile::new().unwrap().into_temp_path();
            let sst_path = sst.to_str().unwrap();
            let mut sst_writer = RocksSstWriterBuilder::new()
                .set_db(&raw_engine)
                .set_cf(CF_WRITE)
                .build(sst_path)
                .unwrap();

            let opts = IterOptions::new(None, None, true);
            let mut iter = raw_engine.iterator_cf_opt(CF_WRITE, opts).unwrap();
            let mut valid = iter.seek(SeekKey::Start).unwrap();
            while valid {
                sst_writer.put(iter.key(), iter.value()).unwrap();
                valid = iter.next().unwrap();
            }
            sst_writer.finish().unwrap();
            drop(iter);

            let mut opts = RocksIngestExternalFileOptions::new();
            opts.move_files(true);
            raw_engine
                .ingest_external_file_cf(CF_WRITE, &opts, &[sst_path])
                .unwrap();

            let opts = IterOptions::new(None, None, true);
            let mut iter = raw_engine.iterator_cf_opt(CF_WRITE, opts).unwrap();
            let mut valid = iter.seek(SeekKey::Start).unwrap();
            while valid {
                let seqno = iter.as_raw().sequence().unwrap();
                assert!(seqno > 0, "ingested sst's seqno must be greater than 0");
                valid = iter.next().unwrap();
            }
        }
    }

    // Test a key can be GCed correctly if its MVCC versions cover multiple SST files.
    mod mvcc_versions_cover_multiple_ssts {
        use super::*;

        #[test]
        fn at_bottommost_level() {
            let engine = TestEngineBuilder::new().build().unwrap();
            let raw_engine = engine.get_rocksdb();

            let split_key = Key::from_raw(b"zkey")
                .append_ts(TimeStamp::from(135))
                .into_encoded();

            // So the construction of SST files will be:
            // L6: |key_110|
            must_prewrite_put(&engine, b"zkey", b"zvalue", b"zkey", 100);
            must_commit(&engine, b"zkey", 100, 110);
            do_gc_by_compact(&raw_engine, None, None, 50, None);
            assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[6], 1);

            // So the construction of SST files will be:
            // L6: |key_140, key_130|, |key_110|
            must_prewrite_put(&engine, b"zkey", b"zvalue", b"zkey", 120);
            must_commit(&engine, b"zkey", 120, 130);
            must_prewrite_delete(&engine, b"zkey", b"zkey", 140);
            must_commit(&engine, b"zkey", 140, 150);
            do_gc_by_compact(&raw_engine, None, Some(&split_key), 50, None);
            assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[6], 2);

            // Put more key/value pairs so that 1 file in L0 and 1 file in L6 can be merged.
            must_prewrite_put(&engine, b"zkex", b"zvalue", b"zkex", 100);
            must_commit(&engine, b"zkex", 100, 110);

            do_gc_by_compact(&raw_engine, None, Some(&split_key), 200, None);

            // There are still 2 files in L6 because the SST contains key_110 is not touched.
            assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[6], 2);

            // Although the SST files is not involved in the last compaction,
            // all versions of "key" should be cleared.
            let key = Key::from_raw(b"zkey")
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
            // L6: |AAAAA_101, BBBBB_101, CCCCC_111|
            must_prewrite_put(&engine, b"zAAAAA", b"zvalue", b"zkey", 100);
            must_commit(&engine, b"zAAAAA", 100, 101);
            must_prewrite_put(&engine, b"zBBBBB", b"zvalue", b"zkey", 100);
            must_commit(&engine, b"zBBBBB", 100, 101);
            must_prewrite_put(&engine, b"zCCCCC", b"zvalue", b"zkey", 110);
            must_commit(&engine, b"zCCCCC", 110, 111);
            do_gc_by_compact(&raw_engine, None, None, 50, Some(6));
            assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[6], 1);

            // So the construction of SST files will be:
            // L0: |BBBBB_101, DDDDD_101|
            // L6: |AAAAA_101, BBBBB_101, CCCCC_111|
            let b_101_k = Key::from_raw(b"zBBBBB").append_ts(101.into());
            let b_101_k = b_101_k.into_encoded();
            let b_101_v = raw_engine
                .get_value_cf(CF_WRITE, &b_101_k)
                .unwrap()
                .unwrap();
            raw_engine.put_cf(CF_WRITE, &b_101_k, &b_101_v).unwrap();
            must_prewrite_put(&engine, b"zDDDDD", b"zvalue", b"zkey", 100);
            must_commit(&engine, b"zDDDDD", 100, 101);
            raw_engine.flush_cf(CF_WRITE, true).unwrap();
            assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[0], 1);

            // So the construction of SST files will be:
            // L0: |AAAAA_111, BBBBB_111|, |BBBBB_101, DDDDD_101|
            // L6: |AAAAA_101, BBBBB_101, CCCCC_111|
            must_prewrite_put(&engine, b"zAAAAA", b"zvalue", b"zkey", 110);
            must_commit(&engine, b"zAAAAA", 110, 111);
            must_prewrite_delete(&engine, b"zBBBBB", b"zBBBBB", 110);
            must_commit(&engine, b"zBBBBB", 110, 111);
            raw_engine.flush_cf(CF_WRITE, true).unwrap();
            assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[0], 2);

            // Compact |AAAAA_111, BBBBB_111| at L0 and |AAAA_101, BBBBB_101, CCCCC_111| at L6.
            let start = Key::from_raw(b"zAAAAA").into_encoded();
            let end = Key::from_raw(b"zAAAAAA").into_encoded();
            do_gc_by_compact(&raw_engine, Some(&start), Some(&end), 200, Some(6));

            must_get_none(&engine, b"zBBBBB", 101);
        }
    }
}
