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
use engine_traits::{Iterator, MiscExt, Mutable, MvccProperties, SeekKey, WriteBatch, CF_WRITE};
use pd_client::{Feature, FeatureGate};
use prometheus::{local::*, *};
use txn_types::{Key, TimeStamp, WriteRef, WriteType};

use super::{GcConfig, GcWorkerConfigManager};
use crate::storage::mvcc::{GC_DELETE_VERSIONS_HISTOGRAM, MVCC_VERSIONS_HISTOGRAM};

const DEFAULT_DELETE_BATCH_SIZE: usize = 256 * 1024;
const DEFAULT_DELETE_BATCH_COUNT: usize = 128;
const NEAR_SEEK_LIMIT: usize = 8;

// The default version that can enable compaction filter for GC. This is necessary because after
// compaction filter is enabled, it's impossible to fallback to ealier version which modifications
// of GC are distributed to other replicas by Raft.
const COMPACTION_FILTER_GC_FEATURE: Feature = Feature::require(5, 0, 0);

// Global context to create a compaction filter for write CF. It's necessary as these fields are
// not available when construcing `WriteCompactionFilterFactory`.
struct GcContext {
    db: Arc<DB>,
    safe_point: Arc<AtomicU64>,
    cfg_tracker: GcWorkerConfigManager,
    feature_gate: FeatureGate,
    #[cfg(test)]
    callbacks_on_drop: Vec<Arc<dyn Fn(&WriteCompactionFilter) + Send + Sync>>,
}

lazy_static! {
    static ref GC_CONTEXT: Mutex<Option<GcContext>> = Mutex::new(None);

    // Filtered keys in `WriteCompactionFilter::filter_v2`.
    static ref GC_COMPACTION_FILTERED: IntCounter = register_int_counter!(
        "tikv_gc_compaction_filtered",
        "Filtered versions by compaction"
    )
    .unwrap();
    // Deleted keys in `WriteCompactionFilter::filter_v2`. It only happends when handling
    // MVCC-delete marks at the bottommost level.
    static ref GC_COMPACTION_DELETED: IntCounter = register_int_counter!(
        "tikv_gc_compaction_deleted",
        "Deleted versions by compaction"
    )
    .unwrap();
    // A counter for errors met by `WriteCompactionFilter`.
    static ref GC_COMPACTION_FAILURE: IntCounter = register_int_counter!(
        "tikv_gc_compaction_failure",
        "Compaction filter meets failure"
    )
    .unwrap();
    // A counter for skip performing GC in compactions.
    static ref GC_COMPACTION_FILTER_SKIP: IntCounter = register_int_counter!(
        "tikv_gc_compaction_filter_skip",
        "Skip to create compaction filter for GC because of table properties"
    )
    .unwrap();

    // When handling MVCC-delete marks an iterator is used to scan all elder versions.
    static ref GC_COMPACTION_FILTER_SEEK: IntCounter = register_int_counter!(
        "tikv_gc_compaction_filter_seek",
        "Seek times for the compaction filter internal iterator"
    )
    .unwrap();
    static ref GC_COMPACTION_FILTER_NEXT: IntCounter = register_int_counter!(
        "tikv_gc_compaction_filter_next",
        "Next times for the compaction filter internal iterator"
    )
    .unwrap();

    // It's relative to a key logic for handling mvcc delete marks.
    // When handling a MVCC-delete mark all elder versions should be deleted explicitly. However if
    // some of them are already filtered by the compaction, it's unnecessary to delete them again.
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
        feature_gate: FeatureGate,
    );
}

impl<T> CompactionFilterInitializer for T {
    default fn init_compaction_filter(
        &self,
        _safe_point: Arc<AtomicU64>,
        _cfg_tracker: GcWorkerConfigManager,
        _feature_gate: FeatureGate,
    ) {
    }
}

impl CompactionFilterInitializer for RocksEngine {
    fn init_compaction_filter(
        &self,
        safe_point: Arc<AtomicU64>,
        cfg_tracker: GcWorkerConfigManager,
        feature_gate: FeatureGate,
    ) {
        info!("initialize GC context for compaction filter");
        let mut gc_context = GC_CONTEXT.lock().unwrap();
        if gc_context.is_none() {
            *gc_context = Some(GcContext {
                db: self.as_inner().clone(),
                safe_point,
                cfg_tracker,
                feature_gate,
                #[cfg(test)]
                callbacks_on_drop: vec![],
            });
        } else {
            let ctx = gc_context.as_mut().unwrap();
            ctx.db = self.as_inner().clone();
            ctx.safe_point = safe_point;
            ctx.cfg_tracker = cfg_tracker;
            ctx.feature_gate = feature_gate;
        }
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
            // Safe point has not been initialized yet.
            debug!("skip gc in compaction filter because of no safe point");
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

        let db = Arc::clone(&gc_context.db);

        debug!(
            "creating compaction filter"; "feature_enable" => enable,
            "skip_version_check" => skip_vcheck,
            "ratio_threshold" => ratio_threshold,
        );

        if !do_check_allowed(enable, skip_vcheck, &gc_context.feature_gate) {
            debug!("skip gc in compaction filter because it's not allowed");
            return std::ptr::null_mut();
        }
        drop(gc_context_option);

        if !check_need_gc(safe_point.into(), ratio_threshold, context) {
            debug!("skip gc in compaction filter because it's not necessary");
            GC_COMPACTION_FILTER_SKIP.inc();
            return std::ptr::null_mut();
        }

        let name = CString::new("write_compaction_filter").unwrap();
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

    mvcc_key_prefix: Vec<u8>,
    remove_older: bool,

    // For handling MVCC delete marks at the bottommost level.
    is_bottommost_level: bool,
    // Layout: (delete_mark, vec![(ts, sequence), ...]).
    #[allow(clippy::type_complexity)]
    deleting_filtered: Option<(Vec<u8>, Vec<(u64, u64)>)>,
    // To determine whether use `seek` or `next` to find the next MVCC-delete mark.
    // If `near_seek_distance` is less then `NEAR_SEEK_LIMIT`, `next` will be used
    // instead of `seek`.
    near_seek_distance: usize,
    write_iter: Option<RocksEngineIterator>,
    write_iter_initialized: bool,

    versions: usize,
    filtered: usize,
    deleted: usize,
    total_versions: usize,
    total_filtered: usize,
    total_deleted: usize,
    versions_hist: LocalHistogram,
    filtered_hist: LocalHistogram,

    // Some metrics about implementation detail.
    seek_times: usize,
    next_times: usize,
    mvcc_delete_skip_older: usize,
    mvcc_rollback_and_locks: usize,

    #[cfg(test)]
    callbacks_on_drop: Vec<Arc<dyn Fn(&WriteCompactionFilter) + Send + Sync>>,
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

            mvcc_key_prefix: vec![],
            remove_older: false,

            is_bottommost_level: context.is_bottommost_level(),
            deleting_filtered: None,
            near_seek_distance: NEAR_SEEK_LIMIT,
            write_iter: None,
            write_iter_initialized: false,

            versions: 0,
            filtered: 0,
            deleted: 0,
            total_versions: 0,
            total_filtered: 0,
            total_deleted: 0,
            versions_hist: MVCC_VERSIONS_HISTOGRAM.local(),
            filtered_hist: GC_DELETE_VERSIONS_HISTOGRAM.local(),
            seek_times: 0,
            next_times: 0,
            mvcc_delete_skip_older: 0,
            mvcc_rollback_and_locks: 0,
            #[cfg(test)]
            callbacks_on_drop: {
                let ctx = GC_CONTEXT.lock().unwrap();
                ctx.as_ref().unwrap().callbacks_on_drop.clone()
            },
        };

        if filter.is_bottommost_level {
            // MVCC-delete marks can only be handled at the bottommost level to forbid deleted
            // RocksDB key/value pairs get exposed incorrectly. For example:
            //   L0: |key_130_MVCC_DEL, key_110_MVCC_PUT(tombstone)|
            //   L6: |key_110_MVCC_PUT|
            // If key_130_MVCC_DEL is handled at L0 and the SST in L6 is not involved,
            // key_110_MVCC_PUT could be exposed incorrectly.
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
        if self.is_bottommost_level && self.deleting_filtered.is_none() {
            // The compaction hasn't met the next MVCC-delete mark.
            self.near_seek_distance += 1;
        }

        let (mvcc_key_prefix, commit_ts) = split_ts(key)?;
        if commit_ts > self.safe_point || value_type != CompactionFilterValueType::Value {
            return Ok(CompactionFilterDecision::Keep);
        }

        self.versions += 1;
        if self.mvcc_key_prefix != mvcc_key_prefix {
            self.handle_delete_mark();
            self.switch_key_metrics();
            self.mvcc_key_prefix.clear();
            self.mvcc_key_prefix.extend_from_slice(mvcc_key_prefix);
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

        if !filtered {
            return Ok(CompactionFilterDecision::Keep);
        }
        self.filtered += 1;
        self.handle_filtered_write(write)?;
        let decision = if self.remove_older {
            // Use `Decision::RemoveAndSkipUntil` instead of `Decision::Remove` to avoid
            // leaving tombstones, which can only be freed at the bottommost level.
            debug_assert!(commit_ts > 0);
            let prefix = Key::from_encoded_slice(mvcc_key_prefix);
            let skip_until = prefix.append_ts((commit_ts - 1).into()).into_encoded();
            CompactionFilterDecision::RemoveAndSkipUntil(skip_until)
        } else {
            CompactionFilterDecision::Remove
        };
        Ok(decision)
    }

    // It's possible that elder versions than a MVCC-delete mark occurs in a higher level due to
    // replica migration. So those versions needs to be handled by scan + delete.
    fn handle_delete_mark(&mut self) {
        if let Err(e) = self.handle_delete_mark_impl() {
            error!(
                "WriteCompactionFilter meets a fatal error and can't recover";
                "err" => ?e,
            );
            // Because the section is executed in RocksDB compaction threads,
            // `exit` seems better than `panic`.
            unsafe { libc::exit(-1) };
        }
    }

    fn handle_delete_mark_impl(&mut self) -> Result<(), String> {
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
            if truncate_ts(key)? != self.mvcc_key_prefix.as_slice() {
                break;
            }
            let (_, current_ts) = split_ts(key)?;
            let mut need_delete = true;
            while let Some((ts, filtered_seqno)) = filtered.peek() {
                match current_ts.cmp(ts) {
                    CmpOrdering::Greater => break,
                    CmpOrdering::Less => {
                        let _ = filtered.next().unwrap();
                    }
                    CmpOrdering::Equal => {
                        // It's possible that there are multiple `key_ts` in different levels.
                        // A potential case is that a snapshot has been ingested. So using seqno
                        // to identify different RocksDB versions for one user key.
                        let seq = match write_iter.sequence() {
                            Some(seqno) => seqno,
                            // The iterator should support to get sequence numbers.
                            None => return Err("Iterator can't get seqno".to_owned()),
                        };
                        debug_assert!(seq >= *filtered_seqno);
                        // NOTE: in the bottommost level sequence could be 0. So it's required that
                        // an ingested file's sequence is not 0 if it overlaps with the DB. PTAL at
                        // test case `test_ingested_ssts_seqno`.
                        need_delete = seq > *filtered_seqno;
                        let _ = filtered.next().unwrap();
                        break;
                    }
                }
            }
            if need_delete {
                self.delete_write_key(key)?;
                self.handle_filtered_write(parse_write(value)?)?;
            } else {
                self.mvcc_delete_skip_older += 1;
            }
            valid = write_iter.next()?;
        }
        self.write_iter = Some(write_iter);
        Ok(())
    }

    fn write_iter_seek_to(&mut self, mark: &[u8]) -> Result<bool, String> {
        let write_iter = self.write_iter.as_mut().unwrap();

        if std::mem::replace(&mut self.write_iter_initialized, true) {
            // The range can be cleared by `DeleteFilesInRange` or some similar RocksDB APIs,
            // in which case it's unnecessary to call re-seek for the iterator.
            if !write_iter.valid()? {
                return Ok(false);
            }
            if write_iter.key() >= mark {
                return Ok(true);
            }
        }

        if self.near_seek_distance >= NEAR_SEEK_LIMIT {
            self.near_seek_distance = 0;
            let valid = write_iter.seek(SeekKey::Key(mark))?;
            self.seek_times += 1;
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
        self.next_times += next_count;

        if valid && !found {
            // Fallback to `seek` after some `next`s.
            valid = write_iter.seek(SeekKey::Key(mark))?;
            self.seek_times += 1;
        }
        self.near_seek_distance = 0;
        Ok(valid)
    }

    fn handle_filtered_write(&mut self, write: WriteRef) -> Result<(), String> {
        if write.short_value.is_none() && write.write_type == WriteType::Put {
            let prefix = Key::from_encoded_slice(&self.mvcc_key_prefix);
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
            self.write_batch.write()?;
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
    // NOTE: it's required that `CompactionFilter` is dropped before the compaction result
    // becomes installed into the DB instance.
    fn drop(&mut self) {
        self.handle_delete_mark();
        self.switch_key_metrics();
        if !self.write_batch.is_empty() {
            self.write_batch.write().unwrap();
            self.write_batch.clear();
        }
        self.engine.sync_wal().unwrap();

        GC_COMPACTION_FILTERED.inc_by(self.total_filtered as i64);
        GC_COMPACTION_DELETED.inc_by(self.total_deleted as i64);
        GC_COMPACTION_FILTER_SEEK.inc_by(self.seek_times as i64);
        GC_COMPACTION_FILTER_NEXT.inc_by(self.next_times as i64);
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
            if filtered > 0 || deleted > 0 {
                info!(
                    "Compaction filter reports"; "total" => versions,
                    "filtered" => filtered, "deleted" => deleted,
                );
            }
        }

        #[cfg(test)]
        for callback in &self.callbacks_on_drop {
            callback(&self);
        }
    }
}

impl CompactionFilter for WriteCompactionFilter {
    fn featured_filter(
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
        Err(_) => Err(format!(
            "invalid write cf key: {}",
            log_wrappers::Value(key)
        )),
    }
}

fn truncate_ts(key: &[u8]) -> Result<&[u8], String> {
    match Key::truncate_ts_for(key) {
        Ok(prefix) => Ok(prefix),
        Err(_) => Err(format!(
            "invalid write cf key: {}",
            log_wrappers::Value(key)
        )),
    }
}

fn parse_write(value: &[u8]) -> Result<WriteRef, String> {
    match WriteRef::parse(value) {
        Ok(write) => Ok(write),
        Err(_) => Err(format!(
            "invalid write cf value: {}",
            log_wrappers::Value(value)
        )),
    }
}

pub fn is_compaction_filter_allowed(cfg_value: &GcConfig, feature_gate: &FeatureGate) -> bool {
    do_check_allowed(
        cfg_value.enable_compaction_filter,
        cfg_value.compaction_filter_skip_version_check,
        feature_gate,
    )
}

fn do_check_allowed(enable: bool, skip_vcheck: bool, feature_gate: &FeatureGate) -> bool {
    enable && (skip_vcheck || feature_gate.can_enable(COMPACTION_FILTER_GC_FEATURE))
}

fn check_need_gc(
    safe_point: TimeStamp,
    ratio_threshold: f64,
    context: &CompactionFilterContext,
) -> bool {
    let check_props = |props: &MvccProperties| -> (bool, bool /*skip_more_checks*/) {
        if props.min_ts > safe_point {
            return (false, false);
        }
        if ratio_threshold < 1.0 || context.is_bottommost_level() {
            // According to our tests, `split_ts` on keys and `parse_write` on values
            // won't utilize much CPU. So always perform GC at the bottommost level
            // to avoid garbage accumulation.
            return (true, true);
        }
        if props.num_versions as f64 > props.num_rows as f64 * ratio_threshold {
            // When comparing `num_versions` with `num_rows`, it's unnecessary to
            // treat internal levels specially.
            return (true, false);
        }

        // When comparing `num_versions` with `num_puts`, trait internal levels specially
        // because MVCC-delete marks can't be handled at those levels.
        let num_rollback_and_locks = (props.num_versions - props.num_deletes) as f64;
        if num_rollback_and_locks > props.num_puts as f64 * ratio_threshold {
            return (true, false);
        }
        (props.max_row_versions > 1024, false)
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
            let (sst_needs_gc, skip_more_checks) = check_props(&props);
            if sst_needs_gc {
                needs_gc += 1;
            }
            if skip_more_checks {
                // It's the bottommost level or ratio_threshold is less than 1.
                needs_gc = context.file_numbers().len();
                break;
            }
        }
    }

    (needs_gc >= ((context.file_numbers().len() + 1) / 2)) || check_props(&sum_props).0
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
    use std::sync::mpsc;
    use std::time::Duration;
    use tikv_util::config::VersionTrack;

    /// Do a global GC with the given safe point.
    pub fn gc_by_compact(engine: &StorageRocksEngine, _: &[u8], safe_point: u64) {
        let engine = engine.get_rocksdb();
        // Put a new key-value pair to ensure compaction can be triggered correctly.
        engine.delete_cf("write", b"znot-exists-key").unwrap();

        let mut runner = TestGCRunner {
            safe_point,
            ..Default::default()
        };
        runner.gc(&engine);
    }

    lazy_static! {
        // Use a lock to protect concurrent compactions.
        static ref LOCK: Mutex<()> = std::sync::Mutex::new(());
    }

    fn default_compact_options() -> CompactOptions {
        let mut compact_opts = CompactOptions::new();
        compact_opts.set_exclusive_manual_compaction(false);
        compact_opts.set_max_subcompactions(1);
        compact_opts
    }

    #[derive(Default)]
    struct TestGCRunner<'a> {
        safe_point: u64,
        ratio_threshold: Option<f64>,
        start: Option<&'a [u8]>,
        end: Option<&'a [u8]>,
        target_level: Option<usize>,
        callbacks_on_drop: Vec<Arc<dyn Fn(&WriteCompactionFilter) + Send + Sync>>,
    }

    impl<'a> TestGCRunner<'a> {
        fn safe_point(&mut self, sp: u64) -> &mut Self {
            self.safe_point = sp;
            self
        }
        fn prepare_gc(&self, raw_engine: &RocksEngine) {
            let mut gc_cfg = GcConfig::default();
            if let Some(ratio_threshold) = self.ratio_threshold {
                gc_cfg.ratio_threshold = ratio_threshold;
            }
            gc_cfg.enable_compaction_filter = true;
            let feature_gate = FeatureGate::default();
            feature_gate.set_version("5.0.0").unwrap();
            raw_engine.init_compaction_filter(
                Arc::new(AtomicU64::new(self.safe_point)),
                GcWorkerConfigManager(Arc::new(VersionTrack::new(gc_cfg))),
                feature_gate,
            );

            let mut gc_context = GC_CONTEXT.lock().unwrap();
            let callbacks = &mut gc_context.as_mut().unwrap().callbacks_on_drop;
            callbacks.clear();
            for callback in &self.callbacks_on_drop {
                callbacks.push(callback.clone());
            }
        }
        fn post_gc(&mut self) {
            self.callbacks_on_drop.clear();
            let mut gc_context = GC_CONTEXT.lock().unwrap();
            let callbacks = &mut gc_context.as_mut().unwrap().callbacks_on_drop;
            callbacks.clear();
        }

        fn gc(&mut self, raw_engine: &RocksEngine) {
            let _guard = LOCK.lock().unwrap();
            self.prepare_gc(raw_engine);

            let db = raw_engine.as_inner();
            let handle = get_cf_handle(db, CF_WRITE).unwrap();
            let mut compact_opts = default_compact_options();
            if let Some(target_level) = self.target_level {
                compact_opts.set_change_level(true);
                compact_opts.set_target_level(target_level as i32);
            }
            db.compact_range_cf_opt(handle, &compact_opts, self.start, self.end);
            self.post_gc();
        }

        fn gc_on_files(&mut self, raw_engine: &RocksEngine, input_files: &[String]) {
            let _guard = LOCK.lock().unwrap();
            self.prepare_gc(raw_engine);
            let db = raw_engine.as_inner();
            let handle = get_cf_handle(db, CF_WRITE).unwrap();
            let level = self.target_level.unwrap() as i32;
            db.compact_files_cf(handle, &CompactionOptions::new(), input_files, level)
                .unwrap();
            self.post_gc();
        }
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
        let gate = FeatureGate::default();
        gate.set_version("4.1.0").unwrap();
        let mut cfg_value = GcConfig::default();
        assert!(!is_compaction_filter_allowed(&cfg_value, &gate));

        cfg_value.enable_compaction_filter = true;
        assert!(!is_compaction_filter_allowed(&cfg_value, &gate));

        cfg_value.compaction_filter_skip_version_check = true;
        assert!(is_compaction_filter_allowed(&cfg_value, &gate));

        gate.set_version("5.0.0").unwrap();
        cfg_value.compaction_filter_skip_version_check = false;
        assert!(is_compaction_filter_allowed(&cfg_value, &gate));
    }

    // Test compaction filter won't break basic GC rules.
    #[test]
    fn test_compaction_filter_basic() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let raw_engine = engine.get_rocksdb();
        let value = vec![b'v'; 512];
        let mut gc_runner = TestGCRunner::default();

        // GC can't delete keys after the given safe point.
        must_prewrite_put(&engine, b"zkey", &value, b"zkey", 100);
        must_commit(&engine, b"zkey", 100, 110);
        gc_runner.safe_point(50).gc(&raw_engine);
        must_get(&engine, b"zkey", 110, &value);

        // GC can't delete keys before the safe ponit if they are latest versions.
        gc_runner.safe_point(200).gc(&raw_engine);
        must_get(&engine, b"zkey", 110, &value);

        must_prewrite_put(&engine, b"zkey", &value, b"zkey", 120);
        must_commit(&engine, b"zkey", 120, 130);

        // GC can't delete the latest version before the safe ponit.
        gc_runner.safe_point(115).gc(&raw_engine);
        must_get(&engine, b"zkey", 110, &value);

        // GC a version will also delete the key on default CF.
        gc_runner.safe_point(200).gc(&raw_engine);
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
        let mut gc_runner = TestGCRunner::default();

        // No key switch after the delete mark.
        must_prewrite_put(&engine, b"zkey", &value, b"zkey", 100);
        must_commit(&engine, b"zkey", 100, 110);
        must_prewrite_delete(&engine, b"zkey", b"zkey", 120);
        must_commit(&engine, b"zkey", 120, 130);

        gc_runner.safe_point(200).gc(&raw_engine);
        must_get_none(&engine, b"zkey", 110);

        must_prewrite_put(&engine, b"zkey", &value, b"zkey", 100);
        must_commit(&engine, b"zkey", 100, 110);
        must_prewrite_delete(&engine, b"zkey", b"zkey", 120);
        must_commit(&engine, b"zkey", 120, 130);
        must_prewrite_put(&engine, b"zkey1", &value, b"zkey1", 120);
        must_commit(&engine, b"zkey1", 120, 130);
        gc_runner.safe_point(200).gc(&raw_engine);
        must_get_none(&engine, b"zkey", 110);
    }

    // Test if there are not enought garbage in SST files involved by a compaction, no compaction
    // filter will be created.
    #[test]
    fn test_mvcc_properties() {
        let mut cfg = DbConfig::default();
        cfg.writecf.disable_auto_compactions = true;
        cfg.writecf.dynamic_level_bytes = false;
        let dir = tempfile::TempDir::new().unwrap();
        let builder = TestEngineBuilder::new().path(dir.path());
        let engine = builder.build_with_cfg(&cfg).unwrap();
        let raw_engine = engine.get_rocksdb();
        let value = vec![b'v'; 512];
        let mut gc_runner = TestGCRunner::default();

        for start_ts in &[100, 110, 120, 130] {
            must_prewrite_put(&engine, b"zkey", &value, b"zkey", *start_ts);
            must_commit(&engine, b"zkey", *start_ts, *start_ts + 5);
        }
        must_prewrite_delete(&engine, b"zkey", b"zkey", 140);
        must_commit(&engine, b"zkey", 140, 145);

        // Can't perform GC because the min timestamp is greater than safe point.
        gc_runner
            .callbacks_on_drop
            .push(Arc::new(|_: &WriteCompactionFilter| {
                unreachable!();
            }));
        gc_runner.target_level = Some(6);
        gc_runner.safe_point(100).gc(&raw_engine);

        // Can perform GC at the bottommost level even if the threshold can't be reached.
        gc_runner.ratio_threshold = Some(10.0);
        gc_runner.target_level = Some(6);
        gc_runner.safe_point(140).gc(&raw_engine);
        for commit_ts in &[105, 115, 125] {
            must_get_none(&engine, b"zkey", commit_ts);
        }

        // Put an extra key to make the memtable overlap with the bottommost one.
        must_prewrite_put(&engine, b"zkey1", &value, b"zkey1", 200);
        must_commit(&engine, b"zkey1", 200, 205);
        for start_ts in &[200, 210, 220, 230] {
            must_prewrite_put(&engine, b"zkey", &value, b"zkey", *start_ts);
            must_commit(&engine, b"zkey", *start_ts, *start_ts + 5);
        }
        must_prewrite_delete(&engine, b"zkey", b"zkey", 240);
        must_commit(&engine, b"zkey", 240, 245);
        raw_engine.flush_cf(CF_WRITE, true).unwrap();

        // At internal levels can't perform GC because the threshold is not reached.
        let level_files = rocksdb_level_files(&raw_engine, CF_WRITE);
        let l0_file = dir.path().join(&level_files[0][0]);
        let files = &[l0_file.to_str().unwrap().to_owned()];
        gc_runner.target_level = Some(5);
        gc_runner.ratio_threshold = Some(10.0);
        gc_runner.safe_point(300).gc_on_files(&raw_engine, files);
        for commit_ts in &[205, 215, 225, 235] {
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
        let builder = TestEngineBuilder::new().path(dir.path());
        let engine = builder.build_with_cfg(&cfg).unwrap();
        let raw_engine = engine.get_rocksdb();
        let mut gc_runner = TestGCRunner::default();

        // So the construction of SST files will be:
        // L6: |key_110|
        must_prewrite_put(&engine, b"zkey", b"zvalue", b"zkey", 100);
        must_commit(&engine, b"zkey", 100, 110);
        gc_runner.target_level = Some(6);
        gc_runner.safe_point(50).gc(&raw_engine);
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
        gc_runner.target_level = Some(5);
        gc_runner.safe_point(200).gc_on_files(&raw_engine, files);
        assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[5], 1);
        assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[6], 1);
        must_get_none(&engine, b"zkey", 200);

        // Compact the mvcc delete mark to L6, the stale version shouldn't be exposed.
        gc_runner.target_level = Some(6);
        gc_runner.safe_point(200).gc(&raw_engine);
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
            let builder = TestEngineBuilder::new().path(dir.path());
            let engine = builder.build_with_cfg(&cfg).unwrap();
            let raw_engine = engine.get_rocksdb();

            must_prewrite_put(&engine, b"zkey", b"zvalue", b"zkey", 100);
            must_commit(&engine, b"zkey", 100, 110);

            if level == 0 {
                raw_engine.flush_cf(CF_WRITE, true).unwrap();
            } else {
                let db = raw_engine.as_inner();
                let handle = get_cf_handle(db, CF_WRITE).unwrap();
                let mut compact_opts = default_compact_options();
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
                let seqno = iter.sequence().unwrap();
                assert!(seqno > 0, "ingested sst's seqno must be greater than 0");
                valid = iter.next().unwrap();
            }
        }
    }

    // When handling MVCC-delete marks at the bottommost level, filtered keys won't
    // be rewritten again by DB::delete. All pathes in `handle_delete_mark_impl` should
    // be covered by this case.
    #[test]
    fn test_mvcc_delete_skip_filtered() {
        let mut cfg = DbConfig::default();
        cfg.writecf.disable_auto_compactions = true;
        let dir = tempfile::TempDir::new().unwrap();
        let builder = TestEngineBuilder::new().path(dir.path());
        let engine = builder.build_with_cfg(&cfg).unwrap();
        let raw_engine = engine.get_rocksdb();
        let mut gc_runner = TestGCRunner {
            ratio_threshold: Some(0.9),
            ..Default::default()
        };

        // So the construction of SST files will be:
        // L6: |key_106, key_104, key_102|
        must_prewrite_put(&engine, b"zkey", b"zvalue1", b"zkey", 101);
        must_commit(&engine, b"zkey", 101, 102);
        must_prewrite_put(&engine, b"zkey", b"zvalue2", b"zkey", 103);
        must_commit(&engine, b"zkey", 103, 104);
        must_prewrite_put(&engine, b"zkey", b"zvalue3", b"zkey", 105);
        must_commit(&engine, b"zkey", 105, 106);
        gc_runner.target_level = Some(6);
        gc_runner.safe_point(50).gc(&raw_engine);
        assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[6], 1);

        // So the construction of SST files will be:
        // L5: |key_112, key1_112|
        // L6: |key_106, key_104, key_102|
        must_prewrite_put(&engine, b"zkey", b"zvalue4", b"zkey", 111);
        must_commit(&engine, b"zkey", 111, 112);
        must_prewrite_put(&engine, b"zkey1", b"zvalue4", b"zkey1", 111);
        must_commit(&engine, b"zkey1", 111, 112);
        raw_engine.flush_cf(CF_WRITE, true).unwrap();
        assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[0], 1);
        let level_files = rocksdb_level_files(&raw_engine, CF_WRITE);
        let f = dir.path().join(&level_files[0][0]);
        let f = f.to_str().unwrap().to_owned();
        gc_runner.target_level = Some(5);
        gc_runner.safe_point(50).gc_on_files(&raw_engine, &[f]);
        assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[5], 1);
        assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[6], 1);

        // So the construction of SST files will be:
        // L0: |key_122, key1_122|
        // L5: |key_112, key1_112|
        // L6: |key_106, key_104, key_102|
        must_prewrite_delete(&engine, b"zkey", b"zkey", 121);
        must_commit(&engine, b"zkey", 121, 122);
        must_prewrite_delete(&engine, b"zkey1", b"zkey1", 121);
        must_commit(&engine, b"zkey1", 121, 122);
        raw_engine.flush_cf(CF_WRITE, true).unwrap();
        assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[0], 1);

        let key_104 = Key::from_raw(b"zkey").append_ts(104.into()).into_encoded();
        let key_112 = Key::from_raw(b"zkey").append_ts(112.into()).into_encoded();
        let value_112 = raw_engine
            .get_value_cf(CF_WRITE, &key_112)
            .unwrap()
            .unwrap();

        // So the construction of SST files will be:
        // L5: |key_122, key1_122|
        // L6: |key_106, key_104, key_102|
        let level_files = rocksdb_level_files(&raw_engine, CF_WRITE);
        let f = dir.path().join(&level_files[0][0]);
        let f = f.to_str().unwrap().to_owned();
        gc_runner.target_level = Some(5);
        gc_runner.safe_point(130).gc_on_files(&raw_engine, &[f]);
        assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[0], 0);
        assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[5], 1);
        assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[6], 1);
        // key_112 must be cleared by the compaction.
        must_get(&engine, b"zkey", 120, b"zvalue3");

        // Put `k_112` back to simulate it hasn't been compacted.
        raw_engine.put_cf(CF_WRITE, &key_112, &value_112).unwrap();
        // Delete `k_104` to simulate it has been cleared by traditional GC.
        raw_engine.delete_cf(CF_WRITE, &key_104).unwrap();

        // Do the last compaction:
        // [key_112] will be deleted.
        // [key_122, key_106, key_104, key_102, key1_122] will be filtered.
        // [key_106, key_102] will be skiped in `handle_delete_mark`.
        let level_files = rocksdb_level_files(&raw_engine, CF_WRITE);
        let mut files = Vec::with_capacity(2);
        for &level in &[5, 6] {
            let f = dir.path().join(&level_files[level][0]);
            files.push(f.to_str().unwrap().to_owned());
        }
        gc_runner.target_level = Some(6);
        gc_runner
            .callbacks_on_drop
            .push(Arc::new(|filter: &WriteCompactionFilter| {
                assert_eq!(filter.total_filtered, 5);
                assert_eq!(filter.total_deleted, 1);
                assert_eq!(filter.mvcc_delete_skip_older, 2);
            }));
        gc_runner.safe_point(130).gc_on_files(&raw_engine, &files);
        // key_112 must be cleared by the compaction.
        must_get_none(&engine, b"zkey", 120)
    }

    #[test]
    fn test_delete_files_in_range() {
        let mut cfg = DbConfig::default();
        cfg.writecf.disable_auto_compactions = true;
        let dir = tempfile::TempDir::new().unwrap();
        let builder = TestEngineBuilder::new().path(dir.path());
        let engine = builder.build_with_cfg(&cfg).unwrap();
        let raw_engine = engine.get_rocksdb();
        let mut gc_runner = TestGCRunner {
            ratio_threshold: Some(0.9),
            ..Default::default()
        };

        // So the construction of SST files will be:
        // L6: |key1_101, key2_108, key2_106, key2_104, key2_102, key3_101|
        must_prewrite_put(&engine, b"zkey1", b"zvalue1", b"zkey1", 101);
        must_commit(&engine, b"zkey1", 101, 102);
        must_prewrite_put(&engine, b"zkey3", b"zvalue1", b"zkey3", 101);
        must_commit(&engine, b"zkey3", 101, 102);
        must_prewrite_put(&engine, b"zkey2", b"zvalue1", b"zkey2", 101);
        must_commit(&engine, b"zkey2", 101, 102);
        must_prewrite_put(&engine, b"zkey2", b"zvalue2", b"zkey2", 103);
        must_commit(&engine, b"zkey2", 103, 104);
        must_prewrite_put(&engine, b"zkey2", b"zvalue3", b"zkey2", 105);
        must_commit(&engine, b"zkey2", 105, 106);
        must_prewrite_delete(&engine, b"zkey2", b"zkey2", 107);
        must_commit(&engine, b"zkey2", 107, 108);
        gc_runner.target_level = Some(6);
        gc_runner.safe_point(50).gc(&raw_engine);
        assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[6], 1);

        let cf = get_cf_handle(raw_engine.as_inner(), CF_WRITE).unwrap();
        raw_engine
            .as_inner()
            .delete_files_in_range_cf(cf, b"zkey2", b"zkey3", false)
            .unwrap();
        assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[6], 1);

        let (tx, rx) = mpsc::sync_channel::<()>(1);
        gc_runner
            .callbacks_on_drop
            .push(Arc::new(move |filter: &WriteCompactionFilter| {
                tx.send(()).unwrap();
                // With the first `seek` the compaction filter can find that the range
                // is deleted, so no more `seek` or `next` is necessary.
                assert_eq!(filter.seek_times, 1);
                assert_eq!(filter.next_times, 0);
            }));
        gc_runner.safe_point(200).gc(&raw_engine);
        assert!(rx.recv_timeout(Duration::from_millis(100)).is_ok());
    }

    // Test a key can be GCed correctly if its MVCC versions cover multiple SST files.
    mod mvcc_versions_cover_multiple_ssts {
        use super::*;

        #[test]
        fn at_bottommost_level() {
            let engine = TestEngineBuilder::new().build().unwrap();
            let raw_engine = engine.get_rocksdb();
            let mut gc_runner = TestGCRunner::default();

            let split_key = Key::from_raw(b"zkey")
                .append_ts(TimeStamp::from(135))
                .into_encoded();

            // So the construction of SST files will be:
            // L6: |key_110|
            must_prewrite_put(&engine, b"zkey", b"zvalue", b"zkey", 100);
            must_commit(&engine, b"zkey", 100, 110);
            gc_runner.safe_point(50).gc(&raw_engine);
            assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[6], 1);

            // So the construction of SST files will be:
            // L6: |key_140, key_130|, |key_110|
            must_prewrite_put(&engine, b"zkey", b"zvalue", b"zkey", 120);
            must_commit(&engine, b"zkey", 120, 130);
            must_prewrite_delete(&engine, b"zkey", b"zkey", 140);
            must_commit(&engine, b"zkey", 140, 150);
            gc_runner.end = Some(&split_key);
            gc_runner.safe_point(50).gc(&raw_engine);
            assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[6], 2);

            // Put more key/value pairs so that 1 file in L0 and 1 file in L6 can be merged.
            must_prewrite_put(&engine, b"zkex", b"zvalue", b"zkex", 100);
            must_commit(&engine, b"zkex", 100, 110);

            gc_runner.end = Some(&split_key);
            gc_runner.safe_point(200).gc(&raw_engine);

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
            let mut gc_runner = TestGCRunner::default();

            // So the construction of SST files will be:
            // L6: |AAAAA_101, BBBBB_101, CCCCC_111|
            must_prewrite_put(&engine, b"zAAAAA", b"zvalue", b"zkey", 100);
            must_commit(&engine, b"zAAAAA", 100, 101);
            must_prewrite_put(&engine, b"zBBBBB", b"zvalue", b"zkey", 100);
            must_commit(&engine, b"zBBBBB", 100, 101);
            must_prewrite_put(&engine, b"zCCCCC", b"zvalue", b"zkey", 110);
            must_commit(&engine, b"zCCCCC", 110, 111);
            gc_runner.target_level = Some(6);
            gc_runner.safe_point(50).gc(&raw_engine);
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
            gc_runner.start = Some(&start);
            gc_runner.end = Some(&end);
            gc_runner.target_level = Some(6);
            gc_runner.safe_point(200).gc(&raw_engine);
            must_get_none(&engine, b"zBBBBB", 101);
        }
    }
}
