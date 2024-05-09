// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::Cell,
    ffi::CString,
    mem,
    result::Result,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use engine_rocks::{
    raw::{
        CompactionFilter, CompactionFilterContext, CompactionFilterDecision,
        CompactionFilterFactory, CompactionFilterValueType,
    },
    RocksEngine, RocksMvccProperties, RocksWriteBatchVec,
};
use engine_traits::{KvEngine, MiscExt, MvccProperties, WriteBatch, WriteOptions};
use file_system::{IoType, WithIoType};
use keyspace_meta::KeyspaceLevelGCService;
use pd_client::{Feature, FeatureGate};
use prometheus::{local::*, *};
use raftstore::coprocessor::RegionInfoProvider;
use tikv_util::{
    time::Instant,
    worker::{ScheduleError, Scheduler},
    Either,
};
use txn_types::{Key, TimeStamp, WriteRef, WriteType};

use crate::{
    server::gc_worker::{GcConfig, GcTask, GcWorkerConfigManager, STAT_TXN_KEYMODE},
    storage::mvcc::{GC_DELETE_VERSIONS_HISTOGRAM, MVCC_VERSIONS_HISTOGRAM},
};

const DEFAULT_DELETE_BATCH_SIZE: usize = 256 * 1024;
pub const DEFAULT_DELETE_BATCH_COUNT: usize = 128;

// The default version that can enable compaction filter for GC. This is
// necessary because after compaction filter is enabled, it's impossible to
// fallback to earlier version which modifications of GC are distributed to
// other replicas by Raft.
const COMPACTION_FILTER_GC_FEATURE: Feature = Feature::require(5, 0, 0);

// Global context to create a compaction filter for write CF. It's necessary as
// these fields are not available when constructing
// `WriteCompactionFilterFactory`.
pub struct GcContext {
    pub(crate) db: Option<RocksEngine>,
    pub(crate) store_id: u64,
    pub(crate) safe_point: Arc<AtomicU64>,
    pub(crate) cfg_tracker: GcWorkerConfigManager,
    feature_gate: FeatureGate,
    pub(crate) gc_scheduler: Scheduler<GcTask<RocksEngine>>,
    pub(crate) region_info_provider: Arc<dyn RegionInfoProvider + 'static>,
    #[cfg(any(test, feature = "failpoints"))]
    callbacks_on_drop: Vec<Arc<dyn Fn(&WriteCompactionFilter) + Send + Sync>>,

    pub(crate) keyspace_level_gc_service: Arc<Option<KeyspaceLevelGCService>>,
}

// Give all orphan versions an ID to log them.
static ORPHAN_VERSIONS_ID: AtomicUsize = AtomicUsize::new(0);

lazy_static! {
    pub static ref GC_CONTEXT: Mutex<Option<GcContext>> = Mutex::new(None);

    // Filtered keys in `WriteCompactionFilter::filter_v2`.
    pub static ref GC_COMPACTION_FILTERED: IntCounterVec = register_int_counter_vec!(
        "tikv_gc_compaction_filtered",
        "Filtered versions by compaction",
        &["key_mode"]
    )
    .unwrap();
    // A counter for errors met by `WriteCompactionFilter`.
    //TODO: Add test case to check the correctness of GC_COMPACTION_FAILURE
    pub static ref GC_COMPACTION_FAILURE: IntCounterVec = register_int_counter_vec!(
        "tikv_gc_compaction_failure",
        "Compaction filter meets failure",
        &["key_mode", "type"]
    )
    .unwrap();
    // A counter for skip performing GC in compactions.
    pub static ref GC_COMPACTION_FILTER_SKIP: IntCounterVec = register_int_counter_vec!(
        "tikv_gc_compaction_filter_skip",
        "Skip to create compaction filter for GC because of table properties",
        &["key_mode"]
    )
    .unwrap();
    pub static ref GC_COMPACTION_FILTER_PERFORM: IntCounterVec = register_int_counter_vec!(
        "tikv_gc_compaction_filter_perform",
        "perfrom GC in compaction filter",
        &["key_mode"]
    )
    .unwrap();


    // `WriteType::Rollback` and `WriteType::Lock` are handled in different ways.
    //TODO: Add test case to check the correctness of GC_COMPACTION_MVCC_ROLLBACK
    pub static ref GC_COMPACTION_MVCC_ROLLBACK: IntCounterVec = register_int_counter_vec!(
        "tikv_gc_compaction_mvcc_rollback",
        "Compaction of mvcc rollbacks",
        &["key_mode"]
    )
    .unwrap();

    //TODO: Add test case to check the correctness of GC_COMPACTION_FILTER_ORPHAN_VERSIONS
    pub static ref GC_COMPACTION_FILTER_ORPHAN_VERSIONS: IntCounterVec = register_int_counter_vec!(
        "tikv_gc_compaction_filter_orphan_versions",
        "Compaction filter orphan versions for default CF",
        &["key_mode", "tag"]
    ).unwrap();

    /// Counter of mvcc deletions met in compaction filter.
    pub static ref GC_COMPACTION_FILTER_MVCC_DELETION_MET: IntCounterVec = register_int_counter_vec!(
        "tikv_gc_compaction_filter_mvcc_deletion_met",
        "MVCC deletion from compaction filter met",
        &["key_mode"]
    ).unwrap();

    /// Counter of mvcc deletions handled in gc worker.
    pub static ref GC_COMPACTION_FILTER_MVCC_DELETION_HANDLED: IntCounterVec = register_int_counter_vec!(
        "tikv_gc_compaction_filter_mvcc_deletion_handled",
        "MVCC deletion from compaction filter handled",
        &["key_mode"]
    )
    .unwrap();

    /// Mvcc deletions sent to gc worker can have already been cleared, in which case resources are
    /// wasted to seek them.
    //TODO: Add test case to check the correctness of GC_COMPACTION_FILTER_MVCC_DELETION_WASTED
    pub static ref GC_COMPACTION_FILTER_MVCC_DELETION_WASTED: IntCounterVec = register_int_counter_vec!(
        "tikv_gc_compaction_filter_mvcc_deletion_wasted",
        "MVCC deletion from compaction filter wasted",
        &["key_mode"]
    ).unwrap();
}

pub trait CompactionFilterInitializer<EK>
where
    EK: KvEngine,
{
    fn init_compaction_filter(
        &self,
        store_id: u64,
        safe_point: Arc<AtomicU64>,
        cfg_tracker: GcWorkerConfigManager,
        feature_gate: FeatureGate,
        gc_scheduler: Scheduler<GcTask<EK>>,
        region_info_provider: Arc<dyn RegionInfoProvider>,
        keyspace_level_gc_service: Arc<Option<KeyspaceLevelGCService>>,
    );
}

impl<EK> CompactionFilterInitializer<EK> for Option<EK>
where
    EK: KvEngine,
{
    default fn init_compaction_filter(
        &self,
        _store_id: u64,
        _safe_point: Arc<AtomicU64>,
        _cfg_tracker: GcWorkerConfigManager,
        _feature_gate: FeatureGate,
        _gc_scheduler: Scheduler<GcTask<EK>>,
        _region_info_provider: Arc<dyn RegionInfoProvider>,
        _keyspace_level_gc_service: Arc<Option<KeyspaceLevelGCService>>,
    ) {
        info!("Compaction filter is not supported for this engine.");
    }
}

impl CompactionFilterInitializer<RocksEngine> for Option<RocksEngine> {
    fn init_compaction_filter(
        &self,
        store_id: u64,
        safe_point: Arc<AtomicU64>,
        cfg_tracker: GcWorkerConfigManager,
        feature_gate: FeatureGate,
        gc_scheduler: Scheduler<GcTask<RocksEngine>>,
        region_info_provider: Arc<dyn RegionInfoProvider>,
        keyspace_level_gc_service: Arc<Option<KeyspaceLevelGCService>>,
    ) {
        info!("initialize GC context for compaction filter");
        let mut gc_context = GC_CONTEXT.lock().unwrap();
        *gc_context = Some(GcContext {
            db: self.clone(),
            store_id,
            safe_point,
            cfg_tracker,
            feature_gate,
            gc_scheduler,
            region_info_provider,
            #[cfg(any(test, feature = "failpoints"))]
            callbacks_on_drop: vec![],
            keyspace_level_gc_service,
        });
    }
}

pub struct WriteCompactionFilterFactory;

impl CompactionFilterFactory for WriteCompactionFilterFactory {
    type Filter = WriteCompactionFilter;

    fn create_compaction_filter(
        &self,
        context: &CompactionFilterContext,
    ) -> Option<(CString, Self::Filter)> {
        let gc_context_option = GC_CONTEXT.lock().unwrap();
        let gc_context = match *gc_context_option {
            Some(ref ctx) => ctx,
            None => return None,
        };

        let safe_point = gc_context.safe_point.load(Ordering::Relaxed);
        let keyspace_level_gc_service = gc_context.keyspace_level_gc_service.clone();

        let mut is_all_ks_not_init_gc_sp = true;
        if let Some(ref ks_meta_service) = *keyspace_level_gc_service {
            is_all_ks_not_init_gc_sp =
                ks_meta_service.is_all_keyspace_level_gc_have_not_initialized()
        }

        if safe_point == 0 && is_all_ks_not_init_gc_sp {
            // Safe point has not been initialized yet.
            debug!("skip gc in compaction filter because of no safe point");
            return None;
        }

        let (enable, skip_vcheck, ratio_threshold) = {
            let value = &*gc_context.cfg_tracker.value();
            (
                value.enable_compaction_filter,
                value.compaction_filter_skip_version_check,
                value.ratio_threshold,
            )
        };

        let db = gc_context.db.clone();
        let gc_scheduler = gc_context.gc_scheduler.clone();
        let store_id = gc_context.store_id;
        let region_info_provider = gc_context.region_info_provider.clone();

        debug!(
            "creating compaction filter"; "feature_enable" => enable,
            "skip_version_check" => skip_vcheck,
            "ratio_threshold" => ratio_threshold,
            "keyspace_level_gc_service_is_some" => keyspace_level_gc_service.is_some(),
        );

        if db
            .as_ref()
            .map_or(false, RocksEngine::is_stalled_or_stopped)
        {
            debug!("skip gc in compaction filter because the DB is stalled");
            return None;
        }

        if !do_check_allowed(enable, skip_vcheck, &gc_context.feature_gate) {
            debug!("skip gc in compaction filter because it's not allowed");
            return None;
        }

        drop(gc_context_option);
        GC_COMPACTION_FILTER_PERFORM
            .with_label_values(&[STAT_TXN_KEYMODE])
            .inc();
        if !check_need_gc(
            safe_point.into(),
            ratio_threshold,
            context,
            keyspace_level_gc_service.clone(),
        ) {
            debug!("skip gc in compaction filter because it's not necessary");
            GC_COMPACTION_FILTER_SKIP
                .with_label_values(&[STAT_TXN_KEYMODE])
                .inc();
            return None;
        }

        debug!(
            "gc in compaction filter"; "safe_point" => safe_point,
            "files" => ?context.file_numbers(),
            "bottommost" => context.is_bottommost_level(),
            "manual" => context.is_manual_compaction(),
        );

        let filter = WriteCompactionFilter::new(
            db,
            safe_point,
            context,
            gc_scheduler,
            (store_id, region_info_provider),
            keyspace_level_gc_service,
        );
        let name = CString::new("write_compaction_filter").unwrap();
        Some((name, filter))
    }
}

pub struct DeleteBatch<B> {
    pub batch: Either<B, Vec<Key>>,
}

impl<B: WriteBatch> DeleteBatch<B> {
    fn new<EK>(db: &Option<EK>) -> Self
    where
        EK: KvEngine<WriteBatch = B>,
    {
        Self {
            batch: match db {
                Some(db) => Either::Left(db.write_batch_with_cap(DEFAULT_DELETE_BATCH_SIZE)),
                None => Either::Right(Vec::with_capacity(64)),
            },
        }
    }

    // `key` has prefix `DATA_KEY`.
    fn delete(&mut self, key: &[u8], ts: TimeStamp) -> Result<(), String> {
        match &mut self.batch {
            Either::Left(batch) => {
                let key = Key::from_encoded_slice(key).append_ts(ts);
                batch.delete(key.as_encoded())?;
            }
            Either::Right(keys) => {
                let key = Key::from_encoded_slice(keys::origin_key(key)).append_ts(ts);
                keys.push(key);
            }
        }
        Ok(())
    }

    fn is_empty(&self) -> bool {
        match &self.batch {
            Either::Left(batch) => batch.is_empty(),
            Either::Right(keys) => keys.is_empty(),
        }
    }

    pub fn count(&self) -> usize {
        match &self.batch {
            Either::Left(batch) => batch.count(),
            Either::Right(keys) => keys.len(),
        }
    }
}

pub struct WriteCompactionFilter {
    safe_point: u64,
    engine: Option<RocksEngine>,
    is_bottommost_level: bool,
    encountered_errors: bool,

    write_batch: DeleteBatch<RocksWriteBatchVec>,
    gc_scheduler: Scheduler<GcTask<RocksEngine>>,
    // A key batch which is going to be sent to the GC worker.
    mvcc_deletions: Vec<Key>,
    // The count of records covered the current mvcc-deletion mark. The mvcc-deletion
    // mark will be sent to the GC worker only if `mvcc_deletion_overlaps` is 0. It's
    // a little optimization to reduce modifications on write CF.
    mvcc_deletion_overlaps: Option<usize>,
    regions_provider: (u64, Arc<dyn RegionInfoProvider>),

    mvcc_key_prefix: Vec<u8>,
    remove_older: bool,

    // Some metrics about implementation detail.
    versions: usize,
    filtered: usize,
    total_versions: usize,
    total_filtered: usize,
    mvcc_rollback_and_locks: usize,
    orphan_versions: usize,
    versions_hist: LocalHistogramVec,
    filtered_hist: LocalHistogramVec,

    #[cfg(any(test, feature = "failpoints"))]
    callbacks_on_drop: Vec<Arc<dyn Fn(&WriteCompactionFilter) + Send + Sync>>,

    keyspace_level_gc_service: Arc<Option<KeyspaceLevelGCService>>,
}

impl WriteCompactionFilter {
    fn new(
        engine: Option<RocksEngine>,
        safe_point: u64,
        context: &CompactionFilterContext,
        gc_scheduler: Scheduler<GcTask<RocksEngine>>,
        regions_provider: (u64, Arc<dyn RegionInfoProvider>),
        keyspace_level_gc_service: Arc<Option<KeyspaceLevelGCService>>,
    ) -> Self {
        // Safe point must have been initialized.
        let mut is_all_ks_not_init_gc_sp = true;
        if let Some(ref ks_meta_service) = *keyspace_level_gc_service {
            is_all_ks_not_init_gc_sp =
                ks_meta_service.is_all_keyspace_level_gc_have_not_initialized()
        }
        assert!(safe_point > 0 || !is_all_ks_not_init_gc_sp);
        debug!("gc in compaction filter"; "safe_point" => safe_point);

        let write_batch = DeleteBatch::new(&engine);
        WriteCompactionFilter {
            safe_point,
            engine,
            is_bottommost_level: context.is_bottommost_level(),
            encountered_errors: false,

            write_batch,
            gc_scheduler,
            mvcc_deletions: Vec::with_capacity(DEFAULT_DELETE_BATCH_COUNT),
            mvcc_deletion_overlaps: None,
            regions_provider,

            mvcc_key_prefix: vec![],
            remove_older: false,

            versions: 0,
            filtered: 0,
            total_versions: 0,
            total_filtered: 0,
            mvcc_rollback_and_locks: 0,
            orphan_versions: 0,
            versions_hist: MVCC_VERSIONS_HISTOGRAM.local(),
            filtered_hist: GC_DELETE_VERSIONS_HISTOGRAM.local(),
            #[cfg(any(test, feature = "failpoints"))]
            callbacks_on_drop: {
                let ctx = GC_CONTEXT.lock().unwrap();
                ctx.as_ref().unwrap().callbacks_on_drop.clone()
            },
            keyspace_level_gc_service,
        }
    }

    // `log_on_error` indicates whether to print an error log on scheduling
    // failures. It's only enabled for `GcTask::OrphanVersions`.
    fn schedule_gc_task(&self, task: GcTask<RocksEngine>, log_on_error: bool) {
        match self.gc_scheduler.schedule(task) {
            Ok(_) => {}
            Err(e) => {
                if log_on_error {
                    error!("compaction filter schedule {} fail", e);
                }
                match e {
                    ScheduleError::Full(_) => {
                        GC_COMPACTION_FAILURE
                            .with_label_values(&[STAT_TXN_KEYMODE, "full"])
                            .inc();
                    }
                    ScheduleError::Stopped(_) => {
                        GC_COMPACTION_FAILURE
                            .with_label_values(&[STAT_TXN_KEYMODE, "stopped"])
                            .inc();
                    }
                }
            }
        }
    }

    fn handle_bottommost_delete(&mut self) {
        // Valid MVCC records should begin with `DATA_PREFIX`.
        debug_assert_eq!(self.mvcc_key_prefix[0], keys::DATA_PREFIX);
        let key = Key::from_encoded_slice(&self.mvcc_key_prefix[1..]);
        self.mvcc_deletions.push(key);
    }

    fn gc_mvcc_deletions(&mut self) {
        if !self.mvcc_deletions.is_empty() {
            let empty = Vec::with_capacity(DEFAULT_DELETE_BATCH_COUNT);
            let task = GcTask::GcKeys {
                keys: mem::replace(&mut self.mvcc_deletions, empty),
                safe_point: self.safe_point.into(),
                region_info_provider: self.regions_provider.1.clone(),
            };
            self.schedule_gc_task(task, false);
        }
    }

    fn do_filter(
        &mut self,
        _start_level: usize,
        key: &[u8],
        _sequence: u64,
        value: &[u8],
        value_type: CompactionFilterValueType,
    ) -> Result<CompactionFilterDecision, String> {
        let (mvcc_key_prefix, commit_ts) = split_ts(key)?;

        // `keyspace_level_gc_service` will be None when run Api V1 ut.
        if let Some(keyspace_level_gc_service) = self.keyspace_level_gc_service.as_ref() {
            self.safe_point = keyspace_level_gc_service
                .get_gc_safe_point_by_key(self.safe_point, keys::origin_key(key));
        }

        if commit_ts > self.safe_point || value_type != CompactionFilterValueType::Value {
            return Ok(CompactionFilterDecision::Keep);
        }

        self.versions += 1;
        if self.mvcc_key_prefix != mvcc_key_prefix {
            if self.mvcc_deletion_overlaps.take() == Some(0) {
                self.handle_bottommost_delete();
                if self.mvcc_deletions.len() >= DEFAULT_DELETE_BATCH_COUNT {
                    self.gc_mvcc_deletions();
                }
            }
            self.switch_key_metrics();
            self.mvcc_key_prefix.clear();
            self.mvcc_key_prefix.extend_from_slice(mvcc_key_prefix);
            self.remove_older = false;
        } else if let Some(ref mut overlaps) = self.mvcc_deletion_overlaps {
            *overlaps += 1;
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
                        self.mvcc_deletion_overlaps = Some(0);
                        GC_COMPACTION_FILTER_MVCC_DELETION_MET
                            .with_label_values(&[STAT_TXN_KEYMODE])
                            .inc();
                    }
                }
            }
        }

        if !filtered {
            return Ok(CompactionFilterDecision::Keep);
        }
        self.filtered += 1;
        self.handle_filtered_write(write)?;
        self.flush_pending_writes_if_need(false /* force */)?;
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

    fn handle_filtered_write(&mut self, write: WriteRef<'_>) -> Result<(), String> {
        if write.short_value.is_none() && write.write_type == WriteType::Put {
            self.write_batch
                .delete(&self.mvcc_key_prefix, write.start_ts)?;
        }
        Ok(())
    }

    fn flush_pending_writes_if_need(&mut self, force: bool) -> Result<(), engine_traits::Error> {
        if self.write_batch.is_empty() {
            return Ok(());
        }

        fn do_flush(
            wb: &mut RocksWriteBatchVec,
            wopts: &WriteOptions,
        ) -> Result<(), engine_traits::Error> {
            let _io_type_guard = WithIoType::new(IoType::Gc);
            fail_point!("write_compaction_filter_flush_write_batch", true, |_| {
                Err(engine_traits::Error::Engine(
                    engine_traits::Status::with_error(
                        engine_traits::Code::IoError,
                        "Ingested fail point",
                    ),
                ))
            });
            wb.write_opt(wopts)?;
            Ok(())
        }

        if self.write_batch.count() > DEFAULT_DELETE_BATCH_COUNT || force {
            let err = match &mut self.write_batch.batch {
                Either::Left(wb) => {
                    let mut wopts = WriteOptions::default();
                    wopts.set_no_slowdown(true);
                    match do_flush(wb, &wopts) {
                        Ok(()) => {
                            wb.clear();
                            return Ok(());
                        }
                        Err(e) => Some(e),
                    }
                }
                Either::Right(_) => None,
            };

            let wb = mem::replace(&mut self.write_batch, DeleteBatch::new(&self.engine));
            self.orphan_versions += wb.count();
            let id = ORPHAN_VERSIONS_ID.fetch_add(1, Ordering::Relaxed);
            let region_info_provider = self.regions_provider.1.clone();
            let task = GcTask::OrphanVersions {
                wb,
                id,
                region_info_provider,
            };
            if let Some(e) = &err {
                warn!(
                    "compaction filter flush fail, dispatch to gc worker";
                    "task" => %task, "err" => ?e,
                );
            }
            self.schedule_gc_task(task, true);
            if let Some(err) = err {
                return Err(err);
            }
        }
        Ok(())
    }

    fn switch_key_metrics(&mut self) {
        if self.versions != 0 {
            self.versions_hist
                .with_label_values(&[STAT_TXN_KEYMODE])
                .observe(self.versions as f64);
            self.total_versions += self.versions;
            self.versions = 0;
        }
        if self.filtered != 0 {
            self.filtered_hist
                .with_label_values(&[STAT_TXN_KEYMODE])
                .observe(self.filtered as f64);
            self.total_filtered += self.filtered;
            self.filtered = 0;
        }
    }

    fn flush_metrics(&self) {
        GC_COMPACTION_FILTERED
            .with_label_values(&[STAT_TXN_KEYMODE])
            .inc_by(self.total_filtered as u64);
        GC_COMPACTION_MVCC_ROLLBACK
            .with_label_values(&[STAT_TXN_KEYMODE])
            .inc_by(self.mvcc_rollback_and_locks as u64);
        GC_COMPACTION_FILTER_ORPHAN_VERSIONS
            .with_label_values(&[STAT_TXN_KEYMODE, "generated"])
            .inc_by(self.orphan_versions as u64);
        if let Some((versions, filtered)) = STATS.with(|stats| {
            stats.versions.update(|x| x + self.total_versions);
            stats.filtered.update(|x| x + self.total_filtered);
            if stats.need_report() {
                return Some(stats.prepare_report());
            }
            None
        }) {
            if filtered > 0 {
                info!("Compaction filter reports"; "total" => versions, "filtered" => filtered);
            }
        }
    }
}

pub struct CompactionFilterStats {
    pub versions: Cell<usize>, // Total stale versions meet by compaction filters.
    pub filtered: Cell<usize>, // Filtered versions by compaction filters.
    pub last_report: Cell<Instant>,
}
impl CompactionFilterStats {
    pub fn need_report(&self) -> bool {
        self.versions.get() >= 1024 * 1024 // 1M versions.
            || self.last_report.get().saturating_elapsed() >= Duration::from_secs(60)
    }

    pub fn prepare_report(&self) -> (usize, usize) {
        let versions = self.versions.replace(0);
        let filtered = self.filtered.replace(0);
        self.last_report.set(Instant::now());
        (versions, filtered)
    }
}
impl Default for CompactionFilterStats {
    fn default() -> Self {
        CompactionFilterStats {
            versions: Cell::new(0),
            filtered: Cell::new(0),
            last_report: Cell::new(Instant::now()),
        }
    }
}

thread_local! {
    static STATS: CompactionFilterStats = CompactionFilterStats::default();
}

impl Drop for WriteCompactionFilter {
    // NOTE: it's required that `CompactionFilter` is dropped before the compaction
    // result becomes installed into the DB instance.
    fn drop(&mut self) {
        if self.mvcc_deletion_overlaps.take() == Some(0) {
            self.handle_bottommost_delete();
        }
        self.gc_mvcc_deletions();

        if let Err(e) = self.flush_pending_writes_if_need(true) {
            error!("compaction filter flush writes fail"; "err" => ?e);
        }
        if let Some(engine) = &self.engine {
            engine.sync_wal().unwrap();
        }

        self.switch_key_metrics();
        self.flush_metrics();

        #[cfg(any(test, feature = "failpoints"))]
        for callback in &self.callbacks_on_drop {
            callback(self);
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
                GC_COMPACTION_FAILURE
                    .with_label_values(&[STAT_TXN_KEYMODE, "filter"])
                    .inc();
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

fn parse_write(value: &[u8]) -> Result<WriteRef<'_>, String> {
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

pub fn check_need_gc(
    safe_point: TimeStamp,
    ratio_threshold: f64,
    context: &CompactionFilterContext,
    keyspace_level_gc_service: Arc<Option<KeyspaceLevelGCService>>,
) -> bool {
    let check_props = |props: &MvccProperties| -> (bool, bool /* skip_more_checks */) {
        // Disable GC directly once the config is negative or +inf.
        // Disabling GC is useful in some abnormal scenarios where the transaction model
        // would be break (e.g. writes with higher commit TS would be written BEFORE
        // writes with lower commit TS, or write data with TS lower than current GC safe
        // point). Use this at your own risk.
        if ratio_threshold.is_sign_negative() || ratio_threshold.is_infinite() {
            return (false, false);
        }

        // Check is there any keyspace level gc safe point >= props.min_ts, the
        // following check should proceed.
        let mut any_ks_gc_sp_ge_than_props_min_ts = false;
        if let Some(ref ks_meta_service) = *keyspace_level_gc_service {
            let max_all_ks_gc_sp = ks_meta_service.get_max_ts_of_all_ks_gc_safe_point();
            debug!("check props.min_ts and max ts of all keyspace level gc safe point";
                "props.min_ts" => %props.min_ts,
                "max_gc_sp_of_all_ks" => %max_all_ks_gc_sp,
            );
            if props.min_ts <= max_all_ks_gc_sp.into() {
                any_ks_gc_sp_ge_than_props_min_ts = true;
            }
        }
        if props.min_ts > safe_point && !any_ks_gc_sp_ge_than_props_min_ts {
            debug!("skip gc in compaction filter because of props.min_ts greater than all gc safe point";
                "props.min_ts" => %props.min_ts,
                "global_gc_safe_point" => %safe_point,
                "any_ks_gc_sp_ge_than_props_min_ts" =>%any_ks_gc_sp_ge_than_props_min_ts,
            );
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

        // When comparing `num_versions` with `num_puts`, trait internal levels
        // specially because MVCC-deletion marks can't be handled at those
        // levels.
        let num_rollback_and_locks = (props.num_versions - props.num_deletes) as f64;
        if num_rollback_and_locks > props.num_puts as f64 * ratio_threshold {
            return (true, false);
        }
        (props.max_row_versions > 1024, false)
    };

    let (mut sum_props, mut needs_gc) = (MvccProperties::new(), 0);
    for i in 0..context.file_numbers().len() {
        let table_props = context.table_properties(i);
        let user_props = table_props.user_collected_properties();
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

#[allow(dead_code)] // Some interfaces are not used with different compile options.
#[cfg(any(test, feature = "failpoints"))]
pub mod test_utils {
    use engine_rocks::{
        raw::{CompactOptions, CompactionOptions},
        util::get_cf_handle,
        RocksEngine,
    };
    use engine_traits::{SyncMutable, CF_DEFAULT, CF_WRITE};
    use raftstore::coprocessor::region_info_accessor::MockRegionInfoProvider;
    use tikv_util::{
        config::VersionTrack,
        worker::{dummy_scheduler, ReceiverWrapper},
    };

    use super::*;
    use crate::storage::kv::RocksEngine as StorageRocksEngine;

    /// Do a global GC with the given safe point.
    pub fn gc_by_compact(engine: &mut StorageRocksEngine, _: &[u8], safe_point: u64) {
        let engine = engine.get_rocksdb();
        // Put a new key-value pair to ensure compaction can be triggered correctly.
        engine.delete_cf("write", b"znot-exists-key").unwrap();

        TestGcRunner::new(safe_point).gc(&engine);
    }

    lazy_static! {
        // Use a lock to protect concurrent compactions.
        static ref LOCK: Mutex<()> = std::sync::Mutex::new(());
    }

    pub fn default_compact_options() -> CompactOptions {
        let mut compact_opts = CompactOptions::new();
        compact_opts.set_exclusive_manual_compaction(false);
        compact_opts.set_max_subcompactions(1);
        compact_opts
    }

    pub struct TestGcRunner<'a> {
        pub safe_point: u64,
        pub ratio_threshold: Option<f64>,
        pub start: Option<&'a [u8]>,
        pub end: Option<&'a [u8]>,
        pub target_level: Option<usize>,
        pub gc_scheduler: Scheduler<GcTask<RocksEngine>>,
        pub gc_receiver: ReceiverWrapper<GcTask<RocksEngine>>,
        pub(super) callbacks_on_drop: Vec<Arc<dyn Fn(&WriteCompactionFilter) + Send + Sync>>,
        pub keyspace_level_gc_service: Arc<Option<KeyspaceLevelGCService>>,
    }

    impl<'a> TestGcRunner<'a> {
        pub fn new(safe_point: u64) -> Self {
            let (gc_scheduler, gc_receiver) = dummy_scheduler();

            TestGcRunner {
                safe_point,
                ratio_threshold: None,
                start: None,
                end: None,
                target_level: None,
                gc_scheduler,
                gc_receiver,
                callbacks_on_drop: vec![],
                keyspace_level_gc_service: Arc::new(None),
            }
        }
    }

    impl<'a> TestGcRunner<'a> {
        pub fn safe_point(&mut self, sp: u64) -> &mut Self {
            self.safe_point = sp;
            self
        }

        pub fn prepare_gc(&self, engine: &RocksEngine) {
            let safe_point = Arc::new(AtomicU64::new(self.safe_point));
            let cfg_tracker = {
                let mut cfg = GcConfig::default();
                if let Some(ratio_threshold) = self.ratio_threshold {
                    cfg.ratio_threshold = ratio_threshold;
                }
                cfg.enable_compaction_filter = true;
                GcWorkerConfigManager(Arc::new(VersionTrack::new(cfg)), None)
            };
            let feature_gate = {
                let feature_gate = FeatureGate::default();
                feature_gate.set_version("5.0.0").unwrap();
                feature_gate
            };

            let mut gc_context_opt = GC_CONTEXT.lock().unwrap();
            *gc_context_opt = Some(GcContext {
                db: Some(engine.clone()),
                store_id: 1,
                safe_point,
                cfg_tracker,
                feature_gate,
                gc_scheduler: self.gc_scheduler.clone(),
                region_info_provider: Arc::new(MockRegionInfoProvider::new(vec![])),
                callbacks_on_drop: self.callbacks_on_drop.clone(),
                keyspace_level_gc_service: self.keyspace_level_gc_service.clone(),
            });
        }

        pub fn post_gc(&mut self) {
            self.callbacks_on_drop.clear();
            let mut gc_context = GC_CONTEXT.lock().unwrap();
            let callbacks = &mut gc_context.as_mut().unwrap().callbacks_on_drop;
            callbacks.clear();
        }

        pub fn gc(&mut self, engine: &RocksEngine) {
            let _guard = LOCK.lock().unwrap();
            self.prepare_gc(engine);

            let db = engine.as_inner();
            let handle = get_cf_handle(db, CF_WRITE).unwrap();
            let mut compact_opts = default_compact_options();
            if let Some(target_level) = self.target_level {
                compact_opts.set_change_level(true);
                compact_opts.set_target_level(target_level as i32);
            }
            db.compact_range_cf_opt(handle, &compact_opts, self.start, self.end);
            self.post_gc();
        }

        pub fn gc_raw(&mut self, engine: &RocksEngine) {
            let _guard = LOCK.lock().unwrap();
            self.prepare_gc(engine);

            let db = engine.as_inner();
            let handle = get_cf_handle(db, CF_DEFAULT).unwrap();
            let mut compact_opts = default_compact_options();
            if let Some(target_level) = self.target_level {
                compact_opts.set_change_level(true);
                compact_opts.set_target_level(target_level as i32);
            }
            db.compact_range_cf_opt(handle, &compact_opts, None, None);
            self.post_gc();
        }

        pub fn gc_on_files(&mut self, engine: &RocksEngine, input_files: &[String], cf: &str) {
            let _guard = LOCK.lock().unwrap();
            self.prepare_gc(engine);
            let db = engine.as_inner();
            let handle = get_cf_handle(db, cf).unwrap();
            let level = self.target_level.unwrap() as i32;
            db.compact_files_cf(handle, &CompactionOptions::new(), input_files, level)
                .unwrap();
            self.post_gc();
        }
    }

    pub fn rocksdb_level_files(engine: &RocksEngine, cf: &str) -> Vec<Vec<String>> {
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

    pub fn rocksdb_level_file_counts(engine: &RocksEngine, cf: &str) -> Vec<usize> {
        let cf_handle = get_cf_handle(engine.as_inner(), cf).unwrap();
        let metadata = engine.as_inner().get_column_family_meta_data(cf_handle);
        let mut res = Vec::with_capacity(7);
        for level_meta in metadata.get_levels() {
            res.push(level_meta.get_files().len());
        }
        res
    }
}

#[cfg(test)]
pub mod tests {
    use engine_traits::{DeleteStrategy, MiscExt, Peekable, Range, SyncMutable, CF_WRITE};

    use super::{test_utils::*, *};
    use crate::{
        config::DbConfig,
        storage::{
            kv::TestEngineBuilder,
            mvcc::tests::{must_get, must_get_none},
            txn::tests::{must_commit, must_prewrite_delete, must_prewrite_put},
        },
    };

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
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let raw_engine = engine.get_rocksdb();
        let value = vec![b'v'; 512];
        let mut gc_runner = TestGcRunner::new(0);

        // GC can't delete keys after the given safe point.
        must_prewrite_put(&mut engine, b"zkey", &value, b"zkey", 100);
        must_commit(&mut engine, b"zkey", 100, 110);
        gc_runner.safe_point(50).gc(&raw_engine);
        must_get(&mut engine, b"zkey", 110, &value);

        // GC can't delete keys before the safe ponit if they are latest versions.
        gc_runner.safe_point(200).gc(&raw_engine);
        must_get(&mut engine, b"zkey", 110, &value);

        must_prewrite_put(&mut engine, b"zkey", &value, b"zkey", 120);
        must_commit(&mut engine, b"zkey", 120, 130);

        // GC can't delete the latest version before the safe ponit.
        gc_runner.safe_point(115).gc(&raw_engine);
        must_get(&mut engine, b"zkey", 110, &value);

        // GC a version will also delete the key on default CF.
        gc_runner.safe_point(200).gc(&raw_engine);
        must_get_none(&mut engine, b"zkey", 110);
        let default_key = Key::from_encoded_slice(b"zkey").append_ts(100.into());
        let default_key = default_key.into_encoded();
        assert!(raw_engine.get_value(&default_key).unwrap().is_none());

        // If the ratio threshold is less than 0, GC would be skipped.
        must_prewrite_put(&mut engine, b"zkey", &value, b"zkey", 210);
        must_commit(&mut engine, b"zkey", 210, 220);
        gc_runner.ratio_threshold = Some(-1.0);
        gc_runner.safe_point(256).gc(&raw_engine);
        must_get(&mut engine, b"zkey", 210, &value);
    }

    // Test dirty versions before a deletion mark can be handled correctly.
    #[test]
    fn test_compaction_filter_handle_deleting() {
        let value = vec![b'v'; 512];
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let raw_engine = engine.get_rocksdb();
        let mut gc_runner = TestGcRunner::new(0);

        let mut gc_and_check = |expect_tasks: bool, prefix: &[u8]| {
            gc_runner.safe_point(500).gc(&raw_engine);

            // Wait up to 1 second, and treat as no task if timeout.
            if let Ok(Some(task)) = gc_runner.gc_receiver.recv_timeout(Duration::new(1, 0)) {
                assert!(expect_tasks, "unexpected GC task");
                match task {
                    GcTask::GcKeys { keys, .. } => {
                        assert_eq!(keys.len(), 1);
                        let got = keys[0].as_encoded();
                        let expect = Key::from_raw(prefix);
                        assert_eq!(got, &expect.as_encoded()[1..]);
                    }
                    _ => unreachable!(),
                }
                return;
            }
            assert!(!expect_tasks, "no GC task after 1 second");
        };

        // No key switch after the deletion mark.
        must_prewrite_put(&mut engine, b"zkey", &value, b"zkey", 100);
        must_commit(&mut engine, b"zkey", 100, 110);
        must_prewrite_delete(&mut engine, b"zkey", b"zkey", 120);
        must_commit(&mut engine, b"zkey", 120, 130);

        // No GC task should be emit because the mvcc-deletion mark covers some older
        // versions.
        gc_and_check(false, b"zkey");
        // A GC task should be emit after older versions are cleaned.
        gc_and_check(true, b"zkey");

        // Clean the engine, prepare for later tests.
        raw_engine
            .delete_ranges_cf(
                &WriteOptions::default(),
                CF_WRITE,
                DeleteStrategy::DeleteFiles,
                &[Range::new(b"z", b"zz")],
            )
            .unwrap();

        // Key switch after the deletion mark.
        must_prewrite_put(&mut engine, b"zkey1", &value, b"zkey1", 200);
        must_commit(&mut engine, b"zkey1", 200, 210);
        must_prewrite_delete(&mut engine, b"zkey1", b"zkey1", 220);
        must_commit(&mut engine, b"zkey1", 220, 230);
        must_prewrite_put(&mut engine, b"zkey2", &value, b"zkey2", 220);
        must_commit(&mut engine, b"zkey2", 220, 230);

        // No GC task should be emit because the mvcc-deletion mark covers some older
        // versions.
        gc_and_check(false, b"zkey1");
        // A GC task should be emit after older versions are cleaned.
        gc_and_check(true, b"zkey1");
    }

    // Test if there are not enought garbage in SST files involved by a compaction,
    // no compaction filter will be created.
    #[test]
    fn test_mvcc_properties() {
        let mut cfg = DbConfig::default();
        cfg.writecf.disable_auto_compactions = true;
        cfg.writecf.dynamic_level_bytes = false;
        let dir = tempfile::TempDir::new().unwrap();
        let builder = TestEngineBuilder::new().path(dir.path());
        let mut engine = builder.build_with_cfg(&cfg).unwrap();
        let raw_engine = engine.get_rocksdb();
        let value = vec![b'v'; 512];
        let mut gc_runner = TestGcRunner::new(0);

        for start_ts in &[100, 110, 120, 130] {
            must_prewrite_put(&mut engine, b"zkey", &value, b"zkey", *start_ts);
            must_commit(&mut engine, b"zkey", *start_ts, *start_ts + 5);
        }
        must_prewrite_delete(&mut engine, b"zkey", b"zkey", 140);
        must_commit(&mut engine, b"zkey", 140, 145);

        // Can't perform GC because the min timestamp is greater than safe point.
        gc_runner
            .callbacks_on_drop
            .push(Arc::new(|_: &WriteCompactionFilter| {
                unreachable!();
            }));
        gc_runner.target_level = Some(6);
        gc_runner.safe_point(100).gc(&raw_engine);

        // Can perform GC at the bottommost level even if the threshold can't be
        // reached.
        gc_runner.ratio_threshold = Some(10.0);
        gc_runner.target_level = Some(6);
        gc_runner.safe_point(140).gc(&raw_engine);
        for commit_ts in &[105, 115, 125] {
            must_get_none(&mut engine, b"zkey", commit_ts);
        }

        // Put an extra key to make the memtable overlap with the bottommost one.
        must_prewrite_put(&mut engine, b"zkey1", &value, b"zkey1", 200);
        must_commit(&mut engine, b"zkey1", 200, 205);
        for start_ts in &[200, 210, 220, 230] {
            must_prewrite_put(&mut engine, b"zkey", &value, b"zkey", *start_ts);
            must_commit(&mut engine, b"zkey", *start_ts, *start_ts + 5);
        }
        must_prewrite_delete(&mut engine, b"zkey", b"zkey", 240);
        must_commit(&mut engine, b"zkey", 240, 245);
        raw_engine.flush_cf(CF_WRITE, true).unwrap();

        // At internal levels can't perform GC because the threshold is not reached.
        let level_files = rocksdb_level_files(&raw_engine, CF_WRITE);
        let l0_file = dir.path().join(&level_files[0][0]);
        let files = &[l0_file.to_str().unwrap().to_owned()];
        gc_runner.target_level = Some(5);
        gc_runner.ratio_threshold = Some(10.0);
        gc_runner
            .safe_point(300)
            .gc_on_files(&raw_engine, files, CF_WRITE);
        for commit_ts in &[205, 215, 225, 235] {
            must_get(&mut engine, b"zkey", commit_ts, &value);
        }
    }

    // If we use `CompactionFilterDecision::RemoveAndSkipUntil` in compaction
    // filters, deletion marks can only be handled in the bottommost level.
    // Otherwise dirty versions could be exposed incorrectly.
    //
    // This case tests that deletion marks won't be handled at internal levels, and
    // at the bottommost levels, dirty versions still can't be exposed.
    #[test]
    fn test_remove_and_skip_until() {
        let mut cfg = DbConfig::default();
        cfg.writecf.disable_auto_compactions = true;
        cfg.writecf.dynamic_level_bytes = false;

        let dir = tempfile::TempDir::new().unwrap();
        let builder = TestEngineBuilder::new().path(dir.path());
        let mut engine = builder.build_with_cfg(&cfg).unwrap();
        let raw_engine = engine.get_rocksdb();
        let mut gc_runner = TestGcRunner::new(0);

        // So the construction of SST files will be:
        // L6: |key_110|
        must_prewrite_put(&mut engine, b"zkey", b"zvalue", b"zkey", 100);
        must_commit(&mut engine, b"zkey", 100, 110);
        gc_runner.target_level = Some(6);
        gc_runner.safe_point(50).gc(&raw_engine);
        assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[6], 1);

        // So the construction of SST files will be:
        // L0: |key_130, key_110|
        // L6: |key_110|
        must_prewrite_delete(&mut engine, b"zkey", b"zkey", 120);
        must_commit(&mut engine, b"zkey", 120, 130);
        let k_110 = Key::from_raw(b"zkey").append_ts(110.into()).into_encoded();
        raw_engine.delete_cf(CF_WRITE, &k_110).unwrap();
        raw_engine.flush_cf(CF_WRITE, true).unwrap();
        assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[0], 1);
        assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[6], 1);

        // Compact the mvcc deletion mark to L5, the stale version shouldn't be exposed.
        let level_files = rocksdb_level_files(&raw_engine, CF_WRITE);
        let l0_file = dir.path().join(&level_files[0][0]);
        let files = &[l0_file.to_str().unwrap().to_owned()];
        gc_runner.target_level = Some(5);
        gc_runner
            .safe_point(200)
            .gc_on_files(&raw_engine, files, CF_WRITE);
        assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[5], 1);
        assert_eq!(rocksdb_level_file_counts(&raw_engine, CF_WRITE)[6], 1);
        must_get_none(&mut engine, b"zkey", 200);

        // Compact the mvcc deletion mark to L6, the stale version shouldn't be exposed.
        gc_runner.target_level = Some(6);
        gc_runner.safe_point(200).gc(&raw_engine);
        must_get_none(&mut engine, b"zkey", 200);
    }
}
