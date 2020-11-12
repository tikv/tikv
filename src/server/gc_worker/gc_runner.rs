// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use pd_client::{ClusterVersion, PdClient};
use std::cmp::Ordering;
use std::f64::INFINITY;
use std::fmt::{self, Display, Formatter};
use std::mem;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use tikv_util::config::Tracker;
use tikv_util::time::{duration_to_sec, Limiter, SlowTimer};
use tikv_util::worker::{Runnable, RunnableWithTimer};
use txn_types::{Key, TimeStamp};

use raftstore::coprocessor::Result as CopResult;
use raftstore::coprocessor::{RegionInfo, RegionInfoProvider, SeekRegionCallback};
use raftstore::store::msg::StoreMsg;
use raftstore::store::util::find_peer;
use raftstore::store::StoreRouter;

use concurrency_manager::ConcurrencyManager;
use engine_traits::{DeleteStrategy, MiscExt, Range, CF_DEFAULT, CF_LOCK, CF_WRITE};
use futures::executor::block_on;
use kvproto::kvrpcpb::{Context, IsolationLevel, LockInfo};

use crate::server::metrics::*;
use crate::storage::kv::{Engine, ScanMode, Statistics};
use crate::storage::mvcc::{check_need_gc, Error as MvccError, GcInfo, MvccReader, MvccTxn};

use super::config::{GcConfig, GcWorkerConfigManager};
use super::is_compaction_filter_allowd;
use super::{Callback, Error, Result};

const POLL_SAFE_POINT_INTERVAL_SECS: u64 = 60;

/// After the GC scan of a key, output a message to the log if there are at least this many
/// versions of the key.
const GC_LOG_FOUND_VERSION_THRESHOLD: usize = 30;

/// After the GC delete versions of a key, output a message to the log if at least this many
/// versions are deleted.
const GC_LOG_DELETED_VERSION_THRESHOLD: usize = 30;

const GC_TASK_SLOW_SECONDS: u64 = 30;

const BEGIN_KEY: &[u8] = b"";

const PROCESS_TYPE_GC: &str = "gc";
const PROCESS_TYPE_SCAN: &str = "scan";

/// The configurations of automatic GC.
pub struct AutoGcConfig {
    /// Used to find which peer of a region is on this TiKV, so that we can compose a `Context`.
    pub self_store_id: u64,

    pub poll_safe_point_interval: Duration,

    /// If this is set, safe_point will be checked before doing GC on every region while working.
    /// Otherwise safe_point will be only checked when `poll_safe_point_interval` has past since
    /// last checking.
    pub always_check_safe_point: bool,

    /// This will be called when a round of GC has finished and goes back to idle state.
    /// This field is for test purpose.
    pub post_a_round_of_gc: Option<Box<dyn Fn() + Send>>,
}

impl AutoGcConfig {
    /// Creates a new config.
    pub fn new(self_store_id: u64) -> Self {
        Self {
            self_store_id,
            poll_safe_point_interval: Duration::from_secs(POLL_SAFE_POINT_INTERVAL_SECS),
            always_check_safe_point: false,
            post_a_round_of_gc: None,
        }
    }

    /// Creates a config for test purpose. The interval to poll safe point is as short as 0.1s and
    /// during GC it never skips checking safe point.
    pub fn new_test_cfg(self_store_id: u64) -> Self {
        Self {
            self_store_id,
            poll_safe_point_interval: Duration::from_millis(100),
            always_check_safe_point: true,
            post_a_round_of_gc: None,
        }
    }
}

/// Used to represent the state of `GcManager`.
#[derive(PartialEq)]
enum GcManagerState {
    None,
    Init,
    Idle,
    Working,
}

impl GcManagerState {
    pub fn tag(&self) -> &str {
        match self {
            GcManagerState::None => "",
            GcManagerState::Init => "initializing",
            GcManagerState::Idle => "idle",
            GcManagerState::Working => "working",
        }
    }
}

#[inline]
fn set_status_metrics(state: GcManagerState) {
    for s in &[
        GcManagerState::Init,
        GcManagerState::Idle,
        GcManagerState::Working,
    ] {
        AUTO_GC_STATUS_GAUGE_VEC
            .with_label_values(&[s.tag()])
            .set(if state == *s { 1 } else { 0 });
    }
}

pub enum GcTask {
    Gc {
        region_id: u64,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        safe_point: TimeStamp,
        callback: Option<Callback<()>>,
    },
    UnsafeDestroyRange {
        ctx: Context,
        start_key: Key,
        end_key: Key,
        callback: Callback<()>,
    },
    PhysicalScanLock {
        ctx: Context,
        max_ts: TimeStamp,
        start_key: Key,
        limit: usize,
        callback: Callback<Vec<LockInfo>>,
    },
    #[cfg(any(test, feature = "testexport"))]
    Validate(Box<dyn FnOnce(&GcConfig, &Limiter) + Send>),
}

impl GcTask {
    pub fn get_enum_label(&self) -> GcCommandKind {
        match self {
            GcTask::Gc { .. } => GcCommandKind::gc,
            GcTask::UnsafeDestroyRange { .. } => GcCommandKind::unsafe_destroy_range,
            GcTask::PhysicalScanLock { .. } => GcCommandKind::physical_scan_lock,
            #[cfg(any(test, feature = "testexport"))]
            GcTask::Validate(_) => GcCommandKind::validate_config,
        }
    }
}

pub trait Executor: Send {
    fn execute(&mut self, task: GcTask);
}

/// Used to perform GC operations on the engine.
pub struct GcExecutor<E: Engine> {
    engine: E,

    raft_store_router: Box<dyn StoreRouter>,

    /// Used to limit the write flow of GC.
    limiter: Limiter,

    cfg: GcConfig,
    cfg_tracker: Tracker<GcConfig>,

    stats: Statistics,
}

/// Provides safe point.
pub trait GcSafePointProvider: Send + 'static {
    fn get_safe_point(&self) -> Result<TimeStamp>;
}

impl<T: PdClient + 'static> GcSafePointProvider for Arc<T> {
    fn get_safe_point(&self) -> Result<TimeStamp> {
        block_on(self.get_gc_safe_point())
            .map(Into::into)
            .map_err(|e| box_err!("failed to get safe point from PD: {:?}", e))
    }
}

pub struct MockSafePointProvider {
    pub rx: std::sync::mpsc::Receiver<TimeStamp>,
}

impl Default for MockSafePointProvider {
    fn default() -> Self {
        let (_, rx) = std::sync::mpsc::channel();
        Self { rx }
    }
}

impl GcSafePointProvider for MockSafePointProvider {
    fn get_safe_point(&self) -> Result<TimeStamp> {
        // Error will be ignored by `GcManager`, which is equivalent to that the safe_point
        // is not updated.
        self.rx.try_recv().map_err(|e| box_err!(e))
    }
}

#[derive(Clone, Default)]
pub struct MockRegionInfoProvider {
    // start_key -> (region_id, end_key)
    pub regions: std::collections::BTreeMap<Vec<u8>, RegionInfo>,
}

impl RegionInfoProvider for MockRegionInfoProvider {
    fn seek_region(&self, from: &[u8], callback: SeekRegionCallback) -> CopResult<()> {
        let from = from.to_vec();
        callback(&mut self.regions.range(from..).map(|(_, v)| v));
        Ok(())
    }
}

/// Controls how GC runs automatically on the TiKV.
/// It polls safe point periodically, and when the safe point is updated, `GcManager` will start to
/// scan all regions (whose leader is on this TiKV), and does GC on all those regions.
pub(super) struct GcRunner<R: RegionInfoProvider> {
    cfg: AutoGcConfig,
    safe_point_provider: Box<dyn GcSafePointProvider>,
    region_info_provider: R,

    /// The current safe point. `GcManager` will try to update it periodically. When `safe_point` is
    /// updated, `GCManager` will start to do GC on all regions.
    safe_point: Arc<AtomicU64>,

    safe_point_last_check_time: Instant,

    cfg_tracker: GcWorkerConfigManager,
    cluster_version: ClusterVersion,

    executor: Box<dyn Executor>,

    stop: Arc<AtomicBool>,
}

impl<R: RegionInfoProvider> GcRunner<R> {
    pub fn new(
        cfg: AutoGcConfig,
        safe_point_provider: Box<dyn GcSafePointProvider>,
        region_info_provider: R,
        safe_point: Arc<AtomicU64>,
        cfg_tracker: GcWorkerConfigManager,
        cluster_version: ClusterVersion,
        stop: Arc<AtomicBool>,
        executor: Box<dyn Executor>,
    ) -> GcRunner<R> {
        GcRunner {
            cfg,
            safe_point_provider,
            region_info_provider,
            safe_point,
            safe_point_last_check_time: Instant::now(),
            stop,
            cfg_tracker,
            cluster_version,
            executor,
        }
    }

    fn curr_safe_point(&self) -> TimeStamp {
        let ts = self.safe_point.load(AtomicOrdering::Relaxed);
        TimeStamp::new(ts)
    }

    fn save_safe_point(&self, ts: TimeStamp) {
        self.safe_point
            .store(ts.into_inner(), AtomicOrdering::Relaxed);
    }

    /// Sets the initial state of the `GCManger`.
    /// The only task of initializing is to simply get the current safe point as the initial value
    /// of `safe_point`. TiKV won't do any GC automatically until the first time `safe_point` was
    /// updated to a greater value than initial value.
    pub fn start(&mut self) {
        set_status_metrics(GcManagerState::Init);
        debug!("gc-manager is initializing");
        self.save_safe_point(TimeStamp::zero());
        self.try_update_safe_point();
        debug!("gc-manager started"; "safe_point" => self.curr_safe_point());
    }

    /// Tries to update the safe point. Returns true if safe point has been updated to a greater
    /// value. Returns false if safe point didn't change or we encountered an error.
    fn try_update_safe_point(&mut self) -> bool {
        self.safe_point_last_check_time = Instant::now();

        let safe_point = match self.safe_point_provider.get_safe_point() {
            Ok(res) => res,
            // Return false directly so we will check it a while later.
            Err(e) => {
                error!(?e; "failed to get safe point from pd");
                return false;
            }
        };

        let old_safe_point = self.curr_safe_point();
        match safe_point.cmp(&old_safe_point) {
            Ordering::Less => {
                panic!(
                    "got new safe point {} which is less than current safe point {}. \
                     there must be something wrong.",
                    safe_point, old_safe_point,
                );
            }
            Ordering::Equal => false,
            Ordering::Greater => {
                debug!("gc_worker: update safe point"; "safe_point" => safe_point);
                self.save_safe_point(safe_point);
                AUTO_GC_SAFE_POINT_GAUGE.set(safe_point.into_inner() as i64);
                true
            }
        }
    }

    /// Scans all regions on the TiKV whose leader is this TiKV, and does GC on all of them.
    /// Regions are scanned and GC-ed in lexicographical order.
    ///
    /// While the `gc_a_round` function is running, it will periodically check whether safe_point is
    /// updated before the function `gc_a_round` finishes. If so, *Rewinding* will occur. For
    /// example, when we just starts to do GC, our progress is like this: ('^' means our current
    /// progress)
    ///
    /// ```text
    /// | region 1 | region 2 | region 3| region 4 | region 5 | region 6 |
    /// ^
    /// ```
    ///
    /// And after a while, our GC progress is like this:
    ///
    /// ```text
    /// | region 1 | region 2 | region 3| region 4 | region 5 | region 6 |
    /// ----------------------^
    /// ```
    ///
    /// At this time we found that safe point was updated, so rewinding will happen. First we
    /// continue working to the end: ('#' indicates the position that safe point updates)
    ///
    /// ```text
    /// | region 1 | region 2 | region 3| region 4 | region 5 | region 6 |
    /// ----------------------#------------------------------------------^
    /// ```
    ///
    /// Then region 1-2 were GC-ed with the old safe point and region 3-6 were GC-ed with the new
    /// new one. Then, we *rewind* to the very beginning and continue GC to the position that safe
    /// point updates:
    ///
    /// ```text
    /// | region 1 | region 2 | region 3| region 4 | region 5 | region 6 |
    /// ----------------------#------------------------------------------^
    /// ----------------------^
    /// ```
    ///
    /// Then GC finishes.
    /// If safe point updates again at some time, it will still try to GC all regions with the
    /// latest safe point. If safe point always updates before `gc_a_round` finishes, `gc_a_round`
    /// may never stop, but it doesn't matter.
    fn gc_a_round(&mut self) {
        let mut need_rewind = false;
        // Represents where we should stop doing GC. `None` means the very end of the TiKV.
        let mut end = None;
        // Represents where we have GC-ed to. `None` means the very end of the TiKV.
        let mut progress = Some(Key::from_encoded(BEGIN_KEY.to_vec()));

        // Records how many region we have GC-ed.
        let mut processed_regions = 0;

        info!(
            "gc_worker: start auto gc"; "safe_point" => self.curr_safe_point()
        );

        // The following loop iterates all regions whose leader is on this TiKV and does GC on them.
        // At the same time, check whether safe_point is updated periodically. If it's updated,
        // rewinding will happen.
        loop {
            if self.stop.load(AtomicOrdering::Acquire) {
                return;
            }

            // Check the current GC progress and determine if we are going to rewind or we have
            // finished the round of GC.
            if need_rewind {
                if progress.is_none() {
                    // We have worked to the end and we need to rewind. Restart from beginning.
                    progress = Some(Key::from_encoded(BEGIN_KEY.to_vec()));
                    need_rewind = false;
                    info!(
                        "gc_worker: auto gc rewinds"; "processed_regions" => processed_regions
                    );

                    processed_regions = 0;
                    // Set the metric to zero to show that rewinding has happened.
                    AUTO_GC_PROCESSED_REGIONS_GAUGE_VEC
                        .with_label_values(&[PROCESS_TYPE_GC])
                        .set(0);
                    AUTO_GC_PROCESSED_REGIONS_GAUGE_VEC
                        .with_label_values(&[PROCESS_TYPE_SCAN])
                        .set(0);
                }
            } else {
                // We are not going to rewind, So we will stop if `progress` reaches `end`.
                let finished = match (progress.as_ref(), end.as_ref()) {
                    (None, _) => true,
                    (Some(p), Some(e)) => p >= e,
                    _ => false,
                };
                if finished {
                    // We have worked to the end of the TiKV or our progress has reached `end`, and we
                    // don't need to rewind. In this case, the round of GC has finished.
                    info!(
                        "gc_worker: finished auto gc"; "processed_regions" => processed_regions
                    );
                    return;
                }
            }

            assert!(progress.is_some());

            // Before doing GC, check whether safe_point is updated periodically to determine if
            // rewinding is needed.
            self.check_if_need_rewind(&progress, &mut need_rewind, &mut end);

            progress = self.gc_next_region(progress.unwrap(), &mut processed_regions);
        }
    }

    /// Checks whether we need to rewind in this round of GC. Only used in `gc_a_round`.
    fn check_if_need_rewind(
        &mut self,
        progress: &Option<Key>,
        need_rewind: &mut bool,
        end: &mut Option<Key>,
    ) {
        if self.safe_point_last_check_time.elapsed() < self.cfg.poll_safe_point_interval
            && !self.cfg.always_check_safe_point
        {
            // Skip this check.
            return;
        }

        if !self.try_update_safe_point() {
            // Safe point not updated. Skip it.
            return;
        }

        if progress.as_ref().unwrap().as_encoded().is_empty() {
            // `progress` is empty means the starting. We don't need to rewind. We just
            // continue GC to the end.
            *need_rewind = false;
            *end = None;
            info!(
                "gc_worker: auto gc will go to the end"; "safe_point" => self.curr_safe_point()
            );
        } else {
            *need_rewind = true;
            *end = progress.clone();
            info!(
                "gc_worker: auto gc will go to rewind"; "safe_point" => self.curr_safe_point(),
                "next_rewind_key" => %(end.as_ref().unwrap())
            );
        }
    }

    /// Does GC on the next region after `from_key`. Returns the end key of the region it processed.
    /// If we have processed to the end of all regions, returns `None`.
    fn gc_next_region(&mut self, from_key: Key, processed_regions: &mut usize) -> Option<Key> {
        // Get the information of the next region to do GC.
        let (range, next_key) = self.get_next_gc_context(from_key);
        let (region_id, start_key, end_key) = match range {
            Some((r, s, e)) => (r, s, e),
            None => return None,
        };

        let hex_start = hex::encode_upper(&start_key);
        let hex_end = hex::encode_upper(&end_key);
        debug!("trying gc"; "start_key" => &hex_start, "end_key" => &hex_end);
        self.executor.execute(GcTask::Gc {
            region_id,
            start_key,
            end_key,
            safe_point: self.curr_safe_point(),
            callback: None,
        });
        *processed_regions += 1;
        AUTO_GC_PROCESSED_REGIONS_GAUGE_VEC
            .with_label_values(&[PROCESS_TYPE_GC])
            .inc();

        next_key
    }

    /// Gets the next region with end_key greater than given key.
    /// Returns a tuple with 2 fields:
    /// the first is the next region can be sent to GC worker;
    /// the second is the next key which can be passed into this method later.
    #[allow(clippy::type_complexity)]
    fn get_next_gc_context(&mut self, key: Key) -> (Option<(u64, Vec<u8>, Vec<u8>)>, Option<Key>) {
        let (tx, rx) = mpsc::channel();
        let store_id = self.cfg.self_store_id;

        let res = self.region_info_provider.seek_region(
            key.as_encoded(),
            Box::new(move |iter| {
                let mut scanned_regions = 0;
                for info in iter {
                    scanned_regions += 1;
                    if find_peer(&info.region, store_id).is_some() {
                        let _ = tx.send((Some(info.region.clone()), scanned_regions));
                        return;
                    }
                }
                let _ = tx.send((None, scanned_regions));
            }),
        );

        if let Err(e) = res {
            error!(?e; "gc_worker: failed to get next region information");
            return (None, None);
        };

        let seek_region_res = rx.recv().map(|(region, scanned_regions)| {
            AUTO_GC_PROCESSED_REGIONS_GAUGE_VEC
                .with_label_values(&[PROCESS_TYPE_SCAN])
                .add(scanned_regions);
            region
        });

        match seek_region_res {
            Ok(Some(mut region)) => {
                let r = region.get_id();
                let (s, e) = (region.take_start_key(), region.take_end_key());
                let next_key = if e.is_empty() {
                    None
                } else {
                    Some(Key::from_encoded_slice(&e))
                };
                (Some((r, s, e)), next_key)
            }
            Ok(None) => (None, None),
            Err(e) => {
                error!("failed to get next region information"; "err" => ?e);
                (None, None)
            }
        }
    }
}

impl<R: RegionInfoProvider> Runnable for GcRunner<R> {
    type Task = GcTask;

    fn run(&mut self, task: GcTask) {
        self.executor.execute(task);
    }

    fn before_start(&mut self) {
        debug!("gc-manager is started");
    }

    fn shutdown(&mut self) {
        set_status_metrics(GcManagerState::None);
        debug!("gc-manager is stopped");
    }
}

impl<R: RegionInfoProvider> RunnableWithTimer for GcRunner<R> {
    fn on_timeout(&mut self) {
        if self.try_update_safe_point() {
            // Don't need to run GC any more if compaction filter is enabled.
            if !is_compaction_filter_allowd(&*self.cfg_tracker.value(), &self.cluster_version) {
                set_status_metrics(GcManagerState::Working);
                self.gc_a_round();
                if let Some(on_finished) = self.cfg.post_a_round_of_gc.as_ref() {
                    on_finished()
                }
            }
            AUTO_GC_PROCESSED_REGIONS_GAUGE_VEC
                .with_label_values(&[PROCESS_TYPE_GC])
                .set(0);
            AUTO_GC_PROCESSED_REGIONS_GAUGE_VEC
                .with_label_values(&[PROCESS_TYPE_SCAN])
                .set(0);
            set_status_metrics(GcManagerState::Idle);
        }
    }

    fn get_interval(&self) -> Duration {
        self.cfg.poll_safe_point_interval
    }
}

impl Display for GcTask {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            GcTask::Gc {
                start_key,
                end_key,
                safe_point,
                ..
            } => f
                .debug_struct("GC")
                .field("start_key", &hex::encode_upper(&start_key))
                .field("end_key", &hex::encode_upper(&end_key))
                .field("safe_point", safe_point)
                .finish(),
            GcTask::UnsafeDestroyRange {
                start_key, end_key, ..
            } => f
                .debug_struct("UnsafeDestroyRange")
                .field("start_key", &format!("{}", start_key))
                .field("end_key", &format!("{}", end_key))
                .finish(),
            GcTask::PhysicalScanLock { max_ts, .. } => f
                .debug_struct("PhysicalScanLock")
                .field("max_ts", max_ts)
                .finish(),
            #[cfg(any(test, feature = "testexport"))]
            GcTask::Validate(_) => write!(f, "Validate gc worker config"),
        }
    }
}

impl<E: Engine> GcExecutor<E> {
    pub fn new(
        engine: E,
        raft_store_router: Box<dyn StoreRouter>,
        cfg_tracker: Tracker<GcConfig>,
        cfg: GcConfig,
    ) -> Self {
        let limiter = Limiter::new(if cfg.max_write_bytes_per_sec.0 > 0 {
            cfg.max_write_bytes_per_sec.0 as f64
        } else {
            INFINITY
        });
        Self {
            engine,
            raft_store_router,
            limiter,
            cfg,
            cfg_tracker,
            stats: Statistics::default(),
        }
    }

    /// Check need gc without getting snapshot.
    /// If this is not supported or any error happens, returns true to do further check after
    /// getting snapshot.
    fn need_gc(&self, start_key: &[u8], end_key: &[u8], safe_point: TimeStamp) -> bool {
        let props = match self
            .engine
            .get_mvcc_properties_cf(CF_WRITE, safe_point, &start_key, &end_key)
        {
            Some(c) => c,
            None => return true,
        };
        check_need_gc(safe_point, self.cfg.ratio_threshold, props)
    }

    /// Cleans up outdated data.
    fn gc_key(
        &mut self,
        safe_point: TimeStamp,
        key: &Key,
        gc_info: &mut GcInfo,
        txn: &mut MvccTxn<E::Snap>,
    ) -> Result<()> {
        let next_gc_info = txn.gc(key.clone(), safe_point)?;
        gc_info.found_versions += next_gc_info.found_versions;
        gc_info.deleted_versions += next_gc_info.deleted_versions;
        gc_info.is_completed = next_gc_info.is_completed;
        self.stats.add(&txn.take_statistics());
        Ok(())
    }

    fn new_txn(snap: E::Snap) -> MvccTxn<E::Snap> {
        // TODO txn only used for GC, but this is hacky, maybe need an Option?
        let concurrency_manager = ConcurrencyManager::new(1.into());
        MvccTxn::for_scan(
            snap,
            Some(ScanMode::Forward),
            TimeStamp::zero(),
            false,
            concurrency_manager,
        )
    }

    fn flush_txn(txn: MvccTxn<E::Snap>, limiter: &Limiter, engine: &E) -> Result<()> {
        let write_size = txn.write_size();
        let modifies = txn.into_modifies();
        if !modifies.is_empty() {
            limiter.blocking_consume(write_size);
            engine.modify_on_kv_engine(modifies)?;
        }
        Ok(())
    }

    fn gc(&mut self, start_key: &[u8], end_key: &[u8], safe_point: TimeStamp) -> Result<()> {
        if !self.need_gc(start_key, end_key, safe_point) {
            GC_SKIPPED_COUNTER.inc();
            return Ok(());
        }

        let mut reader = MvccReader::new(
            self.engine.snapshot_on_kv_engine(start_key, end_key)?,
            Some(ScanMode::Forward),
            false,
            IsolationLevel::Si,
        );

        let mut next_key = Some(Key::from_encoded_slice(start_key));
        while next_key.is_some() {
            // Scans at most `GcConfig.batch_keys` keys.
            let (keys, updated_next_key) = reader.scan_keys(next_key, self.cfg.batch_keys)?;
            next_key = updated_next_key;

            if keys.is_empty() {
                GC_EMPTY_RANGE_COUNTER.inc();
                break;
            }

            let mut keys = keys.into_iter();
            let mut txn = Self::new_txn(self.engine.snapshot_on_kv_engine(start_key, end_key)?);
            let (mut next_gc_key, mut gc_info) = (keys.next(), GcInfo::default());
            while let Some(ref key) = next_gc_key {
                if let Err(e) = self.gc_key(safe_point, key, &mut gc_info, &mut txn) {
                    error!(?e; "GC meets failure"; "key" => %key,);
                    // Switch to the next key if meets failure.
                    gc_info.is_completed = true;
                }
                if gc_info.is_completed {
                    if gc_info.found_versions >= GC_LOG_FOUND_VERSION_THRESHOLD {
                        debug!(
                            "GC found plenty versions for a key";
                            "key" => %key,
                            "versions" => gc_info.found_versions,
                        );
                    }
                    if gc_info.deleted_versions as usize >= GC_LOG_DELETED_VERSION_THRESHOLD {
                        debug!(
                            "GC deleted plenty versions for a key";
                            "key" => %key,
                            "versions" => gc_info.deleted_versions,
                        );
                    }
                    next_gc_key = keys.next();
                    gc_info = GcInfo::default();
                } else {
                    Self::flush_txn(txn, &self.limiter, &self.engine)?;
                    txn = Self::new_txn(self.engine.snapshot_on_kv_engine(start_key, end_key)?);
                }
            }
            Self::flush_txn(txn, &self.limiter, &self.engine)?;
        }

        self.stats.add(reader.get_statistics());
        debug!(
            "gc has finished";
            "start_key" => hex::encode_upper(start_key),
            "end_key" => hex::encode_upper(end_key),
            "safe_point" => safe_point
        );
        Ok(())
    }

    fn unsafe_destroy_range(&self, _: &Context, start_key: &Key, end_key: &Key) -> Result<()> {
        info!(
            "unsafe destroy range started";
            "start_key" => %start_key, "end_key" => %end_key
        );

        let local_storage = self.engine.kv_engine();

        // Convert keys to RocksDB layer form
        // TODO: Logic coupled with raftstore's implementation. Maybe better design is to do it in
        // somewhere of the same layer with apply_worker.
        let start_data_key = keys::data_key(start_key.as_encoded());
        let end_data_key = keys::data_end_key(end_key.as_encoded());

        let cfs = &[CF_LOCK, CF_DEFAULT, CF_WRITE];

        // First, use DeleteStrategy::DeleteFiles to free as much disk space as possible
        let delete_files_start_time = Instant::now();
        for cf in cfs {
            local_storage
                .delete_ranges_cf(
                    cf,
                    DeleteStrategy::DeleteFiles,
                    &[Range::new(&start_data_key, &end_data_key)],
                )
                .map_err(|e| {
                    let e: Error = box_err!(e);
                    warn!("unsafe destroy range failed at delete_files_in_range_cf"; "err" => ?e);
                    e
                })?;
        }

        info!(
            "unsafe destroy range finished deleting files in range";
            "start_key" => %start_key, "end_key" => %end_key,
            "cost_time" => ?delete_files_start_time.elapsed(),
        );

        // Then, delete all remaining keys in the range.
        let cleanup_all_start_time = Instant::now();
        for cf in cfs {
            // TODO: set use_delete_range with config here.
            local_storage
                .delete_ranges_cf(
                    cf,
                    DeleteStrategy::DeleteByKey,
                    &[Range::new(&start_data_key, &end_data_key)],
                )
                .map_err(|e| {
                    let e: Error = box_err!(e);
                    warn!("unsafe destroy range failed at delete_all_in_range_cf"; "err" => ?e);
                    e
                })?;
            local_storage
                .delete_ranges_cf(
                    cf,
                    DeleteStrategy::DeleteBlobs,
                    &[Range::new(&start_data_key, &end_data_key)],
                )
                .map_err(|e| {
                    let e: Error = box_err!(e);
                    warn!("unsafe destroy range failed at delete_blob_files_in_range"; "err" => ?e);
                    e
                })?;
        }

        info!(
            "unsafe destroy range finished cleaning up all";
            "start_key" => %start_key, "end_key" => %end_key, "cost_time" => ?cleanup_all_start_time.elapsed(),
        );

        self.raft_store_router
            .send(StoreMsg::ClearRegionSizeInRange {
                start_key: start_key.as_encoded().to_vec(),
                end_key: end_key.as_encoded().to_vec(),
            })
            .unwrap_or_else(|e| {
                // Warn and ignore it.
                warn!("unsafe destroy range: failed sending ClearRegionSizeInRange"; "err" => ?e);
            });

        Ok(())
    }

    fn handle_physical_scan_lock(
        &self,
        _: &Context,
        max_ts: TimeStamp,
        start_key: &Key,
        limit: usize,
    ) -> Result<Vec<LockInfo>> {
        let snap = self
            .engine
            .snapshot_on_kv_engine(start_key.as_encoded(), &[])
            .unwrap();
        let mut reader = MvccReader::new(snap, Some(ScanMode::Forward), false, IsolationLevel::Si);
        let (locks, _) = reader.scan_locks(Some(start_key), |l| l.ts <= max_ts, limit)?;

        let mut lock_infos = Vec::with_capacity(locks.len());
        for (key, lock) in locks {
            let raw_key = key.into_raw().map_err(MvccError::from)?;
            lock_infos.push(lock.into_lock_info(raw_key));
        }
        Ok(lock_infos)
    }

    fn update_statistics_metrics(&mut self) {
        let stats = mem::take(&mut self.stats);

        for (cf, details) in stats.details_enum().iter() {
            for (tag, count) in details.iter() {
                GC_KEYS_COUNTER_STATIC
                    .get(*cf)
                    .get(*tag)
                    .inc_by(*count as i64);
            }
        }
    }

    fn refresh_cfg(&mut self) {
        if let Some(incoming) = self.cfg_tracker.any_new() {
            let limit = incoming.max_write_bytes_per_sec.0;
            self.limiter
                .set_speed_limit(if limit > 0 { limit as f64 } else { INFINITY });
            self.cfg = incoming.clone();
        }
    }
}

impl<E: Engine> Executor for GcExecutor<E> {
    #[inline]
    fn execute(&mut self, task: GcTask) {
        let enum_label = task.get_enum_label();

        GC_GCTASK_COUNTER_STATIC.get(enum_label).inc();

        let timer = SlowTimer::from_secs(GC_TASK_SLOW_SECONDS);
        let update_metrics = |is_err| {
            GC_TASK_DURATION_HISTOGRAM_VEC
                .with_label_values(&[enum_label.get_str()])
                .observe(duration_to_sec(timer.elapsed()));

            if is_err {
                GC_GCTASK_FAIL_COUNTER_STATIC.get(enum_label).inc();
            }
        };

        // Refresh config before handle task
        self.refresh_cfg();

        match task {
            GcTask::Gc {
                start_key,
                end_key,
                safe_point,
                callback,
                ..
            } => {
                let res = self.gc(&start_key, &end_key, safe_point);
                if let Some(cb) = callback {
                    cb(res);
                }
                self.update_statistics_metrics();
                slow_log!(
                    T timer,
                    "GC on range [{}, {}), safe_point {}",
                    hex::encode_upper(&start_key),
                    hex::encode_upper(&end_key),
                    safe_point
                );
            }
            GcTask::UnsafeDestroyRange {
                ctx,
                start_key,
                end_key,
                callback,
            } => {
                let res = self.unsafe_destroy_range(&ctx, &start_key, &end_key);
                update_metrics(res.is_err());
                callback(res);
                slow_log!(
                    T timer,
                    "UnsafeDestroyRange start_key {:?}, end_key {:?}",
                    start_key,
                    end_key
                );
            }
            GcTask::PhysicalScanLock {
                ctx,
                max_ts,
                start_key,
                limit,
                callback,
            } => {
                let res = self.handle_physical_scan_lock(&ctx, max_ts, &start_key, limit);
                update_metrics(res.is_err());
                callback(res);
                slow_log!(
                    T timer,
                    "PhysicalScanLock start_key {:?}, max_ts {}, limit {}",
                    start_key,
                    max_ts,
                    limit,
                );
            }
            #[cfg(any(test, feature = "testexport"))]
            GcTask::Validate(f) => {
                f(&self.cfg, &self.limiter);
            }
        };
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::storage::Callback;
    use kvproto::metapb;
    use raft::StateRole;
    use raftstore::store::util::new_peer;
    use std::collections::BTreeMap;
    use std::mem;
    use std::sync::mpsc::{channel, Receiver, Sender};
    use std::thread::Builder as ThreadBuilder;

    fn take_callback(t: &mut GcTask) -> Option<Callback<()>> {
        let callback = match t {
            GcTask::Gc {
                ref mut callback, ..
            } => return callback.take(),
            GcTask::UnsafeDestroyRange {
                ref mut callback, ..
            } => callback,
            GcTask::PhysicalScanLock { .. } => unreachable!(),
            GcTask::Validate(_) => unreachable!(),
        };
        Some(mem::replace(callback, Box::new(|_| {})))
    }

    struct MockGcRunner {
        tx: Sender<GcTask>,
    }

    pub fn make_mock_auto_gc_cfg() -> (AutoGcConfig, MockSafePointProvider) {
        let (_, safe_point_receiver) = channel();

        let mut cfg = AutoGcConfig::new(1);
        cfg.poll_safe_point_interval = Duration::from_millis(100);
        cfg.always_check_safe_point = true;
        (
            cfg,
            MockSafePointProvider {
                rx: safe_point_receiver,
            },
        )
    }

    impl Executor for MockGcRunner {
        fn execute(&mut self, mut t: GcTask) {
            let cb = take_callback(&mut t);
            self.tx.send(t).unwrap();
            if let Some(cb) = cb {
                cb(Ok(()));
            }
        }
    }

    /// A set of utilities that helps testing `GcManager`.
    /// The safe_point polling interval is set to 100 ms.
    struct GcManagerTestUtil {
        gc_manager: Option<GcRunner<MockRegionInfoProvider>>,
        safe_point_sender: Sender<TimeStamp>,
        gc_task_receiver: Receiver<GcTask>,
        stop: Arc<AtomicBool>,
    }

    impl GcManagerTestUtil {
        pub fn new(regions: BTreeMap<Vec<u8>, RegionInfo>) -> Self {
            let (gc_task_sender, gc_task_receiver) = channel();

            let (safe_point_sender, safe_point_receiver) = channel();

            let mut cfg = AutoGcConfig::new(1);
            cfg.poll_safe_point_interval = Duration::from_millis(100);
            cfg.always_check_safe_point = true;

            let stop = Arc::new(AtomicBool::new(false));
            let gc_manager = GcRunner::new(
                cfg,
                Box::new(MockSafePointProvider {
                    rx: safe_point_receiver,
                }),
                MockRegionInfoProvider { regions },
                Arc::new(AtomicU64::new(0)),
                GcWorkerConfigManager::default(),
                Default::default(),
                stop.clone(),
                Box::new(MockGcRunner { tx: gc_task_sender }),
            );
            Self {
                gc_manager: Some(gc_manager),
                safe_point_sender,
                gc_task_receiver,
                stop,
            }
        }

        /// Collect `GcTask`s that `GcManager` tried to execute.
        pub fn collect_scheduled_tasks(&self) -> Vec<GcTask> {
            self.gc_task_receiver.try_iter().collect()
        }

        pub fn add_next_safe_point(&self, safe_point: impl Into<TimeStamp>) {
            self.safe_point_sender.send(safe_point.into()).unwrap();
        }

        pub fn stop(&mut self) {
            self.stop.store(true, AtomicOrdering::Release);
        }
    }

    /// Run a round of auto GC and check if it correctly GC regions as expected.
    ///
    /// Param `regions` is a `Vec` of tuples which is `(start_key, end_key, region_id)`
    ///
    /// The first value in param `safe_points` will be used to initialize the GcManager, and the remaining
    /// values will be checked before every time GC-ing a region. If the length of `safe_points` is
    /// less than executed GC tasks, the last value will be used for extra GC tasks.
    ///
    /// Param `expected_gc_tasks` is a `Vec` of tuples which is `(region_id, safe_point)`.
    fn test_auto_gc(
        regions: Vec<(Vec<u8>, Vec<u8>, u64)>,
        safe_points: Vec<impl Into<TimeStamp> + Copy>,
        expected_gc_tasks: Vec<(u64, impl Into<TimeStamp>)>,
    ) {
        let regions: BTreeMap<_, _> = regions
            .into_iter()
            .map(|(start_key, end_key, id)| {
                let mut r = metapb::Region::default();
                r.set_id(id);
                r.set_start_key(start_key.clone());
                r.set_end_key(end_key);
                r.mut_peers().push(new_peer(1, 1));
                let info = RegionInfo::new(r, StateRole::Leader);
                (start_key, info)
            })
            .collect();

        let mut test_util = GcManagerTestUtil::new(regions);

        for safe_point in &safe_points {
            test_util.add_next_safe_point(*safe_point);
        }

        test_util.gc_manager.as_mut().unwrap().start();
        test_util.gc_manager.as_mut().unwrap().gc_a_round();
        test_util.stop();

        let gc_tasks: Vec<_> = test_util
            .collect_scheduled_tasks()
            .iter()
            .map(|task| match task {
                GcTask::Gc {
                    region_id,
                    safe_point,
                    ..
                } => (*region_id, *safe_point),
                _ => unreachable!(),
            })
            .collect();

        // Following code asserts gc_tasks == expected_gc_tasks.
        assert_eq!(gc_tasks.len(), expected_gc_tasks.len());
        let all_passed = gc_tasks.into_iter().zip(expected_gc_tasks.into_iter()).all(
            |((region, safe_point), (expect_region, expect_safe_point))| {
                region == expect_region && safe_point == expect_safe_point.into()
            },
        );
        assert!(all_passed);
    }

    #[test]
    fn test_update_safe_point() {
        let mut test_util = GcManagerTestUtil::new(BTreeMap::new());
        let mut gc_manager = test_util.gc_manager.take().unwrap();
        assert_eq!(gc_manager.curr_safe_point(), TimeStamp::zero());
        test_util.add_next_safe_point(233);
        assert!(gc_manager.try_update_safe_point());
        assert_eq!(gc_manager.curr_safe_point(), 233.into());

        let (tx, rx) = channel();
        ThreadBuilder::new()
            .spawn(move || {
                while !gc_manager.try_update_safe_point() {
                    std::thread::sleep(Duration::from_millis(100));
                }
                let safe_point = gc_manager.curr_safe_point();
                tx.send(safe_point).unwrap();
            })
            .unwrap();
        test_util.add_next_safe_point(233);
        test_util.add_next_safe_point(233);
        test_util.add_next_safe_point(234);
        assert_eq!(rx.recv().unwrap(), 234.into());

        test_util.stop();
    }

    #[test]
    fn test_gc_manager_initialize() {
        let mut test_util = GcManagerTestUtil::new(BTreeMap::new());
        let mut gc_manager = test_util.gc_manager.take().unwrap();
        assert_eq!(gc_manager.curr_safe_point(), TimeStamp::zero());
        test_util.add_next_safe_point(0);
        test_util.add_next_safe_point(5);
        gc_manager.start();
        assert_eq!(gc_manager.curr_safe_point(), TimeStamp::zero());
        assert!(gc_manager.try_update_safe_point());
        assert_eq!(gc_manager.curr_safe_point(), 5.into());
    }

    #[test]
    fn test_auto_gc_a_round_without_rewind() {
        // First region starts with empty and last region ends with empty.
        let regions = vec![
            (b"".to_vec(), b"1".to_vec(), 1),
            (b"1".to_vec(), b"2".to_vec(), 2),
            (b"3".to_vec(), b"4".to_vec(), 3),
            (b"7".to_vec(), b"".to_vec(), 4),
        ];
        test_auto_gc(
            regions,
            vec![233],
            vec![(1, 233), (2, 233), (3, 233), (4, 233)],
        );

        // First region doesn't starts with empty and last region doesn't ends with empty.
        let regions = vec![
            (b"0".to_vec(), b"1".to_vec(), 1),
            (b"1".to_vec(), b"2".to_vec(), 2),
            (b"3".to_vec(), b"4".to_vec(), 3),
            (b"7".to_vec(), b"8".to_vec(), 4),
        ];
        test_auto_gc(
            regions,
            vec![233],
            vec![(1, 233), (2, 233), (3, 233), (4, 233)],
        );
    }

    #[test]
    fn test_auto_gc_rewinding() {
        for regions in vec![
            // First region starts with empty and last region ends with empty.
            vec![
                (b"".to_vec(), b"1".to_vec(), 1),
                (b"1".to_vec(), b"2".to_vec(), 2),
                (b"3".to_vec(), b"4".to_vec(), 3),
                (b"7".to_vec(), b"".to_vec(), 4),
            ],
            // First region doesn't starts with empty and last region doesn't ends with empty.
            vec![
                (b"0".to_vec(), b"1".to_vec(), 1),
                (b"1".to_vec(), b"2".to_vec(), 2),
                (b"3".to_vec(), b"4".to_vec(), 3),
                (b"7".to_vec(), b"8".to_vec(), 4),
            ],
        ] {
            test_auto_gc(
                regions.clone(),
                vec![233, 234],
                vec![(1, 234), (2, 234), (3, 234), (4, 234)],
            );
            test_auto_gc(
                regions.clone(),
                vec![233, 233, 234],
                vec![(1, 233), (2, 234), (3, 234), (4, 234), (1, 234)],
            );
            test_auto_gc(
                regions.clone(),
                vec![233, 233, 233, 233, 234],
                vec![
                    (1, 233),
                    (2, 233),
                    (3, 233),
                    (4, 234),
                    (1, 234),
                    (2, 234),
                    (3, 234),
                ],
            );
            test_auto_gc(
                regions.clone(),
                vec![233, 233, 233, 234, 235],
                vec![
                    (1, 233),
                    (2, 233),
                    (3, 234),
                    (4, 235),
                    (1, 235),
                    (2, 235),
                    (3, 235),
                ],
            );

            let mut safe_points = vec![233, 233, 233, 234, 234, 234, 235];
            // The logic of `gc_a_round` wastes a loop when the last region's end_key is not null, so it
            // will check safe point one more time before GC-ing the first region after rewinding.
            if !regions.last().unwrap().1.is_empty() {
                safe_points.insert(5, 234);
            }
            test_auto_gc(
                regions.clone(),
                safe_points,
                vec![
                    (1, 233),
                    (2, 233),
                    (3, 234),
                    (4, 234),
                    (1, 234),
                    (2, 235),
                    (3, 235),
                    (4, 235),
                    (1, 235),
                ],
            );
        }
    }
}
