// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::f64::INFINITY;
use std::fmt::{self, Display, Formatter};
use std::mem;
use std::sync::mpsc;
use std::sync::{atomic, Arc, Mutex};
use std::time::{Duration, Instant};

use engine::rocks::util::get_cf_handle;
use engine::rocks::DB;
use engine_rocks::{Compat, RocksEngine};
use engine_traits::{TablePropertiesExt, MiscExt};
use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE};
use futures::Future;
use kvproto::kvrpcpb::{Context, IsolationLevel, LockInfo};
use kvproto::metapb;
use tokio_core::reactor::Handle;

use crate::server::metrics::*;
use crate::storage::kv::{
    Engine, Error as EngineError, ErrorInner as EngineErrorInner, ScanMode, Statistics,
};
use crate::storage::mvcc::{check_need_gc, Error as MvccError, MvccReader, MvccTxn};
use pd_client::PdClient;
use raftstore::coprocessor::{CoprocessorHost, RegionInfoAccessor, RegionInfoProvider};
use raftstore::router::ServerRaftStoreRouter;
use raftstore::store::msg::StoreMsg;
use raftstore::store::RegionSnapshot;
use tikv_util::config::{Tracker, VersionTrack};
use tikv_util::time::{duration_to_sec, Limiter, SlowTimer};
use tikv_util::worker::{
    FutureRunnable, FutureScheduler, FutureWorker, Stopped as FutureWorkerStopped,
};
use txn_types::{Key, TimeStamp};

use super::applied_lock_collector::{AppliedLockCollector, Callback as LockCollectorCallback};
use super::config::{GcConfig, GcWorkerConfigManager};
use super::gc_manager::AutoGcConfig;
use super::gc_manager::{GcManager, GcManagerHandle};
use super::{Callback, Error, ErrorInner, Result};

/// After the GC scan of a key, output a message to the log if there are at least this many
/// versions of the key.
const GC_LOG_FOUND_VERSION_THRESHOLD: usize = 30;

/// After the GC delete versions of a key, output a message to the log if at least this many
/// versions are deleted.
const GC_LOG_DELETED_VERSION_THRESHOLD: usize = 30;

pub const GC_MAX_EXECUTING_TASKS: usize = 10;
const GC_SNAPSHOT_TIMEOUT_SECS: u64 = 10;
const GC_TASK_SLOW_SECONDS: u64 = 30;

/// Provides safe point.
/// TODO: Give it a better name?
pub trait GcSafePointProvider: Send + 'static {
    fn get_safe_point(&self) -> Result<TimeStamp>;
}

impl<T: PdClient + 'static> GcSafePointProvider for Arc<T> {
    fn get_safe_point(&self) -> Result<TimeStamp> {
        let future = self.get_gc_safe_point();
        future
            .wait()
            .map(Into::into)
            .map_err(|e| box_err!("failed to get safe point from PD: {:?}", e))
    }
}

pub enum GcTask {
    Gc {
        ctx: Context,
        safe_point: TimeStamp,
        callback: Callback<()>,
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
    pub fn get_label(&self) -> &'static str {
        match self {
            GcTask::Gc { .. } => "gc",
            GcTask::UnsafeDestroyRange { .. } => "unsafe_destroy_range",
            GcTask::PhysicalScanLock { .. } => "physical_scan_lock",
            #[cfg(any(test, feature = "testexport"))]
            GcTask::Validate(_) => "validate_config",
        }
    }
}

impl Display for GcTask {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            GcTask::Gc {
                ctx, safe_point, ..
            } => {
                let epoch = format!("{:?}", ctx.region_epoch.as_ref());
                f.debug_struct("GC")
                    .field("region_id", &ctx.get_region_id())
                    .field("region_epoch", &epoch)
                    .field("safe_point", safe_point)
                    .finish()
            }
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

/// Used to perform GC operations on the engine.
struct GcRunner<E: Engine> {
    engine: E,
    local_storage: Option<Arc<DB>>,
    raft_store_router: Option<ServerRaftStoreRouter>,
    region_info_accessor: Option<RegionInfoAccessor>,

    /// Used to limit the write flow of GC.
    limiter: Limiter,

    cfg: GcConfig,
    cfg_tracker: Tracker<GcConfig>,

    stats: Statistics,
}

impl<E: Engine> GcRunner<E> {
    pub fn new(
        engine: E,
        local_storage: Option<Arc<DB>>,
        raft_store_router: Option<ServerRaftStoreRouter>,
        cfg_tracker: Tracker<GcConfig>,
        region_info_accessor: Option<RegionInfoAccessor>,
        cfg: GcConfig,
    ) -> Self {
        let limiter = Limiter::new(if cfg.max_write_bytes_per_sec.0 > 0 {
            cfg.max_write_bytes_per_sec.0 as f64
        } else {
            INFINITY
        });
        Self {
            engine,
            local_storage,
            raft_store_router,
            region_info_accessor,
            limiter,
            cfg,
            cfg_tracker,
            stats: Statistics::default(),
        }
    }

    fn get_snapshot(&self, ctx: &mut Context) -> Result<E::Snap> {
        let timeout = Duration::from_secs(GC_SNAPSHOT_TIMEOUT_SECS);
        match wait_op!(|cb| self.engine.async_snapshot(ctx, cb), timeout) {
            Some((cb_ctx, Ok(snapshot))) => {
                if let Some(term) = cb_ctx.term {
                    ctx.set_term(term);
                }
                Ok(snapshot)
            }
            Some((_, Err(e))) => Err(e),
            None => Err(EngineError::from(EngineErrorInner::Timeout(timeout))),
        }
        .map_err(Error::from)
    }

    /// Check need gc without getting snapshot.
    /// If this is not supported or any error happens, returns true to do further check after
    /// getting snapshot.
    fn need_gc(&self, ctx: &Context, safe_point: TimeStamp) -> bool {
        let region_info_accessor = match &self.region_info_accessor {
            Some(r) => r,
            None => {
                info!(
                    "region_info_accessor not set. cannot check need_gc without getting snapshot"
                );
                return true;
            }
        };

        let db = match &self.local_storage {
            Some(db) => db,
            None => {
                info!("local_storage not set. cannot check need_gc without getting snapshot");
                return true;
            }
        };

        let (tx, rx) = mpsc::channel();
        if let Err(e) = region_info_accessor.find_region_by_id(
            ctx.get_region_id(),
            Box::new(move |region| match tx.send(region) {
                Ok(()) => (),
                Err(e) => error!(
                    "find_region_by_id failed to send result";
                    "err" => ?e
                ),
            }),
        ) {
            error!(
                "failed to find_region_by_id from region_info_accessor";
                "region_id" => ctx.get_region_id(),
                "err" => ?e
            );
            return true;
        }

        let region_info = match rx.recv() {
            Ok(None) => return true,
            Ok(Some(r)) => r,
            Err(e) => {
                error!(
                    "failed to find_region_by_id from region_info_accessor";
                    "region_id" => ctx.get_region_id(),
                    "err" => ?e
                );
                return true;
            }
        };

        let start_key = keys::data_key(region_info.region.get_start_key());
        let end_key = keys::data_end_key(region_info.region.get_end_key());

        let collection = match db
            .c()
            .get_range_properties_cf(CF_WRITE, &start_key, &end_key)
        {
            Ok(c) => c,
            Err(e) => {
                error!(
                    "failed to get range properties from write cf";
                    "region_id" => ctx.get_region_id(),
                    "start_key" => hex::encode_upper(&start_key),
                    "end_key" => hex::encode_upper(&end_key),
                    "err" => ?e,
                );
                return true;
            }
        };
        check_need_gc(safe_point, self.cfg.ratio_threshold, collection)
    }

    /// Scans keys in the region. Returns scanned keys if any, and a key indicating scan progress
    fn scan_keys(
        &mut self,
        ctx: &mut Context,
        safe_point: TimeStamp,
        from: Option<Key>,
    ) -> Result<(Vec<Key>, Option<Key>)> {
        let snapshot = self.get_snapshot(ctx)?;
        let mut reader = MvccReader::new(
            snapshot,
            Some(ScanMode::Forward),
            !ctx.get_not_fill_cache(),
            ctx.get_isolation_level(),
        );

        let is_range_start = from.is_none();

        // range start gc with from == None, and this is an optimization to
        // skip gc before scanning all data.
        let skip_gc = is_range_start && !reader.need_gc(safe_point, self.cfg.ratio_threshold);
        let res = if skip_gc {
            GC_SKIPPED_COUNTER.inc();
            Ok((vec![], None))
        } else {
            reader
                .scan_keys(from, self.cfg.batch_keys)
                .map_err(Error::from)
                .and_then(|(keys, next)| {
                    if keys.is_empty() {
                        assert!(next.is_none());
                        if is_range_start {
                            GC_EMPTY_RANGE_COUNTER.inc();
                        }
                    }
                    Ok((keys, next))
                })
        };
        self.stats.add(reader.get_statistics());
        res
    }

    /// Cleans up outdated data.
    fn gc_keys(
        &mut self,
        ctx: &mut Context,
        safe_point: TimeStamp,
        keys: Vec<Key>,
        mut next_scan_key: Option<Key>,
    ) -> Result<Option<Key>> {
        let snapshot = self.get_snapshot(ctx)?;
        let mut txn = MvccTxn::for_scan(
            snapshot,
            Some(ScanMode::Forward),
            TimeStamp::zero(),
            !ctx.get_not_fill_cache(),
        );
        for k in keys {
            let gc_info = txn.gc(k.clone(), safe_point)?;

            if gc_info.found_versions >= GC_LOG_FOUND_VERSION_THRESHOLD {
                debug!(
                    "GC found plenty versions for a key";
                    "region_id" => ctx.get_region_id(),
                    "versions" => gc_info.found_versions,
                    "key" => %k
                );
            }
            // TODO: we may delete only part of the versions in a batch, which may not beyond
            // the logging threshold `GC_LOG_DELETED_VERSION_THRESHOLD`.
            if gc_info.deleted_versions as usize >= GC_LOG_DELETED_VERSION_THRESHOLD {
                debug!(
                    "GC deleted plenty versions for a key";
                    "region_id" => ctx.get_region_id(),
                    "versions" => gc_info.deleted_versions,
                    "key" => %k
                );
            }

            if !gc_info.is_completed {
                next_scan_key = Some(k);
                break;
            }
        }
        self.stats.add(&txn.take_statistics());

        let write_size = txn.write_size();
        let modifies = txn.into_modifies();
        if !modifies.is_empty() {
            self.refresh_cfg();
            self.limiter.blocking_consume(write_size);
            self.engine.write(ctx, modifies)?;
        }
        Ok(next_scan_key)
    }

    fn gc(&mut self, ctx: &mut Context, safe_point: TimeStamp) -> Result<()> {
        debug!(
            "start doing GC";
            "region_id" => ctx.get_region_id(),
            "safe_point" => safe_point
        );

        if !self.need_gc(ctx, safe_point) {
            GC_SKIPPED_COUNTER.inc();
            return Ok(());
        }

        let mut next_key = None;
        loop {
            // Scans at most `GCConfig.batch_keys` keys
            let (keys, next) = self
                .scan_keys(ctx, safe_point, next_key)
                .map_err(|e| {
                    warn!("gc scan_keys failed"; "region_id" => ctx.get_region_id(), "safe_point" => safe_point, "err" => ?e);
                    e
                })?;
            if keys.is_empty() {
                break;
            }

            // Does the GC operation on all scanned keys
            next_key = self.gc_keys(ctx, safe_point, keys, next).map_err(|e| {
                warn!("gc gc_keys failed"; "region_id" => ctx.get_region_id(), "safe_point" => safe_point, "err" => ?e);
                e
            })?;
            if next_key.is_none() {
                break;
            }
        }

        debug!(
            "gc has finished";
            "region_id" => ctx.get_region_id(),
            "safe_point" => safe_point
        );
        Ok(())
    }

    fn unsafe_destroy_range(&self, _: &Context, start_key: &Key, end_key: &Key) -> Result<()> {
        info!(
            "unsafe destroy range started";
            "start_key" => %start_key, "end_key" => %end_key
        );

        // TODO: Refine usage of errors

        let local_storage = self.local_storage.as_ref().ok_or_else(|| {
            let e: Error = box_err!("unsafe destroy range not supported: local_storage not set");
            warn!("unsafe destroy range failed"; "err" => ?e);
            e
        })?;

        // Convert keys to RocksDB layer form
        // TODO: Logic coupled with raftstore's implementation. Maybe better design is to do it in
        // somewhere of the same layer with apply_worker.
        let start_data_key = keys::data_key(start_key.as_encoded());
        let end_data_key = keys::data_end_key(end_key.as_encoded());

        let cfs = &[CF_LOCK, CF_DEFAULT, CF_WRITE];

        // First, call delete_files_in_range to free as much disk space as possible
        let delete_files_start_time = Instant::now();
        for cf in cfs {
            let cf_handle = get_cf_handle(local_storage, cf).unwrap();
            local_storage
                .delete_files_in_range_cf(cf_handle, &start_data_key, &end_data_key, false)
                .map_err(|e| {
                    let e: Error = box_err!(e);
                    warn!(
                        "unsafe destroy range failed at delete_files_in_range_cf"; "err" => ?e
                    );
                    e
                })?;
        }

        info!(
            "unsafe destroy range finished deleting files in range";
            "start_key" => %start_key, "end_key" => %end_key, "cost_time" => ?delete_files_start_time.elapsed()
        );

        // Then, delete all remaining keys in the range.
        let cleanup_all_start_time = Instant::now();
        for cf in cfs {
            // TODO: set use_delete_range with config here.
            local_storage.c().delete_all_in_range_cf(cf, &start_data_key, &end_data_key, false)
                .map_err(|e| {
                    let e: Error = box_err!(e);
                    warn!(
                        "unsafe destroy range failed at delete_all_in_range_cf"; "err" => ?e
                    );
                    e
                })?;
        }

        let cleanup_all_time_cost = cleanup_all_start_time.elapsed();

        if let Some(router) = self.raft_store_router.as_ref() {
            router
                .send_store(StoreMsg::ClearRegionSizeInRange {
                    start_key: start_key.as_encoded().to_vec(),
                    end_key: end_key.as_encoded().to_vec(),
                })
                .unwrap_or_else(|e| {
                    // Warn and ignore it.
                    warn!(
                        "unsafe destroy range: failed sending ClearRegionSizeInRange";
                        "err" => ?e
                    );
                });
        } else {
            warn!("unsafe destroy range: can't clear region size information: raft_store_router not set");
        }

        info!(
            "unsafe destroy range finished cleaning up all";
            "start_key" => %start_key, "end_key" => %end_key, "cost_time" => ?cleanup_all_time_cost,
        );
        Ok(())
    }

    fn handle_physical_scan_lock(
        &self,
        _: &Context,
        max_ts: TimeStamp,
        start_key: &Key,
        limit: usize,
    ) -> Result<Vec<LockInfo>> {
        let db = self.local_storage.clone().ok_or_else::<Error, _>(|| {
            box_err!("local storage not set, physical scan lock not supported")
        })?;

        // Create a `RegionSnapshot`, which can converts the 'z'-prefixed keys into normal keys
        // internally. A fake region meta is given to make the snapshot's range unbounded.
        // TODO: Should we implement a special snapshot and iterator types for this?
        let mut fake_region = metapb::Region::default();
        // Add a peer to pass initialized check.
        fake_region.mut_peers().push(metapb::Peer::default());
        let snap = RegionSnapshot::<RocksEngine>::from_raw(db, fake_region);

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
        let stats = mem::replace(&mut self.stats, Statistics::default());
        for (cf, details) in stats.details().iter() {
            for (tag, count) in details.iter() {
                GC_KEYS_COUNTER_VEC
                    .with_label_values(&[cf, *tag])
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

impl<E: Engine> FutureRunnable<GcTask> for GcRunner<E> {
    #[inline]
    fn run(&mut self, task: GcTask, _handle: &Handle) {
        let label = task.get_label();
        GC_GCTASK_COUNTER_VEC.with_label_values(&[label]).inc();

        let timer = SlowTimer::from_secs(GC_TASK_SLOW_SECONDS);
        let update_metrics = |is_err| {
            GC_TASK_DURATION_HISTOGRAM_VEC
                .with_label_values(&[label])
                .observe(duration_to_sec(timer.elapsed()));

            if is_err {
                GC_GCTASK_FAIL_COUNTER_VEC.with_label_values(&[label]).inc();
            }
        };

        // Refresh config before handle task
        self.refresh_cfg();

        match task {
            GcTask::Gc {
                mut ctx,
                safe_point,
                callback,
            } => {
                let res = self.gc(&mut ctx, safe_point);
                update_metrics(res.is_err());
                callback(res);
                self.update_statistics_metrics();
                slow_log!(
                    timer,
                    "GC on region {}, epoch {:?}, safe_point {}",
                    ctx.get_region_id(),
                    ctx.get_region_epoch(),
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
                    timer,
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
                    timer,
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

/// When we failed to schedule a `GcTask` to `GcRunner`, use this to handle the `ScheduleError`.
fn handle_gc_task_schedule_error(e: FutureWorkerStopped<GcTask>) -> Result<()> {
    error!("failed to schedule gc task: {:?}", e);
    Err(box_err!("failed to schedule gc task: {:?}", e))
}

/// Schedules a `GcTask` to the `GcRunner`.
fn schedule_gc(
    scheduler: &FutureScheduler<GcTask>,
    ctx: Context,
    safe_point: TimeStamp,
    callback: Callback<()>,
) -> Result<()> {
    scheduler
        .schedule(GcTask::Gc {
            ctx,
            safe_point,
            callback,
        })
        .or_else(handle_gc_task_schedule_error)
}

/// Does GC synchronously.
pub fn sync_gc(
    scheduler: &FutureScheduler<GcTask>,
    ctx: Context,
    safe_point: TimeStamp,
) -> Result<()> {
    wait_op!(|callback| schedule_gc(scheduler, ctx, safe_point, callback)).unwrap_or_else(|| {
        error!("failed to receive result of gc");
        Err(box_err!("gc_worker: failed to receive result of gc"))
    })
}

/// Used to schedule GC operations.
pub struct GcWorker<E: Engine> {
    engine: E,
    /// `local_storage` represent the underlying RocksDB of the `engine`.
    local_storage: Option<Arc<DB>>,
    /// `raft_store_router` is useful to signal raftstore clean region size informations.
    raft_store_router: Option<ServerRaftStoreRouter>,
    /// Access the region's meta before getting snapshot, which will wake hibernating regions up.
    /// This is useful to do the `need_gc` check without waking hibernatin regions up.
    /// This is not set for tests.
    region_info_accessor: Option<RegionInfoAccessor>,

    config_manager: GcWorkerConfigManager,

    /// How many requests are scheduled from outside and unfinished.
    scheduled_tasks: Arc<atomic::AtomicUsize>,

    /// How many strong references. The worker will be stopped
    /// once there are no more references.
    refs: Arc<atomic::AtomicUsize>,
    worker: Arc<Mutex<FutureWorker<GcTask>>>,
    worker_scheduler: FutureScheduler<GcTask>,

    applied_lock_collector: Option<Arc<AppliedLockCollector>>,

    gc_manager_handle: Arc<Mutex<Option<GcManagerHandle>>>,
}

impl<E: Engine> Clone for GcWorker<E> {
    #[inline]
    fn clone(&self) -> Self {
        self.refs.fetch_add(1, atomic::Ordering::SeqCst);

        Self {
            engine: self.engine.clone(),
            local_storage: self.local_storage.clone(),
            raft_store_router: self.raft_store_router.clone(),
            config_manager: self.config_manager.clone(),
            region_info_accessor: self.region_info_accessor.clone(),
            scheduled_tasks: self.scheduled_tasks.clone(),
            refs: self.refs.clone(),
            worker: self.worker.clone(),
            worker_scheduler: self.worker_scheduler.clone(),
            applied_lock_collector: self.applied_lock_collector.clone(),
            gc_manager_handle: self.gc_manager_handle.clone(),
        }
    }
}

impl<E: Engine> Drop for GcWorker<E> {
    #[inline]
    fn drop(&mut self) {
        let refs = self.refs.fetch_sub(1, atomic::Ordering::SeqCst);

        if refs != 1 {
            return;
        }

        let r = self.stop();
        if let Err(e) = r {
            error!("Failed to stop gc_worker"; "err" => ?e);
        }
    }
}

impl<E: Engine> GcWorker<E> {
    pub fn new(
        engine: E,
        local_storage: Option<Arc<DB>>,
        raft_store_router: Option<ServerRaftStoreRouter>,
        region_info_accessor: Option<RegionInfoAccessor>,
        cfg: GcConfig,
    ) -> GcWorker<E> {
        let worker = Arc::new(Mutex::new(FutureWorker::new("gc-worker")));
        let worker_scheduler = worker.lock().unwrap().scheduler();
        GcWorker {
            engine,
            local_storage,
            raft_store_router,
            config_manager: GcWorkerConfigManager(Arc::new(VersionTrack::new(cfg))),
            region_info_accessor,
            scheduled_tasks: Arc::new(atomic::AtomicUsize::new(0)),
            refs: Arc::new(atomic::AtomicUsize::new(1)),
            worker,
            worker_scheduler,
            applied_lock_collector: None,
            gc_manager_handle: Arc::new(Mutex::new(None)),
        }
    }

    pub fn start_auto_gc<S: GcSafePointProvider, R: RegionInfoProvider>(
        &self,
        cfg: AutoGcConfig<S, R>,
    ) -> Result<()> {
        let mut handle = self.gc_manager_handle.lock().unwrap();
        assert!(handle.is_none());
        let new_handle = GcManager::new(cfg, self.worker_scheduler.clone()).start()?;
        *handle = Some(new_handle);
        Ok(())
    }

    pub fn start(&mut self) -> Result<()> {
        let runner = GcRunner::new(
            self.engine.clone(),
            self.local_storage.take(),
            self.raft_store_router.take(),
            self.config_manager.0.clone().tracker("gc-woker".to_owned()),
            self.region_info_accessor.take(),
            self.config_manager.value().clone(),
        );
        self.worker
            .lock()
            .unwrap()
            .start(runner)
            .map_err(|e| box_err!("failed to start gc_worker, err: {:?}", e))
    }

    pub fn start_observe_lock_apply(
        &mut self,
        coprocessor_host: &mut CoprocessorHost,
    ) -> Result<()> {
        assert!(self.applied_lock_collector.is_none());
        let collector = Arc::new(AppliedLockCollector::new(coprocessor_host)?);
        self.applied_lock_collector = Some(collector);
        Ok(())
    }

    pub fn stop(&self) -> Result<()> {
        // Stop GcManager.
        if let Some(h) = self.gc_manager_handle.lock().unwrap().take() {
            h.stop()?;
        }
        // Stop self.
        if let Some(h) = self.worker.lock().unwrap().stop() {
            if let Err(e) = h.join() {
                return Err(box_err!("failed to join gc_worker handle, err: {:?}", e));
            }
        }
        Ok(())
    }

    pub fn scheduler(&self) -> FutureScheduler<GcTask> {
        self.worker_scheduler.clone()
    }

    /// Check whether GCWorker is busy. If busy, callback will be invoked with an error that
    /// indicates GCWorker is busy; otherwise, return a new callback that invokes the original
    /// callback as well as decrease the scheduled task counter.
    fn check_is_busy<T: 'static>(&self, callback: Callback<T>) -> Option<Callback<T>> {
        if self.scheduled_tasks.fetch_add(1, atomic::Ordering::SeqCst) >= GC_MAX_EXECUTING_TASKS {
            self.scheduled_tasks.fetch_sub(1, atomic::Ordering::SeqCst);
            callback(Err(Error::from(ErrorInner::GcWorkerTooBusy)));
            return None;
        }
        let scheduled_tasks = Arc::clone(&self.scheduled_tasks);
        Some(Box::new(move |r| {
            scheduled_tasks.fetch_sub(1, atomic::Ordering::SeqCst);
            callback(r);
        }))
    }

    pub fn gc(&self, ctx: Context, safe_point: TimeStamp, callback: Callback<()>) -> Result<()> {
        GC_COMMAND_COUNTER_VEC_STATIC.gc.inc();
        self.check_is_busy(callback).map_or(Ok(()), |callback| {
            self.worker_scheduler
                .schedule(GcTask::Gc {
                    ctx,
                    safe_point,
                    callback,
                })
                .or_else(handle_gc_task_schedule_error)
        })
    }

    /// Cleans up all keys in a range and quickly free the disk space. The range might span over
    /// multiple regions, and the `ctx` doesn't indicate region. The request will be done directly
    /// on RocksDB, bypassing the Raft layer. User must promise that, after calling `destroy_range`,
    /// the range will never be accessed any more. However, `destroy_range` is allowed to be called
    /// multiple times on an single range.
    pub fn unsafe_destroy_range(
        &self,
        ctx: Context,
        start_key: Key,
        end_key: Key,
        callback: Callback<()>,
    ) -> Result<()> {
        GC_COMMAND_COUNTER_VEC_STATIC.unsafe_destroy_range.inc();
        self.check_is_busy(callback).map_or(Ok(()), |callback| {
            self.worker_scheduler
                .schedule(GcTask::UnsafeDestroyRange {
                    ctx,
                    start_key,
                    end_key,
                    callback,
                })
                .or_else(handle_gc_task_schedule_error)
        })
    }

    pub fn get_config_manager(&self) -> GcWorkerConfigManager {
        self.config_manager.clone()
    }

    pub fn physical_scan_lock(
        &self,
        ctx: Context,
        max_ts: TimeStamp,
        start_key: Key,
        limit: usize,
        callback: Callback<Vec<LockInfo>>,
    ) -> Result<()> {
        GC_COMMAND_COUNTER_VEC_STATIC.physical_scan_lock.inc();
        self.check_is_busy(callback).map_or(Ok(()), |callback| {
            self.worker_scheduler
                .schedule(GcTask::PhysicalScanLock {
                    ctx,
                    max_ts,
                    start_key,
                    limit,
                    callback,
                })
                .or_else(handle_gc_task_schedule_error)
        })
    }

    pub fn start_collecting(
        &self,
        max_ts: TimeStamp,
        callback: LockCollectorCallback<()>,
    ) -> Result<()> {
        self.applied_lock_collector
            .as_ref()
            .ok_or_else(|| box_err!("applied_lock_collector not supported"))
            .and_then(move |c| c.start_collecting(max_ts, callback))
    }

    pub fn get_collected_locks(
        &self,
        max_ts: TimeStamp,
        callback: LockCollectorCallback<(Vec<LockInfo>, bool)>,
    ) -> Result<()> {
        self.applied_lock_collector
            .as_ref()
            .ok_or_else(|| box_err!("applied_lock_collector not supported"))
            .and_then(move |c| c.get_collected_locks(max_ts, callback))
    }

    pub fn stop_collecting(
        &self,
        max_ts: TimeStamp,
        callback: LockCollectorCallback<()>,
    ) -> Result<()> {
        self.applied_lock_collector
            .as_ref()
            .ok_or_else(|| box_err!("applied_lock_collector not supported"))
            .and_then(move |c| c.stop_collecting(max_ts, callback))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::kv::{
        self, Callback as EngineCallback, Modify, Result as EngineResult, TestEngineBuilder,
    };
    use crate::storage::lock_manager::DummyLockManager;
    use crate::storage::{txn::commands, Storage, TestStorageBuilder};
    use futures::Future;
    use kvproto::kvrpcpb::Op;
    use kvproto::metapb;
    use std::collections::BTreeMap;
    use std::sync::mpsc::channel;
    use tikv_util::codec::number::NumberEncoder;
    use tikv_util::future::paired_future_callback;
    use txn_types::Mutation;

    /// A wrapper of engine that adds the 'z' prefix to keys internally.
    /// For test engines, they writes keys into db directly, but in production a 'z' prefix will be
    /// added to keys by raftstore layer before writing to db. Some functionalities of `GCWorker`
    /// bypasses Raft layer, so they needs to know how data is actually represented in db. This
    /// wrapper allows test engines write 'z'-prefixed keys to db.
    #[derive(Clone)]
    struct PrefixedEngine(kv::RocksEngine);

    impl Engine for PrefixedEngine {
        // Use RegionSnapshot which can remove the z prefix internally.
        type Snap = RegionSnapshot<RocksEngine>;

        fn async_write(
            &self,
            ctx: &Context,
            mut batch: Vec<Modify>,
            callback: EngineCallback<()>,
        ) -> EngineResult<()> {
            batch.iter_mut().for_each(|modify| match modify {
                Modify::Delete(_, ref mut key) => {
                    *key = Key::from_encoded(keys::data_key(key.as_encoded()));
                }
                Modify::Put(_, ref mut key, _) => {
                    *key = Key::from_encoded(keys::data_key(key.as_encoded()));
                }
                Modify::DeleteRange(_, ref mut start_key, ref mut end_key, _) => {
                    *start_key = Key::from_encoded(keys::data_key(start_key.as_encoded()));
                    *end_key = Key::from_encoded(keys::data_key(end_key.as_encoded()));
                }
            });
            self.0.async_write(ctx, batch, callback)
        }
        fn async_snapshot(
            &self,
            ctx: &Context,
            callback: EngineCallback<Self::Snap>,
        ) -> EngineResult<()> {
            self.0.async_snapshot(
                ctx,
                Box::new(move |(cb_ctx, r)| {
                    callback((
                        cb_ctx,
                        r.map(|snap| {
                            let mut fake_region = metapb::Region::default();
                            // Add a peer to pass initialized check.
                            fake_region.mut_peers().push(metapb::Peer::default());
                            RegionSnapshot::from_snapshot(snap, fake_region)
                        }),
                    ))
                }),
            )
        }
    }

    /// Assert the data in `storage` is the same as `expected_data`. Keys in `expected_data` should
    /// be encoded form without ts.
    fn check_data<E: Engine>(
        storage: &Storage<E, DummyLockManager>,
        expected_data: &BTreeMap<Vec<u8>, Vec<u8>>,
    ) {
        let scan_res = storage
            .scan(
                Context::default(),
                Key::from_encoded_slice(b""),
                None,
                expected_data.len() + 1,
                1.into(),
                false,
                false,
            )
            .wait()
            .unwrap();

        let all_equal = scan_res
            .into_iter()
            .map(|res| res.unwrap())
            .zip(expected_data.iter())
            .all(|((k1, v1), (k2, v2))| &k1 == k2 && &v1 == v2);
        assert!(all_equal);
    }

    fn test_destroy_range_impl(
        init_keys: &[Vec<u8>],
        start_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<()> {
        // Return Result from this function so we can use the `wait_op` macro here.

        let engine = TestEngineBuilder::new().build().unwrap();
        let storage = TestStorageBuilder::from_engine(engine.clone())
            .build()
            .unwrap();
        let db = engine.get_rocksdb();
        let mut gc_worker = GcWorker::new(engine, Some(db), None, None, GcConfig::default());
        gc_worker.start().unwrap();
        // Convert keys to key value pairs, where the value is "value-{key}".
        let data: BTreeMap<_, _> = init_keys
            .iter()
            .map(|key| {
                let mut value = b"value-".to_vec();
                value.extend_from_slice(key);
                (Key::from_raw(key).into_encoded(), value)
            })
            .collect();

        // Generate `Mutation`s from these keys.
        let mutations: Vec<_> = init_keys
            .iter()
            .map(|key| {
                let mut value = b"value-".to_vec();
                value.extend_from_slice(key);
                Mutation::Put((Key::from_raw(key), value))
            })
            .collect();
        let primary = init_keys[0].clone();

        let start_ts = start_ts.into();

        // Write these data to the storage.
        wait_op!(|cb| storage.sched_txn_command(
            commands::Prewrite::with_defaults(mutations, primary, start_ts),
            cb,
        ))
        .unwrap()
        .unwrap();

        // Commit.
        let keys: Vec<_> = init_keys.iter().map(|k| Key::from_raw(k)).collect();
        wait_op!(|cb| storage.sched_txn_command(
            commands::Commit::new(keys, start_ts, commit_ts.into(), Context::default()),
            cb
        ))
        .unwrap()
        .unwrap();

        // Assert these data is successfully written to the storage.
        check_data(&storage, &data);

        let start_key = Key::from_raw(start_key);
        let end_key = Key::from_raw(end_key);

        // Calculate expected data set after deleting the range.
        let data: BTreeMap<_, _> = data
            .into_iter()
            .filter(|(k, _)| k < start_key.as_encoded() || k >= end_key.as_encoded())
            .collect();

        // Invoke unsafe destroy range.
        wait_op!(|cb| gc_worker.unsafe_destroy_range(Context::default(), start_key, end_key, cb))
            .unwrap()
            .unwrap();

        // Check remaining data is as expected.
        check_data(&storage, &data);

        Ok(())
    }

    #[test]
    fn test_destroy_range() {
        test_destroy_range_impl(
            &[
                b"key1".to_vec(),
                b"key2".to_vec(),
                b"key3".to_vec(),
                b"key4".to_vec(),
                b"key5".to_vec(),
            ],
            5,
            10,
            b"key2",
            b"key4",
        )
        .unwrap();

        test_destroy_range_impl(
            &[b"key1".to_vec(), b"key9".to_vec()],
            5,
            10,
            b"key3",
            b"key7",
        )
        .unwrap();

        test_destroy_range_impl(
            &[
                b"key3".to_vec(),
                b"key4".to_vec(),
                b"key5".to_vec(),
                b"key6".to_vec(),
                b"key7".to_vec(),
            ],
            5,
            10,
            b"key1",
            b"key9",
        )
        .unwrap();

        test_destroy_range_impl(
            &[
                b"key1".to_vec(),
                b"key2".to_vec(),
                b"key3".to_vec(),
                b"key4".to_vec(),
                b"key5".to_vec(),
            ],
            5,
            10,
            b"key2\x00",
            b"key4",
        )
        .unwrap();

        test_destroy_range_impl(
            &[
                b"key1".to_vec(),
                b"key1\x00".to_vec(),
                b"key1\x00\x00".to_vec(),
                b"key1\x00\x00\x00".to_vec(),
            ],
            5,
            10,
            b"key1\x00",
            b"key1\x00\x00",
        )
        .unwrap();

        test_destroy_range_impl(
            &[
                b"key1".to_vec(),
                b"key1\x00".to_vec(),
                b"key1\x00\x00".to_vec(),
                b"key1\x00\x00\x00".to_vec(),
            ],
            5,
            10,
            b"key1\x00",
            b"key1\x00",
        )
        .unwrap();
    }

    #[test]
    fn test_physical_scan_lock() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let db = engine.get_rocksdb();
        let prefixed_engine = PrefixedEngine(engine);
        let storage = TestStorageBuilder::from_engine(prefixed_engine.clone())
            .build()
            .unwrap();
        let mut gc_worker =
            GcWorker::new(prefixed_engine, Some(db), None, None, GcConfig::default());
        gc_worker.start().unwrap();

        let physical_scan_lock = |max_ts: u64, start_key, limit| {
            let (cb, f) = paired_future_callback();
            gc_worker
                .physical_scan_lock(Context::default(), max_ts.into(), start_key, limit, cb)
                .unwrap();
            f.wait().unwrap()
        };

        let mut expected_lock_info = Vec::new();

        // Put locks into the storage.
        for i in 0..50 {
            let mut k = vec![];
            k.encode_u64(i).unwrap();
            let v = k.clone();

            let mutation = Mutation::Put((Key::from_raw(&k), v));

            let lock_ts = 10 + i % 3;

            // Collect all locks with ts <= 11 to check the result of physical_scan_lock.
            if lock_ts <= 11 {
                let mut info = LockInfo::default();
                info.set_primary_lock(k.clone());
                info.set_lock_version(lock_ts);
                info.set_key(k.clone());
                info.set_lock_type(Op::Put);
                expected_lock_info.push(info)
            }

            let (tx, rx) = channel();
            storage
                .sched_txn_command(
                    commands::Prewrite::with_defaults(vec![mutation], k, lock_ts.into()),
                    Box::new(move |res| tx.send(res).unwrap()),
                )
                .unwrap();
            rx.recv()
                .unwrap()
                .unwrap()
                .into_iter()
                .for_each(|r| r.unwrap());
        }

        let res = physical_scan_lock(11, Key::from_raw(b""), 50).unwrap();
        assert_eq!(res, expected_lock_info);

        let res = physical_scan_lock(11, Key::from_raw(b""), 5).unwrap();
        assert_eq!(res[..], expected_lock_info[..5]);

        let mut start_key = vec![];
        start_key.encode_u64(4).unwrap();
        let res = physical_scan_lock(11, Key::from_raw(&start_key), 6).unwrap();
        // expected_locks[3] is the key 4.
        assert_eq!(res[..], expected_lock_info[3..9]);
    }
}
