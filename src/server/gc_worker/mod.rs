// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

mod applied_lock_collector;

use std::cmp::Ordering;
use std::f64::INFINITY;
use std::fmt::{self, Display, Formatter};
use std::mem;
use std::sync::mpsc;
use std::sync::{atomic, Arc, Mutex};
use std::thread::{self, Builder as ThreadBuilder, JoinHandle};
use std::time::{Duration, Instant};

use applied_lock_collector::{AppliedLockCollector, Callback as LockCollectorCallback};
use configuration::{ConfigChange, Configuration};
use engine::rocks::util::get_cf_handle;
use engine::rocks::DB;
use engine::util::delete_all_in_range_cf;
use engine::{CF_DEFAULT, CF_LOCK, CF_WRITE};
use engine_rocks::RocksEngine;
use futures::sink::Sink;
use futures::sync::mpsc as future_mpsc;
use futures::{future, Async, Future, Poll, Stream};
use kvproto::kvrpcpb::{Context, IsolationLevel, LockInfo};
use kvproto::metapb;
use log_wrappers::DisplayValue;
use raft::StateRole;
use tokio_core::reactor::Handle;

use crate::config::ConfigManager;
use crate::raftstore::coprocessor::{CoprocessorHost, RegionInfoAccessor};
use crate::raftstore::router::ServerRaftStoreRouter;
use crate::raftstore::store::msg::StoreMsg;
use crate::raftstore::store::util::find_peer;
use crate::raftstore::store::RegionSnapshot;
use crate::server::metrics::*;
use crate::storage::kv::{
    Engine, Error as EngineError, ErrorInner as EngineErrorInner, RegionInfoProvider, ScanMode,
    Snapshot, Statistics,
};
use crate::storage::mvcc::{check_need_gc, Error as MvccError, MvccReader, MvccTxn};
use crate::storage::{Callback, Error, ErrorInner, Result};
use pd_client::PdClient;
use tikv_util::config::{ReadableSize, Tracker, VersionTrack};
use tikv_util::time::{duration_to_sec, Limiter, SlowTimer};
use tikv_util::worker::{
    FutureRunnable, FutureScheduler, FutureWorker, Stopped as FutureWorkerStopped,
};
use txn_types::{Key, TimeStamp};

/// After the GC scan of a key, output a message to the log if there are at least this many
/// versions of the key.
const GC_LOG_FOUND_VERSION_THRESHOLD: usize = 30;

/// After the GC delete versions of a key, output a message to the log if at least this many
/// versions are deleted.
const GC_LOG_DELETED_VERSION_THRESHOLD: usize = 30;

pub const GC_MAX_EXECUTING_TASKS: usize = 10;
const GC_SNAPSHOT_TIMEOUT_SECS: u64 = 10;
const GC_TASK_SLOW_SECONDS: u64 = 30;

const POLL_SAFE_POINT_INTERVAL_SECS: u64 = 60;

const BEGIN_KEY: &[u8] = b"";

const PROCESS_TYPE_GC: &str = "gc";
const PROCESS_TYPE_SCAN: &str = "scan";

const DEFAULT_GC_RATIO_THRESHOLD: f64 = 1.1;
pub const DEFAULT_GC_BATCH_KEYS: usize = 512;
// No limit
const DEFAULT_GC_MAX_WRITE_BYTES_PER_SEC: u64 = 0;

const FUTURE_STREAM_BUFFER_SIZE: usize = 8;
const SCAN_LOCK_BATCH_SIZE: usize = 128;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Configuration)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct GcConfig {
    pub ratio_threshold: f64,
    pub batch_keys: usize,
    pub max_write_bytes_per_sec: ReadableSize,
}

impl Default for GcConfig {
    fn default() -> GcConfig {
        GcConfig {
            ratio_threshold: DEFAULT_GC_RATIO_THRESHOLD,
            batch_keys: DEFAULT_GC_BATCH_KEYS,
            max_write_bytes_per_sec: ReadableSize(DEFAULT_GC_MAX_WRITE_BYTES_PER_SEC),
        }
    }
}

impl GcConfig {
    pub fn validate(&self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        if self.batch_keys == 0 {
            return Err(("gc.batch_keys should not be 0.").into());
        }
        Ok(())
    }
}

pub type GcWorkerConfigManager = Arc<VersionTrack<GcConfig>>;

impl ConfigManager for GcWorkerConfigManager {
    fn dispatch(
        &mut self,
        change: ConfigChange,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        {
            let change = change.clone();
            self.update(move |cfg: &mut GcConfig| cfg.update(change));
        }
        info!(
            "GC worker config changed";
            "change" => ?change,
        );
        Ok(())
    }
}

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

enum GcTask {
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
        sender: future_mpsc::Sender<Result<Vec<LockInfo>>>,
    },
    #[cfg(test)]
    Validate(Box<dyn FnOnce(&GcConfig, &Limiter) + Send>),
}

impl GcTask {
    pub fn get_label(&self) -> &'static str {
        match self {
            GcTask::Gc { .. } => "gc",
            GcTask::UnsafeDestroyRange { .. } => "unsafe_destroy_range",
            GcTask::PhysicalScanLock { .. } => "physical_scan_lock",
            #[cfg(test)]
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
            #[cfg(test)]
            GcTask::Validate(_) => write!(f, "Validate gc worker config"),
        }
    }
}

struct LockScanner<S: Snapshot> {
    reader: MvccReader<S>,
    max_ts: TimeStamp,
    next_key: Option<Key>,
    batch_size: usize,
    is_finished: bool,
}

impl<S: Snapshot> LockScanner<S> {
    pub fn new(snapshot: S, max_ts: TimeStamp) -> Self {
        let reader = MvccReader::new(snapshot, Some(ScanMode::Forward), false, IsolationLevel::Si);
        Self {
            reader,
            max_ts,
            next_key: None,
            batch_size: SCAN_LOCK_BATCH_SIZE,
            is_finished: false,
        }
    }

    fn poll_impl(&mut self) -> Poll<Option<Vec<LockInfo>>, Error> {
        if self.is_finished {
            return Ok(Async::Ready(None));
        }
        let max_ts = self.max_ts;

        let scan_lock_res =
            self.reader
                .scan_locks(self.next_key.as_ref(), |l| l.ts <= max_ts, self.batch_size);
        let (locks, is_remain) = match scan_lock_res {
            Ok(res) => res,
            Err(e) => {
                // Do not scan more elements once error happens.
                self.is_finished = true;
                return Err(e.into());
            }
        };

        if is_remain {
            let mut next_key = locks.last().unwrap().0.clone().into_encoded();
            // Move to the next key
            next_key.push(0);
            self.next_key = Some(Key::from_encoded(next_key));
        } else {
            self.is_finished = true;
        }

        let mut lock_infos = Vec::with_capacity(locks.len());

        for (key, lock) in locks {
            let raw_key = match key.into_raw() {
                Ok(k) => k,
                Err(e) => {
                    self.is_finished = true;
                    let e = Error::from(MvccError::from(e));
                    return Err(e);
                }
            };
            lock_infos.push(lock.into_lock_info(raw_key))
        }

        Ok(Async::Ready(Some(lock_infos)))
    }

    fn update_statistics_metrics(&mut self) {
        let mut stats = Statistics::default();
        self.reader.collect_statistics_into(&mut stats);
        for (cf, details) in stats.details().iter() {
            for (tag, count) in details.iter() {
                GC_PHYSICAL_SCAN_LOCK_COUNTER_VEC
                    .with_label_values(&[cf, *tag])
                    .inc_by(*count as i64);
            }
        }
    }
}

impl<S: Snapshot> Stream for LockScanner<S> {
    type Item = Vec<LockInfo>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let result = self.poll_impl();
        self.update_statistics_metrics();
        result
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

        let collection =
            match engine::util::get_range_properties_cf(&db, CF_WRITE, &start_key, &end_key) {
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
            delete_all_in_range_cf(local_storage, cf, &start_data_key, &end_data_key, false)
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
        handle: &Handle,
        _: &Context,
        max_ts: TimeStamp,
        sender: future_mpsc::Sender<Result<Vec<LockInfo>>>,
    ) {
        let db = match &self.local_storage {
            Some(db) => Arc::clone(&db),
            None => {
                let e = box_err!("local storage not set, physical scan lock not supported");
                handle.spawn(sender.send(Err(e)).map(|_| ()).map_err(|e| {
                    error!("send physical scan lock result from GCRunner failed"; "err" => ?e);
                }));
                return;
            }
        };

        info!("physical scan lock started"; "max_ts" => %max_ts);

        // Create a `RegionSnapshot`, which can converts the 'z'-prefixed keys into normal keys
        // internally. A fake region meta is given to make the snapshot's range unbounded.
        // TODO: Should we implement a special snapshot and iterator types for this?
        let mut fake_region = metapb::Region::default();
        // Add a peer to pass initialized check.
        fake_region.mut_peers().push(metapb::Peer::default());
        let snap = RegionSnapshot::<RocksEngine>::from_raw(db, fake_region);
        let lock_scanner = LockScanner::new(snap, max_ts);

        let future = sender
            .send_all(lock_scanner.then(|res| Ok(res)))
            .map_err(|e| {
                error!("send physical scan lock result from GCRunner failed"; "err" => ?e);
            })
            .map(move |_| {
                info!("physical scan lock finished"; "max_ts" => %max_ts);
            });

        handle.spawn(future);
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
    fn run(&mut self, task: GcTask, handle: &Handle) {
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
                sender,
            } => self.handle_physical_scan_lock(handle, &ctx, max_ts, sender),
            #[cfg(test)]
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
fn gc(scheduler: &FutureScheduler<GcTask>, ctx: Context, safe_point: TimeStamp) -> Result<()> {
    wait_op!(|callback| schedule_gc(scheduler, ctx, safe_point, callback)).unwrap_or_else(|| {
        error!("failed to receive result of gc");
        Err(box_err!("gc_worker: failed to receive result of gc"))
    })
}

/// The configurations of automatic GC.
pub struct AutoGcConfig<S: GcSafePointProvider, R: RegionInfoProvider> {
    pub safe_point_provider: S,
    pub region_info_provider: R,

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

impl<S: GcSafePointProvider, R: RegionInfoProvider> AutoGcConfig<S, R> {
    /// Creates a new config.
    pub fn new(safe_point_provider: S, region_info_provider: R, self_store_id: u64) -> Self {
        Self {
            safe_point_provider,
            region_info_provider,
            self_store_id,
            poll_safe_point_interval: Duration::from_secs(POLL_SAFE_POINT_INTERVAL_SECS),
            always_check_safe_point: false,
            post_a_round_of_gc: None,
        }
    }

    /// Creates a config for test purpose. The interval to poll safe point is as short as 0.1s and
    /// during GC it never skips checking safe point.
    pub fn new_test_cfg(
        safe_point_provider: S,
        region_info_provider: R,
        self_store_id: u64,
    ) -> Self {
        Self {
            safe_point_provider,
            region_info_provider,
            self_store_id,
            poll_safe_point_interval: Duration::from_millis(100),
            always_check_safe_point: true,
            post_a_round_of_gc: None,
        }
    }
}

/// The only error that will break `GcManager`'s process is that the `GcManager` is interrupted by
/// others, maybe due to TiKV shutting down.
#[derive(Debug)]
enum GcManagerError {
    Stopped,
}

type GcManagerResult<T> = std::result::Result<T, GcManagerError>;

/// Used to check if `GcManager` should be stopped.
///
/// When `GcManager` is running, it might take very long time to GC a round. It should be able to
/// break at any time so that we can shut down TiKV in time.
struct GcManagerContext {
    /// Used to receive stop signal. The sender side is hold in `GcManagerHandler`.
    /// If this field is `None`, the `GcManagerContext` will never stop.
    stop_signal_receiver: Option<mpsc::Receiver<()>>,
    /// Whether an stop signal is received.
    is_stopped: bool,
}

impl GcManagerContext {
    pub fn new() -> Self {
        Self {
            stop_signal_receiver: None,
            is_stopped: false,
        }
    }

    /// Sets the receiver that used to receive the stop signal. `GcManagerContext` will be
    /// considered to be stopped as soon as a message is received from the receiver.
    pub fn set_stop_signal_receiver(&mut self, rx: mpsc::Receiver<()>) {
        self.stop_signal_receiver = Some(rx);
    }

    /// Sleeps for a while. if a stop message is received, returns immediately with
    /// `GcManagerError::Stopped`.
    fn sleep_or_stop(&mut self, timeout: Duration) -> GcManagerResult<()> {
        if self.is_stopped {
            return Err(GcManagerError::Stopped);
        }
        match self.stop_signal_receiver.as_ref() {
            Some(rx) => match rx.recv_timeout(timeout) {
                Ok(_) => {
                    self.is_stopped = true;
                    Err(GcManagerError::Stopped)
                }
                Err(mpsc::RecvTimeoutError::Timeout) => Ok(()),
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    panic!("stop_signal_receiver unexpectedly disconnected")
                }
            },
            None => {
                thread::sleep(timeout);
                Ok(())
            }
        }
    }

    /// Checks if a stop message has been fired. Returns `GcManagerError::Stopped` if there's such
    /// a message.
    fn check_stopped(&mut self) -> GcManagerResult<()> {
        if self.is_stopped {
            return Err(GcManagerError::Stopped);
        }
        match self.stop_signal_receiver.as_ref() {
            Some(rx) => match rx.try_recv() {
                Ok(_) => {
                    self.is_stopped = true;
                    Err(GcManagerError::Stopped)
                }
                Err(mpsc::TryRecvError::Empty) => Ok(()),
                Err(mpsc::TryRecvError::Disconnected) => {
                    error!("stop_signal_receiver unexpectedly disconnected, gc_manager will stop");
                    Err(GcManagerError::Stopped)
                }
            },
            None => Ok(()),
        }
    }
}

/// Composites a `kvproto::Context` with the given `region` and `peer`.
fn make_context(mut region: metapb::Region, peer: metapb::Peer) -> Context {
    let mut ctx = Context::default();
    ctx.set_region_id(region.get_id());
    ctx.set_region_epoch(region.take_region_epoch());
    ctx.set_peer(peer);
    ctx.set_not_fill_cache(true);
    ctx
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

/// Wraps `JoinHandle` of `GcManager` and helps to stop the `GcManager` synchronously.
struct GcManagerHandle {
    join_handle: JoinHandle<()>,
    stop_signal_sender: mpsc::Sender<()>,
}

impl GcManagerHandle {
    /// Stops the `GcManager`.
    pub fn stop(self) -> Result<()> {
        let res: Result<()> = self
            .stop_signal_sender
            .send(())
            .map_err(|e| box_err!("failed to send stop signal to gc worker thread: {:?}", e));
        if res.is_err() {
            return res;
        }
        self.join_handle
            .join()
            .map_err(|e| box_err!("failed to join gc worker thread: {:?}", e))
    }
}

/// Controls how GC runs automatically on the TiKV.
/// It polls safe point periodically, and when the safe point is updated, `GcManager` will start to
/// scan all regions (whose leader is on this TiKV), and does GC on all those regions.
struct GcManager<S: GcSafePointProvider, R: RegionInfoProvider> {
    cfg: AutoGcConfig<S, R>,

    /// The current safe point. `GcManager` will try to update it periodically. When `safe_point` is
    /// updated, `GCManager` will start to do GC on all regions.
    safe_point: TimeStamp,

    safe_point_last_check_time: Instant,

    /// Used to schedule `GcTask`s.
    worker_scheduler: FutureScheduler<GcTask>,

    /// Holds the running status. It will tell us if `GcManager` should stop working and exit.
    gc_manager_ctx: GcManagerContext,
}

impl<S: GcSafePointProvider, R: RegionInfoProvider> GcManager<S, R> {
    pub fn new(
        cfg: AutoGcConfig<S, R>,
        worker_scheduler: FutureScheduler<GcTask>,
    ) -> GcManager<S, R> {
        GcManager {
            cfg,
            safe_point: TimeStamp::zero(),
            safe_point_last_check_time: Instant::now(),
            worker_scheduler,
            gc_manager_ctx: GcManagerContext::new(),
        }
    }

    /// Starts working in another thread. This function moves the `GcManager` and returns a handler
    /// of it.
    fn start(mut self) -> Result<GcManagerHandle> {
        set_status_metrics(GcManagerState::Init);
        self.initialize();

        let (tx, rx) = mpsc::channel();
        self.gc_manager_ctx.set_stop_signal_receiver(rx);
        let res: Result<_> = ThreadBuilder::new()
            .name(thd_name!("gc-manager"))
            .spawn(move || {
                self.run();
            })
            .map_err(|e| box_err!("failed to start gc manager: {:?}", e));
        res.map(|join_handle| GcManagerHandle {
            join_handle,
            stop_signal_sender: tx,
        })
    }

    /// Polls safe point and does GC in a loop, again and again, until interrupted by invoking
    /// `GcManagerHandle::stop`.
    fn run(&mut self) {
        debug!("gc-manager is started");
        self.run_impl().unwrap_err();
        set_status_metrics(GcManagerState::None);
        debug!("gc-manager is stopped");
    }

    fn run_impl(&mut self) -> GcManagerResult<()> {
        loop {
            AUTO_GC_PROCESSED_REGIONS_GAUGE_VEC
                .with_label_values(&[PROCESS_TYPE_GC])
                .set(0);
            AUTO_GC_PROCESSED_REGIONS_GAUGE_VEC
                .with_label_values(&[PROCESS_TYPE_SCAN])
                .set(0);

            set_status_metrics(GcManagerState::Idle);
            self.wait_for_next_safe_point()?;

            set_status_metrics(GcManagerState::Working);
            self.gc_a_round()?;

            if let Some(on_finished) = self.cfg.post_a_round_of_gc.as_ref() {
                on_finished();
            }
        }
    }

    /// Sets the initial state of the `GCManger`.
    /// The only task of initializing is to simply get the current safe point as the initial value
    /// of `safe_point`. TiKV won't do any GC automatically until the first time `safe_point` was
    /// updated to a greater value than initial value.
    fn initialize(&mut self) {
        debug!("gc-manager is initializing");
        self.safe_point = TimeStamp::zero();
        self.try_update_safe_point();
        debug!("gc-manager started"; "safe_point" => self.safe_point);
    }

    /// Waits until the safe_point updates. Returns the new safe point.
    fn wait_for_next_safe_point(&mut self) -> GcManagerResult<TimeStamp> {
        loop {
            if self.try_update_safe_point() {
                return Ok(self.safe_point);
            }

            self.gc_manager_ctx
                .sleep_or_stop(self.cfg.poll_safe_point_interval)?;
        }
    }

    /// Tries to update the safe point. Returns true if safe point has been updated to a greater
    /// value. Returns false if safe point didn't change or we encountered an error.
    fn try_update_safe_point(&mut self) -> bool {
        self.safe_point_last_check_time = Instant::now();

        let safe_point = match self.cfg.safe_point_provider.get_safe_point() {
            Ok(res) => res,
            // Return false directly so we will check it a while later.
            Err(e) => {
                error!("failed to get safe point from pd"; "err" => ?e);
                return false;
            }
        };

        match safe_point.cmp(&self.safe_point) {
            Ordering::Less => {
                panic!(
                    "got new safe point {} which is less than current safe point {}. \
                     there must be something wrong.",
                    safe_point, self.safe_point
                );
            }
            Ordering::Equal => false,
            Ordering::Greater => {
                debug!("gc_worker: update safe point"; "safe_point" => safe_point);
                self.safe_point = safe_point;
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
    fn gc_a_round(&mut self) -> GcManagerResult<()> {
        let mut need_rewind = false;
        // Represents where we should stop doing GC. `None` means the very end of the TiKV.
        let mut end = None;
        // Represents where we have GC-ed to. `None` means the very end of the TiKV.
        let mut progress = Some(Key::from_encoded(BEGIN_KEY.to_vec()));

        // Records how many region we have GC-ed.
        let mut processed_regions = 0;

        info!(
            "gc_worker: start auto gc"; "safe_point" => self.safe_point
        );

        // The following loop iterates all regions whose leader is on this TiKV and does GC on them.
        // At the same time, check whether safe_point is updated periodically. If it's updated,
        // rewinding will happen.
        loop {
            self.gc_manager_ctx.check_stopped()?;

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
                    return Ok(());
                }
            }

            assert!(progress.is_some());

            // Before doing GC, check whether safe_point is updated periodically to determine if
            // rewinding is needed.
            self.check_if_need_rewind(&progress, &mut need_rewind, &mut end);

            progress = self.gc_next_region(progress.unwrap(), &mut processed_regions)?;
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
                "gc_worker: auto gc will go to the end"; "safe_point" => self.safe_point
            );
        } else {
            *need_rewind = true;
            *end = progress.clone();
            info!(
                "gc_worker: auto gc will go to rewind"; "safe_point" => self.safe_point,
                "next_rewind_key" => %(end.as_ref().unwrap())
            );
        }
    }

    /// Does GC on the next region after `from_key`. Returns the end key of the region it processed.
    /// If we have processed to the end of all regions, returns `None`.
    fn gc_next_region(
        &mut self,
        from_key: Key,
        processed_regions: &mut usize,
    ) -> GcManagerResult<Option<Key>> {
        // Get the information of the next region to do GC.
        let (ctx, next_key) = self.get_next_gc_context(from_key);
        if ctx.is_none() {
            // No more regions.
            return Ok(None);
        }
        let ctx = ctx.unwrap();

        // Do GC.
        // Ignore the error and continue, since it's useless to retry this.
        // TODO: Find a better way to handle errors. Maybe we should retry.
        debug!(
            "trying gc"; "region_id" => ctx.get_region_id(), "region_epoch" => ?ctx.region_epoch.as_ref(),
            "end_key" => next_key.as_ref().map(DisplayValue)
        );
        if let Err(e) = gc(&self.worker_scheduler, ctx.clone(), self.safe_point) {
            error!(
                "failed gc"; "region_id" => ctx.get_region_id(), "region_epoch" => ?ctx.region_epoch.as_ref(),
                "end_key" => next_key.as_ref().map(DisplayValue),
                "err" => ?e
            );
        }
        *processed_regions += 1;
        AUTO_GC_PROCESSED_REGIONS_GAUGE_VEC
            .with_label_values(&[PROCESS_TYPE_GC])
            .inc();

        Ok(next_key)
    }

    /// Gets the next region with end_key greater than given key, and the current TiKV is its
    /// leader, so we can do GC on it.
    /// Returns context to call GC and end_key of the region. The returned end_key will be none if
    /// the region's end_key is empty.
    fn get_next_gc_context(&mut self, key: Key) -> (Option<Context>, Option<Key>) {
        let (tx, rx) = mpsc::channel();
        let store_id = self.cfg.self_store_id;

        let res = self.cfg.region_info_provider.seek_region(
            key.as_encoded(),
            Box::new(move |iter| {
                let mut scanned_regions = 0;
                for info in iter {
                    scanned_regions += 1;
                    if info.role == StateRole::Leader {
                        if find_peer(&info.region, store_id).is_some() {
                            let _ = tx.send((Some(info.region.clone()), scanned_regions));
                            return;
                        }
                    }
                }
                let _ = tx.send((None, scanned_regions));
            }),
        );

        if let Err(e) = res {
            error!(
                "gc_worker: failed to get next region information"; "err" => ?e
            );
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
                let peer = find_peer(&region, store_id).unwrap().clone();
                let end_key = region.take_end_key();
                let next_key = if end_key.is_empty() {
                    None
                } else {
                    Some(Key::from_encoded(end_key))
                };
                (Some(make_context(region, peer)), next_key)
            }
            Ok(None) => (None, None),
            Err(e) => {
                error!("failed to get next region information"; "err" => ?e);
                (None, None)
            }
        }
    }
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
            config_manager: Arc::new(VersionTrack::new(cfg)),
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
            self.config_manager.clone().tracker("gc-woker".to_owned()),
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
        Arc::clone(&self.config_manager)
    }

    pub fn physical_scan_lock(
        &self,
        ctx: Context,
        max_ts: TimeStamp,
    ) -> impl Stream<Item = Vec<LockInfo>, Error = Error> {
        // TODO: Metrics
        let (tx, rx) = future_mpsc::channel(FUTURE_STREAM_BUFFER_SIZE);
        let res = self
            .worker_scheduler
            .schedule(GcTask::PhysicalScanLock {
                ctx,
                max_ts,
                sender: tx,
            })
            .or_else(handle_gc_task_schedule_error);

        future::result(res)
            .and_then(move |_| {
                let stream = rx
                    .map_err(move |_| {
                        error!(
                            "failed to receive physical scan lock results from GCRunner";
                            "max_ts" => %max_ts
                        );
                        box_err!("failed to receive physical scan lock results from GCRunner")
                    })
                    .and_then(|res| res);
                Ok(stream)
            })
            .into_stream()
            .flatten()
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
    use crate::config::{ConfigController, TiKvConfig};
    use crate::raftstore::coprocessor::{RegionInfo, SeekRegionCallback};
    use crate::raftstore::store::util::new_peer;
    use crate::storage::kv::{
        self, Callback as EngineCallback, Modify, Result as EngineResult, TestEngineBuilder,
    };
    use crate::storage::lock_manager::DummyLockManager;
    use crate::storage::{Storage, TestStorageBuilder};
    use futures::Future;
    use kvproto::kvrpcpb::Op;
    use kvproto::metapb;
    use std::collections::BTreeMap;
    use std::mem;
    use std::sync::mpsc::{channel, Receiver, Sender};
    use tikv_util::codec::number::NumberEncoder;
    use txn_types::Mutation;

    fn take_callback(t: &mut GcTask) -> Callback<()> {
        let callback = match t {
            GcTask::Gc {
                ref mut callback, ..
            } => callback,
            GcTask::UnsafeDestroyRange {
                ref mut callback, ..
            } => callback,
            GcTask::PhysicalScanLock { .. } => unreachable!(),
            GcTask::Validate(_) => unreachable!(),
        };
        mem::replace(callback, Box::new(|_| {}))
    }

    struct MockSafePointProvider {
        rx: Receiver<TimeStamp>,
    }

    impl GcSafePointProvider for MockSafePointProvider {
        fn get_safe_point(&self) -> Result<TimeStamp> {
            // Error will be ignored by `GcManager`, which is equivalent to that the safe_point
            // is not updated.
            self.rx.try_recv().map_err(|e| box_err!(e))
        }
    }

    #[derive(Clone)]
    struct MockRegionInfoProvider {
        // start_key -> (region_id, end_key)
        regions: BTreeMap<Vec<u8>, RegionInfo>,
    }

    impl RegionInfoProvider for MockRegionInfoProvider {
        fn seek_region(&self, from: &[u8], callback: SeekRegionCallback) -> EngineResult<()> {
            let from = from.to_vec();
            callback(&mut self.regions.range(from..).map(|(_, v)| v));
            Ok(())
        }
    }

    struct MockGcRunner {
        tx: Sender<GcTask>,
    }

    impl FutureRunnable<GcTask> for MockGcRunner {
        fn run(&mut self, mut t: GcTask, _handle: &Handle) {
            let cb = take_callback(&mut t);
            self.tx.send(t).unwrap();
            cb(Ok(()));
        }
    }

    /// A set of utilities that helps testing `GcManager`.
    /// The safe_point polling interval is set to 100 ms.
    struct GcManagerTestUtil {
        gc_manager: Option<GcManager<MockSafePointProvider, MockRegionInfoProvider>>,
        worker: FutureWorker<GcTask>,
        safe_point_sender: Sender<TimeStamp>,
        gc_task_receiver: Receiver<GcTask>,
    }

    impl GcManagerTestUtil {
        pub fn new(regions: BTreeMap<Vec<u8>, RegionInfo>) -> Self {
            let mut worker = FutureWorker::new("test-gc-worker");
            let (gc_task_sender, gc_task_receiver) = channel();
            worker.start(MockGcRunner { tx: gc_task_sender }).unwrap();

            let (safe_point_sender, safe_point_receiver) = channel();

            let mut cfg = AutoGcConfig::new(
                MockSafePointProvider {
                    rx: safe_point_receiver,
                },
                MockRegionInfoProvider { regions },
                1,
            );
            cfg.poll_safe_point_interval = Duration::from_millis(100);
            cfg.always_check_safe_point = true;

            let gc_manager = GcManager::new(cfg, worker.scheduler());
            Self {
                gc_manager: Some(gc_manager),
                worker,
                safe_point_sender,
                gc_task_receiver,
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
            self.worker.stop().unwrap().join().unwrap();
        }
    }

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
        test_util.gc_manager.as_mut().unwrap().initialize();

        test_util.gc_manager.as_mut().unwrap().gc_a_round().unwrap();
        test_util.stop();

        let gc_tasks: Vec<_> = test_util
            .collect_scheduled_tasks()
            .iter()
            .map(|task| match task {
                GcTask::Gc {
                    ctx, safe_point, ..
                } => (ctx.get_region_id(), *safe_point),
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
    fn test_make_context() {
        let mut peer = metapb::Peer::default();
        peer.set_id(233);
        peer.set_store_id(2333);

        let mut epoch = metapb::RegionEpoch::default();
        epoch.set_conf_ver(123);
        epoch.set_version(456);
        let mut region = metapb::Region::default();
        region.set_region_epoch(epoch.clone());
        region.set_id(789);

        let ctx = make_context(region.clone(), peer.clone());
        assert_eq!(ctx.get_region_id(), region.get_id());
        assert_eq!(ctx.get_peer(), &peer);
        assert_eq!(ctx.get_region_epoch(), &epoch);
    }

    #[test]
    fn test_update_safe_point() {
        let mut test_util = GcManagerTestUtil::new(BTreeMap::new());
        let mut gc_manager = test_util.gc_manager.take().unwrap();
        assert_eq!(gc_manager.safe_point, TimeStamp::zero());
        test_util.add_next_safe_point(233);
        assert!(gc_manager.try_update_safe_point());
        assert_eq!(gc_manager.safe_point, 233.into());

        let (tx, rx) = channel();
        ThreadBuilder::new()
            .spawn(move || {
                let safe_point = gc_manager.wait_for_next_safe_point().unwrap();
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
        assert_eq!(gc_manager.safe_point, TimeStamp::zero());
        test_util.add_next_safe_point(0);
        test_util.add_next_safe_point(5);
        gc_manager.initialize();
        assert_eq!(gc_manager.safe_point, TimeStamp::zero());
        assert!(gc_manager.try_update_safe_point());
        assert_eq!(gc_manager.safe_point, 5.into());
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
        wait_op!(|cb| storage.prewrite(
            Context::default(),
            mutations,
            primary,
            start_ts,
            0,
            false,
            0,
            TimeStamp::default(),
            cb
        ))
        .unwrap()
        .unwrap();

        // Commit.
        let keys: Vec<_> = init_keys.iter().map(|k| Key::from_raw(k)).collect();
        wait_op!(|cb| storage.commit(Context::default(), keys, start_ts, commit_ts.into(), cb))
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
    fn test_gc_config_validate() {
        let cfg = GcConfig::default();
        cfg.validate().unwrap();

        let mut invalid_cfg = GcConfig::default();
        invalid_cfg.batch_keys = 0;
        assert!(invalid_cfg.validate().is_err());
    }

    fn setup_cfg_controller(
        cfg: TiKvConfig,
    ) -> (GcWorker<crate::storage::kv::RocksEngine>, ConfigController) {
        let engine = TestEngineBuilder::new().build().unwrap();
        let mut gc_worker = GcWorker::new(engine, None, None, None, cfg.gc.clone());
        gc_worker.start().unwrap();

        let mut cfg_controller = ConfigController::new(cfg, Default::default());
        cfg_controller.register("gc", Box::new(gc_worker.get_config_manager()));

        (gc_worker, cfg_controller)
    }

    fn validate<F>(scheduler: &FutureScheduler<GcTask>, f: F)
    where
        F: FnOnce(&GcConfig, &Limiter) + Send + 'static,
    {
        let (tx, rx) = channel();
        scheduler
            .schedule(GcTask::Validate(Box::new(
                move |cfg: &GcConfig, limiter: &Limiter| {
                    f(cfg, limiter);
                    tx.send(()).unwrap();
                },
            )))
            .unwrap();
        rx.recv_timeout(Duration::from_secs(3)).unwrap();
    }

    #[allow(clippy::float_cmp)]
    #[test]
    fn test_gc_worker_config_update() {
        let mut cfg = TiKvConfig::default();
        cfg.validate().unwrap();
        let (gc_worker, mut cfg_controller) = setup_cfg_controller(cfg.clone());
        let scheduler = gc_worker.worker_scheduler.clone();

        // update of other module's config should not effect gc worker config
        let mut incoming = cfg.clone();
        incoming.raft_store.raft_log_gc_threshold = 2000;
        let rollback = cfg_controller.update_or_rollback(incoming).unwrap();
        assert!(rollback.right().unwrap());
        validate(&scheduler, move |cfg: &GcConfig, _| {
            assert_eq!(cfg, &GcConfig::default());
        });

        // Update gc worker config
        let mut incoming = cfg;
        incoming.gc.ratio_threshold = 1.23;
        incoming.gc.batch_keys = 1234;
        incoming.gc.max_write_bytes_per_sec = ReadableSize(1024);
        let rollback = cfg_controller.update_or_rollback(incoming).unwrap();
        assert!(rollback.right().unwrap());
        validate(&scheduler, move |cfg: &GcConfig, _| {
            assert_eq!(cfg.ratio_threshold, 1.23);
            assert_eq!(cfg.batch_keys, 1234);
            assert_eq!(cfg.max_write_bytes_per_sec, ReadableSize(1024));
        });
    }

    #[test]
    fn test_change_io_limit_by_config_manager() {
        let mut cfg = TiKvConfig::default();
        cfg.validate().unwrap();
        let (gc_worker, mut cfg_controller) = setup_cfg_controller(cfg.clone());
        let scheduler = gc_worker.worker_scheduler.clone();

        validate(&scheduler, move |_, limiter: &Limiter| {
            assert_eq!(limiter.speed_limit(), INFINITY);
        });

        // Enable io iolimit
        let mut incoming = cfg.clone();
        incoming.gc.max_write_bytes_per_sec = ReadableSize(1024);
        let rollback = cfg_controller.update_or_rollback(incoming).unwrap();
        assert!(rollback.right().unwrap());
        validate(&scheduler, move |_, limiter: &Limiter| {
            assert_eq!(limiter.speed_limit(), 1024.0);
        });

        // Change io iolimit
        let mut incoming = cfg.clone();
        incoming.gc.max_write_bytes_per_sec = ReadableSize(2048);
        let rollback = cfg_controller.update_or_rollback(incoming).unwrap();
        assert!(rollback.right().unwrap());
        validate(&scheduler, move |_, limiter: &Limiter| {
            assert_eq!(limiter.speed_limit(), 2048.0);
        });

        // Disable io iolimit
        let mut incoming = cfg;
        incoming.gc.max_write_bytes_per_sec = ReadableSize(0);
        let rollback = cfg_controller.update_or_rollback(incoming).unwrap();
        assert!(rollback.right().unwrap());
        validate(&scheduler, move |_, limiter: &Limiter| {
            assert_eq!(limiter.speed_limit(), INFINITY);
        });
    }

    #[test]
    fn test_change_io_limit_by_debugger() {
        // Debugger use GcWorkerConfigManager to change io limit
        let mut cfg = TiKvConfig::default();
        cfg.validate().unwrap();
        let (gc_worker, _) = setup_cfg_controller(cfg);
        let scheduler = gc_worker.worker_scheduler.clone();
        let config_manager = gc_worker.get_config_manager();

        validate(&scheduler, move |_, limiter: &Limiter| {
            assert_eq!(limiter.speed_limit(), INFINITY);
        });

        // Enable io iolimit
        config_manager
            .update(|cfg: &mut GcConfig| cfg.max_write_bytes_per_sec = ReadableSize(1024));
        validate(&scheduler, move |_, limiter: &Limiter| {
            assert_eq!(limiter.speed_limit(), 1024.0);
        });

        // Change io iolimit
        config_manager
            .update(|cfg: &mut GcConfig| cfg.max_write_bytes_per_sec = ReadableSize(2048));
        validate(&scheduler, move |_, limiter: &Limiter| {
            assert_eq!(limiter.speed_limit(), 2048.0);
        });

        // Disable io iolimit
        config_manager.update(|cfg: &mut GcConfig| cfg.max_write_bytes_per_sec = ReadableSize(0));
        validate(&scheduler, move |_, limiter: &Limiter| {
            assert_eq!(limiter.speed_limit(), INFINITY);
        });
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

        let mut expected_lock_info = Vec::new();

        // Put locks into the storage.
        for i in 0..(SCAN_LOCK_BATCH_SIZE as u64 * 3) {
            let mut k = vec![];
            k.encode_u64(i).unwrap();
            let v = k.clone();

            let mutation = Mutation::Put((Key::from_raw(&k), v));

            let lock_ts = 10 + i % 3;

            // Collect all locks with ts < 11 to check the result of physical_scan_lock.
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
                .prewrite(
                    Context::default(),
                    vec![mutation],
                    k,
                    lock_ts.into(),
                    0,
                    false,
                    0,
                    TimeStamp::default(),
                    Box::new(move |res| tx.send(res).unwrap()),
                )
                .unwrap();
            rx.recv()
                .unwrap()
                .unwrap()
                .into_iter()
                .for_each(|r| r.unwrap());
        }

        let res: Vec<_> = gc_worker
            .physical_scan_lock(Context::default(), 11.into())
            .wait()
            .map(|r| r.unwrap())
            .flatten()
            .collect();
        assert_eq!(res, expected_lock_info);
    }
}
