use std::f64::INFINITY;
use std::fmt::{self, Display, Formatter};
use std::mem;
use std::time::Instant;

use concurrency_manager::ConcurrencyManager;
use engine_rocks::RocksEngine;
use engine_traits::{DeleteStrategy, MiscExt, Range, CF_DEFAULT, CF_LOCK, CF_WRITE};
use kvproto::kvrpcpb::{Context, IsolationLevel, LockInfo};
use raftstore::router::RaftStoreRouter;
use raftstore::store::msg::StoreMsg;
use tikv_util::config::Tracker;
use tikv_util::time::{duration_to_sec, Limiter, SlowTimer};
use txn_types::{Key, TimeStamp};

use crate::server::metrics::*;
use crate::storage::kv::{Engine, ScanMode, Statistics};
use crate::storage::mvcc::{check_need_gc, Error as MvccError, GcInfo, MvccReader, MvccTxn};

use super::config::GcConfig;
use super::{Callback, Error, Result};
/// After the GC scan of a key, output a message to the log if there are at least this many
/// versions of the key.
const GC_LOG_FOUND_VERSION_THRESHOLD: usize = 30;

/// After the GC delete versions of a key, output a message to the log if at least this many
/// versions are deleted.
const GC_LOG_DELETED_VERSION_THRESHOLD: usize = 30;

const GC_TASK_SLOW_SECONDS: u64 = 30;

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

pub trait GcRunnable: Send {
    fn run(&mut self, task: GcTask);
}

/// Used to perform GC operations on the engine.
pub struct GcRunner<E, RR>
where
    E: Engine,
    RR: RaftStoreRouter<RocksEngine>,
{
    engine: E,

    raft_store_router: RR,

    /// Used to limit the write flow of GC.
    limiter: Limiter,

    cfg: GcConfig,
    cfg_tracker: Tracker<GcConfig>,

    stats: Statistics,
}

impl<E, RR> GcRunner<E, RR>
where
    E: Engine,
    RR: RaftStoreRouter<RocksEngine>,
{
    pub fn new(
        engine: E,
        raft_store_router: RR,
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
            .send_store_msg(StoreMsg::ClearRegionSizeInRange {
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

impl<E, RR> GcRunnable for GcRunner<E, RR>
where
    E: Engine,
    RR: RaftStoreRouter<RocksEngine>,
{
    #[inline]
    fn run(&mut self, task: GcTask) {
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
                callback.map(|cb| cb(res));
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
