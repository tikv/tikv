// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Display, Formatter},
    iter::Peekable,
    mem,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc::Sender,
        Arc, Mutex,
    },
    vec::IntoIter,
};

use api_version::{ApiV2, KvFormat};
use collections::HashMap;
use concurrency_manager::ConcurrencyManager;
use engine_rocks::FlowInfo;
use engine_traits::{
    raw_ttl::ttl_current_ts, DeleteStrategy, Error as EngineError, KvEngine, MiscExt, Range,
    WriteBatch, WriteOptions, CF_DEFAULT, CF_LOCK, CF_WRITE,
};
use file_system::{IoType, WithIoType};
use futures::executor::block_on;
use kvproto::{kvrpcpb::Context, metapb::Region};
use pd_client::{FeatureGate, PdClient};
use raftstore::coprocessor::RegionInfoProvider;
use tikv_kv::{CfStatistics, CursorBuilder, Modify, SnapContext};
use tikv_util::{
    config::{Tracker, VersionTrack},
    store::find_peer,
    time::{duration_to_sec, Instant, Limiter, SlowTimer},
    worker::{Builder as WorkerBuilder, LazyWorker, Runnable, ScheduleError, Scheduler},
    Either,
};
use txn_types::{Key, TimeStamp};

use super::{
    check_need_gc,
    compaction_filter::{
        CompactionFilterInitializer, GC_COMPACTION_FILTER_MVCC_DELETION_HANDLED,
        GC_COMPACTION_FILTER_MVCC_DELETION_WASTED, GC_COMPACTION_FILTER_ORPHAN_VERSIONS,
    },
    config::{GcConfig, GcWorkerConfigManager},
    gc_manager::{AutoGcConfig, GcManager, GcManagerHandle},
    Callback, Error, ErrorInner, Result,
};
use crate::{
    server::metrics::*,
    storage::{
        kv::{metrics::GcKeyMode, Engine, ScanMode, Statistics},
        mvcc::{GcInfo, MvccReader, MvccTxn},
        txn::{gc, Error as TxnError},
    },
};

/// After the GC scan of a key, output a message to the log if there are at
/// least this many versions of the key.
const GC_LOG_FOUND_VERSION_THRESHOLD: usize = 30;

/// After the GC delete versions of a key, output a message to the log if at
/// least this many versions are deleted.
const GC_LOG_DELETED_VERSION_THRESHOLD: usize = 30;

const GC_TASK_SLOW_SECONDS: u64 = 30;
const GC_MAX_PENDING_TASKS: usize = 4096;

pub const STAT_TXN_KEYMODE: &str = "txn";
pub const STAT_RAW_KEYMODE: &str = "raw";

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

pub enum GcTask<E>
where
    E: KvEngine,
{
    Gc {
        region: Region,
        safe_point: TimeStamp,
        callback: Callback<()>,
    },
    GcKeys {
        keys: Vec<Key>,
        safe_point: TimeStamp,
        region_info_provider: Arc<dyn RegionInfoProvider>,
    },
    RawGcKeys {
        keys: Vec<Key>,
        safe_point: TimeStamp,
        region_info_provider: Arc<dyn RegionInfoProvider>,
    },
    UnsafeDestroyRange {
        ctx: Context,
        start_key: Key,
        end_key: Key,
        callback: Callback<()>,
        region_info_provider: Arc<dyn RegionInfoProvider>,
    },
    /// If GC in compaction filter is enabled, versions on default CF will be
    /// handled with `DB::delete` in write CF's compaction filter. However if
    /// the compaction filter finds the DB is stalled, it will send the task
    /// to GC worker to ensure the compaction can be continued.
    ///
    /// NOTE: It's possible that the TiKV instance fails after a compaction
    /// result is installed but its orphan versions are not deleted. Those
    /// orphan versions will never get cleaned
    /// until `DefaultCompactionFilter` is introduced.
    ///
    /// The tracking issue: <https://github.com/tikv/tikv/issues/9719>.
    OrphanVersions { wb: E::WriteBatch, id: usize },
    #[cfg(any(test, feature = "testexport"))]
    Validate(Box<dyn FnOnce(&GcConfig, &Limiter) + Send>),
}

impl<E> GcTask<E>
where
    E: KvEngine,
{
    pub fn get_enum_label(&self) -> GcCommandKind {
        match self {
            GcTask::Gc { .. } => GcCommandKind::gc,
            GcTask::GcKeys { .. } => GcCommandKind::gc_keys,
            GcTask::RawGcKeys { .. } => GcCommandKind::raw_gc_keys,
            GcTask::UnsafeDestroyRange { .. } => GcCommandKind::unsafe_destroy_range,
            GcTask::OrphanVersions { .. } => GcCommandKind::orphan_versions,
            #[cfg(any(test, feature = "testexport"))]
            GcTask::Validate(_) => GcCommandKind::validate_config,
        }
    }
}

impl<E> Display for GcTask<E>
where
    E: KvEngine,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            GcTask::Gc {
                region, safe_point, ..
            } => f
                .debug_struct("Gc")
                .field("region", region)
                .field("safe_point", safe_point)
                .finish(),
            GcTask::GcKeys { .. } => f.debug_struct("GcKeys").finish(),
            GcTask::RawGcKeys { .. } => f.debug_struct("RawGcKeys").finish(),
            GcTask::UnsafeDestroyRange {
                start_key, end_key, ..
            } => f
                .debug_struct("UnsafeDestroyRange")
                .field("start_key", &format!("{}", start_key))
                .field("end_key", &format!("{}", end_key))
                .finish(),
            GcTask::OrphanVersions { id, wb } => f
                .debug_struct("OrphanVersions")
                .field("id", id)
                .field("count", &wb.count())
                .finish(),
            #[cfg(any(test, feature = "testexport"))]
            GcTask::Validate(_) => write!(f, "Validate gc worker config"),
        }
    }
}

/// Used to perform GC operations on the engine.
pub struct GcRunner<E: Engine> {
    store_id: u64,
    engine: E,

    flow_info_sender: Sender<FlowInfo>,

    /// Used to limit the write flow of GC.
    limiter: Limiter,

    cfg: GcConfig,
    cfg_tracker: Tracker<GcConfig>,

    stats_map: HashMap<GcKeyMode, Statistics>,
}

pub const MAX_RAW_WRITE_SIZE: usize = 32 * 1024;

pub struct MvccRaw {
    pub(crate) write_size: usize,
    pub(crate) modifies: Vec<Modify>,
}

impl MvccRaw {
    pub fn new() -> MvccRaw {
        MvccRaw {
            write_size: 0,
            modifies: vec![],
        }
    }
    pub fn write_size(&self) -> usize {
        self.write_size
    }

    pub fn into_modifies(self) -> Vec<Modify> {
        self.modifies
    }
}

// There are two cases:
// 1. If `keys` has more than two keys, return the regions covered by the range
// [keys.first(), keys.last()]
// 2. If there's only one key (we must have at lease one key), return the region
// this key belongs to.
fn get_regions_for_range_of_keys(
    store_id: u64,
    keys: &[Key],
    region_provider: Arc<dyn RegionInfoProvider>,
) -> Result<Vec<Region>> {
    assert!(!keys.is_empty());

    if keys.len() >= 2 {
        let start = keys.first().unwrap().as_encoded();
        let end = keys.last().unwrap().as_encoded();
        let regions = box_try!(region_provider.get_regions_in_range(start, end))
            .into_iter()
            .filter(|r| find_peer(r, store_id).is_some())
            .peekable()
            .collect();

        Ok(regions)
    } else {
        // We only have one key.
        let key = keys[0].as_encoded();
        let region = box_try!(region_provider.find_region_by_key(key));
        if find_peer(&region, store_id).is_none() {
            return Ok(Vec::new());
        }

        Ok(vec![region])
    }
}

fn get_keys_in_region(keys: &mut Peekable<IntoIter<Key>>, region: &Region) -> Vec<Key> {
    let mut keys_in_region = Vec::new();

    loop {
        let Some(key) = keys.peek() else {break};
        let key = key.as_encoded().as_slice();

        if key < region.get_start_key() {
            keys.next();
        } else if region.get_end_key().is_empty() || key < region.get_end_key() {
            keys_in_region.push(keys.next().unwrap());
        } else {
            break;
        }
    }

    keys_in_region
}

fn init_snap_ctx(store_id: u64, region: &Region) -> Context {
    let mut ctx = Context::default();
    ctx.region_id = region.id;
    ctx.region_epoch = region.region_epoch.clone();
    ctx.stale_read = true;

    if let Some(peer) = region.peers.iter().find(|peer| peer.store_id == store_id) {
        ctx.set_peer(peer.clone());
    }

    ctx
}

impl<E: Engine> GcRunner<E> {
    pub fn new(
        store_id: u64,
        engine: E,
        flow_info_sender: Sender<FlowInfo>,
        cfg_tracker: Tracker<GcConfig>,
        cfg: GcConfig,
    ) -> Self {
        let limiter = Limiter::new(if cfg.max_write_bytes_per_sec.0 > 0 {
            cfg.max_write_bytes_per_sec.0 as f64
        } else {
            f64::INFINITY
        });
        Self {
            store_id,
            engine,
            flow_info_sender,
            limiter,
            cfg,
            cfg_tracker,
            stats_map: Default::default(),
        }
    }

    /// Check need gc without getting snapshot.
    /// If this is not supported or any error happens, returns true to do
    /// further check after getting snapshot.
    fn need_gc(&self, start_key: &[u8], end_key: &[u8], safe_point: TimeStamp) -> bool {
        // todo(SpadeA): multi-rocks db version should handle with this differently
        // which will be reflected in the imlementation of the v2's RaftKv.
        let props = match self
            .engine
            .get_mvcc_properties_cf(CF_WRITE, safe_point, start_key, end_key)
        {
            Some(c) => c,
            None => return true,
        };
        check_need_gc(safe_point, self.cfg.ratio_threshold, &props)
    }

    /// Cleans up outdated data.
    fn gc_key(
        &mut self,
        safe_point: TimeStamp,
        key: &Key,
        gc_info: &mut GcInfo,
        txn: &mut MvccTxn,
        reader: &mut MvccReader<E::Snap>,
    ) -> Result<()> {
        let next_gc_info = gc(txn, reader, key.clone(), safe_point).map_err(TxnError::from_mvcc)?;
        gc_info.found_versions += next_gc_info.found_versions;
        gc_info.deleted_versions += next_gc_info.deleted_versions;
        gc_info.is_completed = next_gc_info.is_completed;
        let stats = mem::take(&mut reader.statistics);
        self.mut_stats(GcKeyMode::txn).add(&stats);
        Ok(())
    }

    fn new_txn() -> MvccTxn {
        // TODO txn only used for GC, but this is hacky, maybe need an Option?
        let concurrency_manager = ConcurrencyManager::new(1.into());
        MvccTxn::new(TimeStamp::zero(), concurrency_manager)
    }

    fn flush_txn(
        limiter: &Limiter,
        engine: &E,
        txns: &mut HashMap<u64, MvccTxn>, // region id -> MvccTxn for this region
    ) -> Result<()> {
        let mut modifies = HashMap::default();
        let mut write_size = 0;
        for (id, txn) in txns.drain() {
            write_size += txn.write_size();
            modifies.insert(id, txn.into_modifies());
        }
        if write_size > 0 {
            limiter.blocking_consume(write_size);
            engine.modify_on_kv_engine(modifies)?;
        }
        Ok(())
    }

    fn gc(&mut self, region: Region, safe_point: TimeStamp) -> Result<()> {
        if !self.need_gc(region.get_start_key(), region.get_end_key(), safe_point) {
            GC_SKIPPED_COUNTER.inc();
            return Ok(());
        }

        let mut reader = MvccReader::new(
            self.get_snapshot(self.store_id, &region)?,
            Some(ScanMode::Forward),
            false,
        );

        let mut next_key = Some(Key::from_encoded_slice(region.get_start_key()));
        while next_key.is_some() {
            // Scans at most `GcConfig.batch_keys` keys.
            let (keys, updated_next_key) = reader
                .scan_keys(next_key, self.cfg.batch_keys)
                .map_err(TxnError::from_mvcc)?;
            next_key = updated_next_key;

            if keys.is_empty() {
                GC_EMPTY_RANGE_COUNTER.inc();
                break;
            }
            self.gc_keys(keys, safe_point, Either::Left(region.clone()))?;
        }

        self.mut_stats(GcKeyMode::txn).add(&reader.statistics);
        debug!(
            "gc has finished";
            "start_key" => log_wrappers::Value::key(region.get_start_key()),
            "end_key" => log_wrappers::Value::key(region.get_end_key()),
            "safe_point" => safe_point
        );
        Ok(())
    }

    pub fn gc_keys(
        &mut self,
        keys: Vec<Key>,
        safe_point: TimeStamp,
        region_or_provider: Either<Region, Arc<dyn RegionInfoProvider>>,
    ) -> Result<(usize, usize)> {
        let store_id = self.store_id;
        let count = keys.len();
        let range_start_key = keys.first().unwrap().clone();
        let range_end_key = {
            let mut k = keys
                .last()
                .unwrap()
                .to_raw()
                .map_err(|e| EngineError::Codec(e))?;
            k.push(0);
            Key::from_raw(&k)
        };

        let (mut handled_keys, mut wasted_keys) = (0, 0);
        let regions = match region_or_provider {
            Either::Left(region) => vec![region],
            Either::Right(region_provider) => {
                get_regions_for_range_of_keys(store_id, &keys, region_provider)?
            }
        };

        // First item is fetched to initialize the reader and kv_engine
        if regions.is_empty() {
            return Ok((handled_keys, wasted_keys));
        }

        let mut txns = HashMap::default();
        let mut gc_info = GcInfo::default();
        let mut keys = keys.into_iter().peekable();
        for region in regions {
            let mut txn = Self::new_txn();
            let mut reader = self.create_reader(
                count,
                &region,
                range_start_key.clone(),
                range_end_key.clone(),
            )?;

            let mut keys_in_region = get_keys_in_region(&mut keys, &region).into_iter();
            let mut next_gc_key = keys_in_region.next();

            while let Some(ref key) = next_gc_key {
                if let Err(e) = self.gc_key(safe_point, key, &mut gc_info, &mut txn, &mut reader) {
                    GC_KEY_FAILURES.inc();
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
                    if gc_info.deleted_versions >= GC_LOG_DELETED_VERSION_THRESHOLD {
                        debug!(
                            "GC deleted plenty versions for a key";
                            "key" => %key,
                            "versions" => gc_info.deleted_versions,
                        );
                    }

                    if gc_info.found_versions > 0 {
                        handled_keys += 1;
                    } else {
                        wasted_keys += 1;
                    }
                    next_gc_key = keys_in_region.next();
                    gc_info = GcInfo::default();
                } else {
                    txns.insert(region.id, txn);
                    Self::flush_txn(&self.limiter, &self.engine, &mut txns)?;
                    txns.clear();

                    reader = self.create_reader(
                        count,
                        &region,
                        range_start_key.clone(),
                        range_end_key.clone(),
                    )?;
                    txn = Self::new_txn();
                }
            }

            txns.insert(region.id, txn);
        }

        Self::flush_txn(&self.limiter, &self.engine, &mut txns)?;
        Ok((handled_keys, wasted_keys))
    }

    fn create_reader(
        &mut self,
        key_count: usize,
        region: &Region,
        range_start_key: Key,
        range_end_key: Key,
    ) -> Result<MvccReader<E::Snap>> {
        let mut reader = {
            let snapshot = self.get_snapshot(self.store_id, region)?;

            if key_count <= 1 {
                MvccReader::new(snapshot, None, false)
            } else {
                // keys are closing to each other in one batch of gc keys, so do not use
                // prefix seek here to avoid too many seeks
                MvccReader::new(snapshot, Some(ScanMode::Forward), false)
            }
        };
        reader.set_range(Some(range_start_key), Some(range_end_key));
        Ok(reader)
    }

    fn raw_gc_keys(
        &mut self,
        keys: Vec<Key>,
        safe_point: TimeStamp,
        regions_provider: Arc<dyn RegionInfoProvider>,
    ) -> Result<(usize, usize)> {
        let range_start_key = keys.first().unwrap().clone();
        let range_end_key = {
            let mut k = keys
                .last()
                .unwrap()
                .to_raw()
                .map_err(|e| EngineError::Codec(e))?;
            k.push(0);
            Key::from_raw(&k)
        };

        let (mut handled_keys, mut wasted_keys) = (0, 0);
        let regions = get_regions_for_range_of_keys(self.store_id, &keys, regions_provider)?;

        if regions.is_empty() {
            return Ok((handled_keys, wasted_keys));
        }

        let mut region_modifies = HashMap::default();
        let mut gc_info = GcInfo::default();
        let mut keys = keys.into_iter().peekable();
        for region in regions {
            let mut raw_modifies = MvccRaw::new();
            let mut snapshot = self.get_snapshot(self.store_id, &region)?;

            let mut keys_in_region = get_keys_in_region(&mut keys, &region).into_iter();
            let mut next_gc_key = keys_in_region.next();
            while let Some(ref key) = next_gc_key {
                if let Err(e) = self.raw_gc_key(
                    safe_point,
                    key,
                    &range_start_key,
                    &range_end_key,
                    &mut raw_modifies,
                    &mut snapshot,
                    &mut gc_info,
                ) {
                    GC_KEY_FAILURES.inc();
                    error!(?e; "Raw GC meets failure"; "key" => %key,);
                    // Switch to the next key if meets failure.
                    gc_info.is_completed = true;
                }

                if gc_info.is_completed {
                    if gc_info.found_versions >= GC_LOG_FOUND_VERSION_THRESHOLD {
                        debug!(
                            "RawKV GC found plenty versions for a key";
                            "key" => %key,
                            "versions" => gc_info.found_versions,
                        );
                    }
                    if gc_info.found_versions > 0 {
                        handled_keys += 1;
                    } else {
                        wasted_keys += 1;
                    }

                    gc_info.report_metrics(STAT_RAW_KEYMODE);

                    next_gc_key = keys_in_region.next();
                    gc_info = GcInfo::default();
                } else {
                    region_modifies.insert(region.id, raw_modifies);
                    // Flush writeBatch to engine.
                    Self::flush_raw_gc(&self.limiter, &self.engine, &mut region_modifies)?;
                    // After flush, reset raw_modifies.
                    raw_modifies = MvccRaw::new();
                    region_modifies.clear();
                }
            }

            region_modifies.insert(region.id, raw_modifies);
        }

        Self::flush_raw_gc(&self.limiter, &self.engine, &mut region_modifies)?;

        Ok((handled_keys, wasted_keys))
    }

    fn raw_gc_key(
        &mut self,
        safe_point: TimeStamp,
        key: &Key,
        range_start_key: &Key,
        range_end_key: &Key,
        raw_modifies: &mut MvccRaw,
        kv_snapshot: &mut <E as Engine>::Snap,
        gc_info: &mut GcInfo,
    ) -> Result<()> {
        let start_key = key.clone().append_ts(safe_point.prev());
        let mut cursor = CursorBuilder::new(kv_snapshot, CF_DEFAULT)
            .range(Some(range_start_key.clone()), Some(range_end_key.clone()))
            .build()?;
        let mut statistics = CfStatistics::default();
        cursor.seek(&start_key, &mut statistics)?;

        let mut remove_older = false;
        let mut latest_version_key = None;
        let current_ts = ttl_current_ts();

        while cursor.valid()? {
            gc_info.found_versions += 1;
            let current_key = cursor.key(&mut statistics);
            if !Key::is_user_key_eq(current_key, key.as_encoded()) {
                break;
            }

            if raw_modifies.write_size >= MAX_RAW_WRITE_SIZE {
                let cf_stats = self.mut_stats(GcKeyMode::raw).mut_cf_statistics(CF_DEFAULT);
                cf_stats.add(&statistics);
                return Ok(());
            }

            if remove_older {
                self.delete_raws(Key::from_encoded_slice(current_key), raw_modifies, gc_info);
            } else {
                remove_older = true;

                let value = ApiV2::decode_raw_value(cursor.value(&mut statistics))?;
                // It's deleted or expired.
                if !value.is_valid(current_ts) {
                    latest_version_key = Some(Key::from_encoded_slice(current_key));
                }
            }
            cursor.next(&mut statistics);
        }

        gc_info.is_completed = true;

        let cf_stats = self.mut_stats(GcKeyMode::raw).mut_cf_statistics(CF_DEFAULT);
        cf_stats.add(&statistics);

        if let Some(to_del_key) = latest_version_key {
            self.delete_raws(to_del_key, raw_modifies, gc_info);
        }

        Ok(())
    }

    pub fn mut_stats(&mut self, key_mode: GcKeyMode) -> &mut Statistics {
        let stats = self
            .stats_map
            .entry(key_mode)
            .or_insert_with(Default::default);
        stats
    }

    fn delete_raws(&mut self, key: Key, raw_modifies: &mut MvccRaw, gc_info: &mut GcInfo) {
        let write = Modify::Delete(CF_DEFAULT, key);
        raw_modifies.write_size += write.size();
        raw_modifies.modifies.push(write);
        gc_info.deleted_versions += 1;
    }

    fn flush_raw_gc(
        limiter: &Limiter,
        engine: &E,
        region_modifies: &mut HashMap<u64, MvccRaw>,
    ) -> Result<()> {
        let mut modifies = HashMap::default();
        let mut write_size = 0;
        for (id, m) in region_modifies.drain() {
            write_size += m.write_size();
            modifies.insert(id, m.into_modifies());
        }
        if write_size > 0 {
            // rate limiter
            limiter.blocking_consume(write_size);
            engine.modify_on_kv_engine(modifies)?;
        }
        Ok(())
    }

    fn unsafe_destroy_range(
        &self,
        ctx: &Context,
        start_key: &Key,
        end_key: &Key,
        regions_provider: Arc<dyn RegionInfoProvider>,
    ) -> Result<()> {
        info!(
            "unsafe destroy range started";
            "start_key" => %start_key, "end_key" => %end_key
        );

        fail_point!("unsafe_destroy_range");

        self.flow_info_sender
            .send(FlowInfo::BeforeUnsafeDestroyRange(ctx.region_id))
            .unwrap();

        // We are in single-rocksdb version if we can get a local_storage, otherwise, we
        // are in multi-rocksdb version.
        if let Some(local_storage) = self.engine.kv_engine() {
            // Convert keys to RocksDB layer form
            // TODO: Logic coupled with raftstore's implementation. Maybe better design is
            // to do it in somewhere of the same layer with apply_worker.
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
                "cost_time" => ?delete_files_start_time.saturating_elapsed(),
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
                "start_key" => %start_key, "end_key" => %end_key, "cost_time" => ?cleanup_all_start_time.saturating_elapsed(),
            );

            self.flow_info_sender
                .send(FlowInfo::AfterUnsafeDestroyRange(ctx.region_id))
                .unwrap();

            self.engine.hint_change_in_range(
                start_key.as_encoded().to_vec(),
                end_key.as_encoded().to_vec(),
            );
        } else {
            let cfs = &[CF_LOCK, CF_DEFAULT, CF_WRITE];
            let keys = vec![start_key.clone(), end_key.clone()];
            let regions = get_regions_for_range_of_keys(self.store_id, &keys, regions_provider)?;

            let count = regions.len();
            let mut region_modifies = HashMap::default();
            for (i, region) in regions.into_iter().enumerate() {
                let range_start = if i == 0 {
                    start_key.clone()
                } else {
                    Key::from_raw(region.get_start_key())
                };
                let range_end = if i == count - 1 {
                    end_key.clone()
                } else {
                    Key::from_raw(region.get_end_key())
                };

                let mut modifies = Vec::new();
                for cf in cfs {
                    modifies.push(Modify::DeleteRange(
                        cf,
                        range_start.clone(),
                        range_end.clone(),
                        false,
                    ));
                }
                region_modifies.insert(region.id, modifies);
            }

            self.engine.modify_on_kv_engine(region_modifies)?;

            // todo(SpadeA): For multi-rocksdb version, sending
            // `FlowInfo::AfterUnsafeDestroyRange` and
            // `StoreMsg::ClearRegionSizeInRange` is different to the
            // single-rocksdb version. Sending these msgs should be implemented
            // when or after implementing the raftkv of multi-rocksdb version.
        }

        Ok(())
    }

    fn update_statistics_metrics(&mut self, key_mode: GcKeyMode) {
        if let Some(mut_stats) = self.stats_map.get_mut(&key_mode) {
            let stats = mem::take(mut_stats);
            for (cf, cf_details) in stats.details_enum().iter() {
                for (tag, count) in cf_details.iter() {
                    GC_KEYS_COUNTER_STATIC
                        .get(key_mode)
                        .get(*cf)
                        .get(*tag)
                        .inc_by(*count as u64);
                }
            }
        }
    }

    fn refresh_cfg(&mut self) {
        if let Some(incoming) = self.cfg_tracker.any_new() {
            let limit = incoming.max_write_bytes_per_sec.0;
            self.limiter.set_speed_limit(if limit > 0 {
                limit as f64
            } else {
                f64::INFINITY
            });
            self.cfg = incoming.clone();
        }
    }

    fn get_snapshot(&mut self, store_id: u64, region: &Region) -> Result<<E as Engine>::Snap> {
        let ctx = init_snap_ctx(store_id, region);
        let snap_ctx = SnapContext {
            pb_ctx: &ctx,
            ..Default::default()
        };

        Ok(block_on(async {
            tikv_kv::snapshot(&mut self.engine, snap_ctx).await
        })?)
    }
}

impl<E: Engine> Runnable for GcRunner<E> {
    type Task = GcTask<E::Local>;

    #[inline]
    fn run(&mut self, task: GcTask<E::Local>) {
        let _io_type_guard = WithIoType::new(IoType::Gc);
        let enum_label = task.get_enum_label();

        GC_GCTASK_COUNTER_STATIC.get(enum_label).inc();

        let timer = SlowTimer::from_secs(GC_TASK_SLOW_SECONDS);
        let update_metrics = |is_err| {
            GC_TASK_DURATION_HISTOGRAM_VEC
                .with_label_values(&[enum_label.get_str()])
                .observe(duration_to_sec(timer.saturating_elapsed()));

            if is_err {
                GC_GCTASK_FAIL_COUNTER_STATIC.get(enum_label).inc();
            }
        };

        // Refresh config before handle task
        self.refresh_cfg();

        match task {
            GcTask::Gc {
                region,
                safe_point,
                callback,
            } => {
                let res = self.gc(region.clone(), safe_point);
                update_metrics(res.is_err());
                callback(res);
                self.update_statistics_metrics(GcKeyMode::txn);
                slow_log!(
                    T timer,
                    "GC on range [{}, {}), safe_point {}",
                    log_wrappers::Value::key(region.get_start_key()),
                    log_wrappers::Value::key(region.get_end_key()),
                    safe_point
                );
            }
            GcTask::GcKeys {
                keys,
                safe_point,
                region_info_provider,
            } => {
                let old_seek_tombstone = self.mut_stats(GcKeyMode::txn).write.seek_tombstone;

                match self.gc_keys(keys, safe_point, Either::Right(region_info_provider)) {
                    Ok((handled, wasted)) => {
                        GC_COMPACTION_FILTER_MVCC_DELETION_HANDLED
                            .with_label_values(&[STAT_TXN_KEYMODE])
                            .inc_by(handled as _);
                        GC_COMPACTION_FILTER_MVCC_DELETION_WASTED
                            .with_label_values(&[STAT_TXN_KEYMODE])
                            .inc_by(wasted as _);
                        update_metrics(false);
                    }
                    Err(e) => {
                        warn!("GcKeys fail"; "err" => ?e);
                        update_metrics(true);
                    }
                }
                let new_seek_tombstone = self.mut_stats(GcKeyMode::txn).write.seek_tombstone;
                let seek_tombstone = new_seek_tombstone - old_seek_tombstone;
                slow_log!(T timer, "GC keys, seek_tombstone {}", seek_tombstone);
                self.update_statistics_metrics(GcKeyMode::txn);
            }
            GcTask::RawGcKeys {
                keys,
                safe_point,
                region_info_provider,
            } => {
                match self.raw_gc_keys(keys, safe_point, region_info_provider) {
                    Ok((handled, wasted)) => {
                        GC_COMPACTION_FILTER_MVCC_DELETION_HANDLED
                            .with_label_values(&[STAT_RAW_KEYMODE])
                            .inc_by(handled as _);
                        GC_COMPACTION_FILTER_MVCC_DELETION_WASTED
                            .with_label_values(&[STAT_RAW_KEYMODE])
                            .inc_by(wasted as _);
                        update_metrics(false);
                    }
                    Err(e) => {
                        warn!("Raw GcKeys fail"; "err" => ?e);
                        update_metrics(true);
                    }
                }
                self.update_statistics_metrics(GcKeyMode::raw);
            }
            GcTask::UnsafeDestroyRange {
                ctx,
                start_key,
                end_key,
                callback,
                region_info_provider,
            } => {
                let res =
                    self.unsafe_destroy_range(&ctx, &start_key, &end_key, region_info_provider);
                update_metrics(res.is_err());
                callback(res);
                slow_log!(
                    T timer,
                    "UnsafeDestroyRange start_key {:?}, end_key {:?}",
                    start_key,
                    end_key
                );
            }
            GcTask::OrphanVersions { mut wb, id } => {
                info!("handling GcTask::OrphanVersions"; "id" => id);
                let mut wopts = WriteOptions::default();
                wopts.set_sync(true);
                if let Err(e) = wb.write_opt(&wopts) {
                    error!("write GcTask::OrphanVersions fail"; "id" => id, "err" => ?e);
                    update_metrics(true);
                    return;
                }
                info!("write GcTask::OrphanVersions success"; "id" => id);
                GC_COMPACTION_FILTER_ORPHAN_VERSIONS
                    .with_label_values(&[STAT_TXN_KEYMODE, "cleaned"])
                    .inc_by(wb.count() as u64);
                update_metrics(false);
            }
            #[cfg(any(test, feature = "testexport"))]
            GcTask::Validate(f) => {
                f(&self.cfg, &self.limiter);
            }
        };
    }
}

/// When we failed to schedule a `GcTask` to `GcRunner`, use this to handle the
/// `ScheduleError`.
fn handle_gc_task_schedule_error(e: ScheduleError<GcTask<impl KvEngine>>) -> Result<()> {
    error!("failed to schedule gc task"; "err" => %e);
    let res = Err(box_err!("failed to schedule gc task: {:?}", e));
    match e.into_inner() {
        GcTask::Gc { callback, .. } | GcTask::UnsafeDestroyRange { callback, .. } => {
            callback(Err(Error::from(ErrorInner::GcWorkerTooBusy)))
        }
        // Attention: If you are adding a new GcTask, do not forget to call the callback if it has a
        // callback.
        GcTask::GcKeys { .. } | GcTask::RawGcKeys { .. } | GcTask::OrphanVersions { .. } => {}
        #[cfg(any(test, feature = "testexport"))]
        GcTask::Validate(_) => {}
    }
    res
}

/// Schedules a `GcTask` to the `GcRunner`.
fn schedule_gc(
    scheduler: &Scheduler<GcTask<impl KvEngine>>,
    region: Region,
    safe_point: TimeStamp,
    callback: Callback<()>,
) -> Result<()> {
    scheduler
        .schedule(GcTask::Gc {
            region,
            safe_point,
            callback,
        })
        .or_else(handle_gc_task_schedule_error)
}

/// Does GC synchronously.
pub fn sync_gc(
    scheduler: &Scheduler<GcTask<impl KvEngine>>,
    region: Region,
    safe_point: TimeStamp,
) -> Result<()> {
    wait_op!(|callback| schedule_gc(scheduler, region, safe_point, callback)).unwrap_or_else(|| {
        error!("failed to receive result of gc");
        Err(box_err!("gc_worker: failed to receive result of gc"))
    })
}

/// Used to schedule GC operations.
pub struct GcWorker<E>
where
    E: Engine,
{
    engine: E,

    /// Used to signal unsafe destroy range is executed.
    flow_info_sender: Option<Sender<FlowInfo>>,
    region_info_provider: Arc<dyn RegionInfoProvider>,

    config_manager: GcWorkerConfigManager,

    /// How many strong references. The worker will be stopped
    /// once there are no more references.
    refs: Arc<AtomicUsize>,
    worker: Arc<Mutex<LazyWorker<GcTask<E::Local>>>>,
    worker_scheduler: Scheduler<GcTask<E::Local>>,

    gc_manager_handle: Arc<Mutex<Option<GcManagerHandle>>>,
    feature_gate: FeatureGate,
}

impl<E: Engine> Clone for GcWorker<E> {
    #[inline]
    fn clone(&self) -> Self {
        self.refs.fetch_add(1, Ordering::SeqCst);

        Self {
            engine: self.engine.clone(),
            flow_info_sender: self.flow_info_sender.clone(),
            config_manager: self.config_manager.clone(),
            refs: self.refs.clone(),
            worker: self.worker.clone(),
            worker_scheduler: self.worker_scheduler.clone(),
            gc_manager_handle: self.gc_manager_handle.clone(),
            feature_gate: self.feature_gate.clone(),
            region_info_provider: self.region_info_provider.clone(),
        }
    }
}

impl<E: Engine> Drop for GcWorker<E> {
    #[inline]
    fn drop(&mut self) {
        let refs = self.refs.fetch_sub(1, Ordering::SeqCst);

        if refs != 1 {
            return;
        }

        let r = self.stop();
        if let Err(e) = r {
            error!(?e; "Failed to stop gc_worker");
        }
    }
}

impl<E: Engine> GcWorker<E> {
    pub fn new(
        engine: E,
        flow_info_sender: Sender<FlowInfo>,
        cfg: GcConfig,
        feature_gate: FeatureGate,
        region_info_provider: Arc<dyn RegionInfoProvider>,
    ) -> Self {
        let worker_builder = WorkerBuilder::new("gc-worker").pending_capacity(GC_MAX_PENDING_TASKS);
        let worker = worker_builder.create().lazy_build("gc-worker");
        let worker_scheduler = worker.scheduler();
        GcWorker {
            engine,
            flow_info_sender: Some(flow_info_sender),
            config_manager: GcWorkerConfigManager(Arc::new(VersionTrack::new(cfg))),
            refs: Arc::new(AtomicUsize::new(1)),
            worker: Arc::new(Mutex::new(worker)),
            worker_scheduler,
            gc_manager_handle: Arc::new(Mutex::new(None)),
            feature_gate,
            region_info_provider,
        }
    }

    pub fn start_auto_gc<S: GcSafePointProvider, R: RegionInfoProvider + Clone + 'static>(
        &self,
        cfg: AutoGcConfig<S, R>,
        safe_point: Arc<AtomicU64>, // Store safe point here.
    ) -> Result<()> {
        assert!(
            cfg.self_store_id > 0,
            "AutoGcConfig::self_store_id shouldn't be 0"
        );

        info!("initialize compaction filter to perform GC when necessary");
        self.engine.kv_engine().unwrap().init_compaction_filter(
            cfg.self_store_id,
            safe_point.clone(),
            self.config_manager.clone(),
            self.feature_gate.clone(),
            self.scheduler(),
            Arc::new(cfg.region_info_provider.clone()),
        );

        let mut handle = self.gc_manager_handle.lock().unwrap();
        assert!(handle.is_none());

        let new_handle = GcManager::new(
            cfg,
            safe_point,
            self.scheduler(),
            self.config_manager.clone(),
            self.feature_gate.clone(),
        )
        .start()?;
        *handle = Some(new_handle);
        Ok(())
    }

    pub fn start(&mut self, store_id: u64) -> Result<()> {
        let runner = GcRunner::new(
            store_id,
            self.engine.clone(),
            self.flow_info_sender.take().unwrap(),
            self.config_manager.0.clone().tracker("gc-woker".to_owned()),
            self.config_manager.value().clone(),
        );
        self.worker.lock().unwrap().start(runner);
        Ok(())
    }

    pub fn stop(&self) -> Result<()> {
        // Stop GcManager.
        if let Some(h) = self.gc_manager_handle.lock().unwrap().take() {
            h.stop()?;
        }
        // Stop self.
        self.worker.lock().unwrap().stop();
        Ok(())
    }

    pub fn scheduler(&self) -> Scheduler<GcTask<E::Local>> {
        self.worker_scheduler.clone()
    }

    /// Only for tests.
    pub fn gc(&self, region: Region, safe_point: TimeStamp, callback: Callback<()>) -> Result<()> {
        self.worker_scheduler
            .schedule(GcTask::Gc {
                region,
                safe_point,
                callback,
            })
            .or_else(handle_gc_task_schedule_error)
    }

    /// Cleans up all keys in a range and quickly free the disk space. The range
    /// might span over multiple regions, and the `ctx` doesn't indicate region.
    /// The request will be done directly on RocksDB, bypassing the Raft layer.
    /// User must promise that, after calling `destroy_range`, the range will
    /// never be accessed any more. However, `destroy_range` is allowed to be
    /// called multiple times on an single range.
    pub fn unsafe_destroy_range(
        &self,
        ctx: Context,
        start_key: Key,
        end_key: Key,
        callback: Callback<()>,
    ) -> Result<()> {
        GC_COMMAND_COUNTER_VEC_STATIC.unsafe_destroy_range.inc();

        // Use schedule_force to allow unsafe_destroy_range to schedule even if
        // the GC worker is full. This will help free up space in the case when
        // the GC worker is busy with other tasks.
        // Unsafe destroy range is in store level, so the number of them is
        // quite small, so we don't need to worry about its memory usage.
        self.worker_scheduler
            .schedule_force(GcTask::UnsafeDestroyRange {
                ctx,
                start_key,
                end_key,
                callback,
                region_info_provider: self.region_info_provider.clone(),
            })
            .or_else(handle_gc_task_schedule_error)
    }

    pub fn get_config_manager(&self) -> GcWorkerConfigManager {
        self.config_manager.clone()
    }
}

#[cfg(any(test, feature = "testexport"))]
pub mod test_gc_worker {
    use std::sync::{Arc, Mutex};

    use collections::HashMap;
    use engine_rocks::{RocksEngine, RocksSnapshot};
    use futures::Future;
    use kvproto::{
        kvrpcpb::Context,
        metapb::{Peer, Region},
    };
    use raftstore::store::RegionSnapshot;
    use tikv_kv::{write_modifies, OnAppliedCb};
    use txn_types::{Key, TimeStamp};

    use crate::{
        server::gc_worker::{GcSafePointProvider, Result as GcWorkerResult},
        storage::{
            kv::{self, Modify, Result as EngineResult, SnapContext, WriteData},
            Engine,
        },
    };

    /// A wrapper of engine that adds the 'z' prefix to keys internally.
    /// For test engines, they writes keys into db directly, but in production a
    /// 'z' prefix will be added to keys by raftstore layer before writing
    /// to db. Some functionalities of `GcWorker` bypasses Raft layer, so
    /// they needs to know how data is actually represented in db. This
    /// wrapper allows test engines write 'z'-prefixed keys to db.
    #[derive(Clone)]
    pub struct PrefixedEngine(pub kv::RocksEngine);

    impl Engine for PrefixedEngine {
        // Use RegionSnapshot which can remove the z prefix internally.
        type Snap = RegionSnapshot<RocksSnapshot>;
        type Local = RocksEngine;

        fn kv_engine(&self) -> Option<RocksEngine> {
            self.0.kv_engine()
        }

        fn modify_on_kv_engine(
            &self,
            region_modifies: HashMap<u64, Vec<Modify>>,
        ) -> kv::Result<()> {
            let mut modifies: Vec<_> = region_modifies.into_values().flatten().collect();
            for modify in &mut modifies {
                match modify {
                    Modify::Delete(_, ref mut key) => {
                        let bytes = keys::data_key(key.as_encoded());
                        *key = Key::from_encoded(bytes);
                    }
                    Modify::Put(_, ref mut key, _) => {
                        let bytes = keys::data_key(key.as_encoded());
                        *key = Key::from_encoded(bytes);
                    }
                    Modify::PessimisticLock(ref mut key, _) => {
                        let bytes = keys::data_key(key.as_encoded());
                        *key = Key::from_encoded(bytes);
                    }
                    Modify::DeleteRange(_, ref mut key1, ref mut key2, _) => {
                        let bytes = keys::data_key(key1.as_encoded());
                        *key1 = Key::from_encoded(bytes);
                        let bytes = keys::data_end_key(key2.as_encoded());
                        *key2 = Key::from_encoded(bytes);
                    }
                }
            }
            write_modifies(&self.kv_engine().unwrap(), modifies)
        }

        type WriteRes = <kv::RocksEngine as Engine>::WriteRes;
        fn async_write(
            &self,
            ctx: &Context,
            mut batch: WriteData,
            subscribed: u8,
            on_applied: Option<OnAppliedCb>,
        ) -> Self::WriteRes {
            batch.modifies.iter_mut().for_each(|modify| match modify {
                Modify::Delete(_, ref mut key) => {
                    *key = Key::from_encoded(keys::data_key(key.as_encoded()));
                }
                Modify::Put(_, ref mut key, _) => {
                    *key = Key::from_encoded(keys::data_key(key.as_encoded()));
                }
                Modify::PessimisticLock(ref mut key, _) => {
                    *key = Key::from_encoded(keys::data_key(key.as_encoded()));
                }
                Modify::DeleteRange(_, ref mut start_key, ref mut end_key, _) => {
                    *start_key = Key::from_encoded(keys::data_key(start_key.as_encoded()));
                    *end_key = Key::from_encoded(keys::data_end_key(end_key.as_encoded()));
                }
            });
            self.0.async_write(ctx, batch, subscribed, on_applied)
        }

        type SnapshotRes = impl Future<Output = EngineResult<Self::Snap>> + Send;
        fn async_snapshot(&mut self, ctx: SnapContext<'_>) -> Self::SnapshotRes {
            let f = self.0.async_snapshot(ctx);
            async move {
                let snap = f.await?;
                let mut region = Region::default();
                // Add a peer to pass initialized check.
                region.mut_peers().push(Peer::default());
                Ok(RegionSnapshot::from_snapshot(snap, Arc::new(region)))
            }
        }
    }

    pub struct MockSafePointProvider(pub u64);

    impl GcSafePointProvider for MockSafePointProvider {
        fn get_safe_point(&self) -> GcWorkerResult<TimeStamp> {
            Ok(self.0.into())
        }
    }

    #[derive(Clone, Default)]
    pub struct MultiRocksEngine {
        pub engines: Arc<Mutex<HashMap<u64, PrefixedEngine>>>,
        pub region_info: HashMap<u64, Region>,
    }

    impl Engine for MultiRocksEngine {
        type Snap = <PrefixedEngine as Engine>::Snap;
        type Local = <PrefixedEngine as Engine>::Local;

        fn kv_engine(&self) -> Option<Self::Local> {
            None
        }

        fn modify_on_kv_engine(
            &self,
            region_modifies: HashMap<u64, Vec<Modify>>,
        ) -> kv::Result<()> {
            for (region_id, modifies) in region_modifies {
                let mut map = HashMap::default();
                map.insert(region_id, modifies);
                self.engines.lock().unwrap()[&region_id].modify_on_kv_engine(map)?;
            }

            Ok(())
        }

        type WriteRes = <PrefixedEngine as Engine>::WriteRes;
        fn async_write(
            &self,
            ctx: &Context,
            batch: WriteData,
            subscribed: u8,
            on_applied: Option<OnAppliedCb>,
        ) -> Self::WriteRes {
            self.engines.lock().unwrap()[&ctx.region_id]
                .async_write(ctx, batch, subscribed, on_applied)
        }

        type SnapshotRes = impl Future<Output = EngineResult<Self::Snap>> + Send;
        fn async_snapshot(&mut self, ctx: SnapContext<'_>) -> Self::SnapshotRes {
            let region_id = ctx.pb_ctx.region_id;
            self.engines
                .lock()
                .unwrap()
                .get_mut(&region_id)
                .unwrap()
                .async_snapshot(ctx)
        }
    }
}

#[cfg(test)]
mod tests {

    use std::{
        collections::{BTreeMap, BTreeSet},
        path::Path,
        sync::mpsc,
        thread,
        time::Duration,
    };

    use api_version::{ApiV2, KvFormat, RawValue};
    use engine_rocks::{util::get_cf_handle, RocksEngine};
    use engine_traits::Peekable as _;
    use futures::executor::block_on;
    use kvproto::{kvrpcpb::ApiVersion, metapb::Peer};
    use raft::StateRole;
    use raftstore::coprocessor::{
        region_info_accessor::{MockRegionInfoProvider, RegionInfoAccessor},
        CoprocessorHost, RegionChangeEvent,
    };
    use tempfile::Builder;
    use tikv_kv::Snapshot;
    use tikv_util::store::new_peer;
    use txn_types::Mutation;

    use super::{test_gc_worker::MultiRocksEngine, *};
    use crate::{
        config::DbConfig,
        server::gc_worker::{MockSafePointProvider, PrefixedEngine},
        storage::{
            kv::{metrics::GcKeyMode, Modify, TestEngineBuilder, WriteData},
            lock_manager::MockLockManager,
            mvcc::{
                tests::{must_get_none, must_get_none_on_region, must_get_on_region},
                MAX_TXN_WRITE_SIZE,
            },
            txn::{
                commands,
                tests::{
                    must_commit, must_commit_on_region, must_gc, must_prewrite_delete,
                    must_prewrite_delete_on_region, must_prewrite_put, must_prewrite_put_on_region,
                    must_rollback,
                },
            },
            Engine, Storage, TestStorageBuilderApiV1,
        },
    };

    fn init_region(
        start_key: &[u8],
        end_key: &[u8],
        region_id: u64,
        store_id: Option<u64>,
    ) -> Region {
        let start_key = Key::from_encoded(start_key.to_vec());
        let end_key = Key::from_encoded(end_key.to_vec());
        let mut region = Region::default();
        region.set_start_key(start_key.as_encoded().clone());
        region.set_end_key(end_key.as_encoded().clone());
        region.id = region_id;
        if let Some(store_id) = store_id {
            region.mut_peers().push(Peer::default());
            region.mut_peers()[0].set_store_id(store_id);
        }
        region
    }

    #[test]
    fn test_get_regions_for_gc() {
        let store_id = 1;

        let r1 = init_region(b"", b"k10", 1, None);
        let r2 = init_region(b"k20", b"k30", 2, Some(store_id));
        let r3 = init_region(b"k30", b"", 3, Some(store_id));

        let ri_provider = Arc::new(MockRegionInfoProvider::new(vec![
            r1,
            r2.clone(),
            r3.clone(),
        ]));

        let keys = vec![Key::from_encoded(b"k05".to_vec())];
        let regions = get_regions_for_range_of_keys(store_id, &keys, ri_provider.clone()).unwrap();
        // store id not match
        assert!(regions.is_empty());

        let keys = vec![
            Key::from_encoded(b"k05".to_vec()),
            Key::from_encoded(b"k10".to_vec()),
            Key::from_encoded(b"k25".to_vec()),
        ];
        let regions = get_regions_for_range_of_keys(store_id, &keys, ri_provider.clone()).unwrap();
        let rs = vec![r2.clone()];
        assert_eq!(regions, rs);

        let keys = vec![
            Key::from_encoded(b"k05".to_vec()),
            Key::from_encoded(b"k10".to_vec()),
            Key::from_encoded(b"k25".to_vec()),
            Key::from_encoded(b"k35".to_vec()),
        ];
        let regions = get_regions_for_range_of_keys(store_id, &keys, ri_provider).unwrap();
        let rs = vec![r2, r3];
        assert_eq!(regions, rs);
    }

    /// Assert the data in `storage` is the same as `expected_data`. Keys in
    /// `expected_data` should be encoded form without ts.
    fn check_data<E: Engine, F: KvFormat>(
        storage: &Storage<E, MockLockManager, F>,
        expected_data: &BTreeMap<Vec<u8>, Vec<u8>>,
    ) {
        let scan_res = block_on(storage.scan(
            Context::default(),
            Key::from_raw(b""),
            None,
            expected_data.len() + 1,
            0,
            1.into(),
            false,
            false,
        ))
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
        split_key: &[u8],
    ) -> Result<()> {
        // Return Result from this function so we can use the `wait_op` macro here.
        let store_id = 1;

        let engine = TestEngineBuilder::new().build().unwrap();
        let storage = TestStorageBuilderApiV1::from_engine_and_lock_mgr(
            engine.clone(),
            MockLockManager::new(),
        )
        .build()
        .unwrap();
        let gate = FeatureGate::default();
        gate.set_version("5.0.0").unwrap();

        let (tx, _rx) = mpsc::channel();

        let mut region1 = Region::default();
        region1.mut_peers().push(new_peer(store_id, 1));
        region1.set_end_key(split_key.to_vec());

        let mut region2 = Region::default();
        region2.mut_peers().push(new_peer(store_id, 2));
        region2.set_start_key(split_key.to_vec());

        let mut gc_worker = GcWorker::new(
            engine,
            tx,
            GcConfig::default(),
            gate,
            Arc::new(MockRegionInfoProvider::new(vec![region1, region2])),
        );
        gc_worker.start(store_id).unwrap();
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
                Mutation::make_put(Key::from_raw(key), value)
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
            b"key3",
        )
        .unwrap();

        test_destroy_range_impl(
            &[b"key1".to_vec(), b"key9".to_vec()],
            5,
            10,
            b"key3",
            b"key7",
            b"key5",
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
            b"key5",
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
            b"key3",
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
            b"key1",
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
            b"key1",
        )
        .unwrap();
    }

    #[test]
    fn test_gc_keys_with_region_info_provider() {
        let store_id = 1;
        let engine = TestEngineBuilder::new().build().unwrap();
        let mut prefixed_engine = PrefixedEngine(engine.clone());

        let (tx, _rx) = mpsc::channel();
        let feature_gate = FeatureGate::default();
        feature_gate.set_version("5.0.0").unwrap();

        let sp_provider = MockSafePointProvider(200);
        let mut host = CoprocessorHost::<RocksEngine>::default();
        let ri_provider = RegionInfoAccessor::new(&mut host);

        let mut gc_worker = GcWorker::new(
            prefixed_engine.clone(),
            tx,
            GcConfig::default(),
            feature_gate,
            Arc::new(ri_provider.clone()),
        );
        gc_worker.start(store_id).unwrap();

        let mut r1 = Region::default();
        r1.set_id(1);
        r1.mut_region_epoch().set_version(1);
        r1.set_start_key(b"".to_vec());
        r1.set_end_key(format!("k{:02}", 10).into_bytes());

        let mut r2 = Region::default();
        r2.set_id(2);
        r2.mut_region_epoch().set_version(1);
        r2.set_start_key(format!("k{:02}", 20).into_bytes());
        r2.set_end_key(format!("k{:02}", 30).into_bytes());
        r2.mut_peers().push(Peer::default());
        r2.mut_peers()[0].set_store_id(store_id);

        let mut r3 = Region::default();
        r3.set_id(3);
        r3.mut_region_epoch().set_version(1);
        r3.set_start_key(format!("k{:02}", 30).into_bytes());
        r3.set_end_key(b"".to_vec());
        r3.mut_peers().push(Peer::default());
        r3.mut_peers()[0].set_store_id(store_id);

        let auto_gc_cfg = AutoGcConfig::new(sp_provider, ri_provider, 1);
        let safe_point = Arc::new(AtomicU64::new(0));
        gc_worker.start_auto_gc(auto_gc_cfg, safe_point).unwrap();
        host.on_region_changed(&r1, RegionChangeEvent::Create, StateRole::Leader);
        host.on_region_changed(&r2, RegionChangeEvent::Create, StateRole::Leader);
        host.on_region_changed(&r3, RegionChangeEvent::Create, StateRole::Leader);

        let db = engine.kv_engine().unwrap().as_inner().clone();
        let cf = get_cf_handle(&db, CF_WRITE).unwrap();

        for i in 0..100 {
            let k = format!("k{:02}", i).into_bytes();
            must_prewrite_put(&mut prefixed_engine, &k, b"value", &k, 101);
            must_commit(&mut prefixed_engine, &k, 101, 102);
            must_prewrite_delete(&mut prefixed_engine, &k, &k, 151);
            must_commit(&mut prefixed_engine, &k, 151, 152);
        }
        db.flush_cf(cf, true).unwrap();

        db.compact_range_cf(cf, None, None);
        for i in 0..100 {
            let k = format!("k{:02}", i).into_bytes();

            // Stale MVCC-PUTs will be cleaned in write CF's compaction filter.
            must_get_none(&mut prefixed_engine, &k, 150);

            // However, MVCC-DELETIONs will be kept.
            let mut raw_k = vec![b'z'];
            let suffix = Key::from_raw(&k).append_ts(152.into());
            raw_k.extend_from_slice(suffix.as_encoded());
            assert!(db.get_cf(cf, &raw_k).unwrap().is_some());
        }

        db.compact_range_cf(cf, None, None);
        thread::sleep(Duration::from_millis(100));
        for i in 0..100 {
            let k = format!("k{:02}", i).into_bytes();
            let mut raw_k = vec![b'z'];
            let suffix = Key::from_raw(&k).append_ts(152.into());
            raw_k.extend_from_slice(suffix.as_encoded());

            if !(20..100).contains(&i) {
                // MVCC-DELETIONs can't be cleaned because region info checks can't pass.
                assert!(db.get_cf(cf, &raw_k).unwrap().is_some());
            } else {
                // MVCC-DELETIONs can be cleaned as expected.
                assert!(db.get_cf(cf, &raw_k).unwrap().is_none());
            }
        }
    }

    #[test]
    fn test_gc_keys_statistics() {
        let store_id = 1;
        let engine = TestEngineBuilder::new().build().unwrap();
        let mut prefixed_engine = PrefixedEngine(engine.clone());

        let (tx, _rx) = mpsc::channel();
        let cfg = GcConfig::default();
        let mut runner = GcRunner::new(
            store_id,
            prefixed_engine.clone(),
            tx,
            GcWorkerConfigManager(Arc::new(VersionTrack::new(cfg.clone())))
                .0
                .tracker("gc-woker".to_owned()),
            cfg,
        );

        let mut r1 = Region::default();
        r1.set_id(1);
        r1.mut_region_epoch().set_version(1);
        r1.set_start_key(b"".to_vec());
        r1.set_end_key(b"".to_vec());
        r1.mut_peers().push(Peer::default());
        r1.mut_peers()[0].set_store_id(store_id);

        let mut host = CoprocessorHost::<RocksEngine>::default();
        let ri_provider = RegionInfoAccessor::new(&mut host);
        host.on_region_changed(&r1, RegionChangeEvent::Create, StateRole::Leader);

        let db = engine.kv_engine().unwrap().as_inner().clone();
        let cf = get_cf_handle(&db, CF_WRITE).unwrap();
        let mut keys = vec![];
        for i in 0..100 {
            let k = format!("k{:02}", i).into_bytes();
            must_prewrite_put(&mut prefixed_engine, &k, b"value", &k, 101);
            must_commit(&mut prefixed_engine, &k, 101, 102);
            must_prewrite_delete(&mut prefixed_engine, &k, &k, 151);
            must_commit(&mut prefixed_engine, &k, 151, 152);
            keys.push(Key::from_raw(&k));
        }
        db.flush_cf(cf, true).unwrap();

        assert_eq!(runner.mut_stats(GcKeyMode::txn).write.seek, 0);
        assert_eq!(runner.mut_stats(GcKeyMode::txn).write.next, 0);
        runner
            .gc_keys(
                keys,
                TimeStamp::new(200),
                Either::Right(Arc::new(ri_provider)),
            )
            .unwrap();
        assert_eq!(runner.mut_stats(GcKeyMode::txn).write.seek, 1);
        assert_eq!(runner.mut_stats(GcKeyMode::txn).write.next, 100 * 2);
    }

    #[test]
    fn test_raw_gc_keys() {
        let store_id = 1;
        // init engine and gc runner
        let mut cfg = DbConfig::default();
        cfg.defaultcf.disable_auto_compactions = true;
        cfg.defaultcf.dynamic_level_bytes = false;
        let dir = tempfile::TempDir::new().unwrap();
        let builder = TestEngineBuilder::new().path(dir.path());
        let engine = builder.build_with_cfg(&cfg).unwrap();
        let mut prefixed_engine = PrefixedEngine(engine);

        let (tx, _rx) = mpsc::channel();
        let cfg = GcConfig::default();
        let mut runner = GcRunner::new(
            store_id,
            prefixed_engine.clone(),
            tx,
            GcWorkerConfigManager(Arc::new(VersionTrack::new(cfg.clone())))
                .0
                .tracker("gc-woker".to_owned()),
            cfg,
        );

        let mut r1 = Region::default();
        r1.set_id(1);
        r1.mut_region_epoch().set_version(1);
        r1.set_start_key(b"".to_vec());
        r1.set_end_key(b"".to_vec());
        r1.mut_peers().push(Peer::default());
        r1.mut_peers()[0].set_store_id(store_id);

        let mut host = CoprocessorHost::<RocksEngine>::default();
        let ri_provider = Arc::new(RegionInfoAccessor::new(&mut host));
        host.on_region_changed(&r1, RegionChangeEvent::Create, StateRole::Leader);
        // Init env end...

        // Prepare data
        let key_a = b"raaaaaaaaaaa";
        let key_b = b"rbbbbbbbbbbb";

        // All data in engine. (key,expir_ts,is_delete,expect_exist)
        let test_raws = vec![
            (key_a, 130, true, true), // ts(130) > safepoint
            (key_a, 120, true, true), // ts(120) = safepoint
            (key_a, 100, true, false),
            (key_a, 50, false, false),
            (key_a, 10, false, false),
            (key_a, 5, false, false),
            (key_b, 50, true, false),
            (key_b, 20, false, false),
            (key_b, 10, false, false),
        ];

        let modifies = test_raws
            .clone()
            .into_iter()
            .map(|(key, ts, is_delete, _expect_exist)| {
                (
                    ApiV2::encode_raw_key(key, Some(ts.into())),
                    ApiV2::encode_raw_value(RawValue {
                        user_value: &[0; 10][..],
                        expire_ts: Some(10),
                        is_delete,
                    }),
                )
            })
            .map(|(k, v)| Modify::Put(CF_DEFAULT, k, v))
            .collect();

        let ctx = Context {
            api_version: ApiVersion::V2,
            ..Default::default()
        };
        let batch = WriteData::from_modifies(modifies);

        prefixed_engine.write(&ctx, batch).unwrap();

        // Simulate the keys passed in from the compaction filter
        let test_keys = vec![key_a, key_b];

        let to_gc_keys: Vec<Key> = test_keys
            .into_iter()
            .map(|key| ApiV2::encode_raw_key(key, None))
            .collect();

        runner
            .raw_gc_keys(to_gc_keys, TimeStamp::new(120), ri_provider)
            .unwrap();

        assert_eq!(7, runner.mut_stats(GcKeyMode::raw).data.next);
        assert_eq!(2, runner.mut_stats(GcKeyMode::raw).data.seek);

        let snapshot = block_on(async {
            tikv_kv::snapshot(&mut prefixed_engine, SnapContext::default()).await
        })
        .unwrap();

        test_raws
            .clone()
            .into_iter()
            .for_each(|(key, ts, _is_delete, expect_exist)| {
                let engine_key = ApiV2::encode_raw_key(key, Some(ts.into()));
                let entry = snapshot.get(&Key::from_encoded(engine_key.into_encoded()));
                assert_eq!(entry.unwrap().is_some(), expect_exist);
            });
    }

    #[test]
    fn test_gc_keys_scan_range_limit() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let mut prefixed_engine = PrefixedEngine(engine.clone());

        let (tx, _rx) = mpsc::channel();
        let cfg = GcConfig::default();
        let mut runner = GcRunner::new(
            1,
            prefixed_engine.clone(),
            tx,
            GcWorkerConfigManager(Arc::new(VersionTrack::new(cfg.clone())))
                .0
                .tracker("gc-woker".to_owned()),
            cfg,
        );

        let mut r1 = Region::default();
        r1.set_id(1);
        r1.mut_region_epoch().set_version(1);
        r1.set_start_key(b"".to_vec());
        r1.set_end_key(b"".to_vec());
        r1.mut_peers().push(Peer::default());
        r1.mut_peers()[0].set_store_id(1);

        let mut host = CoprocessorHost::<RocksEngine>::default();
        let ri_provider = Arc::new(RegionInfoAccessor::new(&mut host));
        host.on_region_changed(&r1, RegionChangeEvent::Create, StateRole::Leader);

        let db = engine.kv_engine().unwrap().as_inner().clone();
        let cf = get_cf_handle(&db, CF_WRITE).unwrap();
        // Generate some tombstone
        for i in 10u64..30 {
            must_rollback(&mut prefixed_engine, b"k2\x00", i, true);
        }
        db.flush_cf(cf, true).unwrap();
        must_gc(&mut prefixed_engine, b"k2\x00", 30);

        // Test tombstone counter works
        assert_eq!(runner.mut_stats(GcKeyMode::txn).write.seek_tombstone, 0);
        runner
            .gc_keys(
                vec![Key::from_raw(b"k2\x00")],
                TimeStamp::new(200),
                Either::Right(ri_provider.clone()),
            )
            .unwrap();
        assert_eq!(runner.mut_stats(GcKeyMode::txn).write.seek_tombstone, 20);

        // gc_keys with single key
        runner
            .mut_stats(GcKeyMode::txn)
            .mut_cf_statistics(CF_WRITE)
            .seek_tombstone = 0;
        assert_eq!(runner.mut_stats(GcKeyMode::txn).write.seek_tombstone, 0);
        runner
            .gc_keys(
                vec![Key::from_raw(b"k2")],
                TimeStamp::new(200),
                Either::Right(ri_provider.clone()),
            )
            .unwrap();
        assert_eq!(runner.mut_stats(GcKeyMode::txn).write.seek_tombstone, 0);

        // gc_keys with multiple key
        runner
            .mut_stats(GcKeyMode::txn)
            .mut_cf_statistics(CF_WRITE)
            .seek_tombstone = 0;
        assert_eq!(runner.mut_stats(GcKeyMode::txn).write.seek_tombstone, 0);
        runner
            .gc_keys(
                vec![Key::from_raw(b"k1"), Key::from_raw(b"k2")],
                TimeStamp::new(200),
                Either::Right(ri_provider.clone()),
            )
            .unwrap();
        assert_eq!(runner.mut_stats(GcKeyMode::txn).write.seek_tombstone, 0);

        // Test rebuilding snapshot when GC write batch limit reached
        // (gc_info.is_completed == false). Build a key with versions that will
        // just reach the limit `MAX_TXN_WRITE_SIZE`.
        let key_size = Modify::Delete(CF_WRITE, Key::from_raw(b"k2").append_ts(1.into())).size();
        // versions = ceil(MAX_TXN_WRITE_SIZE/write_size) + 3
        // Write CF: Put@N, Put@N-2,    Put@N-4, ... Put@5,   Put@3
        //                 ^            ^^^^^^^^^^^^^^^^^^^
        //           safepoint=N-1      Deleted in the first batch,
        // `ceil(MAX_TXN_WRITE_SIZE/write_size)` versions.
        let versions = (MAX_TXN_WRITE_SIZE - 1) / key_size + 4;
        for start_ts in (1..versions).map(|x| x as u64 * 2) {
            let commit_ts = start_ts + 1;
            must_prewrite_put(&mut prefixed_engine, b"k2", b"v2", b"k2", start_ts);
            must_commit(&mut prefixed_engine, b"k2", start_ts, commit_ts);
        }
        db.flush_cf(cf, true).unwrap();
        let safepoint = versions as u64 * 2;

        runner
            .mut_stats(GcKeyMode::txn)
            .mut_cf_statistics(CF_DEFAULT)
            .seek_tombstone = 0;
        runner
            .gc_keys(
                vec![Key::from_raw(b"k2")],
                safepoint.into(),
                Either::Right(ri_provider),
            )
            .unwrap();
        // The first batch will leave tombstones that will be seen while processing the
        // second batch, but it will be seen in `next` after seeking the latest
        // unexpired version, therefore `seek_tombstone` is not affected.
        assert_eq!(runner.mut_stats(GcKeyMode::txn).write.seek_tombstone, 0);
        // ... and next_tombstone indicates there's indeed more than one batches.
        assert_eq!(
            runner.mut_stats(GcKeyMode::txn).write.next_tombstone,
            versions - 3
        );
    }

    #[test]
    fn delete_range_when_worker_is_full() {
        let store_id = 1;
        let mut engine = PrefixedEngine(TestEngineBuilder::new().build().unwrap());
        must_prewrite_put(&mut engine, b"key", b"value", b"key", 10);
        must_commit(&mut engine, b"key", 10, 20);
        let db = engine.kv_engine().unwrap().as_inner().clone();
        let cf = get_cf_handle(&db, CF_WRITE).unwrap();
        db.flush_cf(cf, true).unwrap();

        let gate = FeatureGate::default();
        gate.set_version("5.0.0").unwrap();
        let (tx, _rx) = mpsc::channel();

        let mut region = Region::default();
        region.mut_peers().push(new_peer(store_id, 1));

        let mut gc_worker = GcWorker::new(
            engine.clone(),
            tx,
            GcConfig::default(),
            gate,
            Arc::new(MockRegionInfoProvider::new(vec![region.clone()])),
        );

        // Before starting gc_worker, fill the scheduler to full.
        for _ in 0..GC_MAX_PENDING_TASKS {
            gc_worker
                .scheduler()
                .schedule(GcTask::Gc {
                    region: region.clone(),
                    safe_point: TimeStamp::from(100),
                    callback: Box::new(|_res| {}),
                })
                .unwrap();
        }
        // Then, it will fail to schedule another gc command.
        let (tx, rx) = mpsc::channel();
        gc_worker
            .gc(
                Region::default(),
                TimeStamp::from(1),
                Box::new(move |res| {
                    tx.send(res).unwrap();
                }),
            )
            .unwrap_err();
        rx.recv().unwrap().unwrap_err();

        let (tx, rx) = mpsc::channel();
        // When the gc_worker is full, scheduling an unsafe destroy range task should be
        // still allowed.
        gc_worker
            .unsafe_destroy_range(
                Context::default(),
                Key::from_raw(b"a"),
                Key::from_raw(b"z"),
                Box::new(move |res| {
                    tx.send(res).unwrap();
                }),
            )
            .unwrap();

        gc_worker.start(store_id).unwrap();

        // After the worker starts running, the destroy range task should run,
        // and the key in the range will be deleted.
        rx.recv_timeout(Duration::from_secs(10)).unwrap().unwrap();
        must_get_none(&mut engine, b"key", 30);
    }

    #[test]
    fn test_keys_in_regions_iteration() {
        fn init_region(start_key: &[u8], end_key: &[u8]) -> Region {
            let start_key = Key::from_raw(start_key);
            let end_key = Key::from_raw(end_key);
            let mut region = Region::default();
            region.set_start_key(start_key.as_encoded().clone());
            region.set_end_key(end_key.as_encoded().clone());
            region
        }

        fn generate_keys(start: u64, end: u64) -> Vec<Key> {
            (start..end)
                .into_iter()
                .map(|i| {
                    let key = format!("k{:02}", i);
                    Key::from_raw(key.as_bytes())
                })
                .collect::<Vec<_>>()
        }

        // One region cover all keys
        let keys = generate_keys(1, 4);
        let region = init_region(b"k01", b"k04");
        let mut iter = keys.clone().into_iter().peekable();
        let ks = get_keys_in_region(&mut iter, &region);
        assert!(iter.peek().is_none());
        assert_eq!(ks, keys);

        // More than one regions cover all keys
        let keys = generate_keys(1, 9);
        let region1 = init_region(b"k01", b"k04");
        let region2 = init_region(b"k04", b"k06");
        let region3 = init_region(b"k06", b"k09");
        let mut iter = keys.into_iter().peekable();
        let ks = get_keys_in_region(&mut iter, &region1);
        assert_eq!(ks, generate_keys(1, 4));
        let ks = get_keys_in_region(&mut iter, &region2);
        assert_eq!(ks, generate_keys(4, 6));
        let ks = get_keys_in_region(&mut iter, &region3);
        assert_eq!(ks, generate_keys(6, 9));
        assert!(iter.peek().is_none());

        // Cover partial keys
        let keys = generate_keys(1, 9);
        let region1 = init_region(b"k01", b"k04");
        let region2 = init_region(b"k06", b"k09");
        let mut iter = keys.into_iter().peekable();
        let ks = get_keys_in_region(&mut iter, &region1);
        assert_eq!(ks, generate_keys(1, 4));
        let ks = get_keys_in_region(&mut iter, &region2);
        assert_eq!(ks, generate_keys(6, 9));
        assert!(iter.peek().is_none());

        // No key
        let keys = generate_keys(1, 9);
        let region = init_region(b"k11", b"k20");
        let mut iter = keys.into_iter().peekable();
        let ks = get_keys_in_region(&mut iter, &region);
        assert!(iter.peek().is_none());
        assert!(ks.is_empty());
    }

    // setup engine and prepare some data:
    //  three regions:
    //  region 1: includes ("k00", "value-00") to ("k09", "value-09")
    //  region 2: includes ("k10", "value-10") to ("k19", "value-19")
    //  region 3: includes ("k20", "value-20") to ("k29", "value-29")
    fn multi_gc_engine_setup(
        path: &Path,
        store_id: u64,
        put_start_ts: u64,
        delete_start_ts: u64,
        need_deletion: bool,
    ) -> (
        MultiRocksEngine,
        Arc<MockRegionInfoProvider>,
        GcRunner<MultiRocksEngine>,
        Vec<Region>,
        mpsc::Receiver<FlowInfo>,
    ) {
        let mut engine = MultiRocksEngine::default();

        // Note: as the tablet split is not supported yet, we artificially divide the
        // region to: 1 ["", "k10"], 2 ["k10", "k20"], 3["k20", "30"]
        let r1 = init_region(b"", b"k10", 1, Some(store_id));
        engine.region_info.insert(1, r1.clone());
        engine.engines.lock().unwrap().insert(
            1,
            PrefixedEngine(
                TestEngineBuilder::new()
                    .path(path.join("1"))
                    .build()
                    .unwrap(),
            ),
        );
        let r2 = init_region(b"k10", b"k20", 2, Some(store_id));
        engine.region_info.insert(2, r2.clone());
        engine.engines.lock().unwrap().insert(
            2,
            PrefixedEngine(
                TestEngineBuilder::new()
                    .path(path.join("2"))
                    .build()
                    .unwrap(),
            ),
        );
        let r3 = init_region(b"k20", b"", 3, Some(store_id));
        engine.region_info.insert(3, r3.clone());
        engine.engines.lock().unwrap().insert(
            3,
            PrefixedEngine(
                TestEngineBuilder::new()
                    .path(path.join("3"))
                    .build()
                    .unwrap(),
            ),
        );

        let (tx, rx) = mpsc::channel();
        let feature_gate = FeatureGate::default();
        feature_gate.set_version("5.0.0").unwrap();

        let ri_provider = Arc::new(MockRegionInfoProvider::new(vec![
            r1.clone(),
            r2.clone(),
            r3.clone(),
        ]));

        let cfg = GcConfig::default();
        let gc_runner = GcRunner::new(
            store_id,
            engine.clone(),
            tx,
            GcWorkerConfigManager(Arc::new(VersionTrack::new(cfg.clone())))
                .0
                .tracker("gc-woker".to_owned()),
            cfg,
        );

        let mut region_id = 0;
        for i in 0..30 {
            if i % 10 == 0 {
                region_id += 1;
            }
            let k = format!("k{:02}", i).into_bytes();
            let v = format!("value-{:02}", i).into_bytes();
            must_prewrite_put_on_region(&mut engine, region_id, &k, &v, &k, put_start_ts);
            must_commit_on_region(&mut engine, region_id, &k, put_start_ts, put_start_ts + 1);
            if need_deletion {
                must_prewrite_delete_on_region(&mut engine, region_id, &k, &k, delete_start_ts);
                must_commit_on_region(
                    &mut engine,
                    region_id,
                    &k,
                    delete_start_ts,
                    delete_start_ts + 1,
                );
            }
        }

        (engine, ri_provider, gc_runner, vec![r1, r2, r3], rx)
    }

    #[test]
    fn test_gc_for_multi_rocksdb() {
        let dir = Builder::new()
            .prefix("test_gc_for_multi_rocksdb")
            .tempdir()
            .unwrap();
        let store_id = 1;

        let put_start_ts = 100;
        let delete_start_ts = 150;
        let (mut engine, _ri_provider, mut gc_runner, regions, _) =
            multi_gc_engine_setup(dir.path(), store_id, put_start_ts, delete_start_ts, true);

        gc_runner.gc(regions[0].clone(), 200.into()).unwrap();
        gc_runner.gc(regions[1].clone(), 200.into()).unwrap();
        gc_runner.gc(regions[2].clone(), 200.into()).unwrap();

        for region_id in 1..=3 {
            let region_engine = engine.engines.lock().unwrap()[&region_id].clone();

            for i in 10 * (region_id - 1)..10 * region_id {
                let k = format!("k{:02}", i).into_bytes();

                // Stale MVCC-PUTs will be cleaned in write CF's compaction filter.
                must_get_none_on_region(&mut engine, region_id, &k, delete_start_ts - 1);

                // MVCC-DELETIONs is cleaned
                let mut raw_k = vec![b'z'];
                let suffix = Key::from_raw(&k).append_ts((delete_start_ts + 1).into());
                raw_k.extend_from_slice(suffix.as_encoded());
                assert!(
                    region_engine
                        .kv_engine()
                        .unwrap()
                        .get_value_cf(CF_WRITE, &raw_k)
                        .unwrap()
                        .is_none()
                );
            }
        }
    }

    #[test]
    fn test_gc_keys_for_multi_rocksdb() {
        let dir = Builder::new()
            .prefix("test_gc_keys_for_multi_rocksdb")
            .tempdir()
            .unwrap();
        let store_id = 1;

        let put_start_ts = 100;
        let delete_start_ts = 150;
        let (mut engine, ri_provider, mut gc_runner, ..) =
            multi_gc_engine_setup(dir.path(), store_id, put_start_ts, delete_start_ts, true);

        let mut keys = Vec::new();
        for i in 0..30 {
            if i % 2 == 0 {
                continue;
            }

            let k = format!("k{:02}", i).into_bytes();
            let key = Key::from_raw(&k);
            keys.push(key);
        }
        let _ = gc_runner
            .gc_keys(keys, 200.into(), Either::Right(ri_provider))
            .unwrap();

        for region_id in 1..=3 {
            let region_engine = engine.engines.lock().unwrap()[&region_id].clone();

            for i in 10 * (region_id - 1)..10 * region_id {
                let k = format!("k{:02}", i).into_bytes();
                let val = format!("value-{:02}", i).into_bytes();

                let mut raw_k = vec![b'z'];
                let suffix = Key::from_raw(&k).append_ts((delete_start_ts + 1).into());
                raw_k.extend_from_slice(suffix.as_encoded());

                if i % 2 == 0 {
                    assert!(
                        region_engine
                            .kv_engine()
                            .unwrap()
                            .get_value_cf(CF_WRITE, &raw_k)
                            .unwrap()
                            .is_some()
                    );
                    must_get_on_region(&mut engine, region_id, &k, delete_start_ts - 1, &val);
                } else {
                    assert!(
                        region_engine
                            .kv_engine()
                            .unwrap()
                            .get_value_cf(CF_WRITE, &raw_k)
                            .unwrap()
                            .is_none()
                    );
                    must_get_none_on_region(&mut engine, region_id, &k, delete_start_ts - 1);
                }
            }
        }
    }

    #[test]
    fn test_raw_gc_keys_for_multi_rocksdb() {
        let dir = Builder::new()
            .prefix("test_raw_gc_keys_for_multi_rocksdb")
            .tempdir()
            .unwrap();
        let store_id = 1;

        let mut engine = MultiRocksEngine::default();

        // Note: as the tablet split is not supported yet, we artificially divide the
        // region to: 1 ["", "k10"], 2 ["k10", ""]
        let r1 = init_region(b"", b"k10", 1, Some(store_id));
        engine.region_info.insert(1, r1.clone());
        engine.engines.lock().unwrap().insert(
            1,
            PrefixedEngine(
                TestEngineBuilder::new()
                    .path(dir.path().join("1"))
                    .build()
                    .unwrap(),
            ),
        );
        let r2 = init_region(b"k10", b"", 2, Some(store_id));
        engine.region_info.insert(2, r2.clone());
        engine.engines.lock().unwrap().insert(
            2,
            PrefixedEngine(
                TestEngineBuilder::new()
                    .path(dir.path().join("2"))
                    .build()
                    .unwrap(),
            ),
        );

        let (tx, _rx) = mpsc::channel();
        let ri_provider = Arc::new(MockRegionInfoProvider::new(vec![r1, r2]));

        let cfg = GcConfig::default();
        let mut gc_runner = GcRunner::new(
            store_id,
            engine.clone(),
            tx,
            GcWorkerConfigManager(Arc::new(VersionTrack::new(cfg.clone())))
                .0
                .tracker("gc-woker".to_owned()),
            cfg,
        );

        // region_id -> vec<(key,expir_ts,is_delete,expect_exist)>
        let mut test_raws = HashMap::default();
        let mut test_raws_region = Vec::new();
        let mut test_keys = Vec::new();
        let mut i = 0;
        let mut region_id = 1;
        while i < 20 {
            if i == 10 {
                test_raws.insert(region_id, test_raws_region);
                region_id += 1;
                test_raws_region = Vec::new();
            }
            let k1 = format!("k{:02}", i).into_bytes();
            let k2 = format!("k{:02}", i + 1).into_bytes();
            test_keys.push(k1.clone());
            test_keys.push(k2.clone());

            // All data in engine. (key,expir_ts,is_delete,expect_exist)
            let test_raw = vec![
                (k1.clone(), 130, true, true), // ts(130) > safepoint
                (k1.clone(), 120, true, true), // ts(120) = safepoint
                (k1.clone(), 100, true, false),
                (k1.clone(), 50, false, false),
                (k1.clone(), 10, false, false),
                (k1, 5, false, false),
                (k2.clone(), 50, true, false),
                (k2.clone(), 20, false, false),
                (k2, 10, false, false),
            ];

            let modifies = test_raw
                .clone()
                .into_iter()
                .map(|(key, ts, is_delete, _expect_exist)| {
                    (
                        ApiV2::encode_raw_key(&key, Some(ts.into())),
                        ApiV2::encode_raw_value(RawValue {
                            user_value: &[0; 10][..],
                            expire_ts: Some(10),
                            is_delete,
                        }),
                    )
                })
                .map(|(k, v)| Modify::Put(CF_DEFAULT, k, v))
                .collect();

            let ctx = Context {
                region_id,
                api_version: ApiVersion::V2,
                ..Default::default()
            };
            let batch = WriteData::from_modifies(modifies);
            engine.write(&ctx, batch).unwrap();

            test_raws_region.extend(test_raw);
            i += 2;
        }
        test_raws.insert(region_id, test_raws_region);

        let to_gc_keys: Vec<Key> = test_keys
            .into_iter()
            .map(|key| ApiV2::encode_raw_key(&key, None))
            .collect();

        gc_runner
            .raw_gc_keys(to_gc_keys, TimeStamp::new(120), ri_provider)
            .unwrap();

        assert_eq!(70, gc_runner.mut_stats(GcKeyMode::raw).data.next);
        assert_eq!(20, gc_runner.mut_stats(GcKeyMode::raw).data.seek);

        for (region_id, test_raws_region) in test_raws {
            let mut ctx = Context::default();
            ctx.region_id = region_id;
            let snap_ctx = SnapContext {
                pb_ctx: &ctx,
                ..Default::default()
            };
            let snapshot =
                block_on(async { tikv_kv::snapshot(&mut engine, snap_ctx).await }).unwrap();

            test_raws_region
                .clone()
                .into_iter()
                .for_each(|(key, ts, _is_delete, expect_exist)| {
                    let engine_key = ApiV2::encode_raw_key(&key, Some(ts.into()));
                    let entry = snapshot.get(&Key::from_encoded(engine_key.into_encoded()));
                    assert_eq!(entry.unwrap().is_some(), expect_exist);
                });
        }
    }

    // `start_key` and `end_key` determines which regions or how how many regions or
    // which parts of regions are deleted. The data used in this method are shown in
    // the comment of `multi_gc_engine_setup`.
    fn test_destroy_range_for_multi_rocksdb_impl(
        start_key: &[u8],
        end_key: &[u8],
        exected_regions: Vec<u64>,
    ) {
        let dir = Builder::new()
            .prefix("test_destroy_range_for_multi_rocksdb_impl")
            .tempdir()
            .unwrap();
        let store_id = 1;
        let put_start_ts = 100;
        let (mut engine, ri_provider, gc_runner, _, _rx) =
            multi_gc_engine_setup(dir.path(), store_id, put_start_ts, 0, false);

        let start_key = Key::from_raw(start_key);
        let end_key = Key::from_raw(end_key);

        let ctx = Context::default();
        gc_runner
            .unsafe_destroy_range(&ctx, &start_key, &end_key, ri_provider)
            .unwrap();

        let mut regions = BTreeSet::new();
        for region_id in 1..=3 {
            let region_engine = engine.engines.lock().unwrap()[&region_id].clone();

            for i in 10 * (region_id - 1)..10 * region_id {
                let k = format!("k{:02}", i).into_bytes();
                let key = Key::from_raw(&k);
                let val = format!("value-{:02}", i).into_bytes();

                let mut raw_k = vec![b'z'];
                let suffix = Key::from_raw(&k).append_ts((put_start_ts + 1).into());
                raw_k.extend_from_slice(suffix.as_encoded());

                if start_key <= key && key < end_key {
                    regions.insert(region_id);
                    assert!(
                        region_engine
                            .kv_engine()
                            .unwrap()
                            .get_value_cf(CF_WRITE, &raw_k)
                            .unwrap()
                            .is_none()
                    );
                    must_get_none_on_region(&mut engine, region_id, &k, put_start_ts + 10);
                } else {
                    assert!(
                        region_engine
                            .kv_engine()
                            .unwrap()
                            .get_value_cf(CF_WRITE, &raw_k)
                            .unwrap()
                            .is_some()
                    );
                    must_get_on_region(&mut engine, region_id, &k, put_start_ts + 10, &val);
                }
            }
        }

        let regions: Vec<_> = regions.into_iter().collect();
        assert!(regions == exected_regions);
    }

    #[test]
    fn test_destroy_range_for_multi_rocksdb() {
        // Cover all keys in all regions
        test_destroy_range_for_multi_rocksdb_impl(b"", b"k99", vec![1, 2, 3]);

        // Cover some keys in region 1, and all keys in the other regions
        test_destroy_range_for_multi_rocksdb_impl(b"k051", b"k99", vec![1, 2, 3]);

        // Cover some keys in region 3, and all keys in the other regions
        test_destroy_range_for_multi_rocksdb_impl(b"", b"k25", vec![1, 2, 3]);
        test_destroy_range_for_multi_rocksdb_impl(b"", b"k249", vec![1, 2, 3]);

        // Cover some keys in region 1 and region 3, and all keys in region 2
        test_destroy_range_for_multi_rocksdb_impl(b"k032", b"k249", vec![1, 2, 3]);

        // Cover all keys in region 2, and no keys in others
        test_destroy_range_for_multi_rocksdb_impl(b"k10", b"k20", vec![2]);
        test_destroy_range_for_multi_rocksdb_impl(b"k099", b"k195", vec![2]);

        // Cover two regions
        test_destroy_range_for_multi_rocksdb_impl(b"k05", b"k195", vec![1, 2]);
        test_destroy_range_for_multi_rocksdb_impl(b"k099", b"k25", vec![2, 3]);
    }
}
