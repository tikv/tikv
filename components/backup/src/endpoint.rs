// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    borrow::Cow,
    cell::RefCell,
    fmt,
    sync::{Arc, Mutex, RwLock, atomic::*, mpsc},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_channel::SendError;
use causal_ts::{CausalTsProvider, CausalTsProviderImpl};
use concurrency_manager::ConcurrencyManager;
use engine_traits::{CfName, KvEngine, SstCompressionType, name_to_cf, raw_ttl::ttl_current_ts};
use external_storage::{BackendConfig, ExternalStorage, HdfsConfig, create_storage};
use futures::{channel::mpsc::*, executor::block_on};
use kvproto::{
    brpb::*,
    encryptionpb::EncryptionMethod,
    kvrpcpb::{ApiVersion, Context, IsolationLevel, KeyRange},
    metapb::*,
};
use online_config::OnlineConfig;
use raft::StateRole;
use raftstore::coprocessor::RegionInfoProvider;
use resource_control::{ResourceGroupManager, ResourceLimiter, with_resource_limiter};
use tikv::{
    config::BackupConfig,
    storage::{
        Snapshot, Statistics,
        kv::{CursorBuilder, Engine, LocalTablets, ScanMode, SnapContext},
        mvcc::Error as MvccError,
        raw::raw_mvcc::RawMvccSnapshot,
        txn::{EntryBatch, Error as TxnError, SnapshotStore, TxnEntryScanner, TxnEntryStore},
    },
};
use tikv_util::{
    box_err, debug, error, error_unknown,
    future::RescheduleChecker,
    impl_display_as_debug, info,
    resizable_threadpool::ResizableRuntime,
    store::find_peer,
    time::{Instant, Limiter},
    warn,
    worker::Runnable,
};
use tokio::runtime::{Handle, Runtime};
use txn_types::{Key, Lock, TimeStamp, TsSet};

use crate::{
    Error,
    metrics::*,
    softlimit::{CpuStatistics, SoftLimit, SoftLimitByCpu},
    utils::KeyValueCodec,
    writer::{BackupWriterBuilder, CfNameWrap},
    *,
};

const BACKUP_BATCH_LIMIT: usize = 1024;
// task yield duration when resource limit is on.
const TASK_YIELD_DURATION: Duration = Duration::from_millis(10);

#[derive(Clone)]
struct Request {
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    sub_ranges: Vec<KeyRange>,
    start_ts: TimeStamp,
    end_ts: TimeStamp,
    // cloning on Limiter will share the same underlying token bucket thus can be used as
    // a global rate limiter
    rate_limiter: Limiter,
    backend: StorageBackend,
    cancel: Arc<AtomicBool>,
    is_raw_kv: bool,
    dst_api_ver: ApiVersion,
    cf: CfName,
    compression_type: CompressionType,
    compression_level: i32,
    cipher: CipherInfo,
    replica_read: bool,
    resource_group_name: String,
    source_tag: String,
    bypass_locks: Vec<u64>,
    access_locks: Vec<u64>,
}

/// Backup Task.
pub struct Task {
    request: Request,
    pub(crate) resp: UnboundedSender<BackupResponse>,
}

impl_display_as_debug!(Task);

impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackupTask")
            .field("start_ts", &self.request.start_ts)
            .field("end_ts", &self.request.end_ts)
            .field(
                "start_key",
                &log_wrappers::Value::key(&self.request.start_key),
            )
            .field("end_key", &log_wrappers::Value::key(&self.request.end_key))
            .field("is_raw_kv", &self.request.is_raw_kv)
            .field("dst_api_ver", &self.request.dst_api_ver)
            .field("cf", &self.request.cf)
            .finish()
    }
}

impl Task {
    /// Create a backup task based on the given backup request.
    pub fn new(
        req: BackupRequest,
        resp: UnboundedSender<BackupResponse>,
    ) -> Result<(Task, Arc<AtomicBool>)> {
        let cancel = Arc::new(AtomicBool::new(false));

        let rate_limit = req.get_rate_limit();
        let rate_limiter = Limiter::new(if rate_limit > 0 {
            rate_limit as f64
        } else {
            f64::INFINITY
        });
        let cf = name_to_cf(req.get_cf()).ok_or_else(|| crate::Error::InvalidCf {
            cf: req.get_cf().to_owned(),
        })?;

        let mut source_tag: String = req.get_context().get_request_source().into();
        if source_tag.is_empty() {
            source_tag = "br".into();
        }
        let task = Task {
            request: Request {
                start_key: req.get_start_key().to_owned(),
                end_key: req.get_end_key().to_owned(),
                sub_ranges: req.get_sub_ranges().to_owned(),
                start_ts: req.get_start_version().into(),
                end_ts: req.get_end_version().into(),
                backend: req.get_storage_backend().clone(),
                rate_limiter,
                cancel: cancel.clone(),
                is_raw_kv: req.get_is_raw_kv(),
                dst_api_ver: req.get_dst_api_version(),
                cf,
                compression_type: req.get_compression_type(),
                compression_level: req.get_compression_level(),
                replica_read: req.get_replica_read(),
                resource_group_name: req
                    .get_context()
                    .get_resource_control_context()
                    .get_resource_group_name()
                    .to_owned(),
                source_tag,
                bypass_locks: req.get_context().get_resolved_locks().to_owned(),
                access_locks: req.get_context().get_committed_locks().to_owned(),
                cipher: req.cipher_info.unwrap_or_else(|| {
                    let mut cipher = CipherInfo::default();
                    cipher.set_cipher_type(EncryptionMethod::Plaintext);
                    cipher
                }),
            },
            resp,
        };
        Ok((task, cancel))
    }

    /// Check whether the task is canceled.
    pub fn has_canceled(&self) -> bool {
        self.request.cancel.load(Ordering::SeqCst)
    }
}

#[derive(Debug)]
pub struct BackupRange {
    start_key: Option<Key>,
    end_key: Option<Key>,
    region: Region,
    peer: Peer,
    codec: KeyValueCodec,
    cf: CfName,
    uses_replica_read: bool,
}

/// The generic saveable writer. for generic `InMemBackupFiles`.
/// Maybe what we really need is make Writer a trait...
enum KvWriter<EK: KvEngine> {
    Txn(BackupWriter<EK>),
    Raw(BackupRawKvWriter<EK>),
}

impl<EK: KvEngine> std::fmt::Debug for KvWriter<EK> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Txn(_) => f.debug_tuple("Txn").finish(),
            Self::Raw(_) => f.debug_tuple("Raw").finish(),
        }
    }
}

impl<EK: KvEngine> KvWriter<EK> {
    async fn save(self, storage: &dyn ExternalStorage) -> Result<Vec<File>> {
        match self {
            Self::Txn(writer) => writer.save(storage).await,
            Self::Raw(writer) => writer.save(storage).await,
        }
    }

    fn need_flush_keys(&self) -> bool {
        match self {
            Self::Txn(writer) => writer.need_flush_keys(),
            Self::Raw(_) => true,
        }
    }
}

#[derive(Debug)]
struct InMemBackupFiles<EK: KvEngine> {
    files: KvWriter<EK>,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    start_version: TimeStamp,
    end_version: TimeStamp,
    region: Region,
    resource_limiter: Option<Arc<ResourceLimiter>>,
}

async fn save_backup_file_worker<EK: KvEngine>(
    rx: async_channel::Receiver<InMemBackupFiles<EK>>,
    tx: UnboundedSender<BackupResponse>,
    storage: Arc<dyn ExternalStorage>,
    codec: KeyValueCodec,
) {
    while let Ok(msg) = rx.recv().await {
        let files = if msg.files.need_flush_keys() {
            match with_resource_limiter(msg.files.save(&storage), msg.resource_limiter.clone())
                .await
            {
                Ok(mut split_files) => {
                    let mut has_err = false;
                    for file in split_files.iter_mut() {
                        // In the case that backup from v1 and restore to v2,
                        // the file range need be encoded as v2 format.
                        // And range in response keep in v1 format.
                        let ret = codec.convert_key_range_to_dst_version(
                            msg.start_key.clone(),
                            msg.end_key.clone(),
                        );
                        if ret.is_err() {
                            has_err = true;
                            break;
                        }
                        let (start, end) = ret.unwrap();
                        file.set_start_key(start);
                        file.set_end_key(end);
                        file.set_start_version(msg.start_version.into_inner());
                        file.set_end_version(msg.end_version.into_inner());
                    }
                    if has_err {
                        Err(box_err!("backup convert key range failed"))
                    } else {
                        Ok(split_files)
                    }
                }
                Err(e) => {
                    error_unknown!(?e; "backup save file failed");
                    Err(e)
                }
            }
        } else {
            Ok(vec![])
        };
        let mut response = BackupResponse::default();
        match files {
            Err(e) => {
                error_unknown!(?e; "backup region failed";
                    "region" => ?msg.region,
                    "start_key" => &log_wrappers::Value::key(&msg.start_key),
                    "end_key" => &log_wrappers::Value::key(&msg.end_key),
                );
                response.set_error(e.into());
            }
            Ok(files) => {
                response.set_files(files.into());
            }
        }
        response.set_start_key(msg.start_key.clone());
        response.set_end_key(msg.end_key.clone());
        response.set_api_version(codec.dst_api_ver);
        if let Err(e) = tx.unbounded_send(response) {
            error_unknown!(?e; "backup failed to send response"; "region" => ?msg.region,
            "start_key" => &log_wrappers::Value::key(&msg.start_key),
            "end_key" => &log_wrappers::Value::key(&msg.end_key),);
            if e.is_disconnected() {
                return;
            }
        }
    }
}

/// Send the save task to the save worker.
/// Record the wait time at the same time.
async fn send_to_worker_with_metrics<EK: KvEngine>(
    tx: &async_channel::Sender<InMemBackupFiles<EK>>,
    files: InMemBackupFiles<EK>,
) -> std::result::Result<(), SendError<InMemBackupFiles<EK>>> {
    let files = match tx.try_send(files) {
        Ok(_) => return Ok(()),
        Err(e) => e.into_inner(),
    };
    let begin = Instant::now();
    tx.send(files).await?;
    BACKUP_SCAN_WAIT_FOR_WRITER_HISTOGRAM.observe(begin.saturating_elapsed_secs());
    Ok(())
}

impl BackupRange {
    /// Get entries from the scanner and save them to storage
    async fn backup<E: Engine>(
        &self,
        writer_builder: BackupWriterBuilder<E::Local>,
        mut engine: E,
        concurrency_manager: ConcurrencyManager,
        backup_ts: TimeStamp,
        begin_ts: TimeStamp,
        bypass_locks: Vec<u64>,
        access_locks: Vec<u64>,
        saver: async_channel::Sender<InMemBackupFiles<E::Local>>,
        storage_name: &str,
        resource_limiter: Option<Arc<ResourceLimiter>>,
    ) -> Result<Statistics> {
        assert!(!self.codec.is_raw_kv);

        let mut ctx = Context::default();
        ctx.set_region_id(self.region.get_id());
        ctx.set_region_epoch(self.region.get_region_epoch().to_owned());
        ctx.set_peer(self.peer.clone());
        ctx.set_replica_read(self.uses_replica_read);
        ctx.set_isolation_level(IsolationLevel::Si);

        let mut snap_ctx = SnapContext {
            pb_ctx: &ctx,
            allowed_in_flashback: self.region.is_in_flashback,
            ..Default::default()
        };
        if self.uses_replica_read {
            snap_ctx.start_ts = Some(backup_ts);
            let mut key_range = KeyRange::default();
            if let Some(start_key) = self.start_key.as_ref() {
                key_range.set_start_key(start_key.clone().into_encoded());
            }
            if let Some(end_key) = self.end_key.as_ref() {
                key_range.set_end_key(end_key.clone().into_encoded());
            }
            snap_ctx.key_ranges = vec![key_range];
        } else {
            // Update max_ts and check the in-memory lock table before getting the snapshot
            concurrency_manager
                .update_max_ts(backup_ts, "backup_range")
                .map_err(TxnError::from)?;
            concurrency_manager
                .read_range_check(
                    self.start_key.as_ref(),
                    self.end_key.as_ref(),
                    |key, lock| {
                        Lock::check_ts_conflict(
                            Cow::Borrowed(lock),
                            key,
                            backup_ts,
                            &Default::default(),
                            IsolationLevel::Si,
                        )
                    },
                )
                .map_err(MvccError::from)
                .map_err(TxnError::from)?;
        }

        let start_snapshot = Instant::now();
        let snapshot = match engine.snapshot(snap_ctx) {
            Ok(s) => s,
            Err(e) => {
                warn!("backup snapshot failed"; "err" => ?e, "ctx" => ?ctx);
                return Err(e.into());
            }
        };
        BACKUP_RANGE_HISTOGRAM_VEC
            .with_label_values(&["snapshot"])
            .observe(start_snapshot.saturating_elapsed().as_secs_f64());

        let snap_store = SnapshotStore::new(
            snapshot,
            backup_ts,
            IsolationLevel::Si,
            false, // fill_cache
            TsSet::vec_from_u64s(bypass_locks),
            TsSet::vec_from_u64s(access_locks),
            false,
        );
        let start_key = self.start_key.clone();
        let end_key = self.end_key.clone();
        // Incremental backup needs to output delete records.
        let incremental = !begin_ts.is_zero();
        let mut scanner = snap_store
            .entry_scanner(start_key, end_key, begin_ts, incremental)
            .unwrap();

        let start_scan = Instant::now();
        let mut batch = EntryBatch::with_capacity(BACKUP_BATCH_LIMIT);
        let mut next_file_start_key = self
            .start_key
            .clone()
            .map_or_else(Vec::new, |k| k.into_raw().unwrap());
        let mut writer = writer_builder.build(next_file_start_key.clone(), storage_name)?;
        let mut reschedule_checker =
            RescheduleChecker::new(tokio::task::yield_now, TASK_YIELD_DURATION);
        loop {
            if let Err(e) = scanner.scan_entries(&mut batch) {
                warn!("backup scan entries failed"; "err" => ?e, "ctx" => ?ctx);
                return Err(e.into());
            };
            if batch.is_empty() {
                break;
            }
            debug!("backup scan entries"; "len" => batch.len());

            let entries = batch.drain();
            if writer.need_split_keys() {
                let this_end_key = entries.as_slice().first().map_or_else(
                    || Err(Error::Other(box_err!("get entry error: nothing in batch"))),
                    |x| {
                        x.to_key().map(|k| k.into_raw().unwrap()).map_err(|e| {
                            error!(?e; "backup save file failed");
                            Error::Other(box_err!("Decode error: {:?}", e))
                        })
                    },
                )?;
                let this_start_key = next_file_start_key.clone();
                let msg = InMemBackupFiles {
                    files: KvWriter::Txn(writer),
                    start_key: this_start_key,
                    end_key: this_end_key.clone(),
                    start_version: begin_ts,
                    end_version: backup_ts,
                    region: self.region.clone(),
                    resource_limiter: resource_limiter.clone(),
                };
                send_to_worker_with_metrics(&saver, msg).await?;
                next_file_start_key = this_end_key;
                writer = writer_builder
                    .build(next_file_start_key.clone(), storage_name)
                    .map_err(|e| {
                        error_unknown!(?e; "backup writer failed");
                        e
                    })?;
            }

            // Build sst files.
            if let Err(e) = writer.write(entries, true) {
                warn!("backup build sst failed"; "err" => ?e, "ctx" => ?ctx);
                return Err(e);
            }
            if resource_limiter.is_some() {
                reschedule_checker.check().await;
            }
        }
        drop(snap_store);
        let stat = scanner.take_statistics();
        let take = start_scan.saturating_elapsed_secs();
        if take > 30.0 {
            warn!("backup scan takes long time.";
                "take(s)" => %take,
                "region" => ?self.region,
                "start_key" => %redact_option_key(&self.start_key),
                "end_key" => %redact_option_key(&self.end_key),
                "stat" => ?stat,
            );
        }
        BACKUP_RANGE_HISTOGRAM_VEC
            .with_label_values(&["scan"])
            .observe(take);

        let msg = InMemBackupFiles {
            files: KvWriter::Txn(writer),
            start_key: next_file_start_key,
            end_key: self
                .end_key
                .clone()
                .map_or_else(Vec::new, |k| k.into_raw().unwrap()),
            start_version: begin_ts,
            end_version: backup_ts,
            region: self.region.clone(),
            resource_limiter: resource_limiter.clone(),
        };
        send_to_worker_with_metrics(&saver, msg).await?;

        Ok(stat)
    }

    fn backup_raw<EK: KvEngine, S: Snapshot>(
        &self,
        writer: &mut BackupRawKvWriter<EK>,
        snapshot: &S,
    ) -> Result<Statistics> {
        assert!(self.codec.is_raw_kv);
        let start = Instant::now();
        let mut statistics = Statistics::default();
        let cfstatistics = statistics.mut_cf_statistics(self.cf);
        let mut cursor = CursorBuilder::new(snapshot, self.cf)
            .range(None, self.end_key.clone())
            .scan_mode(ScanMode::Forward)
            .fill_cache(false)
            .build()?;
        if let Some(begin) = self.start_key.clone() {
            if !cursor.seek(&begin, cfstatistics)? {
                return Ok(statistics);
            }
        } else if !cursor.seek_to_first(cfstatistics) {
            return Ok(statistics);
        }
        let mut batch = vec![];
        let current_ts = ttl_current_ts();
        loop {
            while cursor.valid()? && batch.len() < BACKUP_BATCH_LIMIT {
                let key = cursor.key(cfstatistics);
                let value = cursor.value(cfstatistics);
                let (is_valid, expired) = self.codec.is_valid_raw_value(key, value, current_ts)?;
                if is_valid {
                    batch.push(Ok((
                        self.codec
                            .convert_encoded_key_to_dst_version(key)?
                            .into_encoded(),
                        self.codec.convert_encoded_value_to_dst_version(value)?,
                    )));
                } else if expired {
                    cfstatistics.raw_value_tombstone += 1;
                };
                debug!("backup raw key";
                    "key" => &log_wrappers::Value::key(&self.codec.convert_encoded_key_to_dst_version(key)?.into_encoded()),
                    "value" => &log_wrappers::Value::value(&self.codec.convert_encoded_value_to_dst_version(value)?),
                    "valid" => is_valid,
                );
                cursor.next(cfstatistics);
            }
            if batch.is_empty() {
                break;
            }
            debug!("backup scan raw kv entries"; "len" => batch.len());
            // Build sst files.
            if let Err(e) = writer.write(batch.drain(..), true) {
                error_unknown!(?e; "backup raw kv build sst failed");
                return Err(e);
            }
        }
        BACKUP_RANGE_HISTOGRAM_VEC
            .with_label_values(&["raw_scan"])
            .observe(start.saturating_elapsed().as_secs_f64());
        Ok(statistics)
    }

    async fn backup_raw_kv_to_file<E: Engine>(
        &self,
        mut engine: E,
        db: E::Local,
        rate_limiter: &Limiter,
        file_name: String,
        cf: CfNameWrap,
        compression_type: Option<SstCompressionType>,
        compression_level: i32,
        cipher: CipherInfo,
        saver_tx: async_channel::Sender<InMemBackupFiles<E::Local>>,
    ) -> Result<Statistics> {
        let mut writer = match BackupRawKvWriter::new(
            db,
            &file_name,
            cf,
            rate_limiter.clone(),
            compression_type,
            compression_level,
            cipher,
            self.codec,
        ) {
            Ok(w) => w,
            Err(e) => {
                warn!("backup writer failed"; "err" => ?e, "region" => ?self.region);
                return Err(e);
            }
        };

        let mut ctx = Context::default();
        ctx.set_region_id(self.region.get_id());
        ctx.set_region_epoch(self.region.get_region_epoch().to_owned());
        ctx.set_peer(self.peer.clone());

        let snap_ctx = SnapContext {
            pb_ctx: &ctx,
            ..Default::default()
        };
        let engine_snapshot = match engine.snapshot(snap_ctx) {
            Ok(s) => s,
            Err(e) => {
                warn!("backup raw kv snapshot failed"; "err" => ?e, "ctx" => ?ctx);
                return Err(e.into());
            }
        };
        let backup_ret = if self.codec.use_raw_mvcc_snapshot() {
            self.backup_raw(
                &mut writer,
                &RawMvccSnapshot::from_snapshot(engine_snapshot),
            )
        } else {
            self.backup_raw(&mut writer, &engine_snapshot)
        };
        let stat = match backup_ret {
            Ok(s) => s,
            Err(e) => return Err(e),
        };
        let start_key = self.codec.decode_backup_key(self.start_key.clone())?;
        let end_key = self.codec.decode_backup_key(self.end_key.clone())?;
        let msg = InMemBackupFiles {
            files: KvWriter::Raw(writer),
            start_key,
            end_key,
            start_version: TimeStamp::zero(),
            end_version: TimeStamp::zero(),
            region: self.region.clone(),
            resource_limiter: None,
        };
        send_to_worker_with_metrics(&saver_tx, msg).await?;
        Ok(stat)
    }
}

#[derive(Clone)]
pub struct ConfigManager(Arc<RwLock<BackupConfig>>);

impl online_config::ConfigManager for ConfigManager {
    fn dispatch(&mut self, change: online_config::ConfigChange) -> online_config::Result<()> {
        self.0.write().unwrap().update(change)
    }
}

#[cfg(test)]
impl ConfigManager {
    fn set_num_threads(&self, num_threads: usize) {
        self.0.write().unwrap().num_threads = num_threads;
    }
}

/// SoftLimitKeeper can run in the background and adjust the number of threads
/// running based on CPU stats.
/// It only starts to work when enable_auto_tune is turned on in BackupConfig.
/// The initial number of threads is controlled by num_threads in BackupConfig.
#[derive(Clone)]
struct SoftLimitKeeper {
    limit: SoftLimit,
    config: ConfigManager,
}

impl SoftLimitKeeper {
    fn new(config: ConfigManager) -> Self {
        let BackupConfig { num_threads, .. } = *config.0.read().unwrap();
        let limit = SoftLimit::new(num_threads);
        BACKUP_SOFTLIMIT_GAUGE.set(limit.current_cap() as _);
        Self { limit, config }
    }

    /// run the soft limit keeper at background.
    async fn run(self) {
        let mut cpu_quota =
            SoftLimitByCpu::with_remain(self.config.0.read().unwrap().auto_tune_remain_threads);
        loop {
            if let Err(err) = self.on_tick(&mut cpu_quota).await {
                warn!("soft limit on_tick failed."; "err" => %err);
            }
            let auto_tune_refresh_interval =
                self.config.0.read().unwrap().auto_tune_refresh_interval;
            tokio::time::sleep(auto_tune_refresh_interval.0).await;
        }
    }

    async fn on_tick<S: CpuStatistics>(&self, cpu_quota: &mut SoftLimitByCpu<S>) -> Result<()> {
        let BackupConfig {
            enable_auto_tune,
            num_threads,
            auto_tune_remain_threads,
            ..
        } = *self.config.0.read().unwrap();
        cpu_quota.set_remain(auto_tune_remain_threads);

        let mut quota_val = num_threads;
        if enable_auto_tune {
            quota_val = cpu_quota
                .get_quota(|s| s.contains("bkwkr"))
                .clamp(1, num_threads);
        }

        self.limit.resize(quota_val).await.map_err(|err| {
            warn!(
                "error during applying the soft limit for backup.";
                "current_limit" => %self.limit.current_cap(),
                "to_set_value" => %quota_val,
                "err" => %err,
            );
            Error::Other(box_err!("failed to resize softlimit: {}", err))
        })?;

        BACKUP_SOFTLIMIT_GAUGE.set(self.limit.current_cap() as _);
        Ok(())
    }

    fn limit(&self) -> SoftLimit {
        self.limit.clone()
    }
}

/// The endpoint of backup.
///
/// It coordinates backup tasks and dispatches them to different workers.
pub struct Endpoint<E: Engine, R: RegionInfoProvider + Clone + 'static> {
    store_id: u64,
    pool: RefCell<ResizableRuntime>,
    io_pool: Runtime,
    tablets: LocalTablets<E::Local>,
    config_manager: ConfigManager,
    concurrency_manager: ConcurrencyManager,
    soft_limit_keeper: SoftLimitKeeper,
    api_version: ApiVersion,
    causal_ts_provider: Option<Arc<CausalTsProviderImpl>>, // used in rawkv apiv2 only
    resource_ctl: Option<Arc<ResourceGroupManager>>,

    pub(crate) engine: E,
    pub(crate) region_info: R,
}

/// The progress of a backup task
pub struct Progress<R: RegionInfoProvider> {
    store_id: u64,
    ranges: Vec<(Option<Key>, Option<Key>)>,
    next_index: usize,
    next_start: Option<Key>,
    end_key: Option<Key>,
    region_info: R,
    finished: bool,
    codec: KeyValueCodec,
    cf: CfName,
}

impl<R: RegionInfoProvider> Progress<R> {
    fn new_with_range(
        store_id: u64,
        next_start: Option<Key>,
        end_key: Option<Key>,
        region_info: R,
        codec: KeyValueCodec,
        cf: CfName,
    ) -> Self {
        let ranges = vec![(next_start, end_key)];
        Self::new_with_ranges(store_id, ranges, region_info, codec, cf)
    }

    fn new_with_ranges(
        store_id: u64,
        ranges: Vec<(Option<Key>, Option<Key>)>,
        region_info: R,
        codec: KeyValueCodec,
        cf: CfName,
    ) -> Self {
        let mut prs = Progress {
            store_id,
            ranges,
            next_index: 0,
            next_start: None,
            end_key: None,
            region_info,
            finished: false,
            codec,
            cf,
        };
        prs.try_next();
        prs
    }

    /// try the next range. If all the ranges are consumed,
    /// set self.finish true.
    fn try_next(&mut self) {
        if self.ranges.len() > self.next_index {
            (self.next_start, self.end_key) = self.ranges[self.next_index].clone();

            self.next_index += 1;
        } else {
            self.finished = true;
        }
    }

    /// Forward the progress by `ranges` BackupRanges
    ///
    /// The size of the returned BackupRanges should <= `ranges`
    ///
    /// Notice: Returning an empty BackupRanges means that no leader region
    /// corresponding to the current range is sought. The caller should
    /// call `forward` again to seek regions for the next range.
    fn forward(&mut self, limit: usize, replica_read: bool) -> Option<Vec<BackupRange>> {
        if self.finished {
            return None;
        }
        let store_id = self.store_id;
        let (tx, rx) = mpsc::channel();
        let start_key_ = self
            .next_start
            .clone()
            .map_or_else(Vec::new, |k| k.into_encoded());

        let start_key = self.next_start.clone();
        let end_key = self.end_key.clone();
        let codec = self.codec;
        let cf_name = self.cf;
        let res = self.region_info.seek_region(
            &start_key_,
            Box::new(move |iter| {
                let mut count = 0;
                for info in iter {
                    let region = &info.region;
                    if end_key.is_some() {
                        let end_slice = end_key.as_ref().unwrap().as_encoded().as_slice();
                        if end_slice <= region.get_start_key() {
                            // We have reached the end.
                            // The range is defined as [start, end) so break if
                            // region start key is greater or equal to end key.
                            break;
                        }
                    }
                    let peer = if let Some(peer) = find_peer(region, store_id) {
                        peer.to_owned()
                    } else {
                        // skip the region at this time, and would retry to backup the region in
                        // finegrained step.
                        continue;
                    };
                    // Raft peer role has to match the replica read flag.
                    if replica_read || info.role == StateRole::Leader {
                        let ekey = get_min_end_key(end_key.as_ref(), region);
                        let skey = get_max_start_key(start_key.as_ref(), region);
                        assert!(!(skey == ekey && ekey.is_some()), "{:?} {:?}", skey, ekey);
                        let backup_range = BackupRange {
                            start_key: skey,
                            end_key: ekey,
                            region: region.clone(),
                            peer,
                            codec,
                            cf: cf_name,
                            uses_replica_read: info.role != StateRole::Leader,
                        };
                        tx.send(backup_range).unwrap();
                        count += 1;
                        if count >= limit {
                            break;
                        }
                    }
                }
            }),
        );
        if let Err(e) = res {
            error!(?e; "backup seek region failed");
        }

        let branges: Vec<_> = rx.iter().collect();
        if let Some(b) = branges.last() {
            // The region's end key is empty means it is the last
            // region, we need to set the `finished` flag here in case
            // we run with `next_start` set to None
            if b.region.get_end_key().is_empty() || b.end_key == self.end_key {
                self.try_next();
            } else {
                self.next_start = b.end_key.clone();
            }
        } else {
            self.try_next();
        }
        Some(branges)
    }
}

impl<E: Engine, R: RegionInfoProvider + Clone + 'static> Endpoint<E, R> {
    pub fn new(
        store_id: u64,
        engine: E,
        region_info: R,
        tablets: LocalTablets<E::Local>,
        config: BackupConfig,
        concurrency_manager: ConcurrencyManager,
        api_version: ApiVersion,
        causal_ts_provider: Option<Arc<CausalTsProviderImpl>>,
        resource_ctl: Option<Arc<ResourceGroupManager>>,
    ) -> Endpoint<E, R> {
        let pool = ResizableRuntime::new(
            config.num_threads,
            "bkwkr",
            Box::new(utils::create_tokio_runtime),
            Box::new(|new_size| BACKUP_THREAD_POOL_SIZE_GAUGE.set(new_size as i64)),
        );
        let rt = utils::create_tokio_runtime(config.io_thread_size, "backup-io").unwrap();
        let config_manager = ConfigManager(Arc::new(RwLock::new(config)));
        let soft_limit_keeper = SoftLimitKeeper::new(config_manager.clone());
        rt.spawn(soft_limit_keeper.clone().run());
        Endpoint {
            store_id,
            engine,
            region_info,
            pool: RefCell::new(pool),
            tablets,
            io_pool: rt,
            soft_limit_keeper,
            config_manager,
            concurrency_manager,
            api_version,
            causal_ts_provider,
            resource_ctl,
        }
    }

    pub fn get_config_manager(&self) -> ConfigManager {
        self.config_manager.clone()
    }

    fn get_config(&self) -> BackendConfig {
        BackendConfig {
            s3_multi_part_size: self.config_manager.0.read().unwrap().s3_multi_part_size.0 as usize,
            hdfs_config: HdfsConfig {
                hadoop_home: self.config_manager.0.read().unwrap().hadoop.home.clone(),
                linux_user: self
                    .config_manager
                    .0
                    .read()
                    .unwrap()
                    .hadoop
                    .linux_user
                    .clone(),
            },
        }
    }

    fn spawn_backup_worker(
        &self,
        prs: Arc<Mutex<Progress<R>>>,
        request: Request,
        saver_tx: async_channel::Sender<InMemBackupFiles<E::Local>>,
        resp_tx: UnboundedSender<BackupResponse>,
        _backend: Arc<dyn ExternalStorage>,
    ) {
        let start_ts = request.start_ts;
        let backup_ts = request.end_ts;
        let engine = self.engine.clone();
        let tablets = self.tablets.clone();
        let store_id = self.store_id;
        let concurrency_manager = self.concurrency_manager.clone();
        let batch_size = self.config_manager.0.read().unwrap().batch_size;
        let sst_max_size = self.config_manager.0.read().unwrap().sst_max_size.0;
        let soft_limit_keeper = self.soft_limit_keeper.limit();
        let resource_limiter = self.resource_ctl.as_ref().and_then(|r| {
            r.get_background_resource_limiter(&request.resource_group_name, &request.source_tag)
        });

        self.pool.borrow_mut().spawn(async move {
            // Migrated to 2021 migration. This let statement is probably not needed, see
            //   https://doc.rust-lang.org/edition-guide/rust-2021/disjoint-capture-in-closures.html
            let _ = &request;
            loop {
                // when get the guard, release it until we finish scanning a batch,
                // because if we were suspended during scanning,
                // the region info have higher possibility to change (then we must compensate
                // that by the fine-grained backup).
                let guard = soft_limit_keeper.guard().await;
                if let Err(e) = guard {
                    warn!("failed to retrieve limit guard, omitting."; "err" => %e);
                };
                let (batch, is_raw_kv, cf) = {
                    // Release lock as soon as possible.
                    // It is critical to speed up backup, otherwise workers are
                    // blocked by each other.
                    //
                    // If we use [tokio::sync::Mutex] here, until we give back the control flow to
                    // the scheduler or tasks waiting for the lock won't be
                    // waked up due to the characteristic of the runtime...
                    //
                    // The worst case is when using `noop` backend:
                    // the task seems never yielding and in fact the backup would executing
                    // sequentially.
                    //
                    // Anyway, even tokio itself doesn't recommend to use it unless the lock guard
                    // needs to be `Send`. (See https://tokio.rs/tokio/tutorial/shared-state)
                    // Use &mut and mark the type for making rust-analyzer happy.
                    let progress: &mut Progress<_> = &mut prs.lock().unwrap();
                    match progress.forward(batch_size, request.replica_read) {
                        Some(batch) => (batch, progress.codec.is_raw_kv, progress.cf),
                        None => return,
                    }
                };

                for brange in batch {
                    // wake up the scheduler for each loop for awaking tasks waiting for some lock
                    // or channels. because the softlimit permit is held by
                    // current task, there isn't risk of being suspended for long time.
                    tokio::task::yield_now().await;
                    let engine = engine.clone();
                    if request.cancel.load(Ordering::SeqCst) {
                        warn!("backup task has canceled"; "range" => ?brange);
                        return;
                    }
                    // TODO: make file_name unique and short
                    let key = brange.start_key.clone().and_then(|k| {
                        // use start_key sha256 instead of start_key to avoid file name too long os
                        // error
                        let input = brange.codec.decode_backup_key(Some(k)).unwrap_or_default();
                        file_system::sha256(&input).ok().map(hex::encode)
                    });
                    let name = backup_file_name(store_id, &brange.region, key, _backend.name());
                    let ct = to_sst_compression_type(request.compression_type);
                    let db = match tablets.get(brange.region.id) {
                        Some(t) => t,
                        None => {
                            warn!("backup region not found"; "region" => ?brange.region.id);
                            return;
                        }
                    };

                    let stat = if is_raw_kv {
                        brange
                            .backup_raw_kv_to_file(
                                engine,
                                db.into_owned(),
                                &request.rate_limiter,
                                name,
                                cf.into(),
                                ct,
                                request.compression_level,
                                request.cipher.clone(),
                                saver_tx.clone(),
                            )
                            .await
                    } else {
                        let writer_builder = BackupWriterBuilder::new(
                            store_id,
                            request.rate_limiter.clone(),
                            brange.region.clone(),
                            db.into_owned(),
                            ct,
                            request.compression_level,
                            sst_max_size,
                            request.cipher.clone(),
                        );
                        with_resource_limiter(
                            brange.backup(
                                writer_builder,
                                engine,
                                concurrency_manager.clone(),
                                backup_ts,
                                start_ts,
                                request.bypass_locks.clone(),
                                request.access_locks.clone(),
                                saver_tx.clone(),
                                _backend.name(),
                                resource_limiter.clone(),
                            ),
                            resource_limiter.clone(),
                        )
                        .await
                    };
                    match stat {
                        Err(err) => {
                            warn!("error during backup"; "region" => ?brange.region, "err" => %err);
                            let mut resp = BackupResponse::new();
                            resp.set_error(err.into());
                            if let Err(err) = resp_tx.unbounded_send(resp) {
                                warn!("failed to send response"; "err" => ?err)
                            }
                        }
                        Ok(stat) => {
                            BACKUP_RAW_EXPIRED_COUNT.inc_by(stat.data.raw_value_tombstone as u64);
                            // TODO: maybe add the stat to metrics?
                            debug!("backup region finish";
                            "region" => ?brange.region,
                            "details" => ?stat);
                        }
                    }
                }
            }
        });
    }

    fn get_progress_by_req(
        &self,
        request: &Request,
        codec: KeyValueCodec,
    ) -> Arc<Mutex<Progress<R>>> {
        if request.sub_ranges.is_empty() {
            let start_key = codec.encode_backup_key(request.start_key.clone());
            let end_key = codec.encode_backup_key(request.end_key.clone());
            Arc::new(Mutex::new(Progress::new_with_range(
                self.store_id,
                start_key,
                end_key,
                self.region_info.clone(),
                codec,
                request.cf,
            )))
        } else {
            let mut ranges = Vec::with_capacity(request.sub_ranges.len());
            for k in &request.sub_ranges {
                let start_key = codec.encode_backup_key(k.start_key.clone());
                let end_key = codec.encode_backup_key(k.end_key.clone());
                ranges.push((start_key, end_key));
            }
            Arc::new(Mutex::new(Progress::new_with_ranges(
                self.store_id,
                ranges,
                self.region_info.clone(),
                codec,
                request.cf,
            )))
        }
    }

    pub fn handle_backup_task(&self, task: Task) {
        let Task { request, resp } = task;
        let codec = KeyValueCodec::new(request.is_raw_kv, self.api_version, request.dst_api_ver);
        if !codec.check_backup_api_version(&request.start_key, &request.end_key) {
            let mut response = BackupResponse::default();
            let err_msg = format!(
                "invalid backup version, cur: {:?}, dst: {:?}",
                self.api_version, request.dst_api_ver
            );
            response.set_error(crate::Error::Other(box_err!(err_msg)).into());
            if let Err(err) = resp.unbounded_send(response) {
                error_unknown!(?err; "backup failed to send response");
            }
            return;
        }
        // Flush causal timestamp to make sure that future writes will have larger
        // timestamps. And help TiKV-BR acquire a backup-ts with intact data
        // smaller than it. (Note that intactness is not fully ensured now,
        // until the safe-ts of RawKV is implemented. TiKV-BR need a workaround
        // by rewinding backup-ts to a small "safe interval").
        if request.is_raw_kv {
            if let Err(e) = self
                .causal_ts_provider
                .as_ref()
                .map_or(Ok(TimeStamp::new(0)), |provider| {
                    block_on(provider.async_flush())
                })
            {
                error!("backup flush causal timestamp failed"; "err" => ?e);
                let mut response = BackupResponse::default();
                let err_msg = format!("fail to flush causal ts, {:?}", e);
                response.set_error(crate::Error::Other(box_err!(err_msg)).into());
                if let Err(err) = resp.unbounded_send(response) {
                    error_unknown!(?err; "backup failed to send response");
                }
                return;
            }
        }

        let prs = self.get_progress_by_req(&request, codec);

        let backend = match create_storage(&request.backend, self.get_config()) {
            Ok(backend) => backend,
            Err(err) => {
                error_unknown!(?err; "backup create storage failed");
                let mut response = BackupResponse::default();
                response.set_error(crate::Error::Io(err).into());
                if let Err(err) = resp.unbounded_send(response) {
                    error_unknown!(?err; "backup failed to send response");
                }
                return;
            }
        };
        let backend = Arc::<dyn ExternalStorage>::from(backend);
        let num_threads = self.config_manager.0.read().unwrap().num_threads;
        self.pool.borrow_mut().adjust_with(num_threads);
        let (tx, rx) = async_channel::bounded(1);
        for _ in 0..num_threads {
            self.spawn_backup_worker(
                prs.clone(),
                request.clone(),
                tx.clone(),
                resp.clone(),
                backend.clone(),
            );
            self.io_pool.spawn(save_backup_file_worker(
                rx.clone(),
                resp.clone(),
                backend.clone(),
                codec,
            ));
        }
    }

    /// Get the internal handle of the io thread pool used by the backup
    /// endpoint. This is mainly shared for disk snapshot backup (so they
    /// don't need to spawn on the gRPC pool.)
    pub fn io_pool_handle(&self) -> &Handle {
        self.io_pool.handle()
    }
}

impl<E: Engine, R: RegionInfoProvider + Clone + 'static> Runnable for Endpoint<E, R> {
    type Task = Task;

    fn run(&mut self, task: Task) {
        if task.has_canceled() {
            warn!("backup task has canceled"; "task" => %task);
            return;
        }
        info!("run backup task"; "task" => %task);
        self.handle_backup_task(task);
    }
}

/// Get the min end key from the given `end_key` and `Region`'s end key.
fn get_min_end_key(end_key: Option<&Key>, region: &Region) -> Option<Key> {
    let region_end = if region.get_end_key().is_empty() {
        None
    } else {
        Some(Key::from_encoded_slice(region.get_end_key()))
    };
    if region.get_end_key().is_empty() {
        end_key.cloned()
    } else if end_key.is_none() {
        region_end
    } else {
        let end_slice = end_key.as_ref().unwrap().as_encoded().as_slice();
        if end_slice < region.get_end_key() {
            end_key.cloned()
        } else {
            region_end
        }
    }
}

/// Get the max start key from the given `start_key` and `Region`'s start key.
fn get_max_start_key(start_key: Option<&Key>, region: &Region) -> Option<Key> {
    let region_start = if region.get_start_key().is_empty() {
        None
    } else {
        Some(Key::from_encoded_slice(region.get_start_key()))
    };
    if start_key.is_none() {
        region_start
    } else {
        let start_slice = start_key.as_ref().unwrap().as_encoded().as_slice();
        if start_slice < region.get_start_key() {
            region_start
        } else {
            start_key.cloned()
        }
    }
}

/// Construct an backup file name based on the given store id, region, range
/// start key and local unix timestamp. A name consists with five parts: store
/// id, region_id, a epoch version, the hash of range start key and timestamp.
/// range start key is used to keep the unique file name for file, to handle
/// different tables exists on the same region. local unix timestamp is used to
/// keep the unique file name for file, to handle receive the same request after
/// connection reset.
pub fn backup_file_name(
    store_id: u64,
    region: &Region,
    key: Option<String>,
    storage_name: &str,
) -> String {
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");

    match (key, storage_name) {
        // See https://github.com/pingcap/tidb/issues/30087
        // To avoid 503 Slow Down error, if the backup storage is s3,
        // organize the backup files by store_id (use slash (/) as delimiter).
        (Some(k), aws::STORAGE_NAME | external_storage::local::STORAGE_NAME) => {
            format!(
                "{}/{}_{}_{}_{}",
                store_id,
                region.get_id(),
                region.get_region_epoch().get_version(),
                k,
                since_the_epoch.as_millis()
            )
        }
        (Some(k), _) => {
            format!(
                "{}_{}_{}_{}_{}",
                store_id,
                region.get_id(),
                region.get_region_epoch().get_version(),
                k,
                since_the_epoch.as_millis()
            )
        }

        (None, aws::STORAGE_NAME | external_storage::local::STORAGE_NAME) => {
            format!(
                "{}/{}_{}",
                store_id,
                region.get_id(),
                region.get_region_epoch().get_version()
            )
        }
        (None, _) => {
            format!(
                "{}_{}_{}",
                store_id,
                region.get_id(),
                region.get_region_epoch().get_version()
            )
        }
    }
}

// convert BackupCompresionType to rocks db DBCompressionType
fn to_sst_compression_type(ct: CompressionType) -> Option<SstCompressionType> {
    match ct {
        CompressionType::Lz4 => Some(SstCompressionType::Lz4),
        CompressionType::Snappy => Some(SstCompressionType::Snappy),
        CompressionType::Zstd => Some(SstCompressionType::Zstd),
        CompressionType::Unknown => None,
    }
}

/// warp a Option<Key> to the redact wrapper.
fn redact_option_key(key: &Option<Key>) -> log_wrappers::Value<'_> {
    match key {
        None => log_wrappers::Value::key(b""),
        Some(key) => log_wrappers::Value::key(key.as_encoded().as_slice()),
    }
}

#[cfg(test)]
pub mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        sync::{Mutex, RwLock},
        time::Duration,
    };

    use api_version::{KvFormat, RawValue, api_v2::RAW_KEY_PREFIX, dispatch_api_version};
    use collections::HashSet;
    use engine_rocks::RocksSstReader;
    use engine_traits::{IterOptions, Iterator, MiscExt, RefIterable, SstReader};
    use external_storage::{make_local_backend, make_noop_backend};
    use file_system::{IoOp, IoRateLimiter, IoType};
    use futures::{executor::block_on, stream::StreamExt};
    use keys::data_key;
    use kvproto::{kvrpcpb::Context, metapb};
    use raftstore::coprocessor::{RegionCollector, Result as CopResult, SeekRegionCallback};
    use rand::Rng;
    use tempfile::TempDir;
    use tikv::{
        coprocessor::checksum_crc64_xor,
        storage::{
            RocksEngine, TestEngineBuilder,
            kv::LocalTablets,
            txn::tests::{must_commit, must_prewrite_put},
        },
    };
    use tikv_util::{config::ReadableSize, info, store::new_peer};
    use tokio::time;
    use txn_types::SHORT_VALUE_MAX_LEN;

    use super::*;

    #[derive(Clone)]
    pub struct MockRegionInfoProvider {
        regions: Arc<Mutex<RegionCollector>>,
        cancel: Option<Arc<AtomicBool>>,
        need_encode_key: bool,
    }

    impl MockRegionInfoProvider {
        pub fn new(encode_key: bool) -> Self {
            MockRegionInfoProvider {
                regions: Arc::new(Mutex::new(RegionCollector::new(
                    Arc::new(RwLock::new(HashSet::default())),
                    Box::new(|| 0),
                ))),
                cancel: None,
                need_encode_key: encode_key,
            }
        }
        pub fn set_regions(&self, regions: Vec<(Vec<u8>, Vec<u8>, u64)>) {
            let mut map = self.regions.lock().unwrap();
            for (mut start_key, mut end_key, id) in regions {
                if !start_key.is_empty() {
                    if self.need_encode_key {
                        start_key = Key::from_raw(&start_key).into_encoded();
                    } else {
                        start_key = Key::from_encoded(start_key).into_encoded();
                    }
                }
                if !end_key.is_empty() {
                    if self.need_encode_key {
                        end_key = Key::from_raw(&end_key).into_encoded();
                    } else {
                        end_key = Key::from_encoded(end_key).into_encoded();
                    }
                }
                let mut r = metapb::Region::default();
                r.set_id(id);
                r.set_start_key(start_key.clone());
                r.set_end_key(end_key);
                r.mut_peers().push(new_peer(1, 1));
                map.create_region(r, StateRole::Leader);
            }
        }
        pub fn add_region(
            &self,
            id: u64,
            mut start_key: Vec<u8>,
            mut end_key: Vec<u8>,
            peer_role: metapb::PeerRole,
            state_role: StateRole,
        ) {
            let mut region = metapb::Region::default();
            region.set_id(id);
            if !start_key.is_empty() {
                if self.need_encode_key {
                    start_key = Key::from_raw(&start_key).into_encoded();
                } else {
                    start_key = Key::from_encoded(start_key).into_encoded();
                }
            }
            if !end_key.is_empty() {
                if self.need_encode_key {
                    end_key = Key::from_raw(&end_key).into_encoded();
                } else {
                    end_key = Key::from_encoded(end_key).into_encoded();
                }
            }
            region.set_start_key(start_key);
            region.set_end_key(end_key);
            let mut new_peer = new_peer(1, 1);
            new_peer.set_role(peer_role);
            region.mut_peers().push(new_peer);
            let mut map = self.regions.lock().unwrap();
            map.create_region(region, state_role);
        }
        fn canecl_on_seek(&mut self, cancel: Arc<AtomicBool>) {
            self.cancel = Some(cancel);
        }
    }

    impl RegionInfoProvider for MockRegionInfoProvider {
        fn seek_region(&self, from: &[u8], callback: SeekRegionCallback) -> CopResult<()> {
            let from = from.to_vec();
            let regions = self.regions.lock().unwrap();
            if let Some(c) = self.cancel.as_ref() {
                c.store(true, Ordering::SeqCst);
            }
            regions.handle_seek_region(from, callback);
            Ok(())
        }
    }

    pub fn new_endpoint() -> (TempDir, Endpoint<RocksEngine, MockRegionInfoProvider>) {
        new_endpoint_with_limiter(None, ApiVersion::V1, false, None)
    }

    pub fn new_endpoint_with_limiter(
        limiter: Option<Arc<IoRateLimiter>>,
        api_version: ApiVersion,
        is_raw_kv: bool,
        causal_ts_provider: Option<Arc<CausalTsProviderImpl>>,
    ) -> (TempDir, Endpoint<RocksEngine, MockRegionInfoProvider>) {
        let temp = TempDir::new().unwrap();
        let rocks = TestEngineBuilder::new()
            .path(temp.path())
            .cfs([
                engine_traits::CF_DEFAULT,
                engine_traits::CF_LOCK,
                engine_traits::CF_WRITE,
            ])
            .io_rate_limiter(limiter)
            .api_version(api_version)
            .build()
            .unwrap();
        let concurrency_manager = ConcurrencyManager::new_for_test(1.into());
        let need_encode_key = !is_raw_kv || api_version == ApiVersion::V2;
        let db = rocks.get_rocksdb();
        (
            temp,
            Endpoint::new(
                1,
                rocks,
                MockRegionInfoProvider::new(need_encode_key),
                LocalTablets::Singleton(db),
                BackupConfig {
                    num_threads: 4,
                    batch_size: 8,
                    sst_max_size: ReadableSize::mb(144),
                    ..Default::default()
                },
                concurrency_manager,
                api_version,
                causal_ts_provider,
                None,
            ),
        )
    }

    pub fn check_response<F>(rx: UnboundedReceiver<BackupResponse>, check: F)
    where
        F: FnOnce(Option<BackupResponse>),
    {
        let rx = rx.fuse();
        let (resp, rx) = block_on(rx.into_future());
        check(resp);
        let (none, _rx) = block_on(rx.into_future());
        assert!(none.is_none(), "{:?}", none);
    }

    fn make_unique_dir(path: &Path) -> PathBuf {
        let uid: u64 = rand::thread_rng().gen();
        let tmp_suffix = format!("{:016x}", uid);
        let unique = path.join(tmp_suffix);
        fs::create_dir_all(&unique).unwrap();
        unique
    }

    #[test]
    fn test_control_thread_pool_adjust_keep_tasks() {
        use std::thread::sleep;

        let counter = Arc::new(AtomicU32::new(0));
        let mut pool = ResizableRuntime::new(
            3,
            "bkwkr",
            Box::new(utils::create_tokio_runtime),
            Box::new(|new_size: usize| BACKUP_THREAD_POOL_SIZE_GAUGE.set(new_size as i64)),
        );

        for i in 0..8 {
            let ctr = counter.clone();
            pool.spawn(async move {
                time::sleep(Duration::from_millis(100)).await;
                ctr.fetch_or(1 << i, Ordering::SeqCst);
            });
        }

        sleep(Duration::from_millis(150));
        pool.adjust_with(4);

        for i in 8..16 {
            let ctr = counter.clone();
            pool.spawn(async move {
                time::sleep(Duration::from_millis(100)).await;
                ctr.fetch_or(1 << i, Ordering::SeqCst);
            });
        }

        sleep(Duration::from_millis(250));
        assert_eq!(counter.load(Ordering::SeqCst), 0xffff);
    }

    #[test]
    fn test_s3_config() {
        let (_tmp, endpoint) = new_endpoint();
        assert_eq!(
            endpoint.config_manager.0.read().unwrap().s3_multi_part_size,
            ReadableSize::mb(5)
        );
    }

    #[test]
    fn test_seek_range() {
        let (_tmp, endpoint) = new_endpoint();

        endpoint.region_info.set_regions(vec![
            (b"".to_vec(), b"1".to_vec(), 1),
            (b"1".to_vec(), b"2".to_vec(), 2),
            (b"3".to_vec(), b"4".to_vec(), 3),
            (b"7".to_vec(), b"9".to_vec(), 4),
            (b"9".to_vec(), b"".to_vec(), 5),
        ]);
        // Test seek backup range.
        let test_seek_backup_range =
            |start_key: &[u8], end_key: &[u8], expect: Vec<(&[u8], &[u8])>| {
                let start_key = (!start_key.is_empty()).then_some(Key::from_raw(start_key));
                let end_key = (!end_key.is_empty()).then_some(Key::from_raw(end_key));
                let mut prs = Progress::new_with_range(
                    endpoint.store_id,
                    start_key,
                    end_key,
                    endpoint.region_info.clone(),
                    KeyValueCodec::new(false, ApiVersion::V1, ApiVersion::V1),
                    engine_traits::CF_DEFAULT,
                );

                let mut ranges = Vec::with_capacity(expect.len());
                while ranges.len() != expect.len() {
                    let n = (rand::random::<usize>() % 3) + 1;
                    let mut r = prs.forward(n, false).unwrap();
                    // The returned backup ranges should <= n
                    assert!(r.len() <= n);

                    if r.is_empty() {
                        // if return a empty vec then the progress is finished
                        assert_eq!(
                            ranges.len(),
                            expect.len(),
                            "got {:?}, expect {:?}",
                            ranges,
                            expect
                        );
                    }
                    ranges.append(&mut r);
                }

                for (a, b) in ranges.into_iter().zip(expect) {
                    assert_eq!(
                        a.start_key.map_or_else(Vec::new, |k| k.into_raw().unwrap()),
                        b.0
                    );
                    assert_eq!(
                        a.end_key.map_or_else(Vec::new, |k| k.into_raw().unwrap()),
                        b.1
                    );
                }
            };

        // Test whether responses contain correct range.
        #[allow(clippy::blocks_in_conditions)]
        let test_handle_backup_task_range =
            |start_key: &[u8], end_key: &[u8], expect: Vec<(&[u8], &[u8])>| {
                let tmp = TempDir::new().unwrap();
                let backend = make_local_backend(tmp.path());
                let (tx, rx) = unbounded();
                let task = Task {
                    request: Request {
                        start_key: start_key.to_vec(),
                        end_key: end_key.to_vec(),
                        sub_ranges: Vec::new(),
                        start_ts: 1.into(),
                        end_ts: 1.into(),
                        backend,
                        rate_limiter: Limiter::new(f64::INFINITY),
                        cancel: Arc::default(),
                        is_raw_kv: false,
                        dst_api_ver: ApiVersion::V1,
                        cf: engine_traits::CF_DEFAULT,
                        compression_type: CompressionType::Unknown,
                        compression_level: 0,
                        cipher: CipherInfo::default(),
                        replica_read: false,
                        resource_group_name: "".into(),
                        source_tag: "br".into(),
                        bypass_locks: vec![],
                        access_locks: vec![],
                    },
                    resp: tx,
                };
                endpoint.handle_backup_task(task);
                let resps: Vec<_> = block_on(rx.collect());
                for a in &resps {
                    assert!(
                        expect
                            .iter()
                            .any(|b| { a.get_start_key() == b.0 && a.get_end_key() == b.1 }),
                        "{:?} {:?}",
                        resps,
                        expect
                    );
                }
                assert_eq!(resps.len(), expect.len(), "{:?} {:?}", start_key, end_key);
            };

        // Backup range from case.0 to case.1,
        // the case.2 is the expected results.
        type Case<'a> = (&'a [u8], &'a [u8], Vec<(&'a [u8], &'a [u8])>);

        let case: Vec<Case<'_>> = vec![
            (b"", b"1", vec![(b"", b"1")]),
            (b"", b"2", vec![(b"", b"1"), (b"1", b"2")]),
            (b"1", b"2", vec![(b"1", b"2")]),
            (b"1", b"3", vec![(b"1", b"2")]),
            (b"1", b"4", vec![(b"1", b"2"), (b"3", b"4")]),
            (b"4", b"6", vec![]),
            (b"4", b"5", vec![]),
            (b"2", b"7", vec![(b"3", b"4")]),
            (b"7", b"8", vec![(b"7", b"8")]),
            (b"3", b"", vec![(b"3", b"4"), (b"7", b"9"), (b"9", b"")]),
            (b"5", b"", vec![(b"7", b"9"), (b"9", b"")]),
            (b"7", b"", vec![(b"7", b"9"), (b"9", b"")]),
            (b"8", b"91", vec![(b"8", b"9"), (b"9", b"91")]),
            (b"8", b"", vec![(b"8", b"9"), (b"9", b"")]),
            (
                b"",
                b"",
                vec![
                    (b"", b"1"),
                    (b"1", b"2"),
                    (b"3", b"4"),
                    (b"7", b"9"),
                    (b"9", b""),
                ],
            ),
        ];
        for (start_key, end_key, ranges) in case {
            test_seek_backup_range(start_key, end_key, ranges.clone());
            test_handle_backup_task_range(start_key, end_key, ranges);
        }
    }

    #[test]
    fn test_backup_replica_read() {
        let (_tmp, endpoint) = new_endpoint();

        endpoint.region_info.add_region(
            1,
            b"".to_vec(),
            b"1".to_vec(),
            metapb::PeerRole::Voter,
            StateRole::Leader,
        );
        endpoint.region_info.add_region(
            2,
            b"1".to_vec(),
            b"2".to_vec(),
            metapb::PeerRole::Voter,
            StateRole::Follower,
        );
        endpoint.region_info.add_region(
            3,
            b"2".to_vec(),
            b"3".to_vec(),
            metapb::PeerRole::Learner,
            StateRole::Follower,
        );

        let tmp = TempDir::new().unwrap();
        let backend = make_local_backend(tmp.path());

        let (tx, rx) = unbounded();
        let mut ranges = vec![];
        let key_range = KeyRange {
            start_key: b"".to_vec(),
            end_key: b"3".to_vec(),
            ..Default::default()
        };
        ranges.push(key_range);
        let read_leader_task = Task {
            request: Request {
                start_key: b"1".to_vec(),
                end_key: b"2".to_vec(),
                sub_ranges: ranges.clone(),
                start_ts: 1.into(),
                end_ts: 1.into(),
                backend: backend.clone(),
                rate_limiter: Limiter::new(f64::INFINITY),
                cancel: Arc::default(),
                is_raw_kv: false,
                dst_api_ver: ApiVersion::V1,
                cf: engine_traits::CF_DEFAULT,
                compression_type: CompressionType::Unknown,
                compression_level: 0,
                cipher: CipherInfo::default(),
                replica_read: false,
                resource_group_name: "".into(),
                source_tag: "br".into(),
                bypass_locks: vec![],
                access_locks: vec![],
            },
            resp: tx,
        };
        endpoint.handle_backup_task(read_leader_task);
        let resps: Vec<_> = block_on(rx.collect());
        assert_eq!(resps.len(), 1);
        for a in &resps {
            assert_eq!(a.get_start_key(), b"");
            assert_eq!(a.get_end_key(), b"1");
        }

        let (tx, rx) = unbounded();
        let replica_read_task = Task {
            request: Request {
                start_key: b"".to_vec(),
                end_key: b"3".to_vec(),
                sub_ranges: ranges.clone(),
                start_ts: 1.into(),
                end_ts: 1.into(),
                backend,
                rate_limiter: Limiter::new(f64::INFINITY),
                cancel: Arc::default(),
                is_raw_kv: false,
                dst_api_ver: ApiVersion::V1,
                cf: engine_traits::CF_DEFAULT,
                compression_type: CompressionType::Unknown,
                compression_level: 0,
                cipher: CipherInfo::default(),
                replica_read: true,
                resource_group_name: "".into(),
                source_tag: "br".into(),
                bypass_locks: vec![],
                access_locks: vec![],
            },
            resp: tx,
        };
        endpoint.handle_backup_task(replica_read_task);
        let resps: Vec<_> = block_on(rx.collect());
        let expected: Vec<(&[u8], &[u8])> = vec![(b"", b"1"), (b"1", b"2"), (b"2", b"3")];
        assert_eq!(resps.len(), 3);
        for a in &resps {
            assert!(
                expected
                    .iter()
                    .any(|b| { a.get_start_key() == b.0 && a.get_end_key() == b.1 }),
                "{:?} {:?}",
                resps,
                expected
            );
        }
    }

    #[test]
    fn test_seek_ranges() {
        let (_tmp, endpoint) = new_endpoint();

        endpoint.region_info.set_regions(vec![
            (b"".to_vec(), b"1".to_vec(), 1),
            (b"1".to_vec(), b"2".to_vec(), 2),
            (b"3".to_vec(), b"4".to_vec(), 3),
            (b"7".to_vec(), b"9".to_vec(), 4),
            (b"9".to_vec(), b"".to_vec(), 5),
        ]);
        // Test seek backup range.
        let test_seek_backup_ranges =
            |sub_ranges: Vec<(&[u8], &[u8])>, expect: Vec<(&[u8], &[u8])>| {
                let mut ranges = Vec::with_capacity(sub_ranges.len());
                for &(start_key, end_key) in &sub_ranges {
                    let start_key = (!start_key.is_empty()).then_some(Key::from_raw(start_key));
                    let end_key = (!end_key.is_empty()).then_some(Key::from_raw(end_key));
                    ranges.push((start_key, end_key));
                }
                let mut prs = Progress::new_with_ranges(
                    endpoint.store_id,
                    ranges,
                    endpoint.region_info.clone(),
                    KeyValueCodec::new(false, ApiVersion::V1, ApiVersion::V1),
                    engine_traits::CF_DEFAULT,
                );

                let mut ranges = Vec::with_capacity(expect.len());
                loop {
                    let n = (rand::random::<usize>() % 3) + 1;
                    let mut r = match prs.forward(n, false) {
                        None => break,
                        Some(r) => r,
                    };
                    // The returned backup ranges should <= n
                    assert!(r.len() <= n);

                    if !r.is_empty() {
                        ranges.append(&mut r);
                    }
                }

                for (a, b) in ranges.into_iter().zip(expect) {
                    assert_eq!(
                        a.start_key.map_or_else(Vec::new, |k| k.into_raw().unwrap()),
                        b.0
                    );
                    assert_eq!(
                        a.end_key.map_or_else(Vec::new, |k| k.into_raw().unwrap()),
                        b.1
                    );
                }
            };

        // Test whether responses contain correct range.
        #[allow(clippy::blocks_in_conditions)]
        let test_handle_backup_task_ranges =
            |sub_ranges: Vec<(&[u8], &[u8])>, expect: Vec<(&[u8], &[u8])>| {
                let tmp = TempDir::new().unwrap();
                let backend = make_local_backend(tmp.path());
                let (tx, rx) = unbounded();

                let mut ranges = Vec::with_capacity(sub_ranges.len());
                for &(start_key, end_key) in &sub_ranges {
                    let key_range = KeyRange {
                        start_key: start_key.to_vec(),
                        end_key: end_key.to_vec(),
                        ..Default::default()
                    };
                    ranges.push(key_range);
                }
                let task = Task {
                    request: Request {
                        start_key: b"1".to_vec(),
                        end_key: b"2".to_vec(),
                        sub_ranges: ranges,
                        start_ts: 1.into(),
                        end_ts: 1.into(),
                        backend,
                        rate_limiter: Limiter::new(f64::INFINITY),
                        cancel: Arc::default(),
                        is_raw_kv: false,
                        dst_api_ver: ApiVersion::V1,
                        cf: engine_traits::CF_DEFAULT,
                        compression_type: CompressionType::Unknown,
                        compression_level: 0,
                        cipher: CipherInfo::default(),
                        replica_read: false,
                        resource_group_name: "".into(),
                        source_tag: "br".into(),
                        bypass_locks: vec![],
                        access_locks: vec![],
                    },
                    resp: tx,
                };
                endpoint.handle_backup_task(task);
                let resps: Vec<_> = block_on(rx.collect());
                for a in &resps {
                    assert!(
                        expect
                            .iter()
                            .any(|b| { a.get_start_key() == b.0 && a.get_end_key() == b.1 }),
                        "{:?} {:?}",
                        resps,
                        expect
                    );
                }
                assert_eq!(resps.len(), expect.len());
            };

        // Backup range from case.0 to case.1,
        // the case.2 is the expected results.
        type Case<'a> = (Vec<(&'a [u8], &'a [u8])>, Vec<(&'a [u8], &'a [u8])>);

        let case: Vec<Case<'_>> = vec![
            (
                vec![(b"", b"1"), (b"1", b"2")],
                vec![(b"", b"1"), (b"1", b"2")],
            ),
            (
                vec![(b"", b"2"), (b"3", b"4")],
                vec![(b"", b"1"), (b"1", b"2"), (b"3", b"4")],
            ),
            (
                vec![(b"7", b"8"), (b"8", b"9")],
                vec![(b"7", b"8"), (b"8", b"9")],
            ),
            (
                vec![(b"8", b"9"), (b"6", b"8")],
                vec![(b"8", b"9"), (b"7", b"8")],
            ),
            (
                vec![(b"8", b"85"), (b"88", b"89"), (b"7", b"8")],
                vec![(b"8", b"85"), (b"88", b"89"), (b"7", b"8")],
            ),
            (
                vec![(b"8", b"85"), (b"", b"35"), (b"88", b"89"), (b"7", b"8")],
                vec![
                    (b"8", b"85"),
                    (b"", b"1"),
                    (b"1", b"2"),
                    (b"3", b"35"),
                    (b"88", b"89"),
                    (b"7", b"8"),
                ],
            ),
            (vec![(b"", b"1")], vec![(b"", b"1")]),
            (vec![(b"", b"2")], vec![(b"", b"1"), (b"1", b"2")]),
            (vec![(b"1", b"2")], vec![(b"1", b"2")]),
            (vec![(b"1", b"3")], vec![(b"1", b"2")]),
            (vec![(b"1", b"4")], vec![(b"1", b"2"), (b"3", b"4")]),
            (vec![(b"4", b"5")], vec![]),
            (vec![(b"4", b"6")], vec![]),
            (vec![(b"4", b"6"), (b"6", b"7")], vec![]),
            (vec![(b"2", b"3"), (b"4", b"6"), (b"6", b"7")], vec![]),
            (vec![(b"2", b"7")], vec![(b"3", b"4")]),
            (vec![(b"7", b"8")], vec![(b"7", b"8")]),
            (
                vec![(b"3", b"")],
                vec![(b"3", b"4"), (b"7", b"9"), (b"9", b"")],
            ),
            (vec![(b"5", b"")], vec![(b"7", b"9"), (b"9", b"")]),
            (vec![(b"7", b"")], vec![(b"7", b"9"), (b"9", b"")]),
            (vec![(b"8", b"91")], vec![(b"8", b"9"), (b"9", b"91")]),
            (vec![(b"8", b"")], vec![(b"8", b"9"), (b"9", b"")]),
            (
                vec![(b"", b"")],
                vec![
                    (b"", b"1"),
                    (b"1", b"2"),
                    (b"3", b"4"),
                    (b"7", b"9"),
                    (b"9", b""),
                ],
            ),
        ];
        for (ranges, expect_ranges) in case {
            test_seek_backup_ranges(ranges.clone(), expect_ranges.clone());
            test_handle_backup_task_ranges(ranges, expect_ranges);
        }
    }

    fn fake_empty_marker() -> Vec<super::BackupRange> {
        vec![super::BackupRange {
            start_key: None,
            end_key: None,
            region: Region::new(),
            peer: Peer::new(),
            codec: KeyValueCodec::new(false, ApiVersion::V1, ApiVersion::V1),
            cf: "",
            uses_replica_read: false,
        }]
    }

    #[test]
    fn test_seek_ranges_2() {
        let (_tmp, endpoint) = new_endpoint();

        endpoint.region_info.set_regions(vec![
            (b"2".to_vec(), b"4".to_vec(), 1),
            (b"6".to_vec(), b"8".to_vec(), 2),
        ]);
        let sub_ranges: Vec<(&[u8], &[u8])> = vec![(b"1", b"11"), (b"3", b"7"), (b"8", b"9")];
        let expect: Vec<(&[u8], &[u8])> = vec![(b"", b""), (b"3", b"4"), (b"6", b"7"), (b"", b"")];

        let mut ranges = Vec::with_capacity(sub_ranges.len());
        for &(start_key, end_key) in &sub_ranges {
            let start_key = (!start_key.is_empty()).then_some(Key::from_raw(start_key));
            let end_key = (!end_key.is_empty()).then_some(Key::from_raw(end_key));
            ranges.push((start_key, end_key));
        }
        let mut prs = Progress::new_with_ranges(
            endpoint.store_id,
            ranges,
            endpoint.region_info.clone(),
            KeyValueCodec::new(false, ApiVersion::V1, ApiVersion::V1),
            engine_traits::CF_DEFAULT,
        );

        let mut ranges = Vec::with_capacity(expect.len());
        loop {
            let n = (rand::random::<usize>() % 2) + 1;
            let mut r = match prs.forward(n, false) {
                None => break,
                Some(r) => r,
            };
            // The returned backup ranges should <= n
            assert!(r.len() <= n);

            if !r.is_empty() {
                ranges.append(&mut r);
            } else {
                // append the empty marker
                ranges.append(&mut fake_empty_marker());
            }
        }

        assert!(ranges.len() == expect.len());
        for (a, b) in ranges.into_iter().zip(expect) {
            assert_eq!(
                a.start_key.map_or_else(Vec::new, |k| k.into_raw().unwrap()),
                b.0
            );
            assert_eq!(
                a.end_key.map_or_else(Vec::new, |k| k.into_raw().unwrap()),
                b.1
            );
        }
    }

    #[test]
    fn test_handle_backup_task_bypass_locks() {
        // consider both short and long value
        for &len in &[SHORT_VALUE_MAX_LEN - 1, SHORT_VALUE_MAX_LEN * 2] {
            let (tmp, endpoint) = new_endpoint();
            let mut engine = endpoint.engine.clone();
            endpoint
                .region_info
                .set_regions(vec![(b"".to_vec(), b"".to_vec(), 1)]);

            let mut ts = TimeStamp::new(1);
            let mut alloc_ts = || *ts.incr();
            let lock_key = 9;

            // Prepare data: 10 keys, with key 9 locked
            let prewrite_keys: Vec<_> = (0..10u8)
                .map(|i| {
                    let start = alloc_ts();
                    let commit = alloc_ts();
                    let key = i.to_string();
                    must_prewrite_put(
                        &mut engine,
                        key.as_bytes(),
                        &vec![i; len],
                        key.as_bytes(),
                        start,
                    );
                    if i == lock_key {
                        (key, start, commit)
                    } else {
                        must_commit(&mut engine, key.as_bytes(), start, commit);
                        (key, start, commit)
                    }
                })
                .collect();

            let test_cases = vec![
                (
                    "No bypass locks",
                    TimeStamp::new(3),
                    vec![],
                    b"0".to_vec(),
                    false,
                ),
                (
                    "With bypass locks, lock ts > backup ts",
                    TimeStamp::new(12),
                    prewrite_keys.clone(),
                    b"4".to_vec(),
                    false,
                ),
                (
                    "No bypass locks, lock ts > backup ts",
                    TimeStamp::new(12),
                    vec![],
                    b"4".to_vec(),
                    false,
                ),
                (
                    "No bypass locks, lock ts < backup ts",
                    TimeStamp::new(22),
                    vec![],
                    vec![],
                    true,
                ),
                (
                    "With bypass locks, lock ts < backup ts",
                    TimeStamp::new(22),
                    prewrite_keys.clone(),
                    // lock key is 9, so the key (8) should be included
                    (lock_key - 1).to_string().into_bytes(),
                    // the error can be ignored. because the lock key should commit after backup
                    // ts.
                    false,
                ),
            ];

            for (case_name, backup_ts, bypass_locks, expect_end_key, expect_error) in test_cases {
                let mut req = BackupRequest::default();
                req.set_start_key(vec![]);
                req.set_end_key(vec![]);
                req.set_start_version(0);
                req.set_end_version(backup_ts.into_inner());

                let mut context = Context::default();
                context.set_resolved_locks(
                    bypass_locks
                        .iter()
                        .map(|(_, s, _)| s.into_inner())
                        .collect(),
                );
                req.set_context(context);

                let (tx, rx) = unbounded();
                let tmp_dir = make_unique_dir(tmp.path());
                req.set_storage_backend(make_local_backend(&tmp_dir));
                let (task, _) = Task::new(req, tx).unwrap();
                endpoint.handle_backup_task(task);

                let (resp, _) = block_on(rx.into_future());
                let resp = resp.unwrap();

                if expect_error {
                    assert!(
                        resp.has_error(),
                        "Case '{}' should have an error",
                        case_name
                    );
                } else {
                    assert!(
                        !resp.has_error(),
                        "Case '{}' should not have an error: {:?}",
                        case_name,
                        resp
                    );

                    let expected_file_count = if len <= SHORT_VALUE_MAX_LEN { 1 } else { 2 };
                    let files = resp.get_files();

                    assert_eq!(
                        files.len(),
                        expected_file_count,
                        "Case '{}' failed: expected {} files, got {} files. {:?}",
                        case_name,
                        expected_file_count,
                        files.len(),
                        resp
                    );

                    for file in files {
                        let sst_path = tmp_dir.join(&file.name);
                        let sst_reader = RocksSstReader::open_with_env(
                            sst_path.as_os_str().to_str().unwrap(),
                            None,
                        )
                        .unwrap();
                        sst_reader.verify_checksum().unwrap();
                        let mut iter = sst_reader.iter(IterOptions::default()).unwrap();
                        iter.seek_to_last().unwrap();
                        let end_key = Key::truncate_ts_for(iter.key()).unwrap();
                        assert_eq!(
                            end_key,
                            data_key(Key::from_raw(&expect_end_key).as_encoded()),
                            "Case '{}' failed: unexpected end key",
                            case_name
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn test_handle_backup_task() {
        let limiter = Arc::new(IoRateLimiter::new_for_test());
        let stats = limiter.statistics().unwrap();
        let (tmp, endpoint) = new_endpoint_with_limiter(Some(limiter), ApiVersion::V1, false, None);
        let mut engine = endpoint.engine.clone();

        endpoint
            .region_info
            .set_regions(vec![(b"".to_vec(), b"5".to_vec(), 1)]);

        let mut ts = TimeStamp::new(1);
        let mut alloc_ts = || *ts.incr();
        let mut backup_tss = vec![];
        // Multi-versions for key 0..9.
        for len in &[SHORT_VALUE_MAX_LEN - 1, SHORT_VALUE_MAX_LEN * 2] {
            for i in 0..10u8 {
                let start = alloc_ts();
                let commit = alloc_ts();
                let key = format!("{}", i);
                must_prewrite_put(
                    &mut engine,
                    key.as_bytes(),
                    &vec![i; *len],
                    key.as_bytes(),
                    start,
                );
                must_commit(&mut engine, key.as_bytes(), start, commit);
                backup_tss.push((alloc_ts(), len));
            }
        }
        // flush to disk so that read requests can be traced by TiKV limiter.
        engine
            .get_rocksdb()
            .flush_cf(engine_traits::CF_DEFAULT, true /* sync */)
            .unwrap();
        engine
            .get_rocksdb()
            .flush_cf(engine_traits::CF_WRITE, true /* sync */)
            .unwrap();

        // TODO: check key number for each snapshot.
        let limiter = Limiter::new(10.0 * 1024.0 * 1024.0 /* 10 MB/s */);
        for (ts, len) in backup_tss {
            stats.reset();
            let mut req = BackupRequest::default();
            req.set_start_key(vec![]);
            req.set_end_key(vec![b'5']);
            req.set_start_version(0);
            req.set_end_version(ts.into_inner());
            let (tx, rx) = unbounded();

            let tmp1 = make_unique_dir(tmp.path());
            req.set_storage_backend(make_local_backend(&tmp1));
            if len % 2 == 0 {
                req.set_rate_limit(10 * 1024 * 1024);
            }
            let (mut task, _) = Task::new(req, tx).unwrap();
            if len % 2 == 0 {
                // Make sure the rate limiter is set.
                assert!(task.request.rate_limiter.speed_limit().is_finite());
                // Share the same rate limiter.
                task.request.rate_limiter = limiter.clone();
            }
            endpoint.handle_backup_task(task);
            let (resp, rx) = block_on(rx.into_future());
            let resp = resp.unwrap();
            assert!(!resp.has_error(), "{:?}", resp);
            let file_len = if *len <= SHORT_VALUE_MAX_LEN { 1 } else { 2 };
            let files = resp.get_files();
            info!("{:?}", files);
            assert_eq!(
                files.len(),
                file_len, // default and write
                "{:?}",
                resp
            );
            let (none, _rx) = block_on(rx.into_future());
            assert!(none.is_none(), "{:?}", none);
            assert_eq!(stats.fetch(IoType::Export, IoOp::Write), 0);
            assert_ne!(stats.fetch(IoType::Export, IoOp::Read), 0);
        }
    }

    fn generate_test_raw_key(idx: u64, api_ver: ApiVersion) -> String {
        // first key is an empty key for testing purposes
        let mut key = if idx == 0 {
            String::from("")
        } else {
            format!("k{:0>10}", idx)
        };
        if api_ver == ApiVersion::V2 {
            // [0, 0, 0] is the default key space id.
            let mut apiv2_key = [RAW_KEY_PREFIX, 0, 0, 0].to_vec();
            apiv2_key.extend(key.as_bytes());
            key = String::from_utf8(apiv2_key).unwrap();
        }
        key
    }

    fn generate_test_raw_value(idx: u64, api_ver: ApiVersion) -> String {
        format!("v_{}", generate_test_raw_key(idx, api_ver))
    }

    fn generate_engine_test_key(
        user_key: String,
        ts: Option<TimeStamp>,
        api_ver: ApiVersion,
    ) -> Key {
        dispatch_api_version!(api_ver, {
            return API::encode_raw_key_owned(user_key.into_bytes(), ts);
        })
    }

    fn generate_engine_test_value(user_value: String, api_ver: ApiVersion, ttl: u64) -> Vec<u8> {
        let raw_value = RawValue {
            user_value: user_value.into_bytes(),
            expire_ts: Some(ttl),
            is_delete: false,
        };
        dispatch_api_version!(api_ver, {
            return API::encode_raw_value_owned(raw_value);
        })
    }

    fn convert_test_backup_user_key(
        mut raw_key: String,
        cur_ver: ApiVersion,
        dst_ver: ApiVersion,
    ) -> Key {
        if (cur_ver == ApiVersion::V1 || cur_ver == ApiVersion::V1ttl) && dst_ver == ApiVersion::V2
        {
            // [0, 0, 0] is the default key space id.
            let mut apiv2_key = [RAW_KEY_PREFIX, 0, 0, 0].to_vec();
            apiv2_key.extend(raw_key.as_bytes());
            raw_key = String::from_utf8(apiv2_key).unwrap();
        }
        Key::from_encoded(raw_key.into_bytes())
    }

    fn test_handle_backup_raw_task_impl(
        cur_api_ver: ApiVersion,
        dst_api_ver: ApiVersion,
        test_ttl: bool,
    ) -> bool {
        let limiter = Arc::new(IoRateLimiter::new_for_test());
        let stats = limiter.statistics().unwrap();
        let (tmp, endpoint) = new_endpoint_with_limiter(Some(limiter), cur_api_ver, true, None);
        let engine = endpoint.engine.clone();

        let start_key_idx: u64 = 100;
        let end_key_idx: u64 = 110;
        let ttl_expire_cnt = 2;
        endpoint.region_info.set_regions(vec![(
            vec![], // generate_test_raw_key(start_key_idx).into_bytes(),
            vec![], // generate_test_raw_key(end_key_idx).into_bytes(),
            1,
        )]);
        let ctx = Context::default();
        let mut i = start_key_idx;
        let digest = crc64fast::Digest::new();
        let mut checksum: u64 = 0;
        while i < end_key_idx {
            let key_str = generate_test_raw_key(i, cur_api_ver);
            let value_str = generate_test_raw_value(i, cur_api_ver);
            let ttl = if test_ttl && i >= end_key_idx - ttl_expire_cnt {
                1 // let last `ttl_expire_cnt` value expired when backup
            } else {
                u64::MAX
            };
            // engine do not append ts anymore, need write ts encoded key into engine.
            let key = generate_engine_test_key(key_str.clone(), Some(i.into()), cur_api_ver);
            let value = generate_engine_test_value(value_str.clone(), cur_api_ver, ttl);
            let dst_user_key = convert_test_backup_user_key(key_str, cur_api_ver, dst_api_ver);
            let dst_value = value_str.as_bytes();
            if ttl != 1 {
                checksum = checksum_crc64_xor(
                    checksum,
                    digest.clone(),
                    dst_user_key.as_encoded(),
                    dst_value,
                );
            }
            engine.put(&ctx, key, value).unwrap();
            i += 1;
        }
        // flush to disk so that read requests can be traced by TiKV limiter.
        engine
            .get_rocksdb()
            .flush_cf(engine_traits::CF_DEFAULT, true /* sync */)
            .unwrap();

        // TODO: check key number for each snapshot.
        stats.reset();
        let mut req = BackupRequest::default();
        let backup_start = if cur_api_ver == ApiVersion::V2 {
            vec![RAW_KEY_PREFIX, 0, 0, 0] // key space id takes 3 bytes.
        } else {
            vec![]
        };
        let backup_end = if cur_api_ver == ApiVersion::V2 {
            vec![RAW_KEY_PREFIX, 0, 0, 1] // [0, 0, 1] is the end of the file
        } else {
            vec![]
        };
        let file_start = if dst_api_ver == ApiVersion::V2 {
            vec![RAW_KEY_PREFIX, 0, 0, 0] // key space id takes 3 bytes.
        } else {
            vec![]
        };
        let file_end = if dst_api_ver == ApiVersion::V2 {
            vec![RAW_KEY_PREFIX, 0, 0, 1] // [0, 0, 1] is the end of the file
        } else {
            vec![]
        };
        if test_ttl {
            std::thread::sleep(Duration::from_secs(2)); // wait for ttl expired
        }
        let original_expire_cnt = BACKUP_RAW_EXPIRED_COUNT.get();
        req.set_start_key(backup_start.clone());
        req.set_end_key(backup_end.clone());
        req.set_is_raw_kv(true);
        req.set_dst_api_version(dst_api_ver);
        let (tx, rx) = unbounded();

        let limiter = Limiter::new(10.0 * 1024.0 * 1024.0 /* 10 MB/s */);
        let tmp1 = make_unique_dir(tmp.path());
        req.set_storage_backend(make_local_backend(&tmp1));
        req.set_rate_limit(10 * 1024 * 1024);
        let (mut task, _) = Task::new(req, tx).unwrap();
        task.request.rate_limiter = limiter;
        endpoint.handle_backup_task(task);
        let (resp, rx) = block_on(rx.into_future());
        let resp = resp.unwrap();
        if cur_api_ver != dst_api_ver && dst_api_ver != ApiVersion::V2 {
            assert!(resp.has_error());
            return false;
        }

        let current_expire_cnt = BACKUP_RAW_EXPIRED_COUNT.get();
        let expect_expire_cnt = if test_ttl {
            original_expire_cnt + ttl_expire_cnt
        } else {
            original_expire_cnt
        };
        assert_eq!(expect_expire_cnt, current_expire_cnt);
        assert!(!resp.has_error(), "{:?}", resp);
        assert_eq!(resp.get_start_key(), backup_start);
        assert_eq!(resp.get_end_key(), backup_end);
        let file_len = 1;
        let files = resp.get_files();
        info!("{:?}", files);
        let mut expect_cnt = end_key_idx - start_key_idx;
        if test_ttl {
            expect_cnt -= 2;
        }
        assert_eq!(files.len(), file_len /* default cf */, "{:?}", resp);
        assert_eq!(files[0].total_kvs, expect_cnt);
        assert_eq!(files[0].crc64xor, checksum);
        assert_eq!(files[0].get_start_key(), file_start);
        assert_eq!(files[0].get_end_key(), file_end);
        let first_kv_backup_size = {
            let raw_key_str = generate_test_raw_key(start_key_idx, cur_api_ver);
            let raw_value_str = generate_test_raw_value(start_key_idx, cur_api_ver);
            let backup_key = convert_test_backup_user_key(raw_key_str, cur_api_ver, dst_api_ver);
            let backup_value = raw_value_str.as_bytes();
            backup_key.len() + backup_value.len()
        } as u64;
        let kv_backup_size = {
            let raw_key_str = generate_test_raw_key(1, cur_api_ver);
            let raw_value_str = generate_test_raw_value(1, cur_api_ver);
            let backup_key = convert_test_backup_user_key(raw_key_str, cur_api_ver, dst_api_ver);
            let backup_value = raw_value_str.as_bytes();
            backup_key.len() + backup_value.len()
        } as u64;
        assert_eq!(
            files[0].total_bytes,
            (expect_cnt - 1) * kv_backup_size + first_kv_backup_size
        );
        let (none, _rx) = block_on(rx.into_future());
        assert!(none.is_none(), "{:?}", none);
        assert_eq!(stats.fetch(IoType::Export, IoOp::Write), 0);
        assert_ne!(stats.fetch(IoType::Export, IoOp::Read), 0);
        true
    }

    #[test]
    fn test_handle_backup_raw() {
        // (src_api_version, dst_api_version, test_ttl, result)
        let test_backup_cases = vec![
            (ApiVersion::V1, ApiVersion::V1, false, true),
            (ApiVersion::V1ttl, ApiVersion::V1ttl, true, true),
            (ApiVersion::V2, ApiVersion::V2, true, true),
            (ApiVersion::V1, ApiVersion::V2, false, true),
            (ApiVersion::V1ttl, ApiVersion::V2, false, true),
            (ApiVersion::V1, ApiVersion::V1ttl, false, false),
            (ApiVersion::V2, ApiVersion::V1, false, false),
            (ApiVersion::V2, ApiVersion::V1ttl, false, false),
            (ApiVersion::V1ttl, ApiVersion::V1, false, false),
        ];
        for (idx, (src_api, dst_api, test_ttl, result)) in test_backup_cases.into_iter().enumerate()
        {
            assert_eq!(
                test_handle_backup_raw_task_impl(src_api, dst_api, test_ttl),
                result,
                "case {}",
                idx,
            );
        }
    }

    #[test]
    fn test_backup_raw_apiv2_causal_ts() {
        let limiter = Arc::new(IoRateLimiter::new_for_test());
        let ts_provider: Arc<CausalTsProviderImpl> =
            Arc::new(causal_ts::tests::TestProvider::default().into());
        let start_ts = block_on(ts_provider.async_get_ts()).unwrap();
        let (tmp, endpoint) = new_endpoint_with_limiter(
            Some(limiter),
            ApiVersion::V2,
            true,
            Some(ts_provider.clone()),
        );

        let mut req = BackupRequest::default();
        let (tx, _) = unbounded();
        let tmp1 = make_unique_dir(tmp.path());
        req.set_storage_backend(make_local_backend(&tmp1));
        req.set_start_key(b"r".to_vec());
        req.set_end_key(b"s".to_vec());
        req.set_is_raw_kv(true);
        req.set_dst_api_version(ApiVersion::V2);
        let (task, _) = Task::new(req, tx).unwrap();
        endpoint.handle_backup_task(task);
        let end_ts = block_on(ts_provider.async_get_ts()).unwrap();
        assert_eq!(end_ts.into_inner(), start_ts.next().into_inner() + 101);
    }

    #[test]
    fn test_scan_error() {
        let (tmp, endpoint) = new_endpoint();
        let mut engine = endpoint.engine.clone();

        endpoint
            .region_info
            .set_regions(vec![(b"".to_vec(), b"5".to_vec(), 1)]);

        let mut ts: TimeStamp = 1.into();
        let mut alloc_ts = || *ts.incr();
        let start = alloc_ts();
        let key = format!("{}", start);
        must_prewrite_put(
            &mut engine,
            key.as_bytes(),
            key.as_bytes(),
            key.as_bytes(),
            start,
        );

        let now = alloc_ts();
        let mut req = BackupRequest::default();
        req.set_start_key(vec![]);
        req.set_end_key(vec![b'5']);
        req.set_start_version(now.into_inner());
        req.set_end_version(now.into_inner());
        req.set_concurrency(4);
        let tmp1 = make_unique_dir(tmp.path());
        req.set_storage_backend(make_local_backend(&tmp1));
        let (tx, rx) = unbounded();
        let (task, _) = Task::new(req.clone(), tx).unwrap();
        endpoint.handle_backup_task(task);
        check_response(rx, |resp| {
            let resp = resp.unwrap();
            assert!(resp.get_error().has_kv_error(), "{:?}", resp);
            assert!(resp.get_error().get_kv_error().has_locked(), "{:?}", resp);
            assert_eq!(resp.get_files().len(), 0, "{:?}", resp);
        });

        // Commit the perwrite.
        let commit = alloc_ts();
        must_commit(&mut engine, key.as_bytes(), start, commit);

        // Test whether it can correctly convert not leader to region error.
        engine.trigger_not_leader();
        let now = alloc_ts();
        req.set_start_version(now.into_inner());
        req.set_end_version(now.into_inner());
        let tmp2 = make_unique_dir(tmp.path());
        req.set_storage_backend(make_local_backend(&tmp2));
        let (tx, rx) = unbounded();
        let (task, _) = Task::new(req, tx).unwrap();
        endpoint.handle_backup_task(task);
        check_response(rx, |resp| {
            let resp = resp.unwrap();
            assert!(resp.get_error().has_region_error(), "{:?}", resp);
            assert!(
                resp.get_error().get_region_error().has_not_leader(),
                "{:?}",
                resp
            );
        });
    }

    #[test]
    fn test_cancel() {
        let (temp, mut endpoint) = new_endpoint();
        let mut engine = endpoint.engine.clone();

        endpoint
            .region_info
            .set_regions(vec![(b"".to_vec(), b"5".to_vec(), 1)]);

        let mut ts: TimeStamp = 1.into();
        let mut alloc_ts = || *ts.incr();
        let start = alloc_ts();
        let key = format!("{}", start);
        must_prewrite_put(
            &mut engine,
            key.as_bytes(),
            key.as_bytes(),
            key.as_bytes(),
            start,
        );
        // Commit the perwrite.
        let commit = alloc_ts();
        must_commit(&mut engine, key.as_bytes(), start, commit);

        let now = alloc_ts();
        let mut req = BackupRequest::default();
        req.set_start_key(vec![]);
        req.set_end_key(vec![]);
        req.set_start_version(now.into_inner());
        req.set_end_version(now.into_inner());
        req.set_concurrency(4);
        req.set_storage_backend(make_local_backend(temp.path()));

        // Cancel the task before starting the task.
        let (tx, rx) = unbounded();
        let (task, cancel) = Task::new(req.clone(), tx).unwrap();
        // Cancel the task.
        cancel.store(true, Ordering::SeqCst);
        endpoint.handle_backup_task(task);
        check_response(rx, |resp| {
            assert!(resp.is_none());
        });

        // Cancel the task during backup.
        let (tx, rx) = unbounded();
        let (task, cancel) = Task::new(req, tx).unwrap();
        endpoint.region_info.canecl_on_seek(cancel);
        endpoint.handle_backup_task(task);
        check_response(rx, |resp| {
            assert!(resp.is_none());
        });
    }

    #[test]
    fn test_busy() {
        let (_tmp, endpoint) = new_endpoint();
        let engine = endpoint.engine.clone();

        endpoint
            .region_info
            .set_regions(vec![(b"".to_vec(), b"5".to_vec(), 1)]);

        let mut req = BackupRequest::default();
        req.set_start_key(vec![]);
        req.set_end_key(vec![]);
        req.set_start_version(1);
        req.set_end_version(1);
        req.set_concurrency(4);
        req.set_storage_backend(make_noop_backend());

        let (tx, rx) = unbounded();
        let (task, _) = Task::new(req, tx).unwrap();
        // Pause the engine 6 seconds to trigger Timeout error.
        // The Timeout error is translated to server is busy.
        engine.pause(Duration::from_secs(6));
        endpoint.handle_backup_task(task);
        check_response(rx, |resp| {
            let resp = resp.unwrap();
            assert!(resp.get_error().has_region_error(), "{:?}", resp);
            assert!(
                resp.get_error().get_region_error().has_server_is_busy(),
                "{:?}",
                resp
            );
        });
    }

    #[test]
    fn test_adjust_thread_pool_size() {
        let (_tmp, endpoint) = new_endpoint();
        endpoint
            .region_info
            .set_regions(vec![(b"".to_vec(), b"".to_vec(), 1)]);

        let mut req = BackupRequest::default();
        req.set_start_key(vec![b'1']);
        req.set_end_key(vec![]);
        req.set_start_version(1);
        req.set_end_version(1);
        req.set_storage_backend(make_noop_backend());

        let (tx, rx) = unbounded();

        // expand thread pool is needed
        endpoint.get_config_manager().set_num_threads(15);
        let (task, _) = Task::new(req.clone(), tx.clone()).unwrap();
        endpoint.handle_backup_task(task);
        assert!(endpoint.pool.borrow().size() == 15);

        // shrink thread pool
        endpoint.get_config_manager().set_num_threads(10);
        req.set_start_key(vec![b'2']);
        let (task, _) = Task::new(req.clone(), tx.clone()).unwrap();
        endpoint.handle_backup_task(task);
        assert!(endpoint.pool.borrow().size() == 10);

        endpoint.get_config_manager().set_num_threads(3);
        req.set_start_key(vec![b'3']);
        let (task, _) = Task::new(req, tx).unwrap();
        endpoint.handle_backup_task(task);
        assert!(endpoint.pool.borrow().size() == 3);

        // make sure all tasks can finish properly.
        let responses = block_on(rx.collect::<Vec<_>>());
        assert_eq!(responses.len(), 3, "{:?}", responses);

        // for testing whether dropping the pool before all tasks finished causes panic.
        // but the panic must be checked manually. (It may panic at tokio runtime
        // threads)
        let mut pool = ResizableRuntime::new(
            1,
            "bkwkr",
            Box::new(utils::create_tokio_runtime),
            Box::new(|new_size: usize| BACKUP_THREAD_POOL_SIZE_GAUGE.set(new_size as i64)),
        );
        pool.spawn(async { tokio::time::sleep(Duration::from_millis(100)).await });
        pool.adjust_with(2);
        drop(pool);
        std::thread::sleep(Duration::from_millis(150));
    }

    #[test]
    fn test_backup_file_name() {
        let region = metapb::Region::default();
        let store_id = 1;
        let test_cases = ["s3", "local", "gcs", "azure", "hdfs"];
        let test_target = [
            "1/0_0_000",
            "1/0_0_000",
            "1_0_0_000",
            "1_0_0_000",
            "1_0_0_000",
        ];

        let delimiter = "_";
        for (storage_name, target) in test_cases.iter().zip(test_target.iter()) {
            let key = Some(String::from("000"));
            let filename = backup_file_name(store_id, &region, key, storage_name);

            let mut prefix_arr: Vec<&str> = filename.split(delimiter).collect();
            prefix_arr.remove(prefix_arr.len() - 1);

            assert_eq!(target.to_string(), prefix_arr.join(delimiter));
        }

        let test_target = ["1/0_0", "1/0_0", "1_0_0", "1_0_0", "1_0_0"];
        for (storage_name, target) in test_cases.iter().zip(test_target.iter()) {
            let key = None;
            let filename = backup_file_name(store_id, &region, key, storage_name);
            assert_eq!(target.to_string(), filename);
        }
    }

    #[test]
    fn test_transmute_locks() {
        let locks = vec![];
        assert_eq!(TsSet::vec_from_u64s(locks), TsSet::Empty);
    }
}
