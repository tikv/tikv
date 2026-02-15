// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    borrow::Borrow,
    collections::HashMap,
    fmt::Display,
    path::{Path, PathBuf},
    result,
    sync::{
        Arc, RwLock as SyncRwLock,
        atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use dashmap::DashMap;
use encryption::{BackupEncryptionManager, EncrypterReader, Iv, MultiMasterKeyBackend};
use encryption_export::create_async_backend;
use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE, CfName};
use external_storage::{BackendConfig, ExternalStorage, HdfsConfig, UnpinReader, create_storage};
use file_system::Sha256Reader;
use futures::io::Cursor;
use kvproto::{
    brpb::{
        CompressionType, DataFileGroup, DataFileInfo, FileType, MetaVersion, Metadata,
        StreamBackupTaskInfo, StreamBackupTaskSecurityConfig_oneof_encryption,
    },
    encryptionpb::{EncryptionMethod, FileEncryptionInfo, MasterKeyBased, PlainTextDataKey},
    metapb::RegionEpoch,
    raft_cmdpb::CmdType,
};
use openssl::hash::{Hasher, MessageDigest};
use protobuf::Message;
use raftstore::coprocessor::CmdBatch;
use slog_global::debug;
use tidb_query_datatype::codec::table::decode_table_id;
use tikv::config::BackupStreamConfig;
use tikv_util::{
    Either, HandyRwLock, box_err,
    codec::stream_event::EventEncoder,
    config::ReadableSize,
    error, info,
    time::{Instant, Limiter},
    warn,
    worker::Scheduler,
};
use tokio::{
    io::AsyncWriteExt,
    sync::{Mutex, RwLock},
};
use tokio_util::compat::{FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};
use tracing::instrument;
use tracing_active_tree::frame;
use txn_types::{Key, TimeStamp, WriteRef};
use uuid::Uuid;

use super::errors::Result;
use crate::{
    annotate,
    endpoint::Task,
    errors::{ContextualResultExt, Error},
    metadata::StreamTask,
    metrics::{HANDLE_KV_HISTOGRAM, SKIP_KV_COUNTER},
    subscription_manager::ResolvedRegions,
    subscription_track::TwoPhaseResolver,
    tempfiles::{self, ForRead, TempFilePool},
    try_send,
    utils::{self, CompressionWriter, FilesReader, SegmentMap, SlotMap, StopWatch},
};

const FLUSH_FAILURE_BECOME_FATAL_THRESHOLD: usize = 30;

#[derive(Clone)]
pub enum TaskSelector {
    ByName(String),
    ByKey(Vec<u8>),
    ByRange(Vec<u8>, Vec<u8>),
    All,
}

impl std::fmt::Debug for TaskSelector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.reference().fmt(f)
    }
}

impl TaskSelector {
    pub fn reference(&self) -> TaskSelectorRef<'_> {
        match self {
            TaskSelector::ByName(s) => TaskSelectorRef::ByName(s),
            TaskSelector::ByKey(k) => TaskSelectorRef::ByKey(k),
            TaskSelector::ByRange(s, e) => TaskSelectorRef::ByRange(s, e),
            TaskSelector::All => TaskSelectorRef::All,
        }
    }
}

#[derive(Clone, Copy)]
pub enum TaskSelectorRef<'a> {
    ByName(&'a str),
    ByKey(&'a [u8]),
    ByRange(&'a [u8], &'a [u8]),
    All,
}

impl std::fmt::Debug for TaskSelectorRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ByName(name) => f.debug_tuple("ByName").field(name).finish(),
            Self::ByKey(key) => f
                .debug_tuple("ByKey")
                .field(&format_args!("{}", utils::redact(key)))
                .finish(),
            Self::ByRange(start, end) => f
                .debug_tuple("ByRange")
                .field(&format_args!("{}", utils::redact(start)))
                .field(&format_args!("{}", utils::redact(end)))
                .finish(),
            Self::All => write!(f, "All"),
        }
    }
}

impl TaskSelectorRef<'_> {
    fn matches<'c, 'd>(
        self,
        task_name: &str,
        mut task_range: impl Iterator<Item = (&'c [u8], &'d [u8])>,
    ) -> bool {
        match self {
            TaskSelectorRef::ByName(name) => task_name == name,
            TaskSelectorRef::ByKey(k) => task_range.any(|(s, e)| utils::is_in_range(k, (s, e))),
            TaskSelectorRef::ByRange(x1, y1) => {
                task_range.any(|(x2, y2)| utils::is_overlapping((x1, y1), (x2, y2)))
            }
            TaskSelectorRef::All => true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ApplyEvent {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub cf: CfName,
    pub cmd_type: CmdType,
}

#[derive(Debug, Clone)]
pub struct ApplyEvents {
    events: Vec<ApplyEvent>,
    region_id: u64,
    // TODO: this field is useless, maybe remove it.
    region_resolved_ts: u64,
}

#[derive(Clone, Copy)]
pub struct FlushContext<'a> {
    pub task_name: &'a str,
    pub store_id: u64,
    pub resolved_regions: &'a ResolvedRegions,
    pub resolved_ts: TimeStamp,
    pub flush_ts: TimeStamp,
}

impl ApplyEvents {
    /// Convert a [CmdBatch] to a vector of events. Ignoring admin / error
    /// commands. At the same time, advancing status of the `Resolver` by
    /// those keys.
    /// Note: the resolved ts cannot be advanced if there is no command, maybe
    /// we also need to update resolved_ts when flushing?
    pub fn from_cmd_batch(cmd: CmdBatch, resolver: &mut TwoPhaseResolver) -> Result<Self> {
        let region_id = cmd.region_id;
        let mut result = vec![];
        for req in cmd
            .cmds
            .into_iter()
            .filter(|cmd| {
                // We will add some log then, this is just a template.
                #[allow(clippy::if_same_then_else)]
                #[allow(clippy::needless_bool)]
                if cmd.response.get_header().has_error() {
                    // Add some log for skipping the error.
                    false
                } else if cmd.request.has_admin_request() {
                    // Add some log for skipping the admin request.
                    false
                } else {
                    true
                }
            })
            .flat_map(|mut cmd| cmd.request.take_requests().into_iter())
        {
            let cmd_type = req.get_cmd_type();

            let (key, value, cf) = match utils::request_to_triple(req) {
                Either::Left(t) => t,
                Either::Right(req) => {
                    debug!("ignoring unexpected request"; "type" => ?req.get_cmd_type());
                    SKIP_KV_COUNTER.inc();
                    continue;
                }
            };
            if cf == CF_LOCK {
                match cmd_type {
                    CmdType::Put => {
                        match txn_types::parse_lock(&value).map_err(|err| {
                            annotate!(
                                err,
                                "failed to parse lock (value = {})",
                                utils::redact(&value)
                            )
                        }) {
                            Ok(lock_or_shared_locks) => {
                                if let Either::Left(lock) = lock_or_shared_locks {
                                    if utils::should_track_lock(&lock) {
                                        resolver
                                            .track_lock(lock.ts, key, lock.generation)
                                            .map_err(|_| Error::OutOfQuota { region_id })?;
                                    }
                                }
                            }
                            Err(err) => err.report(format!("region id = {}", region_id)),
                        }
                    }
                    CmdType::Delete => resolver.untrack_lock(&key),
                    _ => {}
                }
                continue;
            }
            let item = ApplyEvent {
                key,
                value,
                cf,
                cmd_type,
            };
            if !item.should_record() {
                SKIP_KV_COUNTER.inc();
                continue;
            }
            result.push(item);
        }
        Ok(Self {
            events: result,
            region_id,
            region_resolved_ts: resolver.resolved_ts().into_inner(),
        })
    }

    pub fn push(&mut self, event: ApplyEvent) {
        self.events.push(event);
    }

    pub fn with_capacity(cap: usize, region_id: u64) -> Self {
        Self {
            events: Vec::with_capacity(cap),
            region_id,
            region_resolved_ts: 0,
        }
    }

    pub fn size(&self) -> usize {
        self.events.iter().map(ApplyEvent::size).sum()
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn group_by<T: std::hash::Hash + Clone + Eq, R: Borrow<T>>(
        self,
        mut partition_fn: impl FnMut(&ApplyEvent) -> Option<R>,
    ) -> HashMap<T, Self> {
        let mut result: HashMap<T, Self> = HashMap::new();
        let event_len = self.len();
        for event in self.events {
            if let Some(item) = partition_fn(&event) {
                if let Some(events) = result.get_mut(<R as Borrow<T>>::borrow(&item)) {
                    events.events.push(event);
                } else {
                    result.insert(
                        <R as Borrow<T>>::borrow(&item).clone(),
                        ApplyEvents {
                            events: {
                                // assuming the keys in the same region would probably be in one
                                // group.
                                let mut v = Vec::with_capacity(event_len);
                                v.push(event);
                                v
                            },
                            region_resolved_ts: self.region_resolved_ts,
                            region_id: self.region_id,
                        },
                    );
                }
            }
        }
        result
    }

    fn partition_by_range(self, ranges: &SegmentMap<Vec<u8>, String>) -> HashMap<String, Self> {
        self.group_by(|event| ranges.get_value_by_point(&event.key))
    }

    fn partition_by_table_key(self) -> HashMap<TempFileKey, Self> {
        let region_id = self.region_id;
        self.group_by(move |event| Some(TempFileKey::of(event, region_id)))
    }
}

impl ApplyEvent {
    /// Check whether the key associate to the event is a meta key.
    pub fn is_meta(&self) -> bool {
        // Can we make things not looking so hacky?
        self.key.starts_with(b"m")
    }

    /// Check whether the event should be recorded.
    /// (We would ignore LOCK cf)
    pub fn should_record(&self) -> bool {
        let cf_can_handle = self.cf == CF_DEFAULT || self.cf == CF_WRITE;
        // should we handle prewrite here?
        let cmd_can_handle = self.cmd_type == CmdType::Delete || self.cmd_type == CmdType::Put;
        cf_can_handle && cmd_can_handle
    }

    /// The size of the event.
    pub fn size(&self) -> usize {
        self.key.len() + self.value.len()
    }
}

/// The shared version of router.
#[derive(Debug, Clone)]
pub struct Router(pub(crate) Arc<RouterInner>);

pub struct Config {
    pub prefix: PathBuf,
    pub temp_file_size_limit: u64,
    pub temp_file_memory_quota: u64,
    pub max_flush_interval: Duration,
    pub s3_multi_part_size: usize,
}

impl From<BackupStreamConfig> for Config {
    fn from(value: BackupStreamConfig) -> Self {
        let prefix = PathBuf::from(value.temp_path);
        let temp_file_size_limit = value.file_size_limit.0;
        let temp_file_memory_quota = value.temp_file_memory_quota.0;
        let max_flush_interval = value.max_flush_interval.0;
        let s3_multi_part_size = value.s3_multi_part_size.0 as usize;
        Self {
            prefix,
            temp_file_size_limit,
            temp_file_memory_quota,
            max_flush_interval,
            s3_multi_part_size,
        }
    }
}

impl Router {
    /// Create a new router with the temporary folder.
    pub fn new(
        scheduler: Scheduler<Task>,
        config: Config,
        backup_encryption_manager: BackupEncryptionManager,
    ) -> Self {
        Self(Arc::new(RouterInner::new(
            scheduler,
            config,
            backup_encryption_manager,
        )))
    }
}

impl std::ops::Deref for Router {
    type Target = RouterInner;

    fn deref(&self) -> &Self::Target {
        Arc::deref(&self.0)
    }
}

/// An Router for Backup Stream.
///
/// It works as a table-filter.
///   1. route the kv event to different task
///   2. filter the kv event not belong to the task
// TODO maybe we should introduce table key from tidb_query_datatype module.
pub struct RouterInner {
    // TODO find a proper way to record the ranges of table_filter.
    // TODO replace all map like things with lock free map, to get rid of the Mutex.
    /// The index for search tasks by range.
    /// It uses the `start_key` of range as the key.
    /// Given there isn't overlapping, we can simply use binary search to find
    /// which range a point belongs to.
    ranges: SyncRwLock<SegmentMap<Vec<u8>, String>>,
    /// The temporary files associated to some task.
    tasks: DashMap<String, Arc<StreamTaskHandler>>,
    /// The temporary directory for all tasks.
    prefix: PathBuf,

    /// The handle to Endpoint, we should send `Flush` to endpoint if there are
    /// too many temporary files.
    scheduler: Scheduler<Task>,
    /// The size limit of temporary file per task.
    temp_file_size_limit: AtomicU64,
    temp_file_memory_quota: AtomicU64,
    /// The max duration the local data can be pending.
    max_flush_interval: SyncRwLock<Duration>,

    /// Backup encryption manager
    backup_encryption_manager: BackupEncryptionManager,

    /// S3 multi part size
    s3_multi_part_size: AtomicUsize,
}

impl std::fmt::Debug for RouterInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RouterInner")
            .field("ranges", &self.ranges)
            .field("tasks", &self.tasks)
            .field("prefix", &self.prefix)
            .finish()
    }
}

impl RouterInner {
    pub fn new(
        scheduler: Scheduler<Task>,
        config: Config,
        backup_encryption_manager: BackupEncryptionManager,
    ) -> Self {
        RouterInner {
            ranges: SyncRwLock::new(SegmentMap::default()),
            tasks: DashMap::new(),
            prefix: config.prefix,
            scheduler,
            temp_file_size_limit: AtomicU64::new(config.temp_file_size_limit),
            temp_file_memory_quota: AtomicU64::new(config.temp_file_memory_quota),
            max_flush_interval: SyncRwLock::new(config.max_flush_interval),
            backup_encryption_manager,
            s3_multi_part_size: AtomicUsize::new(config.s3_multi_part_size),
        }
    }

    pub fn update_config(&self, config: &BackupStreamConfig) {
        *self.max_flush_interval.write().unwrap() = config.max_flush_interval.0;
        self.temp_file_size_limit
            .store(config.file_size_limit.0, Ordering::SeqCst);
        self.temp_file_memory_quota
            .store(config.temp_file_memory_quota.0, Ordering::SeqCst);
        for entry in self.tasks.iter() {
            entry
                .temp_file_pool
                .config()
                .cache_size
                .store(config.temp_file_memory_quota.0 as usize, Ordering::SeqCst);
        }
    }

    /// Find the task for a region. If `end_key` is empty, search from start_key
    /// to +inf. It simply search for a random possible overlapping range and
    /// get its task.
    /// FIXME: If a region crosses many tasks, this can only find one of them.
    pub fn find_task_by_range(&self, start_key: &[u8], mut end_key: &[u8]) -> Option<String> {
        let r = self.ranges.rl();
        if end_key.is_empty() {
            end_key = &[0xffu8; 32];
        }
        r.find_overlapping((start_key, end_key))
            .map(|x| x.2.clone())
    }

    /// Register some ranges associated to some task.
    /// Because the observer interface yields encoded data key, the key should
    /// be ENCODED DATA KEY too. (i.e. encoded by
    /// `Key::from_raw(key).into_encoded()`, [`utils::wrap_key`] could be
    /// a shortcut.). We keep ranges in memory to filter kv events not in
    /// these ranges.
    fn register_ranges(&self, task_name: &str, ranges: Vec<(Vec<u8>, Vec<u8>)>) {
        // TODO register ranges to filter kv event
        // register ranges has two main purpose.
        // 1. filter kv event that no need to backup
        // 2. route kv event to the corresponding file.

        let mut w = self.ranges.write().unwrap();
        for range in ranges {
            debug!(
                "backup stream register observe range";
                "task_name" => task_name,
                "start_key" => utils::redact(&range.0),
                "end_key" => utils::redact(&range.1),
            );
            w.insert(range, task_name.to_owned());
        }
    }

    fn unregister_ranges(&self, task_name: &str) {
        let mut ranges = self.ranges.write().unwrap();
        ranges.get_inner().retain(|_, v| v.item != task_name);
    }

    // register task info ans range info to router
    pub async fn register_task(
        &self,
        task: StreamTask,
        ranges: Vec<(Vec<u8>, Vec<u8>)>,
        merged_file_size_limit: u64,
    ) -> Result<()> {
        let task_name = task.info.get_name().to_owned();
        // register task info
        let cfg = self.tempfile_config_for_task(&task);
        let backup_encryption_manager =
            self.build_backup_encryption_manager_for_task(&task).await?;
        let backend_config = BackendConfig {
            s3_multi_part_size: self.s3_multi_part_size.load(Ordering::Relaxed),
            hdfs_config: HdfsConfig::default(),
        };
        let stream_task = StreamTaskHandler::new(
            task,
            ranges.clone(),
            merged_file_size_limit,
            cfg,
            backup_encryption_manager,
            backend_config,
        )
        .await?;
        self.tasks.insert(task_name.clone(), Arc::new(stream_task));

        // register ranges
        self.register_ranges(&task_name, ranges);

        Ok(())
    }

    async fn build_backup_encryption_manager_for_task(
        &self,
        task: &StreamTask,
    ) -> Result<BackupEncryptionManager> {
        if let Some(config) = task.info.security_config.as_ref() {
            if let Some(encryption) = config.encryption.as_ref() {
                match encryption {
                    StreamBackupTaskSecurityConfig_oneof_encryption::PlaintextDataKey(key) => {
                        // sanity check key is valid
                        let opt_key = if !key.cipher_key.is_empty()
                            && (key.cipher_type != EncryptionMethod::Plaintext
                                && key.cipher_type != EncryptionMethod::Unknown)
                        {
                            Some(key.clone())
                        } else {
                            None
                        };
                        Ok(BackupEncryptionManager::new(
                            opt_key,
                            self.backup_encryption_manager
                                .master_key_based_file_encryption_method,
                            self.backup_encryption_manager
                                .multi_master_key_backend
                                .clone(),
                            self.backup_encryption_manager.tikv_data_key_manager.clone(),
                        ))
                    }
                    StreamBackupTaskSecurityConfig_oneof_encryption::MasterKeyConfig(config) => {
                        let multi_master_key_backend = if !config.master_keys.is_empty() {
                            let multi_master_key_backend = MultiMasterKeyBackend::new();
                            multi_master_key_backend
                                .update_from_proto_if_needed(
                                    config.master_keys.to_vec(),
                                    create_async_backend,
                                )
                                .await?;
                            multi_master_key_backend
                        } else {
                            error!(
                                "receive master key config but does not find master key inside, fall back to default"
                            );
                            self.backup_encryption_manager
                                .multi_master_key_backend
                                .clone()
                        };
                        Ok(BackupEncryptionManager::new(
                            None,
                            config.encryption_type,
                            multi_master_key_backend,
                            self.backup_encryption_manager.tikv_data_key_manager.clone(),
                        ))
                    }
                }
            } else {
                Ok(self.backup_encryption_manager.clone())
            }
        } else {
            Ok(self.backup_encryption_manager.clone())
        }
    }

    fn tempfile_config_for_task(&self, task: &StreamTask) -> tempfiles::Config {
        // Note: the scope of this config is per-task. That means, when there are
        // multi tasks, we may need to share the pool over tasks, or at least share the
        // quota between tasks -- but not for now. We don't support that.
        tempfiles::Config {
            // Note: will it be more effective to directly sharing the same atomic value?
            cache_size: AtomicUsize::new(
                self.temp_file_memory_quota.load(Ordering::SeqCst) as usize
            ),
            swap_files: self.prefix.join(task.info.get_name()),
            content_compression: task.info.get_compression_type(),
            minimal_swap_out_file_size: ReadableSize::mb(1).0 as _,
            write_buffer_size: ReadableSize::kb(4).0 as _,
        }
    }

    pub fn unregister_task(&self, task_name: &str) -> Option<StreamBackupTaskInfo> {
        self.tasks.remove(task_name).map(|t| {
            info!(
                "backup stream unregister task";
                "task" => task_name,
            );
            self.unregister_ranges(task_name);
            t.1.task.info.clone()
        })
    }

    /// get the task name by a key.
    pub fn get_task_by_key(&self, key: &[u8]) -> Option<String> {
        let r = self.ranges.read().unwrap();
        r.get_value_by_point(key).cloned()
    }

    pub fn select_task_handler(
        &self,
        selector: TaskSelectorRef<'_>,
    ) -> impl Iterator<Item = Arc<StreamTaskHandler>> {
        self.tasks
            .iter()
            .filter(|entry| {
                let (name, info) = entry.pair();
                selector.matches(
                    name.as_str(),
                    info.ranges
                        .iter()
                        .map(|(s, e)| (s.as_slice(), e.as_slice())),
                )
            })
            .map(|entry| entry.value().clone())
            .collect::<Vec<_>>()
            .into_iter()
    }

    pub fn select_task(&self, selector: TaskSelectorRef<'_>) -> Vec<String> {
        self.tasks
            .iter()
            .filter(|entry| {
                let (name, info) = entry.pair();
                selector.matches(
                    name.as_str(),
                    info.ranges
                        .iter()
                        .map(|(s, e)| (s.as_slice(), e.as_slice())),
                )
            })
            .map(|entry| entry.key().to_owned())
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn must_mut_task_info<F>(&self, task_name: &str, mutator: F)
    where
        F: FnOnce(&mut StreamTaskHandler),
    {
        let t = self.tasks.remove(task_name);
        let mut raw = Arc::try_unwrap(t.unwrap().1).unwrap();
        mutator(&mut raw);
        self.tasks.insert(task_name.to_owned(), Arc::new(raw));
    }

    #[instrument(skip(self))]
    pub fn get_task_handler(&self, task_name: &str) -> Result<Arc<StreamTaskHandler>> {
        let task_handler = match self.tasks.get(task_name) {
            Some(t) => t.clone(),
            None => {
                info!("backup stream no task"; "task" => ?task_name);
                return Err(Error::NoSuchTask {
                    task_name: task_name.to_string(),
                });
            }
        };
        Ok(task_handler)
    }

    #[instrument(skip_all, fields(task))]
    async fn on_events_by_task(&self, task: String, events: ApplyEvents) -> Result<()> {
        let task_handler = self.get_task_handler(&task)?;
        task_handler.on_events(events).await?;
        let file_size_limit = self.temp_file_size_limit.load(Ordering::SeqCst);

        // When this event make the size of temporary files exceeds the size limit, make
        // a flush. Note that we only flush if the size is less than the limit before
        // the event, or we may send multiplied flush requests.
        debug!(
            "backup stream statics size";
            "task" => ?task,
            "next_size" => task_handler.total_size(),
            "size_limit" => file_size_limit,
        );
        let cur_size = task_handler.total_size();
        if cur_size > file_size_limit && !task_handler.is_flushing() {
            info!("try flushing task"; "task" => %task, "size" => %cur_size);
            if task_handler.set_flushing_status_cas(false, true).is_ok() {
                if let Err(e) = self.scheduler.schedule(Task::Flush(task)) {
                    error!("backup stream schedule task failed"; "error" => ?e);
                    task_handler.set_flushing_status(false);
                }
            }
        }
        Ok(())
    }

    pub async fn on_events(&self, kv: ApplyEvents) -> Vec<(String, Result<()>)> {
        use futures::FutureExt;
        HANDLE_KV_HISTOGRAM.observe(kv.len() as _);
        let partitioned_events = kv.partition_by_range(&self.ranges.rl());
        let tasks = partitioned_events.into_iter().map(|(task, events)| {
            self.on_events_by_task(task.clone(), events)
                .map(move |r| (task, r))
        });
        futures::future::join_all(tasks).await
    }

    /// flush the specified task, once success, return the min resolved ts
    /// of this flush. returns `None` if failed.
    #[instrument(skip(self, cx))]
    pub async fn do_flush(&self, cx: FlushContext<'_>) -> Option<u64> {
        let task_handler = match self.get_task_handler(cx.task_name) {
            Ok(h) => h,
            Err(_) => return None,
        };
        let result = task_handler.do_flush(cx).await;
        // set false to flushing whether success or fail
        task_handler.set_flushing_status(false);

        if let Err(e) = result {
            e.report("failed to flush task.");
            warn!("backup steam do flush fail"; "err" => ?e);
            if task_handler.flush_failure_count() > FLUSH_FAILURE_BECOME_FATAL_THRESHOLD {
                // NOTE: Maybe we'd better record all errors and send them to the client?
                try_send!(
                    self.scheduler,
                    Task::FatalError(TaskSelector::ByName(cx.task_name.to_owned()), Box::new(e))
                );
            }
            return None;
        }
        // if succeed in flushing, update flush_time. Or retry do_flush immediately.
        task_handler.update_flush_time();
        result.ok().flatten()
    }

    #[instrument(skip(self))]
    pub async fn update_global_checkpoint(
        &self,
        task_name: &str,
        global_checkpoint: u64,
        store_id: u64,
    ) -> Result<bool> {
        self.get_task_handler(task_name)?
            .update_global_checkpoint(global_checkpoint, store_id)
            .await
    }

    /// tick aims to flush log/meta to extern storage periodically.
    #[instrument(skip_all)]
    pub async fn tick(&self) {
        let max_flush_interval = self.max_flush_interval.rl().to_owned();

        for entry in self.tasks.iter() {
            let name = entry.key();
            let task_info = entry.value();
            if let Err(e) = self
                .scheduler
                .schedule(Task::UpdateGlobalCheckpoint(name.to_string()))
            {
                error!("backup stream schedule task failed"; "error" => ?e);
            }

            // if stream task need flush this time, schedule Task::Flush, or update time
            // justly.
            if task_info.should_flush(&max_flush_interval)
                && task_info.set_flushing_status_cas(false, true).is_ok()
            {
                info!(
                    "backup stream trigger flush task by tick";
                    "task" => ?task_info,
                );

                if let Err(e) = self.scheduler.schedule(Task::Flush(name.clone())) {
                    error!("backup stream schedule task failed"; "error" => ?e);
                    task_info.set_flushing_status(false);
                }
            }
        }
    }
}

/// The handle of a temporary file.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
struct TempFileKey {
    table_id: i64,
    region_id: u64,
    cf: CfName,
    cmd_type: CmdType,
    is_meta: bool,
}

pub enum FormatType {
    Date,
    Hour,
}

impl TempFileKey {
    /// Create the key for an event. The key can be used to find which temporary
    /// file the event should be stored.
    fn of(kv: &ApplyEvent, region_id: u64) -> Self {
        let table_id = if kv.is_meta() {
            // Force table id of meta key be zero.
            0
        } else {
            // When we cannot extract the table key, use 0 for the table key(perhaps we
            // insert meta key here.). Can we elide the copy here(or at least,
            // take a slice of key instead of decoding the whole key)?
            Key::from_encoded_slice(&kv.key)
                .into_raw()
                .ok()
                .and_then(|decoded_key| decode_table_id(&decoded_key).ok())
                .unwrap_or(0)
        };
        Self {
            is_meta: kv.is_meta(),
            table_id,
            region_id,
            cf: kv.cf,
            cmd_type: kv.cmd_type,
        }
    }

    fn get_file_type(&self) -> FileType {
        match self.cmd_type {
            CmdType::Put => FileType::Put,
            CmdType::Delete => FileType::Delete,
            _ => {
                warn!("error cmdtype"; "cmdtype" => ?self.cmd_type);
                panic!("error CmdType");
            }
        }
    }

    /// The full name of the file owns the key.
    #[allow(clippy::redundant_closure_call)]
    fn temp_file_name(&self) -> String {
        let timestamp = (|| {
            fail::fail_point!("temp_file_name_timestamp", |t| t.map_or_else(
                || TimeStamp::physical_now(),
                |v|
                    // reduce the precision of timestamp
                    v.parse::<u64>().ok().map_or(0, |u| TimeStamp::physical_now() / u)
            ));
            TimeStamp::physical_now()
        })();
        let uuid = uuid::Uuid::new_v4();
        if self.is_meta {
            format!(
                "meta_{:08}_{}_{:?}_{:?}_{}.temp.log",
                self.region_id, self.cf, self.cmd_type, uuid, timestamp,
            )
        } else {
            format!(
                "{:08}_{:08}_{}_{:?}_{:?}_{}.temp.log",
                self.table_id, self.region_id, self.cf, self.cmd_type, uuid, timestamp,
            )
        }
    }

    fn format_date_time(ts: u64, t: FormatType) -> impl Display {
        use chrono::prelude::*;
        let millis = TimeStamp::physical(ts.into());
        let dt = Utc.timestamp_millis_opt(millis as _).unwrap();
        match t {
            FormatType::Date => dt.format("%Y%m%d"),
            FormatType::Hour => dt.format("%H"),
        }
    }

    /// path_to_log_file specifies the path of record log for v2.
    /// ```text
    /// V1: v1/${date}/${hour}/${store_id}/t00000071/434098800931373064-f0251bd5-1441-499a-8f53-adc0d1057a73.log
    /// V2: v1/${date}/${hour}/${store_id}/434098800931373064-f0251bd5-1441-499a-8f53-adc0d1057a73.log
    /// ```
    /// For v2, we merged the small files (partition by table_id) into one file.
    fn path_to_log_file(store_id: u64, min_ts: u64, max_ts: u64) -> String {
        format!(
            "v1/{}/{}/{}/{}-{}.log",
            // We may delete a range of files, so using the max_ts for preventing remove some
            // records wrong.
            Self::format_date_time(max_ts, FormatType::Date),
            Self::format_date_time(max_ts, FormatType::Hour),
            store_id,
            min_ts,
            uuid::Uuid::new_v4()
        )
    }

    /// path_to_schema_file specifies the path of schema log for v2.
    /// ```text
    /// V1: v1/${date}/${hour}/${store_id}/schema-meta/434055683656384515-cc3cb7a3-e03b-4434-ab6c-907656fddf67.log
    /// V2: v1/${date}/${hour}/${store_id}/schema-meta/434055683656384515-cc3cb7a3-e03b-4434-ab6c-907656fddf67.log
    /// ```
    /// For v2, we merged the small files (partition by table_id) into one file.
    fn path_to_schema_file(store_id: u64, min_ts: u64, max_ts: u64) -> String {
        format!(
            "v1/{}/{}/{}/schema-meta/{}-{}.log",
            Self::format_date_time(max_ts, FormatType::Date),
            Self::format_date_time(max_ts, FormatType::Hour),
            store_id,
            min_ts,
            uuid::Uuid::new_v4(),
        )
    }

    fn file_name(store_id: u64, min_ts: u64, max_ts: u64, is_meta: bool) -> String {
        if is_meta {
            Self::path_to_schema_file(store_id, min_ts, max_ts)
        } else {
            Self::path_to_log_file(store_id, min_ts, max_ts)
        }
    }
}

/// StreamTaskHandler acts on the events for the backup stream task.
/// It writes the key value pair changes from raft store to local temp files and
/// flushes it to the external storage.
pub struct StreamTaskHandler {
    pub(crate) task: StreamTask,
    /// support external storage. eg local/s3.
    pub(crate) storage: Arc<dyn ExternalStorage>,
    /// The listening range of the task.
    ranges: Vec<(Vec<u8>, Vec<u8>)>,
    // Note: acquiring locks on files, flushing_files, flushing_meta_files at the same time might
    // introduce deadlocks if ordering is not carefully maintained. Better think of another way
    // to make it risk-free.
    /// The temporary file index. Both meta (m prefixed keys) and data (t
    /// prefixed keys).
    files: SlotMap<TempFileKey, DataFile>,
    /// flushing_files contains files pending flush.
    flushing_files: RwLock<Vec<(TempFileKey, DataFile, DataFileInfo)>>,
    /// flushing_meta_files contains meta files pending flush.
    flushing_meta_files: RwLock<Vec<(TempFileKey, DataFile, DataFileInfo)>>,
    /// last_flush_ts represents last time this task flushed to storage.
    last_flush_time: AtomicPtr<Instant>,
    /// The min resolved TS of all regions involved.
    min_resolved_ts: TimeStamp,
    /// Total size of all temporary files in byte.
    total_size: AtomicUsize,
    /// This should only be set to `true` by `compare_and_set(current=false,
    /// value=true)`. The thread who setting it to `true` takes the
    /// responsibility of sending the request to the scheduler for flushing
    /// the files then.
    ///
    /// If the request failed, that thread can set it to `false` back then.
    flushing: AtomicBool,
    /// This counts how many times this task has failed to flush.
    flush_fail_count: AtomicUsize,
    /// global checkpoint ts for this task.
    global_checkpoint_ts: AtomicU64,
    /// The size limit of the merged file for this task.
    merged_file_size_limit: u64,
    /// The pool for holding the temporary files.
    temp_file_pool: Arc<TempFilePool>,
    /// encryption manager to encrypt backup files uploaded
    /// to external storage and local temp files.
    backup_encryption_manager: BackupEncryptionManager,
}

impl Drop for StreamTaskHandler {
    fn drop(&mut self) {
        let (success, failed): (Vec<_>, Vec<_>) = self
            .flushing_files
            .get_mut()
            .drain(..)
            .chain(self.flushing_meta_files.get_mut().drain(..))
            .map(|(_, f, _)| f.inner.path().to_owned())
            .map(|p| self.temp_file_pool.remove(&p))
            .partition(|r| *r);
        info!("stream task handler dropped[1/2], removing flushing_temp files"; "success" => %success.len(), "failure" => %failed.len());
        let (success, failed): (Vec<_>, Vec<_>) = self
            .files
            .get_mut()
            .drain()
            .map(|(_, f)| f.into_inner().inner.path().to_owned())
            .map(|p| self.temp_file_pool.remove(&p))
            .partition(|r| *r);
        info!("stream task handler dropped[2/2], removing temp files"; "success" => %success.len(), "failure" => %failed.len());
    }
}

impl std::fmt::Debug for StreamTaskHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamTaskHandler")
            .field("task", &self.task.info.name)
            .field("min_resolved_ts", &self.min_resolved_ts)
            .field("total_size", &self.total_size)
            .field("flushing", &self.flushing)
            .finish()
    }
}

impl StreamTaskHandler {
    /// Create a new temporary file set at the `temp_dir`.
    pub async fn new(
        task: StreamTask,
        ranges: Vec<(Vec<u8>, Vec<u8>)>,
        merged_file_size_limit: u64,
        temp_pool_cfg: tempfiles::Config,
        backup_encryption_manager: BackupEncryptionManager,
        backend_config: BackendConfig,
    ) -> Result<Self> {
        let temp_dir = &temp_pool_cfg.swap_files;
        tokio::fs::create_dir_all(temp_dir).await?;
        let storage = Arc::from(create_storage(task.info.get_storage(), backend_config)?);
        let start_ts = task.info.get_start_ts();
        Ok(Self {
            task,
            storage,
            ranges,
            min_resolved_ts: TimeStamp::max(),
            files: SlotMap::default(),
            flushing_files: RwLock::default(),
            flushing_meta_files: RwLock::default(),
            last_flush_time: AtomicPtr::new(Box::into_raw(Box::new(Instant::now()))),
            total_size: AtomicUsize::new(0),
            flushing: AtomicBool::new(false),
            flush_fail_count: AtomicUsize::new(0),
            global_checkpoint_ts: AtomicU64::new(start_ts),
            merged_file_size_limit,
            temp_file_pool: Arc::new(TempFilePool::new(
                temp_pool_cfg,
                backup_encryption_manager.clone(),
            )?),
            backup_encryption_manager,
        })
    }

    #[cfg(test)]
    pub fn new_test_only(
        task: StreamTask,
        ranges: Vec<(Vec<u8>, Vec<u8>)>,
        merged_file_size_limit: u64,
        external_storage: Arc<dyn ExternalStorage>,
        pool: Arc<TempFilePool>,
        backup_encryption_manager: BackupEncryptionManager,
    ) -> Result<Self> {
        let start_ts = task.info.get_start_ts();
        Ok(Self {
            task,
            storage: external_storage,
            ranges,
            min_resolved_ts: TimeStamp::max(),
            files: SlotMap::default(),
            flushing_files: RwLock::default(),
            flushing_meta_files: RwLock::default(),
            last_flush_time: AtomicPtr::new(Box::into_raw(Box::new(Instant::now()))),
            total_size: AtomicUsize::new(0),
            flushing: AtomicBool::new(false),
            flush_fail_count: AtomicUsize::new(0),
            global_checkpoint_ts: AtomicU64::new(start_ts),
            merged_file_size_limit,
            temp_file_pool: pool,
            backup_encryption_manager,
        })
    }

    #[instrument(skip(self, events), fields(event_len = events.len()))]
    async fn on_events_of_key(&self, key: TempFileKey, events: ApplyEvents) -> Result<()> {
        fail::fail_point!("before_generate_temp_file");
        if let Some(f) = frame!(self.files.read()).await.get(&key) {
            self.total_size.fetch_add(
                frame!(f.lock()).await.on_events(events).await?,
                Ordering::SeqCst,
            );
            return Ok(());
        }

        // slow path: try to insert the element.
        let mut w = frame!(self.files.write()).await;
        // double check before insert. there may be someone already insert that
        // when we are waiting for the write lock.
        // silence the lint advising us to use the `Entry` API which may introduce
        // copying.
        #[allow(clippy::map_entry)]
        if !w.contains_key(&key) {
            let path = key.temp_file_name();
            let val = Mutex::new(DataFile::new(path, &self.temp_file_pool)?);
            w.insert(key, val);
        }

        let f = w.get(&key).unwrap();
        self.total_size.fetch_add(
            frame!(f.lock()).await.on_events(events).await?,
            Ordering::SeqCst,
        );
        fail::fail_point!("after_write_to_file");
        Ok(())
    }

    /// Append a event to the files. This wouldn't trigger `fsync` syscall.
    /// i.e. No guarantee of persistence.
    #[instrument(skip_all)]
    pub async fn on_events(&self, kv: ApplyEvents) -> Result<()> {
        use futures::FutureExt;
        let now = Instant::now_coarse();
        futures::future::try_join_all(kv.partition_by_table_key().into_iter().map(
            |(key, events)| {
                self.on_events_of_key(key, events)
                    .map(move |r| r.context(format_args!("when handling the file key {:?}", key)))
            },
        ))
        .await?;
        crate::metrics::ON_EVENT_COST_HISTOGRAM
            .with_label_values(&["write_to_tempfile"])
            .observe(now.saturating_elapsed_secs());
        Ok(())
    }

    pub fn get_last_flush_time(&self) -> Instant {
        unsafe { *(self.last_flush_time.load(Ordering::SeqCst) as *const Instant) }
    }

    pub fn total_size(&self) -> u64 {
        self.total_size.load(Ordering::SeqCst) as _
    }

    /// Flush all template files and generate corresponding metadata.
    #[instrument(skip_all)]
    pub async fn generate_backup_metadata(&self, store_id: u64) -> Result<MetadataInfo> {
        let mut w = self.flushing_files.write().await;
        let mut wm = self.flushing_meta_files.write().await;
        // Let's flush all files first...
        futures::future::join_all(
            w.iter_mut()
                .chain(wm.iter_mut())
                .map(|(_, f, _)| f.inner.done()),
        )
        .await
        .into_iter()
        .fold(Ok(()), Result::and)?;

        let mut metadata = MetadataInfo::with_capacity(w.len() + wm.len());
        metadata.set_store_id(store_id);
        // delay push files until log files are flushed
        Ok(metadata)
    }

    fn fill_region_info(&self, cx: FlushContext<'_>, metas: &mut MetadataInfo) {
        let mut rmap = HashMap::<u64, (Vec<&RegionEpoch>, &[u8], &[u8])>::new();
        for res in cx.resolved_regions.resolve_results() {
            rmap.entry(res.region.id)
                .and_modify(|(epoch, start, end)| {
                    epoch.push(res.region.get_region_epoch());
                    if *start > res.region.start_key.as_slice() {
                        *start = &res.region.start_key;
                    }
                    if *end < res.region.end_key.as_slice() {
                        *end = &res.region.end_key;
                    }
                })
                .or_insert({
                    let r = &res.region;
                    (vec![r.get_region_epoch()], &r.start_key, &r.end_key)
                });
        }

        for fg in metas.file_groups.iter_mut() {
            for f in fg.data_files_info.iter_mut() {
                if let Some((epoches, start_key, end_key)) = rmap.get(&(f.region_id as _)) {
                    f.set_region_epoch(epoches.iter().copied().cloned().collect::<Vec<_>>().into());
                    f.set_region_start_key(start_key.to_vec().into());
                    f.set_region_end_key(end_key.to_vec().into());
                }
            }
        }
    }

    pub fn set_flushing_status_cas(&self, expect: bool, new: bool) -> result::Result<bool, bool> {
        self.flushing
            .compare_exchange(expect, new, Ordering::SeqCst, Ordering::SeqCst)
    }

    pub fn set_flushing_status(&self, set_flushing: bool) {
        self.flushing.store(set_flushing, Ordering::SeqCst);
    }

    pub fn update_flush_time(&self) {
        let ptr = self
            .last_flush_time
            .swap(Box::into_raw(Box::new(Instant::now())), Ordering::SeqCst);
        // manual gc last instant
        unsafe {
            let _ = Box::from_raw(ptr);
        };
    }

    pub fn should_flush(&self, flush_interval: &Duration) -> bool {
        // When it doesn't flush since 0.8x of auto-flush interval, we get ready to
        // start flushing. So that we will get a buffer for the cost of actual
        // flushing.
        self.get_last_flush_time().saturating_elapsed_secs() >= flush_interval.as_secs_f64() * 0.8
    }

    pub fn is_flushing(&self) -> bool {
        self.flushing.load(Ordering::SeqCst)
    }

    /// move need-flushing files to flushing_files.
    #[instrument(skip_all)]
    pub async fn move_to_flushing_files(&self) -> Result<&Self> {
        // if flushing_files is not empty, which represents this flush is a retry
        // operation.
        if !self.flushing_files.read().await.is_empty()
            || !self.flushing_meta_files.read().await.is_empty()
        {
            return Ok(self);
        }

        let mut w = frame!(self.files.write()).await;
        let mut fw = frame!(self.flushing_files.write()).await;
        let mut fw_meta = frame!(self.flushing_meta_files.write()).await;
        for (k, v) in w.drain() {
            // we should generate file metadata(calculate sha256) when moving file.
            // because sha256 calculation is a unsafe move operation.
            // we cannot re-calculate it in retry.
            // TODO refactor move_to_flushing_files and generate_metadata
            let mut v = v.into_inner();
            let file_meta = v.generate_metadata(&k)?;
            if file_meta.is_meta {
                fw_meta.push((k, v, file_meta));
            } else {
                fw.push((k, v, file_meta));
            }
        }
        Ok(self)
    }

    #[instrument(skip_all)]
    pub async fn clear_flushing_files(&self) {
        for (_, data_file, _) in self.flushing_files.write().await.drain(..) {
            debug!("removing data file"; "size" => %data_file.file_size, "name" => %data_file.inner.path().display());
            self.total_size
                .fetch_sub(data_file.file_size, Ordering::SeqCst);
            if !self.temp_file_pool.remove(data_file.inner.path()) {
                warn!("Trying to remove file not exists."; "file" => %data_file.inner.path().display());
            }
        }
        for (_, data_file, _) in self.flushing_meta_files.write().await.drain(..) {
            debug!("removing meta data file"; "size" => %data_file.file_size, "name" => %data_file.inner.path().display());
            self.total_size
                .fetch_sub(data_file.file_size, Ordering::SeqCst);
            if !self.temp_file_pool.remove(data_file.inner.path()) {
                warn!("Trying to remove file not exists."; "file" => %data_file.inner.path().display());
            }
        }
    }

    #[instrument(skip_all)]
    async fn merge_and_flush_log_files_to(
        &self,
        files: &mut [(TempFileKey, DataFile, DataFileInfo)],
        metadata: &mut MetadataInfo,
        is_meta: bool,
    ) -> Result<()> {
        let mut data_files_open = Vec::new();
        let mut data_file_infos = Vec::new();
        let mut merged_file_info = DataFileGroup::new();
        let mut stat_length = 0;
        let mut max_ts: Option<u64> = None;
        let mut min_ts: Option<u64> = None;
        let mut min_resolved_ts: Option<u64> = None;
        for (_, data_file, file_info) in files {
            let mut file_info_clone = file_info.to_owned();
            // Update offset of file_info(DataFileInfo)
            //  and push it into merged_file_info(DataFileGroup).
            file_info_clone.set_range_offset(stat_length);
            data_files_open.push({
                // the file content is compressed.
                let file_reader = self
                    .temp_file_pool
                    .open_raw_for_read(data_file.inner.path())
                    .context(format_args!(
                        "failed to open read file {:?}",
                        data_file.inner.path()
                    ))?;
                let compress_length = file_reader.len().await?;
                stat_length += compress_length;
                file_info_clone.set_range_length(compress_length);

                file_reader
            });
            data_file_infos.push(file_info_clone);

            let rts = file_info.resolved_ts;
            min_resolved_ts = min_resolved_ts.map_or(Some(rts), |r| Some(r.min(rts)));
            min_ts = min_ts.map_or(Some(file_info.min_ts), |ts| Some(ts.min(file_info.min_ts)));
            max_ts = max_ts.map_or(Some(file_info.max_ts), |ts| Some(ts.max(file_info.max_ts)));
        }
        let min_ts = min_ts.unwrap_or_default();
        let max_ts = max_ts.unwrap_or_default();
        merged_file_info
            .set_path(TempFileKey::file_name(metadata.store_id, min_ts, max_ts, is_meta).into());
        merged_file_info.set_data_files_info(data_file_infos.into());
        merged_file_info.set_length(stat_length);
        merged_file_info.set_max_ts(max_ts);
        merged_file_info.set_min_ts(min_ts);
        merged_file_info.set_min_resolved_ts(min_resolved_ts.unwrap_or_default());
        let (unpin_reader, file_encryption_info_vec, hasher_vec) = self
            .build_unpin_reader_with_encryption_if_needed(data_files_open)
            .await?;

        let filepath = &merged_file_info.path;

        // flush to external storage
        let ret = self
            .storage
            .write(filepath, unpin_reader, stat_length)
            .await;

        match ret {
            Ok(_) => {
                debug!(
                    "backup stream flush success";
                    "storage_file" => ?filepath,
                    "est_len" => ?stat_length,
                );
            }
            Err(e) => {
                warn!("backup stream flush failed";
                    "est_len" => ?stat_length,
                    "err" => ?e,
                );
                return Err(Error::Io(e));
            }
        }

        // update data file metadata with encryption info and calculated file checksum
        self.update_data_file_metadata(
            &mut merged_file_info,
            hasher_vec,
            file_encryption_info_vec,
        )?;

        // push merged file into metadata
        metadata.push(merged_file_info);
        Ok(())
    }

    #[instrument(skip_all)]
    pub async fn flush_log(&self, backup_metadata: &mut MetadataInfo) -> Result<()> {
        self.merge_and_flush_log(backup_metadata, &self.flushing_files, false)
            .await?;
        self.merge_and_flush_log(backup_metadata, &self.flushing_meta_files, true)
            .await?;
        Ok(())
    }

    #[instrument(skip_all)]
    async fn merge_and_flush_log(
        &self,
        metadata: &mut MetadataInfo,
        files: &RwLock<Vec<(TempFileKey, DataFile, DataFileInfo)>>,
        is_tikv_meta_file: bool,
    ) -> Result<()> {
        let mut files_guard = files.write().await;
        let mut batch_size = 0;
        // file[batch_begin_index, i) is a batch
        let mut batch_begin_index = 0;
        // TODO: upload the merged file concurrently,
        // then collect merged_file_infos and push them into `metadata`.
        for i in 0..files_guard.len() {
            if batch_size >= self.merged_file_size_limit {
                self.merge_and_flush_log_files_to(
                    &mut files_guard[batch_begin_index..i],
                    metadata,
                    is_tikv_meta_file,
                )
                .await?;

                batch_begin_index = i;
                batch_size = 0;
            }

            batch_size += files_guard[i].2.length;
        }
        if batch_begin_index < files_guard.len() {
            self.merge_and_flush_log_files_to(
                &mut files_guard[batch_begin_index..],
                metadata,
                is_tikv_meta_file,
            )
            .await?;
        }

        Ok(())
    }

    #[instrument(skip_all)]
    pub async fn flush_backup_metadata(
        &self,
        metadata_info: MetadataInfo,
        flush_ts: TimeStamp,
    ) -> Result<Vec<String>> {
        let mut meta_files = vec![];
        if !metadata_info.file_groups.is_empty() {
            let mut min_begin_ts = u64::MAX;
            for file_group in metadata_info.file_groups.as_slice() {
                assert!(!file_group.data_files_info.is_empty());
                for d in file_group.data_files_info.as_slice() {
                    assert_ne!(d.min_begin_ts_in_default_cf, 0);
                    min_begin_ts = min_begin_ts.min(d.min_begin_ts_in_default_cf)
                }
            }
            let meta_path = metadata_info.path_to_meta(min_begin_ts, flush_ts.into_inner());
            let meta_buff = metadata_info.marshal_to()?;
            let buflen = meta_buff.len();

            self.storage
                .write(
                    &meta_path,
                    UnpinReader(Box::new(Cursor::new(meta_buff))),
                    buflen as _,
                )
                .await
                .context(format_args!("flush meta {:?}", meta_path))?;
            meta_files.push(meta_path);
        }
        Ok(meta_files)
    }

    /// get the total count of adjacent error.
    pub fn flush_failure_count(&self) -> usize {
        self.flush_fail_count.load(Ordering::SeqCst)
    }

    /// execute the flush: copy local files to external storage.
    /// if success, return the last resolved ts of this flush.
    /// The caller can try to advance the resolved ts and provide it to the
    /// function, and we would use `max(resolved_ts_provided,
    /// resolved_ts_from_file)`.
    #[instrument(skip_all)]
    pub async fn do_flush(&self, cx: FlushContext<'_>) -> Result<Option<u64>> {
        // do nothing if not flushing status.
        let result: Result<Option<u64>> = async move {
            if !self.is_flushing() {
                return Ok(None);
            }
            let begin = Instant::now_coarse();
            let mut sw = StopWatch::by_now();

            // generate backup meta data and prepare to flush to storage
            let mut backup_metadata = self
                .move_to_flushing_files()
                .await?
                .generate_backup_metadata(cx.store_id)
                .await?;

            fail::fail_point!("after_moving_to_flushing_files");
            crate::metrics::FLUSH_DURATION
                .with_label_values(&["generate_metadata"])
                .observe(sw.lap().as_secs_f64());

            // flush log file to storage.
            self.flush_log(&mut backup_metadata).await?;
            // the field `min_resolved_ts` of metadata will be updated
            // only after flush is done.
            backup_metadata.min_resolved_ts = backup_metadata
                .min_resolved_ts
                .max(Some(cx.resolved_ts.into_inner()));
            let rts = backup_metadata.min_resolved_ts;

            // compress length
            let file_size_vec = backup_metadata
                .file_groups
                .iter()
                .map(|d| (d.length, d.data_files_info.len()))
                .collect::<Vec<_>>();

            // flush meta file to storage.
            self.fill_region_info(cx, &mut backup_metadata);
            // flush backup metadata to external storage.
            let meta_files = self
                .flush_backup_metadata(backup_metadata, cx.flush_ts)
                .await?;
            crate::metrics::FLUSH_DURATION
                .with_label_values(&["save_files"])
                .observe(sw.lap().as_secs_f64());

            // clear flushing files
            self.clear_flushing_files().await;
            crate::metrics::FLUSH_DURATION
                .with_label_values(&["clear_temp_files"])
                .observe(sw.lap().as_secs_f64());
            file_size_vec
                .iter()
                .for_each(|(size, _)| crate::metrics::FLUSH_FILE_SIZE.observe(*size as _));
            info!("log backup flush done";
                "meta_files" => ?meta_files,
                "merged_files" => %file_size_vec.len(),    // the number of the merged files
                "files" => %file_size_vec.iter().map(|(_, v)| v).sum::<usize>(),
                "total_size" => %file_size_vec.iter().map(|(v, _)| v).sum::<u64>(), // the size of the merged files after compressed
                "take" => ?begin.saturating_elapsed(),
            );
            Ok(rts)
        }
        .await;

        if result.is_err() {
            self.flush_fail_count.fetch_add(1, Ordering::SeqCst);
        } else {
            self.flush_fail_count.store(0, Ordering::SeqCst);
        }

        result
    }

    pub async fn flush_global_checkpoint(&self, store_id: u64) -> Result<()> {
        let filename = format!("v1/global_checkpoint/{}.ts", store_id);
        let buff = self
            .global_checkpoint_ts
            .load(Ordering::SeqCst)
            .to_le_bytes();
        self.storage
            .write(
                &filename,
                UnpinReader(Box::new(Cursor::new(buff))),
                buff.len() as _,
            )
            .await?;
        Ok(())
    }

    #[instrument(skip_all)]
    pub async fn update_global_checkpoint(
        &self,
        global_checkpoint: u64,
        store_id: u64,
    ) -> Result<bool> {
        let last_global_checkpoint = self.global_checkpoint_ts.load(Ordering::SeqCst);
        if last_global_checkpoint < global_checkpoint {
            let r = self.global_checkpoint_ts.compare_exchange(
                last_global_checkpoint,
                global_checkpoint,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
            if r.is_ok() {
                self.flush_global_checkpoint(store_id).await?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn build_unpin_reader_with_encryption_if_needed(
        &self,
        mut data_file_readers: Vec<ForRead>,
    ) -> Result<(
        UnpinReader<'_>,
        Vec<FileEncryptionInfo>,
        Vec<Arc<std::sync::Mutex<Hasher>>>,
    )> {
        // to do: limiter to storage
        let limiter = Limiter::builder(f64::INFINITY).build();

        // prioritize plaintext key passed from the user, it will override the default
        // master key config if there is any
        //
        if let Some(cipher_info) = self.backup_encryption_manager.plaintext_data_key.as_ref() {
            let mut encrypted_hashing_readers = Vec::new();
            let mut hasher_vec = Vec::new();
            let mut encryption_info_vec = Vec::new();

            let data_key = &cipher_info.cipher_key;
            for data_file_reader in data_file_readers.drain(..) {
                let file_iv = Iv::new_ctr().map_err(|e| {
                    Error::Other(box_err!(
                        "failed to create IV for plaintext data key: {:?}",
                        e
                    ))
                })?;
                let encrypter_reader = EncrypterReader::new(
                    data_file_reader.compat(),
                    cipher_info.cipher_type,
                    &data_key[..],
                    file_iv,
                )
                .map_err(|e| {
                    Error::Other(box_err!(
                        "failed to create encrypted reader for plaintext data key: {:?}",
                        e
                    ))
                })?;
                let (sha256_reader, hasher) = Sha256Reader::new(encrypter_reader.compat())
                    .map_err(|e| {
                        Error::Other(box_err!(
                            "failed to create sha256 reader for plaintext data key: {:?}",
                            e
                        ))
                    })?;

                let mut encryption_info = FileEncryptionInfo::new();
                encryption_info.set_file_iv(file_iv.as_slice().to_vec());
                encryption_info.set_encryption_method(cipher_info.cipher_type);
                encryption_info.set_plain_text_data_key(PlainTextDataKey::new());

                encryption_info_vec.push(encryption_info);
                hasher_vec.push(hasher);
                encrypted_hashing_readers.push(sha256_reader);
            }

            let files_reader = FilesReader::new(encrypted_hashing_readers);
            let unpin_reader = UnpinReader(Box::new(limiter.limit(files_reader.compat())));
            Ok((unpin_reader, encryption_info_vec, hasher_vec))
        } else if self
            .backup_encryption_manager
            .is_master_key_backend_initialized()
            .await
        {
            let mut encrypted_hashing_readers = Vec::new();
            let mut hasher_vec = Vec::new();
            let mut encryption_info_vec = Vec::new();

            let data_key = self
                .backup_encryption_manager
                .generate_data_key()
                .map_err(|e| Error::Other(box_err!("failed to generate data key: {:?}", e)))?;

            let encrypted_data_key = self
                .backup_encryption_manager
                .encrypt_data_key(&data_key)
                .await
                .map_err(|e| Error::Other(box_err!("failed to encrypt data key: {:?}", e)))?;

            // iterate data files readers and wrap with encryption + hashing
            //
            for data_file_reader in data_file_readers.drain(..) {
                let file_iv = Iv::new_ctr().map_err(|e| {
                    Error::Other(box_err!(
                        "failed to create IV for master key based data key: {:?}",
                        e
                    ))
                })?;
                let encrypter_reader = EncrypterReader::new(
                    data_file_reader.compat(),
                    self.backup_encryption_manager
                        .master_key_based_file_encryption_method,
                    &data_key,
                    file_iv,
                )
                .map_err(|e| {
                    Error::Other(box_err!(
                        "failed to create encrypted reader for master key based data key: {:?}",
                        e
                    ))
                })?;
                let (sha256_reader, hasher) = Sha256Reader::new(encrypter_reader.compat())
                    .map_err(|e| {
                        Error::Other(box_err!(
                            "failed to create sha256 reader for master key based data key: {:?}",
                            e
                        ))
                    })?;

                let mut master_key_based_info = MasterKeyBased::new();
                master_key_based_info
                    .set_data_key_encrypted_content(vec![encrypted_data_key.clone()].into());

                let mut encryption_info = FileEncryptionInfo::new();
                encryption_info.set_master_key_based(master_key_based_info);
                encryption_info.set_file_iv(file_iv.as_slice().to_vec());
                encryption_info.set_encryption_method(
                    self.backup_encryption_manager
                        .master_key_based_file_encryption_method,
                );

                encryption_info_vec.push(encryption_info);
                hasher_vec.push(hasher);
                encrypted_hashing_readers.push(sha256_reader);
            }

            let files_reader = FilesReader::new(encrypted_hashing_readers);
            let unpin_reader = UnpinReader(Box::new(limiter.limit(files_reader.compat())));
            Ok((unpin_reader, encryption_info_vec, hasher_vec))
        } else {
            // no encryption
            let files_reader = FilesReader::new(data_file_readers);
            let unpin_reader = UnpinReader(Box::new(limiter.limit(files_reader.compat())));
            Ok((unpin_reader, vec![], vec![]))
        }
    }

    fn update_data_file_metadata(
        &self,
        data_file_meta: &mut DataFileGroup,
        hasher_vec: Vec<Arc<std::sync::Mutex<Hasher>>>,
        encryption_info_vec: Vec<FileEncryptionInfo>,
    ) -> Result<()> {
        // Iterate over the vectors and update the data_file_meta
        for ((meta, hasher), mut encryption_info) in data_file_meta
            .data_files_info
            .iter_mut()
            .zip(hasher_vec.into_iter())
            .zip(encryption_info_vec.into_iter())
        {
            let checksum = hasher
                .lock()
                .unwrap()
                .finish()
                .map(|digest| digest.to_vec())
                .map_err(|e| Error::Other(box_err!("calculate checksum error: {:?}", e)))?;
            encryption_info.set_checksum(checksum);
            meta.set_file_encryption_info(encryption_info);
        }
        Ok(())
    }
}

/// A opened log file with some metadata.
struct DataFile {
    min_ts: TimeStamp,
    max_ts: TimeStamp,
    resolved_ts: TimeStamp,
    min_begin_ts: Option<TimeStamp>,
    // checksum of plaintext kv file , calculated before compression
    sha256: Hasher,
    // TODO: use lz4 with async feature
    inner: tempfiles::ForWrite,
    compression_type: CompressionType,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    number_of_entries: usize,
    file_size: usize,
    crc64xor: u64,
}

#[derive(Debug)]
pub struct MetadataInfo {
    // the field files is deprecated in v6.3.0
    // pub files: Vec<DataFileInfo>,
    pub file_groups: Vec<DataFileGroup>,
    // deprecated
    pub min_resolved_ts: Option<u64>,
    pub min_ts: Option<u64>,
    pub max_ts: Option<u64>,
    pub store_id: u64,
}

impl MetadataInfo {
    fn with_capacity(cap: usize) -> Self {
        Self {
            file_groups: Vec::with_capacity(cap),
            min_resolved_ts: None,
            min_ts: None,
            max_ts: None,
            store_id: 0,
        }
    }

    fn set_store_id(&mut self, store_id: u64) {
        self.store_id = store_id;
    }

    fn push(&mut self, file: DataFileGroup) {
        let rts = file.min_resolved_ts;
        self.min_resolved_ts = self.min_resolved_ts.map_or(Some(rts), |r| Some(r.min(rts)));
        self.min_ts = self
            .min_ts
            .map_or(Some(file.min_ts), |ts| Some(ts.min(file.min_ts)));
        self.max_ts = self
            .max_ts
            .map_or(Some(file.max_ts), |ts| Some(ts.max(file.max_ts)));
        self.file_groups.push(file);
    }

    fn marshal_to(self) -> Result<Vec<u8>> {
        let mut metadata = Metadata::new();
        metadata.set_file_groups(self.file_groups.into());
        metadata.set_store_id(self.store_id as _);
        metadata.set_resolved_ts(self.min_resolved_ts.unwrap_or_default());
        metadata.set_min_ts(self.min_ts.unwrap_or(0));
        metadata.set_max_ts(self.max_ts.unwrap_or(0));
        metadata.set_meta_version(MetaVersion::V2);

        metadata
            .write_to_bytes()
            .map_err(|err| Error::Other(box_err!("failed to marshal proto: {}", err)))
    }

    fn path_to_meta(&self, min_begin_ts: u64, flush_ts: u64) -> String {
        // It is possible for flush_ts to be set to zero when PD is unavailable.
        // In this scenario, a "tso" from the local clock will be synthesized.
        // A special suffix needs to be appended to avoid file name collision.
        let mut actual_flush_ts = flush_ts;
        let suffix = if flush_ts == 0 {
            actual_flush_ts = TimeStamp::compose(TimeStamp::physical_now(), 0).into_inner();
            let uuid = Uuid::new_v4().as_u128();
            format!("-SYNTHETIC{:X}", uuid)
        } else {
            String::new()
        };
        format!(
            "v1/backupmeta/{:016X}-{:016X}-{:016X}-{:016X}{}.meta",
            actual_flush_ts,
            min_begin_ts,
            self.min_ts.unwrap_or_default(),
            self.max_ts.unwrap_or_default(),
            suffix
        )
    }
}

impl DataFile {
    /// create and open a logfile at the path.
    /// Note: if a file with same name exists, would truncate it.
    fn new(local_path: impl AsRef<Path>, files: &Arc<TempFilePool>) -> Result<Self> {
        let sha256 = Hasher::new(MessageDigest::sha256())
            .map_err(|err| Error::Other(box_err!("openssl hasher failed to init: {}", err)))?;
        let inner = files.open_for_write(local_path.as_ref())?;
        Ok(Self {
            min_ts: TimeStamp::max(),
            max_ts: TimeStamp::zero(),
            resolved_ts: TimeStamp::zero(),
            min_begin_ts: None,
            inner,
            compression_type: files.config().content_compression,
            sha256,
            number_of_entries: 0,
            file_size: 0,
            start_key: vec![],
            end_key: vec![],
            crc64xor: 0,
        })
    }

    fn decode_begin_ts(value: Vec<u8>) -> Result<TimeStamp> {
        WriteRef::parse(&value).map_or_else(
            |e| {
                Err(Error::Other(box_err!(
                    "failed to parse write cf value: {}",
                    e
                )))
            },
            |w| Ok(w.start_ts),
        )
    }

    /// Add a new KV pair to the file, returning its size.
    #[instrument(skip_all)]
    async fn on_events(&mut self, events: ApplyEvents) -> Result<usize> {
        let now = Instant::now_coarse();
        let mut total_size = 0;

        for mut event in events.events {
            let mut digest = crc64fast::Digest::new();
            digest.write(&event.key);
            digest.write(&event.value);
            self.crc64xor ^= digest.sum64();
            let encoded = EventEncoder::encode_event(&event.key, &event.value);
            let mut size = 0;
            for slice in encoded {
                let slice = slice.as_ref();
                self.inner.write_all(slice).await?;
                self.sha256.update(slice).map_err(|err| {
                    Error::Other(box_err!("openssl hasher failed to update: {}", err))
                })?;
                size += slice.len();
            }
            let key = Key::from_encoded(std::mem::take(&mut event.key));
            let ts = key.decode_ts().expect("key without ts");
            total_size += size;
            self.min_ts = self.min_ts.min(ts);
            self.max_ts = self.max_ts.max(ts);
            self.resolved_ts = self.resolved_ts.max(events.region_resolved_ts.into());

            // decode_begin_ts is used to maintain the txn when restore log.
            // if value is empty, no need to decode begin_ts.
            if event.cf == CF_WRITE && !event.value.is_empty() {
                let begin_ts = Self::decode_begin_ts(event.value)?;
                self.min_begin_ts = Some(self.min_begin_ts.map_or(begin_ts, |ts| ts.min(begin_ts)));
            }
            self.number_of_entries += 1;
            self.file_size += size;
            self.update_key_bound(key.into_encoded());
        }
        crate::metrics::ON_EVENT_COST_HISTOGRAM
            .with_label_values(&["syscall_write"])
            .observe(now.saturating_elapsed_secs());
        Ok(total_size)
    }

    /// Update the `start_key` and `end_key` of `self` as if a new key added.
    fn update_key_bound(&mut self, key: Vec<u8>) {
        // if there is nothing in file, fill the start_key and end_key by current key.
        if self.start_key.is_empty() && self.end_key.is_empty() {
            self.start_key = key.clone();
            self.end_key = key;
            return;
        }

        // expand the start_key and end_key if key out-of-range joined.
        if self.start_key > key {
            self.start_key = key;
        } else if self.end_key < key {
            self.end_key = key;
        }
    }

    /// generate the metadata v2 where each file becomes a part of the merged
    /// file.
    fn generate_metadata(&mut self, file_key: &TempFileKey) -> Result<DataFileInfo> {
        // Note: the field `storage_path` is empty!!! It will be stored in the upper
        // layer `DataFileGroup`.
        let mut meta = DataFileInfo::new();
        meta.set_sha256(
            self.sha256
                .finish()
                .map(|bytes| bytes.to_vec())
                .map_err(|err| Error::Other(box_err!("openssl hasher failed to init: {}", err)))?
                .into(),
        );
        meta.set_crc64xor(self.crc64xor);
        meta.set_number_of_entries(self.number_of_entries as _);
        meta.set_max_ts(self.max_ts.into_inner() as _);
        meta.set_min_ts(self.min_ts.into_inner() as _);
        meta.set_resolved_ts(self.resolved_ts.into_inner() as _);
        meta.set_min_begin_ts_in_default_cf(
            self.min_begin_ts
                .map_or(self.min_ts.into_inner(), |ts| ts.into_inner()),
        );
        meta.set_start_key(std::mem::take(&mut self.start_key).into());
        meta.set_end_key(std::mem::take(&mut self.end_key).into());
        meta.set_length(self.file_size as _);

        meta.set_is_meta(file_key.is_meta);
        meta.set_table_id(file_key.table_id);
        meta.set_cf(file_key.cf.to_owned().into());
        meta.set_region_id(file_key.region_id as i64);
        meta.set_type(file_key.get_file_type());

        meta.set_compression_type(self.compression_type);

        Ok(meta)
    }
}

impl std::fmt::Debug for DataFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataFile")
            .field("min_ts", &self.min_ts)
            .field("max_ts", &self.max_ts)
            .field("resolved_ts", &self.resolved_ts)
            .field("file_size", &self.file_size)
            .finish()
    }
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
struct TaskRange {
    end: Vec<u8>,
    task_name: String,
}

#[cfg(test)]
mod tests {
    use std::{ffi::OsStr, io, io::Cursor, time::Duration};

    use async_compression::tokio::bufread::ZstdDecoder;
    use encryption::{DecrypterReader, FileConfig, MasterKeyConfig, MultiMasterKeyBackend};
    use external_storage::{BlobObject, ExternalData, NoopStorage};
    use futures::{AsyncReadExt, future::LocalBoxFuture, stream::LocalBoxStream};
    use kvproto::{
        brpb::{CipherInfo, Noop, StorageBackend, StreamBackupTaskInfo},
        encryptionpb::EncryptionMethod,
    };
    use online_config::{ConfigManager, OnlineConfig};
    use rand::Rng;
    use tempfile::TempDir;
    use tikv_util::{
        codec::{
            number::NumberEncoder,
            stream_event::{EventIterator, Iterator},
        },
        config::ReadableDuration,
        worker::{ReceiverWrapper, dummy_scheduler},
    };
    use tokio::{fs::File, io::BufReader};
    use txn_types::{Write, WriteType};

    use super::*;
    use crate::{config::BackupStreamConfigManager, utils};

    static EMPTY_RESOLVE: ResolvedRegions = ResolvedRegions::new(TimeStamp::zero(), vec![]);

    #[derive(Debug)]
    struct KvEventsBuilder {
        events: ApplyEvents,
    }

    fn make_tempfiles_cfg(p: &Path) -> tempfiles::Config {
        tempfiles::Config {
            cache_size: AtomicUsize::new(ReadableSize::mb(512).0 as _),
            swap_files: p.to_owned(),
            content_compression: CompressionType::Zstd,
            minimal_swap_out_file_size: 0,
            write_buffer_size: 0,
        }
    }

    fn make_table_key(table_id: i64, key: &[u8]) -> Vec<u8> {
        use std::io::Write;
        let mut table_key = b"t".to_vec();
        // make it comparable to uint.
        table_key
            .encode_u64(table_id as u64 ^ 0x8000_0000_0000_0000)
            .unwrap();
        Write::write_all(&mut table_key, key).unwrap();
        table_key
    }

    fn make_value(t: WriteType, value: &[u8], start_ts: u64) -> Vec<u8> {
        let start_ts = TimeStamp::new(start_ts);
        let w = Write::new(t, start_ts, Some(value.to_vec()));
        w.as_ref().to_bytes()
    }

    impl KvEventsBuilder {
        fn new(region_id: u64, region_resolved_ts: u64) -> Self {
            Self {
                events: ApplyEvents {
                    events: vec![],
                    region_id,
                    region_resolved_ts,
                },
            }
        }

        fn wrap_key(&self, key: Vec<u8>) -> Vec<u8> {
            let key = Key::from_encoded(utils::wrap_key(key));
            key.append_ts(TimeStamp::compose(
                TimeStamp::physical_now(),
                self.events.len() as _,
            ))
            .into_encoded()
        }

        fn put_event(&mut self, cf: &'static str, key: Vec<u8>, value: Vec<u8>) {
            self.events.push(ApplyEvent {
                key: self.wrap_key(key),
                value,
                cf,
                cmd_type: CmdType::Put,
            })
        }

        fn delete_event(&mut self, cf: &'static str, key: Vec<u8>) {
            self.events.push(ApplyEvent {
                key: self.wrap_key(key),
                value: vec![],
                cf,
                cmd_type: CmdType::Delete,
            })
        }

        fn put_table(&mut self, cf: CfName, table: i64, key: &[u8], value: &[u8]) {
            let table_key = make_table_key(table, key);
            let value = if cf == CF_WRITE {
                make_value(WriteType::Put, value, 12345)
            } else {
                value.to_vec()
            };
            self.put_event(cf, table_key, value);
        }

        fn delete_table(&mut self, cf: &'static str, table: i64, key: &[u8]) {
            let table_key = make_table_key(table, key);
            self.delete_event(cf, table_key);
        }

        fn finish(&mut self) -> ApplyEvents {
            let region_id = self.events.region_id;
            let region_resolved_ts = self.events.region_resolved_ts;
            std::mem::replace(
                &mut self.events,
                ApplyEvents {
                    events: vec![],
                    region_id,
                    region_resolved_ts,
                },
            )
        }
    }

    #[test]
    fn test_register() {
        let (tx, _) = dummy_scheduler();
        let router = RouterInner::new(
            tx,
            Config {
                prefix: PathBuf::new(),
                temp_file_size_limit: 1024,
                temp_file_memory_quota: 1024 * 2,
                max_flush_interval: Duration::from_secs(300),
                s3_multi_part_size: ReadableSize::mb(5).0 as usize,
            },
            BackupEncryptionManager::default(),
        );
        // -----t1.start-----t1.end-----t2.start-----t2.end------
        // --|------------|----------|------------|-----------|--
        // case1        case2      case3        case4       case5
        // None        Found(t1)    None        Found(t2)   None
        router.register_ranges("t1", vec![(vec![1, 2, 3], vec![2, 3, 4])]);

        router.register_ranges("t2", vec![(vec![2, 3, 6], vec![3, 4])]);

        assert_eq!(router.get_task_by_key(&[1, 1, 1]), None);
        assert_eq!(router.get_task_by_key(&[1, 2, 4]), Some("t1".to_string()),);
        assert_eq!(router.get_task_by_key(&[2, 3, 5]), None);
        assert_eq!(router.get_task_by_key(&[2, 4]), Some("t2".to_string()),);
        assert_eq!(router.get_task_by_key(&[4, 4]), None,)
    }

    fn collect_recv(mut rx: ReceiverWrapper<Task>) -> Vec<Task> {
        let mut result = vec![];
        while let Ok(Some(task)) = rx.recv_timeout(Duration::from_secs(0)) {
            result.push(task);
        }
        result
    }

    fn create_noop_storage_backend() -> StorageBackend {
        let nop = Noop::new();
        let mut backend = StorageBackend::default();
        backend.set_noop(nop);
        backend
    }

    async fn task_handler(name: String) -> Result<(StreamBackupTaskInfo, PathBuf)> {
        let mut stream_task = StreamBackupTaskInfo::default();
        stream_task.set_name(name);
        let storage_path = std::env::temp_dir().join(format!("{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&storage_path).await?;
        stream_task.set_storage(external_storage::make_local_backend(storage_path.as_path()));
        Ok((stream_task, storage_path))
    }

    async fn must_register_table(
        router: &RouterInner,
        stream_task: StreamBackupTaskInfo,
        table_id: i64,
    ) {
        router
            .register_task(
                StreamTask {
                    info: stream_task,
                    is_paused: false,
                },
                vec![(
                    utils::wrap_key(make_table_key(table_id, b"")),
                    utils::wrap_key(make_table_key(table_id + 1, b"")),
                )],
                0x100000,
            )
            .await
            .expect("failed to register task")
    }

    fn check_on_events_result(item: &Vec<(String, Result<()>)>) {
        for (task, r) in item {
            if let Err(err) = r {
                warn!("task {} failed: {}", task, err);
            }
        }
    }

    async fn write_simple_data(router: &RouterInner) -> u64 {
        let now = TimeStamp::physical_now();
        let mut region1 = KvEventsBuilder::new(1, now);
        let start_ts = TimeStamp::physical_now();
        region1.put_table(CF_DEFAULT, 1, b"hello", b"world");
        region1.put_table(CF_WRITE, 1, b"hello", b"this isn't a write record :3");
        region1.put_table(CF_WRITE, 1, b"bonjour", b"this isn't a write record :3");
        region1.put_table(CF_WRITE, 1, b"nihao", b"this isn't a write record :3");
        region1.put_table(CF_WRITE, 2, b"hello", b"this isn't a write record :3");
        region1.put_table(CF_WRITE, 1, b"hello", b"still isn't a write record :3");
        region1.delete_table(CF_DEFAULT, 1, b"hello");
        let events = region1.finish();
        check_on_events_result(&router.on_events(events).await);
        start_ts
    }

    #[tokio::test]
    async fn test_basic_file() -> Result<()> {
        let tmp = std::env::temp_dir().join(format!("{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&tmp).await.unwrap();
        let (tx, rx) = dummy_scheduler();
        let router = RouterInner::new(
            tx,
            Config {
                prefix: tmp.clone(),
                temp_file_size_limit: 32,
                temp_file_memory_quota: 32 * 2,
                max_flush_interval: Duration::from_secs(300),
                s3_multi_part_size: ReadableSize::mb(5).0 as usize,
            },
            BackupEncryptionManager::default(),
        );
        let (stream_task, storage_path) = task_handler("dummy".to_owned()).await.unwrap();
        must_register_table(&router, stream_task, 1).await;

        let start_ts = write_simple_data(&router).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let end_ts = TimeStamp::physical_now();
        let task_handler = router.tasks.get("dummy").unwrap().clone();
        let mut meta = task_handler
            .move_to_flushing_files()
            .await
            .unwrap()
            .generate_backup_metadata(1)
            .await
            .unwrap();

        assert!(
            meta.file_groups
                .iter()
                .all(|group| group.data_files_info.iter().all(|item| {
                    TimeStamp::new(item.min_ts as _).physical() >= start_ts
                        && TimeStamp::new(item.max_ts as _).physical() <= end_ts
                        && item.min_ts <= item.max_ts
                })),
            "meta = {:#?}; start ts = {}, end ts = {}",
            meta.file_groups,
            start_ts,
            end_ts
        );

        // in some case when flush failed to write files to storage.
        // we may run `generate_metadata` again with same files.
        let mut another_meta = task_handler
            .move_to_flushing_files()
            .await
            .unwrap()
            .generate_backup_metadata(1)
            .await
            .unwrap();

        task_handler.flush_log(&mut meta).await.unwrap();
        task_handler.flush_log(&mut another_meta).await.unwrap();
        // meta updated
        let files_num = meta
            .file_groups
            .iter()
            .map(|v| v.data_files_info.len())
            .sum::<usize>();
        assert_eq!(files_num, 3, "test file len = {}", files_num);
        for i in 0..meta.file_groups.len() {
            let file_groups1 = meta.file_groups.get(i).unwrap();
            let file_groups2 = another_meta.file_groups.get(i).unwrap();
            // we have to make sure two times sha256 of file must be the same.
            for j in 0..file_groups1.data_files_info.len() {
                let file1 = file_groups1.data_files_info.get(j).unwrap();
                let file2 = file_groups2.data_files_info.get(j).unwrap();
                assert_eq!(file1.sha256, file2.sha256);
                assert_eq!(file1.start_key, file2.start_key);
                assert_eq!(file1.end_key, file2.end_key);
            }
        }

        task_handler
            .flush_backup_metadata(meta, TimeStamp::new(1))
            .await
            .unwrap();
        task_handler.clear_flushing_files().await;

        drop(router);
        let cmds = collect_recv(rx);
        assert_eq!(cmds.len(), 1, "test cmds len = {}", cmds.len());
        match &cmds[0] {
            Task::Flush(task) => assert_eq!(task, "dummy", "task = {}", task),
            _ => warn!("the cmd isn't flush!"),
        }

        let mut meta_count = 0;
        let mut log_count = 0;

        for entry in walkdir::WalkDir::new(storage_path) {
            let entry = entry.unwrap();
            if entry.path().extension() == Some(OsStr::new("meta")) {
                let filename = entry.path().file_stem().unwrap().to_os_string();
                let parts: Vec<&str> = filename.to_str().unwrap().split('-').collect();

                assert!(
                    parts.len() >= 4,
                    "Invalid meta file name format: expected at least 4 parts, got {}, file: {:?}",
                    parts.len(),
                    entry.file_name(),
                );

                for (i, label) in ["flushTs", "minDefaultTs", "minTs", "maxTs"]
                    .iter()
                    .enumerate()
                {
                    let val = u64::from_str_radix(parts[i], 16);
                    assert!(
                        val.is_ok(),
                        "Failed to parse '{}' as u64 (hex) for {} in file name: {:?}",
                        parts[i],
                        label,
                        entry.file_name(),
                    );
                }

                meta_count += 1;
            } else if entry.path().extension() == Some(OsStr::new("log")) {
                log_count += 1;
                let f = entry.metadata().unwrap();
                assert!(
                    f.len() > 10,
                    "the log file {:?} is too small (size = {}B)",
                    entry.file_name(),
                    f.len()
                );
            }
        }

        assert_eq!(meta_count, 1);
        assert_eq!(log_count, 2); // flush twice
        Ok(())
    }

    fn mock_build_large_kv_events(table_id: i64, region_id: u64, resolved_ts: u64) -> ApplyEvents {
        let mut events_builder = KvEventsBuilder::new(region_id, resolved_ts);
        events_builder.put_table(
            "default",
            table_id,
            b"hello",
            "world".repeat(1024).as_bytes(),
        );
        events_builder.finish()
    }

    #[tokio::test]
    async fn test_do_flush() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let backend = external_storage::make_local_backend(tmp_dir.path());
        let mut task_info = StreamBackupTaskInfo::default();
        task_info.set_storage(backend);
        let stream_task = StreamTask {
            info: task_info,
            is_paused: false,
        };
        let merged_file_size_limit = 0x10000;
        let task_handler = StreamTaskHandler::new(
            stream_task,
            vec![(vec![], vec![])],
            merged_file_size_limit,
            make_tempfiles_cfg(tmp_dir.path()),
            BackupEncryptionManager::default(),
            BackendConfig::default(),
        )
        .await
        .unwrap();

        // on_event
        let region_count = merged_file_size_limit / (4 * 1024); // 2 merged log files
        for i in 1..=region_count {
            let kv_events = mock_build_large_kv_events(i as _, i as _, i as _);
            task_handler.on_events(kv_events).await.unwrap();
        }
        // do_flush
        task_handler.set_flushing_status(true);
        let cx = FlushContext {
            task_name: &task_handler.task.info.name,
            store_id: 1,
            resolved_regions: &EMPTY_RESOLVE,
            resolved_ts: TimeStamp::new(1),
            flush_ts: TimeStamp::new(1),
        };
        task_handler.do_flush(cx).await.unwrap();
        assert_eq!(task_handler.flush_failure_count(), 0);
        assert_eq!(task_handler.files.read().await.is_empty(), true);
        assert_eq!(task_handler.flushing_files.read().await.is_empty(), true);

        // assert backup log files
        verify_on_disk_file(tmp_dir.path(), 2, 1);
    }

    struct ErrorStorage<Inner> {
        inner: Inner,
        error_on_write: Box<dyn Fn() -> io::Result<()> + Send + Sync>,
    }

    impl<Inner> ErrorStorage<Inner> {
        fn with_first_time_error(inner: Inner) -> Self {
            let first_time = std::sync::Mutex::new(true);
            Self {
                inner,
                error_on_write: Box::new(move || {
                    let mut fst = first_time.lock().unwrap();
                    if *fst {
                        *fst = false;
                        return Err(io::Error::other(
                            "the absence of the result, is also a kind of result",
                        ));
                    }
                    Ok(())
                }),
            }
        }

        fn with_always_error(inner: Inner) -> Self {
            Self {
                inner,
                error_on_write: Box::new(move || {
                    Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "I won't let you delete my friends!",
                    ))
                }),
            }
        }
    }

    #[async_trait::async_trait]
    impl<Inner: ExternalStorage> ExternalStorage for ErrorStorage<Inner> {
        fn name(&self) -> &'static str {
            self.inner.name()
        }

        fn url(&self) -> io::Result<url::Url> {
            self.inner.url()
        }

        async fn write(
            &self,
            name: &str,
            reader: UnpinReader<'_>,
            content_length: u64,
        ) -> io::Result<()> {
            (self.error_on_write)()?;
            self.inner.write(name, reader, content_length).await
        }

        fn read(&self, name: &str) -> ExternalData<'_> {
            self.inner.read(name)
        }

        fn read_part(&self, name: &str, off: u64, len: u64) -> ExternalData<'_> {
            self.inner.read_part(name, off, len)
        }

        fn iter_prefix(
            &self,
            _prefix: &str,
        ) -> LocalBoxStream<'_, std::result::Result<BlobObject, io::Error>> {
            unreachable!()
        }

        fn delete(&self, _name: &str) -> LocalBoxFuture<'_, io::Result<()>> {
            unreachable!()
        }
    }

    fn build_kv_event(base: i32, count: i32) -> ApplyEvents {
        let mut b = KvEventsBuilder::new(42, 0);
        for i in 0..count {
            let cf = if i % 2 == 0 { CF_WRITE } else { CF_DEFAULT };
            let rnd_key = format!("{:05}", i + base);
            let rnd_value = std::iter::from_fn(|| Some(rand::random::<u8>()))
                .take(rand::random::<usize>() % 32)
                .collect::<Vec<_>>();
            b.put_table(cf, 1, rnd_key.as_bytes(), &rnd_value);
        }
        b.events
    }

    #[tokio::test]
    async fn test_flush_with_error() -> Result<()> {
        let (tx, _rx) = dummy_scheduler();
        let tmp = std::env::temp_dir().join(format!("{}", uuid::Uuid::new_v4()));
        let router = Arc::new(RouterInner::new(
            tx,
            Config {
                prefix: tmp.clone(),
                temp_file_size_limit: 1,
                temp_file_memory_quota: 2,
                max_flush_interval: Duration::from_secs(300),
                s3_multi_part_size: ReadableSize::mb(5).0 as usize,
            },
            BackupEncryptionManager::default(),
        ));
        let cx = FlushContext {
            task_name: "error_prone",
            store_id: 42,
            resolved_regions: &EMPTY_RESOLVE,
            resolved_ts: TimeStamp::max(),
            flush_ts: TimeStamp::max(),
        };
        let (task, _path) = task_handler("error_prone".to_owned()).await?;
        must_register_table(router.as_ref(), task, 1).await;
        router.must_mut_task_info("error_prone", |i| {
            i.storage = Arc::new(ErrorStorage::with_first_time_error(i.storage.clone()))
        });
        check_on_events_result(&router.on_events(build_kv_event(0, 10)).await);
        assert!(router.do_flush(cx).await.is_none());
        check_on_events_result(&router.on_events(build_kv_event(10, 10)).await);
        let t = router.get_task_handler("error_prone").unwrap();
        let _ = router.do_flush(cx).await;
        assert_eq!(t.total_size() > 0, true);

        t.set_flushing_status(true);
        let _ = router.do_flush(cx).await;
        assert_eq!(t.total_size(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_empty_resolved_ts() {
        let (tx, _rx) = dummy_scheduler();
        let tmp = std::env::temp_dir().join(format!("{}", uuid::Uuid::new_v4()));
        let router = RouterInner::new(
            tx,
            Config {
                prefix: tmp.clone(),
                temp_file_size_limit: 32,
                temp_file_memory_quota: 32 * 2,
                max_flush_interval: Duration::from_secs(300),
                s3_multi_part_size: ReadableSize::mb(5).0 as usize,
            },
            BackupEncryptionManager::default(),
        );
        let mut stream_task = StreamBackupTaskInfo::default();
        stream_task.set_name("nothing".to_string());
        stream_task.set_storage(create_noop_storage_backend());

        router
            .register_task(
                StreamTask {
                    info: stream_task,
                    is_paused: false,
                },
                vec![],
                0x100000,
            )
            .await
            .unwrap();
        let task = router.get_task_handler("nothing").unwrap();
        task.set_flushing_status_cas(false, true).unwrap();
        let ts = TimeStamp::compose(TimeStamp::physical_now(), 42);
        let cx = FlushContext {
            task_name: "nothing",
            store_id: 1,
            resolved_regions: &EMPTY_RESOLVE,
            resolved_ts: ts,
            flush_ts: ts,
        };
        let rts = router.do_flush(cx).await.unwrap();
        assert_eq!(ts.into_inner(), rts);
    }

    #[tokio::test]
    async fn test_cleanup_when_stop() -> Result<()> {
        let (tx, _rx) = dummy_scheduler();
        let tmp = std::env::temp_dir().join(format!("{}", uuid::Uuid::new_v4()));
        let router = Arc::new(RouterInner::new(
            tx,
            Config {
                prefix: tmp.clone(),
                temp_file_size_limit: 1,
                temp_file_memory_quota: 2,
                max_flush_interval: Duration::from_secs(300),
                s3_multi_part_size: ReadableSize::mb(5).0 as usize,
            },
            BackupEncryptionManager::default(),
        ));
        let (task, _path) = task_handler("cleanup_test".to_owned()).await?;
        must_register_table(&router, task, 1).await;
        write_simple_data(&router).await;
        let tempfiles = router
            .get_task_handler("cleanup_test")
            .unwrap()
            .temp_file_pool
            .clone();
        router
            .get_task_handler("cleanup_test")?
            .move_to_flushing_files()
            .await?;
        write_simple_data(&router).await;
        let mut w = walkdir::WalkDir::new(&tmp).into_iter();
        assert!(w.next().is_some(), "the temp files doesn't created");
        assert!(tempfiles.mem_used() > 0, "the temp files doesn't created.");
        drop(router);
        let w = walkdir::WalkDir::new(&tmp)
            .into_iter()
            .filter_map(|entry| {
                let e = entry.unwrap();
                e.path()
                    .extension()
                    .filter(|x| x.to_string_lossy() == "log")
                    .map(|_| e.clone())
            })
            .collect::<Vec<_>>();

        assert!(
            w.is_empty(),
            "the temp files should be removed, but it is {:?}",
            w
        );
        assert_eq!(
            tempfiles.mem_used(),
            0,
            "the temp files hasn't been cleared."
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_flush_with_pausing_self() -> Result<()> {
        let (tx, rx) = dummy_scheduler();
        let tmp = std::env::temp_dir().join(format!("{}", uuid::Uuid::new_v4()));
        let router = Arc::new(RouterInner::new(
            tx,
            Config {
                prefix: tmp.clone(),
                temp_file_size_limit: 1,
                temp_file_memory_quota: 2,
                max_flush_interval: Duration::from_secs(300),
                s3_multi_part_size: ReadableSize::mb(5).0 as usize,
            },
            BackupEncryptionManager::default(),
        ));
        let (task, _path) = task_handler("flush_failure".to_owned()).await?;
        must_register_table(router.as_ref(), task, 1).await;
        router.must_mut_task_info("flush_failure", |i| {
            i.storage = Arc::new(ErrorStorage::with_always_error(i.storage.clone()))
        });
        let cx = FlushContext {
            task_name: "flush_failure",
            store_id: 42,
            resolved_regions: &EMPTY_RESOLVE,
            resolved_ts: TimeStamp::zero(),
            flush_ts: TimeStamp::new(1),
        };
        for i in 0..=FLUSH_FAILURE_BECOME_FATAL_THRESHOLD {
            check_on_events_result(&router.on_events(build_kv_event((i * 10) as _, 10)).await);
            assert_eq!(router.do_flush(cx).await, None,);
        }
        let messages = collect_recv(rx);
        assert!(
            messages.iter().any(|task| {
                if let Task::FatalError(name, _err) = task {
                    return matches!(name.reference(), TaskSelectorRef::ByName("flush_failure"));
                }
                false
            }),
            "messages = {:?}",
            messages
        );
        Ok(())
    }

    #[test]
    fn test_format_datetime() {
        let s = TempFileKey::format_date_time(431656320867237891, FormatType::Date);
        let s = s.to_string();
        assert_eq!(s, "20220307");

        let s = TempFileKey::format_date_time(431656320867237891, FormatType::Hour);
        assert_eq!(s.to_string(), "07");
    }

    #[test]
    fn test_decode_begin_ts() {
        let start_ts = TimeStamp::new(12345678);
        let w = Write::new(WriteType::Put, start_ts, Some(b"short_value".to_vec()));
        let value = w.as_ref().to_bytes();

        let begin_ts = DataFile::decode_begin_ts(value).unwrap();
        assert_eq!(begin_ts, start_ts);
    }

    #[test]
    fn test_selector() {
        type DummyTask<'a> = (&'a str, &'a [(&'a [u8], &'a [u8])]);

        #[derive(Debug, Clone, Copy)]
        struct Case<'a /* 'static */> {
            tasks: &'a [DummyTask<'a>],
            selector: TaskSelectorRef<'a>,
            selected: &'a [&'a str],
        }

        let cases = [
            Case {
                tasks: &[("Zhao", &[(b"", b"")]), ("Qian", &[(b"", b"")])],
                selector: TaskSelectorRef::ByName("Zhao"),
                selected: &["Zhao"],
            },
            Case {
                tasks: &[
                    ("Zhao", &[(b"0001", b"1000"), (b"2000", b"")]),
                    ("Qian", &[(b"0002", b"1000")]),
                ],
                selector: TaskSelectorRef::ByKey(b"0001"),
                selected: &["Zhao"],
            },
            Case {
                tasks: &[
                    ("Zhao", &[(b"0001", b"1000"), (b"2000", b"")]),
                    ("Qian", &[(b"0002", b"1000")]),
                    ("Sun", &[(b"0004", b"1024")]),
                    ("Li", &[(b"1001", b"2048")]),
                ],
                selector: TaskSelectorRef::ByRange(b"1001", b"2000"),
                selected: &["Sun", "Li"],
            },
            Case {
                tasks: &[
                    ("Zhao", &[(b"0001", b"1000"), (b"2000", b"")]),
                    ("Qian", &[(b"0002", b"1000")]),
                    ("Sun", &[(b"0004", b"1024")]),
                    ("Li", &[(b"1001", b"2048")]),
                ],
                selector: TaskSelectorRef::All,
                selected: &["Zhao", "Qian", "Sun", "Li"],
            },
        ];

        fn run(c: Case<'static>) {
            assert!(
                c.tasks
                    .iter()
                    .filter(|(name, range)| c.selector.matches(name, range.iter().copied()))
                    .map(|(name, _)| name)
                    .collect::<Vec<_>>()
                    == c.selected.iter().collect::<Vec<_>>(),
                "case = {:?}",
                c
            )
        }

        for case in cases {
            run(case)
        }
    }

    #[tokio::test]
    async fn test_update_global_checkpoint() -> Result<()> {
        // create local storage
        let tmp_dir = tempfile::tempdir().unwrap();
        let backend = external_storage::make_local_backend(tmp_dir.path());
        let backup_encryption_manager = BackupEncryptionManager::default();
        // build a StreamTaskInfo
        let mut task_info = StreamBackupTaskInfo::default();
        task_info.set_storage(backend);
        let stream_task = StreamTask {
            info: task_info,
            is_paused: false,
        };
        let task_handler = StreamTaskHandler::new(
            stream_task,
            vec![(vec![], vec![])],
            0x100000,
            make_tempfiles_cfg(tmp_dir.path()),
            backup_encryption_manager,
            BackendConfig::default(),
        )
        .await
        .unwrap();
        task_handler
            .global_checkpoint_ts
            .store(10001, Ordering::SeqCst);

        // test no need to update global checkpoint
        let store_id = 3;
        let mut global_checkpoint = 10000;
        let is_updated = task_handler
            .update_global_checkpoint(global_checkpoint, store_id)
            .await?;
        assert_eq!(is_updated, false);
        assert_eq!(
            task_handler.global_checkpoint_ts.load(Ordering::SeqCst),
            10001
        );

        // test update global checkpoint
        global_checkpoint = 10002;
        let is_updated = task_handler
            .update_global_checkpoint(global_checkpoint, store_id)
            .await?;
        assert_eq!(is_updated, true);
        assert_eq!(
            task_handler.global_checkpoint_ts.load(Ordering::SeqCst),
            global_checkpoint
        );

        let filename = format!("v1/global_checkpoint/{}.ts", store_id);
        let filepath = tmp_dir.as_ref().join(filename);
        let exist = file_system::file_exists(filepath.clone());
        assert_eq!(exist, true);

        let buff = file_system::read(filepath).unwrap();
        assert_eq!(buff.len(), 8);
        let mut ts = [b'0'; 8];
        ts.copy_from_slice(&buff);
        let ts = u64::from_le_bytes(ts);
        assert_eq!(ts, global_checkpoint);
        Ok(())
    }

    struct MockCheckContentStorage {
        s: Box<dyn ExternalStorage>,
    }

    #[async_trait::async_trait]
    impl ExternalStorage for MockCheckContentStorage {
        fn name(&self) -> &'static str {
            self.s.name()
        }

        fn url(&self) -> io::Result<url::Url> {
            self.s.url()
        }

        async fn write(
            &self,
            _name: &str,
            mut reader: UnpinReader<'_>,
            content_length: u64,
        ) -> io::Result<()> {
            let mut data = Vec::new();
            reader.0.read_to_end(&mut data).await?;
            let data_len: u64 = data.len() as _;

            if data_len == content_length {
                Ok(())
            } else {
                Err(io::Error::other(
                    "the length of content in reader is not equal with content_length",
                ))
            }
        }

        fn read(&self, name: &str) -> ExternalData<'_> {
            self.s.read(name)
        }

        fn read_part(&self, name: &str, off: u64, len: u64) -> ExternalData<'_> {
            self.s.read_part(name, off, len)
        }

        /// Walk the prefix of the blob storage.
        /// It returns the stream of items.
        fn iter_prefix(
            &self,
            _prefix: &str,
        ) -> LocalBoxStream<'_, std::result::Result<BlobObject, io::Error>> {
            unreachable!()
        }

        fn delete(&self, _name: &str) -> LocalBoxFuture<'_, io::Result<()>> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn test_est_len_in_flush() -> Result<()> {
        let noop_s = NoopStorage::default();
        let mock_external_storage = Arc::new(MockCheckContentStorage {
            s: Box::new(noop_s),
        });

        let file_name = format!("{}", uuid::Uuid::new_v4());
        let file_path = Path::new(&file_name);
        let tempfile = TempDir::new().unwrap();
        let cfg = make_tempfiles_cfg(tempfile.path());
        let backup_encryption_manager = BackupEncryptionManager::default();
        let pool = Arc::new(TempFilePool::new(cfg, backup_encryption_manager.clone()).unwrap());
        let mut f = pool.open_for_write(file_path).unwrap();
        f.write_all(b"test-data").await?;
        f.done().await?;
        let mut data_file = DataFile::new(file_path, &pool).unwrap();
        let info = DataFileInfo::new();

        let mut meta = MetadataInfo::with_capacity(1);
        let kv_event = build_kv_event(1, 1);
        let tmp_key = TempFileKey::of(&kv_event.events[0], 1);
        data_file.inner.done().await?;
        let mut files = [(tmp_key, data_file, info)];

        let stream_task = StreamTask {
            info: StreamBackupTaskInfo::default(),
            is_paused: false,
        };
        let stream_task_handler = StreamTaskHandler::new_test_only(
            stream_task,
            vec![(vec![], vec![])],
            0x100000,
            mock_external_storage,
            pool.clone(),
            backup_encryption_manager,
        )
        .unwrap();

        let result = stream_task_handler
            .merge_and_flush_log_files_to(&mut files[0..], &mut meta, false)
            .await;
        result.unwrap();
        Ok(())
    }

    #[test]
    fn test_update_config() {
        let (sched, rx) = dummy_scheduler();
        let cfg = BackupStreamConfig::default();
        let router = Arc::new(RouterInner::new(
            sched.clone(),
            Config {
                prefix: PathBuf::new(),
                temp_file_size_limit: 1,
                temp_file_memory_quota: 2,
                max_flush_interval: cfg.max_flush_interval.0,
                s3_multi_part_size: cfg.s3_multi_part_size.0 as usize,
            },
            BackupEncryptionManager::default(),
        ));

        let mut cfg_manager = BackupStreamConfigManager::new(sched, cfg.clone());

        let _new_cfg = BackupStreamConfig {
            max_flush_interval: ReadableDuration::minutes(2),
            ..Default::default()
        };

        let changed = cfg.diff(&_new_cfg);
        cfg_manager.dispatch(changed).unwrap();

        let cmds = collect_recv(rx);
        assert_eq!(cmds.len(), 1);
        match &cmds[0] {
            Task::ChangeConfig(cfg) => {
                assert!(matches!(cfg, _new_cfg));
                router.update_config(cfg);
                assert_eq!(
                    router.max_flush_interval.rl().to_owned(),
                    _new_cfg.max_flush_interval.0
                );
            }
            _ => panic!("unexpected cmd!"),
        }
    }

    #[test]
    fn test_update_invalid_config() {
        let cfg = BackupStreamConfig::default();
        let (sched, _) = dummy_scheduler();
        let mut cfg_manager = BackupStreamConfigManager::new(sched, cfg.clone());

        let new_cfg = BackupStreamConfig {
            max_flush_interval: ReadableDuration::secs(0),
            ..Default::default()
        };

        let changed = cfg.diff(&new_cfg);
        let r = cfg_manager.dispatch(changed);
        assert!(r.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_flush_on_events_race() -> Result<()> {
        let (tx, _rx) = dummy_scheduler();
        let tmp = std::env::temp_dir().join(format!("{}", uuid::Uuid::new_v4()));
        let router = Arc::new(RouterInner::new(
            tx,
            Config {
                prefix: tmp.clone(),
                // disable auto flush.
                temp_file_size_limit: 1000,
                temp_file_memory_quota: 2,
                max_flush_interval: Duration::from_secs(300),
                s3_multi_part_size: ReadableSize::mb(5).0 as usize,
            },
            BackupEncryptionManager::default(),
        ));

        let (task, _path) = task_handler("race".to_owned()).await?;
        must_register_table(router.as_ref(), task, 1).await;
        router.must_mut_task_info("race", |i| {
            i.storage = Arc::new(NoopStorage::default());
        });
        let mut b = KvEventsBuilder::new(42, 0);
        b.put_table(CF_DEFAULT, 1, b"k1", b"v1");
        let events_before_flush = b.finish();

        b.put_table(CF_DEFAULT, 1, b"k1", b"v1");
        let events_after_flush = b.finish();

        // make timestamp precision to 1 seconds.
        fail::cfg("temp_file_name_timestamp", "return(1000)").unwrap();

        let (trigger_tx, trigger_rx) = std::sync::mpsc::sync_channel(0);
        let trigger_rx = std::sync::Mutex::new(trigger_rx);

        let (fp_tx, fp_rx) = std::sync::mpsc::sync_channel(0);
        let fp_rx = std::sync::Mutex::new(fp_rx);

        let t = router.get_task_handler("race").unwrap();
        let _ = router.on_events(events_before_flush).await;

        // make generate temp files ***happen after*** moving files to flushing_files
        // and read flush file ***happen between*** genenrate file name and
        // write kv to file. T1 is write thread. T2 is flush thread
        // The order likes
        // [T1] generate file name -> [T2] moving files to flushing_files -> [T1] write
        // kv to file -> [T2] read flush file.
        fail::cfg_callback("after_write_to_file", move || {
            fp_tx.send(()).unwrap();
        })
        .unwrap();

        fail::cfg_callback("before_generate_temp_file", move || {
            trigger_rx.lock().unwrap().recv().unwrap();
        })
        .unwrap();

        fail::cfg_callback("after_moving_to_flushing_files", move || {
            trigger_tx.send(()).unwrap();
            fp_rx.lock().unwrap().recv().unwrap();
        })
        .unwrap();

        let cx = FlushContext {
            task_name: "race",
            store_id: 42,
            resolved_regions: &EMPTY_RESOLVE,
            resolved_ts: TimeStamp::max(),
            flush_ts: TimeStamp::new(1),
        };
        // set flush status to true, because we disabled the auto flush.
        t.set_flushing_status(true);
        let router_clone = router.clone();
        let _ = tokio::join!(
            // do flush in another thread
            tokio::spawn(async move {
                router_clone.do_flush(cx).await;
            }),
            router.on_events(events_after_flush)
        );
        fail::remove("after_write_to_file");
        fail::remove("before_generate_temp_file");
        fail::remove("after_moving_to_flushing_files");
        fail::remove("temp_file_name_timestamp");

        // set flush status to true, because we disabled the auto flush.
        t.set_flushing_status(true);
        let res = router.do_flush(cx).await;
        // this time flush should success.
        assert!(res.is_some());
        assert_eq!(t.files.read().await.len(), 0,);
        Ok(())
    }

    #[tokio::test]
    async fn test_encryption_not_set() -> Result<()> {
        test_encryption(BackupEncryptionManager::default()).await
    }

    #[tokio::test]
    async fn test_encryption_plaintext_data_key() -> Result<()> {
        // set up plaintext data key
        //
        let data_key: [u8; 32] = rand::thread_rng().gen();
        let mut cipher = CipherInfo::new();
        cipher.set_cipher_key(data_key.to_vec());
        cipher.set_cipher_type(EncryptionMethod::Aes256Ctr);

        let multi_master_key_backends = MultiMasterKeyBackend::new();
        let backup_encryption_manager = BackupEncryptionManager::new(
            Some(cipher),
            EncryptionMethod::Aes256Ctr,
            multi_master_key_backends,
            None,
        );

        test_encryption(backup_encryption_manager).await
    }
    #[tokio::test]
    async fn test_encryption_master_key_based() -> Result<()> {
        // set up file backed master key
        //
        let hex_bytes = encryption::test_utils::generate_random_master_key();
        let (path, _dir) = encryption::test_utils::create_master_key_file_test_only(&hex_bytes);
        let master_key_config = MasterKeyConfig::File {
            config: FileConfig {
                path: path.to_string_lossy().into_owned(),
            },
        };
        let multi_master_key_backends = MultiMasterKeyBackend::new();
        multi_master_key_backends
            .update_from_config_if_needed(vec![master_key_config], create_async_backend)
            .await?;

        let backup_encryption_manager = BackupEncryptionManager::new(
            None,
            EncryptionMethod::Aes256Ctr,
            multi_master_key_backends,
            None,
        );

        test_encryption(backup_encryption_manager).await
    }

    async fn test_encryption(backup_encryption_manager: BackupEncryptionManager) -> Result<()> {
        // set up local file backend for external storage
        //
        let local_backend_file_path = tempfile::tempdir().unwrap();
        let backend = external_storage::make_local_backend(local_backend_file_path.path());
        let mut task_info = StreamBackupTaskInfo::default();
        task_info.set_storage(backend);
        let stream_task = StreamTask {
            info: task_info,
            is_paused: false,
        };
        let merged_file_size_limit = 0x10000000;

        // configure task handler with optional encryption
        //
        let task_handler = StreamTaskHandler::new(
            stream_task,
            vec![(vec![], vec![])],
            merged_file_size_limit,
            make_tempfiles_cfg(tempfile::tempdir().unwrap().path()),
            backup_encryption_manager.clone(),
            BackendConfig::default(),
        )
        .await
        .unwrap();

        // write some kv into the handler and flush it
        //
        let kv_events = build_kv_event(0, 1000000);
        task_handler.on_events(kv_events.clone()).await?;
        task_handler.set_flushing_status(true);
        let start = Instant::now();
        let cx = FlushContext {
            task_name: &task_handler.task.info.name,
            store_id: 1,
            resolved_regions: &EMPTY_RESOLVE,
            resolved_ts: TimeStamp::new(1),
            flush_ts: TimeStamp::new(1),
        };
        task_handler.do_flush(cx).await?;
        let duration = start.saturating_elapsed();
        println!("Time taken for do_flush: {:?}", duration);

        // verify_on_disk_file(local_backend_file_path.path(), 1, 1);

        // read meta file first to figure out the data file offset
        //
        let meta_file_paths = meta_file_names(local_backend_file_path.path());
        assert_eq!(meta_file_paths.len(), 1);

        let meta_vec = read_and_parse_meta_files(meta_file_paths);
        assert_eq!(meta_vec.len(), 1);

        let meta = meta_vec.first().unwrap();
        let file_group = meta.file_groups.first().unwrap();

        // read log file and parse to kv pairs
        //
        let log_file_paths = log_file_names(local_backend_file_path.path());
        assert_eq!(log_file_paths.len(), 1);

        let mut read_out_kv_pairs = read_and_parse_log_file(
            log_file_paths.first().unwrap(),
            file_group,
            backup_encryption_manager,
        )
        .await;

        // check whether kv pair matches
        //
        let mut expected_kv_pairs = events_to_kv_pair(&kv_events);
        read_out_kv_pairs.sort();
        expected_kv_pairs.sort();
        assert_eq!(read_out_kv_pairs, expected_kv_pairs);
        Ok(())
    }

    fn verify_on_disk_file(path: &Path, num_log: i32, num_backup_meta: i32) {
        let mut meta_count = 0;
        let mut log_count = 0;
        for entry in walkdir::WalkDir::new(path) {
            let entry = entry.unwrap();

            if entry.path().extension() == Some(OsStr::new("meta")) {
                meta_count += 1;
            } else if entry.path().extension() == Some(OsStr::new("log")) {
                log_count += 1;
            }
        }
        assert_eq!(meta_count, num_backup_meta);
        assert_eq!(log_count, num_log);
    }

    fn log_file_names(path: &Path) -> Vec<PathBuf> {
        let mut log_files = Vec::new();
        for entry in walkdir::WalkDir::new(path) {
            let entry = entry.unwrap();
            if entry.path().extension() == Some(OsStr::new("log")) {
                log_files.push(entry.path().to_path_buf());
            }
        }
        log_files
    }

    fn meta_file_names(path: &Path) -> Vec<PathBuf> {
        let mut meta_files = Vec::new();
        for entry in walkdir::WalkDir::new(path) {
            let entry = entry.unwrap();
            if entry.path().extension() == Some(OsStr::new("meta")) {
                if let Some(filename) = entry.path().file_stem().and_then(OsStr::to_str) {
                    // v1/backupmeta/{a}-{b}-{c}-{d}.meta
                    let parts: Vec<&str> = filename.split('-').collect();
                    if parts.len() == 4 {
                        for p in parts {
                            assert!(
                                u64::from_str_radix(p, 16).is_ok(),
                                "Part '{}' is not a valid hex u64",
                                p
                            );
                        }
                        meta_files.push(entry.path().to_path_buf());
                    } else {
                        panic!("backup meta file format changed")
                    }
                }
            }
        }
        meta_files
    }

    fn read_and_parse_meta_files(paths: Vec<PathBuf>) -> Vec<Metadata> {
        let mut meta_vec = Vec::new();
        for path in paths {
            let content = std::fs::read(path).unwrap();
            let metadata = protobuf::parse_from_bytes::<Metadata>(&content).unwrap();
            meta_vec.push(metadata);
        }
        meta_vec
    }

    async fn read_and_parse_log_file(
        path: &Path,
        file_group_meta: &DataFileGroup,
        backup_encryption_manager: BackupEncryptionManager,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        use tokio::io::AsyncReadExt;

        let mut log_file_bytes = Vec::new();

        // read out the entire disk
        //
        let mut bytes_buf = Vec::new();
        BufReader::new(File::open(path).await.unwrap())
            .read_to_end(&mut bytes_buf)
            .await
            .unwrap();
        // find each file length and read it and append to the result
        //
        assert!(!file_group_meta.get_data_files_info().is_empty());
        for file_info in file_group_meta.get_data_files_info() {
            let slice = &bytes_buf[file_info.range_offset as usize
                ..(file_info.range_offset + file_info.range_length) as usize];
            let mut file_buf = Vec::new();
            if let Some(cipher_info) = backup_encryption_manager.plaintext_data_key.as_ref() {
                let iv = Iv::from_slice(&file_info.file_encryption_info.as_ref().unwrap().file_iv)
                    .unwrap();
                let mut decrypter = DecrypterReader::new(
                    BufReader::new(Cursor::new(slice)).compat(),
                    file_info
                        .file_encryption_info
                        .as_ref()
                        .unwrap()
                        .encryption_method,
                    &cipher_info.cipher_key,
                    iv,
                )
                .unwrap();
                let mut decrypted_buf = Vec::new();
                decrypter.read_to_end(&mut decrypted_buf).await.unwrap();
                let mut decoder = ZstdDecoder::new(BufReader::new(Cursor::new(decrypted_buf)));
                decoder.read_to_end(&mut file_buf).await.unwrap();
            } else if backup_encryption_manager
                .is_master_key_backend_initialized()
                .await
            {
                let iv = Iv::from_slice(&file_info.file_encryption_info.as_ref().unwrap().file_iv)
                    .unwrap();
                let cipher_data_key = file_info
                    .file_encryption_info
                    .as_ref()
                    .unwrap()
                    .get_master_key_based()
                    .data_key_encrypted_content
                    .first()
                    .unwrap();
                let plaintext_data_key = backup_encryption_manager
                    .decrypt_data_key(cipher_data_key)
                    .await
                    .unwrap();
                let mut decrypter = DecrypterReader::new(
                    BufReader::new(Cursor::new(slice)).compat(),
                    file_info
                        .file_encryption_info
                        .as_ref()
                        .unwrap()
                        .encryption_method,
                    &plaintext_data_key,
                    iv,
                )
                .unwrap();
                let mut decrypted_buf = Vec::new();
                decrypter.read_to_end(&mut decrypted_buf).await.unwrap();
                let mut decoder = ZstdDecoder::new(BufReader::new(Cursor::new(decrypted_buf)));
                decoder.read_to_end(&mut file_buf).await.unwrap();
            } else {
                let mut decode_reader = ZstdDecoder::new(BufReader::new(Cursor::new(slice)));
                decode_reader.read_to_end(&mut file_buf).await.unwrap();
            }

            // parse to kv pair
            //
            let mut event_iter = EventIterator::new(&file_buf);
            while event_iter.valid() {
                event_iter.next().unwrap();
                let key = event_iter.key();
                let val = event_iter.value();
                log_file_bytes.push((key.to_vec(), val.to_vec()))
            }
        }
        log_file_bytes
    }

    fn events_to_kv_pair(apply_events: &ApplyEvents) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut kv_pairs = Vec::new();
        for apply_event in &apply_events.events {
            kv_pairs.push((apply_event.key.clone(), apply_event.value.clone()));
        }
        kv_pairs
    }

    #[test]
    fn test_path_to_meta() {
        let mut meta = MetadataInfo::with_capacity(0);
        meta.min_ts = Some(100);
        meta.max_ts = Some(200);

        // Case 1: Normal flush_ts
        let path = meta.path_to_meta(50, 300);
        assert_eq!(
            path,
            "v1/backupmeta/000000000000012C-0000000000000032-0000000000000064-00000000000000C8.meta"
        );

        // Case 2: flush_ts is 0
        let path_synthetic = meta.path_to_meta(50, 0);
        let another_path_synthetic = meta.path_to_meta(50, 0);
        // synthetic paths should be unique due to different UUIDs
        assert_ne!(another_path_synthetic, path_synthetic);

        // Verify the synthetic path format with regex
        let synthetic_pattern = regex::Regex::new(
            r"^v1/backupmeta/([0-9A-F]{16})-[0-9A-F]{16}-[0-9A-F]{16}-[0-9A-F]{16}-SYNTHETIC[0-9A-F]+\.meta$"
        ).unwrap();
        let capture = synthetic_pattern
            .captures(&path_synthetic)
            .unwrap_or_else(|| panic!("non match: {}", path_synthetic));

        // Check that the timestamp part is not 0 anymore (it uses physical_now)
        let ts = u64::from_str_radix(capture.get(1).unwrap().as_str(), 16).unwrap();
        assert_ne!(ts, 0);
    }
}
