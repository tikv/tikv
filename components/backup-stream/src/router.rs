// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    borrow::Borrow,
    collections::HashMap,
    fmt::Display,
    io,
    path::{Path, PathBuf},
    result,
    sync::{
        atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering},
        Arc, RwLock as SyncRwLock,
    },
    time::Duration,
};

use engine_traits::{CfName, CF_DEFAULT, CF_LOCK, CF_WRITE};
use external_storage::{BackendConfig, UnpinReader};
use external_storage_export::{create_storage, ExternalStorage};
use futures::io::Cursor;
use kvproto::{
    brpb::{DataFileInfo, FileType, Metadata, StreamBackupTaskInfo},
    raft_cmdpb::CmdType,
};
use openssl::hash::{Hasher, MessageDigest};
use protobuf::Message;
use raftstore::coprocessor::CmdBatch;
use slog_global::debug;
use tidb_query_datatype::codec::table::decode_table_id;
use tikv_util::{
    box_err,
    codec::stream_event::EventEncoder,
    error, info,
    time::{Instant, Limiter},
    warn,
    worker::Scheduler,
    Either, HandyRwLock,
};
use tokio::{
    fs::{remove_file, File},
    io::{AsyncWriteExt, BufWriter},
    sync::{Mutex, RwLock},
};
use tokio_util::compat::TokioAsyncReadCompatExt;
use txn_types::{Key, Lock, TimeStamp, WriteRef};

use super::errors::Result;
use crate::{
    annotate,
    endpoint::Task,
    errors::{ContextualResultExt, Error},
    metadata::StreamTask,
    metrics::{HANDLE_KV_HISTOGRAM, SKIP_KV_COUNTER},
    subscription_track::TwoPhaseResolver,
    try_send,
    utils::{self, SegmentMap, Slot, SlotMap, StopWatch},
};

const FLUSH_FAILURE_BECOME_FATAL_THRESHOLD: usize = 30;

/// FLUSH_LOG_CONCURRENT_BATCH_COUNT specifies the concurrent count to write to storage.
/// 'Log backup' will produce a large mount of small files during flush interval,
/// and storage could take mistaken if writing all of these files to storage concurrently.
const FLUSH_LOG_CONCURRENT_BATCH_COUNT: usize = 128;

#[derive(Clone, Debug)]
pub enum TaskSelector {
    ByName(String),
    ByKey(Vec<u8>),
    ByRange(Vec<u8>, Vec<u8>),
    All,
}

impl TaskSelector {
    pub fn reference(&self) -> TaskSelectorRef<'_> {
        match self {
            TaskSelector::ByName(s) => TaskSelectorRef::ByName(s),
            TaskSelector::ByKey(k) => TaskSelectorRef::ByKey(&*k),
            TaskSelector::ByRange(s, e) => TaskSelectorRef::ByRange(&*s, &*e),
            TaskSelector::All => TaskSelectorRef::All,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum TaskSelectorRef<'a> {
    ByName(&'a str),
    ByKey(&'a [u8]),
    ByRange(&'a [u8], &'a [u8]),
    All,
}

impl<'a> TaskSelectorRef<'a> {
    fn matches<'c, 'd>(
        self,
        task_name: &str,
        mut task_range: impl Iterator<Item = (&'c [u8], &'d [u8])>,
    ) -> bool {
        match self {
            TaskSelectorRef::ByName(name) => task_name == name,
            TaskSelectorRef::ByKey(k) => task_range.any(|(s, e)| utils::is_in_range(k, (&*s, &*e))),
            TaskSelectorRef::ByRange(x1, y1) => {
                task_range.any(|(x2, y2)| utils::is_overlapping((x1, y1), (&*x2, &*y2)))
            }
            TaskSelectorRef::All => true,
        }
    }
}

#[derive(Debug)]
pub struct ApplyEvent {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub cf: CfName,
    pub cmd_type: CmdType,
}

#[derive(Debug)]
pub struct ApplyEvents {
    events: Vec<ApplyEvent>,
    region_id: u64,
    // TODO: this field is useless, maybe remove it.
    region_resolved_ts: u64,
}

impl ApplyEvents {
    /// Convert a [CmdBatch] to a vector of events. Ignoring admin / error commands.
    /// At the same time, advancing status of the `Resolver` by those keys.
    /// Note: the resolved ts cannot be advanced if there is no command,
    ///       maybe we also need to update resolved_ts when flushing?
    pub fn from_cmd_batch(cmd: CmdBatch, resolver: &mut TwoPhaseResolver) -> Self {
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
                        match Lock::parse(&value).map_err(|err| {
                            annotate!(
                                err,
                                "failed to parse lock (value = {})",
                                utils::redact(&value)
                            )
                        }) {
                            Ok(lock) => {
                                if utils::should_track_lock(&lock) {
                                    resolver.track_lock(lock.ts, key)
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
        Self {
            events: result,
            region_id,
            region_resolved_ts: resolver.resolved_ts().into_inner(),
        }
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
                                // assuming the keys in the same region would probably be in one group.
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
pub struct Router(Arc<RouterInner>);

impl Router {
    /// Create a new router with the temporary folder.
    pub fn new(
        prefix: PathBuf,
        scheduler: Scheduler<Task>,
        temp_file_size_limit: u64,
        max_flush_interval: Duration,
    ) -> Self {
        Self(Arc::new(RouterInner::new(
            prefix,
            scheduler,
            temp_file_size_limit,
            max_flush_interval,
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
    tasks: Mutex<HashMap<String, Arc<StreamTaskInfo>>>,
    /// The temporary directory for all tasks.
    prefix: PathBuf,

    /// The handle to Endpoint, we should send `Flush` to endpoint if there are too many temporary files.
    scheduler: Scheduler<Task>,
    /// The size limit of temporary file per task.
    temp_file_size_limit: u64,
    /// The max duration the local data can be pending.
    max_flush_interval: Duration,
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
        prefix: PathBuf,
        scheduler: Scheduler<Task>,
        temp_file_size_limit: u64,
        max_flush_interval: Duration,
    ) -> Self {
        RouterInner {
            ranges: SyncRwLock::new(SegmentMap::default()),
            tasks: Mutex::new(HashMap::default()),
            prefix,
            scheduler,
            temp_file_size_limit,
            max_flush_interval,
        }
    }

    /// Find the task for a region. If `end_key` is empty, search from start_key to +inf.
    /// It simply search for a random possible overlapping range and get its task.
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
    /// Because the observer interface yields encoded data key, the key should be ENCODED DATA KEY too.    
    /// (i.e. encoded by `Key::from_raw(key).into_encoded()`, [`utils::wrap_key`] could be a shortcut.).    
    /// We keep ranges in memory to filter kv events not in these ranges.  
    fn register_ranges(&self, task_name: &str, ranges: Vec<(Vec<u8>, Vec<u8>)>) {
        // TODO reigister ranges to filter kv event
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
        mut task: StreamTask,
        ranges: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<()> {
        let task_name = task.info.take_name();

        // register task info
        let prefix_path = self.prefix.join(&task_name);
        let stream_task =
            StreamTaskInfo::new(prefix_path, task, self.max_flush_interval, ranges.clone()).await?;
        self.tasks
            .lock()
            .await
            .insert(task_name.clone(), Arc::new(stream_task));

        // register ragnes
        self.register_ranges(&task_name, ranges);

        Ok(())
    }

    pub async fn unregister_task(&self, task_name: &str) -> Option<StreamBackupTaskInfo> {
        self.tasks.lock().await.remove(task_name).map(|t| {
            info!(
                "backup stream unregister task";
                "task" => task_name,
            );
            self.unregister_ranges(task_name);
            t.task.info.clone()
        })
    }

    /// get the task name by a key.
    pub fn get_task_by_key(&self, key: &[u8]) -> Option<String> {
        let r = self.ranges.read().unwrap();
        r.get_value_by_point(key).cloned()
    }

    pub async fn select_task(&self, selector: TaskSelectorRef<'_>) -> Vec<String> {
        let s = self.tasks.lock().await;
        s.iter()
            .filter(|(name, info)| {
                selector.matches(
                    name.as_str(),
                    info.ranges
                        .iter()
                        .map(|(s, e)| (s.as_slice(), e.as_slice())),
                )
            })
            .map(|(name, _)| name.to_owned())
            .collect()
    }

    #[cfg(test)]
    pub(crate) async fn must_mut_task_info<F>(&self, task_name: &str, mutator: F)
    where
        F: FnOnce(&mut StreamTaskInfo),
    {
        let mut tasks = self.tasks.lock().await;
        let t = tasks.remove(task_name);
        let mut raw = Arc::try_unwrap(t.unwrap()).unwrap();
        mutator(&mut raw);
        tasks.insert(task_name.to_owned(), Arc::new(raw));
    }

    pub async fn get_task_info(&self, task_name: &str) -> Result<Arc<StreamTaskInfo>> {
        let task_info = match self.tasks.lock().await.get(task_name) {
            Some(t) => t.clone(),
            None => {
                info!("backup stream no task"; "task" => ?task_name);
                return Err(Error::NoSuchTask {
                    task_name: task_name.to_string(),
                });
            }
        };
        Ok(task_info)
    }

    async fn on_event(&self, task: String, events: ApplyEvents) -> Result<()> {
        let task_info = self.get_task_info(&task).await?;
        task_info.on_events(events).await?;

        // When this event make the size of temporary files exceeds the size limit, make a flush.
        // Note that we only flush if the size is less than the limit before the event,
        // or we may send multiplied flush requests.
        debug!(
            "backup stream statics size";
            "task" => ?task,
            "next_size" => task_info.total_size(),
            "size_limit" => self.temp_file_size_limit,
        );
        let cur_size = task_info.total_size();
        if cur_size > self.temp_file_size_limit && !task_info.is_flushing() {
            info!("try flushing task"; "task" => %task, "size" => %cur_size);
            if task_info.set_flushing_status_cas(false, true).is_ok() {
                if let Err(e) = self.scheduler.schedule(Task::Flush(task)) {
                    error!("backup stream schedule task failed"; "error" => ?e);
                    task_info.set_flushing_status(false);
                }
            }
        }
        Ok(())
    }

    pub async fn on_events(&self, kv: ApplyEvents) -> Vec<(String, Result<()>)> {
        use futures::FutureExt;
        HANDLE_KV_HISTOGRAM.observe(kv.len() as _);
        let partitioned_events = kv.partition_by_range(&self.ranges.rl());
        let tasks = partitioned_events
            .into_iter()
            .map(|(task, events)| self.on_event(task.clone(), events).map(move |r| (task, r)));
        futures::future::join_all(tasks).await
    }

    /// flush the specified task, once once success, return the min resolved ts of this flush.
    /// returns `None` if failed.
    pub async fn do_flush(
        &self,
        task_name: &str,
        store_id: u64,
        resolve_to: TimeStamp,
    ) -> Option<u64> {
        let task = self.tasks.lock().await.get(task_name).cloned();
        match task {
            Some(task_info) => {
                let result = task_info.do_flush(store_id, resolve_to).await;
                // set false to flushing whether success or fail
                task_info.set_flushing_status(false);

                if let Err(e) = result {
                    e.report("failed to flush task.");
                    warn!("backup steam do flush fail"; "err" => ?e);
                    if task_info.flush_failure_count() > FLUSH_FAILURE_BECOME_FATAL_THRESHOLD {
                        // NOTE: Maybe we'd better record all errors and send them to the client?
                        try_send!(
                            self.scheduler,
                            Task::FatalError(
                                TaskSelector::ByName(task_name.to_owned()),
                                Box::new(e)
                            )
                        );
                    }
                    return None;
                }
                // if succeed in flushing, update flush_time. Or retry do_flush immediately.
                task_info.update_flush_time();
                result.ok().flatten()
            }
            _ => None,
        }
    }

    pub async fn update_global_checkpoint(
        &self,
        task_name: &str,
        global_checkpoint: u64,
        store_id: u64,
    ) -> Result<bool> {
        self.get_task_info(task_name)
            .await?
            .update_global_checkpoint(global_checkpoint, store_id)
            .await
    }

    /// tick aims to flush log/meta to extern storage periodically.
    pub async fn tick(&self) {
        for (name, task_info) in self.tasks.lock().await.iter() {
            if let Err(e) = self
                .scheduler
                .schedule(Task::UpdateGlobalCheckpoint(name.to_string()))
            {
                error!("backup stream schedule task failed"; "error" => ?e);
            }

            // if stream task need flush this time, schedule Task::Flush, or update time justly.
            if task_info.should_flush() && task_info.set_flushing_status_cas(false, true).is_ok() {
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
    /// Create the key for an event. The key can be used to find which temporary file the event should be stored.
    fn of(kv: &ApplyEvent, region_id: u64) -> Self {
        let table_id = if kv.is_meta() {
            // Force table id of meta key be zero.
            0
        } else {
            // When we cannot extract the table key, use 0 for the table key(perhaps we insert meta key here.).
            // Can we elide the copy here(or at least, take a slice of key instead of decoding the whole key)?
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
        let file_type = match self.cmd_type {
            CmdType::Put => FileType::Put,
            CmdType::Delete => FileType::Delete,
            _ => {
                warn!("error cmdtype"; "cmdtype" => ?self.cmd_type);
                panic!("error CmdType");
            }
        };
        file_type
    }

    /// The full name of the file owns the key.
    fn temp_file_name(&self) -> String {
        if self.is_meta {
            format!(
                "meta_{:08}_{}_{:?}_{}.temp.log",
                self.region_id,
                self.cf,
                self.cmd_type,
                TimeStamp::physical_now(),
            )
        } else {
            format!(
                "{:08}_{:08}_{}_{:?}_{}.temp.log",
                self.table_id,
                self.region_id,
                self.cf,
                self.cmd_type,
                TimeStamp::physical_now(),
            )
        }
    }

    fn format_date_time(ts: u64, t: FormatType) -> impl Display {
        use chrono::prelude::*;
        let millis = TimeStamp::physical(ts.into());
        let dt = Utc.timestamp_millis(millis as _);

        #[cfg(feature = "failpoints")]
        {
            fail::fail_point!("stream_format_date_time", |s| {
                return dt
                    .format(&s.unwrap_or_else(|| "%Y%m".to_owned()))
                    .to_string();
            });
            match t {
                FormatType::Date => dt.format("%Y%m%d").to_string(),
                FormatType::Hour => dt.format("%H").to_string(),
            }
        }
        #[cfg(not(feature = "failpoints"))]
        match t {
            FormatType::Date => dt.format("%Y%m%d"),
            FormatType::Hour => dt.format("%H"),
        }
    }

    /// path_to_log_file specifies the path of record log.
    /// eg. "v1/${date}/${hour}/${store_id}/t00000071/434098800931373064-f0251bd5-1441-499a-8f53-adc0d1057a73.log"
    fn path_to_log_file(&self, store_id: u64, min_ts: u64, max_ts: u64) -> String {
        format!(
            "v1/{}/{}/{}/t{:08}/{:012}-{}.log",
            // We may delete a range of files, so using the max_ts for preventing remove some records wrong.
            Self::format_date_time(max_ts, FormatType::Date),
            Self::format_date_time(max_ts, FormatType::Hour),
            store_id,
            self.table_id,
            min_ts,
            uuid::Uuid::new_v4()
        )
    }

    /// path_to_schema_file specifies the path of schema log.
    /// eg. "v1/${date}/${hour}/${store_id}/schema-meta/434055683656384515-cc3cb7a3-e03b-4434-ab6c-907656fddf67.log"
    fn path_to_schema_file(store_id: u64, min_ts: u64, max_ts: u64) -> String {
        format!(
            "v1/{}/{}/{}/schema-meta/{:012}-{}.log",
            Self::format_date_time(max_ts, FormatType::Date),
            Self::format_date_time(max_ts, FormatType::Hour),
            store_id,
            min_ts,
            uuid::Uuid::new_v4(),
        )
    }

    fn file_name(&self, store_id: u64, min_ts: TimeStamp, max_ts: TimeStamp) -> String {
        if self.is_meta {
            Self::path_to_schema_file(store_id, min_ts.into_inner(), max_ts.into_inner())
        } else {
            self.path_to_log_file(store_id, min_ts.into_inner(), max_ts.into_inner())
        }
    }
}

pub struct StreamTaskInfo {
    pub(crate) task: StreamTask,
    /// support external storage. eg local/s3.
    pub(crate) storage: Arc<dyn ExternalStorage>,
    /// The listening range of the task.
    ranges: Vec<(Vec<u8>, Vec<u8>)>,
    /// The parent directory of temporary files.
    temp_dir: PathBuf,
    /// The temporary file index. Both meta (m prefixed keys) and data (t prefixed keys).
    files: SlotMap<TempFileKey, DataFile>,
    /// flushing_files contains files pending flush.
    flushing_files: RwLock<Vec<(TempFileKey, Slot<DataFile>, DataFileInfo)>>,
    /// last_flush_ts represents last time this task flushed to storage.
    last_flush_time: AtomicPtr<Instant>,
    /// flush_interval represents the tick interval of flush, setting by users.
    flush_interval: Duration,
    /// The min resolved TS of all regions involved.
    min_resolved_ts: TimeStamp,
    /// Total size of all temporary files in byte.
    total_size: AtomicUsize,
    /// This should only be set to `true` by `compare_and_set(current=false, value=ture)`.
    /// The thread who setting it to `true` takes the responsibility of sending the request to the
    /// scheduler for flushing the files then.
    ///
    /// If the request failed, that thread can set it to `false` back then.
    flushing: AtomicBool,
    /// This counts how many times this task has failed to flush.
    flush_fail_count: AtomicUsize,
    /// global checkpoint ts for this task.
    global_checkpoint_ts: AtomicU64,
}

impl Drop for StreamTaskInfo {
    fn drop(&mut self) {
        let (success, failed): (Vec<_>, Vec<_>) = self
            .flushing_files
            .get_mut()
            .drain(..)
            .map(|(a, b, _)| (a, b))
            .chain(self.files.get_mut().drain())
            .map(|(_, f)| f.into_inner().local_path)
            .map(std::fs::remove_file)
            .partition(|r| r.is_ok());
        info!("stream task info dropped, removing temp files"; "success" => %success.len(), "failure" => %failed.len())
    }
}

impl std::fmt::Debug for StreamTaskInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamTaskInfo")
            .field("task", &self.task.info.name)
            .field("temp_dir", &self.temp_dir)
            .field("min_resolved_ts", &self.min_resolved_ts)
            .field("total_size", &self.total_size)
            .field("flushing", &self.flushing)
            .finish()
    }
}

impl StreamTaskInfo {
    /// Create a new temporary file set at the `temp_dir`.
    pub async fn new(
        temp_dir: PathBuf,
        task: StreamTask,
        flush_interval: Duration,
        ranges: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<Self> {
        tokio::fs::create_dir_all(&temp_dir).await?;
        let storage = Arc::from(create_storage(
            task.info.get_storage(),
            BackendConfig::default(),
        )?);
        let start_ts = task.info.get_start_ts();
        Ok(Self {
            task,
            storage,
            temp_dir,
            ranges,
            min_resolved_ts: TimeStamp::max(),
            files: SlotMap::default(),
            flushing_files: RwLock::default(),
            last_flush_time: AtomicPtr::new(Box::into_raw(Box::new(Instant::now()))),
            flush_interval,
            total_size: AtomicUsize::new(0),
            flushing: AtomicBool::new(false),
            flush_fail_count: AtomicUsize::new(0),
            global_checkpoint_ts: AtomicU64::new(start_ts),
        })
    }

    async fn on_events_of_key(&self, key: TempFileKey, events: ApplyEvents) -> Result<()> {
        if let Some(f) = self.files.read().await.get(&key) {
            self.total_size
                .fetch_add(f.lock().await.on_events(events).await?, Ordering::SeqCst);
            return Ok(());
        }

        // slow path: try to insert the element.
        let mut w = self.files.write().await;
        // double check before insert. there may be someone already insert that
        // when we are waiting for the write lock.
        // slience the lint advising us to use the `Entry` API which may introduce copying.
        #[allow(clippy::map_entry)]
        if !w.contains_key(&key) {
            let path = self.temp_dir.join(key.temp_file_name());
            let val = Mutex::new(DataFile::new(path).await?);
            w.insert(key, val);
        }

        let f = w.get(&key).unwrap();
        self.total_size
            .fetch_add(f.lock().await.on_events(events).await?, Ordering::SeqCst);
        Ok(())
    }

    /// Append a event to the files. This wouldn't trigger `fsync` syscall.
    /// i.e. No guarantee of persistence.
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
    pub async fn generate_metadata(&self, store_id: u64) -> Result<MetadataInfo> {
        let w = self.flushing_files.read().await;
        // Let's flush all files first...
        futures::future::join_all(w.iter().map(|(_, f, _)| async move {
            let file = &mut f.lock().await.inner;
            file.flush().await?;
            file.get_ref().sync_all().await?;
            Result::Ok(())
        }))
        .await
        .into_iter()
        .map(|r| r.map_err(Error::from))
        .fold(Ok(()), Result::and)?;

        let mut metadata = MetadataInfo::with_capacity(w.len());
        metadata.set_store_id(store_id);
        for (_, _, file_meta) in w.iter() {
            metadata.push(file_meta.to_owned())
        }
        Ok(metadata)
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
        unsafe { Box::from_raw(ptr) };
    }

    pub fn should_flush(&self) -> bool {
        // When it doesn't flush since 0.8x of auto-flush interval, we get ready to start flushing.
        // So that we will get a buffer for the cost of actual flushing.
        self.get_last_flush_time().saturating_elapsed_secs()
            >= self.flush_interval.as_secs_f64() * 0.8
    }

    pub fn is_flushing(&self) -> bool {
        self.flushing.load(Ordering::SeqCst)
    }

    /// move need-flushing files to flushing_files.
    pub async fn move_to_flushing_files(&self, store_id: u64) -> Result<&Self> {
        // if flushing_files is not empty, which represents this flush is a retry operation.
        if !self.flushing_files.read().await.is_empty() {
            return Ok(self);
        }

        let mut w = self.files.write().await;
        let mut fw = self.flushing_files.write().await;
        for (k, v) in w.drain() {
            // we should generate file metadata(calculate sha256) when moving file.
            // because sha256 calculation is a unsafe move operation.
            // we cannot re-calculate it in retry.
            // TODO refactor move_to_flushing_files and generate_metadata
            let file_meta = v.lock().await.generate_metadata(&k, store_id)?;
            fw.push((k, v, file_meta));
        }
        Ok(self)
    }

    pub async fn clear_flushing_files(&self) {
        for (_, v, _) in self.flushing_files.write().await.drain(..) {
            let data_file = v.lock().await;
            debug!("removing data file"; "size" => %data_file.file_size, "name" => %data_file.local_path.display());
            self.total_size
                .fetch_sub(data_file.file_size, Ordering::SeqCst);
            if let Err(e) = data_file.remove_temp_file().await {
                // if remove template failed, just skip it.
                info!("remove template file"; "err" => ?e);
            }
        }
    }

    async fn flush_log_file_to(
        storage: Arc<dyn ExternalStorage>,
        file: &Mutex<DataFile>,
    ) -> Result<()> {
        let data_file = file.lock().await;
        // to do: limiter to storage
        let limiter = Limiter::builder(std::f64::INFINITY).build();
        let reader = File::open(data_file.local_path.clone()).await?;
        let stat = reader.metadata().await?;
        let reader = UnpinReader(Box::new(limiter.limit(reader.compat())));
        let filepath = &data_file.storage_path;
        // Once we cannot get the stat of the file, use 4K I/O.
        let est_len = stat.len().max(4096);

        let ret = storage.write(filepath, reader, est_len).await;
        match ret {
            Ok(_) => {
                debug!(
                    "backup stream flush success";
                    "tmp file" => ?data_file.local_path,
                    "storage file" => ?filepath,
                );
            }
            Err(e) => {
                warn!("backup stream flush failed";
                    "file" => ?data_file.local_path,
                    "est_len" => ?est_len,
                    "err" => ?e,
                );
                return Err(Error::Io(e));
            }
        }
        Ok(())
    }

    pub async fn flush_log(&self) -> Result<()> {
        // if failed to write storage, we should retry write flushing_files.
        let storage = self.storage.clone();
        let files = self.flushing_files.write().await;

        for batch_files in files.chunks(FLUSH_LOG_CONCURRENT_BATCH_COUNT) {
            let futs = batch_files
                .iter()
                .map(|(_, v, _)| Self::flush_log_file_to(storage.clone(), v));
            futures::future::try_join_all(futs).await?;
        }

        Ok(())
    }

    pub async fn flush_meta(&self, metadata_info: MetadataInfo) -> Result<()> {
        let meta_path = metadata_info.path_to_meta();
        let meta_buff = metadata_info.marshal_to()?;
        let buflen = meta_buff.len();

        self.storage
            .write(
                &meta_path,
                UnpinReader(Box::new(Cursor::new(meta_buff))),
                buflen as _,
            )
            .await?;
        Ok(())
    }

    /// get the total count of adjacent error.
    pub fn flush_failure_count(&self) -> usize {
        self.flush_fail_count.load(Ordering::SeqCst)
    }

    /// execute the flush: copy local files to external storage.
    /// if success, return the last resolved ts of this flush.
    /// The caller can try to advance the resolved ts and provide it to the function,
    /// and we would use max(resolved_ts_provided, resolved_ts_from_file).
    pub async fn do_flush(
        &self,
        store_id: u64,
        resolved_ts_provided: TimeStamp,
    ) -> Result<Option<u64>> {
        // do nothing if not flushing status.
        let result: Result<Option<u64>> = async move {
            if !self.is_flushing() {
                return Ok(None);
            }
            let begin = Instant::now_coarse();
            let mut sw = StopWatch::new();

            // generate meta data and prepare to flush to storage
            let mut metadata_info = self
                .move_to_flushing_files(store_id)
                .await?
                .generate_metadata(store_id)
                .await?;
            metadata_info.min_resolved_ts = metadata_info
                .min_resolved_ts
                .max(Some(resolved_ts_provided.into_inner()));
            let rts = metadata_info.min_resolved_ts;
            crate::metrics::FLUSH_DURATION
                .with_label_values(&["generate_metadata"])
                .observe(sw.lap().as_secs_f64());

            // flush log file to storage.
            self.flush_log().await?;

            let file_size_vec = metadata_info
                .files
                .iter()
                .map(|d| d.length)
                .collect::<Vec<_>>();
            // flush meta file to storage.
            self.flush_meta(metadata_info).await?;
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
                .for_each(|size| crate::metrics::FLUSH_FILE_SIZE.observe(*size as _));
            info!("log backup flush done";
                "files" => %file_size_vec.len(),
                "total_size" => %file_size_vec.iter().sum::<u64>(),
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
}

/// A opened log file with some metadata.
struct DataFile {
    min_ts: TimeStamp,
    max_ts: TimeStamp,
    resolved_ts: TimeStamp,
    min_begin_ts: Option<TimeStamp>,
    sha256: Hasher,
    inner: BufWriter<File>,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    number_of_entries: usize,
    file_size: usize,
    local_path: PathBuf,
    storage_path: String,
}

#[derive(Debug)]
pub struct MetadataInfo {
    pub files: Vec<DataFileInfo>,
    pub min_resolved_ts: Option<u64>,
    pub min_ts: Option<u64>,
    pub max_ts: Option<u64>,
    pub store_id: u64,
}

impl MetadataInfo {
    fn with_capacity(cap: usize) -> Self {
        Self {
            files: Vec::with_capacity(cap),
            min_resolved_ts: None,
            min_ts: None,
            max_ts: None,
            store_id: 0,
        }
    }

    fn set_store_id(&mut self, store_id: u64) {
        self.store_id = store_id;
    }

    fn push(&mut self, file: DataFileInfo) {
        let rts = file.resolved_ts;
        self.min_resolved_ts = self.min_resolved_ts.map_or(Some(rts), |r| Some(r.min(rts)));
        self.min_ts = self
            .min_ts
            .map_or(Some(file.min_ts), |ts| Some(ts.min(file.min_ts)));
        self.max_ts = self
            .max_ts
            .map_or(Some(file.max_ts), |ts| Some(ts.max(file.max_ts)));
        self.files.push(file);
    }

    fn marshal_to(self) -> Result<Vec<u8>> {
        let mut metadata = Metadata::new();
        metadata.set_files(self.files.into());
        metadata.set_store_id(self.store_id as _);
        metadata.set_resolved_ts(self.min_resolved_ts.unwrap_or_default());
        metadata.set_min_ts(self.min_ts.unwrap_or(0));
        metadata.set_max_ts(self.max_ts.unwrap_or(0));

        metadata
            .write_to_bytes()
            .map_err(|err| Error::Other(box_err!("failed to marshal proto: {}", err)))
    }

    fn path_to_meta(&self) -> String {
        format!(
            "v1/backupmeta/{:012}-{}.meta",
            self.min_resolved_ts.unwrap_or_default(),
            uuid::Uuid::new_v4()
        )
    }
}

impl DataFile {
    /// create and open a logfile at the path.
    /// Note: if a file with same name exists, would truncate it.
    async fn new(local_path: impl AsRef<Path>) -> Result<Self> {
        let sha256 = Hasher::new(MessageDigest::sha256())
            .map_err(|err| Error::Other(box_err!("openssl hasher failed to init: {}", err)))?;
        Ok(Self {
            min_ts: TimeStamp::max(),
            max_ts: TimeStamp::zero(),
            resolved_ts: TimeStamp::zero(),
            min_begin_ts: None,
            inner: BufWriter::with_capacity(128 * 1024, File::create(local_path.as_ref()).await?),
            sha256,
            number_of_entries: 0,
            file_size: 0,
            start_key: vec![],
            end_key: vec![],
            local_path: local_path.as_ref().to_owned(),
            storage_path: String::default(),
        })
    }

    async fn remove_temp_file(&self) -> io::Result<()> {
        remove_file(&self.local_path).await
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
    async fn on_events(&mut self, events: ApplyEvents) -> Result<usize> {
        let now = Instant::now_coarse();
        let mut total_size = 0;

        for mut event in events.events {
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

    /// generage path for log file before flushing to Storage
    fn set_storage_path(&mut self, path: String) {
        self.storage_path = path;
    }

    /// generate the metadata in protocol buffer of the file.
    fn generate_metadata(&mut self, file_key: &TempFileKey, store_id: u64) -> Result<DataFileInfo> {
        self.set_storage_path(file_key.file_name(store_id, self.min_ts, self.max_ts));

        let mut meta = DataFileInfo::new();
        meta.set_sha256(
            self.sha256
                .finish()
                .map(|bytes| bytes.to_vec())
                .map_err(|err| Error::Other(box_err!("openssl hasher failed to init: {}", err)))?,
        );
        meta.set_path(self.storage_path.clone());
        meta.set_number_of_entries(self.number_of_entries as _);
        meta.set_max_ts(self.max_ts.into_inner() as _);
        meta.set_min_ts(self.min_ts.into_inner() as _);
        meta.set_resolved_ts(self.resolved_ts.into_inner() as _);
        meta.set_min_begin_ts_in_default_cf(
            self.min_begin_ts
                .map_or(self.min_ts.into_inner(), |ts| ts.into_inner()),
        );
        meta.set_start_key(std::mem::take(&mut self.start_key));
        meta.set_end_key(std::mem::take(&mut self.end_key));
        meta.set_length(self.file_size as _);

        meta.set_is_meta(file_key.is_meta);
        meta.set_table_id(file_key.table_id);
        meta.set_cf(file_key.cf.to_owned());
        meta.set_region_id(file_key.region_id as i64);
        meta.set_type(file_key.get_file_type());

        Ok(meta)
    }
}

impl std::fmt::Debug for DataFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataFile")
            .field("min_ts", &self.min_ts)
            .field("max_ts", &self.max_ts)
            .field("resolved_ts", &self.resolved_ts)
            .field("local_path", &self.local_path.display())
            .finish()
    }
}

#[derive(Clone, Ord, PartialOrd, PartialEq, Eq, Debug)]
struct KeyRange(Vec<u8>);

#[derive(Clone, Debug)]
#[allow(dead_code)]
struct TaskRange {
    end: Vec<u8>,
    task_name: String,
}

#[cfg(test)]
mod tests {
    use std::{ffi::OsStr, time::Duration};

    use kvproto::brpb::{Local, Noop, StorageBackend, StreamBackupTaskInfo};
    use tikv_util::{
        codec::number::NumberEncoder,
        worker::{dummy_scheduler, ReceiverWrapper},
    };
    use txn_types::{Write, WriteType};

    use super::*;
    use crate::utils;

    #[derive(Debug)]
    struct KvEventsBuilder {
        events: ApplyEvents,
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
        let router = RouterInner::new(PathBuf::new(), tx, 1024, Duration::from_secs(300));
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

    fn create_local_storage_backend(path: String) -> StorageBackend {
        let mut local = Local::default();
        local.set_path(path);

        let mut sb = StorageBackend::default();
        sb.set_local(local);
        sb
    }

    fn create_noop_storage_backend() -> StorageBackend {
        let nop = Noop::new();
        let mut backend = StorageBackend::default();
        backend.set_noop(nop);
        backend
    }

    async fn task(name: String) -> Result<(StreamBackupTaskInfo, PathBuf)> {
        let mut stream_task = StreamBackupTaskInfo::default();
        stream_task.set_name(name);
        let storage_path = std::env::temp_dir().join(format!("{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&storage_path).await?;
        println!("storage={:?}", storage_path);
        stream_task.set_storage(create_local_storage_backend(
            storage_path.to_str().unwrap().to_string(),
        ));
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
        tokio::fs::create_dir_all(&tmp).await?;
        let (tx, rx) = dummy_scheduler();
        let router = RouterInner::new(tmp.clone(), tx, 32, Duration::from_secs(300));
        let (stream_task, storage_path) = task("dummy".to_owned()).await?;
        must_register_table(&router, stream_task, 1).await;

        let start_ts = write_simple_data(&router).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let end_ts = TimeStamp::physical_now();
        let files = router.tasks.lock().await.get("dummy").unwrap().clone();
        let meta = files
            .move_to_flushing_files(1)
            .await?
            .generate_metadata(1)
            .await?;
        assert_eq!(meta.files.len(), 3, "test file len = {}", meta.files.len());
        assert!(
            meta.files.iter().all(|item| {
                TimeStamp::new(item.min_ts as _).physical() >= start_ts
                    && TimeStamp::new(item.max_ts as _).physical() <= end_ts
                    && item.min_ts <= item.max_ts
            }),
            "meta = {:#?}; start ts = {}, end ts = {}",
            meta.files,
            start_ts,
            end_ts
        );

        // in some case when flush failed to write files to storage.
        // we may run `generate_metadata` again with same files.
        let another_meta = files
            .move_to_flushing_files(1)
            .await?
            .generate_metadata(1)
            .await?;

        assert_eq!(meta.files.len(), another_meta.files.len());
        for i in 0..meta.files.len() {
            let file1 = meta.files.get(i).unwrap();
            let file2 = another_meta.files.get(i).unwrap();
            // we have to make sure two times sha256 of file must be the same.
            assert_eq!(file1.sha256, file2.sha256);
            assert_eq!(file1.start_key, file2.start_key);
            assert_eq!(file1.end_key, file2.end_key);
        }

        files.flush_log().await?;
        files.flush_meta(meta).await?;
        files.clear_flushing_files().await;

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
            let filename = entry.file_name();
            println!("walking {}", entry.path().display());
            if entry.path().extension() == Some(OsStr::new("meta")) {
                meta_count += 1;
            } else if entry.path().extension() == Some(OsStr::new("log")) {
                log_count += 1;
                let f = entry.metadata().unwrap();
                assert!(
                    f.len() > 10,
                    "the log file {:?} is too small (size = {}B)",
                    filename,
                    f.len()
                );
            }
        }

        assert_eq!(meta_count, 1);
        assert_eq!(log_count, 3);
        Ok(())
    }

    fn mock_build_kv_events(table_id: i64, region_id: u64, resolved_ts: u64) -> ApplyEvents {
        let mut events_builder = KvEventsBuilder::new(region_id, resolved_ts);
        events_builder.put_table("default", table_id, b"hello", b"world");
        events_builder.finish()
    }

    #[tokio::test]
    async fn test_do_flush() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let backend = external_storage_export::make_local_backend(tmp_dir.path());
        let mut task_info = StreamBackupTaskInfo::default();
        task_info.set_storage(backend);
        let stream_task = StreamTask {
            info: task_info,
            is_paused: false,
        };
        let task = StreamTaskInfo::new(
            tmp_dir.path().to_path_buf(),
            stream_task,
            Duration::from_secs(300),
            vec![(vec![], vec![])],
        )
        .await
        .unwrap();

        // on_event
        let region_count = FLUSH_LOG_CONCURRENT_BATCH_COUNT + 5;
        for i in 1..=region_count {
            let kv_events = mock_build_kv_events(i as _, i as _, i as _);
            task.on_events(kv_events).await.unwrap();
        }
        // do_flush
        task.set_flushing_status(true);
        task.do_flush(1, TimeStamp::new(1)).await.unwrap();
        assert_eq!(task.flush_failure_count(), 0);
        assert_eq!(task.files.read().await.is_empty(), true);
        assert_eq!(task.flushing_files.read().await.is_empty(), true);

        // assert backup log files
        let mut meta_count = 0;
        let mut log_count = 0;
        for entry in walkdir::WalkDir::new(tmp_dir.path()) {
            let entry = entry.unwrap();
            if entry.path().extension() == Some(OsStr::new("meta")) {
                meta_count += 1;
            } else if entry.path().extension() == Some(OsStr::new("log")) {
                log_count += 1;
            }
        }
        assert_eq!(meta_count, 1);
        assert_eq!(log_count, region_count);
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
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
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
            reader: UnpinReader,
            content_length: u64,
        ) -> io::Result<()> {
            if let Err(e) = (self.error_on_write)() {
                return Err(e);
            }
            self.inner.write(name, reader, content_length).await
        }

        fn read(&self, name: &str) -> Box<dyn futures::AsyncRead + Unpin + '_> {
            self.inner.read(name)
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
            tmp.clone(),
            tx,
            1,
            Duration::from_secs(300),
        ));
        let (task, _path) = task("error_prone".to_owned()).await?;
        must_register_table(router.as_ref(), task, 1).await;
        router
            .must_mut_task_info("error_prone", |i| {
                i.storage = Arc::new(ErrorStorage::with_first_time_error(i.storage.clone()))
            })
            .await;
        check_on_events_result(&router.on_events(build_kv_event(0, 10)).await);
        assert!(
            router
                .do_flush("error_prone", 42, TimeStamp::max())
                .await
                .is_none()
        );
        check_on_events_result(&router.on_events(build_kv_event(10, 10)).await);
        let t = router.get_task_info("error_prone").await.unwrap();
        let _ = router.do_flush("error_prone", 42, TimeStamp::max()).await;
        assert_eq!(t.total_size() > 0, true);

        t.set_flushing_status(true);
        let _ = router.do_flush("error_prone", 42, TimeStamp::max()).await;
        assert_eq!(t.total_size(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_empty_resolved_ts() {
        let (tx, _rx) = dummy_scheduler();
        let tmp = std::env::temp_dir().join(format!("{}", uuid::Uuid::new_v4()));
        let router = RouterInner::new(tmp.clone(), tx, 32, Duration::from_secs(300));
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
            )
            .await
            .unwrap();
        let task = router.get_task_info("nothing").await.unwrap();
        task.set_flushing_status_cas(false, true).unwrap();
        let ts = TimeStamp::compose(TimeStamp::physical_now(), 42);
        let rts = router.do_flush("nothing", 1, ts).await.unwrap();
        assert_eq!(ts.into_inner(), rts);
    }

    #[tokio::test]
    async fn test_cleanup_when_stop() -> Result<()> {
        let (tx, _rx) = dummy_scheduler();
        let tmp = std::env::temp_dir().join(format!("{}", uuid::Uuid::new_v4()));
        let router = Arc::new(RouterInner::new(
            tmp.clone(),
            tx,
            1,
            Duration::from_secs(300),
        ));
        let (task, _path) = task("cleanup_test".to_owned()).await?;
        must_register_table(&router, task, 1).await;
        write_simple_data(&router).await;
        router
            .get_task_info("cleanup_test")
            .await?
            .move_to_flushing_files(1)
            .await?;
        write_simple_data(&router).await;
        let mut w = walkdir::WalkDir::new(&tmp).into_iter();
        assert!(w.next().is_some(), "the temp files doesn't created");
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
        Ok(())
    }

    #[tokio::test]
    async fn test_flush_with_pausing_self() -> Result<()> {
        let (tx, rx) = dummy_scheduler();
        let tmp = std::env::temp_dir().join(format!("{}", uuid::Uuid::new_v4()));
        let router = Arc::new(RouterInner::new(
            tmp.clone(),
            tx,
            1,
            Duration::from_secs(300),
        ));
        let (task, _path) = task("flush_failure".to_owned()).await?;
        must_register_table(router.as_ref(), task, 1).await;
        router
            .must_mut_task_info("flush_failure", |i| {
                i.storage = Arc::new(ErrorStorage::with_always_error(i.storage.clone()))
            })
            .await;
        for i in 0..=FLUSH_FAILURE_BECOME_FATAL_THRESHOLD {
            check_on_events_result(&router.on_events(build_kv_event((i * 10) as _, 10)).await);
            assert_eq!(
                router
                    .do_flush("flush_failure", 42, TimeStamp::zero())
                    .await,
                None,
            );
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
        let backend = external_storage_export::make_local_backend(tmp_dir.path());

        // build a StreamTaskInfo
        let mut task_info = StreamBackupTaskInfo::default();
        task_info.set_storage(backend);
        let stream_task = StreamTask {
            info: task_info,
            is_paused: false,
        };
        let task = StreamTaskInfo::new(
            tmp_dir.path().to_path_buf(),
            stream_task,
            Duration::from_secs(300),
            vec![(vec![], vec![])],
        )
        .await
        .unwrap();
        task.global_checkpoint_ts.store(10001, Ordering::SeqCst);

        // test no need to update global checkpoint
        let store_id = 3;
        let mut global_checkpoint = 10000;
        let is_updated = task
            .update_global_checkpoint(global_checkpoint, store_id)
            .await?;
        assert_eq!(is_updated, false);
        assert_eq!(task.global_checkpoint_ts.load(Ordering::SeqCst), 10001);

        // test update global checkpoint
        global_checkpoint = 10002;
        let is_updated = task
            .update_global_checkpoint(global_checkpoint, store_id)
            .await?;
        assert_eq!(is_updated, true);
        assert_eq!(
            task.global_checkpoint_ts.load(Ordering::SeqCst),
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
}
