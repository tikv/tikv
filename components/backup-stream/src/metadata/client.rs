// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cmp::Ordering, fmt::Debug, path::Path, sync::Arc};

use chrono::Local;
use dashmap::DashMap;
use kvproto::{
    brpb::{StreamBackupError, StreamBackupTaskInfo},
    metapb::Region,
};
use protobuf::Message as _;
use tikv_util::{defer, time::Instant, warn};
use tokio_stream::StreamExt;
use txn_types::TimeStamp;

use super::{
    checkpoint_cache::CheckpointCache,
    keys::{self, KeyValue, MetaKey},
    store::{
        CondTransaction, Condition, Keys, KvEvent, KvEventType, MetaStore, Snapshot, Subscription,
        Transaction, WithRevision,
    },
};
use crate::{
    annotate, debug,
    errors::{ContextualResultExt, Error, Result},
};

enum Payload {
    #[allow(dead_code)]
    Utf8Text(String),
    LogBackupError(kvproto::brpb::StreamBackupError),
}

impl serde::Serialize for Payload {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let value = match self {
            Payload::Utf8Text(_) => "text/plain;charset=UTF-8",
            Payload::LogBackupError(_) => {
                "application/x-protobuf;messageType=brpb.StreamBackupError"
            }
        };
        let mut iter = std::collections::HashMap::new();
        iter.insert("payload_type", value);
        let payload_value = match self {
            Payload::Utf8Text(s) => s.as_bytes().to_vec(),
            Payload::LogBackupError(err) => err.write_to_bytes().unwrap(),
        };
        let payload_value_base64 = base64::encode(payload_value);
        iter.insert("payload", &payload_value_base64);
        serializer.collect_map(iter)
    }
}

#[derive(derive_more::Deref)]
struct RFC3336Time(chrono::DateTime<Local>);

impl serde::Serialize for RFC3336Time {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(&self.0.to_rfc3339())
    }
}

#[derive(serde::Serialize)]
struct PauseV2 {
    severity: String,
    operation_hostname: String,
    operation_pid: u32,
    operation_time: RFC3336Time,

    #[serde(flatten)]
    payload: Payload,
}

#[derive(Debug)]
pub enum PauseStatus {
    NotPaused,
    PausedV1,
    // The Raw JSON value of the paused V2.
    // For now this API was only used for integration test
    // hence deserializing isn't implemented.
    PausedV2Json(String),
}

impl PauseV2 {
    fn by_error(err: kvproto::brpb::StreamBackupError) -> Self {
        Self {
            severity: "ERROR".to_owned(),
            operation_hostname: tikv_util::sys::hostname().unwrap_or_else(|| "unknown".to_owned()),
            operation_pid: std::process::id(),
            operation_time: RFC3336Time(Local::now()),
            payload: Payload::LogBackupError(err),
        }
    }
}

/// Some operations over stream backup metadata key space.
#[derive(Clone)]
pub struct MetadataClient<Store> {
    store_id: u64,
    caches: Arc<DashMap<String, CheckpointCache>>,
    pub(crate) meta_store: Store,
}

/// a stream backup task.
/// the initial design of this task would bind with a `MetaStore`,
/// which allows fluent API like `task.step(region_id, next_backup_ts)`.
#[derive(Default, Clone)]
pub struct StreamTask {
    pub info: StreamBackupTaskInfo,
    pub is_paused: bool,
}

impl Debug for StreamTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Task")
            .field("name", &self.info.name)
            .field("table_filter", &self.info.table_filter)
            .field("start_ts", &self.info.start_ts)
            .field("end_ts", &self.info.end_ts)
            .field("is_paused", &self.is_paused)
            .finish()
    }
}

#[derive(Debug)]
pub enum MetadataEvent {
    AddTask { task: StreamTask },
    RemoveTask { task: String },
    PauseTask { task: String },
    ResumeTask { task: String },
    Error { err: Error },
}

impl PartialEq for MetadataEvent {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::AddTask { task: l_task }, Self::AddTask { task: r_task }) => {
                l_task.info.get_name() == r_task.info.get_name()
            }
            (Self::RemoveTask { task: l_task }, Self::RemoveTask { task: r_task }) => {
                l_task == r_task
            }
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CheckpointProvider {
    Store(u64),
    Region { id: u64, version: u64 },
    Task,
    Global,
}

/// The polymorphic checkpoint.
/// The global checkpoint should be the minimal checkpoint of all checkpoints.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Checkpoint {
    pub provider: CheckpointProvider,
    pub ts: TimeStamp,
}

impl Checkpoint {
    pub fn from_kv(kv: &KeyValue) -> Result<Self> {
        match std::str::from_utf8(kv.0.0.as_slice()) {
            Ok(key) => Checkpoint::parse_from(Path::new(key), kv.1.as_slice()),
            Err(_) => {
                Ok(Checkpoint {
                    // The V1 checkpoint, maybe fill the store id?
                    provider: CheckpointProvider::Store(0),
                    ts: TimeStamp::new(parse_ts_from_bytes(kv.1.as_slice())?),
                })
            }
        }
    }

    pub fn parse_from(path: &Path, checkpoint_ts: &[u8]) -> Result<Self> {
        let segs = path.iter().map(|os| os.to_str()).collect::<Vec<_>>();
        match segs.as_slice() {
            [
                // We always use '/' as the path.
                // NOTE: Maybe just `split` and don't use `path`?
                Some("/"),
                Some("tidb"),
                Some("br-stream"),
                Some("checkpoint"),
                Some(_task_name),
                Some("region"),
                Some(id),
                Some(epoch),
                ..,
            ] => Self::from_region_parse_result(id, epoch, checkpoint_ts)
                .context(format_args!("during parsing key {}", path.display())),
            [
                // We always use '/' as the path.
                // NOTE: Maybe just `split` and don't use `path`?
                Some("/"),
                Some("tidb"),
                Some("br-stream"),
                Some("checkpoint"),
                Some(_task_name),
                Some("store"),
                Some(id),
                ..,
            ] => Self::from_store_parse_result(id, checkpoint_ts)
                .context(format_args!("during parsing key {}", path.display())),
            [
                // We always use '/' as the path.
                // NOTE: Maybe just `split` and don't use `path`?
                Some("/"),
                Some("tidb"),
                Some("br-stream"),
                Some("checkpoint"),
                Some(_task_name),
                Some("central_global"),
            ] => Ok(Self {
                provider: CheckpointProvider::Global,
                ts: TimeStamp::new(parse_ts_from_bytes(checkpoint_ts)?),
            }),
            _ => Err(Error::MalformedMetadata(format!(
                "cannot parse path {}(segs = {:?}) as checkpoint",
                path.display(),
                segs
            ))),
        }
    }

    fn from_store_parse_result(id: &str, checkpoint_ts: &[u8]) -> Result<Self> {
        let provider_id = id
            .parse::<u64>()
            .map_err(|err| Error::MalformedMetadata(err.to_string()))?;
        let provider = CheckpointProvider::Store(provider_id);
        let checkpoint = TimeStamp::new(parse_ts_from_bytes(checkpoint_ts)?);
        Ok(Self {
            provider,
            ts: checkpoint,
        })
    }

    fn from_region_parse_result(id: &str, version: &str, checkpoint_ts: &[u8]) -> Result<Self> {
        let id = id
            .parse::<u64>()
            .map_err(|err| Error::MalformedMetadata(err.to_string()))?;
        let version = version
            .parse::<u64>()
            .map_err(|err| Error::MalformedMetadata(err.to_string()))?;
        let checkpoint = TimeStamp::new(parse_ts_from_bytes(checkpoint_ts)?);
        let provider = CheckpointProvider::Region { id, version };
        Ok(Self {
            provider,
            ts: checkpoint,
        })
    }
}

impl MetadataEvent {
    fn from_watch_event(event: &KvEvent) -> Option<MetadataEvent> {
        // Maybe report an error when the kv isn't present?
        Some(match event.kind {
            KvEventType::Put => {
                let stream_task = StreamTask {
                    info: protobuf::parse_from_bytes::<StreamBackupTaskInfo>(event.pair.value())
                        .ok()?,
                    is_paused: false,
                };

                MetadataEvent::AddTask { task: stream_task }
            }
            KvEventType::Delete => {
                let key_str = std::str::from_utf8(event.pair.key()).ok()?;
                let task_name = keys::extract_name_from_info(key_str)?;

                MetadataEvent::RemoveTask {
                    task: task_name.to_owned(),
                }
            }
        })
    }

    fn from_watch_pause_event(event: &KvEvent) -> Option<MetadataEvent> {
        let key_str = std::str::from_utf8(event.pair.key()).ok()?;
        let task_name = keys::extrace_name_from_pause(key_str)?;
        Some(match event.kind {
            KvEventType::Put => MetadataEvent::PauseTask {
                task: task_name.to_owned(),
            },
            KvEventType::Delete => MetadataEvent::ResumeTask {
                task: task_name.to_owned(),
            },
        })
    }

    fn metadata_event_metrics(&self) {
        let tag = match self {
            MetadataEvent::AddTask { .. } => &["task_add"],
            MetadataEvent::RemoveTask { .. } => &["task_remove"],
            MetadataEvent::PauseTask { .. } => &["task_pause"],
            MetadataEvent::ResumeTask { .. } => &["task_resume"],
            MetadataEvent::Error { .. } => &["error"],
        };

        super::metrics::METADATA_EVENT_RECEIVED
            .with_label_values(tag)
            .inc();
    }
}

impl<Store: MetaStore> MetadataClient<Store> {
    /// create a new store.
    /// the store_id should be the store id of current TiKV.
    pub fn new(store: Store, store_id: u64) -> Self {
        Self {
            meta_store: store,
            caches: Arc::default(),
            store_id,
        }
    }

    /// Initialize a task: execute some general operations over the keys.
    /// For now, it sets the checkpoint ts if there isn't one for the current
    /// store.
    pub async fn init_task(&self, task: &StreamBackupTaskInfo) -> Result<()> {
        let if_present = Condition::new(
            MetaKey::next_backup_ts_of(&task.name, self.store_id),
            Ordering::Greater,
            vec![],
        );
        let txn = CondTransaction::new(
            if_present,
            Transaction::default(),
            Transaction::default().put(KeyValue(
                MetaKey::next_backup_ts_of(&task.name, self.store_id),
                task.get_start_ts().to_be_bytes().to_vec(),
            )),
        );
        self.meta_store.txn_cond(txn).await
    }

    /// Upload the last error information to the etcd.
    /// This won't pause the task. Even this method would usually be paired with
    /// `pause`.
    pub async fn report_last_error(&self, name: &str, last_error: StreamBackupError) -> Result<()> {
        use protobuf::Message;
        let now = Instant::now();
        defer! {
            super::metrics::METADATA_OPERATION_LATENCY.with_label_values(&["task_report_error"]).observe(now.saturating_elapsed_secs())
        }

        let key = MetaKey::last_error_of(name, self.store_id);
        let mut value = Vec::with_capacity(last_error.compute_size() as _);
        last_error.write_to_vec(&mut value)?;
        self.meta_store.set(KeyValue(key, value)).await?;

        Ok(())
    }

    pub async fn get_last_error(&self, name: &str) -> Result<Option<StreamBackupError>> {
        let key = MetaKey::last_errors_of(name);

        let r = self.meta_store.get_latest(Keys::Prefix(key)).await?.inner;
        if r.is_empty() {
            return Ok(None);
        }
        let r = &r[0];
        let err = protobuf::parse_from_bytes(r.value())?;
        Ok(Some(err))
    }

    pub async fn get_last_error_of(
        &self,
        name: &str,
        store_id: u64,
    ) -> Result<Option<StreamBackupError>> {
        let key = MetaKey::last_error_of(name, store_id);

        let r = self.meta_store.get_latest(Keys::Key(key)).await?.inner;
        if r.is_empty() {
            return Ok(None);
        }
        let r = &r[0];
        let err = protobuf::parse_from_bytes(r.value())?;
        Ok(Some(err))
    }

    /// check whether the task is paused.
    pub async fn check_task_paused(&self, name: &str) -> Result<bool> {
        let kvs = self
            .meta_store
            .get_latest(Keys::Key(MetaKey::pause_of(name)))
            .await?
            .inner;
        Ok(!kvs.is_empty())
    }

    /// pause a task.
    pub async fn pause(&self, name: &str) -> Result<()> {
        self.meta_store
            .set(KeyValue(MetaKey::pause_of(name), vec![]))
            .await
    }

    /// pause a task and upload the error
    pub async fn pause_with_err(
        &self,
        name: &str,
        err: kvproto::brpb::StreamBackupError,
    ) -> Result<()> {
        let pause = PauseV2::by_error(err);
        let pause_bytes = serde_json::to_string(&pause)
            .map_err(|e| annotate!(e, "failed to serialize the pause v2").report("pause_with_err"))
            // Anyway the task must be paused.
            .unwrap_or_default();
        self.meta_store
            .set(KeyValue(MetaKey::pause_of(name), pause_bytes.into_bytes()))
            .await?;

        Ok(())
    }

    /// resume a task.
    pub async fn resume(&self, name: &str) -> Result<()> {
        self.meta_store
            .delete(Keys::Key(MetaKey::pause_of(name)))
            .await
    }

    pub async fn pause_status(&self, name: &str) -> Result<PauseStatus> {
        let kvs = self
            .meta_store
            .get_latest(Keys::Key(MetaKey::pause_of(name)))
            .await?;
        if kvs.inner.is_empty() {
            return Ok(PauseStatus::NotPaused);
        }
        let kv = &kvs.inner[0];
        let pause_bytes = kv.value();
        let pause_str = std::str::from_utf8(pause_bytes).unwrap_or_default();
        if !pause_str.is_empty() {
            Ok(PauseStatus::PausedV2Json(pause_str.to_owned()))
        } else {
            Ok(PauseStatus::PausedV1)
        }
    }

    /// query the named task from the meta store.
    pub async fn get_task(&self, name: &str) -> Result<Option<StreamTask>> {
        let now = Instant::now();
        defer! {
            super::metrics::METADATA_OPERATION_LATENCY.with_label_values(&["task_get"]).observe(now.saturating_elapsed().as_secs_f64())
        }
        fail::fail_point!("failed_to_get_task", |_| {
            Err(Error::MalformedMetadata(
                "failed to connect etcd client".to_string(),
            ))
        });
        let items = self
            .meta_store
            .get_latest(Keys::Key(MetaKey::task_of(name)))
            .await?
            .inner;
        if items.is_empty() {
            return Ok(None);
        }
        let info = protobuf::parse_from_bytes::<StreamBackupTaskInfo>(items[0].value())?;
        let is_paused = self.check_task_paused(name).await?;

        Ok(Some(StreamTask { info, is_paused }))
    }

    /// fetch all tasks from the meta store.
    pub async fn get_tasks(&self) -> Result<WithRevision<Vec<StreamTask>>> {
        let now = Instant::now();
        defer! {
            super::metrics::METADATA_OPERATION_LATENCY.with_label_values(&["task_fetch"]).observe(now.saturating_elapsed().as_secs_f64())
        }
        fail::fail_point!("failed_to_get_tasks", |_| {
            Err(Error::MalformedMetadata(
                "failed to connect etcd client".to_string(),
            ))
        });
        let kvs = self
            .meta_store
            .get_latest(Keys::Prefix(MetaKey::tasks()))
            .await?;

        let mut tasks = Vec::with_capacity(kvs.inner.len());
        for kv in kvs.inner {
            let t = protobuf::parse_from_bytes::<StreamBackupTaskInfo>(kv.value())?;
            let paused = self.check_task_paused(t.get_name()).await?;
            tasks.push(StreamTask {
                info: t,
                is_paused: paused,
            });
        }
        Ok(WithRevision {
            inner: tasks,
            revision: kvs.revision,
        })
    }

    // get the reveresion from meta store.
    pub async fn get_reversion(&self) -> Result<i64> {
        let snap = self.meta_store.snapshot().await?;
        Ok(snap.revision())
    }

    /// watch event stream from the revision(exclusive).
    /// the revision would usually come from a WithRevision struct(which indices
    /// the revision of the inner item).
    pub async fn events_from(&self, revision: i64) -> Result<Subscription<MetadataEvent>> {
        let watcher = self
            .meta_store
            .watch(Keys::Prefix(MetaKey::tasks()), revision + 1)
            .await?;
        Ok(Subscription {
            stream: Box::pin(
                watcher
                    .stream
                    .filter_map(|item| match item {
                        Err(err) => Some(MetadataEvent::Error { err }),
                        Ok(event) => MetadataEvent::from_watch_event(&event),
                    })
                    .map(|event| {
                        event.metadata_event_metrics();
                        event
                    }),
            ),
            cancel: watcher.cancel,
        })
    }

    pub async fn events_from_pause(&self, revision: i64) -> Result<Subscription<MetadataEvent>> {
        let watcher = self
            .meta_store
            .watch(Keys::Prefix(MetaKey::pause_prefix()), revision + 1)
            .await?;
        let stream = watcher
            .stream
            .filter_map(|item| match item {
                Ok(kv_event) => {
                    debug!("watch pause event"; "raw" => ?kv_event);
                    MetadataEvent::from_watch_pause_event(&kv_event)
                }
                Err(err) => Some(MetadataEvent::Error { err }),
            })
            .map(|event| {
                event.metadata_event_metrics();
                event
            });

        Ok(Subscription {
            stream: Box::pin(stream),
            cancel: watcher.cancel,
        })
    }

    /// Set the storage checkpoint to metadata.
    pub async fn set_storage_checkpoint(&self, task_name: &str, ts: u64) -> Result<()> {
        let now = Instant::now();
        defer! {
            super::metrics::METADATA_OPERATION_LATENCY.with_label_values(&["storage_checkpoint"]).observe(now.saturating_elapsed().as_secs_f64())
        }
        self.meta_store
            .set(KeyValue(
                MetaKey::storage_checkpoint_of(task_name, self.store_id),
                ts.to_be_bytes().to_vec(),
            ))
            .await?;
        Ok(())
    }

    /// Get the storage checkpoint from metadata. This function is justly used
    /// for test.
    pub async fn get_storage_checkpoint(&self, task_name: &str) -> Result<TimeStamp> {
        let now = Instant::now();
        defer! {
            super::metrics::METADATA_OPERATION_LATENCY.with_label_values(&["task_step"]).observe(now.saturating_elapsed().as_secs_f64())
        }
        let ts = self
            .meta_store
            .get_latest(Keys::Key(MetaKey::storage_checkpoint_of(
                task_name,
                self.store_id,
            )))
            .await?
            .inner;

        match ts.as_slice() {
            [ts, ..] => Ok(TimeStamp::new(parse_ts_from_bytes(ts.value())?)),
            [] => Ok(self.get_task_start_ts_checkpoint(task_name).await?.ts),
        }
    }
    /// forward the progress of some task.
    pub async fn set_local_task_checkpoint(&self, task_name: &str, ts: u64) -> Result<()> {
        let now = Instant::now();
        defer! {
            super::metrics::METADATA_OPERATION_LATENCY.with_label_values(&["task_step"]).observe(now.saturating_elapsed().as_secs_f64())
        }
        self.meta_store
            .set(KeyValue(
                MetaKey::next_backup_ts_of(task_name, self.store_id),
                ts.to_be_bytes().to_vec(),
            ))
            .await?;
        Ok(())
    }

    pub async fn get_local_task_checkpoint(&self, task_name: &str) -> Result<TimeStamp> {
        let now = Instant::now();
        defer! {
            super::metrics::METADATA_OPERATION_LATENCY.with_label_values(&["task_step"]).observe(now.saturating_elapsed().as_secs_f64())
        }
        let ts = self
            .meta_store
            .get_latest(Keys::Key(MetaKey::next_backup_ts_of(
                task_name,
                self.store_id,
            )))
            .await?
            .inner;

        match ts.as_slice() {
            [ts, ..] => Ok(TimeStamp::new(parse_ts_from_bytes(ts.value())?)),
            [] => Ok(self.get_task_start_ts_checkpoint(task_name).await?.ts),
        }
    }

    /// get all target ranges of some task.
    pub async fn ranges_of_task(
        &self,
        task_name: &str,
    ) -> Result<WithRevision<Vec<(Vec<u8>, Vec<u8>)>>> {
        let ranges = self
            .meta_store
            .get_latest(Keys::Prefix(MetaKey::ranges_of(task_name)))
            .await?;

        Ok(ranges.map(|rs| {
            rs.into_iter()
                .map(|mut kv: KeyValue| kv.take_range(task_name))
                .collect()
        }))
    }

    pub async fn checkpoints_of(&self, task_name: &str) -> Result<Vec<Checkpoint>> {
        let now = Instant::now();
        defer! {
            super::metrics::METADATA_OPERATION_LATENCY.with_label_values(&["checkpoints_of"]).observe(now.saturating_elapsed().as_secs_f64())
        }
        let checkpoints = self.meta_store
            .get_latest(Keys::Prefix(MetaKey::next_backup_ts(task_name)))
            .await?
            .inner
            .iter()
            .filter_map(|kv| {
                Checkpoint::from_kv(kv)
                    .map_err(|err| warn!("br-stream: failed to parse next_backup_ts."; "key" => ?kv.0, "err" => %err))
                    .ok()
            })
            .collect();
        Ok(checkpoints)
    }

    async fn get_task_start_ts_checkpoint(&self, task_name: &str) -> Result<Checkpoint> {
        let task = self
            .get_task(task_name)
            .await?
            .ok_or_else(|| Error::NoSuchTask {
                task_name: task_name.to_owned(),
            })?;
        Ok(Checkpoint {
            ts: TimeStamp::new(task.info.start_ts),
            provider: CheckpointProvider::Task,
        })
    }

    /// Get the global checkpoint of a task.
    /// It is the smallest checkpoint of all types of checkpoint.
    pub async fn global_checkpoint_of_task(&self, task_name: &str) -> Result<Checkpoint> {
        let cp = match self.global_checkpoint_of(task_name).await? {
            Some(cp) => cp,
            None => self.get_task_start_ts_checkpoint(task_name).await?,
        };
        Ok(cp)
    }

    /// get the global progress (the min next_backup_ts among all stores).
    pub async fn global_progress_of_task(&self, task_name: &str) -> Result<u64> {
        let cp = self.global_checkpoint_of_task(task_name).await?;
        debug!("getting global progress of task"; "checkpoint" => ?cp);
        let ts = cp.ts.into_inner();
        Ok(ts)
    }

    /// insert a task with ranges into the metadata store.
    /// the current abstraction of metadata store doesn't support transaction
    /// API. Hence this function is non-transactional and only for testing.
    pub async fn insert_task_with_range(
        &self,
        task: &StreamTask,
        ranges: &[(&[u8], &[u8])],
    ) -> Result<()> {
        let bin = protobuf::Message::write_to_bytes(&task.info)?;
        self.meta_store
            .set(KeyValue(MetaKey::task_of(&task.info.name), bin))
            .await?;
        for (start_key, end_key) in ranges {
            self.meta_store
                .set(KeyValue(
                    MetaKey::range_of(&task.info.name, start_key),
                    end_key.to_owned().to_vec(),
                ))
                .await?;
        }
        Ok(())
    }

    /// remove some task, without the ranges.
    /// only for testing.
    #[cfg(test)]
    pub async fn remove_task(&self, name: &str) -> Result<()> {
        self.meta_store
            .delete(Keys::Key(MetaKey::task_of(name)))
            .await
    }

    pub async fn global_checkpoint_of(&self, task: &str) -> Result<Option<Checkpoint>> {
        let cps = self.checkpoints_of(task).await?;
        let mut min_checkpoint = None;
        for cp in cps {
            match cp.provider {
                CheckpointProvider::Store(..) => {
                    if min_checkpoint
                        .as_ref()
                        .map(|c: &Checkpoint| c.ts > cp.ts)
                        .unwrap_or(true)
                    {
                        min_checkpoint = Some(cp);
                    }
                }
                // The global checkpoint has higher priority than store checkpoint.
                CheckpointProvider::Task | CheckpointProvider::Global => return Ok(Some(cp)),
                CheckpointProvider::Region { .. } => continue,
            }
        }
        Ok(min_checkpoint)
    }

    fn cached_checkpoint(&self, task: &str) -> Option<Checkpoint> {
        self.caches
            .get(task)
            .and_then(|x| x.value().get())
            .map(|x| Checkpoint {
                provider: CheckpointProvider::Global,
                ts: x,
            })
    }

    fn update_cache(&self, task: &str, checkpoint: TimeStamp) {
        let mut c = self.caches.entry(task.to_owned()).or_default();
        c.value_mut().update(checkpoint);
    }

    pub async fn get_region_checkpoint(&self, task: &str, region: &Region) -> Result<Checkpoint> {
        if let Some(c) = self.cached_checkpoint(task) {
            return Ok(c);
        }
        let key = MetaKey::next_bakcup_ts_of_region(task, region);
        let r = self
            .meta_store
            .get_latest(Keys::Key(key.clone()))
            .await?
            .inner;
        let cp = match r.len() {
            0 => {
                let global_cp = self.global_checkpoint_of(task).await?;

                match global_cp {
                    None => self.get_task_start_ts_checkpoint(task).await?,
                    Some(cp) => cp,
                }
            }
            _ => Checkpoint::from_kv(&r[0])?,
        };
        self.update_cache(task, cp.ts);
        Ok(cp)
    }
}

fn parse_ts_from_bytes(next_backup_ts: &[u8]) -> Result<u64> {
    if next_backup_ts.len() != 8 {
        return Err(Error::MalformedMetadata(format!(
            "the length of next_backup_ts is {} bytes, require 8 bytes",
            next_backup_ts.len()
        )));
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(next_backup_ts);
    Ok(u64::from_be_bytes(buf))
}

#[cfg(test)]
mod test {
    use kvproto::metapb::{Region as RegionInfo, RegionEpoch};
    use txn_types::TimeStamp;

    use super::Checkpoint;
    use crate::metadata::{
        client::CheckpointProvider,
        keys::{KeyValue, MetaKey},
    };

    #[test]
    fn test_parse() {
        struct Case {
            provider: CheckpointProvider,
            checkpoint: u64,
        }

        fn run_case(c: Case) {
            let key = match c.provider {
                CheckpointProvider::Region { id, version } => {
                    let mut r = RegionInfo::new();
                    let mut v = RegionEpoch::new();
                    v.set_version(version);
                    r.set_region_epoch(v);
                    r.set_id(id);
                    MetaKey::next_bakcup_ts_of_region("test", &r)
                }
                CheckpointProvider::Store(id) => MetaKey::next_backup_ts_of("test", id),
                _ => unreachable!(),
            };
            let checkpoint = c.checkpoint;
            let cp_bytes = checkpoint.to_be_bytes();
            let kv = KeyValue(key, cp_bytes.to_vec());
            let parsed = Checkpoint::from_kv(&kv).unwrap();
            assert_eq!(
                parsed,
                Checkpoint {
                    provider: c.provider,
                    ts: TimeStamp::new(c.checkpoint),
                }
            );
        }
        use CheckpointProvider::*;

        let cases = vec![
            Case {
                checkpoint: TimeStamp::compose(TimeStamp::physical_now(), 10).into_inner(),
                provider: Region { id: 42, version: 8 },
            },
            Case {
                checkpoint: u64::from_be_bytes(*b"let i=0;"),
                provider: Store(3),
            },
            Case {
                checkpoint: u64::from_be_bytes(*b"(callcc)"),
                provider: Region {
                    id: 16961,
                    version: 16,
                },
            },
        ];

        for case in cases {
            run_case(case)
        }
    }
}
