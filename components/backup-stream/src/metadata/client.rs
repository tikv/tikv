// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, fmt::Debug};

use kvproto::brpb::{StreamBackupError, StreamBackupTaskInfo};
use tikv_util::{defer, time::Instant, warn};
use tokio_stream::StreamExt;

use super::{
    keys::{self, KeyValue, MetaKey},
    store::{
        GetExtra, Keys, KvEvent, KvEventType, MetaStore, Snapshot, Subscription, WithRevision,
    },
};
use crate::errors::{Error, Result};

/// Some operations over stream backup metadata key space.
#[derive(Clone)]
pub struct MetadataClient<Store> {
    store_id: u64,
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
            store_id,
        }
    }

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

    pub async fn get_last_error(
        &self,
        name: &str,
        store_id: u64,
    ) -> Result<Option<StreamBackupError>> {
        let key = MetaKey::last_error_of(name, store_id);

        let s = self.meta_store.snapshot().await?;
        let r = s.get(Keys::Key(key)).await?;
        if r.is_empty() {
            return Ok(None);
        }
        let r = &r[0];
        let err = protobuf::parse_from_bytes(r.value())?;
        Ok(Some(err))
    }

    /// check whether the task is paused.
    pub async fn check_task_paused(&self, name: &str) -> Result<bool> {
        let snap = self.meta_store.snapshot().await?;
        let kvs = snap.get(Keys::Key(MetaKey::pause_of(name))).await?;
        Ok(!kvs.is_empty())
    }

    /// pause a task.
    pub async fn pause(&self, name: &str) -> Result<()> {
        self.meta_store
            .set(KeyValue(MetaKey::pause_of(name), vec![]))
            .await
    }

    pub async fn get_tasks_pause_status(&self) -> Result<HashMap<Vec<u8>, bool>> {
        let snap = self.meta_store.snapshot().await?;
        let kvs = snap.get(Keys::Prefix(MetaKey::pause_prefix())).await?;
        let mut pause_hash = HashMap::new();
        let prefix_len = MetaKey::pause_prefix_len();

        for kv in kvs {
            let task_name = kv.key()[prefix_len..].to_vec();
            pause_hash.insert(task_name, true);
        }

        Ok(pause_hash)
    }

    /// query the named task from the meta store.
    pub async fn get_task(&self, name: &str) -> Result<Option<StreamTask>> {
        let now = Instant::now();
        defer! {
            super::metrics::METADATA_OPERATION_LATENCY.with_label_values(&["task_get"]).observe(now.saturating_elapsed().as_secs_f64())
        }
        let items = self
            .meta_store
            .snapshot()
            .await?
            .get(Keys::Key(MetaKey::task_of(name)))
            .await?;
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
        let snap = self.meta_store.snapshot().await?;
        let kvs = snap.get(Keys::Prefix(MetaKey::tasks())).await?;

        let mut tasks = Vec::with_capacity(kvs.len());
        for kv in kvs {
            let t = protobuf::parse_from_bytes::<StreamBackupTaskInfo>(kv.value())?;
            let paused = self.check_task_paused(t.get_name()).await?;
            tasks.push(StreamTask {
                info: t,
                is_paused: paused,
            });
        }
        Ok(WithRevision {
            inner: tasks,
            revision: snap.revision(),
        })
    }

    // get the reveresion from meta store.
    pub async fn get_reversion(&self) -> Result<i64> {
        let snap = self.meta_store.snapshot().await?;
        Ok(snap.revision())
    }

    /// watch event stream from the revision(exclusive).
    /// the revision would usually come from a WithRevision struct(which indices the revision of the inner item).
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
                Ok(kv_event) => MetadataEvent::from_watch_pause_event(&kv_event),
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

    /// forward the progress of some task.
    pub async fn step_task(&self, task_name: &str, ts: u64) -> Result<()> {
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

    /// get all target ranges of some task.
    pub async fn ranges_of_task(
        &self,
        task_name: &str,
    ) -> Result<WithRevision<Vec<(Vec<u8>, Vec<u8>)>>> {
        let snap = self.meta_store.snapshot().await?;
        let ranges = snap
            .get(Keys::Prefix(MetaKey::ranges_of(task_name)))
            .await?;

        Ok(WithRevision {
            revision: snap.revision(),
            inner: ranges
                .into_iter()
                .map(|mut kv: KeyValue| kv.take_range(task_name))
                .collect(),
        })
    }

    /// Perform a two-phase bisection search algorithm for the intersection of all ranges
    /// and the specificated range (usually region range.)
    /// TODO: explain the algorithm?
    pub async fn range_overlap_of_task(
        &self,
        task_name: &str,
        (start_key, end_key): (Vec<u8>, Vec<u8>),
    ) -> Result<WithRevision<Vec<(Vec<u8>, Vec<u8>)>>> {
        let now = Instant::now();
        defer! {
            super::metrics::METADATA_OPERATION_LATENCY.with_label_values(&["task_range_search"]).observe(now.saturating_elapsed().as_secs_f64())
        }
        let snap = self.meta_store.snapshot().await?;

        let mut prev = snap
            .get_extra(
                Keys::Range(
                    MetaKey::ranges_of(task_name),
                    MetaKey::range_of(task_name, &start_key),
                ),
                GetExtra {
                    desc_order: true,
                    limit: 1,
                    ..Default::default()
                },
            )
            .await?;
        let all = snap
            .get(Keys::Range(
                MetaKey::range_of(task_name, &start_key),
                MetaKey::range_of(task_name, &end_key),
            ))
            .await?;

        let mut result = Vec::with_capacity(all.len() as usize + 1);
        if !prev.kvs.is_empty() {
            let kv = &mut prev.kvs[0];
            if kv.value() > start_key.as_slice() {
                result.push(kv.take_range(task_name));
            }
        }
        for mut kv in all {
            result.push(kv.take_range(task_name));
        }
        Ok(WithRevision {
            revision: snap.revision(),
            inner: result,
        })
    }

    /// access the next backup ts of some task and some region.
    pub async fn progress_of_task(&self, task_name: &str) -> Result<u64> {
        let now = Instant::now();
        defer! {
            super::metrics::METADATA_OPERATION_LATENCY.with_label_values(&["task_progress_get"]).observe(now.saturating_elapsed().as_secs_f64())
        }
        let task = self.get_task(task_name).await?;
        if task.is_none() {
            return Err(Error::NoSuchTask {
                task_name: task_name.to_owned(),
            });
        }

        let timestamp = self.meta_store.snapshot().await?;
        let items = timestamp
            .get(Keys::Key(MetaKey::next_backup_ts_of(
                task_name,
                self.store_id,
            )))
            .await?;
        if items.is_empty() {
            Ok(task.unwrap().info.start_ts)
        } else {
            assert_eq!(items.len(), 1);
            Self::parse_ts_from_bytes(items[0].1.as_slice())
        }
    }

    /// get the global progress (the min next_backup_ts among all stores).
    pub async fn global_progress_of_task(&self, task_name: &str) -> Result<u64> {
        let now = Instant::now();
        defer! {
            super::metrics::METADATA_OPERATION_LATENCY.with_label_values(&["task_progress_get_global"]).observe(now.saturating_elapsed().as_secs_f64())
        }
        let task = self.get_task(task_name).await?;
        if task.is_none() {
            return Err(Error::NoSuchTask {
                task_name: task_name.to_owned(),
            });
        }

        let snap = self.meta_store.snapshot().await?;
        let global_ts = snap.get(Keys::Prefix(MetaKey::next_backup_ts(task_name)))
            .await?
            .iter()
            .filter_map(|kv| {
                Self::parse_ts_from_bytes(kv.1.as_slice())
                    .map_err(|err| warn!("br-stream: failed to parse next_backup_ts."; "key" => ?kv.0, "err" => %err))
                    .ok()
            })
            .min()
            .unwrap_or(task.unwrap().info.start_ts);
        Ok(global_ts)
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

    /// insert a task with ranges into the metadata store.
    /// the current abstraction of metadata store doesn't support transaction API.
    /// Hence this function is non-transactional and only for testing.
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
    pub async fn remove_task(&self, name: &str) -> Result<()> {
        self.meta_store
            .delete(Keys::Key(MetaKey::task_of(name)))
            .await
    }
}
