// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use std::fmt::Debug;

use super::{
    keys::{KeyValue, MetaKey},
    store::{
        GetExtra, Keys, KvEvent, KvEventType, MetaStore, Snapshot, Subscription, WithRevision,
    },
};

use kvproto::brpb::StreamBackupTaskInfo;

use tikv_util::{defer, time::Instant, warn};
use tokio_stream::StreamExt;


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
    AddTask { task: String },
    RemoveTask { task: String },
    Error { err: Error },
}

impl PartialEq for MetadataEvent {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::AddTask { task: l_task }, Self::AddTask { task: r_task }) => l_task == r_task,
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
        let key_str = std::str::from_utf8(event.pair.key()).ok()?;
        let task_name = super::keys::extract_name_from_info(key_str)?;
        Some(match event.kind {
            KvEventType::Put => MetadataEvent::AddTask {
                task: task_name.to_owned(),
            },
            KvEventType::Delete => MetadataEvent::RemoveTask {
                task: task_name.to_owned(),
            },
        })
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

    /// query the named task from the meta store.
    pub async fn get_task(&self, name: &str) -> Result<StreamTask> {
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
            return Err(Error::NoSuchTask {
                task_name: name.to_owned(),
            });
        }
        let info = protobuf::parse_from_bytes::<StreamBackupTaskInfo>(&items[0].1)?;
        Ok(StreamTask { info })
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
            tasks.push(StreamTask {
                info: protobuf::parse_from_bytes(&kv.1)?,
            });
        }
        Ok(WithRevision {
            inner: tasks,
            revision: snap.revision(),
        })
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
                        match event {
                            MetadataEvent::AddTask { .. } => {
                                super::metrics::METADATA_EVENT_RECEIVED
                                    .with_label_values(&["task_add"])
                                    .inc()
                            }
                            MetadataEvent::RemoveTask { .. } => {
                                super::metrics::METADATA_EVENT_RECEIVED
                                    .with_label_values(&["task_remove"])
                                    .inc()
                            }
                            MetadataEvent::Error { .. } => super::metrics::METADATA_EVENT_RECEIVED
                                .with_label_values(&["error"])
                                .inc(),
                        }
                        event
                    }),
            ),
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
        let timestamp = self.meta_store.snapshot().await?;
        let items = timestamp
            .get(Keys::Key(MetaKey::next_backup_ts_of(
                task_name,
                self.store_id,
            )))
            .await?;
        if items.is_empty() {
            Ok(task.info.start_ts)
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
            .unwrap_or(task.info.start_ts);
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
    #[cfg(test)]
    pub(crate) async fn insert_task_with_range(
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
    pub(crate) async fn remove_task(&self, name: &str) -> Result<()> {
        self.meta_store
            .delete(Keys::Key(MetaKey::task_of(name)))
            .await
    }
}
