use std::{fmt::Debug, sync::Arc};

use super::{
    keys::MetaKey,
    store::{Keys, KvEvent, KvEventType, MetaStore, Snapshot, WithRevision},
};
use etcd_client::{GetOptions, SortOrder, SortTarget};
use kvproto::brpb::StreamBackupTaskInfo;
use tokio::sync::Mutex;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};

use crate::errors::{Error, Result};

/// Some operations over metadata key space.
pub struct MetadataClient<Store> {
    // TODO: for better testing, make it an interface.
    // Can we get rid of the mutex? (which means, we must use a singleton client.)
    // Or make a pool of clients?
    cli: Arc<Mutex<etcd_client::Client>>,
    store: Store,
}

pub struct Task {
    pub info: StreamBackupTaskInfo,
    // attache the client into the task can provide some convenient interfaces
    // like task.range_overlap_of().
    cli: Arc<Mutex<etcd_client::Client>>,
}

impl Debug for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Task")
            .field("name", &self.info.name)
            .field("table_filter", &self.info.table_filter)
            .field("start_ts", &self.info.start_ts)
            .field("end_ts", &self.info.end_ts)
            .finish()
    }
}

impl Task {
    async fn range_overlap_of(
        &self,
        (start_key, end_key): (Vec<u8>, Vec<u8>),
    ) -> Result<WithRevision<Vec<(Vec<u8>, Vec<u8>)>>> {
        let prev = self
            .cli
            .lock()
            .await
            .get(
                MetaKey::ranges_of(self.info.name.as_str()),
                Some(
                    GetOptions::new()
                        .with_range(MetaKey::range_of(self.info.name.as_str(), &start_key))
                        .with_sort(SortTarget::Key, SortOrder::Descend)
                        .with_limit(1),
                ),
            )
            .await?;
        // the header should not be taken here!
        let rev = prev.header().unwrap().revision();
        let all = self
            .cli
            .lock()
            .await
            .get(
                MetaKey::range_of(self.info.name.as_str(), &start_key),
                Some(
                    GetOptions::new()
                        .with_range(MetaKey::range_of(&self.info.name, &end_key))
                        .with_revision(rev),
                ),
            )
            .await?;
        let mut result = Vec::with_capacity(all.count() as usize + 1);
        if prev.count() > 0 {
            let kv = &prev.kvs()[0];
            if kv.value() > start_key.as_slice() {
                result.push((kv.key().to_owned(), kv.value().to_owned()));
            }
        }
        for kv in all.kvs() {
            result.push((kv.key().to_owned(), kv.value().to_owned()))
        }
        Ok(WithRevision {
            revision: rev,
            inner: result,
        })
    }
}

#[derive(Debug)]
pub enum MetadataEvent {
    AddTask { task: String },
    RemoveTask { task: String },
    Error { err: Error },
}

impl MetadataEvent {
    fn from_watch_event(event: &KvEvent) -> Option<MetadataEvent> {
        // Maybe report an error when the kv isn't present?
        let key_str = std::str::from_utf8(event.pair.1.as_slice()).ok()?;
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

macro_rules! send_or_break {
    ($sender: expr, $item: expr) => {
        if $sender.send($item).await.is_err() {
            break;
        }
    };
}

impl<Store: MetaStore> MetadataClient<Store> {
    pub async fn new(pd_addrs: &[&str]) -> Result<Self> {
        let cli = etcd_client::Client::connect(pd_addrs, None).await?;
        Ok(Self {
            cli: Arc::new(Mutex::new(cli)),
            store: todo!(),
        })
    }

    pub async fn get_task(&self, name: &str) -> Result<Task> {
        let items = self
            .store
            .snap()
            .get(&Keys::Key(MetaKey::task_of(name)))
            .await?;
        if items.len() == 0 {
            return Err(Error::NoSuchTask {
                task_name: name.to_owned(),
            });
        }
        let info = protobuf::parse_from_bytes::<StreamBackupTaskInfo>(&items[0].1)?;
        Ok(Task {
            info,
            cli: self.cli.clone(),
        })
    }

    pub async fn get_tasks(&self) -> Result<WithRevision<Vec<Task>>> {
        let snap = self.store.snap();
        let kvs = snap.get(&Keys::Prefix(MetaKey::tasks())).await?;
        let mut tasks = Vec::with_capacity(kvs.len());
        for kv in kvs {
            tasks.push(Task {
                info: protobuf::parse_from_bytes(&kv.1)?,
                cli: self.cli.clone(),
            });
        }
        Ok(WithRevision {
            inner: tasks,
            revision: snap.revision(),
        })
    }

    /// watch event stream from the revision(exclusive).
    /// the revision would usually come from a WithRevision struct(which indices the revision of the inner item).
    pub async fn events_from(&self, revision: i64) -> Result<impl Stream<Item = MetadataEvent>> {
        let mut watcher = self
            .store
            .watch(Keys::Prefix(MetaKey::tasks()), revision + 1)
            .await?;
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        tokio::spawn(async move {
            loop {
                match watcher.stream.next().await {
                    Some(Err(err)) => {
                        send_or_break!(tx, MetadataEvent::Error { err: err.into() });
                    }
                    None => break,
                    Some(Ok(event)) => {
                        if let Some(event) = MetadataEvent::from_watch_event(&event) {
                            send_or_break!(tx, event);
                        }
                    }
                }
            }
            // we cannot use defer! here because async closure hasn't been supported,
            // and defer! doesn't support async functions too...
            watcher.cancel.await
        });
        Ok(ReceiverStream::new(rx))
    }
}
