use etcd_client::{EventType, GetOptions, SortOrder, SortTarget, WatchOptions};
use kvproto::brpb::StreamBackupTaskInfo;
use protobuf::core::Message;
use tokio_stream::Stream;

mod path {
    use std::{path::Path, str::pattern::Pattern};

    use regex::Regex;

    const PREFIX: &str = "/tidb/br-stream";
    const PATH_INFO: &str = "/info";
    const PATH_NEXT_BACKUP_TS: &str = "/checkpoint";
    const PATH_RANGES: &str = "/ranges";
    const PATH_PAUSE: &str = "/pause";
    static EXTRACT_NAME_FROM_INFO_RE: Regex =
        Regex::new(format!(r"{}/(?P<task_name>\w+)", tasks())).unwrap();

    pub fn tasks() -> Vec<u8> {
        Path::new(PREFIX).join(PATH_INFO).into()
    }

    pub fn task_of(name: &str) -> Vec<u8> {
        Path::new(PREFIX).join(PATH_INFO).join(name).into()
    }

    pub fn ranges_of(name: &str) -> Vec<u8> {
        Path::new(PREFIX).join(PATH_RANGES).join(name).into()
    }

    pub fn extract_name_from_info(full_path: &str) -> Option<&str> {
        EXTRACT_NAME_FROM_INFO_RE
            .captures(full_path)
            .map(|captures| captures.name("task_name").unwrap().as_str())
    }

    /// Generate the range key of some task.
    /// It should be <prefix>/ranges/<task-name(string)>/<start-key(binary).
    pub fn range_of(name: &str, rng: &[u8]) -> Vec<u8> {
        let mut ranges = ranges_of(name);
        ranges.push(b'/');
        ranges.extend(rng);
    }
}

use crate::errors::{Error, Result};

/// Some operations over metadata key space.
struct MetadataClient {
    // TODO: for better testing, make it an interface.
    cli: etcd_client::Client,
}

struct Task {
    pub info: StreamBackupTaskInfo,
    // attache the client into the task can provide some convenient interfaces
    // like task.range_overlap_of().
    cli: etcd_client::Client,
}

struct WithRevision<T> {
    pub revision: i64,
    pub inner: T,
}

impl Task {
    async fn range_overlap_of(
        &self,
        (start_key, end_key): (Vec<u8>, Vec<u8>),
    ) -> Result<WithRevision<Vec<(Vec<u8>, Vec<u8>)>>> {
        let prev = self
            .cli
            .get(
                self::path::ranges_of(self.name),
                Some(
                    GetOptions::new()
                        .with_range(self::path::range_of(self.name, start_key))
                        .with_sort(SortTarget::Key, SortOrder::Descend)
                        .with_limit(1),
                ),
            )
            .await?;
        // the header should not be taken here!
        let rev = prev.header().unwrap().revision();
        let all = self
            .cli
            .get(
                self::path::range_of(self.name, start_key),
                Some(
                    GetOptions::new()
                        .with_range(self::path::range_of(end_key))
                        .with_revision(rev),
                ),
            )
            .await?;
        let mut result = Vec::with_capacity(all.count() + 1);
        if prev.count() > 0 {
            let kv = prev.kvs()[0];
            if kv.value() > start_key {
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
enum MetadataEvent {
    AddTask { task: String },
    RemoveTask { task: String },
    Error { err: Error },
}

impl MetadataClient {
    async fn get_task(&self, name: &str) -> Result<Task> {
        let task = self
            .cli
            .kv_client()
            .get(self::path::task_of(name), None)
            .await?;
        if task.count() == 0 {
            return Err(Error::NoSuchTask { task_name: name });
        }
        let info = protobuf::parse_from_bytes::<StreamBackupTaskInfo>(task.kvs()[0].value())?;
        Ok(Task {
            info,
            cli: self.clone(),
        })
    }

    async fn get_tasks(&self) -> Result<WithRevision<Vec<Task>>> {
        let tasks = self
            .cli
            .get(self::path::tasks(), Some(GetOptions::new().with_prefix()))
            .await?;
        let result = Vec::with_capacity(tasks.count()?);
        let rev = tasks.header().unwrap().revision();
        for kv in tasks.kvs() {
            result.push(Task {
                info: protobuf::parse_from_bytes(kv.value())?,
                cli: self.clone(),
            })
        }
        Ok(WithRevision {
            inner: result,
            revision: rev,
        })
    }

    async fn events_from(&self, revision: i64) -> Result<impl Stream<Item = MetadataEvent>> {
        let (watcher, stream) = self
            .cli
            .watch(
                self::path::tasks(),
                Some(
                    WatchOptions::new()
                        .with_prefix()
                        .with_start_revision(revision),
                ),
            )
            .await?;
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        tokio::spawn(async move {
            loop {
                let msg = stream.message().await;
                match msg {
                    Err(err) => tx.send(MetadataEvent::Error { err }),
                    Ok(None) => return,
                    Ok(Some(events)) => {
                        for event in events.events() {
                            // Maybe report an error when the kv isn't present?
                            let task_name = event
                                .kv()
                                .and_then(|kv| self::path::extract_name_from_info(kv.key()));
                            match event.event_type() {
                                EventType::Put => {
                                    tx.send(MetadataEvent::AddTask { task: task_name })
                                }
                                EventType::Delete => {
                                    tx.send(MetadataEvent::RemoveTask { task: task_name })
                                }
                            }
                        }
                    }
                }
            }
        });
        Ok(rx)
    }
}
