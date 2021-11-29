// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
#![cfg(test)]

use std::{
    collections::{hash_map::RandomState, HashSet},
    iter::FromIterator,
};

use crate::{errors::Result, metadata::MetadataEvent};
use kvproto::brpb::{Noop, StorageBackend};
use tokio_stream::StreamExt;

use self::slash_etc::SharedSlashEtc;

use super::{MetadataClient, Task};

/// module slash etc (/etc) provides a etcd-like interface for testing.
mod slash_etc {
    use std::{
        cell::Cell,
        collections::{BTreeMap, HashMap},
        ops::Bound,
        sync::Arc,
    };

    use crate::{
        errors::Result,
        metadata::{
            keys::{KeyValue, MetaKey},
            store::{
                GetResponse, KvChangeSubscription, KvEvent, KvEventType, MetaStore, Snapshot,
                Subscription, WithRevision,
            },
        },
    };
    use async_trait::async_trait;
    use tokio::sync::{
        mpsc::{self, Sender},
        Mutex,
    };
    use tokio_stream::StreamExt;
    struct Subscriber {
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        tx: Sender<KvEvent>,
    }

    #[derive(Default)]
    pub struct SlashEtc {
        items: BTreeMap<Vec<u8>, Vec<u8>>,
        // Maybe a range tree here if the test gets too slow.
        subs: HashMap<usize, Subscriber>,
        revision: i64,
        sub_id_alloc: Cell<usize>,
    }

    pub type SharedSlashEtc = Arc<Mutex<SlashEtc>>;

    #[async_trait]
    impl Snapshot for WithRevision<SharedSlashEtc> {
        async fn get_extra(
            &self,
            keys: crate::metadata::store::Keys,
            extra: crate::metadata::store::GetExtra,
        ) -> Result<crate::metadata::store::GetResponse> {
            let data = self.inner.lock().await;
            if data.revision != self.revision {
                panic!("snapshot expired (multi version isn't supported yet.)");
            }
            let (start_key, end_key) = keys.into_bound();
            let mut kvs = data
                .items
                .range::<[u8], _>((
                    Bound::Included(start_key.as_slice()),
                    Bound::Excluded(end_key.as_slice()),
                ))
                .map(|(k, v)| KeyValue(MetaKey(k.clone()), v.clone()))
                .collect::<Vec<_>>();
            // use iterator operations (instead of collect all kv pairs in the range)
            // if the test case get too slow. (How can we figure out whether there are more?)
            if extra.desc_order {
                kvs.reverse();
            }
            let more = if extra.limit > 0 {
                let more = kvs.len() > extra.limit;
                kvs.truncate(extra.limit);
                more
            } else {
                false
            };
            Ok(GetResponse { kvs, more })
        }

        fn revision(&self) -> i64 {
            self.revision
        }
    }

    #[async_trait]
    impl MetaStore for SharedSlashEtc {
        type Snap = WithRevision<Self>;

        async fn snapshot(&self) -> Result<Self::Snap> {
            Ok(WithRevision {
                inner: self.clone(),
                revision: self.lock().await.revision,
            })
        }

        async fn set(&self, mut pair: crate::metadata::keys::KeyValue) -> Result<()> {
            let mut data = self.lock().await;
            data.revision += 1;
            for sub in data.subs.values() {
                if pair.key() < sub.end_key.as_slice() && pair.key() >= sub.start_key.as_slice() {
                    sub.tx
                        .send(KvEvent {
                            kind: KvEventType::Put,
                            pair: pair.clone(),
                        })
                        .await
                        .unwrap();
                }
            }
            data.items.insert(pair.take_key(), pair.take_value());
            Ok(())
        }

        async fn watch(
            &self,
            keys: crate::metadata::store::Keys,
            start_rev: i64,
        ) -> Result<KvChangeSubscription> {
            let mut data = self.lock().await;
            if start_rev != data.revision + 1 {
                panic!(
                    "start from arbitrary revision is not supported yet; only watch (current_rev + 1) supported. (self.revision = {}; start_rev = {})",
                    data.revision, start_rev
                );
            }
            let id = data.sub_id_alloc.get();
            data.sub_id_alloc.set(id + 1);
            let this = self.clone();
            let (tx, rx) = mpsc::channel(64);
            let (start_key, end_key) = keys.into_bound();
            data.subs.insert(
                id,
                Subscriber {
                    start_key,
                    end_key,
                    tx,
                },
            );

            Ok(Subscription {
                stream: Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx).map(Result::Ok)),
                cancel: Box::pin(async move {
                    this.lock().await.subs.remove(&id);
                }),
            })
        }

        async fn delete(&self, keys: crate::metadata::store::Keys) -> Result<()> {
            let mut data = self.lock().await;
            let (start_key, end_key) = keys.into_bound();
            data.revision += 1;
            for mut victim in data
                .items
                .range::<[u8], _>((
                    Bound::Included(start_key.as_slice()),
                    Bound::Excluded(end_key.as_slice()),
                ))
                .map(|(k, _)| k.clone())
                .collect::<Vec<_>>()
            {
                data.items.remove(&victim);

                for sub in data.subs.values() {
                    if victim.as_slice() < sub.end_key.as_slice()
                        && victim.as_slice() >= sub.start_key.as_slice()
                    {
                        sub.tx
                            .send(KvEvent {
                                kind: KvEventType::Delete,
                                pair: KeyValue(MetaKey(std::mem::take(&mut victim)), vec![]),
                            })
                            .await
                            .unwrap();
                    }
                }
            }
            Ok(())
        }
    }
}

fn test_meta_cli() -> MetadataClient<SharedSlashEtc> {
    MetadataClient::new(SharedSlashEtc::default(), 42)
}

fn simple_task(name: &str) -> Task {
    let mut task = Task::default();
    task.info.set_name(name.to_owned());
    task.info.set_start_ts(1);
    task.info.set_end_ts(1000);
    let mut storage = StorageBackend::new();
    storage.set_noop(Noop::new());
    task.info.set_storage(storage);
    task.info.set_table_filter(vec!["*.*".to_owned()].into());
    task
}

// Maybe we can make it more generic...?
// But there isn't a AsIter trait :(
fn assert_range_matches(real: Vec<(Vec<u8>, Vec<u8>)>, expected: &[(&[u8], &[u8])]) {
    assert_eq!(
        real.len(),
        expected.len(),
        "range not match: {:?} vs {:?}",
        real,
        expected
    );
    assert!(
        real.iter()
            .zip(expected.iter())
            .all(|((k, v), (rk, rv))| { k.as_slice() == *rk && v.as_slice() == *rv }),
        "range not match: {:?} vs {:?}",
        real,
        expected,
    );
}

#[tokio::test]
async fn test_basic() -> Result<()> {
    let cli = test_meta_cli();
    let name = "simple";
    let task = simple_task(name);
    let ranges: &[(&[u8], &[u8])] = &[(b"1", b"2"), (b"4", b"5"), (b"6", b"8"), (b"8", b"9")];
    cli.insert_task_with_range(&task, ranges).await?;
    let remote_ranges = cli.ranges_of_task(name).await?.inner;
    assert_range_matches(remote_ranges, ranges);
    let overlap_ranges = cli
        .range_overlap_of_task(name, (b"7".to_vec(), b"9".to_vec()))
        .await?
        .inner;
    assert_range_matches(overlap_ranges, &[(b"6", b"8"), (b"8", b"9")]);
    let overlap_ranges = cli
        .range_overlap_of_task(name, (b"1".to_vec(), b"5".to_vec()))
        .await?
        .inner;
    assert_range_matches(overlap_ranges, &[(b"1", b"2"), (b"4", b"5")]);
    let overlap_ranges = cli
        .range_overlap_of_task(name, (b"1".to_vec(), b"4".to_vec()))
        .await?
        .inner;
    assert_range_matches(overlap_ranges, &[(b"1", b"2")]);
    Ok(())
}

fn task_matches(expected: &[Task], real: &[Task]) {
    assert_eq!(
        expected.len(),
        real.len(),
        "task not match: {:?} vs {:?}",
        expected,
        real
    );
    let name_set =
        HashSet::<_, RandomState>::from_iter(expected.iter().map(|t| t.info.name.clone()));
    let real = HashSet::<_, RandomState>::from_iter(real.iter().map(|t| t.info.name.clone()));
    assert!(
        name_set == real,
        "task not match: {:?} vs {:?}",
        name_set,
        real
    );
}

#[tokio::test]
async fn test_watch() -> Result<()> {
    let cli = test_meta_cli();
    let task = simple_task("simple_1");
    cli.insert_task_with_range(&task, &[]).await?;
    let initial_task_set = cli.get_tasks().await?;
    task_matches(initial_task_set.inner.as_slice(), &[task]);
    let watcher = cli.events_from(initial_task_set.revision).await?;
    let task2 = simple_task("simple_2");
    cli.insert_task_with_range(&task2, &[]).await?;
    cli.remove_task("simple_1").await?;
    watcher.cancel.await;
    let events = watcher.stream.collect::<Vec<_>>().await;
    assert_eq!(
        events,
        vec![
            MetadataEvent::AddTask {
                task: "simple_2".to_owned()
            },
            MetadataEvent::RemoveTask {
                task: "simple_1".to_owned()
            }
        ]
    );
    Ok(())
}

#[tokio::test]
async fn test_progress() -> Result<()> {
    let cli = test_meta_cli();
    let task = simple_task("simple_1");
    cli.insert_task_with_range(&task, &[]).await?;
    let progress = cli.progress_of_task(&task.info.name, 42).await?;
    assert_eq!(progress, task.info.start_ts);
    cli.step_task(&task.info.name, 42, 78).await?;
    let progress = cli.progress_of_task(&task.info.name, 42).await?;
    assert_eq!(progress, 78);
    let progress = cli.progress_of_task(&task.info.name, 24).await?;
    assert_eq!(progress, task.info.start_ts);
    cli.step_task(&task.info.name, 24, 87).await?;
    let progress = cli.progress_of_task(&task.info.name, 24).await?;
    assert_eq!(progress, 87);
    let progress = cli.progress_of_task(&task.info.name, 42).await?;
    assert_eq!(progress, 78);
    let other_store = MetadataClient::new(cli.meta_store.clone(), 43);
    let progress = other_store.progress_of_task(&task.info.name, 24).await?;
    assert_eq!(progress, task.info.start_ts);

    Ok(())
}
