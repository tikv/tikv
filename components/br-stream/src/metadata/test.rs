#![cfg(test)]

use self::slash_etc::SharedSlashEtc;

use super::MetadataClient;

/// module slash etc (/etc) provides a etcd-like interface for testing.
mod slash_etc {
    use std::{
        cell::Cell,
        collections::{BTreeMap, HashMap},
        ops::Bound,
        sync::Arc,
    };

    use crate::metadata::{
        keys::{KeyValue, MetaKey},
        store::{
            GetResponse, KvEvent, KvEventType, MetaStore, Snapshot, Subscription, WithRevision,
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
        ) -> crate::errors::Result<crate::metadata::store::GetResponse> {
            let mut data = self.inner.lock().await;
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

        async fn snapshot(&self) -> crate::errors::Result<Self::Snap> {
            Ok(WithRevision {
                inner: self.clone(),
                revision: self.lock().await.revision,
            })
        }

        async fn set(
            &self,
            mut pair: crate::metadata::keys::KeyValue,
        ) -> crate::errors::Result<()> {
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
        ) -> crate::errors::Result<Subscription> {
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
    }
}

fn test_meta_cli() -> MetadataClient<SharedSlashEtc> {
    MetadataClient::new(SharedSlashEtc::default(), 42)
}

#[test]
fn test_basic() {
    let cli = test_meta_cli();
}
