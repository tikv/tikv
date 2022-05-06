// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::Cell,
    collections::{BTreeMap, HashMap},
    ops::Bound,
    sync::Arc,
};

use async_trait::async_trait;
use slog_global::error;
use tikv_util::warn;
use tokio::sync::{
    mpsc::{self, Sender},
    Mutex,
};
use tokio_stream::StreamExt;

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

struct Subscriber {
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    tx: Sender<KvEvent>,
}

/// An in-memory, single versioned storage.
/// Emulating some interfaces of etcd for testing.
#[derive(Default)]
pub struct SlashEtc {
    items: BTreeMap<Vec<u8>, Vec<u8>>,
    // Maybe a range tree here if the test gets too slow.
    subs: HashMap<usize, Subscriber>,
    revision: i64,
    sub_id_alloc: Cell<usize>,
}

pub type SlashEtcStore = Arc<Mutex<SlashEtc>>;

#[async_trait]
impl Snapshot for WithRevision<SlashEtcStore> {
    async fn get_extra(
        &self,
        keys: crate::metadata::store::Keys,
        extra: crate::metadata::store::GetExtra,
    ) -> Result<crate::metadata::store::GetResponse> {
        let data = self.inner.lock().await;
        if data.revision != self.revision {
            warn!(
                "snapshot expired (multi version isn't supported yet, you may read steal data): {} vs {}",
                data.revision, self.revision
            );
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

impl SlashEtc {
    async fn set(&mut self, mut pair: crate::metadata::keys::KeyValue) -> Result<()> {
        let data = self;
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

    async fn delete(&mut self, keys: crate::metadata::store::Keys) -> Result<()> {
        let mut data = self;
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

#[async_trait]
impl MetaStore for SlashEtcStore {
    type Snap = WithRevision<Self>;

    async fn snapshot(&self) -> Result<Self::Snap> {
        Ok(WithRevision {
            inner: self.clone(),
            revision: self.lock().await.revision,
        })
    }

    async fn watch(
        &self,
        keys: crate::metadata::store::Keys,
        start_rev: i64,
    ) -> Result<KvChangeSubscription> {
        let mut data = self.lock().await;
        if start_rev != data.revision + 1 {
            error!(
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

    async fn txn(&self, txn: super::Transaction) -> Result<()> {
        let mut data = self.lock().await;
        for op in txn.into_ops() {
            match op {
                super::TransactionOp::Put(kv) => data.set(kv).await?,
                super::TransactionOp::Delete(range) => data.delete(range).await?,
            }
        }
        Ok(())
    }
}
