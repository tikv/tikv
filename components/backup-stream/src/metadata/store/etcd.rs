// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{pin::Pin, sync::Arc};

use async_trait::async_trait;
use etcd_client::{
    DeleteOptions, EventType, GetOptions, SortOrder, SortTarget, Txn, TxnOp, WatchOptions,
};
use futures::StreamExt;
use tikv_util::warn;
use tokio::sync::Mutex;
use tokio_stream::Stream;

use super::{GetExtra, GetResponse, Keys, KvChangeSubscription, KvEventType, MetaStore, Snapshot};
use crate::{
    errors::Result,
    metadata::{
        keys::{KeyValue, MetaKey},
        store::{KvEvent, Subscription},
    },
};
// Can we get rid of the mutex? (which means, we must use a singleton client.)
// Or make a pool of clients?
#[derive(Clone)]
pub struct EtcdStore(Arc<Mutex<etcd_client::Client>>);

impl EtcdStore {
    pub fn connect<E: AsRef<str>, S: AsRef<[E]>>(endpoints: S) -> Self {
        // TODO remove block_on
        let cli =
            futures::executor::block_on(etcd_client::Client::connect(&endpoints, None)).unwrap();
        Self(Arc::new(Mutex::new(cli)))
    }
}

impl From<etcd_client::Client> for EtcdStore {
    fn from(cli: etcd_client::Client) -> Self {
        Self(Arc::new(Mutex::new(cli)))
    }
}

impl From<EventType> for KvEventType {
    fn from(e: EventType) -> Self {
        match e {
            EventType::Put => Self::Put,
            EventType::Delete => Self::Delete,
        }
    }
}

impl From<etcd_client::KeyValue> for KeyValue {
    fn from(kv: etcd_client::KeyValue) -> Self {
        // TODO: we can move out the vector in the KeyValue struct here. (instead of copying.)
        // But that isn't possible for now because:
        // - The raw KV pair(defined by the protocol buffer of etcd) is private.
        // - That did could be exported by `pub-fields` feature of the client.
        //   However that feature isn't published in theirs Cargo.toml (Is that a mistake?).
        // - Indeed, we can use `mem::transmute` here because `etcd_client::KeyValue` has `#[repr(transparent)]`.
        //   But before here become a known bottle neck, I'm not sure whether it's worthwhile for involving unsafe code.
        KeyValue(MetaKey(kv.key().to_owned()), kv.value().to_owned())
    }
}

/// Prepare the etcd options required by the keys.
/// Return the start key for requesting.
macro_rules! prepare_opt {
    ($opt: ident, $keys: expr) => {
        match $keys {
            Keys::Prefix(key) => {
                $opt = $opt.with_prefix();
                key
            }
            Keys::Range(key, end_key) => {
                $opt = $opt.with_range(end_key);
                key
            }
            Keys::Key(key) => key,
        }
    };
}

#[async_trait]
impl MetaStore for EtcdStore {
    type Snap = EtcdSnapshot;

    async fn snapshot(&self) -> Result<Self::Snap> {
        let status = self.0.lock().await.status().await?;
        Ok(EtcdSnapshot {
            store: self.clone(),
            revision: status.header().unwrap().revision(),
        })
    }

    async fn set(&self, pair: KeyValue) -> Result<()> {
        self.0.lock().await.put(pair.0, pair.1, None).await?;
        Ok(())
    }

    async fn watch(&self, keys: Keys, start_rev: i64) -> Result<KvChangeSubscription> {
        let mut opt = WatchOptions::new();
        let key = prepare_opt!(opt, keys);
        opt = opt.with_start_revision(start_rev);
        let (mut watcher, stream) = self.0.lock().await.watch(key, Some(opt)).await?;
        Ok(Subscription {
            stream: Box::pin(stream.flat_map(
                |events| -> Pin<Box<dyn Stream<Item = Result<KvEvent>> + Send>> {
                    match events {
                        Err(err) => Box::pin(tokio_stream::once(Err(err.into()))),
                        Ok(events) => Box::pin(tokio_stream::iter(
                            // TODO: remove the copy here via access the protobuf field directly.
                            #[allow(clippy::unnecessary_to_owned)]
                            events.events().to_owned().into_iter().filter_map(|event| {
                                let kv = event.kv()?;
                                Some(Ok(KvEvent {
                                    kind: event.event_type().into(),
                                    pair: kv.clone().into(),
                                }))
                            }),
                        )),
                    }
                },
            )),
            cancel: Box::pin(async move {
                if let Err(err) = watcher.cancel().await {
                    warn!("failed to cancel watch stream!"; "err" => %err);
                }
            }),
        })
    }

    async fn delete(&self, keys: Keys) -> Result<()> {
        let mut opt = DeleteOptions::new();
        let key = prepare_opt!(opt, keys);

        self.0.lock().await.delete(key, Some(opt)).await?;
        Ok(())
    }

    async fn txn(&self, t: super::Transaction) -> Result<()> {
        self.0.lock().await.txn(t.into()).await?;
        Ok(())
    }
}

impl From<super::Transaction> for Txn {
    fn from(etcd_txn: super::Transaction) -> Txn {
        let txn = Txn::default();
        txn.and_then(
            etcd_txn
                .into_ops()
                .into_iter()
                .map(|op| match op {
                    super::TransactionOp::Put(mut pair) => {
                        TxnOp::put(pair.take_key(), pair.take_value(), None)
                    }
                    super::TransactionOp::Delete(rng) => {
                        let mut opt = DeleteOptions::new();
                        let key = prepare_opt!(opt, rng);
                        TxnOp::delete(key, Some(opt))
                    }
                })
                .collect::<Vec<_>>(),
        )
    }
}

pub struct EtcdSnapshot {
    store: EtcdStore,
    revision: i64,
}

#[async_trait]
impl Snapshot for EtcdSnapshot {
    async fn get_extra(&self, keys: Keys, extra: GetExtra) -> Result<GetResponse> {
        let mut opt = GetOptions::new();
        let key = prepare_opt!(opt, keys);
        opt = opt.with_revision(self.revision);
        if extra.desc_order {
            opt = opt.with_sort(SortTarget::Key, SortOrder::Descend);
        }
        if extra.limit > 0 {
            opt = opt.with_limit(extra.limit as _);
        }
        let resp = self.store.0.lock().await.get(key.0, Some(opt)).await?;
        Ok(GetResponse {
            kvs: resp
                .kvs()
                .iter()
                .map(|kv| KeyValue(MetaKey(kv.key().to_owned()), kv.value().to_owned()))
                .collect(),
            more: resp.more(),
        })
    }

    fn revision(&self) -> i64 {
        self.revision
    }
}
