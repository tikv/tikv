use std::{
    future::Future,
    ops::{Bound, Range, RangeBounds},
    pin::Pin,
    sync::Arc,
};

use etcd_client::{EventType, GetOptions, SortOrder, SortTarget, WatchOptions};
use futures::StreamExt;
use tikv_util::warn;
use tokio::sync::Mutex;
use tokio_stream::Stream;

use super::keys::{KeyValue, MetaKey};
use crate::errors::Result;
use async_trait::async_trait;

type BoxStream<T> = Pin<Box<dyn Stream<Item = T> + Send>>;
type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// A simple wrapper for items associated with a revision.
pub struct WithRevision<T> {
    pub revision: i64,
    pub inner: T,
}

/// The key set for getting.
/// I guess there should be a `&[u8]` in meta key,
/// but the etcd client requires Into<Vec<u8>> :(
#[derive(Debug)]
pub enum Keys {
    Prefix(MetaKey),
    Range(MetaKey, MetaKey),
    Key(MetaKey),
}

impl Keys {
    /// convert the key set for corresponding key range.
    pub fn into_bound(self) -> (Vec<u8>, Vec<u8>) {
        match self {
            Keys::Prefix(x) => {
                let next = x.next_prefix().0;
                ((x.0), (next))
            }
            Keys::Range(start, end) => ((start.0), (end.0)),
            Keys::Key(k) => {
                let next = k.next().0;
                ((k.0), (next))
            }
        }
    }
}

#[derive(Default, Debug)]
pub struct GetExtra {
    pub desc_order: bool,
    pub limit: usize,
    pub rev: usize,
}

pub struct GetResponse {
    pub kvs: Vec<KeyValue>,
    pub more: bool,
}

#[async_trait]
pub trait Snapshot: Send + Sync + 'static {
    async fn get_extra(&self, keys: Keys, extra: GetExtra) -> Result<GetResponse>;
    fn revision(&self) -> i64;

    async fn get(&self, keys: Keys) -> Result<Vec<KeyValue>> {
        self.get_extra(keys, GetExtra::default())
            .await
            .map(|r| r.kvs)
    }
}

#[derive(Debug)]
pub enum KvEventType {
    Put,
    Delete,
}

#[derive(Debug)]
pub struct KvEvent {
    pub kind: KvEventType,
    pub pair: KeyValue,
}

pub struct Subscription {
    pub stream: BoxStream<Result<KvEvent>>,
    /// Futures in rust are lazy.
    /// This is actually `async FnOnce()`.
    pub cancel: BoxFuture<()>,
}

#[async_trait]
/// A storage for storing metadata.
pub trait MetaStore: Clone + Send + Sync {
    type Snap: Snapshot;
    /// Take a consistency snapshot from the store.
    /// Use the current timestamp.
    async fn snapshot(&self) -> Result<Self::Snap>;
    /// Set a key in the store.
    async fn set(&self, pair: KeyValue) -> Result<()>;
    /// Watch a change stream from the store.
    /// Can be canceled then by polling the `cancel` future in the Subscription.
    async fn watch(&self, keys: Keys, start_rev: i64) -> Result<Subscription>;
}

// Can we get rid of the mutex? (which means, we must use a singleton client.)
// Or make a pool of clients?
#[derive(Clone)]
pub struct EtcdStore(Arc<Mutex<etcd_client::Client>>);

impl EtcdStore {
    pub async fn connect(pd_addr: &[&str]) -> Result<Self> {
        let cli = etcd_client::Client::connect(pd_addr, None).await?;
        Ok(Self(Arc::new(Mutex::new(cli))))
    }
}

impl Into<KvEventType> for EventType {
    fn into(self) -> KvEventType {
        match self {
            Self::Delete => KvEventType::Delete,
            Self::Put => KvEventType::Put,
        }
    }
}

impl Into<KeyValue> for etcd_client::KeyValue {
    fn into(self) -> KeyValue {
        KeyValue(MetaKey(self.key().to_owned()), self.value().to_owned())
    }
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

    async fn watch(&self, keys: Keys, start_rev: i64) -> Result<Subscription> {
        let mut opt = WatchOptions::new();
        let key = match keys {
            Keys::Prefix(key) => {
                opt = opt.with_prefix();
                key
            }
            Keys::Range(key, end_key) => {
                opt = opt.with_range(end_key);
                key
            }
            Keys::Key(key) => key,
        };
        opt = opt.with_start_revision(start_rev);
        let (mut watcher, stream) = self.0.lock().await.watch(key, Some(opt)).await?;
        Ok(Subscription {
            stream: Box::pin(stream.flat_map(
                |events| -> Pin<Box<dyn Stream<Item = Result<KvEvent>> + Send>> {
                    match events {
                        Err(err) => Box::pin(tokio_stream::once(Err(err.into()))),
                        Ok(events) => Box::pin(tokio_stream::iter(
                            events.events().to_owned().into_iter().filter_map(|event| {
                                // We must clone twice here to make borrow checker happy.
                                // Because the client warps the protobuf type and doesn't provide a take method.
                                let kv = event.kv()?;
                                Some(Ok(KvEvent {
                                    kind: event.event_type().clone().into(),
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
}

pub struct EtcdSnapshot {
    store: EtcdStore,
    revision: i64,
}

#[async_trait]
impl Snapshot for EtcdSnapshot {
    async fn get_extra(&self, keys: Keys, extra: GetExtra) -> Result<GetResponse> {
        let mut opt = GetOptions::new();
        let key = match keys {
            Keys::Prefix(key) => {
                opt = opt.with_prefix();
                key
            }
            Keys::Range(key, end_key) => {
                opt = opt.with_range(end_key);
                key
            }
            Keys::Key(key) => key,
        };
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
