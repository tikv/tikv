use std::{future::Future, pin::Pin};

use tokio_stream::Stream;

use super::keys::{KeyValue, MetaKey};
use crate::errors::Result;
use async_trait::async_trait;

type BoxStream<T> = Pin<Box<dyn Stream<Item = T> + Send>>;

/// A simple wrapper for items associated with a revision.
pub struct WithRevision<T> {
    pub revision: i64,
    pub inner: T,
}

/// The key set for getting.
/// I guess there should be a `&[u8]` in meta key,
/// but the etcd client requires Into<Vec<u8>> :(
pub enum Keys {
    Prefix(MetaKey),
    Range(MetaKey, MetaKey),
    Key(MetaKey),
}

#[derive(Default, Debug)]
pub struct GetExtra {
    pub desc_order: bool,
    pub limit: usize,
}

pub struct GetResponse {
    kvs: Vec<KeyValue>,
    more: bool,
}

#[async_trait]
pub trait Snapshot: Send + Sync + 'static {
    async fn get_extra(&self, keys: &Keys, extra: GetExtra) -> Result<GetResponse>;
    fn revision(&self) -> i64;

    async fn get(&self, keys: &Keys) -> Result<Vec<KeyValue>> {
        self.get_extra(keys, GetExtra::default())
            .await
            .map(|r| r.kvs)
    }
}

pub enum KvEventType {
    Put,
    Delete,
}

pub struct KvEvent {
    pub kind: KvEventType,
    pub pair: KeyValue,
}

pub struct WatchResult {
    pub stream: BoxStream<Result<KvEvent>>,
    pub cancel: Pin<Box<dyn Future<Output = ()> + Send>>,
}

#[async_trait]
pub trait MetaStore {
    type Snap: Snapshot;
    fn snap(&self) -> Self::Snap;
    async fn set(&self, pair: KeyValue) -> Result<()>;
    async fn watch(&self, keys: Keys, start_rev: i64) -> Result<WatchResult>;
}
