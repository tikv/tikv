// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

pub mod slash_etc;
pub use slash_etc::SlashEtcStore;

pub mod etcd;
use std::{future::Future, pin::Pin};

use async_trait::async_trait;
pub use etcd::EtcdStore;
use tokio_stream::Stream;

// ==== Generic interface definition ====
use super::keys::{KeyValue, MetaKey};
use crate::errors::Result;

pub type BoxStream<T> = Pin<Box<dyn Stream<Item = T> + Send>>;
pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

#[derive(Debug, Default)]
pub struct Transaction {
    ops: Vec<TransactionOp>,
}

impl Transaction {
    fn into_ops(self) -> Vec<TransactionOp> {
        self.ops
    }

    fn put(mut self, kv: KeyValue) -> Self {
        self.ops.push(TransactionOp::Put(kv));
        self
    }

    fn delete(mut self, keys: Keys) -> Self {
        self.ops.push(TransactionOp::Delete(keys));
        self
    }
}

#[derive(Debug)]
pub enum TransactionOp {
    Put(KeyValue),
    Delete(Keys),
}

/// A simple wrapper for items associated with a revision.
///
/// Maybe implement Deref<Target = T> for it?
#[derive(Debug)]
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

/// A cancelable event stream.
pub struct Subscription<Event> {
    pub stream: BoxStream<Event>,
    /// Futures in rust are lazy.
    /// This is actually `async FnOnce()`.
    pub cancel: BoxFuture<()>,
}

/// The cancelable stream of kv events.
pub type KvChangeSubscription = Subscription<Result<KvEvent>>;

#[async_trait]
/// A storage for storing metadata.
pub trait MetaStore: Clone + Send + Sync {
    type Snap: Snapshot;
    /// Take a consistency snapshot from the store.
    /// Use the current timestamp.
    async fn snapshot(&self) -> Result<Self::Snap>;
    /// Watch change of some keys from the store.
    /// Can be canceled then by polling the `cancel` future in the Subscription.
    async fn watch(&self, keys: Keys, start_rev: i64) -> Result<KvChangeSubscription>;
    /// Execute an atomic write (write batch) over the store.
    /// Maybe support etcd-like compare operations?
    async fn txn(&self, txn: Transaction) -> Result<()>;

    /// Set a key in the store.
    /// Maybe rename it to `put` to keeping consistency with etcd?
    async fn set(&self, pair: KeyValue) -> Result<()> {
        self.txn(Transaction::default().put(pair)).await
    }
    /// Delete some keys.
    async fn delete(&self, keys: Keys) -> Result<()> {
        self.txn(Transaction::default().delete(keys)).await
    }
}
