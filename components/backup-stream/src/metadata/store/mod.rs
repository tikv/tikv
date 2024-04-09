// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

// Note: these mods also used for integration tests,
//       so we cannot compile them only when `#[cfg(test)]`.
//       (See https://github.com/rust-lang/rust/issues/84629)
//       Maybe we'd better make a feature like `integration-test`?
pub mod slash_etc;
pub use slash_etc::SlashEtcStore;

pub mod pd;

use std::{cmp::Ordering, future::Future, pin::Pin, time::Duration};

use async_trait::async_trait;
use tokio_stream::Stream;

// ==== Generic interface definition ====
use super::keys::{KeyValue, MetaKey};
use crate::errors::Result;

pub type BoxStream<T> = Pin<Box<dyn Stream<Item = T> + Send>>;
pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
pub use pd::PdStore;

#[derive(Debug, Default)]
pub struct Transaction {
    ops: Vec<TransactionOp>,
}

/// A condition for executing a transcation.
/// Compare value a key with arg.
#[derive(Debug)]
pub struct Condition {
    over_key: Vec<u8>,
    result: Ordering,
    arg: Vec<u8>,
}

impl Condition {
    pub fn new(over_key: MetaKey, result: Ordering, arg: Vec<u8>) -> Self {
        Self {
            over_key: over_key.0,
            result,
            arg,
        }
    }
}

/// A conditional transaction.
/// This would atomically evaluate the condition, and execute corresponding
/// transaction.
#[derive(Debug)]
pub struct CondTransaction {
    cond: Condition,
    success: Transaction,
    failure: Transaction,
}

impl CondTransaction {
    pub fn new(cond: Condition, success: Transaction, failure: Transaction) -> Self {
        Self {
            cond,
            success,
            failure,
        }
    }
}

impl Transaction {
    fn into_ops(self) -> Vec<TransactionOp> {
        self.ops
    }

    pub fn put(mut self, kv: KeyValue) -> Self {
        self.ops.push(TransactionOp::Put(kv, PutOption::default()));
        self
    }

    pub fn put_opt(mut self, kv: KeyValue, opt: PutOption) -> Self {
        self.ops.push(TransactionOp::Put(kv, opt));
        self
    }

    pub fn delete(mut self, keys: Keys) -> Self {
        self.ops.push(TransactionOp::Delete(keys));
        self
    }
}

#[derive(Default, Debug)]
pub struct PutOption {
    pub ttl: Duration,
}

#[derive(Debug)]
pub enum TransactionOp {
    Put(KeyValue, PutOption),
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

impl<T> WithRevision<T> {
    pub fn map<R>(self, f: impl FnOnce(T) -> R) -> WithRevision<R> {
        WithRevision {
            revision: self.revision,
            inner: f(self.inner),
        }
    }
}

/// The key set for getting.
/// I guess there should be a `&[u8]` in meta key,
/// but the etcd client requires Into<Vec<u8>> :(
#[derive(Debug, Clone)]
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

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
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
    async fn txn(&self, txn: Transaction) -> Result<()>;
    /// Execute an conditional transaction over the store.
    async fn txn_cond(&self, txn: CondTransaction) -> Result<()>;

    /// Set a key in the store.
    /// Maybe rename it to `put` to keeping consistency with etcd?
    async fn set(&self, pair: KeyValue) -> Result<()> {
        self.txn(Transaction::default().put(pair)).await
    }
    /// Delete some keys.
    async fn delete(&self, keys: Keys) -> Result<()> {
        self.txn(Transaction::default().delete(keys)).await
    }
    /// Get the latest version of some keys.
    async fn get_latest(&self, keys: Keys) -> Result<WithRevision<Vec<KeyValue>>> {
        let s = self.snapshot().await?;
        let keys = s.get(keys).await?;
        Ok(WithRevision {
            revision: s.revision(),
            inner: keys,
        })
    }
}
