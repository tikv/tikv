// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::Cell,
    collections::{BTreeMap, HashMap},
    ops::Bound,
    sync::Arc,
};

use async_trait::async_trait;
use tokio::sync::{
    mpsc::{self, Sender},
    Mutex,
};
use tokio_stream::StreamExt;

use super::{Condition, Keys};
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

/// A key with revision.
#[derive(Default, Eq, PartialEq, Ord, PartialOrd, Clone)]
struct Key(Vec<u8>, i64);

impl std::fmt::Debug for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Key")
            .field(&format_args!(
                "{}@{}",
                log_wrappers::Value::key(&self.0),
                self.1
            ))
            .finish()
    }
}

/// A value (maybe tombstone.)
#[derive(Debug, PartialEq, Clone)]
enum Value {
    Val(Vec<u8>),
    Del,
}

/// An in-memory, single versioned storage.
/// Emulating some interfaces of etcd for testing.
#[derive(Default)]
pub struct SlashEtc {
    items: BTreeMap<Key, Value>,
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
        let mut kvs = data.get_key(keys);

        if extra.desc_order {
            kvs.reverse();
        }

        // use iterator operations (instead of collect all kv pairs in the range)
        // if the test case get too slow. (How can we figure out whether there are
        // more?)
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
    fn alloc_rev(&mut self) -> i64 {
        self.revision += 1;
        self.revision
    }

    fn get_key(&self, keys: super::Keys) -> Vec<KeyValue> {
        let (start_key, end_key) = keys.into_bound();
        let mvccs = self
            .items
            .range((
                Bound::Included(&Key(start_key, 0)),
                Bound::Excluded(&Key(end_key, 0)),
            ))
            .collect::<Vec<_>>();
        let kvs = mvccs
            .as_slice()
            .group_by(|k1, k2| k1.0.0 == k2.0.0)
            .filter_map(|k| {
                let (k, v) = k.last()?;
                match v {
                    Value::Val(val) => Some(KeyValue(MetaKey(k.0.clone()), val.clone())),
                    Value::Del => None,
                }
            })
            .collect::<Vec<_>>();
        kvs
    }

    async fn set(&mut self, mut pair: crate::metadata::keys::KeyValue) -> Result<()> {
        let data = self;
        let rev = data.alloc_rev();
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
        data.items
            .insert(Key(pair.take_key(), rev), Value::Val(pair.take_value()));
        Ok(())
    }

    async fn delete(&mut self, keys: crate::metadata::store::Keys) -> Result<()> {
        let data = self;
        let (start_key, end_key) = keys.into_bound();
        let rev = data.alloc_rev();
        let mut v = data
            .items
            .range((
                Bound::Included(Key(start_key, 0)),
                Bound::Excluded(Key(end_key, data.revision)),
            ))
            .map(|(k, _)| Key::clone(k))
            .collect::<Vec<_>>();
        v.dedup_by(|k1, k2| k1.0 == k2.0);

        for mut victim in v {
            let k = Key(victim.0.clone(), rev);
            data.items.insert(k, Value::Del);

            for sub in data.subs.values() {
                if victim.0.as_slice() < sub.end_key.as_slice()
                    && victim.0.as_slice() >= sub.start_key.as_slice()
                {
                    sub.tx
                        .send(KvEvent {
                            kind: KvEventType::Delete,
                            pair: KeyValue(MetaKey(std::mem::take(&mut victim.0)), vec![]),
                        })
                        .await
                        .unwrap();
                }
            }
        }
        Ok(())
    }

    /// A tool for dumpling the whole storage when test failed.
    /// Add this to test code temporarily for debugging.
    #[allow(dead_code)]
    pub fn dump(&self) {
        println!(">>>>>>> /etc (revision = {}) <<<<<<<", self.revision);
        for (k, v) in self.items.iter() {
            println!("{:?} => {:?}", k, v);
        }
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
        let id = data.sub_id_alloc.get();
        data.sub_id_alloc.set(id + 1);
        let this = self.clone();
        let (tx, rx) = mpsc::channel(1024);
        let (start_key, end_key) = keys.into_bound();

        // Sending events from [start_rev, now) to the client.
        let mut pending = data
            .items
            .iter()
            .filter(|(k, _)| k.1 >= start_rev)
            .collect::<Vec<_>>();
        pending.sort_by_key(|(k, _)| k.1);
        for (k, v) in pending {
            let event = match v {
                Value::Val(val) => KvEvent {
                    kind: KvEventType::Put,
                    pair: KeyValue(MetaKey(k.0.clone()), val.clone()),
                },
                Value::Del => KvEvent {
                    kind: KvEventType::Delete,
                    pair: KeyValue(MetaKey(k.0.clone()), vec![]),
                },
            };
            // Note: may panic if too many pending here?
            tx.send(event).await.expect("too many pending events");
        }

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
                super::TransactionOp::Put(kv, _) => data.set(kv).await?,
                super::TransactionOp::Delete(range) => data.delete(range).await?,
            }
        }
        Ok(())
    }

    async fn txn_cond(&self, txn: super::CondTransaction) -> Result<()> {
        let l = self.lock().await;
        let Condition {
            over_key,
            result,
            arg,
        } = txn.cond;
        let success = l
            .get_key(Keys::Key(MetaKey(over_key)))
            .last()
            .map(|k| k.0.0.cmp(&arg) == result)
            .unwrap_or(false);
        drop(l);
        let do_txn = if success { txn.success } else { txn.failure };
        self.txn(do_txn).await
    }
}
