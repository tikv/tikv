// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use etcd_client::{
    Client, Compare, CompareOp, DeleteOptions, EventType, GetOptions, PutOptions, SortOrder,
    SortTarget, Txn, TxnOp, WatchOptions,
};
use futures::StreamExt;
use tikv_util::warn;
use tokio::sync::Mutex;
use tokio_stream::Stream;

use super::{
    GetExtra, GetResponse, Keys, KvChangeSubscription, KvEventType, MetaStore, Snapshot,
    TransactionOp,
};
use crate::{
    errors::Result,
    metadata::{
        keys::{KeyValue, MetaKey},
        metrics::METADATA_KEY_OPERATION,
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
        // TODO: we can move out the vector in the KeyValue struct here. (instead of
        // copying.) But that isn't possible for now because:
        // - The raw KV pair(defined by the protocol buffer of etcd) is private.
        // - That did could be exported by `pub-fields` feature of the client. However
        //   that feature isn't published in theirs Cargo.toml (Is that a mistake?).
        // - Indeed, we can use `mem::transmute` here because `etcd_client::KeyValue`
        //   has `#[repr(transparent)]`. But before here become a known bottle neck, I'm
        //   not sure whether it's worthwhile for involving unsafe code.
        KeyValue(MetaKey(kv.key().to_owned()), kv.value().to_owned())
    }
}

/// Prepare the etcd options required by the keys.
/// Return the start key for requesting.
macro_rules! prepare_opt {
    ($opt:ident, $keys:expr) => {
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

    async fn txn(&self, t: super::Transaction) -> Result<()> {
        let mut cli = self.0.lock().await;
        let txns = Self::make_txn(&mut cli, t).await?;
        for txn in txns {
            cli.txn(txn).await?;
        }
        Ok(())
    }

    async fn set(&self, pair: KeyValue) -> Result<()> {
        self.0.lock().await.put(pair.0, pair.1, None).await?;
        Ok(())
    }

    async fn delete(&self, keys: Keys) -> Result<()> {
        let mut opt = DeleteOptions::new();
        let key = prepare_opt!(opt, keys);

        self.0.lock().await.delete(key, Some(opt)).await?;
        Ok(())
    }

    async fn txn_cond(&self, txn: super::CondTransaction) -> Result<()> {
        let mut cli = self.0.lock().await;
        let txn = Self::make_conditional_txn(&mut cli, txn).await?;
        cli.txn(txn).await?;
        Ok(())
    }
}

impl EtcdStore {
    fn collect_leases_needed(txn: &super::Transaction) -> HashSet<Duration> {
        txn.ops
            .iter()
            .filter_map(|op| match op {
                TransactionOp::Put(_, opt) if opt.ttl.as_secs() > 0 => Some(opt.ttl),
                _ => None,
            })
            .collect()
    }

    async fn make_leases(
        cli: &mut Client,
        needed: HashSet<Duration>,
    ) -> Result<HashMap<Duration, i64>> {
        let mut map = HashMap::with_capacity(needed.len());
        for lease_time in needed {
            let lease_id = cli.lease_grant(lease_time.as_secs() as _, None).await?.id();
            map.insert(lease_time, lease_id);
        }
        Ok(map)
    }

    fn partition_txns(mut txn: super::Transaction, leases: HashMap<Duration, i64>) -> Vec<Txn> {
        txn.ops
            .chunks_mut(128)
            .map(|txn| Txn::default().and_then(Self::to_txn(txn, &leases)))
            .collect()
    }

    fn to_compare(cond: super::Condition) -> Compare {
        let op = match cond.result {
            Ordering::Less => CompareOp::Less,
            Ordering::Equal => CompareOp::Equal,
            Ordering::Greater => CompareOp::Greater,
        };
        Compare::value(cond.over_key, op, cond.arg)
    }

    /// Convert the transaction operations to etcd transaction ops.
    fn to_txn(ops: &mut [super::TransactionOp], leases: &HashMap<Duration, i64>) -> Vec<TxnOp> {
        ops.iter_mut().map(|op| match op {
                TransactionOp::Put(key, opt) => {
                    let opts = if opt.ttl.as_secs() > 0 {
                        let lease = leases.get(&opt.ttl);
                        match lease {
                            None => {
                                warn!("lease not found, the request key may not have a ttl"; "dur" => ?opt.ttl);
                                None
                            }
                            Some(lease_id) => {
                                Some(PutOptions::new().with_lease(*lease_id))
                            }
                        }
                    } else {
                        None
                    };
                    TxnOp::put(key.take_key(), key.take_value(), opts)
                },
                TransactionOp::Delete(rng) => {
                    let rng = std::mem::replace(rng, Keys::Key(MetaKey(vec![])));
                    let mut opt = DeleteOptions::new();
                    let key = prepare_opt!(opt, rng);
                    TxnOp::delete(key, Some(opt))
                },
            }).collect::<Vec<_>>()
    }

    /// Make a conditional txn.
    /// For now, this wouldn't split huge transaction into smaller ones,
    /// so when playing with etcd in PD, conditional transaction should be
    /// small.
    async fn make_conditional_txn(
        cli: &mut Client,
        mut txn: super::CondTransaction,
    ) -> Result<Txn> {
        let cond = Self::to_compare(txn.cond);

        let mut leases_needed = Self::collect_leases_needed(&txn.success);
        leases_needed.extend(Self::collect_leases_needed(&txn.failure).into_iter());
        let leases = Self::make_leases(cli, leases_needed).await?;
        let success = Self::to_txn(&mut txn.success.ops, &leases);
        let failure = Self::to_txn(&mut txn.failure.ops, &leases);
        Ok(Txn::new().when([cond]).and_then(success).or_else(failure))
    }

    async fn make_txn(cli: &mut Client, etcd_txn: super::Transaction) -> Result<Vec<Txn>> {
        let (put_cnt, delete_cnt) = etcd_txn.ops.iter().fold((0, 0), |(p, d), item| match item {
            TransactionOp::Put(..) => (p + 1, d),
            TransactionOp::Delete(_) => (p, d + 1),
        });
        METADATA_KEY_OPERATION
            .with_label_values(&["put"])
            .inc_by(put_cnt);
        METADATA_KEY_OPERATION
            .with_label_values(&["del"])
            .inc_by(delete_cnt);
        let needed_leases = Self::collect_leases_needed(&etcd_txn);
        let leases = Self::make_leases(cli, needed_leases).await?;
        let txns = Self::partition_txns(etcd_txn, leases);
        Ok(txns)
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
