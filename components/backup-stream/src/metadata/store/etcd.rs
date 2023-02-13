// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    pin::Pin,
    sync::{Arc, Weak},
    time::Duration,
};

use async_trait::async_trait;
use etcd_client::{
    Client, Compare, CompareOp, DeleteOptions, EventType, GetOptions, Member, PutOptions,
    SortOrder, SortTarget, Txn, TxnOp, WatchOptions,
};
use futures::StreamExt;
use tikv_util::{info, warn};
use tokio::sync::Mutex;
use tokio_stream::Stream;

use super::{
    GetExtra, GetResponse, Keys, KvChangeSubscription, KvEventType, MetaStore, Snapshot,
    TransactionOp,
};
use crate::{
    annotate,
    errors::{Error, EtcdErrorExt, Result},
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

#[derive(Default)]
pub(super) struct TopologyUpdater<C> {
    last_urls: HashSet<String>,
    client: Weak<Mutex<C>>,

    // back off configs
    pub(super) loop_interval: Duration,
    pub(super) loop_failure_back_off: Duration,
}

impl<C> std::fmt::Debug for TopologyUpdater<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopologyUpdater")
            .field("last_urls", &self.last_urls)
            .finish()
    }
}

#[async_trait]
pub(super) trait ClusterInfoProvider {
    async fn get_members(&mut self) -> Result<Vec<Member>>;
    async fn add_endpoint(&mut self, endpoint: &str) -> Result<()>;
    async fn remove_endpoint(&mut self, endpoint: &str) -> Result<()>;
}

#[async_trait]
impl ClusterInfoProvider for Client {
    async fn get_members(&mut self) -> Result<Vec<Member>> {
        let result = self.member_list().await?;
        Ok(result.members().to_vec())
    }

    async fn add_endpoint(&mut self, endpoint: &str) -> Result<()> {
        Client::add_endpoint(self, endpoint)
            .await
            .map_err(|err| annotate!(err, "during adding the endpoint {}", endpoint))?;
        Ok(())
    }

    async fn remove_endpoint(&mut self, endpoint: &str) -> Result<()> {
        Client::remove_endpoint(self, endpoint)
            .await
            .map_err(|err| annotate!(err, "during removing the endpoint {}", endpoint))?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum DiffType {
    Add,
    Remove,
}

#[derive(Clone)]
struct Diff {
    diff_type: DiffType,
    url: String,
}

impl std::fmt::Debug for Diff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let syn = match self.diff_type {
            DiffType::Add => "+",
            DiffType::Remove => "-",
        };
        write!(f, "{}{}", syn, self.url)
    }
}

impl<C: ClusterInfoProvider> TopologyUpdater<C> {
    // Note: we may require the initial endpoints from the arguments directly.
    // So the internal map won't get inconsistent when the cluster config changed
    // during initializing.
    // But that is impossible for now because we cannot query the node ID before
    // connecting.
    pub fn new(cluster_ref: Weak<Mutex<C>>) -> Self {
        Self {
            last_urls: Default::default(),
            client: cluster_ref,

            loop_interval: Duration::from_secs(60),
            loop_failure_back_off: Duration::from_secs(10),
        }
    }

    pub fn init(&mut self, members: impl Iterator<Item = String>) {
        for mem in members {
            self.last_urls.insert(mem);
        }
    }

    fn diff(&self, incoming: &[Member]) -> Vec<Diff> {
        let newer = incoming
            .iter()
            .flat_map(|mem| mem.client_urls().iter())
            .collect::<HashSet<_>>();
        let mut result = vec![];
        for url in &newer {
            if !self.last_urls.contains(*url) {
                result.push(Diff {
                    diff_type: DiffType::Add,
                    url: String::clone(url),
                })
            }
        }
        for url in &self.last_urls {
            if !newer.contains(url) {
                result.push(Diff {
                    diff_type: DiffType::Remove,
                    url: String::clone(url),
                })
            }
        }
        result
    }

    fn apply(&mut self, diff: &Diff) -> Option<String> {
        match diff.diff_type {
            DiffType::Add => match self.last_urls.insert(diff.url.clone()) {
                true => None,
                false => Some(format!(
                    "the member to adding with url {} overrides existing urls.",
                    diff.url
                )),
            },
            DiffType::Remove => match self.last_urls.remove(&diff.url) {
                true => None,
                false => Some(format!(
                    "the member to remove with url {} hasn't been added.",
                    diff.url
                )),
            },
        }
    }

    async fn update_topology_by(&mut self, cli: &mut C, diff: &Diff) -> Result<()> {
        match diff.diff_type {
            DiffType::Add => cli.add_endpoint(&diff.url).await?,
            DiffType::Remove => cli.remove_endpoint(&diff.url).await?,
        }
        Ok(())
    }

    async fn do_update(&mut self, cli: &mut C) -> Result<()> {
        let cluster = cli.get_members().await?;
        let diffs = self.diff(cluster.as_slice());
        if !diffs.is_empty() {
            info!("log backup updating store topology."; "diffs" => ?diffs, "current_state" => ?self);
        }
        for diff in diffs {
            match self.apply(&diff) {
                Some(warning) => {
                    warn!("log backup meet some wrong status when updating PD clients, skipping this update."; "warn" => %warning);
                }
                None => self.update_topology_by(cli, &diff).await?,
            }
        }
        Result::Ok(())
    }

    pub(super) async fn update_topology_loop(&mut self) {
        while let Some(cli) = self.client.upgrade() {
            let mut lock = cli.lock().await;
            let result = self.do_update(&mut lock).await;
            drop(lock);
            match result {
                Ok(_) => tokio::time::sleep(self.loop_interval).await,
                Err(err) => {
                    err.report("during updating etcd topology");
                    tokio::time::sleep(self.loop_failure_back_off).await;
                }
            }
        }
    }

    pub async fn main_loop(mut self) {
        info!("log backup topology updater finish initialization."; "current_state" => ?self);
        self.update_topology_loop().await
    }
}

impl EtcdStore {
    pub fn connect<E: AsRef<str>, S: AsRef<[E]>>(endpoints: S) -> Self {
        // TODO remove block_on
        let cli =
            futures::executor::block_on(etcd_client::Client::connect(&endpoints, None)).unwrap();
        Self(Arc::new(Mutex::new(cli)))
    }

    pub fn inner(&self) -> &Arc<Mutex<Client>> {
        &self.0
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
                        Ok(events) => {
                            if events.compact_revision() > 0 && events.canceled() {
                                return Box::pin(tokio_stream::once(Err(Error::Etcd(
                                    EtcdErrorExt::RevisionCompacted {
                                        current: events.compact_revision(),
                                    },
                                ))));
                            }
                            if events.canceled() {
                                return Box::pin(tokio_stream::once(Err(Error::Etcd(
                                    EtcdErrorExt::WatchCanceled,
                                ))));
                            }
                            Box::pin(tokio_stream::iter(
                                // TODO: remove the copy here via access the protobuf field
                                // directly.
                                #[allow(clippy::unnecessary_to_owned)]
                                events.events().to_owned().into_iter().filter_map(|event| {
                                    let kv = event.kv()?;
                                    Some(Ok(KvEvent {
                                        kind: event.event_type().into(),
                                        pair: kv.clone().into(),
                                    }))
                                }),
                            ))
                        }
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

#[cfg(test)]
mod test {
    use std::{
        collections::{HashMap, HashSet},
        fmt::Display,
        sync::Arc,
        time::Duration,
    };

    use async_trait::async_trait;
    use etcd_client::{proto::PbMember, Member};
    use tokio::{sync::Mutex, time::timeout};

    use super::{ClusterInfoProvider, TopologyUpdater};
    use crate::errors::Result;

    #[derive(Default, Debug)]
    struct FakeCluster {
        id_alloc: u64,
        members: HashMap<u64, Member>,
        endpoints: HashSet<String>,
    }

    #[async_trait]
    impl ClusterInfoProvider for FakeCluster {
        async fn get_members(&mut self) -> Result<Vec<Member>> {
            let members = self.members.values().cloned().collect();
            Ok(members)
        }

        async fn add_endpoint(&mut self, endpoint: &str) -> Result<()> {
            self.endpoints.insert(endpoint.to_owned());
            Ok(())
        }

        async fn remove_endpoint(&mut self, endpoint: &str) -> Result<()> {
            self.endpoints.remove(endpoint);
            Ok(())
        }
    }

    impl FakeCluster {
        fn new_id(&mut self) -> u64 {
            let i = self.id_alloc;
            self.id_alloc += 1;
            i
        }

        fn init_with_member(&mut self, n: usize) -> Vec<String> {
            let mut endpoints = Vec::with_capacity(n);
            for _ in 0..n {
                let mem = self.add_member();
                let url = format!("fakestore://{}", mem);
                self.endpoints.insert(url.clone());
                endpoints.push(url);
            }
            endpoints
        }

        fn add_member(&mut self) -> u64 {
            let id = self.new_id();
            let mut mem = PbMember::default();
            mem.id = id;
            mem.client_ur_ls = vec![format!("fakestore://{}", id)];
            // Safety: `Member` is #[repr(transparent)].
            self.members.insert(id, unsafe { std::mem::transmute(mem) });
            id
        }

        fn remove_member(&mut self, id: u64) -> bool {
            self.members.remove(&id).is_some()
        }

        fn check_consistency(&self, message: impl Display) {
            let urls = self
                .members
                .values()
                .flat_map(|mem| mem.client_urls().iter().cloned())
                .collect::<HashSet<_>>();
            assert_eq!(
                urls, self.endpoints,
                "{}: consistency check not passed.",
                message
            );
        }
    }

    #[test]
    fn test_topology_updater() {
        let mut c = FakeCluster::default();
        let eps = c.init_with_member(3);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let sc = Arc::new(Mutex::new(c));
        let mut tu = TopologyUpdater::new(Arc::downgrade(&sc));
        tu.loop_failure_back_off = Duration::ZERO;
        tu.loop_interval = Duration::from_millis(100);
        tu.init(eps.into_iter());

        {
            let mut sc = sc.blocking_lock();
            sc.check_consistency("after init");
            sc.add_member();
            rt.block_on(tu.do_update(&mut sc)).unwrap();
            sc.check_consistency("adding nodes");
            sc.add_member();
            sc.add_member();
            rt.block_on(tu.do_update(&mut sc)).unwrap();
            sc.check_consistency("adding more nodes");
            assert!(sc.remove_member(0), "{:?}", sc);
            rt.block_on(tu.do_update(&mut sc)).unwrap();
            sc.check_consistency("removing nodes");
        }

        drop(sc);
        rt.block_on(async { timeout(Duration::from_secs(1), tu.update_topology_loop()).await })
            .unwrap()
    }
}
