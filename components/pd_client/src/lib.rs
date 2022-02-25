// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.
#[allow(unused_extern_crates)]
extern crate tikv_alloc;

mod client;
mod feature_gate;
pub mod metrics;
mod tso;
mod util;

mod config;
pub mod errors;
pub use self::client::{DummyPdClient, RpcClient};
pub use self::config::Config;
pub use self::errors::{Error, Result};
pub use self::feature_gate::{Feature, FeatureGate};
pub use self::util::PdConnector;
pub use self::util::REQUEST_RECONNECT_INTERVAL;

use std::collections::HashMap;
use std::ops::Deref;
use std::time::Duration;

use futures::future::BoxFuture;
use grpcio::ClientSStreamReceiver;
use kvproto::metapb;
use kvproto::pdpb;
use kvproto::replication_modepb::{RegionReplicationStatus, ReplicationStatus};
use pdpb::{QueryStats, WatchGlobalConfigResponse};
use tikv_util::time::{Instant, UnixSecs};
use txn_types::TimeStamp;

pub type Key = Vec<u8>;
pub type PdFuture<T> = BoxFuture<'static, Result<T>>;

#[derive(Default, Clone)]
pub struct RegionStat {
    pub down_peers: Vec<pdpb::PeerStats>,
    pub pending_peers: Vec<metapb::Peer>,
    pub written_bytes: u64,
    pub written_keys: u64,
    pub read_bytes: u64,
    pub read_keys: u64,
    pub query_stats: QueryStats,
    pub approximate_size: u64,
    pub approximate_keys: u64,
    pub last_report_ts: UnixSecs,
    // cpu_usage is the CPU time usage of the leader region since the last heartbeat,
    // which is calculated by cpu_time_delta/heartbeat_reported_interval.
    pub cpu_usage: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RegionInfo {
    pub region: metapb::Region,
    pub leader: Option<metapb::Peer>,
}

impl RegionInfo {
    pub fn new(region: metapb::Region, leader: Option<metapb::Peer>) -> RegionInfo {
        RegionInfo { region, leader }
    }
}

impl Deref for RegionInfo {
    type Target = metapb::Region;

    fn deref(&self) -> &Self::Target {
        &self.region
    }
}

pub struct BucketStat {
    pub version: u64,
    pub keys: Vec<Vec<u8>>,
    pub stats: metapb::BucketStats,
    pub region_epoch: metapb::RegionEpoch,
    pub last_report_time: Instant,
}

impl Default for BucketStat {
    fn default() -> Self {
        Self {
            last_report_time: Instant::now(),
            ..Default::default()
        }
    }
}

impl BucketStat {
    pub fn merge(&mut self, buckets: &metapb::Buckets) {
        use std::cmp::Ordering;

        match self.version.cmp(&buckets.version) {
            Ordering::Equal => {
                for i in 0..(self.keys.len() - 1) {
                    self.stats.read_bytes[i] += buckets.get_stats().read_bytes[i];
                    self.stats.write_bytes[i] += buckets.get_stats().write_bytes[i];

                    self.stats.read_qps[i] += buckets.get_stats().read_qps[i];
                    self.stats.write_qps[i] += buckets.get_stats().write_qps[i];

                    self.stats.read_keys[i] += buckets.get_stats().read_keys[i];
                    self.stats.write_keys[i] += buckets.get_stats().write_keys[i];
                }
            }
            // Statistics from a old version, merge incoming stats into current stats.
            Ordering::Greater => {
                for i in 0..(buckets.keys.len() - 1) {
                    let start = &self.keys[i];
                    let end = &self.keys[i + 1];
                    let (start_idx, end_idx) = find_overlay_ranges((start, end), &self.keys);
                    for j in start_idx..end_idx {
                        self.stats.read_bytes[j] += buckets.get_stats().read_bytes[i];
                        self.stats.write_bytes[j] += buckets.get_stats().write_bytes[i];

                        self.stats.read_qps[j] += buckets.get_stats().read_qps[i];
                        self.stats.write_qps[j] += buckets.get_stats().write_qps[i];

                        self.stats.read_keys[j] += buckets.get_stats().read_keys[i];
                        self.stats.write_keys[j] += buckets.get_stats().write_keys[i];
                    }
                }
            }
            // Statistics from a new version.
            Ordering::Less => {
                let mut stats = buckets.get_stats().clone();
                for i in 0..(self.keys.len() - 1) {
                    let start = &buckets.keys[i];
                    let end = &buckets.keys[i + 1];
                    let (start_idx, end_idx) = find_overlay_ranges((start, end), &buckets.keys);
                    for j in start_idx..end_idx {
                        stats.read_bytes[j] += self.stats.read_bytes[i];
                        stats.write_bytes[j] += self.stats.write_bytes[i];

                        stats.read_qps[j] += self.stats.read_qps[i];
                        stats.write_qps[j] += self.stats.write_qps[i];

                        stats.read_keys[j] += self.stats.read_keys[i];
                        stats.write_keys[j] += self.stats.write_keys[i];
                    }
                }
                self.stats = stats;
                self.keys = buckets.keys.to_vec();
            }
        }
    }
}

fn find_overlay_ranges(range: (&Vec<u8>, &Vec<u8>), keys: &[Vec<u8>]) -> (usize, usize) {
    let start = keys
        .binary_search(range.0)
        .unwrap_or_else(|i| if i != 0 { i - 1 } else { 0 });
    let end = keys.binary_search(range.1).unwrap_or_else(|i| i);
    (start, end)
}

pub const INVALID_ID: u64 = 0;

/// PdClient communicates with Placement Driver (PD).
/// Because now one PD only supports one cluster, so it is no need to pass
/// cluster id in trait interface every time, so passing the cluster id when
/// creating the PdClient is enough and the PdClient will use this cluster id
/// all the time.
pub trait PdClient: Send + Sync {
    /// Load a list of GlobalConfig
    fn load_global_config(&self, _list: Vec<String>) -> PdFuture<HashMap<String, String>> {
        unimplemented!();
    }

    /// Store a list of GlobalConfig
    fn store_global_config(&self, _list: HashMap<String, String>) -> PdFuture<()> {
        unimplemented!();
    }

    /// Watching change of GlobalConfig
    fn watch_global_config(&self) -> Result<ClientSStreamReceiver<WatchGlobalConfigResponse>> {
        unimplemented!();
    }

    /// Returns the cluster ID.
    fn get_cluster_id(&self) -> Result<u64> {
        unimplemented!();
    }

    /// Creates the cluster with cluster ID, node, stores and first Region.
    /// If the cluster is already bootstrapped, return ClusterBootstrapped error.
    /// When a node starts, if it finds nothing in the node and
    /// cluster is not bootstrapped, it begins to create node, stores, first Region
    /// and then call bootstrap_cluster to let PD know it.
    /// It may happen that multi nodes start at same time to try to
    /// bootstrap, but only one can succeed, while others will fail
    /// and must remove their created local Region data themselves.
    fn bootstrap_cluster(
        &self,
        _stores: metapb::Store,
        _region: metapb::Region,
    ) -> Result<Option<ReplicationStatus>> {
        unimplemented!();
    }

    /// Returns whether the cluster is bootstrapped or not.
    ///
    /// Cluster must be bootstrapped when we use it, so when the
    /// node starts, `is_cluster_bootstrapped` must be called,
    /// and panics if cluster was not bootstrapped.
    fn is_cluster_bootstrapped(&self) -> Result<bool> {
        unimplemented!();
    }

    /// Allocates a unique positive id.
    fn alloc_id(&self) -> Result<u64> {
        unimplemented!();
    }

    /// Informs PD when the store starts or some store information changes.
    fn put_store(&self, _store: metapb::Store) -> Result<Option<ReplicationStatus>> {
        unimplemented!();
    }

    /// We don't need to support Region and Peer put/delete,
    /// because PD knows all Region and Peers itself:
    /// - For bootstrapping, PD knows first Region with `bootstrap_cluster`.
    /// - For changing Peer, PD determines where to add a new Peer in some store
    ///   for this Region.
    /// - For Region splitting, PD determines the new Region id and Peer id for the
    ///   split Region.
    /// - For Region merging, PD knows which two Regions will be merged and which Region
    ///   and Peers will be removed.
    /// - For auto-balance, PD determines how to move the Region from one store to another.

    /// Gets store information if it is not a tombstone store.
    fn get_store(&self, _store_id: u64) -> Result<metapb::Store> {
        unimplemented!();
    }

    /// Gets store information if it is not a tombstone store asynchronously
    fn get_store_async(&self, _store_id: u64) -> PdFuture<metapb::Store> {
        unimplemented!();
    }

    /// Gets all stores information.
    fn get_all_stores(&self, _exclude_tombstone: bool) -> Result<Vec<metapb::Store>> {
        unimplemented!();
    }

    /// Gets cluster meta information.
    fn get_cluster_config(&self) -> Result<metapb::Cluster> {
        unimplemented!();
    }

    /// For route.
    /// Gets Region which the key belongs to.
    fn get_region(&self, _key: &[u8]) -> Result<metapb::Region> {
        unimplemented!();
    }

    /// Gets Region which the key belongs to asynchronously.
    fn get_region_async<'k>(&'k self, _key: &'k [u8]) -> BoxFuture<'k, Result<metapb::Region>> {
        unimplemented!();
    }

    /// Gets Region info which the key belongs to.
    fn get_region_info(&self, _key: &[u8]) -> Result<RegionInfo> {
        unimplemented!();
    }

    /// Gets Region info which the key belongs to asynchronously.
    fn get_region_info_async<'k>(&'k self, _key: &'k [u8]) -> BoxFuture<'k, Result<RegionInfo>> {
        unimplemented!();
    }

    /// Gets Region by Region id.
    fn get_region_by_id(&self, _region_id: u64) -> PdFuture<Option<metapb::Region>> {
        unimplemented!();
    }

    /// Gets Region and its leader by Region id.
    fn get_region_leader_by_id(
        &self,
        _region_id: u64,
    ) -> PdFuture<Option<(metapb::Region, metapb::Peer)>> {
        unimplemented!();
    }

    /// Region's Leader uses this to heartbeat PD.
    fn region_heartbeat(
        &self,
        _term: u64,
        _region: metapb::Region,
        _leader: metapb::Peer,
        _region_stat: RegionStat,
        _replication_status: Option<RegionReplicationStatus>,
    ) -> PdFuture<()> {
        unimplemented!();
    }

    /// Gets a stream of Region heartbeat response.
    ///
    /// Please note that this method should only be called once.
    fn handle_region_heartbeat_response<F>(&self, _store_id: u64, _f: F) -> PdFuture<()>
    where
        Self: Sized,
        F: Fn(pdpb::RegionHeartbeatResponse) + Send + 'static,
    {
        unimplemented!();
    }

    /// Asks PD for split. PD returns the newly split Region id.
    fn ask_split(&self, _region: metapb::Region) -> PdFuture<pdpb::AskSplitResponse> {
        unimplemented!();
    }

    /// Asks PD for batch split. PD returns the newly split Region ids.
    fn ask_batch_split(
        &self,
        _region: metapb::Region,
        _count: usize,
    ) -> PdFuture<pdpb::AskBatchSplitResponse> {
        unimplemented!();
    }

    /// Sends store statistics regularly.
    fn store_heartbeat(
        &self,
        _stats: pdpb::StoreStats,
        _report: Option<pdpb::StoreReport>,
    ) -> PdFuture<pdpb::StoreHeartbeatResponse> {
        unimplemented!();
    }

    /// Reports PD the split Region.
    fn report_batch_split(&self, _regions: Vec<metapb::Region>) -> PdFuture<()> {
        unimplemented!();
    }

    /// Scatters the Region across the cluster.
    fn scatter_region(&self, _: RegionInfo) -> Result<()> {
        unimplemented!();
    }

    /// Registers a handler to the client, which will be invoked after reconnecting to PD.
    ///
    /// Please note that this method should only be called once.
    fn handle_reconnect<F: Fn() + Sync + Send + 'static>(&self, _: F)
    where
        Self: Sized,
    {
    }

    fn get_gc_safe_point(&self) -> PdFuture<u64> {
        unimplemented!();
    }

    /// Gets store state if it is not a tombstone store asynchronously.
    fn get_store_stats_async(&self, _store_id: u64) -> BoxFuture<'_, Result<pdpb::StoreStats>> {
        unimplemented!();
    }

    /// Gets current operator of the region
    fn get_operator(&self, _region_id: u64) -> Result<pdpb::GetOperatorResponse> {
        unimplemented!();
    }

    /// Gets a timestamp from PD.
    fn get_tso(&self) -> PdFuture<TimeStamp> {
        unimplemented!()
    }

    /// Gets the internal `FeatureGate`.
    fn feature_gate(&self) -> &FeatureGate {
        unimplemented!()
    }

    /// Region's Leader uses this to report buckets to PD.
    fn region_buckets(
        &self,
        _region_id: u64,
        _bucket_stat: &BucketStat,
        _period: Duration,
    ) -> PdFuture<()> {
        unimplemented!();
    }
}

const REQUEST_TIMEOUT: u64 = 2; // 2s

/// Takes the peer address (for sending raft messages) from a store.
pub fn take_peer_address(store: &mut metapb::Store) -> String {
    if !store.get_peer_address().is_empty() {
        store.take_peer_address()
    } else {
        store.take_address()
    }
}

#[cfg(test)]
mod tests {
    use crate::find_overlay_ranges;

    #[test]
    fn test_find_overlay_ranges() {
        let keys = [
            b"k1".to_vec(),
            b"k3".to_vec(),
            b"k5".to_vec(),
            b"k7".to_vec(),
            b"k9".to_vec(),
        ];
        let cases = [
            ((&b"k1".to_vec(), &b"k8".to_vec()), (0, 4)),
            ((&b"k3".to_vec(), &b"k8".to_vec()), (1, 4)),
            ((&b"k3".to_vec(), &b"k7".to_vec()), (1, 3)),
            ((&b"k2".to_vec(), &b"k7".to_vec()), (0, 3)),
            ((&b"k2".to_vec(), &b"k8".to_vec()), (0, 4)),
        ];
        for (range, expected) in cases {
            assert_eq!(find_overlay_ranges(range, &keys), expected, "{:?}", range)
        }
    }

    #[test]
    fn test_merge_bucket_stats() {}
}
