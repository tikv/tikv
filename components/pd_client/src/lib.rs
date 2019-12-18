// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

extern crate futures;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate slog_global;
extern crate hex;
extern crate kvproto;

#[allow(unused_extern_crates)]
extern crate tikv_alloc;
#[macro_use]
extern crate tikv_util;

mod client;
pub mod metrics;
mod util;

mod config;
pub mod errors;
pub use self::client::RpcClient;
pub use self::config::Config;
pub use self::errors::{Error, Result};
pub use self::util::validate_endpoints;
pub use self::util::RECONNECT_INTERVAL_SEC;

use std::ops::Deref;

use futures::Future;
use kvproto::configpb;
use kvproto::metapb;
use kvproto::pdpb;
use tikv_util::time::UnixSecs;

pub type Key = Vec<u8>;
pub type PdFuture<T> = Box<dyn Future<Item = T, Error = Error> + Send>;

#[derive(Default, Clone)]
pub struct RegionStat {
    pub down_peers: Vec<pdpb::PeerStats>,
    pub pending_peers: Vec<metapb::Peer>,
    pub written_bytes: u64,
    pub written_keys: u64,
    pub read_bytes: u64,
    pub read_keys: u64,
    pub approximate_size: u64,
    pub approximate_keys: u64,
    pub last_report_ts: UnixSecs,
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

pub const INVALID_ID: u64 = 0;

/// PdClient communicates with Placement Driver (PD).
/// Because now one PD only supports one cluster, so it is no need to pass
/// cluster id in trait interface every time, so passing the cluster id when
/// creating the PdClient is enough and the PdClient will use this cluster id
/// all the time.
pub trait PdClient: Send + Sync {
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
    fn bootstrap_cluster(&self, _stores: metapb::Store, _region: metapb::Region) -> Result<()> {
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
    fn put_store(&self, _store: metapb::Store) -> Result<()> {
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

    /// Gets Region info which the key belongs to.
    fn get_region_info(&self, _key: &[u8]) -> Result<RegionInfo> {
        unimplemented!();
    }

    /// Gets Region by Region id.
    fn get_region_by_id(&self, _region_id: u64) -> PdFuture<Option<metapb::Region>> {
        unimplemented!();
    }

    /// Region's Leader uses this to heartbeat PD.
    fn region_heartbeat(
        &self,
        _term: u64,
        _region: metapb::Region,
        _leader: metapb::Peer,
        _region_stat: RegionStat,
    ) -> PdFuture<()> {
        unimplemented!();
    }

    /// Gets a stream of Region heartbeat response.
    ///
    /// Please note that this method should only be called once.
    fn handle_region_heartbeat_response<F>(&self, _store_id: u64, _f: F) -> PdFuture<()>
    where
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
    fn store_heartbeat(&self, _stats: pdpb::StoreStats) -> PdFuture<()> {
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
    fn handle_reconnect<F: Fn() + Sync + Send + 'static>(&self, _: F) {}

    fn get_gc_safe_point(&self) -> PdFuture<u64> {
        unimplemented!();
    }

    /// Gets store state if it is not a tombstone store.
    fn get_store_stats(&self, _store_id: u64) -> Result<pdpb::StoreStats> {
        unimplemented!();
    }

    /// Gets current operator of the region
    fn get_operator(&self, _region_id: u64) -> Result<pdpb::GetOperatorResponse> {
        unimplemented!();
    }

    fn register_config(
        &self,
        _id: String,
        _version: configpb::Version,
        _cfg: String,
    ) -> Result<configpb::CreateResponse> {
        unimplemented!()
    }

    fn get_config(
        &self,
        _id: String,
        _version: configpb::Version,
    ) -> Result<configpb::GetResponse> {
        unimplemented!()
    }

    fn update_config(
        &self,
        _id: String,
        _version: configpb::Version,
        _entries: Vec<configpb::ConfigEntry>,
    ) -> Result<configpb::UpdateResponse> {
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
