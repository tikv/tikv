// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

mod client;
mod metrics;
mod util;

mod config;
pub mod errors;
pub mod pd;
pub use self::client::RpcClient;
pub use self::config::Config;
pub use self::errors::{Error, Result};
pub use self::pd::{Runner as PdRunner, Task as PdTask};
pub use self::util::validate_endpoints;
pub use self::util::RECONNECT_INTERVAL_SEC;
use raftstore::store::util::RegionApproximateStat;

use std::ops::Deref;

use futures::Future;
use kvproto::metapb;
use kvproto::pdpb;

pub type Key = Vec<u8>;
pub type PdFuture<T> = Box<Future<Item = T, Error = Error> + Send>;

#[derive(Default, Clone)]
pub struct RegionStat {
    pub down_peers: Vec<pdpb::PeerStats>,
    pub pending_peers: Vec<metapb::Peer>,
    pub written_bytes: u64,
    pub written_keys: u64,
    pub read_bytes: u64,
    pub read_keys: u64,
    pub approximate_stat: RegionApproximateStat,
    pub last_report_ts: u64,
}

#[derive(Debug, PartialEq)]
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

// Client to communicate with placement driver (pd) for special cluster.
// Because now one pd only supports one cluster, so it is no need to pass
// cluster id in trait interface every time, so passing the cluster id when
// creating the PdClient is enough and the PdClient will use this cluster id
// all the time.
pub trait PdClient: Send + Sync {
    // Return the cluster ID.
    fn get_cluster_id(&self) -> Result<u64>;

    // Create the cluster with cluster ID, node, stores and first region.
    // If the cluster is already bootstrapped, return ClusterBootstrapped error.
    // When a node starts, if it finds nothing in the node and
    // cluster is not bootstrapped, it begins to create node, stores, first region
    // and then call bootstrap_cluster to let pd know it.
    // It may happen that multi nodes start at same time to try to
    // bootstrap, and only one can success, others will fail
    // and must remove their created local region data themselves.
    fn bootstrap_cluster(&self, stores: metapb::Store, region: metapb::Region) -> Result<()>;

    // Return whether the cluster is bootstrapped or not.
    // We must use the cluster after bootstrapped, so when the
    // node starts, it must check it with is_cluster_bootstrapped,
    // and panic if not bootstrapped.
    fn is_cluster_bootstrapped(&self) -> Result<bool>;

    // Allocate a unique positive id.
    fn alloc_id(&self) -> Result<u64>;

    // When the store starts, or some store information changed, it
    // uses put_store to inform pd.
    fn put_store(&self, store: metapb::Store) -> Result<()>;

    // We don't need to support region and peer put/delete,
    // because pd knows all region and peers itself.
    // When bootstrapping, pd knows first region with bootstrap_cluster.
    // When changing peer, pd determines where to add a new peer in some store
    // for this region.
    // When region splitting, pd determines the new region id and peer id for the
    // split region.
    // When region merging, pd knows which two regions will be merged and which region
    // and peers will be removed.
    // When doing auto-balance, pd determines how to move the region from one store to another.

    // Get store information.
    fn get_store(&self, store_id: u64) -> Result<metapb::Store>;

    // Get all stores information.
    fn get_all_stores(&self) -> Result<Vec<metapb::Store>> {
        unimplemented!();
    }

    // Get cluster meta information.
    fn get_cluster_config(&self) -> Result<metapb::Cluster>;

    // For route.
    // Get region which the key belong to.
    fn get_region(&self, key: &[u8]) -> Result<metapb::Region>;

    // Get region info which the key belong to.
    fn get_region_info(&self, key: &[u8]) -> Result<RegionInfo> {
        self.get_region(key)
            .map(|region| RegionInfo::new(region, None))
    }

    // Get region by region id.
    fn get_region_by_id(&self, region_id: u64) -> PdFuture<Option<metapb::Region>>;

    // Leader for a region will use this to heartbeat Pd.
    fn region_heartbeat(
        &self,
        region: metapb::Region,
        leader: metapb::Peer,
        region_stat: RegionStat,
    ) -> PdFuture<()>;

    // Get a stream of region heartbeat response.
    //
    // Please note that this method should only be called once.
    fn handle_region_heartbeat_response<F>(&self, store_id: u64, f: F) -> PdFuture<()>
    where
        F: Fn(pdpb::RegionHeartbeatResponse) + Send + 'static;

    // Ask pd for split, pd will returns the new split region id.
    fn ask_split(&self, region: metapb::Region) -> PdFuture<pdpb::AskSplitResponse>;

    // Send store statistics regularly.
    fn store_heartbeat(&self, stats: pdpb::StoreStats) -> PdFuture<()>;

    // Report pd the split region.
    fn report_split(&self, left: metapb::Region, right: metapb::Region) -> PdFuture<()>;

    // Scatter the region across the cluster.
    fn scatter_region(&self, _: RegionInfo) -> Result<()> {
        unimplemented!();
    }

    // Register a handler to the client, it will be invoked after reconnecting to PD.
    //
    // Please note that this method should only be called once.
    fn handle_reconnect<F: Fn() + Sync + Send + 'static>(&self, _: F) {}
}

const REQUEST_TIMEOUT: u64 = 2; // 2s
