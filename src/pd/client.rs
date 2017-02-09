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

use util::Either;

use kvproto::{metapb, pdpb};

use super::{Result, PdClient, v1client, v2client};

#[derive(Debug)]
pub struct RpcClient {
    pub client: Either<v2client::Client, v1client::Client>,
}

impl RpcClient {
    pub fn new(endpoints: &str) -> Result<RpcClient> {
        let endpoints: Vec<String> = endpoints.split(',')
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty())
            .collect();

        let client = match v2client::Client::new(endpoints.clone()) {
            Ok(v2) => Either::Left(v2),
            Err(e) => {
                error!("creating PD grpc client failed, use original client: {:?}",
                       e);
                Either::Right(try!(v1client::Client::new(endpoints)))
            }
        };

        Ok(RpcClient { client: client })
    }
}

impl PdClient for RpcClient {
    fn get_cluster_id(&self) -> Result<u64> {
        match self.client {
            Either::Left(ref l) => l.get_cluster_id(),
            Either::Right(ref r) => r.get_cluster_id(),
        }
    }

    fn bootstrap_cluster(&self, store: metapb::Store, region: metapb::Region) -> Result<()> {
        match self.client {
            Either::Left(ref l) => l.bootstrap_cluster(store, region),
            Either::Right(ref r) => r.bootstrap_cluster(store, region),
        }
    }

    fn is_cluster_bootstrapped(&self) -> Result<bool> {
        match self.client {
            Either::Left(ref l) => l.is_cluster_bootstrapped(),
            Either::Right(ref r) => r.is_cluster_bootstrapped(),
        }
    }

    fn alloc_id(&self) -> Result<u64> {
        match self.client {
            Either::Left(ref l) => l.alloc_id(),
            Either::Right(ref r) => r.alloc_id(),
        }
    }

    fn put_store(&self, store: metapb::Store) -> Result<()> {
        match self.client {
            Either::Left(ref l) => l.put_store(store),
            Either::Right(ref r) => r.put_store(store),
        }
    }

    fn get_store(&self, store_id: u64) -> Result<metapb::Store> {
        match self.client {
            Either::Left(ref l) => l.get_store(store_id),
            Either::Right(ref r) => r.get_store(store_id),
        }
    }

    fn get_cluster_config(&self) -> Result<metapb::Cluster> {
        match self.client {
            Either::Left(ref l) => l.get_cluster_config(),
            Either::Right(ref r) => r.get_cluster_config(),
        }
    }

    fn get_region(&self, key: &[u8]) -> Result<metapb::Region> {
        match self.client {
            Either::Left(ref l) => l.get_region(key),
            Either::Right(ref r) => r.get_region(key),
        }
    }

    fn get_region_by_id(&self, region_id: u64) -> Result<Option<metapb::Region>> {
        match self.client {
            Either::Left(ref l) => l.get_region_by_id(region_id),
            Either::Right(ref r) => r.get_region_by_id(region_id),
        }
    }

    fn region_heartbeat(&self,
                        region: metapb::Region,
                        leader: metapb::Peer,
                        down_peers: Vec<pdpb::PeerStats>,
                        pending_peers: Vec<metapb::Peer>)
                        -> Result<pdpb::RegionHeartbeatResponse> {
        match self.client {
            Either::Left(ref l) => l.region_heartbeat(region, leader, down_peers, pending_peers),
            Either::Right(ref r) => r.region_heartbeat(region, leader, down_peers, pending_peers),
        }
    }

    fn ask_split(&self, region: metapb::Region) -> Result<pdpb::AskSplitResponse> {
        match self.client {
            Either::Left(ref l) => l.ask_split(region),
            Either::Right(ref r) => r.ask_split(region),
        }
    }

    fn store_heartbeat(&self, stats: pdpb::StoreStats) -> Result<()> {
        match self.client {
            Either::Left(ref l) => l.store_heartbeat(stats),
            Either::Right(ref r) => r.store_heartbeat(stats),
        }
    }

    fn report_split(&self, left: metapb::Region, right: metapb::Region) -> Result<()> {
        match self.client {
            Either::Left(ref l) => l.report_split(left, right),
            Either::Right(ref r) => r.report_split(left, right),
        }
    }
}
