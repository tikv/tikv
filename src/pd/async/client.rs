// Copyright 2017 PingCAP, Inc.
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

use std::fmt;
use std::sync::Arc;

use protobuf::RepeatedField;
use futures::Future;
use grpc::EnvBuilder;

use kvproto::metapb;
use kvproto::pdpb::{self, Member};
use kvproto::pdpb_grpc;

use super::super::PdFuture;
use super::super::{Result, Error, PdClient, RegionStat};
use super::util::{validate_endpoints, sync_request, check_resp_header, LeaderClient};

const CQ_COUNT: usize = 1;
const CLIENT_PREFIX: &'static str = "pd";

pub struct RpcClient {
    cluster_id: u64,
    leader_client: LeaderClient,
}

impl RpcClient {
    pub fn new(endpoints: &str) -> Result<RpcClient> {
        let endpoints: Vec<_> = endpoints.split(',')
            .map(|s| if !s.starts_with("http://") {
                format!("http://{}", s)
            } else {
                s.to_owned()
            })
            .collect();

        let env = Arc::new(EnvBuilder::new().cq_count(CQ_COUNT).name_prefix(CLIENT_PREFIX).build());
        let (client, members) = try!(validate_endpoints(env.clone(), &endpoints));

        Ok(RpcClient {
            cluster_id: members.get_header().get_cluster_id(),
            leader_client: LeaderClient::new(env, client, members),
        })
    }

    fn header(&self) -> pdpb::RequestHeader {
        let mut header = pdpb::RequestHeader::new();
        header.set_cluster_id(self.cluster_id);
        header
    }

    pub fn get_leader(&self) -> Member {
        self.leader_client.get_leader()
    }
}

impl fmt::Debug for RpcClient {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("RpcClient")
            .field("cluster_id", &self.cluster_id)
            .field("leader", &self.get_leader())
            .finish()
    }
}

const LEADER_CHANGE_RETRY: usize = 10;

impl PdClient for RpcClient {
    fn get_cluster_id(&self) -> Result<u64> {
        Ok(self.cluster_id)
    }

    fn bootstrap_cluster(&self, stores: metapb::Store, region: metapb::Region) -> Result<()> {
        let mut req = pdpb::BootstrapRequest::new();
        req.set_header(self.header());
        req.set_store(stores);
        req.set_region(region);

        let resp = try!(sync_request(&self.leader_client,
                                     LEADER_CHANGE_RETRY,
                                     |client| client.bootstrap(req.clone())));
        try!(check_resp_header(resp.get_header()));
        Ok(())
    }

    fn is_cluster_bootstrapped(&self) -> Result<bool> {
        let mut req = pdpb::IsBootstrappedRequest::new();
        req.set_header(self.header());

        let resp = try!(sync_request(&self.leader_client,
                                     LEADER_CHANGE_RETRY,
                                     |client| client.is_bootstrapped(req.clone())));
        try!(check_resp_header(resp.get_header()));

        Ok(resp.get_bootstrapped())
    }

    fn alloc_id(&self) -> Result<u64> {
        let mut req = pdpb::AllocIDRequest::new();
        req.set_header(self.header());

        let resp = try!(sync_request(&self.leader_client,
                                     LEADER_CHANGE_RETRY,
                                     |client| client.alloc_id(req.clone())));
        try!(check_resp_header(resp.get_header()));

        Ok(resp.get_id())
    }

    fn put_store(&self, store: metapb::Store) -> Result<()> {
        let mut req = pdpb::PutStoreRequest::new();
        req.set_header(self.header());
        req.set_store(store);

        let resp = try!(sync_request(&self.leader_client,
                                     LEADER_CHANGE_RETRY,
                                     |client| client.put_store(req.clone())));
        try!(check_resp_header(resp.get_header()));

        Ok(())
    }

    fn get_store(&self, store_id: u64) -> Result<metapb::Store> {
        let mut req = pdpb::GetStoreRequest::new();
        req.set_header(self.header());
        req.set_store_id(store_id);

        let mut resp = try!(sync_request(&self.leader_client,
                                         LEADER_CHANGE_RETRY,
                                         |client| client.get_store(req.clone())));
        try!(check_resp_header(resp.get_header()));

        Ok(resp.take_store())
    }

    fn get_cluster_config(&self) -> Result<metapb::Cluster> {
        let mut req = pdpb::GetClusterConfigRequest::new();
        req.set_header(self.header());

        let mut resp = try!(sync_request(&self.leader_client,
                                         LEADER_CHANGE_RETRY,
                                         |client| client.get_cluster_config(req.clone())));
        try!(check_resp_header(resp.get_header()));

        Ok(resp.take_cluster())
    }

    fn get_region(&self, key: &[u8]) -> Result<metapb::Region> {
        let mut req = pdpb::GetRegionRequest::new();
        req.set_header(self.header());
        req.set_region_key(key.into());

        let mut resp = try!(sync_request(&self.leader_client,
                                         LEADER_CHANGE_RETRY,
                                         |client| client.get_region(req.clone())));
        try!(check_resp_header(resp.get_header()));

        Ok(resp.take_region())
    }

    fn get_region_by_id(&self, region_id: u64) -> PdFuture<Option<metapb::Region>> {
        let mut req = pdpb::GetRegionByIDRequest::new();
        req.set_header(self.header());
        req.set_region_id(region_id);

        let executor = |client: &pdpb_grpc::PdClient, req: pdpb::GetRegionByIDRequest| {
            let handler = client.get_region_by_id_async(req);
            handler.map_err(Error::Grpc)
                .and_then(|mut resp| {
                    try!(check_resp_header(resp.get_header()));
                    if resp.has_region() {
                        Ok(Some(resp.take_region()))
                    } else {
                        Ok(None)
                    }
                })
                .boxed()
        };

        self.leader_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn region_heartbeat(&self,
                        region: metapb::Region,
                        leader: metapb::Peer,
                        region_stat: RegionStat)
                        -> PdFuture<pdpb::RegionHeartbeatResponse> {
        let mut req = pdpb::RegionHeartbeatRequest::new();
        req.set_header(self.header());
        req.set_region(region);
        req.set_leader(leader);
        req.set_down_peers(RepeatedField::from_vec(region_stat.down_peers));
        req.set_pending_peers(RepeatedField::from_vec(region_stat.pending_peers));
        req.set_bytes_written(region_stat.written_bytes);
        req.set_keys_written(region_stat.written_keys);

        let executor = |client: &pdpb_grpc::PdClient, req: pdpb::RegionHeartbeatRequest| {
            let handler = client.region_heartbeat_async(req);
            handler.map_err(Error::Grpc)
                .and_then(|resp| {
                    try!(check_resp_header(resp.get_header()));
                    Ok(resp)
                })
                .boxed()
        };

        self.leader_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn ask_split(&self, region: metapb::Region) -> PdFuture<pdpb::AskSplitResponse> {
        let mut req = pdpb::AskSplitRequest::new();
        req.set_header(self.header());
        req.set_region(region);

        let executor = |client: &pdpb_grpc::PdClient, req: pdpb::AskSplitRequest| {
            let handler = client.ask_split_async(req);
            handler.map_err(Error::Grpc)
                .and_then(|resp| {
                    try!(check_resp_header(resp.get_header()));
                    Ok(resp)
                })
                .boxed()
        };

        self.leader_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn store_heartbeat(&self, stats: pdpb::StoreStats) -> PdFuture<()> {
        let mut req = pdpb::StoreHeartbeatRequest::new();
        req.set_header(self.header());
        req.set_stats(stats);

        let executor = |client: &pdpb_grpc::PdClient, req: pdpb::StoreHeartbeatRequest| {
            let handler = client.store_heartbeat_async(req);
            handler.map_err(Error::Grpc)
                .and_then(|resp| {
                    try!(check_resp_header(resp.get_header()));
                    Ok(())
                })
                .boxed()
        };

        self.leader_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }

    fn report_split(&self, left: metapb::Region, right: metapb::Region) -> PdFuture<()> {
        let mut req = pdpb::ReportSplitRequest::new();
        req.set_header(self.header());
        req.set_left(left);
        req.set_right(right);

        let executor = |client: &pdpb_grpc::PdClient, req: pdpb::ReportSplitRequest| {
            let handler = client.report_split_async(req);
            handler.map_err(Error::Grpc)
                .and_then(|resp| {
                    try!(check_resp_header(resp.get_header()));
                    Ok(())
                })
                .boxed()
        };

        self.leader_client
            .request(req, executor, LEADER_CHANGE_RETRY)
            .execute()
    }
}
