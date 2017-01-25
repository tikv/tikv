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

use uuid::Uuid;
use kvproto::{metapb, pdpb};
// use protobuf::RepeatedField;
use super::{Result, RpcClient};

impl super::PdClient for RpcClient {
    fn get_cluster_id(&self) -> Result<u64> {
        // PD will not check the cluster ID in the GetPDMembersRequest, so we
        // can send this request with any cluster ID, then PD will return its
        // cluster ID in the response header.
        // let get_pd_members = pdpb::GetPDMembersRequest::new();
        // let mut req = new_request(self.cluster_id, pdpb::CommandType::GetPDMembers);
        // req.set_get_pd_members(get_pd_members);

        // let mut resp = try!(self.send(&req));
        // try!(check_resp(&resp));
        // Ok(resp.take_header().get_cluster_id())
        self.v2.get_cluster_id()
    }

    fn bootstrap_cluster(&self, store: metapb::Store, region: metapb::Region) -> Result<()> {
        // let mut bootstrap = pdpb::BootstrapRequest::new();
        // bootstrap.set_store(store);
        // bootstrap.set_region(region);

        // let mut req = new_request(self.cluster_id, pdpb::CommandType::Bootstrap);
        // req.set_bootstrap(bootstrap);

        // let resp = try!(self.send(&req));
        // check_resp(&resp)
        self.v2.bootstrap_cluster(store, region)
    }

    fn is_cluster_bootstrapped(&self) -> Result<bool> {
        // let mut req = new_request(self.cluster_id, pdpb::CommandType::IsBootstrapped);
        // req.set_is_bootstrapped(pdpb::IsBootstrappedRequest::new());

        // let resp = try!(self.send(&req));
        // try!(check_resp(&resp));
        // Ok(resp.get_is_bootstrapped().get_bootstrapped())
        self.v2.is_cluster_bootstrapped()
    }

    fn alloc_id(&self) -> Result<u64> {
        // let mut req = new_request(self.cluster_id, pdpb::CommandType::AllocId);
        // req.set_alloc_id(pdpb::AllocIdRequest::new());

        // let resp = try!(self.send(&req));
        // try!(check_resp(&resp));
        // Ok(resp.get_alloc_id().get_id())
        self.v2.alloc_id()
    }

    fn put_store(&self, store: metapb::Store) -> Result<()> {
        // let mut put_store = pdpb::PutStoreRequest::new();
        // put_store.set_store(store);

        // let mut req = new_request(self.cluster_id, pdpb::CommandType::PutStore);
        // req.set_put_store(put_store);

        // let resp = try!(self.send(&req));
        // check_resp(&resp)
        self.v2.put_store(store)
    }

    fn get_store(&self, store_id: u64) -> Result<metapb::Store> {
        // let mut get_store = pdpb::GetStoreRequest::new();
        // get_store.set_store_id(store_id);

        // let mut req = new_request(self.cluster_id, pdpb::CommandType::GetStore);
        // req.set_get_store(get_store);

        // let mut resp = try!(self.send(&req));
        // try!(check_resp(&resp));
        // Ok(resp.take_get_store().take_store())
        self.v2.get_store(store_id)
    }

    fn get_cluster_config(&self) -> Result<metapb::Cluster> {
        // let mut req = new_request(self.cluster_id, pdpb::CommandType::GetClusterConfig);
        // req.set_get_cluster_config(pdpb::GetClusterConfigRequest::new());

        // let mut resp = try!(self.send(&req));
        // try!(check_resp(&resp));
        // Ok(resp.take_get_cluster_config().take_cluster())
        self.v2.get_cluster_config()
    }

    fn get_region(&self, key: &[u8]) -> Result<metapb::Region> {
        // let mut get_region = pdpb::GetRegionRequest::new();
        // get_region.set_region_key(key.to_vec());

        // let mut req = new_request(self.cluster_id, pdpb::CommandType::GetRegion);
        // req.set_get_region(get_region);

        // let mut resp = try!(self.send(&req));
        // try!(check_resp(&resp));
        // Ok(resp.take_get_region().take_region())
        self.v2.get_region(key)
    }

    fn get_region_by_id(&self, region_id: u64) -> Result<Option<metapb::Region>> {
        // let mut get_region_by_id = pdpb::GetRegionByIDRequest::new();
        // get_region_by_id.set_region_id(region_id);

        // let mut req = new_request(self.cluster_id, pdpb::CommandType::GetRegionByID);
        // req.set_get_region_by_id(get_region_by_id);

        // let mut resp = try!(self.send(&req));
        // try!(check_resp(&resp));
        // if resp.get_get_region_by_id().has_region() {
        //     Ok(Some(resp.take_get_region_by_id().take_region()))
        // } else {
        //     Ok(None)
        // }
        self.v2.get_region_by_id(region_id)
    }

    fn region_heartbeat(&self,
                        region: metapb::Region,
                        leader: metapb::Peer,
                        down_peers: Vec<pdpb::PeerStats>,
                        pending_peers: Vec<metapb::Peer>)
                        -> Result<pdpb::RegionHeartbeatResponse> {
        // let mut heartbeat = pdpb::RegionHeartbeatRequest::new();
        // heartbeat.set_region(region);
        // heartbeat.set_leader(leader);
        // heartbeat.set_down_peers(RepeatedField::from_vec(down_peers));
        // heartbeat.set_pending_peers(RepeatedField::from_vec(pending_peers));

        // let mut req = new_request(self.cluster_id, pdpb::CommandType::RegionHeartbeat);
        // req.set_region_heartbeat(heartbeat);

        // let mut resp = try!(self.send(&req));
        // try!(check_resp(&resp));
        // Ok(resp.take_region_heartbeat())
        self.v2.region_heartbeat(region, leader, down_peers, pending_peers)
    }

    fn ask_split(&self, region: metapb::Region) -> Result<pdpb::AskSplitResponse> {
        // let mut ask_split = pdpb::AskSplitRequest::new();
        // ask_split.set_region(region);

        // let mut req = new_request(self.cluster_id, pdpb::CommandType::AskSplit);
        // req.set_ask_split(ask_split);

        // let mut resp = try!(self.send(&req));
        // try!(check_resp(&resp));
        // Ok(resp.take_ask_split())
        self.v2.ask_split(region)
    }

    fn store_heartbeat(&self, stats: pdpb::StoreStats) -> Result<()> {
        // let mut heartbeat = pdpb::StoreHeartbeatRequest::new();
        // heartbeat.set_stats(stats);

        // let mut req = new_request(self.cluster_id, pdpb::CommandType::StoreHeartbeat);
        // req.set_store_heartbeat(heartbeat);

        // let resp = try!(self.send(&req));
        // check_resp(&resp)
        self.v2.store_heartbeat(stats)
    }

    fn report_split(&self, left: metapb::Region, right: metapb::Region) -> Result<()> {
        // let mut report_split = pdpb::ReportSplitRequest::new();
        // report_split.set_left(left);
        // report_split.set_right(right);

        // let mut req = new_request(self.cluster_id, pdpb::CommandType::ReportSplit);
        // req.set_report_split(report_split);

        // let resp = try!(self.send(&req));
        // check_resp(&resp)
        self.v2.report_split(left, right)
    }
}

pub fn new_request(cluster_id: u64, cmd_type: pdpb::CommandType) -> pdpb::Request {
    let mut header = pdpb::RequestHeader::new();
    header.set_cluster_id(cluster_id);
    header.set_uuid(Uuid::new_v4().as_bytes().to_vec());
    let mut req = pdpb::Request::new();
    req.set_header(header);
    req.set_cmd_type(cmd_type);
    req
}

// fn check_resp(resp: &pdpb::Response) -> Result<()> {
//     if !resp.has_header() {
//         return Err(box_err!("pd response missing header"));
//     }
//     let header = resp.get_header();
//     if !header.has_error() {
//         return Ok(());
//     }
//     let error = header.get_error();
//     // TODO: translate more error types
//     if error.has_bootstrapped() {
//         Err(Error::ClusterBootstrapped(header.get_cluster_id()))
//     } else {
//         Err(box_err!(error.get_message()))
//     }
// }
