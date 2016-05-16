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
use super::{Error, Result, RpcClient};

impl super::PdClient for RpcClient {
    fn bootstrap_cluster(&self, store: metapb::Store, region: metapb::Region) -> Result<()> {
        let mut bootstrap = pdpb::BootstrapRequest::new();
        bootstrap.set_store(store);
        bootstrap.set_region(region);

        let mut req = self.new_request(pdpb::CommandType::Bootstrap);
        req.set_bootstrap(bootstrap);

        let resp = try!(self.send(&req));
        try!(check_resp(&resp));
        Ok(())
    }

    fn is_cluster_bootstrapped(&self) -> Result<bool> {
        let mut req = self.new_request(pdpb::CommandType::IsBootstrapped);
        req.set_is_bootstrapped(pdpb::IsBootstrappedRequest::new());

        let resp = try!(self.send(&req));
        try!(check_resp(&resp));
        Ok(resp.get_is_bootstrapped().get_bootstrapped())
    }

    fn alloc_id(&self) -> Result<u64> {
        let mut req = self.new_request(pdpb::CommandType::AllocId);
        req.set_alloc_id(pdpb::AllocIdRequest::new());

        let resp = try!(self.send(&req));
        try!(check_resp(&resp));
        Ok(resp.get_alloc_id().get_id())
    }

    fn put_store(&self, store: metapb::Store) -> Result<()> {
        let mut put_store = pdpb::PutStoreRequest::new();
        put_store.set_store(store);

        let mut req = self.new_request(pdpb::CommandType::PutStore);
        req.set_put_store(put_store);

        let resp = try!(self.send(&req));
        try!(check_resp(&resp));
        Ok(())
    }

    fn get_store(&self, store_id: u64) -> Result<metapb::Store> {
        let mut get_store = pdpb::GetStoreRequest::new();
        get_store.set_store_id(store_id);

        let mut req = self.new_request(pdpb::CommandType::GetStore);
        req.set_get_store(get_store);

        let mut resp = try!(self.send(&req));
        try!(check_resp(&resp));
        Ok(resp.take_get_store().take_store())
    }

    fn get_cluster_config(&self) -> Result<metapb::Cluster> {
        let mut req = self.new_request(pdpb::CommandType::GetClusterConfig);
        req.set_get_cluster_config(pdpb::GetClusterConfigRequest::new());

        let mut resp = try!(self.send(&req));
        try!(check_resp(&resp));
        Ok(resp.take_get_cluster_config().take_cluster())
    }

    fn get_region(&self, key: &[u8]) -> Result<metapb::Region> {
        let mut get_region = pdpb::GetRegionRequest::new();
        get_region.set_region_key(key.to_vec());

        let mut req = self.new_request(pdpb::CommandType::GetRegion);
        req.set_get_region(get_region);

        let mut resp = try!(self.send(&req));
        try!(check_resp(&resp));
        Ok(resp.take_get_region().take_region())
    }

    fn ask_change_peer(&self, region: metapb::Region, leader: metapb::Peer) -> Result<()> {
        let mut ask_change_peer = pdpb::AskChangePeerRequest::new();
        ask_change_peer.set_region(region);
        ask_change_peer.set_leader(leader);

        let mut req = self.new_request(pdpb::CommandType::AskChangePeer);
        req.set_ask_change_peer(ask_change_peer);

        let resp = try!(self.send(&req));
        try!(check_resp(&resp));
        Ok(())
    }

    fn ask_split(&self,
                 region: metapb::Region,
                 split_key: &[u8],
                 leader: metapb::Peer)
                 -> Result<()> {
        let mut ask_split = pdpb::AskSplitRequest::new();
        ask_split.set_region(region);
        ask_split.set_split_key(split_key.to_vec());
        ask_split.set_leader(leader);

        let mut req = self.new_request(pdpb::CommandType::AskSplit);
        req.set_ask_split(ask_split);

        let resp = try!(self.send(&req));
        try!(check_resp(&resp));
        Ok(())
    }
}

impl RpcClient {
    fn new_request(&self, cmd_type: pdpb::CommandType) -> pdpb::Request {
        let mut header = pdpb::RequestHeader::new();
        header.set_cluster_id(self.cluster_id);
        header.set_uuid(Uuid::new_v4().as_bytes().to_vec());
        let mut req = pdpb::Request::new();
        req.set_header(header);
        req.set_cmd_type(cmd_type);
        req
    }
}


fn check_resp(resp: &pdpb::Response) -> Result<()> {
    if !resp.has_header() {
        return Err(box_err!("pd response missing header"));
    }
    let header = resp.get_header();
    if !header.has_error() {
        return Ok(());
    }
    let error = header.get_error();
    // TODO: translate more error types
    if error.has_bootstrapped() {
        Err(Error::ClusterBootstrapped(header.get_cluster_id()))
    } else {
        Err(box_err!(error.get_message()))
    }
}
