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

use std::fmt;

use grpc;

use kvproto::{metapb, pdpb};
use kvproto::pdpb2;
use kvproto::pdpb2_grpc::{self, PD};
use super::PdClient;
use super::{Result, Error};

pub struct Client {
    list: Option<pdpb2::GetPDMembersResponse>,

    // TODO: use async client.
    inner: pdpb2_grpc::PDClient,
}

impl Client {
    // FIXME: pass address.
    pub fn new() -> Result<Client> {
        let mut conf: grpc::client::GrpcClientConf = Default::default();
        conf.http.no_delay = Some(true);
        let inner = pdpb2_grpc::PDClient::new("127.0.0.1", 2379, false, conf).unwrap();
        let list = box_try!(inner.GetPDMembers(pdpb2::GetPDMembersRequest::new()));

        let client = Client {
            list: Some(list),
            inner: inner,
        };

        Ok(client)
    }

    fn header(&self) -> pdpb2::RequestHeader {
        let mut header = pdpb2::RequestHeader::new();
        // TODO: return Error if there is no list.
        let id = self.list.as_ref().unwrap().get_header().get_cluster_id();
        header.set_cluster_id(id);
        header
    }
}

// TODO: check has header?
fn check_resp_header(header: &pdpb2::ResponseHeader) -> Result<()> {
    if !header.has_error() {
        return Ok(());
    }
    // TODO: translate more error types
    let err = header.get_error();
    if err.field_type == pdpb2::ErrorType::BOOTSTRAPPED {
        Err(Error::ClusterBootstrapped(header.get_cluster_id()))
    } else {
        Err(box_err!(err.get_message()))
    }
}

impl fmt::Debug for Client {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "V2Client")
    }
}

impl PdClient for Client {
    // Return the cluster ID.
    fn get_cluster_id(&self) -> Result<u64> {
        let id = self.list.as_ref().unwrap().get_header().get_cluster_id();
        Ok(id)
    }

    fn bootstrap_cluster(&self, stores: metapb::Store, region: metapb::Region) -> Result<()> {
        let mut req = pdpb2::BootstrapRequest::new();
        req.set_header(self.header());
        req.set_store(stores);
        req.set_region(region);

        let resp = box_try!(self.inner.Bootstrap(req));
        try!(check_resp_header(resp.get_header()));
        Ok(())
    }

    fn is_cluster_bootstrapped(&self) -> Result<bool> {
        let mut req = pdpb2::IsBootstrappedRequest::new();
        req.set_header(self.header());

        let resp = box_try!(self.inner.IsBootstrapped(req));
        try!(check_resp_header(resp.get_header()));
        Ok(resp.get_bootstrapped())
    }

    fn alloc_id(&self) -> Result<u64> {
        let mut req = pdpb2::AllocIDRequest::new();
        req.set_header(self.header());

        let resp = box_try!(self.inner.AllocID(req));
        try!(check_resp_header(resp.get_header()));
        Ok(resp.get_id())
    }

    fn put_store(&self, store: metapb::Store) -> Result<()> {
        let mut req = pdpb2::PutStoreRequest::new();
        req.set_header(self.header());
        req.set_store(store);

        let resp = box_try!(self.inner.PutStore(req));
        try!(check_resp_header(resp.get_header()));
        Ok(())
    }

    fn get_store(&self, store_id: u64) -> Result<metapb::Store> {
        let mut req = pdpb2::GetStoreRequest::new();
        req.set_header(self.header());
        req.set_store_id(store_id);

        let mut resp = box_try!(self.inner.GetStore(req));
        try!(check_resp_header(resp.get_header()));
        Ok(resp.take_store())
    }

    fn get_cluster_config(&self) -> Result<metapb::Cluster> {
        let mut req = pdpb2::GetClusterConfigRequest::new();
        req.set_header(self.header());

        let mut resp = box_try!(self.inner.GetClusterConfig(req));
        try!(check_resp_header(resp.get_header()));
        Ok(resp.take_cluster())
    }

    fn get_region(&self, key: &[u8]) -> Result<metapb::Region> {
        let mut req = pdpb2::GetRegionRequest::new();
        req.set_header(self.header());
        req.set_region_key(key.to_vec());

        let mut resp = box_try!(self.inner.GetRegion(req));
        try!(check_resp_header(resp.get_header()));
        Ok(resp.take_region())
    }

    // Get region by region id.
    fn get_region_by_id(&self, region_id: u64) -> Result<Option<metapb::Region>> {
        let mut req = pdpb2::GetRegionByIDRequest::new();
        req.set_header(self.header());
        req.set_region_id(region_id);

        let mut resp = box_try!(self.inner.GetRegionByID(req));
        try!(check_resp_header(resp.get_header()));
        if resp.has_region() {
            Ok(Some(resp.take_region()))
        } else {
            Ok(None)
        }
    }

    // Leader for a region will use this to heartbeat Pd.
    #[allow(unused_variables)]
    fn region_heartbeat(&self,
                        region: metapb::Region,
                        leader: metapb::Peer,
                        down_peers: Vec<pdpb::PeerStats>,
                        pending_peers: Vec<metapb::Peer>)
                        -> Result<pdpb::RegionHeartbeatResponse> {
        unimplemented!()
    }

    fn ask_split(&self, region: metapb::Region) -> Result<pdpb::AskSplitResponse> {
        let mut req = pdpb2::AskSplitRequest::new();
        req.set_header(self.header());
        req.set_region(region);

        let mut resp: pdpb2::AskSplitResponse = box_try!(self.inner.AskSplit(req));
        try!(check_resp_header(resp.get_header()));
        let mut ret = pdpb::AskSplitResponse::new();
        ret.set_new_region_id(resp.get_new_region_id());
        ret.set_new_peer_ids(resp.take_new_peer_ids());
        Ok(ret)
    }

    // Send store statistics regularly.
    #[allow(unused_variables)]
    fn store_heartbeat(&self, stats: pdpb::StoreStats) -> Result<()> {
        unimplemented!()
    }

    // Report pd the split region.
    fn report_split(&self, left: metapb::Region, right: metapb::Region) -> Result<()> {
        let mut req = pdpb2::ReportSplitRequest::new();
        req.set_header(self.header());
        req.set_left(left);
        req.set_right(right);

        let resp = box_try!(self.inner.ReportSplit(req));
        try!(check_resp_header(resp.get_header()));
        Ok(())
    }
}
