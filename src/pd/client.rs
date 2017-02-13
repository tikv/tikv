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
use std::result;
use std::thread;
use std::sync::RwLock;
use std::sync::RwLockReadGuard;
use std::time::Duration;
use std::collections::HashSet;

use grpc;

use protobuf::RepeatedField;

use rand::{self, Rng};

use kvproto::{metapb, pdpb};
use kvproto::pdpb2;
use kvproto::pdpb2_grpc::{self, PD};

use super::{Result, PdClient};
use super::metrics::*;

pub struct RpcClient {
    members: pdpb2::GetPDMembersResponse,
    inner: RwLock<pdpb2_grpc::PDClient>,
}

impl RpcClient {
    pub fn new(endpoints: &str) -> Result<RpcClient> {
        let endpoints: Vec<_> = endpoints.split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();

        let (client, members) = try!(validate_endpoints(&endpoints));
        Ok(RpcClient {
            members: members,
            inner: RwLock::new(client),
        })
    }

    fn header(&self) -> pdpb2::RequestHeader {
        let mut header = pdpb2::RequestHeader::new();
        header.set_cluster_id(self.members.get_header().get_cluster_id());
        header
    }
}


pub fn validate_endpoints(endpoints: &[&str])
                          -> Result<(pdpb2_grpc::PDClient, pdpb2::GetPDMembersResponse)> {
    if endpoints.is_empty() {
        return Err(box_err!("empty PD endpoints"));
    }

    let len = endpoints.len();
    let mut endpoints_set = HashSet::with_capacity(len);

    let mut pd_client = None;
    let mut members = None;
    let mut cluster_id = None;
    for ep in endpoints {
        if !endpoints_set.insert(ep) {
            return Err(box_err!("duplicate PD endpoint {}", ep));
        }

        let client = match connect(ep) {
            Ok(c) => c,
            // Ignore failed PD node.
            Err(e) => {
                error!("PD endpoint {} is down: {:?}", ep, e);
                continue;
            }
        };

        let resp = match client.GetPDMembers(pdpb2::GetPDMembersRequest::new()) {
            Ok(resp) => resp,
            // Ignore failed PD node.
            Err(e) => {
                error!("PD endpoint {} failed to respond: {:?}", ep, e);
                continue;
            }
        };

        // Check cluster ID.
        let cid = resp.get_header().get_cluster_id();
        if let Some(sample) = cluster_id {
            if sample != cid {
                return Err(box_err!("PD response cluster_id mismatch, want {}, got {}",
                                    sample,
                                    cid));
            }
        } else {
            cluster_id = Some(cid);
        }
        // TODO: check all fields later?

        if pd_client.is_none() {
            pd_client = Some(client);
        }
        if members.is_none() {
            members = Some(resp);
        }
    }

    match (pd_client, members) {
        (Some(pd_client), Some(members)) => Ok((pd_client, members)),
        _ => Err(box_err!("PD cluster failed to respond")),
    }
}

fn connect(addr: &str) -> Result<pdpb2_grpc::PDClient> {
    let (host, port) = {
        let mut parts = addr.split(':');
        (parts.next().unwrap().to_owned(), parts.next().unwrap().parse::<u16>().unwrap())
    };

    let mut conf: grpc::client::GrpcClientConf = Default::default();
    conf.http.no_delay = Some(true);
    pdpb2_grpc::PDClient::new(&host, port, false, conf).map_err(|e| box_err!(e))
}


fn try_connect(members: &pdpb2::GetPDMembersResponse) -> Result<pdpb2_grpc::PDClient> {
    // Randomize endpoints.
    let members = members.get_members();
    let mut indexes: Vec<usize> = (0..members.len()).collect();
    rand::thread_rng().shuffle(&mut indexes);

    for i in indexes {
        for ep in members[i].get_peer_urls() {
            match connect(ep.as_str()) {
                Ok(cli) => {
                    info!("PD client connects to {}", ep);
                    return Ok(cli);
                }
                Err(_) => {
                    error!("failed to connect to {}, try next", ep);
                    continue;
                }
            }
        }
    }

    Err(box_err!("failed to connect to {:?}", members))
}

const MAX_RETRY_COUNT: usize = 100;

#[inline]
fn do_requst<F, R>(client: &RpcClient, f: F) -> Result<R>
    where F: Fn(RwLockReadGuard<pdpb2_grpc::PDClient>) -> result::Result<R, grpc::error::GrpcError>
{
    let mut resp = None;
    for _ in 0..MAX_RETRY_COUNT {
        let cli = client.inner.read().unwrap();

        let r = {
            let timer = PD_SEND_MSG_HISTOGRAM.start_timer();
            let r = f(cli);
            timer.observe_duration();
            r
        };

        match r {
            Ok(r) => resp = Some(r),
            Err(e) => {
                error!("fail to request: {:?}", e);
                let mut cli = client.inner.write().unwrap();
                match try_connect(&client.members) {
                    Ok(c) => {
                        *cli = c;
                    }
                    Err(e) => {
                        error!("{:?}", e);
                        thread::sleep(Duration::from_secs(1));
                    }
                }
                continue;
            }
        }
    }

    resp.ok_or(box_err!("fail to request"))
}

fn check_resp_header(header: &pdpb2::ResponseHeader) -> Result<()> {
    if !header.has_error() {
        return Ok(());
    }
    // TODO: translate more error types
    let err = header.get_error();
    Err(box_err!(err.get_message()))
}

impl fmt::Debug for RpcClient {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "PD gRPC Client connects to cluster {:?}", self.members)
    }
}

impl PdClient for RpcClient {
    fn get_cluster_id(&self) -> Result<u64> {
        let id = self.members.get_header().get_cluster_id();
        Ok(id)
    }

    fn bootstrap_cluster(&self, stores: metapb::Store, region: metapb::Region) -> Result<()> {
        let mut req = pdpb2::BootstrapRequest::new();
        req.set_header(self.header());
        req.set_store(stores);
        req.set_region(region);

        let resp = try!(do_requst(self, |client| client.Bootstrap(req.clone())));
        try!(check_resp_header(resp.get_header()));
        Ok(())
    }

    fn is_cluster_bootstrapped(&self) -> Result<bool> {
        let mut req = pdpb2::IsBootstrappedRequest::new();
        req.set_header(self.header());

        let resp = try!(do_requst(self, |client| client.IsBootstrapped(req.clone())));
        try!(check_resp_header(resp.get_header()));

        Ok(resp.get_bootstrapped())
    }

    fn alloc_id(&self) -> Result<u64> {
        let mut req = pdpb2::AllocIDRequest::new();
        req.set_header(self.header());

        let resp = try!(do_requst(self, |client| client.AllocID(req.clone())));
        try!(check_resp_header(resp.get_header()));

        Ok(resp.get_id())
    }

    fn put_store(&self, store: metapb::Store) -> Result<()> {
        let mut req = pdpb2::PutStoreRequest::new();
        req.set_header(self.header());
        req.set_store(store);

        let resp = try!(do_requst(self, |client| client.PutStore(req.clone())));
        try!(check_resp_header(resp.get_header()));

        Ok(())
    }

    fn get_store(&self, store_id: u64) -> Result<metapb::Store> {
        let mut req = pdpb2::GetStoreRequest::new();
        req.set_header(self.header());
        req.set_store_id(store_id);

        let mut resp = try!(do_requst(self, |client| client.GetStore(req.clone())));
        try!(check_resp_header(resp.get_header()));

        Ok(resp.take_store())
    }

    fn get_cluster_config(&self) -> Result<metapb::Cluster> {
        let mut req = pdpb2::GetClusterConfigRequest::new();
        req.set_header(self.header());

        let mut resp = try!(do_requst(self, |client| client.GetClusterConfig(req.clone())));
        try!(check_resp_header(resp.get_header()));

        Ok(resp.take_cluster())
    }

    fn get_region(&self, key: &[u8]) -> Result<metapb::Region> {
        let mut req = pdpb2::GetRegionRequest::new();
        req.set_header(self.header());
        req.set_region_key(key.to_vec());

        let mut resp = try!(do_requst(self, |client| client.GetRegion(req.clone())));
        try!(check_resp_header(resp.get_header()));

        Ok(resp.take_region())
    }

    fn get_region_by_id(&self, region_id: u64) -> Result<Option<metapb::Region>> {
        let mut req = pdpb2::GetRegionByIDRequest::new();
        req.set_header(self.header());
        req.set_region_id(region_id);

        let mut resp = try!(do_requst(self, |client| client.GetRegionByID(req.clone())));
        try!(check_resp_header(resp.get_header()));

        if resp.has_region() {
            Ok(Some(resp.take_region()))
        } else {
            Ok(None)
        }
    }

    fn region_heartbeat(&self,
                        region: metapb::Region,
                        leader: metapb::Peer,
                        down_peers: Vec<pdpb::PeerStats>,
                        pending_peers: Vec<metapb::Peer>)
                        -> Result<pdpb::RegionHeartbeatResponse> {
        let mut req = pdpb2::RegionHeartbeatRequest::new();
        req.set_header(self.header());
        req.set_region(region);
        req.set_leader(leader);
        req.set_down_peers(down_peers.into_iter()
            .map(|mut p| {
                let mut p2 = pdpb2::PeerStats::new();
                if p.has_peer() {
                    p2.set_peer(p.take_peer());
                }
                p2.set_down_seconds(p.get_down_seconds());
                p2
            })
            .collect());
        req.set_pending_peers(RepeatedField::from_vec(pending_peers));

        let mut resp = try!(do_requst(self, |client| client.RegionHeartbeat(req.clone())));
        try!(check_resp_header(resp.get_header()));

        let mut ret = pdpb::RegionHeartbeatResponse::new();
        if resp.has_change_peer() {
            let mut cp = pdpb::ChangePeer::new();
            let mut cp2 = resp.take_change_peer();
            if cp2.has_change_type() {
                let ty = cp2.take_change_type();
                cp.set_change_type(ty.get_field_type());
            }
            if cp2.has_peer() {
                cp.set_peer(cp2.take_peer());
            }
            ret.set_change_peer(cp);
        }
        if resp.has_transfer_leader() {
            let mut tf = pdpb::TransferLeader::new();
            let mut tf2 = resp.take_transfer_leader();
            if tf2.has_peer() {
                tf.set_peer(tf2.take_peer());
            }
            ret.set_transfer_leader(tf);
        }
        Ok(ret)
    }

    fn ask_split(&self, region: metapb::Region) -> Result<pdpb::AskSplitResponse> {
        let mut req = pdpb2::AskSplitRequest::new();
        req.set_header(self.header());
        req.set_region(region);

        let mut resp = try!(do_requst(self, |client| client.AskSplit(req.clone())));
        try!(check_resp_header(resp.get_header()));

        let mut ret = pdpb::AskSplitResponse::new();
        ret.set_new_region_id(resp.get_new_region_id());
        ret.set_new_peer_ids(resp.take_new_peer_ids());
        Ok(ret)
    }

    fn store_heartbeat(&self, stats: pdpb::StoreStats) -> Result<()> {
        let mut req = pdpb2::StoreHeartbeatRequest::new();
        req.set_header(self.header());
        let mut stats2 = pdpb2::StoreStats::new();
        if stats.has_store_id() {
            stats2.set_store_id(stats.get_store_id())
        }
        if stats.has_capacity() {
            stats2.set_capacity(stats.get_capacity())
        }
        if stats.has_available() {
            stats2.set_available(stats.get_available())
        }
        if stats.has_region_count() {
            stats2.set_region_count(stats.get_region_count())
        }
        if stats.has_sending_snap_count() {
            stats2.set_sending_snap_count(stats.get_sending_snap_count())
        }
        if stats.has_receiving_snap_count() {
            stats2.set_receiving_snap_count(stats.get_receiving_snap_count())
        }
        if stats.has_start_time() {
            stats2.set_start_time(stats.get_start_time())
        }
        if stats.has_applying_snap_count() {
            stats2.set_applying_snap_count(stats.get_applying_snap_count())
        }
        if stats.has_is_busy() {
            stats2.set_is_busy(stats.get_is_busy())
        }
        req.set_stats(stats2);

        let resp = try!(do_requst(self, |client| client.StoreHeartbeat(req.clone())));

        try!(check_resp_header(resp.get_header()));
        Ok(())
    }

    fn report_split(&self, left: metapb::Region, right: metapb::Region) -> Result<()> {
        let mut req = pdpb2::ReportSplitRequest::new();
        req.set_header(self.header());
        req.set_left(left);
        req.set_right(right);

        let resp = try!(do_requst(self, |client| client.ReportSplit(req.clone())));
        try!(check_resp_header(resp.get_header()));

        Ok(())
    }
}
