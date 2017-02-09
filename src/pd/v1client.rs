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

use std::io::Write;
use std::net::TcpStream;
use std::time::Duration;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::collections::HashSet;

use uuid::Uuid;
use kvproto::{metapb, pdpb};
use protobuf::RepeatedField;
use util::codec::rpc;
use util::make_std_tcp_conn;

use rand::{self, Rng};

use kvproto::pdpb::{Request, Response};
use kvproto::msgpb::{Message, MessageType};

use super::{Result, Error, PdClient};
use super::metrics::*;

const MAX_PD_SEND_RETRY_COUNT: usize = 100;
const SOCKET_READ_TIMEOUT: u64 = 3;
const SOCKET_WRITE_TIMEOUT: u64 = 3;

const PD_RPC_PREFIX: &'static str = "/pd/rpc";

// Only for `validate_endpoints`.
const VALIDATE_MSG_ID: u64 = 0;
const VALIDATE_CLUSTER_ID: u64 = 0;

/// `validate_endpoints` validates pd members, make sure they are in the same cluster.
/// It returns a cluster ID.
/// Note that it ignores failed pd nodes.
/// Export for tests.
pub fn validate_endpoints(endpoints: &[String]) -> Result<u64> {
    if endpoints.is_empty() {
        return Err(box_err!("empty PD endpoints"));
    }

    let len = endpoints.len();
    let mut endpoints_set = HashSet::with_capacity(len);

    let mut cluster_id = None;
    for ep in endpoints {
        if !endpoints_set.insert(ep) {
            return Err(box_err!("a duplicate PD url {}", ep));
        }

        let mut stream = match rpc_connect(ep.as_str()) {
            Ok(stream) => stream,
            // Ignore failed pd node.
            Err(_) => continue,
        };

        let mut req = new_request(VALIDATE_CLUSTER_ID, pdpb::CommandType::GetPDMembers);
        req.set_get_pd_members(pdpb::GetPDMembersRequest::new());
        let (mid, resp) = match send_msg(&mut stream, VALIDATE_MSG_ID, &req) {
            Ok((mid, resp)) => (mid, resp),
            // Ignore failed pd node.
            Err(_) => continue,
        };

        if mid != VALIDATE_MSG_ID {
            return Err(box_err!("PD response msg_id mismatch, want {}, got {}",
                                VALIDATE_MSG_ID,
                                mid));
        }

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
    }

    cluster_id.ok_or(box_err!("PD cluster stop responding"))
}

fn send_msg(stream: &mut TcpStream, msg_id: u64, message: &Request) -> Result<(u64, Response)> {
    let timer = PD_SEND_MSG_HISTOGRAM.start_timer();

    let mut req = Message::new();

    req.set_msg_type(MessageType::PdReq);
    // TODO: optimize clone later in HTTP refactor.
    req.set_pd_req(message.clone());

    try!(stream.set_write_timeout(Some(Duration::from_secs(SOCKET_WRITE_TIMEOUT))));
    try!(rpc::encode_msg(stream, msg_id, &req));

    try!(stream.set_read_timeout(Some(Duration::from_secs(SOCKET_READ_TIMEOUT))));
    let mut resp = Message::new();
    let id = try!(rpc::decode_msg(stream, &mut resp));
    if resp.get_msg_type() != MessageType::PdResp {
        return Err(box_err!("invalid pd response type {:?}", resp.get_msg_type()));
    }
    timer.observe_duration();

    Ok((id, resp.take_pd_resp()))
}

fn rpc_connect(endpoint: &str) -> Result<TcpStream> {
    let mut stream = try!(make_std_tcp_conn(endpoint));
    try!(stream.set_write_timeout(Some(Duration::from_secs(SOCKET_WRITE_TIMEOUT))));

    // Send a HTTP header to tell PD to hijack this connection for RPC.
    let header_str = format!("GET {} HTTP/1.0\r\n\r\n", PD_RPC_PREFIX);
    let header = header_str.as_bytes();
    match stream.write_all(header) {
        Ok(_) => Ok(stream),
        Err(err) => Err(box_err!("failed to connect to {} error: {:?}", endpoint, err)),
    }
}

#[derive(Debug)]
struct Core {
    endpoints: Vec<String>,
    stream: Option<TcpStream>,
}

impl Core {
    #[inline]
    fn try_connect(&mut self) -> Result<()> {
        // Randomize endpoints.
        let len = self.endpoints.len();
        let mut indexes: Vec<usize> = (0..len).collect();
        rand::thread_rng().shuffle(&mut indexes);

        for i in indexes {
            let ep = &self.endpoints[i];
            match rpc_connect(ep.as_str()) {
                Ok(stream) => {
                    info!("PD client connects to {}", ep);
                    self.stream = Some(stream);
                    return Ok(());
                }

                Err(_) => {
                    error!("failed to connect to {}, try next", ep);
                    continue;
                }
            }
        }

        Err(box_err!("failed to connect to {:?}", self.endpoints))
    }

    #[inline]
    fn send(&mut self, msg_id: u64, req: &Request) -> Result<Response> {
        // If we post failed, we should retry.
        for _ in 0..MAX_PD_SEND_RETRY_COUNT {
            // If no stream, try connect first.
            if self.stream.is_none() && self.try_connect().is_err() {
                // TODO: figure out a better way to do backoff
                thread::sleep(Duration::from_millis(50));
                continue;
            }

            let mut stream = self.stream.take().unwrap();
            // We may send message to a not leader pd, retry.

            let (id, resp) = match send_msg(&mut stream, msg_id, req) {
                Err(e) => {
                    warn!("send message to pd failed {:?}", e);
                    // TODO: figure out a better way to do backoff
                    thread::sleep(Duration::from_millis(50));
                    continue;
                }
                Ok((id, resp)) => (id, resp),
            };

            if id != msg_id {
                return Err(box_err!("pd response msg_id not match, want {}, got {}", msg_id, id));
            }

            self.stream = Some(stream);

            return Ok(resp);
        }

        Err(box_err!("send message to pd failed"))
    }
}

#[derive(Debug)]
pub struct Client {
    msg_id: AtomicUsize,
    cluster_id: u64,
    core: Mutex<Core>,
}

impl Client {
    pub fn new(endpoints: Vec<String>) -> Result<Client> {
        let mut cluster_id = VALIDATE_CLUSTER_ID;
        for _ in 0..MAX_PD_SEND_RETRY_COUNT {
            match validate_endpoints(&endpoints) {
                Ok(id) => {
                    cluster_id = id;
                    break;
                }
                Err(e) => {
                    warn!("failed to get cluster id from pd: {:?}", e);
                    thread::sleep(Duration::from_secs(1));
                }
            }
        }

        if cluster_id == VALIDATE_CLUSTER_ID {
            return Err(box_err!("failed to get cluster id from pd"));
        }

        let core = Core {
            endpoints: endpoints,
            stream: None,
        };

        Ok(Client {
            msg_id: AtomicUsize::new(0),
            cluster_id: cluster_id,
            core: Mutex::new(core),
        })
    }

    pub fn send(&self, req: &Request) -> Result<Response> {
        let msg_id = self.alloc_msg_id();
        let resp = try!(self.core.lock().unwrap().send(msg_id, req));
        Ok(resp)
    }

    fn alloc_msg_id(&self) -> u64 {
        self.msg_id.fetch_add(1, Ordering::Relaxed) as u64
    }
}

impl PdClient for Client {
    fn get_cluster_id(&self) -> Result<u64> {
        // PD will not check the cluster ID in the GetPDMembersRequest, so we
        // can send this request with any cluster ID, then PD will return its
        // cluster ID in the response header.
        let get_pd_members = pdpb::GetPDMembersRequest::new();
        let mut req = new_request(self.cluster_id, pdpb::CommandType::GetPDMembers);
        req.set_get_pd_members(get_pd_members);

        let mut resp = try!(self.send(&req));
        try!(check_resp(&resp));
        Ok(resp.take_header().get_cluster_id())
    }

    fn bootstrap_cluster(&self, store: metapb::Store, region: metapb::Region) -> Result<()> {
        let mut bootstrap = pdpb::BootstrapRequest::new();
        bootstrap.set_store(store);
        bootstrap.set_region(region);

        let mut req = new_request(self.cluster_id, pdpb::CommandType::Bootstrap);
        req.set_bootstrap(bootstrap);

        let resp = try!(self.send(&req));
        check_resp(&resp)
    }

    fn is_cluster_bootstrapped(&self) -> Result<bool> {
        let mut req = new_request(self.cluster_id, pdpb::CommandType::IsBootstrapped);
        req.set_is_bootstrapped(pdpb::IsBootstrappedRequest::new());

        let resp = try!(self.send(&req));
        try!(check_resp(&resp));
        Ok(resp.get_is_bootstrapped().get_bootstrapped())
    }

    fn alloc_id(&self) -> Result<u64> {
        let mut req = new_request(self.cluster_id, pdpb::CommandType::AllocId);
        req.set_alloc_id(pdpb::AllocIdRequest::new());

        let resp = try!(self.send(&req));
        try!(check_resp(&resp));
        Ok(resp.get_alloc_id().get_id())
    }

    fn put_store(&self, store: metapb::Store) -> Result<()> {
        let mut put_store = pdpb::PutStoreRequest::new();
        put_store.set_store(store);

        let mut req = new_request(self.cluster_id, pdpb::CommandType::PutStore);
        req.set_put_store(put_store);

        let resp = try!(self.send(&req));
        check_resp(&resp)
    }

    fn get_store(&self, store_id: u64) -> Result<metapb::Store> {
        let mut get_store = pdpb::GetStoreRequest::new();
        get_store.set_store_id(store_id);

        let mut req = new_request(self.cluster_id, pdpb::CommandType::GetStore);
        req.set_get_store(get_store);

        let mut resp = try!(self.send(&req));
        try!(check_resp(&resp));
        Ok(resp.take_get_store().take_store())
    }

    fn get_cluster_config(&self) -> Result<metapb::Cluster> {
        let mut req = new_request(self.cluster_id, pdpb::CommandType::GetClusterConfig);
        req.set_get_cluster_config(pdpb::GetClusterConfigRequest::new());

        let mut resp = try!(self.send(&req));
        try!(check_resp(&resp));
        Ok(resp.take_get_cluster_config().take_cluster())
    }

    fn get_region(&self, key: &[u8]) -> Result<metapb::Region> {
        let mut get_region = pdpb::GetRegionRequest::new();
        get_region.set_region_key(key.to_vec());

        let mut req = new_request(self.cluster_id, pdpb::CommandType::GetRegion);
        req.set_get_region(get_region);

        let mut resp = try!(self.send(&req));
        try!(check_resp(&resp));
        Ok(resp.take_get_region().take_region())
    }

    fn get_region_by_id(&self, region_id: u64) -> Result<Option<metapb::Region>> {
        let mut get_region_by_id = pdpb::GetRegionByIDRequest::new();
        get_region_by_id.set_region_id(region_id);

        let mut req = new_request(self.cluster_id, pdpb::CommandType::GetRegionByID);
        req.set_get_region_by_id(get_region_by_id);

        let mut resp = try!(self.send(&req));
        try!(check_resp(&resp));
        if resp.get_get_region_by_id().has_region() {
            Ok(Some(resp.take_get_region_by_id().take_region()))
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
        let mut heartbeat = pdpb::RegionHeartbeatRequest::new();
        heartbeat.set_region(region);
        heartbeat.set_leader(leader);
        heartbeat.set_down_peers(RepeatedField::from_vec(down_peers));
        heartbeat.set_pending_peers(RepeatedField::from_vec(pending_peers));

        let mut req = new_request(self.cluster_id, pdpb::CommandType::RegionHeartbeat);
        req.set_region_heartbeat(heartbeat);

        let mut resp = try!(self.send(&req));
        try!(check_resp(&resp));
        Ok(resp.take_region_heartbeat())
    }

    fn ask_split(&self, region: metapb::Region) -> Result<pdpb::AskSplitResponse> {
        let mut ask_split = pdpb::AskSplitRequest::new();
        ask_split.set_region(region);

        let mut req = new_request(self.cluster_id, pdpb::CommandType::AskSplit);
        req.set_ask_split(ask_split);

        let mut resp = try!(self.send(&req));
        try!(check_resp(&resp));
        Ok(resp.take_ask_split())
    }

    fn store_heartbeat(&self, stats: pdpb::StoreStats) -> Result<()> {
        let mut heartbeat = pdpb::StoreHeartbeatRequest::new();
        heartbeat.set_stats(stats);

        let mut req = new_request(self.cluster_id, pdpb::CommandType::StoreHeartbeat);
        req.set_store_heartbeat(heartbeat);

        let resp = try!(self.send(&req));
        check_resp(&resp)
    }

    fn report_split(&self, left: metapb::Region, right: metapb::Region) -> Result<()> {
        let mut report_split = pdpb::ReportSplitRequest::new();
        report_split.set_left(left);
        report_split.set_right(right);

        let mut req = new_request(self.cluster_id, pdpb::CommandType::ReportSplit);
        req.set_report_split(report_split);

        let resp = try!(self.send(&req));
        check_resp(&resp)
    }
}

fn new_request(cluster_id: u64, cmd_type: pdpb::CommandType) -> pdpb::Request {
    let mut header = pdpb::RequestHeader::new();
    header.set_cluster_id(cluster_id);
    header.set_uuid(Uuid::new_v4().as_bytes().to_vec());
    let mut req = pdpb::Request::new();
    req.set_header(header);
    req.set_cmd_type(cmd_type);
    req
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
