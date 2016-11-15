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


use std::sync::Arc;
use std::time::Duration;
use std::thread;

use rocksdb::DB;
use uuid::Uuid;
use protobuf;

use kvproto::metapb::{self, RegionEpoch};
use kvproto::raft_cmdpb::{Request, StatusRequest, AdminRequest, RaftCmdRequest, RaftCmdResponse};
use kvproto::raft_cmdpb::{CmdType, StatusCmdType, AdminCmdType};
use kvproto::pdpb::{ChangePeer, RegionHeartbeatResponse, TransferLeader};
use kvproto::eraftpb::ConfChangeType;

use tikv::raftstore::store::*;
use tikv::server::Config as ServerConfig;
use tikv::storage::Config as StorageConfig;
use tikv::util::escape;

pub use tikv::raftstore::store::util::find_peer;

pub fn must_get(engine: &Arc<DB>, cf: &str, key: &[u8], value: Option<&[u8]>) {
    for _ in 1..300 {
        if let Ok(res) = engine.get_value_cf(cf, &keys::data_key(key)) {
            if value.is_some() && res.is_some() {
                assert_eq!(value.unwrap(), &*res.unwrap());
                return;
            }
            if value.is_none() && res.is_none() {
                return;
            }
            thread::sleep(Duration::from_millis(20));
        }
    }
    debug!("last try to get {}", escape(key));
    let res = engine.get_value_cf(cf, &keys::data_key(key)).unwrap();
    if value.is_none() && res.is_none() ||
       value.is_some() && res.is_some() && value.unwrap() == &*res.unwrap() {
        return;
    }
    panic!("can't get value {:?} for key {:?}",
           value.map(escape),
           escape(key))
}

pub fn must_get_equal(engine: &Arc<DB>, key: &[u8], value: &[u8]) {
    must_get(engine, "default", key, Some(value));
}

pub fn must_get_none(engine: &Arc<DB>, key: &[u8]) {
    must_get(engine, "default", key, None);
}

pub fn must_get_cf_equal(engine: &Arc<DB>, cf: &str, key: &[u8], value: &[u8]) {
    must_get(engine, cf, key, Some(value));
}

pub fn must_get_cf_none(engine: &Arc<DB>, cf: &str, key: &[u8]) {
    must_get(engine, cf, key, None);
}

pub fn new_store_cfg() -> Config {
    Config {
        raft_base_tick_interval: 10,
        raft_heartbeat_ticks: 2,
        raft_election_timeout_ticks: 25,
        raft_log_gc_tick_interval: 100,
        raft_log_gc_threshold: 1,
        pd_heartbeat_tick_interval: 20,
        region_check_size_diff: 10000,
        // Use a value of 3 seconds as max_leader_missing_duration just for test.
        // In production environment, the value of max_leader_missing_duration
        // should be configured far beyond the election timeout.
        max_leader_missing_duration: Duration::from_secs(3),
        region_merge_retry_duration: Duration::from_millis(50),
        ..Config::default()
    }
}

pub fn new_server_config(cluster_id: u64) -> ServerConfig {
    let store_cfg = new_store_cfg();

    ServerConfig {
        cluster_id: cluster_id,
        addr: "127.0.0.1:0".to_owned(),
        raft_store: store_cfg,
        storage: StorageConfig::default(),
        send_buffer_size: 64 * 1024,
        recv_buffer_size: 64 * 1024,
        ..ServerConfig::default()
    }
}

// Create a base request.
pub fn new_base_request(region_id: u64, epoch: RegionEpoch, read_quorum: bool) -> RaftCmdRequest {
    let mut req = RaftCmdRequest::new();
    req.mut_header().set_region_id(region_id);
    req.mut_header().set_region_epoch(epoch);
    req.mut_header().set_uuid(Uuid::new_v4().as_bytes().to_vec());
    req.mut_header().set_read_quorum(read_quorum);
    req
}

pub fn new_request(region_id: u64,
                   epoch: RegionEpoch,
                   requests: Vec<Request>,
                   read_quorum: bool)
                   -> RaftCmdRequest {
    let mut req = new_base_request(region_id, epoch, read_quorum);
    req.set_requests(protobuf::RepeatedField::from_vec(requests));
    req
}

pub fn new_put_cmd(key: &[u8], value: &[u8]) -> Request {
    let mut cmd = Request::new();
    cmd.set_cmd_type(CmdType::Put);
    cmd.mut_put().set_key(key.to_vec());
    cmd.mut_put().set_value(value.to_vec());
    cmd
}

pub fn new_put_cf_cmd(cf: &str, key: &[u8], value: &[u8]) -> Request {
    let mut cmd = Request::new();
    cmd.set_cmd_type(CmdType::Put);
    cmd.mut_put().set_key(key.to_vec());
    cmd.mut_put().set_value(value.to_vec());
    cmd.mut_put().set_cf(cf.to_string());
    cmd
}

pub fn new_get_cmd(key: &[u8]) -> Request {
    let mut cmd = Request::new();
    cmd.set_cmd_type(CmdType::Get);
    cmd.mut_get().set_key(key.to_vec());
    cmd
}

pub fn new_delete_cmd(cf: &str, key: &[u8]) -> Request {
    let mut cmd = Request::new();
    cmd.set_cmd_type(CmdType::Delete);
    cmd.mut_delete().set_key(key.to_vec());
    cmd.mut_delete().set_cf(cf.to_string());
    cmd
}

pub fn new_status_request(region_id: u64,
                          peer: metapb::Peer,
                          request: StatusRequest)
                          -> RaftCmdRequest {
    let mut req = new_base_request(region_id, RegionEpoch::new(), false);
    req.mut_header().set_peer(peer);
    req.set_status_request(request);
    req
}

pub fn new_region_detail_cmd() -> StatusRequest {
    let mut cmd = StatusRequest::new();
    cmd.set_cmd_type(StatusCmdType::RegionDetail);
    cmd
}

pub fn new_region_leader_cmd() -> StatusRequest {
    let mut cmd = StatusRequest::new();
    cmd.set_cmd_type(StatusCmdType::RegionLeader);
    cmd
}

pub fn new_admin_request(region_id: u64,
                         epoch: &RegionEpoch,
                         request: AdminRequest)
                         -> RaftCmdRequest {
    let mut req = new_base_request(region_id, epoch.clone(), false);
    req.set_admin_request(request);
    req
}

pub fn new_transfer_leader_cmd(peer: metapb::Peer) -> AdminRequest {
    let mut cmd = AdminRequest::new();
    cmd.set_cmd_type(AdminCmdType::TransferLeader);
    cmd.mut_transfer_leader().set_peer(peer);
    cmd
}

pub fn new_compact_log_cmd(index: u64) -> AdminRequest {
    let mut cmd = AdminRequest::new();
    cmd.set_cmd_type(AdminCmdType::CompactLog);
    cmd.mut_compact_log().set_compact_index(index);
    cmd
}

pub fn new_peer(store_id: u64, peer_id: u64) -> metapb::Peer {
    let mut peer = metapb::Peer::new();
    peer.set_store_id(store_id);
    peer.set_id(peer_id);
    peer
}


pub fn new_store(store_id: u64, addr: String) -> metapb::Store {
    let mut store = metapb::Store::new();
    store.set_id(store_id);
    store.set_address(addr);

    store
}

pub fn sleep_ms(ms: u64) {
    thread::sleep(Duration::from_millis(ms));
}

pub fn is_error_response(resp: &RaftCmdResponse) -> bool {
    resp.get_header().has_error()
}

pub fn new_pd_change_peer(change_type: ConfChangeType,
                          peer: metapb::Peer)
                          -> RegionHeartbeatResponse {
    let mut change_peer = ChangePeer::new();
    change_peer.set_change_type(change_type);
    change_peer.set_peer(peer);

    let mut resp = RegionHeartbeatResponse::new();
    resp.set_change_peer(change_peer);
    resp
}

pub fn new_pd_add_change_peer(region: &metapb::Region,
                              peer: metapb::Peer)
                              -> Option<RegionHeartbeatResponse> {
    if let Some(p) = find_peer(region, peer.get_store_id()) {
        assert_eq!(p.get_id(), peer.get_id());
        return None;
    }

    Some(new_pd_change_peer(ConfChangeType::AddNode, peer))
}

pub fn new_pd_remove_change_peer(region: &metapb::Region,
                                 peer: metapb::Peer)
                                 -> Option<RegionHeartbeatResponse> {
    if find_peer(region, peer.get_store_id()).is_none() {
        return None;
    }

    Some(new_pd_change_peer(ConfChangeType::RemoveNode, peer))
}

pub fn new_pd_transfer_leader(peer: metapb::Peer) -> Option<RegionHeartbeatResponse> {
    let mut transfer_leader = TransferLeader::new();
    transfer_leader.set_peer(peer);

    let mut resp = RegionHeartbeatResponse::new();
    resp.set_transfer_leader(transfer_leader);
    Some(resp)
}
