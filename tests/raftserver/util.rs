#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;
use std::thread;
use tikv::util::{self as tikv_util, logger};
use std::env;

use rocksdb::{DB, WriteBatch, Writable};
use tempdir::TempDir;
use uuid::Uuid;
use protobuf;
use super::cluster::{Cluster, Simulator};

use tikv::raftserver::store::*;
use tikv::raftserver::server::Config as ServerConfig;
use kvproto::metapb;
use kvproto::raft_cmdpb::{Request, StatusRequest, AdminRequest, RaftCmdRequest, RaftCmdResponse};
use kvproto::raft_cmdpb::{CmdType, StatusCmdType, AdminCmdType};
use kvproto::raftpb::ConfChangeType;
use tikv::raft::INVALID_ID;

pub fn must_get_equal(engine: &Arc<DB>, key: &[u8], value: &[u8]) {
    for _ in 1..200 {
        if let Ok(res) = engine.get_value(&keys::data_key(key)) {
            if let Some(val) = res {
                assert_eq!(&*val, value);
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
    assert!(false);
}

pub fn must_get_none(engine: &Arc<DB>, key: &[u8]) {
    for _ in 1..200 {
        if let Ok(res) = engine.get_value(&keys::data_key(key)) {
            if res.is_none() {
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
    assert!(false);
}

pub fn new_engine(path: &TempDir) -> Arc<DB> {
    let db = DB::open_default(path.path().to_str().unwrap()).unwrap();
    Arc::new(db)
}

pub fn new_store_cfg() -> Config {
    Config {
        raft_base_tick_interval: 10,
        raft_heartbeat_ticks: 2,
        raft_election_timeout_ticks: 20,
        raft_log_gc_tick_interval: 100,
        region_check_size_diff: 10000,
        ..Config::default()
    }
}

pub fn new_server_config(cluster_id: u64) -> ServerConfig {
    let store_cfg = new_store_cfg();

    ServerConfig {
        cluster_id: cluster_id,
        addr: "127.0.0.1:0".to_owned(),
        store_cfg: store_cfg,
        ..ServerConfig::default()
    }
}

// Create a base request.
pub fn new_base_request(region_id: u64) -> RaftCmdRequest {
    let mut req = RaftCmdRequest::new();
    req.mut_header().set_region_id(region_id);
    req.mut_header().set_uuid(Uuid::new_v4().as_bytes().to_vec());
    req
}

pub fn new_request(region_id: u64, requests: Vec<Request>) -> RaftCmdRequest {
    let mut req = new_base_request(region_id);
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

pub fn new_get_cmd(key: &[u8]) -> Request {
    let mut cmd = Request::new();
    cmd.set_cmd_type(CmdType::Get);
    cmd.mut_get().set_key(key.to_vec());
    cmd
}

pub fn new_delete_cmd(key: &[u8]) -> Request {
    let mut cmd = Request::new();
    cmd.set_cmd_type(CmdType::Delete);
    cmd.mut_delete().set_key(key.to_vec());
    cmd
}

pub fn new_seek_cmd(key: &[u8]) -> Request {
    let mut cmd = Request::new();
    cmd.set_cmd_type(CmdType::Seek);
    cmd.mut_seek().set_key(key.to_vec());
    cmd
}

pub fn new_status_request(region_id: u64,
                          peer: metapb::Peer,
                          request: StatusRequest)
                          -> RaftCmdRequest {
    let mut req = new_base_request(region_id);
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

pub fn new_admin_request(region_id: u64, request: AdminRequest) -> RaftCmdRequest {
    let mut req = new_base_request(region_id);
    req.set_admin_request(request);
    req
}

pub fn new_change_peer_cmd(change_type: ConfChangeType,
                           peer: metapb::Peer,
                           region_epoch: &metapb::RegionEpoch)
                           -> AdminRequest {
    let mut cmd = AdminRequest::new();
    cmd.set_cmd_type(AdminCmdType::ChangePeer);
    cmd.mut_change_peer().set_change_type(change_type);
    cmd.mut_change_peer().set_peer(peer);
    cmd.mut_change_peer().set_region_epoch(region_epoch.clone());
    cmd
}

pub fn new_split_region_cmd(split_key: Option<Vec<u8>>,
                            new_region_id: u64,
                            region_epoch: &metapb::RegionEpoch,
                            peer_ids: Vec<u64>)
                            -> AdminRequest {
    let mut cmd = AdminRequest::new();
    cmd.set_cmd_type(AdminCmdType::Split);
    if let Some(key) = split_key {
        cmd.mut_split().set_split_key(key);
    }
    cmd.mut_split().set_new_region_id(new_region_id);
    cmd.mut_split().set_region_epoch(region_epoch.clone());
    cmd.mut_split().set_new_peer_ids(peer_ids);
    cmd


}

pub fn new_peer(node_id: u64, store_id: u64, peer_id: u64) -> metapb::Peer {
    let mut peer = metapb::Peer::new();
    peer.set_node_id(node_id);
    peer.set_store_id(store_id);
    peer.set_peer_id(peer_id);
    peer
}

pub fn new_node(node_id: u64, addr: String) -> metapb::Node {
    let mut node = metapb::Node::new();
    node.set_node_id(node_id);
    node.set_address(addr);

    node
}

pub fn new_store(node_id: u64, store_id: u64) -> metapb::Store {
    let mut store = metapb::Store::new();
    store.set_node_id(node_id);
    store.set_store_id(store_id);

    store
}

pub fn sleep_ms(ms: u64) {
    thread::sleep(Duration::from_millis(ms));
}

// A help function to initial logger.
pub fn init_log() {
    let level = logger::get_level_by_string(&env::var("LOG_LEVEL").unwrap_or("debug".to_owned()));
    tikv_util::init_log(level).unwrap();
}

pub fn is_error_response(resp: &RaftCmdResponse) -> bool {
    resp.get_header().has_error()
}

pub fn is_invalid_peer(peer: &metapb::Peer) -> bool {
    peer.get_peer_id() == INVALID_ID
}

pub fn write_kvs(db: &DB, kvs: &[(Vec<u8>, Vec<u8>)]) {
    let wb = WriteBatch::new();
    for &(ref k, ref v) in kvs {
        wb.put(k, &v).expect("");
    }
    db.write(wb).unwrap();
}

pub fn enc_write_kvs(db: &DB, kvs: &[(Vec<u8>, Vec<u8>)]) {
    let wb = WriteBatch::new();
    for &(ref k, ref v) in kvs {
        wb.put(&keys::data_key(k), &v).expect("");
    }
    db.write(wb).expect("");
}

pub fn prepare_cluster<T: Simulator>(cluster: &mut Cluster<T>,
                                     initial_kvs: &[(Vec<u8>, Vec<u8>)]) {
    cluster.bootstrap_region().expect("");
    cluster.start();
    for engine in cluster.engines.values() {
        enc_write_kvs(engine, initial_kvs);
    }
    cluster.leader_of_region(1).unwrap();
}
