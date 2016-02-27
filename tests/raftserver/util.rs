#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use std::thread;
use env_logger;

use rocksdb::{DB, WriteBatch, Writable};
use tempdir::TempDir;
use uuid::Uuid;
use protobuf;

use tikv::raftserver::store::*;
use tikv::raftserver::{Result, other};
use tikv::proto::metapb;
use tikv::proto::raft_serverpb;
use tikv::proto::raft_cmdpb::{Request, StatusRequest, AdminRequest, RaftCommandRequest,
                              RaftCommandResponse};
use tikv::proto::raft_cmdpb::{CommandType, StatusCommandType, AdminCommandType};
use tikv::proto::raftpb::ConfChangeType;
use tikv::raft::INVALID_ID;

pub struct StoreTransport {
    senders: HashMap<u64, SendCh>,
}

impl StoreTransport {
    pub fn new() -> Arc<RwLock<StoreTransport>> {
        Arc::new(RwLock::new(StoreTransport { senders: HashMap::new() }))
    }

    pub fn add_sender(&mut self, node_id: u64, sender: SendCh) {
        self.senders.insert(node_id, sender);
    }

    pub fn remove_sender(&mut self, node_id: u64) {
        self.senders.remove(&node_id);
    }
}

impl Transport for StoreTransport {
    fn send(&self, msg: raft_serverpb::RaftMessage) -> Result<()> {
        let to_store = msg.get_to_peer().get_node_id();
        match self.senders.get(&to_store) {
            None => Err(other(format!("missing sender for store {}", to_store))),
            Some(sender) => sender.send_raft_msg(msg),
        }
    }
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
        ..Config::default()
    }
}

// Create a base request.
pub fn new_base_request(region_id: u64) -> RaftCommandRequest {
    let mut req = RaftCommandRequest::new();
    req.mut_header().set_region_id(region_id);
    req.mut_header().set_uuid(Uuid::new_v4().as_bytes().to_vec());
    req
}

pub fn new_request(region_id: u64, requests: Vec<Request>) -> RaftCommandRequest {
    let mut req = new_base_request(region_id);
    req.set_requests(protobuf::RepeatedField::from_vec(requests));
    req
}

pub fn new_put_cmd(key: &[u8], value: &[u8]) -> Request {
    let mut cmd = Request::new();
    cmd.set_cmd_type(CommandType::Put);
    cmd.mut_put().set_key(key.to_vec());
    cmd.mut_put().set_value(value.to_vec());
    cmd
}

pub fn new_get_cmd(key: &[u8]) -> Request {
    let mut cmd = Request::new();
    cmd.set_cmd_type(CommandType::Get);
    cmd.mut_get().set_key(key.to_vec());
    cmd
}

pub fn new_delete_cmd(key: &[u8]) -> Request {
    let mut cmd = Request::new();
    cmd.set_cmd_type(CommandType::Delete);
    cmd.mut_delete().set_key(key.to_vec());
    cmd
}

pub fn new_seek_cmd(key: &[u8]) -> Request {
    let mut cmd = Request::new();
    cmd.set_cmd_type(CommandType::Seek);
    cmd.mut_seek().set_key(key.to_vec());
    cmd
}

pub fn new_status_request(region_id: u64,
                          peer: &metapb::Peer,
                          request: StatusRequest)
                          -> RaftCommandRequest {
    let mut req = new_base_request(region_id);
    req.mut_header().set_peer(peer.clone());
    req.set_status_request(request);
    req
}

pub fn new_region_leader_cmd() -> StatusRequest {
    let mut cmd = StatusRequest::new();
    cmd.set_cmd_type(StatusCommandType::RegionLeader);
    cmd
}

pub fn new_admin_request(region_id: u64, request: AdminRequest) -> RaftCommandRequest {
    let mut req = new_base_request(region_id);
    req.set_admin_request(request);
    req
}

pub fn new_change_peer_cmd(change_type: ConfChangeType, peer: metapb::Peer) -> AdminRequest {
    let mut cmd = AdminRequest::new();
    cmd.set_cmd_type(AdminCommandType::ChangePeer);
    cmd.mut_change_peer().set_change_type(change_type);
    cmd.mut_change_peer().set_peer(peer);
    cmd
}

pub fn new_peer(node_id: u64, store_id: u64, peer_id: u64) -> metapb::Peer {
    let mut peer = metapb::Peer::new();
    peer.set_node_id(node_id);
    peer.set_store_id(store_id);
    peer.set_peer_id(peer_id);
    peer
}

pub fn sleep_ms(ms: u64) {
    thread::sleep(Duration::from_millis(ms));
}

// A help function to simplify using env_logger.
pub fn init_env_log() {
    env_logger::init().expect("");
}

pub fn is_error_response(resp: &RaftCommandResponse) -> bool {
    resp.get_header().has_error()
}

pub fn is_invalid_peer(peer: &metapb::Peer) -> bool {
    peer.get_peer_id() == INVALID_ID
}

pub fn write_kvs(db: &DB, kvs: &[(Vec<u8>, Vec<u8>)]) {
    let wb = WriteBatch::new();
    for &(ref k, ref v) in kvs {
        wb.put(&keys::data_key(k), &v).expect("");
    }
    db.write(wb).unwrap();
}
