#![allow(dead_code)]

use std::option::Option;
use std::collections::HashMap;
use std::sync::{Arc, RwLock, Mutex, Condvar};
pub use std::time::Duration;
use std::boxed::Box;
use std::thread;


use rocksdb::DB;
pub use tempdir::TempDir;
use uuid::Uuid;
use protobuf;

use tikv::raftserver::store::*;
use tikv::raftserver::{Result, other};
use tikv::proto::metapb;
use tikv::proto::raft_serverpb;
use tikv::proto::raft_cmdpb::{Request, RaftCommandRequest, RaftCommandResponse};
use tikv::proto::raft_cmdpb::{CommandType};

pub struct StoreTransport {
    peers: HashMap<u64, metapb::Peer>,

    senders: HashMap<u64, Sender>,
}

impl StoreTransport {
    pub fn new() -> Arc<RwLock<StoreTransport>> {
        Arc::new(RwLock::new(StoreTransport {
            peers: HashMap::new(),
            senders: HashMap::new(),
        }))
    }

    pub fn add_sender(&mut self, store_id: u64, sender: Sender) {
        self.senders.insert(store_id, sender);
    }
}

impl Transport for StoreTransport {
    fn cache_peer(&mut self, peer_id: u64, peer: metapb::Peer) {
        self.peers.insert(peer_id, peer);
    }

    fn get_peer(&self, peer_id: u64) -> Option<metapb::Peer> {
        self.peers.get(&peer_id).cloned()
    }

    fn send(&self, msg: raft_serverpb::RaftMessage) -> Result<()> {
        let to_store = msg.get_to_peer().get_store_id();
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

pub fn new_store(engine: Arc<DB>, trans: Arc<RwLock<StoreTransport>>) -> Store<StoreTransport> {
    let store = Store::new(Config::default(), engine, trans.clone()).unwrap();

    trans.write().unwrap().add_sender(store.get_store_id(), store.get_sender());

    store
}

// Create a base request.
pub fn new_base_request(region_id: u64, peer: metapb::Peer) -> RaftCommandRequest {
    let mut req = RaftCommandRequest::new();
    req.mut_header().set_region_id(region_id);
    req.mut_header().set_peer(peer);
    req.mut_header().set_uuid(Uuid::new_v4().as_bytes().to_vec());
    req
}

pub fn new_request(region_id: u64,
                   peer: metapb::Peer,
                   requests: Vec<Request>)
                   -> RaftCommandRequest {
    let mut req = new_base_request(region_id, peer);
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

pub fn new_peer(node_id: u64, store_id: u64, peer_id: u64) -> metapb::Peer {
    let mut peer = metapb::Peer::new();
    peer.set_node_id(node_id);
    peer.set_store_id(store_id);
    peer.set_peer_id(peer_id);
    peer
}

// Send the request and wait the response until timeout.
// Use Condvar to support call timeout. if timeout, return None.
// TODO: should we move this to the Sender member function?
pub fn call_timeout(sender: &Sender,
                    request: RaftCommandRequest,
                    timeout: Duration)
                    -> Option<RaftCommandResponse> {
    let resp: Option<RaftCommandResponse> = None;
    let pair = Arc::new((Mutex::new(resp), Condvar::new()));
    let pair2 = pair.clone();

    sender.send_command(request,
                        Box::new(move |resp: RaftCommandResponse| -> Result<()> {
                            let &(ref lock, ref cvar) = &*pair2;
                            let mut v = lock.lock().unwrap();
                            *v = Some(resp);
                            cvar.notify_one();
                            Ok(())
                        }))
          .unwrap();

    let &(ref lock, ref cvar) = &*pair;
    let mut v = lock.lock().unwrap();
    while v.is_none() {
        let (resp, timeout_res) = cvar.wait_timeout(v, timeout).unwrap();
        if timeout_res.timed_out() {
            return None;
        }

        v = resp
    }

    Some(v.take().unwrap())
}

pub fn sleep_ms(ms: u64) {
    thread::sleep(Duration::from_millis(ms));
}
