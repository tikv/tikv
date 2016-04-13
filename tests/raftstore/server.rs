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

use std::collections::{HashMap, HashSet};
use std::thread;
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, Mutex, RwLock, mpsc};
use std::sync::atomic::{Ordering, AtomicUsize};
use std::time::Duration;

use rocksdb::DB;

use super::cluster::{Simulator, Cluster};
use tikv::server::{Server, ServerTransport, SendCh, create_event_loop, Msg, bind};
use tikv::server::{Node, Config, create_raft_storage};
use tikv::raftstore::Result;
use tikv::util::codec::rpc;
use tikv::util::make_std_tcp_conn;
use kvproto::raft_serverpb;
use kvproto::msgpb::{Message, MessageType};
use kvproto::raft_cmdpb::*;
use super::pd::TestPdClient;
use super::pd_ask::run_ask_loop;
use super::transport_simulate::{Strategy, SimulateTransport};

pub struct ServerCluster {
    cluster_id: u64,
    senders: HashMap<u64, SendCh>,
    handles: HashMap<u64, thread::JoinHandle<()>>,
    addrs: HashMap<u64, SocketAddr>,
    conns: Mutex<HashMap<SocketAddr, Vec<TcpStream>>>,

    msg_id: AtomicUsize,
    pd_client: Arc<RwLock<TestPdClient>>,
}

impl ServerCluster {
    pub fn new(cluster_id: u64, pd_client: Arc<RwLock<TestPdClient>>) -> ServerCluster {
        ServerCluster {
            cluster_id: cluster_id,
            senders: HashMap::new(),
            handles: HashMap::new(),
            addrs: HashMap::new(),
            conns: Mutex::new(HashMap::new()),
            msg_id: AtomicUsize::new(1),
            pd_client: pd_client,
        }
    }

    fn alloc_msg_id(&self) -> u64 {
        self.msg_id.fetch_add(1, Ordering::Relaxed) as u64
    }


    fn pool_get(&self, addr: &SocketAddr) -> Result<TcpStream> {
        {
            let mut conns = self.conns
                                .lock()
                                .unwrap();
            let conn = conns.get_mut(addr);
            if let Some(mut pool) = conn {
                if !pool.is_empty() {
                    return Ok(pool.pop().unwrap());
                }
            }
        }

        let conn = make_std_tcp_conn(addr).unwrap();
        Ok(conn)
    }

    fn pool_put(&self, addr: &SocketAddr, conn: TcpStream) {
        let mut conns = self.conns
                            .lock()
                            .unwrap();
        let p = conns.entry(*addr).or_insert_with(Vec::new);
        p.push(conn);
    }
}

impl Simulator for ServerCluster {
    #[allow(useless_format)]
    fn run_node(&mut self,
                node_id: u64,
                cfg: Config,
                engine: Arc<DB>,
                strategy: Vec<Strategy>)
                -> u64 {
        assert!(node_id == 0 || !self.handles.contains_key(&node_id));
        assert!(node_id == 0 || !self.senders.contains_key(&node_id));

        // TODO: simplify creating raft server later.
        let mut event_loop = create_event_loop().unwrap();
        let sendch = SendCh::new(event_loop.channel());
        let trans = Arc::new(RwLock::new(ServerTransport::new(self.cluster_id,
                                                              sendch,
                                                              self.pd_client.clone())));

        let mut cfg = cfg;

        let listener = bind(&cfg.addr).unwrap();
        let addr = listener.local_addr().unwrap();
        cfg.addr = format!("{}", addr);

        let simulate_trans = SimulateTransport::new(strategy, trans.clone());
        let mut node = Node::new(&cfg,
                                 self.pd_client.clone(),
                                 Arc::new(RwLock::new(simulate_trans)));

        node.start(engine.clone()).unwrap();
        let router = node.raft_store_router();

        assert!(node_id == 0 || node_id == node.id());
        let node_id = node.id();

        let store = create_raft_storage(node, engine).unwrap();

        let mut server = Server::new(&mut event_loop, listener, store, router).unwrap();

        let ch = server.get_sendch();

        let t = thread::spawn(move || {
            server.run(&mut event_loop).unwrap();
        });

        self.handles.insert(node_id, t);
        self.senders.insert(node_id, ch);
        self.addrs.insert(node_id, addr);

        node_id
    }

    fn stop_node(&mut self, node_id: u64) {
        let h = self.handles.remove(&node_id).unwrap();
        let ch = self.senders.remove(&node_id).unwrap();
        self.addrs.remove(&node_id).unwrap();

        ch.send(Msg::Quit).unwrap();
        h.join().unwrap();
    }

    fn get_node_ids(&self) -> HashSet<u64> {
        self.senders.keys().cloned().collect()
    }

    fn call_command(&self, request: RaftCmdRequest, timeout: Duration) -> Result<RaftCmdResponse> {
        let store_id = request.get_header().get_peer().get_store_id();
        let addr = self.addrs.get(&store_id).unwrap();
        let mut conn = self.pool_get(addr).unwrap();

        let mut msg = Message::new();
        msg.set_msg_type(MessageType::Cmd);
        msg.set_cmd_req(request);

        let msg_id = self.alloc_msg_id();
        conn.set_write_timeout(Some(timeout)).unwrap();
        try!(rpc::encode_msg(&mut conn, msg_id, &msg));

        conn.set_read_timeout(Some(timeout)).unwrap();
        let mut resp_msg = Message::new();
        let get_msg_id = try!(rpc::decode_msg(&mut conn, &mut resp_msg));

        self.pool_put(addr, conn);

        assert_eq!(resp_msg.get_msg_type(), MessageType::CmdResp);
        assert_eq!(msg_id, get_msg_id);

        Ok(resp_msg.take_cmd_resp())
    }

    fn send_raft_msg(&self, raft_msg: raft_serverpb::RaftMessage) -> Result<()> {
        let store_id = raft_msg.get_to_peer().get_store_id();
        let addr = self.addrs.get(&store_id).unwrap();

        let mut msg = Message::new();
        msg.set_msg_type(MessageType::Raft);
        msg.set_raft(raft_msg);
        let msg_id = self.alloc_msg_id();

        let mut conn = self.pool_get(addr).unwrap();
        conn.set_write_timeout(Some(Duration::from_secs(3))).unwrap();
        try!(rpc::encode_msg(&mut conn, msg_id, &msg));

        self.pool_put(addr, conn);

        Ok(())
    }
}

pub fn new_server_cluster(id: u64, count: usize) -> Cluster<ServerCluster> {
    let (tx, rx) = mpsc::channel();
    let pd_client = Arc::new(RwLock::new(TestPdClient::new(tx)));
    let sim = Arc::new(RwLock::new(ServerCluster::new(id, pd_client.clone())));
    run_ask_loop(pd_client.clone(), sim.clone(), rx);
    Cluster::new(id, count, sim, pd_client)
}
