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
use std::net::SocketAddr;
use std::sync::{Arc, RwLock, mpsc};
use std::time::Duration;
use std::boxed::FnBox;

use grpc::Environment;
use rocksdb::{DB, DBCompressionType};
use tempdir::TempDir;

use super::cluster::{Simulator, Cluster};
use tikv::server::{Server, ServerTransport};
use tikv::server::{Node, Config, create_raft_storage, PdStoreAddrResolver, RaftClient};
use tikv::server::transport::ServerRaftStoreRouter;
use tikv::server::transport::RaftStoreRouter;
use tikv::raftstore::{Error, Result, store};
use tikv::raftstore::store::{Msg as StoreMsg, SnapManager};
use tikv::util::transport::SendCh;
use tikv::storage::{Engine, CfName, ALL_CFS};
use kvproto::raft_serverpb::{self, RaftMessage};
use kvproto::raft_cmdpb::*;

use super::pd::TestPdClient;
use super::transport_simulate::*;

type SimulateStoreTransport = SimulateTransport<StoreMsg, ServerRaftStoreRouter>;
type SimulateServerTransport = SimulateTransport<RaftMessage,
                                                 ServerTransport<SimulateStoreTransport,
                                                                 PdStoreAddrResolver>>;

pub struct ServerCluster {
    routers: HashMap<u64, SimulateStoreTransport>,
    servers: HashMap<u64, Server<SimulateStoreTransport, PdStoreAddrResolver>>,
    nodes: HashMap<u64, Node<TestPdClient>>,
    addrs: HashMap<u64, SocketAddr>,
    sim_trans: HashMap<u64, SimulateServerTransport>,
    store_chs: HashMap<u64, SendCh<StoreMsg>>,
    pub storages: HashMap<u64, Box<Engine>>,
    snap_paths: HashMap<u64, TempDir>,
    pd_client: Arc<TestPdClient>,
    raft_client: RaftClient,
}

impl ServerCluster {
    pub fn new(pd_client: Arc<TestPdClient>) -> ServerCluster {
        ServerCluster {
            routers: HashMap::new(),
            servers: HashMap::new(),
            nodes: HashMap::new(),
            addrs: HashMap::new(),
            sim_trans: HashMap::new(),
            pd_client: pd_client,
            store_chs: HashMap::new(),
            storages: HashMap::new(),
            snap_paths: HashMap::new(),
            raft_client: RaftClient::new(Arc::new(Environment::new(1)), Config::new()),
        }
    }
}

impl Simulator for ServerCluster {
    #[allow(useless_format)]
    fn run_node(&mut self, node_id: u64, mut cfg: Config, engine: Arc<DB>) -> u64 {
        assert!(node_id == 0 || !self.nodes.contains_key(&node_id));
        assert!(node_id == 0 || !self.servers.contains_key(&node_id));

        let (tmp_str, tmp) = if node_id == 0 || !self.snap_paths.contains_key(&node_id) {
            let p = TempDir::new("test_cluster").unwrap();
            (p.path().to_str().unwrap().to_owned(), Some(p))
        } else {
            let p = self.snap_paths[&node_id].path().to_str().unwrap();
            (p.to_owned(), None)
        };

        // Now we cache the store address, so here we should re-use last
        // listening address for the same store.
        if let Some(addr) = self.addrs.get(&node_id) {
            cfg.addr = format!("{}", addr)
        }

        // Initialize raftstore channels.
        let mut event_loop = store::create_event_loop(&cfg.raft_store).unwrap();
        let store_sendch = SendCh::new(event_loop.channel(), "raftstore");
        let raft_router = ServerRaftStoreRouter::new(store_sendch.clone());
        let sim_router = SimulateTransport::new(raft_router);
        let (snap_status_sender, snap_status_receiver) = mpsc::channel();

        // Create storage.
        let mut store = create_raft_storage(sim_router.clone(), engine.clone(), &cfg).unwrap();
        store.start(&cfg.storage).unwrap();
        self.storages.insert(node_id, store.get_engine());

        // Create pd client, snapshot manager, server.
        let resolver = PdStoreAddrResolver::new(self.pd_client.clone()).unwrap();
        let snap_mgr = SnapManager::new(tmp_str,
                                        Some(store_sendch),
                                        cfg.raft_store.use_sst_file_snapshot,
                                        DBCompressionType::DBLz4);
        let mut server = Server::new(&cfg,
                                     store.clone(),
                                     sim_router.clone(),
                                     snap_status_sender,
                                     resolver,
                                     snap_mgr.clone())
            .unwrap();
        let addr = server.listening_addr();
        cfg.addr = format!("{}", addr);
        let trans = server.transport();
        let simulate_trans = SimulateTransport::new(trans.clone());

        // Create node.
        let mut node = Node::new(&mut event_loop, &cfg, self.pd_client.clone());
        node.start(event_loop,
                   engine,
                   simulate_trans.clone(),
                   snap_mgr.clone(),
                   snap_status_receiver)
            .unwrap();
        assert!(node_id == 0 || node_id == node.id());
        let node_id = node.id();
        if let Some(tmp) = tmp {
            self.snap_paths.insert(node_id, tmp);
        }
        self.store_chs.insert(node_id, node.get_sendch());
        self.sim_trans.insert(node_id, simulate_trans);

        server.start(&cfg).unwrap();

        self.nodes.insert(node_id, node);
        self.servers.insert(node_id, server);
        self.routers.insert(node_id, sim_router);
        self.addrs.insert(node_id, addr);

        node_id
    }

    fn get_snap_dir(&self, node_id: u64) -> String {
        self.snap_paths[&node_id].path().to_str().unwrap().to_owned()
    }

    fn stop_node(&mut self, node_id: u64) {
        let mut server = self.servers.remove(&node_id).unwrap();
        server.stop().unwrap();
        let mut node = self.nodes.remove(&node_id).unwrap();
        node.stop().unwrap();
        let _ = self.store_chs.remove(&node_id).unwrap();
    }

    fn get_node_ids(&self) -> HashSet<u64> {
        self.nodes.keys().cloned().collect()
    }

    fn call_command_on_node(&self,
                            node_id: u64,
                            request: RaftCmdRequest,
                            timeout: Duration)
                            -> Result<RaftCmdResponse> {
        if !self.routers.contains_key(&node_id) {
            return Err(box_err!("missing sender for store {}", node_id));
        }

        let router = self.routers.get(&node_id).cloned().unwrap();
        wait_op!(|cb: Box<FnBox(RaftCmdResponse) + 'static + Send>| {
                     router.send_command(request, cb).unwrap()
                 },
                 timeout)
            .ok_or_else(|| Error::Timeout(format!("request timeout for {:?}", timeout)))
    }

    fn send_raft_msg(&mut self, raft_msg: raft_serverpb::RaftMessage) -> Result<()> {
        let store_id = raft_msg.get_to_peer().get_store_id();
        let addr = self.addrs[&store_id];
        self.raft_client.send(store_id, addr, raft_msg).unwrap();
        self.raft_client.flush();
        Ok(())
    }

    fn add_send_filter(&mut self, node_id: u64, filter: SendFilter) {
        self.sim_trans.get_mut(&node_id).unwrap().add_filter(filter);
    }

    fn clear_send_filters(&mut self, node_id: u64) {
        self.sim_trans.get_mut(&node_id).unwrap().clear_filters();
    }

    fn add_recv_filter(&mut self, node_id: u64, filter: RecvFilter) {
        self.routers.get_mut(&node_id).unwrap().add_filter(filter);
    }

    fn clear_recv_filters(&mut self, node_id: u64) {
        self.routers.get_mut(&node_id).unwrap().clear_filters();
    }

    fn get_store_sendch(&self, node_id: u64) -> Option<SendCh<StoreMsg>> {
        self.store_chs.get(&node_id).cloned()
    }
}

pub fn new_server_cluster(id: u64, count: usize) -> Cluster<ServerCluster> {
    new_server_cluster_with_cfs(id, count, ALL_CFS)
}

pub fn new_server_cluster_with_cfs(id: u64,
                                   count: usize,
                                   cfs: &[CfName])
                                   -> Cluster<ServerCluster> {
    let pd_client = Arc::new(TestPdClient::new(id));
    let sim = Arc::new(RwLock::new(ServerCluster::new(pd_client.clone())));
    Cluster::new(id, count, cfs, sim, pd_client)
}
