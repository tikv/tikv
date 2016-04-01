#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock, mpsc};
use std::time::Duration;

use rocksdb::DB;

use super::cluster::{Simulator, Cluster};
use tikv::server::Node;
use tikv::raftstore::store::{Transport, msg};
use kvproto::raft_cmdpb::*;
use kvproto::raft_serverpb;
use tikv::raftstore::{Result, other};
use tikv::util::HandyRwLock;
use tikv::server::Config as ServerConfig;
use tikv::server::transport::{ServerRaftStoreRouter, RaftStoreRouter};
use super::pd::TestPdClient;
use super::pd_ask::run_ask_loop;

pub struct ChannelTransport {
    pub routers: HashMap<u64, Arc<RwLock<ServerRaftStoreRouter>>>,
}

impl ChannelTransport {
    pub fn new() -> Arc<RwLock<ChannelTransport>> {
        Arc::new(RwLock::new(ChannelTransport { routers: HashMap::new() }))
    }
}

impl Transport for ChannelTransport {
    fn send(&self, msg: raft_serverpb::RaftMessage) -> Result<()> {
        let to_store = msg.get_to_peer().get_store_id();

        match self.routers.get(&to_store) {
            Some(h) => h.rl().send_raft_msg(msg),
            _ => Err(other(format!("missing sender for store {}", to_store))),
        }
    }
}


pub struct NodeCluster {
    cluster_id: u64,
    trans: Arc<RwLock<ChannelTransport>>,
    pd_client: Arc<RwLock<TestPdClient>>,
    nodes: HashMap<u64, Node<TestPdClient, ChannelTransport>>,
}

impl NodeCluster {
    pub fn new(cluster_id: u64, pd_client: Arc<RwLock<TestPdClient>>) -> NodeCluster {
        NodeCluster {
            cluster_id: cluster_id,
            trans: ChannelTransport::new(),
            pd_client: pd_client,
            nodes: HashMap::new(),
        }
    }
}

impl Simulator for NodeCluster {
    fn run_node(&mut self, node_id: u64, cfg: ServerConfig, engine: Arc<DB>) -> u64 {
        assert!(node_id == 0 || !self.nodes.contains_key(&node_id));

        let mut node = Node::new(&cfg, self.pd_client.clone(), self.trans.clone());

        node.start(engine).unwrap();
        assert!(node_id == 0 || node_id == node.id());

        let node_id = node.id();
        self.trans.wl().routers.insert(node_id, node.raft_store_router());
        self.nodes.insert(node_id, node);

        node_id
    }

    fn stop_node(&mut self, node_id: u64) {
        let node = self.nodes.remove(&node_id).unwrap();
        self.trans.wl().routers.remove(&node_id).unwrap();

        drop(node);
    }

    fn get_node_ids(&self) -> HashSet<u64> {
        self.nodes.keys().cloned().collect()
    }

    fn call_command(&self, request: RaftCmdRequest, timeout: Duration) -> Result<RaftCmdResponse> {
        let store_id = request.get_header().get_peer().get_store_id();
        let router = self.trans.rl().routers.get(&store_id).cloned().unwrap();
        let ch = router.rl().ch.clone();
        msg::call_command(&ch, request, timeout)
    }

    fn send_raft_msg(&self, msg: raft_serverpb::RaftMessage) -> Result<()> {
        self.trans.rl().send(msg)
    }
}

pub fn new_node_cluster(id: u64, count: usize) -> Cluster<NodeCluster> {
    let (tx, rx) = mpsc::channel();
    let pd_client = Arc::new(RwLock::new(TestPdClient::new(tx)));
    let sim = Arc::new(RwLock::new(NodeCluster::new(id, pd_client.clone())));
    run_ask_loop(pd_client.clone(), sim.clone(), rx);
    Cluster::new(id, count, sim, pd_client)
}
