#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock, mpsc};
use std::time::Duration;

use rocksdb::DB;

use super::cluster::{Simulator, Cluster};
use tikv::server::Node;
use tikv::raftstore::store::{SendCh, Transport, msg, Msg, Callback, StoreSendCh};
use kvproto::raft_cmdpb::*;
use kvproto::raft_serverpb;
use tikv::raftstore::Result;
use tikv::util::HandyRwLock;
use tikv::server::Config as ServerConfig;
use super::pd::TestPdClient;
use super::pd_ask::run_ask_loop;

pub struct ChannelTransport {
    store_id: Option<u64>,
    senders: Arc<RwLock<HashMap<u64, StoreSendCh>>>,
}

impl ChannelTransport {
    pub fn new(trans: Arc<RwLock<HashMap<u64, StoreSendCh>>>) -> Arc<RwLock<ChannelTransport>> {
        Arc::new(RwLock::new(ChannelTransport {
            store_id: None,
            senders: trans,
        }))
    }

    pub fn get_sendch(&self, store_id: u64) -> Option<SendCh> {
        self.senders.rl().get(&store_id).cloned().map(|h| h.ch)
    }
}

fn send_msg(senders: Arc<RwLock<HashMap<u64, StoreSendCh>>>,
            msg: raft_serverpb::RaftMessage)
            -> Result<()> {
    let to_store = msg.get_to_peer().get_store_id();

    match senders.rl().get(&to_store) {
        Some(sender) => sender.ch.send(Msg::RaftMessage(msg)),
        _ => Err(box_err!("missing sender for store {}", to_store)),
    }
}

impl Transport for ChannelTransport {
    fn set_sendch(&mut self, sender: StoreSendCh) {
        assert!(self.store_id.is_none());
        self.store_id = Some(sender.store_id);
        self.senders.wl().insert(sender.store_id, sender);
    }

    fn remove_sendch(&mut self) -> Option<StoreSendCh> {
        let store_id = self.store_id.take().unwrap();
        self.senders.wl().remove(&store_id)
    }

    fn send(&self, msg: raft_serverpb::RaftMessage) -> Result<()> {
        send_msg(self.senders.clone(), msg)
    }

    fn send_raft_msg(&self, _: raft_serverpb::RaftMessage) -> Result<()> {
        unimplemented!();
    }

    fn send_command(&self, _: RaftCmdRequest, _: Callback) -> Result<()> {
        unimplemented!();
    }
}


pub struct NodeCluster {
    cluster_id: u64,
    trans: Arc<RwLock<HashMap<u64, StoreSendCh>>>,
    pd_client: Arc<RwLock<TestPdClient>>,
    nodes: HashMap<u64, Node<TestPdClient, ChannelTransport>>,
}

impl NodeCluster {
    pub fn new(cluster_id: u64, pd_client: Arc<RwLock<TestPdClient>>) -> NodeCluster {
        NodeCluster {
            cluster_id: cluster_id,
            trans: Arc::new(RwLock::new(HashMap::new())),
            pd_client: pd_client,
            nodes: HashMap::new(),
        }
    }
}

impl Simulator for NodeCluster {
    fn run_node(&mut self, node_id: u64, cfg: ServerConfig, engine: Arc<DB>) -> u64 {
        assert!(node_id == 0 || !self.nodes.contains_key(&node_id));

        let trans = ChannelTransport::new(self.trans.clone());
        let mut node = Node::new(&cfg, self.pd_client.clone(), trans);

        node.start(engine).unwrap();
        assert!(node_id == 0 || node_id == node.id());

        let node_id = node.id();
        self.nodes.insert(node_id, node);

        node_id
    }

    fn stop_node(&mut self, node_id: u64) {
        let node = self.nodes.remove(&node_id).unwrap();
        drop(node);
    }

    fn get_node_ids(&self) -> HashSet<u64> {
        self.nodes.keys().cloned().collect()
    }

    fn call_command(&self, request: RaftCmdRequest, timeout: Duration) -> Result<RaftCmdResponse> {
        let store_id = request.get_header().get_peer().get_store_id();
        let sender = self.trans.rl().get(&store_id).cloned().unwrap();
        msg::call_command(&sender.ch, request, timeout)
    }

    fn send_raft_msg(&self, msg: raft_serverpb::RaftMessage) -> Result<()> {
        send_msg(self.trans.clone(), msg)
    }
}

pub fn new_node_cluster(id: u64, count: usize) -> Cluster<NodeCluster> {
    let (tx, rx) = mpsc::channel();
    let pd_client = Arc::new(RwLock::new(TestPdClient::new(tx)));
    let sim = Arc::new(RwLock::new(NodeCluster::new(id, pd_client.clone())));
    run_ask_loop(pd_client.clone(), sim.clone(), rx);
    Cluster::new(id, count, sim, pd_client)
}
