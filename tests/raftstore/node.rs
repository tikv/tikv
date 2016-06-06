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

#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use std::fs::File;
use std::io;

use rocksdb::DB;
use tempdir::TempDir;
use protobuf::Message;

use super::cluster::{Simulator, Cluster};
use tikv::server::Node;
use tikv::raftstore::store::*;
use kvproto::raft_cmdpb::*;
use kvproto::raft_serverpb;
use kvproto::raftpb::MessageType;
use tikv::raftstore::Result;
use tikv::util::HandyRwLock;
use tikv::server::Config as ServerConfig;
use tikv::server::transport::{ServerRaftStoreRouter, RaftStoreRouter};
use tikv::util::codec::number::NumberEncoder;
use super::pd::TestPdClient;
use super::transport_simulate::{SimulateTransport, Filter};

pub struct ChannelTransport {
    snap_paths: HashMap<u64, TempDir>,
    routers: HashMap<u64, Arc<RwLock<ServerRaftStoreRouter>>>,
}

impl ChannelTransport {
    pub fn new() -> Arc<RwLock<ChannelTransport>> {
        Arc::new(RwLock::new(ChannelTransport {
            snap_paths: HashMap::new(),
            routers: HashMap::new(),
        }))
    }
}

impl Transport for ChannelTransport {
    fn send(&self, msg: raft_serverpb::RaftMessage) -> Result<()> {
        let from_store = msg.get_from_peer().get_store_id();
        let to_store = msg.get_to_peer().get_store_id();

        if msg.get_message().get_msg_type() == MessageType::MsgSnapshot {
            let snap = msg.get_message().get_snapshot();
            let source_file = match self.snap_paths.get(&from_store) {
                Some(p) => SnapFile::from_snap(p.path(), snap, SNAP_GEN_PREFIX).unwrap(),
                None => return Err(box_err!("missing temp dir for store {}", from_store)),
            };
            let mut dst_file = match self.snap_paths.get(&to_store) {
                Some(p) => SnapFile::from_snap(p.path(), snap, SNAP_REV_PREFIX).unwrap(),
                None => return Err(box_err!("missing temp dir for store {}", to_store)),
            };
            let mut reader = File::open(source_file.path()).unwrap();

            dst_file.encode_u64(msg.compute_size() as u64).unwrap();
            msg.write_to_writer(&mut dst_file).unwrap();
            io::copy(&mut reader, &mut dst_file).unwrap();
            dst_file.save().unwrap();
        }

        match self.routers.get(&to_store) {
            Some(h) => h.rl().send_raft_msg(msg),
            _ => Err(box_err!("missing sender for store {}", to_store)),
        }
    }
}

type SimulateChannelTransport = SimulateTransport<ChannelTransport>;

pub struct NodeCluster {
    cluster_id: u64,
    trans: Arc<RwLock<ChannelTransport>>,
    pd_client: Arc<TestPdClient>,
    nodes: HashMap<u64, Node<TestPdClient>>,
    simulate_trans: HashMap<u64, Arc<RwLock<SimulateChannelTransport>>>,
}

impl NodeCluster {
    pub fn new(cluster_id: u64, pd_client: Arc<TestPdClient>) -> NodeCluster {
        NodeCluster {
            cluster_id: cluster_id,
            trans: ChannelTransport::new(),
            pd_client: pd_client,
            nodes: HashMap::new(),
            simulate_trans: HashMap::new(),
        }
    }
}

impl Simulator for NodeCluster {
    fn run_node(&mut self, node_id: u64, cfg: ServerConfig, engine: Arc<DB>) -> u64 {
        assert!(node_id == 0 || !self.nodes.contains_key(&node_id));

        let mut cfg = cfg;
        let tmp = TempDir::new("test_cluster").unwrap();
        cfg.store_cfg.snap_path = tmp.path().to_str().unwrap().to_owned();

        let mut event_loop = create_event_loop(&cfg.store_cfg).unwrap();
        let simulate_trans = SimulateTransport::new(self.trans.clone());
        let trans = Arc::new(RwLock::new(simulate_trans));
        let mut node = Node::new(&mut event_loop, &cfg, self.pd_client.clone());

        node.start(event_loop, engine, trans.clone())
            .unwrap();
        assert!(node_id == 0 || node_id == node.id());

        let node_id = node.id();
        self.trans.wl().routers.insert(node_id, node.raft_store_router());
        self.nodes.insert(node_id, node);
        self.simulate_trans.insert(node_id, trans);
        self.trans.wl().snap_paths.insert(node_id, tmp);

        node_id
    }

    fn stop_node(&mut self, node_id: u64) {
        let node = self.nodes.remove(&node_id).unwrap();
        self.trans.wl().routers.remove(&node_id).unwrap();
        self.trans.wl().snap_paths.remove(&node_id).unwrap();

        drop(node);
    }

    fn get_node_ids(&self) -> HashSet<u64> {
        self.nodes.keys().cloned().collect()
    }

    fn call_command(&self, request: RaftCmdRequest, timeout: Duration) -> Result<RaftCmdResponse> {
        let store_id = request.get_header().get_peer().get_store_id();
        if !self.trans.rl().routers.contains_key(&store_id) {
            return Err(box_err!("missing sender for store {}", store_id));
        }

        let router = self.trans.rl().routers.get(&store_id).cloned().unwrap();
        let ch = router.rl().ch.clone();
        msg::call_command(&ch, request, timeout)
    }

    fn send_raft_msg(&self, msg: raft_serverpb::RaftMessage) -> Result<()> {
        self.trans.rl().send(msg)
    }

    fn hook_transport(&self, node_id: u64, filters: Vec<Box<Filter>>) {
        let trans = self.simulate_trans.get(&node_id).unwrap();
        trans.wl().set_filters(filters);
    }

    fn get_store_sendch(&self, node_id: u64) -> Option<SendCh> {
        self.nodes.get(&node_id).map(|node| node.get_sendch())
    }
}

pub fn new_node_cluster(id: u64, count: usize) -> Cluster<NodeCluster> {
    let pd_client = Arc::new(TestPdClient::new(id));
    let sim = Arc::new(RwLock::new(NodeCluster::new(id, pd_client.clone())));
    Cluster::new(id, count, sim, pd_client)
}
