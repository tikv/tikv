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
use std::sync::{Arc, RwLock};
use std::time::Duration;
use std::fs;

use rocksdb::DB;
use tempdir::TempDir;

use super::cluster::{Simulator, Cluster};
use tikv::server::Node;
use tikv::raftstore::store::*;
use kvproto::raft_cmdpb::*;
use kvproto::raft_serverpb;
use kvproto::raftpb::MessageType;
use tikv::raftstore::{store, Result};
use tikv::util::HandyRwLock;
use tikv::server::Config as ServerConfig;
use tikv::server::transport::{ServerRaftStoreRouter, RaftStoreRouter};
use tikv::raft::SnapshotStatus;
use tikv::storage::DEFAULT_CFS;
use super::pd::TestPdClient;
use super::transport_simulate::{SimulateTransport, Filter};

pub struct ChannelTransport {
    snap_paths: HashMap<u64, (SnapManager, TempDir)>,
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
        let to_peer_id = msg.get_to_peer().get_id();
        let region_id = msg.get_region_id();
        let is_snapshot = msg.get_message().get_msg_type() == MessageType::MsgSnapshot;

        if msg.get_message().get_msg_type() == MessageType::MsgSnapshot {
            let snap = msg.get_message().get_snapshot();
            let key = SnapKey::from_snap(snap).unwrap();
            let source_file = match self.snap_paths.get(&from_store) {
                Some(p) => {
                    p.0.wl().register(key.clone(), SnapEntry::Sending);
                    p.0.rl().get_snap_file(&key, true).unwrap()
                }
                None => return Err(box_err!("missing temp dir for store {}", from_store)),
            };
            let dst_file = match self.snap_paths.get(&to_store) {
                Some(p) => {
                    p.0.wl().register(key.clone(), SnapEntry::Receiving);
                    p.0.rl().get_snap_file(&key, false).unwrap()
                }
                None => return Err(box_err!("missing temp dir for store {}", to_store)),
            };

            defer!({
                self.snap_paths[&from_store].0.wl().deregister(&key, &SnapEntry::Sending);
                self.snap_paths[&to_store].0.wl().deregister(&key, &SnapEntry::Receiving);
            });

            if !dst_file.exists() {
                try!(fs::copy(source_file.path(), dst_file.path()));
            }
        }

        match self.routers.get(&to_store) {
            Some(h) => {
                try!(h.rl().send_raft_msg(msg));
                if is_snapshot {
                    // should report snapshot finish.
                    self.routers
                        .get(&from_store)
                        .unwrap()
                        .rl()
                        .report_snapshot(region_id, to_peer_id, SnapshotStatus::Finish)
                        .unwrap();
                }
                Ok(())
            }
            _ => Err(box_err!("missing sender for store {}", to_store)),
        }
    }
}

type SimulateChannelTransport = SimulateTransport<ChannelTransport>;

pub struct NodeCluster {
    trans: Arc<RwLock<ChannelTransport>>,
    pd_client: Arc<TestPdClient>,
    nodes: HashMap<u64, Node<TestPdClient>>,
    simulate_trans: HashMap<u64, Arc<RwLock<SimulateChannelTransport>>>,
}

impl NodeCluster {
    pub fn new(pd_client: Arc<TestPdClient>) -> NodeCluster {
        NodeCluster {
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

        let mut event_loop = create_event_loop(&cfg.store_cfg).unwrap();

        let simulate_trans = SimulateTransport::new(self.trans.clone());
        let trans = Arc::new(RwLock::new(simulate_trans));
        let mut node = Node::new(&mut event_loop, &cfg, self.pd_client.clone());

        let (snap_mgr, tmp) = if node_id == 0 ||
                                 !self.trans.rl().snap_paths.contains_key(&node_id) {
            let tmp = TempDir::new("test_cluster").unwrap();
            let snap_mgr = store::new_snap_mgr(tmp.path().to_str().unwrap(),
                                               Some(node.get_sendch()));
            (snap_mgr, Some(tmp))
        } else {
            let trans = self.trans.rl();
            let &(ref snap_mgr, _) = trans.snap_paths.get(&node_id).unwrap();
            (snap_mgr.clone(), None)
        };

        node.start(event_loop, engine, trans.clone(), snap_mgr.clone()).unwrap();
        assert!(node_id == 0 || node_id == node.id());
        debug!("node_id: {} tmp: {:?}",
               node_id,
               tmp.as_ref().map(|p| p.path().to_str().unwrap().to_owned()));
        if let Some(tmp) = tmp {
            self.trans.wl().snap_paths.insert(node.id(), (snap_mgr, tmp));
        }

        let node_id = node.id();
        self.trans.wl().routers.insert(node_id, node.raft_store_router());
        self.nodes.insert(node_id, node);
        self.simulate_trans.insert(node_id, trans);

        node_id
    }

    fn get_snap_dir(&self, node_id: u64) -> String {
        self.trans.wl().snap_paths.get(&node_id).unwrap().1.path().to_str().unwrap().to_owned()
    }

    fn stop_node(&mut self, node_id: u64) {
        self.nodes.remove(&node_id);
        self.trans.wl().routers.remove(&node_id).unwrap();
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

    fn add_filter(&self, node_id: u64, filter: Box<Filter>) {
        let trans = self.simulate_trans.get(&node_id).unwrap();
        trans.wl().add_filter(filter);
    }

    fn clear_filters(&self, node_id: u64) {
        let trans = self.simulate_trans.get(&node_id).unwrap();
        trans.wl().clear_filters();
    }

    fn get_store_sendch(&self, node_id: u64) -> Option<SendCh> {
        self.nodes.get(&node_id).map(|node| node.get_sendch())
    }
}

pub fn new_node_cluster(id: u64, count: usize) -> Cluster<NodeCluster> {
    let pd_client = Arc::new(TestPdClient::new(id));
    let sim = Arc::new(RwLock::new(NodeCluster::new(pd_client.clone())));
    Cluster::new(id, count, DEFAULT_CFS, sim, pd_client)
}
