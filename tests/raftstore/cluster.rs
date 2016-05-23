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

use rocksdb::DB;
use tempdir::TempDir;

use tikv::raftstore::{Result, Error};
use tikv::raftstore::store::*;
use super::util::*;
use kvproto::raft_cmdpb::*;
use kvproto::metapb::{self, RegionEpoch};
use kvproto::raftpb::ConfChangeType;
use kvproto::raft_serverpb::RaftMessage;
use tikv::pd::PdClient;
use tikv::util::HandyRwLock;
use tikv::server::Config as ServerConfig;
use super::pd::TestPdClient;
use super::transport_simulate::*;

// We simulate 3 or 5 nodes, each has a store.
// Sometimes, we use fixed id to test, which means the id
// isn't allocated by pd, and node id, store id are same.
// E,g, for node 1, the node id and store id are both 1.

pub trait Simulator {
    // Pass 0 to let pd allocate a node id if db is empty.
    // If node id > 0, the node must be created in db already,
    // and the node id must be the same as given argument.
    // Return the node id.
    // TODO: we will rename node name here because now we use store only.
    fn run_node(&mut self, node_id: u64, cfg: ServerConfig, engine: Arc<DB>) -> u64;
    fn stop_node(&mut self, node_id: u64);
    fn get_node_ids(&self) -> HashSet<u64>;
    fn call_command(&self, request: RaftCmdRequest, timeout: Duration) -> Result<RaftCmdResponse>;
    fn send_raft_msg(&self, msg: RaftMessage) -> Result<()>;
    fn hook_transport(&self, node_id: u64, filters: Vec<Box<Filter>>);
}

pub struct Cluster<T: Simulator> {
    pub cfg: ServerConfig,
    leaders: HashMap<u64, metapb::Peer>,
    paths: Vec<TempDir>,
    dbs: Vec<Arc<DB>>,

    // node id -> db engine.
    pub engines: HashMap<u64, Arc<DB>>,

    sim: Arc<RwLock<T>>,
    pub pd_client: Arc<TestPdClient>,
}

impl<T: Simulator> Cluster<T> {
    // Create the default Store cluster.
    pub fn new(id: u64,
               count: usize,
               sim: Arc<RwLock<T>>,
               pd_client: Arc<TestPdClient>)
               -> Cluster<T> {
        let mut c = Cluster {
            cfg: new_server_config(id),
            leaders: HashMap::new(),
            paths: vec![],
            dbs: vec![],
            engines: HashMap::new(),
            sim: sim,
            pd_client: pd_client,
        };

        c.create_engines(count);

        c
    }

    pub fn id(&self) -> u64 {
        self.cfg.cluster_id
    }

    fn create_engines(&mut self, count: usize) {
        for _ in 0..count {
            self.paths.push(TempDir::new("test_cluster").unwrap());
        }

        for item in &self.paths {
            self.dbs.push(new_engine(item));
        }
    }

    pub fn start(&mut self) {
        if self.engines.is_empty() {
            let mut sim = self.sim.wl();
            for engine in &self.dbs {
                let node_id = sim.run_node(0, self.cfg.clone(), engine.clone());
                self.engines.insert(node_id, engine.clone());
            }
        } else {
            // recover from last shutdown.
            let node_ids: Vec<u64> = self.engines.keys().cloned().collect();
            for node_id in node_ids {
                self.run_node(node_id);
            }
        }
    }

    pub fn run_node(&mut self, node_id: u64) {
        let engine = self.engines.get(&node_id).unwrap();
        self.sim.wl().run_node(node_id, self.cfg.clone(), engine.clone());
    }

    pub fn stop_node(&mut self, node_id: u64) {
        self.sim.wl().stop_node(node_id);
    }

    pub fn get_engine(&self, node_id: u64) -> Arc<DB> {
        self.engines.get(&node_id).unwrap().clone()
    }

    pub fn send_raft_msg(&self, msg: RaftMessage) -> Result<()> {
        self.sim.rl().send_raft_msg(msg)
    }

    pub fn call_command(&self,
                        request: RaftCmdRequest,
                        timeout: Duration)
                        -> Result<RaftCmdResponse> {
        self.sim.rl().call_command(request, timeout)
    }

    fn call_command_on_leader_once(&mut self,
                                   mut request: RaftCmdRequest,
                                   timeout: Duration)
                                   -> Result<RaftCmdResponse> {
        let region_id = request.get_header().get_region_id();
        if let Some(leader) = self.leader_of_region(region_id) {
            request.mut_header().set_peer(leader);
            return self.call_command(request, timeout);
        }
        Err(box_err!("can't get leader of region"))
    }

    pub fn call_command_on_leader(&mut self,
                                  request: RaftCmdRequest,
                                  timeout: Duration)
                                  -> Result<RaftCmdResponse> {
        let mut retry_cnt = 0;
        let region_id = request.get_header().get_region_id();
        loop {
            let result = self.call_command_on_leader_once(request.clone(), timeout);
            if result.is_err() {
                return result;
            }
            let resp = result.unwrap();
            if self.refresh_leader_if_needed(&resp, region_id) && retry_cnt < 10 {
                retry_cnt += 1;
                warn!("seems leader changed, let's retry");
                continue;
            }
            return Ok(resp);
        }
    }

    fn valid_leader_id(&self, region_id: u64, leader_id: u64) -> bool {
        let store_ids = self.store_ids_of_region(region_id);
        let node_ids = self.sim.rl().get_node_ids();
        store_ids.contains(&leader_id) && node_ids.contains(&leader_id)
    }

    fn store_ids_of_region(&self, region_id: u64) -> Vec<u64> {
        let peers = self.pd_client
            .get_region_by_id(region_id)
            .unwrap()
            .take_peers();
        peers.iter().map(|peer| peer.get_store_id()).collect()
    }

    fn query_leader(&self, store_id: u64, region_id: u64) -> Option<metapb::Peer> {
        // To get region leader, we don't care real peer id, so use 0 instead.
        let peer = new_peer(store_id, 0);
        let find_leader = new_status_request(region_id, peer, new_region_leader_cmd());
        let mut resp = self.call_command(find_leader, Duration::from_secs(3)).unwrap();
        let mut region_leader = resp.take_status_response().take_region_leader();
        // NOTE: node id can't be 0.
        if self.valid_leader_id(region_id, region_leader.get_leader().get_store_id()) {
            Some(region_leader.take_leader())
        } else {
            None
        }
    }

    pub fn leader_of_region(&mut self, region_id: u64) -> Option<metapb::Peer> {
        let store_ids = self.store_ids_of_region(region_id);
        if let Some(l) = self.leaders.get(&region_id) {
            // leader may be stopped in some tests.
            if self.valid_leader_id(region_id, l.get_store_id()) {
                return Some(l.clone());
            }
        }
        self.reset_leader_of_region(region_id);
        let mut leader = None;
        let mut retry_cnt = 500;

        let node_ids: HashSet<u64> = self.sim.rl().get_node_ids();
        let mut count = 0;
        while (leader.is_none() || count < store_ids.len()) && retry_cnt > 0 {
            count = 0;
            leader = None;
            for store_id in &store_ids {
                // For some tests, we stop the node but pd still has this information,
                // and we must skip this.
                if !node_ids.contains(store_id) {
                    count += 1;
                    continue;
                }
                let l = self.query_leader(*store_id, region_id);
                if l.is_none() {
                    continue;
                }
                if leader.is_none() {
                    leader = l;
                    count += 1;
                } else if l == leader {
                    count += 1;
                }
            }
            sleep_ms(10);
            retry_cnt -= 1;
        }

        if let Some(l) = leader {
            self.leaders.insert(region_id, l);
        }

        self.leaders.get(&region_id).cloned()
    }

    // Multiple nodes with fixed node id, like node 1, 2, .. 5,
    // First region 1 is in all stores with peer 1, 2, .. 5.
    // Peer 1 is in node 1, store 1, etc.
    pub fn bootstrap_region(&mut self) -> Result<()> {
        for (id, engine) in self.dbs.iter().enumerate() {
            let id = id as u64 + 1;
            self.engines.insert(id, engine.clone());
        }

        let mut region = metapb::Region::new();
        region.set_id(1);
        region.set_start_key(keys::EMPTY_KEY.to_vec());
        region.set_end_key(keys::EMPTY_KEY.to_vec());
        region.mut_region_epoch().set_version(1);
        region.mut_region_epoch().set_conf_ver(1);

        for (&id, engine) in &self.engines {
            let peer = new_peer(id, id);
            region.mut_peers().push(peer.clone());
            bootstrap_store(engine, self.id(), id).unwrap();
        }

        for engine in self.engines.values() {
            try!(write_region(&engine, &region));
        }

        self.bootstrap_cluster(region);

        Ok(())
    }

    // Return first region id.
    pub fn bootstrap_conf_change(&mut self) -> u64 {
        for (id, engine) in self.dbs.iter().enumerate() {
            let id = id as u64 + 1;
            self.engines.insert(id, engine.clone());
        }

        for (&id, engine) in &self.engines {
            bootstrap_store(engine, self.id(), id).unwrap();
        }

        let node_id = 1;
        let region = bootstrap_region(self.engines.get(&node_id).unwrap(), 1, 1, 1).unwrap();
        let rid = region.get_id();
        self.bootstrap_cluster(region);
        rid
    }


    // This is only for fixed id test.
    fn bootstrap_cluster(&mut self, region: metapb::Region) {
        self.pd_client
            .bootstrap_cluster(new_store(1, "".to_owned()), region)
            .unwrap();

        for &id in self.engines.keys() {
            self.pd_client.put_store(new_store(id, "".to_owned())).unwrap();
        }
    }

    pub fn reset_leader_of_region(&mut self, region_id: u64) {
        self.leaders.remove(&region_id);
    }

    pub fn check_quorum<F: FnMut(&&Arc<DB>) -> bool>(&self, condition: F) -> bool {
        if self.engines.is_empty() {
            return true;
        }
        self.engines.values().filter(condition).count() > self.engines.len() / 2
    }

    pub fn shutdown(&mut self) {
        let keys: HashSet<u64> = self.sim.rl().get_node_ids();
        for id in keys {
            self.stop_node(id);
        }
        self.leaders.clear();
        debug!("all nodes are shut down.");
    }

    // If the resp is "not leader error", get the real leader.
    // Sometimes, we may still can't get leader even in "not leader error",
    // returns a INVALID_PEER for this.
    pub fn refresh_leader_if_needed(&mut self, resp: &RaftCmdResponse, region_id: u64) -> bool {
        if !is_error_response(resp) {
            return false;
        }

        let err = resp.get_header().get_error();
        if !err.has_not_leader() {
            return false;
        }

        let err = err.get_not_leader();
        if !err.has_leader() {
            self.reset_leader_of_region(region_id);
            return true;
        }
        self.leaders.insert(region_id, err.get_leader().clone());
        true
    }

    pub fn request(&mut self,
                   key: &[u8],
                   reqs: Vec<Request>,
                   timeout: Duration)
                   -> RaftCmdResponse {
        for _ in 0..10 {
            let mut region = self.get_region(key);
            let region_id = region.get_id();
            let req = new_request(region_id, region.take_region_epoch().clone(), reqs.clone());
            let result = self.call_command_on_leader(req, timeout);

            if let Err(Error::Timeout(_)) = result {
                warn!("call command timeout, let's retry");
                continue;
            }

            let resp = result.unwrap();
            if resp.get_header().get_error().has_stale_epoch() {
                warn!("seems split, let's retry");
                continue;
            }
            return resp;
        }
        panic!("request failed after retry for 10 times");
    }

    pub fn get_region(&self, key: &[u8]) -> metapb::Region {
        self.pd_client
            .get_region(key)
            .unwrap()
    }

    pub fn get_region_id(&self, key: &[u8]) -> u64 {
        self.get_region(key).get_id()
    }

    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        let mut resp = self.request(key, vec![new_get_cmd(key)], Duration::from_secs(3));
        if resp.get_header().has_error() {
            panic!("response {:?} has error", resp);
        }
        assert_eq!(resp.get_responses().len(), 1);
        assert_eq!(resp.get_responses()[0].get_cmd_type(), CmdType::Get);
        let mut get = resp.mut_responses()[0].take_get();
        if get.has_value() {
            Some(get.take_value())
        } else {
            None
        }
    }

    pub fn must_put(&mut self, key: &[u8], value: &[u8]) {
        let resp = self.request(key, vec![new_put_cmd(key, value)], Duration::from_secs(3));
        if resp.get_header().has_error() {
            panic!("response {:?} has error", resp);
        }
        assert_eq!(resp.get_responses().len(), 1);
        assert_eq!(resp.get_responses()[0].get_cmd_type(), CmdType::Put);
    }

    pub fn must_seek(&mut self, key: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
        let resp = self.request(key, vec![new_seek_cmd(key)], Duration::from_secs(3));
        if resp.get_header().has_error() {
            panic!("response {:?} has error", resp);
        }
        assert_eq!(resp.get_responses().len(), 1);
        let resp = &resp.get_responses()[0];
        assert_eq!(resp.get_cmd_type(), CmdType::Seek);
        if resp.has_seek() {
            Some((resp.get_seek().get_key().to_vec(), resp.get_seek().get_value().to_vec()))
        } else {
            None
        }
    }

    pub fn must_delete(&mut self, key: &[u8]) {
        let resp = self.request(key, vec![new_delete_cmd(key)], Duration::from_secs(3));
        if resp.get_header().has_error() {
            panic!("response {:?} has error", resp);
        }
        assert_eq!(resp.get_responses().len(), 1);
        assert_eq!(resp.get_responses()[0].get_cmd_type(), CmdType::Delete);
    }

    pub fn get_region_epoch(&self, region_id: u64) -> RegionEpoch {
        self.pd_client
            .get_region_by_id(region_id)
            .unwrap()
            .get_region_epoch()
            .clone()
    }

    pub fn change_peer(&mut self,
                       region_id: u64,
                       change_type: ConfChangeType,
                       peer: metapb::Peer) {
        let epoch = self.get_region_epoch(region_id);
        let change_peer =
            new_admin_request(region_id, &epoch, new_change_peer_cmd(change_type, peer));
        let resp = self.call_command_on_leader(change_peer, Duration::from_secs(3))
            .unwrap();
        assert!(resp.get_admin_response().get_cmd_type() == AdminCmdType::ChangePeer,
                format!("{:?}", resp));

        let region = resp.get_admin_response().get_change_peer().get_region();
        self.pd_client.change_peer(region.clone()).unwrap();
    }

    pub fn split_region(&mut self, region_id: u64, split_key: Option<Vec<u8>>) {
        let new_region_id = self.pd_client.alloc_id().unwrap();
        let region = self.pd_client.get_region_by_id(region_id).unwrap();
        let peer_count = region.get_peers().len();
        let mut peer_ids: Vec<u64> = vec![];
        for _ in 0..peer_count {
            let peer_id = self.pd_client.alloc_id().unwrap();
            peer_ids.push(peer_id);
        }

        // TODO: use region instead of region_id
        let split = new_admin_request(region_id,
                                      region.get_region_epoch(),
                                      new_split_region_cmd(split_key, new_region_id, peer_ids));
        let resp = self.call_command_on_leader(split, Duration::from_secs(3)).unwrap();

        assert_eq!(resp.get_admin_response().get_cmd_type(),
                   AdminCmdType::Split);

        let left = resp.get_admin_response().get_split().get_left();
        let right = resp.get_admin_response().get_split().get_right();

        self.pd_client.split_region(left.clone(), right.clone()).unwrap();
    }

    pub fn region_detail(&mut self, region_id: u64, peer_id: u64) -> RegionDetailResponse {
        let status_cmd = new_region_detail_cmd();
        let peer = new_peer(peer_id, peer_id);
        let req = new_status_request(region_id, peer, status_cmd);
        let resp = self.call_command(req, Duration::from_secs(3));
        assert!(resp.is_ok(), format!("{:?}", resp));

        let mut resp = resp.unwrap();
        assert!(resp.has_status_response());
        let mut status_resp = resp.take_status_response();
        assert_eq!(status_resp.get_cmd_type(), StatusCmdType::RegionDetail);
        assert!(status_resp.has_region_detail());
        status_resp.take_region_detail()
    }

    pub fn hook_transport<F: FilterFactory>(&self, factory: F) {
        let sim = self.sim.wl();
        for node_id in sim.get_node_ids() {
            let filter = factory.generate(node_id);
            sim.hook_transport(node_id, filter);
        }
    }


    pub fn transfer_leader(&mut self, region_id: u64, leader: metapb::Peer) {
        let epoch = self.get_region_epoch(region_id);
        let transfer_leader = new_admin_request(region_id, &epoch, new_transfer_leader_cmd(leader));
        let resp = self.call_command_on_leader(transfer_leader, Duration::from_secs(3))
            .unwrap();
        assert!(resp.get_admin_response().get_cmd_type() == AdminCmdType::TransferLeader,
                format!("{:?}", resp));
    }

    pub fn reset_transport_hooks(&mut self) {
        let sim = self.sim.wl();
        for node_id in sim.get_node_ids() {
            sim.hook_transport(node_id, vec![]);
        }
    }

    // it's so common that we provide an API for it
    pub fn partition(&self, s1: Vec<u64>, s2: Vec<u64>) {
        self.hook_transport(Partition::new(s1, s2));
    }
}

impl<T: Simulator> Drop for Cluster<T> {
    fn drop(&mut self) {
        self.shutdown();
    }
}
