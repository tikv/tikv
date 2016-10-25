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

use rocksdb::DB;
use tempdir::TempDir;

use tikv::raftstore::{Result, Error};
use tikv::raftstore::store::*;
use super::util::*;
use kvproto::pdpb;
use kvproto::raft_cmdpb::*;
use kvproto::metapb::{self, RegionEpoch};
use kvproto::raft_serverpb::RaftMessage;
use tikv::pd::PdClient;
use tikv::util::{HandyRwLock, escape, rocksdb};
use tikv::util::transport::SendCh;
use tikv::server::Config as ServerConfig;
use super::pd::TestPdClient;
use tikv::raftstore::store::keys::data_key;
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
    fn call_command_on_node(&self,
                            node_id: u64,
                            request: RaftCmdRequest,
                            timeout: Duration)
                            -> Result<RaftCmdResponse>;
    fn send_raft_msg(&self, msg: RaftMessage) -> Result<()>;
    fn get_snap_dir(&self, node_id: u64) -> String;
    fn get_store_sendch(&self, node_id: u64) -> Option<SendCh<Msg>>;
    fn add_send_filter(&mut self, node_id: u64, filter: SendFilter);
    fn clear_send_filters(&mut self, node_id: u64);
    fn add_recv_filter(&mut self, node_id: u64, filter: RecvFilter);
    fn clear_recv_filters(&mut self, node_id: u64);

    fn call_command(&self, request: RaftCmdRequest, timeout: Duration) -> Result<RaftCmdResponse> {
        let node_id = request.get_header().get_peer().get_store_id();
        self.call_command_on_node(node_id, request, timeout)
    }
}

pub struct Cluster<T: Simulator> {
    pub cfg: ServerConfig,
    leaders: HashMap<u64, metapb::Peer>,
    paths: Vec<TempDir>,
    dbs: Vec<Arc<DB>>,

    // node id -> db engine.
    pub engines: HashMap<u64, Arc<DB>>,

    pub sim: Arc<RwLock<T>>,
    pub pd_client: Arc<TestPdClient>,
}

impl<T: Simulator> Cluster<T> {
    // Create the default Store cluster.
    pub fn new(id: u64,
               count: usize,
               cfs: &[&str],
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

        c.create_engines(count, cfs);

        c
    }

    pub fn id(&self) -> u64 {
        self.cfg.cluster_id
    }

    fn create_engines(&mut self, count: usize, cfs: &[&str]) {
        for _ in 0..count {
            self.paths.push(TempDir::new("test_cluster").unwrap());
        }

        for item in &self.paths {
            self.dbs
                .push(Arc::new(rocksdb::new_engine(item.path().to_str().unwrap(), cfs).unwrap()));
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
            let node_ids = self.engines.keys().cloned().collect::<Vec<_>>();
            for node_id in node_ids {
                self.run_node(node_id);
            }
        }
    }

    // Bootstrap the store with fixed ID (like 1, 2, .. 5) and
    // initialize first region in all stores, then start the cluster.
    pub fn run(&mut self) {
        self.bootstrap_region().unwrap();
        self.start();
    }

    // Bootstrap the store with fixed ID (like 1, 2, .. 5) and
    // initialize first region in store 1, then start the cluster.
    pub fn run_conf_change(&mut self) -> u64 {
        let region_id = self.bootstrap_conf_change();
        self.start();
        region_id
    }

    pub fn run_node(&mut self, node_id: u64) {
        debug!("starting node {}", node_id);
        let engine = self.engines.get(&node_id).unwrap();
        self.sim.wl().run_node(node_id, self.cfg.clone(), engine.clone());
        debug!("node {} started", node_id);
    }

    pub fn stop_node(&mut self, node_id: u64) {
        debug!("stopping node {}", node_id);
        self.sim.wl().stop_node(node_id);
        debug!("node {} stopped", node_id);
    }

    pub fn get_engine(&self, node_id: u64) -> Arc<DB> {
        self.engines.get(&node_id).unwrap().clone()
    }

    pub fn send_raft_msg(&self, msg: RaftMessage) -> Result<()> {
        self.sim.rl().send_raft_msg(msg)
    }

    pub fn call_command_on_node(&self,
                                node_id: u64,
                                request: RaftCmdRequest,
                                timeout: Duration)
                                -> Result<RaftCmdResponse> {
        match self.sim.rl().call_command_on_node(node_id, request.clone(), timeout) {
            Err(e) => {
                warn!("failed to call command {:?}: {:?}", request, e);
                Err(e)
            }
            a => a,
        }
    }

    pub fn call_command(&self,
                        request: RaftCmdRequest,
                        timeout: Duration)
                        -> Result<RaftCmdResponse> {
        match self.sim.rl().call_command(request.clone(), timeout) {
            Err(e) => {
                warn!("failed to call command {:?}: {:?}", request, e);
                Err(e)
            }
            a => a,
        }
    }

    pub fn call_command_on_leader(&mut self,
                                  mut request: RaftCmdRequest,
                                  timeout: Duration)
                                  -> Result<RaftCmdResponse> {
        let mut retry_cnt = 0;
        let region_id = request.get_header().get_region_id();
        loop {
            let leader = match self.leader_of_region(region_id) {
                None => return Err(box_err!("can't get leader of region {}", region_id)),
                Some(l) => l,
            };
            request.mut_header().set_peer(leader);
            let resp = match self.call_command(request.clone(), timeout) {
                e @ Err(_) => return e,
                Ok(resp) => resp,
            };
            if self.refresh_leader_if_needed(&resp, region_id) && retry_cnt < 10 {
                retry_cnt += 1;
                warn!("{:?} is no longer leader, let's retry",
                      request.get_header().get_peer());
                continue;
            }
            return Ok(resp);
        }
    }

    fn valid_leader_id(&self, region_id: u64, leader_id: u64) -> bool {
        let store_ids = match self.store_ids_of_region(region_id) {
            None => return false,
            Some(ids) => ids,
        };
        let node_ids = self.sim.rl().get_node_ids();
        store_ids.contains(&leader_id) && node_ids.contains(&leader_id)
    }

    fn store_ids_of_region(&self, region_id: u64) -> Option<Vec<u64>> {
        self.pd_client
            .get_region_by_id(region_id)
            .unwrap()
            .map(|region| region.get_peers().into_iter().map(|p| p.get_store_id()).collect())
    }

    fn query_leader(&self, store_id: u64, region_id: u64) -> Option<metapb::Peer> {
        // To get region leader, we don't care real peer id, so use 0 instead.
        let peer = new_peer(store_id, 0);
        let find_leader = new_status_request(region_id, peer, new_region_leader_cmd());
        let mut resp = self.call_command(find_leader, Duration::from_secs(5)).unwrap();
        let mut region_leader = resp.take_status_response().take_region_leader();
        // NOTE: node id can't be 0.
        if self.valid_leader_id(region_id, region_leader.get_leader().get_store_id()) {
            Some(region_leader.take_leader())
        } else {
            None
        }
    }

    pub fn leader_of_region(&mut self, region_id: u64) -> Option<metapb::Peer> {
        let store_ids = match self.store_ids_of_region(region_id) {
            None => return None,
            Some(ids) => ids,
        };
        if let Some(l) = self.leaders.get(&region_id) {
            // leader may be stopped in some tests.
            if self.valid_leader_id(region_id, l.get_store_id()) {
                return Some(l.clone());
            }
        }
        self.reset_leader_of_region(region_id);
        let mut leader = None;
        let mut retry_cnt = 500;

        let node_ids = self.sim.rl().get_node_ids();
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
    fn bootstrap_region(&mut self) -> Result<()> {
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
    fn bootstrap_conf_change(&mut self) -> u64 {
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
        debug!("about to shutdown cluster");
        let keys = self.sim.rl().get_node_ids();
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
                   read_quorum: bool,
                   timeout: Duration)
                   -> RaftCmdResponse {
        for _ in 0..20 {
            let mut region = self.get_region(key);
            let region_id = region.get_id();
            let req = new_request(region_id,
                                  region.take_region_epoch(),
                                  reqs.clone(),
                                  read_quorum);
            let result = self.call_command_on_leader(req, timeout);

            if let Err(Error::Timeout(_)) = result {
                warn!("call command timeout, let's retry");
                sleep_ms(100);
                continue;
            }

            let resp = result.unwrap();
            if resp.get_header().get_error().has_stale_epoch() {
                warn!("seems split, let's retry");
                sleep_ms(100);
                continue;
            }
            return resp;
        }
        panic!("request failed after retry for 20 times");
    }

    pub fn get_region(&self, key: &[u8]) -> metapb::Region {
        for _ in 0..100 {
            match self.pd_client.get_region(key) {
                Ok(region) => return region,
                Err(_) => {
                    // We may meet range gap after split, so here we will
                    // retry to get the region again.
                    sleep_ms(20);
                    continue;
                }
            };
        }

        panic!("find no region for {:?}", escape(key));
    }

    pub fn get_region_id(&self, key: &[u8]) -> u64 {
        self.get_region(key).get_id()
    }

    pub fn get_down_peers(&self) -> HashMap<u64, pdpb::PeerStats> {
        self.pd_client.get_down_peers()
    }

    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        self.get_impl(key, false)
    }

    pub fn must_get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        self.get_impl(key, true)
    }

    fn get_impl(&mut self, key: &[u8], read_quorum: bool) -> Option<Vec<u8>> {
        let mut resp = self.request(key,
                                    vec![new_get_cmd(key)],
                                    read_quorum,
                                    Duration::from_secs(5));
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
        self.must_put_cf("default", key, value);
    }

    pub fn must_put_cf(&mut self, cf: &str, key: &[u8], value: &[u8]) {
        let resp = self.request(key,
                                vec![new_put_cf_cmd(cf, key, value)],
                                false,
                                Duration::from_secs(5));
        if resp.get_header().has_error() {
            panic!("response {:?} has error", resp);
        }
        assert_eq!(resp.get_responses().len(), 1);
        assert_eq!(resp.get_responses()[0].get_cmd_type(), CmdType::Put);
    }

    pub fn must_delete(&mut self, key: &[u8]) {
        self.must_delete_cf("default", key)
    }

    pub fn must_delete_cf(&mut self, cf: &str, key: &[u8]) {
        let resp = self.request(key,
                                vec![new_delete_cmd(cf, key)],
                                false,
                                Duration::from_secs(5));
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
            .unwrap()
            .take_region_epoch()
    }

    pub fn region_detail(&mut self, region_id: u64, peer_id: u64) -> RegionDetailResponse {
        let status_cmd = new_region_detail_cmd();
        let peer = new_peer(peer_id, peer_id);
        let req = new_status_request(region_id, peer, status_cmd);
        let resp = self.call_command(req, Duration::from_secs(5));
        assert!(resp.is_ok(), format!("{:?}", resp));

        let mut resp = resp.unwrap();
        assert!(resp.has_status_response());
        let mut status_resp = resp.take_status_response();
        assert_eq!(status_resp.get_cmd_type(), StatusCmdType::RegionDetail);
        assert!(status_resp.has_region_detail());
        status_resp.take_region_detail()
    }

    pub fn add_send_filter<F: FilterFactory>(&self, factory: F) {
        let mut sim = self.sim.wl();
        for node_id in sim.get_node_ids() {
            for filter in factory.generate(node_id) {
                sim.add_send_filter(node_id, filter);
            }
        }
    }

    pub fn transfer_leader(&mut self, region_id: u64, leader: metapb::Peer) {
        let epoch = self.get_region_epoch(region_id);
        let transfer_leader = new_admin_request(region_id, &epoch, new_transfer_leader_cmd(leader));
        let resp = self.call_command_on_leader(transfer_leader, Duration::from_secs(5))
            .unwrap();
        assert!(resp.get_admin_response().get_cmd_type() == AdminCmdType::TransferLeader,
                format!("{:?}", resp));
    }

    pub fn must_transfer_leader(&mut self, region_id: u64, leader: metapb::Peer) {
        let mut try_cnt = 0;
        loop {
            self.reset_leader_of_region(region_id);
            if self.leader_of_region(region_id).as_ref().unwrap() == &leader {
                return;
            }
            if try_cnt > 250 {
                panic!("failed to transfer leader to [{}] {:?}", region_id, leader);
            }
            if try_cnt % 50 == 0 {
                self.transfer_leader(region_id, leader.clone());
            }
            try_cnt += 1;
        }
    }

    pub fn get_snap_dir(&self, node_id: u64) -> String {
        self.sim.rl().get_snap_dir(node_id)
    }

    pub fn clear_send_filters(&mut self) {
        let mut sim = self.sim.wl();
        for node_id in sim.get_node_ids() {
            sim.clear_send_filters(node_id);
        }
    }

    pub fn ask_split(&mut self, region: &metapb::Region, split_key: &[u8]) {
        // Now we can't control split easily in pd, so here we use store send channel
        // directly to send the AskSplit request.
        let leader = self.leader_of_region(region.get_id()).unwrap();
        let ch = self.sim.rl().get_store_sendch(leader.get_store_id()).unwrap();
        ch.try_send(Msg::SplitCheckResult {
                region_id: region.get_id(),
                epoch: region.get_region_epoch().clone(),
                split_key: data_key(split_key),
            })
            .unwrap();
    }

    pub fn must_split(&mut self, region: &metapb::Region, split_key: &[u8]) {
        let mut try_cnt = 0;
        let split_count = self.pd_client.get_split_count();
        loop {
            // In case ask split message is ignored, we should retry.
            if try_cnt % 50 == 0 {
                self.reset_leader_of_region(region.get_id());
                self.ask_split(region, split_key);
            }

            if self.pd_client.check_split(region, split_key) &&
               self.pd_client.get_split_count() > split_count {
                return;
            }

            if try_cnt > 250 {
                panic!("region {:?} has not been split by {:?}",
                       region,
                       escape(split_key));
            }
            try_cnt += 1;
            sleep_ms(20);
        }
    }

    /// Make sure region exists on that store.
    pub fn must_region_exist(&mut self, region_id: u64, store_id: u64) {
        let mut try_cnt = 0;
        loop {
            let find_leader =
                new_status_request(region_id, new_peer(store_id, 0), new_region_leader_cmd());
            let resp = self.call_command(find_leader, Duration::from_secs(5)).unwrap();

            if !is_error_response(&resp) {
                return;
            }

            if try_cnt > 250 {
                panic!("region {} doesn't exist on store {} after {} tries",
                       region_id,
                       store_id,
                       try_cnt);
            }
            try_cnt += 1;
            sleep_ms(20);
        }

    }

    // it's so common that we provide an API for it
    pub fn partition(&self, s1: Vec<u64>, s2: Vec<u64>) {
        self.add_send_filter(PartitionFilterFactory::new(s1, s2));
    }
}

impl<T: Simulator> Drop for Cluster<T> {
    fn drop(&mut self) {
        self.shutdown();
    }
}
