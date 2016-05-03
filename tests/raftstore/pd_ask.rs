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

use std::sync::{Arc, RwLock, mpsc};
use std::thread;
use std::time::Duration;

use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse};
use kvproto::pdpb::{self, CommandType};
use kvproto::raftpb::ConfChangeType;

use tikv::pd::PdClient;
use tikv::util::{escape, HandyRwLock};

use super::pd::TestPdClient;
use super::cluster::Simulator;
use super::util::*;

pub fn run_ask_loop<T>(pd_client: Arc<RwLock<TestPdClient>>,
                       sim: Arc<RwLock<T>>,
                       rx: mpsc::Receiver<pdpb::Request>)
    where T: Simulator + Send + Sync + 'static
{
    let mut h = AskHandler {
        pd_client: pd_client,
        sim: sim,
    };

    thread::spawn(move || {
        loop {
            let req = rx.recv();
            if req.is_err() {
                return;
            }

            let req = req.unwrap();
            match req.get_cmd_type() {
                CommandType::AskChangePeer => h.handle_change_peer(req),
                CommandType::AskSplit => h.handle_split(req),
                _ => {
                    error!("invalid request {:?}, skip it", req);
                }
            }
        }
    });
}

struct AskHandler<T: Simulator> {
    pd_client: Arc<RwLock<TestPdClient>>,
    sim: Arc<RwLock<T>>,
}

impl<T: Simulator> AskHandler<T> {
    fn handle_change_peer(&mut self, req: pdpb::Request) {
        let cluster_id = req.get_header().get_cluster_id();
        let region = req.get_ask_change_peer().get_region();
        let leader_store_id = req.get_ask_change_peer().get_leader_store_id();
        // because region may change at this point, we should use
        // latest region info instead.
        let region = self.pd_client
                         .rl()
                         .get_region_by_id(cluster_id, region.get_id())
                         .unwrap();

        let meta = self.pd_client.rl().get_cluster_meta(cluster_id).unwrap();
        let max_peer_number = meta.get_max_peer_number() as usize;
        let peer_number = region.get_store_ids().len();
        if max_peer_number == peer_number {
            return;
        }

        let (conf_change_type, store_id) = if max_peer_number < peer_number {
            // Find first follower.
            let pos = region.get_store_ids()
                            .iter()
                            .position(|&id| id != leader_store_id)
                            .unwrap();
            (ConfChangeType::RemoveNode, region.get_store_ids()[pos])
        } else {
            // Choose first store which all peers are not in.
            let stores = self.pd_client.rl().get_stores(cluster_id).unwrap();
            let pos = stores.iter().position(|store| {
                let store_id = store.get_id();
                region.get_store_ids().iter().all(|&id| id != store_id)
            });

            if pos.is_none() {
                // find nothing
                return;
            }

            let store = &stores[pos.unwrap()];
            (ConfChangeType::AddNode, store.get_id())
        };

        let change_peer = new_admin_request(region.get_id(),
                                            region.get_region_epoch(),
                                            new_change_peer_cmd(conf_change_type, store_id));
        let resp = self.call_command(change_peer, leader_store_id);
        if resp.is_none() {
            return;
        }
        let resp = resp.unwrap();
        let region = resp.get_admin_response().get_change_peer().get_region();
        self.pd_client.wl().change_peer(cluster_id, region.clone()).unwrap();
    }

    fn call_command(&self, req: RaftCmdRequest, store_id: u64) -> Option<RaftCmdResponse> {
        let req_type = req.get_admin_request().get_cmd_type();
        let resp = self.sim.wl().call_command(store_id, req, Duration::from_secs(3)).unwrap();
        if resp.get_header().has_error() && resp.get_header().get_error().has_not_leader() {
            // ignore not leader error, as the client will retry anyway.
            return None;
        }
        assert!(!resp.get_header().has_error(), format!("{:?}", resp));
        assert_eq!(resp.get_admin_response().get_cmd_type(), req_type);
        Some(resp)
    }

    fn handle_split(&mut self, req: pdpb::Request) {
        let cluster_id = req.get_header().get_cluster_id();
        let region = req.get_ask_split().get_region();
        let leader_store_id = req.get_ask_split().get_leader_store_id();
        let split_key = req.get_ask_split().get_split_key().to_vec();
        let region = self.pd_client
                         .rl()
                         .get_region_by_id(cluster_id, region.get_id())
                         .unwrap();
        if &*split_key <= region.get_start_key() ||
           (!region.get_end_key().is_empty() && &*split_key >= region.get_end_key()) {
            error!("invalid split key {} for region {:?}",
                   escape(&split_key),
                   region);
            return;
        }

        let new_region_id = self.pd_client.wl().alloc_id(0).unwrap();
        let split = new_admin_request(region.get_id(),
                                      region.get_region_epoch(),
                                      new_split_region_cmd(Some(split_key), new_region_id));
        let resp = self.call_command(split, leader_store_id);
        if resp.is_none() {
            return;
        }
        let resp = resp.unwrap();
        let left = resp.get_admin_response().get_split().get_left();
        let right = resp.get_admin_response().get_split().get_right();

        self.pd_client.wl().split_region(cluster_id, left.clone(), right.clone()).unwrap();
    }
}
