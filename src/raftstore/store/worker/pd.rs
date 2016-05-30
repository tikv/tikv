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

use std::sync::Arc;
use std::fmt::{self, Formatter, Display};

use uuid::Uuid;

use kvproto::metapb;
use kvproto::raftpb::ConfChangeType;
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse, AdminRequest, AdminCmdType};

use util::worker::Runnable;
use util::escape;
use pd::PdClient;
use raftstore::store::{SendCh, Msg};
use raftstore::Result;

// Use an asynchronous thread to tell pd something.
pub enum Task {
    AskSplit {
        region: metapb::Region,
        split_key: Vec<u8>,
        peer: metapb::Peer,
    },
    Heartbeat {
        region: metapb::Region,
        peer: metapb::Peer,
    },
}


impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::AskSplit { ref region, ref split_key, .. } => {
                write!(f,
                       "ask split region {} with key {}",
                       region.get_id(),
                       escape(&split_key))
            }
            Task::Heartbeat { ref region, ref peer } => {
                write!(f,
                       "heartbeat for region {:?}, leader {}",
                       region,
                       peer.get_id())
            }
        }
    }
}

pub struct Runner<T: PdClient> {
    pd_client: Arc<T>,
    ch: SendCh,
}

impl<T: PdClient> Runner<T> {
    pub fn new(pd_client: Arc<T>, ch: SendCh) -> Runner<T> {
        Runner {
            pd_client: pd_client,
            ch: ch,
        }
    }

    fn send_admin_request(&self,
                          mut region: metapb::Region,
                          peer: metapb::Peer,
                          request: AdminRequest) {
        let region_id = region.get_id();
        let cmd_type = request.get_cmd_type();

        let mut req = RaftCmdRequest::new();
        req.mut_header().set_region_id(region_id);
        req.mut_header().set_region_epoch(region.take_region_epoch());
        req.mut_header().set_peer(peer);
        req.mut_header().set_uuid(Uuid::new_v4().as_bytes().to_vec());

        req.set_admin_request(request);

        let cb = Box::new(move |_: RaftCmdResponse| -> Result<()> { Ok(()) });

        if let Err(e) = self.ch.send(Msg::RaftCmd {
            request: req,
            callback: cb,
        }) {
            error!("send {:?} request to region {} err {:?}",
                   cmd_type,
                   region_id,
                   e);
        }
    }
}

impl<T: PdClient> Runnable<Task> for Runner<T> {
    fn run(&mut self, task: Task) {
        debug!("executing task {}", task);

        match task {
            Task::AskSplit { region, split_key, peer } => {
                if let Ok(mut resp) = self.pd_client.ask_split(region.clone()) {
                    info!("try to split with new region id {} for region {:?}",
                          resp.get_new_region_id(),
                          region);
                    let req = new_split_region_request(split_key,
                                                       resp.get_new_region_id(),
                                                       resp.take_new_peer_ids());
                    self.send_admin_request(region, peer, req);

                }
            }
            Task::Heartbeat { region, peer } => {
                // Now we use put region protocol for heartbeat.
                if let Ok(mut resp) = self.pd_client
                    .region_heartbeat(region.clone(), peer.clone()) {
                    if resp.has_change_peer() {
                        let mut change_peer = resp.take_change_peer();
                        info!("try to change peer {:?} {:?} for region {:?}",
                              change_peer.get_change_type(),
                              change_peer.get_peer(),
                              region);
                        let req = new_change_peer_request(change_peer.get_change_type(),
                                                          change_peer.take_peer());
                        self.send_admin_request(region, peer, req);
                    }
                }
            }
        };
    }
}

fn new_change_peer_request(change_type: ConfChangeType, peer: metapb::Peer) -> AdminRequest {
    let mut req = AdminRequest::new();
    req.set_cmd_type(AdminCmdType::ChangePeer);
    req.mut_change_peer().set_change_type(change_type);
    req.mut_change_peer().set_peer(peer);
    req
}

fn new_split_region_request(split_key: Vec<u8>,
                            new_region_id: u64,
                            peer_ids: Vec<u64>)
                            -> AdminRequest {
    let mut req = AdminRequest::new();
    req.set_cmd_type(AdminCmdType::Split);
    req.mut_split().set_split_key(split_key);
    req.mut_split().set_new_region_id(new_region_id);
    req.mut_split().set_new_peer_ids(peer_ids);
    req
}
