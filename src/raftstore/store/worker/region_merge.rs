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

use std::thread;
use std::time::Duration;
use std::fmt::{self, Formatter, Display};
use std::sync::Arc;

use uuid::Uuid;

use kvproto::metapb::{Region, Peer};
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse, AdminRequest, AdminCmdType};
use kvproto::msgpb::{Message, MessageType};

use pd::PdClient;
use util::worker::{Scheduler, Runnable};

use util::transport::SendCh;
use raftstore::store::{Msg, Client};
use raftstore::store::util::ensure_schedule;
use raftstore::Result;

const RETRY_TIMES: isize = 3;

const SEND_STORE_MESSAGE_RETRY_TIME_MS: u64 = 50;
const GET_REGION_FROM_PD_RETRY_TIME_MS: u64 = 50;

#[derive(Clone)]
pub enum Task {
    SuspendRegion {
        // the region to be suspended
        from_region: Region,
        // a hint to start raft rpc with
        from_leader: Peer,
        // local region which controls the region merge procedure
        into_region: Region,
        // local peer which controls the region merge procedure
        into_peer: Peer,
    },
    RetrySuspendRegion {
        // the id of the region to be suspended
        from_region_id: u64,
        // local region which controls the region merge procedure
        into_region: Region,
        // local peer which controls the region merge procedure
        into_peer: Peer,
    },
    CommitMerge {
        // the region to be shutdown
        from_region: Region,
        // a hint to start raft rpc with
        from_leader: Peer,
        // local region which controls the region merge procedure
        into_region: Region,
        // local region which controls the region merge procedure
        into_peer: Peer,
    },
    ShutdownRegion {
        // the region to be shutdown
        region: Region,
        // a hint to start raft rpc with
        leader: Peer,
    },
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::SuspendRegion { ref from_region,
                                  ref from_leader,
                                  ref into_region,
                                  ref into_peer } => {
                write!(f,
                       "suspend region {:?}, leader {:?}, local region {:?}, peer {:?}",
                       from_region,
                       from_leader,
                       into_region,
                       into_peer)
            }
            Task::RetrySuspendRegion { ref from_region_id, ref into_region, ref into_peer } => {
                write!(f,
                       "retry to suspend region id {}, local region {:?}, peer {:?}",
                       from_region_id,
                       into_region,
                       into_peer)
            }
            Task::CommitMerge { ref from_region,
                                ref from_leader,
                                ref into_region,
                                ref into_peer } => {
                write!(f,
                       "commit region merge for region {:?}, leader {:?}, local region {:?}, peer \
                        {:?}",
                       from_region,
                       from_leader,
                       into_region,
                       into_peer)
            }
            Task::ShutdownRegion { ref region, ref leader } => {
                write!(f, "shutdown region {:?}, leader {:?}", region, leader)
            }
        }
    }
}


pub struct Runner<T: PdClient, S: Client> {
    scheduler: Scheduler<Task>,
    client: S,
    pd_client: Arc<T>,
    ch: SendCh<Msg>,
}

impl<T: PdClient, S: Client> Runner<T, S> {
    pub fn new(scheduler: Scheduler<Task>,
               client: S,
               pd_client: Arc<T>,
               ch: SendCh<Msg>)
               -> Runner<T, S> {
        Runner {
            scheduler: scheduler,
            client: client,
            pd_client: pd_client,
            ch: ch,
        }
    }

    fn send_admin_request(&self, mut region: Region, peer: Peer, request: AdminRequest) {
        let region_id = region.get_id();
        let cmd_type = request.get_cmd_type();

        let mut req = RaftCmdRequest::new();
        req.mut_header().set_region_id(region_id);
        req.mut_header().set_region_epoch(region.take_region_epoch());
        req.mut_header().set_peer(peer);
        req.mut_header().set_uuid(Uuid::new_v4().as_bytes().to_vec());

        req.set_admin_request(request);

        for _ in 0..RETRY_TIMES {
            let cb = Box::new(move |_: RaftCmdResponse| -> Result<()> { Ok(()) });
            let cmd = Msg::RaftCmd {
                request: req.clone(),
                callback: cb,
            };
            if let Err(e) = self.ch.try_send(cmd) {
                error!("[region {}] fail to send request {:?} error {:?}",
                       region_id,
                       cmd_type,
                       e);
                thread::sleep(Duration::from_millis(SEND_STORE_MESSAGE_RETRY_TIME_MS));
            } else {
                return;
            }
        }
        error!("[region {}] fail to send admin request to self", region_id);
    }

    fn handle_suspend_region(&self,
                             from_region: Region,
                             from_leader: Peer,
                             into_region: Region,
                             into_peer: Peer) {
        // TODO add impl
        // send a raft command "suspend region" to the specified region/leader
        // if network errors happen, try another peer
        // if get response "not leader", try another peer
        // if get response "leader is another peer", try the given peer
        // if get response "succeed", schedule self a commit merge task
        let store_id = from_leader.get_store_id();
        let message = new_suspend_region_message(&from_region, &from_leader);

        let scheduler = self.scheduler.clone();
        let cb = box move |r| {
            match r {
                Ok(_resp) => {
                    // TODO checking the response message type is correct,
                    // and that the region info in response matches the request
                    //
                    // if resp.get_msg_type() != MessageType::CmdResp {
                    //     error!("response type does not match");
                    // }
                    // let cmd_resp = resp.take_cmd_resp();
                    // if cmd_resp.has_admin_response() {
                    // }
                    // let admin_resp = cmd_resp.take_admin_response();
                    // if admin_resp.has_suspend_region() {
                    // }
                    // let suspend_region_resp = admin_resp.take_suspend_region();
                    // if suspend_region_resp.get_region() != from_region {
                    // }
                    let task = Task::CommitMerge {
                        from_region: from_region,
                        from_leader: from_leader,
                        into_region: into_region,
                        into_peer: into_peer,
                    };
                    ensure_schedule(scheduler, task);
                }
                Err(e) => {
                    error!("failed to send message to store id {}, error {:?}",
                           store_id,
                           e);
                    // TODO what are all the possible errors returned here?
                    // Try another peer in the specified region
                    let next_peer = next_peer(&from_region, from_leader);
                    let task = Task::SuspendRegion {
                        from_region: from_region,
                        from_leader: next_peer,
                        into_region: into_region,
                        into_peer: into_peer,
                    };
                    ensure_schedule(scheduler, task);
                }
            }
        };

        self.client.send(store_id, message, cb);
    }

    fn handle_retry_suspend_region(&self,
                                   from_region_id: u64,
                                   into_region: Region,
                                   into_peer: Peer) {
        for _ in 0..RETRY_TIMES {
            // try to get the specified region info from PD
            match self.pd_client.get_region_by_id(from_region_id) {
                Ok((region, leader)) => {
                    let leader = match leader {
                        Some(p) => p,
                        None => {
                            if region.get_peers().len() == 0 {
                                panic!("[region {}] region {} should not has no peers, region \
                                        {:?}",
                                       into_region.get_id(),
                                       region.get_id(),
                                       region);
                            }
                            // Simply choose the first peer in region as the leader.
                            region.get_peers()[0].clone()
                        }
                    };
                    self.handle_suspend_region(region, leader, into_region, into_peer);
                    return;
                }
                Err(e) => {
                    error!("[region {}] failed to get region by id {}, error {:?}",
                           into_region.get_id(),
                           from_region_id,
                           e);
                    // TODO abort this task if err == region not found
                    thread::sleep(Duration::from_millis(GET_REGION_FROM_PD_RETRY_TIME_MS));
                }
            }
        }
        error!("[region {}] failed to retry to suspend region {}",
               into_region.get_id(),
               from_region_id);
    }

    fn handle_commit_merge(&self,
                           from_region: Region,
                           from_leader: Peer,
                           into_region: Region,
                           into_peer: Peer) {
        // Send a raft command "commit merge" to the specified local region/leader peer.
        // Once this command is sent, it will enter raft log and be committed as long as
        // the local peer is still leader of the corresponding raft group.
        // If there is a leader change in the raft group, this command could be lost and committed,
        // In either case, the new leader will detect it's region merge state as 'Merging',
        // and retry the "suspend region, commit merge" procedure once again.
        // In summary, it's not neccessary to wait for the response of "commit merge" command
        // here.
        let req = new_commit_merge_request(from_region, from_leader, into_region.clone());
        self.send_admin_request(into_region, into_peer, req)
    }

    fn handle_shutdown_region(&self, region: Region, leader: Peer) {
        // TODO add impl
        // send a raft command "shutdown region" to the specified region/leader
        // if get response "not leader", try another peer
        // if get response "leader is another peer", try the given peer
        // if network errors happen, abort this task.
        // PD will tell the specified region to shutdown in heartbeat communication

        let store_id = leader.get_store_id();
        let message = new_shutdown_region_message(&region, &leader);

        let cb = box move |r| {
            match r {
                Ok(_resp) => {
                    // TODO checking the response message is correct,
                    // and that the region info in response matches the request

                    // Succeed to shutdown the specified region.
                }
                Err(e) => {
                    error!("failed to shutdown region {:?}, peer {:?}, error {:?}",
                           region,
                           leader,
                           e);
                    // Abort this task if error happens.
                    // Even though the control region fails to tell the merged region
                    // to shutdown, the merged region would communication with PD
                    // in heartbeat, and then know itself should be shutdown.

                    // try another peer when it's apropiate
                    // let next_peer = next_peer(&region, last_peer);
                    // let task = Task::ShutdownRegion {
                    //     region: region,
                    //     leader: next_peer,
                    // };
                    // ensure_schedule(scheduler, task);
                }
            }
        };

        self.client.send(store_id, message, cb);
    }
}

impl<T: PdClient, S: Client> Runnable<Task> for Runner<T, S> {
    fn run(&mut self, task: Task) {
        debug!("executing task {}", task);

        match task {
            Task::SuspendRegion { from_region, from_leader, into_region, into_peer } => {
                self.handle_suspend_region(from_region, from_leader, into_region, into_peer)
            }
            Task::RetrySuspendRegion { from_region_id, into_region, into_peer } => {
                self.handle_retry_suspend_region(from_region_id, into_region, into_peer)
            }
            Task::CommitMerge { from_region, from_leader, into_region, into_peer } => {
                self.handle_commit_merge(from_region, from_leader, into_region, into_peer)
            }
            Task::ShutdownRegion { region, leader } => self.handle_shutdown_region(region, leader),
        };
    }
}

fn next_peer(region: &Region, last_peer: Peer) -> Peer {
    let last_peer_id = last_peer.get_id();
    let peers = region.get_peers();
    if let Some(index) = peers.iter().position(|p| p.get_id() == last_peer_id) {
        let next_index = (index + 1) % peers.len();
        return peers[next_index].clone();
    }
    if peers.len() == 0 {
        return last_peer;
    }
    peers[0].clone()
}

fn new_commit_merge_request(from_region: Region,
                            from_leader: Peer,
                            into_region: Region)
                            -> AdminRequest {
    let mut req = AdminRequest::new();
    req.set_cmd_type(AdminCmdType::CommitMerge);
    req.mut_commit_merge().set_from_region(from_region);
    req.mut_commit_merge().set_from_leader(from_leader);
    req.mut_commit_merge().set_into_region(into_region);
    req
}

fn new_raft_cmd_message(req: RaftCmdRequest) -> Message {
    let mut message = Message::new();
    message.set_msg_type(MessageType::Cmd);
    message.set_cmd_req(req);
    message
}

fn new_suspend_region_message(region: &Region, peer: &Peer) -> Message {
    let mut admin_req = AdminRequest::new();
    admin_req.set_cmd_type(AdminCmdType::SuspendRegion);
    admin_req.mut_suspend_region().set_region(region.clone());

    let mut cmd_req = RaftCmdRequest::new();
    cmd_req.mut_header().set_region_id(region.get_id());
    cmd_req.mut_header().set_region_epoch(region.get_region_epoch().clone());
    cmd_req.mut_header().set_peer(peer.clone());
    cmd_req.mut_header().set_uuid(Uuid::new_v4().as_bytes().to_vec());
    cmd_req.set_admin_request(admin_req);

    new_raft_cmd_message(cmd_req)
}

fn new_shutdown_region_message(region: &Region, peer: &Peer) -> Message {
    let mut admin_req = AdminRequest::new();
    admin_req.set_cmd_type(AdminCmdType::ShutdownRegion);
    admin_req.mut_shutdown_region().set_region(region.clone());

    let mut cmd_req = RaftCmdRequest::new();
    cmd_req.mut_header().set_region_id(region.get_id());
    cmd_req.mut_header().set_region_epoch(region.get_region_epoch().clone());
    cmd_req.mut_header().set_peer(peer.clone());
    cmd_req.mut_header().set_uuid(Uuid::new_v4().as_bytes().to_vec());
    cmd_req.set_admin_request(admin_req);

    new_raft_cmd_message(cmd_req)
}
