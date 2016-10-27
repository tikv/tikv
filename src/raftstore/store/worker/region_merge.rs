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
use std::net::SocketAddr;
use std::sync::Arc;

use uuid::Uuid;

use kvproto::metapb::{Region, Peer};
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse, AdminRequest, AdminCmdType};

use pd::PdClient;
use server::ResolveTask;
use util::worker::{Scheduler, Runnable};

use util::transport::SendCh;
use raftstore::store::Msg;
use raftstore::store::util::ensure_schedule;
use raftstore::Result;

use super::client::{Client, KVClient};

const RESOLVE_ADDRESS_RETRY_TIME_MILLIS: u64 = 50;
const GET_REGION_FROM_PD_RETRY_TIME_MILLIS: u64 = 50;

/// Client to communicate with tikv region for region merge.
/// It sends Raft command requests to the specified tikv region and
/// waits for the corresponding responses.
pub trait RegionMergeClient {
    /// `suspend_region` suspends the region which is about to be merged.
    fn send_suspend_region(&self, region: Region, leader: Peer) -> Result<()>;
    /// `shutdown_region` shutdowns a region which is merged before.
    fn send_shutdown_region(&self, region: Region, leader: Peer) -> Result<()>;
}

#[derive(Debug, Clone)]
enum TaskType {
    SuspendRegion,
    ShutdownRegion,
}

#[derive(Clone)]
pub struct TaskContext {
    task_type: TaskType,
    from_region: Region,
    from_peer: Peer,
    into_region: Region,
    into_peer: Peer,
    address: SocketAddr,
}

impl Display for TaskContext {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f,
               "task context, type {:?}, from region {:?}, from peer {:?}, into region: {:?}, \
                into peer: {:?}, address: {}",
               self.task_type,
               self.from_region,
               self.from_peer,
               self.into_region,
               self.into_peer,
               self.address)
    }
}

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
    AfterResolve {
        // a struct that passes the task context between callbacks
        context: TaskContext,
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
            Task::AfterResolve { ref context } => write!(f, "after resolve, context {}", context),
        }
    }
}


impl RegionMergeClient for KVClient {
    fn send_suspend_region(&self, region: Region, peer: Peer) -> Result<()> {
        let mut req = RaftCmdRequest::new();
        req.mut_header().set_region_id(region.get_id());
        req.mut_header().set_peer(peer);
        req.mut_header().set_uuid(Uuid::new_v4().as_bytes().to_vec());

        let mut admin_req = AdminRequest::new();
        admin_req.set_cmd_type(AdminCmdType::SuspendRegion);
        admin_req.mut_suspend_region().set_region(region);
        req.set_admin_request(admin_req);

        let _ = try!(self.send_cmd(&req));
        Ok(())
    }

    fn send_shutdown_region(&self, region: Region, peer: Peer) -> Result<()> {
        let mut req = RaftCmdRequest::new();
        req.mut_header().set_region_id(region.get_id());
        req.mut_header().set_peer(peer);
        req.mut_header().set_uuid(Uuid::new_v4().as_bytes().to_vec());

        let mut admin_req = AdminRequest::new();
        admin_req.set_cmd_type(AdminCmdType::ShutdownRegion);
        admin_req.mut_shutdown_region().set_region(region);
        req.set_admin_request(admin_req);

        let _ = try!(self.send_cmd(&req));
        Ok(())
    }
}


pub struct Runner<T: PdClient> {
    scheduler: Scheduler<Task>,
    resolve_scheduler: Scheduler<ResolveTask>,
    pd_client: Arc<T>,
    ch: SendCh<Msg>,
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

impl<T: PdClient> Runner<T> {
    pub fn new(scheduler: Scheduler<Task>,
               resolve_scheduler: Scheduler<ResolveTask>,
               pd_client: Arc<T>,
               ch: SendCh<Msg>)
               -> Runner<T> {
        Runner {
            scheduler: scheduler,
            resolve_scheduler: resolve_scheduler,
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

        loop {
            let cb = Box::new(move |_: RaftCmdResponse| -> Result<()> { Ok(()) });
            let cmd = Msg::RaftCmd {
                request: req.clone(),
                callback: cb,
            };
            if let Err(e) = self.ch.try_send(cmd) {
                error!("[region {}] send {:?} request err {:?}",
                       region_id,
                       cmd_type,
                       e);
            } else {
                return;
            }
        }
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
        // if get response "succeed", send self a commit merge task

        loop {
            let store_id = from_leader.get_store_id();
            let scheduler = self.scheduler.clone();
            let from_region = from_region.clone();
            let last_peer = from_leader.clone();
            let into_region = into_region.clone();
            let into_peer = into_peer.clone();
            let cb = box move |r| {
                match r {
                    Ok(addr) => {
                        let task = Task::AfterResolve {
                            context: TaskContext {
                                task_type: TaskType::SuspendRegion,
                                from_region: from_region,
                                from_peer: last_peer,
                                into_region: into_region,
                                into_peer: into_peer,
                                address: addr,
                            },
                        };
                        ensure_schedule(scheduler, task)
                    }
                    Err(e) => {
                        error!("failed to resolve store id {}, error {:?}", store_id, e);
                        // retry another peer
                        let next_peer = next_peer(&from_region, last_peer);
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
            if let Err(e) = self.resolve_scheduler.schedule(ResolveTask::new(store_id, cb)) {
                error!("failed to schedule resolve task with store id {}, error {:?}",
                       store_id,
                       e);
                thread::sleep(Duration::from_millis(RESOLVE_ADDRESS_RETRY_TIME_MILLIS));
            } else {
                return;
            }
        }
    }

    fn handle_retry_suspend_region(&self,
                                   from_region_id: u64,
                                   into_region: Region,
                                   into_peer: Peer) {
        loop {
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
                    thread::sleep(Duration::from_millis(GET_REGION_FROM_PD_RETRY_TIME_MILLIS));
                }
            }
        }
    }

    fn handle_commit_merge(&self,
                           from_region: Region,
                           from_leader: Peer,
                           into_region: Region,
                           into_peer: Peer) {
        // TODO add impl
        // send a raft cmd "commit merge" to the specified peer
        // if it times out on waiting for response, retry
        // make sure get one response "ok"
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

        loop {
            let store_id = leader.get_store_id();
            let scheduler = self.scheduler.clone();
            let region = region.clone();
            let last_peer = leader.clone();
            let cb = box move |r| {
                match r {
                    Ok(addr) => {
                        let task = Task::AfterResolve {
                            context: TaskContext {
                                task_type: TaskType::ShutdownRegion,
                                from_region: region,
                                from_peer: last_peer,
                                address: addr,
                                // TODO better way to initialize these dummy fields
                                into_region: Region::new(),
                                into_peer: Peer::new(),
                            },
                        };
                        ensure_schedule(scheduler, task);
                    }
                    Err(e) => {
                        error!("failed to resolve store id {}, error {:?}", store_id, e);
                        // retry another peer
                        let next_peer = next_peer(&region, last_peer);
                        let task = Task::ShutdownRegion {
                            region: region,
                            leader: next_peer,
                        };
                        ensure_schedule(scheduler, task);
                    }
                }
            };
            if let Err(e) = self.resolve_scheduler.schedule(ResolveTask::new(store_id, cb)) {
                error!("failed to schedule resolve task with store id {}, error {:?}",
                       store_id,
                       e);
                thread::sleep(Duration::from_millis(RESOLVE_ADDRESS_RETRY_TIME_MILLIS))
            } else {
                return;
            }

        }
    }

    fn handle_after_resolve(&self, context: TaskContext) {
        match context.task_type {
            TaskType::SuspendRegion => {
                let client = KVClient::new(context.address);
                match client.send_suspend_region(context.from_region.clone(),
                                                 context.from_peer.clone()) {
                    Ok(()) => {
                        // TODO check that the region info in response matches the request
                        // Succeed to suspend the specified region, and then go to next step
                        let task = Task::CommitMerge {
                            from_region: context.from_region,
                            from_leader: context.from_peer,
                            into_region: context.into_region,
                            into_peer: context.into_peer,
                        };
                        ensure_schedule(self.scheduler.clone(), task);
                    }
                    Err(e) => {
                        error!("failed to send raft rpc to peer {:?}, address: {}, error {:?}",
                               context.from_peer,
                               context.address,
                               e);
                        // TODO what are all the possible errors returned here?
                        // Try another peer in the specified region
                        let next_peer = next_peer(&context.from_region, context.from_peer);
                        let task = Task::SuspendRegion {
                            from_region: context.from_region,
                            from_leader: next_peer,
                            into_region: context.into_region,
                            into_peer: context.into_peer,
                        };
                        ensure_schedule(self.scheduler.clone(), task);
                    }
                }
            }
            TaskType::ShutdownRegion => {
                // send a shutdown region command to the specified region
                let client = KVClient::new(context.address);
                match client.send_shutdown_region(context.from_region.clone(),
                                                  context.from_peer.clone()) {
                    Ok(()) => {
                        // TODO check that the region info in response matches the request
                        // Succeed to shutdown the specified region.
                        return;
                    }
                    Err(e) => {
                        error!("failed to send raft rpc to peer {:?}, address: {}, error {:?}",
                               context.from_peer,
                               context.address,
                               e);
                        // Abort this task if error happens.
                        // Even though the control region fails to tell the merged region
                        // to shutdown, the merged region would communication with PD
                        // in heartbeat, and then know itself should be shutdown.
                    }
                }
            }
        }
    }
}

impl<T: PdClient> Runnable<Task> for Runner<T> {
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
            Task::AfterResolve { context } => self.handle_after_resolve(context),
        };
    }
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
