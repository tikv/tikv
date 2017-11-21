// Copyright 2017 PingCAP, Inc.
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
use std::fmt::{self, Display, Formatter};
use std::time::Instant;

use kvproto::metapb::{Peer, Region, Store};
use kvproto::raft_cmdpb::{CmdType, RaftCmdRequest, RaftCmdResponse};

use protobuf;
use rocksdb::DB;
use raftstore::{Error, Result};
use util::worker::Runnable;
use util::time::RemoteLease;
use util::collections::HashMap;
use util::Either;

use super::{apply, MsgSender};
use super::super::engine::Snapshot;
use super::super::super::store::{check_epoch, Callback, Msg as StoreMsg, RequestPolicy};

/// Status for leaders
pub struct LeaderStatus {
    region: Region,
    leader: Peer,
    term: u64,
    tag: String,

    applied_index_term: u64,
    leader_lease: Option<RemoteLease>,
}

pub enum Task {
    Msg(StoreMsg),
    Update(LeaderStatus),
    Delete(u64),
}

impl Task {
    pub fn update(
        region: Region,
        leader: Peer,
        term: u64,
        applied_index_term: u64,
        leader_lease: Option<RemoteLease>,
    ) -> Task {
        let tag = format!("[region {}]", region.get_id());
        Task::Update(LeaderStatus {
            region,
            leader,
            term,
            applied_index_term,
            tag,
            leader_lease,
        })
    }

    pub fn delete(region_id: u64) -> Task {
        Task::Delete(region_id)
    }

    pub fn accept(msg: StoreMsg) -> Either<Task, StoreMsg> {
        match msg {
            StoreMsg::RaftCmd {
                send_time,
                request,
                callback,
            } => {
                let read_req = is_read_request(&request);
                let msg = StoreMsg::RaftCmd {
                    send_time,
                    request,
                    callback,
                };
                if read_req {
                    Either::Left(Task::Msg(msg))
                } else {
                    Either::Right(msg)
                }
            }
            msg @ StoreMsg::BatchRaftSnapCmds { .. } => Either::Left(Task::Msg(msg)),
            msg => Either::Right(msg),
        }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::Msg(ref msg) => write!(f, "LocalRead task Msg {:?}", msg),
            Task::Update(LeaderStatus {
                ref region,
                ref leader,
                term,
                applied_index_term,
                ..
            }) => write!(
                f,
                "LocalRead task Update cache for region {},\
                 leader {} at term {}, applied_index_term {}",
                region.get_id(),
                leader.get_id(),
                term,
                applied_index_term
            ),
            Task::Delete(region_id) => write!(f, "LocalRead task Delete region {}", region_id),
        }
    }
}


fn is_read_request(req: &RaftCmdRequest) -> bool {
    if req.has_admin_request() || req.has_status_request() {
        false
    } else {
        for r in req.get_requests() {
            match r.get_cmd_type() {
                CmdType::Get | CmdType::Snap => (),
                CmdType::Delete |
                CmdType::Put |
                CmdType::DeleteRange |
                CmdType::Prewrite |
                CmdType::Invalid => return false,
            }
        }
        true
    }
}

pub struct LocalReader<C: MsgSender> {
    kv_engine: Arc<DB>,
    store: Store,

    // region id -> LeaderStatus
    region_leaders: HashMap<u64, LeaderStatus>,

    // A channel to raftstore.
    ch: C,
}

impl<C: MsgSender> LocalReader<C> {
    pub fn new(kv_engine: Arc<DB>, store: Store, ch: C) -> LocalReader<C> {
        let region_leaders = HashMap::default();
        LocalReader {
            kv_engine,
            store,
            region_leaders,
            ch,
        }
    }

    fn update(&mut self, status: LeaderStatus) {
        // TODO(stn): check status?
        self.region_leaders.insert(status.region.get_id(), status);
    }

    fn delete(&mut self, region_id: u64) {
        self.region_leaders.remove(&region_id);
    }

    fn redirect(&self, send_time: Instant, request: RaftCmdRequest, callback: Callback) {
        self.ch
            .send(StoreMsg::RaftCmd {
                send_time,
                request,
                callback,
            })
            .unwrap()
    }

    fn validate_store_id(&self, msg: &RaftCmdRequest) -> Result<()> {
        let store_id = msg.get_header().get_peer().get_store_id();
        if store_id != self.store.get_id() {
            return Err(Error::StoreNotMatch(store_id, self.store.get_id()));
        }
        Ok(())
    }

    fn validate_region(&self, req: &RaftCmdRequest) -> Result<()> {
        let region_id = req.get_header().get_region_id();
        let peer_id = req.get_header().get_peer().get_id();
        let status = match self.region_leaders.get(&region_id) {
            Some(status) => status,
            None => return Err(Error::RegionNotFound(region_id)),
        };
        if status.leader.get_id() != peer_id {
            return Err(box_err!(
                "mismatch peer id {} != {}",
                status.leader.get_id(),
                peer_id
            ));
        }

        let header = req.get_header();
        // If header's term is 2 verions behind current term, leadership may have been changed away.
        if status.term != header.get_term() {
            return Err(Error::StaleCommand);
        }

        check_epoch(&status.region, req)
    }

    // TODO(stn): return true or false.
    fn pre_propose_raft_command(
        &mut self,
        req: &RaftCmdRequest,
    ) -> Result<Option<RaftCmdResponse>> {
        self.validate_store_id(req)?;
        assert!(
            !req.has_status_request(),
            "LocalReader can not serve status requests {:?}",
            req
        );
        assert!(
            !req.has_admin_request(),
            "LocalReader can not serve admin requests {:?}",
            req
        );
        self.validate_region(req)?;
        Ok(None)
    }

    fn get_handle_policy(&self, req: &RaftCmdRequest) -> Result<RequestPolicy> {
        for r in req.get_requests() {
            match r.get_cmd_type() {
                CmdType::Get | CmdType::Snap => (),
                CmdType::Delete | CmdType::Put | CmdType::DeleteRange => {
                    panic!("LocalReader can not serve write requests {:?}", r);
                }
                CmdType::Prewrite | CmdType::Invalid => {
                    return Err(box_err!(
                        "invalid cmd type {:?}, message maybe currupted",
                        r.get_cmd_type()
                    ));
                }
            }
        }

        if req.has_header() && req.get_header().get_read_quorum() {
            return Ok(RequestPolicy::ReadIndex);
        }

        let region_id = req.get_header().get_region_id();
        let status = match self.region_leaders.get(&region_id) {
            Some(status) => status,
            None => return Err(Error::RegionNotFound(region_id)),
        };

        // If applied index's term is differ from current raft's term, leader transfer
        // must happened, if read locally, we may read old value.
        if status.applied_index_term != status.term || status.leader_lease.is_none() {
            Ok(RequestPolicy::ReadIndex)
        } else {
            let leader_lease = status.leader_lease.as_ref().unwrap().lock().unwrap();
            if leader_lease.in_safe_lease() {
                // Local read should be performed, iff leader is in safe lease.
                Ok(RequestPolicy::ReadLocal)
            } else {
                debug!(
                    "{} leader lease expired time {:?} is outdated",
                    status.tag,
                    status
                        .leader_lease
                        .as_ref()
                        .unwrap()
                        .lock()
                        .unwrap()
                        .expired_time(),
                );
                Ok(RequestPolicy::ReadIndex)
            }
        }
    }

    fn exec_read(&mut self, req: &RaftCmdRequest) -> RaftCmdResponse {
        let region_id = req.get_header().get_region_id();
        let mut snap = None;
        let requests = req.get_requests();
        let mut responses = Vec::with_capacity(requests.len());
        let status = &self.region_leaders[&region_id];
        for req in requests {
            let cmd_type = req.get_cmd_type();
            let mut resp = match cmd_type {
                CmdType::Get => {
                    if snap.is_none() {
                        snap = Some(Snapshot::new(self.kv_engine.clone()));
                    }
                    apply::do_get(&status.tag, &status.region, snap.as_ref().unwrap(), req).unwrap()
                }
                CmdType::Snap => apply::do_snap(status.region.clone()).unwrap(),
                CmdType::Prewrite |
                CmdType::Put |
                CmdType::Delete |
                CmdType::DeleteRange |
                CmdType::Invalid => unreachable!(),
            };
            resp.set_cmd_type(cmd_type);
            responses.push(resp);
        }

        let mut resp = RaftCmdResponse::new();
        resp.set_responses(protobuf::RepeatedField::from_vec(responses));
        resp
    }
}

impl<C: MsgSender> Runnable<Task> for LocalReader<C> {
    fn run(&mut self, task: Task) {
        match task {
            Task::Msg(StoreMsg::RaftCmd {
                send_time,
                request,
                callback,
            }) => match self.pre_propose_raft_command(&request) {
                Ok(_) => match self.get_handle_policy(&request) {
                    Ok(RequestPolicy::ReadLocal) => {
                        callback(self.exec_read(&request));
                    }
                    Ok(RequestPolicy::ReadIndex) => self.redirect(send_time, request, callback),
                    Ok(policy) => unimplemented!("unsuppoted policy {:?}", policy),
                    Err(_) => {
                        self.redirect(send_time, request, callback);
                    }
                },
                Err(_) => {
                    self.redirect(send_time, request, callback);
                }
            },
            Task::Msg(msg @ StoreMsg::BatchRaftSnapCmds { .. }) => {
                // TODO(stn): support BatchRaftSnapCmds
                self.ch.send(msg).unwrap();
            }
            Task::Msg(other) => {
                unimplemented!("unsupported Msg {:?}", other);
            }
            Task::Update(status) => {
                self.update(status);
            }
            Task::Delete(region_id) => {
                self.delete(region_id);
            }
        }
    }
}
