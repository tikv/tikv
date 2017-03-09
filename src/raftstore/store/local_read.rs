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

use time::Timespec;
use std::sync::Arc;
use std::sync::mpsc::Receiver;
use uuid::Uuid;
use protobuf;
use raftstore::{Result, Error};
use raftstore::store::worker::apply;
use kvproto::metapb;
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse, CmdType};
use util::{Either, clocktime, HashMap};
use util::transport::SendCh;
use rocksdb::DB;
use std::collections::BTreeMap;
use std::collections::Bound::{Excluded, Unbounded};
use super::peer::check_epoch;
use super::msg::{Msg, Callback};
use super::cmd_resp::{bind_uuid, bind_term, bind_error, new_error};
use super::util;
use super::keys::enc_end_key;
use super::engine::Snapshot;
use super::metrics::*;

pub enum RangeChangeType {
    Add,
    Remove,
    Override,
}

#[derive(Default, Debug)]
pub struct PeerStatusChange {
    pub region_id: u64,
    pub leader_id: Option<u64>,
    pub region: Option<metapb::Region>,
    pub leader_lease_expired_time: Option<Option<Either<Timespec, Timespec>>>,
    pub term: Option<u64>,
    pub applied_index_term: Option<u64>,
}

#[derive(Default)]
pub struct PeerStatus {
    pub peer: metapb::Peer,
    pub region: metapb::Region,
    pub tag: String,
    pub term: u64,
    pub applied_index_term: u64,
    pub leader_id: u64,
    pub leader_lease_expired_time: Option<Either<Timespec, Timespec>>,
}

impl PeerStatus {
    pub fn new(region: metapb::Region,
               peer: metapb::Peer,
               leader_id: u64,
               term: u64,
               applied_index_term: u64)
               -> PeerStatus {
        let tag = format!("[region {}] {}", region.get_id(), peer.get_id());
        PeerStatus {
            peer: peer,
            region: region,
            tag: tag,
            term: term,
            applied_index_term: applied_index_term,
            leader_id: leader_id,
            leader_lease_expired_time: None,
        }
    }

    pub fn find_peer(&self, peer_id: u64) -> Option<&metapb::Peer> {
        for peer in self.region.get_peers() {
            if peer.get_id() == peer_id {
                return Some(peer);
            }
        }
        None
    }

    pub fn peer_id(&self) -> u64 {
        self.peer.get_id()
    }

    pub fn is_leader(&self) -> bool {
        self.peer_id() == self.leader_id
    }

    pub fn should_read_local(&mut self, req: &RaftCmdRequest) -> bool {
        if (req.has_header() && req.get_header().get_read_quorum()) || !self.is_leader() ||
           req.get_requests().len() == 0 {
            return false;
        }

        // If applied index's term is differ from current raft's term, leader transfer
        // must happened, if read locally, we may read old value.
        if self.applied_index_term == 0 || self.term == 0 || self.applied_index_term != self.term {
            LOCAL_READ_THREAD_COUNTER_VEC.with_label_values(&["term_not_eq"]).inc();
            return false;
        }

        for cmd_req in req.get_requests() {
            if cmd_req.get_cmd_type() != CmdType::Snap && cmd_req.get_cmd_type() != CmdType::Get {
                return false;
            }
        }

        // If the leader lease has expired, local read should not be performed.
        if self.leader_lease_expired_time.is_none() {
            LOCAL_READ_THREAD_COUNTER_VEC.with_label_values(&["no_lease"]).inc();
            return false;
        }

        let mut reset_lease_expired_time = false;
        let mut lost_lease = false;
        let now = clocktime::raw_now();
        match self.leader_lease_expired_time.as_ref().unwrap().as_ref() {
            Either::Left(safe_expired_time) => {
                if now > *safe_expired_time {
                    reset_lease_expired_time = true;
                    lost_lease = true;
                }
            }
            _ => lost_lease = true,
        }
        if reset_lease_expired_time {
            debug!("leader lease expired time {:?} is outdated",
                   self.leader_lease_expired_time);
            // Reset leader lease expiring time.
            self.leader_lease_expired_time = None;
        }
        if lost_lease {
            // Perform a consistent read to Raft quorum and try to renew the leader lease.
            LOCAL_READ_THREAD_COUNTER_VEC.with_label_values(&["lost_lease"]).inc();
            return false;
        }

        true
    }
}

type Key = Vec<u8>;

pub struct LocalReadHandler {
    rx: Receiver<Msg>,
    peers_status: HashMap<u64, PeerStatus>,
    store_id: u64,
    engine: Arc<DB>,

    // used when need redirect request to raftstore
    sendch: SendCh<Msg>,
    redirect_cmds: HashMap<Uuid, Callback>,

    // region end key -> region id
    region_ranges: BTreeMap<Key, u64>,
}

impl LocalReadHandler {
    pub fn new(rx: Receiver<Msg>,
               peers_status: HashMap<u64, PeerStatus>,
               store_id: u64,
               engine: Arc<DB>,
               sendch: SendCh<Msg>,
               region_ranges: BTreeMap<Key, u64>)
               -> LocalReadHandler {
        LocalReadHandler {
            rx: rx,
            peers_status: peers_status,
            store_id: store_id,
            engine: engine,
            sendch: sendch,
            redirect_cmds: HashMap::default(),
            region_ranges: region_ranges,
        }
    }

    pub fn run(&mut self) {
        loop {
            match self.rx.recv() {
                Ok(Msg::RaftCmd { request, callback, .. }) => {
                    LOCAL_READ_THREAD_COUNTER_VEC.with_label_values(&["received"]).inc();
                    self.handle_raft_cmd(request, callback);
                }
                Ok(Msg::RedirectRaftCmdResp { uuid, resp }) => {
                    self.handle_redirect_resp(uuid, resp);
                }
                Ok(Msg::UpdatePeerStatus(change)) => {
                    self.update_peer_status(change);
                }
                Ok(Msg::NewPeerStatus { region, peer, leader_id, term, applied_index_term }) => {
                    self.create_peer_status(region, peer, leader_id, term, applied_index_term);
                }
                Ok(Msg::RemovePeerStatus { region_id }) => {
                    if self.peers_status.remove(&region_id).is_some() {
                        info!("remove peer status for region {}", region_id);
                    } else {
                        panic!("peer status for region {} not found", region_id);
                    }
                }
                Ok(Msg::RegionRangeChange { change_type, region_id, end_key }) => {
                    self.update_region_range(change_type, region_id, end_key);
                }
                Ok(Msg::Quit) => {
                    info!("receive quit command, read only query handler exit.");
                    break;
                }
                Ok(msg) => {
                    panic!("receive unsupported msg {:?} in local read thread.", msg);
                }
                Err(e) => {
                    panic!("receive read only query failed, error {:?}", e);
                }
            }
        }
    }

    fn handle_redirect_resp(&mut self, uuid: Uuid, resp: RaftCmdResponse) {
        match self.redirect_cmds.remove(&uuid) {
            Some(cb) => {
                cb.call_box((resp,));
            }
            None => {
                panic!("command in redirect is lost when receive response, uuid {}",
                       uuid);
            }
        }
    }

    fn create_peer_status(&mut self,
                          region: metapb::Region,
                          peer: metapb::Peer,
                          leader_id: u64,
                          term: u64,
                          applied_index_term: u64) {
        let region_id = region.get_id();
        self.peers_status.insert(region_id,
                                 PeerStatus::new(region,
                                                 peer,
                                                 leader_id,
                                                 term,
                                                 applied_index_term));
    }

    fn update_peer_status(&mut self, change: PeerStatusChange) {
        let region_id = change.region_id;
        if let Some(peer_status) = self.peers_status.get_mut(&region_id) {
            if let Some(leader_id) = change.leader_id {
                peer_status.leader_id = leader_id;
            }
            if let Some(region) = change.region {
                peer_status.region = region;
            }
            if let Some(expired_time) = change.leader_lease_expired_time {
                peer_status.leader_lease_expired_time = expired_time;
            }
            if let Some(term) = change.term {
                peer_status.term = term;
            }
            if let Some(applied_index_term) = change.applied_index_term {
                peer_status.applied_index_term = applied_index_term;
            }
        } else {
            panic!("peer status not found for region {} in store {}, receive unexpected peer \
                    status update [{:?}]",
                   region_id,
                   self.store_id,
                   change);
        }
    }

    fn update_region_range(&mut self,
                           change_type: RangeChangeType,
                           region_id: u64,
                           end_key: Vec<u8>) {
        match change_type {
            RangeChangeType::Add => {
                if self.region_ranges.insert(end_key, region_id).is_some() {
                    panic!("[Add] range for region [{}] is existed.", region_id);
                }
            }
            RangeChangeType::Remove => {
                if self.region_ranges.remove(&end_key).is_none() {
                    panic!("[Remove] range for region [{}] is not exist.", region_id);
                }
            }
            RangeChangeType::Override => {
                if self.region_ranges.insert(end_key, region_id).is_none() {
                    panic!("[Override] range for region [{}] is not exist.", region_id);
                }
            }
        }
    }

    fn redirect_cmd_to_raftstore(&mut self, uuid: Uuid, req: RaftCmdRequest, cb: Callback) {
        self.redirect_cmds.insert(uuid, cb);
        if let Err(e) = self.sendch.send(Msg::RedirectRaftCmd {
            uuid: uuid,
            request: req,
        }) {
            match self.redirect_cmds.remove(&uuid) {
                Some(cb) => {
                    let mut resp = RaftCmdResponse::new();
                    bind_error(&mut resp, e.into());
                    return cb.call_box((resp,));
                }
                None => {
                    panic!("command in redirect is lost, uuid {}", uuid);
                }
            }
        }
    }

    fn handle_raft_cmd(&mut self, req: RaftCmdRequest, cb: Callback) {
        let mut resp = RaftCmdResponse::new();
        let uuid: Uuid = match util::get_uuid_from_req(&req) {
            None => {
                bind_error(&mut resp, Error::Other("missing request uuid".into()));
                return cb.call_box((resp,));
            }
            Some(uuid) => {
                bind_uuid(&mut resp, uuid);
                uuid
            }
        };

        if self.redirect_cmds.contains_key(&uuid) {
            bind_error(&mut resp, box_err!("duplicated uuid {:?}", uuid));
            return cb.call_box((resp,));
        }

        if let Err(e) = self.validate_store_id(&req) {
            bind_error(&mut resp, e);
            return cb.call_box((resp,));
        }

        // redirect command to raftstore
        if req.has_admin_request() || req.has_status_request() {
            LOCAL_READ_THREAD_COUNTER_VEC.with_label_values(&["redirect"]).inc();
            return self.redirect_cmd_to_raftstore(uuid, req, cb);
        }

        if let Err(e) = self.validate_region(&req) {
            bind_error(&mut resp, e);
            return cb.call_box((resp,));
        }

        let region_id = req.get_header().get_region_id();
        let mut need_redirect = false;
        if let Some(peer_status) = self.peers_status.get_mut(&region_id) {
            if !peer_status.should_read_local(&req) {
                need_redirect = true;
            }
        } else {
            bind_error(&mut resp, Error::RegionNotFound(region_id));
            return cb.call_box((resp,));
        }

        // redirect command to raftstore
        if need_redirect {
            LOCAL_READ_THREAD_COUNTER_VEC.with_label_values(&["redirect"]).inc();
            return self.redirect_cmd_to_raftstore(uuid, req, cb);
        }

        // handle read
        LOCAL_READ_THREAD_COUNTER_VEC.with_label_values(&["handled"]).inc();
        if let Some(peer_status) = self.peers_status.get(&region_id) {
            let mut resp = self.exec_read(peer_status, &req).unwrap_or_else(|e| {
                error!("execute raft command err: {:?}", e);
                new_error(e)
            });

            bind_uuid(&mut resp, uuid);
            bind_term(&mut resp, peer_status.term);
            return cb.call_box((resp,));
        }
        panic!("peer status should exist for region {}", region_id);
    }

    fn validate_store_id(&self, req: &RaftCmdRequest) -> Result<()> {
        let store_id = req.get_header().get_peer().get_store_id();
        if store_id != self.store_id {
            return Err(Error::StoreNotMatch(store_id, self.store_id));
        }
        Ok(())
    }

    fn validate_region(&self, msg: &RaftCmdRequest) -> Result<()> {
        let region_id = msg.get_header().get_region_id();
        let peer_id = msg.get_header().get_peer().get_id();

        if let Some(peer_status) = self.peers_status.get(&region_id) {
            if !peer_status.is_leader() {
                let leader = match peer_status.find_peer(peer_status.leader_id) {
                    Some(peer) => Some(peer.clone()),
                    None => None,
                };
                return Err(Error::NotLeader(region_id, leader));
            }
            if peer_status.peer_id() != peer_id {
                return Err(box_err!("mismatch peer id {} != {}", peer_status.peer_id(), peer_id));
            }

            let header = msg.get_header();
            // If header's term is 2 verions behind current term,
            // leadership may have been changed away.
            if header.get_term() > 0 && peer_status.term > header.get_term() + 1 {
                return Err(Error::StaleCommand);
            }

            let res = check_epoch(&peer_status.region, msg);
            if let Err(Error::StaleEpoch(msg, mut new_regions)) = res {
                // Attach the next region which might be split from the
                // current region. But it doesn't matter if the next
                // region is not split from the current region. If the
                // region meta received by the TiKV driver is newer
                // than the meta cached in the driver, the meta is
                // updated.
                if let Some((_, &next_region_id)) = self.region_ranges
                    .range((Excluded(enc_end_key(&peer_status.region)), Unbounded::<Key>))
                    .next() {
                    if let Some(next_peer_status) = self.peers_status.get(&next_region_id) {
                        new_regions.push(next_peer_status.region.clone());
                    }
                }
                return Err(Error::StaleEpoch(msg, new_regions));
            }
            res
        } else {
            Err(Error::RegionNotFound(region_id))
        }
    }

    fn exec_read(&self, peer_status: &PeerStatus, req: &RaftCmdRequest) -> Result<RaftCmdResponse> {
        let snap = Snapshot::new(self.engine.clone());
        let requests = req.get_requests();
        let mut responses = Vec::with_capacity(requests.len());

        for req in requests {
            let cmd_type = req.get_cmd_type();
            let mut resp = match cmd_type {
                CmdType::Get => {
                    try!(apply::do_get(&peer_status.tag, &peer_status.region, &snap, req))
                }
                CmdType::Snap => try!(apply::do_snap(peer_status.region.clone())),
                CmdType::Put | CmdType::Delete | CmdType::Invalid => unreachable!(),
            };

            resp.set_cmd_type(cmd_type);
            responses.push(resp);
        }

        let mut resp = RaftCmdResponse::new();
        resp.set_responses(protobuf::RepeatedField::from_vec(responses));
        Ok(resp)
    }
}
