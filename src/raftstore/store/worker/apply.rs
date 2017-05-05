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
use std::sync::mpsc::Sender;
use std::fmt::{self, Debug, Display, Formatter};
use std::collections::VecDeque;

use rocksdb::{DB, WriteBatch, Writable};
use uuid::Uuid;
use protobuf::RepeatedField;

use kvproto::metapb::{Peer as PeerMeta, Region};
use kvproto::eraftpb::{Entry, EntryType, ConfChange, ConfChangeType};
use kvproto::raft_serverpb::{RaftApplyState, RaftTruncatedState, PeerState};
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse, ChangePeerRequest, CmdType,
                          AdminCmdType, Request, Response, AdminRequest, AdminResponse};

use util::worker::Runnable;
use util::{SlowTimer, rocksdb, escape};
use util::collections::{HashMap, HashMapEntry as MapEntry};
use storage::{CF_LOCK, CF_RAFT};
use raftstore::{Result, Error};
use raftstore::store::{Store, cmd_resp, keys, util};
use raftstore::store::msg::Callback;
use raftstore::store::engine::{Snapshot, Peekable, Mutable};
use raftstore::store::peer_storage::{self, write_initial_state, write_peer_state, compact_raft_log};
use raftstore::store::peer::{parse_data_at, check_epoch, Peer, ProposalBatch};
use raftstore::store::metrics::*;


pub struct PendingCmd {
    pub uuid: Uuid,
    pub term: u64,
    pub cb: Option<Callback>,
}

impl PendingCmd {
    fn new(uuid: Uuid, term: u64, cb: Callback) -> PendingCmd {
        PendingCmd {
            uuid: uuid,
            term: term,
            cb: Some(cb),
        }
    }
}

impl Drop for PendingCmd {
    fn drop(&mut self) {
        if self.cb.is_some() {
            panic!("callback of pending command {} is leak.", self.uuid);
        }
    }
}

impl Debug for PendingCmd {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f,
               "PendingCmd [uuid: {}, term: {}, has_cb: {}]",
               self.uuid,
               self.term,
               self.cb.is_some())
    }
}

#[derive(Default, Debug)]
pub struct PendingCmdQueue {
    normals: VecDeque<PendingCmd>,
    conf_change: Option<PendingCmd>,
}

impl PendingCmdQueue {
    fn pop_normal(&mut self, term: u64) -> Option<PendingCmd> {
        self.normals.pop_front().and_then(|cmd| {
            if cmd.term > term {
                self.normals.push_front(cmd);
                return None;
            }
            Some(cmd)
        })
    }

    fn append_normal(&mut self, cmd: PendingCmd) {
        self.normals.push_back(cmd);
    }

    fn take_conf_change(&mut self) -> Option<PendingCmd> {
        // conf change will not be affected when changing between follower and leader,
        // so there is no need to check term.
        self.conf_change.take()
    }

    // TODO: seems we don't need to seperate conf change from normal entries.
    fn set_conf_change(&mut self, cmd: PendingCmd) {
        self.conf_change = Some(cmd);
    }
}

#[derive(Default, Debug)]
pub struct ChangePeer {
    pub conf_change: ConfChange,
    pub peer: PeerMeta,
    pub region: Region,
}

#[derive(Debug)]
pub enum ExecResult {
    ChangePeer(ChangePeer),
    CompactLog {
        state: RaftTruncatedState,
        first_index: u64,
    },
    SplitRegion {
        left: Region,
        right: Region,
        right_derive: bool,
    },
    ComputeHash {
        region: Region,
        index: u64,
        snap: Snapshot,
    },
    VerifyHash { index: u64, hash: Vec<u8> },
}

/// Call the callback of `cmd` that the region is removed.
fn notify_region_removed(region_id: u64, peer_id: u64, mut cmd: PendingCmd) {
    notify_req_region_removed(region_id, peer_id, cmd.uuid, cmd.cb.take().unwrap());
}

pub fn notify_req_region_removed(region_id: u64, peer_id: u64, uuid: Uuid, cb: Callback) {
    let region_not_found = Error::RegionNotFound(region_id);
    let mut resp = cmd_resp::new_error(region_not_found);
    cmd_resp::bind_uuid(&mut resp, uuid);
    debug!("[region {}] {} is removed, notify {}.",
           region_id,
           peer_id,
           uuid);
    cb(resp);
}

/// Call the callback of `cmd` when it can not be processed further.
fn notify_stale_command(tag: &str, term: u64, mut cmd: PendingCmd) {
    notify_stale_req(tag, term, cmd.uuid, cmd.cb.take().unwrap())
}

pub fn notify_stale_req(tag: &str, term: u64, uuid: Uuid, cb: Callback) {
    let resp = cmd_resp::err_resp(Error::StaleCommand, uuid, term);
    info!("{} command {} is stale, skip", tag, uuid);
    cb(resp);
}

#[derive(Debug)]
pub struct ApplyDelegate {
    // peer_id
    id: u64,
    // peer_tag, "[region region_id] peer_id"
    tag: String,
    engine: Arc<DB>,
    region: Region,
    // if we remove ourself in ChangePeer remove, we should set this flag, then
    // any following committed logs in same Ready should be applied failed.
    pending_remove: bool,
    apply_state: RaftApplyState,
    applied_index_term: u64,
    term: u64,
    pending_cmds: PendingCmdQueue,
    metrics: ApplyMetrics,
}

impl ApplyDelegate {
    pub fn region_id(&self) -> u64 {
        self.region.get_id()
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    fn from_peer(peer: &Peer) -> ApplyDelegate {
        let reg = Registration::new(peer);
        ApplyDelegate::from_registration(peer.engine(), reg)
    }

    fn from_registration(db: Arc<DB>, reg: Registration) -> ApplyDelegate {
        ApplyDelegate {
            id: reg.id,
            tag: format!("[region {}] {}", reg.region.get_id(), reg.id),
            engine: db,
            region: reg.region,
            pending_remove: false,
            apply_state: reg.apply_state,
            applied_index_term: reg.applied_index_term,
            term: reg.term,
            pending_cmds: Default::default(),
            metrics: Default::default(),
        }
    }

    fn handle_raft_committed_entries(&mut self, committed_entries: Vec<Entry>) -> Vec<ExecResult> {
        if committed_entries.is_empty() {
            return vec![];
        }
        // If we send multiple ConfChange commands, only first one will be proposed correctly,
        // others will be saved as a normal entry with no data, so we must re-propose these
        // commands again.
        let t = SlowTimer::new();
        let mut results = vec![];
        let committed_count = committed_entries.len();
        for entry in committed_entries {
            if self.pending_remove {
                // This peer is about to be destroyed, skip everything.
                break;
            }

            let expect_index = self.apply_state.get_applied_index() + 1;
            if expect_index != entry.get_index() {
                panic!("{} expect index {}, but got {}",
                       self.tag,
                       expect_index,
                       entry.get_index());
            }

            let res = match entry.get_entry_type() {
                EntryType::EntryNormal => self.handle_raft_entry_normal(entry),
                EntryType::EntryConfChange => self.handle_raft_entry_conf_change(entry),
            };

            results.extend(res);
        }

        slow_log!(t,
                  "{} handle ready {} committed entries",
                  self.tag,
                  committed_count);
        results
    }

    fn handle_raft_entry_normal(&mut self, entry: Entry) -> Vec<ExecResult> {
        let index = entry.get_index();
        let term = entry.get_term();
        let mut data = entry.get_data();

        if !data.is_empty() {
            let mut res = vec![];
            let (mut data, is_new_version) = ProposalBatch::check_version(data);
            if !is_new_version {
                //let cmd = ProposalBatch::read_old_req(data);
                let cmd = parse_data_at(data, index, &self.tag);
                if let Some(exec_res) = self.process_raft_cmd(index, term, cmd) {
                    res.push(exec_res);
                }
            } else {
                while !data.is_empty() {
                    let (left, cmd) = ProposalBatch::read_req(data);
                    if let Some(exec_res) = self.process_raft_cmd(index, term, cmd) {
                        res.push(exec_res);
                    }
                    data = left;
                }
            }
            return res;
        }

        // when a peer become leader, it will send an empty entry.
        let wb = WriteBatch::new();
        let mut state = self.apply_state.clone();
        state.set_applied_index(index);
        rocksdb::get_cf_handle(&self.engine, CF_RAFT)
            .map_err(From::from)
            .and_then(|handle| {
                wb.put_msg_cf(handle, &keys::apply_state_key(self.region.get_id()), &state)
            })
            .and_then(|_| self.engine.write(wb).map_err(From::from))
            .unwrap_or_else(|e| {
                panic!("{} failed to apply empty entry at {}: {:?}",
                       self.tag,
                       index,
                       e);
            });
        self.apply_state = state;
        self.applied_index_term = term;
        assert!(term > 0);
        while let Some(cmd) = self.pending_cmds.pop_normal(term - 1) {
            // apprently, all the callbacks whose term is less than entry's term are stale.
            notify_stale_command(&self.tag, self.term, cmd);
        }
        vec![]
    }

    fn handle_raft_entry_conf_change(&mut self, entry: Entry) -> Vec<ExecResult> {
        let index = entry.get_index();
        let term = entry.get_term();
        let conf_change: ConfChange = parse_data_at(entry.get_data(), index, &self.tag);
        let cmd = parse_data_at(conf_change.get_context(), index, &self.tag);
        let result = Some(self.process_raft_cmd(index, term, cmd).map_or_else(|| {
            // If failed, tell raft that the config change was aborted.
            ExecResult::ChangePeer(Default::default())
        }, |mut res| {
            if let ExecResult::ChangePeer(ref mut cp) = res {
                cp.conf_change = conf_change;
            } else {
                panic!("{} unexpected result {:?} for conf change {:?} at {}",
                       self.tag,
                       res,
                       conf_change,
                       index);
            }
            res
        }));
        result.map_or_else(|| vec![], |r| vec![r])
    }

    fn find_cb(&mut self, uuid: Uuid, term: u64, cmd: &RaftCmdRequest) -> Option<Callback> {
        if get_change_peer_cmd(cmd).is_some() {
            if let Some(mut cmd) = self.pending_cmds.take_conf_change() {
                if cmd.uuid == uuid {
                    return Some(cmd.cb.take().unwrap());
                } else {
                    notify_stale_command(&self.tag, self.term, cmd);
                }
            }
            return None;
        }
        while let Some(mut head) = self.pending_cmds.pop_normal(term) {
            if head.uuid == uuid {
                return Some(head.cb.take().unwrap());
            }
            // Because of the lack of original RaftCmdRequest, we skip calling
            // coprocessor here.
            // TODO: call coprocessor with uuid instead.
            notify_stale_command(&self.tag, self.term, head);
        }
        None
    }

    fn process_raft_cmd(&mut self,
                        index: u64,
                        term: u64,
                        cmd: RaftCmdRequest)
                        -> Option<ExecResult> {
        if index == 0 {
            panic!("{} processing raft command needs a none zero index",
                   self.tag);
        }

        let uuid = util::get_uuid_from_req(&cmd).unwrap();
        let cmd_cb = self.find_cb(uuid, term, &cmd);
        let timer = PEER_APPLY_LOG_HISTOGRAM.start_timer();
        let (mut resp, exec_result) = self.apply_raft_cmd(index, term, &cmd);
        timer.observe_duration();

        debug!("{} applied command with uuid {:?} at log index {}",
               self.tag,
               uuid,
               index);

        let cb = match cmd_cb {
            None => return exec_result,
            Some(cb) => cb,
        };

        // TODO: Involve post apply hook.
        // TODO: if we have exec_result, maybe we should return this callback too. Outer
        // store will call it after handing exec result.
        // Bind uuid here.
        cmd_resp::bind_uuid(&mut resp, uuid);
        cmd_resp::bind_term(&mut resp, self.term);
        cb.call_box((resp,));

        exec_result
    }

    // apply operation can fail as following situation:
    //   1. encouter an error that will occur on all store, it can continue
    // applying next entry safely, like stale epoch for example;
    //   2. encouter an error that may not occur on all store, in this case
    // we should try to apply the entry again or panic. Considering that this
    // usually due to disk operation fail, which is rare, so just panic is ok.
    fn apply_raft_cmd(&mut self,
                      index: u64,
                      term: u64,
                      req: &RaftCmdRequest)
                      -> (RaftCmdResponse, Option<ExecResult>) {
        // if pending remove, apply should be aborted already.
        assert!(!self.pending_remove);

        let mut ctx = self.new_ctx(index, term, req);
        let (resp, exec_result) = self.exec_raft_cmd(&mut ctx).unwrap_or_else(|e| {
            // clear dirty values.
            ctx.wb.clear();
            match e {
                Error::StaleEpoch(..) => info!("{} stale epoch err: {:?}", self.tag, e),
                _ => error!("{} execute raft command err: {:?}", self.tag, e),
            }
            (cmd_resp::new_error(e), None)
        });

        ctx.apply_state.set_applied_index(index);
        if !self.pending_remove {
            ctx.save(self.region.get_id())
                .unwrap_or_else(|e| panic!("{} failed to save apply context: {:?}", self.tag, e));
        }

        self.metrics.written_bytes += ctx.wb.data_size() as u64;
        self.metrics.written_keys += ctx.wb.count() as u64;

        // Commit write and change storage fields atomically.
        self.engine
            .write(ctx.wb)
            .unwrap_or_else(|e| panic!("{} failed to commit apply result: {:?}", self.tag, e));

        self.apply_state = ctx.apply_state;
        self.applied_index_term = term;

        if let Some(ref exec_result) = exec_result {
            match *exec_result {
                ExecResult::ChangePeer(ref cp) => {
                    self.region = cp.region.clone();
                }
                ExecResult::ComputeHash { .. } |
                ExecResult::VerifyHash { .. } |
                ExecResult::CompactLog { .. } => {}
                ExecResult::SplitRegion { ref left, ref right, right_derive } => {
                    if right_derive {
                        self.region = right.clone();
                    } else {
                        self.region = left.clone();
                    }
                    self.metrics.size_diff_hint = 0;
                    self.metrics.delete_keys_hint = 0;
                }
            }
        }

        (resp, exec_result)
    }

    /// Clear all the pending commands.
    ///
    /// Please note that all the pending callbacks will be lost.
    /// Should not do this when dropping a peer in case of possible leak.
    fn clear_pending_commands(&mut self) {
        if !self.pending_cmds.normals.is_empty() {
            info!("{} clear {} commands",
                  self.tag,
                  self.pending_cmds.normals.len());
            while let Some(mut cmd) = self.pending_cmds.normals.pop_front() {
                cmd.cb.take();
            }
        }
        if let Some(mut cmd) = self.pending_cmds.conf_change.take() {
            info!("{} clear pending conf change", self.tag);
            cmd.cb.take();
        }
    }

    fn destroy(&mut self) {
        for cmd in self.pending_cmds.normals.drain(..) {
            notify_region_removed(self.region.get_id(), self.id, cmd);
        }
        if let Some(cmd) = self.pending_cmds.conf_change.take() {
            notify_region_removed(self.region.get_id(), self.id, cmd);
        }
    }

    fn clear_all_commands_as_stale(&mut self) {
        for cmd in self.pending_cmds.normals.drain(..) {
            notify_stale_command(&self.tag, self.term, cmd);
        }
        if let Some(cmd) = self.pending_cmds.conf_change.take() {
            notify_stale_command(&self.tag, self.term, cmd);
        }
    }

    fn new_ctx<'a>(&self, index: u64, term: u64, req: &'a RaftCmdRequest) -> ExecContext<'a> {
        ExecContext {
            snap: Snapshot::new(self.engine.clone()),
            apply_state: self.apply_state.clone(),
            wb: WriteBatch::new(),
            req: req,
            index: index,
            term: term,
        }
    }
}

struct ExecContext<'a> {
    snap: Snapshot,
    apply_state: RaftApplyState,
    wb: WriteBatch,
    req: &'a RaftCmdRequest,
    index: u64,
    term: u64,
}

impl<'a> ExecContext<'a> {
    fn save(&self, region_id: u64) -> Result<()> {
        let raft_cf = try!(self.snap.cf_handle(CF_RAFT));
        try!(self.wb.put_msg_cf(raft_cf,
                                &keys::apply_state_key(region_id),
                                &self.apply_state));
        Ok(())
    }
}

// Here we implement all commands.
impl ApplyDelegate {
    // Only errors that will also occur on all other stores should be returned.
    fn exec_raft_cmd(&mut self,
                     ctx: &mut ExecContext)
                     -> Result<(RaftCmdResponse, Option<ExecResult>)> {
        try!(check_epoch(&self.region, ctx.req));
        if ctx.req.has_admin_request() {
            self.exec_admin_cmd(ctx)
        } else {
            // Now we don't care write command outer, so use None.
            self.exec_write_cmd(ctx).and_then(|v| Ok((v, None)))
        }
    }

    fn exec_admin_cmd(&mut self,
                      ctx: &mut ExecContext)
                      -> Result<(RaftCmdResponse, Option<ExecResult>)> {
        let request = ctx.req.get_admin_request();
        let cmd_type = request.get_cmd_type();
        info!("{} execute admin command {:?} at [term: {}, index: {}]",
              self.tag,
              request,
              ctx.term,
              ctx.index);

        let (mut response, exec_result) = try!(match cmd_type {
            AdminCmdType::ChangePeer => self.exec_change_peer(ctx, request),
            AdminCmdType::Split => self.exec_split(ctx, request),
            AdminCmdType::CompactLog => self.exec_compact_log(ctx, request),
            AdminCmdType::TransferLeader => Err(box_err!("transfer leader won't exec")),
            AdminCmdType::ComputeHash => self.exec_compute_hash(ctx, request),
            AdminCmdType::VerifyHash => self.exec_verify_hash(ctx, request),
            AdminCmdType::InvalidAdmin => Err(box_err!("unsupported admin command type")),
        });
        response.set_cmd_type(cmd_type);

        let mut resp = RaftCmdResponse::new();
        resp.set_admin_response(response);
        Ok((resp, exec_result))
    }

    fn exec_change_peer(&mut self,
                        ctx: &ExecContext,
                        request: &AdminRequest)
                        -> Result<(AdminResponse, Option<ExecResult>)> {
        let request = request.get_change_peer();
        let peer = request.get_peer();
        let store_id = peer.get_store_id();
        let change_type = request.get_change_type();
        let mut region = self.region.clone();

        info!("{} exec ConfChange {:?}, epoch: {:?}",
              self.tag,
              util::conf_change_type_str(&change_type),
              region.get_region_epoch());

        // TODO: we should need more check, like peer validation, duplicated id, etc.
        let exists = util::find_peer(&region, store_id).is_some();
        let conf_ver = region.get_region_epoch().get_conf_ver() + 1;

        region.mut_region_epoch().set_conf_ver(conf_ver);

        match change_type {
            ConfChangeType::AddNode => {
                PEER_ADMIN_CMD_COUNTER_VEC.with_label_values(&["add_peer", "all"]).inc();

                if exists {
                    error!("{} can't add duplicated peer {:?} to region {:?}",
                           self.tag,
                           peer,
                           self.region);
                    return Err(box_err!("can't add duplicated peer {:?} to region {:?}",
                                        peer,
                                        self.region));
                }

                // TODO: Do we allow adding peer in same node?

                region.mut_peers().push(peer.clone());

                PEER_ADMIN_CMD_COUNTER_VEC.with_label_values(&["add_peer", "success"]).inc();

                info!("{} add peer {:?} to region {:?}",
                      self.tag,
                      peer,
                      self.region);
            }
            ConfChangeType::RemoveNode => {
                PEER_ADMIN_CMD_COUNTER_VEC.with_label_values(&["remove_peer", "all"]).inc();

                if !exists {
                    error!("{} remove missing peer {:?} from region {:?}",
                           self.tag,
                           peer,
                           self.region);
                    return Err(box_err!("remove missing peer {:?} from region {:?}",
                                        peer,
                                        self.region));
                }

                if self.id == peer.get_id() {
                    // Remove ourself, we will destroy all region data later.
                    // So we need not to apply following logs.
                    self.pending_remove = true;
                }

                util::remove_peer(&mut region, store_id).unwrap();

                PEER_ADMIN_CMD_COUNTER_VEC.with_label_values(&["remove_peer", "success"]).inc();

                info!("{} remove {} from region:{:?}",
                      self.tag,
                      peer.get_id(),
                      self.region);
            }
        }

        let state = if self.pending_remove {
            PeerState::Tombstone
        } else {
            PeerState::Normal
        };
        if let Err(e) = write_peer_state(&ctx.wb, &region, state) {
            panic!("{} failed to update region state: {:?}", self.tag, e);
        }

        let mut resp = AdminResponse::new();
        resp.mut_change_peer().set_region(region.clone());

        Ok((resp,
            Some(ExecResult::ChangePeer(ChangePeer {
            conf_change: Default::default(),
            peer: peer.clone(),
            region: region,
        }))))
    }

    fn exec_split(&mut self,
                  ctx: &ExecContext,
                  req: &AdminRequest)
                  -> Result<(AdminResponse, Option<ExecResult>)> {
        PEER_ADMIN_CMD_COUNTER_VEC.with_label_values(&["split", "all"]).inc();

        let split_req = req.get_split();
        let right_derive = split_req.get_right_derive();
        if !split_req.has_split_key() {
            return Err(box_err!("missing split key"));
        }

        let split_key = split_req.get_split_key();
        let mut region = self.region.clone();
        if split_key <= region.get_start_key() {
            return Err(box_err!("invalid split request: {:?}", split_req));
        }

        try!(util::check_key_in_region(split_key, &region));

        info!("{} split at key: {}, region: {:?}",
              self.tag,
              escape(split_key),
              region);

        // TODO: check new region id validation.
        let new_region_id = split_req.get_new_region_id();

        // After split, the left region key range is [start_key, split_key),
        // the right region is [split_key, end).
        let mut new_region = region.clone();
        new_region.set_id(new_region_id);
        if right_derive {
            region.set_start_key(split_key.to_vec());
            new_region.set_end_key(split_key.to_vec());
        } else {
            region.set_end_key(split_key.to_vec());
            new_region.set_start_key(split_key.to_vec());
        }

        // Update new region peer ids.
        let new_peer_ids = split_req.get_new_peer_ids();
        if new_peer_ids.len() != new_region.get_peers().len() {
            return Err(box_err!("invalid new peer id count, need {}, but got {}",
                                new_region.get_peers().len(),
                                new_peer_ids.len()));
        }

        for (peer, &peer_id) in new_region.mut_peers().iter_mut().zip(new_peer_ids) {
            peer.set_id(peer_id);
        }

        // update region version
        let region_ver = region.get_region_epoch().get_version() + 1;
        region.mut_region_epoch().set_version(region_ver);
        new_region.mut_region_epoch().set_version(region_ver);
        write_peer_state(&ctx.wb, &region, PeerState::Normal)
            .and_then(|_| write_peer_state(&ctx.wb, &new_region, PeerState::Normal))
            .and_then(|_| write_initial_state(self.engine.as_ref(), &ctx.wb, new_region.get_id()))
            .unwrap_or_else(|e| {
                panic!("{} failed to save split region {:?}: {:?}",
                       self.tag,
                       new_region,
                       e)
            });

        let mut resp = AdminResponse::new();
        if right_derive {
            resp.mut_split().set_left(new_region.clone());
            resp.mut_split().set_right(region.clone());
        } else {
            resp.mut_split().set_left(region.clone());
            resp.mut_split().set_right(new_region.clone());
        }

        PEER_ADMIN_CMD_COUNTER_VEC.with_label_values(&["split", "success"]).inc();

        if right_derive {
            Ok((resp,
                Some(ExecResult::SplitRegion {
                left: new_region,
                right: region,
                right_derive: true,
            })))
        } else {
            Ok((resp,
                Some(ExecResult::SplitRegion {
                left: region,
                right: new_region,
                right_derive: false,
            })))
        }
    }

    fn exec_compact_log(&mut self,
                        ctx: &mut ExecContext,
                        req: &AdminRequest)
                        -> Result<(AdminResponse, Option<ExecResult>)> {
        PEER_ADMIN_CMD_COUNTER_VEC.with_label_values(&["compact", "all"]).inc();

        let compact_index = req.get_compact_log().get_compact_index();
        let resp = AdminResponse::new();

        let first_index = peer_storage::first_index(&ctx.apply_state);
        if compact_index <= first_index {
            debug!("{} compact index {} <= first index {}, no need to compact",
                   self.tag,
                   compact_index,
                   first_index);
            return Ok((resp, None));
        }

        let compact_term = req.get_compact_log().get_compact_term();
        // TODO: add unit tests to cover all the message integrity checks.
        if compact_term == 0 {
            info!("{} compact term missing in {:?}, skip.",
                  self.tag,
                  req.get_compact_log());
            // old format compact log command, safe to ignore.
            return Err(box_err!("command format is outdated, please upgrade leader."));
        }

        // compact failure is safe to be omitted, no need to assert.
        try!(compact_raft_log(&self.tag, &mut ctx.apply_state, compact_index, compact_term));

        PEER_ADMIN_CMD_COUNTER_VEC.with_label_values(&["compact", "success"]).inc();

        Ok((resp,
            Some(ExecResult::CompactLog {
            state: ctx.apply_state.get_truncated_state().clone(),
            first_index: first_index,
        })))
    }

    fn exec_write_cmd(&mut self, ctx: &ExecContext) -> Result<RaftCmdResponse> {
        let requests = ctx.req.get_requests();
        let mut responses = Vec::with_capacity(requests.len());

        for req in requests {
            let cmd_type = req.get_cmd_type();
            let mut resp = try!(match cmd_type {
                CmdType::Get => self.handle_get(ctx, req),
                CmdType::Put => self.handle_put(ctx, req),
                CmdType::Delete => self.handle_delete(ctx, req),
                CmdType::Snap => self.handle_snap(ctx, req),
                CmdType::Invalid => Err(box_err!("invalid cmd type, message maybe currupted")),
            });

            resp.set_cmd_type(cmd_type);

            responses.push(resp);
        }

        let mut resp = RaftCmdResponse::new();
        resp.set_responses(RepeatedField::from_vec(responses));
        Ok(resp)
    }

    fn handle_get(&mut self, ctx: &ExecContext, req: &Request) -> Result<Response> {
        do_get(&self.tag, &self.region, &ctx.snap, req)
    }

    fn handle_put(&mut self, ctx: &ExecContext, req: &Request) -> Result<Response> {
        let (key, value) = (req.get_put().get_key(), req.get_put().get_value());
        try!(check_data_key(key, &self.region));

        let resp = Response::new();
        let key = keys::data_key(key);
        self.metrics.size_diff_hint += key.len() as i64;
        self.metrics.size_diff_hint += value.len() as i64;
        if req.get_put().has_cf() {
            let cf = req.get_put().get_cf();
            // TODO: don't allow write preseved cfs.
            if cf == CF_LOCK {
                self.metrics.lock_cf_written_bytes += key.len() as u64;
                self.metrics.lock_cf_written_bytes += value.len() as u64;
            }
            // TODO: check whether cf exists or not.
            rocksdb::get_cf_handle(&self.engine, cf)
                .and_then(|handle| ctx.wb.put_cf(handle, &key, value))
                .unwrap_or_else(|e| {
                    panic!("{} failed to write ({}, {}) to cf {}: {:?}",
                           self.tag,
                           escape(&key),
                           escape(value),
                           cf,
                           e)
                });
        } else {
            ctx.wb.put(&key, value).unwrap_or_else(|e| {
                panic!("{} failed to write ({}, {}): {:?}",
                       self.tag,
                       escape(&key),
                       escape(value),
                       e);
            });
        }
        Ok(resp)
    }

    fn handle_delete(&mut self, ctx: &ExecContext, req: &Request) -> Result<Response> {
        let key = req.get_delete().get_key();
        try!(check_data_key(key, &self.region));

        let key = keys::data_key(key);
        // since size_diff_hint is not accurate, so we just skip calculate the value size.
        self.metrics.size_diff_hint -= key.len() as i64;
        let resp = Response::new();
        if req.get_delete().has_cf() {
            let cf = req.get_delete().get_cf();
            // TODO: check whether cf exists or not.
            rocksdb::get_cf_handle(&self.engine, cf)
                .and_then(|handle| ctx.wb.delete_cf(handle, &key))
                .unwrap_or_else(|e| {
                    panic!("{} failed to delete {}: {:?}", self.tag, escape(&key), e)
                });

            if cf == CF_LOCK {
                // delete is a kind of write for RocksDB.
                self.metrics.lock_cf_written_bytes += key.len() as u64;
            } else {
                self.metrics.delete_keys_hint += 1;
            }
        } else {
            ctx.wb.delete(&key).unwrap_or_else(|e| {
                panic!("{} failed to delete {}: {:?}", self.tag, escape(&key), e)
            });
            self.metrics.delete_keys_hint += 1;
        }

        Ok(resp)
    }

    fn handle_snap(&mut self, _: &ExecContext, _: &Request) -> Result<Response> {
        do_snap(self.region.clone())
    }
}

pub fn get_change_peer_cmd(msg: &RaftCmdRequest) -> Option<&ChangePeerRequest> {
    if !msg.has_admin_request() {
        return None;
    }
    let req = msg.get_admin_request();
    if !req.has_change_peer() {
        return None;
    }

    Some(req.get_change_peer())
}

fn check_data_key(key: &[u8], region: &Region) -> Result<()> {
    // region key range has no data prefix, so we must use origin key to check.
    try!(util::check_key_in_region(key, region));

    Ok(())
}

pub fn do_get(tag: &str, region: &Region, snap: &Snapshot, req: &Request) -> Result<Response> {
    // TODO: the get_get looks wried, maybe we should figure out a better name later.
    let key = req.get_get().get_key();
    try!(check_data_key(key, &region));

    let mut resp = Response::new();
    let res = if req.get_get().has_cf() {
        let cf = req.get_get().get_cf();
        // TODO: check whether cf exists or not.
        snap.get_value_cf(cf, &keys::data_key(key)).unwrap_or_else(|e| {
            panic!("{} failed to get {} with cf {}: {:?}",
                   tag,
                   escape(key),
                   cf,
                   e)
        })
    } else {
        snap.get_value(&keys::data_key(key))
            .unwrap_or_else(|e| panic!("{} failed to get {}: {:?}", tag, escape(key), e))
    };
    if let Some(res) = res {
        resp.mut_get().set_value(res.to_vec());
    }

    Ok(resp)
}

pub fn do_snap(region: Region) -> Result<Response> {
    let mut resp = Response::new();
    resp.mut_snap().set_region(region);
    Ok(resp)
}

// Consistency Check
impl ApplyDelegate {
    fn exec_compute_hash(&self,
                         ctx: &ExecContext,
                         _: &AdminRequest)
                         -> Result<(AdminResponse, Option<ExecResult>)> {
        let resp = AdminResponse::new();
        Ok((resp,
            Some(ExecResult::ComputeHash {
            region: self.region.clone(),
            index: ctx.index,
            // This snapshot may be held for a long time, which may cause too many
            // open files in rocksdb.
            // TODO: figure out another way to do consistency check without snapshot
            // or short life snapshot.
            snap: Snapshot::new(self.engine.clone()),
        })))
    }

    fn exec_verify_hash(&self,
                        _: &ExecContext,
                        req: &AdminRequest)
                        -> Result<(AdminResponse, Option<ExecResult>)> {
        let verify_req = req.get_verify_hash();
        let index = verify_req.get_index();
        let hash = verify_req.get_hash().to_vec();
        let resp = AdminResponse::new();
        Ok((resp,
            Some(ExecResult::VerifyHash {
            index: index,
            hash: hash,
        })))
    }
}

pub struct Apply {
    region_id: u64,
    term: u64,
    entries: Vec<Entry>,
}

#[derive(Default, Clone)]
pub struct Registration {
    pub id: u64,
    pub term: u64,
    pub apply_state: RaftApplyState,
    pub applied_index_term: u64,
    pub region: Region,
}

impl Registration {
    pub fn new(peer: &Peer) -> Registration {
        Registration {
            id: peer.peer_id(),
            term: peer.term(),
            apply_state: peer.get_store().apply_state.clone(),
            applied_index_term: peer.get_store().applied_index_term,
            region: peer.region().clone(),
        }
    }
}

pub struct Propose {
    id: u64,
    region_id: u64,
    is_conf_change: bool,
    uuid: Uuid,
    term: u64,
    cb: Callback,
}

pub struct Destroy {
    region_id: u64,
}

/// region related task.
pub enum Task {
    Apply(Apply),
    Registration(Registration),
    Propose(Propose),
    Destroy(Destroy),
}

impl Task {
    pub fn apply(region_id: u64, term: u64, entries: Vec<Entry>) -> Task {
        Task::Apply(Apply {
            region_id: region_id,
            term: term,
            entries: entries,
        })
    }

    pub fn register(peer: &Peer) -> Task {
        Task::Registration(Registration::new(peer))
    }

    pub fn propose(id: u64,
                   region_id: u64,
                   uuid: Uuid,
                   is_conf_change: bool,
                   term: u64,
                   cb: Callback)
                   -> Task {
        Task::Propose(Propose {
            id: id,
            region_id: region_id,
            uuid: uuid,
            term: term,
            is_conf_change: is_conf_change,
            cb: cb,
        })
    }

    pub fn destroy(region_id: u64) -> Task {
        Task::Destroy(Destroy { region_id: region_id })
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::Apply(ref a) => write!(f, "[region {}] async apply", a.region_id),
            Task::Propose(ref p) => write!(f, "[region {}] propose", p.region_id),
            Task::Registration(ref r) => {
                write!(f, "[region {}] Reg {:?}", r.region.get_id(), r.apply_state)
            }
            Task::Destroy(ref d) => write!(f, "[region {}] destroy", d.region_id),
        }
    }
}

#[derive(Default, Clone, Debug, PartialEq)]
pub struct ApplyMetrics {
    /// an inaccurate difference in region size since last reset.
    pub size_diff_hint: i64,
    /// delete keys' count since last reset.
    pub delete_keys_hint: u64,

    pub written_bytes: u64,
    pub written_keys: u64,
    pub lock_cf_written_bytes: u64,
}

#[derive(Debug)]
pub struct ApplyRes {
    pub region_id: u64,
    pub apply_state: RaftApplyState,
    pub applied_index_term: u64,
    pub exec_res: Vec<ExecResult>,
    pub metrics: ApplyMetrics,
}

#[derive(Debug)]
pub enum TaskRes {
    Apply(ApplyRes),
    Destroy(ApplyDelegate),
}

// TODO: use threadpool to do task concurrently
pub struct Runner {
    db: Arc<DB>,
    delegates: HashMap<u64, ApplyDelegate>,
    notifier: Sender<TaskRes>,
}

impl Runner {
    pub fn new<T, C>(store: &Store<T, C>, notifier: Sender<TaskRes>) -> Runner {
        let mut delegates = HashMap::with_capacity(store.get_peers().len());
        for (&region_id, p) in store.get_peers() {
            delegates.insert(region_id, ApplyDelegate::from_peer(p));
        }
        Runner {
            db: store.engine(),
            delegates: delegates,
            notifier: notifier,
        }
    }

    fn handle_apply(&mut self, apply: Apply) {
        if apply.entries.is_empty() {
            return;
        }
        let mut e = match self.delegates.entry(apply.region_id) {
            MapEntry::Vacant(_) => {
                error!("[region {}] is missing", apply.region_id);
                return;
            }
            MapEntry::Occupied(e) => e,
        };
        {
            let delegate = e.get_mut();
            delegate.metrics = ApplyMetrics::default();
            delegate.term = apply.term;
            let results = delegate.handle_raft_committed_entries(apply.entries);

            if delegate.pending_remove {
                delegate.destroy();
            }

            self.notifier
                .send(TaskRes::Apply(ApplyRes {
                    region_id: apply.region_id,
                    apply_state: delegate.apply_state.clone(),
                    exec_res: results,
                    metrics: delegate.metrics.clone(),
                    applied_index_term: delegate.applied_index_term,
                }))
                .unwrap();
        }
        if e.get().pending_remove {
            e.remove();
        }
    }

    fn handle_propose(&mut self, p: Propose) {
        let cmd = PendingCmd::new(p.uuid, p.term, p.cb);
        let delegate = match self.delegates.get_mut(&p.region_id) {
            Some(d) => d,
            None => {
                notify_region_removed(p.region_id, p.id, cmd);
                return;
            }
        };
        assert_eq!(delegate.id, p.id);
        if p.is_conf_change {
            if let Some(cmd) = delegate.pending_cmds.take_conf_change() {
                // if it loses leadership before conf change is replicated, there may be
                // a stale pending conf change before next conf change is applied. If it
                // becomes leader again with the stale pending conf change, will enter
                // this block, so we notify leadership may have been changed.
                notify_stale_command(&delegate.tag, delegate.term, cmd);
            }
            delegate.pending_cmds.set_conf_change(cmd);
        } else {
            delegate.pending_cmds.append_normal(cmd);
        }
    }

    fn handle_registration(&mut self, s: Registration) {
        let peer_id = s.id;
        let region_id = s.region.get_id();
        let term = s.term;
        let delegate = ApplyDelegate::from_registration(self.db.clone(), s);
        if let Some(mut old_delegate) = self.delegates.insert(region_id, delegate) {
            assert_eq!(old_delegate.id, peer_id);
            old_delegate.term = term;
            old_delegate.clear_all_commands_as_stale();
        }
    }

    fn handle_destroy(&mut self, d: Destroy) {
        // Only respond when the meta exists. Otherwise if destroy is triggered
        // multiple times, the store may destroy wrong target peer.
        if let Some(mut meta) = self.delegates.remove(&d.region_id) {
            meta.destroy();
            self.notifier.send(TaskRes::Destroy(meta)).unwrap();
        }
    }

    fn handle_shutdown(&mut self) {
        for p in self.delegates.values_mut() {
            p.clear_pending_commands();
        }
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        match task {
            Task::Apply(a) => self.handle_apply(a),
            Task::Propose(p) => self.handle_propose(p),
            Task::Registration(s) => self.handle_registration(s),
            Task::Destroy(d) => self.handle_destroy(d),
        }
    }

    fn shutdown(&mut self) {
        self.handle_shutdown();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::*;

    use tempdir::TempDir;
    use rocksdb::DB;
    use protobuf::Message;
    use kvproto::metapb::RegionEpoch;
    use kvproto::raft_cmdpb::CmdType;

    use super::*;
    use storage::{CF_WRITE, ALL_CFS};
    use util::collections::HashMap;

    pub fn create_tmp_engine(path: &str) -> (TempDir, Arc<DB>) {
        let path = TempDir::new(path).unwrap();
        let db = Arc::new(rocksdb::new_engine(path.path().to_str().unwrap(), ALL_CFS).unwrap());
        (path, db)
    }

    fn new_runner(db: Arc<DB>, tx: Sender<TaskRes>) -> Runner {
        Runner {
            db: db,
            delegates: HashMap::new(),
            notifier: tx,
        }
    }

    pub fn new_entry(term: u64, index: u64, req: Option<RaftCmdRequest>) -> Entry {
        let mut e = Entry::new();
        e.set_index(index);
        e.set_term(term);
        req.map(|r| e.set_data(r.write_to_bytes().unwrap()));
        e
    }

    #[test]
    fn test_basic_flow() {
        let (tx, rx) = mpsc::channel();
        let (_tmp, db) = create_tmp_engine("apply-basic");
        let mut runner = new_runner(db.clone(), tx);

        let mut reg = Registration::default();
        reg.id = 1;
        reg.region.set_id(2);
        reg.apply_state.set_applied_index(3);
        reg.term = 4;
        reg.applied_index_term = 5;
        runner.run(Task::Registration(reg.clone()));
        assert!(runner.delegates.get(&2).is_some());
        {
            let delegate = &runner.delegates[&2];
            assert_eq!(delegate.id, 1);
            assert_eq!(delegate.tag, "[region 2] 1");
            assert_eq!(delegate.region, reg.region);
            assert!(!delegate.pending_remove);
            assert_eq!(delegate.apply_state, reg.apply_state);
            assert_eq!(delegate.term, reg.term);
            assert_eq!(delegate.applied_index_term, reg.applied_index_term);
        }

        let (resp_tx, resp_rx) = mpsc::channel();
        runner.run(Task::propose(1,
                                 1,
                                 Uuid::new_v4(),
                                 false,
                                 0,
                                 box move |resp| {
                                     resp_tx.send(resp).unwrap();
                                 }));
        // unregistered region should be ignored and notify failed.
        assert!(rx.try_recv().is_err());
        let resp = resp_rx.try_recv().unwrap();
        assert!(resp.get_header().get_error().has_region_not_found());

        let normal_uuid = Uuid::new_v4();
        runner.run(Task::propose(1, 2, normal_uuid, false, 0, box |_| {}));
        assert!(rx.try_recv().is_err());
        {
            let normals = &runner.delegates[&2].pending_cmds.normals;
            assert_eq!(normals.back().map(|c| c.uuid), Some(normal_uuid));
        }

        let (cc_tx, cc_rx) = mpsc::channel();
        let cc_uuid = Uuid::new_v4();
        runner.run(Task::propose(1,
                                 2,
                                 cc_uuid,
                                 true,
                                 0,
                                 box move |resp| {
                                     cc_tx.send(resp).unwrap();
                                 }));
        assert!(rx.try_recv().is_err());
        {
            let cc = &runner.delegates[&2].pending_cmds.conf_change;
            assert_eq!(cc.as_ref().map(|c| c.uuid), Some(cc_uuid));
        }

        let cc_uuid2 = Uuid::new_v4();
        runner.run(Task::propose(1, 2, cc_uuid2, true, 0, box move |_| {}));
        assert!(rx.try_recv().is_err());
        {
            let cc = &runner.delegates[&2].pending_cmds.conf_change;
            assert_eq!(cc.as_ref().map(|c| c.uuid), Some(cc_uuid2));
        }
        // propose another conf change should mark previous stale.
        let cc_resp = cc_rx.try_recv().unwrap();
        assert!(cc_resp.get_header().get_error().has_stale_command());

        runner.run(Task::apply(1, 1, vec![new_entry(2, 3, None)]));
        // non registered region should be ignored.
        assert!(rx.try_recv().is_err());

        runner.run(Task::apply(2, 11, vec![]));
        // empty entries should be ignored.
        assert!(rx.try_recv().is_err());
        assert_eq!(runner.delegates[&2].term, reg.term);

        let apply_state_key = keys::apply_state_key(2);
        assert!(db.get(&apply_state_key).unwrap().is_none());
        runner.run(Task::apply(2, 11, vec![new_entry(5, 4, None)]));
        let apply_res = match rx.try_recv() {
            Ok(TaskRes::Apply(res)) => res,
            e => panic!("unexpected apply result: {:?}", e),
        };
        assert_eq!(apply_res.region_id, 2);
        assert_eq!(apply_res.apply_state.get_applied_index(), 4);
        assert!(apply_res.exec_res.is_empty());
        // empty entry should not change metrics.
        assert_eq!(apply_res.metrics, ApplyMetrics::default());
        assert_eq!(apply_res.applied_index_term, 5);
        {
            let delegate = &runner.delegates[&2];
            assert_eq!(delegate.term, 11);
            assert_eq!(delegate.applied_index_term, 5);
            assert_eq!(delegate.apply_state.get_applied_index(), 4);
            let apply_state: RaftApplyState =
                db.get_msg_cf(CF_RAFT, &apply_state_key).unwrap().unwrap();
            assert_eq!(apply_state, delegate.apply_state);
        }

        runner.run(Task::destroy(2));
        let destroy_res = match rx.try_recv() {
            Ok(TaskRes::Destroy(d)) => d,
            e => panic!("expected destroy result, but got {:?}", e),
        };
        assert_eq!(destroy_res.id, 1);
        assert_eq!(destroy_res.applied_index_term, 5);

        runner.shutdown();
    }

    struct EntryBuilder {
        entry: Entry,
        req: RaftCmdRequest,
    }

    impl EntryBuilder {
        fn new(index: u64, term: u64) -> EntryBuilder {
            let mut req = RaftCmdRequest::new();
            req.mut_header().set_uuid(Uuid::new_v4().as_bytes().to_vec());
            let mut entry = Entry::new();
            entry.set_index(index);
            entry.set_term(term);
            EntryBuilder {
                entry: entry,
                req: req,
            }
        }

        fn capture_resp(self,
                        delegate: &mut ApplyDelegate,
                        tx: Sender<RaftCmdResponse>)
                        -> EntryBuilder {
            let uuid = Uuid::from_bytes(self.req.get_header().get_uuid()).unwrap();
            let cmd = PendingCmd::new(uuid,
                                      self.entry.get_term(),
                                      box move |r| tx.send(r).unwrap());
            delegate.pending_cmds.append_normal(cmd);
            self
        }

        fn epoch(mut self, conf_ver: u64, version: u64) -> EntryBuilder {
            let mut epoch = RegionEpoch::new();
            epoch.set_version(version);
            epoch.set_conf_ver(conf_ver);
            self.req.mut_header().set_region_epoch(epoch);
            self
        }

        fn put(self, key: &[u8], value: &[u8]) -> EntryBuilder {
            self.add_put_req(None, key, value)
        }

        fn put_cf(self, cf: &str, key: &[u8], value: &[u8]) -> EntryBuilder {
            self.add_put_req(Some(cf), key, value)
        }

        fn add_put_req(mut self, cf: Option<&str>, key: &[u8], value: &[u8]) -> EntryBuilder {
            let mut cmd = Request::new();
            cmd.set_cmd_type(CmdType::Put);
            if let Some(cf) = cf {
                cmd.mut_put().set_cf(cf.to_owned());
            }
            cmd.mut_put().set_key(key.to_vec());
            cmd.mut_put().set_value(value.to_vec());
            self.req.mut_requests().push(cmd);
            self
        }

        fn delete(self, key: &[u8]) -> EntryBuilder {
            self.add_delete_req(None, key)
        }

        fn delete_cf(self, cf: &str, key: &[u8]) -> EntryBuilder {
            self.add_delete_req(Some(cf), key)
        }

        fn add_delete_req(mut self, cf: Option<&str>, key: &[u8]) -> EntryBuilder {
            let mut cmd = Request::new();
            cmd.set_cmd_type(CmdType::Delete);
            if let Some(cf) = cf {
                cmd.mut_delete().set_cf(cf.to_owned());
            }
            cmd.mut_delete().set_key(key.to_vec());
            self.req.mut_requests().push(cmd);
            self
        }

        fn build(mut self) -> Entry {
            self.entry.set_data(self.req.write_to_bytes().unwrap());
            self.entry
        }
    }

    #[test]
    fn test_handle_raft_entry_normal() {
        let (_path, db) = create_tmp_engine("test-delegate");
        let mut reg = Registration::default();
        reg.region.set_end_key(b"k5".to_vec());
        reg.region.mut_region_epoch().set_version(3);
        let mut delegate = ApplyDelegate::from_registration(db.clone(), reg);
        let (tx, rx) = mpsc::channel();

        let put_entry = EntryBuilder::new(1, 1)
            .put(b"k1", b"v1")
            .epoch(1, 3)
            .capture_resp(&mut delegate, tx.clone())
            .build();
        let res = delegate.handle_raft_entry_normal(put_entry);
        assert!(res.is_none());
        let resp = rx.try_recv().unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        assert_eq!(resp.get_responses().len(), 1);
        let dk_k1 = keys::data_key(b"k1");
        assert_eq!(db.get(&dk_k1).unwrap().unwrap(), b"v1");
        assert_eq!(delegate.applied_index_term, 1);
        assert_eq!(delegate.apply_state.get_applied_index(), 1);

        let lock_written_bytes = delegate.metrics.lock_cf_written_bytes;
        let written_bytes = delegate.metrics.written_bytes;
        let written_keys = delegate.metrics.written_keys;
        let size_diff_hint = delegate.metrics.size_diff_hint;
        let put_entry = EntryBuilder::new(2, 2).put_cf(CF_LOCK, b"k1", b"v1").epoch(1, 3).build();
        delegate.handle_raft_entry_normal(put_entry);
        let lock_handle = db.cf_handle(CF_LOCK).unwrap();
        assert_eq!(db.get_cf(lock_handle, &dk_k1).unwrap().unwrap(), b"v1");
        assert_eq!(delegate.metrics.lock_cf_written_bytes,
                   lock_written_bytes + 5);
        assert!(delegate.metrics.written_bytes >= written_bytes + 5);
        assert_eq!(delegate.metrics.written_keys, written_keys + 2);
        assert_eq!(delegate.metrics.size_diff_hint, size_diff_hint + 5);
        assert_eq!(delegate.applied_index_term, 2);
        assert_eq!(delegate.apply_state.get_applied_index(), 2);

        let put_entry = EntryBuilder::new(3, 2)
            .put(b"k2", b"v2")
            .epoch(1, 1)
            .capture_resp(&mut delegate, tx.clone())
            .build();
        delegate.handle_raft_entry_normal(put_entry);
        let resp = rx.try_recv().unwrap();
        assert!(resp.get_header().get_error().has_stale_epoch());
        assert_eq!(delegate.applied_index_term, 2);
        assert_eq!(delegate.apply_state.get_applied_index(), 3);

        let put_entry = EntryBuilder::new(4, 2)
            .put(b"k3", b"v3")
            .put(b"k5", b"v5")
            .epoch(1, 3)
            .capture_resp(&mut delegate, tx.clone())
            .build();
        delegate.handle_raft_entry_normal(put_entry);
        let resp = rx.try_recv().unwrap();
        assert!(resp.get_header().get_error().has_key_not_in_region());
        assert_eq!(delegate.applied_index_term, 2);
        assert_eq!(delegate.apply_state.get_applied_index(), 4);
        let dk_k3 = keys::data_key(b"k3");
        // a writebatch should be atomic.
        assert!(db.get(&dk_k3).unwrap().is_none());

        EntryBuilder::new(5, 2).capture_resp(&mut delegate, tx.clone()).build();
        let put_entry = EntryBuilder::new(6, 2)
            .delete(b"k1")
            .delete_cf(CF_LOCK, b"k1")
            .delete_cf(CF_WRITE, b"k1")
            .epoch(1, 3)
            .capture_resp(&mut delegate, tx.clone())
            .build();
        let lock_written_bytes = delegate.metrics.lock_cf_written_bytes;
        let delete_keys_hint = delegate.metrics.delete_keys_hint;
        let size_diff_hint = delegate.metrics.size_diff_hint;
        delegate.handle_raft_entry_normal(put_entry);
        let resp = rx.try_recv().unwrap();
        // stale command should be cleared.
        assert!(resp.get_header().get_error().has_stale_command());
        let resp = rx.try_recv().unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        assert!(db.get(&dk_k1).unwrap().is_none());
        assert_eq!(delegate.metrics.lock_cf_written_bytes,
                   lock_written_bytes + 3);
        assert_eq!(delegate.metrics.delete_keys_hint, delete_keys_hint + 2);
        assert_eq!(delegate.metrics.size_diff_hint, size_diff_hint - 9);

        let delete_entry = EntryBuilder::new(7, 2)
            .delete(b"k5")
            .epoch(1, 3)
            .capture_resp(&mut delegate, tx.clone())
            .build();
        delegate.handle_raft_entry_normal(delete_entry);
        let resp = rx.try_recv().unwrap();
        assert!(resp.get_header().get_error().has_key_not_in_region());
    }
}
