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
use std::sync::mpsc::Sender;
use std::fmt::{self, Display, Formatter};
use std::collections::{HashMap, HashSet, VecDeque};
use std::collections::hash_map::Entry as MapEntry;
use std::vec::Vec;
use std::default::Default;

use rocksdb::{DB, WriteBatch, Writable};
use protobuf::{self, Message, MessageStatic};
use uuid::Uuid;

use kvproto::metapb::{Peer, Region};
use kvproto::eraftpb::{Entry, EntryType, ConfChangeType, ConfChange};
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse, ChangePeerRequest, CmdType,
                          AdminCmdType, Request, Response, AdminRequest, AdminResponse};
use kvproto::raft_serverpb::{RaftApplyState, RaftTruncatedState, PeerState};
use raftstore::{Result, Error};
use util::{escape, SlowTimer, rocksdb};
use util::worker::Runnable;
use storage::{CF_RAFT, CF_LOCK};
use raftstore::store::Store;
use raftstore::store::peer_storage::{self, write_initial_state, write_peer_state};
use raftstore::store::util;
use raftstore::store::msg::Callback;
use raftstore::store::cmd_resp;
use raftstore::store::keys;
use raftstore::store::engine::{Snapshot, Peekable, Mutable};
use raftstore::store::metrics::*;

fn get_change_peer_cmd(msg: &RaftCmdRequest) -> Option<&ChangePeerRequest> {
    if !msg.has_admin_request() {
        return None;
    }
    let req = msg.get_admin_request();
    if !req.has_change_peer() {
        return None;
    }

    Some(req.get_change_peer())
}

/// Call the callback of `cmd` that the region is removed.
fn notify_region_removed(region_id: u64, peer_id: u64, mut cmd: PendingCmd) {
    let region_not_found = Error::RegionNotFound(region_id);
    let mut resp = cmd_resp::new_error(region_not_found);
    cmd_resp::bind_uuid(&mut resp, cmd.uuid);
    debug!("[region {}] {} is removed, notify {}.",
           region_id,
           peer_id,
           cmd.uuid);
    cmd.call(resp);
}

/// Call the callback of `cmd` when it can not be continue processed.
fn notify_stale_command(region_id: u64, term: u64, mut cmd: PendingCmd) {
    let resp = cmd_resp::err_resp(Error::StaleCommand, cmd.uuid, term);
    info!("[region {}] command {} is stale, skip", region_id, cmd.uuid);
    cmd.call(resp);
}

pub struct Apply {
    region_id: u64,
    term: u64,
    entries: Vec<Entry>,
}

pub struct Read {
    region_id: u64,
    term: u64,
    cmd: RaftCmdRequest,
    cb: Callback,
}

pub struct Registration {
    id: u64,
    term: u64,
    apply_state: RaftApplyState,
    applied_index_term: u64,
    region_id: u64,
    region: Region,
}

pub struct Propose {
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
    Read(Read),
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

    pub fn read(region_id: u64, term: u64, cmd: RaftCmdRequest, cb: Callback) -> Task {
        Task::Read(Read {
            region_id: region_id,
            term: term,
            cmd: cmd,
            cb: cb,
        })
    }

    pub fn register(id: u64,
                    term: u64,
                    region_id: u64,
                    apply_state: RaftApplyState,
                    applied_index_term: u64,
                    region: Region)
                    -> Task {
        Task::Registration(Registration {
            id: id,
            term: term,
            region_id: region_id,
            apply_state: apply_state,
            applied_index_term: applied_index_term,
            region: region,
        })
    }

    pub fn propose(region_id: u64,
                   uuid: Uuid,
                   is_conf_change: bool,
                   term: u64,
                   cb: Callback)
                   -> Task {
        Task::Propose(Propose {
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
            Task::Read(ref r) => write!(f, "[region {}] read", r.region_id),
            Task::Registration(ref r) => {
                write!(f, "[region {}] snap {:?}", r.region_id, r.apply_state)
            }
            Task::Destroy(ref d) => write!(f, "[region {}] destroy", d.region_id),
        }
    }
}

pub struct ApplyRes {
    pub region_id: u64,
    pub apply_state: RaftApplyState,
    pub size_diff_hint: i64,
    pub delete_keys_hint: usize,
    pub applied_index_term: u64,
    pub exec_res: Vec<ExecResult>,
}

pub enum TaskRes {
    Apply(ApplyRes),
    Destroy(PeerMeta),
}

pub struct PendingCmd {
    pub uuid: Uuid,
    pub term: u64,
    pub cb: Option<Callback>,
}

impl PendingCmd {
    #[inline]
    fn call(&mut self, resp: RaftCmdResponse) {
        self.cb.take().unwrap().call_box((resp,));
    }
}

impl Drop for PendingCmd {
    fn drop(&mut self) {
        if self.cb.is_some() {
            panic!("callback of {} is leak.", self.uuid);
        }
    }
}

#[derive(Default)]
struct PendingCmdQueue {
    normals: VecDeque<PendingCmd>,
    conf_change: Option<PendingCmd>,
    uuids: HashSet<Uuid>,
}

impl PendingCmdQueue {
    fn remove(&mut self, cmd: &Option<PendingCmd>) {
        if let Some(ref cmd) = *cmd {
            self.uuids.remove(&cmd.uuid);
        }
    }

    fn pop_normal(&mut self, term: u64) -> Option<PendingCmd> {
        self.normals.pop_front().and_then(|cmd| {
            if cmd.term > term {
                self.normals.push_front(cmd);
                return None;
            }
            let res = Some(cmd);
            self.remove(&res);
            res
        })
    }

    fn append_normal(&mut self, cmd: PendingCmd) {
        self.uuids.insert(cmd.uuid);
        self.normals.push_back(cmd);
    }

    fn take_conf_change(&mut self) -> Option<PendingCmd> {
        // conf change will not be affected when changing between follower and leader,
        // so there is no need to check term.
        let cmd = self.conf_change.take();
        self.remove(&cmd);
        cmd
    }

    fn set_conf_change(&mut self, cmd: PendingCmd) {
        self.uuids.insert(cmd.uuid);
        self.conf_change = Some(cmd);
    }
}

#[derive(Debug)]
pub enum ExecResult {
    ChangePeer {
        cc: Option<ConfChange>,
        meta: Option<(Peer, Region)>,
    },
    CompactLog {
        state: RaftTruncatedState,
        first_index: u64,
    },
    SplitRegion { left: Region, right: Region },
    ComputeHash {
        region: Region,
        index: u64,
        snap: Snapshot,
    },
    VerifyHash { index: u64, hash: Vec<u8> },
}

// TODO: make sure received entries are not corrupted
// If this happens, TiKV will panic and can't recover without extra effort.
#[inline]
fn parse_data_at<T: Message + MessageStatic>(data: &[u8], index: u64, region_id: u64) -> T {
    protobuf::parse_from_bytes::<T>(data).unwrap_or_else(|e| {
        panic!("[region {}] data is corrupted at {}: {:?}",
               region_id,
               index,
               e);
    })
}

struct ExecContext<'a> {
    pub db: Arc<DB>,
    pub snap: Snapshot,
    pub apply_state: RaftApplyState,
    pub wb: WriteBatch,
    pub req: &'a RaftCmdRequest,
    pub index: u64,
    pub term: u64,
}

impl<'a> ExecContext<'a> {
    fn new<'b>(meta: &'b PeerMeta,
               db: Arc<DB>,
               index: u64,
               term: u64,
               req: &'a RaftCmdRequest)
               -> ExecContext<'a> {
        ExecContext {
            db: db.clone(),
            snap: Snapshot::new(db),
            apply_state: meta.apply_state.clone(),
            wb: WriteBatch::new(),
            req: req,
            index: index,
            term: term,
        }
    }

    fn save(&self, region_id: u64) -> Result<()> {
        let raft_cf = try!(self.snap.cf_handle(CF_RAFT));
        try!(self.wb.put_msg_cf(raft_cf,
                                &keys::apply_state_key(region_id),
                                &self.apply_state));
        Ok(())
    }
}

pub struct PeerMeta {
    pub id: u64,
    pub region_id: u64,
    apply_state: RaftApplyState,
    applied_index_term: u64,
    region: Region,
    pending_remove: bool,
    delete_keys_hint: usize,
    size_diff_hint: i64,
    pending_cmds: PendingCmdQueue,
    term: u64,
}

impl PeerMeta {
    fn new(id: u64,
           term: u64,
           region_id: u64,
           apply_state: RaftApplyState,
           applied_index_term: u64,
           region: Region)
           -> PeerMeta {
        PeerMeta {
            id: id,
            term: term,
            region_id: region_id,
            apply_state: apply_state,
            applied_index_term: applied_index_term,
            size_diff_hint: 0,
            delete_keys_hint: 0,
            region: region,
            pending_remove: false,
            pending_cmds: Default::default(),
        }
    }

    fn handle_raft_entry_normal(&mut self, db: Arc<DB>, entry: &Entry) -> Option<ExecResult> {
        let index = entry.get_index();
        let term = entry.get_term();
        let data = entry.get_data();

        if !data.is_empty() {
            let cmd = parse_data_at(data, index, self.region_id);
            return self.process_raft_cmd(db, index, term, cmd);
        }

        // when a peer become leader, it will send an empty entry.
        let wb = WriteBatch::new();
        let mut state = self.apply_state.clone();
        state.set_applied_index(index);
        rocksdb::get_cf_handle(&db, CF_RAFT)
            .map_err(From::from)
            .and_then(|handle| {
                wb.put_msg_cf(handle, &keys::apply_state_key(self.region_id), &state)
            })
            .and_then(|_| db.write(wb).map_err(From::from))
            .unwrap_or_else(|e| {
                panic!("[region {}] failed to apply empty entry at {}: {:?}",
                       self.region_id,
                       index,
                       e);
            });
        self.apply_state = state;
        self.applied_index_term = term;
        assert!(term > 0);
        while let Some(cmd) = self.pending_cmds.pop_normal(term - 1) {
            // apprently, all the callbacks whose term is less than entry's term are stale.
            notify_stale_command(self.region_id, self.term, cmd);
        }
        None
    }

    fn handle_raft_entry_conf_change(&mut self, db: Arc<DB>, entry: &Entry) -> Option<ExecResult> {
        let index = entry.get_index();
        let term = entry.get_term();
        let conf_change: ConfChange = parse_data_at(entry.get_data(), index, self.region_id);
        let cmd = parse_data_at(conf_change.get_context(), index, self.region_id);
        Some(self.process_raft_cmd(db, index, term, cmd).map_or_else(|| {
                                                                         ExecResult::ChangePeer {
                                                                             cc: None,
                                                                             meta: None,
                                                                         }
                                                                     },
                                                                     |r| {
            if let ExecResult::ChangePeer { meta, .. } = r {
                ExecResult::ChangePeer {
                    meta: meta,
                    cc: Some(conf_change),
                }
            } else {
                panic!("unexpected result: {:?}", r);
            }
        }))
    }

    fn process_raft_cmd(&mut self,
                        db: Arc<DB>,
                        index: u64,
                        term: u64,
                        cmd: RaftCmdRequest)
                        -> Option<ExecResult> {
        if index == 0 {
            panic!("[region {}] processing raft command needs a none zero index",
                   self.region_id);
        }

        let uuid = util::get_uuid_from_req(&cmd).unwrap();
        let cmd_cb = self.find_cb(uuid, term, &cmd);
        let timer = PEER_APPLY_LOG_HISTOGRAM.start_timer();
        let (mut resp, exec_result) = self.apply_raft_cmd(db, index, term, &cmd);
        timer.observe_duration();

        debug!("[region {}] applied command with uuid {:?} at log index {}",
               self.region_id,
               uuid,
               index);

        let cb = match cmd_cb {
            None => return exec_result,
            Some(cb) => cb,
        };

        // TODO: if we have exec_result, maybe we should return this callback too. Outer
        // store will call it after handing exec result.
        // Bind uuid here.
        cmd_resp::bind_uuid(&mut resp, uuid);
        cmd_resp::bind_term(&mut resp, self.term);
        cb.call_box((resp,));

        exec_result
    }

    fn clear_all_commands_as_stale(&mut self) {
        while let Some(cmd) = self.pending_cmds.normals.pop_front() {
            notify_stale_command(self.region_id, self.term, cmd);
        }
        if let Some(cmd) = self.pending_cmds.conf_change.take() {
            notify_stale_command(self.region_id, self.term, cmd);
        }
    }

    fn find_cb(&mut self, uuid: Uuid, term: u64, cmd: &RaftCmdRequest) -> Option<Callback> {
        if get_change_peer_cmd(cmd).is_some() {
            if let Some(mut cmd) = self.pending_cmds.take_conf_change() {
                if cmd.uuid == uuid {
                    return Some(cmd.cb.take().unwrap());
                } else {
                    notify_stale_command(self.region_id, self.term, cmd);
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
            notify_stale_command(self.region_id, self.term, head);
        }
        None
    }

    // apply operation can fail as following situation:
    //   1. encouter an error that will occur on all store, it can continue
    // applying next entry safely, like stale epoch for example;
    //   2. encouter an error that may not occur on all store, in this case
    // we should try to apply the entry again or panic. Considering that this
    // usually due to disk operation fail, which is rare, so just panic is ok.
    fn apply_raft_cmd(&mut self,
                      db: Arc<DB>,
                      index: u64,
                      term: u64,
                      req: &RaftCmdRequest)
                      -> (RaftCmdResponse, Option<ExecResult>) {
        // if pending remove, apply should be aborted already.
        assert!(!self.pending_remove);

        let mut ctx = ExecContext::new(self, db.clone(), index, term, req);
        let (resp, exec_result) = self.exec_raft_cmd(&mut ctx).unwrap_or_else(|e| {
            error!("[region {}] execute raft command err: {:?}",
                   self.region_id,
                   e);
            (cmd_resp::new_error(e), None)
        });

        ctx.apply_state.set_applied_index(index);
        if !self.pending_remove {
            ctx.save(self.region_id)
                .unwrap_or_else(|e| {
                    panic!("[region {}] failed to save apply context: {:?}",
                           self.region_id,
                           e)
                });
        }

        // Commit write and change storage fields atomically.
        db.write(ctx.wb)
            .unwrap_or_else(|e| {
                panic!("[region {}] failed to commit apply result: {:?}",
                       self.region_id,
                       e)
            });

        self.apply_state = ctx.apply_state;
        self.applied_index_term = term;

        if let Some(ref exec_result) = exec_result {
            match *exec_result {
                ExecResult::ChangePeer { ref meta, .. } => {
                    self.region = meta.as_ref().unwrap().1.clone();
                }
                ExecResult::ComputeHash { .. } |
                ExecResult::VerifyHash { .. } |
                ExecResult::CompactLog { .. } => {}
                ExecResult::SplitRegion { ref left, .. } => {
                    self.region = left.clone();
                }
            }
        }

        (resp, exec_result)
    }

    pub fn check_epoch(&self, req: &RaftCmdRequest) -> Result<()> {
        let (mut check_ver, mut check_conf_ver) = (false, false);
        if req.has_admin_request() {
            match req.get_admin_request().get_cmd_type() {
                AdminCmdType::CompactLog |
                AdminCmdType::InvalidAdmin |
                AdminCmdType::ComputeHash |
                AdminCmdType::VerifyHash => {}
                AdminCmdType::Split => check_ver = true,
                AdminCmdType::ChangePeer => check_conf_ver = true,
                AdminCmdType::TransferLeader => {
                    check_ver = true;
                    check_conf_ver = true;
                }
            };
        } else {
            // for get/set/delete, we don't care conf_version.
            check_ver = true;
        }

        if !check_ver && !check_conf_ver {
            return Ok(());
        }

        if !req.get_header().has_region_epoch() {
            return Err(box_err!("missing epoch!"));
        }

        let from_epoch = req.get_header().get_region_epoch();
        let latest_region = &self.region;
        let latest_epoch = latest_region.get_region_epoch();

        // should we use not equal here?
        if (check_conf_ver && from_epoch.get_conf_ver() < latest_epoch.get_conf_ver()) ||
           (check_ver && from_epoch.get_version() < latest_epoch.get_version()) {
            debug!("[region {}] received stale epoch {:?}, mime: {:?}",
                   self.region_id,
                   from_epoch,
                   latest_epoch);
            return Err(Error::StaleEpoch(format!("latest_epoch of region {} is {:?}, but you \
                                                  sent {:?}",
                                                 self.region_id,
                                                 latest_epoch,
                                                 from_epoch),
                                         vec![self.region.clone()]));
        }

        Ok(())
    }

    // Only errors that will also occur on all other stores should be returned.
    fn exec_raft_cmd(&mut self,
                     ctx: &mut ExecContext)
                     -> Result<(RaftCmdResponse, Option<ExecResult>)> {
        try!(self.check_epoch(ctx.req));
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
        info!("[region {}] execute admin command {:?} at [term: {}, index: {}]",
              self.region_id,
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

        info!("[region {}] exec ConfChange {:?}, epoch: {:?}",
              self.region_id,
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
                    error!("[region {}] can't add duplicated peer {:?} to region {:?}",
                           self.region_id,
                           peer,
                           self.region);
                    return Err(box_err!("can't add duplicated peer {:?} to region {:?}",
                                        peer,
                                        self.region));
                }
                // TODO: Do we allow adding peer in same node?
                region.mut_peers().push(peer.clone());

                PEER_ADMIN_CMD_COUNTER_VEC.with_label_values(&["add_peer", "success"]).inc();

                info!("[region {}] add peer {:?} to region {:?}",
                      self.region_id,
                      peer,
                      self.region);
            }
            ConfChangeType::RemoveNode => {
                PEER_ADMIN_CMD_COUNTER_VEC.with_label_values(&["remove_peer", "all"]).inc();

                if !exists {
                    error!("[region {}] remove missing peer {:?} from region {:?}",
                           self.region_id,
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

                info!("[region {}] remove {} from region:{:?}",
                      self.region_id,
                      peer.get_id(),
                      self.region);
            }
        }

        write_peer_state(&ctx.wb, &region, PeerState::Normal).unwrap_or_else(|e| {
            panic!("[region {}] failed to update region state: {:?}",
                   self.region_id,
                   e)
        });

        let mut resp = AdminResponse::new();
        resp.mut_change_peer().set_region(region.clone());

        Ok((resp,
            Some(ExecResult::ChangePeer {
            cc: None,
            meta: Some((peer.to_owned(), region)),
        })))
    }

    fn exec_split(&mut self,
                  ctx: &ExecContext,
                  req: &AdminRequest)
                  -> Result<(AdminResponse, Option<ExecResult>)> {
        PEER_ADMIN_CMD_COUNTER_VEC.with_label_values(&["split", "all"]).inc();

        let split_req = req.get_split();
        if !split_req.has_split_key() {
            return Err(box_err!("missing split key"));
        }

        let split_key = split_req.get_split_key();
        let mut region = self.region.clone();
        if split_key <= region.get_start_key() {
            return Err(box_err!("invalid split request: {:?}", split_req));
        }

        try!(util::check_key_in_region(split_key, &region));

        info!("[region {}] split at key: {}, region: {:?}",
              self.region_id,
              escape(split_key),
              region);

        // TODO: check new region id validation.
        let new_region_id = split_req.get_new_region_id();

        // After split, the origin region key range is [start_key, split_key),
        // the new split region is [split_key, end).
        let mut new_region = region.clone();
        region.set_end_key(split_key.to_vec());

        new_region.set_start_key(split_key.to_vec());
        new_region.set_id(new_region_id);

        // Update new region peer ids.
        let new_peer_ids = split_req.get_new_peer_ids();
        if new_peer_ids.len() != new_region.get_peers().len() {
            return Err(box_err!("invalid new peer id count, need {}, but got {}",
                                new_region.get_peers().len(),
                                new_peer_ids.len()));
        }

        for (index, peer) in new_region.mut_peers().iter_mut().enumerate() {
            let peer_id = new_peer_ids[index];
            peer.set_id(peer_id);
        }

        // update region version
        let region_ver = region.get_region_epoch().get_version() + 1;
        region.mut_region_epoch().set_version(region_ver);
        new_region.mut_region_epoch().set_version(region_ver);
        write_peer_state(&ctx.wb, &region, PeerState::Normal)
            .and_then(|_| write_peer_state(&ctx.wb, &new_region, PeerState::Normal))
            .and_then(|_| write_initial_state(&ctx.db, &ctx.wb, new_region.get_id()))
            .unwrap_or_else(|e| {
                panic!("[region {}] failed to save split region {:?}: {:?}",
                       self.region_id,
                       new_region,
                       e)
            });

        let mut resp = AdminResponse::new();
        resp.mut_split().set_left(region.clone());
        resp.mut_split().set_right(new_region.clone());

        PEER_ADMIN_CMD_COUNTER_VEC.with_label_values(&["split", "success"]).inc();

        Ok((resp,
            Some(ExecResult::SplitRegion {
            left: region,
            right: new_region,
        })))
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
            debug!("[region {}] compact index {} <= first index {}, no need to compact",
                   self.region_id,
                   compact_index,
                   first_index);
            return Ok((resp, None));
        }

        let compact_term = req.get_compact_log().get_compact_term();
        if compact_term == 0 {
            info!("[region {}] compact term missing in {:?}, skip.",
                  self.region_id,
                  req.get_compact_log());
            // old format compact log command, safe to ignore.
            return Err(box_err!("command format is outdated, please upgrade leader."));
        }

        // compact failure is safe to be omitted, no need to assert.
        try!(peer_storage::compact_raft_log(self.region_id,
                                            &mut ctx.apply_state,
                                            compact_index,
                                            compact_term));

        PEER_ADMIN_CMD_COUNTER_VEC.with_label_values(&["compact", "success"]).inc();

        Ok((resp,
            Some(ExecResult::CompactLog {
            first_index: first_index,
            state: ctx.apply_state.get_truncated_state().clone(),
        })))
    }

    fn exec_write_cmd(&mut self, ctx: &ExecContext) -> Result<RaftCmdResponse> {
        let requests = ctx.req.get_requests();
        let mut responses = Vec::with_capacity(requests.len());

        for req in requests {
            let cmd_type = req.get_cmd_type();
            let mut resp = try!(match cmd_type {
                CmdType::Get => self.do_get(ctx, req),
                CmdType::Put => self.do_put(ctx, req),
                CmdType::Delete => self.do_delete(ctx, req),
                CmdType::Snap => self.do_snap(ctx, req),
                CmdType::Invalid => Err(box_err!("invalid cmd type, message maybe currupted")),
            });

            resp.set_cmd_type(cmd_type);

            responses.push(resp);
        }

        let mut resp = RaftCmdResponse::new();
        resp.set_responses(protobuf::RepeatedField::from_vec(responses));
        Ok(resp)
    }

    fn check_data_key(&self, key: &[u8]) -> Result<()> {
        // region key range has no data prefix, so we must use origin key to check.
        try!(util::check_key_in_region(key, &self.region));

        Ok(())
    }

    fn do_get(&mut self, ctx: &ExecContext, req: &Request) -> Result<Response> {
        // TODO: the get_get looks wried, maybe we should figure out a better name later.
        let key = req.get_get().get_key();
        try!(self.check_data_key(key));

        let mut resp = Response::new();
        let res = if req.get_get().has_cf() {
            let cf = req.get_get().get_cf();
            // TODO: check whether cf exists or not.
            ctx.snap.get_value_cf(cf, &keys::data_key(key)).unwrap_or_else(|e| {
                panic!("[region {}] failed to get {} with cf {}: {:?}",
                       self.region_id,
                       escape(key),
                       cf,
                       e)
            })
        } else {
            ctx.snap
                .get_value(&keys::data_key(key))
                .unwrap_or_else(|e| {
                    panic!("[region {}] failed to get {}: {:?}",
                           self.region_id,
                           escape(key),
                           e)
                })
        };
        if let Some(res) = res {
            resp.mut_get().set_value(res.to_vec());
        }

        Ok(resp)
    }

    fn do_put(&mut self, ctx: &ExecContext, req: &Request) -> Result<Response> {
        let (key, value) = (req.get_put().get_key(), req.get_put().get_value());
        try!(self.check_data_key(key));

        let resp = Response::new();
        let key = keys::data_key(key);
        self.size_diff_hint += key.len() as i64;
        self.size_diff_hint += value.len() as i64;
        if req.get_put().has_cf() {
            let cf = req.get_put().get_cf();
            // TODO: check whether cf exists or not.
            rocksdb::get_cf_handle(&ctx.db, cf)
                .and_then(|handle| ctx.wb.put_cf(handle, &key, value))
                .unwrap_or_else(|e| {
                    panic!("[region {}] failed to write ({}, {}) to cf {}: {:?}",
                           self.region_id,
                           escape(&key),
                           escape(value),
                           cf,
                           e)
                });
        } else {
            ctx.wb.put(&key, value).unwrap_or_else(|e| {
                panic!("[region {}] failed to write ({}, {}): {:?}",
                       self.region_id,
                       escape(&key),
                       escape(value),
                       e);
            });
        }
        Ok(resp)
    }

    fn do_delete(&mut self, ctx: &ExecContext, req: &Request) -> Result<Response> {
        let key = req.get_delete().get_key();
        try!(self.check_data_key(key));

        let key = keys::data_key(key);
        // since size_diff_hint is not accurate, so we just skip calculate the value size.
        self.size_diff_hint -= key.len() as i64;
        let resp = Response::new();
        if req.get_delete().has_cf() {
            let cf = req.get_delete().get_cf();
            // TODO: check whether cf exists or not.
            rocksdb::get_cf_handle(&ctx.db, cf)
                .and_then(|handle| ctx.wb.delete_cf(handle, &key))
                .unwrap_or_else(|e| {
                    panic!("[region {}] failed to delete {}: {:?}",
                           self.region_id,
                           escape(&key),
                           e)
                });
            // lock cf is compact periodically.
            if cf != CF_LOCK {
                self.delete_keys_hint += 1;
            }
        } else {
            ctx.wb.delete(&key).unwrap_or_else(|e| {
                panic!("[region {}] failed to delete {}: {:?}",
                       self.region_id,
                       escape(&key),
                       e)
            });
            self.delete_keys_hint += 1;
        }

        Ok(resp)
    }

    fn do_snap(&mut self, _: &ExecContext, _: &Request) -> Result<Response> {
        let mut resp = Response::new();
        resp.mut_snap().set_region(self.region.clone());
        Ok(resp)
    }

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
            snap: Snapshot::new(ctx.db.clone()),
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

    /// Clear all the pending commands.
    ///
    /// Please note that all the pending callbacks will be lost.
    /// Should not do this when dropping a peer in case of possible leak.
    pub fn clear_pending_commands(&mut self) {
        if !self.pending_cmds.normals.is_empty() {
            info!("[region {}] clear {} commands",
                  self.region_id,
                  self.pending_cmds.normals.len());
            while let Some(mut cmd) = self.pending_cmds.normals.pop_front() {
                cmd.cb.take();
            }
        }
        if let Some(mut cmd) = self.pending_cmds.conf_change.take() {
            info!("[region {}] clear pending conf change", self.region_id);
            cmd.cb.take();
        }
    }

    fn destroy(&mut self) {
        for cmd in self.pending_cmds.normals.drain(..) {
            notify_region_removed(self.region_id, self.id, cmd);
        }
        if let Some(cmd) = self.pending_cmds.conf_change.take() {
            notify_region_removed(self.region_id, self.id, cmd);
        }
    }
}

// TODO: use threadpool to do task concurrently
pub struct Runner {
    db: Arc<DB>,
    peers: HashMap<u64, PeerMeta>,
    notifier: Sender<TaskRes>,
}

impl Runner {
    pub fn new<T, C>(store: &Store<T, C>, notifier: Sender<TaskRes>) -> Runner {
        let mut peers = HashMap::with_capacity(store.region_peers.len());
        for (&region_id, p) in &store.region_peers {
            let store = p.get_store();
            peers.insert(region_id,
                         PeerMeta::new(p.peer_id(),
                                       p.term(),
                                       region_id,
                                       store.apply_state.clone(),
                                       store.applied_index_term,
                                       p.region().clone()));
        }
        Runner {
            db: store.engine(),
            peers: peers,
            notifier: notifier,
        }
    }

    fn handle_apply(&mut self, apply: Apply) {
        if apply.entries.is_empty() {
            return;
        }
        // If we send multiple ConfChange commands, only first one will be proposed correctly,
        // others will be saved as a normal entry with no data, so we must re-propose these
        // commands again.
        let t = SlowTimer::new();
        let mut results = vec![];
        let committed_count = apply.entries.len();
        let mut e = match self.peers.entry(apply.region_id) {
            MapEntry::Vacant(_) => {
                error!("[region {}] is missing", apply.region_id);
                return;
            }
            MapEntry::Occupied(e) => e,
        };
        {
            let meta = e.get_mut();
            meta.size_diff_hint = 0;
            meta.delete_keys_hint = 0;
            meta.term = apply.term;
            for entry in apply.entries {
                if meta.pending_remove {
                    // This peer is about to be destroyed, skip everything.
                    break;
                }

                let expect_index = meta.apply_state.get_applied_index() + 1;
                if expect_index != entry.get_index() {
                    panic!("[region {}] expect index {}, but got {}",
                           apply.region_id,
                           expect_index,
                           entry.get_index());
                }

                let res = match entry.get_entry_type() {
                    EntryType::EntryNormal => {
                        meta.handle_raft_entry_normal(self.db.clone(), &entry)
                    }
                    EntryType::EntryConfChange => {
                        meta.handle_raft_entry_conf_change(self.db.clone(), &entry)
                    }
                };

                if let Some(res) = res {
                    results.push(res);
                }
            }

            if meta.pending_remove {
                meta.destroy();
            }

            slow_log!(t,
                      "[region {}] handle ready {} committed entries",
                      apply.region_id,
                      committed_count);

            self.notifier
                .send(TaskRes::Apply(ApplyRes {
                    region_id: apply.region_id,
                    apply_state: meta.apply_state.clone(),
                    exec_res: results,
                    size_diff_hint: meta.size_diff_hint,
                    delete_keys_hint: meta.delete_keys_hint,
                    applied_index_term: meta.applied_index_term,
                }))
                .unwrap();
        }
        if e.get().pending_remove {
            e.remove();
        }
    }

    fn handle_propose(&mut self, p: Propose) {
        let cmd = PendingCmd {
            uuid: p.uuid,
            term: p.term,
            cb: Some(p.cb),
        };
        let meta = self.peers.get_mut(&p.region_id).unwrap();
        if p.is_conf_change {
            if let Some(cmd) = meta.pending_cmds.take_conf_change() {
                // if it loses leadership before conf change is replicated, there may be
                // a stale pending conf change before next conf change is applied. If it
                // becomes leader again with the stale pending conf change, will enter
                // this block, so we notify leadership may have changed.
                notify_stale_command(meta.region_id, meta.term, cmd);
            }
            meta.pending_cmds.set_conf_change(cmd);
        } else {
            meta.pending_cmds.append_normal(cmd);
        }
    }

    fn handle_read(&mut self, r: Read) {
        let meta = self.peers.get_mut(&r.region_id).unwrap();
        let mut ctx = ExecContext::new(meta, self.db.clone(), 0, 0, &r.cmd);
        let (mut resp, _) = meta.exec_raft_cmd(&mut ctx).unwrap_or_else(|e| {
            error!("[region {}] execute raft command err: {:?}", r.region_id, e);
            (cmd_resp::new_error(e), None)
        });

        cmd_resp::bind_uuid(&mut resp, util::get_uuid_from_req(&r.cmd).unwrap());
        cmd_resp::bind_term(&mut resp, r.term);
        (r.cb)(resp);
    }

    fn handle_registration(&mut self, s: Registration) {
        let meta = PeerMeta::new(s.id,
                                 s.term,
                                 s.region_id,
                                 s.apply_state,
                                 s.applied_index_term,
                                 s.region);
        if let Some(mut old_meta) = self.peers.insert(s.region_id, meta) {
            old_meta.term = s.term;
            old_meta.clear_all_commands_as_stale();
        }
    }

    fn handle_destroy(&mut self, d: Destroy) {
        // Only respond when the meta exists. Otherwise if destroy is triggered
        // multiple times, the store may destroy wrong target peer.
        if let Some(mut meta) = self.peers.remove(&d.region_id) {
            meta.destroy();
            self.notifier.send(TaskRes::Destroy(meta)).unwrap();
        }
    }

    fn handle_shutdown(&mut self) {
        for p in self.peers.values_mut() {
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
            Task::Read(r) => self.handle_read(r),
            Task::Destroy(d) => self.handle_destroy(d),
        }
    }

    fn shutdown(&mut self) {
        self.handle_shutdown();
    }
}
