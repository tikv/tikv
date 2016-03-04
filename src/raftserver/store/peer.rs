use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use std::vec::Vec;
use std::default::Default;

use rocksdb::{DB, WriteBatch, Writable};
use rocksdb::rocksdb::Snapshot;
use protobuf::{self, Message};
use uuid::Uuid;

use proto::metapb;
use proto::raftpb::{self, ConfChangeType};
use proto::raft_cmdpb::{RaftCommandRequest, RaftCommandResponse, ChangePeerRequest};
use proto::raft_cmdpb::{self as cmd, Request, Response, AdminRequest, AdminResponse};
use proto::raft_serverpb::{RaftMessage, RaftTruncatedState};
use raft::{self, Ready, RawNode, SnapshotStatus};
use raftserver::{Result, other};
use raftserver::coprocessor::CoprocessorHost;
use super::store::Store;
use super::peer_storage::{self, PeerStorage, RaftStorage, ApplySnapResult};
use super::util;
use super::msg::Callback;
use super::cmd_resp;
use super::transport::Transport;
use super::keys;
use super::engine::{Peekable, Iterable, Mutable};

#[derive(Default)]
pub struct PendingCmd {
    pub uuid: Uuid,
    pub cb: Option<Callback>,
    // Sometimes we should re-propose pending command (only ConfChnage).
    pub cmd: Option<RaftCommandRequest>,
}

pub enum ExecResult {
    ChangePeer {
        change_type: ConfChangeType,
        peer: metapb::Peer,
        region: metapb::Region,
    },
    CompactLog {
        state: RaftTruncatedState,
    },
}

// When we apply commands in handing ready, we should also need a way to
// let outer store do something after handing ready over.
// We can save these intermediate results in ready result.
// We only need to care administration commands now.
pub struct ReadyResult {
    // We can execute multi commands like 1, conf change, 2 split region, ...
    // in one ready, and outer store should handle these results sequentially too.
    pub exec_results: Vec<ExecResult>,
}

pub struct Peer {
    engine: Arc<DB>,
    pub peer: metapb::Peer,
    region_id: u64,
    leader_id: u64,
    pub raft_group: RawNode<RaftStorage>,
    pub storage: Arc<RaftStorage>,
    pub pending_cmds: HashMap<Uuid, PendingCmd>,
    peer_cache: Arc<RwLock<HashMap<u64, metapb::Peer>>>,
    coprocessor_host: CoprocessorHost,
}

impl Peer {
    // If we create the peer actively, like bootstrap/split/merge region, we should
    // use this function to create the peer. The region must contain the peer info
    // for this store.
    pub fn create<T: Transport>(store: &mut Store<T>, region: metapb::Region) -> Result<Peer> {
        let store_id = store.get_store_id();
        let peer_id = match util::find_peer(&region, store_id) {
            None => {
                return Err(other(format!("find no peer for store {} in region {:?}",
                                         store_id,
                                         region)))
            }
            Some(peer) => peer.get_peer_id(),
        };

        Peer::new(store, region, peer_id)
    }

    // The peer can be created from another node with raft membership changes, and we only
    // know the region_id and peer_id when creating this replicated peer, the region info
    // will be retrieved later after appling snapshot.
    pub fn replicate<T: Transport>(store: &mut Store<T>,
                                   region_id: u64,
                                   peer_id: u64)
                                   -> Result<Peer> {
        // we must first check the peer id validation.
        let last_max_id = try!(store.get_engine().get_u64(&keys::region_tombstone_key(region_id)));
        if let Some(last_max_id) = last_max_id {
            if peer_id <= last_max_id {
                // We receive a stale message and we can't re-create the peer with the peer id.
                return Err(other(format!("peer id {} <= tombstone id {}", peer_id, last_max_id)));
            }
        }

        let mut region = metapb::Region::new();
        region.set_region_id(region_id);
        Peer::new(store, region, peer_id)
    }

    fn new<T: Transport>(store: &mut Store<T>,
                         region: metapb::Region,
                         peer_id: u64)
                         -> Result<Peer> {
        if peer_id == raft::INVALID_ID {
            return Err(other("invalid peer id"));
        }

        let store_id = store.get_store_id();

        let s = try!(PeerStorage::new(store.get_engine(), &region));

        let applied_index = s.applied_index();
        let storage = Arc::new(RaftStorage::new(s));

        let cfg = store.get_config();
        let raft_cfg = raft::Config {
            id: peer_id,
            peers: vec![],
            election_tick: cfg.raft_election_timeout_ticks,
            heartbeat_tick: cfg.raft_heartbeat_ticks,
            max_size_per_msg: cfg.raft_max_size_per_msg,
            max_inflight_msgs: cfg.raft_max_inflight_msgs,
            applied: applied_index,
            check_quorum: false,
        };

        let raft_group = try!(RawNode::new(&raft_cfg, storage.clone(), &[]));

        let node_id = store.get_node_id();
        let mut peer = Peer {
            engine: store.get_engine(),
            peer: util::new_peer(node_id, store_id, peer_id),
            region_id: region.get_region_id(),
            leader_id: raft::INVALID_ID,
            storage: storage,
            raft_group: raft_group,
            pending_cmds: HashMap::new(),
            peer_cache: store.get_peer_cache(),
            coprocessor_host: CoprocessorHost::new(),
        };

        peer.load_all_coprocessors();

        // If this region has only one peer and I am the one, campaign directly.
        if region.get_peers().len() == 1 && region.get_peers()[0].get_store_id() == store_id {
            try!(peer.raft_group.campaign());
        }

        Ok(peer)
    }

    pub fn destroy(&mut self) -> Result<()> {
        // Delete all data in this peer.
        let batch = WriteBatch::new();
        try!(self.storage.wl().scan_region(self.engine.as_ref(),
                                           &mut |key, _| -> Result<bool> {
                                               try!(batch.delete(key));
                                               Ok(true)
                                           }));

        // set the tombstone key here.
        try!(batch.put_u64(&keys::region_tombstone_key(self.region_id),
                           self.get_region().get_max_peer_id()));

        try!(self.engine.write(batch));

        self.coprocessor_host.shutdown();

        Ok(())
    }

    pub fn load_all_coprocessors(&mut self) {
        // TODO: load processors.
    }

    pub fn update_region(&mut self, region: &metapb::Region) -> Result<()> {
        if self.region_id != region.get_region_id() {
            return Err(other(format!("invalid region id {} != {}",
                                     region.get_region_id(),
                                     self.region_id)));
        }

        self.storage.wl().set_region(region);
        Ok(())
    }

    pub fn get_region(&self) -> metapb::Region {
        self.storage.rl().get_region().clone()
    }

    pub fn get_peer_id(&self) -> u64 {
        self.peer.get_peer_id()
    }

    pub fn get_raft_status(&self) -> raft::Status {
        self.raft_group.status()
    }

    pub fn get_leader(&self) -> u64 {
        self.leader_id
    }

    pub fn is_leader(&self) -> bool {
        self.leader_id == self.get_peer_id()
    }

    pub fn handle_raft_ready<T: Transport>(&mut self,
                                           trans: &Arc<RwLock<T>>)
                                           -> Result<Option<ReadyResult>> {
        if !self.raft_group.has_ready() {
            return Ok(None);
        }

        debug!("handle raft ready for peer {:?} at region {}",
               self.peer,
               self.region_id);

        let ready = self.raft_group.ready();

        if let Some(ref ss) = ready.ss {
            self.leader_id = ss.lead;
        }

        try!(self.handle_raft_ready_in_storage(&ready));

        for msg in &ready.messages {
            try!(self.send_raft_message(&msg, trans));
        }

        let exec_results = try!(self.handle_raft_commit_entries(&ready.committed_entries));

        self.raft_group.advance(ready);
        Ok(Some(ReadyResult { exec_results: exec_results }))
    }

    pub fn propose_pending_cmd(&mut self, pending_cmd: &mut PendingCmd) -> Result<()> {
        if pending_cmd.cmd.is_none() {
            // This may only occur in re-propose.
            debug!("pending command msg is none for region {} in peer {:?}",
                   self.region_id,
                   self.peer);
            return Ok(());
        }

        // We handle change_peer command as ConfChange entry, and others as normal entry.
        if let Some(change_peer) = get_change_peer_command(pending_cmd.cmd.as_ref().unwrap()) {
            let data = try!(pending_cmd.cmd.as_ref().unwrap().write_to_bytes());

            let mut cc = raftpb::ConfChange::new();
            cc.set_change_type(change_peer.get_change_type());
            cc.set_node_id(change_peer.get_peer().get_peer_id());
            cc.set_context(data);

            info!("propose conf change {:?} peer {:?} at region {}",
                  cc.get_change_type(),
                  cc.get_node_id(),
                  self.region_id);

            try!(self.raft_group.propose_conf_change(cc));
            return Ok(());
        }

        // TODO: validate request for unexpected changes.
        let mut cmd = pending_cmd.cmd.take().unwrap();
        self.coprocessor_host.pre_propose(&self.storage.rl(), &mut cmd);
        let data = try!(cmd.write_to_bytes());
        try!(self.raft_group.propose(data));

        Ok(())
    }

    fn handle_raft_ready_in_storage(&mut self, ready: &Ready) -> Result<()> {
        let batch = WriteBatch::new();
        let mut storage = self.storage.wl();
        let mut last_index = storage.last_index();
        let mut apply_snap_res: Option<ApplySnapResult> = None;
        if !raft::is_empty_snap(&ready.snapshot) {
            apply_snap_res = try!(storage.apply_snapshot(&batch, &ready.snapshot).map(|res| {
                last_index = res.last_index;
                Some(res)
            }));
        }

        if !ready.entries.is_empty() {
            last_index = try!(storage.append(&batch, last_index, &ready.entries));
        }

        if let Some(ref hs) = ready.hs {
            try!(peer_storage::save_hard_state(&batch, self.region_id, hs));
        }

        try!(self.engine.write(batch));

        storage.set_last_index(last_index);
        // If we apply snapshot ok, we should update some infos like applied index too.
        if let Some(res) = apply_snap_res {
            storage.set_applied_index(res.applied_index);
            storage.set_region(&res.region);
        }

        Ok(())
    }

    pub fn get_peer_from_cache(&self, peer_id: u64) -> Option<metapb::Peer> {
        if let Some(peer) = self.peer_cache.read().unwrap().get(&peer_id).cloned() {
            return Some(peer);
        }

        // Try to find in region, if found, set in cache.
        for peer in self.storage.rl().get_region().get_peers() {
            if peer.get_peer_id() == peer_id {
                self.peer_cache.write().unwrap().insert(peer_id, peer.clone());
                return Some(peer.clone());
            }
        }

        None
    }

    fn send_raft_message<T: Transport>(&mut self,
                                       msg: &raftpb::Message,
                                       trans: &Arc<RwLock<T>>)
                                       -> Result<()> {
        let msg_type = msg.get_msg_type();

        let mut send_msg = RaftMessage::new();
        send_msg.set_region_id(self.region_id);
        send_msg.set_message(msg.clone());

        let mut snap_status = SnapshotStatus::Finish;
        let mut unreachable = false;

        let trans = trans.read().unwrap();
        let from_peer = try!(self.get_peer_from_cache(msg.get_from()).ok_or_else(|| {
            other(format!("failed to lookup sender peer {} in region {}",
                          msg.get_from(),
                          self.region_id))
        }));

        let to_peer = try!(self.get_peer_from_cache(msg.get_to()).ok_or_else(|| {
            other(format!("failed to look up recipient peer {} in region {}",
                          msg.get_to(),
                          self.region_id))
        }));

        let to_peer_id = to_peer.get_peer_id();
        let to_store_id = to_peer.get_store_id();

        send_msg.set_from_peer(from_peer);
        send_msg.set_to_peer(to_peer);

        if let Err(e) = trans.send(send_msg) {
            warn!("region {} with peer {:?} failed to send msg to {} in store {}, err: {:?}",
                  self.region_id,
                  self.peer,
                  to_peer_id,
                  to_store_id,
                  e);

            unreachable = true;
            snap_status = SnapshotStatus::Failure;
        }

        if unreachable {
            self.raft_group.report_unreachable(to_peer_id);
        }

        if msg_type == raftpb::MessageType::MsgSnapshot {
            // TODO: report status until receiving ack, error or timeout.
            self.raft_group.report_snapshot(to_peer_id, snap_status);
        }

        Ok(())
    }

    fn handle_raft_commit_entries(&mut self,
                                  committed_entries: &[raftpb::Entry])
                                  -> Result<Vec<ExecResult>> {
        // If we send multi ConfChange commands, only first one will be proposed correctly,
        // others will be saved as a normal entry with no data, so we must re-propose these
        // commands again.
        let mut results = vec![];
        let mut need_repropose = false;
        for entry in committed_entries {
            let res = try!(match entry.get_entry_type() {
                raftpb::EntryType::EntryNormal => {
                    self.handle_raft_entry_normal(entry, &mut need_repropose)
                }
                raftpb::EntryType::EntryConfChange => self.handle_raft_entry_conf_change(entry),
            });

            if let Some(res) = res {
                results.push(res);
            }
        }

        if need_repropose {
            try!(self.repropose_pending_cmds());
        }

        Ok(results)
    }

    fn handle_raft_entry_normal(&mut self,
                                entry: &raftpb::Entry,
                                repropose: &mut bool)
                                -> Result<Option<ExecResult>> {
        let index = entry.get_index();
        let data = entry.get_data();
        if data.len() == 0 {
            *repropose = true;
            return Ok(None);
        }

        let cmd = try!(protobuf::parse_from_bytes::<RaftCommandRequest>(data));
        // no need to return error here.
        self.process_raft_command(index, cmd).or_else(|e| {
            error!("process raft command at index {} err: {:?}", index, e);
            Ok(None)
        })
    }

    fn handle_raft_entry_conf_change(&mut self,
                                     entry: &raftpb::Entry)
                                     -> Result<Option<ExecResult>> {

        let index = entry.get_index();
        let mut conf_change =
            try!(protobuf::parse_from_bytes::<raftpb::ConfChange>(entry.get_data()));

        let cmd = try!(protobuf::parse_from_bytes::<RaftCommandRequest>(conf_change.get_context()));
        let res = self.process_raft_command(index, cmd).or_else(|e| {
            error!("process raft command at index {} err: {:?}", index, e);
            // If failed, tell raft that the config change was aborted.
            conf_change = raftpb::ConfChange::new();
            Ok(None)
        });

        self.raft_group.apply_conf_change(conf_change);
        res
    }

    fn repropose_pending_cmds(&mut self) -> Result<()> {
        if !self.pending_cmds.is_empty() {
            info!("re-propose {} pending commands after empty entry",
                  self.pending_cmds.len());
            // TODO: use a better way to avoid clone.
            let mut cmds: Vec<PendingCmd> = Vec::with_capacity(self.pending_cmds.len());
            for cmd in self.pending_cmds.values() {
                // We only need cmd for later re-propose.
                cmds.push(PendingCmd { cmd: cmd.cmd.clone(), ..Default::default() });
            }

            for mut cmd in &mut cmds {
                try!(self.propose_pending_cmd(&mut cmd));
            }
        }
        Ok(())
    }

    fn process_raft_command(&mut self,
                            index: u64,
                            cmd: RaftCommandRequest)
                            -> Result<Option<ExecResult>> {
        if index == 0 {
            return Err(other("processing raft command needs a none zero index"));
        }

        let uuid = util::get_uuid_from_req(&cmd).unwrap();

        let pending_cmd = self.pending_cmds.remove(&uuid);

        let (mut resp, exec_result) = self.apply_raft_command(index, &cmd).unwrap_or_else(|e| {
            error!("apply raft command err {:?}", e);
            (cmd_resp::message_error(e), None)
        });

        if let Some(mut pending_cmd) = pending_cmd {
            self.coprocessor_host.post_apply(&self.storage.rl(), &cmd, &mut resp);
            if pending_cmd.cb.is_none() {
                warn!("pending command callback for entry {} is None", index);
            } else {
                // TODO: if we have exec_result, maybe we should return this callback too. Outer
                // store will call it after handing exec result.
                let cb = pending_cmd.cb.take().unwrap();
                // Bind uuid here.
                cmd_resp::bind_uuid(&mut resp, uuid);
                if let Err(e) = cb.call_box((resp,)) {
                    error!("callback err {:?}", e);
                }
            }
        }

        Ok(exec_result)
    }

    fn apply_raft_command(&mut self,
                          index: u64,
                          cmd: &RaftCommandRequest)
                          -> Result<(RaftCommandResponse, Option<ExecResult>)> {
        let last_applied_index = self.storage.rl().applied_index();

        if last_applied_index >= index {
            return Err(other(format!("applied index moved backwards, {} >= {}",
                                     last_applied_index,
                                     index)));
        }

        let wb = WriteBatch::new();
        let (mut resp, exec_result) = {
            let engine = self.engine.clone();
            let ctx = ExecContext {
                snap: engine.snapshot(),
                wb: &wb,
                request: cmd,
            };

            self.execute_raft_command(&ctx).unwrap_or_else(|e| {
                error!("execute raft command err: {:?}", e);
                (cmd_resp::message_error(e), None)
            })
        };

        peer_storage::save_applied_index(&wb, self.region_id, index)
            .expect("save applied index must not fail");

        match self.engine
                  .write(wb) {
            Ok(_) => {
                self.storage.wl().set_applied_index(index);

                if let Some(ref exec_result) = exec_result {
                    match *exec_result {
                        ExecResult::ChangePeer{ref region, ..} => {
                            self.storage.wl().set_region(region);
                        }
                        ExecResult::CompactLog{ref state} => {
                            self.storage.wl().set_truncated_state(state);
                            // TODO: we can set exec_result to None, because outer store
                            // doesn't need it.
                        }
                    }
                };
            }
            Err(e) => {
                error!("commit batch failed err {:?}", e);
                resp = cmd_resp::message_error(e);
            }
        };

        Ok((resp, exec_result))
    }
}

fn get_change_peer_command(msg: &RaftCommandRequest) -> Option<&ChangePeerRequest> {
    if !msg.has_admin_request() {
        return None;
    }
    let req = msg.get_admin_request();
    if !req.has_change_peer() {
        return None;
    }

    Some(req.get_change_peer())
}

struct ExecContext<'a> {
    pub snap: Snapshot<'a>,
    pub wb: &'a WriteBatch,
    pub request: &'a RaftCommandRequest,
}

// Here we implement all commands.
impl Peer {
    fn execute_raft_command(&mut self,
                            ctx: &ExecContext)
                            -> Result<(RaftCommandResponse, Option<ExecResult>)> {
        if ctx.request.has_admin_request() {
            self.execute_admin_command(ctx)
        } else {
            // Now we don't care write command outer, so use None.
            self.execute_write_command(ctx).and_then(|v| Ok((v, None)))
        }
    }

    fn execute_admin_command(&mut self,
                             ctx: &ExecContext)
                             -> Result<(RaftCommandResponse, Option<ExecResult>)> {
        let request = ctx.request.get_admin_request();
        let cmd_type = request.get_cmd_type();
        info!("execute admin command {:?} at region {}",
              request,
              self.region_id);

        let (mut response, exec_result) = try!(match cmd_type {
            cmd::AdminCommandType::ChangePeer => self.execute_change_peer(ctx, request),
            cmd::AdminCommandType::Split => self.execute_split(ctx, request),
            cmd::AdminCommandType::CompactLog => self.execute_compact_log(ctx, request),
            e => Err(other(format!("unsupported admin command type {:?}", e))),
        });
        response.set_cmd_type(cmd_type);

        let mut resp = RaftCommandResponse::new();
        resp.set_admin_response(response);
        Ok((resp, exec_result))
    }

    fn execute_change_peer(&mut self,
                           ctx: &ExecContext,
                           request: &AdminRequest)
                           -> Result<(AdminResponse, Option<ExecResult>)> {
        let request = request.get_change_peer();
        let peer = request.get_peer();
        let mut region = self.get_region().clone();
        let store_id = peer.get_store_id();
        // TODO: we should need more check, like peer validation, duplicated id, etc.
        let exists = util::find_peer(&region, store_id).is_some();
        let change_type = request.get_change_type();
        match change_type {
            raftpb::ConfChangeType::AddNode => {
                if exists {
                    return Err(other(format!("add duplicated peer {:?} to store {}",
                                             peer,
                                             store_id)));
                }

                if peer.get_peer_id() <= region.get_max_peer_id() {
                    return Err(other(format!("add peer id {} <= max region peer id {}",
                                             peer.get_peer_id(),
                                             region.get_max_peer_id())));
                }

                region.set_max_peer_id(peer.get_peer_id());

                // TODO: Do we allow adding peer in same node?

                // Add this peer to cache.
                self.peer_cache.write().unwrap().insert(peer.get_peer_id(), peer.clone());

                region.mut_peers().push(peer.clone());
            }
            raftpb::ConfChangeType::RemoveNode => {
                if !exists {
                    return Err(other(format!("remove missing peer {:?} from store {}",
                                             peer,
                                             store_id)));
                }

                // Remove this peer from cache.
                self.peer_cache.write().unwrap().remove(&peer.get_peer_id());

                util::remove_peer(&mut region, store_id).unwrap();
            }
        }

        try!(ctx.wb.put_msg(&keys::region_info_key(region.get_region_id()), &region));

        let mut resp = AdminResponse::new();
        resp.mut_change_peer().set_region(region.clone());

        Ok((resp,
            Some(ExecResult::ChangePeer {
            change_type: change_type,
            peer: peer.clone(),
            region: region,
        })))
    }

    fn execute_split(&mut self,
                     _: &ExecContext,
                     _: &AdminRequest)
                     -> Result<(AdminResponse, Option<ExecResult>)> {
        unimplemented!();
    }

    fn execute_compact_log(&mut self,
                           ctx: &ExecContext,
                           request: &AdminRequest)
                           -> Result<(AdminResponse, Option<ExecResult>)> {
        let request = request.get_compact_log();
        let compact_index = request.get_compact_index();
        let resp = AdminResponse::new();

        let first_index = self.storage.rl().first_index();
        if compact_index <= first_index {
            debug!("compact index {} <= first index {}, no need to compact",
                   compact_index,
                   first_index);
            return Ok((resp, None));
        }

        let state = try!(self.storage.rl().compact(ctx.wb, compact_index));
        Ok((resp, Some(ExecResult::CompactLog { state: state })))
    }

    fn execute_write_command(&mut self, ctx: &ExecContext) -> Result<RaftCommandResponse> {
        let requests = ctx.request.get_requests();

        let mut responses: Vec<Response> = Vec::with_capacity(requests.len());

        for request in requests {
            let cmd_type = request.get_cmd_type();
            let mut resp = try!(match cmd_type {
                cmd::CommandType::Get => self.execute_get(ctx, request),
                cmd::CommandType::Seek => self.execute_seek(ctx, request),
                cmd::CommandType::Put => self.execute_put(ctx, request),
                cmd::CommandType::Delete => self.execute_delete(ctx, request),
                e => Err(other(format!("unsupported command type {:?}", e))),
            });

            resp.set_cmd_type(cmd_type);

            responses.push(resp);
        }

        let mut resp = RaftCommandResponse::new();
        resp.set_responses(protobuf::RepeatedField::from_vec(responses));
        Ok(resp)
    }

    fn execute_get(&mut self, ctx: &ExecContext, request: &Request) -> Result<Response> {
        // TODO: the get_get looks wried, maybe we should think a better name later.
        let request = request.get_get();
        let key = request.get_key();
        try!(keys::validate_data_key(key));

        let mut resp = Response::new();
        let res: Option<Vec<u8>> = try!(ctx.snap.get_value(key).map(|r| r.map(|v| v.to_vec())));
        if let Some(res) = res {
            resp.mut_get().set_value(res)
        }

        Ok(resp)
    }

    fn execute_seek(&mut self, ctx: &ExecContext, request: &Request) -> Result<Response> {
        let request = request.get_seek();
        let key = request.get_key();
        try!(keys::validate_data_key(key));

        let mut resp = Response::new();
        let res: Option<(Vec<u8>, Vec<u8>)> = try!(ctx.snap.seek(key));
        if let Some(res) = res {
            resp.mut_seek().set_key(res.0);
            resp.mut_seek().set_value(res.1);
        }

        Ok(resp)
    }

    fn execute_put(&mut self, ctx: &ExecContext, request: &Request) -> Result<Response> {
        let request = request.get_put();
        let key = request.get_key();
        try!(keys::validate_data_key(key));

        let resp = Response::new();
        try!(ctx.wb.put(key, request.get_value()));

        // Should we call mut_put() explicitly?
        Ok(resp)
    }

    fn execute_delete(&mut self, ctx: &ExecContext, request: &Request) -> Result<Response> {
        let request = request.get_delete();
        let key = request.get_key();
        try!(keys::validate_data_key(key));

        let resp = Response::new();
        try!(ctx.wb.delete(key));

        // Should we call mut_delete() explicitly?
        Ok(resp)
    }
}
