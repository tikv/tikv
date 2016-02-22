use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use std::vec::Vec;
use std::default::Default;

use rocksdb::{DB, WriteBatch, Writable};
use rocksdb::rocksdb::Snapshot;
use protobuf::{self, Message};
use uuid::Uuid;

use proto::metapb;
use proto::raftpb;
use proto::raft_cmdpb::{RaftCommandRequest, RaftCommandResponse, ChangePeerRequest};
use proto::raft_cmdpb::{self as cmd, Request, Response, AdminRequest, AdminResponse};
use proto::raft_serverpb::RaftMessage;
use raft::{self, Ready, RawNode, SnapshotStatus};
use raftserver::{Result, other};
use super::store::Store;
use super::peer_storage::{self, PeerStorage, RaftStorage, ApplySnapResult};
use super::util;
use super::msg::Callback;
use super::cmd_resp;
use super::transport::Transport;
use super::keys;
use super::engine::Retriever;

#[derive(Default)]
pub struct PendingCmd {
    pub uuid: Uuid,
    pub cb: Option<Callback>,
    // Sometimes we should re-propose pending command (only ConfChnage).
    pub cmd: Option<RaftCommandRequest>,
}

pub struct Peer {
    engine: Arc<DB>,
    store_id: u64,
    peer_id: u64,
    region_id: u64,
    leader_id: u64,
    pub raft_group: RawNode<RaftStorage>,
    pub storage: Arc<RaftStorage>,
    pub pending_cmds: HashMap<Uuid, PendingCmd>,
    peer_cache: Arc<RwLock<HashMap<u64, metapb::Peer>>>,
}

impl Peer {
    // If we create the peer actively, like bootstrap/split/merge region, we should
    // use this function to create the peer. The region must contain the peer info
    // for this store.
    pub fn create<T: Transport>(store: &mut Store<T>, region: metapb::Region) -> Result<Peer> {
        let store_id = store.get_store_id();
        let peer_id = match util::find_peer(&region, store_id) {
            None => return Err(other(format!("find no peer for store {}", store_id))),
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

        let mut peer = Peer {
            engine: store.get_engine(),
            store_id: store_id,
            peer_id: peer_id,
            region_id: region.get_region_id(),
            leader_id: raft::INVALID_ID,
            storage: storage,
            raft_group: raft_group,
            pending_cmds: HashMap::new(),
            peer_cache: store.get_peer_cache(),
        };

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

        try!(self.engine.write(batch));

        Ok(())
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
        self.peer_id
    }

    pub fn get_raft_status(&self) -> raft::Status {
        self.raft_group.status()
    }

    pub fn get_leader(&self) -> u64 {
        self.leader_id
    }

    pub fn is_leader(&self) -> bool {
        self.leader_id == self.peer_id
    }

    pub fn handle_raft_ready<T: Transport>(&mut self, trans: &Arc<RwLock<T>>) -> Result<()> {
        if !self.raft_group.has_ready() {
            debug!("raft group is not ready for {}", self.peer_id);
            return Ok(());
        }

        debug!("handle raft ready for peer {} at region {}",
               self.peer_id,
               self.region_id);

        let ready = self.raft_group.ready();

        if let Some(ref ss) = ready.ss {
            self.leader_id = ss.lead;
        }

        try!(self.handle_raft_ready_in_storage(&ready));

        for msg in &ready.messages {
            try!(self.send_raft_message(&msg, trans));
        }

        try!(self.handle_raft_commit_entries(&ready.committed_entries));

        self.raft_group.advance(ready);
        Ok(())
    }

    pub fn propose_pending_cmd(&mut self, pending_cmd: &mut PendingCmd) -> Result<()> {
        if pending_cmd.cmd.is_none() {
            // This may only occur in re-propose.
            debug!("pending command msg is none for region {} in peer {}",
                   self.region_id,
                   self.peer_id);
            return Ok(());
        }

        let cmd = pending_cmd.cmd.take().unwrap();
        let data = try!(cmd.write_to_bytes());

        let mut reproposable = false;
        // We handle change_peer command as ConfChange entry, and others as normal entry.
        if let Some(change_peer) = get_change_peer_command(&cmd) {
            let mut cc = raftpb::ConfChange::new();
            cc.set_change_type(change_peer.get_change_type());
            cc.set_node_id(change_peer.get_peer().get_peer_id());
            cc.set_context(data);

            try!(self.raft_group.propose_conf_change(cc));
            reproposable = true;
        } else {
            try!(self.raft_group.propose(data));
        }

        if reproposable {
            pending_cmd.cmd = Some(cmd);
        }

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

        let mut snap_status = SnapshotStatus::SnapshotFinish;
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
            warn!("region {} on store {} failed to send msg to {} in store {}, err: {:?}",
                  self.region_id,
                  self.store_id,
                  to_peer_id,
                  to_store_id,
                  e);

            unreachable = true;
            snap_status = SnapshotStatus::SnapshotFailure;
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

    fn handle_raft_commit_entries(&mut self, committed_entries: &[raftpb::Entry]) -> Result<()> {
        // If we send multi ConfChange commands, only first one will be proposed correctly,
        // others will be saved as a normal entry with no data, so we must re-propose these
        // commands again.
        let mut need_repropose = false;
        for entry in committed_entries {
            match entry.get_entry_type() {
                raftpb::EntryType::EntryNormal => {
                    try!(self.handle_raft_entry_normal(entry, &mut need_repropose));
                }
                raftpb::EntryType::EntryConfChange => {
                    try!(self.handle_raft_entry_conf_change(entry));
                }
            }
        }

        if need_repropose {
            try!(self.repropose_pending_cmds());
        }

        Ok(())
    }

    fn handle_raft_entry_normal(&mut self,
                                entry: &raftpb::Entry,
                                repropose: &mut bool)
                                -> Result<()> {
        let index = entry.get_index();
        let data = entry.get_data();
        if data.len() == 0 {
            *repropose = true;
            return Ok(());
        }

        let cmd = try!(protobuf::parse_from_bytes::<RaftCommandRequest>(data));
        // no need to return error here.
        if let Err(e) = self.process_raft_command(index, cmd) {
            error!("process raft command at index {} err: {:?}", index, e);
        }

        Ok(())
    }

    fn handle_raft_entry_conf_change(&mut self, entry: &raftpb::Entry) -> Result<()> {
        let index = entry.get_index();
        let mut conf_change =
            try!(protobuf::parse_from_bytes::<raftpb::ConfChange>(entry.get_data()));

        let cmd = try!(protobuf::parse_from_bytes::<RaftCommandRequest>(conf_change.get_context()));
        if let Err(e) = self.process_raft_command(index, cmd) {
            error!("process raft command at index {} err: {:?}", index, e);
            // If failed, tell raft that the config change was aborted.
            conf_change = raftpb::ConfChange::new();
        }

        self.raft_group.apply_conf_change(conf_change);
        Ok(())
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

    fn process_raft_command(&mut self, index: u64, cmd: RaftCommandRequest) -> Result<()> {
        if index == 0 {
            return Err(other("processing raft command needs a none zero index"));
        }

        let uuid = Uuid::from_bytes(cmd.get_header().get_uuid()).unwrap_or_else(|| {
            error!("missing request uuid, but we still try to apply this command");
            Uuid::new_v4()
        });

        let pending_cmd = self.pending_cmds.remove(&uuid);

        let mut resp = match self.apply_raft_command(index, cmd) {
            Err(e) => {
                error!("apply raft command err {:?}", e);
                cmd_resp::message_error(e)
            }
            Ok(resp) => resp,
        };

        if let Some(mut pending_cmd) = pending_cmd {
            if pending_cmd.cb.is_none() {
                warn!("pending command callback for entry {} is None", index);
            } else {
                let cb = pending_cmd.cb.take().unwrap();
                // Bind uuid here.
                cmd_resp::bind_uuid(&mut resp, uuid);
                if let Err(e) = cb.call_box((resp,)) {
                    error!("callback err {:?}", e);
                }
            }
        }

        Ok(())
    }

    fn apply_raft_command(&mut self,
                          index: u64,
                          cmd: RaftCommandRequest)
                          -> Result<RaftCommandResponse> {
        let last_applied_index = self.storage.rl().applied_index();

        if last_applied_index >= index {
            return Err(other(format!("applied index moved backwards, {} >= {}",
                                     last_applied_index,
                                     index)));
        }

        let wb = WriteBatch::new();
        let mut resp = {
            let engine = self.engine.clone();
            let ctx = ExecContext {
                snap: engine.snapshot(),
                wb: &wb,
                request: cmd,
            };

            self.execute_raft_command(&ctx).unwrap_or_else(|e| {
                error!("execute raft command err: {:?}", e);
                cmd_resp::message_error(e)
            })
        };

        peer_storage::save_applied_index(&wb, self.region_id, index)
            .expect("save applied index must not fail");

        match self.engine
                  .write(wb) {
            Ok(()) => {
                self.storage.wl().set_applied_index(index);

                // TODO: handle truncate log command and set truncate log state
                ()
            }
            Err(e) => {
                error!("commit batch failed err {:?}", e);
                resp = cmd_resp::message_error(e);
            }
        };

        Ok(resp)
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
    pub request: RaftCommandRequest,
}

// Here we implement all commands.
impl Peer {
    fn execute_raft_command(&mut self, ctx: &ExecContext) -> Result<RaftCommandResponse> {
        if ctx.request.has_admin_request() {
            self.execute_admin_command(ctx)
        } else {
            self.execute_write_command(ctx)
        }
    }

    fn execute_admin_command(&mut self, ctx: &ExecContext) -> Result<RaftCommandResponse> {
        let request = ctx.request.get_admin_request();
        let cmd_type = request.get_cmd_type();
        let mut response = try!(match cmd_type {
            cmd::AdminCommandType::ChangePeer => self.execute_change_peer(ctx, request),
            cmd::AdminCommandType::Split => self.execute_split(ctx, request),
            e => Err(other(format!("unsupported admin command type {:?}", e))),
        });
        response.set_cmd_type(cmd_type);

        let mut resp = RaftCommandResponse::new();
        resp.set_admin_response(response);
        Ok(resp)
    }

    fn execute_change_peer(&mut self, _: &ExecContext, _: &AdminRequest) -> Result<AdminResponse> {
        // TODO: remove peer cache after ConfChange remove node.
        unimplemented!();
    }

    fn execute_split(&mut self, _: &ExecContext, _: &AdminRequest) -> Result<AdminResponse> {
        unimplemented!();
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
