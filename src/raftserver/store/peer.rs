use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use std::vec::Vec;
use std::default::Default;

use rocksdb::{DB, WriteBatch};
use protobuf::{self, Message};
use uuid::Uuid;

use proto::metapb;
use proto::raftpb;
use proto::raft_cmdpb::{RaftCommandRequest, RaftCommandResponse, ChangePeerRequest};
use proto::raft_serverpb::RaftMessage;
use raft::{self, Ready, RawNode, SnapshotStatus};
use raftserver::{Result, other};
use super::store::Store;
use super::peer_storage::{self, PeerStorage, RaftStorage, ApplySnapResult};
use super::util;
use super::msg::Callback;
use super::cmd_resp;
use super::transport::Transport;

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
    pub raft_group: RawNode<RaftStorage>,
    pub storage: Arc<RwLock<PeerStorage>>,
    pub pending_cmds: HashMap<Uuid, PendingCmd>,
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
        let store_id = store.get_store_id();

        let s = try!(PeerStorage::new(store.get_engine(), &region));

        let applied_index = s.applied_index();
        let storage = Arc::new(RwLock::new(s));

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
            storage: Arc::new(RaftStorage::new(storage.clone())),
        };

        let raft_group = try!(RawNode::new(&raft_cfg, &[]));

        let mut peer = Peer {
            engine: store.get_engine(),
            store_id: store_id,
            peer_id: peer_id,
            region_id: region.get_region_id(),
            storage: storage,
            raft_group: raft_group,
            pending_cmds: HashMap::new(),
        };

        // If this region has only one peer and I am the one, campaign directly.
        if region.get_peers().len() == 1 && region.get_peers()[0].get_store_id() == store_id {
            try!(peer.raft_group.campaign());
        }

        Ok(peer)
    }

    pub fn update_region(&mut self, region: &metapb::Region) -> Result<()> {
        if self.region_id != region.get_region_id() {
            return Err(other(format!("invalid region id {} != {}",
                                     region.get_region_id(),
                                     self.region_id)));
        }

        let mut store = self.storage.write().unwrap();
        store.set_region(region);
        Ok(())
    }

    pub fn get_region(&self) -> metapb::Region {
        let store = self.storage.read().unwrap();
        store.get_region().clone()
    }

    pub fn get_raft_status(&self) -> raft::Status {
        self.raft_group.status()
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
        let data = try!(encode_raft_command(&pending_cmd.uuid, &cmd));

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
        let mut storage = self.storage.write().unwrap();
        let mut last_index = storage.last_index();
        let mut apply_snap_res: Option<ApplySnapResult> = None;
        if !raft::is_empty_snap(&ready.snapshot) {
            apply_snap_res = try!(storage.apply_snapshot(&batch, &ready.snapshot).map(|res| {
                last_index = res.last_index;
                Some(res)
            }));
        }

        if ready.entries.len() > 0 {
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

        let to_peer_id;
        let to_store_id;

        {
            let trans = trans.read().unwrap();
            let from_peer = try!(trans.get_peer(msg.get_from()).ok_or_else(|| {
                other(format!("failed to lookup sender peer {} in region {}",
                              msg.get_from(),
                              self.region_id))
            }));

            let to_peer = try!(trans.get_peer(msg.get_to()).ok_or_else(|| {
                other(format!("failed to look up recipient peer {} in region {}",
                              msg.get_to(),
                              self.region_id))
            }));

            to_peer_id = to_peer.get_peer_id();
            to_store_id = to_peer.get_store_id();

            send_msg.set_from_peer(from_peer);
            send_msg.set_to_peer(to_peer);

            let _ = trans.send(send_msg).map_err(|e| {
                warn!("region {} on store {} failed to send msg to {} in store {}, err: {:?}",
                      self.region_id,
                      self.store_id,
                      to_peer_id,
                      to_store_id,
                      e);

                unreachable = true;
                snap_status = SnapshotStatus::SnapshotFailure;
            });
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

        let (uuid, cmd) = try!(decode_raft_command(data));
        // no need to return error here.
        if let Err(e) = self.process_raft_command(index, uuid, cmd) {
            error!("process raft command at index {} err: {:?}", index, e);
        }

        Ok(())
    }

    fn handle_raft_entry_conf_change(&mut self, entry: &raftpb::Entry) -> Result<()> {
        let index = entry.get_index();
        let mut conf_change =
            try!(protobuf::parse_from_bytes::<raftpb::ConfChange>(entry.get_data()));

        let (uuid, cmd) = try!(decode_raft_command(conf_change.get_context()));
        if let Err(e) = self.process_raft_command(index, uuid, cmd) {
            error!("process raft command at index {} err: {:?}", index, e);
            // If failed, tell raft that the config change was aborted.
            conf_change = raftpb::ConfChange::new();
        }

        self.raft_group.apply_conf_change(conf_change);
        Ok(())
    }

    fn repropose_pending_cmds(&mut self) -> Result<()> {
        if self.pending_cmds.len() > 0 {
            info!("re-propose {} pending commands after empty entry",
                  self.pending_cmds.len());
            let mut cmds: Vec<PendingCmd> = Vec::with_capacity(self.pending_cmds.len());
            for (_, cmd) in self.pending_cmds.iter() {
                // We only need uuid and cmd for later re-propose.
                cmds.push(PendingCmd {
                    uuid: cmd.uuid.clone(),
                    cmd: cmd.cmd.clone(),
                    ..Default::default()
                });
            }

            for mut cmd in cmds.iter_mut() {
                try!(self.propose_pending_cmd(&mut cmd));
            }
        }
        Ok(())
    }

    fn process_raft_command(&mut self,
                            index: u64,
                            uuid: Uuid,
                            cmd: RaftCommandRequest)
                            -> Result<()> {
        if index == 0 {
            return Err(other("processing raft command needs a none zero index"));
        }

        let pending_cmd = self.pending_cmds.remove(&uuid);

        let resp = match self.apply_raft_command(index, cmd) {
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
        let last_applied_index = self.storage.read().unwrap().applied_index();

        if last_applied_index >= index {
            return Err(other(format!("applied index moved backwards, {} >= {}",
                                     last_applied_index,
                                     index)));
        }

        let wb = WriteBatch::new();

        let mut resp = self.execute_raft_command(&wb, index, cmd).unwrap_or_else(|e| {
            error!("execute raft command err: {:?}", e);
            cmd_resp::message_error(e)
        });

        peer_storage::save_applied_index(&wb, self.region_id, index)
            .expect("save applied index must not fail");

        match self.engine
                  .write(wb) {
            Ok(()) => {
                let mut storage = self.storage.write().unwrap();
                storage.set_applied_index(index);

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

    fn execute_raft_command(&mut self,
                            wb: &WriteBatch,
                            index: u64,
                            cmd: RaftCommandRequest)
                            -> Result<RaftCommandResponse> {
        // implement later.
        unreachable!();
    }
}

fn encode_raft_command(uuid: &Uuid, cmd: &RaftCommandRequest) -> Result<Vec<u8>> {
    let mut data = Vec::with_capacity(16 + cmd.compute_size() as usize);
    data.extend_from_slice(uuid.as_bytes());
    try!(cmd.write_to_vec(&mut data));

    Ok(data)
}

fn decode_raft_command(data: &[u8]) -> Result<(Uuid, RaftCommandRequest)> {
    if data.len() < 16 {
        return Err(other(format!("invalid encoded raft data len {}", data.len())));
    }

    let uuid = Uuid::from_bytes(&data[0..16]).unwrap();

    let msg = try!(protobuf::parse_from_bytes::<RaftCommandRequest>(&data[16..]));

    Ok((uuid, msg))
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
