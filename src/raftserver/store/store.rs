use std::sync::{Arc, RwLock};
use std::option::Option;
use std::collections::{HashMap, HashSet};
use std::boxed::{Box, FnBox};

use rocksdb::DB;
use mio::{self, EventLoop, EventLoopConfig};
use protobuf;
use uuid::Uuid;

use proto::raft_serverpb::{RaftMessage, StoreIdent};
use proto::raft_cmdpb::{self as cmd, RaftCommandRequest, RaftCommandResponse};
use proto::raftpb::ConfChangeType;
use raftserver::{Result, other};
use proto::metapb;
use super::{SendCh, Msg};
use super::keys;
use super::engine::Retriever;
use super::config::Config;
use super::peer::{Peer, PendingCmd, ReadyResult, ExecResult};
use super::msg::Callback;
use super::cmd_resp::{self, bind_uuid};
use super::transport::Transport;

pub struct Store<T: Transport> {
    cfg: Config,
    ident: StoreIdent,
    engine: Arc<DB>,
    sendch: SendCh,

    peers: HashMap<u64, Peer>,
    pending_raft_groups: HashSet<u64>,

    trans: Arc<RwLock<T>>,

    peer_cache: Arc<RwLock<HashMap<u64, metapb::Peer>>>,
}

pub fn create_event_loop<T: Transport>(cfg: &Config) -> Result<EventLoop<Store<T>>> {
    // We use base raft tick as the event loop timer tick.
    let mut event_cfg = EventLoopConfig::new();
    event_cfg.timer_tick_ms(cfg.raft_base_tick_interval);
    let event_loop = try!(EventLoop::configured(event_cfg));
    Ok(event_loop)
}

impl<T: Transport> Store<T> {
    pub fn new(event_loop: &mut EventLoop<Self>,
               cfg: Config,
               engine: Arc<DB>,
               trans: Arc<RwLock<T>>)
               -> Result<Store<T>> {
        try!(cfg.validate());

        let ident: StoreIdent = try!(load_store_ident(engine.as_ref()).and_then(|res| {
            match res {
                None => Err(other("store must be bootstrapped first")),
                Some(ident) => Ok(ident),
            }
        }));

        let sendch = SendCh::new(event_loop.channel());

        let peer_cache = HashMap::new();

        Ok(Store {
            cfg: cfg,
            ident: ident,
            engine: engine,
            sendch: sendch,
            peers: HashMap::new(),
            pending_raft_groups: HashSet::new(),
            trans: trans,
            peer_cache: Arc::new(RwLock::new(peer_cache)),
        })
    }

    // Do something before store runs.
    fn prepare(&mut self) -> Result<()> {
        // Scan region meta to get saved regions.
        let start_key = keys::REGION_META_MIN_KEY;
        let end_key = keys::REGION_META_MAX_KEY;
        let engine = self.engine.clone();
        try!(engine.scan(start_key,
                         end_key,
                         &mut |key, value| -> Result<bool> {
                             let (region_id, suffix) = try!(keys::decode_region_meta_key(key));
                             if suffix != keys::REGION_INFO_SUFFIX {
                                 return Ok(true);
                             }

                             let region = try!(protobuf::parse_from_bytes::<metapb::Region>(value));
                             let peer = try!(Peer::create(self, region));
                             // TODO: check duplicated region id later?
                             self.peers.insert(region_id, peer);
                             Ok(true)
                         }));

        Ok(())
    }

    pub fn run(&mut self, event_loop: &mut EventLoop<Self>) -> Result<()> {
        try!(self.prepare());

        self.register_raft_base_tick(event_loop);
        self.register_raft_gc_log_tick(event_loop);
        try!(event_loop.run(self));
        Ok(())
    }

    pub fn get_sendch(&self) -> SendCh {
        self.sendch.clone()
    }

    pub fn get_engine(&self) -> Arc<DB> {
        self.engine.clone()
    }

    pub fn get_ident(&self) -> &StoreIdent {
        &self.ident
    }

    pub fn get_node_id(&self) -> u64 {
        self.ident.get_node_id()
    }

    pub fn get_store_id(&self) -> u64 {
        self.ident.get_store_id()
    }

    pub fn get_config(&self) -> &Config {
        &self.cfg
    }

    pub fn get_peer_cache(&self) -> Arc<RwLock<HashMap<u64, metapb::Peer>>> {
        self.peer_cache.clone()
    }

    fn register_raft_base_tick(&self, event_loop: &mut EventLoop<Self>) {
        // If we register raft base tick failed, the whole raft can't run correctly,
        // TODO: shutdown the store?
        if let Err(e) = register_timer(event_loop,
                                       Msg::RaftBaseTick,
                                       self.cfg.raft_base_tick_interval) {
            error!("register raft base tick err: {:?}", e);
        };
    }

    fn handle_raft_base_tick(&mut self, event_loop: &mut EventLoop<Self>) {
        for (region_id, peer) in &mut self.peers {
            peer.raft_group.tick();
            self.pending_raft_groups.insert(*region_id);
        }

        self.register_raft_base_tick(event_loop);
    }

    // Clippy doesn't allow hash_map contains_key followed by insert, and suggests
    // using entry().or_insert() instead, but we can't use this because creating peer
    // may fail, so we allow map_entry.
    #[allow(map_entry)]
    fn handle_raft_message(&mut self, msg: RaftMessage) -> Result<()> {
        let region_id = msg.get_region_id();
        let from_peer = msg.get_from_peer();
        let to_peer = msg.get_to_peer();
        debug!("handle raft message for region {}, from {} to {}",
               region_id,
               from_peer.get_peer_id(),
               to_peer.get_peer_id());

        // TODO: We may receive a message which is not in region, and
        // if the message peer id is <= region max_peer_id, we can think
        // this is a stale message and can ignore it directly.
        // Should we only handle this in heartbeat message?

        self.peer_cache.write().unwrap().insert(from_peer.get_peer_id(), from_peer.clone());
        self.peer_cache.write().unwrap().insert(to_peer.get_peer_id(), to_peer.clone());

        if !self.peers.contains_key(&region_id) {
            let peer = try!(Peer::replicate(self, region_id, to_peer.get_peer_id()));
            self.peers.insert(region_id, peer);
        }

        let mut peer = self.peers.get_mut(&region_id).unwrap();

        try!(peer.raft_group.step(msg.get_message().clone()));

        // Add in pending raft group for later handing ready.
        self.pending_raft_groups.insert(region_id);

        Ok(())
    }

    fn handle_raft_ready(&mut self) -> Result<()> {
        let ids: Vec<u64> = self.pending_raft_groups.drain().collect();

        for region_id in ids {
            let mut ready_result = None;
            if let Some(peer) = self.peers.get_mut(&region_id) {
                match peer.handle_raft_ready(&self.trans) {
                    Err(e) => {
                        // TODO: should we panic here or shutdown the store?
                        error!("handle raft ready at region {} err: {:?}", region_id, e);
                        return Err(e);
                    }
                    Ok(ready) => ready_result = ready,
                }
            }

            if let Some(ready_result) = ready_result {
                if let Err(e) = self.handle_ready_result(region_id, ready_result) {
                    error!("handle raft ready result at region {} err: {:?}",
                           region_id,
                           e);
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    fn handle_ready_result(&mut self, region_id: u64, ready_result: ReadyResult) -> Result<()> {
        // handle executing committed log results
        for result in &ready_result.exec_results {
            match *result {
                ExecResult::ChangePeer{ref change_type, ref peer, ..} => {
                    // We only care remove itself now.
                    if *change_type == ConfChangeType::RemoveNode &&
                       peer.get_store_id() == self.get_store_id() {
                        info!("destory peer {:?} for region {}", peer, region_id);
                        // The remove peer is in the same store.
                        // TODO: should we check None here?
                        // Can we destroy it in another thread later?
                        let mut p = self.peers.remove(&region_id).unwrap();
                        if let Err(e) = p.destroy() {
                            error!("destroy peer {:?} for region {} in store {} err {:?}",
                                   peer,
                                   region_id,
                                   self.get_store_id(),
                                   e);
                        }
                    }
                }
                ExecResult::CompactLog{..} => {
                    // Nothing to do, skip to handle it.
                }
            }
        }

        Ok(())
    }

    fn propose_raft_command(&mut self, msg: RaftCommandRequest, cb: Callback) -> Result<()> {
        let mut resp: RaftCommandResponse;
        let uuid: Uuid = match Uuid::from_bytes(msg.get_header().get_uuid()) {
            None => {
                resp = cmd_resp::message_error("missing request uuid");
                return cb.call_box((resp,));
            }
            Some(uuid) => uuid,
        };

        if msg.has_status_request() {
            // For status commands, we handle it here directly.
            resp = match self.execute_status_command(msg) {
                Err(e) => cmd_resp::message_error(format!("{:?}", e)),
                Ok(resp) => resp,
            };
            bind_uuid(&mut resp, uuid);
            return cb.call_box((resp,));
        }

        let region_id = msg.get_header().get_region_id();
        let mut peer = match self.peers.get_mut(&region_id) {
            None => {
                resp = cmd_resp::region_not_found_error(region_id);
                bind_uuid(&mut resp, uuid);
                return cb.call_box((resp,));
            }
            Some(peer) => peer,
        };

        if !peer.is_leader() {
            resp = cmd_resp::not_leader_error(region_id,
                                              peer.get_peer_from_cache(peer.get_leader()));
            bind_uuid(&mut resp, uuid);
            return cb.call_box((resp,));
        }

        let peer_id = msg.get_header().get_peer().get_peer_id();
        if peer.get_peer_id() != peer_id {
            resp = cmd_resp::message_error(format!("mismatch peer id {} != {}",
                                                   peer.get_peer_id(),
                                                   peer_id));
            bind_uuid(&mut resp, uuid);
            return cb.call_box((resp,));
        }

        if peer.pending_cmds.contains_key(&uuid) {
            resp = cmd_resp::message_error(format!("duplicated uuid {:?}", uuid));
            bind_uuid(&mut resp, uuid);
            return cb.call_box((resp,));
        }

        // Notice:
        // Here means the peer is leader, it can still step down to follower later,
        // but it doesn't matter, if the peer is not leader, the proposing command
        // log entry can't be committed.


        // TODO: support handing read-only commands later.
        // for read-only, if we don't care stale read, we can
        // execute these commands immediately in leader.

        let mut pending_cmd = PendingCmd {
            uuid: uuid,
            cb: None,
            cmd: Some(msg),
        };

        if let Err(e) = peer.propose_pending_cmd(&mut pending_cmd) {
            resp = cmd_resp::message_error(format!("{:?}", e));
            bind_uuid(&mut resp, uuid);
            return cb.call_box((resp,));
        };

        // Keep the callback in pending_cmd so that we can call it later
        // after command applied.
        pending_cmd.cb = Some(cb);
        peer.pending_cmds.insert(uuid, pending_cmd);

        self.pending_raft_groups.insert(region_id);

        // TODO: add timeout, if the command is not applied after timeout,
        // we will call the callback with timeout error.

        Ok(())
    }

    fn register_raft_gc_log_tick(&self, event_loop: &mut EventLoop<Self>) {
        if let Err(e) = register_timer(event_loop,
                                       Msg::RaftLogGcTick,
                                       self.cfg.raft_log_gc_tick_interval) {
            // If failed, we can't cleanup the raft log regularly.
            // Although the log size will grow larger and larger, it doesn't affect
            // whole raft logic, and we can send truncate log command to compact it.
            error!("register raft gc log tick err: {:?}", e);
        };
    }

    fn handle_raft_gc_log_tick(&mut self, event_loop: &mut EventLoop<Self>) {
        for (&region_id, peer) in &mut self.peers {
            if !peer.is_leader() {
                continue;
            }

            // Leader will replicate the compact log command to followers,
            // If we use current applied_index (like 10) as the compact index,
            // when we apply this log, the newest applied_index will be 11,
            // but we only compact the log to 10, not 11, at that time,
            // the first index is 10, and applied index is 11, with an extra log,
            // and we will do compact again with compact index 11, in cycles...
            // So we introduce a threshold, if applied index - first index > threshold,
            // we will try to compact log.
            // raft log entries[..............................................]
            //                  ^                                       ^
            //                  |-----------------threshold------------ |
            //              first_index                         applied_index
            let applied_index = peer.storage.rl().applied_index();
            let first_index = peer.storage.rl().first_index();
            if applied_index < first_index {
                // The peer is leader, so the applied_index can't < first index.
                error!("applied_index {} < first_index {} for leader peer {:?} at region {}",
                       applied_index,
                       first_index,
                       peer.peer,
                       region_id);
                continue;
            }

            if applied_index - first_index <= self.cfg.raft_log_gc_threshold {
                continue;
            }

            // Create a compact log request and notify directly.
            let request = new_compact_log_request(region_id, peer.peer.clone(), applied_index);

            let cb = Box::new(move |_: RaftCommandResponse| -> Result<()> { Ok(()) });

            if let Err(e) = self.sendch.send_command(request, cb) {
                error!("send compact log {} to region {} err {:?}",
                       applied_index,
                       region_id,
                       e);
            }
        }


        self.register_raft_gc_log_tick(event_loop);
    }
}

fn load_store_ident<T: Retriever>(r: &T) -> Result<Option<StoreIdent>> {
    let ident = try!(r.get_msg::<StoreIdent>(&keys::store_ident_key()));

    Ok(ident)
}

fn register_timer<T: Transport>(event_loop: &mut EventLoop<Store<T>>,
                                msg: Msg,
                                delay: u64)
                                -> Result<mio::Timeout> {
    // TODO: now mio TimerError doesn't implement Error trait,
    // so we can't use `try!` directly.
    event_loop.timeout_ms(msg, delay).map_err(|e| other(format!("register timer err: {:?}", e)))
}

fn new_compact_log_request(region_id: u64,
                           peer: metapb::Peer,
                           compact_index: u64)
                           -> RaftCommandRequest {
    let mut request = RaftCommandRequest::new();
    request.mut_header().set_region_id(region_id);
    request.mut_header().set_peer(peer);
    request.mut_header().set_uuid(Uuid::new_v4().as_bytes().to_vec());

    let mut admin = cmd::AdminRequest::new();
    admin.set_cmd_type(cmd::AdminCommandType::CompactLog);
    admin.mut_compact_log().set_compact_index(compact_index);
    request.set_admin_request(admin);
    request
}

impl<T: Transport> mio::Handler for Store<T> {
    type Timeout = Msg;
    type Message = Msg;

    fn notify(&mut self, event_loop: &mut EventLoop<Self>, msg: Msg) {
        match msg {
            Msg::RaftMessage(data) => {
                if let Err(e) = self.handle_raft_message(data) {
                    error!("handle raft message err: {:?}", e);
                }
            }
            Msg::RaftCommand{request, callback} => {
                if let Err(e) = self.propose_raft_command(request, callback) {
                    error!("propose raft command err: {:?}", e);
                }
            }
            Msg::Quit => {
                info!("receive quit message");
                event_loop.shutdown();
            }
            _ => panic!("invalid notify msg type {:?}", msg),
        }
    }

    fn timeout(&mut self, event_loop: &mut EventLoop<Self>, timeout: Msg) {
        match timeout {
            Msg::RaftBaseTick => self.handle_raft_base_tick(event_loop),
            Msg::RaftLogGcTick => self.handle_raft_gc_log_tick(event_loop),
            _ => panic!("invalid timeout msg type {:?}", timeout),
        }
    }

    fn tick(&mut self, event_loop: &mut EventLoop<Self>) {
        if !event_loop.is_running() {
            // TODO: do some close here.
            return;
        }

        // We handle raft ready in event loop.
        if let Err(e) = self.handle_raft_ready() {
            // TODO: should we panic here or shutdown the store?
            error!("handle raft ready err: {:?}", e);
        }
    }
}

impl<T: Transport> Store<T> {
    // Handle status commands here, separate the logic, maybe we can move it
    // to another file later.
    // Unlike other commands (write or admin), status commands only show current
    // store status, so no need to handle it in raft group.
    fn execute_status_command(&mut self,
                              request: RaftCommandRequest)
                              -> Result<RaftCommandResponse> {
        let cmd_type = request.get_status_request().get_cmd_type();
        let mut response = try!(match cmd_type {
            cmd::StatusCommandType::RegionLeader => self.execute_region_leader(request),
            e => Err(other(format!("unsupported status command type {:?}", e))),
        });
        response.set_cmd_type(cmd_type);

        let mut resp = RaftCommandResponse::new();
        resp.set_status_response(response);
        Ok(resp)
    }

    fn execute_region_leader(&mut self,
                             request: RaftCommandRequest)
                             -> Result<cmd::StatusResponse> {
        let region_id = request.get_header().get_region_id();
        let peer = match self.peers.get_mut(&region_id) {
            None => return Err(other(format!("region {} not found", region_id))),
            Some(peer) => peer,
        };

        let mut resp = cmd::StatusResponse::new();
        if let Some(leader) = peer.get_peer_from_cache(peer.get_leader()) {
            resp.mut_region_leader().set_leader(leader);
        }

        Ok(resp)
    }
}
