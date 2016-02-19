use std::sync::{Arc, RwLock};
use std::option::Option;
use std::collections::{HashMap, HashSet};

use rocksdb::DB;
use mio::{self, EventLoop, EventLoopConfig};
use protobuf;
use uuid::Uuid;

use proto::raft_serverpb::{RaftMessage, StoreIdent};
use proto::raft_cmdpb::{self as cmd, RaftCommandRequest, RaftCommandResponse};
use raftserver::{Result, other};
use proto::metapb;
use super::{Sender, Msg};
use super::keys;
use super::engine::Retriever;
use super::config::Config;
use super::peer::{Peer, PendingCmd};
use super::msg::Callback;
use super::cmd_resp::{self, bind_uuid};
use super::transport::Transport;

pub struct Store<T: Transport> {
    cfg: Config,
    ident: StoreIdent,
    engine: Arc<DB>,
    sender: Sender,
    event_loop: Option<EventLoop<Store<T>>>,

    peers: HashMap<u64, Peer>,
    pending_raft_groups: HashSet<u64>,

    trans: Arc<RwLock<T>>,
}

impl<T: Transport> Store<T> {
    pub fn new(cfg: Config, engine: Arc<DB>, trans: Arc<RwLock<T>>) -> Result<Store<T>> {
        let ident: StoreIdent = try!(load_store_ident(engine.as_ref()).and_then(|res| {
            match res {
                None => Err(other("store must be bootstrapped first")),
                Some(ident) => Ok(ident),
            }
        }));

        // We use base raft tick as the event loop timer tick.
        let mut event_cfg = EventLoopConfig::new();
        event_cfg.timer_tick_ms(cfg.raft_base_tick_interval);
        let event_loop = try!(EventLoop::configured(event_cfg));

        let sender = Sender::new(event_loop.channel());

        Ok(Store {
            cfg: cfg,
            ident: ident,
            engine: engine,
            sender: sender,
            event_loop: Some(event_loop),
            peers: HashMap::new(),
            pending_raft_groups: HashSet::new(),
            trans: trans,
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
                             let (_, suffix) = try!(keys::decode_region_meta_key(key));
                             if suffix != keys::REGION_INFO_SUFFIX {
                                 return Ok(true);
                             }

                             let region = try!(protobuf::parse_from_bytes::<metapb::Region>(value));
                             let region_id = region.get_region_id();
                             let peer = try!(Peer::create(self, region));
                             // TODO: check duplicated region id later?
                             self.peers.insert(region_id, peer);
                             Ok(true)
                         }));

        Ok(())
    }

    pub fn run(&mut self) -> Result<()> {
        try!(self.prepare());

        let mut event_loop = self.event_loop.take().unwrap();
        self.register_raft_base_tick(&mut event_loop);
        try!(event_loop.run(self));
        Ok(())
    }

    pub fn get_sender(&self) -> Sender {
        self.sender.clone()
    }

    pub fn get_engine(&self) -> Arc<DB> {
        self.engine.clone()
    }

    pub fn get_ident(&self) -> &StoreIdent {
        &self.ident
    }

    pub fn get_store_id(&self) -> u64 {
        self.ident.get_store_id()
    }

    pub fn get_config(&self) -> &Config {
        &self.cfg
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

        {
            let mut trans = self.trans.write().unwrap();
            trans.cache_peer(from_peer.get_peer_id(), from_peer.clone());
            trans.cache_peer(to_peer.get_peer_id(), to_peer.clone());
        }

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
        let ids = self.pending_raft_groups.drain();

        for region_id in ids {
            if let Some(peer) = self.peers.get_mut(&region_id) {
                if let Err(e) = peer.handle_raft_ready(&self.trans) {
                    // TODO: should we panic here or shutdown the store?
                    error!("handle raft ready err: {:?}", e);
                    return Err(e);
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
            let trans = self.trans.read().unwrap();
            resp = cmd_resp::not_leader_error(region_id, trans.get_peer(peer.get_leader()));
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

        match peer.propose_pending_cmd(&mut pending_cmd) {
            Err(e) => {
                resp = cmd_resp::message_error(format!("{:?}", e));
                bind_uuid(&mut resp, uuid);
                return cb.call_box((resp,));
            }
            Ok(()) => (), // nothing to do.
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
        let trans = self.trans.read().unwrap();
        if let Some(leader) = trans.get_peer(peer.get_leader()) {
            resp.mut_region_leader().set_leader(leader);
        }

        Ok(resp)
    }
}
