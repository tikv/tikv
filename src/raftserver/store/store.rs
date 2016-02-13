#![allow(unused_variables)]
#![allow(unused_must_use)]

use std::sync::{Arc, Mutex};
use std::option::Option;
use std::collections::{HashMap, HashSet};

use rocksdb::DB;
use mio::{self, EventLoop};
use protobuf;

use proto::raft_serverpb::{RaftMessage, StoreIdent};
use raftserver::{Result, other};
use proto::metapb;
use super::{Sender, Msg};
use super::keys;
use super::engine::Retriever;
use super::config::Config;
use super::peer::Peer;

pub struct Store {
    cfg: Config,
    ident: StoreIdent,
    engine: Arc<DB>,
    sender: Sender,
    event_loop: Option<EventLoop<Store>>,

    peers: HashMap<u64, Peer>,
    pending_raft_groups: HashSet<u64>,
}

impl Store {
    pub fn new(engine: Arc<DB>, cfg: Config) -> Result<Store> {
        let ident: StoreIdent = try!(load_store_ident(engine.as_ref()).and_then(|res| {
            match res {
                None => Err(other("store must be bootstrapped first")),
                Some(ident) => Ok(ident),
            }
        }));

        let event_loop = try!(EventLoop::new());
        let sender = Sender::new(event_loop.channel());

        Ok(Store {
            cfg: cfg,
            ident: ident,
            engine: engine,
            sender: sender,
            event_loop: Some(event_loop),
            peers: HashMap::new(),
            pending_raft_groups: HashSet::new(),
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
                             let peer = try!(Peer::create(self, region));
                             // TODO: check duplicated peer id later?
                             self.peers.insert(peer.get_peer_id(), peer);
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

    fn register_raft_base_tick(&self, event_loop: &mut EventLoop<Store>) {
        // If we register raft base tick failed, the whole raft can't run correctly,
        // TODO: shudown the store?
        let _ = register_timer(event_loop,
                               Msg::RaftBaseTick,
                               self.cfg.raft_base_tick_interval)
                    .map_err(|e| {
                        error!("register raft base tick err: {:?}", e);
                    });
    }

    fn handle_raft_base_tick(&mut self, event_loop: &mut EventLoop<Store>) {
        for (region_id, peer) in self.peers.iter_mut() {
            peer.raft_group.tick();
            self.pending_raft_groups.insert(*region_id);
        }

        self.register_raft_base_tick(event_loop);
    }

    fn handle_raft_message(&mut self, msg: Mutex<RaftMessage>) -> Result<()> {
        let msg = msg.lock().unwrap();

        let region_id = msg.get_region_id();
        let from_peer = msg.get_from_peer();
        let to_peer = msg.get_to_peer();
        debug!("handle raft message for region {}, from {} to {}",
               region_id,
               from_peer.get_peer_id(),
               to_peer.get_peer_id());

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
                try!(peer.handle_raft_ready());
            }
        }

        Ok(())
    }
}

fn load_store_ident<T: Retriever>(r: &T) -> Result<Option<StoreIdent>> {
    let ident = try!(r.get_msg::<StoreIdent>(&keys::store_ident_key()));

    Ok(ident)
}

fn register_timer(event_loop: &mut EventLoop<Store>, msg: Msg, delay: u64) -> Result<mio::Timeout> {
    // TODO: now mio TimerError doesn't implement Error trait,
    // so we can't use `try!` directly.
    event_loop.timeout_ms(msg, delay).map_err(|e| other(format!("register timer err: {:?}", e)))
}

impl mio::Handler for Store {
    type Timeout = Msg;
    type Message = Msg;

    fn notify(&mut self, event_loop: &mut EventLoop<Store>, msg: Msg) {
        match msg {
            Msg::RaftMessage(data) => {
                self.handle_raft_message(data).map_err(|e| {
                    error!("handle raft message err: {:?}", e);
                });
            }
            _ => panic!("invalid notify msg type"),
        }
    }

    fn timeout(&mut self, event_loop: &mut EventLoop<Store>, timeout: Msg) {
        match timeout {
            Msg::RaftBaseTick => self.handle_raft_base_tick(event_loop),
            _ => panic!("invalid timeout msg type"),
        }
    }

    fn tick(&mut self, event_loop: &mut EventLoop<Store>) {
        // We handle raft ready in event loop.
        self.handle_raft_ready().map_err(|e| {
            // TODO: should we panic here or shutdown the store?
            error!("handle raft ready err: {:?}", e);
        });
    }
}
