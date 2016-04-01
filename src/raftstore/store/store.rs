use std::sync::{Arc, RwLock};
use std::option::Option;
use std::collections::{HashMap, HashSet, VecDeque, BTreeMap};
use std::boxed::{Box, FnBox};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use std::collections::Bound::{Excluded, Unbounded};

use rocksdb::DB;
use mio::{self, EventLoop, EventLoopConfig};
use protobuf;
use uuid::Uuid;

use kvproto::raft_serverpb::{RaftMessage, StoreIdent, RaftSnapshotData};
use kvproto::raftpb::ConfChangeType;
use util::HandyRwLock;
use pd::PdClient;
use kvproto::raft_cmdpb::{AdminCmdType, AdminRequest, StatusCmdType, StatusResponse,
                          RaftCmdRequest, RaftCmdResponse};
use protobuf::Message;

use raftstore::{Result, Error};
use kvproto::metapb;
use super::util;
use super::{SendCh, Msg, Tick};
use super::keys::{self, enc_start_key, enc_end_key};
use super::engine::{Peekable, Iterable};
use super::config::Config;
use super::peer::{Peer, PendingCmd, ReadyResult, ExecResult};
use super::peer_storage::{PeerStorage, RAFT_INIT_LOG_INDEX};
use super::msg::Callback;
use super::cmd_resp::{self, bind_uuid, bind_term, bind_error};
use super::transport::Transport;

type Key = Vec<u8>;

const SPLIT_TASK_PEEK_INTERVAL_SECS: u64 = 1;

pub struct Store<T: Transport, C: PdClient> {
    cluster_meta: metapb::Cluster,
    cfg: Config,
    ident: StoreIdent,
    engine: Arc<DB>,
    sendch: SendCh,

    // region_id -> peers
    region_peers: HashMap<u64, Peer>,
    pending_raft_groups: HashSet<u64>,
    // region end key -> region id
    region_ranges: BTreeMap<Key, u64>,

    split_checking_queue: Arc<RwLock<VecDeque<SplitCheckTask>>>,
    /// A join handle to split scan thread, currently only for test purpose.
    split_handle: Option<JoinHandle<()>>,

    /// A flag indicates whether store has been shutdown.
    stopped: Arc<RwLock<bool>>,

    trans: Arc<RwLock<T>>,
    pd_client: Arc<RwLock<C>>,

    peer_cache: Arc<RwLock<HashMap<u64, metapb::Peer>>>,
}

pub fn create_event_loop<T, C>(cfg: &Config) -> Result<EventLoop<Store<T, C>>>
    where T: Transport,
          C: PdClient
{
    // We use base raft tick as the event loop timer tick.
    let mut event_cfg = EventLoopConfig::new();
    event_cfg.timer_tick_ms(cfg.raft_base_tick_interval);
    let event_loop = try!(EventLoop::configured(event_cfg));
    Ok(event_loop)
}

impl<T: Transport, C: PdClient> Store<T, C> {
    pub fn new(event_loop: &mut EventLoop<Self>,
               cluster_meta: metapb::Cluster,
               cfg: Config,
               engine: Arc<DB>,
               trans: Arc<RwLock<T>>,
               pd_client: Arc<RwLock<C>>)
               -> Result<Store<T, C>> {
        try!(cfg.validate());

        let ident: StoreIdent = try!(load_store_ident(engine.as_ref()).and_then(|res| {
            match res {
                None => Err(box_err!("store must be bootstrapped first")),
                Some(ident) => Ok(ident),
            }
        }));

        let sendch = SendCh::new(event_loop.channel());

        let peer_cache = HashMap::new();

        Ok(Store {
            cluster_meta: cluster_meta,
            cfg: cfg,
            ident: ident,
            engine: engine,
            sendch: sendch,
            region_peers: HashMap::new(),
            pending_raft_groups: HashSet::new(),
            split_checking_queue: Arc::new(RwLock::new(VecDeque::new())),
            region_ranges: BTreeMap::new(),
            stopped: Arc::new(RwLock::new(false)),
            split_handle: None,
            trans: trans,
            pd_client: pd_client,
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
                             let peer = try!(Peer::create(self, &region));

                             self.region_ranges.insert(enc_end_key(&region), region_id);
        // No need to check duplicated here, because we use region id as the key
        // in DB.
                             self.region_peers.insert(region_id, peer);
                             Ok(true)
                         }));

        Ok(())
    }

    pub fn run(&mut self, event_loop: &mut EventLoop<Self>) -> Result<()> {
        try!(self.prepare());

        self.register_raft_base_tick(event_loop);
        self.register_raft_gc_log_tick(event_loop);
        self.register_split_region_check_tick(event_loop);
        self.register_replica_check_tick(event_loop);

        let handle = try!(start_split_check(self.split_checking_queue.clone(),
                                            self.stopped.clone(),
                                            self.sendch.clone(),
                                            self.cfg.region_max_size,
                                            self.cfg.region_split_size));
        self.split_handle = Some(handle);

        try!(event_loop.run(self));
        Ok(())
    }

    pub fn get_sendch(&self) -> SendCh {
        self.sendch.clone()
    }

    pub fn engine(&self) -> Arc<DB> {
        self.engine.clone()
    }

    pub fn ident(&self) -> &StoreIdent {
        &self.ident
    }

    pub fn store_id(&self) -> u64 {
        self.ident.get_store_id()
    }

    pub fn config(&self) -> &Config {
        &self.cfg
    }

    pub fn peer_cache(&self) -> Arc<RwLock<HashMap<u64, metapb::Peer>>> {
        self.peer_cache.clone()
    }

    fn register_raft_base_tick(&self, event_loop: &mut EventLoop<Self>) {
        // If we register raft base tick failed, the whole raft can't run correctly,
        // TODO: shutdown the store?
        if let Err(e) = register_timer(event_loop, Tick::Raft, self.cfg.raft_base_tick_interval) {
            error!("register raft base tick err: {:?}", e);
        };
    }

    fn on_raft_base_tick(&mut self, event_loop: &mut EventLoop<Self>) {
        for (region_id, peer) in &mut self.region_peers {
            peer.raft_group.tick();
            self.pending_raft_groups.insert(*region_id);
        }

        self.register_raft_base_tick(event_loop);
    }

    // Clippy doesn't allow hash_map contains_key followed by insert, and suggests
    // using entry().or_insert() instead, but we can't use this because creating peer
    // may fail, so we allow map_entry.
    #[allow(map_entry)]
    fn on_raft_message(&mut self, msg: RaftMessage) -> Result<()> {
        let region_id = msg.get_region_id();
        let from = msg.get_from_peer();
        let to = msg.get_to_peer();
        debug!("handle raft message for region {}, from {} to {}",
               region_id,
               from.get_id(),
               to.get_id());

        if !msg.has_region_epoch() {
            error!("missing epoch in raft message, ignore it");
            return Ok(());
        }

        if !self.region_peers.contains_key(&region_id) {
            let peer = try!(Peer::replicate(self, region_id, msg.get_region_epoch(), to.get_id()));
            // We don't have start_key of the region, so there is no need to insert into
            // region_ranges
            self.region_peers.insert(region_id, peer);
        }

        self.peer_cache.wl().insert(from.get_id(), from.clone());
        self.peer_cache.wl().insert(to.get_id(), to.clone());

        // Check if we can accept the snapshot
        // TODO: we need to inject failure or re-order network packet to test the situtain
        if !self.region_peers[&region_id].storage.rl().is_initialized() &&
           msg.get_message().has_snapshot() {
            let snap = msg.get_message().get_snapshot();
            let mut snap_data = RaftSnapshotData::new();
            try!(snap_data.merge_from_bytes(snap.get_data()));
            let snap_region = snap_data.get_region();
            if let Some((_, &region_id)) = self.region_ranges
                                               .range(Excluded(&enc_start_key(snap_region)),
                                                      Unbounded::<&Key>)
                                               .next() {
                let exist_region = self.region_peers[&region_id].region();
                if enc_start_key(&exist_region) < enc_end_key(snap_region) {
                    warn!("region overlapped {:?}, {:?}", exist_region, snap_region);
                    return Ok(());
                }
            }
        }

        let peer = self.region_peers.get_mut(&region_id).unwrap();

        try!(peer.raft_group.step(msg.get_message().clone()));

        // Add into pending raft groups for later handling ready.
        self.pending_raft_groups.insert(region_id);

        Ok(())
    }

    fn on_raft_ready(&mut self) -> Result<()> {
        let ids: Vec<u64> = self.pending_raft_groups.drain().collect();

        for region_id in ids {
            let mut ready_result = None;
            if let Some(peer) = self.region_peers.get_mut(&region_id) {
                match peer.handle_raft_ready(&self.trans) {
                    Err(e) => {
                        // TODO: should we panic or shutdown the store?
                        error!("handle raft ready at region {} err: {:?}", region_id, e);
                        return Err(e);
                    }
                    Ok(ready) => ready_result = ready,
                }
            }

            if let Some(ready_result) = ready_result {
                if let Err(e) = self.on_ready_result(region_id, ready_result) {
                    error!("handle raft ready result at region {} err: {:?}",
                           region_id,
                           e);
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    fn on_ready_result(&mut self, region_id: u64, ready_result: ReadyResult) -> Result<()> {
        if let Some(region) = ready_result.snap_applied_region {
            self.region_ranges.insert(enc_end_key(&region), region.get_id());
        }

        // handle executing committed log results
        for result in &ready_result.exec_results {
            match *result {
                ExecResult::ChangePeer{ref change_type, ref peer, ..} => {
                    // We only care remove itself now.
                    if *change_type == ConfChangeType::RemoveNode &&
                       peer.get_store_id() == self.store_id() {
                        info!("destory peer {:?} for region {}", peer, region_id);
                        // The remove peer is in the same store.
                        // TODO: should we check None here?
                        // Can we destroy it in another thread later?
                        let mut p = self.region_peers.remove(&region_id).unwrap();
                        let end_key = enc_end_key(&p.region());
                        if let Err(e) = p.destroy() {
                            error!("destroy peer {:?} for region {} in store {} err {:?}",
                                   peer,
                                   region_id,
                                   self.store_id(),
                                   e);
                        } else {
                            if self.region_ranges.remove(&end_key).is_none() {
                                panic!("Remove region, peer {:?}, region {} in store {}",
                                       peer,
                                       region_id,
                                       self.store_id());

                            }
                        }
                    }
                }
                ExecResult::CompactLog{..} => {
                    // Nothing to do, skip to handle it.
                }
                ExecResult::SplitRegion{ref left, ref right} => {
                    let new_region_id = right.get_id();
                    match Peer::create(self, &right) {
                        Err(e) => {
                            error!("create new split region {:?} err {:?}", right, e);
                        }
                        Ok(mut new_peer) => {
                            if self.region_peers.contains_key(&new_region_id) {
                                // This is a very serious error, should we close the store here?
                                // because the raft group can not run correctly
                                // for this split region.
                                error!("duplicated region {} for split region", new_region_id);
                                break;
                            }

                            // If the peer for the region before split is leader,
                            // we can force the new peer for the new split region to campaign
                            // to become the leader too.
                            let is_leader = self.region_peers.get(&region_id).unwrap().is_leader();
                            if is_leader && right.get_peers().len() > 1 {
                                if let Err(e) = new_peer.raft_group.campaign() {
                                    error!("peer {:?} campaigns for region {} err {:?}",
                                           new_peer.peer,
                                           new_region_id,
                                           e);
                                }
                            }

                            // Insert new regions and validation
                            if self.region_ranges
                                   .insert(enc_end_key(left), left.get_id())
                                   .is_some() {
                                panic!("region should not exist, {:?}", left);
                            }
                            if self.region_ranges
                                   .insert(enc_end_key(right), new_region_id)
                                   .is_none() {
                                panic!("region should exist, {:?}", right);
                            }
                            self.region_peers.insert(new_region_id, new_peer);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn propose_raft_command(&mut self, msg: RaftCmdRequest, cb: Callback) -> Result<()> {
        let mut resp = RaftCmdResponse::new();
        let uuid: Uuid = match util::get_uuid_from_req(&msg) {
            None => {
                bind_error(&mut resp, Error::Other("missing request uuid".into()));
                return cb.call_box((resp,));
            }
            Some(uuid) => {
                bind_uuid(&mut resp, uuid);
                uuid
            }
        };

        if msg.has_status_request() {
            // For status commands, we handle it here directly.
            resp = match self.execute_status_command(msg) {
                Err(e) => cmd_resp::new_error(e),
                Ok(resp) => resp,
            };
            bind_uuid(&mut resp, uuid);
            return cb.call_box((resp,));
        }

        let region_id = msg.get_header().get_region_id();
        let mut peer = match self.region_peers.get_mut(&region_id) {
            None => {
                bind_error(&mut resp, Error::RegionNotFound(region_id));
                return cb.call_box((resp,));
            }
            Some(peer) => peer,
        };

        bind_term(&mut resp, peer.current_term);

        if !peer.is_leader() {
            bind_error(&mut resp,
                       Error::NotLeader(region_id, peer.get_peer_from_cache(peer.leader_id())));
            return cb.call_box((resp,));
        }

        let peer_id = msg.get_header().get_peer().get_id();
        if peer.peer_id() != peer_id {
            bind_error(&mut resp,
                       box_err!("mismatch peer id {} != {}", peer.peer_id(), peer_id));
            return cb.call_box((resp,));
        }

        if peer.pending_cmds.contains_key(&uuid) {
            bind_error(&mut resp, box_err!("duplicated uuid {:?}", uuid));
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
            bind_error(&mut resp, e);
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
                                       Tick::RaftLogGc,
                                       self.cfg.raft_log_gc_tick_interval) {
            // If failed, we can't cleanup the raft log regularly.
            // Although the log size will grow larger and larger, it doesn't affect
            // whole raft logic, and we can send truncate log command to compact it.
            error!("register raft gc log tick err: {:?}", e);
        };
    }

    fn on_raft_gc_log_tick(&mut self, event_loop: &mut EventLoop<Self>) {
        for (&region_id, peer) in &mut self.region_peers {
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
                // test if we are just started.
                if first_index != RAFT_INIT_LOG_INDEX + 1 {
                    // The peer is leader, so the applied_index can't < first index.
                    error!("applied_index {} < first_index {} for leader peer {:?} at region {}",
                           applied_index,
                           first_index,
                           peer.peer,
                           region_id);
                }
                continue;
            }

            if applied_index - first_index <= self.cfg.raft_log_gc_threshold {
                continue;
            }

            // Create a compact log request and notify directly.
            let request = new_compact_log_request(region_id, peer.peer.clone(), applied_index);

            let cb = Box::new(move |_: RaftCmdResponse| -> Result<()> { Ok(()) });

            if let Err(e) = self.sendch.send(Msg::RaftCmd {
                request: request,
                callback: cb,
            }) {
                error!("send compact log {} to region {} err {:?}",
                       applied_index,
                       region_id,
                       e);
            }
        }

        self.register_raft_gc_log_tick(event_loop);
    }

    fn register_split_region_check_tick(&self, event_loop: &mut EventLoop<Self>) {
        if let Err(e) = register_timer(event_loop,
                                       Tick::SplitRegionCheck,
                                       self.cfg.split_region_check_tick_interval) {
            error!("register split region check tick err: {:?}", e);
        };
    }

    fn on_split_region_check_tick(&mut self, event_loop: &mut EventLoop<Self>) {
        // To avoid frequent scan, we only add new scan tasks if all previous tasks
        // have finished.
        // TODO: check whether a gc progress has been started.
        if !self.split_checking_queue.rl().is_empty() {
            self.register_split_region_check_tick(event_loop);
            return;
        }
        let mut tasks = VecDeque::new();
        for (id, peer) in &mut self.region_peers {
            if !peer.is_leader() {
                continue;
            }

            if peer.size_diff_hint < self.cfg.region_check_size_diff {
                continue;
            }
            info!("region {}'s size diff {} >= {}, need to check whether should split",
                  id,
                  peer.size_diff_hint,
                  self.cfg.region_check_size_diff);
            tasks.push_back(SplitCheckTask::new(&peer.storage.rl()));
            peer.size_diff_hint = 0;
        }

        if !tasks.is_empty() {
            self.split_checking_queue.wl().append(&mut tasks);
        }

        self.register_split_region_check_tick(event_loop);
    }

    fn on_split_check_result(&mut self, region_id: u64, split_key: Vec<u8>) {
        if split_key.is_empty() {
            error!("split key should not be empty!!!");
            return;
        }
        let p = self.region_peers.get(&region_id);
        if p.is_none() || !p.unwrap().is_leader() {
            // region on this store is no longer leader, skipped.
            return;
        }
        let key = keys::origin_key(&split_key);
        let peer = p.unwrap();
        if let Err(e) = self.pd_client
                            .rl()
                            .ask_split(self.cluster_meta.get_id(),
                                       peer.region(),
                                       key,
                                       peer.peer.clone()) {
            error!("failed to notify pd to split region {} at {:?}: {}",
                   region_id,
                   split_key,
                   e);
        }
    }

    fn register_replica_check_tick(&self, event_loop: &mut EventLoop<Self>) {
        if let Err(e) = register_timer(event_loop,
                                       Tick::ReplicaCheck,
                                       self.cfg.replica_check_tick_interval) {
            error!("register replica check tick err: {:?}", e);
        };
    }

    fn on_replica_check_tick(&mut self, event_loop: &mut EventLoop<Self>) {
        for peer in self.region_peers.values() {
            if !peer.is_leader() {
                continue;
            }

            let peer_count = peer.region().get_peers().len();
            let max_count = self.cluster_meta.get_max_peer_number() as usize;
            if peer_count == max_count {
                continue;
            }
            info!("peer count {} != max_peer_number {}, notifying pd",
                  peer_count,
                  max_count);
            if let Err(e) = self.pd_client
                                .rl()
                                .ask_change_peer(self.cluster_meta.get_id(),
                                                 peer.region(),
                                                 peer.peer.clone()) {
                error!("failed to notify pd: {}", e);
            }
        }

        self.register_replica_check_tick(event_loop);
    }
}

fn load_store_ident<T: Peekable>(r: &T) -> Result<Option<StoreIdent>> {
    let ident = try!(r.get_msg::<StoreIdent>(&keys::store_ident_key()));

    Ok(ident)
}

fn register_timer<T: Transport, C: PdClient>(event_loop: &mut EventLoop<Store<T, C>>,
                                             tick: Tick,
                                             delay: u64)
                                             -> Result<mio::Timeout> {
    // TODO: now mio TimerError doesn't implement Error trait,
    // so we can't use `try!` directly.
    event_loop.timeout_ms(tick, delay).map_err(|e| box_err!("register timer err: {:?}", e))
}

fn new_compact_log_request(region_id: u64,
                           peer: metapb::Peer,
                           compact_index: u64)
                           -> RaftCmdRequest {
    let mut request = RaftCmdRequest::new();
    request.mut_header().set_region_id(region_id);
    request.mut_header().set_peer(peer);
    request.mut_header().set_uuid(Uuid::new_v4().as_bytes().to_vec());

    let mut admin = AdminRequest::new();
    admin.set_cmd_type(AdminCmdType::CompactLog);
    admin.mut_compact_log().set_compact_index(compact_index);
    request.set_admin_request(admin);
    request
}

impl<T: Transport, C: PdClient> mio::Handler for Store<T, C> {
    type Timeout = Tick;
    type Message = Msg;

    fn notify(&mut self, event_loop: &mut EventLoop<Self>, msg: Msg) {
        match msg {
            Msg::RaftMessage(data) => {
                if let Err(e) = self.on_raft_message(data) {
                    error!("handle raft message err: {:?}", e);
                }
            }
            Msg::RaftCmd{request, callback} => {
                if let Err(e) = self.propose_raft_command(request, callback) {
                    error!("propose raft command err: {:?}", e);
                }
            }
            Msg::Quit => {
                info!("receive quit message");
                event_loop.shutdown();
            }
            Msg::SplitCheckResult {region_id, split_key} => {
                info!("split check of {} complete.", region_id);
                self.on_split_check_result(region_id, split_key);
            }
        }
    }

    fn timeout(&mut self, event_loop: &mut EventLoop<Self>, timeout: Tick) {
        match timeout {
            Tick::Raft => self.on_raft_base_tick(event_loop),
            Tick::RaftLogGc => self.on_raft_gc_log_tick(event_loop),
            Tick::SplitRegionCheck => self.on_split_region_check_tick(event_loop),
            Tick::ReplicaCheck => self.on_replica_check_tick(event_loop),
        }
    }

    fn tick(&mut self, event_loop: &mut EventLoop<Self>) {
        if !event_loop.is_running() {
            *self.stopped.wl() = true;
            // TODO: do some close here.

            if let Some(handle) = self.split_handle.take() {
                if handle.join().is_err() {
                    error!("failed stop split scan thread!!!");
                }
            }
            return;
        }

        // We handle raft ready in event loop.
        if let Err(e) = self.on_raft_ready() {
            // TODO: should we panic here or shutdown the store?
            error!("handle raft ready err: {:?}", e);
        }
    }
}

impl<T: Transport, C: PdClient> Store<T, C> {
    /// load the target peer of request as mutable borrow.
    fn mut_target_peer(&mut self, request: &RaftCmdRequest) -> Result<&mut Peer> {
        let region_id = request.get_header().get_region_id();
        match self.region_peers.get_mut(&region_id) {
            None => Err(Error::RegionNotFound(region_id)),
            Some(peer) => Ok(peer),
        }
    }

    // Handle status commands here, separate the logic, maybe we can move it
    // to another file later.
    // Unlike other commands (write or admin), status commands only show current
    // store status, so no need to handle it in raft group.
    fn execute_status_command(&mut self, request: RaftCmdRequest) -> Result<RaftCmdResponse> {
        let cmd_type = request.get_status_request().get_cmd_type();
        let region_id = request.get_header().get_region_id();

        let mut response = try!(match cmd_type {
            StatusCmdType::RegionLeader => self.execute_region_leader(request),
            StatusCmdType::RegionDetail => self.execute_region_detail(request),
            StatusCmdType::StoreStats => Err(box_err!("unsupported store statistic now")),
            StatusCmdType::InvalidStatus => Err(box_err!("invalid status command!")),
        });
        response.set_cmd_type(cmd_type);

        let mut resp = RaftCmdResponse::new();
        resp.set_status_response(response);
        // Bind peer current term here.
        if let Some(peer) = self.region_peers.get(&region_id) {
            bind_term(&mut resp, peer.current_term);
        }
        Ok(resp)
    }

    fn execute_region_leader(&mut self, request: RaftCmdRequest) -> Result<StatusResponse> {
        let peer = try!(self.mut_target_peer(&request));

        let mut resp = StatusResponse::new();
        if let Some(leader) = peer.get_peer_from_cache(peer.leader_id()) {
            resp.mut_region_leader().set_leader(leader);
        }

        Ok(resp)
    }

    fn execute_region_detail(&mut self, request: RaftCmdRequest) -> Result<StatusResponse> {
        let peer = try!(self.mut_target_peer(&request));
        if !peer.storage.rl().is_initialized() {
            let region_id = request.get_header().get_region_id();
            return Err(Error::RegionNotInitialized(region_id));
        }
        let mut resp = StatusResponse::new();
        resp.mut_region_detail().set_region(peer.region());
        if let Some(leader) = peer.get_peer_from_cache(peer.leader_id()) {
            resp.mut_region_detail().set_leader(leader);
        }

        Ok(resp)
    }
}

struct SplitCheckTask {
    region_id: u64,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    engine: Arc<DB>,
}

impl SplitCheckTask {
    fn new(ps: &PeerStorage) -> SplitCheckTask {
        SplitCheckTask {
            region_id: ps.get_region_id(),
            start_key: enc_start_key(&ps.region),
            end_key: enc_end_key(&ps.region),
            engine: ps.get_engine().clone(),
        }
    }
}

fn new_split_check_result(region_id: u64, split_key: Vec<u8>) -> Msg {
    Msg::SplitCheckResult {
        region_id: region_id,
        split_key: split_key,
    }
}

fn start_split_check(tasks: Arc<RwLock<VecDeque<SplitCheckTask>>>,
                     stopped: Arc<RwLock<bool>>,
                     ch: SendCh,
                     region_max_size: u64,
                     split_size: u64)
                     -> Result<JoinHandle<()>> {
    thread::Builder::new()
        .name("split-check".to_owned())
        .spawn(move || {
            loop {
                let t = tasks.wl().pop_front();
                if t.is_none() {
                    // TODO find or implement a concurrent deque instead.
                    thread::sleep(Duration::from_secs(SPLIT_TASK_PEEK_INTERVAL_SECS));
                    if *stopped.rl() {
                        break;
                    }
                    continue;
                }
                execute_task(t.unwrap(), &ch, region_max_size, split_size);
            }
        })
        .map_err(|e| e.into())
}

fn execute_task(task: SplitCheckTask, ch: &SendCh, region_max_size: u64, split_size: u64) {
    debug!("executing task {:?} {:?}", task.start_key, task.end_key);
    let mut size = 0;
    let mut split_key = vec![];
    let res = task.engine.scan(&task.start_key,
                               &task.end_key,
                               &mut |k, v| {
                                   size += k.len() as u64;
                                   size += v.len() as u64;
                                   if split_key.is_empty() && size > split_size {
                                       split_key = k.to_vec();
                                   }
                                   Ok(size < region_max_size)
                               });
    if let Err(e) = res {
        error!("failed to scan split key of region {}: {:?}",
               task.region_id,
               e);
        return;
    }
    if size < region_max_size {
        debug!("no need to send for {} < {}", size, region_max_size);
        return;
    }
    let res = ch.send(new_split_check_result(task.region_id, split_key));
    if let Err(e) = res {
        warn!("failed to send check result of {}: {}", task.region_id, e);
    }
}
