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

use std::sync::{Arc, RwLock};
use std::option::Option;
use std::collections::{HashMap, HashSet, BTreeMap};
use std::boxed::{Box, FnBox};
use std::collections::Bound::{Excluded, Unbounded};
use std::time::Duration;

use rocksdb::DB;
use mio::{self, EventLoop, EventLoopBuilder};
use protobuf;
use uuid::Uuid;

use kvproto::raft_serverpb::{RaftMessage, StoreIdent, RaftSnapshotData, RaftTruncatedState};
use kvproto::raftpb::ConfChangeType;
use util::{HandyRwLock, SlowTimer};
use pd::PdClient;
use kvproto::raft_cmdpb::{AdminCmdType, AdminRequest, StatusCmdType, StatusResponse,
                          RaftCmdRequest, RaftCmdResponse};
use protobuf::Message;
use raft::SnapshotStatus;
use raftstore::{Result, Error};
use kvproto::metapb;
use util::worker::Worker;
use super::worker::{SplitCheckRunner, SplitCheckTask, SnapTask, SnapRunner, CompactTask,
                    CompactRunner, PdRunner, PdTask};
use super::util;
use super::{SendCh, Msg, Tick};
use super::keys::{self, enc_start_key, enc_end_key};
use super::engine::{Peekable, Iterable};
use super::config::Config;
use super::peer::{Peer, PendingCmd, ReadyResult, ExecResult};
use super::peer_storage::SnapState;
use super::msg::Callback;
use super::cmd_resp::{bind_uuid, bind_term, bind_error};
use super::transport::Transport;

type Key = Vec<u8>;

const SPLIT_TASK_PEEK_INTERVAL_SECS: u64 = 1;

pub struct Store<T: Transport, C: PdClient + 'static> {
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

    split_check_worker: Worker<SplitCheckTask>,
    snap_worker: Worker<SnapTask>,
    compact_worker: Worker<CompactTask>,
    pd_worker: Worker<PdTask>,

    /// A flag indicates whether store has been shutdown.
    stopped: Arc<RwLock<bool>>,

    trans: Arc<RwLock<T>>,
    pd_client: Arc<RwLock<C>>,
}

pub fn create_event_loop<T, C>(cfg: &Config) -> Result<EventLoop<Store<T, C>>>
    where T: Transport,
          C: PdClient
{
    // We use base raft tick as the event loop timer tick.
    let mut builder = EventLoopBuilder::new();
    builder.timer_tick(Duration::from_millis(cfg.raft_base_tick_interval));
    let event_loop = try!(builder.build());
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

        Ok(Store {
            cluster_meta: cluster_meta,
            cfg: cfg,
            ident: ident,
            engine: engine,
            sendch: sendch,
            region_peers: HashMap::new(),
            pending_raft_groups: HashSet::new(),
            split_check_worker: Worker::new("split check worker".to_owned()),
            snap_worker: Worker::new("snapshot worker".to_owned()),
            compact_worker: Worker::new("compact worker".to_owned()),
            pd_worker: Worker::new("pd worker".to_owned()),
            region_ranges: BTreeMap::new(),
            stopped: Arc::new(RwLock::new(false)),
            trans: trans,
            pd_client: pd_client,
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
                         &mut |key, value|{
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

        let split_check_runner = SplitCheckRunner::new(self.sendch.clone(),
                                                       self.cfg.region_max_size,
                                                       self.cfg.region_split_size);
        box_try!(self.split_check_worker.start(split_check_runner));

        box_try!(self.snap_worker.start(SnapRunner));

        box_try!(self.compact_worker.start(CompactRunner));

        let pd_runner = PdRunner::new(self.cluster_meta.get_id(), self.pd_client.clone());
        box_try!(self.pd_worker.start(pd_runner));

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
            // ALERT!!! patern matching won't release lock here.
            if peer.is_leader() && SnapState::Pending == peer.storage.rl().snap_state {
                debug!("handling snapshot for {}", region_id);
                let task = SnapTask::new(peer.storage.clone());
                debug!("task generated");
                peer.storage.wl().snap_state = SnapState::Generating;
                if let Err(e) = self.snap_worker.schedule(task) {
                    error!("failed to schedule snap task {}", e);
                    peer.storage.wl().snap_state = SnapState::Failed;
                }
            }
        }

        self.register_raft_base_tick(event_loop);
    }

    // Clippy doesn't allow hash_map contains_key followed by insert, and suggests
    // using entry().or_insert() instead, but we can't use this because creating peer
    // may fail, so we allow map_entry.
    #[allow(map_entry)]
    fn on_raft_message(&mut self, mut msg: RaftMessage) -> Result<()> {
        let region_id = msg.get_region_id();
        let from_store_id = msg.get_message().get_from();
        let to_store_id = msg.get_message().get_to();
        debug!("handle raft message {:?} for region {}, from {} to {}",
               msg.get_message().get_msg_type(),
               region_id,
               from_store_id,
               to_store_id);

        if to_store_id != self.store_id() {
            warn!("mismatch store {} != {}, ignore it",
                  to_store_id,
                  self.store_id());
        }

        if !msg.has_region_epoch() {
            error!("missing epoch in raft message, ignore it");
            return Ok(());
        }

        // If we receive a message whose sender peer is not in current region and
        // epoch is stale, we can ignore it.
        if let Some(peer) = self.region_peers.get(&region_id) {
            let from_epoch = msg.get_region_epoch();
            let conf_ver = peer.storage.rl().region.get_region_epoch().get_conf_ver();
            let version = peer.storage.rl().region.get_region_epoch().get_version();

            if (from_epoch.get_conf_ver() < conf_ver || from_epoch.get_version() < version) &&
               !util::find_peer(&peer.storage.rl().region, from_store_id) {

                warn!("raft message {:?} is stale {:?}, ignore it",
                      msg.get_message().get_msg_type(),
                      from_epoch);
                return Ok(());
            }
        }


        if !self.region_peers.contains_key(&region_id) {
            let peer = try!(Peer::replicate(self, region_id, msg.get_region_epoch()));
            // We don't have start_key of the region, so there is no need to insert into
            // region_ranges
            self.region_peers.insert(region_id, peer);
        }

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
        let timer = SlowTimer::new();
        try!(peer.raft_group.step(msg.take_message()));
        slow_log!(timer, "step takes {:?}", timer.elapsed());

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

    fn on_ready_change_peer(&mut self,
                            region_id: u64,
                            change_type: ConfChangeType,
                            store_id: u64) {
        // We only care remove itself now.
        if change_type == ConfChangeType::RemoveNode && store_id == self.store_id() {
            warn!("destroy peer {:?} for region {}", store_id, region_id);
            // The remove peer is in the same store.
            // TODO: should we check None here?
            // Can we destroy it in another thread later?
            let mut p = self.region_peers.remove(&region_id).unwrap();
            let end_key = enc_end_key(&p.region());
            if let Err(e) = p.destroy() {
                error!("destroy peer {} for region {} err {:?}",
                       store_id,
                       region_id,
                       e);
            } else {
                if self.region_ranges.remove(&end_key).is_none() {
                    panic!("Remove region, peer {}, region {}", store_id, region_id);

                }
            }
        }
    }

    fn on_ready_compact_log(&mut self, region_id: u64, state: RaftTruncatedState) {
        let peer = self.region_peers.get(&region_id).unwrap();
        let task = CompactTask::new(&peer.storage.rl(), state.get_index() + 1);
        if let Err(e) = self.compact_worker.schedule(task) {
            error!("failed to schedule compact task: {}", e);
        }
    }

    fn on_ready_split_region(&mut self,
                             region_id: u64,
                             left: metapb::Region,
                             right: metapb::Region) {
        let new_region_id = right.get_id();
        if let Some(peer) = self.region_peers.get(&new_region_id) {
            // If the store received a raft msg with the new region raft group
            // before splitting, it will creates a uninitialized peer.
            // We can remove this uninitialized peer directly.
            if peer.storage.rl().is_initialized() {
                panic!("duplicated region {} for split region", new_region_id);
            }
        }

        match Peer::create(self, &right) {
            Err(e) => {
                error!("create new split region {:?} err {:?}", right, e);
            }
            Ok(mut new_peer) => {
                // If the peer for the region before split is leader,
                // we can force the new peer for the new split region to campaign
                // to become the leader too.
                let is_leader = self.region_peers.get(&region_id).unwrap().is_leader();
                if is_leader && right.get_store_ids().len() > 1 {
                    if let Err(e) = new_peer.raft_group.campaign() {
                        error!("peer {} campaigns for region {} err {:?}",
                               new_peer.store_id(),
                               new_region_id,
                               e);
                    }
                }

                // Insert new regions and validation
                info!("insert new regions left: {:?}, right:{:?}", left, right);
                if self.region_ranges
                       .insert(enc_end_key(&left), left.get_id())
                       .is_some() {
                    panic!("region should not exist, {:?}", left);
                }
                if self.region_ranges
                       .insert(enc_end_key(&right), new_region_id)
                       .is_none() {
                    panic!("region should exist, {:?}", right);
                }
                self.region_peers.insert(new_region_id, new_peer);
            }
        }

    }

    fn on_ready_result(&mut self, region_id: u64, ready_result: ReadyResult) -> Result<()> {
        if let Some(region) = ready_result.snap_applied_region {
            self.region_ranges.insert(enc_end_key(&region), region.get_id());
        }

        // handle executing committed log results
        for result in ready_result.exec_results {
            match result {
                ExecResult::ChangePeer { change_type, store_id, .. } => {
                    self.on_ready_change_peer(region_id, change_type, store_id)
                }
                ExecResult::CompactLog { state } => self.on_ready_compact_log(region_id, state),
                ExecResult::SplitRegion { left, right } => {
                    self.on_ready_split_region(region_id, left, right)
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
            match self.execute_status_command(msg) {
                Err(e) => bind_error(&mut resp, e),
                Ok(status_resp) => resp = status_resp,
            };
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

        bind_term(&mut resp, peer.term());

        if !peer.is_leader() {
            bind_error(&mut resp,
                       Error::NotLeader(region_id, peer.leader_store_id()));
            return cb.call_box((resp,));
        }

        // Notice:
        // Here means the peer is leader, it can still step down to follower later,
        // but it doesn't matter, if the peer is not leader, the proposing command
        // log entry can't be committed.


        // TODO: support handing read-only commands later.
        // for read-only, if we don't care stale read, we can
        // execute these commands immediately in leader.

        let pending_cmd = PendingCmd {
            uuid: uuid,
            cb: cb,
        };
        try!(peer.propose(pending_cmd, msg, resp));

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
            // If we use current replicated_index (like 10) as the compact index,
            // when we replicate this log, the newest replicated_index will be 11,
            // but we only compact the log to 10, not 11, at that time,
            // the first index is 10, and replicated_index is 11, with an extra log,
            // and we will do compact again with compact index 11, in cycles...
            // So we introduce a threshold, if replicated index - first index > threshold,
            // we will try to compact log.
            // raft log entries[..............................................]
            //                  ^                                       ^
            //                  |-----------------threshold------------ |
            //              first_index                         replicated_index
            let replicated_idx = peer.raft_group
                                     .status()
                                     .progress
                                     .values()
                                     .map(|p| p.matched)
                                     .min()
                                     .unwrap();
            let applied_idx = peer.storage.rl().applied_index();
            let first_idx = peer.storage.rl().first_index();
            let compact_idx;
            if applied_idx > first_idx && applied_idx - first_idx >= self.cfg.raft_log_gc_limit {
                compact_idx = applied_idx;
            } else if replicated_idx < first_idx ||
               replicated_idx - first_idx <= self.cfg.raft_log_gc_threshold {
                continue;
            } else {
                compact_idx = replicated_idx;
            }

            // Create a compact log request and notify directly.
            let request = new_compact_log_request(region_id, compact_idx);

            let cb = Box::new(move |_: RaftCmdResponse| -> Result<()> { Ok(()) });

            if let Err(e) = self.sendch.send(Msg::RaftCmd {
                request: request,
                callback: cb,
            }) {
                error!("send compact log {} to region {} err {:?}",
                       compact_idx,
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
        if self.split_check_worker.is_busy() {
            self.register_split_region_check_tick(event_loop);
            return;
        }
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
            let task = SplitCheckTask::new(&peer.storage.rl());
            if let Err(e) = self.split_check_worker.schedule(task) {
                error!("failed to schedule split check: {}", e);
            }
            peer.size_diff_hint = 0;
        }

        self.register_split_region_check_tick(event_loop);
    }

    fn on_split_check_result(&mut self,
                             region_id: u64,
                             epoch: metapb::RegionEpoch,
                             split_key: Vec<u8>) {
        if split_key.is_empty() {
            error!("split key should not be empty!!!");
            return;
        }
        let p = self.region_peers.get(&region_id);
        if p.is_none() || !p.unwrap().is_leader() {
            // region on this store is no longer leader, skipped.
            info!("{} doesn't exist or is not leader, skip.", region_id);
            return;
        }

        let peer = p.unwrap();
        let region = peer.region();

        if region.get_region_epoch().get_version() != epoch.get_version() {
            info!("{} epoch changed {:?} != {:?}, need re-check later",
                  region_id,
                  region.get_region_epoch(),
                  epoch);
            return;
        }

        let key = keys::origin_key(&split_key);
        let task = PdTask::AskSplit {
            region: region,
            split_key: key.to_vec(),
            leader_store_id: peer.store_id(),
        };

        if let Err(e) = self.pd_worker.schedule(task) {
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

            let peer_count = peer.region().get_store_ids().len();
            let max_count = self.cluster_meta.get_max_peer_number() as usize;
            if peer_count == max_count {
                continue;
            }
            let mut change_type = ConfChangeType::AddNode;
            if peer_count > max_count {
                change_type = ConfChangeType::RemoveNode;
            }
            info!("peer count {} != max_peer_number {}, notifying pd",
                  peer_count,
                  max_count);
            let task = PdTask::AskChangePeer {
                change_type: change_type,
                region: peer.region(),
                leader_store_id: peer.store_id(),
            };
            if let Err(e) = self.pd_worker.schedule(task) {
                error!("failed to notify pd: {}", e);
            }
        }

        self.register_replica_check_tick(event_loop);
    }

    fn on_report_snapshot(&mut self, region_id: u64, to_store_id: u64, status: SnapshotStatus) {
        if let Some(mut peer) = self.region_peers.get_mut(&region_id) {
            info!("report to snapshot {} for {} {:?}",
                  to_store_id,
                  region_id,
                  status);
            peer.raft_group.report_snapshot(to_store_id, status)
        }
    }

    fn on_unreachable(&mut self, region_id: u64, to_store_id: u64) {
        if let Some(mut peer) = self.region_peers.get_mut(&region_id) {
            peer.raft_group.report_unreachable(to_store_id);
        }
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
    event_loop.timeout(tick, Duration::from_millis(delay))
              .map_err(|e| box_err!("register timer err: {:?}", e))
}

fn new_compact_log_request(region_id: u64, compact_index: u64) -> RaftCmdRequest {
    let mut request = RaftCmdRequest::new();
    request.mut_header().set_region_id(region_id);
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
        let t = SlowTimer::new();
        let msg_str = format!("{:?}", msg);
        match msg {
            Msg::RaftMessage(data) => {
                if let Err(e) = self.on_raft_message(data) {
                    error!("handle raft message err: {:?}", e);
                }
            }
            Msg::RaftCmd { request, callback } => {
                if let Err(e) = self.propose_raft_command(request, callback) {
                    error!("propose raft command err: {:?}", e);
                }
            }
            Msg::Quit => {
                info!("receive quit message");
                event_loop.shutdown();
            }
            Msg::SplitCheckResult { region_id, epoch, split_key } => {
                info!("split check of {} complete.", region_id);
                self.on_split_check_result(region_id, epoch, split_key);
            }
            Msg::ReportSnapshot { region_id, to_store_id, status } => {
                self.on_report_snapshot(region_id, to_store_id, status);
            }
            Msg::ReportUnreachable { region_id, to_store_id } => {
                self.on_unreachable(region_id, to_store_id);
            }
        }
        slow_log!(t, "handle {:?} takes {:?}", msg_str, t.elapsed());
    }

    fn timeout(&mut self, event_loop: &mut EventLoop<Self>, timeout: Tick) {
        let t = SlowTimer::new();
        match timeout {
            Tick::Raft => self.on_raft_base_tick(event_loop),
            Tick::RaftLogGc => self.on_raft_gc_log_tick(event_loop),
            Tick::SplitRegionCheck => self.on_split_region_check_tick(event_loop),
            Tick::ReplicaCheck => self.on_replica_check_tick(event_loop),
        }
        slow_log!(t, "handle timeout {:?} takes {:?}", timeout, t.elapsed());
    }

    fn tick(&mut self, event_loop: &mut EventLoop<Self>) {
        if !event_loop.is_running() {
            *self.stopped.wl() = true;
            // TODO: do some close here.

            if let Err(e) = self.split_check_worker.stop() {
                error!("failed to stop split scan thread: {:?}!!!", e);
            }

            if let Err(e) = self.snap_worker.stop() {
                error!("failed to stop snap thread: {:?}!!!", e);
            }

            if let Err(e) = self.compact_worker.stop() {
                error!("failed to stop compact thread: {:?}!!!", e);
            }

            if let Err(e) = self.pd_worker.stop() {
                error!("failed to stop pd thread: {:?}!!!", e);
            }

            return;
        }

        let t = SlowTimer::new();
        // We handle raft ready in event loop.
        if let Err(e) = self.on_raft_ready() {
            // TODO: should we panic here or shutdown the store?
            error!("handle raft ready err: {:?}", e);
        }
        slow_log!(t, "handle raft ready takes {:?}", t.elapsed());
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
            bind_term(&mut resp, peer.term());
        }
        Ok(resp)
    }

    fn execute_region_leader(&mut self, request: RaftCmdRequest) -> Result<StatusResponse> {
        let peer = try!(self.mut_target_peer(&request));

        let mut resp = StatusResponse::new();
        if let Some(leader) = peer.leader_store_id() {
            resp.mut_region_leader().set_leader_store_id(leader);
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
        if let Some(leader) = peer.leader_store_id() {
            resp.mut_region_detail().set_leader_store_id(leader);
        }

        Ok(resp)
    }
}
