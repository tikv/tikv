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
use std::rc::Rc;
use std::cell::RefCell;
use std::option::Option;
use std::collections::{HashMap, HashSet, BTreeMap};
use std::boxed::Box;
use std::collections::Bound::{Excluded, Unbounded};
use std::time::{Duration, Instant};
use std::{cmp, u64};

use rocksdb::DB;
use mio::{self, EventLoop, EventLoopBuilder, Sender};
use protobuf;
use fs2;
use uuid::Uuid;
use time::{self, Timespec};

use kvproto::raft_serverpb::{RaftMessage, RaftSnapshotData, RaftTruncatedState, RegionLocalState,
                             PeerState};
use kvproto::eraftpb::{ConfChangeType, Snapshot, MessageType};
use kvproto::pdpb::StoreStats;
use util::{HandyRwLock, SlowTimer, duration_to_nanos};
use pd::PdClient;
use kvproto::raft_cmdpb::{AdminCmdType, AdminRequest, StatusCmdType, StatusResponse,
                          RaftCmdRequest, RaftCmdResponse};
use protobuf::Message;
use raft::{SnapshotStatus, INVALID_INDEX};
use raftstore::{Result, Error};
use kvproto::metapb;
use util::worker::{Worker, Scheduler};
use util::transport::SendCh;
use util::rocksdb;
use storage::{ALL_CFS, CF_DEFAULT, CF_LOCK, CF_WRITE};
use super::worker::{SplitCheckRunner, SplitCheckTask, RegionTask, RegionRunner, CompactTask,
                    CompactRunner, RaftlogGcTask, RaftlogGcRunner, PdRunner, PdTask};
use super::{util, Msg, Tick, SnapManager};
use super::keys::{self, enc_start_key, enc_end_key, data_end_key, data_key};
use super::engine::{Iterable, Peekable, delete_all_in_range};
use super::config::Config;
use super::peer::{Peer, PendingCmd, ReadyResult, ExecResult, StaleState};
use super::peer_storage::{ApplySnapResult, SnapState};
use super::msg::Callback;
use super::cmd_resp::{bind_uuid, bind_term, bind_error};
use super::transport::Transport;
use super::metrics::*;
use super::local_metrics::RaftMetrics;

type Key = Vec<u8>;

const ROCKSDB_TOTAL_SST_FILE_SIZE_PROPERTY: &'static str = "rocksdb.total-sst-files-size";

pub struct Store<T: Transport, C: PdClient + 'static> {
    cfg: Config,
    store: metapb::Store,
    engine: Arc<DB>,
    sendch: SendCh<Msg>,

    // region_id -> peers
    region_peers: HashMap<u64, Peer>,
    pending_raft_groups: HashSet<u64>,
    // region end key -> region id
    region_ranges: BTreeMap<Key, u64>,
    pending_regions: Vec<metapb::Region>,
    split_check_worker: Worker<SplitCheckTask>,
    region_worker: Worker<RegionTask>,
    raftlog_gc_worker: Worker<RaftlogGcTask>,
    compact_worker: Worker<CompactTask>,
    pd_worker: Worker<PdTask>,

    trans: T,
    pd_client: Arc<C>,

    peer_cache: Rc<RefCell<HashMap<u64, metapb::Peer>>>,

    snap_mgr: SnapManager,

    raft_metrics: RaftMetrics,

    tag: String,

    start_time: Timespec,
}

pub fn create_event_loop<T, C>(cfg: &Config) -> Result<EventLoop<Store<T, C>>>
    where T: Transport,
          C: PdClient
{
    // We use base raft tick as the event loop timer tick.
    let mut builder = EventLoopBuilder::new();
    builder.timer_tick(Duration::from_millis(cfg.raft_base_tick_interval));
    builder.notify_capacity(cfg.notify_capacity);
    builder.messages_per_tick(cfg.messages_per_tick);
    let event_loop = try!(builder.build());
    Ok(event_loop)
}

impl<T: Transport, C: PdClient> Store<T, C> {
    pub fn new(sender: Sender<Msg>,
               meta: metapb::Store,
               cfg: Config,
               engine: Arc<DB>,
               trans: T,
               pd_client: Arc<C>,
               mgr: SnapManager)
               -> Result<Store<T, C>> {
        // TODO: we can get cluster meta regularly too later.
        try!(cfg.validate());

        let sendch = SendCh::new(sender, "raftstore");
        let peer_cache = HashMap::new();
        let tag = format!("[store {}]", meta.get_id());

        let mut s = Store {
            cfg: cfg,
            store: meta,
            engine: engine,
            sendch: sendch,
            region_peers: HashMap::new(),
            pending_raft_groups: HashSet::new(),
            split_check_worker: Worker::new("split check worker"),
            region_worker: Worker::new("snapshot worker"),
            raftlog_gc_worker: Worker::new("raft gc worker"),
            compact_worker: Worker::new("compact worker"),
            pd_worker: Worker::new("pd worker"),
            region_ranges: BTreeMap::new(),
            pending_regions: vec![],
            trans: trans,
            pd_client: pd_client,
            peer_cache: Rc::new(RefCell::new(peer_cache)),
            snap_mgr: mgr,
            raft_metrics: RaftMetrics::default(),
            tag: tag,
            start_time: time::get_time(),
        };
        try!(s.init());
        Ok(s)
    }

    /// Initialize this store. It scans the db engine, loads all regions
    /// and their peers from it, and schedules snapshot worker if neccessary.
    /// WARN: This store should not be used before initialized.
    fn init(&mut self) -> Result<()> {
        // Scan region meta to get saved regions.
        let start_key = keys::REGION_META_MIN_KEY;
        let end_key = keys::REGION_META_MAX_KEY;
        let engine = self.engine.clone();
        let mut total_count = 0;
        let mut tomebstone_count = 0;
        let mut applying_count = 0;

        let t = Instant::now();
        try!(engine.scan(start_key,
                         end_key,
                         false,
                         &mut |key, value| {
            let (region_id, suffix) = try!(keys::decode_region_meta_key(key));
            if suffix != keys::REGION_STATE_SUFFIX {
                return Ok(true);
            }

            total_count += 1;

            let local_state = try!(protobuf::parse_from_bytes::<RegionLocalState>(value));
            let region = local_state.get_region();
            if local_state.get_state() == PeerState::Tombstone {
                tomebstone_count += 1;
                debug!("region {:?} is tombstone in store {}",
                       region,
                       self.store_id());
                return Ok(true);
            }
            let mut peer = try!(Peer::create(self, region));

            if local_state.get_state() == PeerState::Applying {
                applying_count += 1;
                info!("region {:?} is applying in store {}",
                      local_state.get_region(),
                      self.store_id());
                peer.mut_store().schedule_applying_snapshot();
            }

            self.region_ranges.insert(enc_end_key(region), region_id);
            // No need to check duplicated here, because we use region id as the key
            // in DB.
            self.region_peers.insert(region_id, peer);
            Ok(true)
        }));

        info!("{} starts with {} regions, including {} tombstones and {} applying \
               regions, takes {:?}",
              self.tag,
              total_count,
              tomebstone_count,
              applying_count,
              t.elapsed());

        try!(self.clean_up());

        Ok(())
    }

    /// `clean_up` clean up all possible garbage data.
    fn clean_up(&mut self) -> Result<()> {
        let t = Instant::now();
        let mut last_start_key = keys::data_key(b"");
        for region_id in self.region_ranges.values() {
            let region = self.region_peers[region_id].region();
            let start_key = keys::enc_start_key(region);
            try!(delete_all_in_range(&self.engine, &last_start_key, &start_key));
            last_start_key = keys::enc_end_key(region);
        }

        try!(delete_all_in_range(&self.engine, &last_start_key, keys::DATA_MAX_KEY));

        info!("{} cleans up garbage data, takes {:?}",
              self.tag,
              t.elapsed());
        Ok(())
    }

    pub fn run(&mut self, event_loop: &mut EventLoop<Self>) -> Result<()> {
        try!(self.snap_mgr.wl().init());

        self.register_raft_base_tick(event_loop);
        self.register_raft_gc_log_tick(event_loop);
        self.register_split_region_check_tick(event_loop);
        self.register_compact_check_tick(event_loop);
        self.register_pd_heartbeat_tick(event_loop);
        self.register_pd_store_heartbeat_tick(event_loop);
        self.register_snap_mgr_gc_tick(event_loop);
        self.register_compact_lock_cf_tick(event_loop);

        let split_check_runner = SplitCheckRunner::new(self.sendch.clone(),
                                                       self.cfg.region_max_size,
                                                       self.cfg.region_split_size);
        box_try!(self.split_check_worker.start(split_check_runner));

        let runner = RegionRunner::new(self.engine.clone(),
                                       self.get_sendch(),
                                       self.snap_mgr.clone(),
                                       self.cfg.snap_apply_batch_size);
        box_try!(self.region_worker.start(runner));

        let raftlog_gc_runner = RaftlogGcRunner;
        box_try!(self.raftlog_gc_worker.start(raftlog_gc_runner));

        let compact_runner = CompactRunner::new(self.engine.clone());
        box_try!(self.compact_worker.start(compact_runner));

        let pd_runner = PdRunner::new(self.pd_client.clone(), self.sendch.clone());
        box_try!(self.pd_worker.start(pd_runner));

        try!(event_loop.run(self));
        Ok(())
    }

    pub fn get_sendch(&self) -> SendCh<Msg> {
        self.sendch.clone()
    }

    #[inline]
    pub fn get_snap_mgr(&self) -> SnapManager {
        self.snap_mgr.clone()
    }

    pub fn snap_scheduler(&self) -> Scheduler<RegionTask> {
        self.region_worker.scheduler()
    }

    pub fn engine(&self) -> Arc<DB> {
        self.engine.clone()
    }

    pub fn store_id(&self) -> u64 {
        self.store.get_id()
    }

    pub fn config(&self) -> &Config {
        &self.cfg
    }

    pub fn peer_cache(&self) -> Rc<RefCell<HashMap<u64, metapb::Peer>>> {
        self.peer_cache.clone()
    }

    fn register_raft_base_tick(&self, event_loop: &mut EventLoop<Self>) {
        // If we register raft base tick failed, the whole raft can't run correctly,
        // TODO: shutdown the store?
        if let Err(e) = register_timer(event_loop, Tick::Raft, self.cfg.raft_base_tick_interval) {
            error!("{} register raft base tick err: {:?}", self.tag, e);
        };
    }

    fn on_raft_base_tick(&mut self, event_loop: &mut EventLoop<Self>) {
        let t = Instant::now();
        for (&region_id, peer) in &mut self.region_peers {
            if !peer.get_store().is_applying() {
                peer.raft_group.tick();

                // If this peer detects the leader is missing for a long long time,
                // it should consider itself as a stale peer which is removed from
                // the original cluster.
                // This most likely happens in the following scenario:
                // At first, there are three peer A, B, C in the cluster, and A is leader.
                // Peer B gets down. And then A adds D, E, F into the cluster.
                // Peer D becomes leader of the new cluster, and then removes peer A, B, C.
                // After all these peer in and out, now the cluster has peer D, E, F.
                // If peer B goes up at this moment, it still thinks it is one of the cluster
                // and has peers A, C. However, it could not reach A, C since they are removed
                // from the cluster or probably destroyed.
                // Meantime, D, E, F would not reach B, since it's not in the cluster anymore.
                // In this case, peer B would notice that the leader is missing for a long time,
                // and it would check with pd to confirm whether it's still a member of the cluster.
                // If not, it destroys itself as a stale peer which is removed out already.
                match peer.check_stale_state(self.cfg.max_leader_missing_duration) {
                    StaleState::Valid => {
                        self.pending_raft_groups.insert(region_id);
                    }
                    StaleState::ToValidate => {
                        // for peer B in case 1 above
                        info!("{} detects leader missing for a long time. To check with pd \
                               whether it's still valid",
                              peer.tag);
                        let task = PdTask::ValidatePeer {
                            peer: peer.peer.clone(),
                            region: peer.region().clone(),
                        };
                        if let Err(e) = self.pd_worker.schedule(task) {
                            error!("{} failed to notify pd: {}", peer.tag, e)
                        }

                        self.pending_raft_groups.insert(region_id);
                    }
                }
            }
        }

        PEER_RAFT_PROCESS_NANOS_COUNTER_VEC.with_label_values(&["tick"])
            .inc_by(duration_to_nanos(t.elapsed()) as f64)
            .unwrap();

        self.raft_metrics.flush();

        self.register_raft_base_tick(event_loop);
    }

    /// If target peer doesn't exist, create it.
    ///
    /// return false to indicate that target peer is in invalid state or
    /// doesn't exist and can't be created.
    fn maybe_create_peer(&mut self, region_id: u64, msg: &RaftMessage) -> Result<bool> {
        let target = msg.get_to_peer();
        // we may encounter a message with larger peer id, which means
        // current peer is stale, then we should remove current peer
        let mut has_peer = false;
        let mut stale_peer = None;
        if let Some(p) = self.region_peers.get_mut(&region_id) {
            has_peer = true;
            let target_peer_id = target.get_id();
            if p.peer_id() < target_peer_id {
                if p.is_applying() && !p.mut_store().cancel_applying_snap() {
                    info!("[region {}] Stale peer {} is applying snapshot, will destroy next \
                           time.",
                          region_id,
                          p.peer_id());
                    return Ok(false);
                }
                stale_peer = Some(p.peer.clone());
            } else if p.peer_id() > target_peer_id {
                info!("target peer id {} is less than {}, msg maybe stale.",
                      target_peer_id,
                      p.peer_id());
                return Ok(false);
            }
        }
        if let Some(p) = stale_peer {
            info!("[region {}] destroying stale peer {:?}", region_id, p);
            self.destroy_peer(region_id, p);
            has_peer = false;
        }

        if has_peer {
            return Ok(true);
        }

        let message = msg.get_message();
        let msg_type = message.get_msg_type();
        if msg_type != MessageType::MsgRequestVote &&
           (msg_type != MessageType::MsgHeartbeat || message.get_commit() != INVALID_INDEX) {
            info!("target peer {:?} doesn't exist, stale message {:?}.",
                  target,
                  msg_type);
            return Ok(false);
        }

        let start_key = data_key(msg.get_start_key());
        if let Some((_, &exist_region_id)) = self.region_ranges
            .range(Excluded(&start_key), Unbounded::<&Key>)
            .next() {
            let exist_region = self.region_peers[&exist_region_id].region();
            if enc_start_key(exist_region) < data_end_key(msg.get_end_key()) {
                debug!("msg {:?} is overlapped with region {:?}, ignored",
                       msg,
                       exist_region);
                return Ok(false);
            }
        }

        let peer = try!(Peer::replicate(self, region_id, target.get_id()));
        // following snapshot may overlap, should insert into region_ranges after
        // snapshot is applied.
        self.region_peers.insert(region_id, peer);
        Ok(true)
    }

    // Clippy doesn't allow hash_map contains_key followed by insert, and suggests
    // using entry().or_insert() instead, but we can't use this because creating peer
    // may fail, so we allow map_entry.
    fn on_raft_message(&mut self, mut msg: RaftMessage) -> Result<()> {
        let region_id = msg.get_region_id();
        if !self.is_raft_msg_valid(&msg) {
            return Ok(());
        }

        if msg.get_is_tombstone() {
            // we receive a message tells us to remove ourself.
            self.handle_gc_peer_msg(&msg);
            return Ok(());
        }

        if try!(self.is_msg_stale(&msg)) {
            return Ok(());
        }

        if !try!(self.maybe_create_peer(region_id, &msg)) {
            return Ok(());
        }

        if !try!(self.check_snapshot(&msg)) {
            return Ok(());
        }

        self.insert_peer_cache(msg.take_from_peer());
        self.insert_peer_cache(msg.take_to_peer());

        let peer = self.region_peers.get_mut(&region_id).unwrap();
        let timer = SlowTimer::new();
        try!(peer.step(msg.take_message()));
        slow_log!(timer, "{} raft step", peer.tag);

        // Add into pending raft groups for later handling ready.
        self.pending_raft_groups.insert(region_id);

        Ok(())
    }

    // return false means the message is invalid, and can be ignored.
    fn is_raft_msg_valid(&self, msg: &RaftMessage) -> bool {
        let region_id = msg.get_region_id();
        let from = msg.get_from_peer();
        let to = msg.get_to_peer();

        debug!("[region {}] handle raft message {:?}, from {} to {}",
               region_id,
               msg.get_message().get_msg_type(),
               from.get_id(),
               to.get_id());

        if to.get_store_id() != self.store_id() {
            warn!("[region {}] store not match, to store id {}, mine {}, ignore it",
                  region_id,
                  to.get_store_id(),
                  self.store_id());
            return false;
        }

        if !msg.has_region_epoch() {
            error!("[region {}] missing epoch in raft message, ignore it",
                   region_id);
            return false;
        }

        true
    }

    fn is_msg_stale(&self, msg: &RaftMessage) -> Result<bool> {
        let region_id = msg.get_region_id();
        let from_epoch = msg.get_region_epoch();
        let is_vote_msg = msg.get_message().get_msg_type() == MessageType::MsgRequestVote;
        let from_store_id = msg.get_from_peer().get_store_id();

        // Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
        // a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
        //  We should ignore this stale message and let 2 remove itself after
        //  applying the ConfChange log.
        // b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
        //  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
        // c. 2 is isolated but can communicate with 3. 1 removes 3.
        //  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
        // d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
        //  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
        // e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
        //  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
        //  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
        //  rejoin the raft group again.
        // f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
        //  unlike case e, 2 will be stale forever.
        // TODO: for case f, if 2 is stale for a long time, 2 will communicate with pd and pd will
        // tell 2 is stale, so 2 can remove itself.
        if let Some(peer) = self.region_peers.get(&region_id) {
            let region = &peer.get_store().region;
            let epoch = region.get_region_epoch();

            if util::is_epoch_stale(from_epoch, epoch) &&
               util::find_peer(region, from_store_id).is_none() {
                // The message is stale and not in current region.
                self.handle_stale_msg(msg, epoch, is_vote_msg);
                return Ok(true);
            }

            return Ok(false);
        }

        // no exist, check with tombstone key.
        let state_key = keys::region_state_key(region_id);
        if let Some(local_state) = try!(self.engine.get_msg::<RegionLocalState>(&state_key)) {
            assert_eq!(local_state.get_state(), PeerState::Tombstone);
            let region = local_state.get_region();
            let region_epoch = region.get_region_epoch();
            // The region in this peer is already destroyed
            if util::is_epoch_stale(from_epoch, region_epoch) {
                info!("[region {}] tombstone peer [epoch: {:?}] \
                    receive a stale message {:?}", region_id,
                    region_epoch,
                        msg,
                        );

                let not_exist = util::find_peer(region, from_store_id).is_none();
                self.handle_stale_msg(msg, region_epoch, is_vote_msg && not_exist);

                return Ok(true);
            }

            if from_epoch.get_conf_ver() == region_epoch.get_conf_ver() {
                return Err(box_err!("tombstone peer [epoch: {:?}] receive an invalid \
                                        message {:?}, ignore it",
                                    region_epoch,
                                    msg));
            }
        }

        Ok(false)
    }

    fn handle_stale_msg(&self, msg: &RaftMessage, cur_epoch: &metapb::RegionEpoch, need_gc: bool) {
        let region_id = msg.get_region_id();
        let from_peer = msg.get_from_peer();
        let to_peer = msg.get_to_peer();

        if !need_gc {
            info!("[region {}] raft message {:?} is stale, current {:?}, ignore it",
                  region_id,
                  msg,
                  cur_epoch);
            return;
        }

        info!("[region {}] raft message {:?} is stale, current {:?}, tell to gc",
              region_id,
              msg,
              cur_epoch);

        let mut gc_msg = RaftMessage::new();
        gc_msg.set_region_id(region_id);
        gc_msg.set_from_peer(to_peer.clone());
        gc_msg.set_to_peer(from_peer.clone());
        gc_msg.set_region_epoch(cur_epoch.clone());
        gc_msg.set_is_tombstone(true);
        if let Err(e) = self.trans.send(gc_msg) {
            error!("[region {}] send gc message failed {:?}", region_id, e);
        }
    }

    fn handle_gc_peer_msg(&mut self, msg: &RaftMessage) {
        let region_id = msg.get_region_id();

        let mut need_remove = false;
        if let Some(peer) = self.region_peers.get(&region_id) {
            // TODO: need checking peer id changed?
            let from_epoch = msg.get_region_epoch();
            if util::is_epoch_stale(peer.get_store().region.get_region_epoch(), from_epoch) {
                // TODO: ask pd to guarantee we are stale now.
                info!("[region {}] peer {:?} receives gc message, remove",
                      region_id,
                      msg.get_to_peer());
                need_remove = true;
            }
        }

        if need_remove {
            self.destroy_peer(region_id, msg.get_to_peer().clone());
        }
    }

    fn check_snapshot(&mut self, msg: &RaftMessage) -> Result<bool> {
        let region_id = msg.get_region_id();

        // Check if we can accept the snapshot
        if self.region_peers[&region_id].get_store().is_initialized() ||
           !msg.get_message().has_snapshot() {
            return Ok(true);
        }

        let snap = msg.get_message().get_snapshot();
        let mut snap_data = RaftSnapshotData::new();
        try!(snap_data.merge_from_bytes(snap.get_data()));
        let snap_region = snap_data.take_region();
        let peer_id = msg.get_to_peer().get_id();
        if snap_region.get_peers().into_iter().all(|p| p.get_id() != peer_id) {
            info!("region {:?} doesn't contain peer {:?}, skip.",
                  snap_region,
                  msg.get_to_peer());
            return Ok(false);
        }
        if let Some((_, &exist_region_id)) = self.region_ranges
            .range(Excluded(&enc_start_key(&snap_region)), Unbounded::<&Key>)
            .next() {
            let exist_region = self.region_peers[&exist_region_id].region();
            if enc_start_key(exist_region) < enc_end_key(&snap_region) {
                info!("region overlapped {:?}, {:?}", exist_region, snap_region);
                return Ok(false);
            }
        }
        for region in &self.pending_regions {
            if enc_start_key(region) < enc_end_key(&snap_region) &&
               enc_end_key(region) > enc_start_key(&snap_region) &&
               // Same region can overlap, we will apply the latest version of snapshot.
               region.get_id() != snap_region.get_id() {
                info!("pending region overlapped {:?}, {:?}", region, snap_region);
                return Ok(false);
            }
        }
        self.pending_regions.push(snap_region);

        Ok(true)
    }

    fn insert_peer_cache(&mut self, peer: metapb::Peer) {
        self.peer_cache.borrow_mut().insert(peer.get_id(), peer);
    }

    fn on_raft_ready(&mut self) {
        let t = SlowTimer::new();
        let ids: Vec<u64> = self.pending_raft_groups.drain().collect();
        let pending_count = ids.len();

        let mut ready_results: Vec<(u64, ReadyResult)> = Vec::with_capacity(ids.len());
        for region_id in ids {
            if let Some(peer) = self.region_peers.get_mut(&region_id) {
                match peer.handle_raft_ready_append(&self.trans, &mut self.raft_metrics) {
                    Err(e) => {
                        // TODO: should we panic or shutdown the store?
                        error!("{} handle raft ready append err: {:?}", peer.tag, e);
                        continue;
                    }
                    Ok(Some(res)) => ready_results.push((region_id, res)),
                    Ok(None) => {}
                }
            }
        }

        for (region_id, mut res) in ready_results {
            {
                let peer = self.region_peers.get_mut(&region_id).unwrap();

                if let Err(e) = peer.handle_raft_ready_apply(&mut res) {
                    // TODO: should we panic or shutdown the store?
                    error!("{} handle raft ready err: {:?}", peer.tag, e);
                    continue;
                }
            };

            self.on_ready_result(region_id, res)
        }

        PEER_RAFT_PROCESS_NANOS_COUNTER_VEC.with_label_values(&["ready"])
            .inc_by(duration_to_nanos(t.elapsed()) as f64)
            .unwrap();
        slow_log!(t, "{} on {} regions raft ready", self.tag, pending_count);
    }

    fn destroy_peer(&mut self, region_id: u64, peer: metapb::Peer) {
        info!("[region {}] destroy peer {:?}", region_id, peer);
        // TODO: should we check None here?
        // Can we destroy it in another thread later?
        let mut p = self.region_peers.remove(&region_id).unwrap();
        // We can't destroy a peer which is applying snapshot.
        assert!(!p.is_applying());

        let is_initialized = p.is_initialized();
        if let Err(e) = p.destroy() {
            // If not panic here, the peer will be recreated in the next restart,
            // then it will be gc again. But if some overlap region is created
            // before restarting, the gc action will delete the overlap region's
            // data too.
            panic!("[region {}] destroy peer {:?} in store {} err {:?}",
                   region_id,
                   peer,
                   self.store_id(),
                   e);
        }

        if is_initialized && self.region_ranges.remove(&enc_end_key(p.region())).is_none() {
            panic!("[region {}] remove peer {:?} in store {}",
                   region_id,
                   peer,
                   self.store_id());

        }
    }

    fn on_ready_change_peer(&mut self,
                            region_id: u64,
                            change_type: ConfChangeType,
                            peer: metapb::Peer) {
        let mut peer_id = 0;
        if let Some(p) = self.region_peers.get(&region_id) {
            if p.is_leader() {
                // Notify pd immediately.
                info!("{} notify pd with change peer region {:?}",
                      p.tag,
                      p.region());
                self.heartbeat_pd(p);
            }
            peer_id = p.peer_id();
        }

        // We only care remove itself now.
        if change_type == ConfChangeType::RemoveNode && peer.get_store_id() == self.store_id() {
            if peer_id == peer.get_id() {
                self.destroy_peer(region_id, peer)
            } else {
                panic!("trying to remove unknown peer {:?}", peer);
            }
        }
    }

    fn on_ready_compact_log(&mut self, region_id: u64, state: RaftTruncatedState) {
        let mut peer = self.region_peers.get_mut(&region_id).unwrap();
        let task = RaftlogGcTask {
            engine: peer.get_store().get_engine().clone(),
            region_id: peer.get_store().get_region_id(),
            start_idx: peer.last_compacted_idx,
            end_idx: state.get_index() + 1,
        };
        peer.last_compacted_idx = state.get_index() + 1;
        if let Err(e) = self.raftlog_gc_worker.schedule(task) {
            error!("[region {}] failed to schedule compact task: {}",
                   region_id,
                   e);
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
            if peer.get_store().is_initialized() {
                panic!("duplicated region {} for split region", new_region_id);
            }
        }

        match Peer::create(self, &right) {
            Err(e) => {
                // peer information is already written into db, can't recover.
                // there is probably a bug.
                panic!("create new split region {:?} err {:?}", right, e);
            }
            Ok(mut new_peer) => {
                // If the peer for the region before split is leader,
                // we can force the new peer for the new split region to campaign
                // to become the leader too.
                let is_leader = self.region_peers.get(&region_id).unwrap().is_leader();
                if is_leader && right.get_peers().len() > 1 {
                    if let Err(e) = new_peer.raft_group.campaign() {
                        error!("[region {}] peer {:?} campaigns  err {:?}",
                               new_region_id,
                               new_peer.peer,
                               e);
                    }
                }

                if is_leader {
                    // Notify pd immediately to let it update the region meta.
                    let left = self.region_peers.get(&region_id).unwrap();
                    self.report_split_pd(left, &new_peer);
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
                new_peer.size_diff_hint = self.cfg.region_check_size_diff;
                self.region_peers.insert(new_region_id, new_peer);
            }
        }
    }

    fn report_split_pd(&self, left: &Peer, right: &Peer) {
        let left_region = left.region();
        let right_region = right.region();

        info!("notify pd with split left {:?}, right {:?}",
              left_region,
              right_region);
        self.heartbeat_pd(left);
        self.heartbeat_pd(right);

        // Now pd only uses ReportSplit for history operation show,
        // so we send it independently here.
        let task = PdTask::ReportSplit {
            left: left_region.clone(),
            right: right_region.clone(),
        };

        if let Err(e) = self.pd_worker.schedule(task) {
            error!("{} failed to notify pd: {}", self.tag, e);
        }
    }

    fn on_ready_apply_snapshot(&mut self, apply_result: ApplySnapResult) {
        let prev_region = apply_result.prev_region;
        let region = apply_result.region;
        let region_id = region.get_id();

        info!("[region {}] snapshot for region {:?} is applied",
              region_id,
              region);

        if !prev_region.get_peers().is_empty() {
            info!("[region {}] region changed from {:?} -> {:?} after applying snapshot",
                  region_id,
                  prev_region,
                  region);
            // we have already initialized the peer, so it must exist in region_ranges.
            if self.region_ranges.remove(&enc_end_key(&prev_region)).is_none() {
                panic!("[region {}] region should exist {:?}",
                       region_id,
                       prev_region);
            }
        }

        self.region_ranges.insert(enc_end_key(&region), region.get_id());
    }

    fn on_ready_result(&mut self, region_id: u64, ready_result: ReadyResult) {
        if let Some(apply_result) = ready_result.apply_snap_result {
            self.on_ready_apply_snapshot(apply_result);
        }

        let t = SlowTimer::new();
        let result_count = ready_result.exec_results.len();
        // handle executing committed log results
        for result in ready_result.exec_results {
            match result {
                ExecResult::ChangePeer { change_type, peer, .. } => {
                    self.on_ready_change_peer(region_id, change_type, peer)
                }
                ExecResult::CompactLog { state } => self.on_ready_compact_log(region_id, state),
                ExecResult::SplitRegion { left, right } => {
                    self.on_ready_split_region(region_id, left, right)
                }
            }
        }
        slow_log!(t,
                  "[region {}] on ready {} results",
                  region_id,
                  result_count);
    }

    fn propose_raft_command(&mut self, msg: RaftCmdRequest, cb: Callback) {
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

        let store_id = msg.get_header().get_peer().get_store_id();
        if store_id != self.store.get_id() {
            bind_error(&mut resp,
                       box_err!("mismatch store id {} != {}", store_id, self.store.get_id()));
            return cb.call_box((resp,));
        }

        if msg.has_status_request() {
            // For status commands, we handle it here directly.
            match self.execute_status_command(msg) {
                Err(e) => bind_error(&mut resp, e),
                Ok(status_resp) => resp = status_resp,
            };
            return cb.call_box((resp,));
        }

        if let Err(e) = self.validate_region(&msg) {
            bind_error(&mut resp, e);
            return cb.call_box((resp,));
        }

        // Note:
        // The peer that is being checked is a leader. It might step down to be a follower later. It
        // doesn't matter whether the peer is a leader or not. If it's not a leader, the proposing
        // command log entry can't be committed.

        let region_id = msg.get_header().get_region_id();
        let mut peer = self.region_peers.get_mut(&region_id).unwrap();
        let term = peer.term();
        bind_term(&mut resp, term);
        let pending_cmd = PendingCmd {
            uuid: uuid,
            term: term,
            cb: Some(cb),
            renew_lease_time: None,
        };
        if peer.propose(pending_cmd, msg, resp, &mut self.raft_metrics.propose) {
            self.pending_raft_groups.insert(region_id);
        }

        // TODO: add timeout, if the command is not applied after timeout,
        // we will call the callback with timeout error.
    }

    fn validate_region(&self, msg: &RaftCmdRequest) -> Result<()> {
        let region_id = msg.get_header().get_region_id();
        let peer_id = msg.get_header().get_peer().get_id();

        let peer = match self.region_peers.get(&region_id) {
            Some(peer) => peer,
            None => return Err(Error::RegionNotFound(region_id)),
        };
        if !peer.is_leader() {
            return Err(Error::NotLeader(region_id, peer.get_peer_from_cache(peer.leader_id())));
        }
        if peer.peer_id() != peer_id {
            return Err(box_err!("mismatch peer id {} != {}", peer.peer_id(), peer_id));
        }

        let res = peer.check_epoch(msg);
        if let Err(Error::StaleEpoch(msg, mut new_regions)) = res {
            // Attach the next region which might be split from the current region. But it doesn't
            // matter if the next region is not split from the current region. If the region meta
            // received by the TiKV driver is newer than the meta cached in the driver, the meta is
            // updated.
            if let Some((_, &next_region_id)) = self.region_ranges
                .range(Excluded(&enc_end_key(peer.region())), Unbounded::<&Key>)
                .next() {
                let next_region = self.region_peers[&next_region_id].region();
                new_regions.push(next_region.to_owned());
            }
            return Err(Error::StaleEpoch(msg, new_regions));
        }
        res
    }

    fn register_raft_gc_log_tick(&self, event_loop: &mut EventLoop<Self>) {
        if let Err(e) = register_timer(event_loop,
                                       Tick::RaftLogGc,
                                       self.cfg.raft_log_gc_tick_interval) {
            // If failed, we can't cleanup the raft log regularly.
            // Although the log size will grow larger and larger, it doesn't affect
            // whole raft logic, and we can send truncate log command to compact it.
            error!("{} register raft gc log tick err: {:?}", self.tag, e);
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
            let applied_idx = peer.get_store().applied_index();
            let first_idx = peer.get_store().first_index();
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
            let request = new_compact_log_request(region_id, peer.peer.clone(), compact_idx);

            if let Err(e) = self.sendch.try_send(Msg::RaftCmd {
                request: request,
                callback: Box::new(|_| {}),
            }) {
                error!("{} send compact log {} err {:?}", peer.tag, compact_idx, e);
            }
        }

        self.register_raft_gc_log_tick(event_loop);
    }

    fn register_split_region_check_tick(&self, event_loop: &mut EventLoop<Self>) {
        if let Err(e) = register_timer(event_loop,
                                       Tick::SplitRegionCheck,
                                       self.cfg.split_region_check_tick_interval) {
            error!("{} register split region check tick err: {:?}", self.tag, e);
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
        for (_, peer) in &mut self.region_peers {
            if !peer.is_leader() {
                continue;
            }

            if peer.size_diff_hint < self.cfg.region_check_size_diff {
                continue;
            }
            info!("{} region's size diff {} >= {}, need to check whether should split",
                  peer.tag,
                  peer.size_diff_hint,
                  self.cfg.region_check_size_diff);
            let task = SplitCheckTask::new(peer.get_store());
            if let Err(e) = self.split_check_worker.schedule(task) {
                error!("{} failed to schedule split check: {}", self.tag, e);
            }
            peer.size_diff_hint = 0;
        }

        self.register_split_region_check_tick(event_loop);
    }

    fn register_compact_check_tick(&self, event_loop: &mut EventLoop<Self>) {
        if let Err(e) = register_timer(event_loop,
                                       Tick::CompactCheck,
                                       self.cfg.region_compact_check_interval_secs * 1000) {
            error!("{} register compact check tick err: {:?}", self.tag, e);
        };
    }

    fn on_compact_check_tick(&mut self, event_loop: &mut EventLoop<Self>) {
        for (_, peer) in &mut self.region_peers {
            if peer.delete_keys_hint < self.cfg.region_compact_delete_keys_count {
                continue;
            }
            for &cf in &[CF_DEFAULT, CF_WRITE] {
                let task = CompactTask {
                    cf_name: String::from(cf),
                    start_key: Some(keys::enc_start_key(peer.region())),
                    end_key: Some(keys::enc_end_key(peer.region())),
                };
                if let Err(e) = self.compact_worker.schedule(task) {
                    error!("{} failed to schedule compact task: {}", self.tag, e);
                }
            }
            peer.delete_keys_hint = 0;
            // Compact only 1 region each check in case compact task accumulates.
            break;
        }
        self.register_compact_check_tick(event_loop);
    }

    fn on_split_check_result(&mut self,
                             region_id: u64,
                             epoch: metapb::RegionEpoch,
                             split_key: Vec<u8>) {
        if split_key.is_empty() {
            error!("[region {}] split key should not be empty!!!", region_id);
            return;
        }
        let p = self.region_peers.get(&region_id);
        if p.is_none() || !p.unwrap().is_leader() {
            // region on this store is no longer leader, skipped.
            info!("[region {}] region on {} doesn't exist or is not leader, skip.",
                  region_id,
                  self.store_id());
            return;
        }

        let peer = p.unwrap();
        let region = peer.region();

        if region.get_region_epoch().get_version() != epoch.get_version() {
            info!("{} epoch changed {:?} != {:?}, need re-check later",
                  peer.tag,
                  region.get_region_epoch(),
                  epoch);
            return;
        }

        let key = keys::origin_key(&split_key);
        let task = PdTask::AskSplit {
            region: region.clone(),
            split_key: key.to_vec(),
            peer: peer.peer.clone(),
        };

        if let Err(e) = self.pd_worker.schedule(task) {
            error!("{} failed to notify pd to split at {:?}: {}",
                   peer.tag,
                   split_key,
                   e);
        }
    }

    fn heartbeat_pd(&self, peer: &Peer) {
        let task = PdTask::Heartbeat {
            region: peer.region().clone(),
            peer: peer.peer.clone(),
            down_peers: peer.collect_down_peers(self.cfg.max_peer_down_duration),
        };
        if let Err(e) = self.pd_worker.schedule(task) {
            error!("{} failed to notify pd: {}", peer.tag, e);
        }
    }

    fn on_pd_heartbeat_tick(&mut self, event_loop: &mut EventLoop<Self>) {
        for peer in self.region_peers.values_mut() {
            peer.check_peers();
        }

        let mut leader_count = 0;
        for peer in self.region_peers.values() {
            if peer.is_leader() {
                leader_count += 1;
                self.heartbeat_pd(peer);
            }
        }

        STORE_PD_HEARTBEAT_GAUGE_VEC.with_label_values(&["leader"]).set(leader_count as f64);
        STORE_PD_HEARTBEAT_GAUGE_VEC.with_label_values(&["region"])
            .set(self.region_peers.len() as f64);

        self.register_pd_heartbeat_tick(event_loop);
    }


    fn register_pd_heartbeat_tick(&self, event_loop: &mut EventLoop<Self>) {
        if let Err(e) = register_timer(event_loop,
                                       Tick::PdHeartbeat,
                                       self.cfg.pd_heartbeat_tick_interval) {
            error!("{} register pd heartbeat tick err: {:?}", self.tag, e);
        };
    }

    fn store_heartbeat_pd(&mut self) {
        let mut stats = StoreStats::new();
        let disk_stats = match fs2::statvfs(self.engine.path()) {
            Err(e) => {
                error!("{} get disk stat for rocksdb {} failed: {}",
                       self.tag,
                       self.engine.path(),
                       e);
                return;
            }
            Ok(stats) => stats,
        };

        let capacity = cmp::min(disk_stats.total_space(), self.cfg.capacity);

        stats.set_capacity(capacity);

        // Must get the total SST file size here.
        let mut used_size: u64 = 0;
        for cf in ALL_CFS {
            let handle = rocksdb::get_cf_handle(&self.engine, cf).unwrap();
            let cf_used_size = self.engine
                .get_property_int_cf(handle, ROCKSDB_TOTAL_SST_FILE_SIZE_PROPERTY)
                .expect("rocksdb is too old, missing total-sst-files-size property");

            // It is important to monitor each cf's size, especially the "raft" and "lock" column
            // families.
            STORE_ENGINE_SIZE_GAUGE_VEC.with_label_values(&[cf]).set(cf_used_size as f64);

            used_size += cf_used_size;
        }

        used_size += self.snap_mgr.rl().get_total_snap_size();

        let mut available = if capacity > used_size {
            capacity - used_size
        } else {
            warn!("{} no available space", self.tag);
            0
        };

        // We only care rocksdb SST file size, so we should
        // check disk available here.
        if available > disk_stats.free_space() {
            available = disk_stats.free_space();
        }

        stats.set_store_id(self.store_id());
        stats.set_available(available);
        stats.set_region_count(self.region_peers.len() as u32);

        let snap_stats = self.snap_mgr.rl().stats();
        stats.set_sending_snap_count(snap_stats.sending_count as u32);
        stats.set_receiving_snap_count(snap_stats.receiving_count as u32);

        STORE_SIZE_GAUGE_VEC.with_label_values(&["capacity"]).set(capacity as f64);
        STORE_SIZE_GAUGE_VEC.with_label_values(&["available"]).set(available as f64);

        STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC.with_label_values(&["sending"])
            .set(snap_stats.sending_count as f64);
        STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC.with_label_values(&["receiving"])
            .set(snap_stats.receiving_count as f64);

        let mut apply_snapshot_count = 0;
        for peer in self.region_peers.values_mut() {
            if peer.mut_store().check_applying_snap() {
                apply_snapshot_count += 1;
            }
        }

        stats.set_applying_snap_count(apply_snapshot_count as u32);
        STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC.with_label_values(&["applying"])
            .set(apply_snapshot_count as f64);

        stats.set_start_time(self.start_time.sec as u32);

        if let Err(e) = self.pd_worker.schedule(PdTask::StoreHeartbeat { stats: stats }) {
            error!("{} failed to notify pd: {}", self.tag, e);
        }
    }

    fn on_pd_store_heartbeat_tick(&mut self, event_loop: &mut EventLoop<Self>) {
        self.store_heartbeat_pd();
        self.register_pd_store_heartbeat_tick(event_loop);
    }

    fn handle_snap_mgr_gc(&mut self) -> Result<()> {
        let mut snap_keys = try!(self.snap_mgr.wl().list_snap());
        if snap_keys.is_empty() {
            return Ok(());
        }
        snap_keys.sort();
        let (mut last_region_id, mut compacted_idx, mut compacted_term) = (0, u64::MAX, u64::MAX);
        let mut is_applying_snap = false;
        for (key, is_sending) in snap_keys {
            if self.snap_mgr.rl().has_registered(&key) {
                continue;
            }
            if last_region_id != key.region_id {
                last_region_id = key.region_id;
                match self.region_peers.get(&key.region_id) {
                    None => {
                        // region is deleted
                        compacted_idx = u64::MAX;
                        compacted_term = u64::MAX;
                        is_applying_snap = false;
                    }
                    Some(peer) => {
                        let s = peer.get_store();
                        compacted_idx = s.truncated_index();
                        compacted_term = s.truncated_term();
                        is_applying_snap = s.is_applying();
                    }
                };
            }

            let f = try!(self.snap_mgr.rl().get_snap_file(&key, is_sending));
            if is_sending {
                if key.term < compacted_term || key.idx < compacted_idx {
                    info!("snap file {} has been compacted, delete.", key);
                    f.delete();
                } else if let Ok(meta) = f.meta() {
                    let modified = box_try!(meta.modified());
                    if let Ok(elapsed) = modified.elapsed() {
                        if elapsed > Duration::from_secs(self.cfg.snap_gc_timeout) {
                            info!("snap file {} has been expired, delete.", key);
                            f.delete();
                        }
                    }
                }
            } else if key.term <= compacted_term &&
                      (key.idx < compacted_idx || key.idx == compacted_idx && !is_applying_snap) {
                info!("snap file {} has been applied, delete.", key);
                f.delete();
            }
        }
        Ok(())
    }

    fn on_snap_mgr_gc(&mut self, event_loop: &mut EventLoop<Self>) {
        if let Err(e) = self.handle_snap_mgr_gc() {
            error!("{} failed to gc snap manager: {:?}", self.tag, e);
        }
        self.register_snap_mgr_gc_tick(event_loop);
    }

    fn on_compact_lock_cf(&mut self, event_loop: &mut EventLoop<Self>) {
        // Create a compact lock cf task(compact whole range) and schedule directly.
        let task = CompactTask {
            cf_name: String::from(CF_LOCK),
            start_key: None,
            end_key: None,
        };
        if let Err(e) = self.compact_worker.schedule(task) {
            error!("{} failed to schedule compact lock cf task: {:?}",
                   self.tag,
                   e);
        }

        self.register_compact_lock_cf_tick(event_loop);
    }

    fn register_pd_store_heartbeat_tick(&self, event_loop: &mut EventLoop<Self>) {
        if let Err(e) = register_timer(event_loop,
                                       Tick::PdStoreHeartbeat,
                                       self.cfg.pd_store_heartbeat_tick_interval) {
            error!("{} register pd store heartbeat tick err: {:?}", self.tag, e);
        };
    }

    fn register_snap_mgr_gc_tick(&self, event_loop: &mut EventLoop<Self>) {
        if let Err(e) = register_timer(event_loop,
                                       Tick::SnapGc,
                                       self.cfg.snap_mgr_gc_tick_interval) {
            error!("{} register snap mgr gc tick err: {:?}", self.tag, e);
        }
    }

    fn register_compact_lock_cf_tick(&self, event_loop: &mut EventLoop<Self>) {
        if let Err(e) = register_timer(event_loop,
                                       Tick::CompactLockCf,
                                       self.cfg.lock_cf_compact_interval_secs * 1000) {
            error!("{} register compact cf-lock tick err: {:?}", self.tag, e);
        }
    }

    fn on_report_snapshot(&mut self, region_id: u64, to_peer_id: u64, status: SnapshotStatus) {
        if let Some(mut peer) = self.region_peers.get_mut(&region_id) {
            // The peer must exist in peer_cache.
            let to_peer = match self.peer_cache.borrow().get(&to_peer_id).cloned() {
                Some(peer) => peer,
                None => {
                    // If to_peer is removed immediately after sending snapshot, the command
                    // may be applied before SnapshotStatus is reported. So here just ignore.
                    warn!("[region {}] peer {} not found, skip reporting snap {:?}",
                          region_id,
                          to_peer_id,
                          status);
                    return;
                }
            };
            info!("[region {}] report snapshot status {:?} {:?}",
                  region_id,
                  to_peer,
                  status);
            peer.raft_group.report_snapshot(to_peer_id, status)
        }
    }

    fn on_unreachable(&mut self, region_id: u64, to_peer_id: u64) {
        if let Some(mut peer) = self.region_peers.get_mut(&region_id) {
            peer.raft_group.report_unreachable(to_peer_id);
        }
    }

    fn on_snap_gen_res(&mut self, region_id: u64, snap: Option<Snapshot>) {
        let peer = match self.region_peers.get_mut(&region_id) {
            None => return,
            Some(peer) => peer,
        };
        let mut storage = peer.mut_store();
        if !storage.is_snap_state(SnapState::Generating) {
            // snapshot no need anymore.
            return;
        }
        match snap {
            Some(snap) => {
                storage.set_snap_state(SnapState::Snap(snap));
            }
            None => {
                storage.set_snap_state(SnapState::Failed);
            }
        }
    }
}


fn register_timer<T: Transport, C: PdClient>(event_loop: &mut EventLoop<Store<T, C>>,
                                             tick: Tick,
                                             delay: u64)
                                             -> Result<()> {
    // TODO: now mio TimerError doesn't implement Error trait,
    // so we can't use `try!` directly.
    if delay == 0 {
        // 0 delay means turn off the timer.
        return Ok(());
    }
    box_try!(event_loop.timeout(tick, Duration::from_millis(delay)));
    Ok(())
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
        let t = SlowTimer::new();
        let msg_str = format!("{:?}", msg);
        match msg {
            Msg::RaftMessage(data) => {
                if let Err(e) = self.on_raft_message(data) {
                    error!("{} handle raft message err: {:?}", self.tag, e);
                }
            }
            Msg::RaftCmd { request, callback } => self.propose_raft_command(request, callback),
            Msg::Quit => {
                info!("{} receive quit message", self.tag);
                event_loop.shutdown();
            }
            Msg::SplitCheckResult { region_id, epoch, split_key } => {
                info!("[region {}] split check complete.", region_id);
                self.on_split_check_result(region_id, epoch, split_key);
            }
            Msg::ReportSnapshot { region_id, to_peer_id, status } => {
                self.on_report_snapshot(region_id, to_peer_id, status);
            }
            Msg::ReportUnreachable { region_id, to_peer_id } => {
                self.on_unreachable(region_id, to_peer_id);
            }
            Msg::SnapshotStats => self.store_heartbeat_pd(),
            Msg::SnapGenRes { region_id, snap } => {
                self.on_snap_gen_res(region_id, snap);
            }
        }
        slow_log!(t, "{} handle {}", self.tag, msg_str);
    }

    fn timeout(&mut self, event_loop: &mut EventLoop<Self>, timeout: Tick) {
        let t = SlowTimer::new();
        match timeout {
            Tick::Raft => self.on_raft_base_tick(event_loop),
            Tick::RaftLogGc => self.on_raft_gc_log_tick(event_loop),
            Tick::SplitRegionCheck => self.on_split_region_check_tick(event_loop),
            Tick::CompactCheck => self.on_compact_check_tick(event_loop),
            Tick::PdHeartbeat => self.on_pd_heartbeat_tick(event_loop),
            Tick::PdStoreHeartbeat => self.on_pd_store_heartbeat_tick(event_loop),
            Tick::SnapGc => self.on_snap_mgr_gc(event_loop),
            Tick::CompactLockCf => self.on_compact_lock_cf(event_loop),
        }
        slow_log!(t, "{} handle timeout {:?}", self.tag, timeout);
    }

    fn tick(&mut self, event_loop: &mut EventLoop<Self>) {
        if !event_loop.is_running() {
            for (handle, name) in vec![(self.split_check_worker.stop(),
                                        self.split_check_worker.name()),
                                       (self.region_worker.stop(), self.region_worker.name()),
                                       (self.raftlog_gc_worker.stop(),
                                        self.raftlog_gc_worker.name()),
                                       (self.compact_worker.stop(), self.compact_worker.name()),
                                       (self.pd_worker.stop(), self.pd_worker.name())] {
                if let Some(Err(e)) = handle.map(|h| h.join()) {
                    error!("{} failed to stop {}: {:?}", self.tag, name, e);
                }
            }
            for peer in self.region_peers.values_mut() {
                peer.clear_pending_commands();
            }

            return;
        }

        // We handle raft ready in event loop.
        self.on_raft_ready();
        self.pending_regions.clear();
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
        if let Some(leader) = peer.get_peer_from_cache(peer.leader_id()) {
            resp.mut_region_leader().set_leader(leader);
        }

        Ok(resp)
    }

    fn execute_region_detail(&mut self, request: RaftCmdRequest) -> Result<StatusResponse> {
        let peer = try!(self.mut_target_peer(&request));
        if !peer.get_store().is_initialized() {
            let region_id = request.get_header().get_region_id();
            return Err(Error::RegionNotInitialized(region_id));
        }
        let mut resp = StatusResponse::new();
        resp.mut_region_detail().set_region(peer.region().clone());
        if let Some(leader) = peer.get_peer_from_cache(peer.leader_id()) {
            resp.mut_region_detail().set_leader(leader);
        }

        Ok(resp)
    }
}
