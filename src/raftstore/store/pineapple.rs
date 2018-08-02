// Copyright 2018 PingCAP, Inc.
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

use super::worker::{
    ApplyTask, ApplyTaskRes, CleanupSSTTask, ConsistencyCheckTask, RaftlogGcTask, SplitCheckTask,
};
use futures_cpupool::CpuPool;
use kvproto::import_sstpb::SSTMeta;
use kvproto::metapb::{self, Region};
use kvproto::pdpb::CheckPolicy;
use kvproto::raft_cmdpb::{
    AdminCmdType, AdminRequest, RaftCmdRequest, RaftCmdResponse, StatusCmdType, StatusResponse,
};
use kvproto::raft_serverpb::{
    MergeState, PeerState, RaftMessage, RaftSnapshotData, RaftTruncatedState, RegionLocalState,
};
use pd::{PdClient, PdTask};
use protobuf::{Message, RepeatedField};
use raft::eraftpb::{ConfChangeType, Entry, MessageType};
use raft::{self, SnapshotStatus, NO_LIMIT};
use raftstore::store::actor_store::Store;
use raftstore::store::actor_store::{self, DestroyPeerJob};
use raftstore::store::cmd_resp::{bind_term, new_error};
use raftstore::store::engine::{Peekable, Snapshot as EngineSnapshot};
use raftstore::store::keys::{self, enc_end_key, enc_start_key};
use raftstore::store::local_metrics::RaftMetrics;
use raftstore::store::metrics::*;
use raftstore::store::msg::{Callback, ReadResponse};
use raftstore::store::peer::{ReadyContext, StaleState};
use raftstore::store::peer_storage::ApplySnapResult;
use raftstore::store::router::{self, InternalTransport, RangeState};
use raftstore::store::snap::SnapshotDeleter;
use raftstore::store::worker::apply::{ApplyRes, ChangePeer, ExecResult};
use raftstore::store::Peer;
use raftstore::store::Transport;
use raftstore::store::{util, AllMsg, Msg, SignificantMsg, SnapKey, SnapManager, Tick};
use raftstore::{Error, Result};
use rocksdb::WriteOptions;
use std::cmp;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use storage::CF_RAFT;
use tokio_timer::timer::Handle;
use util::escape;
use util::mpsc::{self, LooseBoundedSender, Receiver};
use util::time::{duration_to_sec, SlowTimer};
use util::timer::GLOBAL_TIMER_HANDLE;
use util::worker::{FutureScheduler, Scheduler, Stopped};

pub struct PeerHolder<R> {
    pub peer: Peer,
    sender: LooseBoundedSender<AllMsg>,
    receiver: Receiver<AllMsg>,
    pool: CpuPool,
    handle: Handle,
    raft_metrics: RaftMetrics,
    apply_scheduler: Scheduler<ApplyTask>,
    split_scheduler: Scheduler<SplitCheckTask>,
    pub pd_scheduler: FutureScheduler<PdTask>,
    consistency_check_scheduler: Scheduler<ConsistencyCheckTask>,
    cleanup_sst_scheduler: Scheduler<CleanupSSTTask>,
    raftlog_gc_scheduler: Scheduler<RaftlogGcTask>,
    range_state: Arc<Mutex<RangeState>>,
    internal_transport: InternalTransport,
    external_transport: R,
    snap_mgr: SnapManager,
    pub has_ready: bool,
    stopped: bool,
    pending_snapshot_region: bool,
}

impl<R: Transport + 'static> PeerHolder<R> {
    pub fn new<T: Transport, C: PdClient>(
        peer: Peer,
        store: &Store<T, C>,
        poller: CpuPool,
        range_state: Arc<Mutex<RangeState>>,
        it: InternalTransport,
        et: R,
    ) -> (LooseBoundedSender<AllMsg>, PeerHolder<R>) {
        let (sender, receiver) = mpsc::loose_bounded(4096);
        (
            sender.clone(),
            PeerHolder {
                peer,
                sender,
                receiver,
                pool: poller,
                apply_scheduler: store.apply_worker.scheduler(),
                split_scheduler: store.split_check_worker.scheduler(),
                consistency_check_scheduler: store.consistency_check_worker.scheduler(),
                raftlog_gc_scheduler: store.raftlog_gc_worker.scheduler(),
                cleanup_sst_scheduler: store.cleanup_sst_worker.scheduler(),
                raft_metrics: RaftMetrics::default(),
                pd_scheduler: store.pd_worker.scheduler(),
                range_state,
                internal_transport: it,
                external_transport: et,
                snap_mgr: store.snap_mgr.clone(),
                handle: GLOBAL_TIMER_HANDLE.clone(),
                has_ready: false,
                stopped: false,
                pending_snapshot_region: false,
            },
        )
    }

    pub fn start(&mut self) {
        self.register_raft_base_tick();
        self.register_pd_heartbeat_tick();
        // TODO: move such checks to role_change.
        self.register_raft_gc_log_tick();
        self.register_check_peer_stale_state_tick();
        self.register_consistency_check_tick();
        self.register_split_region_check_tick();
        self.register_snap_gc_tick();
        self.apply_scheduler
            .schedule(ApplyTask::register(&self.peer))
            .unwrap();
        if self.peer.pending_merge_state.is_some() {
            info!("{} is merging on store {}", self.peer.tag, self.store_id());
            // Schedule it asynchronously to avoid deadlock.
            self.register_tick(Tick::CheckMerge, Duration::from_millis(1));
        }
    }

    #[inline]
    fn store_id(&self) -> u64 {
        self.peer.peer.get_store_id()
    }

    // return false means the message is invalid, and can be ignored.
    fn validate_raft_msg(&mut self, msg: &RaftMessage) -> bool {
        let region_id = msg.get_region_id();
        let from = msg.get_from_peer();
        let to = msg.get_to_peer();

        debug!(
            "[region {}] handle raft message {:?}, from {} to {}",
            region_id,
            msg.get_message().get_msg_type(),
            from.get_id(),
            to.get_id()
        );

        if to.get_store_id() != self.store_id() {
            warn!(
                "[region {}] store not match, to store id {}, mine {}, ignore it",
                region_id,
                to.get_store_id(),
                self.store_id()
            );
            self.raft_metrics.message_dropped.mismatch_store_id += 1;
            return false;
        }

        if !msg.has_region_epoch() {
            error!(
                "[region {}] missing epoch in raft message, ignore it",
                region_id
            );
            self.raft_metrics.message_dropped.mismatch_region_epoch += 1;
            return false;
        }

        true
    }

    fn handle_destroy_peer(&mut self, job: DestroyPeerJob) -> bool {
        if job.initialized {
            self.apply_scheduler
                .schedule(ApplyTask::destroy(job.region_id))
                .unwrap();
        }
        if job.async_remove {
            info!(
                "[region {}] {} is destroyed asychroniously",
                job.region_id,
                job.peer.get_id()
            );
            false
        } else {
            self.destroy_peer(job.peer, false);
            true
        }
    }

    pub fn destroy_peer(&mut self, peer: metapb::Peer, keep_data: bool) {
        // Can we destroy it in another thread later?

        // Suppose cluster removes peer a from store and then add a new
        // peer b to the same store again, if peer a is applying snapshot,
        // then it will be considered stale and removed immediately, and the
        // apply meta will be removed asynchronously. So the `destroy_peer` will
        // be called again when `poll_apply`. We need to check if the peer exists
        // and is the very target.
        assert_eq!(peer.get_id(), self.peer.peer_id());
        info!("{} destroy peer {:?}", self.peer.tag, peer);
        let region_id = self.peer.region().get_id();
        // We can't destroy a peer which is applying snapshot.
        assert!(!self.peer.is_applying_snapshot());
        let mut range_state = self.range_state.lock().unwrap();
        range_state.pending_cross_snap.remove(&region_id);
        let task = PdTask::DestroyPeer { region_id };
        if let Err(e) = self.pd_scheduler.schedule(task) {
            error!("{} failed to notify pd: {}", self.peer.tag, e);
        }
        let is_initialized = self.peer.is_initialized();
        if let Err(e) = self.peer.destroy(keep_data) {
            // If not panic here, the peer will be recreated in the next restart,
            // then it will be gc again. But if some overlap region is created
            // before restarting, the gc action will delete the overlap region's
            // data too.
            panic!(
                "[region {}] destroy peer {:?} in store {} err {:?}",
                region_id,
                peer,
                self.store_id(),
                e
            );
        }

        if is_initialized && !keep_data {
            debug!("{} destroy region {:?}", self.peer.tag, self.peer.region());
            let meta_exists = range_state
                .region_ranges
                .remove(&enc_end_key(self.peer.region()))
                .is_some();
            range_state.region_peers.remove(&region_id);
            if !meta_exists {
                panic!(
                    "{} remove peer {:?} in store {}",
                    self.peer.tag,
                    peer,
                    self.store_id()
                );
            }
        }

        self.internal_transport
            .mailboxes()
            .lock()
            .unwrap()
            .remove(&region_id);
        self.stopped = true;
    }

    fn handle_gc_peer_msg(&mut self, msg: &RaftMessage) {
        let from_epoch = msg.get_region_epoch();
        if !util::is_epoch_stale(self.peer.region().get_region_epoch(), from_epoch) {
            return;
        }

        if self.peer.peer != *msg.get_to_peer() {
            info!("{} receive stale gc message, ignore.", self.peer.tag);
            self.raft_metrics.message_dropped.stale_msg += 1;
            return;
        }
        // TODO: ask pd to guarantee we are stale now.
        info!("{} receives gc message, trying to remove", self.peer.tag);
        match self.peer.maybe_destroy() {
            Some(job) => {
                self.handle_destroy_peer(job);
            }
            None => self.raft_metrics.message_dropped.applying_snap += 1,
        }
    }

    /// Check if it's necessary to gc the source merge peer.
    ///
    /// If the target merge peer won't be created on this store,
    /// then it's appropriate to destroy it immediately.
    fn need_gc_merge(&mut self, msg: &RaftMessage) -> Result<bool> {
        let merge_target = msg.get_merge_target();
        let target_region_id = merge_target.get_id();

        let range_state = self.range_state.lock().unwrap();
        if let Some(epoch) = range_state
            .pending_cross_snap
            .get(&target_region_id)
            .or_else(|| {
                range_state
                    .region_peers
                    .get(&target_region_id)
                    .map(|r| r.get_region_epoch())
            }) {
            info!(
                "[region {}] checking target {} epoch: {:?}",
                msg.get_region_id(),
                target_region_id,
                epoch
            );
            // So the target peer has moved on, we should let it go.
            if epoch.get_version() > merge_target.get_region_epoch().get_version() {
                return Ok(true);
            }
            // Wait till it catching up logs.
            return Ok(false);
        }

        let state_key = keys::region_state_key(target_region_id);
        if let Some(state) = self
            .peer
            .engines()
            .kv
            .get_msg_cf::<RegionLocalState>(CF_RAFT, &state_key)?
        {
            debug!(
                "[region {}] check local state {:?}",
                target_region_id, state
            );
            if state.get_state() == PeerState::Tombstone
                && state.get_region().get_region_epoch().get_conf_ver()
                    >= merge_target.get_region_epoch().get_conf_ver()
            {
                // Replica was destroyed.
                return Ok(true);
            }
        }

        info!(
            "[region {}] no replica of region {} exist, check pd.",
            msg.get_region_id(),
            target_region_id
        );
        // We can't know whether the peer is destroyed or not for sure locally, ask
        // pd for help.
        let merge_source = match range_state.region_peers.get(&msg.get_region_id()) {
            // It has been gc.
            None => return Ok(false),
            Some(p) => p,
        };
        let target_peer = merge_target
            .get_peers()
            .iter()
            .find(|p| p.get_store_id() == self.store_id())
            .unwrap();
        let task = PdTask::ValidatePeer {
            peer: target_peer.to_owned(),
            region: merge_target.to_owned(),
            merge_source: Some(merge_source.get_id()),
        };
        if let Err(e) = self.pd_scheduler.schedule(task) {
            error!(
                "[region {}] failed to validate target peer {:?}: {}",
                msg.get_region_id(),
                target_peer,
                e
            );
        }
        Ok(false)
    }

    fn on_merge_result(&mut self, peer: metapb::Peer, successful: bool) {
        if successful {
            info!("{} merge successfully, stop self.", self.peer.tag);
            self.destroy_peer(peer, true);
        } else {
            info!("{} merge fail, try gc stale peer.", self.peer.tag);
            if let Some(job) = self.peer.maybe_destroy() {
                self.handle_destroy_peer(job);
            }
        }
    }

    fn check_msg(&mut self, msg: &RaftMessage) -> Result<bool> {
        let from_epoch = msg.get_region_epoch();
        let msg_type = msg.get_message().get_msg_type();
        let is_vote_msg = msg_type == MessageType::MsgRequestVote;
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
        let trans = &self.external_transport;
        let region = self.peer.region();
        let epoch = region.get_region_epoch();

        if util::is_epoch_stale(from_epoch, epoch)
            && util::find_peer(region, from_store_id).is_none()
        {
            // The message is stale and not in current region.
            router::handle_stale_msg(trans, msg, epoch, is_vote_msg, None);
            return Ok(true);
        }

        Ok(false)
    }

    fn check_snapshot(&mut self, msg: &RaftMessage) -> Result<Option<SnapKey>> {
        if !msg.get_message().has_snapshot() {
            return Ok(None);
        }

        let region_id = msg.get_region_id();
        let snap = msg.get_message().get_snapshot();
        let key = SnapKey::from_region_snap(region_id, snap);
        let mut snap_data = RaftSnapshotData::new();
        snap_data.merge_from_bytes(snap.get_data())?;
        let snap_region = snap_data.take_region();
        let peer_id = msg.get_to_peer().get_id();

        if snap_region
            .get_peers()
            .iter()
            .all(|p| p.get_id() != peer_id)
        {
            info!(
                "[region {}] {:?} doesn't contain peer {:?}, skip.",
                snap_region.get_id(),
                snap_region,
                msg.get_to_peer()
            );
            self.raft_metrics.message_dropped.region_no_peer += 1;
            return Ok(Some(key));
        }

        let mut range_state = self.range_state.lock().unwrap();
        let r = range_state
            .region_ranges
            .range((Excluded(enc_start_key(&snap_region)), Unbounded::<Vec<u8>>))
            .map(|(_, &region_id)| &range_state.region_peers[&region_id])
            .take_while(|r| enc_start_key(r) < enc_end_key(&snap_region))
            .skip_while(|r| r.get_id() == region_id)
            .next()
            .map(|r| r.to_owned());
        if let Some(exist_region) = r {
            info!("region overlapped {:?}, {:?}", exist_region, snap_region);
            range_state
                .pending_cross_snap
                .insert(region_id, snap_region.get_region_epoch().to_owned());
            self.raft_metrics.message_dropped.region_overlap += 1;
            return Ok(Some(key));
        }
        for region in &range_state.pending_snapshot_regions {
            if enc_start_key(region) < enc_end_key(&snap_region) &&
               enc_end_key(region) > enc_start_key(&snap_region) &&
               // Same region can overlap, we will apply the latest version of snapshot.
               region.get_id() != snap_region.get_id()
            {
                info!("pending region overlapped {:?}, {:?}", region, snap_region);
                self.raft_metrics.message_dropped.region_overlap += 1;
                return Ok(Some(key));
            }
        }
        if let Some(r) = range_state.pending_cross_snap.get(&region_id) {
            // Check it to avoid epoch moves backward.
            if util::is_epoch_stale(snap_region.get_region_epoch(), r) {
                info!(
                    "[region {}] snapshot epoch is stale, drop: {:?} < {:?}",
                    snap_region.get_id(),
                    snap_region.get_region_epoch(),
                    r
                );
                self.raft_metrics.message_dropped.stale_msg += 1;
                return Ok(Some(key));
            }
        }
        // check if snapshot file exists.
        self.snap_mgr.get_snapshot_for_applying(&key)?;

        range_state.pending_snapshot_regions.push(snap_region);
        self.pending_snapshot_region = true;
        range_state.pending_cross_snap.remove(&region_id);

        Ok(None)
    }

    fn verify_peer(&mut self, msg: &RaftMessage) -> Result<bool> {
        let target = msg.get_to_peer();
        let target_peer_id = target.get_id();
        if self.peer.peer_id() < target_peer_id {
            match self.peer.maybe_destroy() {
                None => self.raft_metrics.message_dropped.applying_snap += 1,
                Some(job) => {
                    info!("{} try to destroy stale peer {:?}", self.peer.tag, job.peer);
                    self.handle_destroy_peer(job);
                }
            };
            Ok(false)
        } else if self.peer.peer_id() > target_peer_id {
            info!(
                "{} target peer id {} is smaller, msg maybe stale.",
                self.peer.tag, target_peer_id,
            );
            self.raft_metrics.message_dropped.stale_msg += 1;
            Ok(false)
        } else {
            Ok(true)
        }
    }

    fn on_raft_message(&mut self, mut msg: RaftMessage) -> Result<()> {
        if !self.validate_raft_msg(&msg) {
            return Ok(());
        }
        if msg.get_is_tombstone() {
            self.handle_gc_peer_msg(&msg);
            return Ok(());
        }
        if msg.has_merge_target() {
            if self.need_gc_merge(&msg)? {
                let target_peer = util::find_peer(msg.get_merge_target(), self.store_id())
                    .unwrap()
                    .clone();
                self.on_merge_result(target_peer, false);
            }
            return Ok(());
        }
        if self.check_msg(&msg)? {
            return Ok(());
        }

        if !self.verify_peer(&msg)? {
            return Ok(());
        }

        if let Some(key) = self.check_snapshot(&msg)? {
            // If the snapshot file is not used again, then it's OK to
            // delete them here. If the snapshot file will be reused when
            // receiving, then it will fail to pass the check again, so
            // missing snapshot files should not be noticed.
            let s = self.snap_mgr.get_snapshot_for_applying(&key)?;
            self.snap_mgr.delete_snapshot(&key, s.as_ref(), false);
            return Ok(());
        }

        let from_peer_id = msg.get_from_peer().get_id();
        self.peer.insert_peer_cache(msg.take_from_peer());
        self.peer.step(msg.take_message())?;

        if self.peer.any_new_peer_catch_up(from_peer_id) {
            self.peer.heartbeat_pd(&self.pd_scheduler);
        }

        self.has_ready = true;

        Ok(())
    }

    fn execute_region_leader(&mut self) -> Result<StatusResponse> {
        let mut resp = StatusResponse::new();
        if let Some(leader) = self.peer.get_peer_from_cache(self.peer.leader_id()) {
            resp.mut_region_leader().set_leader(leader);
        }

        Ok(resp)
    }

    fn execute_region_detail(&mut self, request: &RaftCmdRequest) -> Result<StatusResponse> {
        if !self.peer.get_store().is_initialized() {
            let region_id = request.get_header().get_region_id();
            return Err(Error::RegionNotInitialized(region_id));
        }
        let mut resp = StatusResponse::new();
        resp.mut_region_detail()
            .set_region(self.peer.region().clone());
        if let Some(leader) = self.peer.get_peer_from_cache(self.peer.leader_id()) {
            resp.mut_region_detail().set_leader(leader);
        }

        Ok(resp)
    }

    // Handle status commands here, separate the logic, maybe we can move it
    // to another file later.
    // Unlike other commands (write or admin), status commands only show current
    // store status, so no need to handle it in raft group.
    fn execute_status_command(&mut self, request: &RaftCmdRequest) -> Result<RaftCmdResponse> {
        let cmd_type = request.get_status_request().get_cmd_type();

        let mut response = match cmd_type {
            StatusCmdType::RegionLeader => self.execute_region_leader(),
            StatusCmdType::RegionDetail => self.execute_region_detail(request),
            StatusCmdType::InvalidStatus => Err(box_err!("invalid status command!")),
        }?;
        response.set_cmd_type(cmd_type);

        let mut resp = RaftCmdResponse::new();
        resp.set_status_response(response);
        // Bind peer current term here.
        bind_term(&mut resp, self.peer.term());
        Ok(resp)
    }

    pub fn find_sibling_region(&self, region: &metapb::Region) -> Option<Region> {
        let start = if self.peer.cfg.right_derive_when_split {
            Included(enc_start_key(region))
        } else {
            Excluded(enc_end_key(region))
        };
        let range_state = self.range_state.lock().unwrap();
        range_state
            .region_ranges
            .range((start, Unbounded::<Vec<u8>>))
            .next()
            .map(|(_, region_id)| range_state.region_peers[region_id].clone())
    }

    /// Check if a request is valid if it has valid prepare_merge/commit_merge proposal.
    fn check_merge_proposal(&self, msg: &mut RaftCmdRequest) -> Result<()> {
        if !msg.get_admin_request().has_prepare_merge()
            && !msg.get_admin_request().has_commit_merge()
        {
            return Ok(());
        }

        let region = self.peer.region();

        if msg.get_admin_request().has_prepare_merge() {
            let target_region = msg.get_admin_request().get_prepare_merge().get_target();
            {
                let range_state = self.range_state.lock().unwrap();
                let region = match range_state.region_peers.get(&target_region.get_id()) {
                    None => return Err(box_err!("target region doesn't exist.")),
                    Some(r) => r,
                };
                if region != target_region {
                    return Err(box_err!(
                        "target region not matched, skip proposing: {:?} != {:?}",
                        target_region,
                        region
                    ));
                }
            }
            if !util::is_sibling_regions(target_region, region) {
                return Err(box_err!("regions are not sibling, skip proposing."));
            }
            if !util::region_on_same_stores(target_region, region) {
                return Err(box_err!(
                    "peers doesn't match {:?} != {:?}, reject merge",
                    region.get_peers(),
                    target_region.get_peers()
                ));
            }
        } else {
            let source_region = msg.get_admin_request().get_commit_merge().get_source();
            {
                let range_state = self.range_state.lock().unwrap();
                let region = &range_state.region_peers[&source_region.get_id()];
                assert_eq!(source_region, region, "{}", self.peer.tag);
            }
            assert!(
                util::is_sibling_regions(source_region, region),
                "{:?} {:?} should be sibling",
                source_region,
                region
            );
            assert!(
                util::region_on_same_stores(source_region, region),
                "peers not matched: {:?} {:?}",
                source_region,
                region
            );
        };

        Ok(())
    }

    fn pre_propose_raft_command(
        &mut self,
        msg: &RaftCmdRequest,
    ) -> Result<Option<RaftCmdResponse>> {
        // Check store_id, make sure that the msg is dispatched to the right place.
        util::check_store_id(msg, self.store_id())?;
        if msg.has_status_request() {
            // For status commands, we handle it here directly.
            let resp = self.execute_status_command(msg)?;
            return Ok(Some(resp));
        }

        // Check whether the store has the right peer to handle the request.
        if !self.peer.is_leader() {
            return Err(Error::NotLeader(
                self.peer.region().get_id(),
                self.peer.get_peer_from_cache(self.peer.leader_id()),
            ));
        }
        // peer_id must be the same as peer's.
        util::check_peer_id(msg, self.peer.peer_id())?;
        // Check whether the term is stale.
        util::check_term(msg, self.peer.term())?;

        match util::check_region_epoch(msg, self.peer.region(), true) {
            Err(Error::StaleEpoch(msg, mut new_regions)) => {
                // Attach the region which might be split from the current region. But it doesn't
                // matter if the region is not split from the current region. If the region meta
                // received by the TiKV driver is newer than the meta cached in the driver, the meta is
                // updated.
                if let Some(sibling_region) = self.find_sibling_region(self.peer.region()) {
                    new_regions.push(sibling_region.to_owned());
                }
                Err(Error::StaleEpoch(msg, new_regions))
            }
            Err(e) => Err(e),
            Ok(()) => Ok(None),
        }
    }

    fn propose_raft_command(&mut self, mut msg: RaftCmdRequest, cb: Callback) {
        match self.pre_propose_raft_command(&msg) {
            Ok(Some(resp)) => {
                cb.invoke_with_response(resp);
                return;
            }
            Err(e) => {
                debug!("{} failed to propose {:?}: {:?}", self.peer.tag, msg, e);
                cb.invoke_with_response(new_error(e));
                return;
            }
            _ => (),
        }

        if let Err(e) = self.check_merge_proposal(&mut msg) {
            warn!(
                "{} failed to propose merge: {:?}: {}",
                self.peer.tag, msg, e
            );
            cb.invoke_with_response(new_error(e));
            return;
        }

        // Note:
        // The peer that is being checked is a leader. It might step down to be a follower later. It
        // doesn't matter whether the peer is a leader or not. If it's not a leader, the proposing
        // command log entry can't be committed.

        let mut resp = RaftCmdResponse::new();
        let term = self.peer.term();
        bind_term(&mut resp, term);
        if self
            .peer
            .propose(cb, msg, resp, &mut self.raft_metrics.propose)
        {
            self.has_ready = true;
        }

        // TODO: add timeout, if the command is not applied after timeout,
        // we will call the callback with timeout error.
    }

    fn propose_batch_raft_snapshot_command(
        &mut self,
        batch: Vec<RaftCmdRequest>,
        on_finished: Callback,
    ) {
        let size = batch.len();
        BATCH_SNAPSHOT_COMMANDS.observe(size as f64);
        let mut ret = Vec::with_capacity(size);
        for msg in batch {
            match self.pre_propose_raft_command(&msg) {
                Ok(Some(resp)) => {
                    ret.push(Some(ReadResponse {
                        response: resp,
                        snapshot: None,
                    }));
                    continue;
                }
                Err(e) => {
                    ret.push(Some(ReadResponse {
                        response: new_error(e),
                        snapshot: None,
                    }));
                    continue;
                }
                _ => (),
            }

            ret.push(
                self.peer
                    .propose_snapshot(msg, &mut self.raft_metrics.propose),
            );
        }
        on_finished.invoke_batch_read(ret)
    }

    fn on_hash_computed(&mut self, index: u64, hash: Vec<u8>) {
        let region_id = self.peer.region().get_id();
        let msg = {
            let (state, peer) = (&mut self.peer.consistency_state, &self.peer.peer);
            if !actor_store::verify_and_store_hash(region_id, state, index, hash) {
                return;
            }

            actor_store::new_verify_hash_request(region_id, peer.to_owned(), state)
        };
        self.propose_raft_command(msg, Callback::None);
    }

    fn validate_split_region(
        &mut self,
        epoch: &metapb::RegionEpoch,
        split_key: &[u8], // `split_key` is a encoded key.
    ) -> Result<()> {
        if split_key.is_empty() {
            error!("{} split key should not be empty!!!", self.peer.tag);
            return Err(box_err!("{} split key should not be empty", self.peer.tag));
        }

        if !self.peer.is_leader() {
            // region on this store is no longer leader, skipped.
            info!("{} is not leader, skip.", self.peer.tag,);
            return Err(Error::NotLeader(
                self.peer.region().get_id(),
                self.peer.get_peer_from_cache(self.peer.leader_id()),
            ));
        }

        let region = self.peer.region();
        let latest_epoch = region.get_region_epoch();

        if latest_epoch.get_version() != epoch.get_version() {
            info!(
                "{} epoch changed {:?} != {:?}, retry later",
                self.peer.tag,
                region.get_region_epoch(),
                epoch
            );
            return Err(Error::StaleEpoch(
                format!(
                    "{} epoch changed {:?} != {:?}, retry later",
                    self.peer.tag, latest_epoch, epoch
                ),
                vec![region.to_owned()],
            ));
        }
        Ok(())
    }

    fn on_prepare_split_region(
        &mut self,
        region_epoch: metapb::RegionEpoch,
        split_key: Vec<u8>, // `split_key` is a encoded key.
        cb: Callback,
    ) {
        if let Err(e) = self.validate_split_region(&region_epoch, &split_key) {
            cb.invoke_with_response(new_error(e));
            return;
        }
        let region = self.peer.region();
        let task = PdTask::AskSplit {
            region: region.clone(),
            split_key,
            peer: self.peer.peer.clone(),
            right_derive: self.peer.cfg.right_derive_when_split,
            callback: cb,
        };
        if let Err(Stopped(t)) = self.pd_scheduler.schedule(task) {
            error!("{} failed to notify pd to split: Stopped", self.peer.tag);
            match t {
                PdTask::AskSplit { callback, .. } => {
                    callback.invoke_with_response(new_error(box_err!("failed to split: Stopped")));
                }
                _ => unreachable!(),
            }
        }
    }

    fn on_schedule_half_split_region(
        &mut self,
        policy: CheckPolicy,
        region_epoch: &metapb::RegionEpoch,
    ) {
        if !self.peer.is_leader() {
            // region on this store is no longer leader, skipped.
            warn!(
                "[region {}] region on {} is not leader, skip.",
                self.peer.region().get_id(),
                self.store_id()
            );
            return;
        }

        let region = self.peer.region();
        if util::is_epoch_stale(region_epoch, region.get_region_epoch()) {
            warn!("{} receive a stale halfsplit message", self.peer.tag);
            return;
        }

        let task = SplitCheckTask::new(region.clone(), false, policy);
        if let Err(e) = self.split_scheduler.schedule(task) {
            error!("{} failed to schedule split check: {}", self.peer.tag, e);
        }
    }

    fn propose_merge(&mut self, target: Region, source: Region, commit: u64, entries: Vec<Entry>) {
        if !self.peer.is_leader() {
            return;
        }
        if target != *self.peer.region() {
            // Probably a stale proposal. Rollback will be scheduled when it
            // checks next time, no need to send back a rollback.
            info!(
                "{} merge proposal is stale, skip: {:?} != {:?}",
                self.peer.tag,
                target,
                self.peer.region()
            );
            return;
        }
        let mut request = actor_store::new_admin_request(target.get_id(), self.peer.peer.clone());
        request
            .mut_header()
            .set_region_epoch(target.get_region_epoch().clone());
        let mut admin = AdminRequest::new();
        admin.set_cmd_type(AdminCmdType::CommitMerge);
        admin.mut_commit_merge().set_source(source);
        admin.mut_commit_merge().set_commit(commit);
        admin
            .mut_commit_merge()
            .set_entries(RepeatedField::from_vec(entries));
        request.set_admin_request(admin);
        self.propose_raft_command(request, Callback::None);
    }

    fn on_compacted_declined_bytes(&mut self, bytes: u64) {
        self.peer.compaction_declined_bytes += bytes;
        if self.peer.compaction_declined_bytes >= self.peer.cfg.region_split_check_diff.0 {
            UPDATE_REGION_SIZE_BY_COMPACTION_COUNTER.inc();
        }
    }

    fn handle_msg(&mut self, msg: Msg) {
        match msg {
            Msg::RaftMessage(data) => if let Err(e) = self.on_raft_message(data) {
                error!("{} handle raft message err: {:?}", self.peer.tag, e);
            },
            Msg::RaftCmd {
                send_time,
                request,
                callback,
            } => {
                self.raft_metrics
                    .propose
                    .request_wait_time
                    .observe(duration_to_sec(send_time.elapsed()) as f64);
                self.propose_raft_command(request, callback)
            }

            // For now, it is only called by batch snapshot.
            Msg::BatchRaftSnapCmds {
                send_time,
                batch,
                on_finished,
            } => {
                self.raft_metrics
                    .propose
                    .request_wait_time
                    .observe(duration_to_sec(send_time.elapsed()) as f64);
                self.propose_batch_raft_snapshot_command(batch, on_finished);
            }
            Msg::Quit => self.stopped = true,
            Msg::ComputeHashResult { index, hash, .. } => {
                self.on_hash_computed(index, hash);
            }
            Msg::SplitRegion {
                region_epoch,
                split_key,
                callback,
                ..
            } => {
                info!(
                    "{} on split region at key {}.",
                    self.peer.tag,
                    escape(&split_key)
                );
                self.on_prepare_split_region(region_epoch, split_key, callback);
            }
            Msg::RegionApproximateSize { size, .. } => self.peer.approximate_size = Some(size),
            Msg::RegionApproximateKeys { keys, .. } => self.peer.approximate_keys = Some(keys),
            Msg::HalfSplitRegion {
                policy,
                region_epoch,
                ..
            } => self.on_schedule_half_split_region(policy, &region_epoch),
            Msg::ProposeMerge {
                target_region,
                source_region,
                commit,
                entries,
            } => self.propose_merge(target_region, source_region, commit, entries),
            Msg::MergeResult {
                peer, successful, ..
            } => self.on_merge_result(peer, successful),
            Msg::CompactedDeclinedBytes(bytes) => self.on_compacted_declined_bytes(bytes),
            Msg::InitSplit { .. }
            | Msg::CompactedEvent(_)
            | Msg::SnapshotStats
            | Msg::ValidateSSTResult { .. } => unreachable!(),
        }
    }

    fn report_snapshot_status(&mut self, to_peer_id: u64, status: SnapshotStatus) {
        let to_peer = match self.peer.get_peer_from_cache(to_peer_id) {
            Some(peer) => peer,
            None => {
                // If to_peer is gone, ignore this snapshot status
                warn!(
                    "{} peer {} not found, ignore snapshot status {:?}",
                    self.peer.tag, to_peer_id, status
                );
                return;
            }
        };
        info!(
            "{} report snapshot status {:?} {:?}",
            self.peer.tag, to_peer, status
        );
        self.peer.raft_group.report_snapshot(to_peer_id, status)
    }

    fn handle_significant_msg(&mut self, msg: SignificantMsg) {
        match msg {
            SignificantMsg::SnapshotStatus {
                to_peer_id, status, ..
            } => self.report_snapshot_status(to_peer_id, status),
            SignificantMsg::Unreachable { to_peer_id, .. } => {
                self.peer.raft_group.report_unreachable(to_peer_id)
            }
        }
    }

    #[inline]
    fn register_raft_base_tick(&self) {
        self.register_tick(Tick::Raft, self.peer.cfg.raft_base_tick_interval.0);
    }

    fn on_tick_raft_base(&mut self) {
        if !self.peer.pending_remove {
            self.has_ready = self.peer.is_applying_snapshot()
                || self.peer.has_pending_snapshot()
                || self.peer.raft_group.tick();
        }
        self.register_raft_base_tick();
    }

    fn register_tick(&self, tick: Tick, delay: Duration) {
        let mut sender = self.sender.clone();
        if delay.as_secs() == 0 && delay.subsec_nanos() == 0 {
            return;
        }
        self.pool
            .spawn(self.handle.delay(Instant::now() + delay).map(move |_| {
                let _ = sender.force_send(AllMsg::Tick(tick));
            }))
            .forget()
    }

    fn register_raft_gc_log_tick(&self) {
        self.register_tick(Tick::RaftLogGc, self.peer.cfg.raft_log_gc_tick_interval.0);
    }

    #[cfg_attr(feature = "cargo-clippy", allow(if_same_then_else))]
    fn on_tick_raft_gc_log(&mut self) {
        let applied_idx = self.peer.get_store().applied_index();
        if !self.peer.is_leader() {
            self.peer.mut_store().compact_to(applied_idx + 1);
            self.register_raft_gc_log_tick();
            return;
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
        // `healthy_replicated_index` is the smallest `replicated_index` of healthy nodes.
        let truncated_idx = self.peer.get_store().truncated_index();
        let last_idx = self.peer.get_store().last_index();
        let (mut replicated_idx, mut healthy_replicated_idx) = (last_idx, last_idx);
        for (_, p) in self.peer.raft_group.raft.prs().iter() {
            if replicated_idx > p.matched {
                replicated_idx = p.matched;
            }
            if healthy_replicated_idx > p.matched && p.matched >= truncated_idx {
                healthy_replicated_idx = p.matched;
            }
        }
        // When an election happened or a new peer is added, replicated_idx can be 0.
        if replicated_idx > 0 {
            assert!(
                last_idx >= replicated_idx,
                "expect last index {} >= replicated index {}",
                last_idx,
                replicated_idx
            );
            REGION_MAX_LOG_LAG.observe((last_idx - replicated_idx) as f64);
        }
        self.peer
            .mut_store()
            .maybe_gc_cache(healthy_replicated_idx, applied_idx);
        let first_idx = self.peer.get_store().first_index();
        let mut compact_idx;
        if applied_idx > first_idx
            && applied_idx - first_idx >= self.peer.cfg.raft_log_gc_count_limit
        {
            compact_idx = applied_idx;
        } else if self.peer.raft_log_size_hint >= self.peer.cfg.raft_log_gc_size_limit.0 {
            compact_idx = applied_idx;
        } else if replicated_idx < first_idx
            || replicated_idx - first_idx <= self.peer.cfg.raft_log_gc_threshold
        {
            self.register_raft_gc_log_tick();
            return;
        } else {
            compact_idx = replicated_idx;
        }

        // Have no idea why subtract 1 here, but original code did this by magic.
        assert!(compact_idx > 0);
        compact_idx -= 1;
        if compact_idx < first_idx {
            // In case compact_idx == first_idx before subtraction.
            self.register_raft_gc_log_tick();
            return;
        }

        let term = self
            .peer
            .raft_group
            .raft
            .raft_log
            .term(compact_idx)
            .unwrap();

        // Create a compact log request and notify directly.
        let request = actor_store::new_compact_log_request(
            self.peer.region().get_id(),
            self.peer.peer.clone(),
            compact_idx,
            term,
        );
        self.propose_raft_command(request, Callback::None);

        PEER_GC_RAFT_LOG_COUNTER.inc_by((compact_idx - first_idx) as i64);
        self.register_raft_gc_log_tick();
    }

    fn register_split_region_check_tick(&self) {
        self.register_tick(
            Tick::SplitRegionCheck,
            self.peer.cfg.split_region_check_tick_interval.0,
        );
    }

    fn on_tick_split_region_check(&mut self) {
        // To avoid frequent scan, we only add new scan tasks if all previous tasks
        // have finished.
        // TODO: check whether a gc progress has been started.
        if !self.peer.is_leader() {
            self.register_split_region_check_tick();
            return;
        }
        if self.split_scheduler.is_busy() {
            self.register_split_region_check_tick();
            return;
        }
        // When restart, the approximate size will be None. The
        // split check will first check the region size, and then
        // check whether the region should split.  This should
        // work even if we change the region max size.
        // If peer says should update approximate size, update region
        // size and check whether the region should split.
        if self.peer.approximate_size.is_some()
            && self.peer.compaction_declined_bytes < self.peer.cfg.region_split_check_diff.0
            && self.peer.size_diff_hint < self.peer.cfg.region_split_check_diff.0
        {
            self.register_split_region_check_tick();
            return;
        }
        let task = SplitCheckTask::new(self.peer.region().clone(), true, CheckPolicy::SCAN);
        if let Err(e) = self.split_scheduler.schedule(task) {
            error!("{} failed to schedule split check: {}", self.peer.tag, e);
        }
        self.peer.size_diff_hint = 0;
        self.peer.compaction_declined_bytes = 0;

        self.register_split_region_check_tick();
    }

    fn register_pd_heartbeat_tick(&self) {
        self.register_tick(
            Tick::PdHeartbeat,
            self.peer.cfg.pd_heartbeat_tick_interval.0,
        );
    }

    fn on_tick_pd_heartbeat(&mut self) {
        self.peer.check_peers();
        if self.peer.is_leader() {
            self.peer.heartbeat_pd(&self.pd_scheduler);
        }
        // TODO: fix metrics.
        self.register_pd_heartbeat_tick();
    }

    fn register_consistency_check_tick(&self) {
        self.register_tick(
            Tick::ConsistencyCheck,
            self.peer.cfg.consistency_check_interval.0,
        )
    }

    fn on_tick_consistency_check(&mut self) {
        // TODO: should not rely on worker busy
        if self.consistency_check_scheduler.is_busy() {
            // To avoid frequent scan, schedule new check only when all the
            // scheduled check is done.
            self.register_consistency_check_tick();
            return;
        }
        if !self.peer.is_leader() {
            self.register_consistency_check_tick();
            return;
        }

        info!("{} scheduling consistent check", self.peer.tag);
        let msg = actor_store::new_compute_hash_request(
            self.peer.region().get_id(),
            self.peer.peer.clone(),
        );
        self.propose_raft_command(msg, Callback::None);

        self.register_consistency_check_tick();
    }

    fn register_check_peer_stale_state_tick(&self) {
        self.register_tick(
            Tick::CheckPeerStaleState,
            self.peer.cfg.peer_stale_state_check_interval.0,
        )
    }

    fn on_tick_check_peer_stale_state(&mut self) {
        if self.peer.pending_remove {
            return;
        }
        if self.peer.is_applying_snapshot() || self.peer.has_pending_snapshot() {
            self.register_check_peer_stale_state_tick();
            return;
        }

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
        match self.peer.check_stale_state() {
            StaleState::Valid => (),
            StaleState::LeaderMissing => {
                warn!(
                    "{} leader missing longer than abnormal_leader_missing_duration {:?}",
                    self.peer.tag, self.peer.cfg.abnormal_leader_missing_duration.0,
                );
            }
            StaleState::ToValidate => {
                // for peer B in case 1 above
                warn!(
                    "{} leader missing longer than max_leader_missing_duration {:?}. \
                     To check with pd whether it's still valid",
                    self.peer.tag, self.peer.cfg.max_leader_missing_duration.0,
                );
                let task = PdTask::ValidatePeer {
                    peer: self.peer.peer.clone(),
                    region: self.peer.region().clone(),
                    merge_source: None,
                };
                if let Err(e) = self.pd_scheduler.schedule(task) {
                    error!("{} failed to notify pd: {}", self.peer.tag, e)
                }
            }
        }
        // TODO: fix metrics.

        self.register_check_peer_stale_state_tick();
    }

    fn on_tick_check_merge(&mut self) {
        if self.peer.pending_merge_state.is_none() {
            // Maybe rollback or merged.
            return;
        }
        let region = self.peer.region().to_owned();
        if let Err(e) = self.schedule_merge(&region) {
            info!(
                "{} failed to schedule merge, rollback: {:?}",
                self.peer.tag, e
            );
            self.rollback_merge(&region);
        }
        self.register_tick(Tick::CheckMerge, self.peer.cfg.merge_check_tick_interval.0);
    }

    fn handle_snap_mgr_gc(&mut self) -> Result<bool> {
        let region_id = self.peer.region().get_id();
        let snap_keys = self.snap_mgr.list_idle_region_snap(region_id)?;
        if snap_keys.is_empty() {
            return Ok(false);
        }
        let store = self.peer.get_store();
        let compacted_idx = store.truncated_index();
        let compacted_term = store.truncated_term();
        let is_applying_snap = store.is_applying_snapshot();
        for (key, is_sending) in snap_keys {
            if is_sending {
                let s = self.snap_mgr.get_snapshot_for_sending(&key)?;
                if key.term < compacted_term || key.idx < compacted_idx {
                    info!(
                        "{} snap file {} has been compacted, delete.",
                        self.peer.tag, key
                    );
                    self.snap_mgr.delete_snapshot(&key, s.as_ref(), false);
                } else if let Ok(meta) = s.meta() {
                    let modified = box_try!(meta.modified());
                    if let Ok(elapsed) = modified.elapsed() {
                        if elapsed > self.peer.cfg.snap_gc_timeout.0 {
                            info!(
                                "[region {}] snap file {} has been expired, delete.",
                                key.region_id, key
                            );
                            self.snap_mgr.delete_snapshot(&key, s.as_ref(), false);
                        }
                    }
                }
            } else if key.term <= compacted_term
                && (key.idx < compacted_idx || key.idx == compacted_idx && !is_applying_snap)
            {
                info!(
                    "{} snap file {} has been applied, delete.",
                    self.peer.tag, key
                );
                let a = self.snap_mgr.get_snapshot_for_applying(&key)?;
                self.snap_mgr.delete_snapshot(&key, a.as_ref(), false);
            }
        }
        Ok(true)
    }

    fn register_snap_gc_tick(&self) {
        self.register_tick(Tick::SnapGc, self.peer.cfg.snap_mgr_gc_tick_interval.0)
    }

    fn on_tick_snap_mgr_gc(&mut self) {
        if let Err(e) = self.handle_snap_mgr_gc() {
            error!("{} failed to gc snap: {:?}", self.peer.tag, e);
        }
        self.register_snap_gc_tick();
    }

    fn handle_tick(&mut self, tick: Tick) {
        match tick {
            Tick::Raft => self.on_tick_raft_base(),
            Tick::RaftLogGc => self.on_tick_raft_gc_log(),
            Tick::SplitRegionCheck => self.on_tick_split_region_check(),
            Tick::PdHeartbeat => self.on_tick_pd_heartbeat(),
            Tick::ConsistencyCheck => self.on_tick_consistency_check(),
            Tick::CheckMerge => self.on_tick_check_merge(),
            Tick::CheckPeerStaleState => self.on_tick_check_peer_stale_state(),
            Tick::SnapGc => self.on_tick_snap_mgr_gc(),
            _ => unimplemented!(),
        }
    }

    fn set_region(&mut self, region: metapb::Region) {
        let r = region.clone();
        let region_id = self.peer.region().get_id();
        {
            let mut range_state = self.range_state.lock().unwrap();
            debug!(
                "{} before set: ranges: {:?} region {:?}",
                self.peer.tag, range_state.region_ranges, region
            );
            let last_region = range_state.region_peers.insert(region_id, r);
            if let Some(r) = last_region {
                if r.get_end_key() != region.get_end_key() {
                    assert_eq!(
                        range_state.region_ranges.remove(&enc_end_key(&r)),
                        Some(region.get_id())
                    );
                    let res = range_state
                        .region_ranges
                        .insert(enc_end_key(&region), region.get_id());
                    if let Some(r) = res {
                        panic!(
                            "{} unexpected region {} at key {}",
                            self.peer.tag,
                            r,
                            escape(region.get_end_key())
                        )
                    }
                }
            } else {
                range_state.region_peers.remove(&region_id);
            }
        }
        self.peer.set_region(region);
    }

    fn on_ready_change_peer(&mut self, cp: ChangePeer) {
        let change_type = cp.conf_change.get_change_type();
        self.peer.raft_group.apply_conf_change(&cp.conf_change);
        if cp.conf_change.get_node_id() == raft::INVALID_ID {
            // Apply failed, skip.
            return;
        }
        self.set_region(cp.region);
        if self.peer.is_leader() {
            // Notify pd immediately.
            info!(
                "{} notify pd with change peer region {:?}",
                self.peer.tag,
                self.peer.region()
            );
            self.peer.heartbeat_pd(&self.pd_scheduler);
        }

        let peer_id = cp.peer.get_id();
        match change_type {
            ConfChangeType::AddNode | ConfChangeType::AddLearnerNode => {
                let peer = cp.peer.clone();
                if self.peer.peer_id() == peer_id && self.peer.peer.get_is_learner() {
                    self.peer.peer = peer.clone();
                }

                // Add this peer to cache and heartbeats.
                let now = Instant::now();
                self.peer.peer_heartbeats.insert(peer.get_id(), now);
                if self.peer.is_leader() {
                    self.peer
                        .peers_start_pending_time
                        .push((peer.get_id(), now));
                }
                self.peer.insert_peer_cache(peer);
            }
            ConfChangeType::RemoveNode => {
                // Remove this peer from cache.
                self.peer.peer_heartbeats.remove(&peer_id);
                if self.peer.is_leader() {
                    self.peer
                        .peers_start_pending_time
                        .retain(|&(p, _)| p != peer_id);
                }
                self.peer.remove_peer_from_cache(peer_id);
            }
        }

        let peer = cp.peer;

        // We only care remove itself now.
        if change_type == ConfChangeType::RemoveNode && peer.get_store_id() == self.store_id() {
            if self.peer.peer.get_id() == peer.get_id() {
                self.destroy_peer(peer, false)
            } else {
                panic!("{} trying to remove unknown peer {:?}", self.peer.tag, peer);
            }
        }
    }

    fn on_ready_compact_log(&mut self, first_index: u64, state: RaftTruncatedState) {
        let total_cnt = self.peer.last_applying_idx - first_index;
        // the size of current CompactLog command can be ignored.
        let remain_cnt = self.peer.last_applying_idx - state.get_index() - 1;
        self.peer.raft_log_size_hint = self.peer.raft_log_size_hint * remain_cnt / total_cnt;
        let task = RaftlogGcTask {
            raft_engine: Arc::clone(&self.peer.get_store().get_raft_engine()),
            region_id: self.peer.get_store().get_region_id(),
            start_idx: self.peer.last_compacted_idx,
            end_idx: state.get_index() + 1,
        };
        self.peer.last_compacted_idx = task.end_idx;
        self.peer.mut_store().compact_to(task.end_idx);
        if let Err(e) = self.raftlog_gc_scheduler.schedule(task) {
            error!("{} failed to schedule compact task: {}", self.peer.tag, e);
        }
    }

    fn on_ready_split_region(
        &mut self,
        left: metapb::Region,
        right: metapb::Region,
        right_derive: bool,
    ) {
        let (origin_region, new_region) = if right_derive {
            (right.clone(), left.clone())
        } else {
            (left.clone(), right.clone())
        };

        self.set_region(origin_region);
        self.peer.post_split();

        self.internal_transport
            .force_send(
                0,
                Msg::InitSplit {
                    region: new_region,
                    peer_stat: self.peer.peer_stat.clone(),
                    right_derive,
                    is_leader: self.peer.is_leader(),
                },
            )
            .unwrap();

        if self.peer.is_leader() {
            self.peer.heartbeat_pd(&self.pd_scheduler);
            info!("notify pd with split left {:?}, right {:?}", left, right);
            let task = PdTask::ReportSplit { left, right };
            self.pd_scheduler.schedule(task).unwrap();
        }

        if right_derive {
            self.peer.size_diff_hint = self.peer.cfg.region_split_check_diff.0;
        }
    }

    fn check_merge_peer(&self, target_region: &metapb::Region) -> Result<bool> {
        let region_id = target_region.get_id();
        let range_state = self.range_state.lock().unwrap();
        if let Some(exist_target_region) = range_state.region_peers.get(&region_id) {
            let exist_epoch = exist_target_region.get_region_epoch();
            let expect_epoch = target_region.get_region_epoch();
            // exist_epoch > expect_epoch
            if util::is_epoch_stale(expect_epoch, exist_epoch) {
                return Err(box_err!(
                    "target region changed {:?} -> {:?}",
                    target_region,
                    exist_target_region
                ));
            }
            // exist_epoch < expect_epoch
            if util::is_epoch_stale(exist_epoch, expect_epoch) {
                info!(
                    "{} target region still not catch up: {:?} vs {:?}, skip.",
                    self.peer.tag, target_region, exist_target_region
                );
                return Ok(false);
            }
            return Ok(true);
        }

        let state_key = keys::region_state_key(region_id);
        let state: RegionLocalState = match self.peer.engines.kv.get_msg_cf(CF_RAFT, &state_key) {
            Err(e) => {
                error!(
                    "{} failed to load region state of {}, ignore: {}",
                    self.peer.tag, region_id, e
                );
                return Ok(false);
            }
            Ok(None) => {
                info!(
                    "{} seems to merge into a new replica of region {}, let's wait.",
                    self.peer.tag, region_id
                );
                return Ok(false);
            }
            Ok(Some(state)) => state,
        };
        if state.get_state() != PeerState::Tombstone {
            info!("{} wait for region {} split.", self.peer.tag, region_id);
            return Ok(false);
        }

        let tombstone_region = state.get_region();
        if tombstone_region.get_region_epoch().get_conf_ver()
            < target_region.get_region_epoch().get_conf_ver()
        {
            info!(
                "{} seems to merge into a new replica of region {}, let's wait.",
                self.peer.tag, region_id
            );
            return Ok(false);
        }

        Err(box_err!("region {} is destroyed", region_id))
    }

    fn schedule_merge(&mut self, region: &metapb::Region) -> Result<()> {
        fail_point!("on_schedule_merge", |_| Ok(()));
        let state = self.peer.pending_merge_state.as_ref().unwrap();
        let expect_region = state.get_target();
        if !self.check_merge_peer(expect_region)? {
            // Wait till next round.
            return Ok(());
        }
        let min_index = self.peer.get_min_progress() + 1;
        let low = cmp::max(min_index, state.get_min_index());
        let entries = if low > state.get_commit() {
            vec![]
        } else {
            self.peer
                .get_store()
                .entries(low, state.get_commit() + 1, NO_LIMIT)
                .unwrap()
        };
        let merge = Msg::ProposeMerge {
            target_region: expect_region.to_owned(),
            source_region: region.to_owned(),
            commit: state.get_commit(),
            entries,
        };
        // Please note that, here assumes that the unit of network isolation is store rather than
        // peer. So a quorum stores of souce region should also be the quorum stores of target
        // region. Otherwise we need to enable proposal forwarding.
        self.internal_transport
            .force_send(expect_region.get_id(), merge)
            .unwrap();
        Ok(())
    }

    fn rollback_merge(&mut self, region: &metapb::Region) {
        let req = {
            let state = self.peer.pending_merge_state.as_ref().unwrap();
            let mut request =
                actor_store::new_admin_request(region.get_id(), self.peer.peer.clone());
            request
                .mut_header()
                .set_region_epoch(self.peer.region().get_region_epoch().clone());
            let mut admin = AdminRequest::new();
            admin.set_cmd_type(AdminCmdType::RollbackMerge);
            admin.mut_rollback_merge().set_commit(state.get_commit());
            request.set_admin_request(admin);
            request
        };
        self.propose_raft_command(req, Callback::None);
    }

    fn on_ready_prepare_merge(&mut self, region: metapb::Region, state: MergeState, merged: bool) {
        self.peer.pending_merge_state = Some(state);
        self.set_region(region.clone());

        if merged {
            // CommitMerge will try to catch up log for source region. If PrepareMerge is executed
            // in the progress of catching up, there is no need to schedule merge again.
            return;
        }

        self.on_tick_check_merge();
    }

    fn on_ready_commit_merge(&mut self, region: metapb::Region, source: metapb::Region) {
        // If merge backward, then stale meta is clear when source region is destroyed.
        // So only forward needs to be considered.
        {
            let mut range_state = self.range_state.lock().unwrap();
            debug!(
                "{} before committing: ranges: {:?} region {:?} source {:?}",
                self.peer.tag, range_state.region_ranges, region, source
            );
            range_state.region_ranges.remove(&enc_end_key(&source));
            range_state.region_peers.remove(&source.get_id());
        }
        self.set_region(region);
        let source_peer = util::find_peer(&source, self.store_id()).unwrap().clone();
        let source_region_id = source.get_id();
        let _ = self.internal_transport.force_send(
            source.get_id(),
            Msg::MergeResult {
                region_id: source_region_id,
                peer: source_peer,
                successful: true,
            },
        );
        if self.peer.is_leader() {
            info!(
                "notify pd with merge {:?} into {:?}",
                source,
                self.peer.region()
            );
            self.peer.heartbeat_pd(&self.pd_scheduler);
        }
    }

    /// Handle rollbacking Merge result.
    ///
    /// If commit is 0, it means that Merge is rollbacked by a snapshot; otherwise
    /// it's rollbacked by a proposal, and its value should be equal to the commit
    /// index of previous PrepareMerge.
    fn on_ready_rollback_merge(&mut self, commit: u64, region: Option<metapb::Region>) {
        let pending_commit = self.peer.pending_merge_state.as_ref().unwrap().get_commit();
        if commit != 0 && pending_commit != commit {
            panic!(
                "{} rollbacks a wrong merge: {} != {}",
                self.peer.tag, pending_commit, commit
            );
        }
        self.peer.pending_merge_state = None;
        if let Some(r) = region {
            self.set_region(r);
        }
        if self.peer.is_leader() {
            info!("{} notify pd with rollback merge {}", self.peer.tag, commit);
            self.peer.heartbeat_pd(&self.pd_scheduler);
        }
    }

    fn on_ready_compute_hash(&mut self, region: metapb::Region, index: u64, snap: EngineSnapshot) {
        self.peer.consistency_state.last_check_time = Instant::now();
        let task = ConsistencyCheckTask::compute_hash(region, index, snap);
        info!("{} schedule {}", self.peer.tag, task);
        if let Err(e) = self.consistency_check_scheduler.schedule(task) {
            error!("{} schedule failed: {:?}", self.peer.tag, e);
        }
    }

    fn on_ready_verify_hash(&mut self, expected_index: u64, expected_hash: Vec<u8>) {
        let region_id = self.peer.region().get_id();
        actor_store::verify_and_store_hash(
            region_id,
            &mut self.peer.consistency_state,
            expected_index,
            expected_hash,
        );
    }

    fn on_ingest_sst_result(&mut self, ssts: Vec<SSTMeta>) {
        for sst in &ssts {
            assert_eq!(sst.get_region_id(), self.peer.region().get_id());
            self.peer.size_diff_hint += sst.get_length();
        }

        let task = CleanupSSTTask::DeleteSST { ssts };
        if let Err(e) = self.cleanup_sst_scheduler.schedule(task) {
            error!("schedule to delete ssts: {:?}", e);
        }
    }

    fn on_ready_result(&mut self, merged: bool, exec_results: Vec<ExecResult>) {
        // handle executing committed log results
        for result in exec_results {
            match result {
                ExecResult::ChangePeer(cp) => self.on_ready_change_peer(cp),
                ExecResult::CompactLog { first_index, state } => if !merged {
                    self.on_ready_compact_log(first_index, state)
                },
                ExecResult::SplitRegion {
                    left,
                    right,
                    right_derive,
                } => self.on_ready_split_region(left, right, right_derive),
                ExecResult::PrepareMerge { region, state } => {
                    self.on_ready_prepare_merge(region, state, merged);
                }
                ExecResult::CommitMerge { region, source } => {
                    self.on_ready_commit_merge(region, source);
                }
                ExecResult::RollbackMerge { region, commit } => {
                    self.on_ready_rollback_merge(commit, Some(region))
                }
                ExecResult::ComputeHash {
                    region,
                    index,
                    snap,
                } => self.on_ready_compute_hash(region, index, snap),
                ExecResult::VerifyHash { index, hash } => self.on_ready_verify_hash(index, hash),
                ExecResult::DeleteRange { .. } => {
                    // TODO: clean user properties?
                }
                ExecResult::IngestSST { ssts } => self.on_ingest_sst_result(ssts),
            }
        }
    }

    fn handle_apply(&mut self, msg: ApplyTaskRes) {
        match msg {
            ApplyTaskRes::Applys(mut multi_res) => {
                // TODO: change it to only one res.
                let res = multi_res.pop().unwrap();
                debug!("{} async apply finish: {:?}", self.peer.tag, res);
                let ApplyRes {
                    apply_state,
                    applied_index_term,
                    exec_res,
                    metrics,
                    merged,
                    ..
                } = res;
                self.on_ready_result(merged, exec_res);
                self.peer
                    .post_apply(apply_state, applied_index_term, merged, &metrics);
            }
            ApplyTaskRes::Destroy(_) => {
                let peer = self.peer.peer.clone();
                self.destroy_peer(peer, false);
            }
        }
    }

    fn handle_msgs(&mut self, msgs: &mut Vec<AllMsg>) {
        for msg in msgs.drain(..) {
            match msg {
                AllMsg::Msg(msg) => self.handle_msg(msg),
                AllMsg::SignificantMsg(msg) => self.handle_significant_msg(msg),
                AllMsg::Tick(tick) => self.handle_tick(tick),
                AllMsg::ApplyRes(res) => self.handle_apply(res),
            }
        }
    }

    fn on_ready_apply_snapshot(&mut self, apply_result: ApplySnapResult) {
        let prev_region = apply_result.prev_region;
        let region = apply_result.region;

        info!(
            "{} snapshot for region {:?} is applied",
            self.peer.tag, region
        );

        let mut range_state = self.range_state.lock().unwrap();
        if !prev_region.get_peers().is_empty() {
            info!(
                "{} region changed from {:?} -> {:?} after applying snapshot",
                self.peer.tag, prev_region, region
            );
            // we have already initialized the peer, so it must exist in region_ranges.
            if range_state
                .region_ranges
                .remove(&enc_end_key(&prev_region))
                .is_none()
            {
                panic!("{} region should exist {:?}", self.peer.tag, prev_region);
            }
        }

        range_state
            .region_ranges
            .insert(enc_end_key(&region), region.get_id());
        range_state.region_peers.insert(region.get_id(), region);
    }

    fn on_raft_ready(&mut self) {
        let t = SlowTimer::new();
        let previous_ready_metrics = self.raft_metrics.ready.clone();

        let proposals = self.peer.take_apply_proposals();
        let (kv_wb, raft_wb, append_res, sync_log) = {
            let mut ctx = ReadyContext::new(&mut self.raft_metrics, &self.external_transport);
            self.peer
                .handle_raft_ready_append(&mut ctx, &self.pd_scheduler);
            (ctx.kv_wb, ctx.raft_wb, ctx.ready_res, ctx.sync_log)
        };

        if let Some(proposals) = proposals {
            if self
                .apply_scheduler
                .schedule(ApplyTask::Proposals(proposals))
                .is_err()
            {
                // It close asynchonously.
                self.stopped = true;
            }

            // In most cases, if the leader proposes a message, it will also
            // broadcast the message to other followers, so we should flush the
            // messages ASAP.
            self.external_transport.flush();
        }

        // apply_snapshot, peer_destroy will clear_meta, so we need write region state first.
        // otherwise, if program restart between two write, raft log will be removed,
        // but region state may not changed in disk.
        fail_point!("raft_before_save");
        if !kv_wb.is_empty() {
            // RegionLocalState, ApplyState
            let mut write_opts = WriteOptions::new();
            write_opts.set_sync(true);
            self.peer
                .engines
                .kv
                .write_opt(kv_wb, &write_opts)
                .unwrap_or_else(|e| {
                    panic!(
                        "{} failed to save append state result: {:?}",
                        self.peer.tag, e
                    );
                });
        }
        fail_point!("raft_between_save");

        if !raft_wb.is_empty() {
            // RaftLocalState, Raft Log Entry
            let mut write_opts = WriteOptions::new();
            write_opts.set_sync(self.peer.cfg.sync_log || sync_log);
            self.peer
                .engines
                .raft
                .write_opt(raft_wb, &write_opts)
                .unwrap_or_else(|e| {
                    panic!(
                        "{} failed to save raft append result: {:?}",
                        self.peer.tag, e
                    );
                });
        }
        fail_point!("raft_after_save");

        let ready_result = append_res.map(|(mut ready, invoke_ctx)| {
            let is_merging = self.peer.pending_merge_state.is_some();
            let res = self.peer.post_raft_ready_append(
                &mut self.raft_metrics,
                &self.external_transport,
                &mut ready,
                invoke_ctx,
            );
            if is_merging && res.is_some() {
                // After applying a snapshot, merge is rollbacked implicitly.
                self.on_ready_rollback_merge(0, None);
            }
            (ready, res)
        });

        self.raft_metrics
            .append_log
            .observe(duration_to_sec(t.elapsed()) as f64);

        slow_log!(
            t,
            "{} handle ready include {} entries, {} messages and {} snapshots",
            self.peer.tag,
            self.raft_metrics.ready.append - previous_ready_metrics.append,
            self.raft_metrics.ready.message - previous_ready_metrics.message,
            self.raft_metrics.ready.snapshot - previous_ready_metrics.snapshot
        );

        if let Some((ready, res)) = ready_result {
            let apply_task = self.peer.handle_raft_ready_apply(ready);
            if let Some(apply_result) = res {
                self.on_ready_apply_snapshot(apply_result);
            }
            if let Some(apply_task) = apply_task {
                if self
                    .apply_scheduler
                    .schedule(ApplyTask::applies(apply_task))
                    .is_err()
                {
                    self.stopped = true;
                }
            }
        }

        let dur = t.elapsed();
        self.raft_metrics
            .process_ready
            .observe(duration_to_sec(dur) as f64);

        self.external_transport.flush();
        if self.pending_snapshot_region {
            let region_id = self.peer.region().get_id();
            self.range_state
                .lock()
                .unwrap()
                .pending_snapshot_regions
                .retain(|r| r.get_id() != region_id);
            self.pending_snapshot_region = false;
        }

        slow_log!(t, "{} on raft ready", self.peer.tag);
    }
}

use futures::{Async, Future, Poll, Stream};

impl<R: Transport + 'static> Future for PeerHolder<R> {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        let mut msgs;
        match self.receiver.poll() {
            Ok(Async::Ready(Some(msg))) => {
                msgs = Vec::with_capacity(1024);
                msgs.push(msg);
            }
            Ok(Async::NotReady) => return Ok(Async::NotReady),
            _ => unreachable!(),
        }
        loop {
            for _ in msgs.len()..msgs.capacity() {
                match self.receiver.poll() {
                    Ok(Async::Ready(Some(msg))) => msgs.push(msg),
                    Ok(Async::NotReady) => break,
                    _ => unreachable!(),
                }
            }
            if !msgs.is_empty() {
                self.handle_msgs(&mut msgs);
            } else {
                return Ok(Async::NotReady);
            }
            if self.stopped {
                return Ok(Async::Ready(()));
            } else if self.has_ready {
                self.on_raft_ready();
                self.has_ready = false;
            }
        }
        // TODO: handle ready and remove pending snapshot
    }
}
