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

use protobuf::{Message, RepeatedField};
use std::collections::hash_map::Entry;
use std::collections::Bound::{Excluded, Included, Unbounded};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{cmp, u64};

use futures::sync::oneshot;
use futures::{Async, Future, Poll, Stream};
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
use raft::eraftpb::{ConfChangeType, MessageType};
use raft::{self, SnapshotStatus, StateRole, INVALID_INDEX, NO_LIMIT};
use rocksdb::rocksdb_options::WriteOptions;
use rocksdb::DB;
use tokio_timer::timer::Handle;

use pd::PdTask;
use raftstore::{Error, Result};
use storage::CF_RAFT;
use util::escape;
use util::future::CountDownLatch;
use util::mpsc::{loose_bounded, Receiver};
use util::time::{duration_to_sec, SlowTimer};
use util::timer::GLOBAL_TIMER_HANDLE;
use util::worker::{FutureScheduler, Scheduler, Stopped};

use super::Key;
use raftstore::coprocessor::CoprocessorHost;
use raftstore::store::cmd_resp::{bind_term, new_error};
use raftstore::store::engine::{Peekable, Snapshot as EngineSnapshot};
use raftstore::store::fsm::{ConfigProvider, LocalStoreStat, MailBox, Router, StoreMeta};
use raftstore::store::keys::{self, enc_end_key, enc_start_key};
use raftstore::store::local_metrics::RaftMetrics;
use raftstore::store::metrics::*;
use raftstore::store::msg::Callback;
use raftstore::store::peer::{self, ConsistencyState, ReadyContext, StaleState};
use raftstore::store::peer_storage::{ApplySnapResult, PeerStorage};
use raftstore::store::transport::Transport;
use raftstore::store::worker::apply::{ApplyMetrics, ApplyRes, ChangePeer, ExecResult};
use raftstore::store::worker::{
    ApplyTask, ApplyTaskRes, CleanupSSTTask, ConsistencyCheckTask, RaftlogGcTask, ReadTask,
    RegionTask, SplitCheckTask,
};
use raftstore::store::{
    util, Config, Engines, PeerMsg, PeerTick, SignificantMsg, SnapKey, SnapManager,
    SnapshotDeleter, StoreMsg,
};

pub struct DestroyPeerJob {
    pub initialized: bool,
    pub async_remove: bool,
    pub region_id: u64,
    pub peer: metapb::Peer,
}

bitflags! {
    // TODO: maybe declare it as protobuf struct is better.
    pub struct TickSchedulerTracker: u8 {
        const STALE_PEER = 0b00000001;
        const SPLIT_CHECK = 0b00000010;
        const PD_HEARTBEAT = 0b00000100;
    }
}

struct MergeAsyncWait {
    res: ApplyRes,
    notifier: oneshot::Receiver<()>,
}

pub struct Peer<T: 'static> {
    peer: peer::Peer,
    pd_scheduler: FutureScheduler<PdTask>,
    raftlog_gc_scheduler: Scheduler<RaftlogGcTask>,
    consistency_check_scheduler: Scheduler<ConsistencyCheckTask>,
    split_check_scheduler: Scheduler<SplitCheckTask>,
    cleanup_sst_scheduler: Scheduler<CleanupSSTTask>,
    store_meta: Arc<Mutex<StoreMeta>>,
    poller: CpuPool,
    raft_metrics: RaftMetrics,
    snap_mgr: SnapManager,
    timer: Handle,
    mail_box: MailBox,
    router: Router,
    receiver: Receiver<PeerMsg>,
    trans: T,
    latch: CountDownLatch,
    store_stat: LocalStoreStat,
    tick_tracker: TickSchedulerTracker,
    pending_apply: Option<MergeAsyncWait>,
    need_flush_trans: bool,
    queued_snapshot: bool,
    has_ready: bool,
    stopped: bool,
}

impl<T: Transport> ConfigProvider<T> for Peer<T> {
    #[inline]
    fn store_id(&self) -> u64 {
        self.peer.peer.get_store_id()
    }

    #[inline]
    fn config(&self) -> Arc<Config> {
        Arc::clone(&self.peer.cfg)
    }

    #[inline]
    fn snap_scheduler(&self) -> Scheduler<RegionTask> {
        self.peer.get_store().region_sched()
    }

    #[inline]
    fn apply_scheduler(&self) -> Scheduler<ApplyTask> {
        self.peer.apply_scheduler.clone()
    }

    #[inline]
    fn read_scheduler(&self) -> Scheduler<ReadTask> {
        self.peer.read_scheduler.clone()
    }

    #[inline]
    fn engines(&self) -> Engines {
        self.peer.engines.clone()
    }

    #[inline]
    fn coprocessor_host(&self) -> Arc<CoprocessorHost> {
        Arc::clone(&self.peer.coprocessor_host)
    }

    #[inline]
    fn pd_scheduler(&self) -> FutureScheduler<PdTask> {
        self.pd_scheduler.clone()
    }

    #[inline]
    fn raft_log_gc_scheduler(&self) -> Scheduler<RaftlogGcTask> {
        self.raftlog_gc_scheduler.clone()
    }

    #[inline]
    fn consistency_check_scheduler(&self) -> Scheduler<ConsistencyCheckTask> {
        self.consistency_check_scheduler.clone()
    }

    #[inline]
    fn store_meta(&self) -> Arc<Mutex<StoreMeta>> {
        self.store_meta.clone()
    }

    #[inline]
    fn snap_manager(&self) -> SnapManager {
        self.snap_mgr.clone()
    }

    #[inline]
    fn split_check_scheduler(&self) -> Scheduler<SplitCheckTask> {
        self.split_check_scheduler.clone()
    }

    #[inline]
    fn cleanup_sst_scheduler(&self) -> Scheduler<CleanupSSTTask> {
        self.cleanup_sst_scheduler.clone()
    }

    #[inline]
    fn transport(&self) -> T {
        self.trans.clone()
    }

    #[inline]
    fn poller(&self) -> CpuPool {
        self.poller.clone()
    }

    #[inline]
    fn count_down_latch(&self) -> CountDownLatch {
        self.latch.clone()
    }

    #[inline]
    fn local_store_stat(&self) -> LocalStoreStat {
        self.store_stat.clone()
    }

    #[inline]
    fn router(&self) -> Router {
        self.router.clone()
    }
}

impl<T> Peer<T> {
    pub fn peer_id(&self) -> u64 {
        self.peer.peer_id()
    }

    pub fn term(&self) -> u64 {
        self.peer.term()
    }

    pub fn get_store(&self) -> &PeerStorage {
        self.peer.get_store()
    }

    pub fn region(&self) -> &Region {
        self.peer.region()
    }

    pub fn region_id(&self) -> u64 {
        self.peer.region().get_id()
    }

    pub fn kv_engine(&self) -> &Arc<DB> {
        &self.peer.engines.kv
    }

    pub fn mail_box(&self) -> MailBox {
        self.mail_box.clone()
    }
}

impl<T: Transport> Peer<T> {
    pub fn create<P: ConfigProvider<T>>(p: &P, region: &Region) -> Result<Peer<T>> {
        let peer = peer::Peer::create(p, region)?;
        Ok(Peer::new(p, peer))
    }

    pub fn replicate<P: ConfigProvider<T>>(
        p: &P,
        region_id: u64,
        peer: metapb::Peer,
    ) -> Result<Peer<T>> {
        let peer = peer::Peer::replicate(p, region_id, peer)?;
        Ok(Peer::new(p, peer))
    }

    fn new<P: ConfigProvider<T>>(p: &P, peer: peer::Peer) -> Peer<T> {
        let (tx, rx) = loose_bounded(peer.cfg.notify_capacity);
        Peer {
            peer,
            pd_scheduler: p.pd_scheduler(),
            raftlog_gc_scheduler: p.raft_log_gc_scheduler(),
            consistency_check_scheduler: p.consistency_check_scheduler(),
            split_check_scheduler: p.split_check_scheduler(),
            cleanup_sst_scheduler: p.cleanup_sst_scheduler(),
            store_meta: p.store_meta(),
            raft_metrics: RaftMetrics::default(),
            poller: p.poller(),
            snap_mgr: p.snap_manager(),
            timer: GLOBAL_TIMER_HANDLE.clone(),
            mail_box: tx,
            router: p.router(),
            receiver: rx,
            trans: p.transport(),
            latch: p.count_down_latch(),
            store_stat: p.local_store_stat(),
            tick_tracker: TickSchedulerTracker::empty(),
            pending_apply: None,
            need_flush_trans: false,
            queued_snapshot: false,
            has_ready: false,
            stopped: false,
        }
    }

    #[inline]
    fn on_significant_msg(&mut self, msg: SignificantMsg) {
        match msg {
            SignificantMsg::SnapshotStatus { to_peer_id, status } => {
                // Report snapshot status to the corresponding peer.
                self.report_snapshot_status(to_peer_id, status);
            }
            SignificantMsg::Unreachable { to_peer_id } => {
                self.peer.raft_group.report_unreachable(to_peer_id)
            }
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

    pub fn resume_applying_snapshot(&mut self) {
        info!(
            "{} resume applying snapshot [region {:?}]",
            self.peer.tag,
            self.region()
        );
        self.peer.mut_store().schedule_applying_snapshot();
    }

    pub fn resume_merging(&mut self, state: MergeState, meta: &mut StoreMeta) {
        info!(
            "{} resume merging [region {:?}, state: {:?}]",
            self.peer.tag,
            self.region(),
            state
        );
        self.peer.pending_merge_state = Some(state);
        self.notify_prepare_merge(meta);
    }

    fn notify_prepare_merge(&self, meta: &mut StoreMeta) {
        let lock_key = (
            self.region_id(),
            self.region().get_region_epoch().get_version(),
        );
        let (tx, _) = oneshot::channel();
        // If it's registered already, then remove it to notify
        // Commit merge, and then insert a new one to indicate prepare merge
        // is handled.
        meta.merge_locks.insert(lock_key, tx);
    }

    pub fn start(mut self, poller: &CpuPool) {
        self.schedule_raft_base_tick();
        self.schedule_raft_gc_log_tick();
        self.schedule_check_peer_stale_state_tick();
        if self.peer.pending_merge_state.is_some() {
            self.schedule_merge_check_tick();
        }
        poller.spawn(self).forget()
    }

    fn on_role_changed(&mut self, role: StateRole) {
        match role {
            StateRole::Leader => {
                self.schedule_pd_heartbeat_tick();
                self.schedule_split_region_check_tick();
            }
            _ => {
                self.schedule_check_peer_stale_state_tick();
            }
        }
    }
}

impl<T: Transport> Peer<T> {
    #[inline]
    fn schedule_tick(&self, dur: Duration, tick: PeerTick) {
        if dur != Duration::new(0, 0) {
            let tx = self.mail_box.clone();
            let f = self.timer.delay(Instant::now() + dur).map(move |_| {
                let _ = tx.force_send(PeerMsg::Tick(tick));
            });
            self.poller.spawn(f).forget()
        }
    }

    #[inline]
    fn schedule_raft_base_tick(&self) {
        self.schedule_tick(self.peer.cfg.raft_base_tick_interval.0, PeerTick::Raft)
    }

    fn on_raft_base_tick(&mut self) {
        if !self.peer.pending_remove {
            if !self.peer.is_applying_snapshot() && !self.peer.has_pending_snapshot() {
                if self.peer.raft_group.tick() {
                    self.has_ready = true;
                }
            } else {
                // need to check if snapshot is applied
                self.has_ready = true;
            }
            self.schedule_raft_base_tick();
        }

        self.raft_metrics.flush();
        self.store_stat.flush();
        self.peer.mut_store().flush_cache_metrics();
    }

    #[inline]
    fn on_apply_res(&mut self, res: ApplyTaskRes) {
        match res {
            ApplyTaskRes::Apply(mut apply_res) => {
                debug!("{} async apply finish: {:?}", self.peer.tag, apply_res);
                if let Some(rx) = self.on_ready_result(
                    apply_res.merged,
                    &mut apply_res.exec_res,
                    &apply_res.metrics,
                ) {
                    self.pending_apply = Some(MergeAsyncWait {
                        res: apply_res,
                        notifier: rx,
                    });
                    return;
                }
                if self.peer.post_apply(
                    apply_res.apply_state,
                    apply_res.applied_index_term,
                    apply_res.merged,
                    &apply_res.metrics,
                ) {
                    self.has_ready = true;
                }
            }
            ApplyTaskRes::Destroy(p) => {
                assert_eq!(p.id(), self.peer_id());
                assert_eq!(p.region_id(), self.region_id());
                self.destroy_peer(false);
            }
        }
    }

    fn resume_handling_pending_apply(&mut self) -> bool {
        let pending_apply = self.pending_apply.take().unwrap();
        let mut res = pending_apply.res;
        debug!("{} resume handling apply result {:?}", self.peer.tag, res);
        if let Some(rx) = self.on_ready_exec_results(res.merged, &mut res.exec_res) {
            self.pending_apply = Some(MergeAsyncWait { res, notifier: rx });
            return false;
        }
        if self.peer.post_apply(
            res.apply_state,
            res.applied_index_term,
            res.merged,
            &res.metrics,
        ) {
            self.has_ready = true;
        }
        true
    }

    fn on_raft_message(&mut self, mut msg: RaftMessage) -> Result<()> {
        debug!(
            "{} handle raft message {:?}, from {} to {}",
            self.peer.tag,
            msg.get_message().get_msg_type(),
            msg.get_from_peer().get_id(),
            msg.get_to_peer().get_id()
        );

        if !self.validate_raft_msg(&msg) {
            return Ok(());
        }

        if msg.get_is_tombstone() {
            // we receive a message tells us to remove ourself.
            self.handle_gc_peer_msg(&msg);
            return Ok(());
        }

        if msg.has_merge_target() {
            if self.need_gc_merge(&msg)? {
                self.on_stale_merge();
            }
            return Ok(());
        }

        if self.check_msg(&msg) {
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

    // return false means the message is invalid, and can be ignored.
    fn validate_raft_msg(&mut self, msg: &RaftMessage) -> bool {
        let to = msg.get_to_peer();

        if to.get_store_id() != self.store_id() {
            warn!(
                "{} store not match, to store id {}, mine {}, ignore it",
                self.peer.tag,
                to.get_store_id(),
                self.store_id()
            );
            self.raft_metrics.message_dropped.mismatch_store_id += 1;
            return false;
        }

        if !msg.has_region_epoch() {
            error!("{} missing epoch in raft message, ignore it", self.peer.tag);
            self.raft_metrics.message_dropped.mismatch_region_epoch += 1;
            return false;
        }

        true
    }

    fn check_msg(&mut self, msg: &RaftMessage) -> bool {
        let from_epoch = msg.get_region_epoch();
        let msg_type = msg.get_message().get_msg_type();
        let is_vote_msg =
            msg_type == MessageType::MsgRequestVote || msg_type == MessageType::MsgRequestPreVote;
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

        if util::is_epoch_stale(from_epoch, self.peer.region().get_region_epoch())
            && util::find_peer(self.peer.region(), from_store_id).is_none()
        {
            // The message is stale and not in current region.
            self.handle_stale_msg(msg, is_vote_msg, None);
            return true;
        }

        let target = msg.get_to_peer();
        if target.get_id() < self.peer.peer_id() {
            info!(
                "{} target peer id {} is less than {}, msg maybe stale.",
                self.peer.tag,
                target.get_id(),
                self.peer.peer_id()
            );
            self.raft_metrics.message_dropped.stale_msg += 1;
            true
        } else if target.get_id() > self.peer.peer_id() {
            match self.peer.maybe_destroy() {
                Some(job) => {
                    info!(
                        "{} is stale as received a larger peer {:?}, destroying.",
                        self.peer.tag, target
                    );
                    if self.handle_destroy_peer(job) {
                        let _ = self
                            .router
                            .send_store_message(StoreMsg::RaftMessage(msg.clone()));
                    }
                }
                None => self.raft_metrics.message_dropped.applying_snap += 1,
            }
            true
        } else {
            false
        }
    }

    fn handle_stale_msg(
        &mut self,
        msg: &RaftMessage,
        need_gc: bool,
        target_region: Option<metapb::Region>,
    ) {
        let region_id = msg.get_region_id();
        let from_peer = msg.get_from_peer();
        let to_peer = msg.get_to_peer();
        let msg_type = msg.get_message().get_msg_type();
        let cur_epoch = self.peer.region().get_region_epoch();

        if !need_gc {
            info!(
                "{} raft message {:?} is stale, current {:?}, ignore it",
                self.peer.tag, msg_type, cur_epoch
            );
            self.raft_metrics.message_dropped.stale_msg += 1;
            return;
        }

        info!(
            "{} raft message {:?} is stale, current {:?}, tell to gc",
            self.peer.tag, msg_type, cur_epoch
        );

        let mut gc_msg = RaftMessage::new();
        gc_msg.set_region_id(region_id);
        gc_msg.set_from_peer(to_peer.clone());
        gc_msg.set_to_peer(from_peer.clone());
        gc_msg.set_region_epoch(cur_epoch.clone());
        if let Some(r) = target_region {
            gc_msg.set_merge_target(r);
        } else {
            gc_msg.set_is_tombstone(true);
        }
        if let Err(e) = self.trans.send(gc_msg) {
            error!("{} send gc message failed {:?}", self.peer.tag, e);
        }
        self.need_flush_trans = true;
    }

    /// Check if it's necessary to gc the source merge peer.
    ///
    /// If the target merge peer won't be created on this store,
    /// then it's appropriate to destroy it immediately.
    fn need_gc_merge(&mut self, msg: &RaftMessage) -> Result<bool> {
        let merge_target = msg.get_merge_target();
        let target_region_id = merge_target.get_id();

        let meta = self.store_meta.lock().unwrap();
        if let Some(epoch) = meta.pending_cross_snap.get(&target_region_id).or_else(|| {
            meta.regions
                .get(&target_region_id)
                .map(|r| r.get_region_epoch())
        }) {
            info!(
                "{} checking target {} epoch: {:?}",
                self.peer.tag, target_region_id, epoch
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
            .kv_engine()
            .get_msg_cf::<RegionLocalState>(CF_RAFT, &state_key)?
        {
            debug!(
                "{} check local state {:?} for region {}",
                self.peer.tag, state, target_region_id
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
            "{} no replica of region {} exist, check pd.",
            self.peer.tag, target_region_id
        );
        // We can't know whether the peer is destroyed or not for sure locally, ask
        // pd for help.
        let target_peer = merge_target
            .get_peers()
            .iter()
            .find(|p| p.get_store_id() == self.store_id())
            .unwrap();
        let task = PdTask::ValidatePeer {
            peer: target_peer.to_owned(),
            region: merge_target.to_owned(),
            merge_source: Some(self.region_id()),
        };
        if let Err(e) = self.pd_scheduler.schedule(task) {
            error!(
                "{} failed to validate target peer {:?}: {}",
                self.peer.tag, target_peer, e
            );
        }
        Ok(false)
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
        info!(
            "{} peer {:?} receives gc message, trying to remove",
            self.peer.tag,
            msg.get_to_peer()
        );
        match self.peer.maybe_destroy() {
            Some(job) => {
                self.handle_destroy_peer(job);
            }
            None => self.raft_metrics.message_dropped.applying_snap += 1,
        }
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
                "{} {:?} doesn't contain peer {:?}, skip.",
                self.peer.tag,
                snap_region,
                msg.get_to_peer()
            );
            self.raft_metrics.message_dropped.region_no_peer += 1;
            return Ok(Some(key));
        }

        let mut meta = self.store_meta.lock().unwrap();
        if meta.regions[&self.region_id()] != *self.region() {
            if !self.peer.is_initialized() {
                info!("{} stale delegate detected, skip.", self.peer.tag);
                return Ok(Some(key));
            } else {
                panic!(
                    "{} meta corrupted: {:?} != {:?}",
                    self.peer.tag,
                    meta.regions[&self.region_id()],
                    self.region()
                );
            }
        }
        let r = meta
            .region_ranges
            .range((Excluded(enc_start_key(&snap_region)), Unbounded::<Key>))
            .map(|(_, region_id)| &meta.regions[region_id])
            .take_while(|r| enc_start_key(r) < enc_end_key(&snap_region))
            .skip_while(|r| r.get_id() == region_id)
            .next()
            .map(|r| r.to_owned());
        if let Some(exist_region) = r {
            info!(
                "{} region overlapped {:?}, {:?}",
                self.peer.tag, exist_region, snap_region
            );
            meta.pending_cross_snap
                .insert(region_id, snap_region.get_region_epoch().to_owned());
            self.raft_metrics.message_dropped.region_overlap += 1;
            return Ok(Some(key));
        }
        for region in &meta.pending_snapshot_regions {
            if enc_start_key(region) < enc_end_key(&snap_region) &&
               enc_end_key(region) > enc_start_key(&snap_region) &&
               // Same region can overlap, we will apply the latest version of snapshot.
               region.get_id() != snap_region.get_id()
            {
                info!(
                    "{} pending region overlapped {:?}, {:?}",
                    self.peer.tag, region, snap_region
                );
                self.raft_metrics.message_dropped.region_overlap += 1;
                return Ok(Some(key));
            }
        }
        if let Some(r) = meta.pending_cross_snap.get(&region_id) {
            // Check it to avoid epoch moves backward.
            if util::is_epoch_stale(snap_region.get_region_epoch(), r) {
                info!(
                    "{} snapshot epoch is stale, drop: {:?} < {:?}",
                    self.peer.tag,
                    snap_region.get_region_epoch(),
                    r
                );
                self.raft_metrics.message_dropped.stale_msg += 1;
                return Ok(Some(key));
            }
        }
        // check if snapshot file exists.
        self.snap_mgr.get_snapshot_for_applying(&key)?;

        meta.pending_snapshot_regions.push(snap_region);
        self.queued_snapshot = true;
        meta.pending_cross_snap.remove(&region_id);

        Ok(None)
    }

    fn handle_raft_ready(&mut self) {
        let t = SlowTimer::new();
        let previous_ready_metrics = self.raft_metrics.ready.clone();

        self.raft_metrics.ready.pending_region += 1;

        let (kv_wb, raft_wb, append_res, sync_log) = {
            let mut ctx = ReadyContext::new(&mut self.raft_metrics, &mut self.trans);
            self.peer
                .handle_raft_ready_append(&mut ctx, &self.pd_scheduler);
            (ctx.kv_wb, ctx.raft_wb, ctx.ready_res, ctx.sync_log)
        };

        if let Some((ref ready, _)) = append_res {
            if let Some(ref ss) = ready.ss {
                self.on_role_changed(ss.raft_state)
            }
        }

        if let Some(proposals) = self.peer.take_apply_proposals() {
            if self
                .peer
                .apply_scheduler
                .schedule(ApplyTask::Proposals(proposals))
                .is_err()
            {
                warn!(
                    "{} fail to schedule apply tasks, are we shutting down?",
                    self.peer.tag
                );
            }

            // In most cases, if the leader proposes a message, it will also
            // broadcast the message to other followers, so we should flush the
            // messages ASAP.
            self.trans.flush();
            self.need_flush_trans = false;
        }

        self.raft_metrics.ready.has_ready_region += append_res.is_some() as u64;

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
                &mut self.trans,
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
            "{} handle {} ready, {} entries, {} messages and {} \
             snapshots",
            self.peer.tag,
            ready_result.is_some() as u8,
            self.raft_metrics.ready.append - previous_ready_metrics.append,
            self.raft_metrics.ready.message - previous_ready_metrics.message,
            self.raft_metrics.ready.snapshot - previous_ready_metrics.snapshot
        );

        if let Some((ready, res)) = ready_result {
            if let Some(apply_task) = self.peer.handle_raft_ready_apply(ready) {
                if self
                    .peer
                    .apply_scheduler
                    .schedule(ApplyTask::apply(apply_task))
                    .is_err()
                {
                    warn!(
                        "{} failed to schedule apply task, are we shutting down?",
                        self.peer.tag
                    );
                }
            }
            if let Some(apply_result) = res {
                self.on_ready_apply_snapshot(apply_result);
            }
        }

        let dur = t.elapsed();
        if !self.store_stat.is_busy {
            let election_timeout = Duration::from_millis(
                self.peer.cfg.raft_base_tick_interval.as_millis()
                    * self.peer.cfg.raft_election_timeout_ticks as u64,
            );
            if dur >= election_timeout {
                self.store_stat.is_busy = true;
            }
        }

        self.raft_metrics
            .process_ready
            .observe(duration_to_sec(dur) as f64);

        self.trans.flush();
        self.need_flush_trans = false;

        slow_log!(t, "{} on raft ready", self.peer.tag);
    }

    fn handle_destroy_peer(&mut self, job: DestroyPeerJob) -> bool {
        if job.initialized {
            self.peer
                .apply_scheduler
                .schedule(ApplyTask::destroy(job.region_id))
                .unwrap();
            self.peer
                .read_scheduler
                .schedule(ReadTask::destroy(job.region_id))
                .unwrap();
        }
        if job.async_remove {
            info!("{} is destroyed asychroniously", self.peer.tag);
            false
        } else {
            self.destroy_peer(false);
            true
        }
    }

    fn destroy_peer(&mut self, merged_by_target: bool) {
        info!(
            "{} starts destroy [merged_by_target: {}]",
            self.peer.tag, merged_by_target
        );
        self.stopped = true;
        let region_id = self.region_id();

        // We can't destroy a peer which is applying snapshot.
        assert!(!self.peer.is_applying_snapshot());
        let mut meta = self.store_meta.lock().unwrap();
        meta.pending_cross_snap.remove(&region_id);
        let task = PdTask::DestroyPeer { region_id };
        if let Err(e) = self.pd_scheduler.schedule(task) {
            error!("{} failed to notify pd: {}", self.peer.tag, e);
        }
        let is_initialized = self.peer.is_initialized();
        if let Err(e) = self.peer.destroy(merged_by_target) {
            // If not panic here, the peer will be recreated in the next restart,
            // then it will be gc again. But if some overlap region is created
            // before restarting, the gc action will delete the overlap region's
            // data too.
            panic!(
                "{} destroy peer {:?} in store {} err {:?}",
                self.peer.tag,
                self.peer.peer,
                self.store_id(),
                e
            );
        }
        self.router.unregister_mailbox(region_id);

        // Do do the removal when it's merged. As the meta is cleared when committing
        // merge.
        if is_initialized && !merged_by_target
            && meta
                .region_ranges
                .remove(&enc_end_key(self.peer.region()))
                .is_none()
        {
            panic!(
                "{} meta corruption detected remove peer {:?} in store {}",
                self.peer.tag,
                self.peer.peer,
                self.store_id()
            );
        }
        if meta.regions.remove(&region_id).is_none() && !merged_by_target {
            panic!(
                "{} meta corruption detected when removing peer {:?} on store {}",
                self.peer.tag,
                self.peer.peer,
                self.store_id()
            );
        }
    }

    fn on_ready_change_peer(&mut self, cp: ChangePeer) {
        let my_peer_id;
        let change_type = cp.conf_change.get_change_type();
        self.peer.raft_group.apply_conf_change(&cp.conf_change);
        if cp.conf_change.get_node_id() == raft::INVALID_ID {
            // Apply failed, skip.
            return;
        }
        {
            let mut meta = self.store_meta.lock().unwrap();
            meta.set_region(cp.region, &mut self.peer);
        }
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
        my_peer_id = self.peer.peer_id();

        let peer = cp.peer;

        // We only care remove itself now.
        if change_type == ConfChangeType::RemoveNode && peer.get_store_id() == self.store_id() {
            if my_peer_id == peer.get_id() {
                self.destroy_peer(false)
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
            region_id: self.region_id(),
            start_idx: self.peer.last_compacted_idx,
            end_idx: state.get_index() + 1,
        };
        self.peer.last_compacted_idx = task.end_idx;
        self.peer.mut_store().compact_to(task.end_idx);
        if let Err(e) = self.raftlog_gc_scheduler.schedule(task) {
            error!("{} failed to schedule compact task: {}", self.peer.tag, e);
        }
    }

    fn on_ready_split_region(&mut self, derived: metapb::Region, regions: Vec<metapb::Region>) {
        let mut meta = self.store_meta.lock().unwrap();
        let region_id = derived.get_id();
        meta.set_region(derived, &mut self.peer);
        self.peer.post_split();
        let is_leader = self.peer.is_leader();
        if is_leader {
            self.peer.heartbeat_pd(&self.pd_scheduler);
            // Notify pd immediately to let it update the region meta.
            info!(
                "{} notify pd with split count {}",
                self.peer.tag,
                regions.len()
            );

            // Now pd only uses ReportBatchSplit for history operation show,
            // so we send it independently here.
            let task = PdTask::ReportBatchSplit {
                regions: regions.to_vec(),
            };

            if let Err(e) = self.pd_scheduler.schedule(task) {
                error!("{} failed to notify pd: {}", self.peer.tag, e);
            }
        }
        let last_key = enc_end_key(regions.last().unwrap());
        if meta.region_ranges.remove(&last_key).is_none() {
            panic!("{} original region should exists", self.peer.tag);
        }
        // It's not correct anymore.
        self.peer.approximate_size.take();
        let last_region_id = regions.last().unwrap().get_id();
        for new_region in regions {
            let new_region_id = new_region.get_id();
            let not_exist = meta
                .region_ranges
                .insert(enc_end_key(&new_region), new_region_id)
                .is_none();
            assert!(
                not_exist,
                "[region {}] should not exists",
                new_region.get_id()
            );
            if new_region_id == region_id {
                continue;
            }
            // Insert new regions and validation
            info!(
                "[region {}] insert new region {:?}",
                new_region.get_id(),
                new_region
            );

            let mut new_peer = match Peer::create(self, &new_region) {
                Ok(new_peer) => new_peer,
                Err(e) => {
                    // peer information is already written into db, can't recover.
                    // there is probably a bug.
                    panic!(
                        "{} create new split region {:?} err {:?}",
                        self.peer.tag, new_region, e
                    );
                }
            };
            for peer in new_region.get_peers() {
                // Add this peer to cache.
                new_peer.peer.insert_peer_cache(peer.clone());
            }
            let meta_peer = new_peer.peer.peer.clone();
            // New peer derive write flow from parent region,
            // this will be used by balance write flow.
            new_peer.peer.peer_stat = self.peer.peer_stat.clone();
            let campaigned = new_peer.peer.maybe_campaign(is_leader);
            new_peer.has_ready = campaigned;

            if is_leader {
                // The new peer is likely to become leader, send a heartbeat immediately to reduce
                // client query miss.
                new_peer.peer.heartbeat_pd(&self.pd_scheduler);
            }

            new_peer.peer.register_delegates();
            if let Some(r) = meta.regions.insert(new_region_id, new_region) {
                // If the store received a raft msg with the new region raft group
                // before splitting, it will creates a uninitialized peer.
                // We can remove this uninitialized peer directly.
                if !r.get_peers().is_empty() {
                    panic!(
                        "{} duplicated region {:?} for split region {:?}",
                        new_peer.peer.tag,
                        r,
                        new_peer.peer.region()
                    )
                }
                self.router
                    .unregister_mailbox(new_region_id)
                    .unwrap()
                    .close();
            }

            if !campaigned {
                if let Some(msg) = meta
                    .pending_votes
                    .swap_remove_front(|m| m.get_to_peer() == &meta_peer)
                {
                    let _ = new_peer.on_raft_message(msg);
                }
            }
            if new_region_id == last_region_id {
                // To prevent from big region, the right region needs run split
                // check again after split.
                new_peer.peer.size_diff_hint = self.peer.cfg.region_split_check_diff.0;
            }
            self.router
                .register_mailbox(new_region_id, new_peer.mail_box());
            new_peer.start(&self.poller);
        }
    }

    #[inline]
    fn schedule_merge_check_tick(&self) {
        self.schedule_tick(
            self.peer.cfg.merge_check_tick_interval.0,
            PeerTick::CheckMerge,
        )
    }

    fn validate_merge_target(&self, target_region: &metapb::Region) -> Result<bool> {
        let region_id = target_region.get_id();
        let exist_region = {
            let meta = self.store_meta.lock().unwrap();
            meta.regions.get(&region_id).cloned()
        };
        if let Some(r) = exist_region {
            let exist_epoch = r.get_region_epoch();
            let expect_epoch = target_region.get_region_epoch();
            // exist_epoch > expect_epoch
            if util::is_epoch_stale(expect_epoch, exist_epoch) {
                return Err(box_err!(
                    "target region changed {:?} -> {:?}",
                    target_region,
                    r
                ));
            }
            // exist_epoch < expect_epoch
            if util::is_epoch_stale(exist_epoch, expect_epoch) {
                info!(
                    "{} target region still not catch up: {:?} vs {:?}, skip.",
                    self.peer.tag, target_region, r
                );
                return Ok(false);
            }
            return Ok(true);
        }

        let state_key = keys::region_state_key(region_id);
        let state: RegionLocalState = match self.kv_engine().get_msg_cf(CF_RAFT, &state_key) {
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

    fn schedule_merge(&mut self) -> Result<()> {
        fail_point!("on_schedule_merge", |_| Ok(()));
        let req = {
            let state = self.peer.pending_merge_state.as_ref().unwrap();
            let expect_region = state.get_target();
            if !self.validate_merge_target(expect_region)? {
                return Ok(());
            }
            let sibling_region = expect_region;

            let min_index = self.peer.get_min_progress() + 1;
            let low = cmp::max(min_index, state.get_min_index());
            // TODO: move this into raft module.
            // > over >= to include the PrepareMerge proposal.
            let entries = if low > state.get_commit() {
                vec![]
            } else {
                self.peer
                    .get_store()
                    .entries(low, state.get_commit() + 1, NO_LIMIT)
                    .unwrap()
            };

            let sibling_peer = util::find_peer(&sibling_region, self.store_id()).unwrap();
            let mut request = new_admin_request(sibling_region.get_id(), sibling_peer.clone());
            request
                .mut_header()
                .set_region_epoch(sibling_region.get_region_epoch().clone());
            let mut admin = AdminRequest::new();
            admin.set_cmd_type(AdminCmdType::CommitMerge);
            admin.mut_commit_merge().set_source(self.region().clone());
            admin.mut_commit_merge().set_commit(state.get_commit());
            admin
                .mut_commit_merge()
                .set_entries(RepeatedField::from_vec(entries));
            request.set_admin_request(admin);
            request
        };
        // Please note that, here assumes that the unit of network isolation is store rather than
        // peer. So a quorum stores of souce region should also be the quorum stores of target
        // region. Otherwise we need to enable proposal forwarding.
        let _ = self.router.send_cmd(req, Callback::None);
        Ok(())
    }

    fn rollback_merge(&mut self) {
        let req = {
            let state = self.peer.pending_merge_state.as_ref().unwrap();
            let mut request = new_admin_request(self.region_id(), self.peer.peer.clone());
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

    fn on_check_merge(&mut self) {
        // In case merge is rollback.
        if !self.stopped && self.peer.pending_merge_state.is_some() {
            match self.schedule_merge() {
                Ok(_) => self.schedule_merge_check_tick(),
                Err(e) => {
                    info!(
                        "{} failed to schedule merge, rollback: {:?}",
                        self.peer.tag, e
                    );
                    self.rollback_merge();
                }
            }
        }
    }

    fn on_ready_prepare_merge(&mut self, region: metapb::Region, state: MergeState, merged: bool) {
        {
            let mut meta = self.store_meta.lock().unwrap();
            meta.set_region(region.clone(), &mut self.peer);
            self.peer.pending_merge_state = Some(state);
            self.notify_prepare_merge(&mut meta);
        }

        if merged {
            // CommitMerge will try to catch up log for source region. If PrepareMerge is executed
            // in the progress of catching up, there is no need to schedule merge again.
            return;
        }

        self.on_check_merge();
    }

    fn on_ready_commit_merge(
        &mut self,
        region: metapb::Region,
        source: metapb::Region,
    ) -> Option<oneshot::Receiver<()>> {
        let mut meta = self.store_meta.lock().unwrap();
        let lock_key = (source.get_id(), source.get_region_epoch().get_version());
        match meta.merge_locks.entry(lock_key) {
            // So premerge is executed.
            Entry::Occupied(e) => {
                e.remove();
            }
            Entry::Vacant(v) => {
                let (tx, mut rx) = oneshot::channel();
                let _ = rx.poll();
                v.insert(tx);
                return Some(rx);
            }
        }
        let prev = meta.region_ranges.remove(&enc_end_key(&source));
        assert_eq!(prev, Some(source.get_id()));
        let prev = if region.get_end_key() == source.get_end_key() {
            meta.region_ranges.remove(&enc_start_key(&source))
        } else {
            meta.region_ranges.remove(&enc_end_key(&region))
        };
        if prev != Some(region.get_id()) {
            panic!(
                "{} meta corrupted: prev: {:?}, ranges: {:?}",
                self.peer.tag, prev, meta.region_ranges
            );
        }
        meta.region_ranges
            .insert(enc_end_key(&region), region.get_id());
        assert!(meta.regions.remove(&source.get_id()).is_some());
        meta.set_region(region, &mut self.peer);
        if self.peer.is_leader() {
            // make approximate size and keys updated in time.
            self.peer.size_diff_hint = self.peer.cfg.region_split_check_diff.0;
            info!(
                "{} notify pd with merge {:?} into {:?}",
                self.peer.tag,
                source,
                self.peer.region()
            );
            self.peer.heartbeat_pd(&self.pd_scheduler);
        }
        let _ = self.router.send_peer_message(
            source.get_id(),
            PeerMsg::MergeResult {
                target: self.peer.peer.clone(),
                stale: false,
            },
        );
        None
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
            let mut meta = self.store_meta.lock().unwrap();
            meta.set_region(r, &mut self.peer);
            let lock_key = (
                self.region_id(),
                // rollback will increase the version by 1.
                self.region().get_region_epoch().get_version() - 1,
            );
            assert!(meta.merge_locks.remove(&lock_key).is_some());
        }
        if self.peer.is_leader() {
            info!("{} notify pd with rollback merge {}", self.peer.tag, commit);
            self.peer.heartbeat_pd(&self.pd_scheduler);
        }
    }

    fn on_stale_merge(&mut self) {
        info!(
            "{} successful merge to {:?} can't be continued, try to gc stale peer.",
            self.peer.tag, self.peer.pending_merge_state
        );
        if let Some(job) = self.peer.maybe_destroy() {
            self.handle_destroy_peer(job);
        }
    }

    fn on_merge_result(&mut self, target: metapb::Peer, stale: bool) {
        let exists = self
            .peer
            .pending_merge_state
            .as_ref()
            .map_or(true, |s| s.get_target().get_peers().contains(&target));
        if !exists {
            panic!(
                "{} unexpected merge result: {:?} {:?} {}",
                self.peer.tag, self.peer.pending_merge_state, target, stale
            );
        }

        if !stale {
            info!(
                "{} merge to {:?} finish.",
                self.peer.tag,
                self.peer.pending_merge_state.as_ref().unwrap().target
            );
            self.destroy_peer(true);
        } else {
            self.on_stale_merge();
        }
    }

    fn on_ready_apply_snapshot(&mut self, apply_result: ApplySnapResult) {
        let prev_region = apply_result.prev_region;
        let region = apply_result.region;

        info!(
            "{} snapshot for region {:?} is applied",
            self.peer.tag, region
        );

        let mut meta = self.store_meta.lock().unwrap();

        debug!(
            "{} ranges {:?} prev_region {:?}",
            self.peer.tag, meta.region_ranges, prev_region
        );

        let initialized = !prev_region.get_peers().is_empty();
        if initialized {
            info!(
                "{} region changed from {:?} -> {:?} after applying snapshot",
                self.peer.tag, prev_region, region
            );
            let prev = meta.region_ranges.remove(&enc_end_key(&prev_region));
            if prev != Some(region.get_id()) {
                panic!(
                    "{} meta corrupted, expect {:?} got {:?}",
                    self.peer.tag, prev_region, prev
                );
            }
        }

        if let Some(r) = meta
            .region_ranges
            .insert(enc_end_key(&region), region.get_id())
        {
            panic!("{} unexpected region {:?}", self.peer.tag, r);
        }
        let prev = meta.regions.insert(region.get_id(), region);
        assert_eq!(prev, Some(prev_region));
    }

    fn on_ready_result(
        &mut self,
        merged: bool,
        exec_results: &mut VecDeque<ExecResult>,
        metrics: &ApplyMetrics,
    ) -> Option<oneshot::Receiver<()>> {
        self.store_stat.lock_cf_bytes_written += metrics.lock_cf_written_bytes;
        self.store_stat.engine_total_bytes_written += metrics.written_bytes;
        self.store_stat.engine_total_keys_written += metrics.written_keys;

        self.on_ready_exec_results(merged, exec_results)
    }

    fn on_ready_exec_results(
        &mut self,
        merged: bool,
        exec_results: &mut VecDeque<ExecResult>,
    ) -> Option<oneshot::Receiver<()>> {
        // handle executing committed log results
        while let Some(result) = exec_results.pop_front() {
            match result {
                ExecResult::ChangePeer(cp) => self.on_ready_change_peer(cp),
                ExecResult::CompactLog { first_index, state } => if !merged {
                    self.on_ready_compact_log(first_index, state)
                },
                ExecResult::SplitRegion { derived, regions } => {
                    self.on_ready_split_region(derived, regions)
                }
                ExecResult::PrepareMerge { region, state } => {
                    self.on_ready_prepare_merge(region, state, merged);
                }
                ExecResult::CommitMerge { region, source } => {
                    if let Some(rx) = self.on_ready_commit_merge(region.clone(), source.clone()) {
                        exec_results.push_front(ExecResult::CommitMerge { region, source });
                        return Some(rx);
                    }
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
        None
    }

    /// Check if a request is valid if it has valid prepare_merge/commit_merge proposal.
    fn check_merge_proposal(&self, msg: &RaftCmdRequest) -> Result<()> {
        if !msg.get_admin_request().has_prepare_merge()
            && !msg.get_admin_request().has_commit_merge()
        {
            return Ok(());
        }

        let region = self.peer.region();

        if msg.get_admin_request().has_prepare_merge() {
            let target_region = msg.get_admin_request().get_prepare_merge().get_target();
            {
                let meta = self.store_meta.lock().unwrap();
                match meta.regions.get(&target_region.get_id()) {
                    Some(r) => if r != target_region {
                        return Err(box_err!(
                            "target region not matched, skip proposing: {:?} != {:?}",
                            r,
                            target_region
                        ));
                    },
                    None => {
                        return Err(box_err!(
                            "target region {} doesn't exist.",
                            target_region.get_id()
                        ))
                    }
                }
            }
            if !util::is_sibling_regions(target_region, region) {
                return Err(box_err!(
                    "{:?} and {:?} are not sibling, skip proposing",
                    target_region,
                    region
                ));
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
            if !util::is_sibling_regions(source_region, region) {
                return Err(box_err!(
                    "{:?} {:?} should be sibling",
                    source_region,
                    region
                ));
            }
            if !util::region_on_same_stores(source_region, region) {
                return Err(box_err!(
                    "peers not matched: {:?} {:?}",
                    source_region,
                    region
                ));
            }
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
        let region_id = self.region_id();
        let leader_id = self.peer.leader_id();
        if !self.peer.is_leader() {
            let leader = self.peer.get_peer_from_cache(leader_id);
            return Err(Error::NotLeader(region_id, leader));
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
                let sibling_region = self.find_sibling_region();
                if let Some(sibling_region) = sibling_region {
                    new_regions.push(sibling_region);
                }
                Err(Error::StaleEpoch(msg, new_regions))
            }
            Err(e) => Err(e),
            Ok(()) => Ok(None),
        }
    }

    fn propose_raft_command(&mut self, msg: RaftCmdRequest, cb: Callback) {
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

        if let Err(e) = self.check_merge_proposal(&msg) {
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

    fn find_sibling_region(&self) -> Option<Region> {
        let start = if self.peer.cfg.right_derive_when_split {
            Included(enc_start_key(self.peer.region()))
        } else {
            Excluded(enc_end_key(self.peer.region()))
        };
        let meta = self.store_meta.lock().unwrap();
        meta.region_ranges
            .range((start, Unbounded::<Key>))
            .next()
            .map(|(_, region_id)| meta.regions[&region_id].clone())
    }

    #[inline]
    fn schedule_raft_gc_log_tick(&self) {
        self.schedule_tick(
            self.peer.cfg.raft_log_gc_tick_interval.0,
            PeerTick::RaftLogGc,
        )
    }

    #[cfg_attr(feature = "cargo-clippy", allow(if_same_then_else))]
    fn on_raft_gc_log_tick(&mut self) {
        self.schedule_raft_gc_log_tick();

        let applied_idx = self.peer.get_store().applied_index();
        if !self.peer.is_leader() {
            self.peer.mut_store().compact_to(applied_idx + 1);
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
            return;
        } else {
            compact_idx = replicated_idx;
        }

        // Have no idea why subtract 1 here, but original code did this by magic.
        assert!(compact_idx > 0);
        compact_idx -= 1;
        if compact_idx < first_idx {
            // In case compact_idx == first_idx before subtraction.
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
        let request =
            new_compact_log_request(self.region_id(), self.peer.peer.clone(), compact_idx, term);

        self.propose_raft_command(request, Callback::None);

        PEER_GC_RAFT_LOG_COUNTER.inc_by((compact_idx - first_idx) as i64);
    }

    #[inline]
    fn schedule_split_region_check_tick(&mut self) {
        if !self
            .tick_tracker
            .contains(TickSchedulerTracker::SPLIT_CHECK)
        {
            self.tick_tracker.insert(TickSchedulerTracker::SPLIT_CHECK);
            self.schedule_tick(
                self.peer.cfg.split_region_check_tick_interval.0,
                PeerTick::SplitRegionCheck,
            );
        }
    }

    fn on_split_region_check_tick(&mut self) {
        self.tick_tracker.remove(TickSchedulerTracker::SPLIT_CHECK);
        // TODO: avoid frequent scan.
        if !self.peer.is_leader() {
            return;
        }

        self.schedule_split_region_check_tick();
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
            return;
        }
        let task = SplitCheckTask::new(self.peer.region().clone(), true, CheckPolicy::SCAN);
        if let Err(e) = self.split_check_scheduler.schedule(task) {
            error!("{} failed to schedule split check: {}", self.peer.tag, e);
        }
        self.peer.size_diff_hint = 0;
        self.peer.compaction_declined_bytes = 0;
    }

    fn on_prepare_split_region(
        &mut self,
        region_epoch: metapb::RegionEpoch,
        split_keys: Vec<Vec<u8>>,
        cb: Callback,
    ) {
        if let Err(e) = self.validate_split_region(&region_epoch, &split_keys) {
            warn!("{} invalid split request: {:?}", self.peer.tag, e);
            cb.invoke_with_response(new_error(e));
            return;
        }
        let region = self.peer.region();
        let task = PdTask::AskBatchSplit {
            region: region.clone(),
            split_keys,
            peer: self.peer.peer.clone(),
            right_derive: self.peer.cfg.right_derive_when_split,
            callback: cb,
        };
        if let Err(Stopped(t)) = self.pd_scheduler.schedule(task) {
            error!("{} failed to notify pd to split: Stopped", self.peer.tag);
            match t {
                PdTask::AskBatchSplit { callback, .. } => {
                    callback.invoke_with_response(new_error(box_err!("failed to split: Stopped")));
                }
                _ => unreachable!(),
            }
        }
    }

    fn validate_split_region(
        &mut self,
        epoch: &metapb::RegionEpoch,
        split_keys: &[Vec<u8>],
    ) -> Result<()> {
        if split_keys.is_empty() {
            return Err(box_err!("no split key is specified."));
        }
        for key in split_keys {
            if key.is_empty() {
                return Err(box_err!("split key should not be empty"));
            }
        }
        if !self.peer.is_leader() {
            return Err(Error::NotLeader(
                self.region_id(),
                self.peer.get_peer_from_cache(self.peer.leader_id()),
            ));
        }

        let region = self.peer.region();
        let latest_epoch = region.get_region_epoch();

        if latest_epoch.get_version() != epoch.get_version() {
            return Err(Error::StaleEpoch(
                format!(
                    "epoch changed {:?} != {:?}, retry later",
                    latest_epoch, epoch
                ),
                vec![region.to_owned()],
            ));
        }
        Ok(())
    }

    fn on_schedule_half_split_region(
        &mut self,
        region_epoch: &metapb::RegionEpoch,
        policy: CheckPolicy,
    ) {
        if !self.peer.is_leader() {
            // region on this store is no longer leader, skipped.
            warn!(
                "{} region on {} is not leader, skip.",
                self.peer.tag,
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
        if let Err(e) = self.split_check_scheduler.schedule(task) {
            error!("{} failed to schedule split check: {}", self.peer.tag, e);
        }
    }

    fn on_pd_heartbeat_tick(&mut self) {
        self.tick_tracker.remove(TickSchedulerTracker::PD_HEARTBEAT);
        self.peer.check_peers();

        if !self.peer.is_leader() {
            return;
        }
        self.peer.heartbeat_pd(&self.pd_scheduler);

        self.schedule_pd_heartbeat_tick();

        // TODO: update leader and region metrics in time.
    }

    #[inline]
    fn schedule_pd_heartbeat_tick(&mut self) {
        if !self
            .tick_tracker
            .contains(TickSchedulerTracker::PD_HEARTBEAT)
        {
            self.tick_tracker.insert(TickSchedulerTracker::PD_HEARTBEAT);
            self.schedule_tick(
                self.peer.cfg.pd_heartbeat_tick_interval.0,
                PeerTick::PdHeartbeat,
            );
        }
    }

    fn on_check_peer_stale_state_tick(&mut self) {
        self.tick_tracker.remove(TickSchedulerTracker::STALE_PEER);
        let mut leader_missing = 0;
        if self.peer.pending_remove || self.peer.is_leader() {
            return;
        }
        self.schedule_check_peer_stale_state_tick();

        if self.peer.is_applying_snapshot() || self.peer.has_pending_snapshot() {
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
        let state = self.peer.check_stale_state();
        fail_point!("peer_check_stale_state", state != StaleState::Valid, |_| {});
        match state {
            StaleState::Valid => (),
            StaleState::LeaderMissing => {
                warn!(
                    "{} leader missing longer than abnormal_leader_missing_duration {:?}",
                    self.peer.tag, self.peer.cfg.abnormal_leader_missing_duration.0,
                );
                leader_missing += 1;
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
                    region: self.region().clone(),
                    merge_source: None,
                };
                if let Err(e) = self.pd_scheduler.schedule(task) {
                    error!("{} failed to notify pd: {}", self.peer.tag, e)
                }
            }
        }
        // TODO: fix this metrics.
        self.raft_metrics.leader_missing = leader_missing;
    }

    #[inline]
    fn schedule_check_peer_stale_state_tick(&mut self) {
        if !self.tick_tracker.contains(TickSchedulerTracker::STALE_PEER) {
            self.tick_tracker.insert(TickSchedulerTracker::STALE_PEER);
            self.schedule_tick(
                self.peer.cfg.peer_stale_state_check_interval.0,
                PeerTick::CheckPeerStaleState,
            )
        }
    }
}

// Consistency Check implementation.

impl<T: Transport> Peer<T> {
    /// Verify and store the hash to state. return true means the hash has been stored successfully.
    fn verify_and_store_hash(&mut self, expected_index: u64, expected_hash: Vec<u8>) -> bool {
        if expected_index < self.peer.consistency_state.index {
            REGION_HASH_COUNTER_VEC
                .with_label_values(&["verify", "miss"])
                .inc();
            warn!(
                "{} has scheduled a new hash: {} > {}, skip.",
                self.peer.tag, self.peer.consistency_state.index, expected_index
            );
            return false;
        }

        if self.peer.consistency_state.index == expected_index {
            if self.peer.consistency_state.hash.is_empty() {
                warn!(
                    "{} duplicated consistency check detected, skip.",
                    self.peer.tag
                );
                return false;
            }
            if self.peer.consistency_state.hash != expected_hash {
                panic!(
                    "{} hash at {} not correct, want \"{}\", got \"{}\"!!!",
                    self.peer.tag,
                    self.peer.consistency_state.index,
                    escape(&expected_hash),
                    escape(&self.peer.consistency_state.hash)
                );
            }
            info!(
                "{} consistency check at {} pass.",
                self.peer.tag, self.peer.consistency_state.index
            );
            REGION_HASH_COUNTER_VEC
                .with_label_values(&["verify", "matched"])
                .inc();
            self.peer.consistency_state.hash = vec![];
            return false;
        }

        if self.peer.consistency_state.index != INVALID_INDEX
            && !self.peer.consistency_state.hash.is_empty()
        {
            // Maybe computing is too slow or computed result is dropped due to channel full.
            // If computing is too slow, miss count will be increased twice.
            REGION_HASH_COUNTER_VEC
                .with_label_values(&["verify", "miss"])
                .inc();
            warn!(
                "{} hash belongs to index {}, but we want {}, skip.",
                self.peer.tag, self.peer.consistency_state.index, expected_index
            );
        }

        info!(
            "{} save hash of {} for consistency check later.",
            self.peer.tag, expected_index
        );
        self.peer.consistency_state.index = expected_index;
        self.peer.consistency_state.hash = expected_hash;
        true
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
        self.verify_and_store_hash(expected_index, expected_hash);
    }

    fn on_hash_computed(&mut self, index: u64, hash: Vec<u8>) {
        if !self.verify_and_store_hash(index, hash) {
            return;
        }

        let request = new_verify_hash_request(
            self.region_id(),
            self.peer.peer.clone(),
            &self.peer.consistency_state,
        );
        self.propose_raft_command(request, Callback::None);
    }

    fn on_ingest_sst_result(&mut self, ssts: Vec<SSTMeta>) {
        for sst in &ssts {
            self.peer.size_diff_hint += sst.get_length();
        }

        let task = CleanupSSTTask::DeleteSST { ssts };
        if let Err(e) = self.cleanup_sst_scheduler.schedule(task) {
            error!("{} schedule to delete ssts: {:?}", self.peer.tag, e);
        }
    }

    fn on_gc_snap(&mut self, keys: Vec<(SnapKey, bool)>) {
        let s = self.peer.get_store();
        let compacted_idx = s.truncated_index();
        let compacted_term = s.truncated_term();
        let is_applying_snap = s.is_applying_snapshot();
        for (key, is_sending) in keys {
            if is_sending {
                let s = match self.snap_mgr.get_snapshot_for_sending(&key) {
                    Ok(s) => s,
                    Err(e) => {
                        error!(
                            "{} failed to load snapshot for {}: {:?}",
                            self.peer.tag, key, e
                        );
                        continue;
                    }
                };
                if key.term < compacted_term || key.idx < compacted_idx {
                    info!(
                        "{} snap file {} has been compacted, delete.",
                        self.peer.tag, key
                    );
                    self.snap_mgr.delete_snapshot(&key, s.as_ref(), false);
                } else if let Ok(meta) = s.meta() {
                    let modified = match meta.modified() {
                        Ok(m) => m,
                        Err(e) => {
                            error!(
                                "{} failed to load snapshot for {}: {:?}",
                                self.peer.tag, key, e
                            );
                            continue;
                        }
                    };
                    if let Ok(elapsed) = modified.elapsed() {
                        if elapsed > self.peer.cfg.snap_gc_timeout.0 {
                            info!(
                                "{} snap file {} has been expired, delete.",
                                self.peer.tag, key
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
                let a = match self.snap_mgr.get_snapshot_for_applying(&key) {
                    Ok(a) => a,
                    Err(e) => {
                        error!(
                            "{} failed to load snapshot for {}: {:?}",
                            self.peer.tag, key, e
                        );
                        continue;
                    }
                };
                self.snap_mgr.delete_snapshot(&key, a.as_ref(), false);
            }
        }
    }
}

impl<T: Transport> Peer<T> {
    #[inline]
    fn on_tick(&mut self, tick: PeerTick) {
        match tick {
            PeerTick::Raft => self.on_raft_base_tick(),
            PeerTick::SplitRegionCheck => self.on_split_region_check_tick(),
            PeerTick::PdHeartbeat => self.on_pd_heartbeat_tick(),
            PeerTick::RaftLogGc => self.on_raft_gc_log_tick(),
            PeerTick::CheckMerge => self.on_check_merge(),
            PeerTick::CheckPeerStaleState => self.on_check_peer_stale_state_tick(),
        }
    }

    fn stop(&mut self) {
        self.stopped = true;
        self.peer.stop();
    }

    fn on_peer_msg(&mut self, msg: PeerMsg) {
        match msg {
            PeerMsg::RaftMessage(data) => if let Err(e) = self.on_raft_message(data) {
                error!("{} handle raft message err: {:?}", self.peer.tag, e);
            },
            PeerMsg::RaftCmd {
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
            PeerMsg::Tick(t) => self.on_tick(t),
            PeerMsg::ApplyRes(res) => self.on_apply_res(res),
            PeerMsg::SignificantMsg(m) => self.on_significant_msg(m),
            PeerMsg::ComputeHashResult { index, hash } => self.on_hash_computed(index, hash),
            // TODO: format keys
            PeerMsg::SplitRegion {
                region_epoch,
                split_keys,
                callback,
            } => {
                info!("{} on split region at key {:?}.", self.peer.tag, split_keys);
                self.on_prepare_split_region(region_epoch, split_keys, callback);
            }
            PeerMsg::RegionApproximateSize { size } => {
                self.peer.approximate_size = Some(size);
            }
            PeerMsg::RegionApproximateKeys { keys } => {
                self.peer.approximate_keys = Some(keys);
            }
            PeerMsg::CompactionDeclinedBytes(bytes) => {
                self.peer.compaction_declined_bytes += bytes;
                if self.peer.compaction_declined_bytes >= self.peer.cfg.region_split_check_diff.0 {
                    UPDATE_REGION_SIZE_BY_COMPACTION_COUNTER.inc();
                }
            }
            PeerMsg::GcSnap(keys) => self.on_gc_snap(keys),
            PeerMsg::HalfSplitRegion {
                region_epoch,
                policy,
            } => self.on_schedule_half_split_region(&region_epoch, policy),
            PeerMsg::MergeResult { target, stale } => self.on_merge_result(target, stale),
            PeerMsg::BatchRaftSnapCmds { .. } => unreachable!(),
        }
    }
}

impl<T> AsRef<peer::Peer> for Peer<T> {
    #[inline]
    fn as_ref(&self) -> &peer::Peer {
        &self.peer
    }
}

impl<T: Transport> Future for Peer<T> {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        if self.pending_apply.is_some() {
            match self.pending_apply.as_mut().unwrap().notifier.poll() {
                Ok(Async::Ready(())) | Err(_) => {}
                Ok(Async::NotReady) => return Ok(Async::NotReady),
            }
            if !self.resume_handling_pending_apply() {
                return Ok(Async::NotReady);
            }
        }
        let mut msgs;
        match self.receiver.poll() {
            Ok(Async::Ready(Some(m))) => {
                msgs = Vec::with_capacity(self.peer.cfg.messages_per_tick);
                msgs.push(m);
            }
            Ok(Async::NotReady) => return Ok(Async::NotReady),
            // stop implicitly.
            Ok(Async::Ready(None)) => {
                self.stop();
                return Ok(Async::Ready(()));
            }
            _ => unreachable!(),
        }
        loop {
            while msgs.len() < self.peer.cfg.messages_per_tick {
                match self.receiver.poll() {
                    Ok(Async::Ready(Some(m))) => msgs.push(m),
                    Ok(Async::NotReady) => break,
                    Ok(Async::Ready(None)) => {
                        self.stop();
                        return Ok(Async::Ready(()));
                    }
                    _ => unreachable!(),
                }
            }
            let keep_going = msgs.len() == self.peer.cfg.messages_per_tick;
            for m in msgs.drain(..) {
                self.on_peer_msg(m);
            }
            // TODO: should stopped be checked first?
            if self.has_ready {
                self.handle_raft_ready();
                self.has_ready = false;
            }
            if !self.stopped {
                if self.queued_snapshot {
                    let mut meta = self.store_meta.lock().unwrap();
                    meta.pending_snapshot_regions
                        .retain(|r| r.get_id() != self.region_id());
                }
                if keep_going {
                    continue;
                }
                if self.need_flush_trans {
                    self.trans.flush();
                    self.need_flush_trans = false;
                }
                return Ok(Async::NotReady);
            }
            return Ok(Async::Ready(()));
        }
    }
}

fn new_admin_request(region_id: u64, peer: metapb::Peer) -> RaftCmdRequest {
    let mut request = RaftCmdRequest::new();
    request.mut_header().set_region_id(region_id);
    request.mut_header().set_peer(peer);
    request
}

fn new_verify_hash_request(
    region_id: u64,
    peer: metapb::Peer,
    state: &ConsistencyState,
) -> RaftCmdRequest {
    let mut request = new_admin_request(region_id, peer);

    let mut admin = AdminRequest::new();
    admin.set_cmd_type(AdminCmdType::VerifyHash);
    admin.mut_verify_hash().set_index(state.index);
    admin.mut_verify_hash().set_hash(state.hash.clone());
    request.set_admin_request(admin);
    request
}

#[allow(dead_code)]
fn new_compute_hash_request(region_id: u64, peer: metapb::Peer) -> RaftCmdRequest {
    let mut request = new_admin_request(region_id, peer);

    let mut admin = AdminRequest::new();
    admin.set_cmd_type(AdminCmdType::ComputeHash);
    request.set_admin_request(admin);
    request
}

fn new_compact_log_request(
    region_id: u64,
    peer: metapb::Peer,
    compact_index: u64,
    compact_term: u64,
) -> RaftCmdRequest {
    let mut request = new_admin_request(region_id, peer);

    let mut admin = AdminRequest::new();
    admin.set_cmd_type(AdminCmdType::CompactLog);
    admin.mut_compact_log().set_compact_index(compact_index);
    admin.mut_compact_log().set_compact_term(compact_term);
    request.set_admin_request(admin);
    request
}

impl<T: Transport> Peer<T> {
    // Handle status commands here, separate the logic, maybe we can move it
    // to another file later.
    // Unlike other commands (write or admin), status commands only show current
    // store status, so no need to handle it in raft group.
    fn execute_status_command(&mut self, request: &RaftCmdRequest) -> Result<RaftCmdResponse> {
        let cmd_type = request.get_status_request().get_cmd_type();
        let mut response = match cmd_type {
            StatusCmdType::RegionLeader => self.execute_region_leader(request),
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

    fn execute_region_leader(&mut self, _: &RaftCmdRequest) -> Result<StatusResponse> {
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
}
