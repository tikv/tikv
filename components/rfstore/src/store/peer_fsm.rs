// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use bytes::Bytes;
use fail::fail_point;
use kvengine::ShardMeta;
use kvproto::metapb::{self, Region, RegionEpoch};
use kvproto::raft_cmdpb::{
    CmdType, RaftCmdRequest, RaftCmdResponse, RaftRequestHeader, Request, StatusCmdType,
    StatusResponse,
};
use kvproto::raft_serverpb::RaftMessage;
use raft;
use raft::eraftpb::{ConfChangeType, MessageType};
use raft_proto::eraftpb;
use rand::{thread_rng, Rng};
use std::collections::Bound::{Excluded, Unbounded};
use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Instant;
use std::{cmp, u64};
use tikv_util::{box_err, debug, error, info, trace, warn};

use crate::store::cmd_resp::{bind_term, new_error};
use crate::store::msg::Callback;
use crate::store::peer::Peer;
use crate::store::{notify_req_region_removed, CustomBuilder, PdTask, PersistReady};
use crate::store::{
    raw_end_key, ApplyMsg, Engines, MsgApplyResult, RaftContext, ReadDelegate, Ticker,
    PEER_TICK_PD_HEARTBEAT, PEER_TICK_RAFT, PEER_TICK_SPLIT_CHECK,
};
use crate::store::{util as _util, CasualMessage, Config, PeerMsg, SignificantMsg};
use crate::store::{write_peer_state, ChangePeer, ExecResult, MsgApplyChangeSetResult, SnapState};
use crate::{Error, Result};
use raftstore::coprocessor::RegionChangeEvent;
use raftstore::store::util;
use raftstore::store::util::is_region_initialized;
use txn_types::{Key, WriteBatchFlags};

/// Limits the maximum number of regions returned by error.
///
/// Another choice is using coprocessor batch limit, but 10 should be a good fit in most case.
const MAX_REGIONS_IN_ERROR: usize = 10;

pub struct PeerFsm {
    pub(crate) peer: Peer,
    pub(crate) stopped: bool,
    // apply_worker_idx is initialized randomly, and can be changed based on workload.
    pub(crate) apply_worker_idx: usize,
    // applying_cnt is increased by raft worker and decreased by apply worker.
    // When we need to change the worker idx, we need to make sure the applying_cnt is zero.
    pub(crate) applying_cnt: Arc<AtomicU64>,
    ticker: Ticker,
}

impl PeerFsm {
    // If we create the peer actively, like bootstrap/split/merge region, we should
    // use this function to create the peer. The region must contain the peer info
    // for this store.
    pub fn create(
        store_id: u64,
        cfg: &Config,
        engines: Engines,
        region: &metapb::Region,
    ) -> Result<PeerFsm> {
        let meta_peer = match util::find_peer(region, store_id) {
            None => {
                return Err(box_err!(
                    "find no peer for store {} in region {:?}",
                    store_id,
                    region
                ));
            }
            Some(peer) => peer.clone(),
        };

        info!(
            "create peer";
            "region_id" => region.get_id(),
            "peer_id" => meta_peer.get_id(),
        );
        Ok(PeerFsm {
            peer: Peer::new(store_id, cfg, engines, region, meta_peer)?,
            stopped: false,
            apply_worker_idx: thread_rng().gen_range(0..cfg.apply_pool_size),
            applying_cnt: Arc::new(AtomicU64::new(0)),
            ticker: Ticker::new(cfg),
        })
    }

    // The peer can be created from another node with raft membership changes, and we only
    // know the region_id and peer_id when creating this replicated peer, the region info
    // will be retrieved later after applying snapshot.
    pub fn replicate(
        store_id: u64,
        cfg: &Config,
        engines: Engines,
        region_id: u64,
        peer: metapb::Peer,
    ) -> Result<PeerFsm> {
        // We will remove tombstone key when apply snapshot
        info!(
            "replicate peer";
            "region_id" => region_id,
            "peer_id" => peer.get_id(),
        );

        let mut region = metapb::Region::default();
        region.set_id(region_id);
        Ok(PeerFsm {
            peer: Peer::new(store_id, cfg, engines, &region, peer)?,
            stopped: false,
            ticker: Ticker::new(cfg),
            apply_worker_idx: thread_rng().gen_range(0..cfg.apply_pool_size),
            applying_cnt: Arc::new(AtomicU64::new(0)),
        })
    }

    #[inline]
    pub fn region_id(&self) -> u64 {
        self.peer.region().get_id()
    }

    #[inline]
    pub(crate) fn get_peer(&self) -> &Peer {
        &self.peer
    }

    #[inline]
    pub fn peer_id(&self) -> u64 {
        self.peer.peer_id()
    }

    #[inline]
    pub fn stop(&mut self) {
        self.stopped = true;
    }
}

pub(crate) struct PeerMsgHandler<'a> {
    fsm: &'a mut PeerFsm,
    pub(crate) ctx: &'a mut RaftContext,
}

impl<'a> Deref for PeerMsgHandler<'a> {
    type Target = PeerFsm;

    fn deref(&self) -> &Self::Target {
        self.fsm
    }
}

impl<'a> DerefMut for PeerMsgHandler<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.fsm
    }
}

impl<'a> PeerMsgHandler<'a> {
    pub(crate) fn new(fsm: &'a mut PeerFsm, ctx: &'a mut RaftContext) -> PeerMsgHandler<'a> {
        PeerMsgHandler { fsm, ctx }
    }

    pub fn handle_msgs(&mut self, msgs: &mut Vec<PeerMsg>) {
        for m in msgs.drain(..) {
            match m {
                PeerMsg::RaftMessage(msg) => {
                    if let Err(e) = self.on_raft_message(msg) {
                        error!(%e;
                            "handle raft message err";
                            "region_id" => self.fsm.region_id(),
                            "peer_id" => self.fsm.peer_id(),
                        );
                    }
                }
                PeerMsg::RaftCommand(cmd) => {
                    self.propose_raft_command(cmd.request, cmd.callback);
                }
                PeerMsg::Tick => self.on_tick(),
                PeerMsg::ApplyResult(res) => {
                    self.on_apply_result(res);
                }
                PeerMsg::SignificantMsg(msg) => self.on_significant_msg(msg),
                PeerMsg::CasualMessage(msg) => self.on_casual_msg(msg),
                PeerMsg::Start => self.start(),
                PeerMsg::GenerateEngineChangeSet(cs) => self.on_generate_engine_change_set(cs),
                PeerMsg::ApplyChangeSetResult(res) => {
                    self.on_apply_change_set_result(res);
                }
                PeerMsg::Persisted(ready) => {
                    self.on_persisted(ready);
                }
            }
        }
    }

    fn on_casual_msg(&mut self, msg: CasualMessage) {
        match msg {
            CasualMessage::SplitRegion {
                region_epoch,
                split_keys,
                callback,
                source,
            } => {
                self.on_prepare_split_region(region_epoch, split_keys, callback, &source);
            }
            CasualMessage::HalfSplitRegion {
                region_epoch: _,
                policy: _,
                source: _,
            } => {
                // TODO(x) handle half split region;
                warn!("ignore half split region");
            }
        }
    }

    fn on_tick(&mut self) {
        if self.fsm.stopped {
            return;
        }
        trace!(
            "tick";
            "peer_id" => self.fsm.peer_id(),
            "region_id" => self.region_id(),
        );
        self.ticker.tick_clock();
        if self.ticker.is_on_tick(PEER_TICK_RAFT) {
            self.on_raft_base_tick();
        }
        if self.ticker.is_on_tick(PEER_TICK_PD_HEARTBEAT) {
            self.on_pd_heartbeat_tick();
        }
        if self.ticker.is_on_tick(PEER_TICK_SPLIT_CHECK) {
            self.on_split_region_check_tick();
        }
    }

    fn start(&mut self) {
        self.ticker.schedule(PEER_TICK_RAFT);
        self.ticker.schedule(PEER_TICK_PD_HEARTBEAT);
        self.ticker.schedule(PEER_TICK_SPLIT_CHECK);
    }

    fn on_significant_msg(&mut self, msg: SignificantMsg) {
        match msg {
            SignificantMsg::Unreachable { to_peer_id, .. } => {
                if self.fsm.peer.is_leader() {
                    self.fsm.peer.raft_group.report_unreachable(to_peer_id);
                }
            }
            SignificantMsg::StoreUnreachable { store_id } => {
                if let Some(peer_id) = util::find_peer(self.region(), store_id).map(|p| p.get_id())
                {
                    if self.fsm.peer.is_leader() {
                        self.fsm.peer.raft_group.report_unreachable(peer_id);
                    }
                }
            }
        }
    }

    #[inline]
    fn region_id(&self) -> u64 {
        self.fsm.peer.region().get_id()
    }

    #[inline]
    fn region(&self) -> &Region {
        self.fsm.peer.region()
    }

    #[inline]
    fn store_id(&self) -> u64 {
        self.fsm.peer.peer.get_store_id()
    }

    fn on_raft_base_tick(&mut self) {
        if self.peer.pending_remove {
            self.peer.mut_store().flush_cache_metrics();
            return;
        }
        self.ticker.schedule(PEER_TICK_RAFT);
        // When having pending snapshot, if election timeout is met, it can't pass
        // the pending conf change check because first index has been updated to
        // a value that is larger than last index.
        if self.fsm.peer.is_applying_snapshot() || self.fsm.peer.has_pending_snapshot() {
            // need to check if snapshot is applied.
            return;
        }
        self.fsm.peer.raft_group.tick();
        self.fsm.peer.mut_store().flush_cache_metrics();
    }

    fn on_apply_result(&mut self, mut res: MsgApplyResult) {
        fail_point!("on_apply_res", |_| {});
        debug!(
            "async apply finish";
            "region_id" => self.region_id(),
            "peer_id" => self.fsm.peer_id(),
            "res" => ?res,
        );
        self.fsm.peer.post_apply(self.ctx, &res);
        self.on_ready_result(&mut res.results);
        if self.fsm.stopped {
            return;
        }
    }

    fn on_transfer_leader_msg(&mut self, msg: &eraftpb::Message) {
        // log_term is set by original leader, represents the term last log is written
        // in, which should be equal to the original leader's term.
        if msg.get_log_term() != self.fsm.peer.term() {
            return;
        }
        if self.fsm.peer.is_leader() {
            let from = match self.fsm.peer.get_peer_from_cache(msg.get_from()) {
                Some(p) => p,
                None => return,
            };
            match self
                .fsm
                .peer
                .ready_to_transfer_leader(&mut self.ctx, msg.get_index(), &from)
            {
                Some(reason) => {
                    info!(
                        "reject to transfer leader";
                        "region_id" => self.fsm.region_id(),
                        "peer_id" => self.fsm.peer_id(),
                        "to" => ?from,
                        "reason" => reason,
                        "index" => msg.get_index(),
                        "last_index" => self.fsm.peer.get_store().last_index(),
                    );
                }
                None => {
                    self.fsm.peer.transfer_leader(&from);
                }
            }
        } else {
            self.fsm.peer.execute_transfer_leader(&mut self.ctx, msg);
        }
    }

    fn on_raft_message(&mut self, mut msg: RaftMessage) -> Result<()> {
        debug!(
            "handle raft message";
            "region_id" => self.region_id(),
            "peer_id" => self.fsm.peer_id(),
            "message_type" => %util::MsgType(&msg),
            "from_peer_id" => msg.get_from_peer().get_id(),
            "to_peer_id" => msg.get_to_peer().get_id(),
        );

        if !self.validate_raft_msg(&msg) {
            return Ok(());
        }
        if self.fsm.peer.pending_remove || self.fsm.stopped {
            return Ok(());
        }

        if msg.get_is_tombstone() {
            // we receive a message tells us to remove ourself.
            self.handle_gc_peer_msg(&msg);
            return Ok(());
        }

        if self.check_msg(&msg) {
            return Ok(());
        }

        if msg.has_extra_msg() {
            self.on_extra_message(msg);
            return Ok(());
        }

        let is_snapshot = msg.get_message().has_snapshot();
        let regions_to_destroy = self.check_snapshot(&msg)?;

        let from_peer_id = msg.get_from_peer().get_id();
        self.fsm.peer.insert_peer_cache(msg.take_from_peer());

        let result = if msg.get_message().get_msg_type() == MessageType::MsgTransferLeader {
            self.on_transfer_leader_msg(msg.get_message());
            Ok(())
        } else {
            self.fsm.peer.step(msg.take_message())
        };

        if is_snapshot {
            if self.fsm.peer.has_pending_snapshot() {
                self.destroy_regions_for_snapshot(regions_to_destroy);
            }
        }

        if result.is_err() {
            return result;
        }
        Ok(())
    }

    fn on_extra_message(&mut self, _msg: RaftMessage) {
        // TODO(x)
    }

    // return false means the message is invalid, and can be ignored.
    fn validate_raft_msg(&mut self, msg: &RaftMessage) -> bool {
        let region_id = msg.get_region_id();
        let to = msg.get_to_peer();

        if to.get_store_id() != self.store_id() {
            warn!(
                "store not match, ignore it";
                "region_id" => region_id,
                "to_store_id" => to.get_store_id(),
                "my_store_id" => self.store_id(),
            );
            self.ctx.raft_metrics.message_dropped.mismatch_store_id += 1;
            return false;
        }

        if !msg.has_region_epoch() {
            error!(
                "missing epoch in raft message, ignore it";
                "region_id" => region_id,
            );
            self.ctx.raft_metrics.message_dropped.mismatch_region_epoch += 1;
            return false;
        }

        true
    }

    /// Checks if the message is sent to the correct peer.
    ///
    /// Returns true means that the message can be dropped silently.
    fn check_msg(&mut self, msg: &RaftMessage) -> bool {
        let from_epoch = msg.get_region_epoch();
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
        if util::is_epoch_stale(from_epoch, self.fsm.peer.region().get_region_epoch())
            && util::find_peer(self.fsm.peer.region(), from_store_id).is_none()
        {
            self.ctx
                .handle_stale_msg(msg, self.fsm.peer.region().get_region_epoch().clone(), None);
            return true;
        }

        let target = msg.get_to_peer();
        match target.get_id().cmp(&self.fsm.peer.peer_id()) {
            cmp::Ordering::Less => {
                info!(
                    "target peer id is smaller, msg maybe stale";
                    "region_id" => self.fsm.region_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "target_peer" => ?target,
                );
                self.ctx.raft_metrics.message_dropped.stale_msg += 1;
                true
            }
            cmp::Ordering::Greater => {
                if self.fsm.peer.maybe_destroy() {
                    self.ctx.apply_msgs.msgs.push(ApplyMsg::UnsafeDestroy {
                        region_id: self.region_id(),
                    });
                } else {
                    self.ctx.raft_metrics.message_dropped.applying_snap += 1;
                }
                true
            }
            cmp::Ordering::Equal => false,
        }
    }

    fn handle_gc_peer_msg(&mut self, msg: &RaftMessage) {
        let from_epoch = msg.get_region_epoch();
        if !util::is_epoch_stale(self.fsm.peer.region().get_region_epoch(), from_epoch) {
            return;
        }

        if self.fsm.peer.peer != *msg.get_to_peer() {
            info!(
                "receive stale gc message, ignore.";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
            );
            return;
        }
        // TODO: ask pd to guarantee we are stale now.
        info!(
            "receives gc message, trying to remove";
            "region_id" => self.fsm.region_id(),
            "peer_id" => self.fsm.peer_id(),
            "to_peer" => ?msg.get_to_peer(),
        );
        if self.fsm.peer.maybe_destroy() {
            // Destroy the apply fsm first, wait for the reply msg from apply fsm
            self.ctx.apply_msgs.msgs.push(ApplyMsg::UnsafeDestroy {
                region_id: self.region_id(),
            });
        } else {
            self.ctx.raft_metrics.message_dropped.applying_snap += 1;
        }
    }

    // Returns `Vec<(u64, bool)>` indicated (source_region_id, merge_to_this_peer) if the `msg`
    // doesn't contain a snapshot or this snapshot doesn't conflict with any other snapshots or regions.
    // Otherwise a `SnapKey` is returned.
    fn check_snapshot(&mut self, msg: &RaftMessage) -> Result<Vec<(u64, bool)>> {
        if !msg.get_message().has_snapshot() {
            return Ok(vec![]);
        }
        // TODO(x)
        return Ok(vec![]);
    }

    fn destroy_regions_for_snapshot(&mut self, regions_to_destroy: Vec<(u64, bool)>) {
        if regions_to_destroy.is_empty() {
            return;
        }
        // TODO(x)
    }

    fn destroy_peer(&mut self, keep_data: bool) {
        fail_point!("destroy_peer");
        info!(
            "starts destroy";
            "region_id" => self.fsm.region_id(),
            "peer_id" => self.fsm.peer_id(),
        );
        let region_id = self.region_id();
        // We can't destroy a peer which is applying snapshot.
        assert!(!self.fsm.peer.is_applying_snapshot());

        // Mark itself as pending_remove
        self.fsm.peer.pending_remove = true;
        if let Some(parent_id) = self.peer.mut_store().parent_id() {
            self.ctx
                .global
                .engines
                .raft
                .remove_dependent(parent_id, self.region_id());
        }

        let mut meta = self.ctx.global.store_meta.lock().unwrap();

        // Destroy read delegates.
        meta.readers.remove(&region_id);

        // Trigger region change observer
        self.ctx.global.coprocessor_host.on_region_changed(
            self.fsm.peer.region(),
            RegionChangeEvent::Destroy,
            self.fsm.peer.get_role(),
        );
        let task = PdTask::DestroyPeer { region_id };
        if let Err(e) = self.ctx.global.pd_scheduler.schedule(task) {
            error!(
                "failed to notify pd";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "err" => %e,
            );
        }
        let is_initialized = self.fsm.peer.is_initialized();
        if let Err(e) = self.fsm.peer.destroy(&mut self.ctx.raft_wb) {
            // If not panic here, the peer will be recreated in the next restart,
            // then it will be gc again. But if some overlap region is created
            // before restarting, the gc action will delete the overlap region's
            // data too.
            panic!("{} destroy err {:?}", self.fsm.peer.tag(), e);
        }
        // Some places use `force_send().unwrap()` if the StoreMeta lock is held.
        // So in here, it's necessary to held the StoreMeta lock when closing the router.
        self.ctx.global.router.close(region_id);
        self.fsm.stop();

        if is_initialized
            && !keep_data
            && meta
                .region_ranges
                .remove(&raw_end_key(self.fsm.peer.region()))
                .is_none()
        {
            panic!("{} meta corruption detected", self.fsm.peer.tag());
        }
        if meta.regions.remove(&region_id).is_none() && !keep_data {
            panic!("{} meta corruption detected", self.fsm.peer.tag())
        }
        meta.leaders.remove(&region_id);
        self.ctx.global.engines.kv.remove_shard(region_id);
    }

    // Update some region infos
    fn update_region(&mut self, mut region: metapb::Region) {
        {
            let mut meta = self.ctx.global.store_meta.lock().unwrap();
            meta.set_region(
                &self.ctx.global.coprocessor_host,
                region.clone(),
                &mut self.fsm.peer,
            );
        }
        write_peer_state(&mut self.ctx.raft_wb, &region);
        for peer in region.take_peers().into_iter() {
            if self.fsm.peer.peer_id() == peer.get_id() {
                self.fsm.peer.peer = peer.clone();
            }
            self.fsm.peer.insert_peer_cache(peer);
        }
    }

    fn on_ready_change_peer(&mut self, cp: ChangePeer) {
        if cp.index == raft::INVALID_INDEX {
            // Apply failed, skip.
            return;
        }

        if cp.index >= self.fsm.peer.raft_group.raft.raft_log.first_index() {
            match self.fsm.peer.raft_group.apply_conf_change(&cp.conf_change) {
                Ok(_) => {}
                // PD could dispatch redundant conf changes.
                Err(raft::Error::NotExists { .. }) | Err(raft::Error::Exists { .. }) => {}
                _ => unreachable!(),
            }
        } else {
            // Please take a look at test case test_redundant_conf_change_by_snapshot.
        }

        self.update_region(cp.region);

        fail_point!("change_peer_after_update_region");

        let now = Instant::now();
        let (mut remove_self, mut need_ping) = (false, false);
        for mut change in cp.changes {
            let (change_type, peer) = (change.get_change_type(), change.take_peer());
            let (store_id, peer_id) = (peer.get_store_id(), peer.get_id());
            match change_type {
                ConfChangeType::AddNode | ConfChangeType::AddLearnerNode => {
                    // Add this peer to peer_heartbeats.
                    self.fsm.peer.peer_heartbeats.insert(peer_id, now);
                    if self.fsm.peer.is_leader() {
                        need_ping = true;
                        self.fsm.peer.peers_start_pending_time.push((peer_id, now));
                    }
                }
                ConfChangeType::RemoveNode => {
                    // Remove this peer from cache.
                    self.fsm.peer.peer_heartbeats.remove(&peer_id);
                    if self.fsm.peer.is_leader() {
                        self.fsm
                            .peer
                            .peers_start_pending_time
                            .retain(|&(p, _)| p != peer_id);
                    }
                    self.fsm.peer.remove_peer_from_cache(peer_id);
                    // We only care remove itself now.
                    if self.store_id() == store_id {
                        if self.fsm.peer.peer_id() == peer_id {
                            remove_self = true;
                        } else {
                            panic!(
                                "{} trying to remove unknown peer {:?}",
                                self.fsm.peer.tag(),
                                peer
                            );
                        }
                    }
                }
            }
        }

        // In pattern matching above, if the peer is the leader,
        // it will push the change peer into `peers_start_pending_time`
        // without checking if it is duplicated. We move `heartbeat_pd` here
        // to utilize `collect_pending_peers` in `heartbeat_pd` to avoid
        // adding the redundant peer.
        if self.fsm.peer.is_leader() {
            // Notify pd immediately.
            info!(
                "notify pd with change peer region";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "region" => ?self.fsm.peer.region(),
            );
            self.fsm.peer.heartbeat_pd(&self.ctx);

            // Remove or demote leader will cause this raft group unavailable
            // until new leader elected, but we can't revert this operation
            // because its result is already persisted in apply worker
            // TODO: should we transfer leader here?
            let demote_self = util::is_learner(&self.fsm.peer.peer);
            if remove_self || demote_self {
                warn!(
                    "Removing or demoting leader";
                    "region_id" => self.fsm.region_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "remove" => remove_self,
                    "demote" => demote_self,
                );
                if demote_self {
                    self.fsm
                        .peer
                        .raft_group
                        .raft
                        .become_follower(self.fsm.peer.term(), raft::INVALID_ID);
                }
                // Don't ping to speed up leader election
                need_ping = false;
            }
        }
        if need_ping {
            // Speed up snapshot instead of waiting another heartbeat.
            self.fsm.peer.ping();
        }
        if remove_self {
            self.destroy_peer(false);
        }
    }

    fn on_ready_split_region(&mut self, regions: Vec<metapb::Region>) {
        fail_point!("on_split", self.ctx.store_id() == 3, |_| {});
        let derived = regions.last().unwrap().clone();
        let region_id = derived.get_id();
        let mut meta = self.ctx.global.store_meta.lock().unwrap();
        meta.set_region(
            &self.ctx.global.coprocessor_host,
            derived,
            &mut self.fsm.peer,
        );

        let is_leader = self.fsm.peer.is_leader();
        if is_leader {
            self.fsm.peer.heartbeat_pd(&self.ctx);
            // Notify pd immediately to let it update the region meta.
            info!(
                "notify pd with split";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "split_count" => regions.len(),
            );
            // Now pd only uses ReportBatchSplit for history operation show,
            // so we send it independently here.
            let task = PdTask::ReportBatchSplit {
                regions: regions.to_vec(),
            };
            if let Err(e) = self.ctx.global.pd_scheduler.schedule(task) {
                error!(
                    "failed to notify pd";
                    "region_id" => self.fsm.region_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "err" => %e,
                );
            }
        }

        let last_key = raw_end_key(regions.last().unwrap());
        if meta.region_ranges.remove(&last_key).is_none() {
            panic!("{} original region should exist", self.fsm.peer.tag());
        }
        for new_region in regions {
            let new_region_id = new_region.get_id();

            if new_region_id == region_id {
                let not_exist = meta
                    .region_ranges
                    .insert(raw_end_key(&new_region), new_region_id)
                    .is_none();
                assert!(not_exist, "[region {}] should not exist", new_region_id);
                continue;
            }
            if let Some(r) = meta.regions.get(&new_region_id) {
                if is_region_initialized(r) {
                    // The region is created by raft message.
                    info!("initialized region already exists, must be created by raft message.");
                    continue;
                }
            }
            // Now all checking passed.
            // Insert new regions and validation
            info!(
                "insert new region";
                "region_id" => new_region_id,
                "region" => ?new_region,
            );
            meta.pending_new_regions.remove(&new_region_id);

            let mut new_peer = match PeerFsm::create(
                self.ctx.store_id(),
                &self.ctx.cfg,
                self.ctx.global.engines.clone(),
                &new_region,
            ) {
                Ok(new_peer) => new_peer,
                Err(e) => {
                    // peer information is already written into db, can't recover.
                    // there is probably a bug.
                    panic!("create new split region {:?} err {:?}", new_region, e);
                }
            };

            let meta_peer = new_peer.peer.peer.clone();

            for p in new_region.get_peers() {
                // Add this peer to cache.
                new_peer.peer.insert_peer_cache(p.clone());
            }

            // New peer derive write flow from parent region,
            // this will be used by balance write flow.
            new_peer.peer.peer_stat = self.fsm.peer.peer_stat.clone();
            let campaigned = new_peer.peer.maybe_campaign(is_leader);

            if is_leader {
                // The new peer is likely to become leader, send a heartbeat immediately to reduce
                // client query miss.
                new_peer.peer.heartbeat_pd(&self.ctx);
            }
            self.ctx.global.coprocessor_host.on_region_changed(
                &new_region,
                RegionChangeEvent::Create,
                new_peer.peer.get_role(),
            );
            meta.regions.insert(new_region_id, new_region.clone());
            let not_exist = meta
                .region_ranges
                .insert(raw_end_key(&new_region), new_region_id)
                .is_none();
            assert!(not_exist, "[region {}] should not exist", new_region_id);
            meta.readers
                .insert(new_region_id, ReadDelegate::from_peer(new_peer.get_peer()));
            self.ctx.global.router.register(new_peer);
            self.ctx
                .global
                .router
                .send(new_region_id, PeerMsg::Start)
                .unwrap();

            if !campaigned {
                if let Some(msg) = meta
                    .pending_msgs
                    .swap_remove_front(|m| m.get_to_peer() == &meta_peer)
                {
                    if let Err(e) = self
                        .ctx
                        .global
                        .router
                        .send(new_region_id, PeerMsg::RaftMessage(msg))
                    {
                        warn!("handle first requset failed"; "region_id" => region_id, "error" => ?e);
                    }
                }
            }
        }
        drop(meta);
        self.peer.mut_store().reset_meta();
        if is_leader {
            if let Some(shard) = self.ctx.global.engines.kv.get_shard(self.region_id()) {
                self.ctx.global.engines.kv.trigger_flush(&shard);
            }
        }
        fail_point!("after_split", self.ctx.store_id() == 3, |_| {});
    }

    fn on_ready_result(&mut self, exec_results: &mut VecDeque<ExecResult>) {
        // handle executing committed log results
        while let Some(result) = exec_results.pop_front() {
            match result {
                ExecResult::ChangePeer(cp) => self.on_ready_change_peer(cp),
                ExecResult::SplitRegion { regions } => self.on_ready_split_region(regions),
                ExecResult::DeleteRange { .. } => {
                    // TODO: clean user properties?
                }
                ExecResult::UnsafeDestroy => {
                    self.destroy_peer(false);
                }
            }
        }
        // TODO(x) update metrics.
    }

    fn pre_propose_raft_command(
        &mut self,
        msg: &RaftCmdRequest,
    ) -> Result<Option<RaftCmdResponse>> {
        // Check store_id, make sure that the msg is dispatched to the right place.
        if let Err(e) = _util::check_store_id(msg, self.store_id()) {
            self.ctx.raft_metrics.invalid_proposal.mismatch_store_id += 1;
            return Err(e);
        }
        if msg.has_status_request() {
            // For status commands, we handle it here directly.
            let resp = self.execute_status_command(msg)?;
            return Ok(Some(resp));
        }

        // Check whether the store has the right peer to handle the request.
        let region_id = self.region_id();
        let leader_id = self.fsm.peer.leader_id();
        let request = msg.get_requests();

        // ReadIndex can be processed on the replicas.
        let is_read_index_request =
            request.len() == 1 && request[0].get_cmd_type() == CmdType::ReadIndex;
        let mut read_only = true;
        for r in msg.get_requests() {
            match r.get_cmd_type() {
                CmdType::Get | CmdType::Snap | CmdType::ReadIndex => (),
                _ => read_only = false,
            }
        }
        let allow_replica_read = read_only && msg.get_header().get_replica_read();
        let flags = WriteBatchFlags::from_bits_check(msg.get_header().get_flags());
        let allow_stale_read = read_only && flags.contains(WriteBatchFlags::STALE_READ);
        if !self.fsm.peer.is_leader()
            && !is_read_index_request
            && !allow_replica_read
            && !allow_stale_read
        {
            self.ctx.raft_metrics.invalid_proposal.not_leader += 1;
            let leader = self.fsm.peer.get_peer_from_cache(leader_id);
            return Err(Error::NotLeader(region_id, leader));
        }
        // peer_id must be the same as peer's.
        if let Err(e) = _util::check_peer_id(msg, self.fsm.peer.peer_id()) {
            self.ctx.raft_metrics.invalid_proposal.mismatch_peer_id += 1;
            return Err(e);
        }
        // check whether the peer is initialized.
        if !self.fsm.peer.is_initialized() {
            self.ctx
                .raft_metrics
                .invalid_proposal
                .region_not_initialized += 1;
            return Err(Error::RegionNotInitialized(region_id));
        }
        // If the peer is applying snapshot, it may drop some sending messages, that could
        // make clients wait for response until timeout.
        if self.fsm.peer.is_applying_snapshot() {
            self.ctx.raft_metrics.invalid_proposal.is_applying_snapshot += 1;
            // TODO: replace to a more suitable error.
            return Err(Error::Other(box_err!(
                "{} peer is applying snapshot",
                self.fsm.peer.tag()
            )));
        }
        // Check whether the term is stale.
        if let Err(e) = _util::check_term(msg, self.fsm.peer.term()) {
            self.ctx.raft_metrics.invalid_proposal.stale_command += 1;
            return Err(e);
        }

        match _util::check_region_epoch(msg, self.fsm.peer.region(), true) {
            Err(Error::EpochNotMatch(m, mut new_regions)) => {
                // Attach the region which might be split from the current region. But it doesn't
                // matter if the region is not split from the current region. If the region meta
                // received by the TiKV driver is newer than the meta cached in the driver, the meta is
                // updated.
                let requested_version = msg.get_header().get_region_epoch().get_version();
                self.collect_sibling_region(requested_version, &mut new_regions);
                self.ctx.raft_metrics.invalid_proposal.epoch_not_match += 1;
                Err(Error::EpochNotMatch(m, new_regions))
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
                debug!(
                    "failed to propose";
                    "region_id" => self.region_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "message" => ?msg,
                    "err" => %e,
                );
                cb.invoke_with_response(new_error(e));
                return;
            }
            _ => (),
        }

        if self.fsm.peer.pending_remove {
            notify_req_region_removed(self.region_id(), cb);
            return;
        }

        // Note:
        // The peer that is being checked is a leader. It might step down to be a follower later. It
        // doesn't matter whether the peer is a leader or not. If it's not a leader, the proposing
        // command log entry can't be committed.

        let mut resp = RaftCmdResponse::default();
        let term = self.fsm.peer.term();
        bind_term(&mut resp, term);
        self.fsm.peer.propose(self.ctx, cb, msg, resp);

        // TODO: add timeout, if the command is not applied after timeout,
        // we will call the callback with timeout error.
    }

    fn collect_sibling_region(&self, requested_version: u64, regions: &mut Vec<Region>) {
        let mut max_version = self.fsm.peer.region().get_region_epoch().version;
        if requested_version >= max_version {
            // Our information is stale.
            return;
        }
        // Current region is included in the vec.
        let mut collect_cnt = max_version - requested_version;
        let anchor = Excluded(raw_end_key(self.fsm.peer.region()));
        let meta = self.ctx.global.store_meta.lock().unwrap();
        let mut ranges = meta.region_ranges.range((Unbounded::<Bytes>, anchor));

        for _ in 0..MAX_REGIONS_IN_ERROR {
            let res = ranges.next_back();
            if let Some((_, id)) = res {
                let r = &meta.regions[id];
                collect_cnt -= 1;
                // For example, A is split into B, A, and then B is split into C, B.
                if r.get_region_epoch().version >= max_version {
                    // It doesn't matter if it's a false positive, as it's limited by MAX_REGIONS_IN_ERROR.
                    collect_cnt += r.get_region_epoch().version - max_version;
                    max_version = r.get_region_epoch().version;
                }
                regions.push(r.to_owned());
                if collect_cnt == 0 {
                    return;
                }
            } else {
                return;
            }
        }
    }

    fn on_split_region_check_tick(&mut self) {
        self.ticker.schedule(PEER_TICK_SPLIT_CHECK);
        if let Some(shard) = self.ctx.global.engines.kv.get_shard(self.region_id()) {
            self.peer.peer_stat.approximate_size = shard.get_estimated_size();
            if !self.fsm.peer.is_leader() {
                return;
            }
            if !shard.get_initial_flushed() {
                return;
            }
            let region_max_size = self.ctx.cfg.region_max_size.0;
            let region_split_size = self.ctx.cfg.region_split_size.0;
            let estimated_size = shard.get_estimated_size();
            if estimated_size < region_max_size {
                return;
            }
            info!(
                "region {} estimated size {} is greater than region max size {}, split size is {}",
                self.peer.tag(),
                estimated_size,
                region_max_size,
                region_split_size,
            );
            let raw_keys = shard.get_suggest_split_keys(region_split_size);
            let encoded_split_keys = raw_keys
                .iter()
                .map(|k| {
                    let key = Key::from_raw(k);
                    key.as_encoded().to_vec()
                })
                .collect();
            let task = PdTask::AskBatchSplit {
                region: self.region().clone(),
                split_keys: encoded_split_keys,
                peer: self.peer.peer.clone(),
                right_derive: true,
                callback: Callback::None,
            };
            self.ctx.global.pd_scheduler.schedule(task).unwrap();
        }
    }

    fn on_prepare_split_region(
        &mut self,
        region_epoch: metapb::RegionEpoch,
        split_keys: Vec<Vec<u8>>,
        cb: Callback,
        source: &str,
    ) {
        if let Err(e) = self.validate_split_region(&region_epoch, &split_keys) {
            info!(
                "prepare split error";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "split_keys" => %util::KeysInfoFormatter(split_keys.iter()),
                "source" => source,
                "error" => ?e,
            );
            cb.invoke_with_response(new_error(e));
            return;
        }
        info!(
            "on split";
            "region_id" => self.fsm.region_id(),
            "peer_id" => self.fsm.peer_id(),
            "split_keys" => %util::KeysInfoFormatter(split_keys.iter()),
            "source" => source,
        );
        let task = PdTask::AskBatchSplit {
            region: self.region().clone(),
            split_keys,
            peer: self.peer.peer.clone(),
            right_derive: true,
            callback: cb,
        };
        self.ctx.global.pd_scheduler.schedule(task).unwrap();
    }

    fn validate_split_region(
        &mut self,
        epoch: &metapb::RegionEpoch,
        split_keys: &[Vec<u8>],
    ) -> Result<()> {
        if split_keys.is_empty() {
            error!(
                "no split key is specified.";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
            );
            return Err(box_err!(
                "{} no split key is specified.",
                self.fsm.peer.tag()
            ));
        }
        for key in split_keys {
            if key.is_empty() {
                error!(
                    "split key should not be empty!!!";
                    "region_id" => self.fsm.region_id(),
                    "peer_id" => self.fsm.peer_id(),
                );
                return Err(box_err!(
                    "{} split key should not be empty",
                    self.fsm.peer.tag()
                ));
            }
        }
        if !self.fsm.peer.is_leader() {
            // region on this store is no longer leader, skipped.
            info!(
                "not leader, skip.";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
            );
            return Err(Error::NotLeader(
                self.region_id(),
                self.fsm.peer.get_peer_from_cache(self.fsm.peer.leader_id()),
            ));
        }

        let region = self.fsm.peer.region();
        let latest_epoch = region.get_region_epoch();

        // This is a little difference for `check_region_epoch` in region split case.
        // Here we just need to check `version` because `conf_ver` will be update
        // to the latest value of the peer, and then send to PD.
        if latest_epoch.get_version() != epoch.get_version() {
            info!(
                "epoch changed, retry later";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "prev_epoch" => ?region.get_region_epoch(),
                "epoch" => ?epoch,
            );
            return Err(Error::EpochNotMatch(
                format!(
                    "{} epoch changed {:?} != {:?}, retry later",
                    self.fsm.peer.tag(),
                    latest_epoch,
                    epoch
                ),
                vec![region.to_owned()],
            ));
        }
        if !self.peer.get_store().initial_flushed {
            return Err(Error::RegionNotInitialized(self.region_id()));
        }
        Ok(())
    }

    fn on_pd_heartbeat_tick(&mut self) {
        self.ticker.schedule(PEER_TICK_PD_HEARTBEAT);
        self.fsm.peer.check_peers();

        if !self.fsm.peer.is_leader() {
            return;
        }
        self.fsm.peer.heartbeat_pd(&self.ctx);
    }

    fn on_generate_engine_change_set(&mut self, cs: kvenginepb::ChangeSet) {
        let tag = self.peer.tag();
        info!("generate meta change event {:?}", &cs; "region" => tag);
        self.propose_change_set(cs);
    }

    fn propose_change_set(&mut self, cs: kvenginepb::ChangeSet) {
        let tag = self.peer.tag();
        let mut req = self.new_raft_cmd_request();
        let mut builder = CustomBuilder::new();
        builder.set_change_set(cs);
        let custom_req = builder.build();
        req.set_custom_request(custom_req);
        let cb = Callback::write(Box::new(move |resp| {
            if resp.response.get_header().has_error() {
                let err_msg = resp.response.get_header().get_error().get_message();
                error!(
                    "failed to propose engine change set {:?} for {:?}",
                    err_msg, tag
                );
                // TODO(x): handle the error.
                // We need to detect if this error can be retried and retry it.
                // Or we may lose data if it is a flush.
                // And we need to stop propose change set if previous change set propose failed.
            } else {
                info!("proposed meta change event for {:?}", tag);
            }
        }));
        self.propose_raft_command(req, cb);
    }

    fn new_raft_cmd_request(&self) -> RaftCmdRequest {
        let mut req = RaftCmdRequest::default();
        let mut header = RaftRequestHeader::default();
        header.set_region_id(self.region_id());
        header.set_peer(self.peer.peer.clone());
        header.set_region_epoch(self.region().get_region_epoch().clone());
        header.set_term(self.peer.term());
        req.set_header(header);
        req
    }

    fn on_apply_change_set_result(&mut self, msg: MsgApplyChangeSetResult) {
        let tag = self.peer.tag();
        if msg.result.is_err() {
            let err = msg.result.unwrap_err();
            error!(
                "region failed to apply change set";
                "err" => ?err,
                "region" => tag,
            );
            if self.peer.mut_store().snap_state == SnapState::Applying {
                self.peer.mut_store().snap_state = SnapState::ApplyAborted;
            }
            return;
        }
        let change = msg.result.unwrap();
        if change.shard_ver != self.region().get_region_epoch().get_version() {
            error!("change set version not match change {:?}", &change; "region" => tag);
            return;
        }
        if change.has_flush() {
            let write_sequence = self
                .peer
                .mut_store()
                .shard_meta
                .as_ref()
                .unwrap()
                .data_sequence;
            self.ctx.raft_wb.truncate_raft_log(tag.id(), write_sequence);
        }
        if change.has_snapshot() {
            let store = self.peer.mut_store();
            let parent_id = store.parent_id();
            store.initial_flushed = true;
            store.snap_state = SnapState::Relax;
            store.shard_meta = Some(ShardMeta::new(&change));
            if let Some(parent_id) = parent_id {
                self.ctx
                    .global
                    .engines
                    .raft
                    .remove_dependent(parent_id, tag.id());
            }
        }
        if change.has_initial_flush() {
            let store = self.peer.mut_store();
            store.initial_flushed = true;
            let parent_id = store.parent_id();
            if let Some(parent_id) = parent_id {
                self.ctx
                    .global
                    .engines
                    .raft
                    .remove_dependent(parent_id, tag.id());
            }
        }
    }

    fn on_persisted(&mut self, ready: PersistReady) {
        if ready.peer_id != self.fsm.peer_id() {
            error!(
                "peer id not match";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "persisted_peer_id" => ready.peer_id,
                "persisted_number" => ready.ready_number,
            );
            return;
        }
        self.peer.raft_group.on_persist_ready(ready.ready_number);
    }
}

pub fn new_read_index_request(
    region_id: u64,
    region_epoch: RegionEpoch,
    peer: metapb::Peer,
) -> RaftCmdRequest {
    let mut request = RaftCmdRequest::default();
    request.mut_header().set_region_id(region_id);
    request.mut_header().set_region_epoch(region_epoch);
    request.mut_header().set_peer(peer);
    let mut cmd = Request::default();
    cmd.set_cmd_type(CmdType::ReadIndex);
    request
}

pub fn new_admin_request(region_id: u64, peer: metapb::Peer) -> RaftCmdRequest {
    let mut request = RaftCmdRequest::default();
    request.mut_header().set_region_id(region_id);
    request.mut_header().set_peer(peer);
    request
}

/// For status command.
impl PeerMsgHandler<'_> {
    // Handle status commands here, separate the logic, maybe we can move it
    // to another file later.
    // Unlike other commands (write or admin), status commands only show current
    // store status, so no need to handle it in raft group.
    fn execute_status_command(&mut self, request: &RaftCmdRequest) -> Result<RaftCmdResponse> {
        let cmd_type = request.get_status_request().get_cmd_type();

        let mut response = match cmd_type {
            StatusCmdType::RegionLeader => self.execute_region_leader(),
            StatusCmdType::RegionDetail => self.execute_region_detail(request),
            StatusCmdType::InvalidStatus => {
                Err(box_err!("{} invalid status command!", self.fsm.peer.tag()))
            }
        }?;
        response.set_cmd_type(cmd_type);

        let mut resp = RaftCmdResponse::default();
        resp.set_status_response(response);
        // Bind peer current term here.
        bind_term(&mut resp, self.fsm.peer.term());
        Ok(resp)
    }

    fn execute_region_leader(&mut self) -> Result<StatusResponse> {
        let mut resp = StatusResponse::default();
        if let Some(leader) = self.fsm.peer.get_peer_from_cache(self.fsm.peer.leader_id()) {
            resp.mut_region_leader().set_leader(leader);
        }

        Ok(resp)
    }

    fn execute_region_detail(&mut self, request: &RaftCmdRequest) -> Result<StatusResponse> {
        if !self.fsm.peer.get_store().is_initialized() {
            let region_id = request.get_header().get_region_id();
            return Err(Error::RegionNotInitialized(region_id));
        }
        let mut resp = StatusResponse::default();
        resp.mut_region_detail()
            .set_region(self.fsm.peer.region().clone());
        if let Some(leader) = self.fsm.peer.get_peer_from_cache(self.fsm.peer.leader_id()) {
            resp.mut_region_detail().set_leader(leader);
        }

        Ok(resp)
    }
}
