// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! This module implements the creation and destruction of peer.
//!
//! A peer can only be created by either:
//! - bootstrapping a cluster, it's coverred in crate::bootstrap;
//! - receiving a RaftMessage.
//!
//! In v1, it can also be created by split. In v2, it's required to create by
//! sending a message to store fsm first, and then using split to initialized
//! the peer.
//!
//! A peer can only be removed in a raft group by conf change or merge. When
//! applying conf change, removed peer is added to `removed_records`; when
//! applying merge, source peer is added to merged_records. Quorum must agree
//! on the removal, but the removed peer may not necessary be in the quorum. So
//! the peer may not really destroy itself until either:
//! - applying conf change remove;
//! - receiving a RaftMessage with `is_tombstone` set;
//! - receiving a RaftMessage targeting larger ID.
//!
//! Leader is responsible to keep polling all removed peers and guarantee they
//! are really destroyed. A peer is considered destroyed only when a tombstone
//! record with the same ID or larger ID is persisted. For `removed_records`,
//! leader only needs to send a message with `is_tombstone` set. For
//! `merged_records`, to avoid race between destroy and merge, leader needs to
//! ask target peer to destroy source peer.

use std::{cmp, mem};

use batch_system::BasicMailbox;
use crossbeam::channel::{SendError, TrySendError};
use engine_traits::{KvEngine, RaftEngine, RaftLogBatch};
use kvproto::{
    metapb::{self, Region},
    raft_cmdpb::{AdminCmdType, RaftCmdRequest},
    raft_serverpb::{ExtraMessage, ExtraMessageType, PeerState, RaftMessage},
};
use raftstore::store::{
    fsm::{
        apply,
        life::{build_peer_destroyed_report, forward_destroy_to_source_peer},
        Proposal,
    },
    metrics::RAFT_PEER_PENDING_DURATION,
    util, Transport, WriteTask,
};
use slog::{debug, error, info, warn};
use tikv_util::{
    store::find_peer,
    time::{duration_to_sec, Instant},
};

use super::command::SplitInit;
use crate::{
    batch::StoreContext,
    fsm::{PeerFsm, Store},
    operation::command::report_split_init_finish,
    raft::{Peer, Storage},
    router::{CmdResChannel, PeerMsg, PeerTick},
};

/// When a peer is about to destroy, it becomes `WaitReady` first. If there is
/// no pending asynchronous apply, it becomes `Destroying` and then start
/// destroying asynchronously during handling ready. After the asynchronously
/// destroying is finished, it becomes `Destroyed`.
pub enum DestroyProgress {
    /// Alive means destroy is not triggered at all. It's the same as None for
    /// `Option<DestroyProgress>`. Not using Option to avoid unwrap everywhere.
    None,
    /// If the destroy is triggered by message, then the message will be used
    /// for creating new peer immediately.
    WaitReady(Option<Box<RaftMessage>>),
    Destroying(Option<Box<RaftMessage>>),
    Destroyed,
}

impl DestroyProgress {
    #[inline]
    pub fn started(&self) -> bool {
        matches!(
            self,
            DestroyProgress::Destroying(_) | DestroyProgress::Destroyed
        )
    }

    #[inline]
    pub fn waiting(&self) -> bool {
        matches!(self, DestroyProgress::WaitReady(_))
    }

    #[inline]
    fn start(&mut self) {
        match self {
            DestroyProgress::WaitReady(msg) => *self = DestroyProgress::Destroying(msg.take()),
            _ => panic!("must wait ready first to start destroying"),
        }
    }

    #[inline]
    fn wait_with(&mut self, triggered_msg: Option<Box<RaftMessage>>) {
        match self {
            DestroyProgress::None => *self = DestroyProgress::WaitReady(triggered_msg),
            _ => panic!("must be alive to wait"),
        }
    }

    #[inline]
    fn finish(&mut self) -> Option<Box<RaftMessage>> {
        match self {
            DestroyProgress::Destroying(msg) => {
                let msg = msg.take();
                *self = DestroyProgress::Destroyed;
                msg
            }
            _ => panic!("must be destroying to finish"),
        }
    }
}

#[derive(Default)]
pub struct AbnormalPeerContext {
    /// Record the instants of peers being added into the configuration.
    /// Remove them after they are not pending any more.
    /// (u64, Instant) represents (peer id, time when peer starts pending)
    pending_peers: Vec<(u64, Instant)>,
    /// A inaccurate cache about which peer is marked as down.
    down_peers: Vec<u64>,
}

impl AbnormalPeerContext {
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.pending_peers.is_empty() && self.down_peers.is_empty()
    }

    #[inline]
    pub fn reset(&mut self) {
        self.pending_peers.clear();
        self.down_peers.clear();
    }

    #[inline]
    pub fn down_peers(&self) -> &[u64] {
        &self.down_peers
    }

    #[inline]
    pub fn down_peers_mut(&mut self) -> &mut Vec<u64> {
        &mut self.down_peers
    }

    #[inline]
    pub fn pending_peers(&self) -> &[(u64, Instant)] {
        &self.pending_peers
    }

    #[inline]
    pub fn pending_peers_mut(&mut self) -> &mut Vec<(u64, Instant)> {
        &mut self.pending_peers
    }

    #[inline]
    pub fn retain_pending_peers(&mut self, f: impl FnMut(&mut (u64, Instant)) -> bool) -> bool {
        let len = self.pending_peers.len();
        self.pending_peers.retain_mut(f);
        len != self.pending_peers.len()
    }

    #[inline]
    pub fn flush_metrics(&self) {
        let _ = self.pending_peers.iter().map(|(_, pending_after)| {
            let elapsed = duration_to_sec(pending_after.saturating_elapsed());
            RAFT_PEER_PENDING_DURATION.observe(elapsed);
        });
    }
}

#[derive(Default)]
pub struct GcPeerContext {
    // Peers that are confirmed to be deleted.
    confirmed_ids: Vec<u64>,
}

fn check_if_to_peer_destroyed<ER: RaftEngine>(
    engine: &ER,
    msg: &RaftMessage,
    store_id: u64,
) -> engine_traits::Result<bool> {
    let region_id = msg.get_region_id();
    let to_peer = msg.get_to_peer();
    let local_state = match engine.get_region_state(region_id, u64::MAX)? {
        Some(s) => s,
        None => return Ok(false),
    };
    // Split will not create peer in v2, so the state must be Tombstone.
    if local_state.get_state() != PeerState::Tombstone {
        panic!(
            "[region {}] {} peer doesn't exist but has valid local state {:?}",
            region_id, to_peer.id, local_state
        );
    }
    // Compared to v1, we rely on leader to confirm destroy actively, so here
    // skip handling gc for simplicity.
    let local_epoch = local_state.get_region().get_region_epoch();
    // The region in this peer is already destroyed
    if util::is_epoch_stale(msg.get_region_epoch(), local_epoch) {
        return Ok(true);
    }
    if let Some(local_peer) = find_peer(local_state.get_region(), store_id) && to_peer.id <= local_peer.get_id() {
        return Ok(true);
    }
    // If the peer is destroyed by conf change, all above checks will pass.
    if local_state
        .get_removed_records()
        .iter()
        .find(|p| p.get_store_id() == store_id)
        .map_or(false, |p| to_peer.id <= p.get_id())
    {
        return Ok(true);
    }
    Ok(false)
}

// An empty raft message for creating peer fsm.
fn empty_split_message(store_id: u64, region: &Region) -> Box<RaftMessage> {
    let mut raft_msg = Box::<RaftMessage>::default();
    raft_msg.set_region_id(region.get_id());
    raft_msg.set_region_epoch(region.get_region_epoch().clone());
    raft_msg.set_to_peer(
        region
            .get_peers()
            .iter()
            .find(|p| p.get_store_id() == store_id)
            .unwrap()
            .clone(),
    );
    raft_msg
}

pub fn is_empty_split_message(msg: &RaftMessage) -> bool {
    !msg.has_from_peer() && msg.has_to_peer() && msg.has_region_epoch() && !msg.has_message()
}

impl Store {
    /// The method is called during split.
    /// The creation process is:
    /// 1. create an uninitialized peer if not existed before
    /// 2. initialize the peer by the information sent from parent peer
    #[inline]
    pub fn on_split_init<EK, ER, T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        msg: Box<SplitInit>,
    ) where
        EK: KvEngine,
        ER: RaftEngine,
        T: Transport,
    {
        let derived_region_id = msg.derived_region_id;
        let region_id = msg.region.id;
        let raft_msg = empty_split_message(self.store_id(), &msg.region);

        (|| {
            fail::fail_point!(
                "on_store_2_split_init_race_with_initial_message",
                self.store_id() == 2,
                |_| {
                    let mut initial_msg = raft_msg.clone();
                    initial_msg.set_from_peer(
                        msg.region
                            .get_peers()
                            .iter()
                            .find(|p| p.get_store_id() != self.store_id())
                            .unwrap()
                            .clone(),
                    );
                    let m = initial_msg.mut_message();
                    m.set_msg_type(raft::prelude::MessageType::MsgRequestPreVote);
                    m.set_term(raftstore::store::RAFT_INIT_LOG_TERM);
                    m.set_index(raftstore::store::RAFT_INIT_LOG_INDEX);
                    assert!(util::is_initial_msg(initial_msg.get_message()));
                    self.on_raft_message(ctx, initial_msg);
                }
            )
        })();

        // It will create the peer if it does not exist
        self.on_raft_message(ctx, raft_msg);

        if let Err(SendError(m)) = ctx.router.force_send(region_id, PeerMsg::SplitInit(msg)) {
            warn!(
                self.logger(),
                "Split peer is destroyed before sending the intialization msg";
                "split init msg" => ?m,
            );
            report_split_init_finish(ctx, derived_region_id, region_id, true);
        }
    }

    #[inline]
    pub fn on_ask_commit_merge<EK, ER, T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        req: RaftCmdRequest,
    ) where
        EK: KvEngine,
        ER: RaftEngine,
        T: Transport,
    {
        let region_id = req.get_header().get_region_id();
        let mut raft_msg = Box::<RaftMessage>::default();
        raft_msg.set_region_id(region_id);
        raft_msg.set_region_epoch(req.get_header().get_region_epoch().clone());
        raft_msg.set_to_peer(req.get_header().get_peer().clone());

        // It will create the peer if it does not exist
        self.on_raft_message(ctx, raft_msg);

        if let Err(SendError(PeerMsg::AskCommitMerge(req))) = ctx
            .router
            .force_send(region_id, PeerMsg::AskCommitMerge(req))
        {
            let commit_merge = req.get_admin_request().get_commit_merge();
            let source_id = commit_merge.get_source().get_id();
            let _ = ctx.router.force_send(
                source_id,
                PeerMsg::RejectCommitMerge {
                    index: commit_merge.get_commit(),
                },
            );
        }
    }

    /// When a message's recipient doesn't exist, it will be redirected to
    /// store. Store is responsible for checking if it's neccessary to create
    /// a peer to handle the message.
    #[inline]
    pub fn on_raft_message<EK, ER, T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        msg: Box<RaftMessage>,
    ) where
        EK: KvEngine,
        ER: RaftEngine,
        T: Transport,
    {
        debug!(
            self.logger(),
            "store handle raft message";
            "message_type" => %util::MsgType(&msg),
            "from_peer_id" => msg.get_from_peer().get_id(),
            "to_peer_id" => msg.get_to_peer().get_id(),
        );
        let region_id = msg.get_region_id();
        // The message can be sent when the peer is being created, so try send it first.
        let mut msg = if let Err(TrySendError::Disconnected(PeerMsg::RaftMessage(m))) =
            ctx.router.send(region_id, PeerMsg::RaftMessage(msg))
        {
            m
        } else {
            return;
        };
        let from_peer = msg.get_from_peer();
        let to_peer = msg.get_to_peer();
        // Now the peer should not exist.
        debug!(
            self.logger(),
            "handle raft message";
            "from_peer_id" => from_peer.id,
            "to_peer_id" => to_peer.id,
            "region_id" => region_id,
            "msg_type" => %util::MsgType(&msg)
        );
        if to_peer.store_id != self.store_id() {
            ctx.raft_metrics.message_dropped.mismatch_store_id.inc();
            return;
        }
        if !msg.has_region_epoch() {
            ctx.raft_metrics.message_dropped.mismatch_region_epoch.inc();
            return;
        }
        if msg.has_merge_target() {
            // Target tombstone peer doesn't exist, so ignore it.
            ctx.raft_metrics.message_dropped.stale_msg.inc();
            return;
        }
        let destroyed = match check_if_to_peer_destroyed(&ctx.engine, &msg, self.store_id()) {
            Ok(d) => d,
            Err(e) => {
                error!(self.logger(), "failed to get region state"; "region_id" => region_id, "err" => ?e);
                return;
            }
        };
        if destroyed {
            if msg.get_is_tombstone() {
                if let Some(msg) = build_peer_destroyed_report(&mut msg) {
                    let _ = ctx.trans.send(msg);
                }
                return;
            }
            if msg.has_extra_msg() {
                let extra_msg = msg.get_extra_msg();
                // Only the direct request has `is_tombstone` set to false. We are certain this
                // message needs to be forwarded.
                if extra_msg.get_type() == ExtraMessageType::MsgGcPeerRequest
                    && extra_msg.has_check_gc_peer()
                {
                    forward_destroy_to_source_peer(&msg, |m| {
                        let _ = ctx.router.send_raft_message(m.into());
                    });
                    return;
                }
            }
            ctx.raft_metrics.message_dropped.region_tombstone_peer.inc();
            return;
        }
        // If it's not destroyed, and the message is a tombstone message, create the
        // peer and destroy immediately to leave a tombstone record.

        // So the peer must need to be created. We don't need to synchronous with split
        // as split won't create peer in v2. And we don't check for range
        // conflict as v2 depends on tablet, which allows conflict ranges.
        let mut region = Region::default();
        region.set_id(region_id);
        region.set_region_epoch(msg.get_region_epoch().clone());

        // Peer list doesn't have to be complete, as it's uninitialized.
        //
        // If the id of the from_peer is INVALID_ID, this msg must be sent from parent
        // peer in the split execution in which case we do not add it into the region.
        if from_peer.id != raft::INVALID_ID
            // Check merge may be sent from different region
            && (msg.get_extra_msg().get_type() != ExtraMessageType::MsgGcPeerRequest
                || msg.get_extra_msg().get_check_gc_peer().get_from_region_id() == region_id)
        {
            region.mut_peers().push(from_peer.clone());
        }
        region.mut_peers().push(to_peer.clone());
        // We don't set the region range here as we allow range conflict.
        let (tx, fsm) = match Storage::uninit(
            self.store_id(),
            region,
            ctx.engine.clone(),
            ctx.schedulers.read.clone(),
            &ctx.logger,
        )
        .and_then(|s| {
            PeerFsm::new(
                &ctx.cfg,
                &ctx.tablet_registry,
                ctx.key_manager.as_deref(),
                &ctx.snap_mgr,
                s,
            )
        }) {
            Ok(p) => p,
            res => {
                error!(self.logger(), "failed to create peer"; "region_id" => region_id, "peer_id" => to_peer.id, "err" => ?res.err());
                return;
            }
        };
        ctx.store_meta
            .lock()
            .unwrap()
            .set_region(fsm.peer().region(), false, fsm.logger());
        let mailbox = BasicMailbox::new(tx, fsm, ctx.router.state_cnt().clone());
        if ctx
            .router
            .send_and_register(region_id, mailbox, PeerMsg::Start(None))
            .is_err()
        {
            panic!(
                "[region {}] {} failed to register peer",
                region_id, to_peer.id
            );
        }
        // Only forward valid message. Split may use a message without sender to trigger
        // creating a peer.
        if from_peer.id != raft::INVALID_ID {
            // For now the peer only exists in memory. It will persist its states when
            // handling its first readiness.
            let _ = ctx.router.send(region_id, PeerMsg::RaftMessage(msg));
        }
    }

    pub fn on_update_latency_inspectors<EK, ER, T>(
        &self,
        ctx: &mut StoreContext<EK, ER, T>,
        start_ts: Instant,
        mut inspector: util::LatencyInspector,
    ) where
        EK: KvEngine,
        ER: RaftEngine,
        T: Transport,
    {
        // Record the last statistics of commit-log-duration and store-write-duration.
        inspector.record_store_wait(start_ts.saturating_elapsed());
        inspector.record_store_commit(ctx.raft_metrics.stat_commit_log.avg());
        // Reset the stat_commit_log and wait it to be refreshed in the next tick.
        ctx.raft_metrics.stat_commit_log.reset();
        ctx.pending_latency_inspect.push(inspector);
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub fn on_availability_request<T: Transport>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        from_region_id: u64,
        from_peer: &metapb::Peer,
    ) {
        let mut msg = RaftMessage::default();
        msg.set_region_id(from_region_id);
        msg.set_from_peer(self.peer().clone());
        msg.set_to_peer(from_peer.clone());
        msg.mut_extra_msg()
            .set_type(ExtraMessageType::MsgAvailabilityResponse);
        let report = msg.mut_extra_msg().mut_availability_context();
        report.set_from_region_id(self.region_id());
        report.set_from_region_epoch(self.region().get_region_epoch().clone());
        report.set_trimmed(!self.storage().has_dirty_data());
        let _ = ctx.trans.send(msg);
    }

    #[inline]
    pub fn on_availability_response<T: Transport>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        from_peer: u64,
        resp: &ExtraMessage,
    ) {
        self.merge_on_availability_response(ctx, from_peer, resp);
    }

    pub fn maybe_schedule_gc_peer_tick(&mut self) {
        let region_state = self.storage().region_state();
        if !region_state.get_removed_records().is_empty()
            || !region_state.get_merged_records().is_empty()
        {
            self.add_pending_tick(PeerTick::GcPeer);
        }
    }

    /// Returns `true` means the sender will be gced. The message is stale.
    pub fn maybe_gc_sender(&mut self, msg: &RaftMessage) -> bool {
        let removed_peers = self.storage().region_state().get_removed_records();
        // Only removed_records can be determined directly.
        if let Some(peer) = removed_peers
            .iter()
            .find(|p| p.id == msg.get_from_peer().get_id())
        {
            let tombstone_msg = self.tombstone_message_for_same_region(peer.clone());
            self.add_message(tombstone_msg);
            true
        } else {
            false
        }
    }

    fn tombstone_message_for_same_region(&self, peer: metapb::Peer) -> RaftMessage {
        let region_id = self.region_id();
        let mut tombstone_message = RaftMessage::default();
        tombstone_message.set_region_id(region_id);
        tombstone_message.set_from_peer(self.peer().clone());
        tombstone_message.set_to_peer(peer);
        tombstone_message.set_region_epoch(self.region().get_region_epoch().clone());
        tombstone_message.set_is_tombstone(true);
        tombstone_message
    }

    pub fn on_tombstone_message(&mut self, msg: &mut RaftMessage) {
        match msg.get_to_peer().get_id().cmp(&self.peer_id()) {
            cmp::Ordering::Less => {
                if let Some(msg) = build_peer_destroyed_report(msg) {
                    self.add_message(msg);
                }
            }
            // No matter it's greater or equal, the current peer must be destroyed.
            _ => {
                self.mark_for_destroy(None);
            }
        }
    }

    /// When leader tries to gc merged source peer, it will send a gc request to
    /// target peer. If target peer makes sure the merged is finished, it
    /// forward the message to source peer and let source peer send back a
    /// response.
    pub fn on_gc_peer_request<T: Transport>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        msg: &RaftMessage,
    ) {
        let extra_msg = msg.get_extra_msg();
        if !extra_msg.has_check_gc_peer() || extra_msg.get_index() == 0 {
            // Corrupted message.
            return;
        }
        if self.storage().tablet_index() < extra_msg.get_index() {
            // Merge not finish.
            return;
        }

        let check = extra_msg.get_check_gc_peer();
        let check_peer_id = check.get_check_peer().get_id();
        let records = self.storage().region_state().get_merged_records();
        let Some(record) = records.iter().find(|r| {
            r.get_source_peers().iter().any(|p| p.get_id() == check_peer_id)
        }) else { return };
        let source_index = record.get_source_index();
        forward_destroy_to_source_peer(msg, |m| {
            let source_checkpoint = super::merge_source_path(
                &ctx.tablet_registry,
                check.get_check_region_id(),
                source_index,
            );
            if source_checkpoint.exists() {
                let router = ctx.router.clone();
                self.record_tombstone_tablet_path_callback(
                    ctx,
                    source_checkpoint,
                    extra_msg.get_index(),
                    move || {
                        let _ = router.send_raft_message(m.into());
                    },
                );
            }
        });
    }

    /// A peer confirms it's destroyed.
    pub fn on_gc_peer_response(&mut self, msg: &RaftMessage) {
        let gc_peer_id = msg.get_from_peer().get_id();
        let state = self.storage().region_state();
        if state
            .get_removed_records()
            .iter()
            .all(|p| p.get_id() != gc_peer_id)
            && state.get_merged_records().iter().all(|p| {
                p.get_source_peers()
                    .iter()
                    .all(|p| p.get_id() != gc_peer_id)
            })
        {
            return;
        }
        let ctx = self.gc_peer_context_mut();
        if ctx.confirmed_ids.contains(&gc_peer_id) {
            return;
        }
        ctx.confirmed_ids.push(gc_peer_id);
    }

    // Removes deleted peers from region state by proposing a `UpdateGcPeer`
    // command.
    pub fn on_gc_peer_tick<T: Transport>(&mut self, ctx: &mut StoreContext<EK, ER, T>) {
        if !self.is_leader() {
            return;
        }
        let state = self.storage().region_state();
        if state.get_removed_records().is_empty() && state.get_merged_records().is_empty() {
            return;
        }
        let mut need_gc_ids = Vec::with_capacity(5);
        let gc_context = self.gc_peer_context();
        for peer in state.get_removed_records() {
            need_gc_ids.push(peer.get_id());
            if gc_context.confirmed_ids.contains(&peer.get_id()) {
                continue;
            }

            let msg = self.tombstone_message_for_same_region(peer.clone());
            // For leader, it's OK to send gc message immediately.
            let _ = ctx.trans.send(msg);
        }
        for record in state.get_merged_records() {
            // For merge, we ask target to check whether source should be deleted.
            for (source, target) in record
                .get_source_peers()
                .iter()
                .zip(record.get_target_peers())
            {
                need_gc_ids.push(source.get_id());
                if gc_context.confirmed_ids.contains(&source.get_id()) {
                    continue;
                }

                let mut msg = RaftMessage::default();
                msg.set_region_id(record.get_target_region_id());
                msg.set_from_peer(self.peer().clone());
                msg.set_to_peer(target.clone());
                msg.set_region_epoch(record.get_target_epoch().clone());
                let extra_msg = msg.mut_extra_msg();
                extra_msg.set_type(ExtraMessageType::MsgGcPeerRequest);
                extra_msg.set_index(record.get_index());
                let check_peer = extra_msg.mut_check_gc_peer();
                check_peer.set_from_region_id(self.region_id());
                check_peer.set_check_region_id(record.get_source_region_id());
                check_peer.set_check_peer(source.clone());
                check_peer.set_check_region_epoch(record.get_source_epoch().clone());
                let _ = ctx.trans.send(msg);
            }
        }
        let gc_ctx = self.gc_peer_context_mut();
        if !gc_ctx.confirmed_ids.is_empty() {
            let mut confirmed_ids = mem::take(&mut gc_ctx.confirmed_ids);
            confirmed_ids.retain(|id| need_gc_ids.contains(id));
            let mut req = RaftCmdRequest::default();
            let header = req.mut_header();
            header.set_region_id(self.region_id());
            header.set_peer(self.peer().clone());
            let admin = req.mut_admin_request();
            admin.set_cmd_type(AdminCmdType::UpdateGcPeer);
            let gc_peer = admin.mut_update_gc_peers();
            gc_peer.set_peer_id(confirmed_ids);
            let (ch, _) = CmdResChannel::pair();
            // It's OK to fail as we will retry by tick.
            self.on_admin_command(ctx, req, ch);
        }
        self.maybe_schedule_gc_peer_tick();
    }

    /// A peer can be destroyed in three cases:
    /// 1. Received a gc message;
    /// 2. Received a message whose target peer's ID is larger than this;
    /// 3. Applied a conf remove self command.
    /// In all cases, the peer will be destroyed asynchronousely in next
    /// handle_raft_ready.
    /// `triggered_msg` will be sent to store fsm after destroy is finished.
    /// Should set the message only when the target peer is supposed to be
    /// created afterward.
    pub fn mark_for_destroy(&mut self, triggered_msg: Option<Box<RaftMessage>>) {
        if self.serving() {
            self.destroy_progress_mut().wait_with(triggered_msg);
            self.set_has_ready();
        }
    }

    /// In v2, it's possible to destroy the peer without waiting for apply. But
    /// we better wait till all previous entries are applied in case there
    /// are split. It's a waste to use snapshot to restore newly split
    /// tablet.
    #[inline]
    pub fn postponed_destroy(&self) -> bool {
        let last_applying_index = self.compact_log_context().last_applying_index();
        let entry_storage = self.storage().entry_storage();
        // If it's marked as tombstone, then it must be changed by conf change. In
        // this case, all following entries are skipped so applied_index never equals
        // to last_applying_index.
        (self.storage().region_state().get_state() != PeerState::Tombstone
            && entry_storage.applied_index() != last_applying_index)
            // Wait for critical commands like split.
            || self.has_pending_tombstone_tablets()
    }

    /// Start the destroy progress. It will write `Tombstone` state
    /// asynchronously.
    ///
    /// After destroy is finished, `finish_destroy` should be called to clean up
    /// memory states.
    pub fn start_destroy<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        write_task: &mut WriteTask<EK, ER>,
    ) {
        if self.postponed_destroy() {
            return;
        }
        let raft_engine = self.entry_storage().raft_engine();
        let mut region_state = self.storage().region_state().clone();
        let region_id = region_state.get_region().get_id();
        // Use extra write to ensure these writes are the last writes to raft engine.
        let lb = write_task
            .extra_write
            .ensure_v2(|| raft_engine.log_batch(2));
        // We only use raft-log-engine for v2, first index and state are not important.
        let raft_state = self.entry_storage().raft_state();
        raft_engine.clean(region_id, 0, raft_state, lb).unwrap();
        region_state.set_state(PeerState::Tombstone);
        let applied_index = self.entry_storage().applied_index();
        lb.put_region_state(region_id, applied_index, &region_state)
            .unwrap();
        self.record_tombstone_tablet_for_destroy(ctx, write_task);
        self.destroy_progress_mut().start();
    }

    /// Do clean up for destroy. The peer is permanently destroyed when
    /// Tombstone state is persisted. This method is only for cleaning up
    /// memory states.
    pub fn finish_destroy<T>(&mut self, ctx: &mut StoreContext<EK, ER, T>) {
        info!(self.logger, "peer destroyed");
        let region_id = self.region_id();
        {
            let mut meta = ctx.store_meta.lock().unwrap();
            meta.remove_region(region_id);
            meta.readers.remove(&region_id);
            meta.region_read_progress.remove(&region_id);
            ctx.tablet_registry.remove(region_id);
        }
        // Remove tablet first, otherwise in extreme cases, a new peer can be created
        // and race on tablet record removal and creation.
        ctx.router.close(region_id);
        if let Some(msg) = self.destroy_progress_mut().finish() {
            // The message will be dispatched to store fsm, which will create a
            // new peer. Ignore error as it's just a best effort.
            let _ = ctx.router.send_raft_message(msg);
        }
        self.pending_reads_mut().clear_all(Some(region_id));
        for Proposal { cb, .. } in self.proposals_mut().queue_mut().drain(..) {
            apply::notify_req_region_removed(region_id, cb);
        }

        self.clear_apply_scheduler();
    }
}
