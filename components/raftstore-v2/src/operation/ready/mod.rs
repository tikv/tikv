// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! This module contains the actions that will drive a raft state machine.
//!
//! # Raft Ready
//!
//! Every messages or ticks may have side affect. Handling all those side
//! affect immediately is not efficient. Instead, tikv uses `Ready` to batch up
//! all the side affects and handle them at once for throughput.
//!
//! As raft store is the critical path in the whole system, we avoid most
//! blocking IO. So a typical processing is divided into two steps:
//!
//! - Handle raft ready to process the side affect and send IO tasks to
//!   background threads
//! - Receive IO tasks completion and update the raft state machine
//!
//! There two steps can be processed concurrently.

mod apply_trace;
mod async_writer;
mod snapshot;

use std::{
    cmp,
    fmt::{self, Debug, Formatter},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};

use engine_traits::{KvEngine, RaftEngine, DATA_CFS};
use error_code::ErrorCodeExt;
use kvproto::{
    raft_cmdpb::AdminCmdType,
    raft_serverpb::{ExtraMessageType, RaftMessage},
};
use protobuf::Message as _;
use raft::{eraftpb, prelude::MessageType, Ready, SnapshotStatus, StateRole, INVALID_ID};
use raftstore::{
    coprocessor::{RegionChangeEvent, RoleChange},
    store::{
        fsm::store::StoreRegionMeta,
        local_metrics::IoType,
        needs_evict_entry_cache,
        util::{self, is_first_append_entry, is_initial_msg},
        worker_metrics::SNAP_COUNTER,
        FetchedLogs, ReadProgress, Transport, WriteCallback, WriteTask,
    },
};
use slog::{debug, error, info, warn, Logger};
use tikv_util::{
    log::SlogFormat,
    slog_panic,
    store::find_peer,
    sys::disk::DiskUsage,
    time::{duration_to_sec, monotonic_raw_now, Duration, Instant as TiInstant},
};

pub use self::{
    apply_trace::{write_initial_states, ApplyTrace, DataTrace, StateStorage},
    async_writer::AsyncWriter,
    snapshot::{GenSnapTask, SnapState},
};
use crate::{
    batch::StoreContext,
    fsm::{PeerFsmDelegate, Store},
    operation::life::is_empty_split_message,
    raft::{Peer, Storage},
    router::{PeerMsg, PeerTick},
    worker::tablet,
};

const PAUSE_FOR_REPLAY_GAP: u64 = 128;

pub struct ReplayWatch {
    normal_peers: AtomicUsize,
    paused_peers: AtomicUsize,
    logger: Logger,
    timer: Instant,
}

impl Debug for ReplayWatch {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplayWatch")
            .field("normal_peers", &self.normal_peers)
            .field("paused_peers", &self.paused_peers)
            .field("logger", &self.logger)
            .field("timer", &self.timer)
            .finish()
    }
}

impl ReplayWatch {
    pub fn new(logger: Logger) -> Self {
        Self {
            normal_peers: AtomicUsize::new(0),
            paused_peers: AtomicUsize::new(0),
            logger,
            timer: Instant::now(),
        }
    }

    pub fn inc_normal_peer(&self) {
        self.normal_peers.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_paused_peer(&self) {
        self.paused_peers.fetch_add(1, Ordering::Relaxed);
    }
}

impl Drop for ReplayWatch {
    fn drop(&mut self) {
        info!(
            self.logger,
            "The raft log replay completed";
            "normal_peers" => self.normal_peers.load(Ordering::Relaxed),
            "paused_peers" => self.paused_peers.load(Ordering::Relaxed),
            "elapsed" => ?self.timer.elapsed()
        );
    }
}

impl Store {
    pub fn on_store_unreachable<EK, ER, T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        to_store_id: u64,
    ) where
        EK: KvEngine,
        ER: RaftEngine,
    {
        ctx.router
            .broadcast_normal(|| PeerMsg::StoreUnreachable { to_store_id });
    }

    #[cfg(feature = "testexport")]
    pub fn on_wait_flush<EK, ER, T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        region_id: u64,
        ch: crate::router::FlushChannel,
    ) where
        EK: KvEngine,
        ER: RaftEngine,
    {
        let _ = ctx.router.send(region_id, PeerMsg::WaitFlush(ch));
    }
}

impl<'a, EK: KvEngine, ER: RaftEngine, T: Transport> PeerFsmDelegate<'a, EK, ER, T> {
    /// Raft relies on periodic ticks to keep the state machine sync with other
    /// peers.
    pub fn on_raft_tick(&mut self) {
        if self.fsm.peer_mut().tick(self.store_ctx) {
            self.fsm.peer_mut().set_has_ready();
        }
        self.fsm.peer_mut().maybe_clean_up_stale_merge_context();
        self.schedule_tick(PeerTick::Raft);
    }

    pub fn on_check_long_uncommitted(&mut self) {
        if !self.fsm.peer().is_leader() {
            return;
        }
        self.fsm
            .peer_mut()
            .check_long_uncommitted_proposals(self.store_ctx);
        self.schedule_tick(PeerTick::CheckLongUncommitted);
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub fn maybe_pause_for_replay<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        watch: Option<Arc<ReplayWatch>>,
    ) -> bool {
        // The task needs to be scheduled even if the tablet may be replaced during
        // recovery. Otherwise if there are merges during recovery, the FSM may
        // be paused forever.
        if self.storage().has_dirty_data() {
            let region_id = self.region_id();
            let mailbox = store_ctx.router.mailbox(region_id).unwrap();
            let tablet_index = self.storage().tablet_index();
            let _ = store_ctx.schedulers.tablet.schedule(tablet::Task::trim(
                self.tablet().unwrap().clone(),
                self.region(),
                move || {
                    let _ = mailbox.force_send(PeerMsg::TabletTrimmed { tablet_index });
                },
            ));
        }
        let entry_storage = self.entry_storage();
        let committed_index = entry_storage.commit_index();
        let applied_index = entry_storage.applied_index();
        if committed_index > applied_index {
            // Unlike v1, it's a must to set ready when there are pending entries. Otherwise
            // it may block for ever when there is unapplied conf change.
            self.set_has_ready();
        }
        if committed_index > applied_index + PAUSE_FOR_REPLAY_GAP {
            // If there are too many the missing logs, we need to skip ticking otherwise
            // it may block the raftstore thread for a long time in reading logs for
            // election timeout.
            info!(self.logger, "pause for replay"; "applied" => applied_index, "committed" => committed_index);

            // when committed_index > applied_index + PAUSE_FOR_REPLAY_GAP, the peer must be
            // created from StoreSystem on TiKV Start
            let w = watch.unwrap();
            w.inc_paused_peer();
            self.set_replay_watch(Some(w));
            true
        } else {
            if let Some(w) = watch {
                w.inc_normal_peer();
            }
            false
        }
    }

    #[inline]
    fn tick<T>(&mut self, store_ctx: &mut StoreContext<EK, ER, T>) -> bool {
        // When it's handling snapshot, it's pointless to tick as all the side
        // affects have to wait till snapshot is applied. On the other hand, ticking
        // will bring other corner cases like elections.
        if self.is_handling_snapshot() || !self.serving() {
            return false;
        }
        self.retry_pending_reads(&store_ctx.cfg);
        self.check_force_leader(store_ctx);
        self.raft_group_mut().tick()
    }

    pub fn on_peer_unreachable(&mut self, to_peer_id: u64) {
        if self.is_leader() {
            self.raft_group_mut().report_unreachable(to_peer_id);
        }
    }

    pub fn on_store_unreachable(&mut self, to_store_id: u64) {
        if self.is_leader() {
            if let Some(peer_id) = find_peer(self.region(), to_store_id).map(|p| p.get_id()) {
                self.raft_group_mut().report_unreachable(peer_id);
            }
        }
    }

    pub fn on_store_maybe_tombstone(&mut self, store_id: u64) {
        if !self.is_leader() {
            return;
        }
        self.on_store_maybe_tombstone_gc_peer(store_id);
    }

    pub fn on_raft_message<T: Transport>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        mut msg: Box<RaftMessage>,
        send_time: Option<TiInstant>,
    ) {
        debug!(
            self.logger,
            "handle raft message";
            "message_type" => %util::MsgType(&msg),
            "from_peer_id" => msg.get_from_peer().get_id(),
            "to_peer_id" => msg.get_to_peer().get_id(),
            "disk_usage" => ?msg.get_disk_usage(),
        );
        if let Some(send_time) = send_time {
            let process_wait_time = send_time.saturating_elapsed();
            ctx.raft_metrics
                .process_wait_time
                .observe(duration_to_sec(process_wait_time));
        }

        if self.pause_for_replay() && msg.get_message().get_msg_type() == MessageType::MsgAppend {
            ctx.raft_metrics.message_dropped.recovery.inc();
            return;
        }
        if !self.serving() {
            return;
        }
        if util::is_vote_msg(msg.get_message()) {
            if self.maybe_gc_sender(&msg) {
                return;
            }
            if let Some(remain) = ctx.maybe_in_unsafe_vote_period() {
                debug!(self.logger,
                    "drop request vote for one election timeout after node starts";
                    "from_peer_id" => msg.get_message().get_from(),
                    "remain_duration" => ?remain,
                );
                ctx.raft_metrics.message_dropped.unsafe_vote.inc();
                return;
            }
        }

        self.handle_reported_disk_usage(ctx, &msg);

        if msg.get_to_peer().get_store_id() != self.peer().get_store_id() {
            ctx.raft_metrics.message_dropped.mismatch_store_id.inc();
            return;
        }
        if msg.get_is_tombstone() {
            self.on_tombstone_message(&mut msg);
            return;
        }
        if msg.has_extra_msg() && msg.get_to_peer().get_id() == self.peer_id() {
            // GcRequest/GcResponse may be sent from/to different regions, skip further
            // checks.
            match msg.get_extra_msg().get_type() {
                ExtraMessageType::MsgGcPeerResponse => {
                    self.on_gc_peer_response(&msg);
                    return;
                }
                ExtraMessageType::MsgGcPeerRequest => {
                    self.on_gc_peer_request(ctx, &msg);
                    return;
                }
                ExtraMessageType::MsgFlushMemtable => {
                    let region_epoch = msg.as_ref().get_region_epoch();
                    if util::is_epoch_stale(region_epoch, self.region().get_region_epoch()) {
                        return;
                    }
                    let _ = ctx
                        .schedulers
                        .tablet
                        .schedule(crate::worker::tablet::Task::Flush {
                            region_id: self.region().get_id(),
                            reason: "unknown",
                            high_priority: false,
                            threshold: Some(std::time::Duration::from_secs(10)),
                            cb: None,
                        });
                    return;
                }
                ExtraMessageType::MsgWantRollbackMerge => return,
                ExtraMessageType::MsgAvailabilityRequest => {
                    self.on_availability_request(
                        ctx,
                        msg.get_extra_msg()
                            .get_availability_context()
                            .get_from_region_id(),
                        msg.get_from_peer(),
                    );
                    return;
                }
                ExtraMessageType::MsgAvailabilityResponse => {
                    self.on_availability_response(
                        ctx,
                        msg.get_from_peer().get_id(),
                        msg.get_extra_msg(),
                    );
                    return;
                }
                ExtraMessageType::MsgRefreshBuckets => {
                    self.on_msg_refresh_buckets(ctx, &msg);
                    return;
                }
                _ => (),
            }
        }
        if !msg.has_region_epoch() {
            ctx.raft_metrics.message_dropped.mismatch_region_epoch.inc();
            return;
        }
        if msg.has_merge_target() {
            unimplemented!();
            // return;
        }
        // We don't handle stale message like v1, as we rely on leader to actively
        // cleanup stale peers.
        let to_peer = msg.get_to_peer();
        // Check if the message is sent to the right peer.
        match to_peer.get_id().cmp(&self.peer_id()) {
            cmp::Ordering::Equal => (),
            cmp::Ordering::Less => {
                ctx.raft_metrics.message_dropped.stale_msg.inc();
                return;
            }
            cmp::Ordering::Greater => {
                // We need to create the target peer.
                info!(self.logger, "mark for destroy for larger ID"; "larger_id" => to_peer.get_id());
                self.mark_for_destroy(Some(msg));
                return;
            }
        }
        if msg.has_extra_msg() {
            warn!(
                self.logger,
                "unimplemented extra msg, ignore it now";
                "extra_msg_type" => ?msg.get_extra_msg().get_type(),
            );
            return;
        }

        // TODO: drop all msg append when the peer is uninitialized and has conflict
        // ranges with other peers.
        let from_peer = msg.take_from_peer();
        let from_peer_id = from_peer.get_id();
        if from_peer_id != INVALID_ID {
            if self.is_leader() {
                self.add_peer_heartbeat(from_peer.get_id(), Instant::now());
            }
            // We only cache peer with an valid ID.
            // It prevents cache peer(0,0) which is sent by region split.
            self.insert_peer_cache(from_peer);
        }

        // Delay first append message and wait for split snapshot,
        // so that slow split does not trigger leader to send a snapshot.
        if !self.storage().is_initialized() {
            if is_initial_msg(msg.get_message()) {
                let mut is_overlapped = false;
                let meta = ctx.store_meta.lock().unwrap();
                meta.search_region(msg.get_start_key(), msg.get_end_key(), |_| {
                    is_overlapped = true;
                });
                self.split_pending_append_mut()
                    .set_range_overlapped(is_overlapped);
            } else if is_first_append_entry(msg.get_message())
                && !self.ready_to_handle_first_append_message(ctx, &msg)
            {
                return;
            }
        }

        let pre_committed_index = self.raft_group().raft.raft_log.committed;
        if msg.get_message().get_msg_type() == MessageType::MsgTransferLeader {
            self.on_transfer_leader_msg(ctx, msg.get_message(), msg.get_disk_usage())
        } else {
            // As this peer is already created, the empty split message is meaningless.
            if is_empty_split_message(&msg) {
                ctx.raft_metrics.message_dropped.stale_msg.inc();
                return;
            }

            let msg_type = msg.get_message().get_msg_type();
            // This can be a message that sent when it's still a follower. Nevertheleast,
            // it's meaningless to continue to handle the request as callbacks are cleared.
            if msg_type == MessageType::MsgReadIndex
                && self.is_leader()
                && (msg.get_message().get_from() == raft::INVALID_ID
                    || msg.get_message().get_from() == self.peer_id())
            {
                ctx.raft_metrics.message_dropped.stale_msg.inc();
                return;
            }

            if msg_type == MessageType::MsgReadIndex
                && self.is_leader()
                && self.on_step_read_index(ctx, msg.mut_message())
            {
                // Read index has respond in `on_step_read_index`,
                // No need to step again.
            } else if let Err(e) = self.raft_group_mut().step(msg.take_message()) {
                error!(self.logger, "raft step error";
                    "from_peer" => ?msg.get_from_peer(),
                    "region_epoch" => ?msg.get_region_epoch(),
                    "message_type" => ?msg_type,
                    "err" => ?e);
            } else {
                let committed_index = self.raft_group().raft.raft_log.committed;
                self.report_commit_log_duration(ctx, pre_committed_index, committed_index);
            }
        }

        // There are two different cases to check peers can be bring back.
        // 1. If the peer is pending, then only AppendResponse can bring it back to up.
        // 2. If the peer is down, then HeartbeatResponse and AppendResponse can bring
        // it back to up.
        if self.any_new_peer_catch_up(from_peer_id) {
            self.region_heartbeat_pd(ctx)
        }

        self.set_has_ready();
    }

    /// Callback for fetching logs asynchronously.
    pub fn on_raft_log_fetched(&mut self, fetched_logs: FetchedLogs) {
        let FetchedLogs { context, logs } = fetched_logs;
        let low = logs.low;
        // If the peer is not the leader anymore and it's not in entry cache warmup
        // state, or it is being destroyed, ignore the result.
        if !self.is_leader() && self.entry_storage().entry_cache_warmup_state().is_none()
            || !self.serving()
        {
            self.entry_storage_mut().clean_async_fetch_res(low);
            return;
        }
        if self.term() != logs.term {
            self.entry_storage_mut().clean_async_fetch_res(low);
        } else if self.entry_storage().entry_cache_warmup_state().is_some() {
            if self.entry_storage_mut().maybe_warm_up_entry_cache(*logs) {
                self.ack_transfer_leader_msg(false);
                self.set_has_ready();
            }
            self.entry_storage_mut().clean_async_fetch_res(low);
            return;
        } else {
            self.entry_storage_mut()
                .update_async_fetch_res(low, Some(logs));
        }
        self.raft_group_mut().on_entries_fetched(context);
        // clean the async fetch result immediately if not used to free memory
        self.entry_storage_mut().update_async_fetch_res(low, None);
        self.set_has_ready();
    }

    /// Partially filled a raft message that will be sent to other peer.
    fn prepare_raft_message(&mut self) -> RaftMessage {
        let mut raft_msg = RaftMessage::new();
        raft_msg.set_region_id(self.region().id);
        raft_msg.set_from_peer(self.peer().clone());
        // set current epoch
        let epoch = self.storage().region().get_region_epoch();
        let msg_epoch = raft_msg.mut_region_epoch();
        msg_epoch.set_version(epoch.get_version());
        msg_epoch.set_conf_ver(epoch.get_conf_ver());
        raft_msg
    }

    /// Transform a message from raft lib to a message that can be sent to other
    /// peers.
    ///
    /// If the recipient can't be found, `None` is returned.
    #[inline]
    fn build_raft_message(
        &mut self,
        msg: eraftpb::Message,
        disk_usage: DiskUsage,
    ) -> Option<RaftMessage> {
        let to_peer = match self.peer_from_cache(msg.to) {
            Some(p) => p,
            None => {
                warn!(
                    self.logger,
                    "failed to look up recipient peer";
                    "to_peer" => msg.to,
                    "message_type" => ?msg.msg_type
                );
                return None;
            }
        };

        let mut raft_msg = self.prepare_raft_message();
        // Fill in the disk usage.
        raft_msg.set_disk_usage(disk_usage);

        raft_msg.set_to_peer(to_peer);
        if msg.from != self.peer().id {
            debug!(
                self.logger,
                "redirecting message";
                "msg_type" => ?msg.get_msg_type(),
                "from" => msg.get_from(),
                "to" => msg.get_to(),
            );
        }

        // There could be two cases:
        // - Target peer already exists but has not established communication with
        //   leader yet
        // - Target peer is added newly due to member change or region split, but it's
        //   not created yet
        // For both cases the region start key and end key are attached in RequestVote
        // and Heartbeat message for the store of that peer to check whether to create a
        // new peer when receiving these messages, or just to wait for a pending region
        // split to perform later.
        if self.storage().is_initialized() && is_initial_msg(&msg) {
            let region = self.region();
            raft_msg.set_start_key(region.get_start_key().to_vec());
            raft_msg.set_end_key(region.get_end_key().to_vec());
        }

        raft_msg.set_message(msg);
        Some(raft_msg)
    }

    /// Send a message.
    ///
    /// The message is pushed into the send buffer, it may not be sent out until
    /// transport is flushed explicitly.
    pub(crate) fn send_raft_message<T: Transport>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        msg: RaftMessage,
    ) {
        let msg_type = msg.get_message().get_msg_type();
        let to_peer_id = msg.get_to_peer().get_id();
        let to_store_id = msg.get_to_peer().get_store_id();
        if msg_type == MessageType::MsgSnapshot {
            let index = msg.get_message().get_snapshot().get_metadata().get_index();
            self.update_last_sent_snapshot_index(index);
        }

        debug!(
            self.logger,
            "send raft msg";
            "msg_type" => ?msg_type,
            "msg_size" => msg.get_message().compute_size(),
            "to" => to_peer_id,
        );

        match ctx.trans.send(msg) {
            Ok(()) => ctx.raft_metrics.send_message.add(msg_type, true),
            Err(e) => {
                // We use metrics to observe failure on production.
                debug!(
                    self.logger,
                    "failed to send msg to other peer";
                    "target_peer_id" => to_peer_id,
                    "target_store_id" => to_store_id,
                    "err" => ?e,
                    "error_code" => %e.error_code(),
                );
                // unreachable store
                self.raft_group_mut().report_unreachable(to_peer_id);
                if msg_type == eraftpb::MessageType::MsgSnapshot {
                    self.raft_group_mut()
                        .report_snapshot(to_peer_id, SnapshotStatus::Failure);
                }
                ctx.raft_metrics.send_message.add(msg_type, false);
            }
        }
    }

    /// Send a message.
    ///
    /// The message is pushed into the send buffer, it may not be sent out until
    /// transport is flushed explicitly.
    fn send_raft_message_on_leader<T: Transport>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        msg: RaftMessage,
    ) {
        let message = msg.get_message();
        if message.get_msg_type() == MessageType::MsgAppend
            && let Some(fe) = message.get_entries().first()
            && let Some(le) = message.get_entries().last()
        {
            let last = (le.get_term(), le.get_index());
            let first = (fe.get_term(), fe.get_index());
            let now = Instant::now();
            let queue = self.proposals_mut().queue_mut();
            // Proposals are batched up, so it will liely hit after one or two steps.
            for p in queue.iter_mut().rev() {
                if p.sent {
                    break;
                }
                let cur = (p.term, p.index);
                if cur > last {
                    continue;
                }
                if cur < first {
                    break;
                }
                for tracker in p.cb.write_trackers() {
                    tracker.observe(now, &ctx.raft_metrics.wf_send_proposal, |t| {
                        &mut t.metrics.wf_send_proposal_nanos
                    });
                }
                p.sent = true;
            }
        }
        if message.get_msg_type() == MessageType::MsgTimeoutNow {
            // After a leader transfer procedure is triggered, the lease for
            // the old leader may be expired earlier than usual, since a new leader
            // may be elected and the old leader doesn't step down due to
            // network partition from the new leader.
            // For lease safety during leader transfer, transit `leader_lease`
            // to suspect.
            self.leader_lease_mut().suspect(monotonic_raw_now());
        }
        self.send_raft_message(ctx, msg)
    }

    fn handle_raft_committed_entries<T>(
        &mut self,
        ctx: &mut crate::batch::StoreContext<EK, ER, T>,
        committed_entries: Vec<raft::prelude::Entry>,
    ) {
        // TODO: skip handling committed entries if a snapshot is being applied
        // asynchronously.
        let mut update_lease = self.is_leader();
        if update_lease {
            for entry in committed_entries.iter().rev() {
                self.compact_log_context_mut()
                    .add_log_size(entry.get_data().len() as u64);
                if update_lease {
                    let propose_time = self
                        .proposals()
                        .find_propose_time(entry.get_term(), entry.get_index());
                    if let Some(propose_time) = propose_time {
                        // We must renew current_time because this value may be created a long time
                        // ago. If we do not renew it, this time may be
                        // smaller than propose_time of a command, which was
                        // proposed in another thread while this thread receives its
                        // AppendEntriesResponse and is ready to calculate its commit-log-duration.
                        let current_time = monotonic_raw_now();
                        ctx.current_time.replace(current_time);
                        ctx.raft_metrics.commit_log.observe(duration_to_sec(
                            (current_time - propose_time).to_std().unwrap(),
                        ));
                        self.maybe_renew_leader_lease(propose_time, &ctx.store_meta, None);
                        update_lease = false;
                    }
                }
            }
        }
        let applying_index = committed_entries.last().unwrap().index;
        let commit_to_current_term = committed_entries.last().unwrap().term == self.term();
        self.compact_log_context_mut()
            .set_last_applying_index(applying_index);
        if needs_evict_entry_cache(ctx.cfg.evict_cache_on_memory_ratio) {
            // Compact all cached entries instead of half evict.
            self.entry_storage_mut().evict_entry_cache(false);
        }
        self.schedule_apply_committed_entries(ctx, committed_entries);
        if self.is_leader()
            && commit_to_current_term
            && !self.proposal_control().has_uncommitted_admin()
        {
            self.raft_group_mut().skip_bcast_commit(true);
        }
    }

    /// Processing the ready of raft. A detail description of how it's handled
    /// can be found at https://docs.rs/raft/latest/raft/#processing-the-ready-state.
    ///
    /// It's should be called at the end of every round of processing. Any
    /// writes will be handled asynchronously, and be notified once writes
    /// are persisted.
    #[inline]
    pub fn handle_raft_ready<T: Transport>(&mut self, ctx: &mut StoreContext<EK, ER, T>) {
        let has_ready = self.reset_has_ready();
        let has_extra_write = self.reset_has_extra_write();
        if !has_ready || self.destroy_progress().started() {
            #[cfg(feature = "testexport")]
            self.async_writer.notify_flush();
            return;
        }
        ctx.has_ready = true;

        if !has_extra_write
            && !self.has_pending_messages()
            && !self.raft_group().has_ready()
            && (self.serving() || self.postponed_destroy())
        {
            self.maybe_schedule_gen_snapshot();
            #[cfg(feature = "testexport")]
            self.async_writer.notify_flush();
            return;
        }

        // Note even the group has no ready, we can still get an empty ready.

        debug!(self.logger, "handle raft ready");

        let mut ready = self.raft_group_mut().ready();
        // Update it after unstable entries pagination is introduced.
        debug_assert!(ready.entries().last().map_or_else(
            || true,
            |entry| entry.index == self.raft_group().raft.raft_log.last_index()
        ));

        fail::fail_point!(
            "before_handle_snapshot_ready_3",
            self.peer_id() == 3 && self.get_pending_snapshot().is_some(),
            |_| ()
        );

        self.on_role_changed(ctx, &ready);

        if let Some(hs) = ready.hs() {
            let prev_commit_index = self.entry_storage().commit_index();
            assert!(
                hs.get_commit() >= prev_commit_index,
                "{} {:?} {}",
                SlogFormat(&self.logger),
                hs,
                prev_commit_index
            );
            if self.is_leader() && hs.get_commit() > prev_commit_index {
                self.on_leader_commit_index_changed(hs.get_commit());
            }
        }

        if !ready.messages().is_empty() {
            debug_assert!(self.is_leader());
            let disk_usage = ctx.self_disk_usage;
            for msg in ready.take_messages() {
                if let Some(msg) = self.build_raft_message(msg, disk_usage) {
                    self.send_raft_message_on_leader(ctx, msg);
                }
            }
            if self.has_pending_messages() {
                for msg in self.take_pending_messages() {
                    self.send_raft_message_on_leader(ctx, msg);
                }
            }
        }

        self.apply_reads(ctx, &ready);
        if !ready.committed_entries().is_empty() {
            self.handle_raft_committed_entries(ctx, ready.take_committed_entries());
        }

        self.maybe_schedule_gen_snapshot();

        let ready_number = ready.number();
        let mut write_task = WriteTask::new(self.region_id(), self.peer_id(), ready_number);
        self.report_send_to_queue_duration(ctx, &mut write_task, ready.entries());
        let prev_persisted = self.storage().apply_trace().persisted_apply_index();
        self.merge_state_changes_to(&mut write_task);
        self.storage_mut()
            .handle_raft_ready(ctx, &mut ready, &mut write_task);
        self.try_complete_recovery();
        self.on_advance_persisted_apply_index(ctx, prev_persisted, &mut write_task);

        if !ready.persisted_messages().is_empty() {
            let disk_usage = ctx.self_disk_usage;
            write_task.messages = ready
                .take_persisted_messages()
                .into_iter()
                .flat_map(|m| self.build_raft_message(m, disk_usage))
                .collect();
        }
        if self.has_pending_messages() {
            if write_task.messages.is_empty() {
                write_task.messages = self.take_pending_messages();
            } else {
                write_task
                    .messages
                    .append(&mut self.take_pending_messages());
            }
        }
        if !self.serving() {
            self.start_destroy(ctx, &mut write_task);
            if self.persisted_index() != 0 {
                ctx.coprocessor_host.on_region_changed(
                    self.region(),
                    RegionChangeEvent::Destroy,
                    self.raft_group().raft.state,
                );
            }
        }
        // Ready number should increase monotonically.
        assert!(self.async_writer.known_largest_number() < ready.number());
        if let Some(task) = self.async_writer.write(ctx, write_task) {
            // So the task doesn't need to be process asynchronously, directly advance.
            let mut light_rd = self.raft_group_mut().advance_append(ready);
            if !task.messages.is_empty() {
                for m in task.messages {
                    self.send_raft_message(ctx, m);
                }
            }
            if !light_rd.messages().is_empty() || light_rd.commit_index().is_some() {
                slog_panic!(
                    self.logger,
                    "unexpected messages";
                    "messages_count" => ?light_rd.messages().len(),
                    "commit_index" => ?light_rd.commit_index()
                );
            }
            if !light_rd.committed_entries().is_empty() {
                self.handle_raft_committed_entries(ctx, light_rd.take_committed_entries());
            }
        } else {
            // The task will be written asynchronously. Once it's persisted, it will be
            // notified by `on_persisted`.
            self.raft_group_mut().advance_append_async(ready);
        }

        ctx.raft_metrics.ready.has_ready_region.inc();
        #[cfg(feature = "testexport")]
        self.async_writer.notify_flush();
    }

    /// Called when an asynchronously write finishes.
    pub fn on_persisted<T: Transport>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        peer_id: u64,
        ready_number: u64,
    ) {
        if peer_id != self.peer_id() {
            error!(self.logger, "peer id not matched"; "persisted_peer_id" => peer_id, "persisted_number" => ready_number);
            return;
        }
        let (persisted_message, flushed_epoch, has_snapshot) =
            self.async_writer
                .on_persisted(ctx, ready_number, &self.logger);
        for msgs in persisted_message {
            for msg in msgs {
                self.send_raft_message(ctx, msg);
            }
        }

        let persisted_number = self.async_writer.persisted_number();
        let pre_persisted_index = self.persisted_index();
        let pre_committed_index = self.raft_group().raft.raft_log.committed;
        self.raft_group_mut().on_persist_ready(persisted_number);
        let persisted_index = self.persisted_index();
        let committed_index = self.raft_group().raft.raft_log.committed;
        self.report_persist_log_duration(ctx, pre_persisted_index, persisted_index);
        self.report_commit_log_duration(ctx, pre_committed_index, committed_index);
        // The apply snapshot process order would be:
        // - Get the snapshot from the ready
        // - Wait for async writer to load this tablet
        // In this step, the snapshot loading has been finished, but some apply
        // state need to update.
        if has_snapshot {
            self.on_applied_snapshot(ctx);

            if self.unsafe_recovery_state().is_some() {
                debug!(self.logger, "unsafe recovery finishes applying a snapshot");
                self.check_unsafe_recovery_state(ctx);
            }
        }

        if let Some(flushed_epoch) = flushed_epoch {
            self.storage_mut().set_flushed_epoch(&flushed_epoch);
        }

        self.storage_mut()
            .entry_storage_mut()
            .update_cache_persisted(persisted_index);
        if let Some(idx) = self
            .storage_mut()
            .apply_trace_mut()
            .take_flush_index(ready_number)
        {
            let apply_index = self.flush_state().applied_index();
            self.cleanup_stale_ssts(ctx, DATA_CFS, idx, apply_index);
        }

        if self.is_in_force_leader() {
            // forward commit index, the committed entries will be applied in
            // the next raft tick round.
            self.maybe_force_forward_commit_index();
        }

        if !self.destroy_progress().started() {
            // We may need to check if there is persisted committed logs.
            self.set_has_ready();
        } else if self.async_writer.all_ready_persisted() {
            // Destroy ready is the last ready. All readies are persisted means destroy
            // is persisted.
            self.finish_destroy(ctx);
        }
    }

    #[inline]
    fn report_persist_log_duration<T>(
        &self,
        ctx: &mut StoreContext<EK, ER, T>,
        old_index: u64,
        new_index: u64,
    ) {
        if !ctx.cfg.waterfall_metrics || self.proposals().is_empty() || old_index >= new_index {
            return;
        }
        let now = Instant::now();
        for i in old_index + 1..=new_index {
            if let Some((term, trackers)) = self.proposals().find_trackers(i) {
                if self.entry_storage().term(i).map_or(false, |t| t == term) {
                    for tracker in trackers {
                        tracker.observe(now, &ctx.raft_metrics.wf_persist_log, |t| {
                            &mut t.metrics.wf_persist_log_nanos
                        });
                    }
                }
            }
        }
    }

    #[inline]
    fn report_commit_log_duration<T>(
        &self,
        ctx: &mut StoreContext<EK, ER, T>,
        old_index: u64,
        new_index: u64,
    ) {
        if !ctx.cfg.waterfall_metrics || self.proposals().is_empty() || old_index >= new_index {
            return;
        }
        let now = Instant::now();
        let health_stats = &mut ctx.raft_metrics.health_stats;
        for i in old_index + 1..=new_index {
            if let Some((term, trackers)) = self.proposals().find_trackers(i) {
                if self.entry_storage().term(i).map_or(false, |t| t == term) {
                    let commit_persisted = i <= self.persisted_index();
                    let hist = if commit_persisted {
                        &ctx.raft_metrics.wf_commit_log
                    } else {
                        &ctx.raft_metrics.wf_commit_not_persist_log
                    };
                    for tracker in trackers {
                        // Collect the metrics related to commit_log
                        // durations.
                        let duration = tracker.observe(now, hist, |t| {
                            t.metrics.commit_not_persisted = !commit_persisted;
                            &mut t.metrics.wf_commit_log_nanos
                        });
                        health_stats.observe(Duration::from_nanos(duration), IoType::Network);
                    }
                }
            }
        }
    }

    #[inline]
    fn report_send_to_queue_duration<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        write_task: &mut WriteTask<EK, ER>,
        entries: &[raft::eraftpb::Entry],
    ) {
        if !ctx.cfg.waterfall_metrics || self.proposals().is_empty() {
            return;
        }
        let now = Instant::now();
        for entry in entries {
            if let Some((term, trackers)) = self.proposals().find_trackers(entry.index) {
                if entry.term == term {
                    for tracker in trackers {
                        write_task.trackers.push(*tracker);
                        tracker.observe(now, &ctx.raft_metrics.wf_send_to_queue, |t| {
                            &mut t.metrics.wf_send_to_queue_nanos
                        });
                    }
                }
            }
        }
    }

    #[cfg(feature = "testexport")]
    pub fn on_wait_flush(&mut self, ch: crate::router::FlushChannel) {
        self.async_writer.subscribe_flush(ch);
    }

    pub fn on_role_changed<T>(&mut self, ctx: &mut StoreContext<EK, ER, T>, ready: &Ready) {
        // Update leader lease when the Raft state changes.
        if let Some(ss) = ready.ss() {
            let term = self.term();
            match ss.raft_state {
                StateRole::Leader => {
                    // The local read can only be performed after a new leader has applied
                    // the first empty entry on its term. After that the lease expiring time
                    // should be updated to
                    //   send_to_quorum_ts + max_lease
                    // as the comments in `Lease` explain.
                    // It is recommended to update the lease expiring time right after
                    // this peer becomes leader because it's more convenient to do it here and
                    // it has no impact on the correctness.
                    let progress_term = ReadProgress::term(term);
                    self.maybe_renew_leader_lease(
                        monotonic_raw_now(),
                        &ctx.store_meta,
                        Some(progress_term),
                    );
                    debug!(
                        self.logger,
                        "becomes leader with lease";
                        "lease" => ?self.leader_lease(),
                    );
                    // If the predecessor reads index during transferring leader and receives
                    // quorum's heartbeat response after that, it may wait for applying to
                    // current term to apply the read. So broadcast eagerly to avoid unexpected
                    // latency.
                    self.raft_group_mut().skip_bcast_commit(false);
                    self.update_last_sent_snapshot_index(
                        self.raft_group().raft.raft_log.last_index(),
                    );

                    self.txn_context().on_became_leader(
                        ctx,
                        self.term(),
                        self.region(),
                        &self.logger,
                    );

                    // Exit entry cache warmup state when the peer becomes leader.
                    self.entry_storage_mut().clear_entry_cache_warmup_state();

                    if !ctx.store_disk_usages.is_empty() {
                        self.refill_disk_full_peers(ctx);
                        debug!(
                            self.logger,
                            "become leader refills disk full peers to {:?}",
                            self.abnormal_peer_context().disk_full_peers();
                            "region_id" => self.region_id(),
                        );
                    }

                    self.region_heartbeat_pd(ctx);
                    self.add_pending_tick(PeerTick::CompactLog);
                    self.add_pending_tick(PeerTick::SplitRegionCheck);
                    self.add_pending_tick(PeerTick::CheckLongUncommitted);
                    self.add_pending_tick(PeerTick::ReportBuckets);
                    self.add_pending_tick(PeerTick::CheckLeaderLease);
                    self.maybe_schedule_gc_peer_tick();
                }
                StateRole::Follower => {
                    self.expire_lease_on_became_follower(&ctx.store_meta);
                    self.storage_mut().cancel_generating_snap(None);
                    self.txn_context()
                        .on_became_follower(self.term(), self.region());
                    self.update_merge_progress_on_became_follower();
                }
                _ => {}
            }

            if self.is_in_force_leader() && ss.raft_state != StateRole::Leader {
                // for some reason, it's not leader anymore
                info!(self.logger,
                    "step down in force leader state";
                    "state" => ?ss.raft_state,
                );
                self.on_force_leader_fail();
            }

            self.read_progress()
                .update_leader_info(ss.leader_id, term, self.region());
            let target = self.refresh_leader_transferee();
            ctx.coprocessor_host.on_role_change(
                self.region(),
                RoleChange {
                    state: ss.raft_state,
                    leader_id: ss.leader_id,
                    prev_lead_transferee: target,
                    vote: self.raft_group().raft.vote,
                    initialized: self.storage().is_initialized(),
                    peer_id: self.peer().get_id(),
                },
            );
            self.proposal_control_mut().maybe_update_term(term);
        }
    }

    /// If leader commits new admin commands, it may break lease assumption. So
    /// we need to cancel lease whenever necessary.
    ///
    /// Note this method should be called before sending out any messages.
    fn on_leader_commit_index_changed(&mut self, commit_index: u64) {
        let mut committed_prepare_merge = false;
        self.proposal_control_mut().commit_to(commit_index, |cmd| {
            committed_prepare_merge |= cmd.cmd_type() == AdminCmdType::PrepareMerge
        });
        // There are two types of operations that will change the ownership of a range:
        // split and merge.
        //
        // - For split, after the split command is committed, it's
        // possible that the same range is govened by different region on different
        // nodes due to different apply progress. But because only the peers on the
        // same node as old leader will campaign despite election timeout, so there
        // will be no modification to the overlapped range until either the original
        // leader apply the split command or an election timeout is passed since split
        // is committed. We already forbid renewing lease after committing split, and
        // original leader will update the reader delegate with latest epoch after
        // applying split before the split peer starts campaign, so what needs to be
        // done are 1. mark split is committed, which is done by `commit_to` above,
        // 2. make sure split result is invisible until epoch is updated or reader may
        // miss data from the new tablet. This is done by always publish tablet in
        // `on_apply_res_split`. So it's correct to allow local read during split.
        //
        // - For merge, after the prepare merge command is committed, the target peers
        // may apply commit merge at any time, so we need to forbid any type of read
        // to avoid missing the modifications from target peers.
        if committed_prepare_merge {
            // After prepare_merge is committed and the leader broadcasts commit
            // index to followers, the leader can not know when the target region
            // merges majority of this region, also it can not know when the target
            // region writes new values.
            // To prevent unsafe local read, we suspect its leader lease.
            self.leader_lease_mut().suspect(monotonic_raw_now());
            // Stop updating `safe_ts`
            self.read_progress_mut().discard();
        }
    }

    /// Check if there is long uncommitted proposal.
    ///
    /// This will increase the threshold when a long uncommitted proposal is
    /// detected, and reset the threshold when there is no long uncommitted
    /// proposal.
    fn has_long_uncommitted_proposals<T>(&mut self, ctx: &mut StoreContext<EK, ER, T>) -> bool {
        let mut has_long_uncommitted = false;
        let base_threshold = ctx.cfg.long_uncommitted_base_threshold.0;
        if let Some(propose_time) = self.proposals().oldest().and_then(|p| p.propose_time) {
            // When a proposal was proposed with this ctx before, the current_time can be
            // some.
            let current_time = *ctx.current_time.get_or_insert_with(monotonic_raw_now);
            let elapsed = match (current_time - propose_time).to_std() {
                Ok(elapsed) => elapsed,
                Err(_) => return false,
            };
            // Increase the threshold for next turn when a long uncommitted proposal is
            // detected.
            let threshold = self.long_uncommitted_threshold();
            if elapsed >= threshold {
                has_long_uncommitted = true;
                self.set_long_uncommitted_threshold(threshold + base_threshold);
            } else if elapsed < base_threshold {
                self.set_long_uncommitted_threshold(base_threshold);
            }
        } else {
            self.set_long_uncommitted_threshold(base_threshold);
        }
        has_long_uncommitted
    }

    fn check_long_uncommitted_proposals<T>(&mut self, ctx: &mut StoreContext<EK, ER, T>) {
        fail::fail_point!(
            "on_check_long_uncommitted_proposals_1",
            self.peer_id() == 1,
            |_| {}
        );
        if self.has_long_uncommitted_proposals(ctx) {
            let status = self.raft_group().status();
            let mut buffer: Vec<(u64, u64, u64)> = Vec::new();
            if let Some(prs) = status.progress {
                for (id, p) in prs.iter() {
                    buffer.push((*id, p.commit_group_id, p.matched));
                }
            }
            warn!(
                self.logger,
                "found long uncommitted proposals";
                "progress" => ?buffer,
                "cache_first_index" => ?self.entry_storage().entry_cache_first_index(),
                "next_turn_threshold" => ?self.long_uncommitted_threshold(),
            );
        }
    }

    fn handle_reported_disk_usage<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        msg: &RaftMessage,
    ) {
        let store_id = msg.get_from_peer().get_store_id();
        let peer_id = msg.get_from_peer().get_id();
        let disk_full_peers = self.abnormal_peer_context().disk_full_peers();
        let refill_disk_usages = if matches!(msg.get_disk_usage(), DiskUsage::Normal) {
            ctx.store_disk_usages.remove(&store_id);
            if !self.is_leader() {
                return;
            }
            disk_full_peers.has(peer_id)
        } else {
            ctx.store_disk_usages.insert(store_id, msg.get_disk_usage());
            if !self.is_leader() {
                return;
            }

            disk_full_peers.is_empty()
                || disk_full_peers
                    .get(peer_id)
                    .map_or(true, |x| x != msg.get_disk_usage())
        };

        if refill_disk_usages || self.has_region_merge_proposal {
            let prev = disk_full_peers.get(peer_id);
            if Some(msg.get_disk_usage()) != prev {
                info!(
                    self.logger,
                    "reported disk usage changes {:?} -> {:?}", prev, msg.get_disk_usage();
                    "region_id" => self.region_id(),
                    "peer_id" => peer_id,
                );
            }
            self.refill_disk_full_peers(ctx);
            debug!(
                self.logger,
                "raft message refills disk full peers to {:?}",
                self.abnormal_peer_context().disk_full_peers();
                "region_id" => self.region_id(),
            );
        }
    }
}

impl<EK: KvEngine, ER: RaftEngine> Storage<EK, ER> {
    /// Apply the ready to the storage. If there is any states need to be
    /// persisted, it will be written to `write_task`.
    fn handle_raft_ready<T: Transport>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        ready: &mut Ready,
        write_task: &mut WriteTask<EK, ER>,
    ) {
        let prev_raft_state = self.entry_storage().raft_state().clone();
        let prev_ever_persisted = self.ever_persisted();

        if !ready.snapshot().is_empty() {
            if let Err(e) = self.apply_snapshot(
                ready.snapshot(),
                write_task,
                &ctx.snap_mgr,
                &ctx.tablet_registry,
            ) {
                SNAP_COUNTER.apply.fail.inc();
                error!(self.logger(),"failed to apply snapshot";"error" => ?e)
            }
        }

        if !ready.entries().is_empty() {
            assert!(self.ever_persisted(), "{}", SlogFormat(self.logger()));
            self.entry_storage_mut()
                .append(ready.take_entries(), write_task);
        }
        if let Some(hs) = ready.hs() {
            self.entry_storage_mut()
                .raft_state_mut()
                .set_hard_state(hs.clone());
        }
        let entry_storage = self.entry_storage();
        if !prev_ever_persisted || prev_raft_state != *entry_storage.raft_state() {
            write_task.raft_state = Some(entry_storage.raft_state().clone());
        }
        // If snapshot initializes the peer (in `apply_snapshot`), we don't need to
        // write apply trace again.
        if !self.ever_persisted() {
            let region_id = self.region().get_id();
            let raft_engine = entry_storage.raft_engine();
            if write_task.raft_wb.is_none() {
                write_task.raft_wb = Some(raft_engine.log_batch(64));
            }
            let wb = write_task.raft_wb.as_mut().unwrap();
            // There may be tombstone key from last peer.
            raft_engine
                .clean(region_id, 0, entry_storage.raft_state(), wb)
                .unwrap_or_else(|e| {
                    slog_panic!(self.logger(), "failed to clean up region"; "error" => ?e);
                });
            self.init_apply_trace(write_task);
            self.set_ever_persisted();
        }
        if self.apply_trace().should_persist() {
            self.record_apply_trace(write_task);
        }
    }
}
