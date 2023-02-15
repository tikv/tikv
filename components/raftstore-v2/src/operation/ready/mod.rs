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

use std::{cmp, time::Instant};

use engine_traits::{KvEngine, RaftEngine};
use error_code::ErrorCodeExt;
use kvproto::{
    raft_cmdpb::AdminCmdType,
    raft_serverpb::{ExtraMessageType, RaftMessage},
};
use protobuf::Message as _;
use raft::{eraftpb, prelude::MessageType, Ready, StateRole, INVALID_ID};
use raftstore::{
    coprocessor::{RegionChangeEvent, RoleChange},
    store::{
        needs_evict_entry_cache, util, worker_metrics::SNAP_COUNTER, FetchedLogs, ReadProgress,
        Transport, WriteCallback, WriteTask,
    },
};
use slog::{debug, error, info, trace, warn};
use tikv_util::{
    log::SlogFormat,
    slog_panic,
    store::find_peer,
    time::{duration_to_sec, monotonic_raw_now},
};

pub use self::{
    apply_trace::{write_initial_states, ApplyTrace, DataTrace, StateStorage},
    async_writer::AsyncWriter,
    snapshot::{GenSnapTask, SnapState},
};
use crate::{
    batch::StoreContext,
    fsm::{PeerFsmDelegate, Store},
    raft::{Peer, Storage},
    router::{PeerMsg, PeerTick},
    worker::tablet_gc,
};

const PAUSE_FOR_RECOVERY_GAP: u64 = 128;

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
        if self.fsm.peer_mut().tick() {
            self.fsm.peer_mut().set_has_ready();
        }
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
    pub fn maybe_pause_for_recovery<T>(&mut self, store_ctx: &mut StoreContext<EK, ER, T>) -> bool {
        // The task needs to be scheduled even if the tablet may be replaced during
        // recovery. Otherwise if there are merges during recovery, the FSM may
        // be paused forever.
        if self.storage().has_dirty_data() {
            let region_id = self.region_id();
            let mailbox = store_ctx.router.mailbox(region_id).unwrap();
            let tablet_index = self.storage().tablet_index();
            let _ = store_ctx
                .schedulers
                .tablet_gc
                .schedule(tablet_gc::Task::trim(
                    self.tablet().unwrap().clone(),
                    self.region(),
                    move || {
                        let _ = mailbox.force_send(PeerMsg::TabletTrimmed { tablet_index });
                    },
                ));
        }
        let entry_storage = self.storage().entry_storage();
        let committed_index = entry_storage.commit_index();
        let applied_index = entry_storage.applied_index();
        if committed_index > applied_index {
            // Unlike v1, it's a must to set ready when there are pending entries. Otherwise
            // it may block for ever when there is unapplied conf change.
            self.set_has_ready();
        }
        if committed_index > applied_index + PAUSE_FOR_RECOVERY_GAP {
            // If there are too many the missing logs, we need to skip ticking otherwise
            // it may block the raftstore thread for a long time in reading logs for
            // election timeout.
            info!(self.logger, "pause for recovery"; "applied" => applied_index, "committed" => committed_index);
            self.set_pause_for_recovery(true);
            true
        } else {
            false
        }
    }

    #[inline]
    fn tick(&mut self) -> bool {
        // When it's handling snapshot, it's pointless to tick as all the side
        // affects have to wait till snapshot is applied. On the other hand, ticking
        // will bring other corner cases like elections.
        !self.is_handling_snapshot() && self.raft_group_mut().tick()
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

    pub fn on_raft_message<T: Transport>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        mut msg: Box<RaftMessage>,
    ) {
        debug!(
            self.logger,
            "handle raft message";
            "message_type" => %util::MsgType(&msg),
            "from_peer_id" => msg.get_from_peer().get_id(),
            "to_peer_id" => msg.get_to_peer().get_id(),
        );
        if self.pause_for_recovery() && msg.get_message().get_msg_type() == MessageType::MsgAppend {
            ctx.raft_metrics.message_dropped.recovery.inc();
            return;
        }
        if !self.serving() {
            return;
        }
        if util::is_vote_msg(msg.get_message()) && self.maybe_gc_sender(&msg) {
            return;
        }
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
                self.mark_for_destroy(Some(msg));
                return;
            }
        }
        if msg.has_extra_msg() {
            unimplemented!();
        }

        // TODO: drop all msg append when the peer is uninitialized and has conflict
        // ranges with other peers.
        let from_peer = msg.take_from_peer();
        if self.is_leader() && from_peer.get_id() != INVALID_ID {
            self.add_peer_heartbeat(from_peer.get_id(), Instant::now());
        }
        self.insert_peer_cache(from_peer);
        let pre_committed_index = self.raft_group().raft.raft_log.committed;
        if msg.get_message().get_msg_type() == MessageType::MsgTransferLeader {
            self.on_transfer_leader_msg(ctx, msg.get_message(), msg.disk_usage)
        } else if let Err(e) = self.raft_group_mut().step(msg.take_message()) {
            error!(self.logger, "raft step error"; "err" => ?e);
        } else {
            let committed_index = self.raft_group().raft.raft_log.committed;
            self.report_commit_log_duration(ctx, pre_committed_index, committed_index);
        }

        self.set_has_ready();
    }

    /// Callback for fetching logs asynchronously.
    pub fn on_logs_fetched(&mut self, fetched_logs: FetchedLogs) {
        let FetchedLogs { context, logs } = fetched_logs;
        let low = logs.low;
        if !self.is_leader() {
            self.entry_storage_mut().clean_async_fetch_res(low);
            return;
        }
        if self.term() != logs.term {
            self.entry_storage_mut().clean_async_fetch_res(low);
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
    fn build_raft_message(&mut self, msg: eraftpb::Message) -> Option<RaftMessage> {
        let to_peer = match self.peer_from_cache(msg.to) {
            Some(p) => p,
            None => {
                warn!(self.logger, "failed to look up recipient peer"; "to_peer" => msg.to);
                return None;
            }
        };

        let mut raft_msg = self.prepare_raft_message();

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
        raft_msg.set_message(msg);
        Some(raft_msg)
    }

    /// Send a message.
    ///
    /// The message is pushed into the send buffer, it may not be sent out until
    /// transport is flushed explicitly.
    fn send_raft_message<T: Transport>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        msg: RaftMessage,
    ) {
        let msg_type = msg.get_message().get_msg_type();
        let to_peer_id = msg.get_to_peer().get_id();
        let to_store_id = msg.get_to_peer().get_store_id();

        trace!(
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
            for msg in ready.take_messages() {
                if let Some(msg) = self.build_raft_message(msg) {
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
        self.on_advance_persisted_apply_index(ctx, prev_persisted, &mut write_task);

        if !ready.persisted_messages().is_empty() {
            write_task.messages = ready
                .take_persisted_messages()
                .into_iter()
                .flat_map(|m| self.build_raft_message(m))
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
        let (persisted_message, has_snapshot) =
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
        }

        self.storage_mut()
            .entry_storage_mut()
            .update_cache_persisted(persisted_index);
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
        from: u64,
        to: u64,
    ) {
        if !ctx.cfg.waterfall_metrics || self.proposals().is_empty() || from >= to {
            return;
        }
        let now = Instant::now();
        for i in from + 1..to {
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
    fn report_commit_log_duration<T>(&self, ctx: &mut StoreContext<EK, ER, T>, from: u64, to: u64) {
        if !ctx.cfg.waterfall_metrics || self.proposals().is_empty() || from >= to {
            return;
        }
        let now = Instant::now();
        for i in from + 1..to {
            if let Some((term, trackers)) = self.proposals().find_trackers(i) {
                if self.entry_storage().term(i).map_or(false, |t| t == term) {
                    let commit_persisted = i <= self.persisted_index();
                    let hist = if commit_persisted {
                        &ctx.raft_metrics.wf_commit_log
                    } else {
                        &ctx.raft_metrics.wf_commit_not_persist_log
                    };
                    for tracker in trackers {
                        tracker.observe(now, hist, |t| {
                            t.metrics.commit_not_persisted = !commit_persisted;
                            &mut t.metrics.wf_commit_log_nanos
                        });
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
        self.async_writer.subscirbe_flush(ch);
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

                    self.txn_context().on_became_leader(
                        ctx,
                        self.term(),
                        self.region(),
                        &self.logger,
                    );

                    // Exit entry cache warmup state when the peer becomes leader.
                    self.entry_storage_mut().clear_entry_cache_warmup_state();

                    self.region_heartbeat_pd(ctx);
                    self.add_pending_tick(PeerTick::CompactLog);
                    self.add_pending_tick(PeerTick::SplitRegionCheck);
                    self.add_pending_tick(PeerTick::CheckLongUncommitted);
                    self.maybe_schedule_gc_peer_tick();
                }
                StateRole::Follower => {
                    self.leader_lease_mut().expire();
                    self.storage_mut().cancel_generating_snap(None);
                    self.txn_context()
                        .on_became_follower(self.term(), self.region());
                }
                _ => {}
            }
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
        let ever_persisted = self.ever_persisted();

        if !ready.snapshot().is_empty() {
            if let Err(e) = self.apply_snapshot(
                ready.snapshot(),
                write_task,
                ctx.snap_mgr.clone(),
                ctx.tablet_registry.clone(),
            ) {
                SNAP_COUNTER.apply.fail.inc();
                error!(self.logger(),"failed to apply snapshot";"error" => ?e)
            }
        }

        let entry_storage = self.entry_storage_mut();
        if !ready.entries().is_empty() {
            entry_storage.append(ready.take_entries(), write_task);
        }
        if let Some(hs) = ready.hs() {
            entry_storage.raft_state_mut().set_hard_state(hs.clone());
        }
        if !ever_persisted || prev_raft_state != *entry_storage.raft_state() {
            write_task.raft_state = Some(entry_storage.raft_state().clone());
        }
        // If snapshot initializes the peer, we don't need to write apply trace again.
        if !self.ever_persisted() {
            self.init_apply_trace(write_task);
            self.set_ever_persisted();
        }
        if self.apply_trace().should_persist() {
            self.record_apply_trace(write_task);
        }
    }
}
