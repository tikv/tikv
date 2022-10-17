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

mod async_writer;

use std::cmp;

use engine_traits::{KvEngine, RaftEngine};
use error_code::ErrorCodeExt;
use kvproto::raft_serverpb::RaftMessage;
use protobuf::Message as _;
use raft::{eraftpb, Ready};
use raftstore::store::{util, ExtraStates, FetchedLogs, Transport, WriteTask};
use slog::{debug, error, trace, warn};

pub use self::async_writer::AsyncWriter;
use crate::{
    batch::StoreContext,
    fsm::PeerFsmDelegate,
    raft::{Peer, Storage},
    router::PeerTick,
};
impl<'a, EK: KvEngine, ER: RaftEngine, T: Transport> PeerFsmDelegate<'a, EK, ER, T> {
    /// Raft relies on periodic ticks to keep the state machine sync with other
    /// peers.
    pub fn on_raft_tick(&mut self) {
        if self.fsm.peer_mut().tick() {
            self.fsm.peer_mut().set_has_ready();
        }
        self.schedule_tick(PeerTick::Raft);
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    #[inline]
    fn tick(&mut self) -> bool {
        self.raft_group_mut().tick()
    }

    pub fn on_raft_message<T>(
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
        if !self.serving() {
            return;
        }
        if msg.get_to_peer().get_store_id() != self.peer().get_store_id() {
            ctx.raft_metrics.message_dropped.mismatch_store_id.inc();
            return;
        }
        if !msg.has_region_epoch() {
            ctx.raft_metrics.message_dropped.mismatch_region_epoch.inc();
            return;
        }
        if msg.get_is_tombstone() {
            self.mark_for_destroy(None);
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
            // return;
        }
        // TODO: drop all msg append when the peer is uninitialized and has conflict
        // ranges with other peers.
        self.insert_peer_cache(msg.take_from_peer());
        if let Err(e) = self.raft_group_mut().step(msg.take_message()) {
            error!(self.logger, "raft step error"; "err" => ?e);
        }
        self.set_has_ready();
    }

    /// Callback for fetching logs asynchronously.
    pub fn on_fetched_logs(&mut self, fetched_logs: FetchedLogs) {
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
    fn build_raft_message<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        msg: eraftpb::Message,
    ) -> Option<RaftMessage> {
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

    fn handle_raft_committed_entries<T>(
        &mut self,
        ctx: &mut crate::batch::StoreContext<EK, ER, T>,
        committed_entries: Vec<raft::prelude::Entry>,
    ) {
        // TODO: skip handling committed entries if a snapshot is being applied
        // asynchronously.
        if self.is_leader() {
            // TODO: Update lease
        }
        self.schedule_apply_committed_entries(ctx, committed_entries);
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
        if !has_ready || self.destroy_progress().started() {
            return;
        }
        ctx.has_ready = true;

        if !self.raft_group().has_ready() && (self.serving() || self.postpond_destroy()) {
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

        if !ready.messages().is_empty() {
            debug_assert!(self.is_leader());
            for msg in ready.take_messages() {
                if let Some(msg) = self.build_raft_message(ctx, msg) {
                    self.send_raft_message(ctx, msg);
                }
            }
        }

        self.apply_reads(ctx, &ready);
        if !ready.committed_entries().is_empty() {
            self.handle_raft_committed_entries(ctx, ready.take_committed_entries());
        }

        let ready_number = ready.number();
        let mut write_task = WriteTask::new(self.region_id(), self.peer_id(), ready_number);
        self.storage_mut()
            .handle_raft_ready(&mut ready, &mut write_task);
        if !ready.persisted_messages().is_empty() {
            write_task.messages = ready
                .take_persisted_messages()
                .into_iter()
                .flat_map(|m| self.build_raft_message(ctx, m))
                .collect();
        }
        if !self.serving() {
            self.start_destroy(&mut write_task);
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
                panic!(
                    "{:?} unexpected messages [{}] commit index [{:?}]",
                    self.logger.list(),
                    light_rd.messages().len(),
                    light_rd.commit_index()
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
        let persisted_message = self
            .async_writer
            .on_persisted(ctx, ready_number, &self.logger);
        for msgs in persisted_message {
            for msg in msgs {
                self.send_raft_message(ctx, msg);
            }
        }
        let persisted_number = self.async_writer.persisted_number();
        self.raft_group_mut().on_persist_ready(persisted_number);
        let persisted_index = self.raft_group().raft.raft_log.persisted;
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
}

impl<ER: RaftEngine> Storage<ER> {
    /// Apply the ready to the storage. If there is any states need to be
    /// persisted, it will be written to `write_task`.
    fn handle_raft_ready<EK: KvEngine>(
        &mut self,
        ready: &mut Ready,
        write_task: &mut WriteTask<EK, ER>,
    ) {
        let prev_raft_state = self.entry_storage().raft_state().clone();
        let ever_persisted = self.ever_persisted();

        // TODO: handle snapshot

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
        if !ever_persisted {
            let mut extra_states = ExtraStates::new(self.apply_state().clone());
            extra_states.set_region_state(self.region_state().clone());
            write_task.extra_write.set_v2(extra_states);
            self.set_ever_persisted();
        }
    }
}
