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

use std::cmp;

use batch_system::BasicMailbox;
use crossbeam::channel::TrySendError;
use engine_traits::{KvEngine, RaftEngine};
use kvproto::{
    metapb::Region,
    raft_serverpb::{PeerState, RaftMessage},
};
use raftstore::store::{util, ExtraStates, WriteTask};
use slog::{debug, error, info};

use crate::{
    batch::StoreContext,
    fsm::{PeerFsm, Store, StoreFsmDelegate},
    raft::{Peer, Storage},
    router::PeerMsg,
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

impl Store {
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
    {
        let region_id = msg.get_region_id();
        // The message can be sent when the peer is being created, so try send it first.
        let msg = if let Err(TrySendError::Disconnected(PeerMsg::RaftMessage(m))) =
            ctx.router.send(region_id, PeerMsg::RaftMessage(msg))
        {
            m
        } else {
            return;
        };
        let msg_type = msg.get_message().get_msg_type();
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
        // TODO: maybe we need to ack the message to confirm the peer is destroyed.
        if msg.get_is_tombstone() || msg.has_merge_target() {
            // Target tombstone peer doesn't exist, so ignore it.
            ctx.raft_metrics.message_dropped.stale_msg.inc();
            return;
        }
        let from_epoch = msg.get_region_epoch();
        let local_state = match ctx.engine.get_region_state(region_id) {
            Ok(s) => s,
            Err(e) => {
                error!(self.logger(), "failed to get region state"; "region_id" => region_id, "err" => ?e);
                return;
            }
        };
        if let Some(local_state) = local_state {
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
            if util::is_epoch_stale(from_epoch, local_epoch) {
                ctx.raft_metrics.message_dropped.region_tombstone_peer.inc();
                return;
            }
            if let Some(local_peer) = util::find_peer(local_state.get_region(), self.store_id()) {
                if to_peer.id <= local_peer.get_id() {
                    ctx.raft_metrics.message_dropped.region_tombstone_peer.inc();
                    return;
                }
            }
        }

        // So the peer must need to be created. We don't need to synchronous with split
        // as split won't create peer in v2. And we don't check for range
        // conflict as v2 depends on tablet, which allows conflict ranges.
        let mut region = Region::default();
        region.set_id(region_id);
        region.set_region_epoch(from_epoch.clone());
        // Peer list doesn't have to be complete, as it's uninitialized.
        region.mut_peers().push(from_peer.clone());
        region.mut_peers().push(to_peer.clone());
        // We don't set the region range here as we allow range conflict.
        let (tx, fsm) = match Storage::uninit(
            self.store_id(),
            region,
            ctx.engine.clone(),
            ctx.log_fetch_scheduler.clone(),
            &ctx.logger,
        )
        .and_then(|s| PeerFsm::new(&ctx.cfg, &*ctx.tablet_factory, s))
        {
            Ok(p) => p,
            res => {
                error!(self.logger(), "failed to create peer"; "region_id" => region_id, "peer_id" => to_peer.id, "err" => ?res.err());
                return;
            }
        };
        let mailbox = BasicMailbox::new(tx, fsm, ctx.router.state_cnt().clone());
        if let Err((p, _)) = ctx
            .router
            .send_and_register(region_id, mailbox, PeerMsg::Start)
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
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
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
    pub fn postpond_destroy(&self) -> bool {
        let entry_storage = self.storage().entry_storage();
        // TODO: check actual split index instead of commit index.
        entry_storage.applied_index() != entry_storage.commit_index()
    }

    /// Start the destroy progress. It will write `Tombstone` state
    /// asynchronously.
    ///
    /// After destroy is finished, `finish_destroy` should be called to clean up
    /// memory states.
    pub fn start_destroy(&mut self, write_task: &mut WriteTask<EK, ER>) {
        let entry_storage = self.storage().entry_storage();
        if self.postpond_destroy() {
            return;
        }
        let first_index = entry_storage.first_index();
        let last_index = entry_storage.last_index();
        if first_index <= last_index {
            write_task.cut_logs = match write_task.cut_logs {
                None => Some((first_index, last_index)),
                Some((f, l)) => Some((cmp::min(first_index, f), cmp::max(last_index, l))),
            };
        }
        let mut extra_states = ExtraStates::new(entry_storage.apply_state().clone());
        let mut region_state = self.storage().region_state().clone();
        // Write worker will do the clean up when meeting tombstone state.
        region_state.set_state(PeerState::Tombstone);
        extra_states.set_region_state(region_state);
        extra_states.set_raft_state(entry_storage.raft_state().clone());
        write_task.extra_write.set_v2(extra_states);
        self.destroy_progress_mut().start();
    }

    /// Do clean up for destroy. The peer is permanently destroyed when
    /// Tombstone state is persisted. This method is only for cleaning up
    /// memory states.
    pub fn finish_destroy<T>(&mut self, ctx: &mut StoreContext<EK, ER, T>) {
        info!(self.logger, "peer destroyed");
        ctx.router.close(self.region_id());
        if let Some(msg) = self.destroy_progress_mut().finish() {
            // The message will be dispatched to store fsm, which will create a
            // new peer. Ignore error as it's just a best effort.
            let _ = ctx.router.send_raft_message(msg);
        }
        // TODO: close apply mailbox.
    }
}
