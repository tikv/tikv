// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! This module contains the peer implementation for batch system.

use std::borrow::Cow;

use batch_system::{BasicMailbox, Fsm};
use crossbeam::channel::TryRecvError;
use engine_traits::{KvEngine, RaftEngine, TabletRegistry};
use raftstore::store::{Config, LocksStatus, TabletSnapManager, Transport};
use slog::{debug, error, info, trace, Logger};
use tikv_util::{
    is_zero_duration,
    mpsc::{self, LooseBoundedSender, Receiver},
    time::{duration_to_sec, Instant},
};

use crate::{
    batch::StoreContext,
    raft::{Peer, Storage},
    router::{PeerMsg, PeerTick},
    Result,
};

pub type SenderFsmPair<EK, ER> = (LooseBoundedSender<PeerMsg>, Box<PeerFsm<EK, ER>>);

pub struct PeerFsm<EK: KvEngine, ER: RaftEngine> {
    peer: Peer<EK, ER>,
    mailbox: Option<BasicMailbox<PeerFsm<EK, ER>>>,
    receiver: Receiver<PeerMsg>,
    /// A registry for all scheduled ticks. This can avoid scheduling ticks
    /// twice accidentally.
    tick_registry: [bool; PeerTick::VARIANT_COUNT],
    is_stopped: bool,
    reactivate_memory_lock_ticks: usize,
}

impl<EK: KvEngine, ER: RaftEngine> PeerFsm<EK, ER> {
    pub fn new(
        cfg: &Config,
        tablet_registry: &TabletRegistry<EK>,
        snap_mgr: &TabletSnapManager,
        storage: Storage<EK, ER>,
    ) -> Result<SenderFsmPair<EK, ER>> {
        let peer = Peer::new(cfg, tablet_registry, snap_mgr, storage)?;
        info!(peer.logger, "create peer");
        let (tx, rx) = mpsc::loose_bounded(cfg.notify_capacity);
        let fsm = Box::new(PeerFsm {
            peer,
            mailbox: None,
            receiver: rx,
            tick_registry: [false; PeerTick::VARIANT_COUNT],
            is_stopped: false,
            reactivate_memory_lock_ticks: 0,
        });
        Ok((tx, fsm))
    }

    #[inline]
    pub fn peer(&self) -> &Peer<EK, ER> {
        &self.peer
    }

    #[inline]
    pub fn peer_mut(&mut self) -> &mut Peer<EK, ER> {
        &mut self.peer
    }

    #[inline]
    pub fn logger(&self) -> &Logger {
        &self.peer.logger
    }

    /// Fetches messages to `peer_msg_buf`. It will stop when the buffer
    /// capacity is reached or there is no more pending messages.
    ///
    /// Returns how many messages are fetched.
    pub fn recv(&mut self, peer_msg_buf: &mut Vec<PeerMsg>, batch_size: usize) -> usize {
        let l = peer_msg_buf.len();
        for i in l..batch_size {
            match self.receiver.try_recv() {
                Ok(msg) => peer_msg_buf.push(msg),
                Err(e) => {
                    if let TryRecvError::Disconnected = e {
                        self.is_stopped = true;
                    }
                    return i - l;
                }
            }
        }
        batch_size - l
    }
}

impl<EK: KvEngine, ER: RaftEngine> Fsm for PeerFsm<EK, ER> {
    type Message = PeerMsg;

    #[inline]
    fn is_stopped(&self) -> bool {
        self.is_stopped
    }

    /// Set a mailbox to FSM, which should be used to send message to itself.
    fn set_mailbox(&mut self, mailbox: Cow<'_, BasicMailbox<Self>>)
    where
        Self: Sized,
    {
        self.mailbox = Some(mailbox.into_owned());
    }

    /// Take the mailbox from FSM. Implementation should ensure there will be
    /// no reference to mailbox after calling this method.
    fn take_mailbox(&mut self) -> Option<BasicMailbox<Self>>
    where
        Self: Sized,
    {
        self.mailbox.take()
    }
}

pub struct PeerFsmDelegate<'a, EK: KvEngine, ER: RaftEngine, T> {
    pub fsm: &'a mut PeerFsm<EK, ER>,
    pub store_ctx: &'a mut StoreContext<EK, ER, T>,
}

impl<'a, EK: KvEngine, ER: RaftEngine, T: Transport> PeerFsmDelegate<'a, EK, ER, T> {
    pub fn new(fsm: &'a mut PeerFsm<EK, ER>, store_ctx: &'a mut StoreContext<EK, ER, T>) -> Self {
        Self { fsm, store_ctx }
    }

    #[inline]
    fn schedule_pending_ticks(&mut self) {
        let pending_ticks = self.fsm.peer.take_pending_ticks();
        for tick in pending_ticks {
            if tick == PeerTick::ReactivateMemoryLock {
                self.fsm.reactivate_memory_lock_ticks = 0;
            }
            self.schedule_tick(tick);
        }
    }

    pub fn schedule_tick(&mut self, tick: PeerTick) {
        assert!(PeerTick::VARIANT_COUNT <= u16::BITS as usize);
        let idx = tick as usize;
        if self.fsm.tick_registry[idx] {
            return;
        }
        if is_zero_duration(&self.store_ctx.tick_batch[idx].wait_duration) {
            return;
        }
        trace!(
            self.fsm.logger(),
            "schedule tick";
            "tick" => ?tick,
            "timeout" => ?self.store_ctx.tick_batch[idx].wait_duration,
        );

        let region_id = self.fsm.peer.region_id();
        let mb = match self.store_ctx.router.mailbox(region_id) {
            Some(mb) => mb,
            None => {
                error!(
                    self.fsm.logger(),
                    "failed to get mailbox";
                    "tick" => ?tick,
                );
                return;
            }
        };
        self.fsm.tick_registry[idx] = true;
        let logger = self.fsm.logger().clone();
        // TODO: perhaps following allocation can be removed.
        let cb = Box::new(move || {
            // This can happen only when the peer is about to be destroyed
            // or the node is shutting down. So it's OK to not to clean up
            // registry.
            if let Err(e) = mb.force_send(PeerMsg::Tick(tick)) {
                debug!(
                    logger,
                    "failed to schedule peer tick";
                    "tick" => ?tick,
                    "err" => %e,
                );
            }
        });
        self.store_ctx.tick_batch[idx].ticks.push(cb);
    }

    fn on_start(&mut self) {
        self.schedule_tick(PeerTick::Raft);
        self.schedule_tick(PeerTick::SplitRegionCheck);
        self.schedule_tick(PeerTick::PdHeartbeat);
        self.schedule_tick(PeerTick::CompactLog);
        if self.fsm.peer.storage().is_initialized() {
            self.fsm.peer.schedule_apply_fsm(self.store_ctx);
        }
        // Unlike v1, it's a must to set ready when there are pending entries. Otherwise
        // it may block for ever when there is unapplied conf change.
        let entry_storage = self.fsm.peer.storage().entry_storage();
        if entry_storage.commit_index() > entry_storage.applied_index()
            // Speed up setup if there is only one peer.
            || self.fsm.peer.is_leader()
        {
            self.fsm.peer.set_has_ready();
        }
    }

    #[inline]
    fn on_receive_command(&self, send_time: Instant) {
        self.store_ctx
            .raft_metrics
            .propose_wait_time
            .observe(duration_to_sec(send_time.saturating_elapsed()));
    }

    fn on_tick(&mut self, tick: PeerTick) {
        self.fsm.tick_registry[tick as usize] = false;
        match tick {
            PeerTick::Raft => self.on_raft_tick(),
            PeerTick::PdHeartbeat => self.on_pd_heartbeat(),
            PeerTick::CompactLog => self.on_compact_log_tick(false),
            PeerTick::SplitRegionCheck => self.on_split_region_check(),
            PeerTick::CheckMerge => unimplemented!(),
            PeerTick::CheckPeerStaleState => unimplemented!(),
            PeerTick::EntryCacheEvict => self.on_entry_cache_evict(),
            PeerTick::CheckLeaderLease => unimplemented!(),
            PeerTick::ReactivateMemoryLock => self.on_reactivate_memory_lock_tick(),
            PeerTick::ReportBuckets => unimplemented!(),
            PeerTick::CheckLongUncommitted => unimplemented!(),
        }
    }

    pub fn on_msgs(&mut self, peer_msgs_buf: &mut Vec<PeerMsg>) {
        for msg in peer_msgs_buf.drain(..) {
            match msg {
                PeerMsg::RaftMessage(msg) => {
                    self.fsm.peer.on_raft_message(self.store_ctx, msg);
                }
                PeerMsg::RaftQuery(cmd) => {
                    self.on_receive_command(cmd.send_time);
                    self.on_query(cmd.request, cmd.ch)
                }
                PeerMsg::AdminCommand(cmd) => {
                    self.on_receive_command(cmd.send_time);
                    self.fsm
                        .peer_mut()
                        .on_admin_command(self.store_ctx, cmd.request, cmd.ch)
                }
                PeerMsg::SimpleWrite(write) => {
                    self.on_receive_command(write.send_time);
                    self.fsm.peer_mut().on_simple_write(
                        self.store_ctx,
                        write.header,
                        write.data,
                        write.ch,
                    );
                }
                PeerMsg::UnsafeWrite(write) => {
                    self.on_receive_command(write.send_time);
                    self.fsm
                        .peer_mut()
                        .on_unsafe_write(self.store_ctx, write.data);
                }
                PeerMsg::Tick(tick) => self.on_tick(tick),
                PeerMsg::ApplyRes(res) => self.fsm.peer.on_apply_res(self.store_ctx, res),
                PeerMsg::SplitInit(msg) => self.fsm.peer.on_split_init(self.store_ctx, msg),
                PeerMsg::SplitInitFinish(region_id) => {
                    self.fsm.peer.on_split_init_finish(region_id)
                }
                PeerMsg::Start => self.on_start(),
                PeerMsg::Noop => unimplemented!(),
                PeerMsg::Persisted {
                    peer_id,
                    ready_number,
                } => self
                    .fsm
                    .peer_mut()
                    .on_persisted(self.store_ctx, peer_id, ready_number),
                PeerMsg::LogsFetched(fetched_logs) => {
                    self.fsm.peer_mut().on_logs_fetched(fetched_logs)
                }
                PeerMsg::SnapshotGenerated(snap_res) => {
                    self.fsm.peer_mut().on_snapshot_generated(snap_res)
                }
                PeerMsg::QueryDebugInfo(ch) => self.fsm.peer_mut().on_query_debug_info(ch),
                PeerMsg::DataFlushed {
                    cf,
                    tablet_index,
                    flushed_index,
                } => {
                    self.fsm
                        .peer_mut()
                        .on_data_flushed(cf, tablet_index, flushed_index);
                }
                PeerMsg::PeerUnreachable { to_peer_id } => {
                    self.fsm.peer_mut().on_peer_unreachable(to_peer_id)
                }
                PeerMsg::StoreUnreachable { to_store_id } => {
                    self.fsm.peer_mut().on_store_unreachable(to_store_id)
                }
                PeerMsg::SnapshotSent { to_peer_id, status } => {
                    self.fsm.peer_mut().on_snapshot_sent(to_peer_id, status)
                }
                PeerMsg::RequestSplit { request, ch } => {
                    self.fsm
                        .peer_mut()
                        .on_request_split(self.store_ctx, request, ch)
                }
                PeerMsg::ForceCompactLog => self.on_compact_log_tick(true),
                #[cfg(feature = "testexport")]
                PeerMsg::WaitFlush(ch) => self.fsm.peer_mut().on_wait_flush(ch),
            }
        }
        // TODO: instead of propose pending commands immediately, we should use timeout.
        self.fsm.peer.propose_pending_writes(self.store_ctx);
        self.schedule_pending_ticks();
    }

    pub fn on_reactivate_memory_lock_tick(&mut self) {
        let mut pessimistic_locks = self.fsm.peer.txn_ext().pessimistic_locks.write();

        // If it is not leader, we needn't reactivate by tick. In-memory pessimistic
        // lock will be enabled when this region becomes leader again.
        // And this tick is currently only used for the leader transfer failure case.
        if !self.fsm.peer().is_leader()
            || pessimistic_locks.status != LocksStatus::TransferringLeader
        {
            return;
        }

        self.fsm.reactivate_memory_lock_ticks += 1;
        let transferring_leader = self.fsm.peer.raft_group().raft.lead_transferee.is_some();
        // `lead_transferee` is not set immediately after the lock status changes. So,
        // we need the tick count condition to avoid reactivating too early.
        if !transferring_leader
            && self.fsm.reactivate_memory_lock_ticks
                >= self.store_ctx.cfg.reactive_memory_lock_timeout_tick
        {
            pessimistic_locks.status = LocksStatus::Normal;
            self.fsm.reactivate_memory_lock_ticks = 0;
        } else {
            drop(pessimistic_locks);
            self.schedule_tick(PeerTick::ReactivateMemoryLock);
        }
    }
}
