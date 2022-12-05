// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! This module contains the peer implementation for batch system.

use std::borrow::Cow;

use batch_system::{BasicMailbox, Fsm};
use crossbeam::channel::TryRecvError;
use engine_traits::{KvEngine, RaftEngine, TabletFactory};
use raftstore::store::{Config, Transport};
use slog::{debug, error, info, trace, Logger};
use tikv_util::{
    is_zero_duration,
    mpsc::{self, LooseBoundedSender, Receiver},
    time::{duration_to_sec, Instant},
    yatp_pool::FuturePool,
};

use super::ApplyFsm;
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
    tick_registry: u16,
    is_stopped: bool,
}

impl<EK: KvEngine, ER: RaftEngine> PeerFsm<EK, ER> {
    pub fn new(
        cfg: &Config,
        tablet_factory: &dyn TabletFactory<EK>,
        storage: Storage<EK, ER>,
    ) -> Result<SenderFsmPair<EK, ER>> {
        let peer = Peer::new(cfg, tablet_factory, storage)?;
        info!(peer.logger, "create peer");
        let (tx, rx) = mpsc::loose_bounded(cfg.notify_capacity);
        let fsm = Box::new(PeerFsm {
            peer,
            mailbox: None,
            receiver: rx,
            tick_registry: 0,
            is_stopped: false,
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

    pub fn schedule_tick(&mut self, tick: PeerTick) {
        assert!(PeerTick::VARIANT_COUNT <= u16::BITS as usize);
        let idx = tick as usize;
        let key = 1u16 << (idx as u16);
        if self.fsm.tick_registry & key != 0 {
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
        self.fsm.tick_registry |= key;
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
        if self.fsm.peer.storage().is_initialized() {
            self.fsm.peer.schedule_apply_fsm(self.store_ctx);
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
        match tick {
            PeerTick::Raft => self.on_raft_tick(),
            PeerTick::PdHeartbeat => self.on_pd_heartbeat(),
            PeerTick::RaftLogGc => unimplemented!(),
            PeerTick::SplitRegionCheck => unimplemented!(),
            PeerTick::CheckMerge => unimplemented!(),
            PeerTick::CheckPeerStaleState => unimplemented!(),
            PeerTick::EntryCacheEvict => unimplemented!(),
            PeerTick::CheckLeaderLease => unimplemented!(),
            PeerTick::ReactivateMemoryLock => unimplemented!(),
            PeerTick::ReportBuckets => unimplemented!(),
            PeerTick::CheckLongUncommitted => unimplemented!(),
        }
    }

    pub fn on_msgs(&mut self, peer_msgs_buf: &mut Vec<PeerMsg>) {
        for msg in peer_msgs_buf.drain(..) {
            match msg {
                PeerMsg::RaftMessage(msg) => self.fsm.peer.on_raft_message(self.store_ctx, msg),
                PeerMsg::RaftQuery(cmd) => {
                    self.on_receive_command(cmd.send_time);
                    self.on_query(cmd.request, cmd.ch)
                }
                PeerMsg::RaftCommand(cmd) => {
                    self.on_receive_command(cmd.send_time);
                    self.on_command(cmd.request, cmd.ch)
                }
                PeerMsg::Tick(tick) => self.on_tick(tick),
                PeerMsg::ApplyRes(res) => self.fsm.peer.on_apply_res(self.store_ctx, res),
                PeerMsg::SplitInit(msg) => self.fsm.peer.on_split_init(self.store_ctx, msg),
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
                #[cfg(feature = "testexport")]
                PeerMsg::WaitFlush(ch) => self.fsm.peer_mut().on_wait_flush(ch),
            }
        }
        // TODO: instead of propose pending commands immediately, we should use timeout.
        self.fsm.peer.propose_pending_writes(self.store_ctx);
    }
}
