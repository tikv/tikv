// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! This module contains the peer implementation for batch system.

use std::{borrow::Cow, sync::Arc};

use batch_system::{BasicMailbox, Fsm};
use crossbeam::channel::TryRecvError;
use encryption_export::DataKeyManager;
use engine_traits::{KvEngine, RaftEngine, TabletRegistry};
use kvproto::{errorpb, raft_cmdpb::RaftCmdResponse};
use raftstore::store::{Config, ReadCallback, TabletSnapManager, Transport};
use slog::{debug, info, trace, Logger};
use tikv_util::{
    is_zero_duration,
    mpsc::{self, LooseBoundedSender, Receiver},
    slog_panic,
    time::{duration_to_sec, Instant},
};
use tracker::{TrackerToken, GLOBAL_TRACKERS};

use crate::{
    batch::StoreContext,
    operation::ReplayWatch,
    raft::{Peer, Storage},
    router::{PeerMsg, PeerTick, QueryResult},
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
}

impl<EK: KvEngine, ER: RaftEngine> PeerFsm<EK, ER> {
    pub fn new(
        cfg: &Config,
        tablet_registry: &TabletRegistry<EK>,
        key_manager: Option<&DataKeyManager>,
        snap_mgr: &TabletSnapManager,
        storage: Storage<EK, ER>,
    ) -> Result<SenderFsmPair<EK, ER>> {
        let peer = Peer::new(cfg, tablet_registry, key_manager, snap_mgr, storage)?;
        info!(peer.logger, "create peer";
            "raft_state" => ?peer.storage().raft_state(),
            "apply_state" => ?peer.storage().apply_state(),
            "region_state" => ?peer.storage().region_state()
        );
        let (tx, rx) = mpsc::loose_bounded(cfg.notify_capacity);
        let fsm = Box::new(PeerFsm {
            peer,
            mailbox: None,
            receiver: rx,
            tick_registry: [false; PeerTick::VARIANT_COUNT],
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

    #[inline]
    fn schedule_pending_ticks(&mut self) {
        let pending_ticks = self.fsm.peer.take_pending_ticks();
        for tick in pending_ticks {
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
                if !self.fsm.peer.serving() || self.store_ctx.router.is_shutdown() {
                    return;
                }
                slog_panic!(self.fsm.logger(), "failed to get mailbox"; "tick" => ?tick);
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

    fn on_start(&mut self, watch: Option<Arc<ReplayWatch>>) {
        if !self.fsm.peer.maybe_pause_for_replay(self.store_ctx, watch) {
            self.schedule_tick(PeerTick::Raft);
        }
        self.schedule_tick(PeerTick::SplitRegionCheck);
        self.schedule_tick(PeerTick::PdHeartbeat);
        self.schedule_tick(PeerTick::CompactLog);
        self.fsm.peer.on_check_merge(self.store_ctx);
        if self.fsm.peer.storage().is_initialized() {
            self.fsm.peer.schedule_apply_fsm(self.store_ctx);
        }
        self.fsm.peer.maybe_gen_approximate_buckets(self.store_ctx);
        // Speed up setup if there is only one peer.
        if self.fsm.peer.is_leader() {
            self.fsm.peer.set_has_ready();
        }
    }

    #[inline]
    fn on_receive_command(&self, send_time: Instant, read_token: Option<TrackerToken>) {
        let propose_wait_time = send_time.saturating_elapsed();
        self.store_ctx
            .raft_metrics
            .propose_wait_time
            .observe(duration_to_sec(propose_wait_time));
        if let Some(token) = read_token {
            GLOBAL_TRACKERS.with_tracker(token, |tracker| {
                tracker.metrics.read_index_propose_wait_nanos = propose_wait_time.as_nanos() as u64;
            });
        }
    }

    fn on_tick(&mut self, tick: PeerTick) {
        self.fsm.tick_registry[tick as usize] = false;
        if !self.fsm.peer().serving() {
            return;
        }
        match tick {
            PeerTick::Raft => self.on_raft_tick(),
            PeerTick::PdHeartbeat => self.on_pd_heartbeat(),
            PeerTick::CompactLog => self.on_compact_log_tick(false),
            PeerTick::SplitRegionCheck => self.on_split_region_check(),
            PeerTick::CheckMerge => self.fsm.peer_mut().on_check_merge(self.store_ctx),
            PeerTick::CheckPeerStaleState => unimplemented!(),
            PeerTick::EntryCacheEvict => self.on_entry_cache_evict(),
            PeerTick::CheckLeaderLease => self.on_check_leader_lease_tick(),
            PeerTick::ReactivateMemoryLock => {
                self.fsm.peer.on_reactivate_memory_lock_tick(self.store_ctx)
            }
            PeerTick::ReportBuckets => self.on_report_region_buckets_tick(),
            PeerTick::CheckLongUncommitted => self.on_check_long_uncommitted(),
            PeerTick::GcPeer => self.fsm.peer_mut().on_gc_peer_tick(self.store_ctx),
        }
    }

    pub fn on_msgs(&mut self, peer_msgs_buf: &mut Vec<PeerMsg>) {
        for msg in peer_msgs_buf.drain(..) {
            match msg {
                PeerMsg::RaftMessage(msg, send_time) => {
                    self.fsm
                        .peer
                        .on_raft_message(self.store_ctx, msg, send_time);
                }
                PeerMsg::RaftQuery(cmd) => {
                    self.on_receive_command(cmd.send_time, cmd.ch.read_tracker());
                    self.on_query(cmd.request, cmd.ch)
                }
                PeerMsg::AdminCommand(cmd) => {
                    self.on_receive_command(cmd.send_time, None);
                    self.fsm
                        .peer_mut()
                        .on_admin_command(self.store_ctx, cmd.request, cmd.ch)
                }
                PeerMsg::SimpleWrite(write) => {
                    self.on_receive_command(write.send_time, None);
                    self.fsm.peer_mut().on_simple_write(
                        self.store_ctx,
                        write.header,
                        write.data,
                        write.ch,
                        Some(write.extra_opts),
                    );
                }
                PeerMsg::UnsafeWrite(write) => {
                    self.on_receive_command(write.send_time, None);
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
                PeerMsg::Start(w) => self.on_start(w),
                PeerMsg::Noop => unimplemented!(),
                PeerMsg::Persisted {
                    peer_id,
                    ready_number,
                } => self
                    .fsm
                    .peer_mut()
                    .on_persisted(self.store_ctx, peer_id, ready_number),
                PeerMsg::LogsFetched(fetched_logs) => {
                    self.fsm.peer_mut().on_raft_log_fetched(fetched_logs)
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
                    self.fsm.peer_mut().on_data_flushed(
                        self.store_ctx,
                        cf,
                        tablet_index,
                        flushed_index,
                    );
                }
                PeerMsg::PeerUnreachable { to_peer_id } => {
                    self.fsm.peer_mut().on_peer_unreachable(to_peer_id)
                }
                PeerMsg::StoreUnreachable { to_store_id } => {
                    self.fsm.peer_mut().on_store_unreachable(to_store_id)
                }
                PeerMsg::StoreMaybeTombstone { store_id } => {
                    self.fsm.peer_mut().on_store_maybe_tombstone(store_id)
                }
                PeerMsg::SnapshotSent { to_peer_id, status } => {
                    self.fsm.peer_mut().on_snapshot_sent(to_peer_id, status)
                }
                PeerMsg::RequestSplit { request, ch } => {
                    self.fsm
                        .peer_mut()
                        .on_request_split(self.store_ctx, request, ch)
                }
                PeerMsg::RefreshRegionBuckets {
                    region_epoch,
                    buckets,
                    bucket_ranges,
                } => self.on_refresh_region_buckets(region_epoch, buckets, bucket_ranges),
                PeerMsg::RequestHalfSplit { request, ch } => self
                    .fsm
                    .peer_mut()
                    .on_request_half_split(self.store_ctx, request, ch),
                PeerMsg::UpdateRegionSize { size } => {
                    self.fsm.peer_mut().on_update_region_size(size)
                }
                PeerMsg::UpdateRegionKeys { keys } => {
                    self.fsm.peer_mut().on_update_region_keys(keys)
                }
                PeerMsg::ClearRegionSize => self.fsm.peer_mut().on_clear_region_size(),
                PeerMsg::ForceCompactLog => self.on_compact_log_tick(true),
                PeerMsg::TabletTrimmed { tablet_index } => {
                    self.fsm.peer_mut().on_tablet_trimmed(tablet_index)
                }
                PeerMsg::CleanupImportSst(ssts) => self
                    .fsm
                    .peer_mut()
                    .on_cleanup_import_sst(self.store_ctx, ssts),
                PeerMsg::SnapGc(keys) => self.fsm.peer_mut().on_snap_gc(self.store_ctx, keys),
                PeerMsg::AskCommitMerge(req) => {
                    self.fsm.peer_mut().on_ask_commit_merge(self.store_ctx, req)
                }
                PeerMsg::AckCommitMerge { index, target_id } => {
                    self.fsm.peer_mut().on_ack_commit_merge(index, target_id)
                }
                PeerMsg::RejectCommitMerge { index } => self
                    .fsm
                    .peer_mut()
                    .on_reject_commit_merge(self.store_ctx, index),
                PeerMsg::RedirectCatchUpLogs(c) => self
                    .fsm
                    .peer_mut()
                    .on_redirect_catch_up_logs(self.store_ctx, c),
                PeerMsg::CatchUpLogs(c) => self.fsm.peer_mut().on_catch_up_logs(self.store_ctx, c),
                PeerMsg::CaptureChange(capture_change) => self.on_capture_change(capture_change),
                PeerMsg::LeaderCallback(ch) => self.on_leader_callback(ch),
                #[cfg(feature = "testexport")]
                PeerMsg::WaitFlush(ch) => self.fsm.peer_mut().on_wait_flush(ch),
                PeerMsg::FlushBeforeClose { tx } => {
                    self.fsm.peer_mut().flush_before_close(self.store_ctx, tx)
                }
                PeerMsg::EnterForceLeaderState {
                    syncer,
                    failed_stores,
                } => self.fsm.peer_mut().on_enter_pre_force_leader(
                    self.store_ctx,
                    syncer,
                    failed_stores,
                ),
                PeerMsg::ExitForceLeaderState => self
                    .fsm
                    .peer_mut()
                    .on_exit_force_leader(self.store_ctx, false),
                PeerMsg::ExitForceLeaderStateCampaign => {
                    self.fsm.peer_mut().on_exit_force_leader_campaign()
                }
                PeerMsg::UnsafeRecoveryWaitApply(syncer) => {
                    self.fsm.peer_mut().on_unsafe_recovery_wait_apply(syncer)
                }
                PeerMsg::UnsafeRecoveryFillOutReport(syncer) => self
                    .fsm
                    .peer_mut()
                    .on_unsafe_recovery_fill_out_report(syncer),
                PeerMsg::UnsafeRecoveryWaitInitialized(syncer) => self
                    .fsm
                    .peer_mut()
                    .on_unsafe_recovery_wait_initialized(syncer),
                PeerMsg::UnsafeRecoveryDestroy(syncer) => {
                    self.fsm.peer_mut().on_unsafe_recovery_destroy_peer(syncer)
                }
                PeerMsg::UnsafeRecoveryDemoteFailedVoters {
                    failed_voters,
                    syncer,
                } => self
                    .fsm
                    .peer_mut()
                    .on_unsafe_recovery_pre_demote_failed_voters(
                        self.store_ctx,
                        syncer,
                        failed_voters,
                    ),
            }
        }
        // TODO: instead of propose pending commands immediately, we should use timeout.
        self.fsm.peer.propose_pending_writes(self.store_ctx);
        self.schedule_pending_ticks();
    }
}

impl<EK: KvEngine, ER: RaftEngine> Drop for PeerFsm<EK, ER> {
    fn drop(&mut self) {
        self.peer_mut().pending_reads_mut().clear_all(None);

        let region_id = self.peer().region_id();

        let build_resp = || {
            let mut err = errorpb::Error::default();
            err.set_message("region is not found".to_owned());
            err.mut_region_not_found().set_region_id(region_id);
            let mut resp = RaftCmdResponse::default();
            resp.mut_header().set_error(err);
            resp
        };
        while let Ok(msg) = self.receiver.try_recv() {
            match msg {
                // Only these messages need to be responded explicitly as they rely on
                // deterministic response.
                PeerMsg::RaftQuery(query) => {
                    query.ch.set_result(QueryResult::Response(build_resp()));
                }
                PeerMsg::SimpleWrite(w) => {
                    w.ch.set_result(build_resp());
                }
                _ => continue,
            }
        }
    }
}
