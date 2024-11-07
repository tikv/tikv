// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::{
    borrow::Cow,
    cell::Cell,
    cmp,
    collections::{
        Bound::{Excluded, Unbounded},
        VecDeque,
    },
    iter::Iterator,
    mem,
    sync::{atomic::Ordering, Arc, Mutex},
    time::{Duration, Instant},
    u64,
};

use batch_system::{BasicMailbox, Fsm};
use collections::{HashMap, HashSet};
use engine_traits::{
    Engines, KvEngine, RaftEngine, RaftLogBatch, SstMetaInfo, WriteBatchExt, CF_LOCK, CF_RAFT,
};
use error_code::ErrorCodeExt;
use fail::fail_point;
use futures::channel::mpsc::UnboundedSender;
use itertools::Itertools;
use keys::{self, enc_end_key, enc_start_key};
use kvproto::{
    brpb::CheckAdminResponse,
    errorpb,
    import_sstpb::SwitchMode,
    kvrpcpb::DiskFullOpt,
    metapb::{self, Region, RegionEpoch},
    pdpb::{self, CheckPolicy},
    raft_cmdpb::{
        AdminCmdType, AdminRequest, CmdType, PutRequest, RaftCmdRequest, RaftCmdResponse, Request,
        StatusCmdType, StatusResponse,
    },
    raft_serverpb::{
        ExtraMessage, ExtraMessageType, MergeState, PeerState, RaftMessage, RaftSnapshotData,
        RaftTruncatedState, RefreshBuckets, RegionLocalState,
    },
    replication_modepb::{DrAutoSyncState, ReplicationMode},
};
use parking_lot::RwLockWriteGuard;
use pd_client::BucketMeta;
use protobuf::Message;
use raft::{
    self,
    eraftpb::{self, ConfChangeType, MessageType},
    GetEntriesContext, Progress, ReadState, SnapshotStatus, StateRole, INVALID_INDEX, NO_LIMIT,
};
use smallvec::SmallVec;
use strum::{EnumCount, VariantNames};
use tikv_alloc::trace::TraceEvent;
use tikv_util::{
    box_err, debug, defer, error, escape, info, info_or_debug, is_zero_duration,
    mpsc::{self, LooseBoundedSender, Receiver},
    slow_log,
    store::{find_peer, find_peer_by_id, is_learner, region_on_same_stores},
    sys::disk::DiskUsage,
    time::{monotonic_raw_now, Instant as TiInstant, SlowTimer},
    trace, warn,
    worker::{ScheduleError, Scheduler},
    Either,
};
use tracker::GLOBAL_TRACKERS;
use txn_types::WriteBatchFlags;

use self::memtrace::*;
use super::life::forward_destroy_to_source_peer;
#[cfg(any(test, feature = "testexport"))]
use crate::store::PeerInternalStat;
use crate::{
    coprocessor::{RegionChangeEvent, RegionChangeReason},
    store::{
        cmd_resp::{bind_term, new_error},
        demote_failed_voters_request,
        entry_storage::MAX_WARMED_UP_CACHE_KEEP_TIME,
        fsm::{
            apply,
            store::{PollContext, StoreMeta},
            ApplyMetrics, ApplyTask, ApplyTaskRes, CatchUpLogs, ChangeObserver, ChangePeer,
            ExecResult, SwitchWitness,
        },
        hibernate_state::{GroupState, HibernateState},
        local_metrics::{RaftMetrics, TimeTracker},
        memory::*,
        metrics::*,
        msg::{Callback, ExtCallback, InspectedRaftMessage},
        peer::{
            ConsistencyState, Peer, PersistSnapshotResult, StaleState,
            TRANSFER_LEADER_COMMAND_REPLY_CTX,
        },
        region_meta::RegionMeta,
        snapshot_backup::{AbortReason, SnapshotBrState, SnapshotBrWaitApplyRequest},
        transport::Transport,
        unsafe_recovery::{
            exit_joint_request, ForceLeaderState, UnsafeRecoveryExecutePlanSyncer,
            UnsafeRecoveryFillOutReportSyncer, UnsafeRecoveryForceLeaderSyncer,
            UnsafeRecoveryState, UnsafeRecoveryWaitApplySyncer,
        },
        util::{self, compare_region_epoch, KeysInfoFormatter, LeaseState},
        worker::{
            Bucket, BucketRange, CleanupTask, ConsistencyCheckTask, GcSnapshotTask, RaftlogGcTask,
            ReadDelegate, ReadProgress, RegionTask, SplitCheckTask,
        },
        CasualMessage, Config, LocksStatus, MergeResultKind, PdTask, PeerMsg, PeerTick,
        ProposalContext, RaftCmdExtraOpts, RaftCommand, RaftlogFetchResult, ReadCallback,
        ReadIndexContext, ReadTask, SignificantMsg, SnapKey, StoreMsg, WriteCallback,
        RAFT_INIT_LOG_INDEX,
    },
    Error, Result,
};

#[derive(Clone, Copy, Debug)]
pub struct DelayDestroy {
    merged_by_target: bool,
    reason: DelayReason,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum DelayReason {
    UnPersistedReady,
    UnFlushLogGc,
    Shutdown,
}

/// Limits the maximum number of regions returned by error.
///
/// Another choice is using coprocessor batch limit, but 10 should be a good fit
/// in most case.
const MAX_REGIONS_IN_ERROR: usize = 10;
const REGION_SPLIT_SKIP_MAX_COUNT: usize = 3;
const UNSAFE_RECOVERY_STATE_TIMEOUT: Duration = Duration::from_secs(60);

pub const MAX_PROPOSAL_SIZE_RATIO: f64 = 0.4;

pub struct DestroyPeerJob {
    pub initialized: bool,
    pub region_id: u64,
    pub peer: metapb::Peer,
}

pub struct PeerFsm<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub peer: Peer<EK, ER>,
    /// A registry for all scheduled ticks. This can avoid scheduling ticks
    /// twice accidentally.
    tick_registry: [bool; PeerTick::VARIANT_COUNT],
    /// Ticks for speed up campaign in chaos state.
    ///
    /// Followers will keep ticking in Idle mode to measure how many ticks have
    /// been skipped. Once it becomes chaos, those skipped ticks will be
    /// ticked so that it can campaign quickly instead of waiting an
    /// election timeout.
    ///
    /// This will be reset to 0 once it receives any messages from leader.
    missing_ticks: usize,
    hibernate_state: HibernateState,
    stopped: bool,
    has_ready: bool,
    mailbox: Option<BasicMailbox<PeerFsm<EK, ER>>>,
    pub receiver: Receiver<PeerMsg<EK>>,
    /// when snapshot is generating or sending, skip split check at most
    /// REGION_SPLIT_SKIT_MAX_COUNT times.
    skip_split_count: usize,
    /// Sometimes applied raft logs won't be compacted in time, because less
    /// compact means less sync-log in apply threads. Stale logs will be
    /// deleted if the skip time reaches this `skip_gc_raft_log_ticks`.
    skip_gc_raft_log_ticks: usize,
    reactivate_memory_lock_ticks: usize,

    /// Batch raft command which has the same header into an entry
    batch_req_builder: BatchRaftCmdRequestBuilder<EK>,

    trace: PeerMemoryTrace,

    /// Destroy is delayed because of some unpersisted readies in Peer.
    /// Should call `destroy_peer` again after persisting all readies.
    delayed_destroy: Option<DelayDestroy>,
    /// Before actually destroying a peer, ensure all log gc tasks are finished,
    /// so we can start destroying without seeking.
    logs_gc_flushed: bool,
}

pub struct BatchRaftCmdRequestBuilder<E>
where
    E: KvEngine,
{
    batch_req_size: u64,
    has_proposed_cb: bool,
    propose_checked: Option<bool>,
    request: Option<RaftCmdRequest>,
    callbacks: Vec<Callback<E::Snapshot>>,
}

impl<EK, ER> Drop for PeerFsm<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn drop(&mut self) {
        self.peer.stop();
        let mut raft_messages_size = 0;
        while let Ok(msg) = self.receiver.try_recv() {
            let callback = match msg {
                PeerMsg::RaftCommand(cmd) => cmd.callback,
                PeerMsg::CasualMessage(box CasualMessage::SplitRegion { callback, .. }) => callback,
                PeerMsg::RaftMessage(im, _) => {
                    raft_messages_size += im.heap_size;
                    continue;
                }
                _ => continue,
            };

            let mut err = errorpb::Error::default();
            err.set_message("region is not found".to_owned());
            err.mut_region_not_found().set_region_id(self.region_id());
            let mut resp = RaftCmdResponse::default();
            resp.mut_header().set_error(err);
            callback.invoke_with_response(resp);
        }
        (match self.hibernate_state.group_state() {
            GroupState::Idle | GroupState::PreChaos => &HIBERNATED_PEER_STATE_GAUGE.hibernated,
            _ => &HIBERNATED_PEER_STATE_GAUGE.awaken,
        })
        .dec();

        MEMTRACE_RAFT_MESSAGES.trace(TraceEvent::Sub(raft_messages_size));
        MEMTRACE_RAFT_ENTRIES.trace(TraceEvent::Sub(self.peer.memtrace_raft_entries));

        let mut event = TraceEvent::default();
        if let Some(e) = self.trace.reset(PeerMemoryTrace::default()) {
            event = event + e;
        }
        MEMTRACE_PEERS.trace(event);
    }
}

pub type SenderFsmPair<EK, ER> = (LooseBoundedSender<PeerMsg<EK>>, Box<PeerFsm<EK, ER>>);

impl<EK, ER> PeerFsm<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    // If we create the peer actively, like bootstrap/split/merge region, we should
    // use this function to create the peer. The region must contain the peer info
    // for this store.
    pub fn create(
        store_id: u64,
        cfg: &Config,
        region_scheduler: Scheduler<RegionTask<EK::Snapshot>>,
        raftlog_fetch_scheduler: Scheduler<ReadTask<EK>>,
        engines: Engines<EK, ER>,
        region: &metapb::Region,
        wait_data: bool,
    ) -> Result<SenderFsmPair<EK, ER>> {
        let meta_peer = match find_peer(region, store_id) {
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
        HIBERNATED_PEER_STATE_GAUGE.awaken.inc();
        let (tx, rx) = mpsc::loose_bounded(cfg.notify_capacity);
        Ok((
            tx,
            Box::new(PeerFsm {
                peer: Peer::new(
                    store_id,
                    cfg,
                    region_scheduler,
                    raftlog_fetch_scheduler,
                    engines,
                    region,
                    meta_peer,
                    wait_data,
                    None,
                )?,
                tick_registry: [false; PeerTick::VARIANT_COUNT],
                missing_ticks: 0,
                hibernate_state: HibernateState::ordered(),
                stopped: false,
                has_ready: false,
                mailbox: None,
                receiver: rx,
                skip_split_count: 0,
                skip_gc_raft_log_ticks: 0,
                reactivate_memory_lock_ticks: 0,
                batch_req_builder: BatchRaftCmdRequestBuilder::new(),
                trace: PeerMemoryTrace::default(),
                delayed_destroy: None,
                logs_gc_flushed: false,
            }),
        ))
    }

    // The peer can be created from another node with raft membership changes, and
    // we only know the region_id and peer_id when creating this replicated peer,
    // the region info will be retrieved later after applying snapshot.
    pub fn replicate(
        store_id: u64,
        cfg: &Config,
        region_scheduler: Scheduler<RegionTask<EK::Snapshot>>,
        raftlog_fetch_scheduler: Scheduler<ReadTask<EK>>,
        engines: Engines<EK, ER>,
        region_id: u64,
        peer: metapb::Peer,
        create_by_peer: metapb::Peer,
    ) -> Result<SenderFsmPair<EK, ER>> {
        // We will remove tombstone key when apply snapshot
        info!(
            "replicate peer";
            "region_id" => region_id,
            "peer_id" => peer.get_id(),
            "store_id" => store_id,
            "create_by_peer_id" => create_by_peer.get_id(),
            "create_by_peer_store_id" => create_by_peer.get_store_id(),
        );

        let mut region = metapb::Region::default();
        region.set_id(region_id);

        HIBERNATED_PEER_STATE_GAUGE.awaken.inc();
        let (tx, rx) = mpsc::loose_bounded(cfg.notify_capacity);
        Ok((
            tx,
            Box::new(PeerFsm {
                peer: Peer::new(
                    store_id,
                    cfg,
                    region_scheduler,
                    raftlog_fetch_scheduler,
                    engines,
                    &region,
                    peer,
                    false,
                    Some(create_by_peer),
                )?,
                tick_registry: [false; PeerTick::VARIANT_COUNT],
                missing_ticks: 0,
                hibernate_state: HibernateState::ordered(),
                stopped: false,
                has_ready: false,
                mailbox: None,
                receiver: rx,
                skip_split_count: 0,
                skip_gc_raft_log_ticks: 0,
                reactivate_memory_lock_ticks: 0,
                batch_req_builder: BatchRaftCmdRequestBuilder::new(),
                trace: PeerMemoryTrace::default(),
                delayed_destroy: None,
                logs_gc_flushed: false,
            }),
        ))
    }

    #[inline]
    pub fn region_id(&self) -> u64 {
        self.peer.region().get_id()
    }

    #[inline]
    pub fn get_peer(&self) -> &Peer<EK, ER> {
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

    pub fn set_pending_merge_state(&mut self, state: MergeState) {
        self.peer.pending_merge_state = Some(state);
    }

    pub fn schedule_applying_snapshot(&mut self) {
        self.peer.mut_store().schedule_applying_snapshot();
    }

    pub fn reset_hibernate_state(&mut self, state: GroupState) {
        self.hibernate_state.reset(state);
        if state == GroupState::Idle {
            self.peer.raft_group.raft.maybe_free_inflight_buffers();
        }
    }

    pub fn maybe_hibernate(&mut self) -> bool {
        self.hibernate_state
            .maybe_hibernate(self.peer.peer_id(), self.peer.region())
    }

    pub fn update_memory_trace(&mut self, event: &mut TraceEvent) {
        let task = PeerMemoryTrace {
            read_only: self.raft_read_size(),
            progress: self.raft_progress_size(),
            proposals: self.peer.proposal_size(),
            rest: self.peer.rest_size(),
        };
        if let Some(e) = self.trace.reset(task) {
            *event = *event + e;
        }
    }
}

impl<E> BatchRaftCmdRequestBuilder<E>
where
    E: KvEngine,
{
    fn new() -> BatchRaftCmdRequestBuilder<E> {
        BatchRaftCmdRequestBuilder {
            batch_req_size: 0,
            has_proposed_cb: false,
            propose_checked: None,
            request: None,
            callbacks: vec![],
        }
    }

    fn can_batch(&self, cfg: &Config, req: &RaftCmdRequest, req_size: u32) -> bool {
        // No batch request whose size exceed 20% of raft_entry_max_size,
        // so total size of request in batch_raft_request would not exceed
        // (40% + 20%) of raft_entry_max_size
        if req.get_requests().is_empty()
            || req_size as u64 > (cfg.raft_entry_max_size.0 as f64 * 0.2) as u64
        {
            return false;
        }
        for r in req.get_requests() {
            match r.get_cmd_type() {
                CmdType::Delete | CmdType::Put => (),
                _ => {
                    return false;
                }
            }
        }

        if let Some(batch_req) = self.request.as_ref() {
            if batch_req.get_header() != req.get_header() {
                return false;
            }
        }
        true
    }

    fn add(&mut self, cmd: RaftCommand<E::Snapshot>, req_size: u32) {
        let RaftCommand {
            mut request,
            mut callback,
            ..
        } = cmd;
        if let Some(batch_req) = self.request.as_mut() {
            let requests: Vec<_> = request.take_requests().into();
            for q in requests {
                batch_req.mut_requests().push(q);
            }
        } else {
            self.request = Some(request);
        };
        if callback.has_proposed_cb() {
            self.has_proposed_cb = true;
            if self.propose_checked.unwrap_or(false) {
                callback.invoke_proposed();
            }
        }
        self.callbacks.push(callback);
        self.batch_req_size += req_size as u64;
    }

    fn should_finish(&self, cfg: &Config) -> bool {
        if let Some(batch_req) = self.request.as_ref() {
            // Limit the size of batch request so that it will not exceed
            // raft_entry_max_size after adding header.
            if self.batch_req_size
                > (cfg.raft_entry_max_size.0 as f64 * MAX_PROPOSAL_SIZE_RATIO) as u64
            {
                return true;
            }
            if batch_req.get_requests().len() > <E as WriteBatchExt>::WRITE_BATCH_MAX_KEYS {
                return true;
            }
        }
        false
    }

    fn build(
        &mut self,
        metric: &mut RaftMetrics,
    ) -> Option<(RaftCmdRequest, Callback<E::Snapshot>)> {
        if let Some(req) = self.request.take() {
            self.batch_req_size = 0;
            self.has_proposed_cb = false;
            self.propose_checked = None;
            if self.callbacks.len() == 1 {
                let cb = self.callbacks.pop().unwrap();
                return Some((req, cb));
            }
            metric.propose.batch.inc_by(self.callbacks.len() as u64 - 1);
            let mut cbs = std::mem::take(&mut self.callbacks);
            let proposed_cbs: Vec<ExtCallback> = cbs
                .iter_mut()
                .filter_map(|cb| cb.take_proposed_cb())
                .collect();
            let proposed_cb: Option<ExtCallback> = if proposed_cbs.is_empty() {
                None
            } else {
                Some(Box::new(move || {
                    for proposed_cb in proposed_cbs {
                        proposed_cb();
                    }
                }))
            };
            let committed_cbs: Vec<_> = cbs
                .iter_mut()
                .filter_map(|cb| cb.take_committed_cb())
                .collect();
            let committed_cb: Option<ExtCallback> = if committed_cbs.is_empty() {
                None
            } else {
                Some(Box::new(move || {
                    for committed_cb in committed_cbs {
                        committed_cb();
                    }
                }))
            };

            let trackers: SmallVec<[TimeTracker; 4]> = cbs
                .iter_mut()
                .flat_map(|cb| cb.write_trackers())
                .cloned()
                .collect();

            let cb = Callback::Write {
                cb: Box::new(move |resp| {
                    for cb in cbs {
                        let mut cmd_resp = RaftCmdResponse::default();
                        cmd_resp.set_header(resp.response.get_header().clone());
                        cb.invoke_with_response(cmd_resp);
                    }
                }),
                proposed_cb,
                committed_cb,
                trackers,
            };
            return Some((req, cb));
        }
        None
    }
}

impl<EK, ER> Fsm for PeerFsm<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    type Message = PeerMsg<EK>;

    #[inline]
    fn is_stopped(&self) -> bool {
        self.stopped
    }

    /// Set a mailbox to FSM, which should be used to send message to itself.
    #[inline]
    fn set_mailbox(&mut self, mailbox: Cow<'_, BasicMailbox<Self>>)
    where
        Self: Sized,
    {
        self.mailbox = Some(mailbox.into_owned());
    }

    /// Take the mailbox from FSM. Implementation should ensure there will be
    /// no reference to mailbox after calling this method.
    #[inline]
    fn take_mailbox(&mut self) -> Option<BasicMailbox<Self>>
    where
        Self: Sized,
    {
        self.mailbox.take()
    }
}

pub struct PeerFsmDelegate<'a, EK, ER, T: 'static>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fsm: &'a mut PeerFsm<EK, ER>,
    ctx: &'a mut PollContext<EK, ER, T>,
}

impl<'a, EK, ER, T: Transport> PeerFsmDelegate<'a, EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub fn new(
        fsm: &'a mut PeerFsm<EK, ER>,
        ctx: &'a mut PollContext<EK, ER, T>,
    ) -> PeerFsmDelegate<'a, EK, ER, T> {
        PeerFsmDelegate { fsm, ctx }
    }

    pub fn handle_msgs(&mut self, msgs: &mut Vec<PeerMsg<EK>>) {
        let timer = SlowTimer::from_millis(100);
        let count = msgs.len();
        #[allow(const_evaluatable_unchecked)]
        let mut distribution = [0; PeerMsg::<EK>::COUNT];
        // As the detail of one msg is not very useful when handling multiple messages,
        // only format the msg detail in slow log when there is only one message.
        let detail = if msgs.len() == 1 {
            msgs.first().map(|m| format!("{:?}", m))
        } else {
            None
        };

        for m in msgs.drain(..) {
            distribution[m.discriminant()] += 1;
            match m {
                PeerMsg::RaftMessage(msg, sent_time) => {
                    if let Some(sent_time) = sent_time {
                        let wait_time = sent_time.saturating_elapsed().as_secs_f64();
                        self.ctx.raft_metrics.process_wait_time.observe(wait_time);
                    }

                    if !self.ctx.coprocessor_host.on_raft_message(&msg.msg) {
                        continue;
                    }

                    if let Err(e) = self.on_raft_message(msg) {
                        error!(%e;
                            "handle raft message err";
                            "region_id" => self.fsm.region_id(),
                            "peer_id" => self.fsm.peer_id(),
                        );
                    }
                }
                PeerMsg::RaftCommand(cmd) => {
                    let propose_time = cmd.send_time.saturating_elapsed();
                    self.ctx
                        .raft_metrics
                        .propose_wait_time
                        .observe(propose_time.as_secs_f64());
                    cmd.callback.read_tracker().map(|tracker| {
                        GLOBAL_TRACKERS.with_tracker(tracker, |t| {
                            t.metrics.read_index_propose_wait_nanos =
                                propose_time.as_nanos() as u64;
                        })
                    });

                    if let Some(Err(e)) = cmd.extra_opts.deadline.map(|deadline| deadline.check()) {
                        cmd.callback.invoke_with_response(new_error(e.into()));
                        continue;
                    }

                    let req_size = cmd.request.compute_size();
                    if self.ctx.cfg.cmd_batch
                        && self.fsm.batch_req_builder.can_batch(&self.ctx.cfg, &cmd.request, req_size)
                        // Avoid to merge requests with different `DiskFullOpt`s into one,
                        // so that normal writes can be rejected when proposing if the
                        // store's disk is full.
                        && ((self.ctx.self_disk_usage == DiskUsage::Normal
                        && !self.fsm.peer.disk_full_peers.majority())
                        || cmd.extra_opts.disk_full_opt == DiskFullOpt::NotAllowedOnFull)
                    {
                        self.fsm.batch_req_builder.add(*cmd, req_size);
                        if self.fsm.batch_req_builder.should_finish(&self.ctx.cfg) {
                            self.propose_pending_batch_raft_command();
                        }
                    } else {
                        self.propose_raft_command(
                            cmd.request,
                            cmd.callback,
                            cmd.extra_opts.disk_full_opt,
                        );
                    }
                }
                PeerMsg::Tick(tick) => self.on_tick(tick),
                PeerMsg::ApplyRes(res) => {
                    self.on_apply_res(res);
                }
                PeerMsg::SignificantMsg(msg) => self.on_significant_msg(msg),
                PeerMsg::CasualMessage(msg) => self.on_casual_msg(msg),
                PeerMsg::Start => self.start(),
                PeerMsg::HeartbeatPd => {
                    if self.fsm.peer.is_leader() {
                        self.register_pd_heartbeat_tick()
                    }
                }
                PeerMsg::Noop => {}
                PeerMsg::Persisted {
                    peer_id,
                    ready_number,
                } => self.on_persisted_msg(peer_id, ready_number),
                PeerMsg::UpdateReplicationMode => self.on_update_replication_mode(),
                PeerMsg::Destroy(peer_id) => {
                    if self.fsm.peer.peer_id() == peer_id {
                        self.maybe_destroy();
                    }
                }
            }
        }
        self.on_loop_finished();
        slow_log!(
            T timer,
            "{} handle {} peer messages {:?}, detail: {:?}",
            self.fsm.peer.tag,
            count,
            PeerMsg::<EK>::VARIANTS.iter().zip(distribution).filter(|(_, c)| *c > 0).format(", "),
            detail,
        );
        self.ctx.raft_metrics.peer_msg_len.observe(count as f64);
        self.ctx
            .raft_metrics
            .event_time
            .peer_msg
            .observe(timer.saturating_elapsed().as_secs_f64());
    }

    #[inline]
    fn on_loop_finished(&mut self) {
        let ready_concurrency = self.ctx.cfg.cmd_batch_concurrent_ready_max_count;
        // Allow to propose pending commands iff all ongoing commands are persisted or
        // committed. this is trying to batch proposes as many as possible to
        // minimize the cpu overhead.
        let should_propose = self.ctx.sync_write_worker.is_some()
            || ready_concurrency == 0
            || self.fsm.peer.unpersisted_ready_len() < ready_concurrency
            // Allow to propose if all ongoing proposals are committed to avoiding io jitter block
            // new commands.
            || !self.fsm.peer.has_uncommitted_log();
        let force_delay_fp = || {
            fail_point!(
                "force_delay_propose_batch_raft_command",
                self.ctx.sync_write_worker.is_none(),
                |_| true
            );
            false
        };
        // Propose batch request which may be still waiting for more raft-command
        if should_propose && !force_delay_fp() {
            self.propose_pending_batch_raft_command();
        } else if self.fsm.batch_req_builder.has_proposed_cb
            && self.fsm.batch_req_builder.propose_checked.is_none()
            && let Some(cmd) = self.fsm.batch_req_builder.request.take()
        {
            // We are delaying these requests to next loop. Try to fulfill their
            // proposed callback early.
            self.fsm.batch_req_builder.propose_checked = Some(false);
            if let Ok(None) = self.pre_propose_raft_command(&cmd) {
                if self.fsm.peer.will_likely_propose(&cmd) {
                    self.fsm.batch_req_builder.propose_checked = Some(true);
                    for cb in &mut self.fsm.batch_req_builder.callbacks {
                        cb.invoke_proposed();
                    }
                }
            }
            self.fsm.batch_req_builder.request = Some(cmd);
        }
    }

    /// Flushes all pending raft commands for immediate execution.
    #[inline]
    fn propose_pending_batch_raft_command(&mut self) {
        if self.fsm.batch_req_builder.request.is_none() {
            return;
        }
        let (request, callback) = self
            .fsm
            .batch_req_builder
            .build(&mut self.ctx.raft_metrics)
            .unwrap();
        self.propose_raft_command_internal(request, callback, DiskFullOpt::NotAllowedOnFull);
    }

    fn on_update_replication_mode(&mut self) {
        self.fsm
            .peer
            .switch_replication_mode(&self.ctx.global_replication_state);
        if self.fsm.peer.is_leader() {
            self.reset_raft_tick(GroupState::Ordered);
            self.register_pd_heartbeat_tick();
        }
    }

    fn on_unsafe_recovery_pre_demote_failed_voters(
        &mut self,
        syncer: UnsafeRecoveryExecutePlanSyncer,
        failed_voters: Vec<metapb::Peer>,
    ) {
        if let Some(state) = &self.fsm.peer.unsafe_recovery_state
            && !state.is_abort()
        {
            warn!(
                "Unsafe recovery, demote failed voters has already been initiated";
                "region_id" => self.region().get_id(),
                "peer_id" => self.fsm.peer.peer.get_id(),
                "state" => ?state,
            );
            syncer.abort();
            return;
        }

        if !self.fsm.peer.is_in_force_leader() {
            error!(
                "Unsafe recovery, demoting failed voters failed, since this peer is not forced leader";
                "region_id" => self.region().get_id(),
                "peer_id" => self.fsm.peer.peer.get_id(),
            );
            return;
        }

        if self.fsm.peer.in_joint_state() {
            info!(
                "Unsafe recovery, already in joint state, exit first";
                "region_id" => self.region().get_id(),
                "peer_id" => self.fsm.peer.peer.get_id(),
            );
            let failed = Arc::new(Mutex::new(false));
            let failed_clone = failed.clone();
            let callback = Callback::<EK::Snapshot>::write(Box::new(move |resp| {
                if resp.response.get_header().has_error() {
                    *failed_clone.lock().unwrap() = true;
                    error!(
                        "Unsafe recovery, fail to exit residual joint state";
                        "err" => ?resp.response.get_header().get_error(),
                    );
                }
            }));
            self.propose_raft_command_internal(
                exit_joint_request(self.region(), &self.fsm.peer.peer),
                callback,
                DiskFullOpt::AllowedOnAlmostFull,
            );

            if !*failed.lock().unwrap() {
                self.fsm.peer.unsafe_recovery_state =
                    Some(UnsafeRecoveryState::DemoteFailedVoters {
                        syncer,
                        failed_voters,
                        target_index: self.fsm.peer.raft_group.raft.raft_log.last_index(),
                        demote_after_exit: true,
                    });
            } else {
                self.fsm.peer.unsafe_recovery_state = Some(UnsafeRecoveryState::Failed);
            }
        } else {
            self.unsafe_recovery_demote_failed_voters(syncer, failed_voters);
        }
    }

    fn unsafe_recovery_demote_failed_voters(
        &mut self,
        syncer: UnsafeRecoveryExecutePlanSyncer,
        failed_voters: Vec<metapb::Peer>,
    ) {
        if let Some(req) =
            demote_failed_voters_request(self.region(), &self.fsm.peer.peer, failed_voters)
        {
            info!(
                "Unsafe recovery, demoting failed voters";
                "region_id" => self.region().get_id(),
                "peer_id" => self.fsm.peer.peer.get_id(),
                "req" => ?req);
            let failed = Arc::new(Mutex::new(false));
            let failed_clone = failed.clone();
            let callback = Callback::<EK::Snapshot>::write(Box::new(move |resp| {
                if resp.response.get_header().has_error() {
                    *failed_clone.lock().unwrap() = true;
                    error!(
                        "Unsafe recovery, fail to finish demotion";
                        "err" => ?resp.response.get_header().get_error(),
                    );
                }
            }));
            self.propose_raft_command_internal(req, callback, DiskFullOpt::AllowedOnAlmostFull);
            if !*failed.lock().unwrap() {
                self.fsm.peer.unsafe_recovery_state =
                    Some(UnsafeRecoveryState::DemoteFailedVoters {
                        syncer,
                        failed_voters: vec![], // No longer needed since here.
                        target_index: self.fsm.peer.raft_group.raft.raft_log.last_index(),
                        demote_after_exit: false,
                    });
            } else {
                self.fsm.peer.unsafe_recovery_state = Some(UnsafeRecoveryState::Failed);
            }
        } else {
            warn!(
                "Unsafe recovery, no need to demote failed voters";
                "region_id" => self.region().get_id(),
                "peer_id" => self.fsm.peer_id(),
                "region" => ?self.region(),
            );
        }
    }

    fn on_unsafe_recovery_destroy(&mut self, syncer: UnsafeRecoveryExecutePlanSyncer) {
        if let Some(state) = &self.fsm.peer.unsafe_recovery_state
            && !state.is_abort()
        {
            warn!(
                "Unsafe recovery, can't destroy, another plan is executing in progress";
                "region_id" => self.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "state" => ?state,
            );
            syncer.abort();
            return;
        }
        self.fsm.peer.unsafe_recovery_state = Some(UnsafeRecoveryState::Destroy(syncer));
        self.handle_destroy_peer(DestroyPeerJob {
            initialized: self.fsm.peer.is_initialized(),
            region_id: self.region_id(),
            peer: self.fsm.peer.peer.clone(),
        });
    }

    fn on_unsafe_recovery_wait_apply(&mut self, syncer: UnsafeRecoveryWaitApplySyncer) {
        if let Some(state) = &self.fsm.peer.unsafe_recovery_state
            && !state.is_abort()
        {
            warn!(
                "Unsafe recovery, can't wait apply, another plan is executing in progress";
                "region_id" => self.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "state" => ?state,
            );
            syncer.abort();
            return;
        }
        let target_index = if self.fsm.peer.force_leader.is_some() {
            // For regions that lose quorum (or regions have force leader), whatever has
            // been proposed will be committed. Based on that fact, we simply use "last
            // index" here to avoid implementing another "wait commit" process.
            self.fsm.peer.raft_group.raft.raft_log.last_index()
        } else {
            self.fsm.peer.raft_group.raft.raft_log.committed
        };

        if target_index > self.fsm.peer.raft_group.raft.raft_log.applied {
            info!(
                "Unsafe recovery, start wait apply";
                "region_id" => self.region().get_id(),
                "peer_id" => self.fsm.peer_id(),
                "target_index" => target_index,
                "applied" =>  self.fsm.peer.raft_group.raft.raft_log.applied,
            );
            self.fsm.peer.unsafe_recovery_state = Some(UnsafeRecoveryState::WaitApply {
                target_index,
                syncer,
            });
            self.fsm
                .peer
                .unsafe_recovery_maybe_finish_wait_apply(/* force= */ self.fsm.stopped);
        }
    }

    // func be invoked firstly after assigned leader by BR, wait all leader apply to
    // last log index func be invoked secondly wait follower apply to last
    // index, however the second call is broadcast, it may improve in future
    fn on_snapshot_br_wait_apply(&mut self, req: SnapshotBrWaitApplyRequest) {
        if let Some(state) = &self.fsm.peer.snapshot_recovery_state {
            warn!(
                "can't wait apply, another recovery in progress";
                "region_id" => self.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "state" => ?state,
            );
            req.syncer.abort(AbortReason::Duplicated);
            return;
        }

        let target_index = self.fsm.peer.raft_group.raft.raft_log.last_index();
        let applied_index = self.fsm.peer.raft_group.raft.raft_log.applied;
        let term = self.fsm.peer.raft_group.raft.term;
        if let Some(e) = &req.expected_epoch {
            if let Err(err) = compare_region_epoch(e, self.region(), true, true, true) {
                warn!("epoch not match for wait apply, aborting.";
                    "err" => %err,
                    "peer" => self.fsm.peer.peer_id(),
                    "region" => self.fsm.peer.region().get_id());
                let mut pberr = errorpb::Error::from(err);
                req.syncer
                    .abort(AbortReason::EpochNotMatch(pberr.take_epoch_not_match()));
                return;
            }
        }

        // trivial case: no need to wait apply -- already the latest.
        // Return directly for avoiding to print tons of logs.
        if target_index == applied_index {
            debug!(
                "skip trivial case of waiting apply.";
                "region_id" => self.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "target_index" => target_index,
                "applied_index" => applied_index,
            );
            SNAP_BR_WAIT_APPLY_EVENT.trivial.inc();
            return;
        }

        // during the snapshot recovery, broadcast waitapply, some peer may stale
        if !self.fsm.peer.is_leader() {
            info!(
                "snapshot follower wait apply started";
                "region_id" => self.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "target_index" => target_index,
                "applied_index" => applied_index,
                "pending_remove" => self.fsm.peer.pending_remove,
                "voter" => self.fsm.peer.raft_group.raft.vote,
            );

            // do some sanity check, for follower, leader already apply to last log,
            // case#1 if it is learner during backup and never vote before, vote is 0
            // case#2 if peer is suppose to remove
            if self.fsm.peer.raft_group.raft.vote == 0 || self.fsm.peer.pending_remove {
                info!(
                    "this peer is never vote before or pending remove, it should be skip to wait apply";
                    "region" => %self.region_id(),
                );
                return;
            }
        } else {
            info!(
                "snapshot leader wait apply started";
                "region_id" => self.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "target_index" => target_index,
                "applied_index" => applied_index,
            );
        }
        SNAP_BR_WAIT_APPLY_EVENT.accepted.inc();

        self.fsm.peer.snapshot_recovery_state = Some(SnapshotBrState::WaitLogApplyToLast {
            target_index,
            valid_for_term: req.abort_when_term_change.then_some(term),
            syncer: req.syncer,
        });
        self.fsm
            .peer
            .snapshot_recovery_maybe_finish_wait_apply(self.fsm.stopped);
    }

    fn on_unsafe_recovery_fill_out_report(&mut self, syncer: UnsafeRecoveryFillOutReportSyncer) {
        if self.fsm.peer.pending_remove || self.fsm.stopped {
            return;
        }
        let mut self_report = pdpb::PeerReport::default();
        self_report.set_raft_state(self.fsm.peer.get_store().raft_state().clone());
        let mut region_local_state = RegionLocalState::default();
        region_local_state.set_region(self.region().clone());
        self_report.set_region_state(region_local_state);
        self_report.set_is_force_leader(self.fsm.peer.force_leader.is_some());
        self_report.set_applied_index(self.fsm.peer.get_store().applied_index());
        match self.fsm.peer.get_store().entries(
            self.fsm.peer.raft_group.store().commit_index() + 1,
            self.fsm.peer.get_store().last_index() + 1,
            NO_LIMIT,
            GetEntriesContext::empty(false),
        ) {
            Ok(entries) => {
                for entry in entries {
                    let ctx = ProposalContext::from_bytes(&entry.context);
                    if ctx.contains(ProposalContext::COMMIT_MERGE) {
                        self_report.set_has_commit_merge(true);
                        break;
                    }
                }
            }
            Err(e) => panic!("Unsafe recovery, fail to get uncommitted entries, {:?}", e),
        }
        syncer.report_for_self(self_report);
    }

    fn on_check_pending_admin(&mut self, ch: UnboundedSender<CheckAdminResponse>) {
        if !self.fsm.peer.is_leader() {
            // no need to check non-leader pending conf change.
            // in snapshot recovery after we stopped all conf changes from PD.
            // if the follower slow than leader and has the pending conf change.
            // that's means
            // 1. if the follower didn't finished the conf change => it cannot be chosen to
            //    be leader during recovery.
            // 2. if the follower has been chosen to be leader => it already apply the
            //    pending conf change already.
            return;
        }
        debug!(
            "check pending conf for leader";
            "region_id" => self.region().get_id(),
            "peer_id" => self.fsm.peer.peer_id(),
        );
        let region = self.fsm.peer.region();
        let mut resp = CheckAdminResponse::default();
        resp.set_region(region.clone());
        let pending_admin = self.fsm.peer.raft_group.raft.has_pending_conf()
            || self.fsm.peer.is_merging()
            || self.fsm.peer.is_splitting();
        resp.set_has_pending_admin(pending_admin);
        if let Err(err) = ch.unbounded_send(resp) {
            warn!("failed to send check admin response";
            "err" => ?err,
            "region_id" => self.region().get_id(),
            "peer_id" => self.fsm.peer.peer_id(),
            );
        }
    }

    fn on_casual_msg(&mut self, msg: Box<CasualMessage<EK>>) {
        match *msg {
            CasualMessage::SplitRegion {
                region_epoch,
                split_keys,
                callback,
                source,
                share_source_region_size,
            } => {
                self.on_prepare_split_region(
                    region_epoch,
                    split_keys,
                    callback,
                    &source,
                    share_source_region_size,
                );
            }
            CasualMessage::ComputeHashResult {
                index,
                context,
                hash,
            } => {
                self.on_hash_computed(index, context, hash);
            }
            CasualMessage::RegionApproximateSize { size, splitable } => {
                self.on_approximate_region_size(size, splitable);
            }
            CasualMessage::RegionApproximateKeys { keys, splitable } => {
                self.on_approximate_region_keys(keys, splitable);
            }
            CasualMessage::RefreshRegionBuckets {
                region_epoch,
                buckets,
                bucket_ranges,
                cb,
            } => {
                self.on_refresh_region_buckets(region_epoch, buckets, bucket_ranges, cb);
            }
            CasualMessage::CompactionDeclinedBytes { bytes } => {
                self.on_compaction_declined_bytes(bytes);
            }
            CasualMessage::HalfSplitRegion {
                region_epoch,
                start_key,
                end_key,
                policy,
                source,
                cb,
            } => {
                self.on_schedule_half_split_region(
                    &region_epoch,
                    start_key,
                    end_key,
                    policy,
                    source,
                    cb,
                );
            }
            CasualMessage::GcSnap { snaps } => {
                self.on_gc_snap(snaps);
            }
            CasualMessage::ClearRegionSize => {
                self.on_clear_region_size();
            }
            CasualMessage::RegionOverlapped => {
                debug!("start ticking for overlapped"; "region_id" => self.region_id(), "peer_id" => self.fsm.peer_id());
                // Maybe do some safe check first?
                self.fsm.reset_hibernate_state(GroupState::Chaos);
                self.register_raft_base_tick();

                if is_learner(&self.fsm.peer.peer) {
                    // FIXME: should use `bcast_check_stale_peer_message` instead.
                    // Sending a new enum type msg to a old tikv may cause panic during rolling
                    // update we should change the protobuf behavior and check if properly handled
                    // in all place
                    self.fsm.peer.bcast_wake_up_message(self.ctx);
                }
            }
            CasualMessage::SnapshotGenerated => {
                // Resume snapshot handling again to avoid waiting another heartbeat.
                self.fsm.peer.ping();
                self.fsm.has_ready = true;
            }
            CasualMessage::ForceCompactRaftLogs => {
                self.on_raft_gc_log_tick(true);
            }
            CasualMessage::AccessPeer(cb) => {
                let peer = &self.fsm.peer;
                let store = peer.get_store();
                let mut local_state = RegionLocalState::default();
                local_state.set_region(store.region().clone());
                if let Some(s) = &peer.pending_merge_state {
                    local_state.set_merge_state(s.clone());
                }
                if store.is_applying_snapshot() {
                    local_state.set_state(PeerState::Applying);
                }
                cb(RegionMeta::new(
                    &local_state,
                    store.apply_state(),
                    self.fsm.hibernate_state.group_state(),
                    peer.raft_group.status(),
                    peer.raft_group.raft.raft_log.last_index(),
                    peer.raft_group.raft.raft_log.persisted,
                ))
            }
            CasualMessage::QueryRegionLeaderResp { region, leader } => {
                // the leader already updated
                if self.fsm.peer.raft_group.raft.leader_id != raft::INVALID_ID
                    // the returned region is stale
                    || util::is_epoch_stale(
                        region.get_region_epoch(),
                        self.fsm.peer.region().get_region_epoch(),
                ) {
                    // Stale message
                    return;
                }

                // Wake up the leader if the peer is on the leader's peer list
                if region
                    .get_peers()
                    .iter()
                    .any(|p| p.get_id() == self.fsm.peer_id())
                {
                    self.fsm.peer.send_wake_up_message(self.ctx, &leader);
                }
            }
            CasualMessage::RenewLease => {
                self.try_renew_leader_lease("casual message");
                self.reset_raft_tick(GroupState::Ordered);
            }
            CasualMessage::RejectRaftAppend { peer_id } => {
                let mut msg = raft::eraftpb::Message::new();
                msg.msg_type = MessageType::MsgUnreachable;
                msg.to = peer_id;
                msg.from = self.fsm.peer.peer_id();

                let raft_msg = self.fsm.peer.build_raft_messages(self.ctx, vec![msg]);
                self.fsm.peer.send_raft_messages(self.ctx, raft_msg);
            }
            CasualMessage::SnapshotApplied { peer_id, tombstone } => {
                self.fsm.has_ready = true;
                // If failed on applying snapshot, it should record the peer as an invalid peer.
                if tombstone && self.fsm.peer.peer_id() == peer_id && !self.fsm.peer.is_leader() {
                    info!(
                        "mark the region damaged on applying snapshot";
                        "region_id" => self.region_id(),
                        "peer_id" => peer_id,
                    );
                    let mut meta = self.ctx.store_meta.lock().unwrap();
                    meta.damaged_regions.insert(self.region_id());
                }
                if self.fsm.peer.should_destroy_after_apply_snapshot() {
                    self.maybe_destroy();
                }
            }
            CasualMessage::Campaign => {
                let _ = self.fsm.peer.raft_group.campaign();
                self.fsm.has_ready = true;
            }
            CasualMessage::InMemoryEngineLoadRegion {
                region_id,
                trigger_load_cb,
            } => self.ctx.apply_router.schedule_task(
                region_id,
                ApplyTask::InMemoryEngineLoadRegion {
                    region_id,
                    trigger_load_cb,
                },
            ),
        }
    }

    fn on_tick(&mut self, tick: PeerTick) {
        if self.fsm.stopped {
            return;
        }
        trace!(
            "tick";
            "tick" => ?tick,
            "peer_id" => self.fsm.peer_id(),
            "region_id" => self.region_id(),
        );
        self.fsm.tick_registry[tick as usize] = false;
        self.fsm.peer.adjust_cfg_if_changed(self.ctx);
        match tick {
            PeerTick::Raft => self.on_raft_base_tick(),
            PeerTick::RaftLogGc => self.on_raft_gc_log_tick(false),
            PeerTick::PdHeartbeat => self.on_pd_heartbeat_tick(),
            PeerTick::SplitRegionCheck => self.on_split_region_check_tick(),
            PeerTick::CheckMerge => self.on_check_merge(),
            PeerTick::CheckPeerStaleState => self.on_check_peer_stale_state_tick(),
            PeerTick::EntryCacheEvict => self.on_entry_cache_evict_tick(),
            PeerTick::CheckLeaderLease => self.on_check_leader_lease_tick(),
            PeerTick::ReactivateMemoryLock => self.on_reactivate_memory_lock_tick(),
            PeerTick::ReportBuckets => self.on_report_region_buckets_tick(),
            PeerTick::CheckLongUncommitted => self.on_check_long_uncommitted_tick(),
            PeerTick::CheckPeersAvailability => self.on_check_peers_availability(),
            PeerTick::RequestSnapshot => self.on_request_snapshot_tick(),
            PeerTick::RequestVoterReplicatedIndex => self.on_request_voter_replicated_index(),
        }
    }

    fn start(&mut self) {
        self.register_raft_base_tick();
        self.register_raft_gc_log_tick();
        self.register_pd_heartbeat_tick();
        self.register_split_region_check_tick();
        self.register_check_peer_stale_state_tick();
        self.on_check_merge();
        if self.fsm.peer.wait_data {
            self.on_request_snapshot_tick();
        }
        // Apply committed entries more quickly.
        // Or if it's a leader. This implicitly means it's a singleton
        // because it becomes leader in `Peer::new` when it's a
        // singleton. It has a no-op entry that need to be persisted,
        // committed, and then it should apply it.
        if self.fsm.peer.raft_group.store().commit_index()
            > self.fsm.peer.raft_group.store().applied_index()
            || self.fsm.peer.is_leader()
        {
            self.fsm.has_ready = true;
        }
        self.fsm.peer.maybe_gen_approximate_buckets(self.ctx);
        if self.fsm.peer.is_witness() {
            self.register_pull_voter_replicated_index_tick();
        }
    }

    fn on_gc_snap(&mut self, snaps: Vec<(SnapKey, bool)>) {
        let schedule_delete_snapshot_files = |key: SnapKey, snap| {
            if let Err(e) = self.ctx.cleanup_scheduler.schedule(CleanupTask::GcSnapshot(
                GcSnapshotTask::DeleteSnapshotFiles {
                    key: key.clone(),
                    snapshot: snap,
                    check_entry: false,
                },
            )) {
                error!(
                    "failed to schedule task to delete compacted snap file";
                    "key" => %key,
                    "err" => %e,
                )
            }
        };

        let is_applying_snap = self.fsm.peer.is_handling_snapshot();
        let s = self.fsm.peer.get_store();
        let compacted_idx = s.truncated_index();
        let compacted_term = s.truncated_term();
        for (key, is_sending) in snaps {
            if is_sending {
                let s = match self.ctx.snap_mgr.get_snapshot_for_gc(&key, is_sending) {
                    Ok(s) => s,
                    Err(e) => {
                        error!(%e;
                            "failed to load snapshot";
                            "region_id" => self.fsm.region_id(),
                            "peer_id" => self.fsm.peer_id(),
                            "snapshot" => ?key,
                        );
                        continue;
                    }
                };
                if key.term < compacted_term || key.idx < compacted_idx {
                    info!(
                        "deleting compacted snap file";
                        "region_id" => self.fsm.region_id(),
                        "peer_id" => self.fsm.peer_id(),
                        "snap_file" => %key,
                    );
                    schedule_delete_snapshot_files(key, s);
                } else if let Ok(meta) = s.meta() {
                    let modified = match meta.modified() {
                        Ok(m) => m,
                        Err(e) => {
                            error!(
                                "failed to load snapshot";
                                "region_id" => self.fsm.region_id(),
                                "peer_id" => self.fsm.peer_id(),
                                "snapshot" => ?key,
                                "err" => %e,
                            );
                            continue;
                        }
                    };
                    if let Ok(elapsed) = modified.elapsed() {
                        if elapsed > self.ctx.cfg.snap_gc_timeout.0 {
                            info!(
                                "deleting expired snap file";
                                "region_id" => self.fsm.region_id(),
                                "peer_id" => self.fsm.peer_id(),
                                "snap_file" => %key,
                            );
                            schedule_delete_snapshot_files(key, s);
                        }
                    }
                }
            } else if key.term <= compacted_term
                && (key.idx < compacted_idx
                    || key.idx == compacted_idx
                        && !is_applying_snap
                        && !self.fsm.peer.pending_remove)
            {
                info!(
                    "deleting applied snap file";
                    "region_id" => self.fsm.region_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "snap_file" => %key,
                );
                let a = match self.ctx.snap_mgr.get_snapshot_for_gc(&key, is_sending) {
                    Ok(a) => a,
                    Err(e) => {
                        error!(%e;
                            "failed to load snapshot";
                            "region_id" => self.fsm.region_id(),
                            "peer_id" => self.fsm.peer_id(),
                            "snap_file" => %key,
                        );
                        continue;
                    }
                };
                schedule_delete_snapshot_files(key, a);
            }
        }
    }

    fn on_clear_region_size(&mut self) {
        self.fsm.peer.split_check_trigger.on_clear_region_size();
        self.register_split_region_check_tick();
    }

    fn on_capture_change(
        &mut self,
        cmd: ChangeObserver,
        region_epoch: RegionEpoch,
        cb: Callback<EK::Snapshot>,
    ) {
        fail_point!("raft_on_capture_change");
        let region_id = self.region_id();
        let mut msg =
            new_read_index_request(region_id, region_epoch.clone(), self.fsm.peer.peer.clone());
        // Allow to capture change even is in flashback state.
        // TODO: add a test case for this kind of situation.
        if self.region().is_in_flashback {
            let mut flags = WriteBatchFlags::from_bits_check(msg.get_header().get_flags());
            flags.insert(WriteBatchFlags::FLASHBACK);
            msg.mut_header().set_flags(flags.bits());
        }
        let apply_router = self.ctx.apply_router.clone();
        self.propose_raft_command_internal(
            msg,
            Callback::read(Box::new(move |resp| {
                // Return the error
                if resp.response.get_header().has_error() {
                    cb.invoke_read(resp);
                    return;
                }
                apply_router.schedule_task(
                    region_id,
                    ApplyTask::Change {
                        cmd,
                        region_epoch,
                        cb,
                    },
                )
            })),
            DiskFullOpt::NotAllowedOnFull,
        );
    }

    fn on_significant_msg(&mut self, msg: Box<SignificantMsg<EK::Snapshot>>) {
        match *msg {
            SignificantMsg::SnapshotStatus {
                to_peer_id, status, ..
            } => {
                // Report snapshot status to the corresponding peer.
                self.report_snapshot_status(to_peer_id, status);
            }
            SignificantMsg::Unreachable { to_peer_id, .. } => {
                if self.fsm.peer.is_leader() {
                    self.fsm.peer.raft_group.report_unreachable(to_peer_id);
                    let unreachable_store_id =
                        find_peer(self.fsm.peer.region(), to_peer_id).map(|p| p.get_store_id());
                    self.fsm
                        .peer
                        .maybe_cancel_gen_snap_task(unreachable_store_id);
                } else if to_peer_id == self.fsm.peer.leader_id() {
                    self.fsm.reset_hibernate_state(GroupState::Chaos);
                    self.register_raft_base_tick();
                }
            }
            SignificantMsg::StoreUnreachable { store_id } => {
                if let Some(peer_id) = find_peer(self.region(), store_id).map(|p| p.get_id()) {
                    if self.fsm.peer.is_leader() {
                        self.fsm.peer.raft_group.report_unreachable(peer_id);
                        self.fsm.peer.maybe_cancel_gen_snap_task(Some(store_id));
                    } else if peer_id == self.fsm.peer.leader_id() {
                        self.fsm.reset_hibernate_state(GroupState::Chaos);
                        self.register_raft_base_tick();
                    }
                }
            }
            SignificantMsg::MergeResult {
                target_region_id,
                target,
                result,
            } => {
                self.on_merge_result(target_region_id, target, result);
            }
            SignificantMsg::CatchUpLogs(catch_up_logs) => {
                self.on_catch_up_logs_for_merge(catch_up_logs);
            }
            SignificantMsg::StoreResolved { group_id, store_id } => {
                let state = self.ctx.global_replication_state.lock().unwrap();
                if state.status().get_mode() != ReplicationMode::DrAutoSync {
                    return;
                }
                if state.status().get_dr_auto_sync().get_state() == DrAutoSyncState::Async {
                    return;
                }
                drop(state);
                if let Some(peer_id) = find_peer(self.region(), store_id).map(|p| p.get_id()) {
                    self.fsm
                        .peer
                        .raft_group
                        .raft
                        .assign_commit_groups(&[(peer_id, group_id)]);
                }
            }
            SignificantMsg::CaptureChange {
                cmd,
                region_epoch,
                callback,
            } => self.on_capture_change(cmd, region_epoch, callback),
            SignificantMsg::LeaderCallback(cb) => {
                self.on_leader_callback(cb);
            }
            SignificantMsg::RaftLogGcFlushed => {
                self.on_raft_log_gc_flushed();
            }
            SignificantMsg::RaftlogFetched(fetched_logs) => {
                self.on_raft_log_fetched(fetched_logs.context, fetched_logs.logs);
            }
            SignificantMsg::EnterForceLeaderState {
                syncer,
                failed_stores,
            } => {
                self.on_enter_pre_force_leader(syncer, failed_stores);
            }
            SignificantMsg::ExitForceLeaderState => self.on_exit_force_leader(false),
            SignificantMsg::UnsafeRecoveryDemoteFailedVoters {
                syncer,
                failed_voters,
            } => self.on_unsafe_recovery_pre_demote_failed_voters(syncer, failed_voters),
            SignificantMsg::UnsafeRecoveryDestroy(syncer) => {
                self.on_unsafe_recovery_destroy(syncer)
            }
            SignificantMsg::UnsafeRecoveryWaitApply(syncer) => {
                self.on_unsafe_recovery_wait_apply(syncer)
            }
            SignificantMsg::UnsafeRecoveryFillOutReport(syncer) => {
                self.on_unsafe_recovery_fill_out_report(syncer)
            }
            // for snapshot recovery (safe recovery)
            SignificantMsg::SnapshotBrWaitApply(syncer) => self.on_snapshot_br_wait_apply(syncer),
            SignificantMsg::CheckPendingAdmin(ch) => self.on_check_pending_admin(ch),
        }
    }

    fn on_enter_pre_force_leader(
        &mut self,
        syncer: UnsafeRecoveryForceLeaderSyncer,
        failed_stores: HashSet<u64>,
    ) {
        match self.fsm.peer.force_leader {
            Some(ForceLeaderState::PreForceLeader { .. }) => {
                self.on_force_leader_fail();
            }
            Some(ForceLeaderState::ForceLeader { .. }) => {
                // already is a force leader, do nothing
                return;
            }
            Some(ForceLeaderState::WaitTicks { .. }) => {
                self.fsm.peer.force_leader = None;
            }
            Some(ForceLeaderState::WaitForceCompact { .. }) => {
                self.fsm.peer.force_leader = None;
            }
            None => {}
        }

        if !self.fsm.peer.is_initialized() {
            warn!(
                "Unsafe recovery, cannot force leader since this peer is not initialized";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
            );
            return;
        }

        let ticks = if self.fsm.peer.is_leader() {
            if self.fsm.hibernate_state.group_state() == GroupState::Ordered {
                warn!(
                    "Unsafe recovery, reject pre force leader due to already being leader";
                    "region_id" => self.fsm.region_id(),
                    "peer_id" => self.fsm.peer_id(),
                );
                return;
            }
            // wait two rounds of election timeout to trigger check quorum to step down the
            // leader note: check quorum is triggered every `election_timeout` instead of
            // `randomized_election_timeout`
            Some(
                self.fsm.peer.raft_group.raft.election_timeout() * 2
                    - self.fsm.peer.raft_group.raft.election_elapsed,
            )
        // When election timeout is triggered, leader_id is set to INVALID_ID.
        // But learner(not promotable) is a exception here as it wouldn't tick
        // election.
        } else if self.fsm.peer.raft_group.raft.promotable()
            && self.fsm.peer.leader_id() != raft::INVALID_ID
        {
            if self.fsm.hibernate_state.group_state() == GroupState::Ordered
                || self.fsm.hibernate_state.group_state() == GroupState::Chaos
            {
                warn!(
                    "Unsafe recovery, reject pre force leader due to leader lease may not expired";
                    "region_id" => self.fsm.region_id(),
                    "peer_id" => self.fsm.peer_id(),
                );
                return;
            }
            // wait one round of election timeout to make sure leader_id is invalid
            Some(
                self.fsm.peer.raft_group.raft.randomized_election_timeout()
                    - self.fsm.peer.raft_group.raft.election_elapsed,
            )
        } else {
            None
        };

        if let Some(ticks) = ticks {
            info!(
                "Unsafe recovery, enter wait ticks";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "ticks" => ticks,
            );
            self.fsm.peer.force_leader = Some(ForceLeaderState::WaitTicks {
                syncer,
                failed_stores,
                ticks,
            });
            self.reset_raft_tick(if self.fsm.peer.is_leader() {
                GroupState::Ordered
            } else {
                GroupState::Chaos
            });
            self.fsm.has_ready = true;
            return;
        }

        // The applied index is ahead of raft last index, that means some raft logs are
        // missing. schedule a UnsafeForceCompact task to let ApplyFsm advance
        // the committed index and compact index to the applied index so raft
        // and apply state are compatible with each other. This can happen when
        // feature "apply unpersisted raft log" is enable(by setting config
        // `raftstore.max-apply-unpersisted-log-limit` > 0).
        if self.fsm.peer.raft_group.raft.r.raft_log.last_index()
            < self.fsm.peer.raft_group.raft.r.raft_log.applied
        {
            self.ctx.apply_router.schedule_task(
                self.region_id(),
                ApplyTask::UnsafeForceCompact {
                    region_id: self.region_id(),
                    compact_index: self.fsm.peer.raft_group.raft.r.raft_log.applied,
                    term: self.fsm.peer.raft_group.raft.r.term,
                },
            );

            self.fsm.peer.force_leader = Some(ForceLeaderState::WaitForceCompact {
                syncer,
                failed_stores,
            });
            return;
        }

        let expected_alive_voter = self.get_force_leader_expected_alive_voter(&failed_stores);
        if !expected_alive_voter.is_empty()
            && self
                .fsm
                .peer
                .raft_group
                .raft
                .prs()
                .has_quorum(&expected_alive_voter)
        {
            warn!(
                "Unsafe recovery, reject pre force leader due to has quorum";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
            );
            return;
        }

        info!(
            "Unsafe recovery, enter pre force leader state";
            "region_id" => self.fsm.region_id(),
            "peer_id" => self.fsm.peer_id(),
            "alive_voter" => ?expected_alive_voter,
        );

        // Do not use prevote as prevote won't set `vote` to itself.
        // When PD issues force leader on two different peer, it may cause
        // two force leader in same term.
        self.fsm.peer.raft_group.raft.pre_vote = false;
        // trigger vote request to all voters, will check the vote result in
        // `check_force_leader`
        if let Err(e) = self.fsm.peer.raft_group.campaign() {
            warn!(
                "Unsafe recovery, campaign failed";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "err" => ?e,
            );
        }
        assert_eq!(self.fsm.peer.get_role(), StateRole::Candidate);
        if !self
            .fsm
            .peer
            .raft_group
            .raft
            .prs()
            .votes()
            .get(&self.fsm.peer.peer_id())
            .unwrap()
        {
            warn!(
                "Unsafe recovery, pre force leader failed to campaign";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
            );
            self.on_force_leader_fail();
            return;
        }

        self.fsm.peer.force_leader = Some(ForceLeaderState::PreForceLeader {
            syncer,
            failed_stores,
        });
        self.fsm.has_ready = true;
    }

    fn on_force_leader_fail(&mut self) {
        self.fsm.peer.raft_group.raft.pre_vote = true;
        self.fsm.peer.raft_group.raft.set_check_quorum(true);
        self.fsm.peer.force_leader = None;
    }

    fn on_enter_force_leader(&mut self) {
        info!(
            "Unsafe recovery, enter force leader state";
            "region_id" => self.fsm.region_id(),
            "peer_id" => self.fsm.peer_id(),
        );
        assert_eq!(self.fsm.peer.get_role(), StateRole::Candidate);

        let failed_stores = match self.fsm.peer.force_leader.take() {
            Some(ForceLeaderState::PreForceLeader { failed_stores, .. }) => failed_stores,
            _ => unreachable!(),
        };

        let peer_ids: Vec<_> = self.fsm.peer.voters().iter().collect();
        for peer_id in peer_ids {
            let store_id = self
                .region()
                .get_peers()
                .iter()
                .find(|p| p.get_id() == peer_id)
                .unwrap()
                .get_store_id();
            if !failed_stores.contains(&store_id) {
                continue;
            }

            // make fake vote response
            let mut msg = raft::eraftpb::Message::new();
            msg.msg_type = MessageType::MsgRequestVoteResponse;
            msg.reject = false;
            msg.term = self.fsm.peer.term();
            msg.from = peer_id;
            msg.to = self.fsm.peer.peer_id();
            self.fsm.peer.raft_group.step(msg).unwrap();
        }

        // after receiving all votes, should become leader
        assert!(self.fsm.peer.is_leader());
        self.fsm.peer.raft_group.raft.set_check_quorum(false);

        // make sure it's not hibernated
        self.reset_raft_tick(GroupState::Ordered);

        self.fsm.peer.force_leader = Some(ForceLeaderState::ForceLeader {
            time: TiInstant::now_coarse(),
            failed_stores,
        });
        self.fsm.has_ready = true;
    }

    fn on_exit_force_leader(&mut self, force: bool) {
        if self.fsm.peer.force_leader.is_none() {
            return;
        }
        if let Some(UnsafeRecoveryState::Failed) = self.fsm.peer.unsafe_recovery_state
            && !force
        {
            // Skip force leader if the plan failed, so wait for the next retry of plan with
            // force leader state holding
            info!(
                "skip exiting force leader state";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
            );
            return;
        }

        info!(
            "exit force leader state";
            "region_id" => self.fsm.region_id(),
            "peer_id" => self.fsm.peer_id(),
        );
        self.fsm.peer.force_leader = None;
        // make sure it's not hibernated
        assert_ne!(self.fsm.hibernate_state.group_state(), GroupState::Idle);
        // leader lease shouldn't be renewed in force leader state.
        assert_eq!(
            self.fsm.peer.leader_lease().inspect(None),
            LeaseState::Expired
        );
        self.fsm
            .peer
            .raft_group
            .raft
            .become_follower(self.fsm.peer.term(), raft::INVALID_ID);

        self.fsm.peer.raft_group.raft.set_check_quorum(true);
        self.fsm.peer.raft_group.raft.pre_vote = true;
        if self.fsm.peer.raft_group.raft.promotable() {
            // Do not campaign directly here, otherwise on_role_changed() won't called for
            // follower state
            let _ = self.ctx.router.send(
                self.region_id(),
                PeerMsg::CasualMessage(Box::new(CasualMessage::Campaign)),
            );
        }
        self.fsm.has_ready = true;
    }

    #[inline]
    fn get_force_leader_expected_alive_voter(&self, failed_stores: &HashSet<u64>) -> HashSet<u64> {
        let region = self.region();
        self.fsm
            .peer
            .voters()
            .iter()
            .filter(|peer_id| {
                let store_id = region
                    .get_peers()
                    .iter()
                    .find(|p| p.get_id() == *peer_id)
                    .unwrap()
                    .get_store_id();
                !failed_stores.contains(&store_id)
            })
            .collect()
    }

    #[inline]
    fn check_force_leader(&mut self) {
        if let Some(ForceLeaderState::WaitTicks {
            syncer,
            failed_stores,
            ticks,
        }) = &mut self.fsm.peer.force_leader
        {
            if *ticks == 0 {
                let syncer_clone = syncer.clone();
                let s = mem::take(failed_stores);
                self.on_enter_pre_force_leader(syncer_clone, s);
            } else {
                *ticks -= 1;
            }
            return;
        };

        let failed_stores = match &self.fsm.peer.force_leader {
            None => return,
            Some(ForceLeaderState::ForceLeader { .. }) => {
                if self.fsm.peer.maybe_force_forward_commit_index() {
                    self.fsm.has_ready = true;
                }
                return;
            }
            Some(ForceLeaderState::PreForceLeader { failed_stores, .. }) => failed_stores,
            Some(ForceLeaderState::WaitForceCompact { .. }) => return,
            Some(ForceLeaderState::WaitTicks { .. }) => unreachable!(),
        };

        if self.fsm.peer.raft_group.raft.election_elapsed + 1
            < self.ctx.cfg.raft_election_timeout_ticks
        {
            // wait as longer as it can to collect responses of request vote
            return;
        }

        let expected_alive_voter: HashSet<_> =
            self.get_force_leader_expected_alive_voter(failed_stores);
        let check = || {
            if self.fsm.peer.raft_group.raft.state != StateRole::Candidate {
                Err(format!(
                    "unexpected role {:?}",
                    self.fsm.peer.raft_group.raft.state
                ))
            } else {
                let mut granted = 0;
                for (id, vote) in self.fsm.peer.raft_group.raft.prs().votes() {
                    if expected_alive_voter.contains(id) {
                        if *vote {
                            granted += 1;
                        } else {
                            return Err(format!("receive reject response from {}", *id));
                        }
                    } else if *id == self.fsm.peer_id() {
                        // self may be a learner
                        continue;
                    } else {
                        return Err(format!(
                            "receive unexpected vote from {} vote {}",
                            *id, *vote
                        ));
                    }
                }
                Ok(granted)
            }
        };

        match check() {
            Err(err) => {
                warn!(
                    "Unsafe recovery, pre force leader check failed";
                    "region_id" => self.fsm.region_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "alive_voter" => ?expected_alive_voter,
                    "reason" => err,
                );
                self.on_force_leader_fail();
            }
            Ok(granted) => {
                info!(
                    "Unsafe recovery, expected live voters:";
                    "voters" => ?expected_alive_voter,
                    "granted" => granted,
                    "region_id" => self.fsm.region_id(),
                    "peer_id" => self.fsm.peer_id(),
                );
                if granted == expected_alive_voter.len() {
                    self.on_enter_force_leader();
                }
            }
        }
    }

    fn on_raft_log_fetched(&mut self, context: GetEntriesContext, res: Box<RaftlogFetchResult>) {
        let low = res.low;
        // If the peer is not the leader anymore and it's not in entry cache warmup
        // state, or it is being destroyed, ignore the result.
        if !self.fsm.peer.is_leader()
            && self
                .fsm
                .peer
                .get_store()
                .entry_cache_warmup_state()
                .is_none()
            || self.fsm.peer.pending_remove
        {
            self.fsm.peer.mut_store().clean_async_fetch_res(low);
            return;
        }

        if self.fsm.peer.term() != res.term {
            // term has changed, the result may be not correct.
            self.fsm.peer.mut_store().clean_async_fetch_res(low);
        } else if self
            .fsm
            .peer
            .get_store()
            .entry_cache_warmup_state()
            .is_some()
        {
            if self.fsm.peer.mut_store().maybe_warm_up_entry_cache(*res) {
                self.fsm.peer.ack_transfer_leader_msg(false);
                self.fsm.has_ready = true;
            }
            self.fsm.peer.mut_store().clean_async_fetch_res(low);
            return;
        } else {
            self.fsm
                .peer
                .mut_store()
                .update_async_fetch_res(low, Some(res));
        }
        self.fsm.peer.raft_group.on_entries_fetched(context);
        // clean the async fetch result immediately if not used to free memory
        self.fsm.peer.mut_store().update_async_fetch_res(low, None);
        self.fsm.has_ready = true;
    }

    fn on_persisted_msg(&mut self, peer_id: u64, ready_number: u64) {
        if peer_id != self.fsm.peer_id() {
            error!(
                "peer id not match";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "persisted_peer_id" => peer_id,
                "persisted_number" => ready_number,
            );
            return;
        }
        if let Some(persist_snap_res) = self.fsm.peer.on_persist_ready(self.ctx, ready_number) {
            self.on_ready_persist_snapshot(persist_snap_res);
            if self.fsm.peer.pending_merge_state.is_some() {
                // After applying a snapshot, merge is rollbacked implicitly.
                self.on_ready_rollback_merge(0, None);
            }
            self.register_raft_base_tick();
        }

        self.fsm.has_ready = true;

        if let Some(delay) = self.fsm.delayed_destroy {
            if delay.reason == DelayReason::UnPersistedReady
                && !self.fsm.peer.has_unpersisted_ready()
            {
                self.destroy_peer(delay.merged_by_target);
            }
        }
    }

    pub fn post_raft_ready_append(&mut self) {
        if let Some(persist_snap_res) = self.fsm.peer.handle_raft_ready_advance(self.ctx) {
            self.on_ready_persist_snapshot(persist_snap_res);
            if self.fsm.peer.pending_merge_state.is_some() {
                // After applying a snapshot, merge is rollbacked implicitly.
                self.on_ready_rollback_merge(0, None);
            }
            self.register_raft_base_tick();
        }
    }

    fn report_snapshot_status(&mut self, to_peer_id: u64, status: SnapshotStatus) {
        let to_peer = match self.fsm.peer.get_peer_from_cache(to_peer_id) {
            Some(peer) => peer,
            None => {
                // If to_peer is gone, ignore this snapshot status
                warn!(
                    "peer not found, ignore snapshot status";
                    "region_id" => self.region_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "to_peer_id" => to_peer_id,
                    "status" => ?status,
                );
                return;
            }
        };
        info!(
            "report snapshot status";
            "region_id" => self.fsm.region_id(),
            "peer_id" => self.fsm.peer_id(),
            "to" => ?to_peer,
            "status" => ?status,
        );
        self.fsm.peer.raft_group.report_snapshot(to_peer_id, status)
    }

    fn on_leader_callback(&mut self, cb: Callback<EK::Snapshot>) {
        let msg = new_read_index_request(
            self.region_id(),
            self.region().get_region_epoch().clone(),
            self.fsm.peer.peer.clone(),
        );
        self.propose_raft_command_internal(msg, cb, DiskFullOpt::NotAllowedOnFull);
    }

    fn on_role_changed(&mut self, role: Option<StateRole>) {
        // Update leader lease when the Raft state changes.
        if let Some(r) = role {
            if StateRole::Leader == r {
                self.fsm.missing_ticks = 0;
                self.register_split_region_check_tick();
                self.fsm.peer.heartbeat_pd(self.ctx);
                self.register_pd_heartbeat_tick();
                self.register_raft_gc_log_tick();
                self.register_check_leader_lease_tick();
                self.register_report_region_buckets_tick();
                self.register_check_peers_availability_tick();
                self.register_check_long_uncommitted_tick();
            }

            if self.fsm.peer.is_in_force_leader() && r != StateRole::Leader {
                // for some reason, it's not leader anymore
                info!(
                    "step down in force leader state";
                    "region_id" => self.fsm.region_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "state" => ?r,
                );
                self.on_force_leader_fail();
            }
        }
    }

    /// Collect ready if any.
    ///
    /// Returns false is no readiness is generated.
    pub fn collect_ready(&mut self) -> bool {
        let has_ready = self.fsm.has_ready;
        self.fsm.has_ready = false;
        if !has_ready || self.fsm.stopped {
            return false;
        }
        self.ctx.pending_count += 1;
        self.ctx.has_ready = true;
        let res = self.fsm.peer.handle_raft_ready_append(self.ctx);
        if let Some(r) = res {
            self.on_role_changed(r.state_role);
            if r.has_new_entries {
                self.register_raft_gc_log_tick();
                self.register_entry_cache_evict_tick();
            }
            self.ctx.ready_count += 1;
            self.ctx.raft_metrics.ready.has_ready_region.inc();

            if self.fsm.peer.leader_unreachable {
                self.fsm.reset_hibernate_state(GroupState::Chaos);
                self.register_raft_base_tick();
                self.fsm.peer.leader_unreachable = false;
            }

            return r.has_write_ready;
        }
        false
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
    fn peer(&self) -> &metapb::Peer {
        &self.fsm.peer.peer
    }

    #[inline]
    fn store_id(&self) -> u64 {
        self.fsm.peer.peer.get_store_id()
    }

    #[inline]
    fn schedule_tick(&mut self, tick: PeerTick) {
        let idx = tick as usize;
        if self.fsm.tick_registry[idx] {
            return;
        }
        if is_zero_duration(&self.ctx.tick_batch[idx].wait_duration) {
            return;
        }
        trace!(
            "schedule tick";
            "tick" => ?tick,
            "timeout" => ?self.ctx.tick_batch[idx].wait_duration,
            "region_id" => self.region_id(),
            "peer_id" => self.fsm.peer_id(),
        );
        self.fsm.tick_registry[idx] = true;

        let region_id = self.region_id();
        let mb = match self.ctx.router.mailbox(region_id) {
            Some(mb) => mb,
            None => {
                self.fsm.tick_registry[idx] = false;
                error!(
                    "failed to get mailbox";
                    "region_id" => self.fsm.region_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "tick" => ?tick,
                );
                return;
            }
        };
        let peer_id = self.fsm.peer.peer_id();
        let cb = Box::new(move || {
            // This can happen only when the peer is about to be destroyed
            // or the node is shutting down. So it's OK to not to clean up
            // registry.
            if let Err(e) = mb.force_send(PeerMsg::Tick(tick)) {
                debug!(
                    "failed to schedule peer tick";
                    "region_id" => region_id,
                    "peer_id" => peer_id,
                    "tick" => ?tick,
                    "err" => %e,
                );
            }
        });
        self.ctx.tick_batch[idx].ticks.push(cb);
    }

    fn register_raft_base_tick(&mut self) {
        // If we register raft base tick failed, the whole raft can't run correctly,
        // TODO: shutdown the store?
        self.schedule_tick(PeerTick::Raft)
    }

    fn on_raft_base_tick(&mut self) {
        fail_point!(
            "on_raft_base_tick_idle",
            self.fsm.hibernate_state.group_state() == GroupState::Idle,
            |_| {}
        );
        fail_point!(
            "on_raft_base_tick_chaos",
            self.fsm.hibernate_state.group_state() == GroupState::Chaos,
            |_| {}
        );

        if self.fsm.peer.pending_remove {
            self.fsm.peer.mut_store().flush_entry_cache_metrics();
            return;
        }

        // Update the state whether the peer is pending on applying raft
        // logs if necesssary.
        self.on_check_peer_complete_apply_logs();

        // If the peer is busy on apply and missing the last leader committed index,
        // it should propose a read index to check whether its lag is behind the leader.
        // It won't generate flooding fetching messages. This proposal will only be sent
        // out before it gets response and updates the `last_leader_committed_index`.
        self.try_to_fetch_committed_index();

        // When having pending snapshot, if election timeout is met, it can't pass
        // the pending conf change check because first index has been updated to
        // a value that is larger than last index.
        if self.fsm.peer.is_handling_snapshot() || self.fsm.peer.has_pending_snapshot() {
            // need to check if snapshot is applied.
            self.fsm.has_ready = true;
            self.fsm.missing_ticks = 0;
            self.register_raft_base_tick();
            return;
        }

        self.fsm.peer.retry_pending_reads(&self.ctx.cfg);

        self.check_force_leader();

        // If there has any uncleared records in the uncampaigned_new_regions list,
        // clear them if the timestamp exceeds the election timeout.
        {
            let (has_uncompaigned_regions, ts) = (
                !self.fsm.peer.uncampaigned_new_regions.0.is_empty(),
                self.fsm.peer.uncampaigned_new_regions.1,
            );
            if has_uncompaigned_regions {
                let max_election_timeout = Duration::from_millis(
                    self.ctx.cfg.raft_base_tick_interval.as_millis()
                        * (self.ctx.cfg.raft_election_timeout_ticks * 2 + 1) as u64,
                );
                if ts.elapsed() >= max_election_timeout {
                    self.fsm.peer.uncampaigned_new_regions.0.clear();
                }
            }
        }

        let mut res = None;
        if self.ctx.cfg.hibernate_regions {
            if self.fsm.hibernate_state.group_state() == GroupState::Idle {
                // missing_ticks should be less than election timeout ticks otherwise
                // follower may tick more than an election timeout in chaos state.
                // Before stopping tick, `missing_tick` should be `raft_election_timeout_ticks`
                // - 2 - `raft_heartbeat_ticks` (default 10 - 2 - 2 = 6) and the follower's
                //   `election_elapsed` in raft-rs is 1.
                // After the group state becomes Chaos, the next tick will call
                // `raft_group.tick` `missing_tick` + 1 times(default 7).
                // Then the follower's `election_elapsed` will be 1 + `missing_tick` + 1
                // (default 1 + 6 + 1 = 8) which is less than the min election timeout.
                // The reason is that we don't want let all followers become (pre)candidate if
                // one follower may receive a request, then becomes (pre)candidate and sends
                // (pre)vote msg to others. As long as the leader can wake up and broadcast
                // heartbeats in one `raft_heartbeat_ticks` time(default 2s), no more followers
                // will wake up and sends vote msg again.
                if self.fsm.missing_ticks + 1 /* for the next tick after the peer isn't Idle */
                    + self.fsm.peer.raft_group.raft.election_elapsed
                    + self.ctx.cfg.raft_heartbeat_ticks
                    < self.ctx.cfg.raft_election_timeout_ticks
                {
                    self.register_raft_base_tick();
                    self.fsm.missing_ticks += 1;
                } else {
                    debug!("follower hibernates";
                        "region_id" => self.region_id(),
                        "peer_id" => self.fsm.peer_id(),
                        "election_elapsed" => self.fsm.peer.raft_group.raft.election_elapsed,
                        "missing_ticks" => self.fsm.missing_ticks);
                }
                return;
            }
            res = Some(self.fsm.peer.check_before_tick(&self.ctx.cfg));
            if self.fsm.missing_ticks > 0 {
                for _ in 0..self.fsm.missing_ticks {
                    if self.fsm.peer.raft_group.tick() {
                        self.fsm.has_ready = true;
                    }
                }
                self.fsm.missing_ticks = 0;
            }
        }

        // Tick the raft peer and update some states which can be changed in `tick`.
        if self.fsm.peer.raft_group.tick() {
            self.fsm.has_ready = true;
        }
        self.fsm.peer.post_raft_group_tick();

        self.fsm.peer.mut_store().flush_entry_cache_metrics();

        // Keep ticking if there are still pending read requests or this node is within
        // hibernate timeout.
        if res.is_none() /* hibernate_region is false */ ||
            !self.fsm.peer.check_after_tick(self.fsm.hibernate_state.group_state(), res.unwrap()) ||
            (self.fsm.peer.is_leader() && !self.all_agree_to_hibernate())
        {
            self.register_raft_base_tick();
            // We need pd heartbeat tick to collect down peers and pending peers.
            self.register_pd_heartbeat_tick();
            return;
        }

        debug!("stop ticking"; "res" => ?res,
            "region_id" => self.region_id(),
            "peer_id" => self.fsm.peer_id(),
            "election_elapsed" => self.fsm.peer.raft_group.raft.election_elapsed);
        self.fsm.reset_hibernate_state(GroupState::Idle);
        // Followers will stop ticking at L789. Keep ticking for followers
        // to allow it to campaign quickly when abnormal situation is detected.
        if !self.fsm.peer.is_leader() {
            self.register_raft_base_tick();
        } else {
            self.register_pd_heartbeat_tick();
        }
    }

    fn check_unsafe_recovery_state(&mut self) {
        match &self.fsm.peer.unsafe_recovery_state {
            Some(UnsafeRecoveryState::WaitApply { .. }) => self
                .fsm
                .peer
                .unsafe_recovery_maybe_finish_wait_apply(/* force= */ false),
            Some(UnsafeRecoveryState::DemoteFailedVoters {
                syncer,
                failed_voters,
                target_index,
                demote_after_exit,
            }) => {
                if self.fsm.peer.raft_group.raft.raft_log.applied >= *target_index {
                    if *demote_after_exit {
                        let syncer_clone = syncer.clone();
                        let failed_voters_clone = failed_voters.clone();
                        self.fsm.peer.unsafe_recovery_state = None;
                        if !self.fsm.peer.is_in_force_leader() {
                            error!(
                                "Unsafe recovery, lost forced leadership after exiting joint state";
                                "region_id" => self.region().get_id(),
                                "peer_id" => self.fsm.peer_id(),
                            );
                            return;
                        }
                        self.unsafe_recovery_demote_failed_voters(
                            syncer_clone,
                            failed_voters_clone,
                        );
                    } else {
                        if self.fsm.peer.in_joint_state() {
                            info!(
                                "Unsafe recovery, exiting joint state";
                                "region_id" => self.region().get_id(),
                                "peer_id" => self.fsm.peer_id(),
                            );
                            if self.fsm.peer.is_in_force_leader() {
                                self.propose_raft_command_internal(
                                    exit_joint_request(self.region(), &self.fsm.peer.peer),
                                    Callback::<EK::Snapshot>::write(Box::new(|resp| {
                                        if resp.response.get_header().has_error() {
                                            error!(
                                                "Unsafe recovery, fail to exit joint state";
                                                "err" => ?resp.response.get_header().get_error(),
                                            );
                                        }
                                    })),
                                    DiskFullOpt::AllowedOnAlmostFull,
                                );
                            } else {
                                error!(
                                    "Unsafe recovery, lost forced leadership while trying to exit joint state";
                                    "region_id" => self.region().get_id(),
                                    "peer_id" => self.fsm.peer_id(),
                                );
                            }
                        }

                        self.fsm.peer.unsafe_recovery_state = None;
                    }
                }
            }
            // Destroy does not need be processed, the state is cleaned up together with peer.
            Some(UnsafeRecoveryState::Destroy { .. })
            | Some(UnsafeRecoveryState::Failed)
            | Some(UnsafeRecoveryState::WaitInitialize(..))
            | None => {}
        }
    }

    fn on_apply_res(&mut self, res: Box<ApplyTaskRes<EK::Snapshot>>) {
        fail_point!("on_apply_res", |_| {});
        match *res {
            ApplyTaskRes::Apply(mut res) => {
                debug!(
                    "async apply finish";
                    "region_id" => self.region_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "res" => ?res,
                );
                if self.fsm.peer.wait_data {
                    return;
                }
                self.on_ready_result(&mut res.exec_res, &res.metrics);
                if self.fsm.stopped {
                    return;
                }
                let applied_index = res.apply_state.applied_index;
                self.fsm
                    .peer
                    .region_buckets_info_mut()
                    .add_bucket_flow(&res.bucket_stat);

                self.fsm.has_ready |= self.fsm.peer.post_apply(
                    self.ctx,
                    res.apply_state,
                    res.applied_term,
                    &res.metrics,
                );
                // After applying, several metrics are updated, report it to pd to
                // get fair schedule.
                if self.fsm.peer.is_leader() {
                    self.register_pd_heartbeat_tick();
                    self.register_split_region_check_tick();
                    self.retry_pending_prepare_merge(applied_index);
                    self.fsm
                        .peer
                        .maybe_update_apply_unpersisted_log_state(applied_index);
                }
            }
            ApplyTaskRes::Destroy {
                region_id,
                peer_id,
                merge_from_snapshot,
            } => {
                assert_eq!(peer_id, self.fsm.peer.peer_id());
                if !merge_from_snapshot {
                    self.destroy_peer(false);
                } else {
                    // Wait for its target peer to apply snapshot and then send `MergeResult` back
                    // to destroy itself
                    let mut meta = self.ctx.store_meta.lock().unwrap();
                    // The `need_atomic` flag must be true
                    assert!(*meta.destroyed_region_for_snap.get(&region_id).unwrap());

                    let target_region_id = *meta.targets_map.get(&region_id).unwrap();
                    let is_ready = meta
                        .atomic_snap_regions
                        .get_mut(&target_region_id)
                        .unwrap()
                        .get_mut(&region_id)
                        .unwrap();
                    *is_ready = true;
                }
            }
        }
        if self.fsm.peer.unsafe_recovery_state.is_some() {
            self.check_unsafe_recovery_state();
        }

        if self.fsm.peer.snapshot_recovery_state.is_some() {
            self.fsm
                .peer
                .snapshot_recovery_maybe_finish_wait_apply(false);
        }
    }

    fn retry_pending_prepare_merge(&mut self, applied_index: u64) {
        if self.fsm.peer.prepare_merge_fence > 0
            && applied_index >= self.fsm.peer.prepare_merge_fence
        {
            if let Some(pending_prepare_merge) = self.fsm.peer.pending_prepare_merge.take() {
                self.propose_raft_command_internal(
                    pending_prepare_merge,
                    Callback::None,
                    DiskFullOpt::AllowedOnAlmostFull,
                );
            }
            // When applied index reaches prepare_merge_fence, always clear the fence.
            // So, even if the PrepareMerge fails to propose, we can ensure the region
            // will be able to serve again.
            self.fsm.peer.prepare_merge_fence = 0;
            assert!(self.fsm.peer.pending_prepare_merge.is_none());
        }
    }

    // If lease expired, we will send a noop read index to renew lease.
    fn try_renew_leader_lease(&mut self, reason: &str) {
        debug!(
            "renew lease";
            "region_id" => self.region_id(),
            "peer_id" => self.fsm.peer_id(),
            "reason" => reason,
        );
        if !self.fsm.peer.is_leader() {
            return;
        }
        if let Err(e) = self.fsm.peer.pre_read_index() {
            debug!(
                "prevent unsafe read index to renew leader lease";
                "region_id" => self.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "err" => ?e,
            );
            self.ctx.raft_metrics.propose.unsafe_read_index.inc();
            return;
        }

        let current_time = *self.ctx.current_time.get_or_insert_with(monotonic_raw_now);
        if self.fsm.peer.need_renew_lease_at(self.ctx, current_time) {
            let mut cmd = new_read_index_request(
                self.region_id(),
                self.region().get_region_epoch().clone(),
                self.fsm.peer.peer.clone(),
            );
            cmd.mut_header().set_read_quorum(true);
            self.propose_raft_command_internal(
                cmd,
                Callback::read(Box::new(|_| ())),
                DiskFullOpt::AllowedOnAlmostFull,
            );
        }
    }

    fn handle_reported_disk_usage(&mut self, msg: &RaftMessage) {
        let store_id = msg.get_from_peer().get_store_id();
        let peer_id = msg.get_from_peer().get_id();
        let refill_disk_usages = if matches!(msg.disk_usage, DiskUsage::Normal) {
            self.ctx.store_disk_usages.remove(&store_id);
            if !self.fsm.peer.is_leader() {
                return;
            }
            self.fsm.peer.disk_full_peers.has(peer_id)
        } else {
            self.ctx.store_disk_usages.insert(store_id, msg.disk_usage);
            if !self.fsm.peer.is_leader() {
                return;
            }
            let disk_full_peers = &self.fsm.peer.disk_full_peers;

            disk_full_peers.is_empty()
                || disk_full_peers
                    .get(peer_id)
                    .map_or(true, |x| x != msg.disk_usage)
        };
        if refill_disk_usages || self.fsm.peer.has_region_merge_proposal {
            let prev = self.fsm.peer.disk_full_peers.get(peer_id);
            if Some(msg.disk_usage) != prev {
                info!(
                    "reported disk usage changes {:?} -> {:?}", prev, msg.disk_usage;
                    "region_id" => self.fsm.region_id(),
                    "peer_id" => peer_id,
                );
            }
            self.fsm.peer.refill_disk_full_peers(self.ctx);
            debug!(
                "raft message refills disk full peers to {:?}",
                self.fsm.peer.disk_full_peers;
                "region_id" => self.fsm.region_id(),
            );
        }
    }

    fn on_raft_message(&mut self, m: Box<InspectedRaftMessage>) -> Result<()> {
        let InspectedRaftMessage { heap_size, mut msg } = *m;
        let peer_disk_usage = msg.disk_usage;
        let stepped = Cell::new(false);
        let memtrace_raft_entries = &mut self.fsm.peer.memtrace_raft_entries as *mut usize;
        defer!({
            fail_point!(
                "memtrace_raft_messages_overflow_check_peer_recv",
                MEMTRACE_RAFT_MESSAGES.sum() < heap_size,
                |_| {}
            );
            MEMTRACE_RAFT_MESSAGES.trace(TraceEvent::Sub(heap_size));
            if stepped.get() {
                unsafe {
                    // It could be less than exact for entry overwriting.
                    *memtrace_raft_entries += heap_size;
                    MEMTRACE_RAFT_ENTRIES.trace(TraceEvent::Add(heap_size));
                }
            }
        });

        let is_initialized_peer = self.fsm.peer.is_initialized();
        debug!(
            "handle raft message";
            "region_id" => self.region_id(),
            "peer_id" => self.fsm.peer_id(),
            "message_type" => %util::MsgType(&msg),
            "from_peer_id" => msg.get_from_peer().get_id(),
            "to_peer_id" => msg.get_to_peer().get_id(),
            "is_initialized_peer" => is_initialized_peer,
        );

        let msg_type = msg.get_message().get_msg_type();
        #[cfg(feature = "failpoints")]
        let fp_enable = |target_msg_type: MessageType| -> bool {
            self.fsm.region_id() == 1000
                && self.store_id() == 2
                && !is_initialized_peer
                && msg_type == target_msg_type
        };
        fail_point!(
            "on_snap_msg_1000_2",
            fp_enable(MessageType::MsgSnapshot),
            |_| Ok(())
        );
        fail_point!(
            "on_vote_msg_1000_2",
            fp_enable(MessageType::MsgRequestVote),
            |_| Ok(())
        );
        fail_point!(
            "on_append_msg_1000_2",
            fp_enable(MessageType::MsgAppend),
            |_| Ok(())
        );
        fail_point!(
            "on_heartbeat_msg_1000_2",
            fp_enable(MessageType::MsgHeartbeat),
            |_| Ok(())
        );

        if self.fsm.peer.pending_remove || self.fsm.stopped {
            return Ok(());
        }

        self.handle_reported_disk_usage(&msg);

        if matches!(self.ctx.self_disk_usage, DiskUsage::AlreadyFull)
            && MessageType::MsgTimeoutNow == msg_type
        {
            debug!(
                "skip {:?} because of disk full", msg_type;
                "region_id" => self.region_id(), "peer_id" => self.fsm.peer_id()
            );
            self.ctx.raft_metrics.message_dropped.disk_full.inc();
            return Ok(());
        }

        if MessageType::MsgAppend == msg_type
            && self.fsm.peer.wait_data
            && self.fsm.peer.should_reject_msgappend
        {
            debug!("skip {:?} because of non-witness waiting data", msg_type;
                   "region_id" => self.region_id(), "peer_id" => self.fsm.peer_id()
            );
            self.ctx.raft_metrics.message_dropped.non_witness.inc();
            return Ok(());
        }

        if !self.validate_raft_msg(&msg) {
            return Ok(());
        }

        if msg.get_is_tombstone() {
            // we receive a message tells us to remove ourself.
            self.handle_gc_peer_msg(&msg);
            return Ok(());
        }

        if msg.has_merge_target() {
            fail_point!("on_has_merge_target", |_| Ok(()));
            if self.need_gc_merge(&msg)? {
                self.on_stale_merge(msg.get_merge_target().get_id());
            }
            return Ok(());
        }

        if self.check_msg(&msg) {
            return Ok(());
        }

        // If this peer is restarting, it may lose some logs, so it should update
        // the `last_leader_committed_idx` with the commited index of the first
        // `MsgAppend`` message or the committed index in `MsgReadIndexResp` it received
        // from leader.
        if self.fsm.peer.needs_update_last_leader_committed_idx()
            && (MessageType::MsgAppend == msg_type || MessageType::MsgReadIndexResp == msg_type)
        {
            let committed_index = cmp::max(
                msg.get_message().get_commit(), // from MsgAppend
                msg.get_message().get_index(),  // from MsgReadIndexResp
            );
            self.fsm
                .peer
                .update_last_leader_committed_idx(committed_index);
        }

        if msg.has_extra_msg() {
            self.on_extra_message(msg);
            return Ok(());
        }

        let is_snapshot = msg.get_message().has_snapshot();

        let regions_to_destroy = match self.check_snapshot(&msg)? {
            Either::Left(key) => {
                if let Some(key) = key {
                    // If the snapshot file is not used again, then it's OK to
                    // delete them here. If the snapshot file will be reused when
                    // receiving, then it will fail to pass the check again, so
                    // missing snapshot files should not be noticed.
                    let snap = self.ctx.snap_mgr.get_snapshot_for_gc(&key, false)?;
                    if let Err(e) = self.ctx.cleanup_scheduler.schedule(CleanupTask::GcSnapshot(
                        GcSnapshotTask::DeleteSnapshotFiles {
                            key: key.clone(),
                            snapshot: snap,
                            check_entry: false,
                        },
                    )) {
                        error!(
                            "failed to schedule task to delete snap file";
                            "key" => %key,
                            "err" => %e,
                        )
                    }
                }
                return Ok(());
            }
            Either::Right(v) => v,
        };

        if util::is_vote_msg(msg.get_message()) || msg_type == MessageType::MsgTimeoutNow {
            if self.fsm.hibernate_state.group_state() != GroupState::Chaos {
                self.fsm.reset_hibernate_state(GroupState::Chaos);
                self.register_raft_base_tick();
            }
        } else if msg.get_from_peer().get_id() == self.fsm.peer.leader_id() {
            self.reset_raft_tick(GroupState::Ordered);
        }

        let from_peer_id = msg.get_from_peer().get_id();
        self.fsm.peer.insert_peer_cache(msg.take_from_peer());

        let result = if msg_type == MessageType::MsgTransferLeader {
            self.on_transfer_leader_msg(msg.get_message(), peer_disk_usage);
            Ok(())
        } else {
            // This can be a message that sent when it's still a follower. Nevertheleast,
            // it's meaningless to continue to handle the request as callbacks are cleared.
            if msg_type == MessageType::MsgReadIndex
                && self.fsm.peer.is_leader()
                && (msg.get_message().get_from() == raft::INVALID_ID
                    || msg.get_message().get_from() == self.fsm.peer_id())
            {
                self.ctx.raft_metrics.message_dropped.stale_msg.inc();
                return Ok(());
            }
            self.fsm.peer.step(self.ctx, msg.take_message())
        };

        stepped.set(result.is_ok());

        if is_snapshot {
            if !self.fsm.peer.has_pending_snapshot() {
                // This snapshot is rejected by raft-rs.
                let mut meta = self.ctx.store_meta.lock().unwrap();
                meta.pending_snapshot_regions
                    .retain(|r| self.fsm.region_id() != r.get_id());
            } else {
                // This snapshot may be accepted by raft-rs.
                // If it's rejected by raft-rs, the snapshot region in
                // `pending_snapshot_regions` will be removed together with the latest snapshot
                // region after applying that snapshot.
                // But if `regions_to_destroy` is not empty, the pending snapshot must be this
                // msg's snapshot because this kind of snapshot is exclusive.
                self.destroy_regions_for_snapshot(regions_to_destroy);
            }
        }

        result?;

        if self.fsm.peer.any_new_peer_catch_up(from_peer_id) {
            self.fsm.peer.heartbeat_pd(self.ctx);
            self.fsm.peer.should_wake_up = true;
        }

        if self.fsm.peer.should_wake_up {
            self.reset_raft_tick(GroupState::Ordered);
        }

        self.fsm.has_ready = true;
        Ok(())
    }

    fn all_agree_to_hibernate(&mut self) -> bool {
        if self.fsm.maybe_hibernate() {
            return true;
        }
        if !self
            .fsm
            .hibernate_state
            .should_bcast(&self.ctx.feature_gate)
        {
            return false;
        }
        for peer in self.fsm.peer.region().get_peers() {
            if peer.get_id() == self.fsm.peer.peer_id() {
                continue;
            }

            let mut extra = ExtraMessage::default();
            extra.set_type(ExtraMessageType::MsgHibernateRequest);
            self.fsm
                .peer
                .send_extra_message(extra, &mut self.ctx.trans, peer);
        }
        false
    }

    fn on_hibernate_request(&mut self, from: &metapb::Peer) {
        if !self.ctx.cfg.hibernate_regions
            || self.fsm.peer.has_uncommitted_log()
            || self.fsm.peer.wait_data
            || from.get_id() != self.fsm.peer.leader_id()
        {
            // Ignore the message means rejecting implicitly.
            return;
        }
        let mut extra = ExtraMessage::default();
        extra.set_type(ExtraMessageType::MsgHibernateResponse);
        self.fsm
            .peer
            .send_extra_message(extra, &mut self.ctx.trans, from);
    }

    fn on_hibernate_response(&mut self, from: &metapb::Peer) {
        if !self.fsm.peer.is_leader() {
            return;
        }
        if self
            .fsm
            .peer
            .region()
            .get_peers()
            .iter()
            .all(|p| p.get_id() != from.get_id())
        {
            return;
        }
        self.fsm.hibernate_state.count_vote(from.get_id());
    }

    fn on_availability_response(&mut self, from: &metapb::Peer, msg: &ExtraMessage) {
        if !self.fsm.peer.is_leader() {
            return;
        }
        if !msg.wait_data {
            let original_remains_nr = self.fsm.peer.wait_data_peers.len();
            self.fsm
                .peer
                .wait_data_peers
                .retain(|id| *id != from.get_id());
            debug!(
                "receive peer ready info";
                "peer_id" => self.fsm.peer.peer.get_id(),
            );
            if original_remains_nr != self.fsm.peer.wait_data_peers.len() {
                info!(
                   "notify pd with change peer region";
                   "region_id" => self.fsm.region_id(),
                   "peer_id" => from.get_id(),
                   "region" => ?self.fsm.peer.region(),
                );
                self.fsm.peer.heartbeat_pd(self.ctx);
            }
            return;
        }
        self.register_check_peers_availability_tick();
    }

    fn on_availability_request(&mut self, from: &metapb::Peer) {
        if self.fsm.peer.is_leader() {
            return;
        }
        let mut resp = ExtraMessage::default();
        resp.set_type(ExtraMessageType::MsgAvailabilityResponse);
        resp.wait_data = self.fsm.peer.wait_data;
        let report = resp.mut_availability_context();
        report.set_from_region_id(self.region_id());
        report.set_from_region_epoch(self.region().get_region_epoch().clone());
        report.set_trimmed(true);
        self.fsm
            .peer
            .send_extra_message(resp, &mut self.ctx.trans, from);
        debug!(
            "peer responses availability info to leader";
            "region_id" => self.region().get_id(),
            "peer_id" => self.fsm.peer.peer.get_id(),
            "leader_id" => from.id,
        );
    }

    fn on_voter_replicated_index_request(&mut self, from: &metapb::Peer) {
        if !self.fsm.peer.is_leader() {
            return;
        }
        let mut voter_replicated_idx = self.fsm.peer.get_store().last_index();
        for (peer_id, p) in self.fsm.peer.raft_group.raft.prs().iter() {
            let peer = find_peer_by_id(self.region(), *peer_id).unwrap();
            if voter_replicated_idx > p.matched && !is_learner(peer) {
                voter_replicated_idx = p.matched;
            }
        }
        let first_index = self.fsm.peer.get_store().first_index();
        if voter_replicated_idx > first_index {
            voter_replicated_idx = first_index;
        }
        let mut resp = ExtraMessage::default();
        resp.set_type(ExtraMessageType::MsgVoterReplicatedIndexResponse);
        resp.index = voter_replicated_idx;
        self.fsm
            .peer
            .send_extra_message(resp, &mut self.ctx.trans, from);
        debug!(
            "leader responses voter_replicated_index to witness";
            "region_id" => self.region().get_id(),
            "witness_id" => from.id,
            "leader_id" => self.fsm.peer.peer.get_id(),
            "voter_replicated_index" => voter_replicated_idx,
        );
    }

    fn on_voter_replicated_index_response(&mut self, msg: &ExtraMessage) {
        if self.fsm.peer.is_leader() || !self.fsm.peer.is_witness() {
            return;
        }
        let voter_replicated_index = msg.index;
        if let Ok(voter_replicated_term) = self.fsm.peer.get_store().term(voter_replicated_index) {
            self.ctx.apply_router.schedule_task(
                self.region_id(),
                ApplyTask::CheckCompact {
                    region_id: self.region_id(),
                    voter_replicated_index,
                    voter_replicated_term,
                },
            )
        }
    }

    // In v1, gc_peer_request is handled to be compatible with v2.
    // Note: it needs to be consistent with Peer::on_gc_peer_request in v2.
    fn on_gc_peer_request(&mut self, msg: RaftMessage) {
        let extra_msg = msg.get_extra_msg();

        if !extra_msg.has_check_gc_peer() || extra_msg.get_index() == 0 {
            // Corrupted message.
            return;
        }
        if self.fsm.peer.get_store().applied_index() < extra_msg.get_index() {
            // Merge not finish.
            return;
        }

        forward_destroy_to_source_peer(&msg, |m| {
            let _ = self.ctx.router.send_raft_message(m);
        });
    }

    fn on_extra_message(&mut self, mut msg: RaftMessage) {
        self.ctx
            .coprocessor_host
            .on_extra_message(self.fsm.peer.region(), msg.get_extra_msg());
        match msg.get_extra_msg().get_type() {
            ExtraMessageType::MsgRegionWakeUp | ExtraMessageType::MsgCheckStalePeer => {
                if msg.get_extra_msg().forcely_awaken {
                    // Forcely awaken this region by manually setting the GroupState
                    // into `Chaos` to trigger a new voting in the Raft Group.
                    // Meanwhile, it avoids the peer entering the `PreChaos` state,
                    // which would wait for another long tick to enter the `Chaos` state.
                    self.reset_raft_tick(if !self.fsm.peer.is_leader() {
                        GroupState::Chaos
                    } else {
                        GroupState::Ordered
                    });
                }
                if self.fsm.hibernate_state.group_state() == GroupState::Idle {
                    self.reset_raft_tick(GroupState::Ordered);
                }
                if msg.get_extra_msg().get_type() == ExtraMessageType::MsgRegionWakeUp
                    && self.fsm.peer.is_leader()
                {
                    self.fsm.peer.raft_group.raft.ping();
                }
            }
            ExtraMessageType::MsgWantRollbackMerge => {
                self.fsm.peer.maybe_add_want_rollback_merge_peer(
                    msg.get_from_peer().get_id(),
                    msg.get_extra_msg(),
                );
            }
            ExtraMessageType::MsgCheckStalePeerResponse => {
                self.fsm.peer.on_check_stale_peer_response(
                    msg.get_region_epoch().get_conf_ver(),
                    msg.mut_extra_msg().take_check_peers().into(),
                );
            }
            ExtraMessageType::MsgHibernateRequest => {
                self.on_hibernate_request(msg.get_from_peer());
            }
            ExtraMessageType::MsgHibernateResponse => {
                self.on_hibernate_response(msg.get_from_peer());
            }
            ExtraMessageType::MsgRejectRaftLogCausedByMemoryUsage => {
                unimplemented!()
            }
            ExtraMessageType::MsgAvailabilityRequest => {
                self.on_availability_request(msg.get_from_peer());
            }
            ExtraMessageType::MsgAvailabilityResponse => {
                self.on_availability_response(msg.get_from_peer(), msg.get_extra_msg());
            }
            ExtraMessageType::MsgVoterReplicatedIndexRequest => {
                self.on_voter_replicated_index_request(msg.get_from_peer());
            }
            ExtraMessageType::MsgVoterReplicatedIndexResponse => {
                self.on_voter_replicated_index_response(msg.get_extra_msg());
            }
            ExtraMessageType::MsgGcPeerRequest => {
                // To make learner (e.g. tiflash engine) compatiable with raftstore v2,
                // it needs to response GcPeerResponse.
                if self.ctx.cfg.enable_v2_compatible_learner {
                    self.on_gc_peer_request(msg);
                }
            }
            // It's v2 only message and ignore does no harm.
            ExtraMessageType::MsgGcPeerResponse | ExtraMessageType::MsgFlushMemtable => (),
            ExtraMessageType::MsgRefreshBuckets => self.on_msg_refresh_buckets(msg),
            ExtraMessageType::MsgSnapGenPrecheckRequest => {
                let passed = self.ctx.snap_mgr.recv_snap_precheck(msg.region_id);
                self.fsm.peer.send_snap_gen_precheck_response(
                    self.ctx,
                    &msg.from_peer.unwrap(),
                    passed,
                )
            }
            ExtraMessageType::MsgSnapGenPrecheckResponse => {
                let passed = msg.get_extra_msg().get_snap_gen_precheck_passed();
                fail_point!("snap_gen_precheck_failed", !passed, |_| {});
                info!(
                    "snap gen precheck response: {}", passed;
                    "region_id" => self.region_id(),
                    "peer_id" => self.peer().id,
                    "store_id" => self.store_id(),
                    "receiver_peer_id" => msg.get_from_peer().get_id(),
                    "receiver_store_id" => msg.get_from_peer().get_store_id(),
                    "ignored" => !self.fsm.peer.get_store().has_gen_snap_task(),
                );
                if passed {
                    if let Some(gen_task) = self.fsm.peer.mut_store().take_gen_snap_task() {
                        self.fsm
                            .peer
                            .pending_request_snapshot_count
                            .fetch_add(1, Ordering::SeqCst);
                        self.ctx
                            .apply_router
                            .schedule_task(self.region_id(), ApplyTask::Snapshot(gen_task));
                    }
                }
            }
            ExtraMessageType::MsgPreLoadRegionRequest => {
                // It has been handled in on_extra_message in coprocessor_host
            }
            ExtraMessageType::MsgPreLoadRegionResponse => {
                // Ignore now
            }
        }
    }

    fn reset_raft_tick(&mut self, state: GroupState) {
        debug!(
            "reset raft tick to {:?}", state;
            "region_id"=> self.fsm.region_id(),
            "peer_id" => self.fsm.peer_id(),
        );
        self.fsm.reset_hibernate_state(state);
        self.fsm.missing_ticks = 0;
        self.fsm.peer.should_wake_up = false;
        self.register_raft_base_tick();
        if self.fsm.peer.is_leader() {
            self.register_check_leader_lease_tick();
            self.register_report_region_buckets_tick();
            self.register_check_long_uncommitted_tick();
        }
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
                "msg_type" => %util::MsgType(msg),
            );
            self.ctx
                .raft_metrics
                .message_dropped
                .mismatch_store_id
                .inc();
            return false;
        }

        if !msg.has_region_epoch() {
            error!(
                "missing epoch in raft message, ignore it";
                "region_id" => region_id,
            );
            self.ctx
                .raft_metrics
                .message_dropped
                .mismatch_region_epoch
                .inc();
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
        // - 1 removes 2, 2 may still send MsgAppendResponse to 1.
        //  We should ignore this stale message and let 2 remove itself after
        //  applying the ConfChange log.
        // - 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
        //  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc
        // itself.
        // - 2 is isolated but can communicate with 3. 1 removes 3.
        //  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
        // - 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove
        //   3.
        //  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
        // - 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
        //  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
        //  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
        //  rejoin the raft group again.
        // - 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4
        //   removes 2.
        //  unlike case e, 2 will be stale forever.
        // TODO: for case f, if 2 is stale for a long time, 2 will communicate with pd
        // and pd will tell 2 is stale, so 2 can remove itself.
        let self_epoch = self.fsm.peer.region().get_region_epoch();
        if util::is_epoch_stale(from_epoch, self_epoch)
            && find_peer(self.fsm.peer.region(), from_store_id).is_none()
        {
            self.ctx.handle_stale_msg(msg, self_epoch.clone(), None);
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
                    "msg_type" => %util::MsgType(msg),
                );
                self.ctx.raft_metrics.message_dropped.stale_msg.inc();
                true
            }
            cmp::Ordering::Greater => {
                match self.fsm.peer.maybe_destroy(self.ctx) {
                    Some(job) => {
                        info!(
                            "target peer id is larger, destroying self";
                            "region_id" => self.fsm.region_id(),
                            "peer_id" => self.fsm.peer_id(),
                            "target_peer" => ?target,
                            "job.initialized" => job.initialized,
                        );
                        if self.handle_destroy_peer(job) {
                            // It's not frequent, so use 0 as `heap_size` is ok.
                            let store_msg = StoreMsg::RaftMessage(Box::new(InspectedRaftMessage {
                                heap_size: 0,
                                msg: msg.clone(),
                            }));
                            if let Err(e) = self.ctx.router.send_control(store_msg) {
                                info!(
                                    "failed to send back store message, are we shutting down?";
                                    "region_id" => self.fsm.region_id(),
                                    "peer_id" => self.fsm.peer_id(),
                                    "err" => %e,
                                );
                            }
                        }
                    }
                    None => self.ctx.raft_metrics.message_dropped.applying_snap.inc(),
                }
                true
            }
            cmp::Ordering::Equal => false,
        }
    }

    /// Check if it's necessary to gc the source merge peer.
    ///
    /// If the target merge peer won't be created on this store,
    /// then it's appropriate to destroy it immediately.
    fn need_gc_merge(&mut self, msg: &RaftMessage) -> Result<bool> {
        let merge_target = msg.get_merge_target();
        let target_region_id = merge_target.get_id();
        debug!(
            "receive merge target";
            "region_id" => self.fsm.region_id(),
            "peer_id" => self.fsm.peer_id(),
            "merge_target" => ?merge_target,
        );

        // When receiving message that has a merge target, it indicates that the source
        // peer on this store is stale, the peers on other stores are already merged.
        // The epoch in merge target is the state of target peer at the time when source
        // peer is merged. So here we record the merge target epoch version to let the
        // target peer on this store to decide whether to destroy the source peer.
        let mut meta = self.ctx.store_meta.lock().unwrap();
        meta.targets_map.insert(self.region_id(), target_region_id);
        let v = meta
            .pending_merge_targets
            .entry(target_region_id)
            .or_default();
        let mut no_range_merge_target = merge_target.clone();
        no_range_merge_target.clear_start_key();
        no_range_merge_target.clear_end_key();
        if let Some(pre_merge_target) = v.insert(self.region_id(), no_range_merge_target) {
            // Merge target epoch records the version of target region when source region is
            // merged. So it must be same no matter when receiving merge target.
            if pre_merge_target.get_region_epoch().get_version()
                != merge_target.get_region_epoch().get_version()
            {
                panic!(
                    "conflict merge target epoch version {:?} {:?}",
                    pre_merge_target.get_region_epoch().get_version(),
                    merge_target.get_region_epoch()
                );
            }
        }

        if let Some(r) = meta.regions.get(&target_region_id) {
            // In the case that the source peer's range isn't overlapped with target's
            // anymore:
            //     | region 2 | region 3 | region 1 |
            //                   || merge 3 into 2
            //                   \/
            //     |       region 2      | region 1 |
            //                   || merge 1 into 2
            //                   \/
            //     |            region 2            |
            //                   || split 2 into 4
            //                   \/
            //     |        region 4       |region 2|
            // so the new target peer can't find the source peer.
            // e.g. new region 2 is overlapped with region 1
            //
            // If that, source peer still need to decide whether to destroy itself. When the
            // target peer has already moved on, source peer can destroy itself.
            if util::is_epoch_stale(merge_target.get_region_epoch(), r.get_region_epoch()) {
                return Ok(true);
            }
            return Ok(false);
        }
        drop(meta);

        // All of the target peers must exist before merging which is guaranteed by PD.
        // Now the target peer is not in region map, so if everything is ok, the merge
        // target region should be staler than the local target region
        if self.is_merge_target_region_stale(merge_target)? {
            Ok(true)
        } else {
            if self.ctx.cfg.dev_assert {
                panic!(
                    "something is wrong, maybe PD do not ensure all target peers exist before merging"
                );
            }
            error!(
                "something is wrong, maybe PD do not ensure all target peers exist before merging"
            );
            Ok(false)
        }
    }

    fn handle_gc_peer_msg(&mut self, msg: &RaftMessage) {
        let from_epoch = msg.get_region_epoch();
        if !util::is_epoch_stale(self.fsm.peer.region().get_region_epoch(), from_epoch) {
            return;
        }

        if self.fsm.peer.peer.get_id() != msg.get_to_peer().get_id() {
            info!(
                "receive stale gc message, ignore.";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "to_peer" => ?msg.get_to_peer(),
                "from_peer" => ?msg.get_from_peer(),
            );
            self.ctx.raft_metrics.message_dropped.stale_msg.inc();
            return;
        }
        // TODO: ask pd to guarantee we are stale now.
        info!(
            "receives gc message, trying to remove";
            "region_id" => self.fsm.region_id(),
            "peer_id" => self.fsm.peer_id(),
            "to_peer" => ?msg.get_to_peer(),
            "from_peer" => ?msg.get_from_peer(),
        );

        // Destroy peer in next round in order to apply more committed entries if any.
        // It depends on the implementation that msgs which are handled in this round
        // have already fetched.
        let _ = self
            .ctx
            .router
            .force_send(self.fsm.region_id(), PeerMsg::Destroy(self.fsm.peer_id()));
    }

    // Returns `Vec<(u64, bool)>` indicated (source_region_id, merge_to_this_peer)
    // if the `msg` doesn't contain a snapshot or this snapshot doesn't conflict
    // with any other snapshots or regions. Otherwise a `SnapKey` is returned.
    fn check_snapshot(
        &mut self,
        msg: &RaftMessage,
    ) -> Result<Either<Option<SnapKey>, Vec<(u64, bool)>>> {
        if !msg.get_message().has_snapshot() {
            return Ok(Either::Right(vec![]));
        }

        let region_id = msg.get_region_id();
        let snap = msg.get_message().get_snapshot();
        let mut snap_data = RaftSnapshotData::default();
        snap_data.merge_from_bytes(snap.get_data())?;

        let key = if !snap_data.get_meta().get_for_witness() {
            // Check if snapshot file exists.
            // No need to get snapshot for witness, as witness's empty snapshot bypass
            // snapshot manager.
            let key = SnapKey::from_region_snap(region_id, snap);
            self.ctx.snap_mgr.meta_file_exist(&key)?;
            Some(key)
        } else {
            None
        };

        // If the index of snapshot is not newer than peer's apply index, it
        // is possibly because there is witness -> non-witness switch, and the peer
        // requests snapshot from leader but leader doesn't applies the switch yet.
        // In that case, the snapshot is a witness snapshot whereas non-witness snapshot
        // is expected.
        if snap.get_metadata().get_index() < self.fsm.peer.get_store().applied_index()
            && snap_data.get_meta().get_for_witness() != self.fsm.peer.is_witness()
        {
            error!(
                "mismatch witness snapshot";
                "region_id" => region_id,
                "peer_id" => self.fsm.peer_id(),
                "for_witness" => snap_data.get_meta().get_for_witness(),
                "is_witness" => self.fsm.peer.is_witness(),
                "index" => snap.get_metadata().get_index(),
                "applied_index" => self.fsm.peer.get_store().applied_index(),
            );
            self.ctx
                .raft_metrics
                .message_dropped
                .mismatch_witness_snapshot
                .inc();
            return Ok(Either::Left(key));
        }

        let snap_region = snap_data.take_region();
        let peer_id = msg.get_to_peer().get_id();
        let snap_enc_start_key = enc_start_key(&snap_region);
        let snap_enc_end_key = enc_end_key(&snap_region);

        let before_check_snapshot_1_2_fp = || -> bool {
            fail_point!(
                "before_check_snapshot_1_2",
                self.fsm.region_id() == 1 && self.store_id() == 2,
                |_| true
            );
            false
        };
        let before_check_snapshot_1000_2_fp = || -> bool {
            fail_point!(
                "before_check_snapshot_1000_2",
                self.fsm.region_id() == 1000 && self.store_id() == 2,
                |_| true
            );
            false
        };
        if before_check_snapshot_1_2_fp() || before_check_snapshot_1000_2_fp() {
            return Ok(Either::Left(key));
        }

        if snap_region
            .get_peers()
            .iter()
            .all(|p| p.get_id() != peer_id)
        {
            info!(
                "snapshot doesn't contain to peer, skip";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "snap" => ?snap_region,
                "to_peer" => ?msg.get_to_peer(),
                "msg_type" => %util::MsgType(msg),
            );
            self.ctx.raft_metrics.message_dropped.region_no_peer.inc();
            return Ok(Either::Left(key));
        }

        let mut meta = self.ctx.store_meta.lock().unwrap();
        // Check if the region matches the metadata. A mismatch means another
        // peer has replaced the current peer, which can happen during a split: a
        // peer is first created via raft message, then replaced by another peer
        // (of the same region) when the split is applied.
        let region_mismatch = match meta.regions.get(&self.region_id()) {
            Some(region) => *region != *self.region(),
            None => {
                // If the region doesn't exist, treat it as a mismatch. This can
                // happen in rare situations (e.g. #17469).
                warn!(
                    "region not found in meta";
                    "region_id" => self.fsm.region_id(),
                    "peer_id" => self.fsm.peer_id(),
                );
                true
            }
        };
        if region_mismatch {
            if !self.fsm.peer.is_initialized() {
                info!(
                    "stale delegate detected, skip";
                    "region_id" => self.fsm.region_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "msg_type" => %util::MsgType(msg),
                );
                self.ctx.raft_metrics.message_dropped.stale_msg.inc();
                return Ok(Either::Left(key));
            } else {
                panic!(
                    "{} meta corrupted: {:?} != {:?}",
                    self.fsm.peer.tag,
                    meta.regions.get(&self.region_id()),
                    self.region()
                );
            }
        }

        if meta.atomic_snap_regions.contains_key(&region_id) {
            info!(
                "atomic snapshot is applying, skip";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
            );
            return Ok(Either::Left(key));
        }

        for region in &meta.pending_snapshot_regions {
            if enc_start_key(region) < snap_enc_end_key &&
               enc_end_key(region) > snap_enc_start_key &&
               // Same region can overlap, we will apply the latest version of snapshot.
               region.get_id() != snap_region.get_id()
            {
                info!(
                    "pending region overlapped";
                    "region_id" => self.fsm.region_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "region" => ?region,
                    "snap" => ?snap_region,
                    "msg_type" => %util::MsgType(msg),
                );
                self.ctx.raft_metrics.message_dropped.region_overlap.inc();
                return Ok(Either::Left(key));
            }
        }

        let mut is_overlapped = false;
        let mut regions_to_destroy = vec![];
        // In some extreme cases, it may cause source peer destroyed improperly so that
        // a later CommitMerge may panic because source is already destroyed, so just
        // drop the message:
        // - A new snapshot is received whereas a snapshot is still in applying, and the
        //   snapshot under applying is generated before merge and the new snapshot is
        //   generated after merge. After the applying snapshot is finished, the log may
        //   able to catch up and so a CommitMerge will be applied.
        // - There is a CommitMerge pending in apply thread.
        let ready = !self.fsm.peer.is_handling_snapshot()
            && !self.fsm.peer.has_pending_snapshot()
            // It must be ensured that all logs have been applied.
            // Suppose apply fsm is applying a `CommitMerge` log and this snapshot is generated after
            // merge, its corresponding source peer can not be destroy by this snapshot.
            && self.fsm.peer.ready_to_handle_pending_snap();
        for exist_region in meta
            .region_ranges
            .range((Excluded(snap_enc_start_key), Unbounded::<Vec<u8>>))
            .map(|(_, &region_id)| &meta.regions[&region_id])
            .take_while(|r| enc_start_key(r) < snap_enc_end_key)
            .filter(|r| r.get_id() != region_id)
        {
            info!(
                "region overlapped";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "exist" => ?exist_region,
                "snap" => ?snap_region,
            );
            let (can_destroy, merge_to_this_peer) = maybe_destroy_source(
                &meta,
                self.fsm.region_id(),
                self.fsm.peer_id(),
                exist_region.get_id(),
                snap_region.get_region_epoch().to_owned(),
            );
            if ready && can_destroy {
                // The snapshot that we decide to whether destroy peer based on must can be
                // applied. So here not to destroy peer immediately, or the snapshot maybe
                // dropped in later check but the peer is already destroyed.
                regions_to_destroy.push((exist_region.get_id(), merge_to_this_peer));
                continue;
            }
            is_overlapped = true;
            if !can_destroy
                && snap_region.get_region_epoch().get_version()
                    > exist_region.get_region_epoch().get_version()
            {
                // If snapshot's epoch version is greater than exist region's, the exist region
                // may has been merged/splitted already.
                let _ = self.ctx.router.force_send(
                    exist_region.get_id(),
                    PeerMsg::CasualMessage(Box::new(CasualMessage::RegionOverlapped)),
                );
            }
        }
        if is_overlapped {
            self.ctx.raft_metrics.message_dropped.region_overlap.inc();
            return Ok(Either::Left(key));
        }

        // WARNING: The checking code must be above this line.
        // Now all checking passed.

        if self.fsm.peer.local_first_replicate && !self.fsm.peer.is_initialized() {
            // If the peer is not initialized and passes the snapshot range check,
            // `is_splitting` flag must be false.
            // - If `is_splitting` is set to true, then the uninitialized peer is created
            //   before split is applied and the peer id is the same as split one. So there
            //   should be no initialized peer before.
            // - If the peer is also created by splitting, then the snapshot range is not
            //   overlapped with parent peer. It means leader has applied merge and split at
            //   least one time. However, the prerequisite of merge includes the
            //   initialization of all target peers and source peers, which is conflict with
            //   1.
            let pending_create_peers = self.ctx.pending_create_peers.lock().unwrap();
            let status = pending_create_peers.get(&region_id).cloned();
            if status != Some((self.fsm.peer_id(), false)) {
                drop(pending_create_peers);
                panic!("{} status {:?} is not expected", self.fsm.peer.tag, status);
            }
        }
        meta.pending_snapshot_regions.push(snap_region);

        Ok(Either::Right(regions_to_destroy))
    }

    fn destroy_regions_for_snapshot(&mut self, regions_to_destroy: Vec<(u64, bool)>) {
        if regions_to_destroy.is_empty() {
            return;
        }
        let mut meta = self.ctx.store_meta.lock().unwrap();
        assert!(!meta.atomic_snap_regions.contains_key(&self.fsm.region_id()));
        for (source_region_id, merge_to_this_peer) in regions_to_destroy {
            if !meta.regions.contains_key(&source_region_id) {
                if merge_to_this_peer {
                    drop(meta);
                    panic!(
                        "{}'s source region {} has been destroyed",
                        self.fsm.peer.tag, source_region_id
                    );
                }
                continue;
            }
            info!(
                "source region destroy due to target region's snapshot";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "source_region_id" => source_region_id,
                "need_atomic" => merge_to_this_peer,
            );
            meta.atomic_snap_regions
                .entry(self.fsm.region_id())
                .or_default()
                .insert(source_region_id, false);
            meta.destroyed_region_for_snap
                .insert(source_region_id, merge_to_this_peer);

            let result = if merge_to_this_peer {
                MergeResultKind::FromTargetSnapshotStep1
            } else {
                MergeResultKind::Stale
            };
            // Use `unwrap` is ok because the StoreMeta lock is held and these source peers
            // still exist in regions and region_ranges map.
            // It depends on the implementation of `destroy_peer`.
            self.ctx
                .router
                .force_send(
                    source_region_id,
                    PeerMsg::SignificantMsg(Box::new(SignificantMsg::MergeResult {
                        target_region_id: self.fsm.region_id(),
                        target: self.fsm.peer.peer.clone(),
                        result,
                    })),
                )
                .unwrap();
        }
    }

    fn on_transfer_leader_msg(&mut self, msg: &eraftpb::Message, peer_disk_usage: DiskUsage) {
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
                .ready_to_transfer_leader(self.ctx, msg.get_index(), &from)
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
                    self.propose_pending_batch_raft_command();
                    if self.propose_locks_before_transfer_leader(msg) {
                        // If some pessimistic locks are just proposed, we propose another
                        // TransferLeader command instead of transferring leader immediately.
                        info!("propose transfer leader command";
                            "region_id" => self.fsm.region_id(),
                            "peer_id" => self.fsm.peer_id(),
                            "to" => ?from,
                        );
                        let mut cmd = new_admin_request(
                            self.fsm.peer.region().get_id(),
                            self.fsm.peer.peer.clone(),
                        );
                        cmd.mut_header()
                            .set_region_epoch(self.region().get_region_epoch().clone());
                        // Set this flag to propose this command like a normal proposal.
                        cmd.mut_header()
                            .set_flags(WriteBatchFlags::TRANSFER_LEADER_PROPOSAL.bits());
                        cmd.mut_admin_request()
                            .set_cmd_type(AdminCmdType::TransferLeader);
                        cmd.mut_admin_request().mut_transfer_leader().set_peer(from);
                        self.propose_raft_command(
                            cmd,
                            Callback::None,
                            DiskFullOpt::AllowedOnAlmostFull,
                        );
                    } else {
                        self.fsm.peer.transfer_leader(&from);
                    }
                }
            }
        } else if !self
            .fsm
            .peer
            .maybe_reject_transfer_leader_msg(self.ctx, msg, peer_disk_usage)
            && self.fsm.peer.pre_ack_transfer_leader_msg(self.ctx, msg)
        {
            self.fsm.peer.ack_transfer_leader_msg(false);
        }
    }

    // Returns whether we should propose another TransferLeader command. This is
    // for:
    // - Considering the amount of pessimistic locks can be big, it can reduce
    //   unavailable time caused by waiting for the transferee catching up logs.
    // - Make transferring leader strictly after write commands that executes before
    //   proposing the locks, preventing unexpected lock loss.
    fn propose_locks_before_transfer_leader(&mut self, msg: &eraftpb::Message) -> bool {
        // 1. Disable in-memory pessimistic locks.

        // Clone to make borrow checker happy when registering ticks.
        let txn_ext = self.fsm.peer.txn_ext.clone();
        let mut pessimistic_locks = txn_ext.pessimistic_locks.write();

        // If the message context == TRANSFER_LEADER_COMMAND_REPLY_CTX, the message
        // is a reply to a transfer leader command before. If the locks status remain
        // in the TransferringLeader status, we can safely initiate transferring leader
        // now.
        // If it's not in TransferringLeader status now, it is probably because several
        // ticks have passed after proposing the locks in the last time and we
        // reactivate the memory locks. Then, we should propose the locks again.
        if msg.get_context() == TRANSFER_LEADER_COMMAND_REPLY_CTX
            && pessimistic_locks.status == LocksStatus::TransferringLeader
        {
            return false;
        }

        // If it is not writable, it's probably because it's a retried TransferLeader
        // and the locks have been proposed. But we still need to return true to
        // propose another TransferLeader command. Otherwise, some write requests that
        // have marked some locks as deleted will fail because raft rejects more
        // proposals.
        // It is OK to return true here if it's in other states like MergingRegion or
        // NotLeader. In those cases, the locks will fail to propose and nothing will
        // happen.
        if !pessimistic_locks.is_writable() {
            return true;
        }
        pessimistic_locks.status = LocksStatus::TransferringLeader;
        self.fsm.reactivate_memory_lock_ticks = 0;
        self.register_reactivate_memory_lock_tick();

        // 2. Propose pessimistic locks
        if pessimistic_locks.is_empty() {
            return false;
        }
        // FIXME: Raft command has size limit. Either limit the total size of
        // pessimistic locks in a region, or split commands here.
        let mut cmd = RaftCmdRequest::default();
        {
            // Downgrade to a read guard, do not block readers in the scheduler as far as
            // possible.
            let pessimistic_locks = RwLockWriteGuard::downgrade(pessimistic_locks);
            fail_point!("invalidate_locks_before_transfer_leader");
            for (key, (lock, deleted)) in &*pessimistic_locks {
                if *deleted {
                    continue;
                }
                let mut put = PutRequest::default();
                put.set_cf(CF_LOCK.to_string());
                put.set_key(key.as_encoded().to_owned());
                put.set_value(lock.to_lock().to_bytes());
                let mut req = Request::default();
                req.set_cmd_type(CmdType::Put);
                req.set_put(put);
                cmd.mut_requests().push(req);
            }
        }
        if cmd.get_requests().is_empty() {
            // If the map is not empty but all locks are deleted, it is possible that a
            // write command has just marked locks deleted but not proposed yet.
            // It might cause that command to fail if we skip proposing the
            // extra TransferLeader command here.
            return true;
        }
        cmd.mut_header().set_region_id(self.fsm.region_id());
        cmd.mut_header()
            .set_region_epoch(self.region().get_region_epoch().clone());
        cmd.mut_header().set_peer(self.fsm.peer.peer.clone());
        info!("propose {} locks before transferring leader", cmd.get_requests().len(); "region_id" => self.fsm.region_id());
        self.propose_raft_command(cmd, Callback::None, DiskFullOpt::AllowedOnAlmostFull);
        true
    }

    fn handle_destroy_peer(&mut self, job: DestroyPeerJob) -> bool {
        // The initialized flag implicitly means whether apply fsm exists or not.
        if job.initialized {
            // Destroy the apply fsm first, wait for the reply msg from apply fsm
            self.ctx
                .apply_router
                .schedule_task(job.region_id, ApplyTask::destroy(job.region_id, false));
            false
        } else {
            // Destroy the peer fsm directly
            self.destroy_peer(false)
        }
    }

    fn maybe_destroy(&mut self) {
        match self.fsm.peer.maybe_destroy(self.ctx) {
            None => self.ctx.raft_metrics.message_dropped.applying_snap.inc(),
            Some(job) => {
                self.handle_destroy_peer(job);
            }
        }
    }

    /// Check if destroy can be executed immediately. If it can't, the reason is
    /// returned.
    fn maybe_delay_destroy(&mut self) -> Option<DelayReason> {
        if self.fsm.peer.has_unpersisted_ready() {
            assert!(self.ctx.sync_write_worker.is_none());
            // The destroy must be delayed if there are some unpersisted readies.
            // Otherwise there is a race of writing kv db and raft db between here
            // and write worker.
            return Some(DelayReason::UnPersistedReady);
        }

        let is_initialized = self.fsm.peer.is_initialized();
        if !is_initialized {
            // If the peer is uninitialized, then it can't receive any logs from leader. So
            // no need to gc. If there was a peer with same region id on the store, and it
            // had logs written, then it must be initialized, hence its log should be gc
            // either before it's destroyed or during node restarts.
            self.fsm.logs_gc_flushed = true;
        }
        if !self.fsm.logs_gc_flushed {
            let start_index = self.fsm.peer.last_compacted_idx;
            let mut end_index = start_index;
            if end_index == 0 {
                // Technically, all logs between first index and last index should be accessible
                // before being destroyed.
                end_index = self.fsm.peer.get_store().first_index();
                self.fsm.peer.last_compacted_idx = end_index;
            }
            let region_id = self.region_id();
            let peer_id = self.fsm.peer.peer_id();
            let mb = match self.ctx.router.mailbox(region_id) {
                Some(mb) => mb,
                None => {
                    if tikv_util::thread_group::is_shutdown(!cfg!(test)) {
                        // It's shutting down, nothing we can do.
                        return Some(DelayReason::Shutdown);
                    }
                    panic!("{} failed to get mailbox", self.fsm.peer.tag);
                }
            };
            let task = RaftlogGcTask::gc(
                self.fsm.peer.get_store().get_region_id(),
                start_index,
                end_index,
            )
            .flush()
            .when_done(move || {
                if let Err(e) = mb.force_send(PeerMsg::SignificantMsg(Box::new(
                    SignificantMsg::RaftLogGcFlushed,
                ))) {
                    if tikv_util::thread_group::is_shutdown(!cfg!(test)) {
                        return;
                    }
                    panic!(
                        "[region {}] {} failed to respond flush message {:?}",
                        region_id, peer_id, e
                    );
                }
            });
            if let Err(e) = self.ctx.raftlog_gc_scheduler.schedule(task) {
                if tikv_util::thread_group::is_shutdown(!cfg!(test)) {
                    // It's shutting down, nothing we can do.
                    return Some(DelayReason::Shutdown);
                }
                panic!(
                    "{} failed to schedule raft log task {:?}",
                    self.fsm.peer.tag, e
                );
            }
            // We need to delete all logs entries to avoid introducing race between
            // new peers and old peers. Flushing gc logs allow last_compact_index be
            // used directly without seeking.
            return Some(DelayReason::UnFlushLogGc);
        }
        None
    }

    fn on_raft_log_gc_flushed(&mut self) {
        self.fsm.logs_gc_flushed = true;
        let delay = match self.fsm.delayed_destroy {
            Some(delay) => delay,
            None => panic!("{} a delayed destroy should not recover", self.fsm.peer.tag),
        };
        self.destroy_peer(delay.merged_by_target);
    }

    // [PerformanceCriticalPath] TODO: spin off the I/O code (self.fsm.peer.destroy)
    fn destroy_peer(&mut self, merged_by_target: bool) -> bool {
        fail_point!("destroy_peer");
        // Mark itself as pending_remove
        self.fsm.peer.pending_remove = true;

        // try to decrease the RAFT_ENABLE_UNPERSISTED_APPLY_GAUGE count.
        self.fsm.peer.disable_apply_unpersisted_log(0);

        fail_point!("destroy_peer_after_pending_move", |_| { true });

        if let Some(reason) = self.maybe_delay_destroy() {
            if self
                .fsm
                .delayed_destroy
                .map_or(false, |delay| delay.reason == reason)
            {
                panic!(
                    "{} destroy peer twice with same delay reason, original {:?}, now {}",
                    self.fsm.peer.tag, self.fsm.delayed_destroy, merged_by_target
                );
            }
            self.fsm.delayed_destroy = Some(DelayDestroy {
                merged_by_target,
                reason,
            });
            // TODO: The destroy process can also be asynchronous as snapshot process,
            // if so, all write db operations are removed in store thread.
            info!(
                "delays destroy";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "merged_by_target" => merged_by_target,
                "reason" => ?reason,
            );
            return false;
        }

        let region_id = self.region_id();
        let is_peer_initialized = self.fsm.peer.is_initialized();
        // We can't destroy a peer which is handling snapshot.
        assert!(!self.fsm.peer.is_handling_snapshot());

        // No need to wait for the apply anymore.
        if self.fsm.peer.unsafe_recovery_state.is_some() {
            self.fsm
                .peer
                .unsafe_recovery_maybe_finish_wait_apply(/* force= */ true);
        }

        if self.fsm.peer.snapshot_recovery_state.is_some() {
            self.fsm
                .peer
                .snapshot_recovery_maybe_finish_wait_apply(/* force= */ true);
        }

        (|| {
            fail_point!(
                "before_destroy_peer_on_peer_1003",
                self.fsm.peer.peer_id() == 1003,
                |_| {}
            );
        })();
        let mut meta = self.ctx.store_meta.lock().unwrap();
        meta.damaged_regions.remove(&self.fsm.region_id());
        meta.damaged_regions.shrink_to_fit();
        let is_latest_initialized = {
            if let Some(latest_region_info) = meta.regions.get(&region_id) {
                util::is_region_initialized(latest_region_info)
            } else {
                false
            }
        };

        if !is_peer_initialized && is_latest_initialized {
            info!("skip destroy uninitialized peer as it's already initialized in meta";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "merged_by_target" => merged_by_target,
            );
            return false;
        }

        info!(
            "starts destroy";
            "region_id" => self.fsm.region_id(),
            "peer_id" => self.fsm.peer_id(),
            "merged_by_target" => merged_by_target,
            "is_peer_initialized" => is_peer_initialized,
            "is_latest_initialized" => is_latest_initialized,
        );

        // Ensure this peer is removed in the pending apply list.
        meta.busy_apply_peers.remove(&self.fsm.peer_id());
        if let Some(count) = meta.completed_apply_peers_count.as_mut() {
            *count += 1;
        }

        if meta.atomic_snap_regions.contains_key(&self.region_id()) {
            drop(meta);
            panic!(
                "{} is applying atomic snapshot during destroying",
                self.fsm.peer.tag
            );
        }

        // It's possible that this region gets a snapshot then gets a stale peer msg.
        // So the data in `pending_snapshot_regions` should be removed here.
        meta.pending_snapshot_regions
            .retain(|r| self.fsm.region_id() != r.get_id());

        // Remove `read_progress` and reset the `safe_ts` to zero to reject
        // incoming stale read request
        meta.region_read_progress.remove(&region_id);
        self.fsm.peer.read_progress.pause();

        // Destroy read delegates.
        meta.readers.remove(&region_id);

        // Trigger region change observer
        self.ctx.coprocessor_host.on_region_changed(
            self.fsm.peer.region(),
            RegionChangeEvent::Destroy,
            self.fsm.peer.get_role(),
        );
        let task = PdTask::DestroyPeer { region_id };
        if let Err(e) = self.ctx.pd_scheduler.schedule(task) {
            error!(
                "failed to notify pd";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "err" => %e,
            );
        }
        if let Err(e) = self.fsm.peer.destroy(
            &self.ctx.engines,
            &mut self.ctx.raft_perf_context,
            merged_by_target,
            &self.ctx.pending_create_peers,
        ) {
            // If not panic here, the peer will be recreated in the next restart,
            // then it will be gc again. But if some overlap region is created
            // before restarting, the gc action will delete the overlap region's
            // data too.
            panic!("{} destroy err {:?}", self.fsm.peer.tag, e);
        }

        // Some places use `force_send().unwrap()` if the StoreMeta lock is held.
        // So in here, it's necessary to held the StoreMeta lock when closing the
        // router.
        self.ctx.router.close(region_id);
        self.fsm.stop();

        if is_peer_initialized
            && !merged_by_target
            && meta
                .region_ranges
                .remove(&enc_end_key(self.fsm.peer.region()))
                .is_none()
        {
            panic!("{} meta corruption detected", self.fsm.peer.tag);
        }

        if meta.regions.remove(&region_id).is_none() && !merged_by_target {
            panic!("{} meta corruption detected", self.fsm.peer.tag)
        }

        // Clear merge related structures.
        if let Some(&need_atomic) = meta.destroyed_region_for_snap.get(&region_id) {
            if need_atomic {
                panic!(
                    "{} should destroy with target region atomically",
                    self.fsm.peer.tag
                );
            } else {
                // Remove itself from atomic_snap_regions as it has cleaned both
                // data and metadata.
                let target_region_id = *meta.targets_map.get(&region_id).unwrap();
                meta.atomic_snap_regions
                    .get_mut(&target_region_id)
                    .unwrap()
                    .remove(&region_id);
                meta.destroyed_region_for_snap.remove(&region_id);
                info!("peer has destroyed, clean up for incoming overlapped snapshot";
                    "region_id" => region_id,
                    "peer_id" => self.fsm.peer_id(),
                    "target_region_id" => target_region_id,
                );
            }
        }

        meta.pending_merge_targets.remove(&region_id);
        if let Some(target) = meta.targets_map.remove(&region_id) {
            if meta.pending_merge_targets.contains_key(&target) {
                meta.pending_merge_targets
                    .get_mut(&target)
                    .unwrap()
                    .remove(&region_id);
                // When the target doesn't exist(add peer but the store is isolated), source
                // peer decide to destroy by itself. Without target, the
                // `pending_merge_targets` for target won't be removed, so here source peer help
                // target to clear.
                if meta.regions.get(&target).is_none()
                    && meta.pending_merge_targets.get(&target).unwrap().is_empty()
                {
                    meta.pending_merge_targets.remove(&target);
                }
            }
        }

        fail_point!("raft_store_finish_destroy_peer");

        true
    }

    // Update some region infos
    fn update_region(&mut self, mut region: metapb::Region) {
        {
            let mut meta = self.ctx.store_meta.lock().unwrap();
            meta.set_region(
                &self.ctx.coprocessor_host,
                region.clone(),
                &mut self.fsm.peer,
                RegionChangeReason::ChangePeer,
            );
        }
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

        self.fsm.peer.mut_store().cancel_generating_snap(None);

        if cp.index >= self.fsm.peer.raft_group.raft.raft_log.first_index() {
            match self.fsm.peer.raft_group.apply_conf_change(&cp.conf_change) {
                Ok(_) => {}
                // PD could dispatch redundant conf changes.
                Err(raft::Error::NotExists { .. }) | Err(raft::Error::Exists { .. }) => {}
                _ => unreachable!(),
            }
        } else {
            // Please take a look at test case
            // test_redundant_conf_change_by_snapshot.
        }

        self.update_region(cp.region);

        fail_point!("change_peer_after_update_region");
        fail_point!(
            "change_peer_after_update_region_store_3",
            self.store_id() == 3,
            |_| panic!("should not use return")
        );

        let now = Instant::now();
        let (mut remove_self, mut need_ping) = (false, false);
        for mut change in cp.changes {
            let (change_type, peer) = (change.get_change_type(), change.take_peer());
            let (store_id, peer_id) = (peer.get_store_id(), peer.get_id());
            match change_type {
                ConfChangeType::AddNode | ConfChangeType::AddLearnerNode => {
                    let group_id = self
                        .ctx
                        .global_replication_state
                        .lock()
                        .unwrap()
                        .group
                        .group_id(self.fsm.peer.replication_mode_version, store_id);
                    if group_id.unwrap_or(0) != 0 {
                        info!("updating group"; "peer_id" => peer_id, "group_id" => group_id.unwrap());
                        self.fsm
                            .peer
                            .raft_group
                            .raft
                            .assign_commit_groups(&[(peer_id, group_id.unwrap())]);
                    }
                    // Add this peer to peer_heartbeats.
                    self.fsm.peer.peer_heartbeats.insert(peer_id, now);
                    if self.fsm.peer.is_leader() {
                        need_ping = true;
                        self.fsm.peer.peers_start_pending_time.push((peer_id, now));
                        // As `raft_max_inflight_msgs` may have been updated via online config
                        self.fsm
                            .peer
                            .raft_group
                            .raft
                            .adjust_max_inflight_msgs(peer_id, self.ctx.cfg.raft_max_inflight_msgs);
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
                        self.fsm.peer.wait_data_peers.retain(|id| *id != peer_id);
                    }
                    self.fsm.peer.remove_peer_from_cache(peer_id);
                    // We only care remove itself now.
                    if self.store_id() == store_id {
                        if self.fsm.peer.peer_id() == peer_id {
                            remove_self = true;
                        } else {
                            panic!(
                                "{} trying to remove unknown peer {:?}",
                                self.fsm.peer.tag, peer
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
            self.fsm.peer.heartbeat_pd(self.ctx);

            if !self.fsm.peer.disk_full_peers.is_empty() {
                self.fsm.peer.refill_disk_full_peers(self.ctx);
                debug!(
                    "conf change refills disk full peers to {:?}",
                    self.fsm.peer.disk_full_peers;
                    "region_id" => self.fsm.region_id(),
                );
            }

            // Remove or demote leader will cause this raft group unavailable
            // until new leader elected, but we can't revert this operation
            // because its result is already persisted in apply worker
            // TODO: should we transfer leader here?
            let demote_self =
                is_learner(&self.fsm.peer.peer) && !self.fsm.peer.is_in_force_leader();
            if remove_self || demote_self {
                warn!(
                    "Removing or demoting leader";
                    "region_id" => self.fsm.region_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "remove" => remove_self,
                    "demote" => demote_self,
                );
                // If demote_self is true, there is no doubt to become follower.
                // If remove_self is true, we also choose to become follower for the
                // following reasons.
                // There are some functions in raft-rs using `unwrap` to get itself
                // progress which will panic when calling them.
                // Before introduing async io, this peer will destroy immediately so
                // there is no chance to call these functions.
                // But maybe it's not true due to delay destroy.
                // Most of these functions are only called when the peer is a leader.
                // (it's pretty reasonable because progress is used to track others' status)
                // The only exception is `Raft::restore` at the time of writing, which is ok
                // because the raft msgs(including snapshot) don't be handled when
                // `pending_remove` is true(it will be set in `destroy_peer`).
                // TODO: totally avoid calling these raft-rs functions when `pending_remove` is
                // true.
                self.fsm
                    .peer
                    .raft_group
                    .raft
                    .become_follower(self.fsm.peer.term(), raft::INVALID_ID);
                // Don't ping to speed up leader election
                need_ping = false;
            }
        } else if !self.fsm.peer.has_valid_leader() {
            self.fsm.reset_hibernate_state(GroupState::Chaos);
            self.register_raft_base_tick();
        }
        if need_ping {
            // Speed up snapshot instead of waiting another heartbeat.
            self.fsm.peer.ping();
            self.fsm.has_ready = true;
        }
        if remove_self {
            self.destroy_peer(false);
        }
    }

    fn on_ready_compact_log(&mut self, first_index: u64, state: RaftTruncatedState) {
        // Since this peer may be warming up the entry cache, log compaction should be
        // temporarily skipped. Otherwise, the warmup task may fail.
        if let Some(state) = self.fsm.peer.mut_store().entry_cache_warmup_state_mut() {
            if !state.check_stale(MAX_WARMED_UP_CACHE_KEEP_TIME) {
                return;
            }
        }

        let total_cnt = self.fsm.peer.last_applying_idx - first_index;
        // the size of current CompactLog command can be ignored.
        let remain_cnt = self.fsm.peer.last_applying_idx - state.get_index() - 1;
        self.fsm.peer.raft_log_size_hint =
            self.fsm.peer.raft_log_size_hint * remain_cnt / total_cnt;
        let compact_to = state.get_index() + 1;
        self.fsm.peer.schedule_raftlog_gc(self.ctx, compact_to);
        self.fsm.peer.last_compacted_idx = compact_to;
        self.fsm.peer.mut_store().on_compact_raftlog(compact_to);
        if self.fsm.peer.is_witness() {
            self.fsm.peer.last_compacted_time = Instant::now();
        }
    }

    fn on_ready_split_region(
        &mut self,
        derived: metapb::Region,
        regions: Vec<metapb::Region>,
        new_split_regions: HashMap<u64, apply::NewSplitPeer>,
        share_source_region_size: bool,
    ) {
        fail_point!("on_split", self.ctx.store_id() == 3, |_| {});

        let region_id = derived.get_id();

        // Group in-memory pessimistic locks in the original region into new regions.
        // The locks of new regions will be put into the corresponding new regions
        // later. And the locks belonging to the old region will stay in the original
        // map.
        let region_locks = {
            let mut pessimistic_locks = self.fsm.peer.txn_ext.pessimistic_locks.write();
            info!("moving {} locks to new regions", pessimistic_locks.len(); "region_id" => region_id);
            // Update the version so the concurrent reader will fail due to EpochNotMatch
            // instead of PessimisticLockNotFound.
            pessimistic_locks.version = derived.get_region_epoch().get_version();
            pessimistic_locks.group_by_regions(&regions, &derived)
        };
        fail_point!("on_split_invalidate_locks");

        // Roughly estimate the size and keys for new regions.
        let new_region_count = regions.len() as u64;
        let mut share_size = None;
        let mut share_keys = None;
        // if share_source_region_size is true, it means the new region contains any
        // data from the origin region
        if share_source_region_size {
            share_size = self
                .fsm
                .peer
                .approximate_size()
                .map(|v| v / new_region_count);
            share_keys = self
                .fsm
                .peer
                .approximate_keys()
                .map(|v| v / new_region_count);
        }

        let mut meta = self.ctx.store_meta.lock().unwrap();
        meta.set_region(
            &self.ctx.coprocessor_host,
            derived,
            &mut self.fsm.peer,
            RegionChangeReason::Split,
        );
        self.fsm.peer.post_split();

        let (is_leader, is_follower) = (self.fsm.peer.is_leader(), self.fsm.peer.is_follower());
        if is_leader {
            if share_source_region_size {
                self.fsm.peer.set_approximate_size(share_size);
                self.fsm.peer.set_approximate_keys(share_keys);
            }
            self.fsm.peer.heartbeat_pd(self.ctx);
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
            if let Err(e) = self.ctx.pd_scheduler.schedule(task) {
                error!(
                    "failed to notify pd";
                    "region_id" => self.fsm.region_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "err" => %e,
                );
            }
        } else {
            // It means that there does not exist pending uncompleted split (previous splits
            // are already finished), so we can clear the previous uncompaigned
            // list.
            self.fsm.peer.uncampaigned_new_regions.0.clear();
        }

        let last_key = enc_end_key(regions.last().unwrap());
        if meta.region_ranges.remove(&last_key).is_none() {
            panic!("{} original region should exist", self.fsm.peer.tag);
        }
        for (new_region, locks) in regions.into_iter().zip(region_locks) {
            let new_region_id = new_region.get_id();

            if new_region_id == region_id {
                let not_exist = meta
                    .region_ranges
                    .insert(enc_end_key(&new_region), new_region_id)
                    .is_none();
                assert!(not_exist, "[region {}] should not exist", new_region_id);
                continue;
            }

            // Check if this new region should be splitted
            let new_split_peer = new_split_regions.get(&new_region.get_id()).unwrap();
            if new_split_peer.result.is_some() {
                if let Err(e) = self
                    .fsm
                    .peer
                    .mut_store()
                    .clear_extra_split_data(enc_start_key(&new_region), enc_end_key(&new_region))
                {
                    error!(?e;
                        "failed to cleanup extra split data, may leave some dirty data";
                        "region_id" => new_region.get_id(),
                    );
                }
                continue;
            }

            // Now all checking passed.
            {
                let mut pending_create_peers = self.ctx.pending_create_peers.lock().unwrap();
                assert_eq!(
                    pending_create_peers.remove(&new_region_id),
                    Some((new_split_peer.peer_id, true))
                );
            }

            // Insert new regions and validation
            let mut is_uninitialized_peer_exist = false;
            let self_store_id = self.ctx.store.get_id();
            if let Some(r) = meta.regions.get(&new_region_id) {
                // Suppose a new node is added by conf change and the snapshot comes slowly.
                // Then, the region splits and the first vote message comes to the new node
                // before the old snapshot, which will create an uninitialized peer on the
                // store. After that, the old snapshot comes, followed with the last split
                // proposal. After it's applied, the uninitialized peer will be met.
                // We can remove this uninitialized peer directly.
                if util::is_region_initialized(r) {
                    panic!(
                        "[region {}] duplicated region {:?} for split region {:?}",
                        new_region_id, r, new_region
                    );
                }
                is_uninitialized_peer_exist = true;
                self.ctx.router.close(new_region_id);
            }
            info!(
                "insert new region";
                "region_id" => new_region_id,
                "region" => ?new_region,
                "is_uninitialized_peer_exist" => is_uninitialized_peer_exist,
                "store_id" => self_store_id,
            );

            let (sender, mut new_peer) = match PeerFsm::create(
                self.ctx.store_id(),
                &self.ctx.cfg,
                self.ctx.region_scheduler.clone(),
                self.ctx.raftlog_fetch_scheduler.clone(),
                self.ctx.engines.clone(),
                &new_region,
                false,
            ) {
                Ok((sender, new_peer)) => (sender, new_peer),
                Err(e) => {
                    // peer information is already written into db, can't recover.
                    // there is probably a bug.
                    panic!("create new split region {:?} err {:?}", new_region, e);
                }
            };
            let mut replication_state = self.ctx.global_replication_state.lock().unwrap();
            new_peer.peer.init_replication_mode(&mut replication_state);
            drop(replication_state);

            let meta_peer = new_peer.peer.peer.clone();

            for p in new_region.get_peers() {
                // Add this peer to cache.
                new_peer.peer.insert_peer_cache(p.clone());
            }

            // New peer derive write flow from parent region,
            // this will be used by balance write flow.
            new_peer.peer.peer_stat = self.fsm.peer.peer_stat.clone();
            new_peer.peer.last_compacted_idx = new_peer
                .peer
                .get_store()
                .apply_state()
                .get_truncated_state()
                .get_index()
                + 1;
            let campaigned = new_peer.peer.maybe_campaign(is_leader);
            new_peer.has_ready |= campaigned;

            if is_leader {
                new_peer.peer.set_approximate_size(share_size);
                new_peer.peer.set_approximate_keys(share_keys);
                *new_peer.peer.txn_ext.pessimistic_locks.write() = locks;
                // The new peer is likely to become leader, send a heartbeat immediately to
                // reduce client query miss.
                new_peer.peer.heartbeat_pd(self.ctx);
            }

            new_peer.peer.activate(self.ctx);
            meta.regions.insert(new_region_id, new_region.clone());
            let not_exist = meta
                .region_ranges
                .insert(enc_end_key(&new_region), new_region_id)
                .is_none();
            assert!(not_exist, "[region {}] should not exist", new_region_id);
            meta.readers
                .insert(new_region_id, ReadDelegate::from_peer(new_peer.get_peer()));
            meta.region_read_progress
                .insert(new_region_id, new_peer.peer.read_progress.clone());
            let mailbox = BasicMailbox::new(sender, new_peer, self.ctx.router.state_cnt().clone());
            self.ctx.router.register(new_region_id, mailbox);
            self.ctx
                .router
                .force_send(new_region_id, PeerMsg::Start)
                .unwrap();

            if !campaigned {
                // The new peer has not campaigned yet, record it for later campaign.
                if !is_follower {
                    self.fsm.peer.uncampaigned_new_regions.0.push(new_region_id);
                }
                if let Some(msg) = meta
                    .pending_msgs
                    .swap_remove_front(|m| m.get_to_peer() == &meta_peer)
                {
                    let peer_msg = PeerMsg::RaftMessage(
                        Box::new(InspectedRaftMessage { heap_size: 0, msg }),
                        Some(TiInstant::now()),
                    );
                    if let Err(e) = self.ctx.router.force_send(new_region_id, peer_msg) {
                        warn!("handle first requset failed"; "region_id" => region_id, "error" => ?e);
                    }
                }
            }
        }
        drop(meta);
        if is_leader {
            self.on_split_region_check_tick();
        } else {
            // Update the timestamp if there exists uncampaigned regions.
            if !self.fsm.peer.uncampaigned_new_regions.0.is_empty() {
                self.fsm.peer.uncampaigned_new_regions.1 = Instant::now();
            }
        }
        fail_point!("after_split", self.ctx.store_id() == 3, |_| {});
    }

    fn register_merge_check_tick(&mut self) {
        self.schedule_tick(PeerTick::CheckMerge)
    }

    /// Check if merge target region is staler than the local one in kv engine.
    /// It should be called when target region is not in region map in memory.
    /// If everything is ok, the answer should always be true because PD should
    /// ensure all target peers exist. So if not, error log will be printed
    /// and return false.
    fn is_merge_target_region_stale(&self, target_region: &metapb::Region) -> Result<bool> {
        let target_region_id = target_region.get_id();
        let target_peer_id = find_peer(target_region, self.ctx.store_id())
            .unwrap()
            .get_id();

        let state_key = keys::region_state_key(target_region_id);
        if let Some(target_state) = self
            .ctx
            .engines
            .kv
            .get_msg_cf::<RegionLocalState>(CF_RAFT, &state_key)?
        {
            let state_epoch = target_state.get_region().get_region_epoch();
            if util::is_epoch_stale(target_region.get_region_epoch(), state_epoch) {
                return Ok(true);
            }
            // The local target region epoch is staler than target region's.
            // In the case where the peer is destroyed by receiving gc msg rather than
            // applying conf change, the epoch may staler but it's legal, so check peer id
            // to assure that.
            if let Some(local_target_peer_id) =
                find_peer(target_state.get_region(), self.ctx.store_id()).map(|r| r.get_id())
            {
                match local_target_peer_id.cmp(&target_peer_id) {
                    cmp::Ordering::Equal => {
                        if target_state.get_state() == PeerState::Tombstone {
                            // The local target peer has already been destroyed.
                            return Ok(true);
                        }
                        error!(
                            "the local target peer state is not tombstone in kv engine";
                            "target_peer_id" => target_peer_id,
                            "target_peer_state" => ?target_state.get_state(),
                            "target_region" => ?target_region,
                            "region_id" => self.fsm.region_id(),
                            "peer_id" => self.fsm.peer_id(),
                        );
                    }
                    cmp::Ordering::Greater => {
                        if state_epoch.get_version() == 0 && state_epoch.get_conf_ver() == 0 {
                            // There is a new peer and it's destroyed without being initialised.
                            return Ok(true);
                        }
                        // The local target peer id is greater than the one in target region, but
                        // its epoch is staler than target_region's. That is contradictory.
                        panic!("{} local target peer id {} is greater than the one in target region {}, but its epoch is staler, local target region {:?},
                                    target region {:?}", self.fsm.peer.tag, local_target_peer_id, target_peer_id, target_state.get_region(), target_region);
                    }
                    cmp::Ordering::Less => {
                        error!(
                            "the local target peer id in kv engine is less than the one in target region";
                            "local_target_peer_id" => local_target_peer_id,
                            "target_peer_id" => target_peer_id,
                            "target_region" => ?target_region,
                            "region_id" => self.fsm.region_id(),
                            "peer_id" => self.fsm.peer_id(),
                        );
                    }
                }
            } else {
                // Can't get local target peer id probably because this target peer is removed
                // by applying conf change
                error!(
                    "the local target peer does not exist in target region state";
                    "target_region" => ?target_region,
                    "local_target" => ?target_state.get_region(),
                    "region_id" => self.fsm.region_id(),
                    "peer_id" => self.fsm.peer_id(),
                );
            }
        } else {
            error!(
                "failed to load target peer's RegionLocalState from kv engine";
                "target_peer_id" => target_peer_id,
                "target_region" => ?target_region,
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
            );
        }
        Ok(false)
    }

    fn validate_merge_peer(&self, target_region: &metapb::Region) -> Result<bool> {
        let target_region_id = target_region.get_id();
        let exist_region = {
            let meta = self.ctx.store_meta.lock().unwrap();
            meta.regions.get(&target_region_id).cloned()
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
                    "target region still not catch up, skip.";
                    "region_id" => self.fsm.region_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "target_region" => ?target_region,
                    "exist_region" => ?r,
                );
                return Ok(false);
            }
            return Ok(true);
        }

        // All of the target peers must exist before merging which is guaranteed by PD.
        // Now the target peer is not in region map.
        match self.is_merge_target_region_stale(target_region) {
            Err(e) => {
                error!(%e;
                    "failed to load region state, ignore";
                    "region_id" => self.fsm.region_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "target_region_id" => target_region_id,
                );
                Ok(false)
            }
            Ok(true) => Err(box_err!("region {} is destroyed", target_region_id)),
            Ok(false) => {
                if self.ctx.cfg.dev_assert {
                    panic!(
                        "something is wrong, maybe PD do not ensure all target peers exist before merging"
                    );
                }
                error!(
                    "something is wrong, maybe PD do not ensure all target peers exist before merging"
                );
                Ok(false)
            }
        }
    }

    fn schedule_merge(&mut self) -> Result<()> {
        fail_point!("on_schedule_merge", |_| Ok(()));
        fail_point!("on_schedule_merge_ret_err", |_| Err(Error::RegionNotFound(
            1
        )));
        let (request, target_id) = {
            let state = self.fsm.peer.pending_merge_state.as_ref().unwrap();
            let expect_region = state.get_target();

            if !self.validate_merge_peer(expect_region)? {
                // Wait till next round.
                return Ok(());
            }
            let target_id = expect_region.get_id();
            let sibling_region = expect_region;

            let (min_index, _) = self.fsm.peer.get_min_progress()?;
            let low = cmp::max(min_index + 1, state.get_min_index());
            // TODO: move this into raft module.
            // > over >= to include the PrepareMerge proposal.
            let entries = if low > state.get_commit() {
                vec![]
            } else {
                // TODO: fetch entries in async way
                match self.fsm.peer.get_store().entries(
                    low,
                    state.get_commit() + 1,
                    NO_LIMIT,
                    GetEntriesContext::empty(false),
                ) {
                    Ok(ents) => ents,
                    Err(e) => panic!(
                        "[region {}] {} failed to get merge entires: {:?}, low:{}, commit: {}",
                        self.fsm.region_id(),
                        self.fsm.peer_id(),
                        e,
                        low,
                        state.get_commit()
                    ),
                }
            };

            let sibling_peer = find_peer(sibling_region, self.store_id()).unwrap();
            let mut request = new_admin_request(sibling_region.get_id(), sibling_peer.clone());
            request
                .mut_header()
                .set_region_epoch(sibling_region.get_region_epoch().clone());
            let mut admin = AdminRequest::default();
            admin.set_cmd_type(AdminCmdType::CommitMerge);
            admin
                .mut_commit_merge()
                .set_source(self.fsm.peer.region().clone());
            admin.mut_commit_merge().set_commit(state.get_commit());
            admin.mut_commit_merge().set_entries(entries.into());
            request.set_admin_request(admin);
            (request, target_id)
        };
        // Please note that, here assumes that the unit of network isolation is store
        // rather than peer. So a quorum stores of source region should also be the
        // quorum stores of target region. Otherwise we need to enable proposal
        // forwarding.
        self.ctx
            .router
            .force_send(
                target_id,
                PeerMsg::RaftCommand(Box::new(RaftCommand::new_ext(
                    request,
                    Callback::None,
                    RaftCmdExtraOpts {
                        deadline: None,
                        disk_full_opt: DiskFullOpt::AllowedOnAlmostFull,
                    },
                ))),
            )
            .map_err(|_| Error::RegionNotFound(target_id))
    }

    fn rollback_merge(&mut self) {
        let req = {
            let state = self.fsm.peer.pending_merge_state.as_ref().unwrap();
            let mut request =
                new_admin_request(self.fsm.peer.region().get_id(), self.fsm.peer.peer.clone());
            request
                .mut_header()
                .set_region_epoch(self.fsm.peer.region().get_region_epoch().clone());
            let mut admin = AdminRequest::default();
            admin.set_cmd_type(AdminCmdType::RollbackMerge);
            admin.mut_rollback_merge().set_commit(state.get_commit());
            request.set_admin_request(admin);
            request
        };
        self.propose_raft_command(req, Callback::None, DiskFullOpt::AllowedOnAlmostFull);
    }

    fn on_check_merge(&mut self) {
        if self.fsm.stopped
            || self.fsm.peer.pending_remove
            || self.fsm.peer.pending_merge_state.is_none()
        {
            return;
        }
        self.register_merge_check_tick();
        fail_point!(
            "on_check_merge_not_1001",
            self.fsm.peer_id() != 1001,
            |_| {}
        );
        if let Err(e) = self.schedule_merge() {
            if self.fsm.peer.is_leader() {
                self.fsm
                    .peer
                    .add_want_rollback_merge_peer(self.fsm.peer_id());
                if self
                    .fsm
                    .peer
                    .raft_group
                    .raft
                    .prs()
                    .has_quorum(&self.fsm.peer.want_rollback_merge_peers)
                {
                    info!(
                        "failed to schedule merge, rollback";
                        "region_id" => self.fsm.region_id(),
                        "peer_id" => self.fsm.peer_id(),
                        "err" => %e,
                        "error_code" => %e.error_code(),
                    );
                    self.rollback_merge();
                } else if let Some(ForceLeaderState::ForceLeader { .. }) =
                    &self.fsm.peer.force_leader
                {
                    info!(
                        "failed to schedule merge, rollback in force leader state";
                        "region_id" => self.fsm.region_id(),
                        "peer_id" => self.fsm.peer_id(),
                        "err" => %e,
                        "error_code" => %e.error_code(),
                    );
                    self.rollback_merge();
                }
            } else if !is_learner(&self.fsm.peer.peer) {
                info!(
                    "want to rollback merge";
                    "region_id" => self.fsm.region_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "leader_id" => self.fsm.peer.leader_id(),
                    "err" => %e,
                    "error_code" => %e.error_code(),
                );
                if self.fsm.peer.leader_id() != raft::INVALID_ID {
                    self.fsm.peer.send_want_rollback_merge(
                        self.fsm
                            .peer
                            .pending_merge_state
                            .as_ref()
                            .unwrap()
                            .get_commit(),
                        self.ctx,
                    );
                }
            }
        }
    }

    fn on_ready_prepare_merge(&mut self, region: metapb::Region, state: MergeState) {
        fail_point!("on_apply_res_prepare_merge");
        {
            let mut meta = self.ctx.store_meta.lock().unwrap();
            meta.set_region(
                &self.ctx.coprocessor_host,
                region,
                &mut self.fsm.peer,
                RegionChangeReason::PrepareMerge,
            );
        }

        self.fsm.peer.pending_merge_state = Some(state);
        let state = self.fsm.peer.pending_merge_state.as_ref().unwrap();

        if let Some(ref catch_up_logs) = self.fsm.peer.catch_up_logs {
            if state.get_commit() == catch_up_logs.merge.get_commit() {
                assert_eq!(state.get_target().get_id(), catch_up_logs.target_region_id);
                // Indicate that `on_catch_up_logs_for_merge` has already executed.
                // Mark pending_remove because its apply fsm will be destroyed.
                self.fsm.peer.pending_remove = true;
                // Send CatchUpLogs back to destroy source apply fsm,
                // then it will send `Noop` to trigger target apply fsm.
                self.ctx.apply_router.schedule_task(
                    self.fsm.region_id(),
                    ApplyTask::LogsUpToDate(self.fsm.peer.catch_up_logs.take().unwrap()),
                );
                return;
            }
        }

        self.on_check_merge();
    }

    fn on_catch_up_logs_for_merge(&mut self, mut catch_up_logs: CatchUpLogs) {
        let region_id = self.fsm.region_id();
        assert_eq!(region_id, catch_up_logs.merge.get_source().get_id());

        if let Some(ref cul) = self.fsm.peer.catch_up_logs {
            panic!(
                "{} get catch_up_logs from {} but has already got from {}",
                self.fsm.peer.tag, catch_up_logs.target_region_id, cul.target_region_id
            )
        }

        if let Some(ref pending_merge_state) = self.fsm.peer.pending_merge_state {
            if pending_merge_state.get_commit() == catch_up_logs.merge.get_commit() {
                assert_eq!(
                    pending_merge_state.get_target().get_id(),
                    catch_up_logs.target_region_id
                );
                // Indicate that `on_ready_prepare_merge` has already executed.
                // Mark pending_remove because its apply fsm will be destroyed.
                self.fsm.peer.pending_remove = true;
                // Just for saving memory.
                catch_up_logs.merge.clear_entries();
                // Send CatchUpLogs back to destroy source apply fsm,
                // then it will send `Noop` to trigger target apply fsm.
                self.ctx
                    .apply_router
                    .schedule_task(region_id, ApplyTask::LogsUpToDate(catch_up_logs));
                return;
            }
        }

        // Directly append these logs to raft log and then commit them.
        match self
            .fsm
            .peer
            .maybe_append_merge_entries(&catch_up_logs.merge)
        {
            Some(last_index) => {
                info!(
                    "append and commit entries to source region";
                    "region_id" => region_id,
                    "peer_id" => self.fsm.peer.peer_id(),
                    "last_index" => last_index,
                );
                // Now it has some committed entries, so mark it to take `Ready` in next round.
                self.fsm.has_ready = true;
            }
            None => {
                info!(
                    "no need to catch up logs";
                    "region_id" => region_id,
                    "peer_id" => self.fsm.peer.peer_id(),
                );
            }
        }
        // Just for saving memory.
        catch_up_logs.merge.clear_entries();
        self.fsm.peer.catch_up_logs = Some(catch_up_logs);
    }

    fn on_ready_commit_merge(
        &mut self,
        merge_index: u64,
        region: metapb::Region,
        source: metapb::Region,
    ) {
        self.register_split_region_check_tick();
        let mut meta = self.ctx.store_meta.lock().unwrap();

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
                self.fsm.peer.tag, prev, meta.region_ranges
            );
        }

        meta.region_ranges
            .insert(enc_end_key(&region), region.get_id());
        assert!(meta.regions.remove(&source.get_id()).is_some());
        meta.set_region(
            &self.ctx.coprocessor_host,
            region,
            &mut self.fsm.peer,
            RegionChangeReason::CommitMerge,
        );
        if let Some(d) = meta.readers.get_mut(&source.get_id()) {
            d.mark_pending_remove();
        }

        // After the region commit merged, the region's key range is extended and the
        // region's `safe_ts` should reset to `min(source_safe_ts, target_safe_ts)`
        let source_read_progress = meta.region_read_progress.remove(&source.get_id()).unwrap();
        self.fsm.peer.read_progress.merge_safe_ts(
            source_read_progress.safe_ts(),
            merge_index,
            &self.ctx.coprocessor_host,
        );

        // If a follower merges into a leader, a more recent read may happen
        // on the leader of the follower. So max ts should be updated after
        // a region merge.
        self.fsm
            .peer
            .require_updating_max_ts(&self.ctx.pd_scheduler);

        drop(meta);

        // make approximate size and keys updated in time.
        // the reason why follower need to update is that there is a issue that after
        // merge and then transfer leader, the new leader may have stale size and keys.
        self.fsm.peer.split_check_trigger.reset_skip_check();
        self.fsm.peer.reset_region_buckets();
        if self.fsm.peer.is_leader() {
            info!(
                "notify pd with merge";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "source_region" => ?source,
                "target_region" => ?self.fsm.peer.region(),
            );
            self.fsm.peer.heartbeat_pd(self.ctx);
        }
        if let Err(e) = self.ctx.router.force_send(
            source.get_id(),
            PeerMsg::SignificantMsg(Box::new(SignificantMsg::MergeResult {
                target_region_id: self.fsm.region_id(),
                target: self.fsm.peer.peer.clone(),
                result: MergeResultKind::FromTargetLog,
            })),
        ) {
            panic!(
                "{} failed to send merge result(FromTargetLog) to source region {}, err {}",
                self.fsm.peer.tag,
                source.get_id(),
                e
            );
        }
    }

    /// Handle rollbacking Merge result.
    ///
    /// If commit is 0, it means that Merge is rollbacked by a snapshot;
    /// otherwise it's rollbacked by a proposal, and its value should be
    /// equal to the commit index of previous PrepareMerge.
    fn on_ready_rollback_merge(&mut self, commit: u64, region: Option<metapb::Region>) {
        let pending_commit = self
            .fsm
            .peer
            .pending_merge_state
            .as_ref()
            .unwrap()
            .get_commit();
        if commit != 0 && pending_commit != commit {
            panic!(
                "{} rollbacks a wrong merge: {} != {}",
                self.fsm.peer.tag, pending_commit, commit
            );
        }
        // Clear merge releted data
        self.fsm.peer.pending_merge_state = None;
        self.fsm.peer.want_rollback_merge_peers.clear();

        // Resume updating `safe_ts`
        self.fsm.peer.read_progress.resume();

        if let Some(r) = region {
            let mut meta = self.ctx.store_meta.lock().unwrap();
            meta.set_region(
                &self.ctx.coprocessor_host,
                r,
                &mut self.fsm.peer,
                RegionChangeReason::RollbackMerge,
            );
        }
        if self.fsm.peer.is_leader() {
            info!(
                "notify pd with rollback merge";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "commit_index" => commit,
            );
            {
                let mut pessimistic_locks = self.fsm.peer.txn_ext.pessimistic_locks.write();
                if pessimistic_locks.status == LocksStatus::MergingRegion {
                    pessimistic_locks.status = LocksStatus::Normal;
                }
            }
            self.fsm.peer.heartbeat_pd(self.ctx);
        }
    }

    fn on_merge_result(
        &mut self,
        target_region_id: u64,
        target: metapb::Peer,
        result: MergeResultKind,
    ) {
        let exists = self
            .fsm
            .peer
            .pending_merge_state
            .as_ref()
            .map_or(true, |s| {
                s.get_target().get_peers().iter().any(|p| {
                    p.get_store_id() == target.get_store_id() && p.get_id() <= target.get_id()
                })
            });
        if !exists {
            panic!(
                "{} unexpected merge result: {:?} {:?} {:?}",
                self.fsm.peer.tag, self.fsm.peer.pending_merge_state, target, result
            );
        }
        // Because of the checking before proposing `PrepareMerge`, which is
        // no `CompactLog` proposal between the smallest commit index and the latest
        // index. If the merge succeed, all source peers are impossible in apply
        // snapshot state and must be initialized.
        {
            let meta = self.ctx.store_meta.lock().unwrap();
            if meta.atomic_snap_regions.contains_key(&self.region_id()) {
                panic!(
                    "{} is applying atomic snapshot on getting merge result, target region id {}, target peer {:?}, merge result type {:?}",
                    self.fsm.peer.tag, target_region_id, target, result
                );
            }
        }
        if self.fsm.peer.is_handling_snapshot() {
            panic!(
                "{} is applying snapshot on getting merge result, target region id {}, target peer {:?}, merge result type {:?}",
                self.fsm.peer.tag, target_region_id, target, result
            );
        }
        if !self.fsm.peer.is_initialized() {
            panic!(
                "{} is not initialized on getting merge result, target region id {}, target peer {:?}, merge result type {:?}",
                self.fsm.peer.tag, target_region_id, target, result
            );
        }
        match result {
            MergeResultKind::FromTargetLog => {
                info!(
                    "merge finished";
                    "region_id" => self.fsm.region_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "target_region" => ?self.fsm.peer.pending_merge_state.as_ref().unwrap().target,
                );
                self.destroy_peer(true);
            }
            MergeResultKind::FromTargetSnapshotStep1 => {
                info!(
                    "merge finished with target snapshot";
                    "region_id" => self.fsm.region_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "target_region_id" => target_region_id,
                );
                self.fsm.peer.pending_remove = true;
                // Destroy apply fsm at first
                self.ctx.apply_router.schedule_task(
                    self.fsm.region_id(),
                    ApplyTask::destroy(self.fsm.region_id(), true),
                );
            }
            MergeResultKind::FromTargetSnapshotStep2 => {
                // `merged_by_target` is true because this region's range already belongs to
                // its target region so we must not clear data otherwise its target region's
                // data will corrupt.
                self.destroy_peer(true);
            }
            MergeResultKind::Stale => {
                self.on_stale_merge(target_region_id);
            }
        };
    }

    fn on_stale_merge(&mut self, target_region_id: u64) {
        if self.fsm.peer.pending_remove {
            return;
        }
        info!(
            "successful merge can't be continued, try to gc stale peer";
            "region_id" => self.fsm.region_id(),
            "peer_id" => self.fsm.peer_id(),
            "target_region_id" => target_region_id,
            "merge_state" => ?self.fsm.peer.pending_merge_state,
        );
        // Because of the checking before proposing `PrepareMerge`, which is
        // no `CompactLog` proposal between the smallest commit index and the latest
        // index. If the merge succeed, all source peers are impossible in apply
        // snapshot state and must be initialized.
        // So `maybe_destroy` must succeed here.
        let job = self.fsm.peer.maybe_destroy(self.ctx).unwrap();
        self.handle_destroy_peer(job);
    }

    fn on_ready_persist_snapshot(&mut self, persist_res: PersistSnapshotResult) {
        let prev_region = persist_res.prev_region;
        let region = persist_res.region;

        info!(
            "snapshot is persisted";
            "region_id" => self.fsm.region_id(),
            "peer_id" => self.fsm.peer_id(),
            "region" => ?region,
            "destroy_regions" => ?persist_res.destroy_regions,
        );

        let mut state = self.ctx.global_replication_state.lock().unwrap();
        let gb = state
            .calculate_commit_group(self.fsm.peer.replication_mode_version, region.get_peers());
        self.fsm.peer.raft_group.raft.clear_commit_group();
        self.fsm.peer.raft_group.raft.assign_commit_groups(gb);
        fail_point!("after_assign_commit_groups_on_apply_snapshot");
        // drop it before access `store_meta`.
        drop(state);

        let mut meta = self.ctx.store_meta.lock().unwrap();
        debug!(
            "check snapshot range";
            "region_id" => self.fsm.region_id(),
            "peer_id" => self.fsm.peer_id(),
            "prev_region" => ?prev_region,
        );

        meta.readers.insert(
            self.fsm.region_id(),
            ReadDelegate::from_peer(&self.fsm.peer),
        );

        // Remove this region's snapshot region from the `pending_snapshot_regions`
        // The `pending_snapshot_regions` is only used to occupy the key range, so if
        // this peer is added to `region_ranges`, it can be remove from
        // `pending_snapshot_regions`
        meta.pending_snapshot_regions
            .retain(|r| self.fsm.region_id() != r.get_id());

        // Remove its source peers' metadata
        for r in &persist_res.destroy_regions {
            let prev = meta.region_ranges.remove(&enc_end_key(r));
            assert_eq!(prev, Some(r.get_id()));
            assert!(meta.regions.remove(&r.get_id()).is_some());
            if let Some(d) = meta.readers.get_mut(&r.get_id()) {
                d.mark_pending_remove();
            }
        }
        // Remove the data from `atomic_snap_regions` and `destroyed_region_for_snap`
        // which are added before applying snapshot
        if let Some(wait_destroy_regions) = meta.atomic_snap_regions.remove(&self.fsm.region_id()) {
            for (source_region_id, _) in wait_destroy_regions {
                assert_eq!(
                    meta.destroyed_region_for_snap
                        .remove(&source_region_id)
                        .is_some(),
                    true
                );
            }
        }

        if util::is_region_initialized(&prev_region) {
            info!(
                "region changed after persisting snapshot";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "prev_region" => ?prev_region,
                "region" => ?region,
            );
            let prev = meta.region_ranges.remove(&enc_end_key(&prev_region));
            if prev != Some(region.get_id()) {
                panic!(
                    "{} meta corrupted, expect {:?} got {:?}",
                    self.fsm.peer.tag, prev_region, prev,
                );
            }
        } else if self.fsm.peer.local_first_replicate {
            // This peer is uninitialized previously.
            // More accurately, the `RegionLocalState` has been persisted so the data can be
            // removed from `pending_create_peers`.
            let mut pending_create_peers = self.ctx.pending_create_peers.lock().unwrap();
            assert_eq!(
                pending_create_peers.remove(&self.fsm.region_id()),
                Some((self.fsm.peer_id(), false))
            );
        }

        if let Some(r) = meta
            .region_ranges
            .insert(enc_end_key(&region), region.get_id())
        {
            panic!("{} unexpected region {:?}", self.fsm.peer.tag, r);
        }
        let prev = meta.regions.insert(region.get_id(), region.clone());
        assert_eq!(prev, Some(prev_region));
        drop(meta);

        self.fsm.peer.read_progress.update_leader_info(
            self.fsm.peer.leader_id(),
            self.fsm.peer.term(),
            &region,
        );

        for r in &persist_res.destroy_regions {
            if let Err(e) = self.ctx.router.force_send(
                r.get_id(),
                PeerMsg::SignificantMsg(Box::new(SignificantMsg::MergeResult {
                    target_region_id: self.fsm.region_id(),
                    target: self.fsm.peer.peer.clone(),
                    result: MergeResultKind::FromTargetSnapshotStep2,
                })),
            ) {
                panic!(
                    "{} failed to send merge result(FromTargetSnapshotStep2) to source region {}, err {}",
                    self.fsm.peer.tag,
                    r.get_id(),
                    e
                );
            }
        }
    }

    fn on_ready_result(
        &mut self,
        exec_results: &mut VecDeque<ExecResult<EK::Snapshot>>,
        metrics: &ApplyMetrics,
    ) {
        // handle executing committed log results
        while let Some(result) = exec_results.pop_front() {
            match result {
                ExecResult::ChangePeer(cp) => self.on_ready_change_peer(cp),
                ExecResult::CompactLog {
                    state,
                    first_index,
                    has_pending,
                } => {
                    self.fsm.peer.has_pending_compact_cmd = has_pending;
                    self.on_ready_compact_log(first_index, state);
                }
                ExecResult::SplitRegion {
                    derived,
                    regions,
                    new_split_regions,
                    share_source_region_size,
                } => self.on_ready_split_region(
                    derived,
                    regions,
                    new_split_regions,
                    share_source_region_size,
                ),
                ExecResult::PrepareMerge { region, state } => {
                    self.on_ready_prepare_merge(region, state)
                }
                ExecResult::CommitMerge {
                    index,
                    region,
                    source,
                } => self.on_ready_commit_merge(index, region, source),
                ExecResult::RollbackMerge { region, commit } => {
                    self.on_ready_rollback_merge(commit, Some(region))
                }
                ExecResult::ComputeHash {
                    region,
                    index,
                    context,
                    snap,
                } => self.on_ready_compute_hash(region, index, context, snap),
                ExecResult::VerifyHash {
                    index,
                    context,
                    hash,
                } => self.on_ready_verify_hash(index, context, hash),
                ExecResult::DeleteRange { .. } => {
                    // TODO: clean user properties?
                }
                ExecResult::IngestSst { ssts } => self.on_ingest_sst_result(ssts),
                ExecResult::TransferLeader { term } => self.on_transfer_leader(term),
                ExecResult::Flashback { region } => self.on_set_flashback_state(region),
                ExecResult::BatchSwitchWitness(switches) => {
                    self.on_ready_batch_switch_witness(switches)
                }
                ExecResult::HasPendingCompactCmd(has_pending) => {
                    self.fsm.peer.has_pending_compact_cmd = has_pending;
                    if has_pending {
                        self.register_pull_voter_replicated_index_tick();
                    }
                }
                ExecResult::UnsafeForceCompact { apply_state } => {
                    let last_index = apply_state.get_truncated_state().index;
                    let first_index = self.fsm.peer.raft_group.raft.r.raft_log.first_index();

                    let raft_engine = self.fsm.peer.get_store().raft_engine();
                    let mut batch = raft_engine.log_batch(2);
                    raft_engine
                        .gc(self.region_id(), first_index, last_index, &mut batch)
                        .unwrap();
                    batch
                        .put_raft_state(self.region_id(), self.fsm.peer.get_store().raft_state())
                        .unwrap();
                    // FIXME: generally, we should avoiding do io tasks on the raft thread, but make
                    // it async make the overall procss more complex.
                    // Considering unsafe recovery happens very rarely, thus the potential
                    // performance impact is acceptable in this scenario.
                    raft_engine.consume(&mut batch, true).unwrap();

                    {
                        let peer_store = self.fsm.peer.mut_store();
                        peer_store.set_apply_state(apply_state);
                        peer_store.clear_entry_cache_warmup_state();
                        peer_store.compact_entry_cache(last_index + 1);
                        peer_store.raft_state_mut().mut_hard_state().commit = last_index;
                        peer_store.raft_state_mut().last_index = last_index;
                    }
                    assert!(
                        self.fsm
                            .peer
                            .raft_group
                            .raft
                            .raft_log
                            .unstable
                            .entries
                            .is_empty()
                    );
                    self.fsm.peer.raft_group.raft.raft_log.unstable.offset = last_index + 1;
                    self.fsm.peer.raft_group.raft.raft_log.committed = last_index;
                    self.fsm.peer.raft_group.raft.raft_log.persisted = last_index;

                    if let Some(ForceLeaderState::WaitForceCompact {
                        syncer,
                        failed_stores,
                    }) = &self.fsm.peer.force_leader
                    {
                        self.on_enter_pre_force_leader(syncer.clone(), failed_stores.clone());
                    }
                }
            }
        }

        // Update metrics only when all exec_results are finished in case the metrics is
        // counted multiple times when waiting for commit merge
        self.ctx.store_stat.lock_cf_bytes_written += metrics.lock_cf_written_bytes;
        self.ctx.store_stat.engine_total_bytes_written += metrics.written_bytes;
        self.ctx.store_stat.engine_total_keys_written += metrics.written_keys;
    }

    /// Check if a request is valid if it has valid prepare_merge/commit_merge
    /// proposal.
    fn check_merge_proposal(&self, msg: &mut RaftCmdRequest) -> Result<()> {
        if !msg.get_admin_request().has_prepare_merge()
            && !msg.get_admin_request().has_commit_merge()
        {
            return Ok(());
        }

        let region = self.fsm.peer.region();
        if msg.get_admin_request().has_prepare_merge() {
            // Just for simplicity, do not start region merge while in joint state
            if self.fsm.peer.in_joint_state() {
                return Err(box_err!(
                    "{} region in joint state, can not propose merge command, command: {:?}",
                    self.fsm.peer.tag,
                    msg.get_admin_request()
                ));
            }
            let target_region = msg.get_admin_request().get_prepare_merge().get_target();
            {
                let meta = self.ctx.store_meta.lock().unwrap();
                match meta.regions.get(&target_region.get_id()) {
                    Some(r) => {
                        if r != target_region {
                            return Err(box_err!(
                                "target region not matched, skip proposing: {:?} != {:?}",
                                r,
                                target_region
                            ));
                        }
                    }
                    None => {
                        return Err(box_err!(
                            "target region {} doesn't exist.",
                            target_region.get_id()
                        ));
                    }
                }
            }
            if !util::is_sibling_regions(target_region, region) {
                return Err(box_err!(
                    "{:?} and {:?} are not sibling, skip proposing.",
                    target_region,
                    region
                ));
            }
            if !region_on_same_stores(target_region, region) {
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
                    "{:?} and {:?} should be sibling",
                    source_region,
                    region
                ));
            }
            if !region_on_same_stores(source_region, region) {
                return Err(box_err!(
                    "peers not matched: {:?} {:?}",
                    source_region,
                    region
                ));
            }
        }

        Ok(())
    }

    fn pre_propose_raft_command(
        &mut self,
        msg: &RaftCmdRequest,
    ) -> Result<Option<RaftCmdResponse>> {
        // failpoint
        fail_point!(
            "fail_pre_propose_split",
            msg.has_admin_request()
                && msg.get_admin_request().get_cmd_type() == AdminCmdType::BatchSplit,
            |_| Err(Error::Other(box_err!("fail_point")))
        );

        // Check store_id, make sure that the msg is dispatched to the right place.
        if let Err(e) = util::check_store_id(msg.get_header(), self.store_id()) {
            self.ctx
                .raft_metrics
                .invalid_proposal
                .mismatch_store_id
                .inc();
            return Err(e);
        }
        if msg.has_status_request() {
            // For status commands, we handle it here directly.
            let resp = self.execute_status_command(msg)?;
            return Ok(Some(resp));
        }

        // Check whether the store has the right peer to handle the request.
        let leader_id = self.fsm.peer.leader_id();
        let request = msg.get_requests();

        // peer_id must be the same as peer's.
        if let Err(e) = util::check_peer_id(msg.get_header(), self.fsm.peer.peer_id()) {
            self.ctx
                .raft_metrics
                .invalid_proposal
                .mismatch_peer_id
                .inc();
            return Err(e);
        }

        if self.fsm.peer.force_leader.is_some() {
            self.ctx.raft_metrics.invalid_proposal.force_leader.inc();
            // in force leader state, forbid requests to make the recovery progress less
            // error-prone
            if !(msg.has_admin_request()
                && (msg.get_admin_request().get_cmd_type() == AdminCmdType::ChangePeer
                    || msg.get_admin_request().get_cmd_type() == AdminCmdType::ChangePeerV2
                    || msg.get_admin_request().get_cmd_type() == AdminCmdType::RollbackMerge))
            {
                return Err(Error::RecoveryInProgress(self.region_id()));
            }
        }

        // ReadIndex can be processed on the replicas.
        let is_read_index_request =
            request.len() == 1 && request[0].get_cmd_type() == CmdType::ReadIndex;
        let read_only = msg.get_requests().iter().all(|r| {
            matches!(
                r.get_cmd_type(),
                CmdType::Get | CmdType::Snap | CmdType::ReadIndex,
            )
        });
        let region_id = self.region_id();
        let allow_replica_read = read_only && msg.get_header().get_replica_read();
        let flags = WriteBatchFlags::from_bits_check(msg.get_header().get_flags());
        let allow_stale_read = read_only && flags.contains(WriteBatchFlags::STALE_READ);
        if !self.fsm.peer.is_leader()
            && !is_read_index_request
            && !allow_replica_read
            && !allow_stale_read
        {
            self.ctx.raft_metrics.invalid_proposal.not_leader.inc();
            let leader = self.fsm.peer.get_peer_from_cache(leader_id);
            self.fsm.reset_hibernate_state(GroupState::Chaos);
            self.register_raft_base_tick();
            return Err(Error::NotLeader(region_id, leader));
        }

        // Forbid requests when it's a witness unless it's transfer leader
        if self.fsm.peer.is_witness()
            && !(msg.has_admin_request()
                && msg.get_admin_request().get_cmd_type() == AdminCmdType::TransferLeader)
        {
            self.ctx.raft_metrics.invalid_proposal.witness.inc();
            return Err(Error::IsWitness(self.region_id()));
        }

        fail_point!("ignore_forbid_leader_to_be_witness", |_| Ok(None));

        // Forbid requests to switch it into a witness when it's a leader
        if self.fsm.peer.is_leader()
            && msg.has_admin_request()
            && msg.get_admin_request().get_cmd_type() == AdminCmdType::BatchSwitchWitness
            && msg
                .get_admin_request()
                .get_switch_witnesses()
                .get_switch_witnesses()
                .iter()
                .any(|s| s.get_peer_id() == self.fsm.peer.peer.get_id() && s.get_is_witness())
        {
            self.ctx.raft_metrics.invalid_proposal.witness.inc();
            return Err(Error::IsWitness(self.region_id()));
        }

        // Forbid requests when it becomes to non-witness but not finish applying
        // snapshot.
        if self.fsm.peer.wait_data {
            self.ctx.raft_metrics.invalid_proposal.non_witness.inc();
            return Err(Error::IsWitness(self.region_id()));
        }

        // check whether the peer is initialized.
        if !self.fsm.peer.is_initialized() {
            self.ctx
                .raft_metrics
                .invalid_proposal
                .region_not_initialized
                .inc();
            return Err(Error::RegionNotInitialized(region_id));
        }
        // If the peer is applying snapshot, it may drop some sending messages, that
        // could make clients wait for response until timeout.
        if self.fsm.peer.is_handling_snapshot() {
            self.ctx
                .raft_metrics
                .invalid_proposal
                .is_applying_snapshot
                .inc();
            // TODO: replace to a more suitable error.
            return Err(Error::Other(box_err!(
                "{} peer is applying snapshot",
                self.fsm.peer.tag
            )));
        }
        // Check whether the term is stale.
        if let Err(e) = util::check_term(msg.get_header(), self.fsm.peer.term()) {
            self.ctx.raft_metrics.invalid_proposal.stale_command.inc();
            return Err(e);
        }

        match util::check_req_region_epoch(msg, self.fsm.peer.region(), true) {
            Err(Error::EpochNotMatch(m, mut new_regions)) => {
                // Attach the region which might be split from the current region. But it
                // doesn't matter if the region is not split from the current region. If the
                // region meta received by the TiKV driver is newer than the meta cached in the
                // driver, the meta is updated.
                let requested_version = msg.get_header().get_region_epoch().version;
                self.collect_sibling_region(requested_version, &mut new_regions);
                self.ctx.raft_metrics.invalid_proposal.epoch_not_match.inc();
                return Err(Error::EpochNotMatch(m, new_regions));
            }
            Err(e) => return Err(e),
            _ => {}
        };
        // Check whether the region is in the flashback state and the request could be
        // proposed. Skip the not prepared error because the
        // `self.region().is_in_flashback` may not be the latest right after applying
        // the `PrepareFlashback` admin command, we will let it pass here and check in
        // the apply phase and because a read-only request doesn't need to be applied,
        // so it will be allowed during the flashback progress, for example, a snapshot
        // request.
        let header = msg.get_header();
        let admin_type = msg.admin_request.as_ref().map(|req| req.get_cmd_type());
        if let Err(e) = util::check_flashback_state(
            self.region().is_in_flashback,
            self.region().flashback_start_ts,
            header,
            admin_type,
            region_id,
            true,
        ) {
            match e {
                Error::FlashbackInProgress(..) => self
                    .ctx
                    .raft_metrics
                    .invalid_proposal
                    .flashback_in_progress
                    .inc(),
                Error::FlashbackNotPrepared(_) => self
                    .ctx
                    .raft_metrics
                    .invalid_proposal
                    .flashback_not_prepared
                    .inc(),
                _ => unreachable!("{:?}", e),
            }
            return Err(e);
        }

        Ok(None)
    }

    /// Proposes pending batch raft commands (if any), then proposes the
    /// provided raft command.
    #[inline]
    fn propose_raft_command(
        &mut self,
        msg: RaftCmdRequest,
        cb: Callback<EK::Snapshot>,
        diskfullopt: DiskFullOpt,
    ) {
        // Propose pending commands before processing new one.
        self.propose_pending_batch_raft_command();
        self.propose_raft_command_internal(msg, cb, diskfullopt);
    }

    /// Propose the raft command directly.
    /// Note that this function introduces a reorder between this command and
    /// batched commands.
    fn propose_raft_command_internal(
        &mut self,
        mut msg: RaftCmdRequest,
        cb: Callback<EK::Snapshot>,
        diskfullopt: DiskFullOpt,
    ) {
        if self.fsm.peer.pending_remove {
            apply::notify_req_region_removed(self.region_id(), cb);
            return;
        }

        if self.ctx.raft_metrics.waterfall_metrics {
            let now = Instant::now();
            for tracker in cb.write_trackers() {
                tracker.observe(now, &self.ctx.raft_metrics.wf_batch_wait, |t| {
                    &mut t.metrics.wf_batch_wait_nanos
                });
            }
        }

        match self.pre_propose_raft_command(&msg) {
            Ok(Some(resp)) => {
                cb.invoke_with_response(resp);
                return;
            }
            Err(e) => {
                // log for admin requests
                let is_admin_request = msg.has_admin_request();
                info_or_debug!(
                    is_admin_request;
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

        if let Err(e) = self.check_merge_proposal(&mut msg) {
            warn!(
                "failed to propose merge";
                "region_id" => self.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "message" => ?msg,
                "err" => %e,
                "error_code" => %e.error_code(),
            );
            cb.invoke_with_response(new_error(e));
            return;
        }

        // Note:
        // The peer that is being checked is a leader. It might step down to be a
        // follower later. It doesn't matter whether the peer is a leader or not. If
        // it's not a leader, the proposing command log entry can't be committed.

        let mut resp = RaftCmdResponse::default();
        let term = self.fsm.peer.term();
        bind_term(&mut resp, term);
        if self.fsm.peer.propose(self.ctx, cb, msg, resp, diskfullopt) {
            self.fsm.has_ready = true;
        }

        if self.fsm.peer.should_wake_up {
            self.reset_raft_tick(GroupState::Ordered);
        }

        self.register_pd_heartbeat_tick();

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
        let anchor = Excluded(enc_end_key(self.fsm.peer.region()));
        let meta = self.ctx.store_meta.lock().unwrap();
        let mut ranges = if self.ctx.cfg.right_derive_when_split {
            meta.region_ranges.range((Unbounded::<Vec<u8>>, anchor))
        } else {
            meta.region_ranges.range((anchor, Unbounded::<Vec<u8>>))
        };

        for _ in 0..MAX_REGIONS_IN_ERROR {
            let res = if self.ctx.cfg.right_derive_when_split {
                ranges.next_back()
            } else {
                ranges.next()
            };
            if let Some((_, id)) = res {
                let r = &meta.regions[id];
                collect_cnt -= 1;
                // For example, A is split into B, A, and then B is split into C, B.
                if r.get_region_epoch().version >= max_version {
                    // It doesn't matter if it's a false positive, as it's limited by
                    // MAX_REGIONS_IN_ERROR.
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

    fn register_raft_gc_log_tick(&mut self) {
        self.schedule_tick(PeerTick::RaftLogGc)
    }

    #[allow(clippy::if_same_then_else)]
    fn on_raft_gc_log_tick(&mut self, force_compact: bool) {
        if !self.fsm.peer.is_leader() {
            // `compact_cache_to` is called when apply, there is no need to call
            // `compact_to` here, snapshot generating has already been cancelled
            // when the role becomes follower.
            return;
        }
        if !self.fsm.peer.get_store().is_entry_cache_empty() || !self.ctx.cfg.hibernate_regions {
            self.register_raft_gc_log_tick();
        }
        fail_point!("on_raft_log_gc_tick_1", self.fsm.peer_id() == 1, |_| {});
        fail_point!("on_raft_gc_log_tick", |_| {});
        debug_assert!(!self.fsm.stopped);

        // As leader, we would not keep caches for the peers that didn't response
        // heartbeat in the last few seconds. That happens probably because
        // another TiKV is down. In this case if we do not clean up the cache,
        // it may keep growing.
        let drop_cache_duration =
            self.ctx.cfg.raft_heartbeat_interval() + self.ctx.cfg.raft_entry_cache_life_time.0;
        let cache_alive_limit = Instant::now() - drop_cache_duration;

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
        // `alive_cache_idx` is the smallest `replicated_index` of healthy up nodes.
        // `alive_cache_idx` is only used to gc cache.
        let applied_idx = self.fsm.peer.get_store().applied_index();
        let truncated_idx = self.fsm.peer.get_store().truncated_index();
        let first_idx = self.fsm.peer.get_store().first_index();
        let last_idx = self.fsm.peer.get_store().last_index();

        let mut voter_replicated_idx = last_idx;
        let (mut replicated_idx, mut alive_cache_idx) = (last_idx, last_idx);
        for (peer_id, p) in self.fsm.peer.raft_group.raft.prs().iter() {
            let peer = find_peer_by_id(self.region(), *peer_id).unwrap();
            if !is_learner(peer) && voter_replicated_idx > p.matched {
                voter_replicated_idx = p.matched;
            }
            if replicated_idx > p.matched {
                replicated_idx = p.matched;
            }
            if let Some(last_heartbeat) = self.fsm.peer.peer_heartbeats.get(peer_id) {
                if *last_heartbeat > cache_alive_limit {
                    if alive_cache_idx > p.matched && p.matched >= truncated_idx {
                        alive_cache_idx = p.matched;
                    } else if p.matched == 0 {
                        // the new peer is still applying snapshot, do not compact cache now
                        alive_cache_idx = 0;
                    }
                }
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

        // leader may call `get_term()` on the latest replicated index, so compact
        // entries before `alive_cache_idx` instead of `alive_cache_idx + 1`.
        self.fsm
            .peer
            .mut_store()
            .compact_entry_cache(std::cmp::min(alive_cache_idx, applied_idx + 1));
        if needs_evict_entry_cache(self.ctx.cfg.evict_cache_on_memory_ratio) {
            self.fsm.peer.mut_store().evict_entry_cache(true);
            if !self.fsm.peer.get_store().is_entry_cache_empty() {
                self.register_entry_cache_evict_tick();
            }
        }

        let mut compact_idx = if force_compact && replicated_idx > first_idx {
            replicated_idx
        } else if (applied_idx > first_idx
            && applied_idx - first_idx >= self.ctx.cfg.raft_log_gc_count_limit())
            || (self.fsm.peer.raft_log_size_hint >= self.ctx.cfg.raft_log_gc_size_limit().0)
        {
            std::cmp::max(first_idx + (last_idx - first_idx) / 2, replicated_idx)
        } else if replicated_idx < first_idx || last_idx - first_idx < 3 {
            // In the current implementation one compaction can't delete all stale Raft
            // logs. There will be at least 3 entries left after one compaction:
            // ```
            // |------------- entries needs to be compacted ----------|
            // [entries...][the entry at `compact_idx`][the last entry][new compaction entry]
            //             |-------------------- entries will be left ----------------------|
            // ```
            self.ctx.raft_metrics.raft_log_gc_skipped.reserve_log.inc();
            return;
        } else if replicated_idx - first_idx < self.ctx.cfg.raft_log_gc_threshold
            && self.fsm.skip_gc_raft_log_ticks < self.ctx.cfg.raft_log_reserve_max_ticks
        {
            self.ctx
                .raft_metrics
                .raft_log_gc_skipped
                .threshold_limit
                .inc();
            // Logs will only be kept `max_ticks` * `raft_log_gc_tick_interval`.
            self.fsm.skip_gc_raft_log_ticks += 1;
            self.register_raft_gc_log_tick();
            return;
        } else {
            replicated_idx
        };
        // Avoid compacting unpersisted raft logs when persist is far behind apply.
        if compact_idx > self.fsm.peer.raft_group.raft.raft_log.persisted {
            compact_idx = self.fsm.peer.raft_group.raft.raft_log.persisted;
        }
        assert!(compact_idx >= first_idx);
        // Have no idea why subtract 1 here, but original code did this by magic.
        compact_idx -= 1;
        if compact_idx < first_idx {
            // In case compact_idx == first_idx before subtraction.
            self.ctx
                .raft_metrics
                .raft_log_gc_skipped
                .compact_idx_too_small
                .inc();
            return;
        }

        // Create a compact log request and notify directly.
        let region_id = self.fsm.peer.region().get_id();
        let peer = self.fsm.peer.peer.clone();
        let term = self.fsm.peer.get_index_term(compact_idx);
        let request =
            new_compact_log_request(region_id, peer, compact_idx, term, voter_replicated_idx);
        self.propose_raft_command_internal(
            request,
            Callback::None,
            DiskFullOpt::AllowedOnAlmostFull,
        );

        self.fsm.skip_gc_raft_log_ticks = 0;
        self.register_raft_gc_log_tick();
        PEER_GC_RAFT_LOG_COUNTER.inc_by(compact_idx - first_idx);
    }

    fn register_entry_cache_evict_tick(&mut self) {
        self.schedule_tick(PeerTick::EntryCacheEvict)
    }

    fn on_entry_cache_evict_tick(&mut self) {
        fail_point!("on_entry_cache_evict_tick", |_| {});
        if needs_evict_entry_cache(self.ctx.cfg.evict_cache_on_memory_ratio) {
            self.fsm.peer.mut_store().evict_entry_cache(true);
            if !self.fsm.peer.get_store().is_entry_cache_empty() {
                self.register_entry_cache_evict_tick();
            }
        }
    }

    fn register_check_long_uncommitted_tick(&mut self) {
        self.schedule_tick(PeerTick::CheckLongUncommitted)
    }

    fn on_check_long_uncommitted_tick(&mut self) {
        if !self.fsm.peer.is_leader() || self.fsm.hibernate_state.group_state() == GroupState::Idle
        {
            return;
        }
        fail_point!(
            "on_check_long_uncommitted_tick_1",
            self.fsm.peer.peer_id() == 1,
            |_| {}
        );
        self.fsm.peer.check_long_uncommitted_proposals(self.ctx);
        self.register_check_long_uncommitted_tick();
    }

    fn on_request_snapshot_tick(&mut self) {
        fail_point!("ignore request snapshot", |_| {
            self.schedule_tick(PeerTick::RequestSnapshot);
        });
        if !self.fsm.peer.wait_data {
            return;
        }
        if self.fsm.peer.is_leader()
            || self.fsm.peer.is_handling_snapshot()
            || self.fsm.peer.has_pending_snapshot()
        {
            self.schedule_tick(PeerTick::RequestSnapshot);
            return;
        }
        self.fsm.peer.request_index = self.fsm.peer.raft_group.raft.raft_log.last_index();
        let last_term = self.fsm.peer.get_index_term(self.fsm.peer.request_index);
        if last_term == self.fsm.peer.term() {
            self.fsm.peer.should_reject_msgappend = true;
            if let Err(e) = self.fsm.peer.raft_group.request_snapshot() {
                error!(
                    "failed to request snapshot";
                    "region_id" => self.fsm.region_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "err" => %e,
                );
            }
        } else {
            // If a leader change occurs after switch to non-witness, it should be
            // continue processing `MsgAppend` until `last_term == term`, then retry
            // to request snapshot.
            self.fsm.peer.should_reject_msgappend = false;
        }
        // Requesting a snapshot may fail, so register a periodic event as a defense
        // until succeeded.
        self.schedule_tick(PeerTick::RequestSnapshot);
    }

    fn on_request_voter_replicated_index(&mut self) {
        if !self.fsm.peer.is_witness() || !self.fsm.peer.has_pending_compact_cmd {
            return;
        }
        if self.fsm.peer.last_compacted_time.elapsed()
            > self.ctx.cfg.request_voter_replicated_index_interval.0
        {
            let mut msg = ExtraMessage::default();
            msg.set_type(ExtraMessageType::MsgVoterReplicatedIndexRequest);
            let leader_id = self.fsm.peer.leader_id();
            let leader = self.fsm.peer.get_peer_from_cache(leader_id);
            if let Some(leader) = leader {
                self.fsm
                    .peer
                    .send_extra_message(msg, &mut self.ctx.trans, &leader);
            }
        }
        self.register_pull_voter_replicated_index_tick();
    }

    fn register_check_leader_lease_tick(&mut self) {
        self.schedule_tick(PeerTick::CheckLeaderLease)
    }

    fn on_check_leader_lease_tick(&mut self) {
        if !self.fsm.peer.is_leader() || self.fsm.hibernate_state.group_state() == GroupState::Idle
        {
            return;
        }
        self.try_renew_leader_lease("tick");
        self.register_check_leader_lease_tick();
    }

    fn register_split_region_check_tick(&mut self) {
        self.schedule_tick(PeerTick::SplitRegionCheck);
    }

    #[inline]
    fn region_split_skip_max_count(&self) -> usize {
        fail_point!("region_split_skip_max_count", |_| { usize::max_value() });
        REGION_SPLIT_SKIP_MAX_COUNT
    }

    fn on_split_region_check_tick(&mut self) {
        if !self.fsm.peer.is_leader() {
            return;
        }

        // When restart, the may_skip_split_check will be false. The split check will
        // first check the region size, and then check whether the region should split.
        // This should work even if we change the region max size.
        // If peer says should update approximate size, update region size and check
        // whether the region should split.
        // We assume that `may_skip_split_check` is only set true after the split check
        // task is scheduled.
        if self
            .fsm
            .peer
            .split_check_trigger
            .should_skip(self.ctx.cfg.region_split_check_diff().0)
        {
            return;
        }

        fail_point!("on_split_region_check_tick", |_| {});
        self.register_split_region_check_tick();

        // To avoid frequent scan, we only add new scan tasks if all previous tasks
        // have finished.
        // TODO: check whether a gc progress has been started.
        if self.ctx.split_check_scheduler.is_busy() {
            return;
        }

        // To avoid run the check if it's splitting.
        if self.fsm.peer.is_splitting() {
            return;
        }

        // When Lightning or BR is importing data to TiKV, their ingest-request may fail
        // because of region-epoch not matched. So we hope TiKV do not check region size
        // and split region during importing.
        if self.ctx.importer.get_mode() == SwitchMode::Import
            || self.ctx.importer.region_in_import_mode(self.region())
        {
            return;
        }

        // bulk insert too fast may cause snapshot stale very soon, worst case it stale
        // before sending. so when snapshot is generating or sending, skip split check
        // at most 3 times. There is a trade off between region size and snapshot
        // success rate. Split check is triggered every 10 seconds. If a snapshot can't
        // be generated in 30 seconds, it might be just too large to be generated. Split
        // it into smaller size can help generation. check issue 330 for more
        // info.
        if self.fsm.peer.get_store().is_generating_snapshot()
            && self.fsm.skip_split_count < self.region_split_skip_max_count()
        {
            self.fsm.skip_split_count += 1;
            return;
        }
        self.fsm.skip_split_count = 0;
        let task = SplitCheckTask::split_check(
            self.region().clone(),
            true,
            CheckPolicy::Scan,
            self.gen_bucket_range_for_update(),
        );
        if let Err(e) = self.ctx.split_check_scheduler.schedule(task) {
            error!(
                "failed to schedule split check";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "err" => %e,
            );
            return;
        }
        self.fsm.peer.split_check_trigger.post_triggered();
    }

    fn on_prepare_split_region(
        &mut self,
        region_epoch: metapb::RegionEpoch,
        split_keys: Vec<Vec<u8>>,
        cb: Callback<EK::Snapshot>,
        source: &str,
        share_source_region_size: bool,
    ) {
        info!(
            "on split";
            "region_id" => self.fsm.region_id(),
            "peer_id" => self.fsm.peer_id(),
            "split_keys" => %KeysInfoFormatter(split_keys.iter()),
            "source" => source,
        );

        if !self.fsm.peer.is_leader() {
            // region on this store is no longer leader, skipped.
            info!(
                "not leader, skip proposing split";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
            );
            cb.invoke_with_response(new_error(Error::NotLeader(
                self.region_id(),
                self.fsm.peer.get_peer_from_cache(self.fsm.peer.leader_id()),
            )));
            return;
        }
        if let Err(e) = util::validate_split_region(
            self.fsm.region_id(),
            self.fsm.peer_id(),
            self.region(),
            &region_epoch,
            &split_keys,
        ) {
            info!(
                "invalid split request";
                "err" => ?e,
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "source" => %source
            );
            cb.invoke_with_response(new_error(e));
            return;
        }
        let region = self.fsm.peer.region();
        let task = PdTask::AskBatchSplit {
            region: region.clone(),
            split_keys,
            peer: self.fsm.peer.peer.clone(),
            right_derive: self.ctx.cfg.right_derive_when_split,
            share_source_region_size,
            callback: cb,
        };
        if let Err(ScheduleError::Stopped(t)) = self.ctx.pd_scheduler.schedule(task) {
            error!(
                "failed to notify pd to split: Stopped";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
            );
            match t {
                PdTask::AskBatchSplit { callback, .. } => {
                    callback.invoke_with_response(new_error(box_err!(
                        "{} failed to split: Stopped",
                        self.fsm.peer.tag
                    )));
                }
                _ => unreachable!(),
            }
        }
    }

    fn on_approximate_region_size(&mut self, size: Option<u64>, splitable: Option<bool>) {
        self.fsm
            .peer
            .split_check_trigger
            .on_approximate_region_size(size, splitable);
        self.register_split_region_check_tick();
        self.register_pd_heartbeat_tick();
        fail_point!("on_approximate_region_size");
    }

    fn on_approximate_region_keys(&mut self, keys: Option<u64>, splitable: Option<bool>) {
        self.fsm
            .peer
            .split_check_trigger
            .on_approximate_region_keys(keys, splitable);
        self.register_split_region_check_tick();
        self.register_pd_heartbeat_tick();
    }

    fn on_refresh_region_buckets(
        &mut self,
        region_epoch: RegionEpoch,
        buckets: Vec<Bucket>,
        bucket_ranges: Option<Vec<BucketRange>>,
        _cb: Callback<EK::Snapshot>,
    ) {
        #[cfg(any(test, feature = "testexport"))]
        let test_only_callback = |_callback, region_buckets| {
            if let Callback::Test { cb } = _callback {
                let peer_stat = PeerInternalStat {
                    buckets: region_buckets,
                    bucket_ranges: None,
                };
                cb(peer_stat);
            }
        };

        let region = self.fsm.peer.region();
        if util::is_epoch_stale(&region_epoch, region.get_region_epoch()) {
            info!(
                "receive a stale refresh region bucket message";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "epoch" => ?region_epoch,
                "current_epoch" => ?region.get_region_epoch(),
            );

            // test purpose
            #[cfg(any(test, feature = "testexport"))]
            {
                test_only_callback(
                    _cb,
                    self.fsm
                        .peer
                        .region_buckets_info()
                        .bucket_stat()
                        .unwrap()
                        .meta
                        .clone(),
                );
            }
            return;
        }

        let current_version = self.fsm.peer.region_buckets_info().version();
        let next_bucket_version = util::gen_bucket_version(self.fsm.peer.term(), current_version);
        let region = self.region().clone();
        let change_bucket_version = self
            .fsm
            .peer
            .region_buckets_info_mut()
            .on_refresh_region_buckets(
                &self.ctx.coprocessor_host.cfg,
                next_bucket_version,
                buckets,
                region_epoch,
                &region,
                bucket_ranges,
            );
        let region_buckets = self
            .fsm
            .peer
            .region_buckets_info()
            .bucket_stat()
            .unwrap()
            .clone();
        let buckets_count = region_buckets.meta.keys.len() - 1;
        if change_bucket_version {
            // TODO: we may need to make it debug once the coprocessor timeout is resolved.
            info!(
                "finished on_refresh_region_buckets";
                "region_id" => self.fsm.region_id(),
                "buckets_count" => buckets_count,
                "buckets_size" => ?region_buckets.meta.sizes,
            );
        } else {
            // it means the buckets key range not any change, so don't need to refresh.
            #[cfg(any(test, feature = "testexport"))]
            test_only_callback(_cb, region_buckets.meta);
            return;
        }
        self.ctx.coprocessor_host.on_region_changed(
            self.region(),
            RegionChangeEvent::UpdateBuckets(buckets_count),
            self.fsm.peer.get_role(),
        );
        let keys = region_buckets.meta.keys.clone();
        let version = region_buckets.meta.version;
        let mut store_meta = self.ctx.store_meta.lock().unwrap();
        if let Some(reader) = store_meta.readers.get_mut(&self.fsm.region_id()) {
            reader.update(ReadProgress::region_buckets(region_buckets.meta.clone()));
        }

        // Notify followers to refresh their buckets version
        if self.fsm.peer.is_leader() {
            let peers = self.region().get_peers().to_vec();
            for p in peers {
                if &p == self.peer() || p.is_witness {
                    continue;
                }
                let mut extra_msg = ExtraMessage::default();
                extra_msg.set_type(ExtraMessageType::MsgRefreshBuckets);
                let mut refresh_buckets = RefreshBuckets::new();
                refresh_buckets.set_version(version);
                refresh_buckets.set_keys(keys.clone().into());
                extra_msg.set_refresh_buckets(refresh_buckets);
                self.fsm
                    .peer
                    .send_extra_message(extra_msg, &mut self.ctx.trans, &p);
            }
        }
        // test purpose
        #[cfg(any(test, feature = "testexport"))]
        test_only_callback(_cb, region_buckets.meta);
    }

    pub fn on_msg_refresh_buckets(&mut self, msg: RaftMessage) {
        // leader should not receive this message
        if self.fsm.peer.is_leader() {
            return;
        }
        let version = msg.get_extra_msg().get_refresh_buckets().get_version();
        let keys = msg.get_extra_msg().get_refresh_buckets().get_keys();
        let region_epoch = msg.get_region_epoch().clone();

        let meta = BucketMeta {
            region_id: self.region_id(),
            version,
            region_epoch,
            keys: keys.to_vec(),
            sizes: vec![],
        };

        let mut store_meta = self.ctx.store_meta.lock().unwrap();
        if let Some(reader) = store_meta.readers.get_mut(&self.region_id()) {
            reader.update(ReadProgress::region_buckets(Arc::new(meta)));
        }
    }

    fn on_compaction_declined_bytes(&mut self, declined_bytes: u64) {
        self.fsm.peer.split_check_trigger.compaction_declined_bytes += declined_bytes;
        if self.fsm.peer.split_check_trigger.compaction_declined_bytes
            >= self.ctx.cfg.region_split_check_diff().0
        {
            UPDATE_REGION_SIZE_BY_COMPACTION_COUNTER.inc();
        }
        self.register_split_region_check_tick();
    }

    // generate bucket range list to run split-check (to further split buckets)
    fn gen_bucket_range_for_update(&self) -> Option<Vec<BucketRange>> {
        if !self.ctx.coprocessor_host.cfg.enable_region_bucket() {
            return None;
        }
        let region_bucket_max_size = self.ctx.coprocessor_host.cfg.region_bucket_size.0 * 2;
        self.fsm
            .peer
            .region_buckets_info()
            .gen_bucket_range_for_update(region_bucket_max_size)
    }

    fn on_schedule_half_split_region(
        &mut self,
        region_epoch: &metapb::RegionEpoch,
        start_key: Option<Vec<u8>>,
        end_key: Option<Vec<u8>>,
        policy: CheckPolicy,
        source: &str,
        _cb: Callback<EK::Snapshot>,
    ) {
        let is_key_range = start_key.is_some() && end_key.is_some();
        info!(
            "on half split";
            "region_id" => self.fsm.region_id(),
            "peer_id" => self.fsm.peer_id(),
            "is_key_range" => is_key_range,
            "policy" => ?policy,
            "source" => source,
        );
        if !self.fsm.peer.is_leader() {
            // region on this store is no longer leader, skipped.
            warn!(
                "not leader, skip";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "is_key_range" => is_key_range,
            );
            return;
        }

        let region = self.fsm.peer.region();
        if util::is_epoch_stale(region_epoch, region.get_region_epoch()) {
            warn!(
                "receive a stale halfsplit message";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "is_key_range" => is_key_range,
            );
            return;
        }

        // Do not check the bucket ranges if we want to split the region with a given
        // key range, this is to avoid compatibility issues.
        let split_check_bucket_ranges = if !is_key_range {
            self.gen_bucket_range_for_update()
        } else {
            None
        };
        #[cfg(any(test, feature = "testexport"))]
        {
            if let Callback::Test { cb } = _cb {
                let peer_stat = PeerInternalStat {
                    buckets: Arc::default(),
                    bucket_ranges: split_check_bucket_ranges.clone(),
                };
                cb(peer_stat);
            }
        }

        // only check the suspect buckets, not split region.
        if source == "bucket" {
            return;
        }
        let task = SplitCheckTask::split_check_key_range(
            region.clone(),
            start_key,
            end_key,
            false,
            policy,
            split_check_bucket_ranges,
        );
        if let Err(e) = self.ctx.split_check_scheduler.schedule(task) {
            error!(
                "failed to schedule split check";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "is_key_range" => is_key_range,
                "err" => %e,
            );
        }
    }

    fn on_pd_heartbeat_tick(&mut self) {
        if !self.ctx.cfg.hibernate_regions {
            self.register_pd_heartbeat_tick();
        }
        self.fsm.peer.check_peers();

        if !self.fsm.peer.is_leader() {
            return;
        }
        self.fsm.peer.heartbeat_pd(self.ctx);
        if self.ctx.cfg.hibernate_regions && self.fsm.peer.replication_mode_need_catch_up() {
            self.register_pd_heartbeat_tick();
        }
    }

    fn register_pd_heartbeat_tick(&mut self) {
        self.schedule_tick(PeerTick::PdHeartbeat)
    }

    fn register_check_peers_availability_tick(&mut self) {
        fail_point!("ignore schedule check peers availability tick", |_| {});
        self.schedule_tick(PeerTick::CheckPeersAvailability)
    }

    fn on_check_peers_availability(&mut self) {
        let mut invalid_peers: Vec<u64> = Vec::new();
        for peer_id in self.fsm.peer.wait_data_peers.iter() {
            match self.fsm.peer.get_peer_from_cache(*peer_id) {
                Some(peer) => {
                    let mut msg = ExtraMessage::default();
                    msg.set_type(ExtraMessageType::MsgAvailabilityRequest);
                    self.fsm
                        .peer
                        .send_extra_message(msg, &mut self.ctx.trans, &peer);
                    debug!(
                        "check peer availability";
                        "target_peer_id" => *peer_id,
                    );
                }
                None => invalid_peers.push(*peer_id),
            }
        }
        // For some reasons, the peer corresponding to the previously saved peer_id
        // no longer exists. In order to avoid passing invalid information to pd when
        // reporting pending peers and affecting pd scheduling, remove it from the
        // `wait_data_peers`.
        self.fsm
            .peer
            .wait_data_peers
            .retain(|peer_id| !invalid_peers.contains(peer_id));
    }

    fn register_pull_voter_replicated_index_tick(&mut self) {
        self.schedule_tick(PeerTick::RequestVoterReplicatedIndex);
    }

    fn on_check_peer_stale_state_tick(&mut self) {
        if self.fsm.peer.pending_remove {
            return;
        }

        self.register_check_peer_stale_state_tick();

        if self.fsm.peer.is_handling_snapshot() || self.fsm.peer.has_pending_snapshot() {
            return;
        }

        if let Some(state) = &mut self.fsm.peer.unsafe_recovery_state {
            let unsafe_recovery_state_timeout_failpoint = || -> bool {
                fail_point!("unsafe_recovery_state_timeout", |_| true);
                false
            };
            // Clean up the unsafe recovery state after a timeout, since the PD recovery
            // process may have been aborted for some reasons.
            if unsafe_recovery_state_timeout_failpoint()
                || state.check_timeout(UNSAFE_RECOVERY_STATE_TIMEOUT)
            {
                info!("timeout, abort unsafe recovery"; "state" => ?state);
                state.abort();
                self.fsm.peer.unsafe_recovery_state = None;
            }
        }

        if let Some(ForceLeaderState::ForceLeader { time, .. }) = self.fsm.peer.force_leader {
            // Clean up the force leader state after a timeout, since the PD recovery
            // process may have been aborted for some reasons.
            if time.saturating_elapsed() > UNSAFE_RECOVERY_STATE_TIMEOUT {
                self.on_exit_force_leader(true);
            }
        }

        if self.ctx.cfg.hibernate_regions {
            let group_state = self.fsm.hibernate_state.group_state();
            if group_state == GroupState::Idle {
                self.fsm.peer.ping();
                if !self.fsm.peer.is_leader() {
                    // The peer will keep tick some times after its state becomes
                    // GroupState::Idle, in which case its state shouldn't be changed.
                    if !self.fsm.tick_registry[PeerTick::Raft as usize] {
                        // If leader is able to receive message but can't send out any,
                        // follower should be able to start an election.
                        self.fsm.reset_hibernate_state(GroupState::PreChaos);
                    }
                } else {
                    self.fsm.has_ready = true;
                    // Schedule a pd heartbeat to discover down and pending peer when
                    // hibernate_regions is enabled.
                    self.register_pd_heartbeat_tick();
                }
            } else if group_state == GroupState::PreChaos {
                self.fsm.reset_hibernate_state(GroupState::Chaos);
            } else if group_state == GroupState::Chaos {
                // Register tick if it's not yet. Only when it fails to receive ping from leader
                // after two stale check can a follower actually tick.
                self.register_raft_base_tick();
            }
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
        // and it would check with pd to confirm whether it's still a member of the
        // cluster. If not, it destroys itself as a stale peer which is removed out
        // already.
        let state = self.fsm.peer.check_stale_state(self.ctx);
        fail_point!("peer_check_stale_state", state != StaleState::Valid, |_| {});
        match state {
            StaleState::Valid => (),
            StaleState::LeaderMissing | StaleState::MaybeLeaderMissing => {
                if state == StaleState::LeaderMissing {
                    warn!(
                        "leader missing longer than abnormal_leader_missing_duration";
                        "region_id" => self.fsm.region_id(),
                        "peer_id" => self.fsm.peer_id(),
                        "expect" => %self.ctx.cfg.abnormal_leader_missing_duration,
                    );
                    self.ctx
                        .raft_metrics
                        .leader_missing
                        .lock()
                        .unwrap()
                        .insert(self.region_id());
                }

                // It's very likely that this is a stale peer. To prevent
                // resolved ts from being blocked for too long, we check stale
                // peer eagerly.
                self.fsm.peer.bcast_check_stale_peer_message(self.ctx);
            }
            StaleState::ToValidate => {
                // for peer B in case 1 above
                warn!(
                    "leader missing longer than max_leader_missing_duration. \
                     To check with pd and other peers whether it's still valid";
                    "region_id" => self.fsm.region_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "expect" => %self.ctx.cfg.max_leader_missing_duration,
                );

                self.fsm.peer.bcast_check_stale_peer_message(self.ctx);

                let task = PdTask::ValidatePeer {
                    peer: self.fsm.peer.peer.clone(),
                    region: self.fsm.peer.region().clone(),
                };
                if let Err(e) = self.ctx.pd_scheduler.schedule(task) {
                    error!(
                        "failed to notify pd";
                        "region_id" => self.fsm.region_id(),
                        "peer_id" => self.fsm.peer_id(),
                        "err" => %e,
                    )
                }
            }
        }
    }

    fn register_check_peer_stale_state_tick(&mut self) {
        self.schedule_tick(PeerTick::CheckPeerStaleState)
    }

    fn register_reactivate_memory_lock_tick(&mut self) {
        self.schedule_tick(PeerTick::ReactivateMemoryLock)
    }

    fn on_reactivate_memory_lock_tick(&mut self) {
        let mut pessimistic_locks = self.fsm.peer.txn_ext.pessimistic_locks.write();

        // If it is not leader, we needn't reactivate by tick. In-memory pessimistic
        // lock will be enabled when this region becomes leader again.
        // And this tick is currently only used for the leader transfer failure case.
        if !self.fsm.peer.is_leader() || pessimistic_locks.status != LocksStatus::TransferringLeader
        {
            return;
        }

        self.fsm.reactivate_memory_lock_ticks += 1;
        let transferring_leader = self.fsm.peer.raft_group.raft.lead_transferee.is_some();
        // `lead_transferee` is not set immediately after the lock status changes. So,
        // we need the tick count condition to avoid reactivating too early.
        if !transferring_leader
            && self.fsm.reactivate_memory_lock_ticks
                >= self.ctx.cfg.reactive_memory_lock_timeout_tick
        {
            pessimistic_locks.status = LocksStatus::Normal;
            self.fsm.reactivate_memory_lock_ticks = 0;
        } else {
            drop(pessimistic_locks);
            self.register_reactivate_memory_lock_tick();
        }
    }

    fn on_report_region_buckets_tick(&mut self) {
        if !self.fsm.peer.is_leader()
            || self.fsm.peer.region_buckets_info().bucket_stat().is_none()
            || self.fsm.hibernate_state.group_state() == GroupState::Idle
        {
            return;
        }

        let region_id = self.region_id();
        let peer_id = self.fsm.peer_id();
        let region_buckets = self.fsm.peer.region_buckets_info_mut().report_bucket_stat();
        if let Err(e) = self
            .ctx
            .pd_scheduler
            .schedule(PdTask::ReportBuckets(region_buckets))
        {
            error!(
                "failed to report region buckets";
                "region_id" => region_id,
                "peer_id" => peer_id,
                "err" => ?e,
            );
        }

        self.register_report_region_buckets_tick();
    }

    fn register_report_region_buckets_tick(&mut self) {
        self.schedule_tick(PeerTick::ReportBuckets)
    }

    /// Check whether the peer should send a request to fetch the committed
    /// index from the leader.
    fn try_to_fetch_committed_index(&mut self) {
        // Already completed, skip.
        if !self.fsm.peer.needs_update_last_leader_committed_idx() || self.fsm.peer.is_leader() {
            return;
        }
        // Construct a MsgReadIndex message and send it to the leader to
        // fetch the latest committed index of this raft group.
        let leader_id = self.fsm.peer.leader_id();
        if leader_id == raft::INVALID_ID {
            // The leader is unknown, so we can't fetch the committed index.
            return;
        }
        let rctx = ReadIndexContext {
            id: uuid::Uuid::new_v4(),
            request: None,
            locked: None,
        };
        self.fsm.peer.raft_group.read_index(rctx.to_bytes());
        debug!(
            "try to fetch committed index from leader";
            "region_id" => self.region_id(),
            "peer_id" => self.fsm.peer_id()
        );
    }

    /// Check whether the peer is pending on applying raft logs.
    ///
    /// If busy, the peer will be recorded, until the pending logs are
    /// applied. And after it completes applying, it will be removed from
    /// the recording list.
    fn on_check_peer_complete_apply_logs(&mut self) {
        // Already completed, skip.
        if self.fsm.peer.busy_on_apply.is_none() {
            return;
        }

        let peer_id = self.fsm.peer.peer_id();
        // No need to check the applying state if the peer is leader.
        if self.fsm.peer.is_leader() {
            self.fsm.peer.busy_on_apply = None;
            // Clear it from recoding list and update the counter, to avoid
            // missing it when the peer is changed to leader.
            let mut meta = self.ctx.store_meta.lock().unwrap();
            meta.busy_apply_peers.remove(&peer_id);
            if let Some(count) = meta.completed_apply_peers_count.as_mut() {
                *count += 1;
            }
            return;
        }

        let applied_idx = self.fsm.peer.get_store().applied_index();
        let mut last_idx = self.fsm.peer.get_store().last_index();
        // If the peer is newly added or created, no need to check the apply status.
        if last_idx <= RAFT_INIT_LOG_INDEX {
            self.fsm.peer.busy_on_apply = None;
            // And it should be recorded in the `completed_apply_peers_count`.
            let mut meta = self.ctx.store_meta.lock().unwrap();
            meta.busy_apply_peers.remove(&peer_id);
            if let Some(count) = meta.completed_apply_peers_count.as_mut() {
                *count += 1;
            }
            debug!(
                "no need to check initialized peer";
                "last_commit_idx" => last_idx,
                "last_applied_idx" => applied_idx,
                "region_id" => self.fsm.region_id(),
                "peer_id" => peer_id,
            );
            return;
        }
        assert!(self.fsm.peer.busy_on_apply.is_some());

        // This peer is restarted and the last leader commit index is not set, so
        // it use `u64::MAX` as the last commit index to make it wait for the update
        // of the `last_leader_committed_idx` until the `last_leader_committed_idx` has
        // been updated.
        last_idx = self.fsm.peer.last_leader_committed_idx.unwrap_or(u64::MAX);

        // If the peer has large unapplied logs, this peer should be recorded until
        // the lag is less than the given threshold.
        if last_idx >= applied_idx + self.ctx.cfg.leader_transfer_max_log_lag {
            if !self.fsm.peer.busy_on_apply.unwrap() {
                let mut meta = self.ctx.store_meta.lock().unwrap();
                meta.busy_apply_peers.insert(peer_id);
            }
            self.fsm.peer.busy_on_apply = Some(true);
            debug!(
                "peer is busy on applying logs";
                "last_commit_idx" => last_idx,
                "last_applied_idx" => applied_idx,
                "region_id" => self.fsm.region_id(),
                "peer_id" => peer_id,
            );
        } else {
            // Already finish apply, remove it from recording list.
            {
                let mut meta = self.ctx.store_meta.lock().unwrap();
                meta.busy_apply_peers.remove(&peer_id);
                if let Some(count) = meta.completed_apply_peers_count.as_mut() {
                    *count += 1;
                }
            }
            debug!(
                "peer completes applying logs";
                "last_commit_idx" => last_idx,
                "last_applied_idx" => applied_idx,
                "region_id" => self.fsm.region_id(),
                "peer_id" => peer_id,
            );
            self.fsm.peer.busy_on_apply = None;
        }
    }
}

impl<'a, EK, ER, T: Transport> PeerFsmDelegate<'a, EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn on_ready_compute_hash(
        &mut self,
        region: metapb::Region,
        index: u64,
        context: Vec<u8>,
        snap: EK::Snapshot,
    ) {
        self.fsm.peer.consistency_state.last_check_time = Instant::now();
        let task = ConsistencyCheckTask::compute_hash(region, index, context, snap);
        info!(
            "schedule compute hash task";
            "region_id" => self.fsm.region_id(),
            "peer_id" => self.fsm.peer_id(),
            "task" => %task,
        );
        if let Err(e) = self.ctx.consistency_check_scheduler.schedule(task) {
            error!(
                "schedule failed";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "err" => %e,
            );
        }
    }

    fn on_ready_verify_hash(
        &mut self,
        expected_index: u64,
        context: Vec<u8>,
        expected_hash: Vec<u8>,
    ) {
        self.verify_and_store_hash(expected_index, context, expected_hash);
    }

    fn on_hash_computed(&mut self, index: u64, context: Vec<u8>, hash: Vec<u8>) {
        if !self.verify_and_store_hash(index, context, hash) {
            return;
        }

        let req = new_verify_hash_request(
            self.region_id(),
            self.fsm.peer.peer.clone(),
            &self.fsm.peer.consistency_state,
        );
        self.propose_raft_command_internal(req, Callback::None, DiskFullOpt::NotAllowedOnFull);
    }

    fn on_ingest_sst_result(&mut self, ssts: Vec<SstMetaInfo>) {
        let mut size = 0;
        let mut keys = 0;
        for sst in &ssts {
            size += sst.total_bytes;
            keys += sst.total_kvs;
        }
        self.fsm
            .peer
            .split_check_trigger
            .on_ingest_sst_result(size, keys);

        if let Some(buckets) = &mut self.fsm.peer.region_buckets_info_mut().bucket_stat_mut() {
            buckets.ingest_sst(keys, size);
        }
        if self.fsm.peer.is_leader() {
            self.on_pd_heartbeat_tick();
            self.register_split_region_check_tick();
        }
    }

    fn on_transfer_leader(&mut self, term: u64) {
        // If the term has changed between proposing and executing the TransferLeader
        // request, ignore it because this request may be stale.
        if term != self.fsm.peer.term() {
            return;
        }
        self.fsm.peer.ack_transfer_leader_msg(true);
        self.fsm.has_ready = true;
    }

    fn on_set_flashback_state(&mut self, region: metapb::Region) {
        // Update the region meta.
        self.update_region((|| {
            #[cfg(feature = "failpoints")]
            fail_point!("keep_peer_fsm_flashback_state_false", |_| {
                let mut region = region.clone();
                region.is_in_flashback = false;
                region
            });
            region
        })());
        // Let the leader lease to None to ensure that local reads are not executed.
        self.fsm.peer.leader_lease_mut().expire_remote_lease();
        let mut pessimistic_locks = self.fsm.peer.txn_ext.pessimistic_locks.write();
        pessimistic_locks.status = if self.region().is_in_flashback {
            // To prevent the insertion of any new pessimistic locks, set the lock status
            // to `LocksStatus::IsInFlashback` and clear all the existing locks.
            pessimistic_locks.clear();
            LocksStatus::IsInFlashback
        } else if self.fsm.peer.is_leader() {
            // If the region is not in flashback, the leader can continue to insert
            // pessimistic locks.
            LocksStatus::Normal
        } else {
            // If the region is not in flashback and the peer is not the leader, it
            // cannot insert pessimistic locks.
            LocksStatus::NotLeader
        }
    }

    fn on_ready_batch_switch_witness(&mut self, sw: SwitchWitness) {
        {
            let mut meta = self.ctx.store_meta.lock().unwrap();
            meta.set_region(
                &self.ctx.coprocessor_host,
                sw.region,
                &mut self.fsm.peer,
                RegionChangeReason::SwitchWitness,
            );
        }
        for s in sw.switches {
            let (peer_id, is_witness) = (s.get_peer_id(), s.get_is_witness());
            if self.fsm.peer_id() == peer_id {
                if is_witness {
                    self.fsm.peer.raft_group.set_priority(-1);
                    if !self.fsm.peer.is_leader() {
                        let _ = self.fsm.peer.get_store().clear_data();
                    } else {
                        // Avoid calling `clear_data` as the region worker may be scanning snapshot,
                        // to avoid problems (although no problems were found by testing).
                        self.fsm.peer.delay_clean_data = true;
                    }
                } else {
                    self.fsm
                        .peer
                        .update_read_progress(self.ctx, ReadProgress::WaitData(true));
                    self.fsm.peer.wait_data = true;
                    self.on_request_snapshot_tick();
                }
                self.fsm.peer.peer.is_witness = is_witness;
                continue;
            }
            if !is_witness && !self.fsm.peer.wait_data_peers.contains(&peer_id) {
                self.fsm.peer.wait_data_peers.push(peer_id);
            }
        }
        if self.fsm.peer.is_leader() {
            info!(
               "notify pd with change peer region";
               "region_id" => self.fsm.region_id(),
               "peer_id" => self.fsm.peer_id(),
               "region" => ?self.fsm.peer.region(),
            );
            self.fsm.peer.heartbeat_pd(self.ctx);
            if !self.fsm.peer.wait_data_peers.is_empty() {
                self.register_check_peers_availability_tick();
            }
        }
    }

    /// Verify and store the hash to state. return true means the hash has been
    /// stored successfully.
    // TODO: Consider context in the function.
    fn verify_and_store_hash(
        &mut self,
        expected_index: u64,
        _context: Vec<u8>,
        expected_hash: Vec<u8>,
    ) -> bool {
        if expected_index < self.fsm.peer.consistency_state.index {
            REGION_HASH_COUNTER.verify.miss.inc();
            warn!(
                "has scheduled a new hash, skip.";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "index" => self.fsm.peer.consistency_state.index,
                "expected_index" => expected_index,
            );
            return false;
        }
        if self.fsm.peer.consistency_state.index == expected_index {
            if self.fsm.peer.consistency_state.hash.is_empty() {
                warn!(
                    "duplicated consistency check detected, skip.";
                    "region_id" => self.fsm.region_id(),
                    "peer_id" => self.fsm.peer_id(),
                );
                return false;
            }
            if self.fsm.peer.consistency_state.hash != expected_hash {
                panic!(
                    "{} hash at {} not correct, want \"{}\", got \"{}\"!!!",
                    self.fsm.peer.tag,
                    self.fsm.peer.consistency_state.index,
                    escape(&expected_hash),
                    escape(&self.fsm.peer.consistency_state.hash)
                );
            }
            info!(
                "consistency check pass.";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "index" => self.fsm.peer.consistency_state.index
            );
            REGION_HASH_COUNTER.verify.matched.inc();
            self.fsm.peer.consistency_state.hash = vec![];
            return false;
        }
        if self.fsm.peer.consistency_state.index != INVALID_INDEX
            && !self.fsm.peer.consistency_state.hash.is_empty()
        {
            // Maybe computing is too slow or computed result is dropped due to channel
            // full. If computing is too slow, miss count will be increased
            // twice.
            REGION_HASH_COUNTER.verify.miss.inc();
            warn!(
                "hash belongs to wrong index, skip.";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "index" => self.fsm.peer.consistency_state.index,
                "expected_index" => expected_index,
            );
        }

        info!(
            "save hash for consistency check later.";
            "region_id" => self.fsm.region_id(),
            "peer_id" => self.fsm.peer_id(),
            "index" => expected_index,
        );
        self.fsm.peer.consistency_state.index = expected_index;
        self.fsm.peer.consistency_state.hash = expected_hash;
        true
    }
}

/// Checks merge target, returns whether the source peer should be destroyed and
/// whether the source peer is merged to this target peer.
///
/// It returns (`can_destroy`, `merge_to_this_peer`).
///
/// `can_destroy` is true when there is a network isolation which leads to a
/// follower of a merge target Region's log falls behind and then receive a
/// snapshot with epoch version after merge.
///
/// `merge_to_this_peer` is true when `can_destroy` is true and the source peer
/// is merged to this target peer.
pub fn maybe_destroy_source(
    meta: &StoreMeta,
    target_region_id: u64,
    target_peer_id: u64,
    source_region_id: u64,
    region_epoch: RegionEpoch,
) -> (bool, bool) {
    if let Some(merge_targets) = meta.pending_merge_targets.get(&target_region_id) {
        if let Some(target_region) = merge_targets.get(&source_region_id) {
            info!(
                "[region {}] checking source {} epoch: {:?}, merge target epoch: {:?}",
                target_region_id,
                source_region_id,
                region_epoch,
                target_region.get_region_epoch(),
            );
            // The target peer will move on, namely, it will apply a snapshot generated
            // after merge, so destroy source peer.
            if region_epoch.get_version() > target_region.get_region_epoch().get_version() {
                return (
                    true,
                    target_peer_id
                        == find_peer(target_region, meta.store_id.unwrap())
                            .unwrap()
                            .get_id(),
                );
            }
            // Wait till the target peer has caught up logs and source peer will be
            // destroyed at that time.
            return (false, false);
        }
    }
    (false, false)
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

fn new_verify_hash_request(
    region_id: u64,
    peer: metapb::Peer,
    state: &ConsistencyState,
) -> RaftCmdRequest {
    let mut request = new_admin_request(region_id, peer);

    let mut admin = AdminRequest::default();
    admin.set_cmd_type(AdminCmdType::VerifyHash);
    admin.mut_verify_hash().set_index(state.index);
    admin.mut_verify_hash().set_context(state.context.clone());
    admin.mut_verify_hash().set_hash(state.hash.clone());
    request.set_admin_request(admin);
    request
}

fn new_compact_log_request(
    region_id: u64,
    peer: metapb::Peer,
    compact_index: u64,
    compact_term: u64,
    voter_replicated_index: u64,
) -> RaftCmdRequest {
    let mut request = new_admin_request(region_id, peer);

    let mut admin = AdminRequest::default();
    admin.set_cmd_type(AdminCmdType::CompactLog);
    admin.mut_compact_log().set_compact_index(compact_index);
    admin.mut_compact_log().set_compact_term(compact_term);
    admin
        .mut_compact_log()
        .set_voter_replicated_index(voter_replicated_index);
    request.set_admin_request(admin);
    request
}

impl<'a, EK, ER, T: Transport> PeerFsmDelegate<'a, EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
{
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
                Err(box_err!("{} invalid status command!", self.fsm.peer.tag))
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

mod memtrace {
    use memory_trace_macros::MemoryTraceHelper;

    use super::*;

    /// Heap size for Raft internal `ReadOnly`.
    #[derive(MemoryTraceHelper, Default, Debug)]
    pub struct PeerMemoryTrace {
        /// `ReadOnly` memory usage in Raft groups.
        pub read_only: usize,
        /// `Progress` memory usage in Raft groups.
        pub progress: usize,
        /// `Proposal` memory usage for peers.
        pub proposals: usize,
        pub rest: usize,
    }

    impl<EK, ER> PeerFsm<EK, ER>
    where
        EK: KvEngine,
        ER: RaftEngine,
    {
        pub fn raft_read_size(&self) -> usize {
            let msg_size = mem::size_of::<raft::eraftpb::Message>();
            let raft = &self.peer.raft_group.raft;

            // We use Uuid for read request.
            let mut size = raft.read_states.len() * (mem::size_of::<ReadState>() + 16);
            size += raft.read_only.read_index_queue.len() * 16;

            // Every requests have at least header, which should be at least 8 bytes.
            size + raft.read_only.pending_read_index.len() * (16 + msg_size)
        }

        pub fn raft_progress_size(&self) -> usize {
            let peer_cnt = self.peer.region().get_peers().len();
            mem::size_of::<Progress>() * peer_cnt * 6 / 5
                + self.peer.raft_group.raft.inflight_buffers_size()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    use engine_test::kv::KvTestEngine;
    use kvproto::raft_cmdpb::{
        AdminRequest, CmdType, PutRequest, RaftCmdRequest, RaftCmdResponse, Request, Response,
        StatusRequest,
    };
    use protobuf::Message;
    use tikv_util::config::ReadableSize;

    use super::*;
    use crate::store::{
        local_metrics::RaftMetrics,
        msg::{Callback, ExtCallback, RaftCommand},
    };

    #[test]
    fn test_batch_raft_cmd_request_builder() {
        let mut cfg = Config::default();
        cfg.raft_entry_max_size = ReadableSize(1000);
        let mut builder = BatchRaftCmdRequestBuilder::<KvTestEngine>::new();
        let mut q = Request::default();
        let mut metric = RaftMetrics::new(true);

        let mut req = RaftCmdRequest::default();
        req.set_admin_request(AdminRequest::default());
        assert!(!builder.can_batch(&cfg, &req, 0));

        let mut req = RaftCmdRequest::default();
        req.set_status_request(StatusRequest::default());
        assert!(!builder.can_batch(&cfg, &req, 0));

        let mut req = RaftCmdRequest::default();
        let mut put = PutRequest::default();
        put.set_key(b"aaaa".to_vec());
        put.set_value(b"bbbb".to_vec());
        q.set_cmd_type(CmdType::Put);
        q.set_put(put);
        req.mut_requests().push(q.clone());
        let _ = q.take_put();
        let req_size = req.compute_size();
        assert!(builder.can_batch(&cfg, &req, req_size));

        let mut req = RaftCmdRequest::default();
        q.set_cmd_type(CmdType::Snap);
        req.mut_requests().push(q.clone());
        let mut put = PutRequest::default();
        put.set_key(b"aaaa".to_vec());
        put.set_value(b"bbbb".to_vec());
        q.set_cmd_type(CmdType::Put);
        q.set_put(put);
        req.mut_requests().push(q.clone());
        let req_size = req.compute_size();
        assert!(!builder.can_batch(&cfg, &req, req_size));

        let mut req = RaftCmdRequest::default();
        let mut put = PutRequest::default();
        put.set_key(b"aaaa".to_vec());
        put.set_value(vec![8_u8; 2000]);
        q.set_cmd_type(CmdType::Put);
        q.set_put(put);
        req.mut_requests().push(q.clone());
        let req_size = req.compute_size();
        assert!(!builder.can_batch(&cfg, &req, req_size));

        // Check batch callback
        let mut req = RaftCmdRequest::default();
        let mut put = PutRequest::default();
        put.set_key(b"aaaa".to_vec());
        put.set_value(vec![8_u8; 20]);
        q.set_cmd_type(CmdType::Put);
        q.set_put(put);
        req.mut_requests().push(q);
        let mut cbs_flags = vec![];
        let mut proposed_cbs_flags = vec![];
        let mut committed_cbs_flags = vec![];
        let mut response = RaftCmdResponse::default();
        for i in 0..10 {
            let flag = Arc::new(AtomicBool::new(false));
            cbs_flags.push(flag.clone());
            // Some commands don't have proposed_cb.
            let proposed_cb: Option<ExtCallback> = if i % 2 == 0 {
                let proposed_flag = Arc::new(AtomicBool::new(false));
                proposed_cbs_flags.push(proposed_flag.clone());
                Some(Box::new(move || {
                    proposed_flag.store(true, Ordering::Release);
                }))
            } else {
                None
            };
            let committed_cb: Option<ExtCallback> = if i % 3 == 0 {
                let committed_flag = Arc::new(AtomicBool::new(false));
                committed_cbs_flags.push(committed_flag.clone());
                Some(Box::new(move || {
                    committed_flag.store(true, Ordering::Release);
                }))
            } else {
                None
            };
            let cb = Callback::write_ext(
                Box::new(move |_resp| {
                    flag.store(true, Ordering::Release);
                }),
                proposed_cb,
                committed_cb,
            );
            response.mut_responses().push(Response::default());
            let cmd = RaftCommand::new(req.clone(), cb);
            builder.add(cmd, 100);
        }
        let (request, mut callback) = builder.build(&mut metric).unwrap();
        callback.invoke_proposed();
        for flag in proposed_cbs_flags {
            assert!(flag.load(Ordering::Acquire));
        }
        callback.invoke_committed();
        for flag in committed_cbs_flags {
            assert!(flag.load(Ordering::Acquire));
        }
        assert_eq!(10, request.get_requests().len());
        callback.invoke_with_response(response);
        for flag in cbs_flags {
            assert!(flag.load(Ordering::Acquire));
        }
    }
}
