// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::borrow::Cow;
use std::collections::Bound::{Excluded, Included, Unbounded};
use std::collections::VecDeque;
use std::iter::Iterator;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Instant;
use std::{cmp, u64};

use batch_system::{BasicMailbox, Fsm};
use collections::HashMap;
use engine_traits::CF_RAFT;
use engine_traits::{Engines, KvEngine, RaftEngine, WriteBatchExt};
use error_code::ErrorCodeExt;
use kvproto::errorpb;
use kvproto::import_sstpb::SstMeta;
use kvproto::metapb::{self, Region, RegionEpoch};
use kvproto::pdpb::CheckPolicy;
use kvproto::raft_cmdpb::{
    AdminCmdType, AdminRequest, CmdType, RaftCmdRequest, RaftCmdResponse, Request, StatusCmdType,
    StatusResponse,
};
use kvproto::raft_serverpb::{
    ExtraMessage, ExtraMessageType, MergeState, PeerState, RaftApplyState, RaftMessage,
    RaftSnapshotData, RaftTruncatedState, RegionLocalState,
};
use kvproto::replication_modepb::{DrAutoSyncState, ReplicationMode};
use protobuf::Message;
use raft::eraftpb::{ConfChangeType, MessageType};
use raft::{self, SnapshotStatus, INVALID_INDEX, NO_LIMIT};
use raft::{Ready, StateRole};
use tikv_util::mpsc::{self, LooseBoundedSender, Receiver};
use tikv_util::time::duration_to_sec;
use tikv_util::worker::{Scheduler, Stopped};
use tikv_util::{escape, is_zero_duration, Either};

use crate::coprocessor::RegionChangeEvent;
use crate::store::cmd_resp::{bind_term, new_error};
use crate::store::fsm::store::{PollContext, StoreMeta};
use crate::store::fsm::{
    apply, ApplyMetrics, ApplyTask, ApplyTaskRes, CatchUpLogs, ChangeCmd, ChangePeer, ExecResult,
};
use crate::store::hibernate_state::{GroupState, HibernateState};
use crate::store::local_metrics::RaftProposeMetrics;
use crate::store::metrics::*;
use crate::store::msg::{Callback, ExtCallback};
use crate::store::peer::{ConsistencyState, Peer, StaleState};
use crate::store::peer_storage::{ApplySnapResult, InvokeContext};
use crate::store::transport::Transport;
use crate::store::util::{is_learner, KeysInfoFormatter};
use crate::store::worker::{
    ConsistencyCheckTask, RaftlogGcTask, ReadDelegate, RegionTask, SplitCheckTask,
};
use crate::store::PdTask;
use crate::store::{
    util, AbstractPeer, CasualMessage, Config, MergeResultKind, PeerMsg, PeerTicks, RaftCommand,
    SignificantMsg, SnapKey, StoreMsg,
};
use crate::{Error, Result};
use keys::{self, enc_end_key, enc_start_key};

const REGION_SPLIT_SKIP_MAX_COUNT: usize = 3;

pub struct DestroyPeerJob {
    pub initialized: bool,
    pub region_id: u64,
    pub peer: metapb::Peer,
}

pub struct CollectedReady {
    /// The offset of source peer in the batch system.
    pub batch_offset: usize,
    pub ctx: InvokeContext,
    pub ready: Ready,
}

impl CollectedReady {
    pub fn new(ctx: InvokeContext, ready: Ready) -> CollectedReady {
        CollectedReady {
            batch_offset: 0,
            ctx,
            ready,
        }
    }
}

pub struct PeerFsm<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub peer: Peer<EK, ER>,
    /// A registry for all scheduled ticks. This can avoid scheduling ticks twice accidentally.
    tick_registry: PeerTicks,
    /// Ticks for speed up campaign in chaos state.
    ///
    /// Followers will keep ticking in Idle mode to measure how many ticks have been skipped.
    /// Once it becomes chaos, those skipped ticks will be ticked so that it can campaign
    /// quickly instead of waiting an election timeout.
    ///
    /// This will be reset to 0 once it receives any messages from leader.
    missing_ticks: usize,
    hibernate_state: HibernateState,
    stopped: bool,
    has_ready: bool,
    mailbox: Option<BasicMailbox<PeerFsm<EK, ER>>>,
    pub receiver: Receiver<PeerMsg<EK>>,
    /// when snapshot is generating or sending, skip split check at most REGION_SPLIT_SKIT_MAX_COUNT times.
    skip_split_count: usize,
    /// Sometimes applied raft logs won't be compacted in time, because less compact means less
    /// sync-log in apply threads. Stale logs will be deleted if the skip time reaches this
    /// `skip_gc_raft_log_ticks`.
    skip_gc_raft_log_ticks: usize,

    // Batch raft command which has the same header into an entry
    batch_req_builder: BatchRaftCmdRequestBuilder<EK>,
}

pub struct BatchRaftCmdRequestBuilder<E>
where
    E: KvEngine,
{
    raft_entry_max_size: f64,
    batch_req_size: u32,
    request: Option<RaftCmdRequest>,
    callbacks: Vec<(Callback<E::Snapshot>, usize)>,
}

impl<EK, ER> Drop for PeerFsm<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn drop(&mut self) {
        self.peer.stop();
        while let Ok(msg) = self.receiver.try_recv() {
            let callback = match msg {
                PeerMsg::RaftCommand(cmd) => cmd.callback,
                PeerMsg::CasualMessage(CasualMessage::SplitRegion { callback, .. }) => callback,
                _ => continue,
            };

            let mut err = errorpb::Error::default();
            err.set_message("region is not found".to_owned());
            err.mut_region_not_found().set_region_id(self.region_id());
            let mut resp = RaftCmdResponse::default();
            resp.mut_header().set_error(err);
            callback.invoke_with_response(resp);
        }
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
        sched: Scheduler<RegionTask<EK::Snapshot>>,
        engines: Engines<EK, ER>,
        region: &metapb::Region,
    ) -> Result<SenderFsmPair<EK, ER>> {
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
        let (tx, rx) = mpsc::loose_bounded(cfg.notify_capacity);
        Ok((
            tx,
            Box::new(PeerFsm {
                peer: Peer::new(store_id, cfg, sched, engines, region, meta_peer)?,
                tick_registry: PeerTicks::empty(),
                missing_ticks: 0,
                hibernate_state: HibernateState::ordered(),
                stopped: false,
                has_ready: false,
                mailbox: None,
                receiver: rx,
                skip_split_count: 0,
                skip_gc_raft_log_ticks: 0,
                batch_req_builder: BatchRaftCmdRequestBuilder::new(
                    cfg.raft_entry_max_size.0 as f64,
                ),
            }),
        ))
    }

    // The peer can be created from another node with raft membership changes, and we only
    // know the region_id and peer_id when creating this replicated peer, the region info
    // will be retrieved later after applying snapshot.
    pub fn replicate(
        store_id: u64,
        cfg: &Config,
        sched: Scheduler<RegionTask<EK::Snapshot>>,
        engines: Engines<EK, ER>,
        region_id: u64,
        peer: metapb::Peer,
    ) -> Result<SenderFsmPair<EK, ER>> {
        // We will remove tombstone key when apply snapshot
        info!(
            "replicate peer";
            "region_id" => region_id,
            "peer_id" => peer.get_id(),
        );

        let mut region = metapb::Region::default();
        region.set_id(region_id);

        let (tx, rx) = mpsc::loose_bounded(cfg.notify_capacity);
        Ok((
            tx,
            Box::new(PeerFsm {
                peer: Peer::new(store_id, cfg, sched, engines, &region, peer)?,
                tick_registry: PeerTicks::empty(),
                missing_ticks: 0,
                hibernate_state: HibernateState::ordered(),
                stopped: false,
                has_ready: false,
                mailbox: None,
                receiver: rx,
                skip_split_count: 0,
                skip_gc_raft_log_ticks: 0,
                batch_req_builder: BatchRaftCmdRequestBuilder::new(
                    cfg.raft_entry_max_size.0 as f64,
                ),
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
}

impl<E> BatchRaftCmdRequestBuilder<E>
where
    E: KvEngine,
{
    fn new(raft_entry_max_size: f64) -> BatchRaftCmdRequestBuilder<E> {
        BatchRaftCmdRequestBuilder {
            raft_entry_max_size,
            request: None,
            batch_req_size: 0,
            callbacks: vec![],
        }
    }

    fn can_batch(&self, req: &RaftCmdRequest, req_size: u32) -> bool {
        // No batch request whose size exceed 20% of raft_entry_max_size,
        // so total size of request in batch_raft_request would not exceed
        // (40% + 20%) of raft_entry_max_size
        if req.get_requests().is_empty() || f64::from(req_size) > self.raft_entry_max_size * 0.2 {
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
        let req_num = cmd.request.get_requests().len();
        let RaftCommand {
            mut request,
            callback,
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
        self.callbacks.push((callback, req_num));
        self.batch_req_size += req_size;
    }

    fn should_finish(&self) -> bool {
        if let Some(batch_req) = self.request.as_ref() {
            // Limit the size of batch request so that it will not exceed raft_entry_max_size after
            // adding header.
            if f64::from(self.batch_req_size) > self.raft_entry_max_size * 0.4 {
                return true;
            }
            if batch_req.get_requests().len() > <E as WriteBatchExt>::WRITE_BATCH_MAX_KEYS {
                return true;
            }
        }
        false
    }

    fn build(&mut self, metric: &mut RaftProposeMetrics) -> Option<RaftCommand<E::Snapshot>> {
        if let Some(req) = self.request.take() {
            self.batch_req_size = 0;
            if self.callbacks.len() == 1 {
                let (cb, _) = self.callbacks.pop().unwrap();
                return Some(RaftCommand::new(req, cb));
            }
            metric.batch += self.callbacks.len() - 1;
            let mut cbs = std::mem::take(&mut self.callbacks);
            let proposed_cbs: Vec<ExtCallback> = cbs
                .iter_mut()
                .filter_map(|cb| {
                    if let Callback::Write { proposed_cb, .. } = &mut cb.0 {
                        proposed_cb.take()
                    } else {
                        None
                    }
                })
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
                .filter_map(|cb| {
                    if let Callback::Write { committed_cb, .. } = &mut cb.0 {
                        committed_cb.take()
                    } else {
                        None
                    }
                })
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
            let cb = Callback::write_ext(
                Box::new(move |resp| {
                    let mut last_index = 0;
                    let has_error = resp.response.get_header().has_error();
                    for (cb, req_num) in cbs {
                        let next_index = last_index + req_num;
                        let mut cmd_resp = RaftCmdResponse::default();
                        cmd_resp.set_header(resp.response.get_header().clone());
                        if !has_error {
                            cmd_resp.set_responses(
                                resp.response.get_responses()[last_index..next_index].into(),
                            );
                        }
                        cb.invoke_with_response(cmd_resp);
                        last_index = next_index;
                    }
                }),
                proposed_cb,
                committed_cb,
            );
            return Some(RaftCommand::new(req, cb));
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

    /// Set a mailbox to Fsm, which should be used to send message to itself.
    #[inline]
    fn set_mailbox(&mut self, mailbox: Cow<'_, BasicMailbox<Self>>)
    where
        Self: Sized,
    {
        self.mailbox = Some(mailbox.into_owned());
    }

    /// Take the mailbox from Fsm. Implementation should ensure there will be
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
                    self.ctx
                        .raft_metrics
                        .propose
                        .request_wait_time
                        .observe(duration_to_sec(cmd.send_time.elapsed()) as f64);
                    let req_size = cmd.request.compute_size();
                    if self.fsm.batch_req_builder.can_batch(&cmd.request, req_size) {
                        self.fsm.batch_req_builder.add(cmd, req_size);
                        if self.fsm.batch_req_builder.should_finish() {
                            self.propose_batch_raft_command();
                        }
                    } else {
                        self.propose_batch_raft_command();
                        self.propose_raft_command(cmd.request, cmd.callback)
                    }
                }
                PeerMsg::Tick(tick) => self.on_tick(tick),
                PeerMsg::ApplyRes { res } => {
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
                PeerMsg::UpdateReplicationMode => self.on_update_replication_mode(),
            }
        }
        // Propose batch request which may be still waiting for more raft-command
        self.propose_batch_raft_command();
    }

    fn propose_batch_raft_command(&mut self) {
        if let Some(cmd) = self
            .fsm
            .batch_req_builder
            .build(&mut self.ctx.raft_metrics.propose)
        {
            self.propose_raft_command(cmd.request, cmd.callback)
        }
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

    fn on_casual_msg(&mut self, msg: CasualMessage<EK>) {
        match msg {
            CasualMessage::SplitRegion {
                region_epoch,
                split_keys,
                callback,
                source,
            } => {
                self.on_prepare_split_region(region_epoch, split_keys, callback, &source);
            }
            CasualMessage::ComputeHashResult {
                index,
                context,
                hash,
            } => {
                self.on_hash_computed(index, context, hash);
            }
            CasualMessage::RegionApproximateSize { size } => {
                self.on_approximate_region_size(size);
            }
            CasualMessage::RegionApproximateKeys { keys } => {
                self.on_approximate_region_keys(keys);
            }
            CasualMessage::CompactionDeclinedBytes { bytes } => {
                self.on_compaction_declined_bytes(bytes);
            }
            CasualMessage::HalfSplitRegion {
                region_epoch,
                policy,
                source,
            } => {
                self.on_schedule_half_split_region(&region_epoch, policy, source);
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
                self.fsm.hibernate_state.reset(GroupState::Chaos);
                self.register_raft_base_tick();

                if is_learner(&self.fsm.peer.peer) {
                    // FIXME: should use `bcast_check_stale_peer_message` instead.
                    // Sending a new enum type msg to a old tikv may cause panic during rolling update
                    // we should change the protobuf behavior and check if properly handled in all place
                    self.fsm.peer.bcast_wake_up_message(&mut self.ctx);
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
            CasualMessage::AccessPeer(cb) => cb(self.fsm as &mut dyn AbstractPeer),
            CasualMessage::QueryRegionLeaderResp { region, leader } => {
                // the leader already updated
                if self.fsm.peer.raft_group.raft.leader_id != raft::INVALID_ID
                    // the returned region is stale
                    || util::is_epoch_stale(
                        region.get_region_epoch(),
                        self.fsm.peer.region().get_region_epoch(),
                    )
                {
                    // Stale message
                    return;
                }

                // Wake up the leader if the peer is on the leader's peer list
                if region
                    .get_peers()
                    .iter()
                    .any(|p| p.get_id() == self.fsm.peer_id())
                {
                    self.fsm.peer.send_wake_up_message(&mut self.ctx, &leader);
                }
            }
        }
    }

    fn on_tick(&mut self, tick: PeerTicks) {
        if self.fsm.stopped {
            return;
        }
        trace!(
            "tick";
            "tick" => ?tick,
            "peer_id" => self.fsm.peer_id(),
            "region_id" => self.region_id(),
        );
        self.fsm.tick_registry.remove(tick);
        match tick {
            PeerTicks::RAFT => self.on_raft_base_tick(),
            PeerTicks::RAFT_LOG_GC => self.on_raft_gc_log_tick(false),
            PeerTicks::PD_HEARTBEAT => self.on_pd_heartbeat_tick(),
            PeerTicks::SPLIT_REGION_CHECK => self.on_split_region_check_tick(),
            PeerTicks::CHECK_MERGE => self.on_check_merge(),
            PeerTicks::CHECK_PEER_STALE_STATE => self.on_check_peer_stale_state_tick(),
            _ => unreachable!(),
        }
    }

    fn start(&mut self) {
        self.register_raft_base_tick();
        self.register_raft_gc_log_tick();
        self.register_pd_heartbeat_tick();
        self.register_split_region_check_tick();
        self.register_check_peer_stale_state_tick();
        self.on_check_merge();
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
    }

    fn on_gc_snap(&mut self, snaps: Vec<(SnapKey, bool)>) {
        let s = self.fsm.peer.get_store();
        let compacted_idx = s.truncated_index();
        let compacted_term = s.truncated_term();
        let is_applying_snap = s.is_applying_snapshot();
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
                    self.ctx.snap_mgr.delete_snapshot(&key, s.as_ref(), false);
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
                            self.ctx.snap_mgr.delete_snapshot(&key, s.as_ref(), false);
                        }
                    }
                }
            } else if key.term <= compacted_term
                && (key.idx < compacted_idx || key.idx == compacted_idx && !is_applying_snap)
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
                self.ctx.snap_mgr.delete_snapshot(&key, a.as_ref(), false);
            }
        }
    }

    fn on_clear_region_size(&mut self) {
        self.fsm.peer.approximate_size = None;
        self.fsm.peer.approximate_keys = None;
        self.register_split_region_check_tick();
    }

    fn on_capture_change(
        &mut self,
        cmd: ChangeCmd,
        region_epoch: RegionEpoch,
        cb: Callback<EK::Snapshot>,
    ) {
        fail_point!("raft_on_capture_change");
        let region_id = self.region_id();
        let msg =
            new_read_index_request(region_id, region_epoch.clone(), self.fsm.peer.peer.clone());
        let apply_router = self.ctx.apply_router.clone();
        self.propose_raft_command(
            msg,
            Callback::Read(Box::new(move |resp| {
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
        );
    }

    fn on_significant_msg(&mut self, msg: SignificantMsg<EK::Snapshot>) {
        match msg {
            SignificantMsg::SnapshotStatus {
                to_peer_id, status, ..
            } => {
                // Report snapshot status to the corresponding peer.
                self.report_snapshot_status(to_peer_id, status);
            }
            SignificantMsg::Unreachable { to_peer_id, .. } => {
                if self.fsm.peer.is_leader() {
                    self.fsm.peer.raft_group.report_unreachable(to_peer_id);
                } else if to_peer_id == self.fsm.peer.leader_id() {
                    self.fsm.hibernate_state.reset(GroupState::Chaos);
                    self.register_raft_base_tick();
                }
            }
            SignificantMsg::StoreUnreachable { store_id } => {
                if let Some(peer_id) = util::find_peer(self.region(), store_id).map(|p| p.get_id())
                {
                    if self.fsm.peer.is_leader() {
                        self.fsm.peer.raft_group.report_unreachable(peer_id);
                    } else if peer_id == self.fsm.peer.leader_id() {
                        self.fsm.hibernate_state.reset(GroupState::Chaos);
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
            SignificantMsg::StoreResolved { store_id, group_id } => {
                let state = self.ctx.global_replication_state.lock().unwrap();
                if state.status().get_mode() != ReplicationMode::DrAutoSync {
                    return;
                }
                if state.status().get_dr_auto_sync().get_state() == DrAutoSyncState::Async {
                    return;
                }
                drop(state);
                self.fsm
                    .peer
                    .raft_group
                    .raft
                    .assign_commit_groups(&[(store_id, group_id)]);
            }
            SignificantMsg::CaptureChange {
                cmd,
                region_epoch,
                callback,
            } => self.on_capture_change(cmd, region_epoch, callback),
            SignificantMsg::LeaderCallback(cb) => {
                self.on_leader_callback(cb);
            }
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
        self.propose_raft_command(msg, cb);
    }

    fn on_role_changed(&mut self, ready: &Ready) {
        // Update leader lease when the Raft state changes.
        if let Some(ss) = ready.ss() {
            if StateRole::Leader == ss.raft_state {
                self.fsm.missing_ticks = 0;
                self.register_split_region_check_tick();
                self.fsm.peer.heartbeat_pd(&self.ctx);
                self.register_pd_heartbeat_tick();
            }
        }
    }

    pub fn collect_ready(&mut self) {
        let has_ready = self.fsm.has_ready;
        self.fsm.has_ready = false;
        if !has_ready || self.fsm.stopped {
            return;
        }
        self.ctx.pending_count += 1;
        self.ctx.has_ready = true;
        let res = self.fsm.peer.handle_raft_ready_append(self.ctx);
        if let Some(mut r) = res {
            // This bases on an assumption that fsm array passed in `end` method will have
            // the same order of processing.
            r.batch_offset = self.ctx.processed_fsm_count;
            self.on_role_changed(&r.ready);
            if r.ctx.has_new_entries {
                self.register_raft_gc_log_tick();
                self.register_split_region_check_tick();
            }
            self.ctx.ready_res.push(r);
        }
    }

    pub fn post_raft_ready_append(&mut self, ready: CollectedReady) {
        if ready.ctx.region_id != self.fsm.region_id() {
            panic!(
                "{} region id not matched: {} # {}",
                self.fsm.peer.tag,
                ready.ctx.region_id,
                self.fsm.region_id()
            );
        }
        let is_merging = self.fsm.peer.pending_merge_state.is_some();
        let res = self.fsm.peer.post_raft_ready_append(self.ctx, ready.ctx);
        self.fsm
            .peer
            .handle_raft_ready_advance(self.ctx, ready.ready);
        if let Some(apply_res) = res {
            self.on_ready_apply_snapshot(apply_res);
            if is_merging {
                // After applying a snapshot, merge is rollbacked implicitly.
                self.on_ready_rollback_merge(0, None);
            }
            self.register_raft_base_tick();
        }
        if self.fsm.peer.leader_unreachable {
            self.fsm.hibernate_state.reset(GroupState::Chaos);
            self.register_raft_base_tick();
            self.fsm.peer.leader_unreachable = false;
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

    #[inline]
    fn schedule_tick(&mut self, tick: PeerTicks) {
        if self.fsm.tick_registry.contains(tick) {
            return;
        }
        let idx = tick.bits() as usize;
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
        self.fsm.tick_registry.insert(tick);

        let region_id = self.region_id();
        let mb = match self.ctx.router.mailbox(region_id) {
            Some(mb) => mb,
            None => {
                self.fsm.tick_registry.remove(tick);
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
        self.schedule_tick(PeerTicks::RAFT)
    }

    fn on_raft_base_tick(&mut self) {
        if self.fsm.peer.pending_remove {
            self.fsm.peer.mut_store().flush_cache_metrics();
            return;
        }
        // When having pending snapshot, if election timeout is met, it can't pass
        // the pending conf change check because first index has been updated to
        // a value that is larger than last index.
        if self.fsm.peer.is_applying_snapshot() || self.fsm.peer.has_pending_snapshot() {
            // need to check if snapshot is applied.
            self.fsm.has_ready = true;
            self.fsm.missing_ticks = 0;
            self.register_raft_base_tick();
            return;
        }

        self.fsm.peer.retry_pending_reads(&self.ctx.cfg);

        let mut res = None;
        if self.ctx.cfg.hibernate_regions {
            if self.fsm.hibernate_state.group_state() == GroupState::Idle {
                // missing_ticks should be less than election timeout ticks otherwise
                // follower may tick more than an election timeout in chaos state.
                // Before stopping tick, `missing_tick` should be `raft_election_timeout_ticks` - 2
                // - `raft_heartbeat_ticks` (default 10 - 2 - 2 = 6)
                // and the follwer's `election_elapsed` in raft-rs is 1.
                // After the group state becomes Chaos, the next tick will call `raft_group.tick`
                // `missing_tick` + 1 times(default 7).
                // Then the follower's `election_elapsed` will be 1 + `missing_tick` + 1
                // (default 1 + 6 + 1 = 8) which is less than the min election timeout.
                // The reason is that we don't want let all followers become (pre)candidate if one
                // follower may receive a request, then becomes (pre)candidate and sends (pre)vote msg
                // to others. As long as the leader can wake up and broadcast hearbeats in one `raft_heartbeat_ticks`
                // time(default 2s), no more followers will wake up and sends vote msg again.
                if self.fsm.missing_ticks + 2 + self.ctx.cfg.raft_heartbeat_ticks
                    < self.ctx.cfg.raft_election_timeout_ticks
                {
                    self.register_raft_base_tick();
                    self.fsm.missing_ticks += 1;
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
        if self.fsm.peer.raft_group.tick() {
            self.fsm.has_ready = true;
        }

        self.fsm.peer.mut_store().flush_cache_metrics();

        // Keep ticking if there are still pending read requests or this node is within hibernate timeout.
        if res.is_none() /* hibernate_region is false */ ||
            !self.fsm.peer.check_after_tick(self.fsm.hibernate_state.group_state(), res.unwrap()) ||
            (self.fsm.peer.is_leader() && !self.all_agree_to_hibernate())
        {
            self.register_raft_base_tick();
            // We need pd heartbeat tick to collect down peers and pending peers.
            self.register_pd_heartbeat_tick();
            return;
        }

        debug!("stop ticking"; "region_id" => self.region_id(), "peer_id" => self.fsm.peer_id(), "res" => ?res);
        self.fsm.hibernate_state.reset(GroupState::Idle);
        // Followers will stop ticking at L789. Keep ticking for followers
        // to allow it to campaign quickly when abnormal situation is detected.
        if !self.fsm.peer.is_leader() {
            self.register_raft_base_tick();
        } else {
            self.register_pd_heartbeat_tick();
        }
    }

    fn on_apply_res(&mut self, res: ApplyTaskRes<EK::Snapshot>) {
        fail_point!("on_apply_res", |_| {});
        match res {
            ApplyTaskRes::Apply(mut res) => {
                debug!(
                    "async apply finish";
                    "region_id" => self.region_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "res" => ?res,
                );
                self.on_ready_result(&mut res.exec_res, &res.metrics);
                if self.fsm.stopped {
                    return;
                }
                self.fsm.has_ready |= self.fsm.peer.post_apply(
                    self.ctx,
                    res.apply_state,
                    res.applied_index_term,
                    &res.metrics,
                );
                // After applying, several metrics are updated, report it to pd to
                // get fair schedule.
                self.register_pd_heartbeat_tick();
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

        if msg.has_extra_msg() {
            self.on_extra_message(msg);
            return Ok(());
        }

        let is_snapshot = msg.get_message().has_snapshot();
        let regions_to_destroy = match self.check_snapshot(&msg)? {
            Either::Left(key) => {
                // If the snapshot file is not used again, then it's OK to
                // delete them here. If the snapshot file will be reused when
                // receiving, then it will fail to pass the check again, so
                // missing snapshot files should not be noticed.
                let s = self.ctx.snap_mgr.get_snapshot_for_applying(&key)?;
                self.ctx.snap_mgr.delete_snapshot(&key, s.as_ref(), false);
                return Ok(());
            }
            Either::Right(v) => v,
        };

        if !self.check_request_snapshot(&msg) {
            return Ok(());
        }

        if util::is_vote_msg(&msg.get_message())
            || msg.get_message().get_msg_type() == MessageType::MsgTimeoutNow
        {
            if self.fsm.hibernate_state.group_state() != GroupState::Chaos {
                self.fsm.hibernate_state.reset(GroupState::Chaos);
                self.register_raft_base_tick();
            }
        } else if msg.get_from_peer().get_id() == self.fsm.peer.leader_id() {
            self.reset_raft_tick(GroupState::Ordered);
        }

        let from_peer_id = msg.get_from_peer().get_id();
        self.fsm.peer.insert_peer_cache(msg.take_from_peer());

        let result = self.fsm.peer.step(self.ctx, msg.take_message());

        if is_snapshot {
            if !self.fsm.peer.has_pending_snapshot() {
                // This snapshot is rejected by raft-rs.
                let mut meta = self.ctx.store_meta.lock().unwrap();
                meta.pending_snapshot_regions
                    .retain(|r| self.fsm.region_id() != r.get_id());
            } else {
                // This snapshot may be accepted by raft-rs.
                // If it's rejected by raft-rs, the snapshot region in `pending_snapshot_regions`
                // will be removed together with the latest snapshot region after applying that snapshot.
                // But if `regions_to_destroy` is not empty, the pending snapshot must be this msg's snapshot
                // because this kind of snapshot is exclusive.
                self.destroy_regions_for_snapshot(regions_to_destroy);
            }
        }

        if result.is_err() {
            return result;
        }

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
        if self
            .fsm
            .hibernate_state
            .maybe_hibernate(self.fsm.peer_id(), self.fsm.peer.region())
        {
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

    fn on_extra_message(&mut self, mut msg: RaftMessage) {
        match msg.get_extra_msg().get_type() {
            ExtraMessageType::MsgRegionWakeUp | ExtraMessageType::MsgCheckStalePeer => {
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
        }
    }

    fn reset_raft_tick(&mut self, state: GroupState) {
        self.fsm.hibernate_state.reset(state);
        self.fsm.missing_ticks = 0;
        self.fsm.peer.should_wake_up = false;
        self.register_raft_base_tick();
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
            let mut need_gc_msg = util::is_vote_msg(msg.get_message());
            if msg.has_extra_msg() {
                // A learner can't vote so it sends the check-stale-peer msg to others to find out whether
                // it is removed due to conf change or merge.
                need_gc_msg |=
                    msg.get_extra_msg().get_type() == ExtraMessageType::MsgCheckStalePeer;
                // For backward compatibility
                need_gc_msg |= msg.get_extra_msg().get_type() == ExtraMessageType::MsgRegionWakeUp;
            }
            // The message is stale and not in current region.
            self.ctx.handle_stale_msg(
                msg,
                self.fsm.peer.region().get_region_epoch().clone(),
                need_gc_msg,
                None,
            );
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
                match self.fsm.peer.maybe_destroy(&self.ctx) {
                    Some(job) => {
                        info!(
                            "target peer id is larger, destroying self";
                            "region_id" => self.fsm.region_id(),
                            "peer_id" => self.fsm.peer_id(),
                            "target_peer" => ?target,
                        );
                        if self.handle_destroy_peer(job) {
                            if let Err(e) = self
                                .ctx
                                .router
                                .send_control(StoreMsg::RaftMessage(msg.clone()))
                            {
                                info!(
                                    "failed to send back store message, are we shutting down?";
                                    "region_id" => self.fsm.region_id(),
                                    "peer_id" => self.fsm.peer_id(),
                                    "err" => %e,
                                );
                            }
                        }
                    }
                    None => self.ctx.raft_metrics.message_dropped.applying_snap += 1,
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

        // When receiving message that has a merge target, it indicates that the source peer on this
        // store is stale, the peers on other stores are already merged. The epoch in merge target
        // is the state of target peer at the time when source peer is merged. So here we record the
        // merge target epoch version to let the target peer on this store to decide whether to
        // destroy the source peer.
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
            // Merge target epoch records the version of target region when source region is merged.
            // So it must be same no matter when receiving merge target.
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
            // In the case that the source peer's range isn't overlapped with target's anymore:
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
            // If that, source peer still need to decide whether to destroy itself. When the target
            // peer has already moved on, source peer can destroy itself.
            if util::is_epoch_stale(merge_target.get_region_epoch(), r.get_region_epoch()) {
                return Ok(true);
            }
            return Ok(false);
        }
        drop(meta);

        // All of the target peers must exist before merging which is guaranteed by PD.
        // Now the target peer is not in region map, so if everything is ok, the merge target
        // region should be staler than the local target region
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

        if self.fsm.peer.peer != *msg.get_to_peer() {
            info!(
                "receive stale gc message, ignore.";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
            );
            self.ctx.raft_metrics.message_dropped.stale_msg += 1;
            return;
        }
        // TODO: ask pd to guarantee we are stale now.
        info!(
            "receives gc message, trying to remove";
            "region_id" => self.fsm.region_id(),
            "peer_id" => self.fsm.peer_id(),
            "to_peer" => ?msg.get_to_peer(),
        );
        match self.fsm.peer.maybe_destroy(&self.ctx) {
            None => self.ctx.raft_metrics.message_dropped.applying_snap += 1,
            Some(job) => {
                self.handle_destroy_peer(job);
            }
        }
    }

    // Returns `Vec<(u64, bool)>` indicated (source_region_id, merge_to_this_peer) if the `msg`
    // doesn't contain a snapshot or this snapshot doesn't conflict with any other snapshots or regions.
    // Otherwise a `SnapKey` is returned.
    fn check_snapshot(&mut self, msg: &RaftMessage) -> Result<Either<SnapKey, Vec<(u64, bool)>>> {
        if !msg.get_message().has_snapshot() {
            return Ok(Either::Right(vec![]));
        }

        let region_id = msg.get_region_id();
        let snap = msg.get_message().get_snapshot();
        let key = SnapKey::from_region_snap(region_id, snap);
        let mut snap_data = RaftSnapshotData::default();
        snap_data.merge_from_bytes(snap.get_data())?;
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
            );
            self.ctx.raft_metrics.message_dropped.region_no_peer += 1;
            return Ok(Either::Left(key));
        }

        let mut meta = self.ctx.store_meta.lock().unwrap();
        if meta.regions[&self.region_id()] != *self.region() {
            if !self.fsm.peer.is_initialized() {
                info!(
                    "stale delegate detected, skip";
                    "region_id" => self.fsm.region_id(),
                    "peer_id" => self.fsm.peer_id(),
                );
                self.ctx.raft_metrics.message_dropped.stale_msg += 1;
                return Ok(Either::Left(key));
            } else {
                panic!(
                    "{} meta corrupted: {:?} != {:?}",
                    self.fsm.peer.tag,
                    meta.regions[&self.region_id()],
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
                );
                self.ctx.raft_metrics.message_dropped.region_overlap += 1;
                return Ok(Either::Left(key));
            }
        }

        let mut is_overlapped = false;
        let mut regions_to_destroy = vec![];
        // In some extreme cases, it may cause source peer destroyed improperly so that a later
        // CommitMerge may panic because source is already destroyed, so just drop the message:
        // 1. A new snapshot is received whereas a snapshot is still in applying, and the snapshot
        // under applying is generated before merge and the new snapshot is generated after merge.
        // After the applying snapshot is finished, the log may able to catch up and so a
        // CommitMerge will be applied.
        // 2. There is a CommitMerge pending in apply thread.
        let ready = !self.fsm.peer.is_applying_snapshot()
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
                // The snapshot that we decide to whether destroy peer based on must can be applied.
                // So here not to destroy peer immediately, or the snapshot maybe dropped in later
                // check but the peer is already destroyed.
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
                    PeerMsg::CasualMessage(CasualMessage::RegionOverlapped),
                );
            }
        }
        if is_overlapped {
            self.ctx.raft_metrics.message_dropped.region_overlap += 1;
            return Ok(Either::Left(key));
        }

        // Check if snapshot file exists.
        self.ctx.snap_mgr.get_snapshot_for_applying(&key)?;

        // WARNING: The checking code must be above this line.
        // Now all checking passed.

        if self.fsm.peer.local_first_replicate && !self.fsm.peer.is_initialized() {
            // If the peer is not initialized and passes the snapshot range check, `is_splitting` flag must
            // be false.
            // 1. If `is_splitting` is set to true, then the uninitialized peer is created before split is applied
            //    and the peer id is the same as split one. So there should be no initialized peer before.
            // 2. If the peer is also created by splitting, then the snapshot range is not overlapped with
            //    parent peer. It means leader has applied merge and split at least one time. However,
            //    the prerequisite of merge includes the initialization of all target peers and source peers,
            //    which is conflict with 1.
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
            // Use `unwrap` is ok because the StoreMeta lock is held and these source peers still
            // exist in regions and region_ranges map.
            // It depends on the implementation of `destroy_peer`.
            self.ctx
                .router
                .force_send(
                    source_region_id,
                    PeerMsg::SignificantMsg(SignificantMsg::MergeResult {
                        target_region_id: self.fsm.region_id(),
                        target: self.fsm.peer.peer.clone(),
                        result,
                    }),
                )
                .unwrap();
        }
    }

    // Check if this peer can handle request_snapshot.
    fn check_request_snapshot(&mut self, msg: &RaftMessage) -> bool {
        let m = msg.get_message();
        let request_index = m.get_request_snapshot();
        if request_index == raft::INVALID_INDEX {
            // If it's not a request snapshot, then go on.
            return true;
        }
        self.fsm
            .peer
            .ready_to_handle_request_snapshot(request_index)
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
            self.destroy_peer(false);
            true
        }
    }

    fn destroy_peer(&mut self, merged_by_target: bool) {
        fail_point!("destroy_peer");
        info!(
            "starts destroy";
            "region_id" => self.fsm.region_id(),
            "peer_id" => self.fsm.peer_id(),
            "merged_by_target" => merged_by_target,
        );
        let region_id = self.region_id();
        // We can't destroy a peer which is applying snapshot.
        assert!(!self.fsm.peer.is_applying_snapshot());

        // Mark itself as pending_remove
        self.fsm.peer.pending_remove = true;

        let mut meta = self.ctx.store_meta.lock().unwrap();

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
        let is_initialized = self.fsm.peer.is_initialized();
        if let Err(e) = self.fsm.peer.destroy(self.ctx, merged_by_target) {
            // If not panic here, the peer will be recreated in the next restart,
            // then it will be gc again. But if some overlap region is created
            // before restarting, the gc action will delete the overlap region's
            // data too.
            panic!("{} destroy err {:?}", self.fsm.peer.tag, e);
        }
        // Some places use `force_send().unwrap()` if the StoreMeta lock is held.
        // So in here, it's necessary to held the StoreMeta lock when closing the router.
        self.ctx.router.close(region_id);
        self.fsm.stop();

        if is_initialized
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

        if self.fsm.peer.local_first_replicate {
            let mut pending_create_peers = self.ctx.pending_create_peers.lock().unwrap();
            if is_initialized {
                assert!(pending_create_peers.get(&region_id).is_none());
            } else {
                // If this region's data in `pending_create_peers` is not equal to `(peer_id, false)`,
                // it means this peer will be replaced by the split one.
                if let Some(status) = pending_create_peers.get(&region_id) {
                    if *status == (self.fsm.peer_id(), false) {
                        pending_create_peers.remove(&region_id);
                    }
                }
            }
        }

        // Clear merge related structures.
        if let Some(&need_atomic) = meta.destroyed_region_for_snap.get(&region_id) {
            if need_atomic {
                panic!(
                    "{} should destroy with target region atomically",
                    self.fsm.peer.tag
                );
            } else {
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

        meta.pending_merge_targets.remove(&region_id);
        if let Some(target) = meta.targets_map.remove(&region_id) {
            if meta.pending_merge_targets.contains_key(&target) {
                meta.pending_merge_targets
                    .get_mut(&target)
                    .unwrap()
                    .remove(&region_id);
                // When the target doesn't exist(add peer but the store is isolated), source peer decide to destroy by itself.
                // Without target, the `pending_merge_targets` for target won't be removed, so here source peer help target to clear.
                if meta.regions.get(&target).is_none()
                    && meta.pending_merge_targets.get(&target).unwrap().is_empty()
                {
                    meta.pending_merge_targets.remove(&target);
                }
            }
        }
        meta.leaders.remove(&region_id);
    }

    // Update some region infos
    fn update_region(&mut self, mut region: metapb::Region) {
        {
            let mut meta = self.ctx.store_meta.lock().unwrap();
            meta.set_region(
                &self.ctx.coprocessor_host,
                region.clone(),
                &mut self.fsm.peer,
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

        if cp.index >= self.fsm.peer.raft_group.raft.raft_log.first_index() {
            match self.fsm.peer.raft_group.apply_conf_change(&cp.conf_change) {
                Ok(_) => {}
                // PD could dispatch redundant conf changes.
                Err(raft::Error::NotExists(_, _)) | Err(raft::Error::Exists(_, _)) => {}
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

            // Remove or demote leader will cause this raft group unavailable
            // until new leader elected, but we can't revert this operation
            // because its result is already persisted in apply worker
            // TODO: should we transfer leader here?
            let demote_self = is_learner(&self.fsm.peer.peer);
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
        } else if !self.fsm.peer.has_valid_leader() {
            self.fsm.hibernate_state.reset(GroupState::Chaos);
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
        let total_cnt = self.fsm.peer.last_applying_idx - first_index;
        // the size of current CompactLog command can be ignored.
        let remain_cnt = self.fsm.peer.last_applying_idx - state.get_index() - 1;
        self.fsm.peer.raft_log_size_hint =
            self.fsm.peer.raft_log_size_hint * remain_cnt / total_cnt;
        let compact_to = state.get_index() + 1;
        let task = RaftlogGcTask::gc(
            self.fsm.peer.get_store().get_region_id(),
            self.fsm.peer.last_compacted_idx,
            compact_to,
        );
        self.fsm.peer.last_compacted_idx = compact_to;
        self.fsm.peer.mut_store().compact_to(compact_to);
        if let Err(e) = self.ctx.raftlog_gc_scheduler.schedule(task) {
            error!(
                "failed to schedule compact task";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "err" => %e,
            );
        }
    }

    fn on_ready_split_region(
        &mut self,
        derived: metapb::Region,
        regions: Vec<metapb::Region>,
        new_split_regions: HashMap<u64, apply::NewSplitPeer>,
    ) {
        fail_point!("on_split", self.ctx.store_id() == 3, |_| {});
        self.register_split_region_check_tick();
        let mut meta = self.ctx.store_meta.lock().unwrap();
        let region_id = derived.get_id();
        meta.set_region(&self.ctx.coprocessor_host, derived, &mut self.fsm.peer);
        self.fsm.peer.post_split();

        // Roughly estimate the size and keys for new regions.
        let new_region_count = regions.len() as u64;
        let estimated_size = self.fsm.peer.approximate_size.map(|x| x / new_region_count);
        let estimated_keys = self.fsm.peer.approximate_keys.map(|x| x / new_region_count);
        // It's not correct anymore, so set it to None to let split checker update it.
        self.fsm.peer.approximate_size = None;
        self.fsm.peer.approximate_keys = None;

        let is_leader = self.fsm.peer.is_leader();
        if is_leader {
            self.fsm.peer.approximate_size = estimated_size;
            self.fsm.peer.approximate_keys = estimated_keys;
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
            if let Err(e) = self.ctx.split_check_scheduler.schedule(
                SplitCheckTask::GetRegionApproximateSizeAndKeys {
                    region: self.fsm.peer.region().clone(),
                    pending_tasks: Arc::new(AtomicU64::new(1)),
                    cb: Box::new(move |_, _| {}),
                },
            ) {
                error!(
                    "failed to schedule split check task";
                    "region_id" => self.fsm.region_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "err" => ?e,
                );
            }
        }

        let last_key = enc_end_key(regions.last().unwrap());
        if meta.region_ranges.remove(&last_key).is_none() {
            panic!("{} original region should exist", self.fsm.peer.tag);
        }
        let last_region_id = regions.last().unwrap().get_id();
        for new_region in regions {
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
            info!(
                "insert new region";
                "region_id" => new_region_id,
                "region" => ?new_region,
            );
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
                self.ctx.router.close(new_region_id);
            }

            let (sender, mut new_peer) = match PeerFsm::create(
                self.ctx.store_id(),
                &self.ctx.cfg,
                self.ctx.region_scheduler.clone(),
                self.ctx.engines.clone(),
                &new_region,
            ) {
                Ok((sender, new_peer)) => (sender, new_peer),
                Err(e) => {
                    // peer information is already written into db, can't recover.
                    // there is probably a bug.
                    panic!("create new split region {:?} err {:?}", new_region, e);
                }
            };
            let mut replication_state = self.ctx.global_replication_state.lock().unwrap();
            new_peer.peer.init_replication_mode(&mut *replication_state);
            drop(replication_state);

            let meta_peer = new_peer.peer.peer.clone();

            for p in new_region.get_peers() {
                // Add this peer to cache.
                new_peer.peer.insert_peer_cache(p.clone());
            }

            // New peer derive write flow from parent region,
            // this will be used by balance write flow.
            new_peer.peer.peer_stat = self.fsm.peer.peer_stat.clone();
            let campaigned = new_peer.peer.maybe_campaign(is_leader);
            new_peer.has_ready |= campaigned;

            if is_leader {
                new_peer.peer.approximate_size = estimated_size;
                new_peer.peer.approximate_keys = estimated_keys;
                // The new peer is likely to become leader, send a heartbeat immediately to reduce
                // client query miss.
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
            if last_region_id == new_region_id {
                // To prevent from big region, the right region needs run split
                // check again after split.
                new_peer.peer.size_diff_hint = self.ctx.cfg.region_split_check_diff.0;
            }
            let mailbox = BasicMailbox::new(sender, new_peer);
            self.ctx.router.register(new_region_id, mailbox);
            self.ctx
                .router
                .force_send(new_region_id, PeerMsg::Start)
                .unwrap();

            if !campaigned {
                if let Some(msg) = meta
                    .pending_msgs
                    .swap_remove_front(|m| m.get_to_peer() == &meta_peer)
                {
                    if let Err(e) = self
                        .ctx
                        .router
                        .force_send(new_region_id, PeerMsg::RaftMessage(msg))
                    {
                        warn!("handle first requset failed"; "region_id" => region_id, "error" => ?e);
                    }
                }
            }

            if is_leader {
                // The size and keys for new region may be far from the real value.
                // So we let split checker to update it immediately.
                if let Err(e) = self.ctx.split_check_scheduler.schedule(
                    SplitCheckTask::GetRegionApproximateSizeAndKeys {
                        region: new_region,
                        pending_tasks: Arc::new(AtomicU64::new(1)),
                        cb: Box::new(move |_, _| {}),
                    },
                ) {
                    error!(
                        "failed to schedule split check task";
                        "region_id" => new_region_id,
                        "err" => ?e,
                    );
                }
            }
        }
        fail_point!("after_split", self.ctx.store_id() == 3, |_| {});
    }

    fn register_merge_check_tick(&mut self) {
        self.schedule_tick(PeerTicks::CHECK_MERGE)
    }

    /// Check if merge target region is staler than the local one in kv engine.
    /// It should be called when target region is not in region map in memory.
    /// If everything is ok, the answer should always be true because PD should ensure all target peers exist.
    /// So if not, error log will be printed and return false.
    fn is_merge_target_region_stale(&self, target_region: &metapb::Region) -> Result<bool> {
        let target_region_id = target_region.get_id();
        let target_peer_id = util::find_peer(target_region, self.ctx.store_id())
            .unwrap()
            .get_id();

        let state_key = keys::region_state_key(target_region_id);
        if let Some(target_state) = self
            .ctx
            .engines
            .kv
            .get_msg_cf::<RegionLocalState>(CF_RAFT, &state_key)?
        {
            if util::is_epoch_stale(
                target_region.get_region_epoch(),
                target_state.get_region().get_region_epoch(),
            ) {
                return Ok(true);
            }
            // The local target region epoch is staler than target region's.
            // In the case where the peer is destroyed by receiving gc msg rather than applying conf change,
            // the epoch may staler but it's legal, so check peer id to assure that.
            if let Some(local_target_peer_id) =
                util::find_peer(target_state.get_region(), self.ctx.store_id()).map(|r| r.get_id())
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
                        // The local target peer id is greater than the one in target region, but its epoch
                        // is staler than target_region's. That is contradictory.
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
                // Can't get local target peer id probably because this target peer is removed by applying conf change
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
                error!("something is wrong, maybe PD do not ensure all target peers exist before merging");
                Ok(false)
            }
        }
    }

    fn schedule_merge(&mut self) -> Result<()> {
        fail_point!("on_schedule_merge", |_| Ok(()));
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
                match self
                    .fsm
                    .peer
                    .get_store()
                    .entries(low, state.get_commit() + 1, NO_LIMIT)
                {
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

            let sibling_peer = util::find_peer(&sibling_region, self.store_id()).unwrap();
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
        // Please note that, here assumes that the unit of network isolation is store rather than
        // peer. So a quorum stores of source region should also be the quorum stores of target
        // region. Otherwise we need to enable proposal forwarding.
        self.ctx
            .router
            .force_send(
                target_id,
                PeerMsg::RaftCommand(RaftCommand::new(request, Callback::None)),
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
        self.propose_raft_command(req, Callback::None);
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
                        &mut self.ctx,
                    );
                }
            }
        }
    }

    fn on_ready_prepare_merge(&mut self, region: metapb::Region, state: MergeState) {
        {
            let mut meta = self.ctx.store_meta.lock().unwrap();
            meta.set_region(&self.ctx.coprocessor_host, region, &mut self.fsm.peer);
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

    fn on_ready_commit_merge(&mut self, region: metapb::Region, source: metapb::Region) {
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
        meta.set_region(&self.ctx.coprocessor_host, region, &mut self.fsm.peer);
        meta.readers.remove(&source.get_id());

        // If a follower merges into a leader, a more recent read may happen
        // on the leader of the follower. So max ts should be updated after
        // a region merge.
        self.fsm
            .peer
            .require_updating_max_ts(&self.ctx.pd_scheduler);

        drop(meta);

        // make approximate size and keys updated in time.
        // the reason why follower need to update is that there is a issue that after merge
        // and then transfer leader, the new leader may have stale size and keys.
        self.fsm.peer.size_diff_hint = self.ctx.cfg.region_split_check_diff.0;
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
            PeerMsg::SignificantMsg(SignificantMsg::MergeResult {
                target_region_id: self.fsm.region_id(),
                target: self.fsm.peer.peer.clone(),
                result: MergeResultKind::FromTargetLog,
            }),
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
    /// If commit is 0, it means that Merge is rollbacked by a snapshot; otherwise
    /// it's rollbacked by a proposal, and its value should be equal to the commit
    /// index of previous PrepareMerge.
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

        if let Some(r) = region {
            let mut meta = self.ctx.store_meta.lock().unwrap();
            meta.set_region(&self.ctx.coprocessor_host, r, &mut self.fsm.peer);
        }
        if self.fsm.peer.is_leader() {
            info!(
                "notify pd with rollback merge";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "commit_index" => commit,
            );
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
            .map_or(true, |s| s.get_target().get_peers().contains(&target));
        if !exists {
            panic!(
                "{} unexpected merge result: {:?} {:?} {:?}",
                self.fsm.peer.tag, self.fsm.peer.pending_merge_state, target, result
            );
        }
        // Because of the checking before proposing `PrepareMerge`, which is
        // no `CompactLog` proposal between the smallest commit index and the latest index.
        // If the merge succeed, all source peers are impossible in apply snapshot state
        // and must be initialized.
        {
            let meta = self.ctx.store_meta.lock().unwrap();
            if meta.atomic_snap_regions.contains_key(&self.region_id()) {
                panic!(
                    "{} is applying atomic snapshot on getting merge result, target region id {}, target peer {:?}, merge result type {:?}",
                    self.fsm.peer.tag, target_region_id, target, result
                );
            }
        }
        if self.fsm.peer.is_applying_snapshot() {
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
                // `merge_by_target` is true because this region's range already belongs to
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
        // no `CompactLog` proposal between the smallest commit index and the latest index.
        // If the merge succeed, all source peers are impossible in apply snapshot state
        // and must be initialized.
        // So `maybe_destroy` must succeed here.
        let job = self.fsm.peer.maybe_destroy(&self.ctx).unwrap();
        self.handle_destroy_peer(job);
    }

    fn on_ready_apply_snapshot(&mut self, apply_result: ApplySnapResult) {
        let prev_region = apply_result.prev_region;
        let region = apply_result.region;

        info!(
            "snapshot is applied";
            "region_id" => self.fsm.region_id(),
            "peer_id" => self.fsm.peer_id(),
            "region" => ?region,
        );

        if prev_region.get_peers() != region.get_peers() {
            let mut state = self.ctx.global_replication_state.lock().unwrap();
            let gb = state
                .calculate_commit_group(self.fsm.peer.replication_mode_version, region.get_peers());
            self.fsm.peer.raft_group.raft.clear_commit_group();
            self.fsm.peer.raft_group.raft.assign_commit_groups(gb);
        }

        let mut meta = self.ctx.store_meta.lock().unwrap();
        debug!(
            "check snapshot range";
            "region_id" => self.fsm.region_id(),
            "peer_id" => self.fsm.peer_id(),
            "prev_region" => ?prev_region,
        );

        // Remove this region's snapshot region from the `pending_snapshot_regions`
        // The `pending_snapshot_regions` is only used to occupy the key range, so if this
        // peer is added to `region_ranges`, it can be remove from `pending_snapshot_regions`
        meta.pending_snapshot_regions
            .retain(|r| self.fsm.region_id() != r.get_id());

        // Remove its source peers' metadata
        for r in &apply_result.destroyed_regions {
            let prev = meta.region_ranges.remove(&enc_end_key(&r));
            assert_eq!(prev, Some(r.get_id()));
            assert!(meta.regions.remove(&r.get_id()).is_some());
            meta.readers.remove(&r.get_id());
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
                "region changed after applying snapshot";
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
            // More accurately, the `RegionLocalState` has been persisted so the data can be removed from `pending_create_peers`.
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
        let prev = meta.regions.insert(region.get_id(), region);
        assert_eq!(prev, Some(prev_region));

        drop(meta);

        for r in &apply_result.destroyed_regions {
            if let Err(e) = self.ctx.router.force_send(
                r.get_id(),
                PeerMsg::SignificantMsg(SignificantMsg::MergeResult {
                    target_region_id: self.fsm.region_id(),
                    target: self.fsm.peer.peer.clone(),
                    result: MergeResultKind::FromTargetSnapshotStep2,
                }),
            ) {
                panic!("{} failed to send merge result(FromTargetSnapshotStep2) to source region {}, err {}",
                       self.fsm.peer.tag, r.get_id(), e);
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
                ExecResult::CompactLog { first_index, state } => {
                    self.on_ready_compact_log(first_index, state)
                }
                ExecResult::SplitRegion {
                    derived,
                    regions,
                    new_split_regions,
                } => self.on_ready_split_region(derived, regions, new_split_regions),
                ExecResult::PrepareMerge { region, state } => {
                    self.on_ready_prepare_merge(region, state)
                }
                ExecResult::CommitMerge { region, source } => {
                    self.on_ready_commit_merge(region.clone(), source.clone())
                }
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
            }
        }

        // Update metrics only when all exec_results are finished in case the metrics is counted multiple times
        // when waiting for commit merge
        self.ctx.store_stat.lock_cf_bytes_written += metrics.lock_cf_written_bytes;
        self.ctx.store_stat.engine_total_bytes_written += metrics.written_bytes;
        self.ctx.store_stat.engine_total_keys_written += metrics.written_keys;
    }

    /// Check if a request is valid if it has valid prepare_merge/commit_merge proposal.
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
            if !util::region_on_same_stores(target_region, region) {
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
            if !util::region_on_same_stores(source_region, region) {
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
        // Check store_id, make sure that the msg is dispatched to the right place.
        if let Err(e) = util::check_store_id(msg, self.store_id()) {
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
        if !(self.fsm.peer.is_leader() || is_read_index_request || allow_replica_read) {
            self.ctx.raft_metrics.invalid_proposal.not_leader += 1;
            let leader = self.fsm.peer.get_peer_from_cache(leader_id);
            self.fsm.hibernate_state.reset(GroupState::Chaos);
            self.register_raft_base_tick();
            return Err(Error::NotLeader(region_id, leader));
        }
        // peer_id must be the same as peer's.
        if let Err(e) = util::check_peer_id(msg, self.fsm.peer.peer_id()) {
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
                self.fsm.peer.tag
            )));
        }
        // Check whether the term is stale.
        if let Err(e) = util::check_term(msg, self.fsm.peer.term()) {
            self.ctx.raft_metrics.invalid_proposal.stale_command += 1;
            return Err(e);
        }

        match util::check_region_epoch(msg, self.fsm.peer.region(), true) {
            Err(Error::EpochNotMatch(msg, mut new_regions)) => {
                // Attach the region which might be split from the current region. But it doesn't
                // matter if the region is not split from the current region. If the region meta
                // received by the TiKV driver is newer than the meta cached in the driver, the meta is
                // updated.
                let sibling_region = self.find_sibling_region();
                if let Some(sibling_region) = sibling_region {
                    new_regions.push(sibling_region);
                }
                self.ctx.raft_metrics.invalid_proposal.epoch_not_match += 1;
                Err(Error::EpochNotMatch(msg, new_regions))
            }
            Err(e) => Err(e),
            Ok(()) => Ok(None),
        }
    }

    fn propose_raft_command(&mut self, mut msg: RaftCmdRequest, cb: Callback<EK::Snapshot>) {
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
            apply::notify_req_region_removed(self.region_id(), cb);
            return;
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
        // The peer that is being checked is a leader. It might step down to be a follower later. It
        // doesn't matter whether the peer is a leader or not. If it's not a leader, the proposing
        // command log entry can't be committed.

        let mut resp = RaftCmdResponse::default();
        let term = self.fsm.peer.term();
        bind_term(&mut resp, term);
        if self.fsm.peer.propose(self.ctx, cb, msg, resp) {
            self.fsm.has_ready = true;
        }

        if self.fsm.peer.should_wake_up {
            self.reset_raft_tick(GroupState::Ordered);
        }

        self.register_pd_heartbeat_tick();

        // TODO: add timeout, if the command is not applied after timeout,
        // we will call the callback with timeout error.
    }

    fn find_sibling_region(&self) -> Option<Region> {
        let start = if self.ctx.cfg.right_derive_when_split {
            Included(enc_start_key(self.fsm.peer.region()))
        } else {
            Excluded(enc_end_key(self.fsm.peer.region()))
        };
        let meta = self.ctx.store_meta.lock().unwrap();
        meta.region_ranges
            .range((start, Unbounded::<Vec<u8>>))
            .next()
            .map(|(_, region_id)| meta.regions[region_id].to_owned())
    }

    fn register_raft_gc_log_tick(&mut self) {
        self.schedule_tick(PeerTicks::RAFT_LOG_GC)
    }

    #[allow(clippy::if_same_then_else)]
    fn on_raft_gc_log_tick(&mut self, force_compact: bool) {
        if !self.fsm.peer.get_store().is_cache_empty() || !self.ctx.cfg.hibernate_regions {
            self.register_raft_gc_log_tick();
        }
        fail_point!("on_raft_log_gc_tick_1", self.fsm.peer_id() == 1, |_| {});
        fail_point!("on_raft_gc_log_tick", |_| {});
        debug_assert!(!self.fsm.stopped);

        // As leader, we would not keep caches for the peers that didn't response heartbeat in the
        // last few seconds. That happens probably because another TiKV is down. In this case if we
        // do not clean up the cache, it may keep growing.
        let drop_cache_duration =
            self.ctx.cfg.raft_heartbeat_interval() + self.ctx.cfg.raft_entry_cache_life_time.0;
        let cache_alive_limit = Instant::now() - drop_cache_duration;

        let mut total_gc_logs = 0;

        let applied_idx = self.fsm.peer.get_store().applied_index();
        if !self.fsm.peer.is_leader() {
            self.fsm.peer.mut_store().compact_to(applied_idx + 1);
            return;
        }

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
        let truncated_idx = self.fsm.peer.get_store().truncated_index();
        let last_idx = self.fsm.peer.get_store().last_index();
        let (mut replicated_idx, mut alive_cache_idx) = (last_idx, last_idx);
        for (peer_id, p) in self.fsm.peer.raft_group.raft.prs().iter() {
            if replicated_idx > p.matched {
                replicated_idx = p.matched;
            }
            if let Some(last_heartbeat) = self.fsm.peer.peer_heartbeats.get(peer_id) {
                if alive_cache_idx > p.matched
                    && p.matched >= truncated_idx
                    && *last_heartbeat > cache_alive_limit
                {
                    alive_cache_idx = p.matched;
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
        self.fsm
            .peer
            .mut_store()
            .maybe_gc_cache(alive_cache_idx, applied_idx);

        let first_idx = self.fsm.peer.get_store().first_index();

        let mut compact_idx = if force_compact
            // Too many logs between applied index and first index.
            || (applied_idx > first_idx && applied_idx - first_idx >= self.ctx.cfg.raft_log_gc_count_limit)
            // Raft log size ecceeds the limit.
            || (self.fsm.peer.raft_log_size_hint >= self.ctx.cfg.raft_log_gc_size_limit.0)
        {
            applied_idx
        } else if replicated_idx < first_idx || last_idx - first_idx < 3 {
            // In the current implementation one compaction can't delete all stale Raft logs.
            // There will be at least 3 entries left after one compaction:
            // |------------- entries needs to be compacted ----------|
            // [entries...][the entry at `compact_idx`][the last entry][new compaction entry]
            //             |-------------------- entries will be left ----------------------|
            return;
        } else if replicated_idx - first_idx < self.ctx.cfg.raft_log_gc_threshold
            && self.fsm.skip_gc_raft_log_ticks < self.ctx.cfg.raft_log_reserve_max_ticks
        {
            // Logs will only be kept `max_ticks` * `raft_log_gc_tick_interval`.
            self.fsm.skip_gc_raft_log_ticks += 1;
            self.register_raft_gc_log_tick();
            return;
        } else {
            replicated_idx
        };
        assert!(compact_idx >= first_idx);
        // Have no idea why subtract 1 here, but original code did this by magic.
        compact_idx -= 1;
        if compact_idx < first_idx {
            // In case compact_idx == first_idx before subtraction.
            return;
        }
        total_gc_logs += compact_idx - first_idx;

        // Create a compact log request and notify directly.
        let region_id = self.fsm.peer.region().get_id();
        let peer = self.fsm.peer.peer.clone();
        let term = self.fsm.peer.get_index_term(compact_idx);
        let request = new_compact_log_request(region_id, peer, compact_idx, term);
        self.propose_raft_command(request, Callback::None);

        self.fsm.skip_gc_raft_log_ticks = 0;
        self.register_raft_gc_log_tick();
        PEER_GC_RAFT_LOG_COUNTER.inc_by(total_gc_logs as i64);
    }

    fn register_split_region_check_tick(&mut self) {
        self.schedule_tick(PeerTicks::SPLIT_REGION_CHECK)
    }

    #[inline]
    fn region_split_skip_max_count(&self) -> usize {
        fail_point!("region_split_skip_max_count", |_| { usize::max_value() });
        REGION_SPLIT_SKIP_MAX_COUNT
    }

    fn on_split_region_check_tick(&mut self) {
        if !self.ctx.cfg.hibernate_regions {
            self.register_split_region_check_tick();
        }
        if !self.fsm.peer.is_leader() {
            return;
        }

        // To avoid frequent scan, we only add new scan tasks if all previous tasks
        // have finished.
        // TODO: check whether a gc progress has been started.
        if self.ctx.split_check_scheduler.is_busy() {
            self.register_split_region_check_tick();
            return;
        }

        // When restart, the approximate size will be None. The split check will first
        // check the region size, and then check whether the region should split. This
        // should work even if we change the region max size.
        // If peer says should update approximate size, update region size and check
        // whether the region should split.
        if self.fsm.peer.approximate_size.is_some()
            && self.fsm.peer.compaction_declined_bytes < self.ctx.cfg.region_split_check_diff.0
            && self.fsm.peer.size_diff_hint < self.ctx.cfg.region_split_check_diff.0
        {
            return;
        }

        // bulk insert too fast may cause snapshot stale very soon, worst case it stale before
        // sending. so when snapshot is generating or sending, skip split check at most 3 times.
        // There is a trade off between region size and snapshot success rate. Split check is
        // triggered every 10 seconds. If a snapshot can't be generated in 30 seconds, it might be
        // just too large to be generated. Split it into smaller size can help generation. check
        // issue 330 for more info.
        if self.fsm.peer.get_store().is_generating_snapshot()
            && self.fsm.skip_split_count < self.region_split_skip_max_count()
        {
            self.fsm.skip_split_count += 1;
            return;
        }
        self.fsm.skip_split_count = 0;

        let task =
            SplitCheckTask::split_check(self.fsm.peer.region().clone(), true, CheckPolicy::Scan);
        if let Err(e) = self.ctx.split_check_scheduler.schedule(task) {
            error!(
                "failed to schedule split check";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "err" => %e,
            );
        }
        self.fsm.peer.size_diff_hint = 0;
        self.fsm.peer.compaction_declined_bytes = 0;
        self.register_split_region_check_tick();
    }

    fn on_prepare_split_region(
        &mut self,
        region_epoch: metapb::RegionEpoch,
        split_keys: Vec<Vec<u8>>,
        cb: Callback<EK::Snapshot>,
        source: &str,
    ) {
        info!(
            "on split";
            "region_id" => self.fsm.region_id(),
            "peer_id" => self.fsm.peer_id(),
            "split_keys" => %KeysInfoFormatter(split_keys.iter()),
            "source" => source,
        );
        if let Err(e) = self.validate_split_region(&region_epoch, &split_keys) {
            cb.invoke_with_response(new_error(e));
            return;
        }
        let region = self.fsm.peer.region();
        let task = PdTask::AskBatchSplit {
            region: region.clone(),
            split_keys,
            peer: self.fsm.peer.peer.clone(),
            right_derive: self.ctx.cfg.right_derive_when_split,
            callback: cb,
        };
        if let Err(Stopped(t)) = self.ctx.pd_scheduler.schedule(task) {
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
            return Err(box_err!("{} no split key is specified.", self.fsm.peer.tag));
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
                    self.fsm.peer.tag
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
                    self.fsm.peer.tag, latest_epoch, epoch
                ),
                vec![region.to_owned()],
            ));
        }
        Ok(())
    }

    fn on_approximate_region_size(&mut self, size: u64) {
        self.fsm.peer.approximate_size = Some(size);
        self.register_split_region_check_tick();
        self.register_pd_heartbeat_tick();
    }

    fn on_approximate_region_keys(&mut self, keys: u64) {
        self.fsm.peer.approximate_keys = Some(keys);
        self.register_split_region_check_tick();
        self.register_pd_heartbeat_tick();
    }

    fn on_compaction_declined_bytes(&mut self, declined_bytes: u64) {
        self.fsm.peer.compaction_declined_bytes += declined_bytes;
        if self.fsm.peer.compaction_declined_bytes >= self.ctx.cfg.region_split_check_diff.0 {
            UPDATE_REGION_SIZE_BY_COMPACTION_COUNTER.inc();
        }
        self.register_split_region_check_tick();
    }

    fn on_schedule_half_split_region(
        &mut self,
        region_epoch: &metapb::RegionEpoch,
        policy: CheckPolicy,
        source: &str,
    ) {
        info!(
            "on half split";
            "region_id" => self.fsm.region_id(),
            "peer_id" => self.fsm.peer_id(),
            "policy" => ?policy,
            "source" => source,
        );
        if !self.fsm.peer.is_leader() {
            // region on this store is no longer leader, skipped.
            warn!(
                "not leader, skip";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
            );
            return;
        }

        let region = self.fsm.peer.region();
        if util::is_epoch_stale(region_epoch, region.get_region_epoch()) {
            warn!(
                "receive a stale halfsplit message";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
            );
            return;
        }

        let task = SplitCheckTask::split_check(region.clone(), false, policy);
        if let Err(e) = self.ctx.split_check_scheduler.schedule(task) {
            error!(
                "failed to schedule split check";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
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
        self.schedule_tick(PeerTicks::PD_HEARTBEAT)
    }

    fn on_check_peer_stale_state_tick(&mut self) {
        if self.fsm.peer.pending_remove {
            return;
        }

        self.register_check_peer_stale_state_tick();

        if self.fsm.peer.is_applying_snapshot() || self.fsm.peer.has_pending_snapshot() {
            return;
        }

        if self.ctx.cfg.hibernate_regions {
            let group_state = self.fsm.hibernate_state.group_state();
            if group_state == GroupState::Idle {
                self.fsm.peer.ping();
                if !self.fsm.peer.is_leader() {
                    // If leader is able to receive messge but can't send out any,
                    // follower should be able to start an election.
                    self.fsm.hibernate_state.reset(GroupState::PreChaos);
                } else {
                    self.fsm.has_ready = true;
                    // Schedule a pd heartbeat to discover down and pending peer when
                    // hibernate_regions is enabled.
                    self.register_pd_heartbeat_tick();
                }
            } else if group_state == GroupState::PreChaos {
                self.fsm.hibernate_state.reset(GroupState::Chaos);
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
        // and it would check with pd to confirm whether it's still a member of the cluster.
        // If not, it destroys itself as a stale peer which is removed out already.
        let state = self.fsm.peer.check_stale_state(self.ctx);
        fail_point!("peer_check_stale_state", state != StaleState::Valid, |_| {});
        match state {
            StaleState::Valid => (),
            StaleState::LeaderMissing => {
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
            StaleState::ToValidate => {
                // for peer B in case 1 above
                warn!(
                    "leader missing longer than max_leader_missing_duration. \
                     To check with pd and other peers whether it's still valid";
                    "region_id" => self.fsm.region_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "expect" => %self.ctx.cfg.max_leader_missing_duration,
                );

                self.fsm.peer.bcast_check_stale_peer_message(&mut self.ctx);

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
        self.schedule_tick(PeerTicks::CHECK_PEER_STALE_STATE)
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
        self.propose_raft_command(req, Callback::None);
    }

    fn on_ingest_sst_result(&mut self, ssts: Vec<SstMeta>) {
        for sst in &ssts {
            self.fsm.peer.size_diff_hint += sst.get_length();
        }
        self.register_split_region_check_tick();
    }

    /// Verify and store the hash to state. return true means the hash has been stored successfully.
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
            // Maybe computing is too slow or computed result is dropped due to channel full.
            // If computing is too slow, miss count will be increased twice.
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

/// Checks merge target, returns whether the source peer should be destroyed and whether the source peer is
/// merged to this target peer.
///
/// It returns (`can_destroy`, `merge_to_this_peer`).
///
/// `can_destroy` is true when there is a network isolation which leads to a follower of a merge target
/// Region's log falls behind and then receive a snapshot with epoch version after merge.
///
/// `merge_to_this_peer` is true when `can_destroy` is true and the source peer is merged to this target peer.
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
            // The target peer will move on, namely, it will apply a snapshot generated after merge,
            // so destroy source peer.
            if region_epoch.get_version() > target_region.get_region_epoch().get_version() {
                return (
                    true,
                    target_peer_id
                        == util::find_peer(target_region, meta.store_id.unwrap())
                            .unwrap()
                            .get_id(),
                );
            }
            // Wait till the target peer has caught up logs and source peer will be destroyed at that time.
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
) -> RaftCmdRequest {
    let mut request = new_admin_request(region_id, peer);

    let mut admin = AdminRequest::default();
    admin.set_cmd_type(AdminCmdType::CompactLog);
    admin.mut_compact_log().set_compact_index(compact_index);
    admin.mut_compact_log().set_compact_term(compact_term);
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

impl<EK: KvEngine, ER: RaftEngine> AbstractPeer for PeerFsm<EK, ER> {
    fn meta_peer(&self) -> &metapb::Peer {
        &self.peer.peer
    }
    fn group_state(&self) -> GroupState {
        self.hibernate_state.group_state()
    }
    fn region(&self) -> &metapb::Region {
        self.peer.raft_group.store().region()
    }
    fn apply_state(&self) -> &RaftApplyState {
        self.peer.raft_group.store().apply_state()
    }
    fn raft_status(&self) -> raft::Status {
        self.peer.raft_group.status()
    }
    fn raft_commit_index(&self) -> u64 {
        self.peer.raft_group.store().commit_index()
    }
    fn raft_request_snapshot(&mut self, index: u64) {
        self.peer.raft_group.request_snapshot(index).unwrap();
    }
    fn pending_merge_state(&self) -> Option<&MergeState> {
        self.peer.pending_merge_state.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::BatchRaftCmdRequestBuilder;
    use crate::store::local_metrics::RaftProposeMetrics;
    use crate::store::msg::{Callback, ExtCallback, RaftCommand};

    use engine_test::kv::KvTestEngine;
    use kvproto::raft_cmdpb::{
        AdminRequest, CmdType, PutRequest, RaftCmdRequest, RaftCmdResponse, Request, Response,
        StatusRequest,
    };
    use protobuf::Message;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[test]
    fn test_batch_raft_cmd_request_builder() {
        let max_batch_size = 1000.0;
        let mut builder = BatchRaftCmdRequestBuilder::<KvTestEngine>::new(max_batch_size);
        let mut q = Request::default();
        let mut metric = RaftProposeMetrics::default();

        let mut req = RaftCmdRequest::default();
        req.set_admin_request(AdminRequest::default());
        assert!(!builder.can_batch(&req, 0));

        let mut req = RaftCmdRequest::default();
        req.set_status_request(StatusRequest::default());
        assert!(!builder.can_batch(&req, 0));

        let mut req = RaftCmdRequest::default();
        let mut put = PutRequest::default();
        put.set_key(b"aaaa".to_vec());
        put.set_value(b"bbbb".to_vec());
        q.set_cmd_type(CmdType::Put);
        q.set_put(put);
        req.mut_requests().push(q.clone());
        let _ = q.take_put();
        let req_size = req.compute_size();
        assert!(builder.can_batch(&req, req_size));

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
        assert!(!builder.can_batch(&req, req_size));

        let mut req = RaftCmdRequest::default();
        let mut put = PutRequest::default();
        put.set_key(b"aaaa".to_vec());
        put.set_value(vec![8_u8; 2000]);
        q.set_cmd_type(CmdType::Put);
        q.set_put(put);
        req.mut_requests().push(q.clone());
        let req_size = req.compute_size();
        assert!(!builder.can_batch(&req, req_size));

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
        let mut cmd = builder.build(&mut metric).unwrap();
        cmd.callback.invoke_proposed();
        for flag in proposed_cbs_flags {
            assert!(flag.load(Ordering::Acquire));
        }
        cmd.callback.invoke_committed();
        for flag in committed_cbs_flags {
            assert!(flag.load(Ordering::Acquire));
        }
        assert_eq!(10, cmd.request.get_requests().len());
        cmd.callback.invoke_with_response(response);
        for flag in cbs_flags {
            assert!(flag.load(Ordering::Acquire));
        }
    }
}
