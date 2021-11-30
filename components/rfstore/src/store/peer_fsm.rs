// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{SSTMetaInfo};
use error_code::ErrorCodeExt;
use fail::fail_point;
use kvproto::errorpb;
use kvproto::import_sstpb::SwitchMode;
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
use raft::eraftpb::{ConfChangeType, Entry, EntryType, MessageType};
use raft::{self, Progress, ReadState, Ready, SnapshotStatus, StateRole, INVALID_INDEX, NO_LIMIT};
use std::borrow::Cow;
use std::collections::Bound::{Excluded, Unbounded};
use std::collections::{HashMap, VecDeque};
use std::iter::Iterator;
use std::ops::{Deref, DerefMut};
use std::time::Instant;
use std::{cmp, mem, u64};
use tikv_alloc::trace::TraceEvent;
use tikv_util::mpsc::{self, Receiver, Sender};
use tikv_util::sys::{disk, memory_usage_reaches_high_water};
use tikv_util::time::duration_to_sec;
use tikv_util::worker::{Scheduler, Stopped};
use tikv_util::{box_err, debug, error, info, trace, warn};
use tikv_util::{escape, is_zero_duration, Either};

use self::memtrace::*;
use crate::store::cmd_resp::{bind_term, new_error};
use crate::store::msg::{Callback, ExtCallback};
use crate::store::peer::{ConsistencyState, Peer, StaleState};
use crate::store::peer_storage::InvokeContext;
use crate::store::transport::Transport;
use crate::store::{ApplySnapResult, notify_req_region_removed, PdTask, ReadyICPair};
use crate::store::{apply, ApplyMetrics, ChangePeer, ExecResult, MsgApply};
use crate::store::{
    raw_end_key, raw_start_key, region_state_key, rlog, ApplyMsg, Engines, MsgApplyResult,
    PeerTick, RaftContext, ReadDelegate, StoreMeta, Ticker, PEER_TICK_PD_HEARTBEAT,
    PEER_TICK_RAFT, PEER_TICK_RAFT_LOG_GC, PEER_TICK_SPLIT_CHECK, PEER_TICK_STALE_STATE_CHECK,
};
use crate::store::{
    util, CasualMessage, Config, MergeResultKind, PeerMsg, RaftCommand, SignificantMsg, StoreMsg,
};
use crate::store::{RegionTask, SplitCheckTask};
use crate::{Error, Result};
use raftstore::coprocessor::RegionChangeEvent;
use raftstore::store::local_metrics::RaftProposeMetrics;
use raftstore::store::memory::*;
use raftstore::store::metrics::*;
use raftstore::store::util as outil;
use txn_types::WriteBatchFlags;

/// Limits the maximum number of regions returned by error.
///
/// Another choice is using coprocessor batch limit, but 10 should be a good fit in most case.
const MAX_REGIONS_IN_ERROR: usize = 10;
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

pub struct PeerFsm {
    pub peer: Peer,
    stopped: bool,
    has_ready: bool,
    ticker: Ticker,
}

pub struct BatchRaftCmdRequestBuilder {
    raft_entry_max_size: f64,
    batch_req_size: u32,
    request: Option<rlog::RaftLog>,
    callbacks: Vec<(Callback, usize)>,
}

impl PeerFsm {
    // If we create the peer actively, like bootstrap/split/merge region, we should
    // use this function to create the peer. The region must contain the peer info
    // for this store.
    pub fn create(
        store_id: u64,
        cfg: &Config,
        sched: Sender<RegionTask>,
        engines: Engines,
        region: &metapb::Region,
    ) -> Result<PeerFsm> {
        let meta_peer = match outil::find_peer(region, store_id) {
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
        Ok(PeerFsm {
            peer: Peer::new(store_id, cfg, engines, sched, region, meta_peer)?,
            stopped: false,
            has_ready: false,
            ticker: Ticker::new(region.get_id(), cfg),
        })
    }

    // The peer can be created from another node with raft membership changes, and we only
    // know the region_id and peer_id when creating this replicated peer, the region info
    // will be retrieved later after applying snapshot.
    pub fn replicate(
        store_id: u64,
        cfg: &Config,
        sched: Sender<RegionTask>,
        engines: Engines,
        region_id: u64,
        peer: metapb::Peer,
    ) -> Result<PeerFsm> {
        // We will remove tombstone key when apply snapshot
        info!(
            "replicate peer";
            "region_id" => region_id,
            "peer_id" => peer.get_id(),
        );

        let mut region = metapb::Region::default();
        region.set_id(region_id);

        HIBERNATED_PEER_STATE_GAUGE.awaken.inc();
        Ok(PeerFsm {
                peer: Peer::new(store_id, cfg, engines, sched, &region, peer)?,
                stopped: false,
                has_ready: false,
                ticker: Ticker::new(region_id, cfg),
            }
        )
    }

    #[inline]
    pub fn region_id(&self) -> u64 {
        self.peer.region().get_id()
    }

    #[inline]
    pub fn get_peer(&self) -> &Peer {
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

impl BatchRaftCmdRequestBuilder {
    fn new(raft_entry_max_size: f64) -> BatchRaftCmdRequestBuilder {
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

    fn add(&mut self, cmd: RaftCommand, req_size: u32) {
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
        }
        false
    }

    fn build(&mut self, metric: &mut RaftProposeMetrics) -> Option<RaftCommand> {
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

pub struct PeerMsgHandler<'a> {
    fsm: &'a mut PeerFsm,
    ctx: &'a mut RaftContext,
}

impl<'a> Deref for PeerMsgHandler<'a> {
    type Target = PeerFsm;

    fn deref(&self) -> &Self::Target {
        self.fsm
    }
}

impl<'a> DerefMut for PeerMsgHandler<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.fsm
    }
}

impl<'a> PeerMsgHandler<'a> {
    pub fn new(fsm: &'a mut PeerFsm, ctx: &'a mut RaftContext) -> PeerMsgHandler<'a> {
        PeerMsgHandler { fsm, ctx }
    }

    pub fn handle_msgs(&mut self, msgs: &mut Vec<PeerMsg>) {
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
                    // TODO
                    // self.ctx
                    //     .raft_metrics
                    //     .propose
                    //     .request_wait_time
                    //     .observe(duration_to_sec(cmd.send_time.elapsed()) as f64);
                    if let Some(Err(e)) = cmd.deadline.map(|deadline| deadline.check()) {
                        cmd.callback.invoke_with_response(new_error(e.into()));
                        continue;
                    }
                    self.propose_raft_command(cmd.request, cmd.callback);
                }
                PeerMsg::Tick => self.on_tick(),
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
                PeerMsg::DestroyRes {
                    peer_id,
                    merge_from_snapshot,
                } => {
                    self.on_destroy_res(peer_id, merge_from_snapshot);
                }
            }
        }
    }

    fn on_casual_msg(&mut self, msg: CasualMessage) {
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
            CasualMessage::HalfSplitRegion {
                region_epoch,
                policy,
                source,
            } => {
                self.on_schedule_half_split_region(&region_epoch, policy, source);
            }
            CasualMessage::RegionOverlapped => {
                debug!("start ticking for overlapped"; "region_id" => self.region_id(), "peer_id" => self.fsm.peer_id());
                // Maybe do some safe check first?
                self.register_raft_base_tick();

                if outil::is_learner(&self.fsm.peer.peer) {
                    // FIXME: should use `bcast_check_stale_peer_message` instead.
                    // Sending a new enum type msg to a old tikv may cause panic during rolling update
                    // we should change the protobuf behavior and check if properly handled in all place
                    self.fsm.peer.bcast_wake_up_message(&mut self.ctx);
                }
            }
            CasualMessage::ForceCompactRaftLogs => {
                self.on_raft_gc_log_tick();
            }
            CasualMessage::QueryRegionLeaderResp { region, leader } => {}
        }
    }

    fn on_tick(&mut self) {
        if self.fsm.stopped {
            return;
        }
        trace!(
            "tick";
            "peer_id" => self.fsm.peer_id(),
            "region_id" => self.region_id(),
        );
        self.ticker.tick_clock();
        if self.ticker.is_on_tick(PEER_TICK_RAFT) {
            self.on_raft_base_tick();
        }
        if self.ticker.is_on_tick(PEER_TICK_RAFT_LOG_GC) {
            self.on_raft_gc_log_tick();
        }
        if self.ticker.is_on_tick(PEER_TICK_PD_HEARTBEAT) {
            self.on_pd_heartbeat_tick();
        }
    }

    fn start(&mut self) {
        self.ticker.schedule(PEER_TICK_RAFT);
        self.ticker.schedule(PEER_TICK_RAFT_LOG_GC);
        self.ticker.schedule(PEER_TICK_PD_HEARTBEAT);
        self.ticker.schedule(PEER_TICK_SPLIT_CHECK);
        self.ticker.schedule(PEER_TICK_STALE_STATE_CHECK);
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

    fn on_significant_msg(&mut self, msg: SignificantMsg) {
        match msg {
            SignificantMsg::Unreachable { to_peer_id, .. } => {
                if self.fsm.peer.is_leader() {
                    self.fsm.peer.raft_group.report_unreachable(to_peer_id);
                }
            }
            SignificantMsg::StoreUnreachable { store_id } => {
                if let Some(peer_id) = outil::find_peer(self.region(), store_id).map(|p| p.get_id())
                {
                    if self.fsm.peer.is_leader() {
                        self.fsm.peer.raft_group.report_unreachable(peer_id);
                    }
                }
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
            SignificantMsg::LeaderCallback(_) => {}
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

    fn on_leader_callback(&mut self, cb: Callback) {
        let msg = new_read_index_request(
            self.region_id(),
            self.region().get_region_epoch().clone(),
            self.fsm.peer.peer.clone(),
        );
        self.propose_raft_command(rlog::RaftLog::Request(msg), cb);
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

    pub(crate) fn new_raft_ready(&mut self) -> Option<CollectedReady> {
        let has_ready = self.has_ready;
        self.has_ready = false;
        if !has_ready || self.stopped {
            return None;
        }
        if let Some(ready) = self.peer.new_raft_ready(self.ctx) {
            if let Some(ss) = ready.ready.ss() {
                if ss.raft_state == raft::StateRole::Leader {
                    self.peer.heartbeat_pd(self.ctx)
                }
            }
            Some(ready)
        } else {
            None
        }
    }

    pub fn handle_raft_ready(&mut self, ready: &mut CollectedReady) {
        if ready.ctx.region.get_id() != self.fsm.region_id() {
            panic!(
                "{} region id not matched: {} # {}",
                self.fsm.peer.tag,
                ready.ctx.region.get_id(),
                self.fsm.region_id()
            );
        }
        let CollectedReady { ctx, mut ready, .. } = ready;
        let res = self
            .fsm
            .peer
            .post_raft_ready_append(self.ctx, ctx, &mut ready);
        self.fsm.peer.handle_raft_ready_advance(self.ctx, ready);
        if let Some(apply_res) = res {
            self.on_ready_apply_snapshot(apply_res);
        }
        if self.fsm.peer.leader_unreachable {
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

    fn on_raft_base_tick(&mut self) {
        if self.peer.pending_remove {
            self.peer.mut_store().flush_cache_metrics();
            return;
        }
        // When having pending snapshot, if election timeout is met, it can't pass
        // the pending conf change check because first index has been updated to
        // a value that is larger than last index.
        if self.fsm.peer.is_applying_snapshot() || self.fsm.peer.has_pending_snapshot() {
            // need to check if snapshot is applied.
            self.fsm.has_ready = true;
            self.ticker.schedule(PEER_TICK_RAFT);
            return;
        }
        if self.fsm.peer.raft_group.tick() {
            self.fsm.has_ready = true;
        }
        self.fsm.peer.mut_store().flush_cache_metrics();
        self.ticker.schedule(PEER_TICK_RAFT)
    }

    fn on_apply_res(&mut self, mut res: MsgApplyResult) {
        fail_point!("on_apply_res", |_| {});
        debug!(
            "async apply finish";
            "region_id" => self.region_id(),
            "peer_id" => self.fsm.peer_id(),
            "res" => ?res,
        );
        self.on_ready_result(&mut res.results, &res.metrics);
        if self.fsm.stopped {
            return;
        }
        self.fsm.has_ready |= self.fsm.peer.post_apply(
            self.ctx,
            res.apply_state,
            &res.metrics,
        );
        // After applying, several metrics are updated, report it to pd to
        // get fair schedule.
        if self.fsm.peer.is_leader() {
            self.ticker.schedule(PEER_TICK_PD_HEARTBEAT);
            self.ticker.schedule(PEER_TICK_SPLIT_CHECK);
        }
    }

    fn on_raft_message(&mut self, mut msg: RaftMessage) -> Result<()> {
        debug!(
            "handle raft message";
            "region_id" => self.region_id(),
            "peer_id" => self.fsm.peer_id(),
            "message_type" => %outil::MsgType(&msg),
            "from_peer_id" => msg.get_from_peer().get_id(),
            "to_peer_id" => msg.get_to_peer().get_id(),
        );

        let msg_type = msg.get_message().get_msg_type();
        let store_id = self.ctx.store_id();

        if disk::disk_full_precheck(store_id) || self.ctx.is_disk_full {
            let mut flag = false;
            if MessageType::MsgAppend == msg_type {
                let entries = msg.get_message().get_entries();
                for i in entries {
                    let entry_type = i.get_entry_type();
                    if EntryType::EntryNormal == entry_type && !i.get_data().is_empty() {
                        flag = true;
                        break;
                    }
                }
            } else if MessageType::MsgTimeoutNow == msg_type {
                flag = true;
            }

            if flag {
                debug!(
                    "skip {:?} because of disk full", msg_type;
                    "region_id" => self.region_id(), "peer_id" => self.fsm.peer_id()
                );
                return Err(Error::Timeout("disk full".to_owned()));
            }
        }

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
        let regions_to_destroy = self.check_snapshot(&msg)?;

        if !self.check_request_snapshot(&msg) {
            return Ok(());
        }

        let from_peer_id = msg.get_from_peer().get_id();
        self.fsm.peer.insert_peer_cache(msg.take_from_peer());

        let result = self.fsm.peer.step(msg.take_message());

        if is_snapshot {
            if !self.fsm.peer.has_pending_snapshot() {
                // This snapshot is rejected by raft-rs.
                let mut meta = self.ctx.global.store_meta.lock().unwrap();
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
            self.fsm.peer.heartbeat_pd(&self.ctx);
        }

        self.fsm.has_ready = true;
        Ok(())
    }

    fn on_extra_message(&mut self, mut msg: RaftMessage) {
        match msg.get_extra_msg().get_type() {
            ExtraMessageType::MsgRegionWakeUp | ExtraMessageType::MsgCheckStalePeer => {}
            ExtraMessageType::MsgWantRollbackMerge => {
                todo!()
            }
            ExtraMessageType::MsgCheckStalePeerResponse => {
                todo!()
            }
            ExtraMessageType::MsgHibernateRequest => {}
            ExtraMessageType::MsgHibernateResponse => {}
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
        if outil::is_epoch_stale(from_epoch, self.fsm.peer.region().get_region_epoch())
            && outil::find_peer(self.fsm.peer.region(), from_store_id).is_none()
        {
            self.ctx
                .handle_stale_msg(msg, self.fsm.peer.region().get_region_epoch().clone(), None);
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
                match self.fsm.peer.maybe_destroy() {
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
        let mut meta = self.ctx.global.store_meta.lock().unwrap();
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
            if outil::is_epoch_stale(merge_target.get_region_epoch(), r.get_region_epoch()) {
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
        if !outil::is_epoch_stale(self.fsm.peer.region().get_region_epoch(), from_epoch) {
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
        match self.fsm.peer.maybe_destroy() {
            None => self.ctx.raft_metrics.message_dropped.applying_snap += 1,
            Some(job) => {
                self.handle_destroy_peer(job);
            }
        }
    }

    // Returns `Vec<(u64, bool)>` indicated (source_region_id, merge_to_this_peer) if the `msg`
    // doesn't contain a snapshot or this snapshot doesn't conflict with any other snapshots or regions.
    // Otherwise a `SnapKey` is returned.
    fn check_snapshot(&mut self, msg: &RaftMessage) -> Result<Vec<(u64, bool)>> {
        if !msg.get_message().has_snapshot() {
            return Ok(vec![]);
        }

        let region_id = msg.get_region_id();
        let snap = msg.get_message().get_snapshot();
        let mut snap_data = RaftSnapshotData::default();
        snap_data.merge_from_bytes(snap.get_data())?;
        let snap_region = snap_data.take_region();
        let peer_id = msg.get_to_peer().get_id();
        let snap_start_key = raw_start_key(&snap_region);
        let snap_end_key = raw_end_key(&snap_region);

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
            return Ok(vec![]);
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
            return Ok(vec![]);
        }

        let mut meta = self.ctx.global.store_meta.lock().unwrap();
        if meta.regions[&self.region_id()] != *self.region() {
            if !self.fsm.peer.is_initialized() {
                info!(
                    "stale delegate detected, skip";
                    "region_id" => self.fsm.region_id(),
                    "peer_id" => self.fsm.peer_id(),
                );
                self.ctx.raft_metrics.message_dropped.stale_msg += 1;
                return Ok(vec![]);
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
            return Ok(vec![]);
        }

        for region in &meta.pending_snapshot_regions {
            if raw_start_key(region) < snap_end_key &&
                raw_start_key(region) > snap_start_key &&
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
                return Ok(vec![]);
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
            .range((Excluded(snap_start_key), Unbounded::<Vec<u8>>))
            .map(|(_, &region_id)| &meta.regions[&region_id])
            .take_while(|r| raw_start_key(r) < snap_end_key)
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
                let _ = self.ctx.global.router.send(
                    exist_region.get_id(),
                    PeerMsg::CasualMessage(CasualMessage::RegionOverlapped),
                );
            }
        }
        if is_overlapped {
            self.ctx.raft_metrics.message_dropped.region_overlap += 1;
            return Ok(vec![]);
        }
        meta.pending_snapshot_regions.push(snap_region);

        Ok(regions_to_destroy)
    }

    fn destroy_regions_for_snapshot(&mut self, regions_to_destroy: Vec<(u64, bool)>) {
        if regions_to_destroy.is_empty() {
            return;
        }
        todo!()
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
            self.ctx.apply_msgs.msgs.push(ApplyMsg::Destroy {
                region_id: job.region_id,
                merge_from_snapshot: false,
            });
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

        let mut meta = self.ctx.global.store_meta.lock().unwrap();

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
        self.ctx.global.coprocessor_host.on_region_changed(
            self.fsm.peer.region(),
            RegionChangeEvent::Destroy,
            self.fsm.peer.get_role(),
        );
        let task = PdTask::DestroyPeer { region_id };
        if let Err(e) = self.ctx.global.pd_scheduler.schedule(task) {
            error!(
                "failed to notify pd";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "err" => %e,
            );
        }
        let is_initialized = self.fsm.peer.is_initialized();
        if let Err(e) = self
            .fsm
            .peer
            .destroy(&self.ctx.global.engines, merged_by_target)
        {
            // If not panic here, the peer will be recreated in the next restart,
            // then it will be gc again. But if some overlap region is created
            // before restarting, the gc action will delete the overlap region's
            // data too.
            panic!("{} destroy err {:?}", self.fsm.peer.tag, e);
        }
        // Some places use `force_send().unwrap()` if the StoreMeta lock is held.
        // So in here, it's necessary to held the StoreMeta lock when closing the router.
        self.ctx.global.router.close(region_id);
        self.fsm.stop();

        if is_initialized
            && !merged_by_target
            && meta
                .region_ranges
                .remove(&raw_end_key(self.fsm.peer.region()))
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
            let mut meta = self.ctx.global.store_meta.lock().unwrap();
            meta.set_region(
                &self.ctx.global.coprocessor_host,
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

        self.fsm.peer.mut_store().cancel_generating_snap(None);

        if cp.index >= self.fsm.peer.raft_group.raft.raft_log.first_index() {
            match self.fsm.peer.raft_group.apply_conf_change(&cp.conf_change) {
                Ok(_) => {}
                // PD could dispatch redundant conf changes.
                Err(raft::Error::NotExists { .. }) | Err(raft::Error::Exists { .. }) => {}
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
            self.fsm.peer.heartbeat_pd(&self.ctx);

            // Remove or demote leader will cause this raft group unavailable
            // until new leader elected, but we can't revert this operation
            // because its result is already persisted in apply worker
            // TODO: should we transfer leader here?
            let demote_self = outil::is_learner(&self.fsm.peer.peer);
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
        self.ctx
            .raft_wb
            .truncate_raft_log(self.region_id(), state.index);
        self.peer.last_compacted_index = state.index;
    }

    fn on_ready_split_region(
        &mut self,
        derived: metapb::Region,
        regions: Vec<metapb::Region>,
        new_split_regions: HashMap<u64, apply::NewSplitPeer>,
    ) {
        fail_point!("on_split", self.ctx.store_id() == 3, |_| {});

        let region_id = derived.get_id();
        // Roughly estimate the size and keys for new regions.
        let new_region_count = regions.len() as u64;
        let mut meta = self.ctx.global.store_meta.lock().unwrap();
        meta.set_region(
            &self.ctx.global.coprocessor_host,
            derived,
            &mut self.fsm.peer,
        );

        let is_leader = self.fsm.peer.is_leader();
        if is_leader {
            self.fsm.peer.heartbeat_pd(&self.ctx);
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
            if let Err(e) = self.ctx.global.pd_scheduler.schedule(task) {
                error!(
                    "failed to notify pd";
                    "region_id" => self.fsm.region_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "err" => %e,
                );
            }
        }

        let last_key = raw_end_key(regions.last().unwrap());
        if meta.region_ranges.remove(&last_key).is_none() {
            panic!("{} original region should exist", self.fsm.peer.tag);
        }
        let last_region_id = regions.last().unwrap().get_id();
        for new_region in regions {
            let new_region_id = new_region.get_id();

            if new_region_id == region_id {
                let not_exist = meta
                    .region_ranges
                    .insert(raw_end_key(&new_region).to_vec(), new_region_id)
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
                    .clear_extra_split_data(raw_start_key(&new_region), raw_end_key(&new_region))
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
                if outil::is_region_initialized(r) {
                    panic!(
                        "[region {}] duplicated region {:?} for split region {:?}",
                        new_region_id, r, new_region
                    );
                }
                self.ctx.router.close(new_region_id);
            }

            let mut new_peer = match PeerFsm::create(
                self.ctx.store_id(),
                &self.ctx.cfg,
                self.ctx.region_scheduler.clone(),
                self.ctx.engines.clone(),
                &new_region,
            ) {
                Ok(new_peer) => new_peer,
                Err(e) => {
                    // peer information is already written into db, can't recover.
                    // there is probably a bug.
                    panic!("create new split region {:?} err {:?}", new_region, e);
                }
            };

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
                // The new peer is likely to become leader, send a heartbeat immediately to reduce
                // client query miss.
                new_peer.peer.heartbeat_pd(&self.ctx);
            }

            new_peer.peer.activate(&mut self.ctx.apply_msgs);
            meta.regions.insert(new_region_id, new_region.clone());
            let not_exist = meta
                .region_ranges
                .insert(raw_end_key(&new_region).to_vec(), new_region_id)
                .is_none();
            assert!(not_exist, "[region {}] should not exist", new_region_id);
            meta.readers
                .insert(new_region_id, ReadDelegate::from_peer(new_peer.get_peer()));
            meta.region_read_progress
                .insert(new_region_id, new_peer.peer.read_progress.clone());
            self.ctx.global.router.register(new_peer);
            self.ctx.global
                .router
                .send(new_region_id, PeerMsg::Start)
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
        }
        drop(meta);
        if is_leader {
            self.on_split_region_check_tick();
        }
        fail_point!("after_split", self.ctx.store_id() == 3, |_| {});
    }

    /// Check if merge target region is staler than the local one in kv engine.
    /// It should be called when target region is not in region map in memory.
    /// If everything is ok, the answer should always be true because PD should ensure all target peers exist.
    /// So if not, error log will be printed and return false.
    fn is_merge_target_region_stale(&self, target_region: &metapb::Region) -> Result<bool> {
        todo!()
    }

    fn validate_merge_peer(&self, target_region: &metapb::Region) -> Result<bool> {
        todo!()
    }

    fn schedule_merge(&mut self) -> Result<()> {
        todo!()
    }

    fn rollback_merge(&mut self) {
        todo!()
    }

    fn on_check_merge(&mut self) {
        todo!()
    }

    fn on_ready_prepare_merge(&mut self, region: metapb::Region, state: MergeState) {
        todo!()
    }

    fn on_merge_result(
        &mut self,
        target_region_id: u64,
        target: metapb::Peer,
        result: MergeResultKind,
    ) {
        todo!()
    }

    fn on_stale_merge(&mut self, target_region_id: u64) {
        todo!()
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

        let mut meta = self.ctx.global.store_meta.lock().unwrap();
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
            let prev = meta.region_ranges.remove(&raw_end_key(&r));
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

        if outil::is_region_initialized(&prev_region) {
            info!(
                "region changed after applying snapshot";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "prev_region" => ?prev_region,
                "region" => ?region,
            );
            let prev = meta.region_ranges.remove(&raw_end_key(&prev_region));
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
            .insert(raw_end_key(&region).to_vec(), region.get_id())
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

        for r in &apply_result.destroyed_regions {
            if let Err(e) = self.ctx.router.force_send(
                r.get_id(),
                PeerMsg::SignificantMsg(SignificantMsg::MergeResult {
                    target_region_id: self.fsm.region_id(),
                    target: self.fsm.peer.peer.clone(),
                    result: MergeResultKind::FromTargetSnapshotStep2,
                }),
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

    fn on_ready_result(&mut self, exec_results: &mut VecDeque<ExecResult>, metrics: &ApplyMetrics) {
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
                } => self.on_ready_compute_hash(region, index, context),
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
        self.ctx.local_stats.lock_cf_bytes_written += metrics.lock_cf_written_bytes;
        self.ctx.local_stats.engine_total_bytes_written += metrics.written_bytes;
        self.ctx.local_stats.engine_total_keys_written += metrics.written_keys;
        self.ctx
            .local_stats
            .engine_total_query_stats
            .add_query_stats(&metrics.written_query_stats.0);
    }

    /// Check if a request is valid if it has valid prepare_merge/commit_merge proposal.
    fn check_merge_proposal(&self, msg: &mut RaftCmdRequest) -> Result<()> {
        todo!()
    }

    fn pre_propose_raft_command(&mut self, msg: &rlog::RaftLog) -> Result<Option<RaftCmdResponse>> {
        // Check store_id, make sure that the msg is dispatched to the right place.
        if let Err(e) = util::check_store_id(msg, self.store_id()) {
            self.ctx.raft_metrics.invalid_proposal.mismatch_store_id += 1;
            return Err(e);
        }
        if let Some(cmd) = msg.get_cmd_request() {
            if cmd.has_status_request() {
                // For status commands, we handle it here directly.
                let resp = self.execute_status_command(cmd)?;
                return Ok(Some(resp));
            }
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
        let flags = WriteBatchFlags::from_bits_check(msg.get_header().get_flags());
        let allow_stale_read = read_only && flags.contains(WriteBatchFlags::STALE_READ);
        if !self.fsm.peer.is_leader()
            && !is_read_index_request
            && !allow_replica_read
            && !allow_stale_read
        {
            self.ctx.raft_metrics.invalid_proposal.not_leader += 1;
            let leader = self.fsm.peer.get_peer_from_cache(leader_id);
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
            Err(Error::EpochNotMatch(m, mut new_regions)) => {
                // Attach the region which might be split from the current region. But it doesn't
                // matter if the region is not split from the current region. If the region meta
                // received by the TiKV driver is newer than the meta cached in the driver, the meta is
                // updated.
                let (requested_version, _) = msg.get_epoch();
                self.collect_sibling_region(requested_version, &mut new_regions);
                self.ctx.raft_metrics.invalid_proposal.epoch_not_match += 1;
                Err(Error::EpochNotMatch(m, new_regions))
            }
            Err(e) => Err(e),
            Ok(()) => Ok(None),
        }
    }

    fn propose_raft_command(&mut self, mut msg: rlog::RaftLog, cb: Callback) {
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
            notify_req_region_removed(self.region_id(), cb);
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
        let anchor = Excluded(raw_end_key(self.fsm.peer.region()));
        let meta = self.ctx.global.store_meta.lock().unwrap();
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
                    // It doesn't matter if it's a false positive, as it's limited by MAX_REGIONS_IN_ERROR.
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

    #[allow(clippy::if_same_then_else)]
    fn on_raft_gc_log_tick(&mut self) {
        self.ticker.schedule(PEER_TICK_RAFT_LOG_GC);
        let store = self.peer.get_store();
        let stable_idx = store.stable_apply_state.applied_index;
        if stable_idx == 0 {
            // No flushed L0 files yet, we delay the raft log GC.
            return;
        }
        if !self.peer.is_leader() {
            return;
        }
        let term = self.peer.raft_group.raft.raft_log.term(stable_idx).unwrap();
        // Create a compact log request and notify directly.
        let region_id = self.region_id();
        let request = new_compact_log_request(region_id, self.peer.peer.clone(), stable_idx, term);
        self.propose_raft_command(rlog::RaftLog::Request(request), Callback::None);
    }

    #[inline]
    fn region_split_skip_max_count(&self) -> usize {
        fail_point!("region_split_skip_max_count", |_| { usize::max_value() });
        REGION_SPLIT_SKIP_MAX_COUNT
    }

    fn on_split_region_check_tick(&mut self) {
        self.ticker.schedule(PEER_TICK_SPLIT_CHECK);
        if !self.fsm.peer.is_leader() {
            return;
        }

        if let Some(shard) = self.ctx.global.engines.kv.get_shard(self.region_id()) {
            if shard.get_split_stage() != kvenginepb::SplitStage::Initial {
                return;
            }
            let cfg_split_size = self.ctx.cfg.region_split_size.0;
            let estimated_size = shard.get_estimated_size();
            if estimated_size < cfg_split_size {
                return;
            }
            let task = SplitCheckTask::new(self.region_id(), cfg_split_size);
            if let Err(e) = self.ctx.split_check_scheduler.schedule(task) {
                error!(
                    "failed to schedule split check";
                    "region_id" => self.fsm.region_id(),
                    "peer_id" => self.fsm.peer_id(),
                    "err" => %e,
                );
                return;
            }
        }
    }

    fn on_prepare_split_region(
        &mut self,
        region_epoch: metapb::RegionEpoch,
        split_keys: Vec<Vec<u8>>,
        cb: Callback,
        source: &str,
    ) {
        info!(
            "on split";
            "region_id" => self.fsm.region_id(),
            "peer_id" => self.fsm.peer_id(),
            "split_keys" => %outil::KeysInfoFormatter(split_keys.iter()),
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
            right_derive: true,
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
        if outil::is_epoch_stale(region_epoch, region.get_region_epoch()) {
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
        self.ticker.schedule(PEER_TICK_PD_HEARTBEAT);
        self.fsm.peer.check_peers();

        if !self.fsm.peer.is_leader() {
            return;
        }
        self.fsm.peer.heartbeat_pd(&self.ctx);
    }

    fn on_check_peer_stale_state_tick(&mut self) {
        if self.fsm.peer.pending_remove {
            return;
        }

        self.ticker.schedule(PEER_TICK_STALE_STATE_CHECK);

        if self.fsm.peer.is_applying_snapshot() || self.fsm.peer.has_pending_snapshot() {
            return;
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
        let state = self.fsm.peer.check_stale_state(&self.ctx.cfg);
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

    fn on_ready_compute_hash(
        &mut self,
        region: metapb::Region,
        index: u64,
        context: Vec<u8>,
    ) {
        todo!()
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
        self.propose_raft_command(rlog::RaftLog::Request(req), Callback::None);
    }

    fn on_ingest_sst_result(&mut self, ssts: Vec<SSTMetaInfo>) {
        todo!()
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
                        == outil::find_peer(target_region, meta.store_id.unwrap())
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

impl PeerMsgHandler<'_> {
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

impl PeerFsm {
    fn meta_peer(&self) -> &metapb::Peer {
        &self.peer.peer
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

mod memtrace {
    use super::*;
    use raft_proto::eraftpb;

    impl PeerFsm {
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
            let inflight_size = self.max_inflight_msgs * mem::size_of::<u64>();
            mem::size_of::<Progress>() * peer_cnt * 6 / 5 + inflight_size * peer_cnt
        }

        /// Unstable entries heap size.
        /// NOTE: Entry::data and Entry::context is not counted.
        pub fn raft_entries_size(&self) -> usize {
            let raft = &self.peer.raft_group.raft;
            let entries = &raft.raft_log.unstable.entries;
            entries.len() * (mem::size_of::<eraftpb::Entry>() + 8)
        }
    }
}
