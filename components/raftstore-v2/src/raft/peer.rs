// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp, mem,
    sync::Arc,
    time::{Duration, Instant},
};

use collections::{HashMap, HashSet};
use encryption_export::DataKeyManager;
use engine_traits::{
    CachedTablet, FlushState, KvEngine, RaftEngine, SstApplyState, TabletContext, TabletRegistry,
};
use kvproto::{
    metapb::{self, PeerRole},
    pdpb,
    raft_serverpb::RaftMessage,
};
use raft::{eraftpb, RawNode, StateRole};
use raftstore::{
    coprocessor::{CoprocessorHost, RegionChangeEvent, RegionChangeReason},
    store::{
        fsm::ApplyMetrics,
        metrics::RAFT_PEER_PENDING_DURATION,
        util::{Lease, RegionReadProgress},
        Config, EntryStorage, PeerStat, ProposalQueue, ReadDelegate, ReadIndexQueue, ReadProgress,
        TabletSnapManager, WriteTask,
    },
};
use slog::{debug, info, Logger};
use tikv_util::{slog_panic, time::duration_to_sec};

use super::storage::Storage;
use crate::{
    fsm::ApplyScheduler,
    operation::{
        AbnormalPeerContext, AsyncWriter, BucketStatsInfo, CompactLogContext, DestroyProgress,
        GcPeerContext, MergeContext, ProposalControl, SimpleWriteReqEncoder, SplitFlowControl,
        TxnContext,
    },
    router::{ApplyTask, CmdResChannel, PeerTick, QueryResChannel},
    Result,
};

const REGION_READ_PROGRESS_CAP: usize = 128;

/// A peer that delegates commands between state machine and raft.
pub struct Peer<EK: KvEngine, ER: RaftEngine> {
    raft_group: RawNode<Storage<EK, ER>>,
    tablet: CachedTablet<EK>,
    tablet_being_flushed: bool,

    /// Statistics for self.
    self_stat: PeerStat,

    /// We use a cache for looking up peers. Not all peers exist in region's
    /// peer list, for example, an isolated peer may need to send/receive
    /// messages with unknown peers after recovery.
    peer_cache: Vec<metapb::Peer>,
    /// Statistics for other peers, only maintained when self is the leader.
    peer_heartbeats: HashMap<u64, Instant>,

    /// For raft log compaction.
    compact_log_context: CompactLogContext,

    merge_context: Option<Box<MergeContext>>,
    last_sent_snapshot_index: u64,

    /// Encoder for batching proposals and encoding them in a more efficient way
    /// than protobuf.
    raw_write_encoder: Option<SimpleWriteReqEncoder>,
    proposals: ProposalQueue<Vec<CmdResChannel>>,
    apply_scheduler: Option<ApplyScheduler>,

    /// Set to true if any side effect needs to be handled.
    has_ready: bool,
    /// Sometimes there is no ready at all, but we need to trigger async write.
    has_extra_write: bool,
    pause_for_recovery: bool,
    /// Writer for persisting side effects asynchronously.
    pub(crate) async_writer: AsyncWriter<EK, ER>,

    destroy_progress: DestroyProgress,

    pub(crate) logger: Logger,
    pending_reads: ReadIndexQueue<QueryResChannel>,
    read_progress: Arc<RegionReadProgress>,
    leader_lease: Lease,

    region_buckets_info: BucketStatsInfo,

    /// Transaction extensions related to this peer.
    txn_context: TxnContext,

    pending_ticks: Vec<PeerTick>,

    /// Check whether this proposal can be proposed based on its epoch.
    proposal_control: ProposalControl,

    // Trace which peers have not finished split.
    split_trace: Vec<(u64, HashSet<u64>)>,
    split_flow_control: SplitFlowControl,

    /// Apply related State changes that needs to be persisted to raft engine.
    ///
    /// To make recovery correct, we need to persist all state changes before
    /// advancing apply index.
    state_changes: Option<Box<ER::LogBatch>>,
    flush_state: Arc<FlushState>,
    sst_apply_state: SstApplyState,

    /// lead_transferee if this peer(leader) is in a leadership transferring.
    leader_transferee: u64,

    long_uncommitted_threshold: u64,

    /// Pending messages to be sent on handle ready. We should avoid sending
    /// messages immediately otherwise it may break the persistence assumption.
    pending_messages: Vec<RaftMessage>,

    gc_peer_context: GcPeerContext,

    abnormal_peer_context: AbnormalPeerContext,
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    /// Creates a new peer.
    ///
    /// If peer is destroyed, `None` is returned.
    pub fn new(
        cfg: &Config,
        tablet_registry: &TabletRegistry<EK>,
        key_manager: Option<&DataKeyManager>,
        snap_mgr: &TabletSnapManager,
        storage: Storage<EK, ER>,
    ) -> Result<Self> {
        let logger = storage.logger().clone();

        let applied_index = storage.apply_state().get_applied_index();
        let peer_id = storage.peer().get_id();
        let raft_cfg = cfg.new_raft_config(peer_id, applied_index);

        let region_id = storage.region().get_id();
        let tablet_index = storage.region_state().get_tablet_index();
        let merge_context = MergeContext::from_region_state(&logger, storage.region_state());

        let raft_group = RawNode::new(&raft_cfg, storage, &logger)?;
        let region = raft_group.store().region_state().get_region().clone();

        let flush_state: Arc<FlushState> = Arc::new(FlushState::new(applied_index));
        let sst_apply_state = SstApplyState::default();
        // We can't create tablet if tablet index is 0. It can introduce race when gc
        // old tablet and create new peer. We also can't get the correct range of the
        // region, which is required for kv data gc.
        if tablet_index != 0 {
            raft_group
                .store()
                .recover_tablet(tablet_registry, key_manager, snap_mgr);
            let mut ctx = TabletContext::new(&region, Some(tablet_index));
            ctx.flush_state = Some(flush_state.clone());
            // TODO: Perhaps we should stop create the tablet automatically.
            tablet_registry.load(ctx, false)?;
        }
        let cached_tablet = tablet_registry.get_or_default(region_id);

        let tag = format!("[region {}] {}", region.get_id(), peer_id);
        let mut peer = Peer {
            tablet: cached_tablet,
            tablet_being_flushed: false,
            self_stat: PeerStat::default(),
            peer_cache: vec![],
            peer_heartbeats: HashMap::default(),
            compact_log_context: CompactLogContext::new(applied_index),
            merge_context: merge_context.map(|c| Box::new(c)),
            last_sent_snapshot_index: 0,
            raw_write_encoder: None,
            proposals: ProposalQueue::new(region_id, raft_group.raft.id),
            async_writer: AsyncWriter::new(region_id, peer_id),
            apply_scheduler: None,
            has_ready: false,
            has_extra_write: false,
            pause_for_recovery: false,
            destroy_progress: DestroyProgress::None,
            raft_group,
            logger,
            pending_reads: ReadIndexQueue::new(tag),
            read_progress: Arc::new(RegionReadProgress::new(
                &region,
                applied_index,
                REGION_READ_PROGRESS_CAP,
                peer_id,
            )),
            leader_lease: Lease::new(
                cfg.raft_store_max_leader_lease(),
                cfg.renew_leader_lease_advance_duration(),
            ),
            region_buckets_info: BucketStatsInfo::default(),
            txn_context: TxnContext::default(),
            proposal_control: ProposalControl::new(0),
            pending_ticks: Vec::new(),
            split_trace: vec![],
            state_changes: None,
            flush_state,
            sst_apply_state,
            split_flow_control: SplitFlowControl::default(),
            leader_transferee: raft::INVALID_ID,
            long_uncommitted_threshold: cmp::max(
                cfg.long_uncommitted_base_threshold.0.as_secs(),
                1,
            ),
            pending_messages: vec![],
            gc_peer_context: GcPeerContext::default(),
            abnormal_peer_context: AbnormalPeerContext::default(),
        };

        // If this region has only one peer and I am the one, campaign directly.
        let region = peer.region();
        if region.get_peers().len() == 1
            && region.get_peers()[0] == *peer.peer()
            && tablet_index != 0
        {
            peer.raft_group.campaign()?;
            peer.set_has_ready();
        }
        let term = peer.term();
        peer.proposal_control.maybe_update_term(term);

        Ok(peer)
    }

    pub fn region_buckets_info_mut(&mut self) -> &mut BucketStatsInfo {
        &mut self.region_buckets_info
    }

    pub fn region_buckets_info(&self) -> &BucketStatsInfo {
        &self.region_buckets_info
    }

    #[inline]
    pub fn region(&self) -> &metapb::Region {
        self.raft_group.store().region()
    }

    #[inline]
    pub fn region_id(&self) -> u64 {
        self.region().get_id()
    }

    /// Set the region of a peer.
    ///
    /// This will update the region of the peer, caller must ensure the region
    /// has been preserved in a durable device.
    pub fn set_region(
        &mut self,
        host: &CoprocessorHost<EK>,
        reader: &mut ReadDelegate,
        region: metapb::Region,
        reason: RegionChangeReason,
        tablet_index: u64,
    ) {
        if self.region().get_region_epoch().get_version() < region.get_region_epoch().get_version()
        {
            // Epoch version changed, disable read on the local reader for this region.
            self.leader_lease.expire_remote_lease();
        }

        self.storage_mut()
            .region_state_mut()
            .set_region(region.clone());
        self.storage_mut()
            .region_state_mut()
            .set_tablet_index(tablet_index);

        let progress = ReadProgress::region(region);
        // Always update read delegate's region to avoid stale region info after a
        // follower becoming a leader.
        self.maybe_update_read_progress(reader, progress);

        if self.is_leader() {
            // Unlike v1, we should renew remote lease if it's leader. This is because v2
            // only provides read in local reader which requires passing the lease check. If
            // lease check fails, it sends query to raftstore to make it renew the remote
            // lease. However, raftstore will answer immediately if the `bound` in
            // `leader_lease` is valid, so the remote lease will not be updated.
            if let Some(progress) = self
                .leader_lease
                .maybe_new_remote_lease(self.term())
                .map(ReadProgress::set_leader_lease)
            {
                self.maybe_update_read_progress(reader, progress);
            }
        }

        // Update leader info
        self.read_progress
            .update_leader_info(self.leader_id(), self.term(), self.region());

        self.txn_context
            .on_region_changed(self.term(), self.region());

        if self.serving() {
            host.on_region_changed(
                self.region(),
                RegionChangeEvent::Update(reason),
                self.state_role(),
            );
        }
    }

    #[inline]
    pub fn peer(&self) -> &metapb::Peer {
        self.raft_group.store().peer()
    }

    #[inline]
    pub fn peer_id(&self) -> u64 {
        self.peer().get_id()
    }

    #[inline]
    pub fn tablet_being_flushed(&self) -> bool {
        self.tablet_being_flushed
    }

    #[inline]
    pub fn set_tablet_being_flushed(&mut self, v: bool) {
        self.tablet_being_flushed = v;
    }

    #[inline]
    pub fn storage(&self) -> &Storage<EK, ER> {
        self.raft_group.store()
    }

    #[inline]
    pub fn read_progress(&self) -> &Arc<RegionReadProgress> {
        &self.read_progress
    }

    #[inline]
    pub fn read_progress_mut(&mut self) -> &mut Arc<RegionReadProgress> {
        &mut self.read_progress
    }

    #[inline]
    pub fn leader_lease(&self) -> &Lease {
        &self.leader_lease
    }

    #[inline]
    pub fn leader_lease_mut(&mut self) -> &mut Lease {
        &mut self.leader_lease
    }

    #[inline]
    pub fn storage_mut(&mut self) -> &mut Storage<EK, ER> {
        self.raft_group.mut_store()
    }

    #[inline]
    pub fn pending_reads(&self) -> &ReadIndexQueue<QueryResChannel> {
        &self.pending_reads
    }

    #[inline]
    pub fn pending_reads_mut(&mut self) -> &mut ReadIndexQueue<QueryResChannel> {
        &mut self.pending_reads
    }

    #[inline]
    pub fn entry_storage(&self) -> &EntryStorage<EK, ER> {
        self.raft_group.store().entry_storage()
    }

    #[inline]
    pub fn entry_storage_mut(&mut self) -> &mut EntryStorage<EK, ER> {
        self.raft_group.mut_store().entry_storage_mut()
    }

    #[inline]
    pub fn tablet(&mut self) -> Option<&EK> {
        self.tablet.latest()
    }

    #[inline]
    pub fn set_tablet(&mut self, tablet: EK) -> Option<EK> {
        self.tablet.set(tablet)
    }

    #[inline]
    pub fn compact_log_context_mut(&mut self) -> &mut CompactLogContext {
        &mut self.compact_log_context
    }

    #[inline]
    pub fn compact_log_context(&self) -> &CompactLogContext {
        &self.compact_log_context
    }

    #[inline]
    pub fn merge_context(&self) -> Option<&MergeContext> {
        self.merge_context.as_deref()
    }

    #[inline]
    pub fn merge_context_mut(&mut self) -> &mut MergeContext {
        self.merge_context.get_or_insert_default()
    }

    #[inline]
    pub fn take_merge_context(&mut self) -> Option<Box<MergeContext>> {
        self.merge_context.take()
    }

    #[inline]
    pub fn raft_group(&self) -> &RawNode<Storage<EK, ER>> {
        &self.raft_group
    }

    #[inline]
    pub fn raft_group_mut(&mut self) -> &mut RawNode<Storage<EK, ER>> {
        &mut self.raft_group
    }

    #[inline]
    pub fn set_raft_group(&mut self, raft_group: RawNode<Storage<EK, ER>>) {
        self.raft_group = raft_group;
    }

    #[inline]
    pub fn persisted_index(&self) -> u64 {
        self.raft_group.raft.raft_log.persisted
    }

    #[inline]
    pub fn get_pending_snapshot(&self) -> Option<&eraftpb::Snapshot> {
        self.raft_group.snap()
    }

    #[inline]
    pub fn self_stat(&self) -> &PeerStat {
        &self.self_stat
    }

    #[inline]
    pub fn update_stat(&mut self, metrics: &ApplyMetrics) {
        self.self_stat.written_bytes += metrics.written_bytes;
        self.self_stat.written_keys += metrics.written_keys;
    }

    /// Mark the peer has a ready so it will be checked at the end of every
    /// processing round.
    #[inline]
    pub fn set_has_ready(&mut self) {
        self.has_ready = true;
    }

    /// Mark the peer has no ready and return its previous state.
    #[inline]
    pub fn reset_has_ready(&mut self) -> bool {
        mem::take(&mut self.has_ready)
    }

    #[inline]
    pub fn set_has_extra_write(&mut self) {
        self.set_has_ready();
        self.has_extra_write = true;
    }

    #[inline]
    pub fn reset_has_extra_write(&mut self) -> bool {
        mem::take(&mut self.has_extra_write)
    }

    #[inline]
    pub fn set_pause_for_recovery(&mut self, pause: bool) {
        self.pause_for_recovery = pause;
    }

    #[inline]
    pub fn pause_for_recovery(&self) -> bool {
        self.pause_for_recovery
    }

    #[inline]
    // we may have skipped scheduling raft tick when start due to noticable gap
    // between commit index and apply index. We should scheduling it when raft log
    // apply catches up.
    pub fn try_compelete_recovery(&mut self) {
        if self.pause_for_recovery()
            && self.storage().entry_storage().commit_index()
                <= self.storage().entry_storage().applied_index()
        {
            info!(
                self.logger,
                "recovery completed";
                "apply_index" =>  self.storage().entry_storage().applied_index()
            );
            self.set_pause_for_recovery(false);
            // Flush to avoid recover again and again.
            if let Some(scheduler) = self.apply_scheduler() {
                scheduler.send(ApplyTask::ManualFlush);
            }
            self.add_pending_tick(PeerTick::Raft);
        }
    }

    #[inline]
    pub fn insert_peer_cache(&mut self, peer: metapb::Peer) {
        for p in self.raft_group.store().region().get_peers() {
            if p.get_id() == peer.get_id() {
                return;
            }
        }
        for p in &mut self.peer_cache {
            if p.get_id() == peer.get_id() {
                *p = peer;
                return;
            }
        }
        self.peer_cache.push(peer);
    }

    #[inline]
    pub fn clear_peer_cache(&mut self) {
        self.peer_cache.clear();
    }

    #[inline]
    pub fn peer_from_cache(&self, peer_id: u64) -> Option<metapb::Peer> {
        for p in self.raft_group.store().region().get_peers() {
            if p.get_id() == peer_id {
                return Some(p.clone());
            }
        }
        self.peer_cache
            .iter()
            .find(|p| p.get_id() == peer_id)
            .cloned()
    }

    #[inline]
    pub fn update_peer_statistics(&mut self) {
        if !self.is_leader() {
            self.peer_heartbeats.clear();
            return;
        }

        if self.peer_heartbeats.len() == self.region().get_peers().len() {
            return;
        }

        // Insert heartbeats in case that some peers never response heartbeats.
        let region = self.raft_group.store().region();
        for peer in region.get_peers() {
            self.peer_heartbeats
                .entry(peer.get_id())
                .or_insert_with(Instant::now);
        }
    }

    #[inline]
    pub fn add_peer_heartbeat(&mut self, peer_id: u64, now: Instant) {
        self.peer_heartbeats.insert(peer_id, now);
    }

    #[inline]
    pub fn remove_peer_heartbeat(&mut self, peer_id: u64) {
        self.peer_heartbeats.remove(&peer_id);
    }

    #[inline]
    pub fn get_peer_heartbeats(&self) -> &HashMap<u64, Instant> {
        &self.peer_heartbeats
    }

    /// Returns whether or not the peer sent heartbeat after the provided
    /// deadline time.
    #[inline]
    pub fn peer_heartbeat_is_fresh(&self, peer_id: u64, deadline: &Instant) -> bool {
        matches!(
            self.peer_heartbeats.get(&peer_id),
            Some(last_heartbeat) if *last_heartbeat >= *deadline
        )
    }

    pub fn collect_down_peers(&mut self, max_duration: Duration) -> Vec<pdpb::PeerStats> {
        let mut down_peers = Vec::new();
        let mut down_peer_ids = Vec::new();
        let now = Instant::now();
        for p in self.region().get_peers() {
            if p.get_id() == self.peer_id() {
                continue;
            }
            if let Some(instant) = self.peer_heartbeats.get(&p.get_id()) {
                let elapsed = instant.saturating_duration_since(now);
                if elapsed >= max_duration {
                    let mut stats = pdpb::PeerStats::default();
                    stats.set_peer(p.clone());
                    stats.set_down_seconds(elapsed.as_secs());
                    down_peers.push(stats);
                    down_peer_ids.push(p.get_id());
                }
            }
        }
        *self.abnormal_peer_context_mut().down_peers_mut() = down_peer_ids;
        // TODO: `refill_disk_full_peers`
        down_peers
    }

    #[inline]
    pub fn state_role(&self) -> StateRole {
        self.raft_group.raft.state
    }

    #[inline]
    pub fn is_leader(&self) -> bool {
        self.raft_group.raft.state == StateRole::Leader
    }

    #[inline]
    pub fn leader_id(&self) -> u64 {
        self.raft_group.raft.leader_id
    }

    /// Get the leader peer meta.
    ///
    /// `None` is returned if there is no leader or the meta can't be found.
    #[inline]
    pub fn leader(&self) -> Option<metapb::Peer> {
        let leader_id = self.leader_id();
        if leader_id != 0 {
            self.peer_from_cache(leader_id)
        } else {
            None
        }
    }

    /// Term of the state machine.
    #[inline]
    pub fn term(&self) -> u64 {
        self.raft_group.raft.term
    }

    #[inline]
    // TODO
    pub fn has_force_leader(&self) -> bool {
        false
    }

    pub fn serving(&self) -> bool {
        matches!(self.destroy_progress, DestroyProgress::None)
    }

    #[inline]
    pub fn destroy_progress(&self) -> &DestroyProgress {
        &self.destroy_progress
    }

    #[inline]
    pub fn destroy_progress_mut(&mut self) -> &mut DestroyProgress {
        &mut self.destroy_progress
    }

    #[inline]
    pub fn simple_write_encoder_mut(&mut self) -> &mut Option<SimpleWriteReqEncoder> {
        &mut self.raw_write_encoder
    }

    #[inline]
    pub fn simple_write_encoder(&self) -> &Option<SimpleWriteReqEncoder> {
        &self.raw_write_encoder
    }

    #[inline]
    pub fn applied_to_current_term(&self) -> bool {
        self.storage().entry_storage().applied_term() == self.term()
    }

    #[inline]
    pub fn proposals_mut(&mut self) -> &mut ProposalQueue<Vec<CmdResChannel>> {
        &mut self.proposals
    }

    #[inline]
    pub fn proposals(&self) -> &ProposalQueue<Vec<CmdResChannel>> {
        &self.proposals
    }

    pub fn apply_scheduler(&self) -> Option<&ApplyScheduler> {
        self.apply_scheduler.as_ref()
    }

    #[inline]
    pub fn set_apply_scheduler(&mut self, apply_scheduler: ApplyScheduler) {
        self.apply_scheduler = Some(apply_scheduler);
    }

    #[inline]
    pub fn clear_apply_scheduler(&mut self) {
        self.apply_scheduler.take();
    }

    /// Whether the snapshot is handling.
    /// See the comments of `check_snap_status` for more details.
    #[inline]
    pub fn is_handling_snapshot(&self) -> bool {
        self.persisted_index() < self.entry_storage().truncated_index()
    }

    /// Returns `true` if the raft group has replicated a snapshot but not
    /// committed it yet.
    #[inline]
    pub fn has_pending_snapshot(&self) -> bool {
        self.raft_group().snap().is_some()
    }

    #[inline]
    pub fn add_pending_tick(&mut self, tick: PeerTick) {
        // Msg per batch is 4096/256 by default, the buffer won't grow too large.
        self.pending_ticks.push(tick);
    }

    #[inline]
    pub fn take_pending_ticks(&mut self) -> Vec<PeerTick> {
        mem::take(&mut self.pending_ticks)
    }

    #[inline]
    pub fn post_split(&mut self) {
        self.region_buckets_info_mut().set_bucket_stat(None);
    }

    pub fn maybe_campaign(&mut self) -> bool {
        if self.region().get_peers().len() <= 1 {
            // The peer campaigned when it was created, no need to do it again.
            return false;
        }

        // If last peer is the leader of the region before split, it's intuitional for
        // it to become the leader of new split region.
        let _ = self.raft_group.campaign();
        true
    }

    #[inline]
    pub fn txn_context(&self) -> &TxnContext {
        &self.txn_context
    }

    #[inline]
    pub fn txn_context_mut(&mut self) -> &mut TxnContext {
        &mut self.txn_context
    }

    pub fn generate_read_delegate(&self) -> ReadDelegate {
        let peer_id = self.peer().get_id();

        ReadDelegate::new(
            peer_id,
            self.term(),
            self.region().clone(),
            self.storage().entry_storage().applied_term(),
            self.txn_context.extra_op().clone(),
            self.txn_context.ext().clone(),
            self.read_progress().clone(),
            self.region_buckets_info()
                .bucket_stat()
                .as_ref()
                .map(|b| b.meta.clone()),
        )
    }

    #[inline]
    pub fn proposal_control_mut(&mut self) -> &mut ProposalControl {
        &mut self.proposal_control
    }

    #[inline]
    pub fn proposal_control(&self) -> &ProposalControl {
        &self.proposal_control
    }

    #[inline]
    pub fn proposal_control_advance_apply(&mut self, apply_index: u64) {
        let region = self.raft_group.store().region();
        let term = self.term();
        self.proposal_control
            .advance_apply(apply_index, term, region);
    }

    #[inline]
    pub fn in_joint_state(&self) -> bool {
        self.region().get_peers().iter().any(|p| {
            p.get_role() == PeerRole::IncomingVoter || p.get_role() == PeerRole::DemotingVoter
        })
    }

    #[inline]
    pub fn split_trace_mut(&mut self) -> &mut Vec<(u64, HashSet<u64>)> {
        &mut self.split_trace
    }

    #[inline]
    pub fn flush_state(&self) -> &Arc<FlushState> {
        &self.flush_state
    }

    #[inline]
    pub fn sst_apply_state(&self) -> &SstApplyState {
        &self.sst_apply_state
    }

    pub fn reset_flush_state(&mut self, index: u64) {
        self.flush_state = Arc::new(FlushState::new(index));
    }

    // Note: Call `set_has_extra_write` after adding new state changes.
    #[inline]
    pub fn state_changes_mut(&mut self) -> &mut ER::LogBatch {
        if self.state_changes.is_none() {
            self.state_changes = Some(Box::new(self.entry_storage().raft_engine().log_batch(0)));
        }
        self.state_changes.as_mut().unwrap()
    }

    #[inline]
    pub fn merge_state_changes_to(&mut self, task: &mut WriteTask<EK, ER>) {
        if self.state_changes.is_none() {
            return;
        }
        task.extra_write
            .merge_v2(Box::into_inner(self.state_changes.take().unwrap()));
    }

    #[inline]
    pub fn split_flow_control_mut(&mut self) -> &mut SplitFlowControl {
        &mut self.split_flow_control
    }

    #[inline]
    pub fn refresh_leader_transferee(&mut self) -> u64 {
        mem::replace(
            &mut self.leader_transferee,
            self.raft_group
                .raft
                .lead_transferee
                .unwrap_or(raft::INVALID_ID),
        )
    }

    #[inline]
    pub fn long_uncommitted_threshold(&self) -> Duration {
        Duration::from_secs(self.long_uncommitted_threshold)
    }

    #[inline]
    pub fn set_long_uncommitted_threshold(&mut self, dur: Duration) {
        self.long_uncommitted_threshold = cmp::max(dur.as_secs(), 1);
    }

    #[inline]
    pub fn add_message(&mut self, msg: RaftMessage) {
        self.pending_messages.push(msg);
        self.set_has_ready();
    }

    #[inline]
    pub fn has_pending_messages(&mut self) -> bool {
        !self.pending_messages.is_empty()
    }

    #[inline]
    pub fn take_pending_messages(&mut self) -> Vec<RaftMessage> {
        mem::take(&mut self.pending_messages)
    }

    #[inline]
    pub fn gc_peer_context(&self) -> &GcPeerContext {
        &self.gc_peer_context
    }

    #[inline]
    pub fn gc_peer_context_mut(&mut self) -> &mut GcPeerContext {
        &mut self.gc_peer_context
    }

    #[inline]
    pub fn update_last_sent_snapshot_index(&mut self, i: u64) {
        if i > self.last_sent_snapshot_index {
            self.last_sent_snapshot_index = i;
        }
    }

    #[inline]
    pub fn last_sent_snapshot_index(&self) -> u64 {
        self.last_sent_snapshot_index
    }

    #[inline]
    pub fn index_term(&self, idx: u64) -> u64 {
        match self.raft_group.raft.raft_log.term(idx) {
            Ok(t) => t,
            Err(e) => slog_panic!(self.logger, "failed to load term"; "index" => idx, "err" => ?e),
        }
    }

    #[inline]
    pub fn abnormal_peer_context_mut(&mut self) -> &mut AbnormalPeerContext {
        &mut self.abnormal_peer_context
    }

    #[inline]
    pub fn abnormal_peer_context(&self) -> &AbnormalPeerContext {
        &self.abnormal_peer_context
    }

    pub fn any_new_peer_catch_up(&mut self, from_peer_id: u64) -> bool {
        // no pending or down peers
        if self.abnormal_peer_context.is_empty() {
            return false;
        }
        if !self.is_leader() {
            self.abnormal_peer_context.reset();
            return false;
        }

        if self
            .abnormal_peer_context
            .down_peers()
            .contains(&from_peer_id)
        {
            return true;
        }

        let logger = self.logger.clone();
        self.abnormal_peer_context
            .retain_pending_peers(|(peer_id, pending_after)| {
                // TODO check wait data peers here
                let truncated_idx = self.raft_group.store().entry_storage().truncated_index();
                if let Some(progress) = self.raft_group.raft.prs().get(*peer_id) {
                    if progress.matched >= truncated_idx {
                        let elapsed = duration_to_sec(pending_after.saturating_elapsed());
                        RAFT_PEER_PENDING_DURATION.observe(elapsed);
                        debug!(
                            logger,
                            "peer has caught up logs";
                            "from_peer_id" => %from_peer_id,
                            "takes" => %elapsed,
                        );
                        return false;
                    }
                }
                true
            })
    }
}
