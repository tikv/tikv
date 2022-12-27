// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    mem,
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant},
};

use collections::{HashMap, HashSet};
use crossbeam::atomic::AtomicCell;
use engine_traits::{
    CachedTablet, FlushState, KvEngine, RaftEngine, TabletContext, TabletRegistry,
};
use kvproto::{kvrpcpb::ExtraOp as TxnExtraOp, metapb, pdpb, raft_serverpb::RegionLocalState};
use pd_client::BucketStat;
use raft::{RawNode, StateRole};
use raftstore::{
    coprocessor::{CoprocessorHost, RegionChangeEvent, RegionChangeReason},
    store::{
        fsm::ApplyMetrics,
        util::{Lease, RegionReadProgress},
        Config, EntryStorage, LocksStatus, PeerStat, ProposalQueue, ReadDelegate, ReadIndexQueue,
        ReadProgress, TabletSnapManager, TxnExt, WriteTask,
    },
};
use slog::Logger;

use super::storage::Storage;
use crate::{
    batch::StoreContext,
    fsm::ApplyScheduler,
    operation::{
        AsyncWriter, DestroyProgress, ProposalControl, SimpleWriteReqEncoder, SplitFlowControl,
    },
    router::{CmdResChannel, PeerTick, QueryResChannel},
    worker::tablet_gc,
    Result,
};

const REGION_READ_PROGRESS_CAP: usize = 128;

/// A peer that delegates commands between state machine and raft.
pub struct Peer<EK: KvEngine, ER: RaftEngine> {
    raft_group: RawNode<Storage<EK, ER>>,
    tablet: CachedTablet<EK>,
    /// Tombstone tablets can only be destroyed when the tablet that replaces it
    /// is persisted. This is a list of tablet index that awaits to be
    /// persisted. When persisted_apply is advanced, we need to notify tablet_gc
    /// worker to destroy them.
    pending_tombstone_tablets: Vec<u64>,

    /// Statistics for self.
    self_stat: PeerStat,

    /// We use a cache for looking up peers. Not all peers exist in region's
    /// peer list, for example, an isolated peer may need to send/receive
    /// messages with unknown peers after recovery.
    peer_cache: Vec<metapb::Peer>,
    /// Statistics for other peers, only maintained when self is the leader.
    peer_heartbeats: HashMap<u64, Instant>,

    /// For raft log compaction.
    skip_compact_log_ticks: usize,
    approximate_raft_log_size: u64,

    /// Encoder for batching proposals and encoding them in a more efficient way
    /// than protobuf.
    raw_write_encoder: Option<SimpleWriteReqEncoder>,
    proposals: ProposalQueue<Vec<CmdResChannel>>,
    apply_scheduler: Option<ApplyScheduler>,

    /// Set to true if any side effect needs to be handled.
    has_ready: bool,
    /// Sometimes there is no ready at all, but we need to trigger async write.
    has_extra_write: bool,
    /// Writer for persisting side effects asynchronously.
    pub(crate) async_writer: AsyncWriter<EK, ER>,

    destroy_progress: DestroyProgress,

    pub(crate) logger: Logger,
    pending_reads: ReadIndexQueue<QueryResChannel>,
    read_progress: Arc<RegionReadProgress>,
    leader_lease: Lease,

    /// region buckets.
    region_buckets: Option<BucketStat>,
    last_region_buckets: Option<BucketStat>,

    /// Transaction extensions related to this peer.
    txn_ext: Arc<TxnExt>,
    txn_extra_op: Arc<AtomicCell<TxnExtraOp>>,

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

    /// lead_transferee if this peer(leader) is in a leadership transferring.
    leader_transferee: u64,
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    /// Creates a new peer.
    ///
    /// If peer is destroyed, `None` is returned.
    pub fn new(
        cfg: &Config,
        tablet_registry: &TabletRegistry<EK>,
        snap_mgr: &TabletSnapManager,
        storage: Storage<EK, ER>,
    ) -> Result<Self> {
        let logger = storage.logger().clone();

        let applied_index = storage.apply_state().get_applied_index();
        let peer_id = storage.peer().get_id();
        let raft_cfg = cfg.new_raft_config(peer_id, applied_index);

        let region_id = storage.region().get_id();
        let tablet_index = storage.region_state().get_tablet_index();

        let raft_group = RawNode::new(&raft_cfg, storage, &logger)?;
        let region = raft_group.store().region_state().get_region().clone();

        let flush_state: Arc<FlushState> = Arc::default();
        // We can't create tablet if tablet index is 0. It can introduce race when gc
        // old tablet and create new peer. We also can't get the correct range of the
        // region, which is required for kv data gc.
        if tablet_index != 0 {
            raft_group.store().recover_tablet(tablet_registry, snap_mgr);
            let mut ctx = TabletContext::new(&region, Some(tablet_index));
            ctx.flush_state = Some(flush_state.clone());
            // TODO: Perhaps we should stop create the tablet automatically.
            tablet_registry.load(ctx, false)?;
        }
        let cached_tablet = tablet_registry.get_or_default(region_id);

        let tag = format!("[region {}] {}", region.get_id(), peer_id);
        let mut peer = Peer {
            tablet: cached_tablet,
            pending_tombstone_tablets: Vec::new(),
            self_stat: PeerStat::default(),
            peer_cache: vec![],
            peer_heartbeats: HashMap::default(),
            skip_compact_log_ticks: 0,
            approximate_raft_log_size: 0,
            raw_write_encoder: None,
            proposals: ProposalQueue::new(region_id, raft_group.raft.id),
            async_writer: AsyncWriter::new(region_id, peer_id),
            apply_scheduler: None,
            has_ready: false,
            has_extra_write: false,
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
            region_buckets: None,
            last_region_buckets: None,
            txn_ext: Arc::default(),
            txn_extra_op: Arc::new(AtomicCell::new(TxnExtraOp::Noop)),
            proposal_control: ProposalControl::new(0),
            pending_ticks: Vec::new(),
            split_trace: vec![],
            state_changes: None,
            flush_state,
            split_flow_control: SplitFlowControl::default(),
            leader_transferee: raft::INVALID_ID,
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

        let mut region_state = RegionLocalState::default();
        region_state.set_region(region.clone());
        region_state.set_tablet_index(tablet_index);
        region_state.set_state(self.storage().region_state().get_state());
        self.storage_mut().set_region_state(region_state);

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
                .map(ReadProgress::leader_lease)
            {
                self.maybe_update_read_progress(reader, progress);
            }
        }

        // Update leader info
        self.read_progress
            .update_leader_info(self.leader_id(), self.term(), self.region());

        {
            let mut pessimistic_locks = self.txn_ext.pessimistic_locks.write();
            pessimistic_locks.term = self.term();
            pessimistic_locks.version = self.region().get_region_epoch().get_version();
        }

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
    pub fn record_tablet_as_tombstone_and_refresh<T>(
        &mut self,
        new_tablet_index: u64,
        ctx: &StoreContext<EK, ER, T>,
    ) {
        if let Some(old_tablet) = self.tablet.cache() {
            self.pending_tombstone_tablets.push(new_tablet_index);
            let _ = ctx
                .schedulers
                .tablet_gc
                .schedule(tablet_gc::Task::prepare_destroy(
                    old_tablet.clone(),
                    self.region_id(),
                    new_tablet_index,
                ));
        }
        // TODO: Handle race between split and snapshot. So that we can assert
        // `self.tablet.refresh() == 1`
        assert!(self.tablet.refresh() > 0);
    }

    /// Returns if there's any tombstone being removed.
    #[inline]
    pub fn remove_tombstone_tablets_before(&mut self, persisted: u64) -> bool {
        let mut removed = 0;
        while let Some(i) = self.pending_tombstone_tablets.first()
            && *i <= persisted
        {
            removed += 1;
        }
        self.pending_tombstone_tablets.drain(..removed);
        removed > 0
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

    /// Returns whether or not the peer sent heartbeat after the provided
    /// deadline time.
    #[inline]
    pub fn peer_heartbeat_is_fresh(&self, peer_id: u64, deadline: &Instant) -> bool {
        matches!(
            self.peer_heartbeats.get(&peer_id),
            Some(last_heartbeat) if *last_heartbeat >= *deadline
        )
    }

    pub fn collect_down_peers(&self, max_duration: Duration) -> Vec<pdpb::PeerStats> {
        let mut down_peers = Vec::new();
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
                }
            }
        }
        // TODO: `refill_disk_full_peers`
        down_peers
    }

    #[inline]
    pub fn reset_skip_compact_log_ticks(&mut self) {
        self.skip_compact_log_ticks = 0;
    }

    #[inline]
    pub fn maybe_skip_compact_log(&mut self, max_skip_ticks: usize) -> bool {
        if self.skip_compact_log_ticks < max_skip_ticks {
            self.skip_compact_log_ticks += 1;
            true
        } else {
            false
        }
    }

    #[inline]
    pub fn approximate_raft_log_size(&self) -> u64 {
        self.approximate_raft_log_size
    }

    #[inline]
    pub fn update_approximate_raft_log_size(&mut self, f: impl Fn(u64) -> u64) {
        self.approximate_raft_log_size = f(self.approximate_raft_log_size);
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

    #[inline]
    // TODO
    pub fn has_pending_merge_state(&self) -> bool {
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
        // todo: This method may be unnecessary now?
        false
    }

    /// Returns `true` if the raft group has replicated a snapshot but not
    /// committed it yet.
    #[inline]
    pub fn has_pending_snapshot(&self) -> bool {
        self.raft_group().snap().is_some()
    }

    #[inline]
    pub fn add_pending_tick(&mut self, tick: PeerTick) {
        self.pending_ticks.push(tick);
    }

    #[inline]
    pub fn take_pending_ticks(&mut self) -> Vec<PeerTick> {
        mem::take(&mut self.pending_ticks)
    }

    pub fn activate_in_memory_pessimistic_locks(&mut self) {
        let mut pessimistic_locks = self.txn_ext.pessimistic_locks.write();
        pessimistic_locks.status = LocksStatus::Normal;
        pessimistic_locks.term = self.term();
        pessimistic_locks.version = self.region().get_region_epoch().get_version();
    }

    pub fn clear_in_memory_pessimistic_locks(&mut self) {
        let mut pessimistic_locks = self.txn_ext.pessimistic_locks.write();
        pessimistic_locks.status = LocksStatus::NotLeader;
        pessimistic_locks.clear();
        pessimistic_locks.term = self.term();
        pessimistic_locks.version = self.region().get_region_epoch().get_version();
    }

    #[inline]
    pub fn post_split(&mut self) {
        self.reset_region_buckets();
    }

    pub fn reset_region_buckets(&mut self) {
        if self.region_buckets.is_some() {
            self.last_region_buckets = self.region_buckets.take();
        }
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
    pub fn txn_ext(&self) -> &Arc<TxnExt> {
        &self.txn_ext
    }

    pub fn generate_read_delegate(&self) -> ReadDelegate {
        let peer_id = self.peer().get_id();

        ReadDelegate::new(
            peer_id,
            self.term(),
            self.region().clone(),
            self.storage().entry_storage().applied_term(),
            self.txn_extra_op.clone(),
            self.txn_ext.clone(),
            self.read_progress().clone(),
            self.region_buckets.as_ref().map(|b| b.meta.clone()),
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

    // TODO: find a better place to put all txn related stuff.
    pub fn require_updating_max_ts<T>(&self, ctx: &StoreContext<EK, ER, T>) {
        let epoch = self.region().get_region_epoch();
        let term_low_bits = self.term() & ((1 << 32) - 1); // 32 bits
        let version_lot_bits = epoch.get_version() & ((1 << 31) - 1); // 31 bits
        let initial_status = (term_low_bits << 32) | (version_lot_bits << 1);
        self.txn_ext
            .max_ts_sync_status
            .store(initial_status, Ordering::SeqCst);

        self.update_max_timestamp_pd(ctx, initial_status);
    }

    #[inline]
    pub fn split_trace_mut(&mut self) -> &mut Vec<(u64, HashSet<u64>)> {
        &mut self.split_trace
    }

    #[inline]
    pub fn flush_state(&self) -> &Arc<FlushState> {
        &self.flush_state
    }

    pub fn reset_flush_state(&mut self) {
        self.flush_state = Arc::default();
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
}
