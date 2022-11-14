// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{mem, sync::Arc};

use collections::HashMap;
use crossbeam::atomic::AtomicCell;
use engine_traits::{KvEngine, OpenOptions, RaftEngine, TabletFactory};
use kvproto::{kvrpcpb::ExtraOp as TxnExtraOp, metapb, raft_serverpb::RegionLocalState};
use pd_client::BucketStat;
use raft::{RawNode, StateRole};
use raftstore::{
    coprocessor::{CoprocessorHost, RegionChangeEvent, RegionChangeReason},
    store::{
        fsm::Proposal,
        util::{Lease, RegionReadProgress},
        Config, EntryStorage, PeerStat, ProposalQueue, ReadDelegate, ReadIndexQueue, ReadProgress,
        TxnExt,
    },
    Error,
};
use slog::{debug, error, info, o, warn, Logger};
use tikv_util::{
    box_err,
    config::ReadableSize,
    time::{monotonic_raw_now, Instant as TiInstant},
    worker::Scheduler,
    Either,
};
use time::Timespec;

use super::{storage::Storage, Apply};
use crate::{
    batch::StoreContext,
    fsm::{ApplyFsm, ApplyScheduler},
    operation::{AsyncWriter, DestroyProgress, ProposalControl, SimpleWriteEncoder},
    router::{CmdResChannel, QueryResChannel},
    tablet::CachedTablet,
    Result,
};

const REGION_READ_PROGRESS_CAP: usize = 128;

/// A peer that delegates commands between state machine and raft.
pub struct Peer<EK: KvEngine, ER: RaftEngine> {
    raft_group: RawNode<Storage<EK, ER>>,
    tablet: CachedTablet<EK>,
    /// We use a cache for looking up peers. Not all peers exist in region's
    /// peer list, for example, an isolated peer may need to send/receive
    /// messages with unknown peers after recovery.
    peer_cache: Vec<metapb::Peer>,

    /// Encoder for batching proposals and encoding them in a more efficient way
    /// than protobuf.
    raw_write_encoder: Option<SimpleWriteEncoder>,
    proposals: ProposalQueue<Vec<CmdResChannel>>,
    apply_scheduler: Option<ApplyScheduler>,

    /// Set to true if any side effect needs to be handled.
    has_ready: bool,
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

    /// Check whether this proposal can be proposed based on its epoch.
    proposal_control: ProposalControl,
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    /// Creates a new peer.
    ///
    /// If peer is destroyed, `None` is returned.
    pub fn new(
        cfg: &Config,
        tablet_factory: &dyn TabletFactory<EK>,
        storage: Storage<EK, ER>,
    ) -> Result<Self> {
        let logger = storage.logger().clone();

        let applied_index = storage.apply_state().get_applied_index();
        let peer_id = storage.peer().get_id();
        let raft_cfg = cfg.new_raft_config(peer_id, applied_index);

        let region_id = storage.region().get_id();
        let tablet_index = storage.region_state().get_tablet_index();
        // Another option is always create tablet even if tablet index is 0. But this
        // can introduce race when gc old tablet and create new peer.
        let tablet = if tablet_index != 0 {
            if !tablet_factory.exists(region_id, tablet_index) {
                return Err(box_err!(
                    "missing tablet {} for region {}",
                    tablet_index,
                    region_id
                ));
            }
            // TODO: Perhaps we should stop create the tablet automatically.
            Some(tablet_factory.open_tablet(
                region_id,
                Some(tablet_index),
                OpenOptions::default().set_create(true),
            )?)
        } else {
            None
        };

        let tablet = CachedTablet::new(tablet);

        let raft_group = RawNode::new(&raft_cfg, storage, &logger)?;
        let region = raft_group.store().region_state().get_region().clone();
        let tag = format!("[region {}] {}", region.get_id(), peer_id);
        let mut peer = Peer {
            tablet,
            peer_cache: vec![],
            raw_write_encoder: None,
            proposals: ProposalQueue::new(region_id, raft_group.raft.id),
            async_writer: AsyncWriter::new(region_id, peer_id),
            apply_scheduler: None,
            has_ready: false,
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
        // host: &CoprocessorHost<impl KvEngine>,
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

        // todo: CoprocessorHost
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
    pub fn tablet(&self) -> &CachedTablet<EK> {
        &self.tablet
    }

    #[inline]
    pub fn tablet_mut(&mut self) -> &mut CachedTablet<EK> {
        &mut self.tablet
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
    pub(crate) fn has_applied_to_current_term(&self) -> bool {
        self.entry_storage().applied_term() == self.term()
    }

    #[inline]
    pub fn simple_write_encoder_mut(&mut self) -> &mut Option<SimpleWriteEncoder> {
        &mut self.raw_write_encoder
    }

    #[inline]
    pub fn simple_write_encoder(&self) -> &Option<SimpleWriteEncoder> {
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

    pub fn apply_scheduler(&self) -> &ApplyScheduler {
        self.apply_scheduler.as_ref().unwrap()
    }

    #[inline]
    pub fn set_apply_scheduler(&mut self, apply_scheduler: ApplyScheduler) {
        self.apply_scheduler = Some(apply_scheduler);
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

    pub fn heartbeat_pd<T>(&self, store_ctx: &StoreContext<EK, ER, T>) {
        // todo
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
}
