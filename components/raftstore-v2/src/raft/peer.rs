// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{mem, sync::Arc};

use crossbeam::atomic::AtomicCell;
use engine_traits::{KvEngine, OpenOptions, RaftEngine, TabletFactory};
use fail::fail_point;
use kvproto::{
    kvrpcpb::ExtraOp as TxnExtraOp,
    metapb,
    raft_cmdpb::{self, RaftCmdRequest},
    raft_serverpb::RegionLocalState,
};
use pd_client::BucketStat;
use protobuf::Message;
use raft::{RawNode, StateRole, INVALID_ID};
use raftstore::{
    coprocessor::{CoprocessorHost, RegionChangeEvent, RegionChangeReason},
    store::{
        fsm::Proposal,
        metrics::PEER_PROPOSE_LOG_SIZE_HISTOGRAM,
        util::{Lease, RegionReadProgress},
        Config, EntryStorage, PeerStat, ProposalQueue, RaftlogFetchTask, ReadDelegate,
        ReadIndexQueue, ReadIndexRequest, ReadProgress, Transport, TxnExt, WriteRouter,
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

use super::{storage::Storage, Apply};
use crate::{
    batch::StoreContext,
    fsm::{ApplyFsm, ApplyScheduler},
    operation::{AsyncWriter, DestroyProgress, SimpleWriteEncoder},
    router::{CmdResChannel, QueryResChannel},
    tablet::{self, CachedTablet},
    Result,
};

const REGION_READ_PROGRESS_CAP: usize = 128;

/// A peer that delegates commands between state machine and raft.
pub struct Peer<EK: KvEngine, ER: RaftEngine> {
    raft_group: RawNode<Storage<ER>>,
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

    /// Transaction extensions related to this peer.
    pub(crate) txn_ext: Arc<TxnExt>,
    pub(crate) txn_extra_op: Arc<AtomicCell<TxnExtraOp>>,
    /// region buckets.
    pub(crate) region_buckets: Option<BucketStat>,

    /// Write Statistics for PD to schedule hot spot.
    peer_stat: PeerStat,

    // An inaccurate difference in region size since last reset.
    /// It is used to decide whether split check is needed.
    size_diff_hint: u64,
    /// Approximate size of the region.
    approximate_size: Option<u64>,
    /// Approximate keys of the region.
    approximate_keys: Option<u64>,
    /// Whether this region has scheduled a split check task. If we just
    /// splitted  the region or ingested one file which may be overlapped
    /// with the existed data, reset the flag so that the region can be
    /// splitted again.
    may_skip_split_check: bool,
    /// The index of last compacted raft log. It is used for the next compact
    /// log task.
    last_compacted_idx: u64,

    pending_reads: ReadIndexQueue<QueryResChannel>,
    read_progress: Arc<RegionReadProgress>,
    leader_lease: Lease,

    /// Whether this peer is destroyed asynchronously.
    /// If it's true,
    /// - when merging, its data in storeMeta will be removed early by the
    ///   target peer.
    /// - all read requests must be rejected.
    pending_remove: bool,
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    /// Creates a new peer.
    ///
    /// If peer is destroyed, `None` is returned.
    pub fn new(
        cfg: &Config,
        tablet_factory: &dyn TabletFactory<EK>,
        storage: Storage<ER>,
    ) -> Result<Self> {
        let logger = storage.logger().clone();

        let applied_index = storage.apply_state().get_applied_index();
        let peer_id = storage.peer().get_id();

        let raft_cfg = raft::Config {
            id: peer_id,
            election_tick: cfg.raft_election_timeout_ticks,
            heartbeat_tick: cfg.raft_heartbeat_ticks,
            min_election_tick: cfg.raft_min_election_timeout_ticks,
            max_election_tick: cfg.raft_max_election_timeout_ticks,
            max_size_per_msg: cfg.raft_max_size_per_msg.0,
            max_inflight_msgs: cfg.raft_max_inflight_msgs,
            applied: applied_index,
            check_quorum: true,
            skip_bcast_commit: true,
            pre_vote: cfg.prevote,
            max_committed_size_per_ready: ReadableSize::mb(16).0,
            ..Default::default()
        };

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
            txn_ext: Arc::default(),
            txn_extra_op: Arc::new(AtomicCell::new(TxnExtraOp::Noop)),
            region_buckets: Some(BucketStat {
                meta: Arc::default(),
                stats: metapb::BucketStats::default(),
                create_time: TiInstant::now(),
            }),
            peer_stat: PeerStat::default(),
            last_compacted_idx: 0,
            size_diff_hint: 0,
            approximate_keys: None,
            approximate_size: None,
            may_skip_split_check: false,
            pending_reads: ReadIndexQueue::new(tag.clone()),
            read_progress: Arc::new(RegionReadProgress::new(
                &region,
                applied_index,
                REGION_READ_PROGRESS_CAP,
                tag,
            )),
            leader_lease: Lease::new(
                cfg.raft_store_max_leader_lease(),
                cfg.renew_leader_lease_advance_duration(),
            ),
            pending_remove: false,
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
        reader: &mut ReadDelegate,
        region: metapb::Region,
        reason: RegionChangeReason,
    ) {
        if self.region().get_region_epoch().get_version() < region.get_region_epoch().get_version()
        {
            // Epoch version changed, disable read on the local reader for this region.
            self.leader_lease.expire_remote_lease();
        }
        self.storage_mut().set_region(region.clone());
        let progress = ReadProgress::region(region);
        // Always update read delegate's region to avoid stale region info after a
        // follower becoming a leader.
        self.maybe_update_read_progress(reader, progress);

        // Update leader info
        self.read_progress
            .update_leader_info(self.leader_id(), self.term(), self.region());

        {
            let mut pessimistic_locks = self.txn_ext.pessimistic_locks.write();
            pessimistic_locks.term = self.term();
            pessimistic_locks.version = self.region().get_region_epoch().get_version();
        }

        // todo: CoprocessorHost::new needs a CasualRouter where v2 does not
        // have this now.
        // if !self.pending_remove {
        //     host.on_region_changed(
        //         self.region(),
        //         RegionChangeEvent::Update(reason),
        //         self.get_role(),
        //     )
        // }
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
    pub fn storage(&self) -> &Storage<ER> {
        self.raft_group.store()
    }

    #[inline]
    pub fn read_progress(&self) -> &Arc<RegionReadProgress> {
        &self.read_progress
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
    pub fn storage_mut(&mut self) -> &mut Storage<ER> {
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
    pub fn entry_storage(&self) -> &EntryStorage<ER> {
        self.raft_group.store().entry_storage()
    }

    #[inline]
    pub fn entry_storage_mut(&mut self) -> &mut EntryStorage<ER> {
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
    pub fn raft_group(&self) -> &RawNode<Storage<ER>> {
        &self.raft_group
    }

    #[inline]
    pub fn raft_group_mut(&mut self) -> &mut RawNode<Storage<ER>> {
        &mut self.raft_group
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
    pub fn is_splitting(&self) -> bool {
        false
    }

    #[inline]
    // TODO
    pub fn is_merging(&self) -> bool {
        false
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
    pub fn apply_scheduler(&self) -> &ApplyScheduler {
        self.apply_scheduler.as_ref().unwrap()
    }

    #[inline]
    pub fn set_apply_scheduler(&mut self, apply_scheduler: ApplyScheduler) {
        self.apply_scheduler = Some(apply_scheduler);
    }

    #[inline]
    pub fn post_split(&mut self) {
        unimplemented!()
    }

    pub fn maybe_campaign(&mut self, parent_is_leader: bool) -> bool {
        // if self.region().get_peers().len() <= 1 {
        //     // The peer campaigned when it was created, no need to do it
        // again.     return false;
        // }

        // if !parent_is_leader {
        //     return false;
        // }

        // // If last peer is the leader of the region before split, it's
        // intuitional for // it to become the leader of new split
        // region. let _ = self.raft_group.campaign();
        // true
        unimplemented!()
    }

    /// Register self to apply_scheduler so that the peer is then usable.
    /// Also trigger `RegionChangeEvent::Create` here.
    pub fn activate<T>(&self, ctx: &StoreContext<EK, ER, T>) {
        // self.schedule_apply_fsm(ctx)

        // todo: CoprocessorHost::new needs a CasualRouter where v2 does not
        // have this now.
        // ctx.coprocessor_host.on_region_changed(
        //     self.region(),
        //     RegionChangeEvent::Create,
        //     self.get_role(),
        // );
        // self.maybe_gen_approximate_buckets(ctx);
    }

    fn get_role(&self) -> StateRole {
        self.raft_group.raft.state
    }

    pub fn peer_stat(&self) -> &PeerStat {
        &self.peer_stat
    }

    pub fn set_peer_stat(&mut self, peer_stat: PeerStat) {
        self.peer_stat = peer_stat;
    }

    pub fn last_compacted_idx(&self) -> u64 {
        self.last_compacted_idx
    }

    pub fn set_last_compacted_idx(&mut self, last_compacted_idx: u64) {
        self.last_compacted_idx = last_compacted_idx;
    }

    pub fn size_diff_hint(&self) -> u64 {
        self.size_diff_hint
    }

    pub fn set_size_diff_hint(&mut self, size_diff_hint: u64) {
        self.size_diff_hint = size_diff_hint;
    }

    pub fn approximate_size(&self) -> Option<u64> {
        self.approximate_size
    }

    pub fn set_approximate_size(&mut self, approximate_size: Option<u64>) {
        self.approximate_size = approximate_size;
    }

    pub fn approximate_keys(&self) -> Option<u64> {
        self.approximate_keys
    }

    pub fn set_approximate_keys(&mut self, approximate_keys: Option<u64>) {
        self.approximate_keys = approximate_keys;
    }

    pub fn may_skip_split_check(&self) -> bool {
        self.may_skip_split_check
    }

    pub fn set_may_skip_split_check(&mut self, may_skip_split_check: bool) {
        self.may_skip_split_check = may_skip_split_check;
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
}
