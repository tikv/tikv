// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{mem, sync::Arc};

use crossbeam::atomic::AtomicCell;
use engine_traits::{KvEngine, OpenOptions, RaftEngine, TabletFactory};
use kvproto::{
    kvrpcpb::ExtraOp as TxnExtraOp,
    metapb,
    raft_cmdpb::{self, RaftCmdRequest},
};
use pd_client::BucketMeta;
use protobuf::Message;
use raft::{RawNode, StateRole};
use raftstore::{
    store::{
        fsm::Proposal,
        metrics::PEER_PROPOSE_LOG_SIZE_HISTOGRAM,
        peer::{
            get_sync_log_from_request, CmdEpochChecker, ProposalContext, ProposalQueue, RaftPeer,
            RequestInspector,
        },
        read_queue::{ReadIndexQueue, ReadIndexRequest},
        util::{Lease, LeaseState, RegionReadProgress},
        worker::{LocalReadContext, ReadExecutor},
        Config, EntryStorage, RaftlogFetchTask, Transport, TxnExt,
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

use super::storage::Storage;
use crate::{
    batch::StoreContext,
    operation::AsyncWriter,
    router::{CmdResChannel, QueryResChannel},
    tablet::{self, CachedTablet},
    Result,
};

const REGION_READ_PROGRESS_CAP: usize = 128;

/// A peer that delegates commands between state machine and raft.
pub struct Peer<EK: KvEngine, ER: RaftEngine> {
    pub(crate) raft_group: RawNode<Storage<ER>>,
    tablet: CachedTablet<EK>,
    /// We use a cache for looking up peers. Not all peers exist in region's
    /// peer list, for example, an isolated peer may need to send/receive
    /// messages with unknown peers after recovery.
    peer_cache: Vec<metapb::Peer>,
    pub(crate) async_writer: AsyncWriter<EK, ER>,
    has_ready: bool,
    pub(crate) logger: Logger,
    pub(crate) pending_reads: ReadIndexQueue<QueryResChannel>,
    read_progress: Arc<RegionReadProgress>,
    pub(crate) tag: String,
    txn_extra_op: Arc<AtomicCell<TxnExtraOp>>,
    /// Transaction extensions related to this peer.
    txn_ext: Arc<TxnExt>,

    /// Indicates whether the peer should be woken up.
    pub(crate) should_wake_up: bool,

    /// Time of the last attempt to wake up inactive leader.
    pub(crate) bcast_wake_up_time: Option<TiInstant>,
    pub(crate) leader_lease: Lease,
    pending_remove: bool,

    /// Check whether this proposal can be proposed based on its epoch.
    cmd_epoch_checker: CmdEpochChecker<QueryResChannel>,
    proposals: ProposalQueue<CmdResChannel>,
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    /// Creates a new peer.
    ///
    /// If peer is destroyed, `None` is returned.
    pub fn new(
        cfg: &Config,
        region_id: u64,
        store_id: u64,
        tablet_factory: &dyn TabletFactory<EK>,
        engine: ER,
        scheduler: Scheduler<RaftlogFetchTask>,
        logger: &Logger,
    ) -> Result<Option<Self>> {
        let s = match Storage::new(region_id, store_id, engine, scheduler, logger)? {
            Some(s) => s,
            None => return Ok(None),
        };
        let logger = s.logger().clone();

        let applied_index = s.apply_state().get_applied_index();
        let peer_id = s.peer().get_id();

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

        let tablet_index = s.region_state().get_tablet_index();
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

        let raft_group = RawNode::new(&raft_cfg, s, &logger)?;
        let region = raft_group.store().region_state().get_region().clone();
        let tag = format!("[region {}] {}", region.get_id(), peer_id);
        Ok(Some(Peer {
            raft_group,
            tablet: CachedTablet::new(tablet),
            has_ready: false,
            async_writer: AsyncWriter::new(region_id, peer_id),
            logger,
            peer_cache: vec![],
            pending_reads: Default::default(),
            read_progress: Arc::new(RegionReadProgress::new(
                &region,
                applied_index,
                REGION_READ_PROGRESS_CAP,
                tag.clone(),
            )),
            tag: tag.clone(),
            txn_extra_op: Arc::new(AtomicCell::new(TxnExtraOp::Noop)),
            txn_ext: Arc::new(TxnExt::default()),
            should_wake_up: false,
            bcast_wake_up_time: None,
            leader_lease: Lease::new(
                cfg.raft_store_max_leader_lease(),
                cfg.renew_leader_lease_advance_duration(),
            ),
            pending_remove: false,
            cmd_epoch_checker: Default::default(),
            proposals: ProposalQueue::new(tag),
        }))
    }

    #[inline]
    pub fn storage(&self) -> &Storage<ER> {
        self.raft_group.store()
    }

    #[inline]
    pub fn logger(&self) -> &Logger {
        &self.logger
    }

    #[inline]
    pub fn get_store(&self) -> &Storage<ER> {
        self.raft_group.store()
    }

    pub fn push_pending_read(&mut self, read: ReadIndexRequest<QueryResChannel>, is_leader: bool) {
        self.pending_reads.push_back(read, is_leader);
    }

    fn pre_propose<T: Transport>(
        &mut self,
        poll_ctx: &mut StoreContext<EK, ER, T>,
        req: &mut RaftCmdRequest,
    ) -> Result<ProposalContext> {
        // TODO: coprocessor_host.pre_propose;
        // poll_ctx.coprocessor_host.pre_propose(self.region(), req)?;
        let mut ctx = ProposalContext::empty();

        if get_sync_log_from_request(req) {
            ctx.insert(ProposalContext::SYNC_LOG);
        }

        // TODO: to handle AdminCmdType such as split, merge.
        Ok(ctx)
    }

    fn next_proposal_index(&self) -> u64 {
        self.raft_group.raft.raft_log.last_index() + 1
    }

    // TODO: DiskFull
    pub(crate) fn propose_normal<T: Transport>(
        &mut self,
        poll_ctx: &mut StoreContext<EK, ER, T>,
        mut req: RaftCmdRequest,
    ) -> Result<Either<u64, u64>> {
        // TODO: add force leader check
        // TODO: to handle AdminCmdType::RollbackMerge, AdminCmdType::PrepareMerge

        poll_ctx.raft_metrics.propose.normal.inc();

        if self.has_applied_to_current_term() {
            // Only when applied index's term is equal to current leader's term, the
            // information in epoch checker is up to date and can be used to check epoch.
            if let Some(index) = self
                .cmd_epoch_checker
                .propose_check_epoch(&req, self.term())
            {
                return Ok(Either::Right(index));
            }
        } else if req.has_admin_request() {
            // The admin request is rejected because it may need to update epoch checker
            // which introduces an uncertainty and may breaks the correctness of epoch
            // checker.
            return Err(box_err!(
                "{} peer has not applied to current term, applied_term {}, current_term {}",
                self.tag(),
                self.store_applied_term(),
                self.term()
            ));
        }

        // TODO: validate request for unexpected changes.
        let ctx = match self.pre_propose(poll_ctx, &mut req) {
            Ok(ctx) => ctx,
            Err(e) => {
                // TODO: add PrepareMerge logging code
                return Err(e);
            }
        };

        let data = req.write_to_bytes()?;

        // TODO: use local histogram metrics
        PEER_PROPOSE_LOG_SIZE_HISTOGRAM.observe(data.len() as f64);

        if data.len() as u64 > poll_ctx.cfg.raft_entry_max_size.0 {
            error!(
                self.logger,
                "entry is too large";
                "size" => data.len(),
            );
            return Err(Error::RaftEntryTooLarge {
                region_id: self.region_id(),
                entry_size: data.len() as u64,
            });
        }

        self.maybe_inject_propose_error(&req)?;
        let propose_index = self.next_proposal_index();
        self.raft_group.propose(ctx.to_vec(), data)?;
        if self.next_proposal_index() == propose_index {
            // The message is dropped silently, this usually due to leader absence
            // or transferring leader. Both cases can be considered as NotLeader error.
            return Err(Error::NotLeader(self.region_id(), None));
        }

        Ok(Either::Left(propose_index))
    }

    pub(crate) fn post_propose<T>(
        &mut self,
        poll_ctx: &mut StoreContext<EK, ER, T>,
        mut p: Proposal<CmdResChannel>,
    ) {
        // Try to renew leader lease on every consistent read/write request.
        if poll_ctx.current_time.is_none() {
            poll_ctx.current_time = Some(monotonic_raw_now());
        }
        p.propose_time = poll_ctx.current_time;

        self.proposals.push(p);
    }

    pub fn storage_mut(&mut self) -> &mut Storage<ER> {
        self.raft_group.mut_store()
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
    pub fn get_peer_from_cache(&self, peer_id: u64) -> Option<metapb::Peer> {
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

    /// Term of the state machine.
    #[inline]
    pub fn term(&self) -> u64 {
        self.raft_group.raft.term
    }
}

impl<EK, ER> RaftPeer<EK, ER> for Peer<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    type Callback = QueryResChannel;
    #[inline]
    fn region(&self) -> &metapb::Region {
        self.get_store().region_state().get_region()
    }

    #[inline]
    fn peer_id(&self) -> u64 {
        self.raft_group.store().peer().get_id()
    }

    #[inline]
    // TODO
    fn is_splitting(&self) -> bool {
        false
    }

    #[inline]
    // TODO
    fn is_merging(&self) -> bool {
        false
    }

    #[inline]
    fn is_leader(&self) -> bool {
        self.raft_group.raft.state == StateRole::Leader
    }

    #[inline]
    fn term(&self) -> u64 {
        self.raft_group.raft.term
    }

    #[inline]
    fn store_commit_index(&self) -> u64 {
        self.get_store().commit_index()
    }

    #[inline]
    fn leader_id(&self) -> u64 {
        self.raft_group.raft.leader_id
    }

    #[inline]
    fn region_id(&self) -> u64 {
        self.raft_group.store().region_state().get_region().get_id()
    }

    #[inline]
    fn tag(&self) -> &String {
        &self.tag
    }

    #[inline]
    fn txn_ext(&self) -> Arc<TxnExt> {
        self.txn_ext.clone()
    }

    #[inline]
    fn read_progress(&self) -> Arc<RegionReadProgress> {
        self.read_progress.clone()
    }

    #[inline]
    fn peer(&self) -> &metapb::Peer {
        self.raft_group.store().peer()
    }

    #[inline]
    // TODO
    fn bucket_meta(&self) -> Option<Arc<BucketMeta>> {
        None
    }

    #[inline]
    fn txn_extra_op(&self) -> Arc<AtomicCell<TxnExtraOp>> {
        self.txn_extra_op.clone()
    }

    #[inline]
    fn mut_pending_reads(&mut self) -> &mut ReadIndexQueue<Self::Callback> {
        &mut self.pending_reads
    }

    #[inline]
    fn mut_bcast_wake_up_time(&mut self) -> &mut Option<TiInstant> {
        &mut self.bcast_wake_up_time
    }

    #[inline]
    fn set_should_wake_up(&mut self, should_wake_up: bool) {
        self.should_wake_up = should_wake_up;
    }

    #[inline]
    fn pending_remove(&self) -> bool {
        self.pending_remove
    }

    #[inline]
    fn has_force_leader(&self) -> bool {
        false
    }

    #[inline]
    fn mut_leader_lease(&mut self) -> &mut Lease {
        &mut self.leader_lease
    }

    #[inline]
    fn store_applied_term(&self) -> u64 {
        self.get_store().applied_term()
    }

    #[inline]
    fn store_applied_index(&self) -> u64 {
        self.get_store().applied_index()
    }

    #[inline]
    fn has_pending_merge_state(&self) -> bool {
        // TODOTODO
        false
    }

    #[inline]
    fn is_handling_snapshot(&self) -> bool {
        // TODOTODO
        false
    }
}

impl<EK, ER> RequestInspector for Peer<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn has_applied_to_current_term(&mut self) -> bool {
        self.get_store().applied_term() == self.term()
    }

    fn inspect_lease(&mut self) -> LeaseState {
        if !self.raft_group.raft.in_lease() {
            return LeaseState::Suspect;
        }
        // None means now.
        let state = self.leader_lease.inspect(None);
        if LeaseState::Expired == state {
            debug!(
                self.logger,
                "leader lease is expired, region_id {}, peer_id {}, lease {:?}",
                self.region_id(),
                self.peer_id(),
                self.leader_lease,
            );
            // The lease is expired, call `expire` explicitly.
            self.leader_lease.expire();
        }
        state
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn init_peer<EK: KvEngine, ER: RaftEngine>() -> Result<Option<Peer<EK, ER>>> {
        Ok(None)
    }
}
