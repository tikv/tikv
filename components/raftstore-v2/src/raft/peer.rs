// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::VecDeque, mem, sync::Arc, time::Duration};

use crossbeam::atomic::AtomicCell;
use engine_traits::{KvEngine, OpenOptions, RaftEngine, TabletFactory};
use kvproto::{
    kvrpcpb::{ExtraOp as TxnExtraOp, LockInfo},
    metapb,
    raft_cmdpb::{self, CmdType, RaftCmdRequest, RaftCmdResponse},
    raft_serverpb::RegionLocalState,
};
use pd_client::BucketMeta;
use raft::{RawNode, Ready, StateRole, INVALID_ID};
use raftstore::{
    store::{
        cmd_resp,
        fsm::{apply::notify_stale_req, store::RaftSender, Proposal},
        metrics::RAFT_READ_INDEX_PENDING_COUNT,
        peer::{propose_read_index, ForceLeaderState, ProposalQueue, RaftPeer, RequestInspector},
        read_queue::{ReadIndexContext, ReadIndexQueue, ReadIndexRequest},
        util::{check_region_epoch, find_peer, Lease, LeaseState, RegionReadProgress},
        worker::{LocalReadContext, RaftlogFetchTask, ReadExecutor},
        Callback, Config, EntryStorage, PdTask, RaftCommand, RaftlogFetchTask, Transport, TxnExt,
        WriteRouter,
    },
    Error,
};
use slog::{debug, error, info, o, Logger};
use tikv_util::{
    box_err,
    config::ReadableSize,
    time::{duration_to_sec, monotonic_raw_now, Instant as TiInstant, InstantExt, ThreadReadId},
    worker::Scheduler,
    Either,
};

use super::storage::Storage;
use crate::{
    batch::StoreContext,
    operation::AsyncWriter,
    tablet::{self, CachedTablet},
    Result,
};

const REGION_READ_PROGRESS_CAP: usize = 128;
const MIN_BCAST_WAKE_UP_INTERVAL: u64 = 1_000; // 1s

/// A peer that delegates commands between state machine and raft.
pub struct Peer<EK: KvEngine, ER: RaftEngine> {
    raft_group: RawNode<Storage<ER>>,
    tablet: CachedTablet<EK>,
    /// We use a cache for looking up peers. Not all peers exist in region's
    /// peer list, for example, an isolated peer may need to send/receive
    /// messages with unknown peers after recovery.
    peer_cache: Vec<metapb::Peer>,
    pub(crate) async_writer: AsyncWriter<EK, ER>,
    has_ready: bool,
    pub(crate) logger: Logger,
    pending_reads: ReadIndexQueue<EK::Snapshot>,
    read_progress: Arc<RegionReadProgress>,
    tag: String,
    txn_extra_op: Arc<AtomicCell<TxnExtraOp>>,
    /// Transaction extensions related to this peer.
    txn_ext: Arc<TxnExt>,

    /// Indicates whether the peer should be woken up.
    should_wake_up: bool,

    /// Time of the last attempt to wake up inactive leader.
    bcast_wake_up_time: Option<TiInstant>,
    leader_lease: Lease,
    proposals: ProposalQueue<EK::Snapshot>,
    pending_remove: bool,
    force_leader: Option<ForceLeaderState>,
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
            proposals: ProposalQueue::new(tag),
            pending_remove: false,
            force_leader: None,
        }))
    }

    #[inline]
    pub fn storage(&self) -> &Storage<ER> {
        self.raft_group.store()
    }

    pub fn region(&self) -> &metapb::Region {
        self.raft_group.store().region()
    }

    #[inline]
    pub fn logger(&self) -> &Logger {
        &self.logger
    }

    #[inline]
    pub fn get_store(&self) -> &Storage<ER> {
        self.raft_group.store()
    }

    #[inline]
    pub fn is_leader(&self) -> bool {
        self.raft_group.raft.state == StateRole::Leader
    }

    pub fn push_pending_read(&mut self, read: ReadIndexRequest<EK::Snapshot>, is_leader: bool) {
        self.pending_reads.push_back(read, is_leader);
    }

    // TODO
    fn propose_normal<T: Transport>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        mut req: RaftCmdRequest,
    ) -> Result<Either<u64, u64>> {
        // TODO
        Ok(Either::Left(0))
    }

    // Returns a boolean to indicate whether the `read` is proposed or not.
    // For these cases it won't be proposed:
    // 1. The region is in merging or splitting;
    // 2. The message is stale and dropped by the Raft group internally;
    // 3. There is already a read request proposed in the current lease;
    fn read_index<T: Transport>(
        &mut self,
        poll_ctx: &mut StoreContext<EK, ER, T>,
        mut req: RaftCmdRequest,
        mut err_resp: RaftCmdResponse,
        cb: Callback<EK::Snapshot>,
    ) -> bool {
        if let Err(e) = self.pre_read_index() {
            debug!(
                self.logger,
                "prevents unsafe read index";
                "err" => ?e,
            );
            poll_ctx.raft_metrics.propose.unsafe_read_index += 1;
            cmd_resp::bind_error(&mut err_resp, e);
            cb.invoke_with_response(err_resp);
            self.should_wake_up = true;
            return false;
        }

        let now = monotonic_raw_now();
        if self.is_leader() {
            let lease_state = self.inspect_lease();
            if self.can_amend_read::<Peer<EK, ER>>(
                &req,
                lease_state,
                poll_ctx.cfg.raft_store_max_leader_lease(),
            ) {
                // Must use the commit index of `PeerStorage` instead of the commit index
                // in raft-rs which may be greater than the former one.
                // For more details, see the annotations above `on_leader_commit_idx_changed`.
                let commit_index = self.store_commit_index();
                if let Some(read) = self.mut_pending_reads().back_mut() {
                    // A read request proposed in the current lease is found; combine the new
                    // read request to that previous one, so that no proposing needed.
                    read.push_command(req, cb, commit_index);
                    return false;
                }
            }
        }

        if self.read_index_no_leader(
            &mut poll_ctx.trans,
            &mut poll_ctx.pd_scheduler,
            &mut err_resp,
        ) {
            poll_ctx.raft_metrics.invalid_proposal.read_index_no_leader += 1;
            cb.invoke_with_response(err_resp);
            return false;
        }

        poll_ctx.raft_metrics.propose.read_index += 1;
        self.bcast_wake_up_time = None;

        let request = req
            .mut_requests()
            .get_mut(0)
            .filter(|req| req.has_read_index())
            .map(|req| req.take_read_index());
        let (id, dropped) = propose_read_index(&mut self.raft_group, request.as_ref(), None);
        if dropped && self.is_leader() {
            // The message gets dropped silently, can't be handled anymore.
            notify_stale_req(self.term(), cb);
            poll_ctx.raft_metrics.propose.dropped_read_index += 1;
            return false;
        }

        let mut read = ReadIndexRequest::with_command(id, req, cb, now);
        read.addition_request = request.map(Box::new);
        self.push_pending_read(read, self.is_leader());
        self.should_wake_up = true;

        debug!(
            self.logger,
            "request to get a read index";
            "request_id" => ?id,
            "is_leader" => self.is_leader(),
        );

        // TimeoutNow has been sent out, so we need to propose explicitly to
        // update leader lease.
        if self.leader_lease.is_suspect() {
            let req = RaftCmdRequest::default();
            if let Ok(Either::Left(index)) = self.propose_normal(poll_ctx, req) {
                let p = Proposal {
                    is_conf_change: false,
                    index,
                    term: self.term(),
                    cb: Callback::None,
                    propose_time: Some(now),
                    must_pass_epoch_check: false,
                };
                self.post_propose(&mut poll_ctx.current_time, p);
            }
        }

        true
    }

    fn read_local<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        req: RaftCmdRequest,
        cb: Callback<EK::Snapshot>,
    ) {
        ctx.raft_metrics.propose.local_read += 1;
        let commit_index = self.get_store().commit_index();
        let mut reader = self.tablet.clone();
        cb.invoke_read(self.handle_read(&mut reader, req, false, Some(commit_index)))
    }

    /// Responses to the ready read index request on the replica, the replica is
    /// not a leader.
    fn post_pending_read_index_on_replica<T>(&mut self, ctx: &mut StoreContext<EK, ER, T>) {
        while let Some(mut read) = self.pending_reads.pop_front() {
            // The response of this read index request is lost, but we need it for
            // the memory lock checking result. Resend the request.
            if let Some(read_index) = read.addition_request.take() {
                assert_eq!(read.cmds().len(), 1);
                let (mut req, cb, _) = read.take_cmds().pop().unwrap();
                assert_eq!(req.requests.len(), 1);
                req.requests[0].set_read_index(*read_index);
                let read_cmd = RaftCommand::new(req, cb);
                info!(
                    self.logger,
                    "re-propose read index request because the response is lost";
                    "region_id" => self.region_id(),
                    "peer_id" => self.peer_id(),
                );
                RAFT_READ_INDEX_PENDING_COUNT.sub(1);
                self.send_read_command(&ctx.router, read_cmd);
                continue;
            }

            assert!(read.read_index.is_some());
            let is_read_index_request = read.cmds().len() == 1
                && read.cmds()[0].0.get_requests().len() == 1
                && read.cmds()[0].0.get_requests()[0].get_cmd_type() == CmdType::ReadIndex;

            if is_read_index_request {
                self.response_read(&mut read, ctx, false);
            } else if self.ready_to_handle_unsafe_replica_read(read.read_index.unwrap()) {
                self.response_read(&mut read, ctx, true);
            } else {
                // TODO: `ReadIndex` requests could be blocked.
                self.mut_pending_reads().push_front(read);
                break;
            }
        }
    }

    fn apply_reads<T>(&mut self, ctx: &mut StoreContext<EK, ER, T>, ready: &Ready) {
        let mut propose_time = None;
        let states = ready.read_states().iter().map(|state| {
            let read_index_ctx = ReadIndexContext::parse(state.request_ctx.as_slice()).unwrap();
            (read_index_ctx.id, read_index_ctx.locked, state.index)
        });
        // The follower may lost `ReadIndexResp`, so the pending_reads does not
        // guarantee the orders are consistent with read_states. `advance` will
        // update the `read_index` of read request that before this successful
        // `ready`.
        if !self.is_leader() {
            // NOTE: there could still be some pending reads proposed by the peer when it
            // was leader. They will be cleared in `clear_uncommitted_on_role_change` later
            // in the function.
            self.pending_reads.advance_replica_reads(states);
            self.post_pending_read_index_on_replica(ctx);
        } else {
            self.pending_reads.advance_leader_reads(&self.tag, states);
            propose_time = self.pending_reads.last_ready().map(|r| r.propose_time);
            if self.ready_to_handle_read() {
                while let Some(mut read) = self.pending_reads.pop_front() {
                    self.response_read(&mut read, ctx, false);
                }
            }
        }

        // Note that only after handle read_states can we identify what requests are
        // actually stale.
        if ready.ss().is_some() {
            let term = self.term();
            // all uncommitted reads will be dropped silently in raft.
            self.pending_reads.clear_uncommitted_on_role_change(term);
        }

        if let Some(propose_time) = propose_time {
            if self.leader_lease.is_suspect() {
                return;
            }
            self.maybe_renew_leader_lease(propose_time, &mut ctx.store_meta, None);
        }
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

    #[inline]
    pub fn is_leader(&self) -> bool {
        self.raft_group.raft.state == StateRole::Leader
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
    fn mut_pending_reads(&mut self) -> &mut ReadIndexQueue<EK::Snapshot> {
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
    fn mut_proposals(&mut self) -> &mut ProposalQueue<EK::Snapshot> {
        &mut self.proposals
    }

    #[inline]
    fn pending_remove(&self) -> bool {
        self.pending_remove
    }

    #[inline]
    fn has_force_leader(&self) -> bool {
        self.force_leader.is_some()
    }

    #[inline]
    fn mut_leader_lease(&mut self) -> &mut Lease {
        &mut self.leader_lease
    }

    #[inline]
    fn store_applied_term(&self) -> u64 {
        self.get_store().applied_term()
    }
}

impl<EK> ReadExecutor<EK> for CachedTablet<EK>
where
    EK: KvEngine,
{
    fn get_tablet(&mut self) -> &EK {
        self.latest().unwrap()
    }

    fn get_snapshot(
        &mut self,
        _: Option<ThreadReadId>,
        _: &mut Option<LocalReadContext<'_, EK>>,
    ) -> Arc<EK::Snapshot> {
        Arc::new(self.get_tablet().snapshot())
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
