// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, time::Duration};

use crossbeam::atomic::AtomicCell;
use engine_traits::{KvEngine, RaftEngine, TabletFactory};
use kvproto::{
    kvrpcpb::{ExtraOp as TxnExtraOp, LockInfo},
    metapb,
    raft_cmdpb::{self, RaftCmdRequest, RaftCmdResponse},
    raft_serverpb::RegionLocalState,
};
use pd_client::BucketMeta;
use raft::{RawNode, StateRole, INVALID_ID};
use raftstore::{
    store::{
        cmd_resp,
        fsm::{apply::notify_stale_req, Proposal},
        peer::{propose_read_index, RaftPeer, RequestInspector},
        read_queue::{ReadIndexContext, ReadIndexQueue, ReadIndexRequest},
        util::{check_region_epoch, find_peer, Lease, LeaseState, RegionReadProgress},
        worker::{RaftlogFetchTask, ReadExecutor},
        Callback, Config, PdTask, Transport, TxnExt,
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
    tablet::{self, CachedTablet},
    Result,
};

const REGION_READ_PROGRESS_CAP: usize = 128;
const MIN_BCAST_WAKE_UP_INTERVAL: u64 = 1_000; // 1s

/// A peer that delegates commands between state machine and raft.
pub struct Peer<EK: KvEngine, ER: RaftEngine> {
    raft_group: RawNode<Storage<ER>>,
    tablet: CachedTablet<EK>,
    logger: Logger,
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
        logger: &Logger,
        raftlog_fetch_scheduler: Scheduler<RaftlogFetchTask>,
    ) -> Result<Option<Self>> {
        let s = match Storage::new(region_id, store_id, engine, logger, raftlog_fetch_scheduler)? {
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
            Some(tablet_factory.open_tablet(region_id, tablet_index)?)
        } else {
            None
        };

        let raft_group = RawNode::new(&raft_cfg, s, &logger)?;
        let region = raft_group.store().region_state().get_region().clone();
        let tag = format!("[region {}] {}", region.get_id(), peer_id);
        Ok(Some(Peer {
            raft_group,
            tablet: CachedTablet::new(tablet),
            logger,
            pending_reads: Default::default(),
            read_progress: Arc::new(RegionReadProgress::new(
                &region,
                applied_index,
                REGION_READ_PROGRESS_CAP,
                tag.clone(),
            )),
            tag,
            txn_extra_op: Arc::new(AtomicCell::new(TxnExtraOp::Noop)),
            txn_ext: Arc::new(TxnExt::default()),
            should_wake_up: false,
            bcast_wake_up_time: None,
            leader_lease: Lease::new(
                cfg.raft_store_max_leader_lease(),
                cfg.renew_leader_lease_advance_duration(),
            ),
        }))
    }

    #[inline]
    pub fn region_id(&self) -> u64 {
        self.raft_group.store().region_state().get_region().get_id()
    }

    #[inline]
    pub fn storage(&self) -> &Storage<ER> {
        self.raft_group.store()
    }

    #[inline]
    pub fn tablet(&self) -> &CachedTablet<EK> {
        &self.tablet
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
            // poll_ctx.raft_metrics.propose.unsafe_read_index += 1;
            cmd_resp::bind_error(&mut err_resp, e);
            cb.invoke_with_response(err_resp);
            // self.should_wake_up = true;
            return false;
        }

        let now = monotonic_raw_now();
        if self.is_leader() {
            match self.inspect_lease() {
                // Here combine the new read request with the previous one even if the lease expired is
                // ok because in this case, the previous read index must be sent out with a valid
                // lease instead of a suspect lease. So there must no pending transfer-leader proposals
                // before or after the previous read index, and the lease can be renewed when get
                // heartbeat responses.
                LeaseState::Valid | LeaseState::Expired => {
                    // Must use the commit index of `PeerStorage` instead of the commit index
                    // in raft-rs which may be greater than the former one.
                    // For more details, see the annotations above `on_leader_commit_idx_changed`.
                    let commit_index = self.get_store().commit_index();
                    if let Some(read) = self.pending_reads.back_mut() {
                        let max_lease = poll_ctx.cfg.raft_store_max_leader_lease();
                        let is_read_index_request = req
                            .get_requests()
                            .get(0)
                            .map(|req| req.has_read_index())
                            .unwrap_or_default();
                        // A read index request or a read with addition request always needs the response of
                        // checking memory lock for async commit, so we cannot apply the optimization here
                        if !is_read_index_request
                            && read.addition_request.is_none()
                            && read.propose_time + max_lease > now
                        {
                            // A read request proposed in the current lease is found; combine the new
                            // read request to that previous one, so that no proposing needed.
                            read.push_command(req, cb, commit_index);
                            return false;
                        }
                    }
                }
                // If the current lease is suspect, new read requests can't be appended into
                // `pending_reads` because if the leader is transferred, the latest read could
                // be dirty.
                _ => {}
            }
        }

        // When a replica cannot detect any leader, `MsgReadIndex` will be dropped, which would
        // cause a long time waiting for a read response. Then we should return an error directly
        // in this situation.
        if !self.is_leader() && self.leader_id() == INVALID_ID {
            // poll_ctx.raft_metrics.invalid_proposal.read_index_no_leader += 1;
            // The leader may be hibernated, send a message for trying to awaken the leader.
            if self.bcast_wake_up_time.is_none()
                || self
                    .bcast_wake_up_time
                    .as_ref()
                    .unwrap()
                    .saturating_elapsed()
                    >= Duration::from_millis(MIN_BCAST_WAKE_UP_INTERVAL)
            {
                self.bcast_wake_up_message(&mut poll_ctx.trans);
                self.bcast_wake_up_time = Some(TiInstant::now_coarse());

                let task = PdTask::QueryRegionLeader {
                    region_id: self.region_id(),
                };
                if let Err(e) = poll_ctx.pd_scheduler.schedule(task) {
                    error!(
                        self.logger,
                        "failed to notify pd";
                        "err" => %e,
                    )
                }
            }
            // self.should_wake_up = true;
            cmd_resp::bind_error(&mut err_resp, Error::NotLeader(self.region_id(), None));
            cb.invoke_with_response(err_resp);
            return false;
        }

        //poll_ctx.raft_metrics.propose.read_index += 1;
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
            //poll_ctx.raft_metrics.propose.dropped_read_index += 1;
            return false;
        }

        let mut read = ReadIndexRequest::with_command(id, req, cb, now);
        read.addition_request = request.map(Box::new);
        self.push_pending_read(read, self.is_leader());
        // self.should_wake_up = true;

        debug!(
            self.logger,
            "request to get a read index";
            "request_id" => ?id,
            "region_id" => self.region_id(),
            "peer_id" => self.peer_id(),
            "is_leader" => self.is_leader(),
        );

        // TimeoutNow has been sent out, so we need to propose explicitly to
        // update leader lease.
        if self.leader_lease.is_suspect() {
            let req = RaftCmdRequest::default();
            if let Ok(Either::Left(index)) = self.propose_normal(poll_ctx, req) {
                /*let p = Proposal {
                    is_conf_change: false,
                    index,
                    term: self.term(),
                    cb: Callback::None,
                    propose_time: Some(now),
                    must_pass_epoch_check: false,
                };
                self.post_propose(poll_ctx, p);
                */
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
        // ctx.raft_metrics.propose.local_read += 1;
        let commit_index = self.get_store().commit_index();
        let mut reader = self.tablet.clone();
        cb.invoke_read(self.handle_read(&mut reader, req, false, Some(commit_index)))
    }

    /*
    pub fn propose<T: Transport>(
        &mut self,
        ctx: &mut StoreContext<T>,
        mut cb: Callback<EK::Snapshot>,
        req: RaftCmdRequest,
        mut err_resp: RaftCmdResponse,
        disk_full_opt: DiskFullOpt,
    ) -> bool {
        /*if self.pending_remove {
            return false;
        }

        ctx.raft_metrics.propose.all += 1;*/

        let req_admin_cmd_type = if !req.has_admin_request() {
            None
        } else {
            Some(req.get_admin_request().get_cmd_type())
        };
        let is_urgent = is_request_urgent(&req);

        let policy = self.inspect(&req);
        let res = match policy {
            Ok(RequestPolicy::ReadLocal) | Ok(RequestPolicy::StaleRead) => {
                self.read_local(ctx, req, cb);
                return false;
            }
            Ok(RequestPolicy::ReadIndex) => return self.read_index(ctx, req, err_resp, cb),
            Ok(_) => {},
            Err(e) => Err(e),
        };
        fail_point!("after_propose");

        match res {
            Err(e) => {
                cmd_resp::bind_error(&mut err_resp, e);
                cb.invoke_with_response(err_resp);
                // self.post_propose_fail(req_admin_cmd_type);
                false
            }
        }
    }*/
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
    fn get_region_id(&self) -> u64 {
        self.region_id()
    }

    #[inline]
    fn get_tag(&self) -> &String {
        &self.tag
    }

    #[inline]
    fn get_txn_ext(&self) -> Arc<TxnExt> {
        self.txn_ext.clone()
    }

    #[inline]
    fn get_read_progress(&self) -> Arc<RegionReadProgress> {
        self.read_progress.clone()
    }

    #[inline]
    fn get_peer(&self) -> &metapb::Peer {
        self.raft_group.store().peer()
    }

    #[inline]
    // TODO
    fn bucket_meta(&self) -> Option<Arc<BucketMeta>> {
        None
    }

    #[inline]
    fn get_txn_extra_op(&self) -> Arc<AtomicCell<TxnExtraOp>> {
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
}

impl<EK> ReadExecutor<EK> for CachedTablet<EK>
where
    EK: KvEngine,
{
    fn get_engine(&self) -> &EK {
        // TODO: should check the cache is latest here?
        // If that, get_engine should sue &mut self instead
        self.cache().unwrap()
    }

    fn get_snapshot(&mut self, _: Option<ThreadReadId>) -> Arc<EK::Snapshot> {
        Arc::new(self.get_engine().snapshot())
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
