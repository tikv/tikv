// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! There are two types of Query: KV read and status query.
//!
//! KV Read is implemented in local module and lease module.
//! Read will be executed in callee thread if in lease, which is
//! implemented in local module. If lease is expired, it will extend the lease
//! first. Lease maintainance is implemented in lease module.
//!
//! Status query is implemented in the root module directly.
//! Follower's read index and replica read is implemenented replica module.
//! Leader's read index and lease renew is implemented in lease module.

use std::cmp;

use crossbeam::channel::TrySendError;
use engine_traits::{KvEngine, RaftEngine};
use kvproto::{
    errorpb,
    raft_cmdpb::{CmdType, RaftCmdRequest, RaftCmdResponse, StatusCmdType},
};
use raft::{Ready, StateRole};
use raftstore::{
    errors::RAFTSTORE_IS_BUSY,
    store::{
        cmd_resp, local_metrics::RaftMetrics, metrics::RAFT_READ_INDEX_PENDING_COUNT,
        msg::ErrorCallback, region_meta::RegionMeta, util, util::LeaseState, GroupState,
        ReadIndexContext, ReadProgress, RequestPolicy, Transport,
    },
    Error, Result,
};
use slog::{debug, info};
use tikv_util::box_err;
use txn_types::WriteBatchFlags;

use crate::{
    batch::StoreContext,
    fsm::PeerFsmDelegate,
    raft::Peer,
    router::{
        message::RaftRequest, DebugInfoChannel, PeerMsg, QueryResChannel, QueryResult, ReadResponse,
    },
};

mod lease;
mod local;
mod replica;

pub(crate) use self::local::LocalReader;

impl<'a, EK: KvEngine, ER: RaftEngine, T: raftstore::store::Transport>
    PeerFsmDelegate<'a, EK, ER, T>
{
    fn inspect_read(&mut self, req: &RaftCmdRequest) -> Result<RequestPolicy> {
        if req.get_header().get_read_quorum() {
            return Ok(RequestPolicy::ReadIndex);
        }

        // If applied index's term is differ from current raft's term, leader transfer
        // must happened, if read locally, we may read old value.
        if !self.fsm.peer().applied_to_current_term() {
            return Ok(RequestPolicy::ReadIndex);
        }

        match self.fsm.peer_mut().inspect_lease() {
            LeaseState::Valid => Ok(RequestPolicy::ReadLocal),
            LeaseState::Expired | LeaseState::Suspect => {
                // Perform a consistent read to Raft quorum and try to renew the leader lease.
                Ok(RequestPolicy::ReadIndex)
            }
        }
    }

    #[inline]
    pub fn on_query(&mut self, req: RaftCmdRequest, ch: QueryResChannel) {
        if !req.has_status_request() {
            if let Err(e) = self
                .fsm
                .peer_mut()
                .validate_query_msg(&req, &mut self.store_ctx.raft_metrics)
            {
                let resp = cmd_resp::new_error(e);
                ch.report_error(resp);
                return;
            }
            let policy = self.inspect_read(&req);
            match policy {
                Ok(RequestPolicy::ReadIndex) => {
                    self.fsm.peer_mut().read_index(self.store_ctx, req, ch);
                }
                Ok(RequestPolicy::ReadLocal) => {
                    self.store_ctx.raft_metrics.propose.local_read.inc();
                    let read_resp = ReadResponse::new(0);
                    ch.set_result(QueryResult::Read(read_resp));
                }
                _ => {
                    panic!("inspect_read is expected to only return ReadIndex or ReadLocal");
                }
            };
        } else {
            self.fsm.peer_mut().on_query_status(&req, ch);
        }
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    fn validate_query_msg(
        &mut self,
        msg: &RaftCmdRequest,
        raft_metrics: &mut RaftMetrics,
    ) -> Result<()> {
        // check query specific requirements
        if msg.has_admin_request() {
            return Err(box_err!("PeerMsg::RaftQuery does not allow admin requests"));
        }

        // check query specific requirements
        for r in msg.get_requests() {
            if r.get_cmd_type() != CmdType::Get
                && r.get_cmd_type() != CmdType::Snap
                && r.get_cmd_type() != CmdType::ReadIndex
            {
                return Err(box_err!(
                    "PeerMsg::RaftQuery does not allow write requests: {:?}",
                    r.get_cmd_type()
                ));
            }
        }

        // Check store_id, make sure that the msg is dispatched to the right place.
        if let Err(e) = util::check_store_id(msg.get_header(), self.peer().get_store_id()) {
            raft_metrics.invalid_proposal.mismatch_store_id.inc();
            return Err(e);
        }

        let flags = WriteBatchFlags::from_bits_check(msg.get_header().get_flags());
        if flags.contains(WriteBatchFlags::STALE_READ) {
            return Err(box_err!(
                "PeerMsg::RaftQuery should not get stale read requests"
            ));
        }

        // TODO: add flashback_state check

        // Check whether the store has the right peer to handle the request.
        let request = msg.get_requests();

        // TODO: add force leader

        // ReadIndex can be processed on the replicas.
        let is_read_index_request =
            request.len() == 1 && request[0].get_cmd_type() == CmdType::ReadIndex;

        let allow_replica_read = msg.get_header().get_replica_read();
        if !self.is_leader() && !is_read_index_request && !allow_replica_read {
            raft_metrics.invalid_proposal.not_leader.inc();
            return Err(Error::NotLeader(self.region_id(), self.leader()));
        }

        // peer_id must be the same as peer's.
        if let Err(e) = util::check_peer_id(msg.get_header(), self.peer_id()) {
            raft_metrics.invalid_proposal.mismatch_peer_id.inc();
            return Err(e);
        }

        // TODO: check applying snapshot

        // Check whether the term is stale.
        if let Err(e) = util::check_term(msg.get_header(), self.term()) {
            raft_metrics.invalid_proposal.stale_command.inc();
            return Err(e);
        }

        // TODO: add check of sibling region for split
        util::check_req_region_epoch(msg, self.region(), true)
    }

    // For these cases it won't be proposed:
    // 1. The region is in merging or splitting;
    // 2. The message is stale and dropped by the Raft group internally;
    // 3. There is already a read request proposed in the current lease;
    fn read_index<T: Transport>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        req: RaftCmdRequest,
        ch: QueryResChannel,
    ) {
        // TODO: add pre_read_index to handle splitting or merging
        if self.is_leader() {
            self.read_index_leader(ctx, req, ch);
        } else {
            self.read_index_follower(ctx, req, ch);
        }
    }

    pub(crate) fn apply_reads<T>(&mut self, ctx: &mut StoreContext<EK, ER, T>, ready: &Ready) {
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
            self.pending_reads_mut().advance_replica_reads(states);
            self.post_pending_read_index_on_replica(ctx);
        } else {
            self.pending_reads_mut().advance_leader_reads(states);
            if let Some(propose_time) = self.pending_reads().last_ready().map(|r| r.propose_time) {
                if !self.leader_lease_mut().is_suspect() {
                    self.maybe_renew_leader_lease(propose_time, &ctx.store_meta, None);
                }
            }

            if self.ready_to_handle_read() {
                while let Some(mut read) = self.pending_reads_mut().pop_front() {
                    self.respond_read_index(&mut read);
                }
            }
        }

        // Note that only after handle read_states can we identify what requests are
        // actually stale.
        if ready.ss().is_some() {
            let term = self.term();
            // all uncommitted reads will be dropped silently in raft.
            self.pending_reads_mut()
                .clear_uncommitted_on_role_change(term);
        }
    }

    /// Respond to the ready read index request on the replica, the replica is
    /// not a leader.
    fn post_pending_read_index_on_replica<T>(&mut self, ctx: &mut StoreContext<EK, ER, T>) {
        while let Some(mut read) = self.pending_reads_mut().pop_front() {
            // The response of this read index request is lost, but we need it for
            // the memory lock checking result. Resend the request.
            if let Some(read_index) = read.addition_request.take() {
                assert_eq!(read.cmds().len(), 1);
                let (mut req, ch, _) = read.take_cmds().pop().unwrap();
                assert_eq!(req.requests.len(), 1);
                req.requests[0].set_read_index(*read_index);
                let read_cmd = RaftRequest::new(req, ch);
                info!(
                    self.logger,
                    "re-propose read index request because the response is lost";
                );
                RAFT_READ_INDEX_PENDING_COUNT.sub(1);
                self.send_read_command(ctx, read_cmd);
                continue;
            }

            assert!(read.read_index.is_some());
            let is_read_index_request = read.cmds().len() == 1
                && read.cmds()[0].0.get_requests().len() == 1
                && read.cmds()[0].0.get_requests()[0].get_cmd_type() == CmdType::ReadIndex;

            if is_read_index_request {
                self.respond_read_index(&mut read);
            } else if self.ready_to_handle_unsafe_replica_read(read.read_index.unwrap()) {
                self.respond_replica_read(&mut read);
            } else {
                // TODO: `ReadIndex` requests could be blocked.
                self.pending_reads_mut().push_front(read);
                break;
            }
        }
    }

    // Note: comparing with v1, it removes the snapshot check because in v2 the
    // snapshot will not delete the data anymore.
    fn ready_to_handle_unsafe_replica_read(&self, read_index: u64) -> bool {
        // Wait until the follower applies all values before the read. There is still a
        // problem if the leader applies fewer values than the follower, the follower
        // read could get a newer value, and after that, the leader may read a stale
        // value, which violates linearizability.
        self.storage().apply_state().get_applied_index() >= read_index
            // If it is in pending merge state(i.e. applied PrepareMerge), the data may be stale.
            // TODO: Add a test to cover this case
            && !self.has_pending_merge_state()
    }

    #[inline]
    pub fn ready_to_handle_read(&self) -> bool {
        // TODO: It may cause read index to wait a long time.

        // There may be some values that are not applied by this leader yet but the old
        // leader, if applied_term isn't equal to current term.
        self.applied_to_current_term()
            // There may be stale read if the old leader splits really slow,
            // the new region may already elected a new leader while
            // the old leader still think it owns the split range.
            && !self.proposal_control().is_splitting()
            // There may be stale read if a target leader is in another store and
            // applied commit merge, written new values, but the sibling peer in
            // this store does not apply commit merge, so the leader is not ready
            // to read, until the merge is rollbacked.
            && !self.proposal_control().is_merging()
    }

    fn send_read_command<T>(
        &self,
        ctx: &mut StoreContext<EK, ER, T>,
        read_cmd: RaftRequest<QueryResChannel>,
    ) {
        let mut err = errorpb::Error::default();
        let region_id = read_cmd.request.get_header().get_region_id();
        let read_ch = match ctx.router.send(region_id, PeerMsg::RaftQuery(read_cmd)) {
            Ok(()) => return,
            Err(TrySendError::Full(PeerMsg::RaftQuery(cmd))) => {
                err.set_message(RAFTSTORE_IS_BUSY.to_owned());
                err.mut_server_is_busy()
                    .set_reason(RAFTSTORE_IS_BUSY.to_owned());
                cmd.ch
            }
            Err(TrySendError::Disconnected(PeerMsg::RaftQuery(cmd))) => {
                err.set_message(format!("region {} is missing", self.region_id()));
                err.mut_region_not_found().set_region_id(self.region_id());
                cmd.ch
            }
            _ => unreachable!(),
        };
        let mut resp = RaftCmdResponse::default();
        resp.mut_header().set_error(err);
        read_ch.report_error(resp);
    }

    /// Status command is used to query target region information.
    #[inline]
    fn on_query_status(&mut self, req: &RaftCmdRequest, ch: QueryResChannel) {
        let mut response = RaftCmdResponse::default();
        if let Err(e) = self.query_status(req, &mut response) {
            cmd_resp::bind_error(&mut response, e);
        }
        ch.set_result(QueryResult::Response(response));
    }

    fn query_status(&mut self, req: &RaftCmdRequest, resp: &mut RaftCmdResponse) -> Result<()> {
        util::check_store_id(req.get_header(), self.peer().get_store_id())?;
        let cmd_type = req.get_status_request().get_cmd_type();
        let status_resp = resp.mut_status_response();
        status_resp.set_cmd_type(cmd_type);
        match cmd_type {
            StatusCmdType::RegionLeader => {
                if let Some(leader) = self.leader() {
                    status_resp.mut_region_leader().set_leader(leader);
                }
            }
            StatusCmdType::RegionDetail => {
                if !self.storage().is_initialized() {
                    let region_id = req.get_header().get_region_id();
                    return Err(Error::RegionNotInitialized(region_id));
                }
                status_resp
                    .mut_region_detail()
                    .set_region(self.region().clone());
                if let Some(leader) = self.leader() {
                    status_resp.mut_region_detail().set_leader(leader);
                }
            }
            StatusCmdType::InvalidStatus => {
                return Err(box_err!("{:?} invalid status command!", self.logger.list()));
            }
        }

        // Bind peer current term here.
        cmd_resp::bind_term(resp, self.term());
        Ok(())
    }

    /// Query internal states for debugging purpose.
    pub fn on_query_debug_info(&self, ch: DebugInfoChannel) {
        let entry_storage = self.storage().entry_storage();
        let mut status = self.raft_group().status();
        status
            .progress
            .get_or_insert_with(|| self.raft_group().raft.prs());
        let mut meta = RegionMeta::new(
            self.storage().region_state(),
            entry_storage.apply_state(),
            GroupState::Ordered,
            status,
            self.raft_group().raft.raft_log.last_index(),
            self.raft_group().raft.raft_log.persisted,
        );
        // V2 doesn't persist commit index and term, fill them with in-memory values.
        meta.raft_apply.commit_index = cmp::min(
            self.raft_group().raft.raft_log.committed,
            self.persisted_index(),
        );
        meta.raft_apply.commit_term = self
            .raft_group()
            .raft
            .raft_log
            .term(meta.raft_apply.commit_index)
            .unwrap();
        debug!(self.logger, "on query debug info";
            "tick" => self.raft_group().raft.election_elapsed,
            "election_timeout" => self.raft_group().raft.randomized_election_timeout(),
        );
        ch.set_result(meta);
    }

    // the v1's post_apply
    // As the logic is mostly for read, rename it to handle_read_after_apply
    pub fn handle_read_on_apply<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        applied_term: u64,
        applied_index: u64,
        progress_to_be_updated: bool,
    ) {
        // TODO: add is_handling_snapshot check
        // it could update has_ready

        // TODO: add peer_stat(for PD hotspot scheduling) and deleted_keys_hint
        if !self.is_leader() {
            self.post_pending_read_index_on_replica(ctx)
        } else if self.ready_to_handle_read() {
            while let Some(mut read) = self.pending_reads_mut().pop_front() {
                self.respond_read_index(&mut read);
            }
        }
        self.pending_reads_mut().gc();
        self.read_progress_mut().update_applied_core(applied_index);

        // Only leaders need to update applied_term.
        if progress_to_be_updated && self.is_leader() {
            if applied_term == self.term() {
                ctx.coprocessor_host
                    .on_applied_current_term(StateRole::Leader, self.region());
            }
            let progress = ReadProgress::applied_term(applied_term);
            let mut meta = ctx.store_meta.lock().unwrap();
            let reader = meta.readers.get_mut(&self.region_id()).unwrap();
            self.maybe_update_read_progress(reader, progress);
        }
    }
}
