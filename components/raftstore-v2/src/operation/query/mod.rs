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
//! Stale read check is implemented stale module.

use engine_traits::{KvEngine, RaftEngine};
use kvproto::raft_cmdpb::{CmdType, RaftCmdRequest, RaftCmdResponse, StatusCmdType};
use raftstore::{
    store::{
        cmd_resp, msg::ErrorCallback, region_meta::RegionMeta, util, util::LeaseState, GroupState,
        ReadCallback, RequestPolicy, Transport,
    },
    Error, Result,
};
use tikv_util::box_err;
use txn_types::WriteBatchFlags;

use crate::{
    batch::StoreContext,
    fsm::PeerFsmDelegate,
    raft::Peer,
    router::{DebugInfoChannel, QueryResChannel, QueryResult, ReadResponse},
};

mod lease;
mod local;
mod replica;
mod stale;

impl<'a, EK: KvEngine, ER: RaftEngine, T: raftstore::store::Transport>
    PeerFsmDelegate<'a, EK, ER, T>
{
    fn inspect_read(&mut self, req: &RaftCmdRequest) -> Result<RequestPolicy> {
        let flags = WriteBatchFlags::from_bits_check(req.get_header().get_flags());
        if flags.contains(WriteBatchFlags::STALE_READ) {
            return Ok(RequestPolicy::StaleRead);
        }

        if req.get_header().get_read_quorum() {
            return Ok(RequestPolicy::ReadIndex);
        }

        // If applied index's term is differ from current raft's term, leader transfer
        // must happened, if read locally, we may read old value.
        if !self.fsm.peer_mut().has_applied_to_current_term() {
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
            if let Err(e) = self.fsm.peer_mut().validate_query_msg(&req, self.store_ctx) {
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
                Ok(RequestPolicy::StaleRead) => {
                    self.store_ctx.raft_metrics.propose.local_read.inc();
                    self.fsm.peer_mut().can_stale_read(req, true, None, ch);
                }
                _ => {
                    unimplemented!();
                }
            };
        } else {
            self.fsm.peer_mut().on_query_status(&req, ch);
        }
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    fn validate_query_msg<T: Transport>(
        &mut self,
        msg: &RaftCmdRequest,
        ctx: &mut StoreContext<EK, ER, T>,
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
        if let Err(e) = util::check_store_id(msg, self.peer().get_store_id()) {
            ctx.raft_metrics.invalid_proposal.mismatch_store_id.inc();
            return Err(e);
        }

        // TODO: add flashback_state check

        // Check whether the store has the right peer to handle the request.
        let leader_id = self.leader_id();
        let request = msg.get_requests();

        // TODO: add false leader

        // ReadIndex can be processed on the replicas.
        let is_read_index_request =
            request.len() == 1 && request[0].get_cmd_type() == CmdType::ReadIndex;

        let allow_replica_read = msg.get_header().get_replica_read();
        let flags = WriteBatchFlags::from_bits_check(msg.get_header().get_flags());
        let allow_stale_read = flags.contains(WriteBatchFlags::STALE_READ);
        if !self.is_leader() && !is_read_index_request && !allow_replica_read && !allow_stale_read {
            ctx.raft_metrics.invalid_proposal.not_leader.inc();
            return Err(Error::NotLeader(self.region_id(), None));
        }

        // peer_id must be the same as peer's.
        if let Err(e) = util::check_peer_id(msg, self.peer_id()) {
            ctx.raft_metrics.invalid_proposal.mismatch_peer_id.inc();
            return Err(e);
        }
        // check whether the peer is initialized.
        if !self.storage().is_initialized() {
            ctx.raft_metrics
                .invalid_proposal
                .region_not_initialized
                .inc();
            return Err(Error::RegionNotInitialized(self.region_id()));
        }

        // TODO: check applying snapshot

        // Check whether the term is stale.
        if let Err(e) = util::check_term(msg, self.term()) {
            ctx.raft_metrics.invalid_proposal.stale_command.inc();
            return Err(e);
        }

        // TODO: add check of sibling region for split
        util::check_region_epoch(msg, self.region(), true)
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
        util::check_store_id(req, self.peer().get_store_id())?;
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
        let meta = RegionMeta::new(
            self.storage().region_state(),
            entry_storage.apply_state(),
            GroupState::Ordered,
            self.raft_group().status(),
        );
        ch.set_result(meta);
    }
}
