// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! There are two types of Query: KV read and status query.
//!
//! KV Read is implemented in local module and lease module (not implemented
//! yet). Read will be executed in callee thread if in lease, which is
//! implemented in local module. If lease is expired, it will extend the lease
//! first. Lease maintainance is implemented in lease module.
//!
//! Status query is implemented in the root module directly.

use engine_traits::{KvEngine, RaftEngine};
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse, StatusCmdType};
use raftstore::{
    store::{cmd_resp, region_meta::RegionMeta, util, GroupState, ReadCallback},
    Error, Result,
};
use tikv_util::box_err;

use crate::{
    fsm::PeerFsmDelegate,
    raft::Peer,
    router::{DebugInfoChannel, QueryResChannel, QueryResult},
};

mod local;

impl<'a, EK: KvEngine, ER: RaftEngine, T> PeerFsmDelegate<'a, EK, ER, T> {
    #[inline]
    pub fn on_query(&mut self, req: RaftCmdRequest, ch: QueryResChannel) {
        if !req.has_status_request() {
            unimplemented!();
        } else {
            self.fsm.peer_mut().on_query_status(&req, ch);
        }
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
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
