// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! There are two types of read:
//! - If the ReadDelegate is in the leader lease status, the read is operated
//!   locally and need not to go through the raft layer (namely local read).
//! - Otherwise, redirect the request to the raftstore and proposed as a
//!   RaftCommand in the raft layer.

use engine_traits::{KvEngine, RaftEngine};
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse, StatusCmdType};
use raftstore::{
    store::{cmd_resp, ReadCallback},
    Error,
};
use tikv_util::box_err;

use crate::{
    fsm::PeerFsmDelegate,
    raft::Peer,
    router::{QueryResChannel, QueryResult},
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
    pub fn on_query_status(&mut self, req: &RaftCmdRequest, ch: QueryResChannel) {
        let mut response = RaftCmdResponse::default();
        let cmd_type = req.get_status_request().get_cmd_type();
        let mut status_resp = response.mut_status_response();
        status_resp.set_cmd_type(cmd_type);
        match cmd_type {
            StatusCmdType::RegionLeader => {
                if let Some(leader) = self.leader() {
                    status_resp.mut_region_leader().set_leader(leader);
                }
            }
            StatusCmdType::RegionDetail => self.fill_region_details(req, &mut response),
            StatusCmdType::InvalidStatus => {
                cmd_resp::bind_error(
                    &mut response,
                    box_err!("{:?} invalid status command!", self.logger.list()),
                );
            }
        }

        // Bind peer current term here.
        cmd_resp::bind_term(&mut response, self.term());
        ch.set_result(QueryResult::Response(response));
    }

    fn fill_region_details(&mut self, req: &RaftCmdRequest, resp: &mut RaftCmdResponse) {
        if !self.storage().is_initialized() {
            let region_id = req.get_header().get_region_id();
            cmd_resp::bind_error(resp, Error::RegionNotInitialized(region_id));
            return;
        }
        let status_resp = resp.mut_status_response();
        status_resp
            .mut_region_detail()
            .set_region(self.region().clone());
        if let Some(leader) = self.leader() {
            status_resp.mut_region_leader().set_leader(leader);
        }
    }
}
