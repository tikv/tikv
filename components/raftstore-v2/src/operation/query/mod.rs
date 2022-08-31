// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! There are two types of Query: KV read and status query.
//!
//! KV Read is implemented in local module and lease module (not implemented
//! yet). Read will be executed in callee thread if in lease, which is
//! implemented in local module. If lease is expired, it will extend the lease
//! first. Lease maintainance is implemented in lease module.
//!
//! Status query is implemented in the root module directly.
//! Follower's read index is implemenented follower_read_index
//! Leader's read index is implemented in read_index
//! stale read check is implemented replica_read.rs

use engine_traits::{KvEngine, RaftEngine};
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse, StatusCmdType};
use raftstore::{
    store::{
        cmd_resp,
        peer::{RequestInspector, RequestPolicy},
        util,
        util::LeaseState,
        ReadCallback,
    },
    Error, Result,
};
use tikv_util::box_err;
use txn_types::WriteBatchFlags;

use crate::{
    fsm::PeerFsmDelegate,
    raft::Peer,
    router::{QueryResChannel, QueryResult, ReadResponse},
};

mod follower_read_index;
mod local;
mod read_index;
mod replica_read;

impl<'a, EK: KvEngine, ER: RaftEngine, T: raftstore::store::Transport>
    PeerFsmDelegate<'a, EK, ER, T>
{
    fn inspect_read(&mut self, req: &RaftCmdRequest) -> Result<RequestPolicy> {
        let flags = WriteBatchFlags::from_bits_check(req.get_header().get_flags());
        if flags.contains(WriteBatchFlags::STALE_READ) {
            println!("inspect_read stale read");
            return Ok(RequestPolicy::StaleRead);
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
            let mut resp = RaftCmdResponse::default();
            let term = self.fsm.peer().term();
            cmd_resp::bind_term(&mut resp, term);
            let policy = self.inspect_read(&req);
            match policy {
                Ok(RequestPolicy::ReadIndex) => {
                    self.fsm
                        .peer_mut()
                        .read_index(self.store_ctx, req, resp, ch);
                }
                Ok(RequestPolicy::ReadLocal) => {  
                    let read_resp = ReadResponse::new(0);
                    ch.set_result(QueryResult::Read(read_resp));
                }
                Ok(RequestPolicy::StaleRead) => {
                    println!("ReadStale is called");
                    ch.set_result(self.fsm.peer_mut().can_replica_read(req, true, None));
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
}
