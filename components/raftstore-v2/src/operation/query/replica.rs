// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, RaftEngine};
use kvproto::raft_cmdpb::{self, RaftCmdRequest, RaftCmdResponse};
use pd_client::INVALID_ID;
use raftstore::{
    store::{
        cmd_resp,
        fsm::apply::notify_stale_req,
        metrics::RAFT_READ_INDEX_PENDING_COUNT,
        msg::{ErrorCallback, ReadCallback},
        propose_read_index, ReadIndexRequest, Transport,
    },
    Error,
};
use slog::debug;
use tikv_util::time::monotonic_raw_now;
use tracker::GLOBAL_TRACKERS;

use crate::{
    batch::StoreContext,
    raft::Peer,
    router::{QueryResChannel, QueryResult, ReadResponse},
};
impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    /// read index on follower
    ///
    /// call set_has_ready if it's proposed.
    pub(crate) fn read_index_follower<T: Transport>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        mut req: RaftCmdRequest,
        ch: QueryResChannel,
    ) {
        if self.leader_id() == INVALID_ID {
            ctx.raft_metrics.invalid_proposal.read_index_no_leader.inc();
            let mut err_resp = RaftCmdResponse::default();
            let term = self.term();
            cmd_resp::bind_term(&mut err_resp, term);
            cmd_resp::bind_error(&mut err_resp, Error::NotLeader(self.region_id(), None));
            ch.report_error(err_resp);
            return;
        }

        ctx.raft_metrics.propose.read_index.inc();

        let request = req
            .mut_requests()
            .get_mut(0)
            .filter(|req| req.has_read_index())
            .map(|req| req.take_read_index());
        let (id, _dropped) = propose_read_index(self.raft_group_mut(), request.as_ref(), None);
        let now = monotonic_raw_now();
        let mut read = ReadIndexRequest::with_command(id, req, ch, now);
        read.addition_request = request.map(Box::new);
        self.pending_reads_mut().push_back(read, false);
        debug!(
            self.logger,
            "request to get a read index from follower";
            "request_id" => ?id,
        );
        self.set_has_ready();
    }

    pub(crate) fn respond_replica_read(
        &self,
        read_index_req: &mut ReadIndexRequest<QueryResChannel>,
    ) {
        debug!(
            self.logger,
            "handle replica reads with a read index";
            "request_id" => ?read_index_req.id,
        );
        RAFT_READ_INDEX_PENDING_COUNT.sub(read_index_req.cmds().len() as i64);
        let time = monotonic_raw_now();
        for (req, ch, _) in read_index_req.take_cmds().drain(..) {
            ch.read_tracker().map(|tracker| {
                GLOBAL_TRACKERS.with_tracker(tracker, |t| {
                    t.metrics.read_index_confirm_wait_nanos = (time - read_index_req.propose_time)
                        .to_std()
                        .unwrap()
                        .as_nanos()
                        as u64;
                })
            });

            // leader reports key is locked
            if let Some(locked) = read_index_req.locked.take() {
                let mut response = raft_cmdpb::Response::default();
                response.mut_read_index().set_locked(*locked);
                let mut cmd_resp = RaftCmdResponse::default();
                cmd_resp.mut_responses().push(response);
                ch.report_error(cmd_resp);
                continue;
            }
            if req.get_header().get_replica_read() {
                let read_resp = ReadResponse::new(read_index_req.read_index.unwrap_or(0));
                ch.set_result(QueryResult::Read(read_resp));
            } else {
                // The request could be proposed when the peer was leader.
                // TODO: figure out that it's necessary to notify stale or not.
                let term = self.term();
                notify_stale_req(term, ch);
            }
        }
    }
}
