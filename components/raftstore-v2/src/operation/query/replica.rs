// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, RaftEngine};
use kvproto::raft_cmdpb::{self, CmdType, RaftCmdRequest, RaftCmdResponse};
use pd_client::INVALID_ID;
use raftstore::{
    store::{
        cmd_resp,
        fsm::apply::notify_stale_req,
        metrics::RAFT_READ_INDEX_PENDING_COUNT,
        msg::{ErrorCallback, ReadCallback},
        propose_read_index,
        util::check_region_epoch,
        ReadIndexRequest, Transport,
    },
    Error,
};
use slog::{debug, error, info, o, Logger};
use tikv_util::{box_err, time::monotonic_raw_now};
use time::Timespec;
use tracker::GLOBAL_TRACKERS;

use crate::{
    batch::StoreContext,
    raft::Peer,
    router::{message::RaftRequest, QueryResChannel},
    Result,
};
impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    /// read index on follower
    ///
    /// return true if it's proposed.
    pub(crate) fn read_index_follower<T: Transport>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        mut req: RaftCmdRequest,
        ch: QueryResChannel,
    ) {
        let now = monotonic_raw_now();
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
        propose_read_index(self.raft_group_mut(), request.as_ref(), None);

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

    /// Responses to the ready read index request on the replica, the replica is
    /// not a leader.
    pub(crate) fn post_pending_read_index_on_replica<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
    ) {
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
                self.respond_read(&mut read, ctx);
            } else if self.ready_to_handle_unsafe_replica_read(read.read_index.unwrap()) {
                self.response_replica_read(&mut read, ctx);
            } else {
                // TODO: `ReadIndex` requests could be blocked.
                self.pending_reads_mut().push_front(read);
                break;
            }
        }
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

    fn response_replica_read<T>(
        &self,
        read_index_req: &mut ReadIndexRequest<QueryResChannel>,
        ctx: &mut StoreContext<EK, ER, T>,
    ) {
        debug!(
            self.logger,
            "handle replica reads with a read index";
            "request_id" => ?read_index_req.id,
        );
        RAFT_READ_INDEX_PENDING_COUNT.sub(read_index_req.cmds().len() as i64);
        let time = monotonic_raw_now();
        for (req, ch, mut read_index) in read_index_req.take_cmds().drain(..) {
            ch.read_tracker().map(|tracker| {
                GLOBAL_TRACKERS.with_tracker(*tracker, |t| {
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
                // We should check epoch since the range could be changed.
                let region = self.region().clone();
                if let Err(e) = check_region_epoch(&req, &region, true) {
                    let mut response = cmd_resp::new_error(e);
                    cmd_resp::bind_term(&mut response, self.term());
                    ch.report_error(response);
                }
            } else {
                // The request could be proposed when the peer was leader.
                // TODO: figure out that it's necessary to notify stale or not.
                let term = self.term();
                notify_stale_req(term, ch);
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
}
