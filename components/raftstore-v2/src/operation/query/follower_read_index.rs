// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, RaftEngine};
use kvproto::raft_cmdpb::{self, CmdType, RaftCmdRequest, RaftCmdResponse};
use pd_client::INVALID_ID;
use raftstore::{
    store::{
        cmd_resp,
        metrics::RAFT_READ_INDEX_PENDING_COUNT,
        msg::{ErrorCallback, ReadCallback},
        peer::propose_read_index,
        Transport,
    },
    Error,
};
use slog::{debug, error, info, o, Logger};
use tikv_util::{box_err, time::monotonic_raw_now};
use time::Timespec;

use crate::{
    batch::StoreContext,
    raft::Peer,
    router::{message::RaftRequest, response_channel::QueryResChannel},
    Result,
};
impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    /// read index on follower
    ///
    /// return true if it's proposed.
    pub(crate) fn follower_read_index<T: Transport>(
        &mut self,
        poll_ctx: &mut StoreContext<EK, ER, T>,
        mut req: RaftCmdRequest,
        mut err_resp: RaftCmdResponse,
        ch: QueryResChannel,
        now: Timespec,
    ) -> bool {
        if self.leader_id() == INVALID_ID {
            poll_ctx
                .raft_metrics
                .invalid_proposal
                .read_index_no_leader
                .inc();
            cmd_resp::bind_error(&mut err_resp, Error::NotLeader(self.region_id(), None));
            ch.report_error(err_resp);
            return false;
        }

        self.propose_read_index(poll_ctx, req, self.is_leader(), ch, now)
    }

    /// Responses to the ready read index request on the replica, the replica is
    /// not a leader.
    pub(crate) fn post_pending_read_index_on_replica<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
    ) {
        while let Some(mut read) = self.pending_reads.pop_front() {
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
                self.response_read(&mut read, ctx);
            } else if self.ready_to_handle_unsafe_replica_read(read.read_index.unwrap()) {
                self.response_replica_read(&mut read, ctx);
            } else {
                // TODO: `ReadIndex` requests could be blocked.
                self.pending_reads.push_front(read);
                break;
            }
        }
    }
}
