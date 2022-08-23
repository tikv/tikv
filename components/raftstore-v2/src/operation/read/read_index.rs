// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use crossbeam::channel::TrySendError;
use engine_traits::{KvEngine, RaftEngine};
use kvproto::{
    errorpb,
    kvrpcpb::ExtraOp as TxnExtraOp,
    raft_cmdpb::{self, RaftCmdRequest, RaftCmdResponse},
};
use raft::Ready;
use raftstore::{
    errors::RAFTSTORE_IS_BUSY,
    store::{
        cmd_resp,
        fsm::{apply::notify_stale_req, Proposal},
        metrics::RAFT_READ_INDEX_PENDING_COUNT,
        msg::{ErrorCallback, ReadCallback},
        peer::{propose_read_index, RaftPeer, RequestInspector},
        read_queue::{ReadIndexContext, ReadIndexRequest},
        Transport,
    },
    Error,
};
use slog::{debug, error, info, o, Logger};
use tikv_util::{box_err, time::monotonic_raw_now, Either};
use time::Timespec;

use crate::{
    batch::StoreContext,
    raft::Peer,
    router::{
        message::RaftQuery,
        response_channel::{CmdResChannel, QueryResChannel, QueryResult, ReadResponse},
    },
    Result,
};

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub(crate) fn propose_read_index<T: Transport>(
        &mut self,
        poll_ctx: &mut StoreContext<EK, ER, T>,
        mut req: RaftCmdRequest,
        is_leader: bool,
        cb: QueryResChannel,
        now: Timespec,
    ) -> bool {
        poll_ctx.raft_metrics.propose.read_index.inc();
        self.bcast_wake_up_time = None;

        let request = req
            .mut_requests()
            .get_mut(0)
            .filter(|req| req.has_read_index())
            .map(|req| req.take_read_index());
        let (id, dropped) = propose_read_index(&mut self.raft_group, request.as_ref(), None);
        if dropped && is_leader {
            // The message gets dropped silently, can't be handled anymore.
            notify_stale_req(self.term(), cb);
            poll_ctx.raft_metrics.propose.dropped_read_index.inc();
            return false;
        }

        let mut read = ReadIndexRequest::with_command(id, req, cb, now);
        read.addition_request = request.map(Box::new);
        self.push_pending_read(read, is_leader);
        self.should_wake_up = true;
        debug!(
            self.logger,
            "request to get a read index";
            "request_id" => ?id,
            "is_leader" => is_leader,
        );

        true
    }

    fn leader_read_index<T: Transport>(
        &mut self,
        poll_ctx: &mut StoreContext<EK, ER, T>,
        mut req: RaftCmdRequest,
        mut err_resp: RaftCmdResponse,
        cb: QueryResChannel,
        now: Timespec,
    ) -> bool {
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

        if !self.propose_read_index(poll_ctx, req, self.is_leader(), cb, now) {
            return false;
        }

        // TimeoutNow has been sent out, so we need to propose explicitly to
        // update leader lease.
        if self.leader_lease.is_suspect() {
            let req = RaftCmdRequest::default();
            if let Ok(Either::Left(index)) = self.propose_normal(poll_ctx, req) {
                let (callback, _) = CmdResChannel::pair();
                let p = Proposal {
                    is_conf_change: false,
                    index,
                    term: self.term(),
                    cb: callback,
                    propose_time: Some(now),
                    must_pass_epoch_check: false,
                };
                self.post_propose(poll_ctx, p);
            }
        }
        true
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
        cb: QueryResChannel,
    ) -> bool {
        if let Err(e) = self.pre_read_index() {
            debug!(
                self.logger,
                "prevents unsafe read index";
                "err" => ?e,
            );
            poll_ctx.raft_metrics.propose.unsafe_read_index.inc();
            cmd_resp::bind_error(&mut err_resp, e);
            cb.report_error(err_resp);
            self.should_wake_up = true;
            return false;
        }
        let now = monotonic_raw_now();
        if self.is_leader() {
            self.leader_read_index(poll_ctx, req, err_resp, cb, now)
        } else {
            self.follower_read_index(poll_ctx, req, err_resp, cb, now)
        }
    }

    pub(crate) fn send_read_command<T>(
        &self,
        ctx: &mut StoreContext<EK, ER, T>,
        read_cmd: RaftQuery,
    ) {
        let mut err = errorpb::Error::default();
        let read_ch = match ctx.router.send_read_command(read_cmd) {
            Ok(()) => return,
            Err(TrySendError::Full(cmd)) => {
                err.set_message(RAFTSTORE_IS_BUSY.to_owned());
                err.mut_server_is_busy()
                    .set_reason(RAFTSTORE_IS_BUSY.to_owned());
                cmd.ch
            }
            Err(TrySendError::Disconnected(cmd)) => {
                err.set_message(format!("region {} is missing", self.region_id()));
                err.mut_region_not_found().set_region_id(self.region_id());
                cmd.ch
            }
        };
        let mut resp = RaftCmdResponse::default();
        resp.mut_header().set_error(err);
        read_ch.report_error(resp);
    }

    /// response the read index request
    ///
    /// awake the read tasks waiting in frontend (such as unified thread pool)
    pub(crate) fn response_read<T>(
        &self,
        read_index_req: &mut ReadIndexRequest<QueryResChannel>,
        ctx: &mut StoreContext<EK, ER, T>,
    ) {
        debug!(
            self.logger,
            "handle reads with a read index";
            "request_id" => ?read_index_req.id,
        );
        RAFT_READ_INDEX_PENDING_COUNT.sub(read_index_req.cmds().len() as i64);
        for (req, ch, mut read_index) in read_index_req.take_cmds().drain(..) {
            // leader reports key is locked
            if let Some(locked) = read_index_req.locked.take() {
                let mut response = raft_cmdpb::Response::default();
                response.mut_read_index().set_locked(*locked);
                let mut cmd_resp = RaftCmdResponse::default();
                cmd_resp.mut_responses().push(response);
                let read_resp = ReadResponse {
                    response: cmd_resp,
                    txn_extra_op: TxnExtraOp::Noop,
                };
                ch.set_result(QueryResult::Read(read_resp));
            } else {
                match (read_index, read_index_req.read_index) {
                    (Some(local_responsed_index), Some(batch_index)) => {
                        // `read_index` could be less than `read_index_req.read_index` because the
                        // former is filled with `committed index` when
                        // proposed, and the latter is filled
                        // after a read-index procedure finished.
                        read_index = Some(std::cmp::max(local_responsed_index, batch_index));
                    }
                    (None, _) => {
                        // Actually, the read_index is none if and only if it's the first one in
                        // read_index_req.cmds. Starting from the second, all the following ones'
                        // read_index is not none.
                        read_index = read_index_req.read_index;
                    }
                    _ => {}
                }
                let read_resp = ReadResponse::new(read_index.unwrap_or(0));
                ch.set_result(QueryResult::Read(read_resp));
            }
        }
    }

    fn apply_reads<T>(&mut self, ctx: &mut StoreContext<EK, ER, T>, ready: &Ready) {
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
            if let Some(propose_time) = self.pending_reads.last_ready().map(|r| r.propose_time) {
                if !self.leader_lease.is_suspect() {
                    self.maybe_renew_leader_lease(propose_time, &mut ctx.store_meta, None);
                }
            }
            while let Some(mut read) = self.pending_reads.pop_front() {
                self.response_read(&mut read, ctx);
            }
        }

        // Note that only after handle read_states can we identify what requests are
        // actually stale.
        if ready.ss().is_some() {
            let term = self.term();
            // all uncommitted reads will be dropped silently in raft.
            self.pending_reads.clear_uncommitted_on_role_change(term);
        }
    }
}
