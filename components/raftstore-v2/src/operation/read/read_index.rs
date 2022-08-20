// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::VecDeque, mem, sync::Arc, time::Duration};

use crossbeam::atomic::AtomicCell;
use engine_traits::{KvEngine, OpenOptions, RaftEngine, TabletFactory};
use kvproto::{
    kvrpcpb::{ExtraOp as TxnExtraOp, LockInfo},
    metapb,
    raft_cmdpb::{self, CmdType, RaftCmdRequest, RaftCmdResponse},
    raft_serverpb::RegionLocalState,
};
use pd_client::BucketMeta;
use raft::{RawNode, Ready, StateRole, INVALID_ID};
use raftstore::{
    store::{
        cmd_resp,
        fsm::{apply::notify_stale_req, Proposal},
        metrics::RAFT_READ_INDEX_PENDING_COUNT,
        msg::ReadCallback,
        peer::{propose_read_index, ForceLeaderState, ProposalQueue, RaftPeer, RequestInspector},
        read_queue::{ReadIndexContext, ReadIndexQueue, ReadIndexRequest},
        util::{check_region_epoch, find_peer, Lease, LeaseState, RegionReadProgress},
        worker::{LocalReadContext, RaftlogFetchTask, ReadExecutor},
        Callback, Config, EntryStorage, PdTask, RaftlogFetchTask, Transport, TxnExt, WriteRouter,
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
    operation::AsyncWriter,
    router::{
        message::{RaftCommand, RaftQuery},
        response_channel::{QueryResChannel, QueryResult, ReadResponse},
    },
    tablet::{self, CachedTablet},
    Result,
};

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
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
            poll_ctx.raft_metrics.propose.unsafe_read_index += 1;
            cmd_resp::bind_error(&mut err_resp, e);
            cb.report_error(err_resp);
            self.should_wake_up = true;
            return false;
        }

        let now = monotonic_raw_now();
        if self.is_leader() {
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
        }

        if self.read_index_no_leader(
            &mut poll_ctx.trans,
            &mut poll_ctx.pd_scheduler,
            &mut err_resp,
        ) {
            poll_ctx.raft_metrics.invalid_proposal.read_index_no_leader += 1;
            cb.report_error(err_resp);
            return false;
        }

        poll_ctx.raft_metrics.propose.read_index += 1;
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
            poll_ctx.raft_metrics.propose.dropped_read_index += 1;
            return false;
        }

        let mut read = ReadIndexRequest::with_command(id, req, cb, now);
        read.addition_request = request.map(Box::new);
        self.push_pending_read(read, self.is_leader());
        self.should_wake_up = true;

        debug!(
            self.logger,
            "request to get a read index";
            "request_id" => ?id,
            "is_leader" => self.is_leader(),
        );

        // TimeoutNow has been sent out, so we need to propose explicitly to
        // update leader lease.
        if self.leader_lease.is_suspect() {
            let req = RaftCmdRequest::default();
            if let Ok(Either::Left(index)) = self.propose_normal(poll_ctx, req) {
                let p = Proposal {
                    is_conf_change: false,
                    index,
                    term: self.term(),
                    cb: Callback::None,
                    propose_time: Some(now),
                    must_pass_epoch_check: false,
                };
                self.post_propose(&mut poll_ctx.current_time, p);
            }
        }

        true
    }

    /// Responses to the ready read index request on the replica, the replica is
    /// not a leader.
    fn post_pending_read_index_on_replica<T>(&mut self, ctx: &mut StoreContext<EK, ER, T>) {
        while let Some(mut read) = self.pending_reads.pop_front() {
            // The response of this read index request is lost, but we need it for
            // the memory lock checking result. Resend the request.
            if let Some(read_index) = read.addition_request.take() {
                assert_eq!(read.cmds().len(), 1);
                let (mut req, ch, _) = read.take_cmds().pop().unwrap();
                assert_eq!(req.requests.len(), 1);
                req.requests[0].set_read_index(*read_index);
                let read_cmd = RaftQuery::new(req, ch);
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
                self.response_read(&mut read, ctx, false);
            } else if self.ready_to_handle_unsafe_replica_read(read.read_index.unwrap()) {
                self.response_read(&mut read, ctx, true);
            } else {
                // TODO: `ReadIndex` requests could be blocked.
                self.mut_pending_reads().push_front(read);
                break;
            }
        }
    }

    fn send_read_command<T>(&self, ctx: &mut StoreContext<EK, ER, T>, read_cmd: RaftQuery) {
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
                err.set_message(format!("region {} is missing", self.region_id));
                err.mut_region_not_found().set_region_id(self.region_id);
                cmd.ch
            }
        };
        let mut resp = RaftCmdResponse::default();
        resp.mut_header().set_error(err);
        read_ch.report_error(resp);
    }

    fn response_read<T>(
        &self,
        read_index_req: &mut ReadIndexRequest<QueryResChannel>,
        ctx: &mut StoreContext<EK, ER, T>,
        replica_read: bool,
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
                continue;
            }
            if !replica_read {
                match (read_index, read_index_req.read_index) {
                    (Some(local_responsed_index), Some(batch_index)) => {
                        // `read_index` could be less than `read_index_req.read_index` because the
                        // former is filled with `committed index` when
                        // proposed, and the latter is filled
                        // after a read-index procedure finished.
                        read_index = Some(cmp::max(local_responsed_index, batch_index));
                    }
                    (None, _) => {
                        // Actually, the read_index is none if and only if it's the first one in
                        // read_index_req.cmds. Starting from the second, all the following ones'
                        // read_index is not none.
                        read_index = read_index_req.read_index;
                    }
                    _ => {}
                }
                let read_resp = ReadResponse::new(read_index);
                ch.set_result(QueryResult::Read(read_resp));
                continue;
            }
            if req.get_header().get_replica_read() {
                // We should check epoch since the range could be changed.
                ch.set_result(self.can_replica_read(reader, req, true, read_index_req.read_index));
            } else {
                // The request could be proposed when the peer was leader.
                // TODO: figure out that it's necessary to notify stale or not.
                let term = self.term();
                apply::notify_stale_req(term, ch);
            }
        }
    }

    fn can_replica_read<E: ReadExecutor<EK>>(
        &self,
        reader: &mut E,
        req: RaftCmdRequest,
        check_epoch: bool,
        read_index: Option<u64>,
    ) -> QueryResult {
        let region = self.region().clone();
        if check_epoch {
            if let Err(e) = check_region_epoch(&req, &region, true) {
                debug!("epoch not match"; "region_id" => region.get_id(), "err" => ?e);
                let mut response = cmd_resp::new_error(e);
                cmd_resp::bind_term(&mut response, self.term());
                return QueryResult::Response(err);
            }
        }
        let flags = WriteBatchFlags::from_bits_check(req.get_header().get_flags());
        if flags.contains(WriteBatchFlags::STALE_READ) {
            let read_ts = decode_u64(&mut req.get_header().get_flag_data()).unwrap();
            let safe_ts = self.read_progress().safe_ts();
            if safe_ts < read_ts {
                warn!(
                    "read rejected by safe timestamp";
                    "safe ts" => safe_ts,
                    "read ts" => read_ts,
                    "tag" => self.tag(),
                );
                let mut response = cmd_resp::new_error(Error::DataIsNotReady {
                    region_id: region.get_id(),
                    peer_id: self.peer_id(),
                    safe_ts,
                });
                cmd_resp::bind_term(&mut response, self.term());
                return QueryResult::Response(err);
            }
        }

        QueryResult::Read(ReadResponse::new(read_index))
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
            if self.ready_to_handle_read() {
                while let Some(mut read) = self.pending_reads.pop_front() {
                    self.response_read(&mut read, ctx, false);
                }
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
