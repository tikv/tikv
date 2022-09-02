// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use std::sync::{Arc, Mutex};

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
        can_amend_read, cmd_resp,
        fsm::{apply::notify_stale_req, Proposal},
        metrics::RAFT_READ_INDEX_PENDING_COUNT,
        msg::{ErrorCallback, ReadCallback},
        propose_read_index, should_renew_lease,
        util::{check_region_epoch, LeaseState},
        ReadDelegate, ReadIndexContext, ReadIndexRequest, ReadProgress, RequestInspector,
        Transport,
    },
    Error,
};
use slog::{debug, error, info, o, Logger};
use tikv_util::{box_err, time::monotonic_raw_now, Either};
use time::Timespec;

use crate::{
    batch::StoreContext,
    fsm::StoreMeta,
    raft::Peer,
    router::{
        message::RaftRequest, CmdResChannel, PeerMsg, QueryResChannel, QueryResult, ReadResponse,
    },
    Result,
};

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub(crate) fn propose_read_index<T: Transport>(
        &mut self,
        poll_ctx: &mut StoreContext<EK, ER, T>,
        mut req: RaftCmdRequest,
        is_leader: bool,
        ch: QueryResChannel,
        now: Timespec,
    ) -> bool {
        poll_ctx.raft_metrics.propose.read_index.inc();

        let request = req
            .mut_requests()
            .get_mut(0)
            .filter(|req| req.has_read_index())
            .map(|req| req.take_read_index());
        let (id, dropped) = propose_read_index(self.raft_group_mut(), request.as_ref(), None);
        if dropped && is_leader {
            // The message gets dropped silently, can't be handled anymore.
            notify_stale_req(self.term(), ch);
            poll_ctx.raft_metrics.propose.dropped_read_index.inc();
            return false;
        }

        let mut read = ReadIndexRequest::with_command(id, req, ch, now);
        read.addition_request = request.map(Box::new);
        self.pending_reads_mut().push_back(read, is_leader);
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
        ch: QueryResChannel,
        now: Timespec,
    ) -> bool {
        let lease_state = self.inspect_lease();
        if can_amend_read::<Peer<EK, ER>, QueryResChannel>(
            self.pending_reads().back(),
            &req,
            lease_state,
            poll_ctx.cfg.raft_store_max_leader_lease(),
            now,
        ) {
            // Must use the commit index of `PeerStorage` instead of the commit index
            // in raft-rs which may be greater than the former one.
            // For more details, see the annotations above `on_leader_commit_idx_changed`.
            let commit_index = self.storage().entry_storage().commit_index();
            if let Some(read) = self.pending_reads_mut().back_mut() {
                // A read request proposed in the current lease is found; combine the new
                // read request to that previous one, so that no proposing needed.
                read.push_command(req, ch, commit_index);
                return false;
            }
        }

        if !self.propose_read_index(poll_ctx, req, self.is_leader(), ch, now) {
            return false;
        }

        // TimeoutNow has been sent out, so we need to propose explicitly to
        // update leader lease.
        // TODO:
        // if self.leader_lease.is_suspect() {
        // let req = RaftCmdRequest::default();
        // if let Ok(Either::Left(index)) = self.propose_normal(poll_ctx, req) {
        // let (callback, _) = CmdResChannel::pair();
        // let p = Proposal {
        // is_conf_change: false,
        // index,
        // term: self.term(),
        // cb: callback,
        // propose_time: Some(now),
        // must_pass_epoch_check: false,
        // };
        //
        // self.post_propose(poll_ctx, p);
        // }
        // }
        true
    }

    // Returns a boolean to indicate whether the `read` is proposed or not.
    // For these cases it won't be proposed:
    // 1. The region is in merging or splitting;
    // 2. The message is stale and dropped by the Raft group internally;
    // 3. There is already a read request proposed in the current lease;
    pub fn read_index<T: Transport>(
        &mut self,
        poll_ctx: &mut StoreContext<EK, ER, T>,
        mut req: RaftCmdRequest,
        mut err_resp: RaftCmdResponse,
        ch: QueryResChannel,
    ) -> bool {
        let now = monotonic_raw_now();
        if self.is_leader() {
            self.leader_read_index(poll_ctx, req, err_resp, ch, now)
        } else {
            self.follower_read_index(poll_ctx, req, err_resp, ch, now)
        }
    }

    pub(crate) fn send_read_command<T>(
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
                ch.set_result(QueryResult::Response(cmd_resp));
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
                let region = self.region().clone();
                let read_resp = if let Err(e) = check_region_epoch(&req, &region, true) {
                    debug!(self.logger, "epoch not match"; "err" => ?e);
                    let mut response = cmd_resp::new_error(e);
                    cmd_resp::bind_term(&mut response, self.term());
                    QueryResult::Response(response)
                } else {
                    let read_resp = ReadResponse::new(read_index.unwrap_or(0));
                    QueryResult::Read(read_resp)
                };

                ch.set_result(read_resp);
            }
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
                    self.maybe_renew_leader_lease(propose_time, &mut ctx.store_meta, None);
                }
            }
            while let Some(mut read) = self.pending_reads_mut().pop_front() {
                self.response_read(&mut read, ctx);
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

    /// Try to renew leader lease.
    fn maybe_renew_leader_lease(
        &mut self,
        ts: Timespec,
        store_meta: &mut Arc<Mutex<StoreMeta<EK>>>,
        progress: Option<ReadProgress>,
    ) {
        // A nonleader peer should never has leader lease.
        let read_progress = if !should_renew_lease(
            self.is_leader(),
            self.is_splitting(),
            self.is_merging(),
            self.has_force_leader(),
        ) {
            None
        } else {
            self.leader_lease_mut().renew(ts);
            let term = self.term();
            self.leader_lease_mut()
                .maybe_new_remote_lease(term)
                .map(ReadProgress::leader_lease)
        };
        if let Some(progress) = progress {
            let mut meta = store_meta.lock().unwrap();
            let reader = meta.readers.get_mut(&self.region_id()).unwrap();
            self.maybe_update_read_progress(reader, progress);
        }
        if let Some(progress) = read_progress {
            let mut meta = store_meta.lock().unwrap();
            let reader = meta.readers.get_mut(&self.region_id()).unwrap();
            self.maybe_update_read_progress(reader, progress);
        }
    }

    fn maybe_update_read_progress(&self, reader: &mut ReadDelegate, progress: ReadProgress) {
        debug!(
            self.logger,
            "update read progress";
            "progress" => ?progress,
        );
        reader.update(progress);
    }
}

impl<EK, ER> RequestInspector for Peer<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn has_applied_to_current_term(&mut self) -> bool {
        self.entry_storage().applied_term() == self.term()
    }

    fn inspect_lease(&mut self) -> LeaseState {
        if !self.raft_group().raft.in_lease() {
            return LeaseState::Suspect;
        }
        // None means now.
        let state = self.leader_lease_mut().inspect(None);
        if LeaseState::Expired == state {
            debug!(
                self.logger,
                "leader lease is expired, region_id {}, peer_id {}, lease {:?}",
                self.region_id(),
                self.peer_id(),
                self.leader_lease(),
            );
            // The lease is expired, call `expire` explicitly.
            self.leader_lease_mut().expire();
        }
        state
    }
}
