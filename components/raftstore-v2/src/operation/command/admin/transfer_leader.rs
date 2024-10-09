// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;

use bytes::Bytes;
use engine_traits::{KvEngine, RaftEngine};
use kvproto::{
    disk_usage::DiskUsage,
    metapb,
    raft_cmdpb::{
        AdminCmdType, AdminRequest, AdminResponse, RaftCmdRequest, TransferLeaderRequest,
    },
};
use raft::{eraftpb, ProgressState, Storage};
use raftstore::{
    store::{
        fsm::new_admin_request, make_transfer_leader_response, metrics::PEER_ADMIN_CMD_COUNTER,
        Transport, TRANSFER_LEADER_COMMAND_REPLY_CTX,
    },
    Result,
};
use rand::prelude::SliceRandom;
use slog::{info, warn};
use txn_types::WriteBatchFlags;

use super::AdminCmdResult;
use crate::{
    batch::StoreContext,
    fsm::ApplyResReporter,
    raft::{Apply, Peer},
    router::{CmdResChannel, PeerMsg},
};

fn transfer_leader_cmd(msg: &RaftCmdRequest) -> Option<&TransferLeaderRequest> {
    if !msg.has_admin_request() {
        return None;
    }
    let req = msg.get_admin_request();
    if !req.has_transfer_leader() {
        return None;
    }

    Some(req.get_transfer_leader())
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    /// Return true if the transfer leader request is accepted.
    ///
    /// When transferring leadership begins, leader sends a pre-transfer
    /// to target follower first to ensures it's ready to become leader.
    /// After that the real transfer leader process begin.
    ///
    /// 1. pre_transfer_leader on leader: Leader will send a MsgTransferLeader
    ///    to follower.
    /// 2. execute_transfer_leader on follower: If follower passes all necessary
    ///    checks, it will reply an ACK with type MsgTransferLeader and its
    ///    promised applied index.
    /// 3. ready_to_transfer_leader on leader: Leader checks if it's appropriate
    ///    to transfer leadership. If it does, it calls raft transfer_leader API
    ///    to do the remaining work.
    ///
    /// Additional steps when there are remaining pessimistic
    /// locks to propose (detected in function on_transfer_leader_msg).
    ///    1. Leader firstly proposes pessimistic locks and then proposes a
    ///       TransferLeader command.
    ///    2. The follower applies the TransferLeader command and replies an ACK
    ///       with special context TRANSFER_LEADER_COMMAND_REPLY_CTX.
    ///
    /// See also: tikv/rfcs#37.
    pub fn propose_transfer_leader<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        req: RaftCmdRequest,
        ch: CmdResChannel,
    ) -> bool {
        // If the peer has not finished the previous admin cmd yet, it should
        // abort the current leader transfer operation.
        if !self.last_admin_cmd_finished {
            warn!(
                self.logger,
                "skip transfer leader, previous admin command is still running";
                "region_id" => self.region_id(),
                "peer_id" => self.peer_id(),
            );
            return false;
        }
        ctx.raft_metrics.propose.transfer_leader.inc();

        let transfer_leader = transfer_leader_cmd(&req).unwrap();
        let prs = self.raft_group().raft.prs();

        // Find the target with the largest matched index among the candidate
        // transferee peers
        let (_, peers) = transfer_leader
            .get_peers()
            .iter()
            .filter(|peer| peer.id != self.peer().id)
            .fold((0, vec![]), |(max_matched, mut chosen), p| {
                if let Some(pr) = prs.get(p.id) {
                    match pr.matched.cmp(&max_matched) {
                        Ordering::Greater => (pr.matched, vec![p]),
                        Ordering::Equal => {
                            chosen.push(p);
                            (max_matched, chosen)
                        }
                        Ordering::Less => (max_matched, chosen),
                    }
                } else {
                    (max_matched, chosen)
                }
            });
        let peer = match peers.len() {
            0 => transfer_leader.get_peer(),
            1 => peers.first().unwrap(),
            _ => peers.choose(&mut rand::thread_rng()).unwrap(),
        };

        let transferee = if peer.id == self.peer_id() {
            false
        } else {
            self.pre_transfer_leader(peer)
        };

        // transfer leader command doesn't need to replicate log and apply, so we
        // return immediately. Note that this command may fail, we can view it just as
        // an advice
        ch.set_result(make_transfer_leader_response());

        self.last_admin_cmd_finished = !transferee;

        transferee
    }

    pub fn pre_transfer_leader(&mut self, peer: &metapb::Peer) -> bool {
        if self.raft_group().raft.has_pending_conf() {
            info!(
                self.logger,
                "reject transfer leader due to pending conf change";
                "peer" => ?peer,
            );
            return false;
        }

        // Broadcast heartbeat to make sure followers commit the entries immediately.
        // It's only necessary to ping the target peer, but ping all for simplicity.
        self.raft_group_mut().ping();

        let mut msg = eraftpb::Message::new();
        msg.set_to(peer.get_id());
        msg.set_msg_type(eraftpb::MessageType::MsgTransferLeader);
        msg.set_from(self.peer_id());
        msg.set_index(self.entry_storage().entry_cache_first_index().unwrap_or(0));
        // log term here represents the term of last log. For leader, the term of last
        // log is always its current term. Not just set term because raft library
        // forbids setting it for MsgTransferLeader messages.
        msg.set_log_term(self.term());
        self.raft_group_mut().raft.msgs.push(msg);
        true
    }

    pub fn on_transfer_leader_msg<T: Transport>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        msg: &eraftpb::Message,
        peer_disk_usage: DiskUsage,
    ) {
        // log_term is set by original leader, represents the term last log is written
        // in, which should be equal to the original leader's term.
        if msg.get_log_term() != self.term() {
            return;
        }

        if !self.is_leader() {
            if self.maybe_reject_transfer_leader_msg(ctx, msg.get_from(), peer_disk_usage)
                || !self.pre_ack_transfer_leader_msg(ctx, msg)
            {
                return;
            }

            self.ack_transfer_leader_msg(false);
            return;
        }

        let from = match self.peer_from_cache(msg.get_from()) {
            Some(p) => p,
            None => return,
        };
        match self.ready_to_transfer_leader(ctx, msg.get_index(), &from) {
            Some(reason) => {
                info!(
                    self.logger,
                    "reject to transfer leader";
                    "to" => ?from,
                    "reason" => reason,
                    "index" => msg.get_index(),
                    "last_index" => self.storage().last_index().unwrap_or_default(),
                );
            }
            None => {
                self.propose_pending_writes(ctx);
                if self.propose_locks_before_transfer_leader(ctx, msg) {
                    // If some pessimistic locks are just proposed, we propose another
                    // TransferLeader command instead of transferring leader immediately.
                    info!(
                        self.logger,
                        "propose transfer leader command";
                        "to" => ?from,
                    );
                    let mut cmd = new_admin_request(self.region().get_id(), self.peer().clone());
                    cmd.mut_header()
                        .set_region_epoch(self.region().get_region_epoch().clone());
                    // Set this flag to propose this command like a normal proposal.
                    cmd.mut_header()
                        .set_flags(WriteBatchFlags::TRANSFER_LEADER_PROPOSAL.bits());
                    cmd.mut_admin_request()
                        .set_cmd_type(AdminCmdType::TransferLeader);
                    cmd.mut_admin_request().mut_transfer_leader().set_peer(from);
                    if let PeerMsg::AdminCommand(req) = PeerMsg::admin_command(cmd).0 {
                        self.on_admin_command(ctx, req.request, req.ch);
                    } else {
                        unreachable!();
                    }
                } else {
                    info!(
                        self.logger,
                        "transfer leader";
                        "peer" => ?from,
                    );
                    self.raft_group_mut().transfer_leader(from.get_id());
                    self.refresh_leader_transferee();
                }
            }
        }
    }

    fn maybe_reject_transfer_leader_msg<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        from: u64,
        peer_disk_usage: DiskUsage,
    ) -> bool {
        let pending_snapshot = self.is_handling_snapshot() || self.has_pending_snapshot();
        if pending_snapshot
            || from != self.leader_id()
            // Transfer leader to node with disk full will lead to write availablity downback.
            // But if the current leader is disk full, and send such request, we should allow it,
            // because it may be a read leader balance request.
            || (!matches!(ctx.self_disk_usage, DiskUsage::Normal) &&
            matches!(peer_disk_usage,DiskUsage::Normal))
        {
            info!(
                self.logger,
                "reject transferring leader";
                "from" => from,
                "pending_snapshot" => pending_snapshot,
                "disk_usage" => ?ctx.self_disk_usage,
            );
            return true;
        }
        false
    }

    /// Before ack the transfer leader message sent by the leader.
    /// Currently, it only warms up the entry cache in this stage.
    ///
    /// This return whether the msg should be acked. When cache is warmed up
    /// or the warmup operation is timeout, it is true.
    fn pre_ack_transfer_leader_msg<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        msg: &eraftpb::Message,
    ) -> bool {
        if !ctx.cfg.warmup_entry_cache_enabled() {
            return true;
        }

        // The start index of warmup range. It is leader's entry_cache_first_index,
        // which in general is equal to the lowest matched index.
        let mut low = msg.get_index();
        let last_index = self.entry_storage().last_index();
        let mut should_ack_now = false;

        // Need not to warm up when the index is 0.
        // There are two cases where index can be 0:
        // 1. During rolling upgrade, old instances may not support warmup.
        // 2. The leader's entry cache is empty.
        if low == 0 || low > last_index {
            // There is little possibility that the warmup_range_start
            // is larger than the last index. Check the test case
            // `test_when_warmup_range_start_is_larger_than_last_index`
            // for details.
            should_ack_now = true;
        } else {
            if low < self.compact_log_context().last_compacted_idx() {
                low = self.compact_log_context().last_compacted_idx()
            };
            // Check if the entry cache is already warmed up.
            if let Some(first_index) = self.entry_storage().entry_cache_first_index() {
                if low >= first_index {
                    fail::fail_point!("entry_cache_already_warmed_up");
                    should_ack_now = true;
                }
            }
        }

        if should_ack_now {
            return true;
        }

        // Check if the warmup operation is timeout if warmup is already started.
        if let Some(state) = self
            .storage_mut()
            .entry_storage_mut()
            .entry_cache_warmup_state_mut()
        {
            // If it is timeout, this peer should ack the message so that
            // the leadership transfer process can continue.
            state.check_task_timeout(ctx.cfg.max_entry_cache_warmup_duration.0)
        } else {
            self.storage_mut()
                .entry_storage_mut()
                .async_warm_up_entry_cache(low)
                .is_none()
        }
    }

    pub fn ack_transfer_leader_msg(
        &mut self,
        reply_cmd: bool, // whether it is a reply to a TransferLeader command
    ) {
        let mut msg = eraftpb::Message::new();
        msg.set_from(self.peer_id());
        msg.set_to(self.leader_id());
        msg.set_msg_type(eraftpb::MessageType::MsgTransferLeader);
        msg.set_index(self.storage().apply_state().applied_index);
        msg.set_log_term(self.term());
        if reply_cmd {
            msg.set_context(Bytes::from_static(TRANSFER_LEADER_COMMAND_REPLY_CTX));
        }
        self.raft_group_mut().raft.msgs.push(msg);
    }

    fn ready_to_transfer_leader<T>(
        &self,
        ctx: &mut StoreContext<EK, ER, T>,
        mut index: u64,
        peer: &metapb::Peer,
    ) -> Option<&'static str> {
        let status = self.raft_group().status();
        let progress = status.progress.unwrap();

        if !progress.conf().voters().contains(peer.id) {
            return Some("non voter");
        }

        for (id, pr) in progress.iter() {
            if pr.state == ProgressState::Snapshot {
                return Some("pending snapshot");
            }
            if *id == peer.id && index == 0 {
                // index will be zero if it's sent from an instance without
                // pre-transfer-leader feature. Set it to matched to make it
                // possible to transfer leader to an older version. It may be
                // useful during rolling restart.
                index = pr.matched;
            }
        }

        if self.raft_group().raft.has_pending_conf()
            || self.raft_group().raft.pending_conf_index > index
        {
            return Some("pending conf change");
        }

        if self.storage().last_index().unwrap_or_default()
            >= index + ctx.cfg.leader_transfer_max_log_lag
        {
            return Some("log gap");
        }
        None
    }
}

impl<EK: KvEngine, R: ApplyResReporter> Apply<EK, R> {
    pub fn apply_transfer_leader(
        &mut self,
        req: &AdminRequest,
        term: u64,
    ) -> Result<(AdminResponse, AdminCmdResult)> {
        PEER_ADMIN_CMD_COUNTER.transfer_leader.all.inc();
        let resp = AdminResponse::default();

        let peer = req.get_transfer_leader().get_peer();
        // Only execute TransferLeader if the expected new leader is self.
        if peer.get_id() == self.peer().get_id() {
            Ok((resp, AdminCmdResult::TransferLeader(term)))
        } else {
            Ok((resp, AdminCmdResult::None))
        }
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub fn on_transfer_leader(&mut self, term: u64) {
        // If the term has changed between proposing and executing the TransferLeader
        // request, ignore it because this request may be stale.
        if term != self.term() {
            return;
        }

        // Reply to leader that it is ready to transfer leader now.
        self.ack_transfer_leader_msg(true);

        self.set_has_ready();
    }
}
