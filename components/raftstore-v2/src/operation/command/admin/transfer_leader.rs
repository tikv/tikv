// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cmp::Ordering, time::Instant};

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
        entry_storage::CacheWarmupState, fsm::new_admin_request, make_transfer_leader_response,
        metrics::PEER_ADMIN_CMD_COUNTER, Config, TransferLeaderContext, Transport,
    },
    Result,
};
use rand::prelude::SliceRandom;
use slog::info;
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
    ///       with special context TransferLeaderContext::CommandReply.
    ///
    /// See also: tikv/rfcs#37.
    pub fn propose_transfer_leader<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        req: RaftCmdRequest,
        ch: CmdResChannel,
    ) -> bool {
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
            if !self.maybe_reject_transfer_leader_msg(ctx, msg.get_from(), peer_disk_usage) {
                self.set_pending_transfer_leader_msg(&ctx.cfg, msg);
                if self.maybe_ack_transfer_leader_msg(ctx) {
                    self.set_has_ready();
                }
            }
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

    /// Check if the transferee is eligible to receive the leadership. It
    /// rejects the transfer leader request if any of the following
    /// conditions is met:
    /// * The peer is applying a snapshot
    /// * The peer is a learner/witness peer.
    /// * The message is sent by a different leader.
    /// * Its disk is almost full.
    ///
    /// Called by transferee.
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

    pub fn maybe_ack_transfer_leader_msg<T>(&mut self, ctx: &mut StoreContext<EK, ER, T>) -> bool {
        if self.is_leader() {
            self.transfer_leader_state_mut().transfer_leader_msg = None;
            return false;
        }
        let Some((msg, deadline)) = &self.transfer_leader_state().transfer_leader_msg else {
            // There is no pending transfer leader message, do not ack.
            return false;
        };

        // Ack the message if any of the following conditions is met:
        //
        // * The deadline is exceeded.
        // * The cache has warmed up and coprocessors are ready to ack.
        let is_deadline_exceeded = Instant::now() >= *deadline;
        let is_cop_ready = ctx
            .coprocessor_host
            .pre_ack_transfer_leader(self.region(), msg);
        let is_cache_ready = self.maybe_transfer_leader_cache_warmup(ctx, msg.get_index());

        let is_ready_ack = is_deadline_exceeded || (is_cache_ready && is_cop_ready);
        if !is_ready_ack {
            return false;
        }

        self.ack_transfer_leader_msg(false);
        self.transfer_leader_state_mut().transfer_leader_msg = None;
        true
    }

    /// Set a pending transfer leader message to allow the transferee to
    /// initiate raft log cache warmup.
    /// The message will be cleaned up once the transferee has warmed up its
    /// cache or the peer becomes leader.
    ///
    /// Called by transferee.
    pub fn set_pending_transfer_leader_msg(&mut self, cfg: &Config, msg: &eraftpb::Message) {
        // log_term is set by original leader in pre transfer leader stage.
        // Callers must guarantee that the message is a valid transfer leader
        // message.
        //
        // See more in `Peer::pre_transfer_leader`.
        assert!(
            msg.get_msg_type() == eraftpb::MessageType::MsgTransferLeader
                && msg.get_log_term() == self.term(),
            "[{}] {} unexpected message type {:?}",
            self.region_id(),
            self.peer_id(),
            msg.get_msg_type(),
        );

        // We don't want to block transfer leader indefinitely, so set a
        // deadline for the transferee to warm up its cache and other necessary
        // works. Half of the election timeout should be long enough.
        let half_election_timeout = cfg.raft_base_tick_interval.0
            * std::cmp::max(1, cfg.raft_election_timeout_ticks / 2) as u32;
        let max_wait_duration =
            std::cmp::max(half_election_timeout, cfg.max_entry_cache_warmup_duration.0);
        let deadline = Instant::now() + max_wait_duration;
        self.transfer_leader_state_mut().transfer_leader_msg = Some((msg.clone(), deadline));
    }

    /// Before ack the transfer leader message sent by the leader.
    /// Currently, it only warms up the entry cache in this stage.
    ///
    /// This return whether the msg should be acked. When cache is warmed up
    /// or the warmup operation is timeout, it is true.
    fn maybe_transfer_leader_cache_warmup<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        low_index: u64,
    ) -> bool {
        if !ctx.cfg.warmup_entry_cache_enabled() {
            return true;
        }

        // The start index of warmup range. It is leader's entry_cache_first_index,
        // which in general is equal to the lowest matched index.
        let mut low = low_index;
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

        // Reset cache warmup state if an election timeout has passed since the
        // previous warmup, because cache may have been invalidated by
        // `compact_entry_cache` and a new leader may have been elected.
        // The reset allows it to initiate a new warmup operation.
        if self
            .transfer_leader_state_mut()
            .cache_warmup_state
            .as_mut()
            .is_some_and(|s| s.check_stale())
        {
            info!(
                self.logger,
                "reset stale cache warmup state";
                "range" => ?self.transfer_leader_state().cache_warmup_state.as_ref().unwrap().range(),
            );
            self.transfer_leader_state_mut().cache_warmup_state = None;
        }

        // Check if the warmup operation is timeout if warmup is already started.
        if let Some(state) = &mut self.transfer_leader_state_mut().cache_warmup_state {
            // If it is timeout, this peer should ack the message so that
            // the leadership transfer process can continue.
            state.check_task_timeout()
        } else if let Some((low, high)) = self
            .storage_mut()
            .entry_storage_mut()
            .async_warm_up_entry_cache(low)
        {
            self.transfer_leader_state_mut().cache_warmup_state = Some(CacheWarmupState::new(
                low,
                high,
                ctx.cfg.max_entry_cache_warmup_duration.0,
                ctx.cfg.raft_base_tick_interval.0 * ctx.cfg.raft_election_timeout_ticks as u32,
            ));
            false
        } else {
            // Ack transfer leader immediately if async entry cache fails or
            // have been warmed up already.
            true
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
            msg.set_context(TransferLeaderContext::CommandReply.to_bytes().unwrap());
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
