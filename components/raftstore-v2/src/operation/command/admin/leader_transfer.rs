// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;

use bytes::Bytes;
use engine_traits::{KvEngine, RaftEngine};
use kvproto::{
    disk_usage::DiskUsage,
    metapb,
    raft_cmdpb::{AdminCmdType, RaftCmdRequest, TransferLeaderRequest},
};
use raft::{eraftpb, ProgressState, Storage};
use raftstore::store::{
    fsm::new_admin_request, make_transfer_leader_response, TRANSFER_LEADER_COMMAND_REPLY_CTX,
};
use slog::info;
use txn_types::WriteBatchFlags;

use crate::{
    batch::StoreContext,
    raft::Peer,
    router::{CmdResChannel, PeerMsg},
};

fn get_transfer_leader_cmd(msg: &RaftCmdRequest) -> Option<&TransferLeaderRequest> {
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
    /// Return true to if the transfer leader request is accepted.
    ///
    /// When transferring leadership begins, leader sends a pre-transfer
    /// to target follower first to ensures it's ready to become leader.
    /// After that the real transfer leader process begin.
    ///
    /// 1. pre_transfer_leader on leader:
    ///     Leader will send a MsgTransferLeader to follower.
    /// 2. execute_transfer_leader on follower
    ///     If follower passes all necessary checks, it will reply an
    ///     ACK with type MsgTransferLeader and its promised persistent index.
    /// 3. ready_to_transfer_leader on leader:
    ///     Leader checks if it's appropriate to transfer leadership. If it
    ///     does, it calls raft transfer_leader API to do the remaining work.
    ///
    /// See also: tikv/rfcs#37.
    pub fn propose_transfer_leader<T>(
        &self,
        ctx: &mut StoreContext<EK, ER, T>,
        req: RaftCmdRequest,
        ch: CmdResChannel,
    ) -> bool {
        ctx.raft_metrics.propose.transfer_leader.inc();

        let transfer_leader = get_transfer_leader_cmd(&req).unwrap();
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
            1 => peers.get(0).unwrap(),
            _ => peers.choose(&mut rand::thread_rng()).unwrap(),
        };

        let transferee = if peer.id == self.peer().id {
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

    fn pre_transfer_leader(&mut self, peer: &metapb::Peer) -> bool {
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
        self.raft_group().ping();

        let mut msg = eraftpb::Message::new();
        msg.set_to(peer.get_id());
        msg.set_msg_type(eraftpb::MessageType::MsgTransferLeader);
        msg.set_from(self.peer_id());
        msg.set_index(
            self.storage()
                .entry_storage()
                .entry_cache_first_index()
                .unwrap_or(0),
        );
        // log term here represents the term of last log. For leader, the term of last
        // log is always its current term. Not just set term because raft library
        // forbids setting it for MsgTransferLeader messages.
        msg.set_log_term(self.term());
        self.raft_group().raft.msgs.push(msg);
        true
    }

    pub fn on_transfer_leader_msg<T>(
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
            self.execute_transfer_leader(ctx, msg.get_from(), peer_disk_usage, false);
        } else {
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
                    self.propose_pending_command(ctx);
                    if self.propose_locks_before_transfer_leader(msg) {
                        // If some pessimistic locks are just proposed, we propose another
                        // TransferLeader command instead of transferring leader immediately.
                        info!(
                            self.logger,
                            "propose transfer leader command";
                            "to" => ?from,
                        );
                        let mut cmd =
                            new_admin_request(self.region().get_id(), self.peer().clone());
                        cmd.mut_header()
                            .set_region_epoch(self.region().get_region_epoch().clone());
                        // Set this flag to propose this command like a normal proposal.
                        cmd.mut_header()
                            .set_flags(WriteBatchFlags::TRANSFER_LEADER_PROPOSAL.bits());
                        cmd.mut_admin_request()
                            .set_cmd_type(AdminCmdType::TransferLeader);
                        cmd.mut_admin_request().mut_transfer_leader().set_peer(from);
                        let (PeerMsg::RaftCommand(req), sub) = PeerMsg::raft_command(cmd);
                        self.on_admin_command(ctx, req.request, req.ch);
                    } else {
                        self.transfer_leader(&from);
                        // self.wait_data_peers.clear();
                    }
                }
            }
        }
    }

    pub fn execute_transfer_leader<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        from: u64,
        peer_disk_usage: DiskUsage,
        reply_cmd: bool, // whether it is a reply to a TransferLeader command
    ) {
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
            return;
        }

        let mut msg = eraftpb::Message::new();
        msg.set_from(self.peer_id());
        msg.set_to(self.leader_id());
        msg.set_msg_type(eraftpb::MessageType::MsgTransferLeader);
        msg.set_index(self.storage().apply_state().applied_index);
        msg.set_log_term(self.term());
        if reply_cmd {
            msg.set_context(Bytes::from_static(TRANSFER_LEADER_COMMAND_REPLY_CTX));
        }
        self.raft_group().raft.msgs.push(msg);
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

        if self.storage().last_index() >= index + ctx.cfg.leader_transfer_max_log_lag {
            return Some("log gap");
        }
        None
    }

    // Returns whether we should propose another TransferLeader command. This is
    // for:
    // - Considering the amount of pessimistic locks can be big, it can reduce
    //   unavailable time caused by waiting for the transferee catching up logs.
    // - Make transferring leader strictly after write commands that executes before
    //   proposing the locks, preventing unexpected lock loss.
    fn propose_locks_before_transfer_leader(&mut self, msg: &eraftpb::Message) -> bool {
        false
    }

    fn transfer_leader(&mut self, peer: &metapb::Peer) {}
}
