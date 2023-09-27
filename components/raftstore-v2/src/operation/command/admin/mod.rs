// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

mod compact_log;
mod conf_change;
mod flashback;
mod merge;
mod split;
mod transfer_leader;

pub use compact_log::CompactLogContext;
use compact_log::CompactLogResult;
use conf_change::{ConfChangeResult, UpdateGcPeersResult};
use engine_traits::{KvEngine, RaftEngine};
use kvproto::{
    kvrpcpb::DiskFullOpt,
    metapb::{PeerRole, Region},
    raft_cmdpb::{AdminCmdType, RaftCmdRequest},
    raft_serverpb::{ExtraMessageType, FlushMemtable, RaftMessage},
};
use merge::{
    commit::CommitMergeResult, prepare::PrepareMergeResult, rollback::RollbackMergeResult,
};
pub use merge::{
    commit::{CatchUpLogs, MERGE_IN_PROGRESS_PREFIX},
    merge_source_path, MergeContext, MERGE_SOURCE_PREFIX,
};
use protobuf::Message;
use raftstore::{
    store::{
        cmd_resp,
        fsm::{apply, apply::validate_batch_split},
        msg::ErrorCallback,
        Transport,
    },
    Error,
};
use slog::{debug, error, info};
use split::SplitResult;
pub use split::{
    report_split_init_finish, temp_split_path, RequestHalfSplit, RequestSplit, SplitFlowControl,
    SplitInit, SplitPendingAppend, SPLIT_PREFIX,
};
use tikv_util::{box_err, log::SlogFormat, slog_panic, sys::disk::DiskUsage};
use txn_types::WriteBatchFlags;

use self::flashback::FlashbackResult;
use crate::{
    batch::StoreContext,
    raft::Peer,
    router::{CmdResChannel, PeerMsg, RaftRequest},
};

#[derive(Debug)]
pub enum AdminCmdResult {
    // No side effect produced by the command
    None,
    SplitRegion(SplitResult),
    ConfChange(ConfChangeResult),
    TransferLeader(u64),
    CompactLog(CompactLogResult),
    UpdateGcPeers(UpdateGcPeersResult),
    PrepareMerge(PrepareMergeResult),
    CommitMerge(CommitMergeResult),
    Flashback(FlashbackResult),
    RollbackMerge(RollbackMergeResult),
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    #[inline]
    pub fn on_admin_command<T: Transport>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        mut req: RaftCmdRequest,
        ch: CmdResChannel,
    ) {
        if !self.serving() {
            apply::notify_req_region_removed(self.region_id(), ch);
            return;
        }
        if !req.has_admin_request() {
            let e = box_err!(
                "{} expect only execute admin command",
                SlogFormat(&self.logger)
            );
            let resp = cmd_resp::new_error(e);
            ch.report_error(resp);
            return;
        }
        if let Err(e) = ctx.coprocessor_host.pre_propose(self.region(), &mut req) {
            let resp = cmd_resp::new_error(e.into());
            ch.report_error(resp);
            return;
        }
        let cmd_type = req.get_admin_request().get_cmd_type();
        if let Err(e) =
            self.validate_command(req.get_header(), Some(cmd_type), &mut ctx.raft_metrics)
        {
            let resp = cmd_resp::new_error(e);
            ch.report_error(resp);
            return;
        }

        let is_transfer_leader = cmd_type == AdminCmdType::TransferLeader;
        let pre_transfer_leader = cmd_type == AdminCmdType::TransferLeader
            && !WriteBatchFlags::from_bits_truncate(req.get_header().get_flags())
                .contains(WriteBatchFlags::TRANSFER_LEADER_PROPOSAL);
        let is_conf_change = apply::is_conf_change_cmd(&req);

        // Check whether the admin request can be proposed when disk full.
        let can_skip_check = is_transfer_leader || pre_transfer_leader || is_conf_change;
        if !can_skip_check && let Err(e) =
            self.check_proposal_with_disk_full_opt(ctx, DiskFullOpt::AllowedOnAlmostFull)
        {
            let resp = cmd_resp::new_error(e);
            ch.report_error(resp);
            self.post_propose_fail(cmd_type);
            return;
        }

        // The admin request is rejected because it may need to update epoch checker
        // which introduces an uncertainty and may breaks the correctness of epoch
        // checker.
        // As pre transfer leader is just a warmup phase, applying to the current term
        // is not required.
        if !self.applied_to_current_term() && !pre_transfer_leader {
            let e = box_err!(
                "{} peer has not applied to current term, applied_term {}, current_term {}",
                SlogFormat(&self.logger),
                self.storage().entry_storage().applied_term(),
                self.term()
            );
            let resp = cmd_resp::new_error(e);
            ch.report_error(resp);
            return;
        }
        // Do not check conflict for transfer leader, otherwise we may not
        // transfer leadership out of busy nodes in time.
        if !is_transfer_leader && let Some(conflict) = self.proposal_control_mut().check_conflict(Some(cmd_type)) {
            conflict.delay_channel(ch);
            return;
        }
        if self.proposal_control().has_pending_prepare_merge()
            && cmd_type != AdminCmdType::PrepareMerge
            || self.proposal_control().is_merging() && cmd_type != AdminCmdType::RollbackMerge
        {
            let resp = cmd_resp::new_error(Error::ProposalInMergingMode(self.region_id()));
            ch.report_error(resp);
            return;
        }
        // Prepare Merge need to be broadcast to as many as followers when disk full.
        self.on_prepare_merge(cmd_type, ctx);
        // To maintain propose order, we need to make pending proposal first.
        self.propose_pending_writes(ctx);
        let res = if is_conf_change {
            self.propose_conf_change(ctx, req)
        } else {
            // propose other admin command.
            match cmd_type {
                AdminCmdType::Split => Err(box_err!(
                    "Split is deprecated. Please use BatchSplit instead."
                )),
                AdminCmdType::BatchSplit => {
                    #[allow(clippy::question_mark)]
                    if let Err(err) = validate_batch_split(req.get_admin_request(), self.region()) {
                        Err(err)
                    } else {
                        // To reduce the impact of the expensive operation of `checkpoint` (it will
                        // flush memtables of the rocksdb) in applying batch split, we split the
                        // BatchSplit cmd into two phases:
                        //
                        // 1. Schedule flush memtable task so that the memtables of the rocksdb can
                        // be flushed in advance in a way that will not block the normal raft
                        // operations (`checkpoint` will still cause flush but it will be
                        // significantly lightweight). At the same time, send flush memtable msgs to
                        // the follower so that they can flush memtalbes in advance too.
                        //
                        // 2. When the task finishes, it will propose a batch split with
                        // `PRE_FLUSH_FINISHED` flag.
                        if !WriteBatchFlags::from_bits_truncate(req.get_header().get_flags())
                            .contains(WriteBatchFlags::PRE_FLUSH_FINISHED)
                        {
                            let mailbox = match ctx.router.mailbox(self.region_id()) {
                                Some(mailbox) => mailbox,
                                None => {
                                    assert!(
                                        ctx.router.is_shutdown(),
                                        "{} router should have been closed",
                                        SlogFormat(&self.logger)
                                    );
                                    return;
                                }
                            };
                            req.mut_header()
                                .set_flags(WriteBatchFlags::PRE_FLUSH_FINISHED.bits());
                            let logger = self.logger.clone();
                            let on_flush_finish = move || {
                                if let Err(e) = mailbox
                                    .try_send(PeerMsg::AdminCommand(RaftRequest::new(req, ch)))
                                {
                                    error!(
                                        logger,
                                        "send BatchSplit request failed after pre-flush finished";
                                        "err" => ?e,
                                    );
                                }
                            };
                            self.start_pre_flush(
                                ctx,
                                "split",
                                false,
                                &self.region().clone(),
                                Box::new(on_flush_finish),
                            );
                            return;
                        }

                        info!(
                            self.logger,
                            "Propose split";
                        );
                        self.propose_split(ctx, req)
                    }
                }
                AdminCmdType::TransferLeader => {
                    // Containing TRANSFER_LEADER_PROPOSAL flag means the this transfer leader
                    // request should be proposed to the raft group
                    if WriteBatchFlags::from_bits_truncate(req.get_header().get_flags())
                        .contains(WriteBatchFlags::TRANSFER_LEADER_PROPOSAL)
                    {
                        let data = req.write_to_bytes().unwrap();
                        self.propose(ctx, data)
                    } else {
                        if self.propose_transfer_leader(ctx, req, ch) {
                            self.set_has_ready();
                        }
                        return;
                    }
                }
                AdminCmdType::CompactLog => self.propose_compact_log(ctx, req),
                AdminCmdType::UpdateGcPeer | AdminCmdType::RollbackMerge => {
                    let data = req.write_to_bytes().unwrap();
                    self.propose(ctx, data)
                }
                AdminCmdType::PrepareMerge => self.propose_prepare_merge(ctx, req),
                AdminCmdType::CommitMerge => self.propose_commit_merge(ctx, req),
                AdminCmdType::PrepareFlashback | AdminCmdType::FinishFlashback => {
                    self.propose_flashback(ctx, req)
                }
                _ => slog_panic!(
                    self.logger,
                    "unimplemented";
                    "admin_type" => ?cmd_type,
                ),
            }
        };
        match &res {
            Ok(index) => {
                self.proposal_control_mut()
                    .record_proposed_admin(cmd_type, *index);
                if self.proposal_control_mut().has_uncommitted_admin() {
                    self.raft_group_mut().skip_bcast_commit(false);
                }
            }
            Err(e) => {
                info!(
                    self.logger,
                    "failed to propose admin command";
                    "cmd_type" => ?cmd_type,
                    "error" => ?e,
                );
            }
        }
        self.post_propose_command(ctx, res, vec![ch], true);
    }

    fn on_prepare_merge<T: Transport>(
        &mut self,
        cmd_type: AdminCmdType,
        ctx: &StoreContext<EK, ER, T>,
    ) {
        let is_merge_cmd =
            cmd_type == AdminCmdType::PrepareMerge || cmd_type == AdminCmdType::RollbackMerge;
        let has_disk_full_peers = self.abnormal_peer_context().disk_full_peers().is_empty();
        let proposal_index = self.next_proposal_index();
        if is_merge_cmd
            && (!matches!(ctx.self_disk_usage, DiskUsage::Normal) || !has_disk_full_peers)
        {
            self.has_region_merge_proposal = true;
            self.region_merge_proposal_index = proposal_index;
            let mut peers = vec![];
            self.abnormal_peer_context_mut()
                .disk_full_peers_mut()
                .peers_mut()
                .iter_mut()
                .for_each(|(k, v)| {
                    if !matches!(v.0, DiskUsage::AlreadyFull) {
                        v.1 = true;
                        peers.push(*k);
                    }
                });
            debug!(
                self.logger,
                "adjust max inflight msgs";
                "cmd_type" => ?cmd_type,
                "raft_max_inflight_msgs" => ctx.cfg.raft_max_inflight_msgs,
                "region" => self.region_id()
            );
            self.adjust_peers_max_inflight_msgs(&peers, ctx.cfg.raft_max_inflight_msgs);
        }
    }

    fn start_pre_flush<T: Transport>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        reason: &'static str,
        high_priority: bool,
        target: &Region,
        on_local_flushed: Box<dyn FnOnce() + Send>,
    ) {
        let target_id = target.get_id();
        info!(
            self.logger,
            "Start pre flush tablet";
            "target" => target_id,
            "reason" => reason,
        );
        if let Err(e) = ctx.schedulers.tablet.schedule(crate::TabletTask::Flush {
            region_id: target_id,
            reason,
            high_priority,
            threshold: Some(std::time::Duration::from_secs(10)),
            cb: Some(on_local_flushed),
        }) {
            error!(
                self.logger,
                "Fail to schedule flush task";
                "err" => ?e,
            )
        }
        // Notify followers to flush their relevant memtables
        for p in target.get_peers() {
            if p == self.peer() || p.get_role() != PeerRole::Voter || p.is_witness {
                continue;
            }
            let mut msg = RaftMessage::default();
            msg.set_region_id(target_id);
            msg.set_from_peer(self.peer().clone());
            msg.set_to_peer(p.clone());
            msg.set_region_epoch(target.get_region_epoch().clone());
            let extra_msg = msg.mut_extra_msg();
            extra_msg.set_type(ExtraMessageType::MsgFlushMemtable);
            let mut flush_memtable = FlushMemtable::new();
            flush_memtable.set_region_id(target_id);
            extra_msg.set_flush_memtable(flush_memtable);

            self.send_raft_message(ctx, msg);
        }
    }
}
