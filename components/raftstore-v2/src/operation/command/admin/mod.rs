// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

mod conf_change;
mod split;

use engine_traits::{KvEngine, RaftEngine};
use kvproto::raft_cmdpb::{AdminCmdType, AdminRequest, RaftCmdRequest};
use protobuf::Message;
use raft::prelude::ConfChangeV2;
use raftstore::{
    store::{
        self, cmd_resp,
        fsm::apply,
        msg::ErrorCallback,
<<<<<<< HEAD
        util::{ChangePeerI, ConfChangeKind},
=======
        ProposalContext, Transport,
>>>>>>> 7953ea518c (raftstore-v2: Allow rollback merge during unsafe recovery for raftstore v2 (#15780))
    },
    Result,
};
use slog::info;
pub use split::{SplitInit, SplitResult};
use tikv_util::box_err;

use self::conf_change::ConfChangeResult;
use crate::{
    batch::StoreContext,
    raft::{Apply, Peer},
    router::CmdResChannel,
};

#[derive(Debug)]
pub enum AdminCmdResult {
    SplitRegion(SplitResult),
    ConfChange(ConfChangeResult),
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    #[inline]
    pub fn on_admin_command<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        mut req: RaftCmdRequest,
        ch: CmdResChannel,
    ) {
        if !self.serving() {
            apply::notify_req_region_removed(self.region_id(), ch);
            return;
        }
        if let Err(e) = self.validate_command(&req, &mut ctx.raft_metrics) {
            let resp = cmd_resp::new_error(e);
            ch.report_error(resp);
            return;
        }

        // The admin request is rejected because it may need to update epoch checker
        // which introduces an uncertainty and may breaks the correctness of epoch
        // checker.
        if !self.applied_to_current_term() {
            let e = box_err!(
                "{:?} peer has not applied to current term, applied_term {}, current_term {}",
                self.logger.list(),
                self.storage().entry_storage().applied_term(),
                self.term()
            );
            let resp = cmd_resp::new_error(e);
            ch.report_error(resp);
            return;
        }
        let cmd_type = req.get_admin_request().get_cmd_type();
        if let Some(conflict) = self.proposal_control_mut().check_conflict(Some(cmd_type)) {
            conflict.delay_channel(ch);
            return;
        }
        // To maintain propose order, we need to make pending proposal first.
        self.propose_pending_writes(ctx);
        let res = if apply::is_conf_change_cmd(&req) {
            self.propose_conf_change(ctx, req)
        } else {
            // propose other admin command.
            match cmd_type {
                AdminCmdType::Split => Err(box_err!(
                    "Split is deprecated. Please use BatchSplit instead."
                )),
<<<<<<< HEAD
                AdminCmdType::BatchSplit => self.propose_split(ctx, req),
                _ => unimplemented!(),
=======
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
                AdminCmdType::UpdateGcPeer => {
                    let data = req.write_to_bytes().unwrap();
                    self.propose(ctx, data)
                }
                AdminCmdType::RollbackMerge => {
                    let data = req.write_to_bytes().unwrap();
                    self.propose_with_ctx(ctx, data, ProposalContext::ROLLBACK_MERGE)
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
>>>>>>> 7953ea518c (raftstore-v2: Allow rollback merge during unsafe recovery for raftstore v2 (#15780))
            }
        };
        match &res {
            Ok(index) => self
                .proposal_control_mut()
                .record_proposed_admin(cmd_type, *index),
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
}
