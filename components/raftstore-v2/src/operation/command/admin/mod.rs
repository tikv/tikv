// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

mod compact_log;
mod conf_change;
mod split;
mod transfer_leader;

use compact_log::CompactLogResult;
use conf_change::ConfChangeResult;
use engine_traits::{KvEngine, RaftEngine};
use kvproto::raft_cmdpb::{AdminCmdType, RaftCmdRequest};
use protobuf::Message;
use raftstore::store::{cmd_resp, fsm::apply, msg::ErrorCallback};
use slog::info;
use split::SplitResult;
pub use split::{temp_split_path, RequestSplit, SplitFlowControl, SplitInit, SPLIT_PREFIX};
use tikv_util::box_err;
use txn_types::WriteBatchFlags;

use crate::{batch::StoreContext, raft::Peer, router::CmdResChannel};

#[derive(Debug)]
pub enum AdminCmdResult {
    // No side effect produced by the command
    None,
    SplitRegion(SplitResult),
    ConfChange(ConfChangeResult),
    TransferLeader(u64),
    CompactLog(CompactLogResult),
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
        if !req.has_admin_request() {
            let e = box_err!("{:?} expect only execute admin command", self.logger.list());
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
                AdminCmdType::BatchSplit => self.propose_split(ctx, req),
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
                _ => unimplemented!(),
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
}
