// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

mod conf_change;

use engine_traits::{KvEngine, RaftEngine};
use kvproto::{
    raft_cmdpb::{AdminRequest, RaftCmdRequest},
    raft_serverpb::PeerState,
};
use protobuf::Message;
use raft::prelude::ConfChangeV2;
use raftstore::{
    store::{
        self, cmd_resp,
        fsm::apply,
        msg::ErrorCallback,
        util::{ChangePeerI, ConfChangeKind},
    },
    Result,
};
use slog::info;
use tikv_util::box_err;

use self::conf_change::ConfChangeResult;
use crate::{
    batch::StoreContext,
    raft::{Apply, Peer},
    router::CmdResChannel,
};

#[derive(Debug)]
pub enum AdminCmdResult {
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
        // To maintain propose order, we need to make pending proposal first.
        self.propose_pending_command(ctx);
        let cmd_type = req.get_admin_request().get_cmd_type();
        let res = if apply::is_conf_change_cmd(&req) {
            self.propose_conf_change(ctx, req)
        } else {
            // propose other admin command.
            unimplemented!()
        };
        if let Err(e) = &res {
            info!(
                self.logger,
                "failed to propose admin command";
                "cmd_type" => ?cmd_type,
                "error" => ?e,
            );
        }
        self.post_propose_write(ctx, res, vec![ch]);
    }
}
