// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, RaftEngine};
use kvproto::raft_cmdpb::RaftCmdRequest;
use raftstore::{
    store::{
        cmd_resp,
        fsm::{apply, Proposal, MAX_PROPOSAL_SIZE_RATIO},
        msg::ErrorCallback,
        WriteCallback,
    },
    Result,
};
use tikv_util::Either;

use crate::{batch::StoreContext, raft::Peer, router::CmdResChannel};

mod simple_write;

pub use simple_write::{SimpleWriteDecoder, SimpleWriteEncoder};

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    #[inline]
    pub fn on_write_command<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        mut req: RaftCmdRequest,
        ch: CmdResChannel,
    ) {
        if !self.serving() {
            apply::notify_req_region_removed(self.region_id(), ch);
            return;
        }
        if let Some(encoder) = self.raw_write_encoder_mut() {
            match encoder.amend(req) {
                Ok(()) => {
                    encoder.add_response_channel(ch);
                    self.set_has_ready();
                    return;
                }
                Err(r) => req = r,
            }
        }
        if let Err(e) = self.validate_command(&req, &mut ctx.raft_metrics) {
            let resp = cmd_resp::new_error(e);
            ch.report_error(resp);
            return;
        }
        // To maintain propose order, we need to make pending proposal first.
        self.propose_pending_command(ctx);
        match SimpleWriteEncoder::new(
            req,
            (ctx.cfg.raft_entry_max_size.0 as f64 * MAX_PROPOSAL_SIZE_RATIO) as usize,
        ) {
            Ok(mut encoder) => {
                encoder.add_response_channel(ch);
                self.set_has_ready();
                self.raw_write_encoder_mut().replace(encoder);
            }
            Err(req) => {
                let res = self.propose_command(ctx, req);
                self.post_propose_write(ctx, res, vec![ch]);
            }
        }
    }

    fn post_propose_write<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        res: Result<u64>,
        ch: Vec<CmdResChannel>,
    ) {
        let idx = match res {
            Ok(i) => i,
            Err(e) => {
                ch.report_error(cmd_resp::err_resp(e, self.term()));
                return;
            }
        };
        let p = Proposal::new(idx, self.term(), ch);
        self.enqueue_pending_proposal(ctx, p);
        self.set_has_ready();
    }

    pub fn propose_pending_command<T>(&mut self, ctx: &mut StoreContext<EK, ER, T>) {
        if let Some(encoder) = self.raw_write_encoder_mut().take() {
            let (data, chs) = encoder.encode();
            let res = self.propose(ctx, data);
            self.post_propose_write(ctx, res, chs);
        }
    }
}
