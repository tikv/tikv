// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, Mutable, RaftEngine, CF_DEFAULT};
use kvproto::raft_cmdpb::{CmdType, RaftCmdRequest, Request};
use raftstore::{
    store::{
        cmd_resp,
        fsm::{apply, Proposal, MAX_PROPOSAL_SIZE_RATIO},
        msg::ErrorCallback,
        util, WriteCallback,
    },
    Result,
};

use crate::{
    batch::StoreContext,
    raft::{Apply, Peer},
    router::CmdResChannel,
};

mod simple_write;

pub use simple_write::{SimpleWriteDecoder, SimpleWriteEncoder};

pub use self::simple_write::SimpleWrite;
use super::CommittedEntries;

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
        if let Some(encoder) = self.simple_write_encoder_mut() {
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
                self.simple_write_encoder_mut().replace(encoder);
            }
            Err(req) => {
                let res = self.propose_command(ctx, req);
                self.post_propose_write(ctx, res, vec![ch]);
            }
        }
    }

    #[inline]
    pub fn post_propose_write<T>(
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
        if let Some(encoder) = self.simple_write_encoder_mut().take() {
            let (data, chs) = encoder.encode();
            let res = self.propose(ctx, data);
            self.post_propose_write(ctx, res, chs);
        }
    }
}

impl<EK: KvEngine, R> Apply<EK, R> {
    #[inline]
    pub fn apply_put(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        util::check_key_in_region(key, self.region_state().get_region())?;
        let res = if cf.is_empty() || cf == CF_DEFAULT {
            // TODO: use write_vector
            self.write_batch_or_default().put(key, value)
        } else {
            self.write_batch_or_default().put_cf(cf, key, value)
        };
        res.unwrap_or_else(|e| {
            panic!(
                "{:?} failed to write ({}, {}) {}: {:?}",
                self.logger.list(),
                log_wrappers::Value::key(key),
                log_wrappers::Value::value(value),
                cf,
                e
            );
        });
        fail::fail_point!("APPLY_PUT", |_| Err(raftstore::Error::Other(
            "aborted by failpoint".into()
        )));
        Ok(())
    }

    #[inline]
    pub fn apply_delete(&mut self, cf: &str, key: &[u8]) -> Result<()> {
        util::check_key_in_region(key, self.region_state().get_region())?;
        let res = if cf.is_empty() || cf == CF_DEFAULT {
            // TODO: use write_vector
            self.write_batch_or_default().delete(key)
        } else {
            self.write_batch_or_default().delete_cf(cf, key)
        };
        res.unwrap_or_else(|e| {
            panic!(
                "{:?} failed to delete {} {}: {:?}",
                self.logger.list(),
                log_wrappers::Value::key(key),
                cf,
                e
            );
        });
        Ok(())
    }

    #[inline]
    pub fn apply_delete_range(
        &mut self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
        notify_only: bool,
    ) -> Result<()> {
        /// TODO: reuse the same delete as split/merge.
        Ok(())
    }
}
