// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, Mutable, RaftEngine, CF_DEFAULT};
use kvproto::raft_cmdpb::RaftRequestHeader;
use raftstore::{
    store::{
        cmd_resp,
        fsm::{apply, MAX_PROPOSAL_SIZE_RATIO},
        msg::ErrorCallback,
        util::{self, NORMAL_REQ_CHECK_CONF_VER, NORMAL_REQ_CHECK_VER},
    },
    Result,
};

use crate::{
    batch::StoreContext,
    operation::cf_offset,
    raft::{Apply, Peer},
    router::{ApplyTask, CmdResChannel},
};

mod simple_write;

pub use simple_write::{
    SimpleWriteBinary, SimpleWriteEncoder, SimpleWriteReqDecoder, SimpleWriteReqEncoder,
};

pub use self::simple_write::SimpleWrite;

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    #[inline]
    pub fn on_simple_write<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        header: Box<RaftRequestHeader>,
        data: SimpleWriteBinary,
        ch: CmdResChannel,
    ) {
        if !self.serving() {
            apply::notify_req_region_removed(self.region_id(), ch);
            return;
        }
        if let Some(encoder) = self.simple_write_encoder_mut() {
            if encoder.amend(&header, &data) {
                encoder.add_response_channel(ch);
                self.set_has_ready();
                return;
            }
        }
        if let Err(e) = self.validate_command(&header, None, &mut ctx.raft_metrics) {
            let resp = cmd_resp::new_error(e);
            ch.report_error(resp);
            return;
        }
        // To maintain propose order, we need to make pending proposal first.
        self.propose_pending_writes(ctx);
        if let Some(conflict) = self.proposal_control_mut().check_conflict(None) {
            conflict.delay_channel(ch);
            return;
        }
        // ProposalControl is reliable only when applied to current term.
        let call_proposed_on_success = self.applied_to_current_term();
        let mut encoder = SimpleWriteReqEncoder::new(
            header,
            data,
            (ctx.cfg.raft_entry_max_size.0 as f64 * MAX_PROPOSAL_SIZE_RATIO) as usize,
            call_proposed_on_success,
        );
        encoder.add_response_channel(ch);
        self.set_has_ready();
        self.simple_write_encoder_mut().replace(encoder);
    }

    #[inline]
    pub fn on_unsafe_write<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        data: SimpleWriteBinary,
    ) {
        if !self.serving() {
            return;
        }
        let bin = SimpleWriteReqEncoder::new(
            Box::<RaftRequestHeader>::default(),
            data,
            ctx.cfg.raft_entry_max_size.0 as usize,
            false,
        )
        .encode()
        .0
        .into_boxed_slice();
        if let Some(scheduler) = self.apply_scheduler() {
            scheduler.send(ApplyTask::UnsafeWrite(bin));
        }
    }

    pub fn propose_pending_writes<T>(&mut self, ctx: &mut StoreContext<EK, ER, T>) {
        if let Some(encoder) = self.simple_write_encoder_mut().take() {
            let call_proposed_on_success = if encoder.notify_proposed() {
                // The request has pass conflict check and called all proposed callbacks.
                false
            } else {
                // Epoch may have changed since last check.
                let from_epoch = encoder.header().get_region_epoch();
                let res = util::compare_region_epoch(
                    from_epoch,
                    self.region(),
                    NORMAL_REQ_CHECK_CONF_VER,
                    NORMAL_REQ_CHECK_VER,
                    true,
                );
                if let Err(e) = res {
                    // TODO: query sibling regions.
                    ctx.raft_metrics.invalid_proposal.epoch_not_match.inc();
                    encoder.encode().1.report_error(cmd_resp::new_error(e));
                    return;
                }
                // Only when it applies to current term, the epoch check can be reliable.
                self.applied_to_current_term()
            };
            let (data, chs) = encoder.encode();
            let res = self.propose(ctx, data);
            self.post_propose_command(ctx, res, chs, call_proposed_on_success);
        }
    }
}

impl<EK: KvEngine, R> Apply<EK, R> {
    #[inline]
    pub fn apply_put(&mut self, cf: &str, index: u64, key: &[u8], value: &[u8]) -> Result<()> {
        let off = cf_offset(cf);
        if self.should_skip(off, index) {
            return Ok(());
        }
        util::check_key_in_region(key, self.region_state().get_region())?;
        // Technically it's OK to remove prefix for raftstore v2. But rocksdb doesn't
        // support specifying infinite upper bound in various APIs.
        keys::data_key_with_buffer(key, &mut self.key_buffer);
        self.ensure_write_buffer();
        let res = if cf.is_empty() || cf == CF_DEFAULT {
            // TODO: use write_vector
            self.write_batch
                .as_mut()
                .unwrap()
                .put(&self.key_buffer, value)
        } else {
            self.write_batch
                .as_mut()
                .unwrap()
                .put_cf(cf, &self.key_buffer, value)
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
        self.metrics.size_diff_hint += (self.key_buffer.len() + value.len()) as i64;
        if index != u64::MAX {
            self.modifications_mut()[off] = index;
        }
        Ok(())
    }

    #[inline]
    pub fn apply_delete(&mut self, cf: &str, index: u64, key: &[u8]) -> Result<()> {
        let off = cf_offset(cf);
        if self.should_skip(off, index) {
            return Ok(());
        }
        util::check_key_in_region(key, self.region_state().get_region())?;
        keys::data_key_with_buffer(key, &mut self.key_buffer);
        self.ensure_write_buffer();
        let res = if cf.is_empty() || cf == CF_DEFAULT {
            // TODO: use write_vector
            self.write_batch.as_mut().unwrap().delete(&self.key_buffer)
        } else {
            self.write_batch
                .as_mut()
                .unwrap()
                .delete_cf(cf, &self.key_buffer)
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
        self.metrics.size_diff_hint -= self.key_buffer.len() as i64;
        if index != u64::MAX {
            self.modifications_mut()[off] = index;
        }
        Ok(())
    }

    #[inline]
    pub fn apply_delete_range(
        &mut self,
        _cf: &str,
        _index: u64,
        _start_key: &[u8],
        _end_key: &[u8],
        _notify_only: bool,
    ) -> Result<()> {
        // TODO: reuse the same delete as split/merge.
        Ok(())
    }
}
