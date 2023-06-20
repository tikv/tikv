// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{
    data_cf_offset, DeleteStrategy, KvEngine, Mutable, RaftEngine, Range as EngineRange, ALL_CFS,
    CF_DEFAULT,
};
use fail::fail_point;
use kvproto::raft_cmdpb::RaftRequestHeader;
use raftstore::{
    store::{
        cmd_resp,
        fsm::{apply, MAX_PROPOSAL_SIZE_RATIO},
        metrics::PEER_WRITE_CMD_COUNTER,
        msg::ErrorCallback,
        util::{self, NORMAL_REQ_CHECK_CONF_VER, NORMAL_REQ_CHECK_VER},
    },
    Error, Result,
};
use slog::info;
use tikv_util::{box_err, slog_panic};

use crate::{
    batch::StoreContext,
    fsm::ApplyResReporter,
    operation::SimpleWriteReqEncoder,
    raft::{Apply, Peer},
    router::{ApplyTask, CmdResChannel},
};

mod ingest;

pub use raftstore::store::simple_write::{
    SimpleWrite, SimpleWriteBinary, SimpleWriteEncoder, SimpleWriteReqDecoder,
};

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
        if self.proposal_control().has_pending_prepare_merge()
            || self.proposal_control().is_merging()
        {
            let resp = cmd_resp::new_error(Error::ProposalInMergingMode(self.region_id()));
            ch.report_error(resp);
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
            fail_point!("after_propose_pending_writes");

            self.post_propose_command(ctx, res, chs, call_proposed_on_success);
        }
    }
}

impl<EK: KvEngine, R: ApplyResReporter> Apply<EK, R> {
    #[inline]
    pub fn apply_put(&mut self, cf: &str, index: u64, key: &[u8], value: &[u8]) -> Result<()> {
        PEER_WRITE_CMD_COUNTER.put.inc();
        let off = data_cf_offset(cf);
        if self.should_skip(off, index) {
            return Ok(());
        }
        util::check_key_in_region(key, self.region())?;
        if let Some(s) = self.buckets.as_mut() {
            s.write_key(key, value.len() as u64);
        }
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
            slog_panic!(
                self.logger,
                "failed to write";
                "key" => %log_wrappers::Value::key(key),
                "value" => %log_wrappers::Value::value(value),
                "cf" => cf,
                "error" => ?e
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
        PEER_WRITE_CMD_COUNTER.delete.inc();
        let off = data_cf_offset(cf);
        if self.should_skip(off, index) {
            return Ok(());
        }
        util::check_key_in_region(key, self.region())?;
        if let Some(s) = self.buckets.as_mut() {
            s.write_key(key, 0);
        }
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
            slog_panic!(
                self.logger,
                "failed to delete";
                "key" => %log_wrappers::Value::key(key),
                "cf" => cf,
                "error" => ?e
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
        mut cf: &str,
        index: u64,
        start_key: &[u8],
        end_key: &[u8],
        notify_only: bool,
        use_delete_range: bool,
    ) -> Result<()> {
        PEER_WRITE_CMD_COUNTER.delete_range.inc();
        let off = data_cf_offset(cf);
        if self.should_skip(off, index) {
            return Ok(());
        }
        if !end_key.is_empty() && start_key >= end_key {
            return Err(box_err!(
                "invalid delete range command, start_key: {:?}, end_key: {:?}",
                start_key,
                end_key
            ));
        }
        // region key range has no data prefix, so we must use origin key to check.
        util::check_key_in_region(start_key, self.region())?;
        let end_key = keys::data_end_key(end_key);
        let region_end_key = keys::data_end_key(self.region().get_end_key());
        if end_key > region_end_key {
            return Err(Error::KeyNotInRegion(
                end_key.to_vec(),
                self.region().clone(),
            ));
        }

        if cf.is_empty() {
            cf = CF_DEFAULT;
        }

        if !ALL_CFS.iter().any(|x| *x == cf) {
            return Err(box_err!("invalid delete range command, cf: {:?}", cf));
        }

        let start_key = keys::data_key(start_key);

        info!(
            self.logger,
            "execute delete range";
            "range_start" => log_wrappers::Value::key(&start_key),
            "range_end" => log_wrappers::Value::key(&end_key),
            "notify_only" => notify_only,
            "use_delete_range" => use_delete_range,
        );

        // Use delete_files_in_range to drop as many sst files as possible, this
        // is a way to reclaim disk space quickly after drop a table/index.
        if !notify_only {
            let range = vec![EngineRange::new(&start_key, &end_key)];
            let fail_f = |e: engine_traits::Error, strategy: DeleteStrategy| {
                slog_panic!(
                    self.logger,
                    "failed to delete";
                    "strategy" => ?strategy,
                    "range_start" => log_wrappers::Value::key(&start_key),
                    "range_end" => log_wrappers::Value::key(&end_key),
                    "error" => ?e,
                )
            };
            let tablet = self.tablet();
            tablet
                .delete_ranges_cf(cf, DeleteStrategy::DeleteFiles, &range)
                .unwrap_or_else(|e| fail_f(e, DeleteStrategy::DeleteFiles));

            let strategy = if use_delete_range {
                DeleteStrategy::DeleteByRange
            } else {
                DeleteStrategy::DeleteByKey
            };
            // Delete all remaining keys.
            tablet
                .delete_ranges_cf(cf, strategy.clone(), &range)
                .unwrap_or_else(move |e| fail_f(e, strategy));

            // to do: support titan?
            // tablet
            //     .delete_ranges_cf(cf, DeleteStrategy::DeleteBlobs, &range)
            //     .unwrap_or_else(move |e| fail_f(e,
            // DeleteStrategy::DeleteBlobs));
        }

        // delete range is an unsafe operation and it cannot be rollbacked to replay, so
        // we don't update modification index for this operation.

        Ok(())
    }
}
