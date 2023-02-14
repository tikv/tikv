// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! This module contains implementations of commmands that will be replicated to
//! all replicas and executed in the same order. Typical commands include:
//! - normal writes like put, delete, etc.
//! - admin commands like split, compact, etc.
//!
//! General proceessing is:
//! - Propose a command to the leader via PeerMsg::Command,
//! - The leader batch up commands and replicates them to followers,
//! - Once they are replicated to majority, leader considers it committed and
//!   send to another thread for execution via
//!   `schedule_apply_committed_entries`,
//! - The apply thread executes the commands in buffer, and write to LSM tree
//!   via `flush`,
//! - Applied result are sent back to peer fsm, and update memory state in
//!   `on_apply_res`.

use std::{mem, sync::atomic::Ordering, time::Duration};

use engine_traits::{KvEngine, PerfContext, RaftEngine, WriteBatch, WriteOptions};
use kvproto::raft_cmdpb::{
    AdminCmdType, CmdType, RaftCmdRequest, RaftCmdResponse, RaftRequestHeader,
};
use protobuf::Message;
use raft::eraftpb::{ConfChange, ConfChangeV2, Entry, EntryType};
use raft_proto::ConfChangeI;
use raftstore::{
    store::{
        cmd_resp,
        fsm::{
            apply::{self, APPLY_WB_SHRINK_SIZE, SHRINK_PENDING_CMD_QUEUE_CAP},
            Proposal,
        },
        local_metrics::RaftMetrics,
        metrics::{APPLY_TASK_WAIT_TIME_HISTOGRAM, APPLY_TIME_HISTOGRAM},
        msg::ErrorCallback,
        util, Config, WriteCallback,
    },
    Error, Result,
};
use slog::{info, warn};
use tikv_util::{
    box_err,
    log::SlogFormat,
    slog_panic,
    time::{duration_to_sec, monotonic_raw_now, Instant},
};

use crate::{
    batch::StoreContext,
    fsm::{ApplyFsm, ApplyResReporter},
    raft::{Apply, Peer},
    router::{ApplyRes, ApplyTask, CmdResChannel, PeerTick},
};

mod admin;
mod control;
mod write;

pub use admin::{
    report_split_init_finish, temp_split_path, AdminCmdResult, CompactLogContext, RequestHalfSplit,
    RequestSplit, SplitFlowControl, SplitInit, SPLIT_PREFIX,
};
pub use control::ProposalControl;
pub use write::{
    SimpleWriteBinary, SimpleWriteEncoder, SimpleWriteReqDecoder, SimpleWriteReqEncoder,
};

use self::write::SimpleWrite;

fn parse_at<M: Message + Default>(logger: &slog::Logger, buf: &[u8], index: u64, term: u64) -> M {
    let mut m = M::default();
    match m.merge_from_bytes(buf) {
        Ok(()) => m,
        Err(e) => slog_panic!(
            logger,
            "data is corrupted";
            "term" => term,
            "index" => index,
            "error" => ?e,
        ),
    }
}

#[derive(Debug)]
pub struct CommittedEntries {
    /// Entries need to be applied. Note some entries may not be included for
    /// flow control.
    entry_and_proposals: Vec<(Entry, Vec<CmdResChannel>)>,
    committed_time: Instant,
}

fn new_response(header: &RaftRequestHeader) -> RaftCmdResponse {
    let mut resp = RaftCmdResponse::default();
    if !header.get_uuid().is_empty() {
        let uuid = header.get_uuid().to_vec();
        resp.mut_header().set_uuid(uuid);
    }
    resp
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    /// Schedule an apply fsm to apply logs in the background.
    ///
    /// Everytime a snapshot is applied or peer is just started, it will
    /// schedule a new apply fsm. The old fsm will stopped automatically
    /// when the old apply scheduler is dropped.
    #[inline]
    pub fn schedule_apply_fsm<T>(&mut self, store_ctx: &mut StoreContext<EK, ER, T>) {
        let region_state = self.storage().region_state().clone();
        let mailbox = match store_ctx.router.mailbox(self.region_id()) {
            Some(m) => m,
            None => {
                assert!(
                    store_ctx.shutdown.load(Ordering::Relaxed),
                    "failed to load mailbox: {}",
                    SlogFormat(&self.logger)
                );
                return;
            }
        };
        let logger = self.logger.clone();
        let read_scheduler = self.storage().read_scheduler();
        let (apply_scheduler, mut apply_fsm) = ApplyFsm::new(
            &store_ctx.cfg,
            self.peer().clone(),
            region_state,
            mailbox,
            store_ctx.tablet_registry.clone(),
            read_scheduler,
            self.flush_state().clone(),
            self.storage().apply_trace().log_recovery(),
            self.entry_storage().applied_term(),
            logger,
        );

        store_ctx
            .apply_pool
            .spawn(async move { apply_fsm.handle_all_tasks().await })
            .unwrap();
        self.set_apply_scheduler(apply_scheduler);
    }

    #[inline]
    fn validate_command(
        &self,
        header: &RaftRequestHeader,
        admin_type: Option<AdminCmdType>,
        metrics: &mut RaftMetrics,
    ) -> Result<()> {
        if let Err(e) = util::check_store_id(header, self.peer().get_store_id()) {
            metrics.invalid_proposal.mismatch_store_id.inc();
            return Err(e);
        }
        if let Err(e) = util::check_peer_id(header, self.peer().get_id()) {
            metrics.invalid_proposal.mismatch_peer_id.inc();
            return Err(e);
        }
        if !self.is_leader() {
            metrics.invalid_proposal.not_leader.inc();
            return Err(Error::NotLeader(self.region_id(), self.leader()));
        }
        if let Err(e) = util::check_term(header, self.term()) {
            metrics.invalid_proposal.stale_command.inc();
            return Err(e);
        }
        if let Err(mut e) = util::check_region_epoch(header, admin_type, self.region(), true) {
            if let Error::EpochNotMatch(_, _new_regions) = &mut e {
                // TODO: query sibling regions.
                metrics.invalid_proposal.epoch_not_match.inc();
            }
            return Err(e);
        }
        Ok(())
    }

    #[inline]
    fn propose<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        data: Vec<u8>,
    ) -> Result<u64> {
        self.propose_with_ctx(store_ctx, data, vec![])
    }

    #[inline]
    fn propose_with_ctx<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        data: Vec<u8>,
        proposal_ctx: Vec<u8>,
    ) -> Result<u64> {
        store_ctx.raft_metrics.propose.normal.inc();
        store_ctx
            .raft_metrics
            .propose_log_size
            .observe(data.len() as f64);
        if data.len() as u64 > store_ctx.cfg.raft_entry_max_size.0 {
            return Err(Error::RaftEntryTooLarge {
                region_id: self.region_id(),
                entry_size: data.len() as u64,
            });
        }
        let last_index = self.raft_group().raft.raft_log.last_index();
        self.raft_group_mut().propose(proposal_ctx, data)?;
        if self.raft_group().raft.raft_log.last_index() == last_index {
            // The message is dropped silently, this usually due to leader absence
            // or transferring leader. Both cases can be considered as NotLeader error.
            return Err(Error::NotLeader(self.region_id(), None));
        }
        Ok(last_index + 1)
    }

    #[inline]
    pub fn post_propose_command<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        res: Result<u64>,
        ch: Vec<CmdResChannel>,
        call_proposed_on_success: bool,
    ) {
        let idx = match res {
            Ok(i) => i,
            Err(e) => {
                ch.report_error(cmd_resp::err_resp(e, self.term()));
                return;
            }
        };
        let mut proposal = Proposal::new(idx, self.term(), ch);
        if call_proposed_on_success {
            proposal.cb.notify_proposed();
        }
        proposal.must_pass_epoch_check = self.applied_to_current_term();
        proposal.propose_time = Some(*ctx.current_time.get_or_insert_with(monotonic_raw_now));
        self.report_batch_wait_duration(ctx, &proposal.cb);
        self.proposals_mut().push(proposal);
        self.set_has_ready();
    }

    fn report_batch_wait_duration<T>(
        &self,
        ctx: &mut StoreContext<EK, ER, T>,
        ch: &Vec<CmdResChannel>,
    ) {
        if !ctx.raft_metrics.waterfall_metrics || ch.is_empty() {
            return;
        }
        let now = std::time::Instant::now();
        for c in ch {
            for tracker in c.write_trackers() {
                tracker.observe(now, &ctx.raft_metrics.wf_batch_wait, |t| {
                    &mut t.metrics.wf_batch_wait_nanos
                });
            }
        }
    }

    #[inline]
    pub fn schedule_apply_committed_entries<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        committed_entries: Vec<Entry>,
    ) {
        if committed_entries.is_empty() {
            return;
        }
        let current_term = self.term();
        let mut entry_and_proposals = vec![];
        let queue = self.proposals_mut();
        if !queue.is_empty() {
            for e in committed_entries {
                let mut proposal = queue.find_proposal(e.term, e.index, current_term);
                if let Some(p) = &mut proposal && p.must_pass_epoch_check {
                    // In this case the apply can be guaranteed to be successful. Invoke the
                    // on_committed callback if necessary.
                    p.cb.notify_committed();
                }
                entry_and_proposals.push((e, proposal.map_or_else(Vec::new, |p| p.cb)));
            }
        } else {
            entry_and_proposals = committed_entries.into_iter().map(|e| (e, vec![])).collect();
        }
        self.report_store_time_duration(ctx, &mut entry_and_proposals);
        // Unlike v1, v2 doesn't need to persist commit index and commit term. The
        // point of persist commit index/term of raft apply state is to recover commit
        // index when the writes to raft engine is lost but writes to kv engine is
        // persisted. But in v2, writes to raft engine must be persisted before
        // memtables in kv engine is flushed.
        let apply = CommittedEntries {
            entry_and_proposals,
            committed_time: Instant::now(),
        };
        assert!(
            self.apply_scheduler().is_some(),
            "apply_scheduler should be something. region_id {}",
            self.region_id()
        );
        self.apply_scheduler()
            .unwrap()
            .send(ApplyTask::CommittedEntries(apply));
    }

    #[inline]
    fn report_store_time_duration<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        entry_and_proposals: &mut [(Entry, Vec<CmdResChannel>)],
    ) {
        let now = std::time::Instant::now();
        for (_, chs) in entry_and_proposals {
            for tracker in chs.write_trackers_mut() {
                tracker.observe(now, &ctx.raft_metrics.store_time, |t| {
                    t.metrics.write_instant = Some(now);
                    &mut t.metrics.store_time_nanos
                });
                tracker.reset(now);
            }
        }
    }

    pub fn on_apply_res<T>(&mut self, ctx: &mut StoreContext<EK, ER, T>, apply_res: ApplyRes) {
        if !self.serving() {
            return;
        }
        // TODO: remove following log once stable.
        info!(self.logger, "on_apply_res"; "apply_res" => ?apply_res, "apply_trace" => ?self.storage().apply_trace());
        // It must just applied a snapshot.
        if apply_res.applied_index < self.entry_storage().first_index() {
            // Ignore admin command side effects, otherwise it may split incomplete
            // region.
            return;
        }

        for admin_res in Vec::from(apply_res.admin_result) {
            match admin_res {
                AdminCmdResult::None => unreachable!(),
                AdminCmdResult::ConfChange(conf_change) => {
                    self.on_apply_res_conf_change(ctx, conf_change)
                }
                AdminCmdResult::SplitRegion(res) => {
                    self.storage_mut()
                        .apply_trace_mut()
                        .on_admin_modify(res.tablet_index);
                    self.on_apply_res_split(ctx, res)
                }
                AdminCmdResult::TransferLeader(term) => self.on_transfer_leader(ctx, term),
                AdminCmdResult::CompactLog(res) => self.on_apply_res_compact_log(ctx, res),
                AdminCmdResult::UpdateGcPeers(state) => self.on_apply_res_update_gc_peers(state),
            }
        }

        self.update_split_flow_control(&apply_res.metrics);
        self.update_stat(&apply_res.metrics);

        self.raft_group_mut()
            .advance_apply_to(apply_res.applied_index);
        self.proposal_control_advance_apply(apply_res.applied_index);
        let is_leader = self.is_leader();
        let progress_to_be_updated = self.entry_storage().applied_term() != apply_res.applied_term;
        let entry_storage = self.entry_storage_mut();
        entry_storage
            .apply_state_mut()
            .set_applied_index(apply_res.applied_index);
        entry_storage.set_applied_term(apply_res.applied_term);
        if !is_leader {
            entry_storage.compact_entry_cache(apply_res.applied_index + 1);
        }
        self.on_data_modified(apply_res.modifications);
        self.handle_read_on_apply(
            ctx,
            apply_res.applied_term,
            apply_res.applied_index,
            progress_to_be_updated,
        );
        if self.pause_for_recovery()
            && self.storage().entry_storage().commit_index() <= apply_res.applied_index
        {
            info!(self.logger, "recovery completed"; "apply_index" => apply_res.applied_index);
            self.set_pause_for_recovery(false);
            // Flush to avoid recover again and again.
            if let Some(scheduler) = self.apply_scheduler() {
                scheduler.send(ApplyTask::ManualFlush);
            }
            self.add_pending_tick(PeerTick::Raft);
        }
        if !self.pause_for_recovery() && self.storage_mut().apply_trace_mut().should_flush() {
            if let Some(scheduler) = self.apply_scheduler() {
                scheduler.send(ApplyTask::ManualFlush);
            }
        }
        let last_applying_index = self.compact_log_context().last_applying_index();
        let committed_index = self.entry_storage().commit_index();
        if last_applying_index < committed_index {
            // We need to continue to apply after previous page is finished.
            self.set_has_ready();
        }
    }
}

#[derive(Debug)]
pub struct ApplyFlowControl {
    timer: Instant,
    last_check_keys: u64,
    need_flush: bool,
    yield_time: Duration,
    yield_written_bytes: u64,
}

impl ApplyFlowControl {
    pub fn new(cfg: &Config) -> Self {
        ApplyFlowControl {
            timer: Instant::now_coarse(),
            last_check_keys: 0,
            need_flush: false,
            yield_time: cfg.apply_yield_duration.0,
            yield_written_bytes: cfg.apply_yield_write_size.0,
        }
    }

    #[cfg(test)]
    pub fn set_need_flush(&mut self, need_flush: bool) {
        self.need_flush = need_flush;
    }
}

impl<EK: KvEngine, R> Apply<EK, R> {
    #[inline]
    pub fn on_start_apply(&mut self) {
        self.apply_flow_control_mut().timer = Instant::now_coarse();
    }

    #[inline]
    fn should_skip(&self, off: usize, index: u64) -> bool {
        let log_recovery = self.log_recovery();
        if log_recovery.is_none() {
            return false;
        }
        log_recovery.as_ref().unwrap()[off] >= index
    }
}

impl<EK: KvEngine, R: ApplyResReporter> Apply<EK, R> {
    pub fn apply_unsafe_write(&mut self, data: Box<[u8]>) {
        let decoder = match SimpleWriteReqDecoder::new(&self.logger, &data, u64::MAX, u64::MAX) {
            Ok(decoder) => decoder,
            Err(req) => unreachable!("unexpected request: {:?}", req),
        };
        for req in decoder {
            match req {
                SimpleWrite::Put(put) => {
                    let _ = self.apply_put(put.cf, u64::MAX, put.key, put.value);
                }
                SimpleWrite::Delete(delete) => {
                    let _ = self.apply_delete(delete.cf, u64::MAX, delete.key);
                }
                SimpleWrite::DeleteRange(dr) => {
                    let _ = self.apply_delete_range(
                        dr.cf,
                        u64::MAX,
                        dr.start_key,
                        dr.end_key,
                        dr.notify_only,
                    );
                }
            }
        }
        self.apply_flow_control_mut().need_flush = true;
    }

    pub async fn on_manual_flush(&mut self) {
        let written_bytes = self.flush();
        if let Err(e) = self.tablet().flush_cfs(&[], false) {
            warn!(self.logger, "failed to flush: {:?}", e);
        }
        self.maybe_reschedule(written_bytes).await
    }

    #[inline]
    pub async fn apply_committed_entries(&mut self, ce: CommittedEntries) {
        fail::fail_point!("APPLY_COMMITTED_ENTRIES");
        APPLY_TASK_WAIT_TIME_HISTOGRAM
            .observe(duration_to_sec(ce.committed_time.saturating_elapsed()));
        for (e, ch) in ce.entry_and_proposals {
            if self.tombstone() {
                apply::notify_req_region_removed(self.region_state().get_region().get_id(), ch);
                continue;
            }
            if !e.get_data().is_empty() {
                let mut set_save_point = false;
                if let Some(wb) = &mut self.write_batch {
                    wb.set_save_point();
                    set_save_point = true;
                }
                let resp = match self.apply_entry(&e).await {
                    Ok(resp) => resp,
                    Err(e) => {
                        if let Some(wb) = &mut self.write_batch {
                            if set_save_point {
                                wb.rollback_to_save_point().unwrap();
                            } else {
                                wb.clear();
                            }
                        }
                        cmd_resp::new_error(e)
                    }
                };
                self.callbacks_mut().push((ch, resp));
            } else {
                assert!(ch.is_empty());
            }
            // Flush may be triggerred in the middle, so always update the index and term.
            self.set_apply_progress(e.index, e.term);
            self.apply_flow_control_mut().need_flush = true;
        }
    }

    #[inline]
    async fn apply_entry(&mut self, entry: &Entry) -> Result<RaftCmdResponse> {
        let mut conf_change = None;
        let log_index = entry.get_index();
        let req = match entry.get_entry_type() {
            EntryType::EntryNormal => match SimpleWriteReqDecoder::new(
                &self.logger,
                entry.get_data(),
                log_index,
                entry.get_term(),
            ) {
                Ok(decoder) => {
                    util::compare_region_epoch(
                        decoder.header().get_region_epoch(),
                        self.region_state().get_region(),
                        false,
                        true,
                        true,
                    )?;
                    let res = Ok(new_response(decoder.header()));
                    for req in decoder {
                        match req {
                            SimpleWrite::Put(put) => {
                                self.apply_put(put.cf, log_index, put.key, put.value)?;
                            }
                            SimpleWrite::Delete(delete) => {
                                self.apply_delete(delete.cf, log_index, delete.key)?;
                            }
                            SimpleWrite::DeleteRange(dr) => {
                                self.apply_delete_range(
                                    dr.cf,
                                    log_index,
                                    dr.start_key,
                                    dr.end_key,
                                    dr.notify_only,
                                )?;
                            }
                        }
                    }
                    return res;
                }
                Err(req) => req,
            },
            EntryType::EntryConfChange => {
                let cc: ConfChange =
                    parse_at(&self.logger, entry.get_data(), log_index, entry.get_term());
                let req: RaftCmdRequest =
                    parse_at(&self.logger, cc.get_context(), log_index, entry.get_term());
                conf_change = Some(cc.into_v2());
                req
            }
            EntryType::EntryConfChangeV2 => {
                let cc: ConfChangeV2 =
                    parse_at(&self.logger, entry.get_data(), log_index, entry.get_term());
                let req: RaftCmdRequest =
                    parse_at(&self.logger, cc.get_context(), log_index, entry.get_term());
                conf_change = Some(cc);
                req
            }
        };

        util::check_req_region_epoch(&req, self.region_state().get_region(), true)?;
        if req.has_admin_request() {
            let admin_req = req.get_admin_request();
            let (admin_resp, admin_result) = match req.get_admin_request().get_cmd_type() {
                AdminCmdType::CompactLog => self.apply_compact_log(admin_req, entry.index)?,
                AdminCmdType::Split => self.apply_split(admin_req, log_index)?,
                AdminCmdType::BatchSplit => self.apply_batch_split(admin_req, log_index)?,
                AdminCmdType::PrepareMerge => unimplemented!(),
                AdminCmdType::CommitMerge => unimplemented!(),
                AdminCmdType::RollbackMerge => unimplemented!(),
                AdminCmdType::TransferLeader => {
                    self.apply_transfer_leader(admin_req, entry.term)?
                }
                AdminCmdType::ChangePeer => {
                    self.apply_conf_change(log_index, admin_req, conf_change.unwrap())?
                }
                AdminCmdType::ChangePeerV2 => {
                    self.apply_conf_change_v2(log_index, admin_req, conf_change.unwrap())?
                }
                AdminCmdType::ComputeHash => unimplemented!(),
                AdminCmdType::VerifyHash => unimplemented!(),
                AdminCmdType::PrepareFlashback => unimplemented!(),
                AdminCmdType::FinishFlashback => unimplemented!(),
                AdminCmdType::BatchSwitchWitness => unimplemented!(),
                AdminCmdType::UpdateGcPeer => self.apply_update_gc_peer(log_index, admin_req),
                AdminCmdType::InvalidAdmin => {
                    return Err(box_err!("invalid admin command type"));
                }
            };

            match admin_result {
                AdminCmdResult::None => (),
                _ => self.push_admin_result(admin_result),
            }
            let mut resp = new_response(req.get_header());
            resp.set_admin_response(admin_resp);
            Ok(resp)
        } else {
            for r in req.get_requests() {
                match r.get_cmd_type() {
                    // These three writes should all use the new codec. Keep them here for
                    // backward compatibility.
                    CmdType::Put => {
                        let put = r.get_put();
                        self.apply_put(put.get_cf(), log_index, put.get_key(), put.get_value())?;
                    }
                    CmdType::Delete => {
                        let delete = r.get_delete();
                        self.apply_delete(delete.get_cf(), log_index, delete.get_key())?;
                    }
                    CmdType::DeleteRange => {
                        let dr = r.get_delete_range();
                        self.apply_delete_range(
                            dr.get_cf(),
                            log_index,
                            dr.get_start_key(),
                            dr.get_end_key(),
                            dr.get_notify_only(),
                        )?;
                    }
                    _ => unimplemented!(),
                }
            }
            Ok(new_response(req.get_header()))
        }
    }

    fn should_reschedule(&self, written_bytes: u64) -> bool {
        let control = self.apply_flow_control();
        written_bytes >= control.yield_written_bytes
            || control.timer.saturating_elapsed() >= control.yield_time
    }

    pub async fn maybe_reschedule(&mut self, written_bytes: u64) {
        if self.should_reschedule(written_bytes) {
            yatp::task::future::reschedule().await;
            self.apply_flow_control_mut().timer = Instant::now_coarse();
        }
    }

    /// Check whether it needs to flush.
    ///
    /// We always batch as much inputs as possible, flush will only be triggered
    /// when it has been processing too long.
    pub async fn maybe_flush(&mut self) {
        let buffer_keys = self.metrics.written_keys;
        let control = self.apply_flow_control_mut();
        if buffer_keys >= control.last_check_keys + 128 {
            // Reschedule by write size was designed to avoid too many deletes impacts
            // performance so it doesn't need pricise control. If checking bytes here may
            // make the batch too small and hurt performance.
            if self.should_reschedule(0) {
                let written_bytes = self.flush();
                self.maybe_reschedule(written_bytes).await;
            } else {
                self.apply_flow_control_mut().last_check_keys = self.metrics.written_keys;
            }
        }
    }

    #[inline]
    pub fn flush(&mut self) -> u64 {
        // TODO: maybe we should check whether there is anything to flush.
        let (index, term) = self.apply_progress();
        let control = self.apply_flow_control_mut();
        control.last_check_keys = 0;
        if !control.need_flush {
            return 0;
        }
        control.need_flush = false;
        let flush_state = self.flush_state().clone();
        if let Some(wb) = &self.write_batch && !wb.is_empty() {
            self.perf_context().start_observe();
            let mut write_opt = WriteOptions::default();
            write_opt.set_disable_wal(true);
            let wb = self.write_batch.as_mut().unwrap();
            if let Err(e) = wb.write_callback_opt(&write_opt, || {
                flush_state.set_applied_index(index);
            }) {
                slog_panic!(self.logger, "failed to write data"; "error" => ?e);
            }
            self.metrics.written_bytes += wb.data_size() as u64;
            self.metrics.written_keys += wb.count() as u64;
            if wb.data_size() <= APPLY_WB_SHRINK_SIZE {
                wb.clear();
            } else {
                self.write_batch.take();
            }
            let tokens: Vec<_> = self
                .callbacks_mut()
                .iter()
                .flat_map(|(v, _)| {
                    v.write_trackers()
                        .flat_map(|t| t.as_tracker_token())
                })
                .collect();
            self.perf_context().report_metrics(&tokens);
        }
        let mut apply_res = ApplyRes::default();
        apply_res.applied_index = index;
        apply_res.applied_term = term;
        apply_res.admin_result = self.take_admin_result().into_boxed_slice();
        apply_res.modifications = *self.modifications_mut();
        apply_res.metrics = mem::take(&mut self.metrics);
        let written_bytes = apply_res.metrics.written_bytes;
        self.res_reporter().report(apply_res);

        // Report result first and then invoking callbacks. This may delays callback a
        // little bit, but can make sure all following messages must see the side
        // effect of admin commands.
        let callbacks = self.callbacks_mut();
        let now = std::time::Instant::now();
        let apply_time = APPLY_TIME_HISTOGRAM.local();
        for (ch, resp) in callbacks.drain(..) {
            for tracker in ch.write_trackers() {
                tracker.observe(now, &apply_time, |t| &mut t.metrics.apply_time_nanos);
            }
            ch.set_result(resp);
        }
        apply_time.flush();
        if callbacks.capacity() > SHRINK_PENDING_CMD_QUEUE_CAP {
            callbacks.shrink_to(SHRINK_PENDING_CMD_QUEUE_CAP);
        }
        written_bytes
    }
}
