// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

//! The handling of `PrepareMerge` command.
//!
//! ## Propose (`Peer::propose_prepare_merge`)
//!
//! Checks for these requirements:
//!
//! - Validate the request. (`Peer::validate_prepare_merge_command`)
//! - Log gap between source region leader and peers is not too large. This is
//!   because these logs need to be embeded in the later `CommitMerge` command.
//! - Logs that aren't fully committed (to all peers) does not contains
//!   `CompactLog` or certain admin commands.
//!
//! Then, transfer all in-memory pessimistic locks to the target region as a
//! Raft proposal. To guarantee the consistency of lock serialization, we might
//! need to wait for some in-flight logs to be applied. During the wait, all
//! incoming write proposals will be rejected. Read the comments of
//! `PrepareStatus::WaitForFence` for more details.
//!
//! ## Apply (`Apply::apply_prepare_merge`)
//!
//! Increase region epoch and write the merge state.
//!
//! ## On Apply Result (`Peer::on_apply_res_prepare_merge`)
//!
//! Start the tick (`Peer::on_check_merge`) to periodically check the
//! eligibility of merge.

use std::{mem, time::Duration};

use collections::HashMap;
use engine_traits::{Checkpointer, KvEngine, RaftEngine, RaftLogBatch, CF_LOCK};
use futures::channel::oneshot;
use kvproto::{
    metapb::RegionEpoch,
    raft_cmdpb::{
        AdminCmdType, AdminRequest, AdminResponse, CmdType, PrepareMergeRequest, PutRequest,
        RaftCmdRequest, Request,
    },
    raft_serverpb::{
        ExtraMessage, ExtraMessageType, MergeState, PeerState, RaftMessage, RegionLocalState,
    },
};
use parking_lot::RwLockUpgradableReadGuard;
use protobuf::Message;
use raft::{eraftpb::EntryType, GetEntriesContext, NO_LIMIT};
use raftstore::{
    coprocessor::RegionChangeReason,
    store::{metrics::PEER_ADMIN_CMD_COUNTER, util, LocksStatus, ProposalContext, Transport},
    Error, Result,
};
use slog::{debug, error, info};
use tikv_util::{
    box_err, log::SlogFormat, slog_panic, store::region_on_same_stores, time::Instant,
};
use txn_types::WriteBatchFlags;

use super::{merge_source_path, CatchUpLogs};
use crate::{
    batch::StoreContext,
    fsm::ApplyResReporter,
    operation::{command::parse_at, AdminCmdResult, SimpleWriteReqDecoder},
    raft::{Apply, Peer},
    router::{CmdResChannel, PeerMsg, RaftRequest},
};

const TRIM_CHECK_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
pub struct PreProposeContext {
    pub min_matched: u64,
    lock_size_limit: usize,
}

/// FSM Graph (forward):
///
///  +-------+------------------+
///  |       |                  v
/// None -> (1) -> (2) ------> (4) -> None(destroyed)
///  |       |      |           ^
///  +-------+------+--> (3) ---+
///
/// - None->1: `start_check_trim_status` / `already_checked_trim_status`
/// - 1->2: `check_pessimistic_locks`
/// - *->3: `on_catch_up_logs`
/// - *->4: `on_apply_res_prepare_merge` / `Peer::new`
/// - 4->None: `on_ack_commit_merge`
///
/// Additional backward paths to None:
///
/// - 1->None: `maybe_clean_up_stale_merge_context` /
///   `merge_on_availability_response`
/// - 1/2->None: `update_merge_progress_on_became_follower` /
///   `propose_prepare_merge`
/// - *->None: `rollback_merge`
pub enum PrepareStatus {
    /// (1)
    WaitForTrimStatus {
        start_time: Instant,
        // Peers that we are not sure if trimmed.
        pending_peers: HashMap<u64, RegionEpoch>,
        // Only when all peers are trimmed, this proposal will be taken into the tablet flush
        // callback.
        req: Option<RaftCmdRequest>,
    },
    /// (2)
    /// When a fence <idx, cmd> is present, we (1) delay the PrepareMerge
    /// command `cmd` until all writes before `idx` are applied (2) reject all
    /// in-coming write proposals.
    /// Before proposing `PrepareMerge`, we first serialize and propose the lock
    /// table. Locks marked as deleted (but not removed yet) will be
    /// serialized as normal locks.
    /// Thanks to the fence, we can ensure at the time of lock transfer, locks
    /// are either removed (when applying logs) or won't be removed before
    /// merge (the proposals to remove them are rejected).
    ///
    /// The request can be `None` because we needs to take it out to redo the
    /// propose. In the meantime the fence is needed to bypass the check.
    WaitForFence {
        fence: u64,
        ctx: PreProposeContext,
        req: Option<RaftCmdRequest>,
    },
    /// (3)
    /// Catch up logs after source has committed `PrepareMerge` and target has
    /// committed `CommitMerge`.
    CatchUpLogs(CatchUpLogs),
    /// (4)
    /// In this state, all write proposals except for `RollbackMerge` will be
    /// rejected.
    Applied(MergeState),
}

impl std::fmt::Debug for PrepareStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            Self::WaitForTrimStatus {
                start_time,
                pending_peers,
                req,
            } => f
                .debug_struct("PrepareStatus::WaitForTrimStatus")
                .field("start_time", start_time)
                .field("pending_peers", pending_peers)
                .field("req", req)
                .finish(),
            Self::WaitForFence { fence, ctx, req } => f
                .debug_struct("PrepareStatus::WaitForFence")
                .field("fence", fence)
                .field("ctx", ctx)
                .field("req", req)
                .finish(),
            Self::CatchUpLogs(cul) => cul.fmt(f),
            Self::Applied(state) => f
                .debug_struct("PrepareStatus::Applied")
                .field("state", state)
                .finish(),
        }
    }
}

#[derive(Debug)]
pub struct PrepareMergeResult {
    region_state: RegionLocalState,
    state: MergeState,
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub fn propose_prepare_merge<T: Transport>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        mut req: RaftCmdRequest,
    ) -> Result<u64> {
        self.validate_prepare_merge_command(
            store_ctx,
            req.get_admin_request().get_prepare_merge(),
        )?;
        // We need to check three things in order:
        // (1) `start_check_trim_status`
        // (2) `check_logs_before_prepare_merge`
        // (3) `check_pessimistic_locks`
        // Check 1 and 3 are async, they yield by returning
        // `Error::PendingPrepareMerge`.
        //
        // Error handling notes:
        // - `WaitForTrimStatus` can only become `WaitForFence` when both check 2&3
        // succeed. We need to clean up the status if any of them failed, otherwise we
        // still can't serve other merge requests (not until status is cleaned up in
        // `maybe_clean_up_stale_merge_context`).
        // - `WaitForFence` will always reach proposing phase when committed logs are
        // applied. No intermediate error.
        let pre_propose = if let Some(r) = self.already_checked_pessimistic_locks()? {
            r
        } else if self.already_checked_trim_status(&req)? {
            self.check_logs_before_prepare_merge(store_ctx)
                .and_then(|r| self.check_pessimistic_locks(r, &mut req))
                .map_err(|e| {
                    if !matches!(e, Error::PendingPrepareMerge) {
                        info!(self.logger, "fail to advance to `WaitForFence`"; "err" => ?e);
                        self.take_merge_context();
                    }
                    e
                })?
        } else {
            return self.start_check_trim_status(store_ctx, &mut req);
        };
        req.mut_admin_request()
            .mut_prepare_merge()
            .set_min_index(pre_propose.min_matched + 1);
        let r = self
            .propose_locks_before_prepare_merge(store_ctx, pre_propose.lock_size_limit)
            .and_then(|_| {
                let mut proposal_ctx = ProposalContext::empty();
                proposal_ctx.insert(ProposalContext::PREPARE_MERGE);
                let data = req.write_to_bytes().unwrap();
                self.propose_with_ctx(store_ctx, data, proposal_ctx)
            });
        if r.is_ok() {
            self.proposal_control_mut().set_pending_prepare_merge(false);
        } else {
            self.post_prepare_merge_fail();
        }
        r
    }

    /// Match v1::check_merge_proposal.
    /// - Target region epoch as requested is identical with the local version.
    /// - Target region is a sibling to the source region.
    /// - Peers of both source and target region are aligned, i.e. located on
    ///   the same set of stores.
    fn validate_prepare_merge_command<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        req: &PrepareMergeRequest,
    ) -> Result<()> {
        // Just for simplicity, do not start region merge while in joint state
        if self.in_joint_state() {
            return Err(box_err!(
                "{} region in joint state, can not propose merge command, command: {:?}",
                SlogFormat(&self.logger),
                req
            ));
        }
        let region = self.region();
        let target_region = req.get_target();
        {
            let store_meta = store_ctx.store_meta.lock().unwrap();
            match store_meta.regions.get(&target_region.get_id()) {
                Some((region, _)) if *region != *target_region => {
                    return Err(box_err!(
                        "target region not matched, skip proposing: {:?} != {:?}",
                        region,
                        target_region
                    ));
                }
                None => {
                    return Err(box_err!(
                        "target region {} doesn't exist.",
                        target_region.get_id()
                    ));
                }
                _ => {}
            }
        }

        if !util::is_sibling_regions(target_region, region) {
            return Err(box_err!(
                "{:?} and {:?} are not sibling, skip proposing.",
                target_region,
                region
            ));
        }
        if !region_on_same_stores(target_region, region) {
            return Err(box_err!(
                "peers doesn't match {:?} != {:?}, reject merge",
                region.get_peers(),
                target_region.get_peers()
            ));
        }
        Ok(())
    }

    // Match v1::pre_propose_prepare_merge.
    fn check_logs_before_prepare_merge<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
    ) -> Result<PreProposeContext> {
        let last_index = self.raft_group().raft.raft_log.last_index();
        let (min_matched, min_committed) = self.calculate_min_progress()?;
        if min_matched == 0
            || min_committed == 0
            || last_index - min_matched > store_ctx.cfg.merge_max_log_gap
            || last_index - min_committed > store_ctx.cfg.merge_max_log_gap * 2
            || min_matched < self.last_sent_snapshot_index()
        {
            return Err(box_err!(
                "log gap too large, skip merge: matched: {}, committed: {}, last index: {}",
                min_matched,
                min_committed,
                last_index
            ));
        }
        let mut entry_size = 0;
        for entry in self.raft_group().raft.raft_log.entries(
            min_committed + 1,
            NO_LIMIT,
            GetEntriesContext::empty(false),
        )? {
            // commit merge only contains entries start from min_matched + 1
            if entry.index > min_matched {
                entry_size += entry.get_data().len();
            }
            if entry.get_entry_type() == EntryType::EntryConfChange
                || entry.get_entry_type() == EntryType::EntryConfChangeV2
            {
                return Err(box_err!(
                    "{} log gap contains conf change, skip merging.",
                    "tag"
                ));
            }
            if entry.get_data().is_empty() {
                continue;
            }
            let Err(cmd) = SimpleWriteReqDecoder::new(
                |buf, index, term| parse_at(&self.logger, buf, index, term),
                &self.logger,
                entry.get_data(),
                entry.get_index(),
                entry.get_term(),
            ) else { continue };
            let cmd_type = cmd.get_admin_request().get_cmd_type();
            match cmd_type {
                AdminCmdType::TransferLeader
                | AdminCmdType::ComputeHash
                | AdminCmdType::VerifyHash
                | AdminCmdType::InvalidAdmin => continue,
                _ => {}
            }
            // Any command that can change epoch or log gap should be rejected.
            return Err(box_err!(
                "log gap contains admin request {:?}, skip merging.",
                cmd_type
            ));
        }
        let entry_size_limit = store_ctx.cfg.raft_entry_max_size.0 as usize * 9 / 10;
        if entry_size > entry_size_limit {
            return Err(box_err!(
                "log gap size exceed entry size limit, skip merging."
            ));
        };
        Ok(PreProposeContext {
            min_matched,
            lock_size_limit: entry_size_limit - entry_size,
        })
    }

    fn start_check_trim_status<T: Transport>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        req: &mut RaftCmdRequest,
    ) -> Result<u64> {
        if self.storage().has_dirty_data() {
            return Err(box_err!(
                "source peer {} not trimmed, skip merging.",
                self.peer_id()
            ));
        }
        let target = req.get_admin_request().get_prepare_merge().get_target();
        let mut pending_peers = HashMap::default();
        for region in [self.region(), target] {
            for p in region.get_peers() {
                if p.get_id() == self.peer_id() {
                    continue;
                }
                let mut msg = RaftMessage::default();
                msg.set_region_id(region.get_id());
                msg.set_from_peer(self.peer().clone());
                msg.set_to_peer(p.clone());
                msg.set_region_epoch(region.get_region_epoch().clone());
                msg.mut_extra_msg()
                    .set_type(ExtraMessageType::MsgAvailabilityRequest);
                msg.mut_extra_msg()
                    .mut_availability_context()
                    .set_from_region_id(self.region_id());
                store_ctx.trans.send(msg)?;
                pending_peers.insert(p.get_id(), region.get_region_epoch().clone());
            }
        }

        // Shouldn't enter this call if trim check is already underway.
        let status = &mut self.merge_context_mut().prepare_status;
        if status.is_some() {
            let logger = self.logger.clone();
            panic!(
                "expect empty prepare merge status {}: {:?}",
                SlogFormat(&logger),
                self.merge_context_mut().prepare_status
            );
        }
        *status = Some(PrepareStatus::WaitForTrimStatus {
            start_time: Instant::now_coarse(),
            pending_peers,
            req: Some(mem::take(req)),
        });
        Err(Error::PendingPrepareMerge)
    }

    pub fn merge_on_availability_response<T: Transport>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        from_peer: u64,
        resp: &ExtraMessage,
    ) {
        let region_id = self.region_id();
        if self.merge_context().is_some()
            && let Some(PrepareStatus::WaitForTrimStatus { pending_peers, req, .. }) = self
                .merge_context_mut()
                .prepare_status
                .as_mut()
            && req.is_some()
        {
            assert!(resp.has_availability_context());
            let from_region = resp.get_availability_context().get_from_region_id();
            let from_epoch = resp.get_availability_context().get_from_region_epoch();
            let trimmed = resp.get_availability_context().get_trimmed();
            if let Some(epoch) = pending_peers.get(&from_peer)
                && util::is_region_epoch_equal(from_epoch, epoch)
            {
                if !trimmed {
                    info!(
                        self.logger,
                        "cancel merge because source peer is not trimmed";
                        "region_id" => from_region,
                        "peer_id" => from_peer,
                    );
                    self.take_merge_context();
                    return;
                } else {
                    pending_peers.remove(&from_peer);
                }
            }
            if pending_peers.is_empty() {
                let mailbox = match store_ctx.router.mailbox(region_id) {
                    Some(mailbox) => mailbox,
                    None => {
                        assert!(
                            store_ctx.router.is_shutdown(),
                            "{} router should have been closed",
                            SlogFormat(&self.logger)
                        );
                        return;
                    }
                };
                let mut req = req.take().unwrap();
                req.mut_header().set_flags(WriteBatchFlags::PRE_FLUSH_FINISHED.bits());
                let logger = self.logger.clone();
                let on_flush_finish = move || {
                    let (ch, _) = CmdResChannel::pair();
                    if let Err(e) = mailbox.force_send(PeerMsg::AdminCommand(RaftRequest::new(req, ch))) {
                        error!(
                            logger,
                            "send PrepareMerge request failed after pre-flush finished";
                            "err" => ?e,
                        );
                        // We rely on `maybe_clean_up_stale_merge_context` to clean this up.
                    }
                };
                self.start_pre_flush(
                    store_ctx,
                    "prepare_merge",
                    false,
                    &self.region().clone(),
                    Box::new(on_flush_finish),
                );
            }
        }
    }

    fn already_checked_trim_status(&mut self, req: &RaftCmdRequest) -> Result<bool> {
        let flushed = WriteBatchFlags::from_bits_truncate(req.get_header().get_flags())
            .contains(WriteBatchFlags::PRE_FLUSH_FINISHED);
        match self
            .merge_context()
            .as_ref()
            .and_then(|c| c.prepare_status.as_ref())
        {
            Some(PrepareStatus::WaitForTrimStatus { pending_peers, .. }) => {
                // We should wait for the request sent from flush callback.
                if pending_peers.is_empty() && flushed {
                    Ok(true)
                } else {
                    Err(Error::PendingPrepareMerge)
                }
            }
            None => {
                // Pre-flush can only be triggered when the request has checked trim status.
                if flushed {
                    self.merge_context_mut().prepare_status =
                        Some(PrepareStatus::WaitForTrimStatus {
                            start_time: Instant::now_coarse(),
                            pending_peers: HashMap::default(),
                            req: None,
                        });
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            // Shouldn't reach here after calling `already_checked_pessimistic_locks` first.
            _ => unreachable!(),
        }
    }

    fn check_pessimistic_locks(
        &mut self,
        ctx: PreProposeContext,
        req: &mut RaftCmdRequest,
    ) -> Result<PreProposeContext> {
        let has_locks = {
            let pessimistic_locks = self.txn_context().ext().pessimistic_locks.read();
            if pessimistic_locks.status != LocksStatus::Normal {
                // If `status` is not `Normal`, it means the in-memory pessimistic locks are
                // being transferred, probably triggered by transferring leader. In this case,
                // we abort merging to simplify the situation.
                return Err(box_err!(
                    "pessimistic locks status is {:?}, skip merging.",
                    pessimistic_locks.status
                ));
            }
            !pessimistic_locks.is_empty()
        };
        let last_index = self.raft_group().raft.raft_log.last_index();
        if has_locks && self.entry_storage().applied_index() < last_index {
            let status = &mut self.merge_context_mut().prepare_status;
            if !matches!(status, Some(PrepareStatus::WaitForTrimStatus { .. })) {
                let logger = self.logger.clone();
                panic!(
                    "expect WaitForTrimStatus {}: {:?}",
                    SlogFormat(&logger),
                    self.merge_context_mut().prepare_status
                );
            }
            *status = Some(PrepareStatus::WaitForFence {
                fence: last_index,
                ctx,
                req: Some(mem::take(req)),
            });
            self.proposal_control_mut().set_pending_prepare_merge(true);
            info!(
                self.logger,
                "start rejecting new proposals before prepare merge";
                "prepare_merge_fence" => last_index
            );
            return Err(Error::PendingPrepareMerge);
        }
        Ok(ctx)
    }

    fn already_checked_pessimistic_locks(&mut self) -> Result<Option<PreProposeContext>> {
        let applied_index = self.entry_storage().applied_index();
        match self
            .merge_context()
            .as_ref()
            .and_then(|c| c.prepare_status.as_ref())
        {
            Some(PrepareStatus::WaitForTrimStatus { .. }) | None => Ok(None),
            Some(PrepareStatus::WaitForFence { fence, ctx, .. }) => {
                if applied_index < *fence {
                    info!(
                        self.logger,
                        "suspend PrepareMerge because applied_index has not reached prepare_merge_fence";
                        "applied_index" => applied_index,
                        "prepare_merge_fence" => fence,
                    );
                    Err(Error::PendingPrepareMerge)
                } else {
                    Ok(Some(ctx.clone()))
                }
            }
            Some(PrepareStatus::CatchUpLogs(cul)) => {
                Err(box_err!("catch up logs is in-progress: {:?}.", cul))
            }
            Some(PrepareStatus::Applied(state)) => Err(box_err!(
                "another merge is in-progress, merge_state: {:?}.",
                state
            )),
        }
    }

    // Free up memory if trim check failed.
    #[inline]
    pub fn maybe_clean_up_stale_merge_context(&mut self) {
        // Check if there's a stale trim check. Ideally this should be implemented as a
        // tick. But this is simpler.
        // We do not check `req.is_some()` here. When req is taken for propose in flush
        // callback, it will either trigger a state transition to `WaitForFence` or
        // abort. If we see the state here, it means the req never made it to
        // `propose_prepare_merge`.
        // If the req is still inflight and reaches `propose_prepare_merge` later,
        // `already_checked_trim_status` will restore the status.
        if let Some(PrepareStatus::WaitForTrimStatus {
            start_time, ..
        }) = self
            .merge_context()
            .as_ref()
            .and_then(|c| c.prepare_status.as_ref())
            && start_time.saturating_elapsed() > TRIM_CHECK_TIMEOUT
        {
            info!(self.logger, "cancel merge because trim check timed out");
            self.take_merge_context();
        }
    }

    /// Called after some new entries have been applied and the fence can
    /// probably be lifted.
    pub fn retry_pending_prepare_merge<T: Transport>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        applied_index: u64,
    ) {
        if self.merge_context().is_none() {
            return;
        }
        // Check the fence.
        if let Some(req) = self
            .merge_context_mut()
            .maybe_take_pending_prepare(applied_index)
        {
            let (ch, _) = CmdResChannel::pair();
            self.on_admin_command(store_ctx, req, ch);
        }
    }

    fn propose_locks_before_prepare_merge<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        size_limit: usize,
    ) -> Result<()> {
        let pessimistic_locks = self.txn_context().ext().pessimistic_locks.upgradable_read();
        if pessimistic_locks.is_empty() {
            let mut pessimistic_locks = RwLockUpgradableReadGuard::upgrade(pessimistic_locks);
            pessimistic_locks.status = LocksStatus::MergingRegion;
            return Ok(());
        }

        // The proposed pessimistic locks here will also be carried in CommitMerge.
        // Check the size to avoid CommitMerge exceeding the size limit of a raft entry.
        // This check is a inaccurate check. We will check the size again accurately
        // later using the protobuf encoding.
        if pessimistic_locks.memory_size > size_limit {
            return Err(box_err!(
                "pessimistic locks size {} exceed size limit {}, skip merging.",
                pessimistic_locks.memory_size,
                size_limit
            ));
        }

        let mut cmd = RaftCmdRequest::default();
        for (key, (lock, _deleted)) in &*pessimistic_locks {
            let mut put = PutRequest::default();
            put.set_cf(CF_LOCK.to_string());
            put.set_key(key.as_encoded().to_owned());
            put.set_value(lock.to_lock().to_bytes());
            let mut req = Request::default();
            req.set_cmd_type(CmdType::Put);
            req.set_put(put);
            cmd.mut_requests().push(req);
        }
        cmd.mut_header().set_region_id(self.region_id());
        cmd.mut_header()
            .set_region_epoch(self.region().get_region_epoch().clone());
        cmd.mut_header().set_peer(self.peer().clone());
        let proposal_size = cmd.compute_size();
        if proposal_size as usize > size_limit {
            return Err(box_err!(
                "pessimistic locks size {} exceed size limit {}, skip merging.",
                proposal_size,
                size_limit
            ));
        }

        {
            let mut pessimistic_locks = RwLockUpgradableReadGuard::upgrade(pessimistic_locks);
            pessimistic_locks.status = LocksStatus::MergingRegion;
        }
        debug!(
            self.logger,
            "propose {} pessimistic locks before prepare merge",
            cmd.get_requests().len();
        );
        self.propose(store_ctx, cmd.write_to_bytes().unwrap())?;
        Ok(())
    }

    pub fn post_prepare_merge_fail(&mut self) {
        // Match v1::post_propose_fail.
        // If we just failed to propose PrepareMerge, the pessimistic locks status
        // may become MergingRegion incorrectly. So, we have to revert it here.
        // Note: The `is_merging` check from v1 is removed because proposed
        // `PrepareMerge` rejects all writes (in `ProposalControl::check_conflict`).
        assert!(
            !self.proposal_control().is_merging(),
            "{}",
            SlogFormat(&self.logger)
        );
        self.take_merge_context();
        self.proposal_control_mut().set_pending_prepare_merge(false);
        let mut pessimistic_locks = self.txn_context().ext().pessimistic_locks.write();
        if pessimistic_locks.status == LocksStatus::MergingRegion {
            pessimistic_locks.status = LocksStatus::Normal;
        }
    }
}

impl<EK: KvEngine, R: ApplyResReporter> Apply<EK, R> {
    // Match v1::exec_prepare_merge.
    pub async fn apply_prepare_merge(
        &mut self,
        req: &AdminRequest,
        log_index: u64,
    ) -> Result<(AdminResponse, AdminCmdResult)> {
        PEER_ADMIN_CMD_COUNTER.prepare_merge.all.inc();

        let prepare_merge = req.get_prepare_merge();
        let index = prepare_merge.get_min_index();
        // Note: the check against first_index is removed in v2.
        let mut region = self.region().clone();
        let region_version = region.get_region_epoch().get_version() + 1;
        region.mut_region_epoch().set_version(region_version);
        // In theory conf version should not be increased when executing prepare_merge.
        // However, we don't want to do conf change after prepare_merge is committed.
        // This can also be done by iterating all proposal to find if prepare_merge is
        // proposed before proposing conf change, but it make things complicated.
        // Another way is make conf change also check region version, but this is not
        // backward compatible.
        let conf_version = region.get_region_epoch().get_conf_ver() + 1;
        region.mut_region_epoch().set_conf_ver(conf_version);
        let mut merging_state = MergeState::default();
        merging_state.set_min_index(index);
        merging_state.set_target(prepare_merge.get_target().to_owned());
        merging_state.set_commit(log_index);

        self.region_state_mut().set_region(region.clone());
        self.region_state_mut().set_state(PeerState::Merging);
        assert!(
            !self.region_state().has_merge_state(),
            "{:?}",
            self.region_state()
        );
        self.region_state_mut()
            .set_merge_state(merging_state.clone());

        PEER_ADMIN_CMD_COUNTER.prepare_merge.success.inc();

        info!(
            self.logger,
            "execute PrepareMerge";
            "index" => log_index,
            "target_region" => ?prepare_merge.get_target(),
        );

        let _ = self.flush();
        let reg = self.tablet_registry();
        let path = merge_source_path(reg, self.region_id(), log_index);
        // We might be replaying this command.
        if !path.exists() {
            let tablet = self.tablet().clone();
            let logger = self.logger.clone();
            let (tx, rx) = oneshot::channel();
            self.high_priority_pool()
                .spawn(async move {
                    let mut checkpointer = tablet.new_checkpointer().unwrap_or_else(|e| {
                        slog_panic!(
                            logger,
                            "fails to create checkpoint object";
                            "error" => ?e
                        )
                    });
                    checkpointer.create_at(&path, None, 0).unwrap_or_else(|e| {
                        slog_panic!(
                            logger,
                            "fails to create checkpoint";
                            "path" => %path.display(),
                            "error" => ?e
                        )
                    });
                    tx.send(()).unwrap();
                })
                .unwrap();
            rx.await.unwrap();
        }

        // Notes on the lifetime of this checkpoint:
        // - Target region is responsible to clean up if it has proposed `CommitMerge`.
        //   It will destroy the checkpoint if the persisted apply index is advanced. It
        //   will also destroy the checkpoint before sending `GcPeerResponse` to target
        //   leader.
        // - Otherwise, the `PrepareMerge` is rollback-ed. In this case the source
        //   region is responsible to clean up (see `rollback_merge`).

        Ok((
            AdminResponse::default(),
            AdminCmdResult::PrepareMerge(PrepareMergeResult {
                region_state: self.region_state().clone(),
                state: merging_state,
            }),
        ))
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    // Match v1::on_ready_prepare_merge.
    pub fn on_apply_res_prepare_merge<T: Transport>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        res: PrepareMergeResult,
    ) {
        fail::fail_point!("on_apply_res_prepare_merge");

        let region = res.region_state.get_region().clone();
        {
            let mut meta = store_ctx.store_meta.lock().unwrap();
            meta.set_region(&region, true, &self.logger);
            let (reader, _) = meta.readers.get_mut(&region.get_id()).unwrap();
            self.set_region(
                &store_ctx.coprocessor_host,
                reader,
                region,
                RegionChangeReason::PrepareMerge,
                self.storage().region_state().get_tablet_index(),
            );
        }

        self.storage_mut()
            .set_region_state(res.region_state.clone());
        let region_id = self.region_id();
        self.state_changes_mut()
            .put_region_state(region_id, res.state.get_commit(), &res.region_state)
            .unwrap();
        self.set_has_extra_write();

        self.proposal_control_mut()
            .enter_prepare_merge(res.state.get_commit());
        if let Some(PrepareStatus::CatchUpLogs(cul)) = self
            .merge_context_mut()
            .prepare_status
            .replace(PrepareStatus::Applied(res.state))
        {
            self.finish_catch_up_logs(store_ctx, cul);
        }

        self.start_commit_merge(store_ctx);
    }
}
