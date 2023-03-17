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

use std::mem;

use engine_traits::{KvEngine, RaftEngine, RaftLogBatch, CF_LOCK};
use kvproto::{
    raft_cmdpb::{
        AdminCmdType, AdminRequest, AdminResponse, CmdType, PrepareMergeRequest, PutRequest,
        RaftCmdRequest, Request,
    },
    raft_serverpb::{MergeState, PeerState, RegionLocalState},
};
use parking_lot::RwLockUpgradableReadGuard;
use protobuf::Message;
use raft::{eraftpb::EntryType, GetEntriesContext, NO_LIMIT};
use raftstore::{
    coprocessor::RegionChangeReason,
    store::{metrics::PEER_ADMIN_CMD_COUNTER, util, LocksStatus, ProposalContext, Transport},
    Error, Result,
};
use slog::{debug, info};
use tikv_util::{box_err, log::SlogFormat, store::region_on_same_stores};

use crate::{
    batch::StoreContext,
    fsm::ApplyResReporter,
    operation::AdminCmdResult,
    raft::{Apply, Peer},
    router::CmdResChannel,
};

#[derive(Clone)]
pub struct PreProposeContext {
    pub min_matched: u64,
    lock_size_limit: usize,
}

pub enum PrepareStatus {
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
    /// In this state, all write proposals except for `RollbackMerge` will be
    /// rejected.
    Applied(MergeState),
}

#[derive(Debug)]
pub struct PrepareMergeResult {
    region_state: RegionLocalState,
    state: MergeState,
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub fn propose_prepare_merge<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        mut req: RaftCmdRequest,
    ) -> Result<u64> {
        if self.storage().has_dirty_data() {
            return Err(box_err!(
                "{} source peer has dirty data, try again later",
                SlogFormat(&self.logger)
            ));
        }
        self.validate_prepare_merge_command(
            store_ctx,
            req.get_admin_request().get_prepare_merge(),
        )?;
        let pre_propose = if let Some(r) = self.already_checked_pessimistic_locks()? {
            r
        } else {
            let r = self.check_logs_before_prepare_merge(store_ctx)?;
            self.check_pessimistic_locks(r, &mut req)?
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
                self.propose_with_ctx(store_ctx, data, proposal_ctx.to_vec())
            });
        if r.is_ok() {
            self.proposal_control_mut().set_pending_prepare_merge(false);
        } else {
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
            let cmd: RaftCmdRequest =
                util::parse_data_at(entry.get_data(), entry.get_index(), "tag");
            if !cmd.has_admin_request() {
                continue;
            }
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
            self.merge_context_mut().prepare_status = Some(PrepareStatus::WaitForFence {
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
            Some(PrepareStatus::WaitForFence { fence, ctx, .. }) => {
                if applied_index < *fence {
                    info!(
                        self.logger,
                        "reject PrepareMerge because applied_index has not reached prepare_merge_fence";
                        "applied_index" => applied_index,
                        "prepare_merge_fence" => fence,
                    );
                    Err(Error::PendingPrepareMerge)
                } else {
                    Ok(Some(ctx.clone()))
                }
            }
            Some(PrepareStatus::Applied(state)) => Err(box_err!(
                "another merge is in-progress, merge_state: {:?}.",
                state
            )),
            None => Ok(None),
        }
    }

    /// Called after some new entries have been applied and the fence can
    /// probably be lifted.
    pub fn retry_pending_prepare_merge<T: Transport>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        applied_index: u64,
    ) {
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
}

impl<EK: KvEngine, R: ApplyResReporter> Apply<EK, R> {
    // Match v1::exec_prepare_merge.
    pub fn apply_prepare_merge(
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
                res.state.get_commit(),
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
        self.merge_context_mut().prepare_status = Some(PrepareStatus::Applied(res.state));

        // TODO: self.
        // update_merge_progress_on_apply_res_prepare_merge(store_ctx);
    }
}
