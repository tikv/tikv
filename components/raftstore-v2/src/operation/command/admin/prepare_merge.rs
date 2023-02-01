// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

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
//! `MergeContext::prepare_fence` for more details.
//!
//! ## Apply (`Apply::apply_prepare_merge`)
//!
//! Increase region epoch and write the merge state.
//!
//! ## On Apply Result (`Peer::on_apply_res_prepare_merge`)
//!
//! Start the tick (`Peer::on_check_merge`) to periodically check the
//! eligibility of merge.

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
use slog::{debug, error, info};
use tikv_util::{box_err, store::region_on_same_stores};

use crate::{
    batch::StoreContext,
    fsm::ApplyResReporter,
    operation::AdminCmdResult,
    raft::{Apply, Peer},
};

#[derive(Debug)]
pub struct PrepareMergeResult {
    pub region_state: RegionLocalState,
    pub state: MergeState,
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub fn propose_prepare_merge<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        req: RaftCmdRequest,
    ) -> Result<u64> {
        if self.storage().has_dirty_data() {
            return Err(box_err!(
                "{:?} source peer has dirty data, try again later",
                self.logger.list()
            ));
        }
        self.validate_prepare_merge_command(
            store_ctx,
            req.get_admin_request().get_prepare_merge(),
        )?;
        let req = self.pre_propose_prepare_merge(store_ctx, req)?;

        let mut proposal_ctx = ProposalContext::empty();
        proposal_ctx.insert(ProposalContext::PREPARE_MERGE);
        let data = req.write_to_bytes().unwrap();
        let r = self.propose_with_ctx(store_ctx, data, proposal_ctx.to_vec());
        if r.is_err() {
            self.restore_merging_region_locks_status();
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
                "{:?} region in joint state, can not propose merge command, command: {:?}",
                self.logger.list(),
                req
            ));
        }
        let region = self.region();
        let target_region = req.get_target();
        {
            let store_meta = store_ctx.store_meta.lock().unwrap();
            match store_meta.readers.get(&target_region.get_id()) {
                Some((reader, _)) if *reader.region != *target_region => {
                    return Err(box_err!(
                        "target region not matched, skip proposing: {:?} != {:?}",
                        reader.region,
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
    fn pre_propose_prepare_merge<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        mut req: RaftCmdRequest,
    ) -> Result<RaftCmdRequest> {
        let applied_index = self.entry_storage().applied_index();
        // Check existing fence.
        let has_prepare_merge_fence = self.merge_context().prepare_fence.is_some();
        if has_prepare_merge_fence {
            if let Err(fence) = self
                .merge_context_mut()
                .release_or_refresh_prepare_merge_fence(applied_index, Some(&req))
            {
                info!(
                    self.logger,
                    "reject PrepareMerge because applied_index has not reached prepare_merge_fence";
                    "applied_index" => applied_index,
                    "prepare_merge_fence" => fence,
                );
                return Err(Error::PendingPrepareMerge);
            }
        }

        let last_index = self.raft_group().raft.raft_log.last_index();
        let (min_matched, min_committed) = self.get_min_progress()?;
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

        // If we haven't already released a fence, maybe install a new fence.
        if !has_prepare_merge_fence {
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
            if has_locks && applied_index < last_index {
                self.merge_context_mut()
                    .install_prepare_merge_fence(last_index, &req);
                info!(
                    self.logger,
                    "start rejecting new proposals before prepare merge";
                    "prepare_merge_fence" => last_index
                );
                return Err(Error::PendingPrepareMerge);
            }
        }
        debug_assert!(self.merge_context().prepare_fence.is_none());

        self.propose_locks_before_prepare_merge(store_ctx, entry_size_limit - entry_size)?;

        req.mut_admin_request()
            .mut_prepare_merge()
            .set_min_index(min_matched + 1);
        Ok(req)
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
        let r = self.propose(store_ctx, cmd.write_to_bytes().unwrap());
        if r.is_err() {
            self.restore_merging_region_locks_status();
            r?;
        }
        Ok(())
    }

    // Match v1::post_propose_fail.
    #[inline]
    fn restore_merging_region_locks_status(&mut self) {
        // If we just failed to propose PrepareMerge, the pessimistic locks status
        // may become MergingRegion incorrectly. So, we have to revert it here.
        // But we have to rule out the case when the region has successfully
        // proposed PrepareMerge or has been in merging, which is decided by
        // the boolean expression below.
        // last_committed_prepare_merge_idx?
        if !self.proposal_control().is_merging() {
            let mut pessimistic_locks = self.txn_context().ext().pessimistic_locks.write();
            if pessimistic_locks.status == LocksStatus::MergingRegion {
                pessimistic_locks.status = LocksStatus::Normal;
            }
        }
    }

    /// Called after some new entries have been applied and the fence can
    /// probably be lifted.
    pub fn retry_pending_prepare_merge<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        applied_index: u64,
    ) {
        if let Ok(Some(cmd)) = self
            .merge_context_mut()
            .release_or_refresh_prepare_merge_fence(applied_index, None)
        {
            if let Err(e) = self.propose(store_ctx, cmd) {
                error!(
                    self.logger,
                    "failed to propose pending prepare merge command";
                    "err" => ?e,
                );
            }
        }
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
        self.merge_context_mut().pending = Some(res.state);

        self.update_merge_progress_on_apply_res_prepare_merge(store_ctx);
    }
}
