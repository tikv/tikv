// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! The handling of `PrepareMerge` command.
//!
//! ## Propose (`Peer::propose_prepare_merge`)
//!
//! Checks for these requirements:
//!
//! - Validate the request. (`Peer::validate_prepare_merge_command`)
//! - Log gap between source region leader and peers is not too large. We need
//!   to catch up these logs before starting the merge.
//! - Logs that aren't fully committed (to all peers) does not contains
//!   `CompactLog` or certain admin commands.
//!
//! Then, transfer all in-memory pessimistic locks to the target region as a
//! Raft proposal. To guarantee the consistency of lock serialization, we might
//! need to wait for some in-flight logs to be applied. Read the comments of
//! `Peer::pending_merge_fence` for more details.
//!
//! ## Apply (`Apply::apply_prepare_merge`)
//!
//! Increase region epoch and write the merge state.
//!
//! ## On Apply Result (`Peer::on_ready_prepare_merge`)
//!
//! Initiate catch up logs. And start the tick (`Peer::on_merge_check_tick`) to
//! periodically check the eligibility of merge.

use engine_traits::{KvEngine, RaftEngine, CF_LOCK};
use kvproto::{
    metapb::Region,
    raft_cmdpb::{
        AdminCmdType, AdminRequest, AdminResponse, CmdType, PrepareMergeRequest, PutRequest,
        RaftCmdRequest, Request,
    },
    raft_serverpb::{MergeState, PeerState},
};
use parking_lot::RwLockUpgradableReadGuard;
use protobuf::Message;
use raft::{eraftpb::EntryType, GetEntriesContext, ProgressState, INVALID_INDEX, NO_LIMIT};
use raftstore::{
    coprocessor::RegionChangeReason,
    store::{entry_storage, metrics::PEER_ADMIN_CMD_COUNTER, util, LocksStatus, ProposalContext},
    Error, Result,
};
use slog::{debug, info, warn, Logger};
use tikv_util::{box_err, store::region_on_same_stores};

use crate::{
    batch::StoreContext,
    fsm::ApplyResReporter,
    operation::AdminCmdResult,
    raft::{Apply, Peer},
    router::ApplyRes,
};

#[derive(Debug)]
pub struct PrepareMergeResult {
    pub region: Region,
    pub state: MergeState,
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub fn propose_prepare_merge<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        mut req: RaftCmdRequest,
    ) -> Result<u64> {
        self.validate_prepare_merge_command(
            store_ctx,
            req.get_admin_request().get_prepare_merge(),
        )?;
        let req = self.pre_propose_prepare_merge(store_ctx, req)?;

        let mut proposal_ctx = ProposalContext::empty();
        proposal_ctx.insert(ProposalContext::PREPARE_MERGE);
        let data = req.write_to_bytes().unwrap();
        let res = self.propose_with_ctx(store_ctx, data, proposal_ctx.to_vec())?;

        // TODO: broadcast to followers when disk full.

        Ok(res)
    }

    /// Mirrors v1::check_merge_proposal.
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
                Some(reader) if *reader.region != *target_region => {
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

    fn pre_propose_prepare_merge<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        mut req: RaftCmdRequest,
    ) -> Result<RaftCmdRequest> {
        let applied_index = self.entry_storage().applied_index();
        // Check existing fence.
        if let Some((idx, cmd)) = self.prepare_merge_fence.as_mut() {
            if applied_index < *idx {
                *cmd = req;
                info!(
                    self.logger,
                    "reject PrepareMerge because applied_index has not reached prepare_merge_fence";
                    "region_id" => self.region_id(),
                    "applied_index" => applied_index,
                    "prepare_merge_fence" => *idx
                );
                return Err(Error::PendingPrepareMerge);
            }
        }

        let last_index = self.raft_group().raft.raft_log.last_index();
        let (min_matched, min_committed) = self.get_min_progress()?;
        // TODO: also check snapshot (`min_matched < self.last_sent_snapshot_idx`).
        if min_matched == 0
            || min_committed == 0
            || last_index - min_matched > store_ctx.cfg.merge_max_log_gap
            || last_index - min_committed > store_ctx.cfg.merge_max_log_gap * 2
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

        // Clear existing fence or install a new fence.
        if self.prepare_merge_fence.is_none() {
            let has_locks = {
                let pessimistic_locks = self.txn_ext().pessimistic_locks.read();
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
                self.prepare_merge_fence = Some((last_index, req));
                info!(
                    self.logger,
                    "start rejecting new proposals before prepare merge";
                    "region_id" => self.region_id(),
                    "prepare_merge_fence" => last_index
                );
                return Err(Error::PendingPrepareMerge);
            }
        } else {
            self.prepare_merge_fence.take();
        }
        debug_assert!(self.prepare_merge_fence.is_none());

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
        let pessimistic_locks = self.txn_ext().pessimistic_locks.upgradable_read();
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
            self.logger,"propose {} pessimistic locks before prepare merge", cmd.get_requests().len();
            "region_id" => self.region_id());
        self.propose_command(store_ctx, cmd)?;
        Ok(())
    }

    /// Returns (minimal matched, minimal committed_index)
    fn get_min_progress(&self) -> Result<(u64, u64)> {
        let (mut min_m, mut min_c) = (None, None);
        if let Some(progress) = self.raft_group().status().progress {
            for (id, pr) in progress.iter() {
                // Reject merge if there is any pending request snapshot,
                // because a target region may merge a source region which is in
                // an invalid state.
                if pr.state == ProgressState::Snapshot
                    || pr.pending_request_snapshot != INVALID_INDEX
                {
                    return Err(box_err!(
                        "there is a pending snapshot peer {} [{:?}], skip merge",
                        id,
                        pr
                    ));
                }
                if min_m.unwrap_or(u64::MAX) > pr.matched {
                    min_m = Some(pr.matched);
                }
                if min_c.unwrap_or(u64::MAX) > pr.committed_index {
                    min_c = Some(pr.committed_index);
                }
            }
        }
        let (mut min_m, min_c) = (min_m.unwrap_or(0), min_c.unwrap_or(0));
        if min_m < min_c {
            warn!(
                self.logger,
                "min_matched < min_committed, raft progress is inaccurate";
                "region_id" => self.region_id(),
                "peer_id" => self.peer().get_id(),
                "min_matched" => min_m,
                "min_committed" => min_c,
            );
            // Reset `min_matched` to `min_committed`, since the raft log at `min_committed`
            // is known to be committed in all peers, all of the peers should also have
            // replicated it
            min_m = min_c;
        }
        Ok((min_m, min_c))
    }

    /// Called after some new entries have been applied and the fence can
    /// probably be lifted.
    pub fn retry_pending_prepare_merge<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        applied_index: u64,
    ) {
        if let Some(idx) = self.prepare_merge_fence.as_ref().map(|f| f.0) && idx <= applied_index {
            let cmd = self.prepare_merge_fence.take().unwrap().1;
            self.propose_command(store_ctx, cmd);
        }
    }

    pub fn on_ready_prepare_merge<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        res: PrepareMergeResult,
    ) {
        self.set_region(
            &mut store_ctx.store_meta.lock().unwrap(),
            res.region.clone(),
            RegionChangeReason::PrepareMerge,
            res.state.get_commit(),
        );

        self.pending_merge_state = Some(res.state);
        let state = self.pending_merge_state.as_ref().unwrap();

        self.update_merge_progress_on_ready_prepare_merge();
        self.on_merge_check_tick();
    }
}

impl<EK: KvEngine, R: ApplyResReporter> Apply<EK, R> {
    pub fn apply_prepare_merge(
        &mut self,
        req: &AdminRequest,
        log_index: u64,
    ) -> Result<(AdminResponse, AdminCmdResult)> {
        PEER_ADMIN_CMD_COUNTER.prepare_merge.all.inc();

        let prepare_merge = req.get_prepare_merge();
        let index = prepare_merge.get_min_index();
        // Note: the check against first_index is removed in v2.
        let mut region = self.region_state().get_region().clone();
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
                region,
                state: merging_state,
            }),
        ))
    }
}
