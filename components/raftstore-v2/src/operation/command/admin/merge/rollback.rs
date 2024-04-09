// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

//! The rollback of `PrepareMerge` command.

use engine_traits::{KvEngine, RaftEngine, RaftLogBatch};
use kvproto::{
    raft_cmdpb::{AdminCmdType, AdminRequest, AdminResponse},
    raft_serverpb::{PeerState, RegionLocalState},
};
use raftstore::{
    coprocessor::RegionChangeReason,
    store::{fsm::new_admin_request, metrics::PEER_ADMIN_CMD_COUNTER, LocksStatus, Transport},
    Result,
};
use slog::{error, info};
use tikv_util::slog_panic;

use super::merge_source_path;
use crate::{
    batch::StoreContext,
    fsm::ApplyResReporter,
    operation::AdminCmdResult,
    raft::{Apply, Peer},
    router::CmdResChannel,
};

#[derive(Debug)]
pub struct RollbackMergeResult {
    commit: u64,
    region_state: RegionLocalState,
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    // Match v1::on_check_merge.
    pub fn on_reject_commit_merge<T: Transport>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        index: u64,
    ) {
        fail::fail_point!("on_reject_commit_merge_1", store_ctx.store_id == 1, |_| {});
        let self_index = self.merge_context().and_then(|c| c.prepare_merge_index());
        if self_index != Some(index) {
            info!(
                self.logger,
                "ignore RejectCommitMerge due to index not match";
                "index" => index,
                "self_index" => ?self_index,
            );
            return;
        }
        let mut request = new_admin_request(self.region_id(), self.peer().clone());
        request
            .mut_header()
            .set_region_epoch(self.region().get_region_epoch().clone());
        let mut admin = AdminRequest::default();
        admin.set_cmd_type(AdminCmdType::RollbackMerge);
        admin.mut_rollback_merge().set_commit(index);
        request.set_admin_request(admin);
        let (ch, res) = CmdResChannel::pair();
        self.on_admin_command(store_ctx, request, ch);
        if let Some(res) = res.take_result()
            && res.get_header().has_error()
        {
            error!(
                self.logger,
                "failed to propose rollback merge";
                "res" => ?res,
            );
        }
    }
}

impl<EK: KvEngine, R: ApplyResReporter> Apply<EK, R> {
    // Match v1::exec_rollback_merge.
    pub fn apply_rollback_merge(
        &mut self,
        req: &AdminRequest,
        index: u64,
    ) -> Result<(AdminResponse, AdminCmdResult)> {
        fail::fail_point!("apply_rollback_merge");
        PEER_ADMIN_CMD_COUNTER.rollback_merge.all.inc();
        if self.region_state().get_state() != PeerState::Merging {
            slog_panic!(
                self.logger,
                "unexpected state of merging region";
                "state" => ?self.region_state(),
            );
        }
        let rollback = req.get_rollback_merge();
        let merge_state = self.region_state().get_merge_state();
        if merge_state.get_commit() != rollback.get_commit() {
            slog_panic!(
                self.logger,
                "unexpected merge state of merging region";
                "state" => ?merge_state,
            );
        }

        let prepare_merge_commit = rollback.commit;
        info!(
            self.logger,
            "execute RollbackMerge";
            "commit" => prepare_merge_commit,
            "index" => index,
        );

        let mut region = self.region().clone();
        let version = region.get_region_epoch().get_version();
        // Update version to avoid duplicated rollback requests.
        region.mut_region_epoch().set_version(version + 1);
        self.region_state_mut().set_region(region.clone());
        self.region_state_mut().set_state(PeerState::Normal);
        self.region_state_mut().take_merge_state();

        PEER_ADMIN_CMD_COUNTER.rollback_merge.success.inc();
        Ok((
            AdminResponse::default(),
            AdminCmdResult::RollbackMerge(RollbackMergeResult {
                commit: rollback.get_commit(),
                region_state: self.region_state().clone(),
            }),
        ))
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    // Match v1::on_ready_rollback_merge.
    pub fn on_apply_res_rollback_merge<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        res: RollbackMergeResult,
    ) {
        let region = res.region_state.get_region();
        assert_ne!(res.commit, 0);
        let current = self.merge_context().and_then(|c| c.prepare_merge_index());
        if current != Some(res.commit) {
            slog_panic!(
                self.logger,
                "rollbacks a wrong merge";
                "pending_commit" => ?current,
                "commit" => res.commit,
            );
        }
        {
            let mut meta = store_ctx.store_meta.lock().unwrap();
            meta.set_region(region, true, &self.logger);
            let (reader, _) = meta.readers.get_mut(&region.get_id()).unwrap();
            self.set_region(
                &store_ctx.coprocessor_host,
                reader,
                region.clone(),
                RegionChangeReason::RollbackMerge,
                self.storage().region_state().get_tablet_index(),
            );
        }
        let region_id = self.region_id();
        self.state_changes_mut()
            .put_region_state(region_id, res.commit, &res.region_state)
            .unwrap();
        self.storage_mut().set_region_state(res.region_state);
        self.set_has_extra_write();

        self.rollback_merge(store_ctx);
    }

    /// This can be called directly without proposal, in which case a snapshot
    /// rollbacks the merge.
    pub fn rollback_merge<T>(&mut self, store_ctx: &mut StoreContext<EK, ER, T>) {
        let index = self
            .merge_context()
            .and_then(|c| c.prepare_merge_index())
            .unwrap_or_else(|| slog_panic!(self.logger, "no applied prepare merge to rollback"));
        // Clear merge releted data
        let checkpoint_path =
            merge_source_path(&store_ctx.tablet_registry, self.region_id(), index);
        if checkpoint_path.exists() {
            // Don't remove it immediately so that next restart we don't need to waste time
            // making the checkpoint again. We double check in `clean_up_tablets` to ensure
            // this checkpoint isn't leaked.
            self.record_tombstone_tablet_path(store_ctx, checkpoint_path, index);
        }
        self.proposal_control_mut().leave_prepare_merge(index);
        self.take_merge_context();

        // Resume updating `safe_ts`
        self.read_progress_mut().resume();

        if self.is_leader() {
            {
                let mut pessimistic_locks = self.txn_context().ext().pessimistic_locks.write();
                if pessimistic_locks.status == LocksStatus::MergingRegion {
                    pessimistic_locks.status = LocksStatus::Normal;
                }
            }
            self.region_heartbeat_pd(store_ctx);
        }
    }
}
