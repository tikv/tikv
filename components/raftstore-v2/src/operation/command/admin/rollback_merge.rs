// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! The rollback of `PrepareMerge` command.
//!
//! # Propose
//!
//! A `RollbackMerge` command is proposed only when peers that have such
//! intention have reached a quorum. The intention is recorded by leader in
//! `MergeContext::rollback_peers`.

use engine_traits::{KvEngine, RaftEngine, RaftLogBatch};
use error_code::ErrorCodeExt;
use kvproto::{
    metapb,
    raft_cmdpb::{AdminCmdType, AdminRequest, AdminResponse},
    raft_serverpb::{ExtraMessage, ExtraMessageType, MergeState, PeerState},
};
use protobuf::Message;
use raftstore::{
    coprocessor::RegionChangeReason,
    store::{fsm::new_admin_request, metrics::PEER_ADMIN_CMD_COUNTER, LocksStatus, Transport},
    Error, Result,
};
use slog::{error, info};
use tikv_util::slog_panic;

use crate::{
    batch::StoreContext,
    fsm::ApplyResReporter,
    operation::AdminCmdResult,
    raft::{Apply, Peer},
};

#[derive(Debug)]
pub struct RollbackMergeResult {
    commit: u64,
    region: metapb::Region,
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    // Match v1::on_check_merge.
    pub fn handle_commit_merge_failure<T: Transport>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        e: Error,
    ) {
        if self.is_leader() {
            let peer_id = self.peer_id();
            self.merge_context_mut().rollback_peers.insert(peer_id);
            if self.quorum_want_to_rollback_merge() {
                info!(
                    self.logger,
                    "failed to schedule merge, rollback";
                    "err" => %e,
                    "error_code" => %e.error_code(),
                );
                let state = self.merge_context().pending.as_ref().unwrap();
                let mut request = new_admin_request(self.region_id(), self.peer().clone());
                request
                    .mut_header()
                    .set_region_epoch(self.region().get_region_epoch().clone());
                let mut admin = AdminRequest::default();
                admin.set_cmd_type(AdminCmdType::RollbackMerge);
                admin.mut_rollback_merge().set_commit(state.get_commit());
                request.set_admin_request(admin);
                if let Err(e) = self.propose(store_ctx, request.write_to_bytes().unwrap()) {
                    error!(self.logger, "failed to propose RollbackMerge"; "err" => ?e);
                }
            }
        } else if !tikv_util::store::is_learner(self.peer()) {
            info!(
                self.logger,
                "want to rollback merge";
                "leader_id" => self.leader_id(),
                "err" => %e,
                "error_code" => %e.error_code(),
            );
            if self.leader_id() != raft::INVALID_ID {
                // Match v1::send_want_rollback_merge.
                let mut msg = self.prepare_raft_message();
                let mut extra_msg = ExtraMessage::default();
                extra_msg.set_type(ExtraMessageType::MsgWantRollbackMerge);
                extra_msg.set_premerge_commit(
                    self.merge_context().pending.as_ref().unwrap().get_commit(),
                );
                msg.set_extra_msg(extra_msg);
                let to_peer = self
                    .region()
                    .get_peers()
                    .iter()
                    .find(|p| p.get_id() == self.leader_id())
                    .unwrap()
                    .clone();
                msg.set_to_peer(to_peer.clone());
                if let Err(e) = store_ctx.trans.send(msg) {
                    error!(
                        self.logger,
                        "failed to send extra message MsgWantRollbackMerge";
                        "err" => ?e,
                        "target" => ?to_peer,
                    );
                }
            }
        }
    }
}

impl<EK: KvEngine, R: ApplyResReporter> Apply<EK, R> {
    // Match v1::exec_rollback_merge.
    pub fn apply_rollback_merge(
        &mut self,
        req: &AdminRequest,
        _index: u64,
    ) -> Result<(AdminResponse, AdminCmdResult)> {
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
        let mut region = self.region().clone();
        let version = region.get_region_epoch().get_version();
        // Update version to avoid duplicated rollback requests.
        region.mut_region_epoch().set_version(version + 1);
        self.region_state_mut().set_region(region.clone());
        self.region_state_mut().set_state(PeerState::Normal);
        self.region_state_mut()
            .set_merge_state(MergeState::default());

        PEER_ADMIN_CMD_COUNTER.rollback_merge.success.inc();
        Ok((
            AdminResponse::default(),
            AdminCmdResult::RollbackMerge(RollbackMergeResult {
                commit: rollback.get_commit(),
                region,
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
        assert!(res.commit != 0);
        let pending_commit = self.merge_context().pending.as_ref().unwrap().get_commit();
        if pending_commit != res.commit {
            slog_panic!(
                self.logger,
                "rollbacks a wrong merge";
                "pending_commit" => pending_commit,
                "commit" => res.commit,
            );
        }
        {
            let mut meta = store_ctx.store_meta.lock().unwrap();
            meta.set_region(&res.region, true, &self.logger);
            let (reader, _) = meta.readers.get_mut(&res.region.get_id()).unwrap();
            self.set_region(
                &store_ctx.coprocessor_host,
                reader,
                res.region.clone(),
                RegionChangeReason::RollbackMerge,
                res.commit,
            );
        }
        let region_state = self.storage().region_state().clone();
        let region_id = self.region_id();
        self.state_changes_mut()
            .put_region_state(region_id, res.commit, &region_state)
            .unwrap();
        self.set_has_extra_write();

        self.rollback_merge(store_ctx);
    }

    /// This can be called directly without proposal, in which case a snapshot
    /// rollbacks the merge.
    pub fn rollback_merge<T>(&mut self, store_ctx: &mut StoreContext<EK, ER, T>) {
        // Clear merge releted data
        let state = self.merge_context_mut().pending.take();
        self.proposal_control_mut()
            .leave_prepare_merge(state.unwrap().get_commit());
        self.merge_context_mut().rollback_peers.clear();

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
