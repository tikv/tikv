// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, RaftEngine};
use kvproto::{pdpb, raft_serverpb::RegionLocalState};
use raft::{GetEntriesContext, Storage, NO_LIMIT};
use raftstore::store::{
    ProposalContext, Transport, UnsafeRecoveryFillOutReportSyncer, UnsafeRecoveryState,
    UnsafeRecoveryWaitApplySyncer,
};
use slog::{info, warn};

use crate::{batch::StoreContext, fsm::Store, raft::Peer};

impl Store {
    pub fn on_unsafe_recovery_report<EK, ER, T>(
        &self,
        ctx: &StoreContext<EK, ER, T>,
        report: pdpb::StoreReport,
    ) where
        EK: KvEngine,
        ER: RaftEngine,
        T: Transport,
    {
        self.store_heartbeat_pd(ctx, Some(report))
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub fn on_unsafe_recovery_wait_apply(&mut self, syncer: UnsafeRecoveryWaitApplySyncer) {
        if let Some(state) = self.unsafe_recovery_state()
            && !state.is_abort()
        {
            warn!(self.logger,
                "Unsafe recovery, can't wait apply, another plan is executing in progress";
                "state" => ?state,
            );
            syncer.abort();
            return;
        }
        let target_index = if self.has_force_leader() {
            // For regions that lose quorum (or regions have force leader), whatever has
            // been proposed will be committed. Based on that fact, we simply use "last
            // index" here to avoid implementing another "wait commit" process.
            self.raft_group().raft.raft_log.last_index()
        } else {
            self.raft_group().raft.raft_log.committed
        };

        if target_index > self.raft_group().raft.raft_log.applied {
            info!(
                self.logger,
                "Unsafe recovery, start wait apply";
                "target_index" => target_index,
                "applied" =>  self.raft_group().raft.raft_log.applied,
            );
            *self.unsafe_recovery_state_mut() = Some(UnsafeRecoveryState::WaitApply {
                target_index,
                syncer,
            });
            self.unsafe_recovery_maybe_finish_wait_apply(!self.serving());
        }
    }

    pub fn unsafe_recovery_maybe_finish_wait_apply(&mut self, force: bool) {
        if let Some(UnsafeRecoveryState::WaitApply { target_index, .. }) =
            self.unsafe_recovery_state()
        {
            if self.raft_group().raft.raft_log.applied >= *target_index || force {
                if self.is_in_force_leader() {
                    info!(self.logger,
                        "Unsafe recovery, finish wait apply";
                        "target_index" => target_index,
                        "applied" =>  self.raft_group().raft.raft_log.applied,
                        "force" => force,
                    );
                }
                *self.unsafe_recovery_state_mut() = None;
            }
        }
    }

    pub fn on_unsafe_recovery_fill_out_report(
        &mut self,
        syncer: UnsafeRecoveryFillOutReportSyncer,
    ) {
        if !self.serving() {
            return;
        }
        let mut self_report = pdpb::PeerReport::default();
        self_report.set_raft_state(self.storage().raft_state().clone());
        let mut region_local_state = RegionLocalState::default();
        region_local_state.set_region(self.region().clone());
        self_report.set_region_state(region_local_state);
        self_report.set_is_force_leader(self.has_force_leader());
        match self.storage().entries(
            self.storage().entry_storage().commit_index() + 1,
            self.storage().entry_storage().last_index() + 1,
            NO_LIMIT,
            GetEntriesContext::empty(false),
        ) {
            Ok(entries) => {
                for entry in entries {
                    let ctx = ProposalContext::from_bytes(&entry.context);
                    if ctx.contains(ProposalContext::COMMIT_MERGE) {
                        self_report.set_has_commit_merge(true);
                        break;
                    }
                }
            }
            Err(e) => panic!("Unsafe recovery, fail to get uncommitted entries, {:?}", e),
        }
        syncer.report_for_self(self_report);
    }

    pub fn check_unsafe_recovery_state<T: Transport>(&mut self, ctx: &mut StoreContext<EK, ER, T>) {
        match self.unsafe_recovery_state() {
            Some(UnsafeRecoveryState::WaitApply { .. }) => {
                self.unsafe_recovery_maybe_finish_wait_apply(false)
            }
            Some(UnsafeRecoveryState::WaitInitialize { .. }) => {
                self.unsafe_recovery_maybe_finish_wait_initialized(false)
            }
            Some(UnsafeRecoveryState::DemoteFailedVoters { .. }) => {
                self.unsafe_recovery_maybe_finish_demote_failed_voters(ctx)
            }
            Some(UnsafeRecoveryState::Destroy(_)) | Some(UnsafeRecoveryState::Failed) | None => {}
        }
    }
}
