// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, RaftEngine};
use kvproto::metapb;
use raftstore::store::{
    demote_failed_voters_request, exit_joint_request, Transport, UnsafeRecoveryExecutePlanSyncer,
    UnsafeRecoveryState,
};
use slog::{error, info, warn};

use crate::{batch::StoreContext, raft::Peer, router::CmdResChannel};

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub fn on_unsafe_recovery_pre_demote_failed_voters<T: Transport>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        syncer: UnsafeRecoveryExecutePlanSyncer,
        failed_voters: Vec<metapb::Peer>,
    ) {
        if let Some(state) = self.unsafe_recovery_state() {
            warn!(self.logger,
                "Unsafe recovery, demote failed voters has already been initiated";
                "state" => ?state,
            );
            syncer.abort();
            return;
        }

        if !self.is_in_force_leader() {
            error!(self.logger,
                "Unsafe recovery, demoting failed voters failed, since this peer is not forced leader";
            );
            return;
        }

        if self.in_joint_state() {
            info!(self.logger,
                "Unsafe recovery, already in joint state, exit first";
            );
            let exit_joint = exit_joint_request(self.region(), self.peer());
            let (ch, sub) = CmdResChannel::pair();
            self.on_admin_command(ctx, exit_joint, ch);
            if let Some(resp) = sub.try_result() && resp.get_header().has_error() {
                error!(self.logger,
                    "Unsafe recovery, fail to exit residual joint state";
                    "err" => ?resp.get_header().get_error(),
                );
                return;
            }
            *self.unsafe_recovery_state_mut() = Some(UnsafeRecoveryState::DemoteFailedVoters {
                syncer,
                failed_voters,
                target_index: self.raft_group().raft.raft_log.last_index(),
                demote_after_exit: true,
            });
        } else {
            self.unsafe_recovery_demote_failed_voters(ctx, failed_voters, syncer);
        }
    }

    pub fn unsafe_recovery_demote_failed_voters<T: Transport>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        failed_voters: Vec<metapb::Peer>,
        syncer: UnsafeRecoveryExecutePlanSyncer,
    ) {
        if let Some(req) = demote_failed_voters_request(self.region(), self.peer(), failed_voters) {
            info!(self.logger,
                "Unsafe recovery, demoting failed voters";
                "req" => ?req);
            let (ch, sub) = CmdResChannel::pair();
            self.on_admin_command(ctx, req, ch);
            if let Some(resp) = sub.try_result() && resp.get_header().has_error() {
                error!(self.logger,
                    "Unsafe recovery, fail to finish demotion";
                    "err" => ?resp.get_header().get_error(),
                );
                *self.unsafe_recovery_state_mut() = Some(UnsafeRecoveryState::Failed);
                return;
            }
            *self.unsafe_recovery_state_mut() = Some(UnsafeRecoveryState::DemoteFailedVoters {
                syncer,
                failed_voters: vec![], // No longer needed since here.
                target_index: self.raft_group().raft.raft_log.last_index(),
                demote_after_exit: false,
            });
        } else {
            warn!(self.logger,
                "Unsafe recovery, no need to demote failed voters";
                "region" => ?self.region(),
            );
        }
    }

    pub fn unsafe_recovery_maybe_finish_demote_failed_voters<T: Transport>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
    ) {
        let Some(UnsafeRecoveryState::DemoteFailedVoters {
            syncer,
            failed_voters,
            target_index,
            demote_after_exit,
        }) = self.unsafe_recovery_state() else { return };

        if self.raft_group().raft.raft_log.applied < *target_index {
            return;
        }

        if *demote_after_exit {
            let syncer_clone = syncer.clone();
            let failed_voters_clone = failed_voters.clone();
            *self.unsafe_recovery_state_mut() = None;
            if !self.is_in_force_leader() {
                error!(self.logger,
                    "Unsafe recovery, lost forced leadership after exiting joint state";
                );
                return;
            }
            self.unsafe_recovery_demote_failed_voters(ctx, failed_voters_clone, syncer_clone);
        } else {
            if self.in_joint_state() {
                info!(self.logger, "Unsafe recovery, exiting joint state");
                if self.is_in_force_leader() {
                    let exit_joint = exit_joint_request(self.region(), self.peer());
                    let (ch, sub) = CmdResChannel::pair();
                    self.on_admin_command(ctx, exit_joint, ch);
                    if let Some(resp) = sub.try_result() && resp.get_header().has_error() {
                        error!(self.logger,
                            "Unsafe recovery, fail to exit joint state";
                            "err" => ?resp.get_header().get_error(),
                        );
                        *self.unsafe_recovery_state_mut()= Some(UnsafeRecoveryState::Failed);
                    }
                } else {
                    error!(self.logger,
                        "Unsafe recovery, lost forced leadership while trying to exit joint state";
                    );
                }
            }

            *self.unsafe_recovery_state_mut() = None;
        }
    }
}
