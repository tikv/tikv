// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Mutex;

use collections::HashSet;
use crossbeam::channel::SendError;
use engine_traits::{KvEngine, RaftEngine};
use kvproto::pdpb::StoreReport;
use tikv_util::box_err;

use super::{
    peer::{
        UnsafeRecoveryFillOutReportSyncer, UnsafeRecoveryForceLeaderSyncer,
        UnsafeRecoveryWaitApplySyncer,
    },
    PeerMsg, RaftRouter, SignificantMsg, SignificantRouter, StoreMsg,
};
use crate::Result;

/// A handle for PD to schedule online unsafe recovery commands back to
/// raftstore.
pub trait UnsafeRecoveryHandle: Sync + Send {
    fn send_enter_force_leader(
        &self,
        region_id: u64,
        syncer: UnsafeRecoveryForceLeaderSyncer,
        failed_stores: HashSet<u64>,
    ) -> Result<()>;

    fn broadcast_exit_force_leader(&self);

    fn broadcast_wait_apply(&self, syncer: UnsafeRecoveryWaitApplySyncer);

    fn broadcast_fill_out_report(&self, syncer: UnsafeRecoveryFillOutReportSyncer);

    fn send_report(&self, report: StoreReport) -> Result<()>;
}

impl<EK: KvEngine, ER: RaftEngine> UnsafeRecoveryHandle for Mutex<RaftRouter<EK, ER>> {
    fn send_enter_force_leader(
        &self,
        region_id: u64,
        syncer: UnsafeRecoveryForceLeaderSyncer,
        failed_stores: HashSet<u64>,
    ) -> Result<()> {
        let router = self.lock().unwrap();
        router.significant_send(
            region_id,
            SignificantMsg::EnterForceLeaderState {
                syncer,
                failed_stores,
            },
        )
    }

    fn broadcast_exit_force_leader(&self) {
        let router = self.lock().unwrap();
        router.broadcast_normal(|| PeerMsg::SignificantMsg(SignificantMsg::ExitForceLeaderState));
    }

    fn broadcast_wait_apply(&self, syncer: UnsafeRecoveryWaitApplySyncer) {
        let router = self.lock().unwrap();
        router.broadcast_normal(|| {
            PeerMsg::SignificantMsg(SignificantMsg::UnsafeRecoveryWaitApply(syncer.clone()))
        });
    }

    fn broadcast_fill_out_report(&self, syncer: UnsafeRecoveryFillOutReportSyncer) {
        let router = self.lock().unwrap();
        router.broadcast_normal(|| {
            PeerMsg::SignificantMsg(SignificantMsg::UnsafeRecoveryFillOutReport(syncer.clone()))
        });
    }

    fn send_report(&self, report: StoreReport) -> Result<()> {
        let router = self.lock().unwrap();
        match router.force_send_control(StoreMsg::UnsafeRecoveryReport(report)) {
            Ok(()) => Ok(()),
            Err(SendError(_)) => Err(box_err!("fail to send unsafe recovery store report")),
        }
    }
}
