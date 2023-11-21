use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use engine_traits::{KvEngine, RaftEngine};
use futures::channel::mpsc::UnboundedSender;
use kvproto::{brpb::CheckAdminResponse, metapb::RegionEpoch};
use tikv_util::{box_err, info, warn};
use tokio::sync::oneshot;

use super::{metrics, PeerMsg, RaftRouter, SignificantMsg, SignificantRouter};
use crate::coprocessor::{
    AdminObserver, BoxAdminObserver, BoxQueryObserver, Coprocessor, CoprocessorHost, QueryObserver,
};

fn epoch_second_coarse() -> u64 {
    let spec = tikv_util::time::monotonic_coarse_now();
    spec.sec as u64
}

#[derive(Debug, Clone)]
pub struct SnapshotBrWaitApplyRequest {
    pub syncer: SnapshotBrWaitApplySyncer,
    pub expected_epoch: Option<RegionEpoch>,
    pub abort_when_term_change: bool,
}

impl SnapshotBrWaitApplyRequest {
    /// Create a "relax" request for waiting apply.
    /// This only waits to the last index, without checking the region epoch or
    /// leadership migrating.
    pub fn relaxed(syncer: SnapshotBrWaitApplySyncer) -> Self {
        Self {
            syncer,
            expected_epoch: None,
            abort_when_term_change: false,
        }
    }

    /// Create a "strict" request for waiting apply.
    /// This will wait to last applied index, and aborts if the region epoch not
    /// match or the last index may not be committed.
    pub fn strict(syncer: SnapshotBrWaitApplySyncer, epoch: RegionEpoch) -> Self {
        Self {
            syncer,
            expected_epoch: Some(epoch),
            abort_when_term_change: true,
        }
    }
}

pub trait SnapshotBrHandle: Sync + Send {
    fn send_wait_apply(&self, region: u64, req: SnapshotBrWaitApplyRequest) -> crate::Result<()>;
    fn broadcast_wait_apply(&self, req: SnapshotBrWaitApplyRequest) -> crate::Result<()>;
    fn broadcast_check_pending_admin(
        &self,
        tx: UnboundedSender<CheckAdminResponse>,
    ) -> crate::Result<()>;
}

impl<EK: KvEngine, ER: RaftEngine> SnapshotBrHandle for Mutex<RaftRouter<EK, ER>> {
    fn send_wait_apply(&self, region: u64, req: SnapshotBrWaitApplyRequest) -> crate::Result<()> {
        let msg = SignificantMsg::SnapshotBrWaitApply(req);
        metrics::SNAP_BR_WAIT_APPLY_EVENT.sent.inc();
        self.lock().unwrap().significant_send(region, msg)
    }

    fn broadcast_wait_apply(&self, req: SnapshotBrWaitApplyRequest) -> crate::Result<()> {
        let msg_gen = || {
            metrics::SNAP_BR_WAIT_APPLY_EVENT.sent.inc();
            PeerMsg::SignificantMsg(SignificantMsg::SnapshotBrWaitApply(req.clone()))
        };
        self.lock().unwrap().broadcast_normal(msg_gen);
        Ok(())
    }

    fn broadcast_check_pending_admin(
        &self,
        tx: UnboundedSender<CheckAdminResponse>,
    ) -> crate::Result<()> {
        self.lock().unwrap().broadcast_normal(|| {
            PeerMsg::SignificantMsg(SignificantMsg::CheckPendingAdmin(tx.clone()))
        });
        Ok(())
    }
}

pub struct UnimplementedHandle {
    pub reason: String,
}

impl SnapshotBrHandle for UnimplementedHandle {
    fn send_wait_apply(&self, _region: u64, _req: SnapshotBrWaitApplyRequest) -> crate::Result<()> {
        Err(crate::Error::Other(box_err!(
            "send_wait_apply not implemented; note: {}",
            self.reason
        )))
    }

    fn broadcast_wait_apply(&self, _req: SnapshotBrWaitApplyRequest) -> crate::Result<()> {
        Err(crate::Error::Other(box_err!(
            "broadcast_wait_apply not implemented; note: {}",
            self.reason
        )))
    }

    fn broadcast_check_pending_admin(
        &self,
        _tx: UnboundedSender<CheckAdminResponse>,
    ) -> crate::Result<()> {
        Err(crate::Error::Other(box_err!(
            "broadcast_check_pending_admin not implemented; note: {}",
            self.reason
        )))
    }
}

pub struct RejectIngestAndAdmin {
    until: AtomicU64,
    initialized: AtomicBool,
}

impl Default for RejectIngestAndAdmin {
    fn default() -> Self {
        Self {
            until: AtomicU64::new(0),
            initialized: AtomicBool::new(false),
        }
    }
}

impl RejectIngestAndAdmin {
    pub fn register_to(self: &Arc<Self>, coprocessor_host: &mut CoprocessorHost<impl KvEngine>) {
        let reg = &mut coprocessor_host.registry;
        reg.register_query_observer(0, BoxQueryObserver::new(Arc::clone(self)));
        reg.register_admin_observer(0, BoxAdminObserver::new(Arc::clone(self)));
        info!("registered reject ingest and admin coprocessor to TiKV.");
    }

    pub fn allowed(&self) -> bool {
        let mut v = self.until.load(Ordering::Acquire);
        if v == 0 {
            return true;
        }
        let mut expired = v < epoch_second_coarse();
        while expired {
            match self
                .until
                .compare_exchange(v, 0, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => {
                    metrics::SNAP_BR_SUSPEND_COMMAND_LEASE_UNTIL.set(0);
                    metrics::SNAP_BR_LEASE_EVENT.expired.inc();
                    break;
                }
                Err(new_val) => {
                    v = new_val;
                    expired = v < epoch_second_coarse();
                }
            }
        }

        expired
    }

    pub fn connected(&self) -> bool {
        self.initialized.load(Ordering::Acquire)
    }

    /// Extend the lease.
    ///
    /// # Returns
    ///
    /// Whether previously there is a lease.
    pub fn heartbeat(&self, lease: Duration) -> bool {
        let mut v = self.until.load(Ordering::SeqCst);
        let now = epoch_second_coarse();
        let new_lease = now + lease.as_secs();
        let last_lease_valid = v > now;
        loop {
            match self
                .until
                .compare_exchange(v, new_lease, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => break,
                Err(new_val) => {
                    if new_val > new_lease {
                        break;
                    }
                    v = new_val;
                }
            }
        }
        metrics::SNAP_BR_SUSPEND_COMMAND_LEASE_UNTIL.set(new_lease as _);
        if last_lease_valid {
            metrics::SNAP_BR_LEASE_EVENT.renew.inc();
        } else {
            metrics::SNAP_BR_LEASE_EVENT.create.inc();
        }
        last_lease_valid
    }

    pub fn reset(&self) {
        self.until.store(0, Ordering::SeqCst);
        metrics::SNAP_BR_SUSPEND_COMMAND_LEASE_UNTIL.set(0);
        metrics::SNAP_BR_LEASE_EVENT.reset.inc();
    }
}

impl Coprocessor for Arc<RejectIngestAndAdmin> {
    fn start(&self) {
        self.initialized.store(true, Ordering::Release)
    }

    fn stop(&self) {
        self.initialized.store(false, Ordering::Release)
    }
}

impl QueryObserver for Arc<RejectIngestAndAdmin> {
    fn pre_propose_query(
        &self,
        cx: &mut crate::coprocessor::ObserverContext<'_>,
        reqs: &mut Vec<kvproto::raft_cmdpb::Request>,
    ) -> crate::coprocessor::Result<()> {
        if self.allowed() {
            return Ok(());
        }
        for req in reqs {
            if req.has_ingest_sst() {
                // Note: this will reject the batch of commands, which isn't so effective.
                // But we cannot reject proposing a subset of command for now...
                cx.bypass = true;
                metrics::SNAP_BR_SUSPEND_COMMAND_TYPE
                    .with_label_values(&["Ingest"])
                    .inc();
                return Err(box_err!(
                    "trying to propose ingest while preparing snapshot backup, abort it"
                ));
            }
        }
        Ok(())
    }
}

impl AdminObserver for Arc<RejectIngestAndAdmin> {
    fn pre_propose_admin(
        &self,
        _: &mut crate::coprocessor::ObserverContext<'_>,
        admin: &mut kvproto::raft_cmdpb::AdminRequest,
    ) -> crate::coprocessor::Result<()> {
        if self.allowed() {
            return Ok(());
        }
        metrics::SNAP_BR_SUSPEND_COMMAND_TYPE
            .with_label_values(&[&format!("{:?}", admin.get_cmd_type())])
            .inc();
        Err(box_err!(
            "rejecting proposing admin commands while preparing snapshot backup: rejected {:?}",
            admin
        ))
    }
}

/// A syncer for wait apply.
/// The sender used for constructing this structure will:
/// Be closed, if the `abort` has been called.
/// Send the report id to the caller, if all replicas of this Syncer has been
/// dropped.
#[derive(Clone, Debug)]
pub struct SnapshotBrWaitApplySyncer {
    channel: Option<Arc<Mutex<Option<oneshot::Sender<u64>>>>>,
    report_id: u64,
}

impl Drop for SnapshotBrWaitApplySyncer {
    fn drop(&mut self) {
        let chan = self
            .channel
            .take()
            .expect("channel taken, maybe dropped twice");
        if let Ok(ch) = Arc::try_unwrap(chan) {
            if let Some(ch) = ch.into_inner().unwrap() {
                if ch.send(self.report_id).is_err() {
                    warn!("reply waitapply states failure."; "report" => self.report_id);
                }
                metrics::SNAP_BR_WAIT_APPLY_EVENT.finished.inc()
            } else {
                warn!("wait apply aborted."; "report" => self.report_id);
            }
        }
    }
}

impl SnapshotBrWaitApplySyncer {
    pub fn new(report_id: u64, sender: oneshot::Sender<u64>) -> Self {
        Self {
            report_id,
            channel: Some(Arc::new(Mutex::new(Some(sender)))),
        }
    }

    pub fn abort(self, reason: AbortReason) {
        warn!("aborting wait apply."; "reason" => ?reason, "id" => %self.report_id);
        match reason {
            AbortReason::EpochNotMatch(_) => {
                metrics::SNAP_BR_WAIT_APPLY_EVENT.epoch_not_match.inc()
            }
            AbortReason::TermMismatch { .. } => {
                metrics::SNAP_BR_WAIT_APPLY_EVENT.term_not_match.inc()
            }
            AbortReason::Duplicated => metrics::SNAP_BR_WAIT_APPLY_EVENT.duplicated.inc(),
        }
        self.channel.as_ref().unwrap().lock().unwrap().take();
    }
}

#[derive(Debug)]
pub enum AbortReason {
    EpochNotMatch(kvproto::errorpb::EpochNotMatch),
    TermMismatch { expected: u64, current: u64 },
    Duplicated,
}

#[derive(Debug)]
pub enum SnapshotBrState {
    // This state is set by the leader peer fsm. Once set, it sync and check leader commit index
    // and force forward to last index once follower appended and then it also is checked
    // every time this peer applies a the last index, if the last index is met, this state is
    // reset / droppeds. The syncer is dropped and send the response to the invoker.
    WaitLogApplyToLast {
        target_index: u64,
        valid_for_term: Option<u64>,
        syncer: SnapshotBrWaitApplySyncer,
    },
}
