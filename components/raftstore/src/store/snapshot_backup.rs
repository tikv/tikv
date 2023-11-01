use std::{
    future::Future,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, UNIX_EPOCH},
};

use engine_traits::{KvEngine, RaftEngine};
use kvproto::{
    errorpb::{Error, StaleCommand},
    metapb::{self, Region},
};
use tikv_util::{box_err, error, info, time::Instant as TiInstant, warn};
use tokio::sync::oneshot;
use txn_types::TimeStamp;

use super::{
    unsafe_recovery::InvokeClosureOnDrop, PeerMsg, RaftRouter, SignificantMsg, SignificantRouter,
};
use crate::coprocessor::{AdminObserver, CmdObserver, Coprocessor, QueryObserver};

fn epoch_second() -> u64 {
    TimeStamp::physical_now() / 1000
}

fn epoch_second_coarse() -> u64 {
    let spec = tikv_util::time::monotonic_coarse_now();
    spec.sec as u64
}

pub trait SnapshotBrHandle {
    fn send_wait_apply(&self, region: u64, syncer: SnapshotBrWaitApplySyncer) -> crate::Result<()>;
    fn broadcast_wait_apply(&self, syncer: SnapshotBrWaitApplySyncer);
}

impl<EK: KvEngine, ER: RaftEngine> SnapshotBrHandle for Mutex<RaftRouter<EK, ER>> {
    fn send_wait_apply(&self, region: u64, syncer: SnapshotBrWaitApplySyncer) -> crate::Result<()> {
        let msg = SignificantMsg::SnapshotBrWaitApply(syncer);
        self.lock().unwrap().significant_send(region, msg)
    }

    fn broadcast_wait_apply(&self, syncer: SnapshotBrWaitApplySyncer) {
        let msg_gen =
            || PeerMsg::SignificantMsg(SignificantMsg::SnapshotBrWaitApply(syncer.clone()));
        self.lock().unwrap().broadcast_normal(msg_gen);
    }
}

struct RejectIngestAndAdmin {
    until: AtomicU64,
}

impl RejectIngestAndAdmin {
    fn disabled(&self) -> bool {
        let mut v = self.until.load(Ordering::Acquire);
        if v == 0 {
            return true;
        }
        let mut expired = v > epoch_second_coarse();
        while expired {
            match self
                .until
                .compare_exchange(v, 0, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => break,
                Err(new_val) => {
                    v = new_val;
                    expired = v > epoch_second_coarse();
                }
            }
        }

        expired
    }

    fn heartbeat(&self, lease: Duration) -> bool {
        let mut v = self.until.load(Ordering::SeqCst);
        let now = epoch_second_coarse();
        let new_lease = now + lease.as_secs();
        let expired = v > now;
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
        expired
    }
}

impl Coprocessor for Arc<RejectIngestAndAdmin> {}

impl QueryObserver for Arc<RejectIngestAndAdmin> {
    fn pre_propose_query(
        &self,
        cx: &mut crate::coprocessor::ObserverContext<'_>,
        reqs: &mut Vec<kvproto::raft_cmdpb::Request>,
    ) -> crate::coprocessor::Result<()> {
        if self.disabled() {
            return Ok(());
        }
        for req in reqs {
            if req.has_ingest_sst() {
                // Note: this will reject the batch of commands, which isn't so effective.
                // But we cannot reject proposing a subset of command for now...
                cx.bypass = true;
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
        if self.disabled() {
            return Ok(());
        }
        Err(box_err!(
            "rejecting proposing admin commands while preparing snapshot backup: rejected {:?}",
            admin
        ))
    }
}

// Syncer only send to leader in 2nd BR restore
#[derive(Clone, Debug)]
pub struct SnapshotBrWaitApplySyncer {
    channel: Option<Arc<Mutex<Option<oneshot::Sender<u64>>>>>,
    report_id: u64,
}

impl Drop for SnapshotBrWaitApplySyncer {
    fn drop(&mut self) {
        let chan = self.channel.take().expect("channel taken");
        if let Ok(ch) = Arc::try_unwrap(chan) {
            if let Some(ch) = ch.into_inner().unwrap() {
                if ch.send(self.report_id).is_err() {
                    warn!("reply waitapply states failure."; "report" => self.report_id);
                }
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

    pub fn abort(self) {
        self.channel.as_ref().unwrap().lock().unwrap().take();
    }
}

#[derive(Debug)]
pub enum SnapshotBrState {
    // This state is set by the leader peer fsm. Once set, it sync and check leader commit index
    // and force forward to last index once follower appended and then it also is checked
    // every time this peer applies a the last index, if the last index is met, this state is
    // reset / droppeds. The syncer is dropped and send the response to the invoker.
    WaitLogApplyToLast {
        target_index: u64,
        syncer: SnapshotBrWaitApplySyncer,
    },
}
