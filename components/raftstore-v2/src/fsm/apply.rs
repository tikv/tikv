// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use batch_system::{Fsm, FsmScheduler, Mailbox};
use crossbeam::channel::TryRecvError;
use engine_traits::{FlushState, KvEngine, SstApplyState, TabletRegistry};
use fail::fail_point;
use futures::{compat::Future01CompatExt, FutureExt, StreamExt};
use kvproto::{metapb, raft_serverpb::RegionLocalState};
use pd_client::BucketStat;
use raftstore::{
    coprocessor::CoprocessorHost,
    store::{Config, ReadTask},
};
use slog::Logger;
use sst_importer::SstImporter;
use tikv_util::{
    mpsc::future::{self, Receiver, Sender, WakePolicy},
    timer::GLOBAL_TIMER_HANDLE,
    worker::Scheduler,
    yatp_pool::FuturePool,
};

use crate::{
    operation::{CatchUpLogs, DataTrace},
    raft::Apply,
    router::{ApplyRes, ApplyTask, PeerMsg},
    TabletTask,
};

/// A trait for reporting apply result.
///
/// Using a trait to make signiture simpler.
pub trait ApplyResReporter {
    fn report(&self, apply_res: ApplyRes);

    fn redirect_catch_up_logs(&self, c: CatchUpLogs);
}

impl<F: Fsm<Message = PeerMsg>, S: FsmScheduler<Fsm = F>> ApplyResReporter for Mailbox<F, S> {
    fn report(&self, apply_res: ApplyRes) {
        // TODO: check shutdown.
        let _ = self.force_send(PeerMsg::ApplyRes(apply_res));
    }

    fn redirect_catch_up_logs(&self, c: CatchUpLogs) {
        let msg = PeerMsg::RedirectCatchUpLogs(c);
        let _ = self.force_send(msg);
    }
}

/// Schedule task to `ApplyFsm`.
#[derive(Clone)]
pub struct ApplyScheduler {
    sender: Sender<ApplyTask>,
}

impl ApplyScheduler {
    #[inline]
    pub fn send(&self, task: ApplyTask) {
        // TODO: ignore error when shutting down.
        self.sender.send(task).unwrap();
    }
}

pub struct ApplyFsm<EK: KvEngine, R> {
    apply: Apply<EK, R>,
    receiver: Receiver<ApplyTask>,
}

impl<EK: KvEngine, R> ApplyFsm<EK, R> {
    pub fn new(
        cfg: &Config,
        peer: metapb::Peer,
        region_state: RegionLocalState,
        res_reporter: R,
        tablet_registry: TabletRegistry<EK>,
        read_scheduler: Scheduler<ReadTask<EK>>,
        tablet_scheduler: Scheduler<TabletTask<EK>>,
        high_priority_pool: FuturePool,
        flush_state: Arc<FlushState>,
        sst_apply_state: SstApplyState,
        log_recovery: Option<Box<DataTrace>>,
        applied_term: u64,
        buckets: Option<BucketStat>,
        sst_importer: Arc<SstImporter<EK>>,
        coprocessor_host: CoprocessorHost<EK>,
        logger: Logger,
    ) -> (ApplyScheduler, Self) {
        let (tx, rx) = future::unbounded(WakePolicy::Immediately);
        let apply = Apply::new(
            cfg,
            peer,
            region_state,
            res_reporter,
            tablet_registry,
            read_scheduler,
            flush_state,
            sst_apply_state,
            log_recovery,
            applied_term,
            buckets,
            sst_importer,
            coprocessor_host,
            tablet_scheduler,
            high_priority_pool,
            logger,
        );
        (
            ApplyScheduler { sender: tx },
            Self {
                apply,
                receiver: rx,
            },
        )
    }
}

impl<EK: KvEngine, R: ApplyResReporter> ApplyFsm<EK, R> {
    pub async fn handle_all_tasks(&mut self) {
        loop {
            let timeout = GLOBAL_TIMER_HANDLE
                .delay(Instant::now() + Duration::from_secs(10))
                .compat();
            let res = futures::select! {
                res = self.receiver.next().fuse() => res,
                _ = timeout.fuse() => None,
            };
            self.apply.on_start_apply();
            let mut task = match res {
                Some(r) => r,
                None => {
                    self.apply.release_memory();
                    match self.receiver.next().await {
                        Some(t) => t,
                        None => return,
                    }
                }
            };
            loop {
                fail_point!("before_handle_tasks");
                match task {
                    // TODO: flush by buffer size.
                    ApplyTask::CommittedEntries(ce) => self.apply.apply_committed_entries(ce).await,
                    ApplyTask::Snapshot(snap_task) => self.apply.schedule_gen_snapshot(snap_task),
                    ApplyTask::UnsafeWrite(raw_write) => {
                        self.apply.apply_unsafe_write(raw_write).await
                    }
                    ApplyTask::ManualFlush => self.apply.on_manual_flush().await,
                    ApplyTask::RefreshBucketStat(bucket_meta) => {
                        self.apply.on_refresh_buckets(bucket_meta)
                    }
                    ApplyTask::CaptureApply(capture_change) => {
                        self.apply.on_capture_apply(capture_change)
                    }
                }

                self.apply.maybe_flush().await;

                // Perhaps spin sometime?
                match self.receiver.try_recv() {
                    Ok(t) => task = t,
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return,
                }
            }
            let written_bytes = self.apply.flush();
            self.apply.maybe_reschedule(written_bytes).await;
        }
    }
}
