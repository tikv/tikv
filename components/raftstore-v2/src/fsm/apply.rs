// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use batch_system::{Fsm, FsmScheduler, Mailbox};
use crossbeam::channel::TryRecvError;
use engine_traits::{FlushState, KvEngine, TabletRegistry};
use futures::{compat::Future01CompatExt, FutureExt, StreamExt};
use kvproto::{metapb, raft_serverpb::RegionLocalState};
use raftstore::store::ReadTask;
use slog::Logger;
use tikv_util::{
    mpsc::future::{self, Receiver, Sender, WakePolicy},
    timer::GLOBAL_TIMER_HANDLE,
    worker::Scheduler,
};

use crate::{
    operation::DataTrace,
    raft::Apply,
    router::{ApplyRes, ApplyTask, PeerMsg},
};

/// A trait for reporting apply result.
///
/// Using a trait to make signiture simpler.
pub trait ApplyResReporter {
    fn report(&self, apply_res: ApplyRes);
}

impl<F: Fsm<Message = PeerMsg>, S: FsmScheduler<Fsm = F>> ApplyResReporter for Mailbox<F, S> {
    fn report(&self, apply_res: ApplyRes) {
        // TODO: check shutdown.
        let _ = self.force_send(PeerMsg::ApplyRes(apply_res));
    }
}

/// Schedule task to `ApplyFsm`.
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
        peer: metapb::Peer,
        region_state: RegionLocalState,
        res_reporter: R,
        tablet_registry: TabletRegistry<EK>,
        read_scheduler: Scheduler<ReadTask<EK>>,
        flush_state: Arc<FlushState>,
        log_recovery: Option<Box<DataTrace>>,
        applied_index: u64,
        applied_term: u64,
        logger: Logger,
    ) -> (ApplyScheduler, Self) {
        let (tx, rx) = future::unbounded(WakePolicy::Immediately);
        let apply = Apply::new(
            peer,
            region_state,
            res_reporter,
            tablet_registry,
            read_scheduler,
            flush_state,
            log_recovery,
            applied_index,
            applied_term,
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
                match task {
                    // TODO: flush by buffer size.
                    ApplyTask::CommittedEntries(ce) => self.apply.apply_committed_entries(ce).await,
                    ApplyTask::Snapshot(snap_task) => self.apply.schedule_gen_snapshot(snap_task),
                    ApplyTask::UnsafeWrite(raw_write) => self.apply.apply_unsafe_write(raw_write),
                    ApplyTask::ManualFlush => self.apply.on_manual_flush(),
                }

                // TODO: yield after some time.

                // Perhaps spin sometime?
                match self.receiver.try_recv() {
                    Ok(t) => task = t,
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return,
                }
            }
            self.apply.flush();
        }
    }
}
