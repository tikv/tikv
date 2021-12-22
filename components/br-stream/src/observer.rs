// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use engine_traits::KvEngine;
use kvproto::metapb::Region;
use raft::StateRole;
use raftstore::coprocessor::*;
use tikv_util::worker::Scheduler;
use tikv_util::{debug, warn};

use crate::endpoint::Task;

/// An Observer for Backup Stream.
///
/// It observes raftstore internal events, such as:
///   1. Apply command events.
#[derive(Clone)]
pub struct BackupStreamObserver {
    scheduler: Scheduler<Task>,
}

impl BackupStreamObserver {
    /// Create a new `BackupStreamObserver`.
    ///
    /// Events are strong ordered, so `scheduler` must be implemented as
    /// a FIFO queue.
    pub fn new(scheduler: Scheduler<Task>) -> BackupStreamObserver {
        BackupStreamObserver { scheduler }
    }

    pub fn register_to(&self, coprocessor_host: &mut CoprocessorHost<impl KvEngine>) {
        // use 0 as the priority of the cmd observer. should have a higher priority than
        // the `resolved-ts`'s cmd observer
        coprocessor_host
            .registry
            .register_cmd_observer(0, BoxCmdObserver::new(self.clone()));
    }
}

impl Coprocessor for BackupStreamObserver {}

impl<E: KvEngine> CmdObserver<E> for BackupStreamObserver {
    // `BackupStreamObserver::on_flush_applied_cmd_batch` should only invoke if `cmd_batches` is not empty
    // and only leader will trigger this.
    fn on_flush_applied_cmd_batch(
        &self,
        max_level: ObserveLevel,
        cmd_batches: &mut Vec<CmdBatch>,
        _engine: &E,
    ) {
        assert!(!cmd_batches.is_empty());
        debug!(
            "observe backup stream kv";
            "cmd_batches len" => cmd_batches.len(),
            "level" => ?max_level,
        );

        // br stream observe write_cf and default_cf
        if max_level == ObserveLevel::None {
            return;
        }

        // TODO may be we should filter cmd batch here, to reduce the cost of clone.
        let cmd_batches: Vec<_> = cmd_batches
            .iter()
            .filter(|cb| !cb.is_empty() && cb.level > ObserveLevel::None)
            .cloned()
            .collect();
        if cmd_batches.is_empty() {
            return;
        }
        if let Err(e) = self.scheduler.schedule(Task::BatchEvent(cmd_batches)) {
            warn!("backup stream schedule task failed"; "error" => ?e);
        }
    }

    fn on_applied_current_term(&self, _: StateRole, _: &Region) {}
}
