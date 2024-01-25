// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::kvrpcpb::ExtraOp;
use tikv_kv::Snapshot;
use tracker::{get_tls_tracker_token, TrackerToken};

use crate::storage::{
    kv::Statistics,
    lock_manager::LockManager,
    txn::{
        commands::{Command, WriteContext, WriteResult},
        ProcessResult,
    },
};

pub(super) struct Task {
    cid: u64,
    tracker: TrackerToken,
    cmd: Option<Command>,
    extra_op: ExtraOp,
}

impl Task {
    /// Creates a task for a running command.
    pub(super) fn new(cid: u64, cmd: Command) -> Task {
        let tracker = get_tls_tracker_token();
        Task {
            cid,
            tracker,
            cmd: Some(cmd),
            extra_op: ExtraOp::Noop,
        }
    }

    pub(super) fn cid(&self) -> u64 {
        self.cid
    }

    pub(super) fn tracker(&self) -> TrackerToken {
        self.tracker
    }

    pub(super) fn cmd(&self) -> &Command {
        self.cmd.as_ref().unwrap()
    }

    pub(super) fn cmd_mut(&mut self) -> &mut Command {
        self.cmd.as_mut().unwrap()
    }

    pub(super) fn extra_op(&self) -> ExtraOp {
        self.extra_op
    }

    pub(super) fn set_extra_op(&mut self, extra_op: ExtraOp) {
        self.extra_op = extra_op
    }

    pub(super) fn process_write<S: Snapshot, L: LockManager>(
        mut self,
        snapshot: S,
        context: WriteContext<'_, L>,
    ) -> super::Result<WriteResult> {
        let cmd = self.cmd.take().unwrap();
        cmd.process_write(snapshot, context)
    }

    pub(super) fn process_read<S: Snapshot>(
        mut self,
        snapshot: S,
        statistics: &mut Statistics,
    ) -> super::Result<ProcessResult> {
        let cmd = self.cmd.take().unwrap();
        cmd.process_read(snapshot, statistics)
    }
}
