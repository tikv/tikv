// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use kvproto::kvrpcpb::ExtraOp;
use tikv_kv::Snapshot;
use tikv_util::memory::{HeapSize, MemoryQuota, MemoryQuotaExceeded, OwnedAllocated};
use tracker::{get_tls_tracker_token, TrackerToken};

use crate::storage::{
    kv::Statistics,
    lock_manager::LockManager,
    metrics::*,
    txn::{
        commands::{Command, WriteContext, WriteResult},
        ProcessResult,
    },
};

pub(super) struct Task {
    cid: u64,
    tracker_token: TrackerToken,
    cmd: Option<Command>,
    extra_op: ExtraOp,
    /// The owned_quota is allocated when Task is created, and freed when Task
    /// is dropped.
    owned_quota: Option<OwnedAllocated>,
}

impl Task {
    /// Creates a task for a running command. The TLS tracker token is set
    /// in the future processing logic in the kv.rs.
    pub(super) fn allocate(
        cid: u64,
        cmd: Command,
        memory_quota: Arc<MemoryQuota>,
    ) -> Result<Self, MemoryQuotaExceeded> {
        let tracker_token = get_tls_tracker_token();
        let cmd_size = cmd.approximate_heap_size();
        let mut task = Task {
            cid,
            tracker_token,
            cmd: Some(cmd),
            extra_op: ExtraOp::Noop,
            owned_quota: None,
        };
        let mut quota = OwnedAllocated::new(memory_quota);
        quota.alloc(cmd_size)?;
        SCHED_TXN_MEMORY_QUOTA
            .in_use
            .set(quota.source().in_use() as i64);
        task.owned_quota = Some(quota);

        Ok(task)
    }

    /// Creates a task without considering memory count. This is just a
    /// temporary solution for the re-schedule command task generations, it
    /// would be deprecated after the re-schedule command and callback
    /// processing are refactored. Do NOT use this function in other places.
    pub(super) fn force_create(cid: u64, cmd: Command) -> Self {
        let tracker_token = get_tls_tracker_token();
        Task {
            cid,
            tracker_token,
            cmd: Some(cmd),
            extra_op: ExtraOp::Noop,
            owned_quota: None,
        }
    }

    pub(super) fn cid(&self) -> u64 {
        self.cid
    }

    pub(super) fn tracker_token(&self) -> TrackerToken {
        self.tracker_token
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

    /// Set_extra_op is called after getting snapshot and before the command is
    /// processed.
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

#[cfg(test)]
mod tests {
    use kvproto::kvrpcpb::PrewriteRequest;

    use super::*;
    use crate::storage::TypedCommand;

    #[test]
    fn test_alloc_memory_quota() {
        let p = PrewriteRequest::default();
        let cmd: TypedCommand<_> = p.into();
        let quota = Arc::new(MemoryQuota::new(1 << 32));
        let task = Task::allocate(0, cmd.cmd, quota.clone()).unwrap();
        assert_ne!(quota.in_use(), 0);
        drop(task);
        assert_eq!(quota.in_use(), 0);
    }
}
