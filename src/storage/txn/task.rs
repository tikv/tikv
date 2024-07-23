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

/// ExecutionContext is used store the query execution context of the command,
/// it is expected to be pass through the whole execution process from
/// scheduler, to raft and engine, to track the status of the command from the
/// perspective of SQL statement or transaction.
/// It's expected to be a read-only struct.
#[derive(Debug, Clone)]
pub struct ExecutionContext {
    tag: CommandKind,
    cid: u64,
    tracker_token: TrackerToken,
    extra_op: ExtraOp,
    owned_quota: Option<OwnedAllocated>,
}

impl ExecutionContext {
    pub fn new(
        tag: CommandKind,
        cid: u64,
        tracker_token: TrackerToken,
        extra_op: ExtraOp,
        owned_quota: Option<OwnedAllocated>,
    ) -> Self {
        ExecutionContext {
            tag,
            cid,
            tracker_token,
            extra_op,
            owned_quota,
        }
    }

    pub fn get_tag(&self) -> CommandKind {
        self.tag
    }

    pub fn get_cid(&self) -> u64 {
        self.cid
    }

    pub fn get_tracker_token(&self) -> TrackerToken {
        self.tracker_token
    }
}

pub(super) struct Task {
    cmd: Option<Command>,
    exec_ctx: ExecutionContext,
}

impl Task {
    /// Creates a task for a running command.
    pub(super) fn allocate(
        cid: u64,
        cmd: Command,
        memory_quota: Arc<MemoryQuota>,
    ) -> Result<Self, MemoryQuotaExceeded> {
        let tracker = get_tls_tracker_token();
        let tag = cmd.tag();
        let mut task = Task {
            cmd: Some(cmd),
            exec_ctx: ExecutionContext::new(tag, cid, tracker, ExtraOp::Noop, None),
        };
        task.alloc_memory_quota(memory_quota)?;

        Ok(task)
    }

    pub(super) fn cid(&self) -> u64 {
        self.exec_ctx.cid
    }

    pub(super) fn tracker(&self) -> TrackerToken {
        self.exec_ctx.tracker_token
    }

    pub(super) fn cmd(&self) -> &Command {
        self.cmd.as_ref().unwrap()
    }

    pub(super) fn cmd_mut(&mut self) -> &mut Command {
        self.cmd.as_mut().unwrap()
    }

    pub(super) fn extra_op(&self) -> ExtraOp {
        self.exec_ctx.extra_op
    }

    /// Set_extra_op is called after getting snapshot and before the command is
    /// processed.
    pub(super) fn set_extra_op(&mut self, extra_op: ExtraOp) {
        self.exec_ctx.extra_op = extra_op
    }

    fn alloc_memory_quota(
        &mut self,
        memory_quota: Arc<MemoryQuota>,
    ) -> Result<(), MemoryQuotaExceeded> {
        if self.exec_ctx.owned_quota.is_none() {
            let mut owned = OwnedAllocated::new(memory_quota);
            owned.alloc(self.cmd.approximate_heap_size())?;
            SCHED_TXN_MEMORY_QUOTA
                .in_use
                .set(owned.source().in_use() as i64);
            self.exec_ctx.owned_quota = Some(owned);
        }
        Ok(())
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

    pub fn get_exec_ctx(&self) -> ExecutionContext {
        self.exec_ctx.clone()
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
        let mut task = Task::allocate(0, cmd.cmd, quota.clone()).unwrap();
        task.alloc_memory_quota(quota.clone()).unwrap();
        assert_ne!(quota.in_use(), 0);
        let in_use = quota.in_use();
        task.alloc_memory_quota(quota.clone()).unwrap();
        let in_use_new = quota.in_use();
        assert_eq!(in_use, in_use_new);
        drop(task);
        assert_eq!(quota.in_use(), 0);
    }
}
