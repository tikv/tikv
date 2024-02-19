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
    tracker: TrackerToken,
    cmd: Option<Command>,
    extra_op: ExtraOp,
    owned_quota: Option<OwnedAllocated>,
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
            owned_quota: None,
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

    pub(super) fn alloc_memory_quota(
        &mut self,
        memory_quota: Arc<MemoryQuota>,
    ) -> Result<(), MemoryQuotaExceeded> {
        if self.owned_quota.is_none() {
            let mut owned = OwnedAllocated::new(memory_quota);
            owned.alloc(self.cmd.approximate_heap_size())?;
            SCHED_TXN_MEMORY_QUOTA
                .in_use
                .set(owned.source().in_use() as i64);
            self.owned_quota = Some(owned);
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
        let mut task = Task::new(0, cmd.cmd);
        let quota = Arc::new(MemoryQuota::new(1 << 32));
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
