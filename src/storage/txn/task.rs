// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use kvproto::kvrpcpb::ExtraOp;
use tikv_kv::Snapshot;
use tikv_util::memory::{HeapSize, MemoryQuota, MemoryQuotaExceeded};
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
    memory_quota: Option<(Arc<MemoryQuota>, usize)>,
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
            memory_quota: None,
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
        if self.memory_quota.is_none() {
            let bytes = self.cmd.heap_size();
            memory_quota.alloc(bytes)?;
            self.memory_quota = Some((memory_quota, bytes));
        }
        Ok(())
    }

    pub(super) fn process_write<S: Snapshot, L: LockManager>(
        mut self,
        snapshot: S,
        context: WriteContext<'_, L>,
    ) -> super::Result<WriteResult> {
        let cmd = self.cmd.take().unwrap();
        let res = cmd.process_write(snapshot, context);
        if let Some((memory_quota, bytes)) = self.memory_quota.take() {
            memory_quota.free(bytes);
        }
        res
    }

    pub(super) fn process_read<S: Snapshot>(
        mut self,
        snapshot: S,
        statistics: &mut Statistics,
    ) -> super::Result<ProcessResult> {
        let cmd = self.cmd.take().unwrap();
        let res = cmd.process_read(snapshot, statistics);
        if let Some((memory_quota, bytes)) = self.memory_quota.take() {
            memory_quota.free(bytes);
        }
        res
    }
}

impl Drop for Task {
    fn drop(&mut self) {
        if let Some((memory_quota, bytes)) = self.memory_quota.take() {
            memory_quota.free(bytes);
        }
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
