use tikv::{
    server::lock_manager::waiter_manager::Callback,
    storage::{
        lock_manager::{DiagnosticContext, Lock, LockManager as LockManagerTrait, WaitTimeout},
        ProcessResult, StorageCallback,
    },
};
use txn_types::TimeStamp;

#[derive(Copy, Clone)]
pub struct HackedLockManager {}

#[allow(dead_code)]
#[allow(unused_variables)]
impl LockManagerTrait for HackedLockManager {
    fn wait_for(
        &self,
        start_ts: TimeStamp,
        cb: StorageCallback,
        pr: ProcessResult,
        lock: Lock,
        is_first_lock: bool,
        timeout: Option<WaitTimeout>,
        diag_ctx: DiagnosticContext,
    ) {
        unimplemented!()
    }

    fn wake_up(
        &self,
        lock_ts: TimeStamp,
        hashes: Vec<u64>,
        commit_ts: TimeStamp,
        is_pessimistic_txn: bool,
    ) {
        unimplemented!()
    }

    fn has_waiter(&self) -> bool {
        todo!()
    }

    fn dump_wait_for_entries(&self, cb: Callback) {
        todo!()
    }
}

impl HackedLockManager {
    pub fn new() -> Self {
        Self {}
    }
    pub fn stop(&mut self) {}
}
