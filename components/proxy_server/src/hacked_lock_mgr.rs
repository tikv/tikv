use kvproto::metapb::RegionEpoch;
use tikv::{
    server::lock_manager::{waiter_manager, waiter_manager::Callback},
    storage::{
        lock_manager::{
            DiagnosticContext, KeyLockWaitInfo, LockManager as LockManagerTrait, LockWaitToken,
            UpdateWaitForEvent, WaitTimeout,
        },
        Error as StorageError, ProcessResult, StorageCallback,
    },
};
use txn_types::TimeStamp;

#[derive(Copy, Clone)]
pub struct HackedLockManager {}

#[allow(dead_code)]
#[allow(unused_variables)]
impl LockManagerTrait for HackedLockManager {
    fn allocate_token(&self) -> LockWaitToken {
        unimplemented!();
    }

    fn wait_for(
        &self,
        token: LockWaitToken,
        region_id: u64,
        region_epoch: RegionEpoch,
        term: u64,
        start_ts: TimeStamp,
        wait_info: KeyLockWaitInfo,
        is_first_lock: bool,
        timeout: Option<WaitTimeout>,
        cancel_callback: Box<dyn FnOnce(StorageError) + Send>,
        diag_ctx: DiagnosticContext,
    ) {
        unimplemented!();
    }

    fn update_wait_for(&self, updated_items: Vec<UpdateWaitForEvent>) {
        unimplemented!();
    }

    fn remove_lock_wait(&self, token: LockWaitToken) {
        unimplemented!();
    }

    fn has_waiter(&self) -> bool {
        unimplemented!();
    }

    fn dump_wait_for_entries(&self, cb: waiter_manager::Callback) {
        unimplemented!();
    }
}

impl HackedLockManager {
    pub fn new() -> Self {
        Self {}
    }
    pub fn stop(&mut self) {}
}
