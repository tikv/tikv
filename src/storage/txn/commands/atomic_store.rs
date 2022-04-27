// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use crate::storage::kv::{Modify, WriteData};
use crate::storage::lock_manager::LockManager;
use crate::storage::txn::commands::{
    Command, CommandExt, ResponsePolicy, TypedCommand, WriteCommand, WriteContext, WriteResult,
};
use crate::storage::txn::Result;
use crate::storage::{ProcessResult, Snapshot};
use engine_traits::CfName;

command! {
    /// Run Put or Delete for keys which may be changed by `RawCompareAndSwap`.
    RawAtomicStore:
        cmd_ty => (),
        display => "kv::command::atomic_store {:?}", (ctx),
        content => {
            /// The set of mutations to apply.
            cf: CfName,
            mutations: Vec<Modify>,
        }
}

impl CommandExt for RawAtomicStore {
    ctx!();
    tag!(raw_atomic_store);
    gen_lock!(mutations: multiple(|x| x.key()));

    fn write_bytes(&self) -> usize {
        self.mutations.iter().map(|x| x.size()).sum()
    }
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for RawAtomicStore {
    fn process_write(self, _: S, _: WriteContext<'_, L>) -> Result<WriteResult> {
        let rows = self.mutations.len();
        let (mutations, ctx) = (self.mutations, self.ctx);
        let mut to_be_write = WriteData::from_modifies(mutations);
        to_be_write.set_allowed_on_disk_almost_full();
        Ok(WriteResult {
            ctx,
            to_be_write,
            rows,
            pr: ProcessResult::Res,
            lock_info: None,
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
        })
    }
}
