// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::{thread, time::Duration};

use txn_types::Key;

use crate::storage::{
    kv::WriteData,
    lock_manager::LockManager,
    txn::{
        commands::{
            Command, CommandExt, ResponsePolicy, TypedCommand, WriteCommand, WriteContext,
            WriteResult,
        },
        Result,
    },
    ProcessResult, Snapshot,
};

command! {
    /// **Testing functionality:** Latch the given keys for given duration.
    ///
    /// This means other write operations that involve these keys will be blocked.
    Pause:
        cmd_ty => (),
        display => "kv::command::pause keys:({}) {} ms | {:?}", (keys.len, duration, ctx),
        content => {
            /// The keys to hold latches on.
            keys: Vec<Key>,
            /// The amount of time in milliseconds to latch for.
            duration: u64,
        }
}

impl CommandExt for Pause {
    ctx!();
    tag!(pause);
    write_bytes!(keys: multiple);
    gen_lock!(keys: multiple);
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for Pause {
    fn process_write(self, _snapshot: S, _context: WriteContext<'_, L>) -> Result<WriteResult> {
        thread::sleep(Duration::from_millis(self.duration));
        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write: WriteData::default(),
            rows: 0,
            pr: ProcessResult::Res,
            lock_info: None,
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
        })
    }
}
