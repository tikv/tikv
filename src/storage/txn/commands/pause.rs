// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::kv::WriteData;
use crate::storage::lock_manager::LockManager;
use crate::storage::txn::commands::{
    Command, CommandExt, TypedCommand, WriteCommand, WriteContext, WriteResult,
};
use crate::storage::txn::Result;
use crate::storage::{ProcessResult, Snapshot};
use pd_client::PdClient;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use txn_types::Key;

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

impl<S: Snapshot, L: LockManager, P: PdClient + 'static> WriteCommand<S, L, P> for Pause {
    fn process_write<'a>(
        self,
        _snapshot: S,
        _lock_mgr: &'a L,
        _pd_client: Arc<P>,
        _context: WriteContext<'a>,
    ) -> Result<WriteResult> {
        thread::sleep(Duration::from_millis(self.duration));
        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write: WriteData::default(),
            rows: 0,
            pr: ProcessResult::Res,
            lock_info: None,
        })
    }
}
