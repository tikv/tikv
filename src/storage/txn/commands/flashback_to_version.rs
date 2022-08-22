// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use tikv_kv::WriteData;
// #[PerformanceCriticalPath]
use txn_types::{Key, TimeStamp};

use crate::storage::{
    kv::ScanMode,
    lock_manager::LockManager,
    mvcc::{MvccReader, MvccTxn},
    txn::{
        commands::{
            Command, CommandExt, ResponsePolicy, TypedCommand, WriteCommand, WriteContext,
            WriteResult,
        },
        flashback_to_version, Result,
    },
    ProcessResult, Snapshot,
};

// TODO: consider add `KvFormat` generic parameter.
command! {
    FlashbackToVersion:
        cmd_ty => (TimeStamp /* Version to reset */, Key /* start_key */, Key /* end_key */),
        display => "kv::command::flashback_to_version [{}, {}) @ {} | {:?}", (start_key, end_key, version ,ctx),
        content => {
            version: TimeStamp,
            start_key: Key,
            end_key: Key,
        }
}

impl CommandExt for FlashbackToVersion {
    ctx!();
    tag!(flashback_to_version);
    // Because we don't know the actual keys affected by this command,
    // we can not generate the corresponding locks. However, since we
    // will block all scheduling, reading and writing operations before
    // this command, so it's ok to not fetch the latch.
    gen_lock!(empty);
    // Same reason as above here to return 0.
    fn write_bytes(&self) -> usize {
        0
    }
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for FlashbackToVersion {
    fn process_write(self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        let mut txn = MvccTxn::new(self.version, context.concurrency_manager);
        let mut reader = MvccReader::new_with_ctx(snapshot, Some(ScanMode::Forward), &self.ctx);
        // Flashback all keys within [start_key, end_key) to the specified version by
        // deleting all keys in `CF_WRITE`, `CF_DEFAULT` and `CF_LOCK` that are written
        // later than this version.
        flashback_to_version(
            &mut txn,
            &mut reader,
            self.version,
            self.start_key,
            self.end_key,
        )?;
        let mut write_data = WriteData::from_modifies(txn.into_modifies());
        write_data.set_allowed_on_disk_almost_full();
        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write: write_data,
            rows: 1,
            pr: ProcessResult::Res,
            lock_info: None,
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
        })
    }
}
