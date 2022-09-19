// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use txn_types::{Key, Lock, TimeStamp, Write};

use crate::storage::{
    kv::WriteData,
    lock_manager::LockManager,
    mvcc::{MvccTxn, SnapshotReader},
    txn::{
        commands::{
            Command, CommandExt, FlashbackToVersionReadPhase, ResponsePolicy, TypedCommand,
            WriteCommand, WriteContext, WriteResult,
        },
        flashback_to_version, latch, Result,
    },
    ProcessResult, Snapshot,
};

command! {
    FlashbackToVersion:
        cmd_ty => (),
        display => "kv::command::flashback_to_version -> {} | {} {} | {:?}", (version, start_ts, commit_ts, ctx),
        content => {
            start_ts: TimeStamp,
            commit_ts: TimeStamp,
            version: TimeStamp,
            end_key: Option<Key>,
            next_lock_key: Option<Key>,
            next_write_key: Option<Key>,
            key_locks: Vec<(Key, Lock)>,
            key_old_writes: Vec<(Key, Option<Write>)>,
        }
}

impl CommandExt for FlashbackToVersion {
    ctx!();
    tag!(flashback_to_version);
    request_type!(KvFlashbackToVersion);

    fn gen_lock(&self) -> latch::Lock {
        latch::Lock::new(
            self.key_locks
                .iter()
                .map(|(key, _)| key)
                .chain(self.key_old_writes.iter().map(|(key, _)| key)),
        )
    }

    fn write_bytes(&self) -> usize {
        self.key_locks
            .iter()
            .map(|(key, _)| key.as_encoded().len())
            .chain(
                self.key_old_writes
                    .iter()
                    .map(|(key, _)| key.as_encoded().len()),
            )
            .sum()
    }
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for FlashbackToVersion {
    fn process_write(mut self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        let mut reader = SnapshotReader::new_with_ctx(self.version, snapshot, &self.ctx);
        let mut txn = MvccTxn::new(TimeStamp::zero(), context.concurrency_manager);

        let mut next_lock_key = self.next_lock_key.take();
        let mut next_write_key = self.next_write_key.take();
        let rows = flashback_to_version(
            &mut txn,
            &mut reader,
            &mut next_lock_key,
            &mut next_write_key,
            self.key_locks,
            self.key_old_writes,
            self.version,
            self.start_ts,
            self.commit_ts,
        )?;
        let mut write_data = WriteData::from_modifies(txn.into_modifies());
        write_data.extra.for_flashback = true;
        Ok(WriteResult {
            ctx: self.ctx.clone(),
            to_be_write: write_data,
            rows,
            pr: if next_lock_key.is_none() && next_write_key.is_none() {
                ProcessResult::Res
            } else {
                let next_cmd = FlashbackToVersionReadPhase {
                    ctx: self.ctx.clone(),
                    deadline: self.deadline,
                    start_ts: self.start_ts,
                    commit_ts: self.commit_ts,
                    version: self.version,
                    end_key: self.end_key,
                    next_lock_key,
                    next_write_key,
                };
                ProcessResult::NextCommand {
                    cmd: Command::FlashbackToVersionReadPhase(next_cmd),
                }
            },
            lock_info: None,
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
        })
    }
}
