// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::mem;

use txn_types::{Key, TimeStamp};

use crate::storage::{
    kv::WriteData,
    lock_manager::LockManager,
    mvcc::{MvccTxn, SnapshotReader},
    txn::{
        actions::flashback_to_version::{
            commit_flashback_key, flashback_to_version_lock, flashback_to_version_write,
            prewrite_flashback_key,
        },
        commands::{
            Command, CommandExt, FlashbackToVersionReadPhase, FlashbackToVersionState,
            ReaderWithStats, ReleasedLocks, ResponsePolicy, TypedCommand, WriteCommand,
            WriteContext, WriteResult,
        },
        latch, Result,
    },
    Context, ProcessResult, Snapshot,
};

pub fn new_flashback_to_version_prewrite_cmd(
    start_key: Key,
    start_ts: TimeStamp,
    version: TimeStamp,
    ctx: Context,
) -> TypedCommand<()> {
    FlashbackToVersion::new(
        start_ts,
        TimeStamp::zero(),
        version,
        // Just pass some dummy values since we don't need them in this phase.
        Key::from_raw(b""),
        Key::from_raw(b""),
        FlashbackToVersionState::Prewrite {
            key_to_lock: start_key,
        },
        ctx,
    )
}

command! {
    FlashbackToVersion:
        cmd_ty => (),
        display => "kv::command::flashback_to_version -> {} | {} {} | {:?}", (version, start_ts, commit_ts, ctx),
        content => {
            start_ts: TimeStamp,
            commit_ts: TimeStamp,
            version: TimeStamp,
            start_key: Key,
            end_key: Key,
            state: FlashbackToVersionState,
        }
}

impl CommandExt for FlashbackToVersion {
    ctx!();
    tag!(flashback_to_version);
    request_type!(KvFlashbackToVersion);

    fn gen_lock(&self) -> latch::Lock {
        match &self.state {
            FlashbackToVersionState::Prewrite { key_to_lock } => latch::Lock::new([key_to_lock]),
            FlashbackToVersionState::FlashbackLock { key_locks, .. } => {
                latch::Lock::new(key_locks.iter().map(|(key, _)| key))
            }
            FlashbackToVersionState::FlashbackWrite { key_old_writes, .. } => {
                latch::Lock::new(key_old_writes.iter().map(|(key, _)| key))
            }
            FlashbackToVersionState::Commit { key_to_commit } => latch::Lock::new([key_to_commit]),
        }
    }

    fn write_bytes(&self) -> usize {
        match &self.state {
            FlashbackToVersionState::Prewrite { key_to_lock } => key_to_lock.as_encoded().len(),
            FlashbackToVersionState::FlashbackLock { key_locks, .. } => key_locks
                .iter()
                .map(|(key, _)| key.as_encoded().len())
                .sum(),
            FlashbackToVersionState::FlashbackWrite { key_old_writes, .. } => key_old_writes
                .iter()
                .map(|(key, _)| key.as_encoded().len())
                .sum(),
            FlashbackToVersionState::Commit { key_to_commit } => key_to_commit.as_encoded().len(),
        }
    }
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for FlashbackToVersion {
    fn process_write(mut self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        let mut reader = ReaderWithStats::new(
            SnapshotReader::new_with_ctx(self.version, snapshot, &self.ctx),
            context.statistics,
        );
        let mut txn = MvccTxn::new(TimeStamp::zero(), context.concurrency_manager);
        // - `FlashbackToVersionState::Prewrite` and `FlashbackToVersionState::Commit`
        //   are to lock/commit the `self.start_key` to prevent `resolved_ts` from
        //   advancing before we finish the actual flashback.
        // - `FlashbackToVersionState::FlashbackWrite` and
        //   `FlashbackToVersionState::FlashbackLock` are to flashback the actual writes
        //   and locks with 1PC.
        match self.state {
            FlashbackToVersionState::Prewrite { ref key_to_lock } => {
                prewrite_flashback_key(
                    &mut txn,
                    &mut reader,
                    key_to_lock,
                    self.version,
                    self.start_ts,
                )?;
            }
            FlashbackToVersionState::FlashbackWrite {
                ref mut next_write_key,
                ref mut key_old_writes,
            } => {
                if let Some(new_next_write_key) = flashback_to_version_write(
                    &mut txn,
                    &mut reader,
                    mem::take(key_old_writes),
                    self.start_ts,
                    self.commit_ts,
                )? {
                    *next_write_key = new_next_write_key;
                }
            }
            FlashbackToVersionState::FlashbackLock {
                ref mut next_lock_key,
                ref mut key_locks,
            } => {
                if let Some(new_next_lock_key) =
                    flashback_to_version_lock(&mut txn, &mut reader, mem::take(key_locks))?
                {
                    *next_lock_key = new_next_lock_key;
                }
            }
            FlashbackToVersionState::Commit { ref key_to_commit } => {
                commit_flashback_key(
                    &mut txn,
                    &mut reader,
                    key_to_commit,
                    self.start_ts,
                    self.commit_ts,
                )?;
            }
        }
        let rows = txn.modifies.len();
        let mut write_data = WriteData::from_modifies(txn.into_modifies());
        // To let the flashback modification could be proposed and applied successfully.
        write_data.extra.for_flashback = true;
        let is_prewrite_or_commit_phase = matches!(
            self.state,
            FlashbackToVersionState::Prewrite { .. } | FlashbackToVersionState::Commit { .. }
        );
        if !is_prewrite_or_commit_phase {
            // To let the CDC treat the flashback modification as an 1PC transaction.
            write_data.extra.one_pc = true;
        }
        Ok(WriteResult {
            ctx: self.ctx.clone(),
            to_be_write: write_data,
            rows,
            pr: (move || {
                fail_point!("flashback_failed_after_first_batch", |_| {
                    ProcessResult::Res
                });
                if is_prewrite_or_commit_phase {
                    return ProcessResult::Res;
                }
                let next_cmd = FlashbackToVersionReadPhase {
                    ctx: self.ctx,
                    deadline: self.deadline,
                    start_ts: self.start_ts,
                    commit_ts: self.commit_ts,
                    version: self.version,
                    start_key: self.start_key,
                    end_key: self.end_key,
                    state: self.state,
                };
                ProcessResult::NextCommand {
                    cmd: Command::FlashbackToVersionReadPhase(next_cmd),
                }
            })(),
            lock_info: vec![],
            released_locks: ReleasedLocks::new(),
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
        })
    }
}
