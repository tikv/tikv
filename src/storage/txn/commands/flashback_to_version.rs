// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::mem;

use txn_types::{Key, Lock, LockType, TimeStamp};

use crate::storage::{
    kv::WriteData,
    lock_manager::LockManager,
    mvcc::{MvccTxn, SnapshotReader},
    txn::{
        actions::flashback_to_version::{flashback_to_version_lock, flashback_to_version_write},
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
    ctx: Context,
) -> TypedCommand<()> {
    FlashbackToVersion::new(
        start_ts,
        // Just pass some dummy values since we don't need them in this phase.
        TimeStamp::zero(),
        TimeStamp::zero(),
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
            FlashbackToVersionState::ScanLock { key_locks, .. } => {
                latch::Lock::new(key_locks.iter().map(|(key, _)| key))
            }
            FlashbackToVersionState::ScanWrite { key_old_writes, .. } => {
                latch::Lock::new(key_old_writes.iter().map(|(key, _)| key))
            }
            FlashbackToVersionState::Commit { key_to_commit } => latch::Lock::new([key_to_commit]),
        }
    }

    fn write_bytes(&self) -> usize {
        match &self.state {
            FlashbackToVersionState::Prewrite { key_to_lock } => key_to_lock.as_encoded().len(),
            FlashbackToVersionState::ScanLock { key_locks, .. } => key_locks
                .iter()
                .map(|(key, _)| key.as_encoded().len())
                .sum(),
            FlashbackToVersionState::ScanWrite { key_old_writes, .. } => key_old_writes
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
        // - `FlashbackToVersionState::ScanWrite` and
        //   `FlashbackToVersionState::ScanLock` are to flashback the actual writes and
        //   locks.
        match self.state {
            FlashbackToVersionState::Prewrite { ref key_to_lock } => {
                txn.put_lock(
                    key_to_lock.clone(),
                    &Lock::new(
                        LockType::Put,
                        key_to_lock.as_encoded().to_vec(),
                        self.start_ts,
                        0,
                        None,
                        TimeStamp::zero(),
                        0,
                        TimeStamp::zero(),
                    ),
                );
            }
            FlashbackToVersionState::ScanWrite {
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
            FlashbackToVersionState::ScanLock {
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
                // Flashback the key first.
                let old_write = reader
                    .get_write_with_commit_ts(key_to_commit, self.version)?
                    .map(|(write, _)| write);
                flashback_to_version_write(
                    &mut txn,
                    &mut reader,
                    vec![(key_to_commit.clone(), old_write)],
                    self.start_ts,
                    self.commit_ts,
                )?;
                // Unlock the key then.
                txn.unlock_key(key_to_commit.clone(), false, self.commit_ts);
            }
        }
        let rows = txn.modifies.len();
        let mut write_data = WriteData::from_modifies(txn.into_modifies());
        // To let the flashback modification could be proposed and applied successfully.
        write_data.extra.for_flashback = true;
        let is_unlock_or_lock_phase = matches!(
            self.state,
            FlashbackToVersionState::Prewrite { .. } | FlashbackToVersionState::Commit { .. }
        );
        if !is_unlock_or_lock_phase {
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
                if is_unlock_or_lock_phase {
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
