// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::mem;

use tikv_kv::ScanMode;
use txn_types::{Key, TimeStamp};

use crate::storage::{
    kv::WriteData,
    lock_manager::LockManager,
    metrics::{CommandKind, KV_COMMAND_COUNTER_VEC_STATIC},
    mvcc::{MvccReader, MvccTxn},
    txn::{
        actions::flashback_to_version::{
            commit_flashback_key, flashback_to_version_write, prewrite_flashback_key,
            rollback_locks,
        },
        commands::{
            Command, CommandExt, FlashbackToVersionReadPhase, FlashbackToVersionState,
            ReleasedLocks, ResponsePolicy, TypedCommand, WriteCommand, WriteContext, WriteResult,
        },
        latch, Result,
    },
    ProcessResult, Snapshot,
};

command! {
    FlashbackToVersion:
        cmd_ty => (),
        display => {
            "kv::command::flashback_to_version -> {} | {} {} | {:?}",
            (version, start_ts, commit_ts, ctx),
        }
        content => {
            start_ts: TimeStamp,
            commit_ts: TimeStamp,
            version: TimeStamp,
            start_key: Key,
            end_key: Option<Key>,
            state: FlashbackToVersionState,
        }
}

impl CommandExt for FlashbackToVersion {
    ctx!();
    request_type!(KvFlashbackToVersion);

    fn gen_lock(&self) -> latch::Lock {
        match &self.state {
            FlashbackToVersionState::RollbackLock { key_locks, .. } => {
                latch::Lock::new(key_locks.iter().map(|(key, _)| key))
            }
            FlashbackToVersionState::Prewrite { key_to_lock } => latch::Lock::new([key_to_lock]),
            FlashbackToVersionState::FlashbackWrite { keys, .. } => latch::Lock::new(keys.iter()),
            FlashbackToVersionState::Commit { key_to_commit } => latch::Lock::new([key_to_commit]),
        }
    }

    fn write_bytes(&self) -> usize {
        match &self.state {
            FlashbackToVersionState::RollbackLock { key_locks, .. } => key_locks
                .iter()
                .map(|(key, _)| key.as_encoded().len())
                .sum(),
            FlashbackToVersionState::Prewrite { key_to_lock } => key_to_lock.as_encoded().len(),
            FlashbackToVersionState::FlashbackWrite { keys, .. } => {
                keys.iter().map(|key| key.as_encoded().len()).sum()
            }
            FlashbackToVersionState::Commit { key_to_commit } => key_to_commit.as_encoded().len(),
        }
    }

    fn tag(&self) -> CommandKind {
        match self.state {
            FlashbackToVersionState::RollbackLock { .. } => {
                CommandKind::flashback_to_version_rollback_lock
            }
            _ => CommandKind::flashback_to_version_write,
        }
    }

    fn incr_cmd_metric(&self) {
        match self.state {
            FlashbackToVersionState::RollbackLock { .. } => {
                KV_COMMAND_COUNTER_VEC_STATIC
                    .flashback_to_version_rollback_lock
                    .inc();
            }
            _ => KV_COMMAND_COUNTER_VEC_STATIC
                .flashback_to_version_write
                .inc(),
        }
    }
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for FlashbackToVersion {
    fn process_write(mut self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        let mut reader =
            MvccReader::new_with_ctx(snapshot.clone(), Some(ScanMode::Forward), &self.ctx);
        reader.set_allow_in_flashback(true);
        let mut txn = MvccTxn::new(TimeStamp::zero(), context.concurrency_manager);
        match self.state {
            FlashbackToVersionState::RollbackLock {
                ref mut next_lock_key,
                ref mut key_locks,
            } => {
                if let Some(new_next_lock_key) =
                    rollback_locks(&mut txn, snapshot, mem::take(key_locks))?
                {
                    *next_lock_key = new_next_lock_key;
                }
            }
            FlashbackToVersionState::Prewrite { ref key_to_lock } => prewrite_flashback_key(
                &mut txn,
                &mut reader,
                key_to_lock,
                self.version,
                self.start_ts,
            )?,
            FlashbackToVersionState::FlashbackWrite {
                ref mut next_write_key,
                ref mut keys,
            } => {
                if let Some(new_next_write_key) = flashback_to_version_write(
                    &mut txn,
                    &mut reader,
                    mem::take(keys),
                    self.version,
                    self.start_ts,
                    self.commit_ts,
                )? {
                    *next_write_key = new_next_write_key;
                }
            }
            FlashbackToVersionState::Commit { ref key_to_commit } => commit_flashback_key(
                &mut txn,
                &mut reader,
                key_to_commit,
                self.start_ts,
                self.commit_ts,
            )?,
        }
        let rows = txn.modifies.len();
        let mut write_data = WriteData::from_modifies(txn.into_modifies());
        // To let the flashback modification could be proposed and applied successfully.
        write_data.extra.allowed_in_flashback = true;
        // To let the CDC treat the flashback modification as an 1PC transaction.
        if matches!(self.state, FlashbackToVersionState::FlashbackWrite { .. }) {
            write_data.extra.one_pc = true;
        }
        context.statistics.add(&reader.statistics);
        Ok(WriteResult {
            ctx: self.ctx.clone(),
            to_be_write: write_data,
            rows,
            pr: (move || {
                if matches!(
                    self.state,
                    FlashbackToVersionState::Prewrite { .. }
                        | FlashbackToVersionState::Commit { .. }
                ) {
                    return ProcessResult::Res;
                }

                #[cfg(feature = "failpoints")]
                if matches!(self.state, FlashbackToVersionState::FlashbackWrite { .. }) {
                    fail_point!("flashback_failed_after_first_batch", |_| {
                        ProcessResult::Res
                    });
                }

                ProcessResult::NextCommand {
                    cmd: Command::FlashbackToVersionReadPhase(FlashbackToVersionReadPhase {
                        ctx: self.ctx,
                        deadline: self.deadline,
                        start_ts: self.start_ts,
                        commit_ts: self.commit_ts,
                        version: self.version,
                        start_key: self.start_key,
                        end_key: self.end_key,
                        state: self.state,
                    }),
                }
            })(),
            lock_info: vec![],
            released_locks: ReleasedLocks::new(),
            new_acquired_locks: vec![],
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
            known_txn_status: vec![],
        })
    }
}
