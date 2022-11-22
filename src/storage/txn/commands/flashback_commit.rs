// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::mem;

use txn_types::{Key, TimeStamp, Write};

use crate::storage::{
    kv::WriteData,
    lock_manager::LockManager,
    mvcc::{MvccReader, MvccTxn, SnapshotReader},
    txn::{
        actions::flashback_to_version::{commit_flashback_key, flashback_to_version_write},
        commands::{
            Command, CommandExt, ProcessResult, ReadCommand, ReaderWithStats, ReleasedLocks,
            ResponsePolicy, TypedCommand, WriteCommand, WriteContext, WriteResult,
        },
        flashback_to_version_read_write, latch,
        sched_pool::tls_collect_keyread_histogram_vec,
        Result,
    },
    Context, ScanMode, Snapshot, Statistics,
};

#[derive(Debug)]
pub enum FlashbackCommitState {
    FlashbackWrite {
        next_write_key: Key,
        key_old_writes: Vec<(Key, Option<Write>)>,
    },
    Commit {
        key_to_commit: Key,
    },
}

pub fn new_flashback_commit_cmd(
    start_ts: TimeStamp,
    commit_ts: TimeStamp,
    version: TimeStamp,
    start_key: Key,
    end_key: Key,
    ctx: Context,
) -> TypedCommand<()> {
    FlashbackToVersionCommit::new(
        start_ts,
        commit_ts,
        version,
        start_key.clone(),
        end_key,
        FlashbackCommitState::FlashbackWrite {
            next_write_key: start_key,
            key_old_writes: Vec::new(),
        },
        true,
        ctx,
    )
}

command! {
    FlashbackToVersionCommit:
        cmd_ty => (),
        display => "kv::command::flashback_to_version_commit -> {} | {} {} | {:?}", (version, start_ts, commit_ts, ctx),
        content => {
            start_ts: TimeStamp,
            commit_ts: TimeStamp,
            version: TimeStamp,
            start_key: Key,
            end_key: Key,
            state: FlashbackCommitState,
            readonly: bool,
        }
}

impl CommandExt for FlashbackToVersionCommit {
    ctx!();
    tag!(flashback_to_version);
    request_type!(KvFlashbackToVersion);

    fn gen_lock(&self) -> latch::Lock {
        match &self.state {
            FlashbackCommitState::FlashbackWrite { key_old_writes, .. } => {
                latch::Lock::new(key_old_writes.iter().map(|(key, _)| key))
            }
            FlashbackCommitState::Commit { key_to_commit } => latch::Lock::new([key_to_commit]),
        }
    }

    fn write_bytes(&self) -> usize {
        match &self.state {
            FlashbackCommitState::FlashbackWrite { key_old_writes, .. } => key_old_writes
                .iter()
                .map(|(key, _)| key.as_encoded().len())
                .sum(),
            FlashbackCommitState::Commit { key_to_commit } => key_to_commit.as_encoded().len(),
        }
    }

    fn readonly(&self) -> bool {
        self.readonly
    }
}

/// The commit flashback progress contains 2 phases:
///   1. FlashbackWrite phase:
///     - Scan all the latest writes and their corresponding values at
///       `self.version`.
///     - Write the old MVCC version writes again for all these keys with
///       `self.commit_ts` excluding the `self.start_key`.
///   2. Commit phase:
///     - Commit the `self.start_key` we write at the second phase to finish the
///       flashback.
impl<S: Snapshot> ReadCommand<S> for FlashbackToVersionCommit {
    fn process_read(self, snapshot: S, statistics: &mut Statistics) -> Result<ProcessResult> {
        let tag = self.tag().get_str();
        let mut reader = MvccReader::new_with_ctx(snapshot, Some(ScanMode::Forward), &self.ctx);
        let next_state = match self.state {
            FlashbackCommitState::FlashbackWrite { next_write_key, .. } => {
                // If the key is not locked, it means that the key has been committed before and
                // we are in a retry.
                if next_write_key == self.start_key && reader.load_lock(&next_write_key)?.is_none()
                {
                    return Ok(ProcessResult::Res);
                }
                let mut key_old_writes = flashback_to_version_read_write(
                    &mut reader,
                    next_write_key,
                    &self.start_key,
                    &self.end_key,
                    self.version,
                    self.commit_ts,
                    statistics,
                )?;
                if key_old_writes.is_empty() {
                    FlashbackCommitState::Commit {
                        key_to_commit: self.start_key.clone(),
                    }
                } else {
                    tls_collect_keyread_histogram_vec(tag, key_old_writes.len() as f64);
                    FlashbackCommitState::FlashbackWrite {
                        // DO NOT pop the last key as the next key when it's the only key to prevent
                        // from making flashback fall into a dead loop.
                        next_write_key: if key_old_writes.len() > 1 {
                            key_old_writes.pop().map(|(key, _)| key).unwrap()
                        } else {
                            key_old_writes.last().map(|(key, _)| key.clone()).unwrap()
                        },
                        key_old_writes,
                    }
                }
            }
            _ => unreachable!(),
        };

        Ok(ProcessResult::NextCommand {
            cmd: Command::FlashbackToVersionCommit(FlashbackToVersionCommit {
                ctx: self.ctx,
                deadline: self.deadline,
                start_ts: self.start_ts,
                commit_ts: self.commit_ts,
                version: self.version,
                start_key: self.start_key,
                end_key: self.end_key,
                state: next_state,
                readonly: false,
            }),
        })
    }
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for FlashbackToVersionCommit {
    fn process_write(mut self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        let mut reader = ReaderWithStats::new(
            SnapshotReader::new_with_ctx(self.version, snapshot, &self.ctx),
            context.statistics,
        );
        let mut txn = MvccTxn::new(TimeStamp::zero(), context.concurrency_manager);
        match self.state {
            FlashbackCommitState::FlashbackWrite {
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
            FlashbackCommitState::Commit { ref key_to_commit } => commit_flashback_key(
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
        write_data.extra.for_flashback = true;
        let is_prewrite_or_commit_phase = matches!(self.state, FlashbackCommitState::Commit { .. });
        if !is_prewrite_or_commit_phase {
            // To let the CDC treat the flashback modification as an 1PC transaction.
            write_data.extra.one_pc = true;
        }
        Ok(WriteResult {
            ctx: self.ctx.clone(),
            to_be_write: write_data,
            rows,
            pr: (move || {
                if is_prewrite_or_commit_phase {
                    return ProcessResult::Res;
                }

                #[cfg(feature = "failpoints")]
                if matches!(self.state, FlashbackCommitState::FlashbackWrite { .. }) {
                    fail_point!("flashback_failed_after_first_batch", |_| {
                        ProcessResult::Res
                    });
                }

                ProcessResult::NextCommand {
                    cmd: Command::FlashbackToVersionCommit(FlashbackToVersionCommit {
                        ctx: self.ctx,
                        deadline: self.deadline,
                        start_ts: self.start_ts,
                        commit_ts: self.commit_ts,
                        version: self.version,
                        start_key: self.start_key,
                        end_key: self.end_key,
                        state: self.state,
                        readonly: true,
                    }),
                }
            })(),
            lock_info: vec![],
            released_locks: ReleasedLocks::new(),
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
        })
    }
}
