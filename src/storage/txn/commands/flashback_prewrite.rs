// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::mem;

use txn_types::{Key, Lock, TimeStamp};

use crate::storage::{
    kv::WriteData,
    lock_manager::LockManager,
    mvcc::{MvccReader, MvccTxn, SnapshotReader},
    txn::{
        actions::flashback_to_version::{prewrite_flashback_key, rollback_locks},
        commands::{
            Command, CommandExt, ProcessResult, ReadCommand, ReaderWithStats, ReleasedLocks,
            ResponsePolicy, TypedCommand, WriteCommand, WriteContext, WriteResult,
        },
        flashback_to_version_read_lock, latch,
        sched_pool::tls_collect_keyread_histogram_vec,
        Result,
    },
    Context, ScanMode, Snapshot, Statistics,
};

#[derive(Debug)]
pub enum FlashbackPrewriteState {
    RollbackLock {
        next_lock_key: Key,
        key_locks: Vec<(Key, Lock)>,
    },
    Prewrite {
        key_to_lock: Key,
    },
}

pub fn new_flashback_prewrite_cmd(
    start_ts: TimeStamp,
    version: TimeStamp,
    start_key: Key,
    end_key: Key,
    ctx: Context,
) -> TypedCommand<()> {
    FlashbackToVersionPrewrite::new(
        start_ts,
        TimeStamp::zero(),
        version,
        start_key.clone(),
        end_key,
        FlashbackPrewriteState::RollbackLock {
            next_lock_key: start_key,
            key_locks: Vec::new(),
        },
        true,
        ctx,
    )
}

command! {
    FlashbackToVersionPrewrite:
        cmd_ty => (),
        display => "kv::command::flashback_to_version_prewrite -> {} | {} {} | {:?} {:?}", (version, start_ts, commit_ts, ctx, state),
        content => {
            start_ts: TimeStamp,
            commit_ts: TimeStamp,
            version: TimeStamp,
            start_key: Key,
            end_key: Key,
            state: FlashbackPrewriteState,
            readonly: bool,
        }
}

impl CommandExt for FlashbackToVersionPrewrite {
    ctx!();
    tag!(flashback_to_version);
    request_type!(KvFlashbackToVersion);

    fn gen_lock(&self) -> latch::Lock {
        match &self.state {
            FlashbackPrewriteState::RollbackLock { key_locks, .. } => {
                latch::Lock::new(key_locks.iter().map(|(key, _)| key))
            }
            FlashbackPrewriteState::Prewrite { key_to_lock } => latch::Lock::new([key_to_lock]),
        }
    }

    fn write_bytes(&self) -> usize {
        match &self.state {
            FlashbackPrewriteState::RollbackLock { key_locks, .. } => key_locks
                .iter()
                .map(|(key, _)| key.as_encoded().len())
                .sum(),
            FlashbackPrewriteState::Prewrite { key_to_lock } => key_to_lock.as_encoded().len(),
        }
    }

    fn readonly(&self) -> bool {
        self.readonly
    }
}

/// The prewrite flashback progress contains 2 phases:
///   1. RollbackLock phase:
///     - Scan all locks.
///     - Rollback all these locks.
///   2. Prewrite phase:
///     - Prewrite the `self.start_key` specifically to prevent the
///       `resolved_ts` from advancing.
impl<S: Snapshot> ReadCommand<S> for FlashbackToVersionPrewrite {
    fn process_read(self, snapshot: S, statistics: &mut Statistics) -> Result<ProcessResult> {
        let tag = self.tag().get_str();
        let mut reader = MvccReader::new_with_ctx(snapshot, Some(ScanMode::Forward), &self.ctx);
        let next_state = match self.state {
            FlashbackPrewriteState::RollbackLock { next_lock_key, .. } => {
                let mut key_locks = flashback_to_version_read_lock(
                    &mut reader,
                    next_lock_key,
                    &self.end_key,
                    statistics,
                )?;
                if key_locks.is_empty() {
                    // No more locks to rollback, continue to the prewrite phase.
                    FlashbackPrewriteState::Prewrite {
                        key_to_lock: self.start_key.clone(),
                    }
                } else {
                    tls_collect_keyread_histogram_vec(tag, key_locks.len() as f64);
                    FlashbackPrewriteState::RollbackLock {
                        next_lock_key: if key_locks.len() > 1 {
                            key_locks.pop().map(|(key, _)| key).unwrap()
                        } else {
                            key_locks.last().map(|(key, _)| key.clone()).unwrap()
                        },
                        key_locks,
                    }
                }
            }
            _ => unreachable!(),
        };
        Ok(ProcessResult::NextCommand {
            cmd: Command::FlashbackToVersionPrewrite(FlashbackToVersionPrewrite {
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

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for FlashbackToVersionPrewrite {
    fn process_write(mut self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        let mut reader = ReaderWithStats::new(
            SnapshotReader::new_with_ctx(self.version, snapshot, &self.ctx),
            context.statistics,
        );
        let mut txn = MvccTxn::new(TimeStamp::zero(), context.concurrency_manager);
        match self.state {
            FlashbackPrewriteState::RollbackLock {
                ref mut next_lock_key,
                ref mut key_locks,
            } => {
                if let Some(new_next_lock_key) =
                    rollback_locks(&mut txn, &mut reader, mem::take(key_locks))?
                {
                    *next_lock_key = new_next_lock_key;
                }
            }
            // TODO: add some test cases for the special prewrite key.
            FlashbackPrewriteState::Prewrite { ref key_to_lock } => prewrite_flashback_key(
                &mut txn,
                &mut reader,
                key_to_lock,
                self.version,
                self.start_ts,
            )?,
        }
        let rows = txn.modifies.len();
        let mut write_data = WriteData::from_modifies(txn.into_modifies());
        // To let the flashback modification could be proposed and applied successfully.
        write_data.extra.for_flashback = true;
        let is_prewrite_or_commit_phase =
            matches!(self.state, FlashbackPrewriteState::Prewrite { .. });
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
                if matches!(self.state, FlashbackPrewriteState::Prewrite { .. }) {
                    fail_point!("flashback_failed_after_first_batch", |_| {
                        ProcessResult::Res
                    });
                }

                ProcessResult::NextCommand {
                    cmd: Command::FlashbackToVersionPrewrite(FlashbackToVersionPrewrite {
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
