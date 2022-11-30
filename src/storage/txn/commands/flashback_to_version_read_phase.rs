// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use txn_types::{Key, Lock, TimeStamp};

use crate::storage::{
    mvcc::MvccReader,
    txn::{
        commands::{
            Command, CommandExt, FlashbackToVersion, ProcessResult, ReadCommand, TypedCommand,
        },
        flashback_to_version_read_lock, flashback_to_version_read_write,
        sched_pool::tls_collect_keyread_histogram_vec,
        Error, ErrorInner, Result,
    },
    Context, ScanMode, Snapshot, Statistics,
};

#[derive(Debug)]
pub enum FlashbackToVersionState {
    RollbackLock {
        next_lock_key: Key,
        key_locks: Vec<(Key, Lock)>,
    },
    Prewrite {
        key_to_lock: Key,
    },
    FlashbackWrite {
        next_write_key: Key,
        keys: Vec<Key>,
    },
    Commit {
        key_to_commit: Key,
    },
}

pub fn new_flashback_rollback_lock_cmd(
    start_ts: TimeStamp,
    version: TimeStamp,
    start_key: Key,
    end_key: Key,
    ctx: Context,
) -> TypedCommand<()> {
    FlashbackToVersionReadPhase::new(
        start_ts,
        TimeStamp::zero(),
        version,
        start_key.clone(),
        end_key,
        FlashbackToVersionState::RollbackLock {
            next_lock_key: start_key,
            key_locks: Vec::new(),
        },
        ctx,
    )
}

pub fn new_flashback_write_cmd(
    start_ts: TimeStamp,
    commit_ts: TimeStamp,
    version: TimeStamp,
    start_key: Key,
    end_key: Key,
    ctx: Context,
) -> TypedCommand<()> {
    FlashbackToVersionReadPhase::new(
        start_ts,
        commit_ts,
        version,
        start_key.clone(),
        end_key,
        FlashbackToVersionState::FlashbackWrite {
            next_write_key: start_key,
            keys: Vec::new(),
        },
        ctx,
    )
}

command! {
    FlashbackToVersionReadPhase:
        cmd_ty => (),
        display => "kv::command::flashback_to_version_read_phase -> {} | {} {} | {:?}", (version, start_ts, commit_ts, ctx),
        content => {
            start_ts: TimeStamp,
            commit_ts: TimeStamp,
            version: TimeStamp,
            start_key: Key,
            end_key: Key,
            state: FlashbackToVersionState,
        }
}

impl CommandExt for FlashbackToVersionReadPhase {
    ctx!();
    tag!(flashback_to_version);
    request_type!(KvFlashbackToVersion);
    property!(readonly);
    gen_lock!(empty);

    fn write_bytes(&self) -> usize {
        0
    }
}

/// The whole flashback progress contains four phases:
///   1. [PrepareFlashback] RollbackLock phase:
///     - Scan all locks.
///     - Rollback all these locks.
///   2. [PrepareFlashback] Prewrite phase:
///     - Prewrite the `self.start_key` specifically to prevent the
///       `resolved_ts` from advancing.
///   3. [FinishFlashback] FlashbackWrite phase:
///     - Scan all the latest writes and their corresponding values at
///       `self.version`.
///     - Write the old MVCC version writes again for all these keys with
///       `self.commit_ts` excluding the `self.start_key`.
///   4. [FinishFlashback] Commit phase:
///     - Commit the `self.start_key` we write at the second phase to finish the
///       flashback.
impl<S: Snapshot> ReadCommand<S> for FlashbackToVersionReadPhase {
    fn process_read(self, snapshot: S, statistics: &mut Statistics) -> Result<ProcessResult> {
        let tag = self.tag().get_str();
        let mut reader = MvccReader::new_with_ctx(snapshot, Some(ScanMode::Forward), &self.ctx);
        let next_state = match self.state {
            FlashbackToVersionState::RollbackLock { next_lock_key, .. } => {
                let mut key_locks = flashback_to_version_read_lock(
                    &mut reader,
                    next_lock_key,
                    &self.end_key,
                    statistics,
                )?;
                if key_locks.is_empty() {
                    // No more locks to rollback, continue to the prewrite phase.
                    FlashbackToVersionState::Prewrite {
                        key_to_lock: self.start_key.clone(),
                    }
                } else {
                    tls_collect_keyread_histogram_vec(tag, key_locks.len() as f64);
                    FlashbackToVersionState::RollbackLock {
                        next_lock_key: if key_locks.len() > 1 {
                            key_locks.pop().map(|(key, _)| key).unwrap()
                        } else {
                            key_locks.last().map(|(key, _)| key.clone()).unwrap()
                        },
                        key_locks,
                    }
                }
            }
            FlashbackToVersionState::FlashbackWrite { next_write_key, .. } => {
                if self.commit_ts <= self.start_ts {
                    return Err(Error::from(ErrorInner::InvalidTxnTso {
                        start_ts: self.start_ts,
                        commit_ts: self.commit_ts,
                    }));
                }
                // If the key is not locked, it means that the key has been committed before and
                // we are in a retry.
                if next_write_key == self.start_key && reader.load_lock(&next_write_key)?.is_none()
                {
                    return Ok(ProcessResult::Res);
                }
                let mut keys = flashback_to_version_read_write(
                    &mut reader,
                    next_write_key,
                    &self.start_key,
                    &self.end_key,
                    self.version,
                    self.commit_ts,
                    statistics,
                )?;
                if keys.is_empty() {
                    FlashbackToVersionState::Commit {
                        key_to_commit: self.start_key.clone(),
                    }
                } else {
                    tls_collect_keyread_histogram_vec(tag, keys.len() as f64);
                    FlashbackToVersionState::FlashbackWrite {
                        // DO NOT pop the last key as the next key when it's the only key to prevent
                        // from making flashback fall into a dead loop.
                        next_write_key: if keys.len() > 1 {
                            keys.pop().unwrap()
                        } else {
                            keys.last().unwrap().clone()
                        },
                        keys,
                    }
                }
            }
            _ => unreachable!(),
        };
        Ok(ProcessResult::NextCommand {
            cmd: Command::FlashbackToVersion(FlashbackToVersion {
                ctx: self.ctx,
                deadline: self.deadline,
                start_ts: self.start_ts,
                commit_ts: self.commit_ts,
                version: self.version,
                start_key: self.start_key,
                end_key: self.end_key,
                state: next_state,
            }),
        })
    }
}
