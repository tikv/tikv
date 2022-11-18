// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use txn_types::{Key, Lock, TimeStamp, Write};

use crate::storage::{
    mvcc::MvccReader,
    txn::{
        commands::{
            Command, CommandExt, FlashbackToVersion, ProcessResult, ReadCommand, TypedCommand,
        },
        flashback_to_version_read_lock, flashback_to_version_read_write,
        sched_pool::tls_collect_keyread_histogram_vec,
        Result,
    },
    Context, ScanMode, Snapshot, Statistics,
};

#[derive(Debug)]
pub enum FlashbackToVersionState {
    Lock {
        key_to_lock: Key,
    },
    ScanWrite {
        next_write_key: Key,
        key_old_writes: Vec<(Key, Option<Write>)>,
    },
    ScanLock {
        next_lock_key: Key,
        key_locks: Vec<(Key, Lock)>,
    },
}

pub fn new_flashback_to_version_read_phase_cmd(
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
        FlashbackToVersionState::ScanWrite {
            next_write_key: start_key,
            key_old_writes: Vec::new(),
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

/// The whole flashback progress contains there phases:
///   1. Lock phase:
///     - Lock the `self.start_key` specifically to prevent the `resolved_ts`
///       from advancing.
///   2. Read-and-flashback writes phase:
///     - Scan all the latest writes and their corresponding values at
///       `self.version`.
///     - Write the old MVCC version writes again for all these keys with
///       `self.commit_ts`.
///   3. Read-and-rollback locks phase:
///     - Scan all locks.
///     - Rollback all these locks including the `self.start_key` lock we write
///       at the first phase.
impl<S: Snapshot> ReadCommand<S> for FlashbackToVersionReadPhase {
    fn process_read(self, snapshot: S, statistics: &mut Statistics) -> Result<ProcessResult> {
        let tag = self.tag().get_str();
        let mut read_again = false;
        let mut reader = MvccReader::new_with_ctx(snapshot, Some(ScanMode::Forward), &self.ctx);
        // Separate the lock and write flashback to prevent from putting two writes for
        // the same key in a single batch to make the TiCDC panic.
        let next_state = match self.state {
            FlashbackToVersionState::ScanWrite { next_write_key, .. } => {
                let mut key_old_writes = flashback_to_version_read_write(
                    &mut reader,
                    next_write_key,
                    &self.end_key,
                    self.version,
                    self.commit_ts,
                    statistics,
                )?;
                if key_old_writes.is_empty() {
                    // No more writes to flashback, continue to scan the locks.
                    read_again = true;
                    FlashbackToVersionState::ScanLock {
                        next_lock_key: self.start_key.clone(),
                        key_locks: Vec::new(),
                    }
                } else {
                    tls_collect_keyread_histogram_vec(tag, key_old_writes.len() as f64);
                    FlashbackToVersionState::ScanWrite {
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
            FlashbackToVersionState::ScanLock { next_lock_key, .. } => {
                let mut key_locks = flashback_to_version_read_lock(
                    &mut reader,
                    next_lock_key,
                    &self.end_key,
                    statistics,
                )?;
                if key_locks.is_empty() {
                    return Ok(ProcessResult::Res);
                }
                tls_collect_keyread_histogram_vec(tag, key_locks.len() as f64);
                FlashbackToVersionState::ScanLock {
                    next_lock_key: if key_locks.len() > 1 {
                        key_locks.pop().map(|(key, _)| key).unwrap()
                    } else {
                        key_locks.last().map(|(key, _)| key.clone()).unwrap()
                    },
                    key_locks,
                }
            }
            _ => unreachable!(),
        };
        Ok(ProcessResult::NextCommand {
            cmd: if read_again {
                Command::FlashbackToVersionReadPhase(FlashbackToVersionReadPhase {
                    ctx: self.ctx,
                    deadline: self.deadline,
                    start_ts: self.start_ts,
                    commit_ts: self.commit_ts,
                    version: self.version,
                    start_key: self.start_key,
                    end_key: self.end_key,
                    state: next_state,
                })
            } else {
                Command::FlashbackToVersion(FlashbackToVersion {
                    ctx: self.ctx,
                    deadline: self.deadline,
                    start_ts: self.start_ts,
                    commit_ts: self.commit_ts,
                    version: self.version,
                    start_key: self.start_key,
                    end_key: self.end_key,
                    state: next_state,
                })
            },
        })
    }
}
