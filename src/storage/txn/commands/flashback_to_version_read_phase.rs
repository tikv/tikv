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
        Error, ErrorInner, Result, FLASHBACK_BATCH_SIZE,
    },
    Context, ScanMode, Snapshot, Statistics,
};

pub enum FlashbackToVersionState {
    ScanLock {
        next_lock_key: Option<Key>,
        key_locks: Vec<(Key, Lock)>,
    },
    ScanWrite {
        next_write_key: Option<Key>,
        key_old_writes: Vec<(Key, Option<Write>)>,
    },
}

pub fn new_flashback_to_version_read_phase_cmd(
    start_ts: TimeStamp,
    commit_ts: TimeStamp,
    version: TimeStamp,
    start_key: Option<Key>,
    end_key: Option<Key>,
    ctx: Context,
) -> TypedCommand<()> {
    FlashbackToVersionReadPhase::new(
        start_ts,
        commit_ts,
        version,
        start_key.clone(),
        end_key,
        FlashbackToVersionState::ScanLock {
            next_lock_key: start_key,
            key_locks: Vec::with_capacity(0),
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
            start_key: Option<Key>,
            end_key: Option<Key>,
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

/// FlashbackToVersion contains two phases:
///   1. Read phase:
///     - Scan all locks to delete them all later.
///     - Scan all the latest writes to flashback them all later.
///   2. Write phase:
///     - Delete all locks we scanned at the read phase.
///     - Write the old MVCC version writes for the keys we scanned at the read
///       phase.
impl<S: Snapshot> ReadCommand<S> for FlashbackToVersionReadPhase {
    fn process_read(self, snapshot: S, statistics: &mut Statistics) -> Result<ProcessResult> {
        if self.commit_ts <= self.start_ts {
            return Err(Error::from(ErrorInner::InvalidTxnTso {
                start_ts: self.start_ts,
                commit_ts: self.commit_ts,
            }));
        }
        let tag = self.tag().get_str();
        let mut read_again = false;
        let mut reader = MvccReader::new_with_ctx(snapshot, Some(ScanMode::Forward), &self.ctx);
        // Separate the lock and write flashback to prevent from putting two writes for
        // the same key in a single batch to make the TiCDC panic.
        let next_state = match self.state {
            FlashbackToVersionState::ScanLock { next_lock_key, .. } => {
                let (mut key_locks, has_remain_locks) = flashback_to_version_read_lock(
                    &mut reader,
                    &next_lock_key,
                    &self.end_key,
                    statistics,
                )?;
                if key_locks.is_empty() && !has_remain_locks {
                    // No more locks to flashback, continue to scan the writes.
                    read_again = true;
                    FlashbackToVersionState::ScanWrite {
                        next_write_key: self.start_key.clone(),
                        key_old_writes: Vec::with_capacity(0),
                    }
                } else {
                    tls_collect_keyread_histogram_vec(tag, key_locks.len() as f64);
                    FlashbackToVersionState::ScanLock {
                        next_lock_key: if has_remain_locks {
                            // The batch must be full.
                            assert_eq!(key_locks.len(), FLASHBACK_BATCH_SIZE);
                            key_locks.pop().map(|(key, _)| key)
                        } else {
                            None
                        },
                        key_locks,
                    }
                }
            }
            FlashbackToVersionState::ScanWrite { next_write_key, .. } => {
                let (mut key_old_writes, has_remain_writes) = flashback_to_version_read_write(
                    &mut reader,
                    &next_write_key,
                    &self.end_key,
                    self.version,
                    self.start_ts,
                    self.commit_ts,
                    statistics,
                )?;
                if key_old_writes.is_empty() {
                    // No more keys to flashback, just return.
                    return Ok(ProcessResult::Res);
                }
                tls_collect_keyread_histogram_vec(tag, key_old_writes.len() as f64);
                FlashbackToVersionState::ScanWrite {
                    next_write_key: if has_remain_writes {
                        assert_eq!(key_old_writes.len(), FLASHBACK_BATCH_SIZE);
                        key_old_writes.pop().map(|(key, _)| key)
                    } else {
                        None
                    },
                    key_old_writes,
                }
            }
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
