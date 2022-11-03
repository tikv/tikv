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
    ScanMode, Snapshot, Statistics,
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

command! {
    FlashbackToVersionReadPhase:
        cmd_ty => (),
        display => "kv::command::flashback_to_version_read_phase -> {} | {} {} | {:?}", (version, start_ts, commit_ts, ctx),
        content => {
            start_ts: TimeStamp,
            commit_ts: TimeStamp,
            version: TimeStamp,
            end_key: Option<Key>,
            start_lock_key: Option<Key>,
            start_write_key: Option<Key>,
            state: Option<FlashbackToVersionState>,
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
        let cur_state = self.state.unwrap_or(FlashbackToVersionState::ScanLock {
            next_lock_key: self.start_lock_key.clone(),
            key_locks: Vec::with_capacity(0),
        });
        let mut read_again = false;
        let mut reader = MvccReader::new_with_ctx(snapshot, Some(ScanMode::Forward), &self.ctx);
        // Separate the lock and write flashback to prevent from putting two writes for
        // the same key in a single batch to make the TiCDC panic.
        let next_state = match cur_state {
            FlashbackToVersionState::ScanLock { next_lock_key, .. } => {
                let (mut key_locks, has_remain_locks) = flashback_to_version_read_lock(
                    &mut reader,
                    &next_lock_key,
                    &self.end_key,
                    statistics,
                )?;
                let next_state;
                if key_locks.is_empty() && !has_remain_locks {
                    // No more locks to flashback, continue to scan the writes.
                    next_state = Some(FlashbackToVersionState::ScanWrite {
                        next_write_key: self.start_write_key.clone(),
                        key_old_writes: Vec::with_capacity(0),
                    });
                    read_again = true;
                } else {
                    tls_collect_keyread_histogram_vec(tag, key_locks.len() as f64);
                    next_state = Some(FlashbackToVersionState::ScanLock {
                        next_lock_key: if has_remain_locks {
                            // The batch must be full.
                            assert_eq!(key_locks.len(), FLASHBACK_BATCH_SIZE);
                            key_locks.pop().map(|(key, _)| key)
                        } else {
                            None
                        },
                        key_locks,
                    });
                }
                next_state
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
                let mut next_state = None;
                if !key_old_writes.is_empty() {
                    tls_collect_keyread_histogram_vec(tag, key_old_writes.len() as f64);
                    next_state = Some(FlashbackToVersionState::ScanWrite {
                        next_write_key: if has_remain_writes {
                            assert_eq!(key_old_writes.len(), FLASHBACK_BATCH_SIZE);
                            key_old_writes.pop().map(|(key, _)| key)
                        } else {
                            None
                        },
                        key_old_writes,
                    });
                };
                next_state
            }
        };
        // No more keys to flashback, just return.
        if next_state.is_none() {
            return Ok(ProcessResult::Res);
        }
        Ok(ProcessResult::NextCommand {
            cmd: if read_again {
                Command::FlashbackToVersionReadPhase(FlashbackToVersionReadPhase {
                    ctx: self.ctx,
                    deadline: self.deadline,
                    start_ts: self.start_ts,
                    commit_ts: self.commit_ts,
                    version: self.version,
                    end_key: self.end_key,
                    start_lock_key: self.start_lock_key,
                    start_write_key: self.start_write_key,
                    state: next_state,
                })
            } else {
                Command::FlashbackToVersion(FlashbackToVersion {
                    ctx: self.ctx,
                    deadline: self.deadline,
                    start_ts: self.start_ts,
                    commit_ts: self.commit_ts,
                    version: self.version,
                    end_key: self.end_key,
                    start_lock_key: self.start_lock_key,
                    start_write_key: self.start_write_key,
                    state: next_state,
                })
            },
        })
    }
}
