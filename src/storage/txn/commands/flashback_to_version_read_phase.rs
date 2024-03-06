// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::ops::Bound;

// #[PerformanceCriticalPath]
use txn_types::{Key, Lock, TimeStamp};

use crate::storage::{
    mvcc::MvccReader,
    txn::{
        actions::flashback_to_version::{check_flashback_commit, get_first_user_key},
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
    end_key: Option<Key>,
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
    end_key: Option<Key>,
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

/// The whole flashback progress contains four phases:
///   1. [PrepareFlashback] RollbackLock phase:
///     - Scan all locks.
///     - Rollback all these locks.
///   2. [PrepareFlashback] Prewrite phase:
///     - Prewrite the first user key after `self.start_key` specifically to
///       prevent the `resolved_ts` from advancing.
///   3. [FinishFlashback] FlashbackWrite phase:
///     - Scan all the latest writes and their corresponding values at
///       `self.version`.
///     - Write the old MVCC version writes again for all these keys with
///       `self.commit_ts` excluding the first user key after `self.start_key`.
///   4. [FinishFlashback] Commit phase:
///     - Commit the first user key after `self.start_key` we write at the
///       second phase to finish the flashback.
impl<S: Snapshot> ReadCommand<S> for FlashbackToVersionReadPhase {
    fn process_read(self, snapshot: S, statistics: &mut Statistics) -> Result<ProcessResult> {
        let tag = self.tag().get_str();
        let mut reader = MvccReader::new_with_ctx(snapshot, Some(ScanMode::Forward), &self.ctx);
        // Filter out the SST that does not have a newer version than `self.version` in
        // `CF_WRITE`, i.e, whose latest `commit_ts` <= `self.version` in the later
        // scan. By doing this, we can only flashback those keys that have version
        // changed since `self.version` as much as possible.
        reader.set_hint_min_ts(Some(Bound::Excluded(self.version)));
        let mut start_key = self.start_key.clone();
        let next_state = match self.state {
            FlashbackToVersionState::RollbackLock { next_lock_key, .. } => {
                let mut key_locks = flashback_to_version_read_lock(
                    &mut reader,
                    next_lock_key,
                    self.end_key.as_ref(),
                    self.start_ts,
                )?;
                if key_locks.is_empty() {
                    // - No more locks to rollback, continue to the Prewrite Phase.
                    // - The start key from the client is actually a range which is used to limit
                    //   the upper bound of this flashback when scanning data, so it may not be a
                    //   real key. In the Prewrite Phase, we make sure that the start key is a real
                    //   key and take this key as a lock for the 2pc. So When overwriting the write,
                    //   we skip the immediate write of this key and instead put it after the
                    //   completion of the 2pc.
                    // - To make sure the key locked in the latch is the same as the actual key
                    //   written, we pass it to the key in `process_write' after getting it.
                    let key_to_lock = if let Some(first_key) = get_first_user_key(
                        &mut reader,
                        &self.start_key,
                        self.end_key.as_ref(),
                        self.version,
                    )? {
                        first_key
                    } else {
                        // If the key is None return directly
                        statistics.add(&reader.statistics);
                        return Ok(ProcessResult::Res);
                    };
                    FlashbackToVersionState::Prewrite { key_to_lock }
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
            FlashbackToVersionState::FlashbackWrite {
                mut next_write_key, ..
            } => {
                if self.commit_ts <= self.start_ts {
                    return Err(Error::from(ErrorInner::InvalidTxnTso {
                        start_ts: self.start_ts,
                        commit_ts: self.commit_ts,
                    }));
                }
                if next_write_key == self.start_key {
                    // The start key from the client is actually a range which is used to limit the
                    // upper bound of this flashback when scanning data, so it may not be a real
                    // key. In the Prewrite Phase, we make sure that the start
                    // key is a real key and take this key as a lock for the
                    // 2pc. So When overwriting the write, we skip the immediate
                    // write of this key and instead put it after the completion
                    // of the 2pc.
                    next_write_key = if let Some(first_key) = get_first_user_key(
                        &mut reader,
                        &self.start_key,
                        self.end_key.as_ref(),
                        self.version,
                    )? {
                        first_key
                    } else {
                        // If the key is None return directly
                        statistics.add(&reader.statistics);
                        return Ok(ProcessResult::Res);
                    };
                    // Commit key needs to match the Prewrite key, which is set as the first user
                    // key.
                    start_key = next_write_key.clone();
                    // If the key has already been committed by the flashback, it means that we are
                    // in a retry. It's safe to just return directly.
                    if check_flashback_commit(
                        &mut reader,
                        &start_key,
                        self.start_ts,
                        self.commit_ts,
                        self.ctx.get_region_id(),
                    )? {
                        statistics.add(&reader.statistics);
                        return Ok(ProcessResult::Res);
                    }
                }
                let mut keys = flashback_to_version_read_write(
                    &mut reader,
                    next_write_key,
                    &start_key,
                    self.end_key.as_ref(),
                    self.version,
                    self.commit_ts,
                )?;
                if keys.is_empty() {
                    FlashbackToVersionState::Commit {
                        key_to_commit: start_key.clone(),
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
        statistics.add(&reader.statistics);
        Ok(ProcessResult::NextCommand {
            cmd: Command::FlashbackToVersion(FlashbackToVersion {
                ctx: self.ctx,
                deadline: self.deadline,
                start_ts: self.start_ts,
                commit_ts: self.commit_ts,
                version: self.version,
                start_key,
                end_key: self.end_key,
                state: next_state,
            }),
        })
    }
}
