// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::cell::RefCell;

use txn_types::{Key, TimeStamp};

use crate::storage::{
    mvcc::MvccReader,
    txn::{
        commands::{
            Command, CommandExt, FlashbackToVersion, ProcessResult, ReadCommand, TypedCommand,
        },
        sched_pool::tls_collect_keyread_histogram_vec,
        Error, ErrorInner, Result,
    },
    ScanMode, Snapshot, Statistics,
};

command! {
    FlashbackToVersionReadPhase:
        cmd_ty => (),
        display => "kv::command::flashback_to_version_read_phase | {:?}", (ctx),
        content => {
            start_ts: TimeStamp,
            commit_ts: TimeStamp,
            version: TimeStamp,
            end_key: Option<Key>,
            next_lock_key: Option<Key>,
            next_write_key: Option<Key>,
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

pub const FLASHBACK_BATCH_SIZE: usize = 256 + 1 /* To store the next key for multiple batches */;

/// FlashbackToVersion contains two phases:
///   1. Read phase:
///     - Scan all locks to delete them all later.
///     - Scan all the latest writes to flashback them all later.
///  2. Write phase:
///    - Delete all locks we scanned at the read phase.
///    - Write the old MVCC version writes for the keys we scanned at the read
///      phase.
impl<S: Snapshot> ReadCommand<S> for FlashbackToVersionReadPhase {
    fn process_read(self, snapshot: S, statistics: &mut Statistics) -> Result<ProcessResult> {
        if self.commit_ts <= self.start_ts {
            return Err(Error::from(ErrorInner::InvalidTxnTso {
                start_ts: self.start_ts,
                commit_ts: self.commit_ts,
            }));
        }
        let mut reader = MvccReader::new_with_ctx(snapshot, Some(ScanMode::Forward), &self.ctx);
        // Scan the locks.
        let mut key_locks = Vec::with_capacity(0);
        let mut has_remain_locks = false;
        if self.next_lock_key.is_some() {
            let key_locks_result = reader.scan_locks(
                self.next_lock_key.as_ref(),
                self.end_key.as_ref(),
                // To flashback `CF_LOCK`, we need to delete all locks.
                |_| true,
                FLASHBACK_BATCH_SIZE,
            );
            statistics.add(&reader.statistics);
            (key_locks, has_remain_locks) = key_locks_result?;
        }
        // Scan the writes.
        let mut keys = Vec::with_capacity(0);
        let mut has_remain_writes = false;
        // The batch is not full, we can still read.
        if self.next_write_key.is_some() && key_locks.len() < FLASHBACK_BATCH_SIZE {
            let cur_key = RefCell::new(None);
            let key_writes_result = reader.scan_writes(
                self.next_write_key.as_ref(),
                self.end_key.as_ref(),
                // To flashback the data, we need to get all the latest writes first by scanning
                // every unique key in `CF_WRITE`.
                move |key| {
                    if let Ok(truncated_key) = key.clone().truncate_ts() {
                        let mut cur_key_rc = cur_key.borrow_mut();
                        if cur_key_rc.is_some() && truncated_key == *cur_key_rc.as_ref().unwrap() {
                            return false;
                        }
                        *cur_key_rc = Some(truncated_key);
                        return true;
                    }
                    false
                },
                FLASHBACK_BATCH_SIZE - key_locks.len(),
                None,
            );
            statistics.add(&reader.statistics);
            // Truncate the timestamps of all the keys.
            let key_writes;
            (key_writes, has_remain_writes) = key_writes_result?;
            for (key, _) in key_writes {
                if key.decode_ts()? >= self.commit_ts {
                    return Err(Error::from(ErrorInner::InvalidTxnTso {
                        start_ts: self.start_ts,
                        commit_ts: self.commit_ts,
                    }));
                }
                keys.push(key.truncate_ts()?);
            }
        } else if self.next_write_key.is_some() && key_locks.len() >= FLASHBACK_BATCH_SIZE {
            // The batch is full, we need to read the writes in the next batch later.
            has_remain_writes = true;
        }
        tls_collect_keyread_histogram_vec(
            self.tag().get_str(),
            (key_locks.len() + keys.len()) as f64,
        );

        if key_locks.is_empty() && keys.is_empty() {
            Ok(ProcessResult::Res)
        } else {
            let next_lock_key = if has_remain_locks {
                key_locks.last().map(|(key, _)| key.clone())
            } else {
                None
            };
            let next_write_key = if has_remain_writes && !keys.is_empty() {
                keys.pop()
            } else if has_remain_writes && keys.is_empty() {
                // We haven't read any write yet, so we need to read the writes in the next
                // batch later.
                self.next_write_key
            } else {
                None
            };
            let next_cmd = FlashbackToVersion {
                ctx: self.ctx,
                deadline: self.deadline,
                start_ts: self.start_ts,
                commit_ts: self.commit_ts,
                version: self.version,
                end_key: self.end_key,
                key_locks,
                keys,
                next_lock_key,
                next_write_key,
            };
            Ok(ProcessResult::NextCommand {
                cmd: Command::FlashbackToVersion(next_cmd),
            })
        }
    }
}
