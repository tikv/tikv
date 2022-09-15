// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
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
        display => "kv::command::flashback_to_version_read_phase -> {} | {} {} | {:?}", (version, start_ts, commit_ts, ctx),
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
        let mut reader = MvccReader::new_with_ctx(snapshot, Some(ScanMode::Forward), &self.ctx);
        // TODO: maybe we should resolve all locks before starting a flashback.
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
        let mut key_old_writes = Vec::with_capacity(0);
        let mut has_remain_writes = false;
        // The batch is not full, we can still read.
        if self.next_write_key.is_some() && key_locks.len() < FLASHBACK_BATCH_SIZE {
            // To flashback the data, we need to get all the latest keys first by scanning
            // every unique key in `CF_WRITE` and to get its corresponding old MVCC write
            // record if exists.
            let key_ts_old_writes;
            (key_ts_old_writes, has_remain_writes) = reader.scan_writes(
                self.next_write_key.as_ref(),
                self.end_key.as_ref(),
                Some(self.version),
                // No need to find an old version for the key if its latest `commit_ts` is smaller
                // than or equal to the version.
                |key| key.decode_ts().unwrap_or(TimeStamp::zero()) > self.version,
                FLASHBACK_BATCH_SIZE - key_locks.len(),
            )?;
            statistics.add(&reader.statistics);
            // Check the latest commit ts to make sure there is no commit change during the
            // flashback, otherwise, we need to abort the flashback.
            for (key, commit_ts, old_write) in key_ts_old_writes {
                if commit_ts >= self.commit_ts {
                    return Err(Error::from(ErrorInner::InvalidTxnTso {
                        start_ts: self.start_ts,
                        commit_ts: self.commit_ts,
                    }));
                }
                key_old_writes.push((key, old_write));
            }
        } else if self.next_write_key.is_some() && key_locks.len() >= FLASHBACK_BATCH_SIZE {
            // The batch is full, we need to read the writes in the next batch later.
            has_remain_writes = true;
        }
        tls_collect_keyread_histogram_vec(
            self.tag().get_str(),
            (key_locks.len() + key_old_writes.len()) as f64,
        );

        if key_locks.is_empty() && key_old_writes.is_empty() {
            Ok(ProcessResult::Res)
        } else {
            let next_lock_key = if has_remain_locks {
                key_locks.last().map(|(key, _)| key.clone())
            } else {
                None
            };
            let next_write_key = if has_remain_writes && !key_old_writes.is_empty() {
                key_old_writes.pop().map(|(key, _)| key)
            } else if has_remain_writes && key_old_writes.is_empty() {
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
                key_old_writes,
                next_lock_key,
                next_write_key,
            };
            Ok(ProcessResult::NextCommand {
                cmd: Command::FlashbackToVersion(next_cmd),
            })
        }
    }
}
