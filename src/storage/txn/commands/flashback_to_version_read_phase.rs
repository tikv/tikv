// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use txn_types::{Key, TimeStamp};

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
        // Scan the locks.
        let (key_locks, has_remain_locks) = flashback_to_version_read_lock(
            &mut reader,
            &self.next_lock_key,
            &self.end_key,
            statistics,
        )?;
        // Scan the writes.
        let (mut key_old_writes, has_remain_writes) = flashback_to_version_read_write(
            &mut reader,
            key_locks.len(),
            &self.next_write_key,
            &self.end_key,
            self.version,
            self.start_ts,
            self.commit_ts,
            statistics,
        )?;
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
