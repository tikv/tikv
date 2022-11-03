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
        Error, ErrorInner, Result, FLASHBACK_BATCH_SIZE,
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
        let (mut key_locks, has_remain_locks) = flashback_to_version_read_lock(
            &mut reader,
            &self.next_lock_key,
            &self.end_key,
            statistics,
        )?;
        // Scan the writes only if there is no more locks to unlock, otherwise, there
        // might be two writes in a single batch which will make the TiCDC panic.
        let (mut key_old_writes, mut has_remain_writes) = (Vec::with_capacity(0), true);
        if key_locks.is_empty() && !has_remain_locks {
            (key_old_writes, has_remain_writes) = flashback_to_version_read_write(
                &mut reader,
                &self.next_write_key,
                &self.end_key,
                self.version,
                self.start_ts,
                self.commit_ts,
                statistics,
            )?;
        }
        tls_collect_keyread_histogram_vec(
            self.tag().get_str(),
            (key_locks.len() + key_old_writes.len()) as f64,
        );
        // Check the next lock key.
        let next_lock_key = if has_remain_locks {
            assert_eq!(key_locks.len(), FLASHBACK_BATCH_SIZE);
            key_locks.pop().map(|(key, _)| key)
        } else {
            None
        };
        // Check the next write key.
        let next_write_key = match (has_remain_writes, key_old_writes.is_empty()) {
            (true, false) => {
                assert_eq!(key_old_writes.len(), FLASHBACK_BATCH_SIZE);
                key_old_writes.pop().map(|(key, _)| key)
            }
            // We haven't read any write yet, so we need to read the writes in the next
            // batch later.
            (true, true) => self.next_write_key,
            (..) => None,
        };
        // No keys to flashback, just return.
        if key_locks.is_empty() && key_old_writes.is_empty() {
            return Ok(ProcessResult::Res);
        }
        Ok(ProcessResult::NextCommand {
            cmd: Command::FlashbackToVersion(FlashbackToVersion {
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
            }),
        })
    }
}
