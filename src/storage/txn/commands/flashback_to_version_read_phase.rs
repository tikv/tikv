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
        Result,
    },
    ScanMode, Snapshot, Statistics,
};

command! {
    FlashbackToVersionReadPhase:
        cmd_ty => (),
        display => "kv::command::flashback_to_version_read_phase | {:?}", (ctx),
        content => {
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

pub const FLASHBACK_BATCH_SIZE: usize = 256;

impl<S: Snapshot> ReadCommand<S> for FlashbackToVersionReadPhase {
    fn process_read(self, snapshot: S, statistics: &mut Statistics) -> Result<ProcessResult> {
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
        let mut key_writes = Vec::with_capacity(0);
        let mut has_remain_writes = false;
        // The batch is not full, we can still read.
        if self.next_write_key.is_some() && key_locks.len() < FLASHBACK_BATCH_SIZE {
            let key_writes_result = reader.scan_writes(
                self.next_write_key.as_ref(),
                self.end_key.as_ref(),
                // To flashback `CF_WRITE` and `CF_DEFAULT`, we need to delete all keys whose
                // commit_ts is greater than the specified version.
                |key| key.decode_ts().unwrap() > self.version,
                FLASHBACK_BATCH_SIZE - key_locks.len(),
                Some(self.version),
            );
            statistics.add(&reader.statistics);
            (key_writes, has_remain_writes) = key_writes_result?;
        } else if self.next_write_key.is_some() && key_locks.len() >= FLASHBACK_BATCH_SIZE {
            // The batch is full, we need to read the writes in the next batch later.
            has_remain_writes = true;
        }
        tls_collect_keyread_histogram_vec(
            self.tag().get_str(),
            (key_locks.len() + key_writes.len()) as f64,
        );

        if key_locks.is_empty() && key_writes.is_empty() {
            Ok(ProcessResult::Res)
        } else {
            let next_lock_key = if has_remain_locks {
                key_locks.last().map(|(key, _)| key.clone())
            } else {
                None
            };
            let next_write_key = if has_remain_writes && !key_writes.is_empty() {
                key_writes.last().map(|(key, _)| key.clone())
            } else if has_remain_writes && key_writes.is_empty() {
                // We haven't read any write yet, so we need to read the writes in the next
                // batch later.
                self.next_write_key
            } else {
                None
            };
            let next_cmd = FlashbackToVersion {
                ctx: self.ctx,
                deadline: self.deadline,
                version: self.version,
                end_key: self.end_key,
                key_locks,
                key_writes,
                next_lock_key,
                next_write_key,
            };
            Ok(ProcessResult::NextCommand {
                cmd: Command::FlashbackToVersion(next_cmd),
            })
        }
    }
}
