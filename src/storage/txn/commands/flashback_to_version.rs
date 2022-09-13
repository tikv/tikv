// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use txn_types::{Key, Lock, LockType, TimeStamp, Write, WriteType};

use crate::storage::{
    kv::WriteData,
    lock_manager::LockManager,
    mvcc::{MvccTxn, SnapshotReader, MAX_TXN_WRITE_SIZE},
    txn::{
        commands::{
            Command, CommandExt, FlashbackToVersionReadPhase, ResponsePolicy, TypedCommand,
            WriteCommand, WriteContext, WriteResult,
        },
        latch, Result,
    },
    ProcessResult, Snapshot,
};

command! {
    FlashbackToVersion:
        cmd_ty => (),
        display => "kv::command::flashback_to_version -> {} | {} {} | {:?}", (version, start_ts, commit_ts, ctx),
        content => {
            start_ts: TimeStamp,
            commit_ts: TimeStamp,
            version: TimeStamp,
            end_key: Option<Key>,
            next_lock_key: Option<Key>,
            next_write_key: Option<Key>,
            key_locks: Vec<(Key, Lock)>,
            key_old_writes: Vec<(Key, Option<Write>)>,
        }
}

impl CommandExt for FlashbackToVersion {
    ctx!();
    tag!(flashback_to_version);
    request_type!(KvFlashbackToVersion);

    fn gen_lock(&self) -> latch::Lock {
        latch::Lock::new(
            self.key_locks
                .iter()
                .map(|(key, _)| key)
                .chain(self.key_old_writes.iter().map(|(key, _)| key)),
        )
    }

    fn write_bytes(&self) -> usize {
        self.key_locks
            .iter()
            .map(|(key, _)| key.as_encoded().len())
            .chain(
                self.key_old_writes
                    .iter()
                    .map(|(key, _)| key.as_encoded().len()),
            )
            .sum()
    }
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for FlashbackToVersion {
    fn process_write(mut self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        let mut reader = SnapshotReader::new_with_ctx(self.version, snapshot, &self.ctx);
        let mut txn = MvccTxn::new(TimeStamp::zero(), context.concurrency_manager);

        let mut rows = 0;
        let mut next_lock_key = self.next_lock_key.take();
        let mut next_write_key = self.next_write_key.take();
        // To flashback the `CF_LOCK`, we need to delete all locks records whose
        // `start_ts` is greater than the specified version, and if it's not a
        // short-value `LockType::Put`, we need to delete the actual data from
        // `CF_DEFAULT` as well.
        // TODO: `resolved_ts` should be taken into account.
        for (key, lock) in self.key_locks {
            if txn.write_size() >= MAX_TXN_WRITE_SIZE {
                next_lock_key = Some(key);
                break;
            }
            txn.unlock_key(key.clone(), lock.is_pessimistic_txn());
            rows += 1;
            // If the short value is none and it's a `LockType::Put`, we should delete the
            // corresponding key from `CF_DEFAULT` as well.
            if lock.short_value.is_none() && lock.lock_type == LockType::Put {
                txn.delete_value(key, lock.ts);
                rows += 1;
            }
        }
        // To flashback the `CF_WRITE` and `CF_DEFAULT`, we need to write a new MVCC
        // record for each key in `self.keys` with its old value at `self.version`,
        // specifically, the flashback will have the following behavior:
        //   - If a key doesn't exist at `self.version`, it will be put a
        //     `WriteType::Delete`.
        //   - If a key exists at `self.version`, it will be put the exact same record
        //     in `CF_WRITE` and `CF_DEFAULT` if needed with `self.commit_ts` and
        //     `self.start_ts`.
        for (key, old_write) in self.key_old_writes {
            if txn.write_size() >= MAX_TXN_WRITE_SIZE {
                next_write_key = Some(key);
                break;
            }
            // If the old write doesn't exist, we should put a `WriteType::Delete` record to
            // delete the current key.
            if old_write.is_none() {
                let new_write = Write::new(WriteType::Delete, self.start_ts, None);
                txn.put_write(key.clone(), self.commit_ts, new_write.as_ref().to_bytes());
                rows += 1;
                continue;
            }
            let old_write = old_write.unwrap();
            // If it's not a short value and it's a `WriteType::Put`, we should put the old
            // value in `CF_DEFAULT` with `self.start_ts` as well.
            if old_write.short_value.is_none() && old_write.write_type == WriteType::Put {
                if let Some(old_value) = reader.get(&key, self.version)? {
                    txn.put_value(key.clone(), self.start_ts, old_value);
                    rows += 1;
                }
            }
            let new_write = Write::new(old_write.write_type, self.start_ts, old_write.short_value);
            txn.put_write(key.clone(), self.commit_ts, new_write.as_ref().to_bytes());
            rows += 1;
        }

        let mut write_data = WriteData::from_modifies(txn.into_modifies());
        write_data.extra.for_flashback = true;
        Ok(WriteResult {
            ctx: self.ctx.clone(),
            to_be_write: write_data,
            rows,
            pr: if next_lock_key.is_none() && next_write_key.is_none() {
                ProcessResult::Res
            } else {
                let next_cmd = FlashbackToVersionReadPhase {
                    ctx: self.ctx.clone(),
                    deadline: self.deadline,
                    start_ts: self.start_ts,
                    commit_ts: self.commit_ts,
                    version: self.version,
                    end_key: self.end_key,
                    next_lock_key,
                    next_write_key,
                };
                ProcessResult::NextCommand {
                    cmd: Command::FlashbackToVersionReadPhase(next_cmd),
                }
            },
            lock_info: None,
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
        })
    }
}
