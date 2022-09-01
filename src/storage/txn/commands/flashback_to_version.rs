// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use txn_types::{Key, Lock, LockType, TimeStamp, Write, WriteType};

use crate::storage::{
    kv::WriteData,
    lock_manager::LockManager,
    mvcc::{MvccTxn, MAX_TXN_WRITE_SIZE},
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
        display => "kv::command::flashback_to_version @{} | {:?}", (version ,ctx),
        content => {
            version: TimeStamp,
            end_key: Option<Key>,
            next_lock_key: Option<Key>,
            next_write_key: Option<Key>,
            key_locks: Vec<(Key, Lock)>,
            key_writes: Vec<(Key, Write)>,
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
                .chain(self.key_writes.iter().map(|(key, _)| key)),
        )
    }

    fn write_bytes(&self) -> usize {
        self.key_locks
            .iter()
            .map(|(key, _)| key.as_encoded().len())
            .chain(
                self.key_writes
                    .iter()
                    .map(|(key, _)| key.as_encoded().len()),
            )
            .sum()
    }
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for FlashbackToVersion {
    fn process_write(mut self, _snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
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
        // To flashback the `CF_WRITE`, we need to delete all write records whose
        // `commit_ts` is greater than the specified version, and if it's not a
        // short-value `WriteType::Put`, we need to delete the actual data from
        // `CF_DEFAULT` as well.
        for (key, write) in self.key_writes {
            if txn.write_size() >= MAX_TXN_WRITE_SIZE {
                next_write_key = Some(key);
                break;
            }
            let encoded_key = key.clone().truncate_ts()?;
            let commit_ts = key.decode_ts()?;
            txn.delete_write(encoded_key.clone(), commit_ts);
            rows += 1;
            // If the short value is none and it's a `WriteType::Put`, we should delete the
            // corresponding key from `CF_DEFAULT` as well.
            if write.short_value.is_none() && write.write_type == WriteType::Put {
                txn.delete_value(encoded_key, write.start_ts);
                rows += 1;
            }
        }

        let mut write_data = WriteData::from_modifies(txn.into_modifies());
        write_data.set_allowed_on_disk_almost_full();
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
