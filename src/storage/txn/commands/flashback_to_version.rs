// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::mem;

use tikv_kv::ScanMode;
use txn_types::{Key, TimeStamp};

use crate::storage::{
    ProcessResult, Snapshot,
    kv::WriteData,
    lock_manager::LockManager,
    metrics::{CommandKind, KV_COMMAND_COUNTER_VEC_STATIC},
    mvcc::{MvccReader, MvccTxn, SnapshotReader},
    txn::{
        Result,
        actions::flashback_to_version::{
            commit_flashback_key, flashback_to_version_write, prewrite_flashback_key,
            rollback_locks,
        },
        commands::{
            Command, CommandExt, FlashbackToVersionReadPhase, FlashbackToVersionState,
            ReaderWithStats, ReleasedLocks, ResponsePolicy, TypedCommand, WriteCommand,
            WriteContext, WriteResult,
        },
        latch,
    },
};

command! {
    FlashbackToVersion:
        cmd_ty => (),
        display => {
            "kv::command::flashback_to_version -> {} | {} {} | {:?}",
            (version, start_ts, commit_ts, ctx),
        }
        content => {
            start_ts: TimeStamp,
            commit_ts: TimeStamp,
            version: TimeStamp,
            start_key: Key,
            end_key: Option<Key>,
            state: FlashbackToVersionState,
        }
        in_heap => {
            start_key,
            end_key,
        }
}

impl CommandExt for FlashbackToVersion {
    ctx!();
    request_type!(KvFlashbackToVersion);

    fn gen_lock(&self) -> latch::Lock {
        match &self.state {
            FlashbackToVersionState::RollbackLock { key_locks, .. } => {
                latch::Lock::new(key_locks.iter().map(|(key, _)| key))
            }
            FlashbackToVersionState::Prewrite { key_to_lock } => latch::Lock::new([key_to_lock]),
            FlashbackToVersionState::FlashbackWrite { keys, .. } => latch::Lock::new(keys.iter()),
            FlashbackToVersionState::Commit { key_to_commit } => latch::Lock::new([key_to_commit]),
        }
    }

    fn write_bytes(&self) -> usize {
        match &self.state {
            FlashbackToVersionState::RollbackLock { key_locks, .. } => key_locks
                .iter()
                .map(|(key, _)| key.as_encoded().len())
                .sum(),
            FlashbackToVersionState::Prewrite { key_to_lock } => key_to_lock.as_encoded().len(),
            FlashbackToVersionState::FlashbackWrite { keys, .. } => {
                keys.iter().map(|key| key.as_encoded().len()).sum()
            }
            FlashbackToVersionState::Commit { key_to_commit } => key_to_commit.as_encoded().len(),
        }
    }

    fn tag(&self) -> CommandKind {
        match self.state {
            FlashbackToVersionState::RollbackLock { .. } => {
                CommandKind::flashback_to_version_rollback_lock
            }
            _ => CommandKind::flashback_to_version_write,
        }
    }

    fn incr_cmd_metric(&self) {
        match self.state {
            FlashbackToVersionState::RollbackLock { .. } => {
                KV_COMMAND_COUNTER_VEC_STATIC
                    .flashback_to_version_rollback_lock
                    .inc();
            }
            _ => KV_COMMAND_COUNTER_VEC_STATIC
                .flashback_to_version_write
                .inc(),
        }
    }
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for FlashbackToVersion {
    fn process_write(mut self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        // Each holder on a shared key restarts the write search at the newest
        // version, so rollback needs a bidirectional cursor.
        let scan_mode = match self.state {
            FlashbackToVersionState::RollbackLock { .. } => None,
            _ => Some(ScanMode::Forward),
        };
        let mut mvcc_reader = MvccReader::new_with_ctx(snapshot, scan_mode, &self.ctx);
        mvcc_reader.set_allow_in_flashback(true);
        let mut reader_with_stats = ReaderWithStats::new(
            SnapshotReader {
                reader: mvcc_reader,
                start_ts: self.start_ts,
            },
            context.statistics,
        );
        let reader = &mut *reader_with_stats;
        let mut txn = MvccTxn::new(TimeStamp::zero(), context.concurrency_manager);
        match self.state {
            FlashbackToVersionState::RollbackLock {
                ref mut next_lock_key,
                ref mut key_locks,
            } => {
                if let Some(new_next_lock_key) =
                    rollback_locks(&mut txn, reader, mem::take(key_locks))?
                {
                    *next_lock_key = new_next_lock_key;
                }
            }
            FlashbackToVersionState::Prewrite { ref key_to_lock } => prewrite_flashback_key(
                &mut txn,
                &mut reader.reader,
                key_to_lock,
                self.version,
                self.start_ts,
            )?,
            FlashbackToVersionState::FlashbackWrite {
                ref mut next_write_key,
                ref mut keys,
            } => {
                if let Some(new_next_write_key) = flashback_to_version_write(
                    &mut txn,
                    &mut reader.reader,
                    mem::take(keys),
                    self.version,
                    self.start_ts,
                    self.commit_ts,
                )? {
                    *next_write_key = new_next_write_key;
                }
            }
            FlashbackToVersionState::Commit { ref key_to_commit } => commit_flashback_key(
                &mut txn,
                &mut reader.reader,
                key_to_commit,
                self.start_ts,
                self.commit_ts,
            )?,
        }
        let rows = txn.modifies.len();
        let shared_lock_upgrade_hints = txn.take_shared_lock_upgrade_hints();
        let mut write_data = WriteData::from_modifies(txn.into_modifies());
        // To let the flashback modification could be proposed and applied successfully.
        write_data.extra.allowed_in_flashback = true;
        // To let the CDC treat the flashback modification as an 1PC transaction.
        if matches!(self.state, FlashbackToVersionState::FlashbackWrite { .. }) {
            write_data.extra.one_pc = true;
        }
        Ok(WriteResult {
            ctx: self.ctx.clone(),
            to_be_write: write_data,
            rows,
            pr: (move || {
                if matches!(
                    self.state,
                    FlashbackToVersionState::Prewrite { .. }
                        | FlashbackToVersionState::Commit { .. }
                ) {
                    return ProcessResult::Res;
                }

                #[cfg(feature = "failpoints")]
                if matches!(self.state, FlashbackToVersionState::FlashbackWrite { .. }) {
                    fail_point!("flashback_failed_after_first_batch", |_| {
                        ProcessResult::Res
                    });
                }

                ProcessResult::NextCommand {
                    cmd: Command::FlashbackToVersionReadPhase(FlashbackToVersionReadPhase {
                        ctx: self.ctx,
                        deadline: self.deadline,
                        start_ts: self.start_ts,
                        commit_ts: self.commit_ts,
                        version: self.version,
                        start_key: self.start_key,
                        end_key: self.end_key,
                        state: self.state,
                    }),
                }
            })(),
            lock_info: vec![],
            released_locks: ReleasedLocks::new(),
            new_acquired_locks: vec![],
            shared_lock_upgrade_hints,
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
            known_txn_status: vec![],
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use concurrency_manager::ConcurrencyManager;
    use tikv_util::deadline::Deadline;

    use super::*;
    use crate::storage::{
        Engine, Statistics, TestEngineBuilder,
        lock_manager::MockLockManager,
        mvcc::{
            MAX_TXN_WRITE_SIZE,
            tests::{must_get_rollback_protected, must_load_shared_lock, must_unlocked, write},
        },
        txn::{
            flashback_to_version_read_lock, scheduler::DEFAULT_EXECUTION_DURATION_LIMIT,
            tests::must_acquire_shared_pessimistic_lock, txn_status_cache::TxnStatusCache,
        },
    };

    #[test]
    fn test_flashback_shared_rollback_budget_and_read_statistics() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let raw = vec![b'q'; 768];
        let key = Key::from_raw(&raw);
        // The wrapper fits in 32 KiB. Rolling back every member at once exceeds
        // it because each write CF key includes the full encoded user key.
        let count = 39;
        for ts in 1..=count {
            must_acquire_shared_pessimistic_lock(&mut engine, &raw, &raw, ts, ts, 3000);
        }

        let start_ts = TimeStamp::from(count + 1);
        let ctx = Default::default();
        let end = Key::from_raw(b"z");
        let mut remaining = count as usize;
        let mut batches = 0;
        loop {
            let snapshot = engine.snapshot(Default::default()).unwrap();
            let mut scan =
                MvccReader::new_with_ctx(snapshot.clone(), Some(ScanMode::Forward), &ctx);
            let key_locks =
                flashback_to_version_read_lock(&mut scan, key.clone(), Some(&end), start_ts)
                    .unwrap();
            let command = FlashbackToVersion {
                ctx: ctx.clone(),
                deadline: Deadline::from_now(DEFAULT_EXECUTION_DURATION_LIMIT),
                start_ts,
                commit_ts: TimeStamp::zero(),
                version: TimeStamp::zero(),
                start_key: key.clone(),
                end_key: Some(end.clone()),
                state: FlashbackToVersionState::RollbackLock {
                    next_lock_key: end.clone(),
                    key_locks,
                },
            };
            let mut statistics = Statistics::default();
            let result = command
                .process_write(
                    snapshot,
                    WriteContext {
                        lock_mgr: &MockLockManager::new(),
                        concurrency_manager: ConcurrencyManager::new_for_test(start_ts),
                        extra_op: Default::default(),
                        statistics: &mut statistics,
                        async_apply_prewrite: false,
                        raw_ext: None,
                        txn_status_cache: Arc::new(TxnStatusCache::new_for_test()),
                    },
                )
                .unwrap();
            assert!(statistics.lock.total_op_count() > 0);
            assert!(statistics.write.total_op_count() > 0);
            let writes = result.to_be_write.modifies;
            let bytes: usize = writes.iter().map(|m| m.size()).sum();
            assert!(bytes <= MAX_TXN_WRITE_SIZE, "batch was {bytes} bytes");
            write(&engine, &ctx, writes);
            let ProcessResult::NextCommand {
                cmd: Command::FlashbackToVersionReadPhase(next),
            } = result.pr
            else {
                panic!("expected another read phase");
            };
            let FlashbackToVersionState::RollbackLock { next_lock_key, .. } = next.state else {
                panic!("expected rollback scan");
            };
            batches += 1;
            if next_lock_key == end {
                must_unlocked(&mut engine, &raw);
                break;
            }
            assert_eq!(next_lock_key, key);
            let left = must_load_shared_lock(&mut engine, &raw).len();
            assert!(left > 0 && left < remaining);
            remaining = left;
        }
        assert!(batches > 1);
        for ts in 1..=count {
            must_get_rollback_protected(&mut engine, &raw, ts, true);
        }
    }
}
