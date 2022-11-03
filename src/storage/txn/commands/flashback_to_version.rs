// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use txn_types::{Key, TimeStamp};

use crate::storage::{
    kv::WriteData,
    lock_manager::LockManager,
    mvcc::{MvccTxn, SnapshotReader},
    txn::{
        actions::flashback_to_version::{flashback_to_version_lock, flashback_to_version_write},
        commands::{
            Command, CommandExt, FlashbackToVersionReadPhase, FlashbackToVersionState,
            ReaderWithStats, ReleasedLocks, ResponsePolicy, TypedCommand, WriteCommand,
            WriteContext, WriteResult,
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
            start_key: Option<Key>,
            end_key: Option<Key>,
            state: Option<FlashbackToVersionState>,
        }
}

impl CommandExt for FlashbackToVersion {
    ctx!();
    tag!(flashback_to_version);
    request_type!(KvFlashbackToVersion);

    fn gen_lock(&self) -> latch::Lock {
        match self.state.as_ref().unwrap() {
            FlashbackToVersionState::ScanLock { key_locks, .. } => {
                latch::Lock::new(key_locks.iter().map(|(key, _)| key))
            }
            FlashbackToVersionState::ScanWrite { key_old_writes, .. } => {
                latch::Lock::new(key_old_writes.iter().map(|(key, _)| key))
            }
        }
    }

    fn write_bytes(&self) -> usize {
        match self.state.as_ref().unwrap() {
            FlashbackToVersionState::ScanLock { key_locks, .. } => key_locks
                .iter()
                .map(|(key, _)| key.as_encoded().len())
                .sum(),
            FlashbackToVersionState::ScanWrite { key_old_writes, .. } => key_old_writes
                .iter()
                .map(|(key, _)| key.as_encoded().len())
                .sum(),
        }
    }
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for FlashbackToVersion {
    fn process_write(mut self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        let mut reader = ReaderWithStats::new(
            SnapshotReader::new_with_ctx(self.version, snapshot, &self.ctx),
            context.statistics,
        );
        let mut txn = MvccTxn::new(TimeStamp::zero(), context.concurrency_manager);
        // The state must be `ScanLock` or `ScanWrite` here.
        match self.state.as_mut().unwrap() {
            FlashbackToVersionState::ScanLock {
                next_lock_key,
                key_locks,
            } => flashback_to_version_lock(&mut txn, &mut reader, next_lock_key, key_locks)?,
            FlashbackToVersionState::ScanWrite {
                next_write_key,
                key_old_writes,
            } => flashback_to_version_write(
                &mut txn,
                &mut reader,
                next_write_key,
                key_old_writes,
                self.start_ts,
                self.commit_ts,
            )?,
        }
        let rows = txn.modifies.len();
        let mut write_data = WriteData::from_modifies(txn.into_modifies());
        write_data.extra.for_flashback = true;
        Ok(WriteResult {
            ctx: self.ctx.clone(),
            to_be_write: write_data,
            rows,
            pr: (move || {
                fail_point!("flashback_failed_after_first_batch", |_| {
                    ProcessResult::Res
                });
                let next_cmd = FlashbackToVersionReadPhase {
                    ctx: self.ctx,
                    deadline: self.deadline,
                    start_ts: self.start_ts,
                    commit_ts: self.commit_ts,
                    version: self.version,
                    start_key: self.start_key,
                    end_key: self.end_key,
                    state: self.state,
                };
                ProcessResult::NextCommand {
                    cmd: Command::FlashbackToVersionReadPhase(next_cmd),
                }
            })(),
            lock_info: None,
            released_locks: ReleasedLocks::new(),
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
        })
    }
}
