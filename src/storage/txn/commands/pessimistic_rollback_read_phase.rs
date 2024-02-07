// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use txn_types::{Key, TimeStamp};

use crate::storage::{
    mvcc::{metrics::ScanLockReadTimeSource::pessimistic_rollback, MvccReader},
    txn,
    txn::{
        commands::{Command, CommandExt, PessimisticRollback, ReadCommand, TypedCommand},
        sched_pool::tls_collect_keyread_histogram_vec,
        ProcessResult, Result, StorageResult, RESOLVE_LOCK_BATCH_SIZE,
    },
    ScanMode, Snapshot, Statistics,
};
command! {
    PessimisticRollbackReadPhase:
        cmd_ty => Vec<StorageResult<()>>,
        display => { "kv::pessimistic_rollback_read_phase", (), }
        content => {
            start_ts: TimeStamp,
            for_update_ts: TimeStamp,
            scan_key: Option<Key>,
        }
        in_heap => {
            scan_key,
        }
}

impl CommandExt for PessimisticRollbackReadPhase {
    ctx!();
    tag!(pessimistic_rollback_read_phase);
    request_type!(KvPessimisticRollback);
    property!(readonly);

    fn write_bytes(&self) -> usize {
        0
    }

    gen_lock!(empty);
}

impl<S: Snapshot> ReadCommand<S> for PessimisticRollbackReadPhase {
    fn process_read(self, snapshot: S, statistics: &mut Statistics) -> Result<ProcessResult> {
        let tag = self.tag();
        let mut reader = MvccReader::new_with_ctx(snapshot, Some(ScanMode::Forward), &self.ctx);
        let res = reader
            .scan_locks(
                self.scan_key.as_ref(),
                None,
                |_, lock| {
                    lock.get_start_ts() == self.start_ts
                        && lock.is_pessimistic_lock()
                        && lock.get_for_update_ts() <= self.for_update_ts
                },
                RESOLVE_LOCK_BATCH_SIZE,
                pessimistic_rollback,
            )
            .map_err(txn::Error::from);
        statistics.add(&reader.statistics);
        let (locks, has_remain) = res?;
        tls_collect_keyread_histogram_vec(tag.get_str(), locks.len() as f64);

        if locks.is_empty() {
            Ok(ProcessResult::MultiRes { results: vec![] })
        } else {
            let next_scan_key = if has_remain {
                // There might be more locks.
                locks.last().map(|(k, _lock)| k.clone())
            } else {
                // All locks are scanned
                None
            };
            let next_cmd = PessimisticRollback {
                ctx: self.ctx.clone(),
                deadline: self.deadline,
                keys: locks.into_iter().map(|(key, _)| key).collect(),
                start_ts: self.start_ts,
                for_update_ts: self.for_update_ts,
                scan_key: next_scan_key,
            };
            Ok(ProcessResult::NextCommand {
                cmd: Command::PessimisticRollback(next_cmd),
            })
        }
    }
}
