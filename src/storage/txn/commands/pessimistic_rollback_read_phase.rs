// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use txn_types::{Key, TimeStamp};

use crate::storage::{
    ScanMode, Snapshot, Statistics,
    mvcc::{MvccReader, metrics::ScanLockReadTimeSource::pessimistic_rollback},
    txn,
    txn::{
        ProcessResult, RESOLVE_LOCK_BATCH_SIZE, Result, StorageResult,
        commands::{Command, CommandExt, PessimisticRollback, ReadCommand, TypedCommand},
        sched_pool::tls_collect_keyread_histogram_vec,
    },
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

#[cfg(test)]
pub mod tests {
    use kvproto::kvrpcpb::Context;
    use tikv_util::deadline::Deadline;

    use super::*;
    use crate::storage::{
        TestEngineBuilder,
        kv::Engine,
        mvcc::tests::must_load_shared_lock,
        txn::{scheduler::DEFAULT_EXECUTION_DURATION_LIMIT, tests::*},
    };

    fn run_read_phase<E: Engine>(
        engine: &mut E,
        start_ts: impl Into<TimeStamp>,
        for_update_ts: impl Into<TimeStamp>,
        scan_key: Option<Key>,
    ) -> ProcessResult {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut statistics = Statistics::default();
        let cmd = PessimisticRollbackReadPhase {
            ctx: Context::default(),
            deadline: Deadline::from_now(DEFAULT_EXECUTION_DURATION_LIMIT),
            start_ts: start_ts.into(),
            for_update_ts: for_update_ts.into(),
            scan_key,
        };

        cmd.process_read(snapshot, &mut statistics).unwrap()
    }

    #[test]
    fn test_read_shared_pessimistic_lock() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let key = b"read-shared-pessimistic";
        let pk1 = b"pk1";
        let pk2 = b"pk2";

        must_acquire_shared_pessimistic_lock(&mut engine, key, pk1, 10, 30, 3000);
        must_acquire_shared_pessimistic_lock(&mut engine, key, pk2, 20, 20, 3000);
        must_shared_prewrite_lock(&mut engine, key, pk2, 20, 30);

        // Verify the shared lock contains the pessimistic entry we are going to roll
        // back.
        let mut shared_lock = must_load_shared_lock(&mut engine, key);
        let shared_entry = shared_lock.get_lock(&10.into()).unwrap().unwrap();
        assert!(shared_entry.is_pessimistic_lock());

        // read pessimistic lock inside shared lock.
        let pr = run_read_phase(&mut engine, 10, 30, None);
        match pr {
            ProcessResult::NextCommand {
                cmd: Command::PessimisticRollback(next),
            } => {
                assert_eq!(next.keys, &[Key::from_raw(key)]);
                assert_eq!(next.scan_key, None);
                assert_eq!(next.start_ts, 10.into());
                assert_eq!(next.for_update_ts, 30.into());
            }
            _ => panic!("expected pessimistic rollback command"),
        }

        // read pessimistic lock with smaller for_update_ts, should not find any lock.
        let pr = run_read_phase(&mut engine, 10, 20, None);
        matches!(pr, ProcessResult::MultiRes { .. });

        // no matching start_ts
        let pr = run_read_phase(&mut engine, 30, 40, None);
        matches!(pr, ProcessResult::MultiRes { .. });

        // should bypass prewrite lock
        let pr = run_read_phase(&mut engine, 20, 40, None);
        matches!(pr, ProcessResult::MultiRes { .. });
    }
}
