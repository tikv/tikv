// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use collections::HashMap;
use tikv_util::Either;
use txn_types::{Key, TimeStamp};

use crate::storage::{
    mvcc::MvccReader,
    txn::{
        commands::{Command, CommandExt, ReadCommand, ResolveLock, TypedCommand},
        sched_pool::tls_collect_keyread_histogram_vec,
        ProcessResult, Result, RESOLVE_LOCK_BATCH_SIZE,
    },
    ScanMode, Snapshot, Statistics,
};

command! {
    /// Scan locks for resolving according to `txn_status`.
    ///
    /// During the GC operation, this should be called to find out stale locks whose timestamp is
    /// before safe point.
    /// This should followed by a `ResolveLock`.
    ResolveLockReadPhase:
        cmd_ty => (),
        display => { "kv::resolve_lock_readphase", (), }
        content => {
            /// Maps lock_ts to commit_ts. See ./resolve_lock.rs for details.
            txn_status: HashMap<TimeStamp, TimeStamp>,
            scan_key: Option<Key>,
        }
        in_heap => {
            txn_status,
            scan_key,
        }
}

impl CommandExt for ResolveLockReadPhase {
    ctx!();
    tag!(resolve_lock);
    request_type!(KvResolveLock);
    property!(readonly);

    fn write_bytes(&self) -> usize {
        0
    }

    gen_lock!(empty);
}

impl<S: Snapshot> ReadCommand<S> for ResolveLockReadPhase {
    fn process_read(self, snapshot: S, statistics: &mut Statistics) -> Result<ProcessResult> {
        let tag = self.tag();
        let (ctx, txn_status) = (self.ctx, self.txn_status);
        let mut reader = MvccReader::new_with_ctx(snapshot, Some(ScanMode::Forward), &ctx);
        let result = reader.scan_locks_from_storage(
            self.scan_key.as_ref(),
            None,
            // Filter function: for Lock, check if ts is in txn_status;
            // for SharedLocks, the filter is applied during scan and returns only matching locks
            |_, lock| txn_status.contains_key(&lock.ts),
            RESOLVE_LOCK_BATCH_SIZE,
        );
        statistics.add(&reader.statistics);
        let (kv_pairs, has_remain) = result?;

        // Flatten the result: convert LockOrSharedLocks to Vec<(Key, Lock)>
        let mut flatten_pairs = Vec::with_capacity(kv_pairs.len());
        for (key, lock_or_shared) in kv_pairs {
            match lock_or_shared {
                Either::Left(lock) => {
                    flatten_pairs.push((key, lock));
                }
                Either::Right(mut shared_locks) => {
                    // For SharedLocks, iterate through all locks that match txn_status
                    let ts_to_process: Vec<_> = shared_locks
                        .iter_ts()
                        .filter(|ts| txn_status.contains_key(ts))
                        .cloned()
                        .collect();
                    for ts in ts_to_process {
                        if let Some(lock) = shared_locks.get_lock(&ts).unwrap() {
                            flatten_pairs.push((key.clone(), lock.clone()));
                        }
                    }
                }
            }
        }
        tls_collect_keyread_histogram_vec(tag.get_str(), flatten_pairs.len() as f64);

        if flatten_pairs.is_empty() {
            Ok(ProcessResult::Res)
        } else {
            let next_scan_key = if has_remain {
                // There might be more locks.
                flatten_pairs.last().map(|(k, _lock)| k.clone())
            } else {
                // All locks are scanned
                None
            };
            let next_cmd = ResolveLock {
                ctx,
                deadline: self.deadline,
                txn_status,
                scan_key: next_scan_key,
                key_locks: flatten_pairs,
            };
            Ok(ProcessResult::NextCommand {
                cmd: Command::ResolveLock(next_cmd),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use concurrency_manager::ConcurrencyManager;
    use kvproto::kvrpcpb::Context;

    use super::*;
    use crate::storage::{
        kv::Engine,
        lock_manager::MockLockManager,
        mvcc::tests::{must_load_shared_lock, must_unlocked, write},
        txn::{
            commands::{WriteCommand, WriteContext},
            tests::*,
            txn_status_cache::TxnStatusCache,
        },
        TestEngineBuilder,
    };

    fn run_resolve_lock_read_phase<E: Engine>(
        engine: &mut E,
        txn_status: HashMap<TimeStamp, TimeStamp>,
        scan_key: Option<Key>,
    ) -> ProcessResult {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut statistics = Statistics::default();
        ResolveLockReadPhase::new(txn_status, scan_key, Context::default())
            .cmd
            .process_read(snapshot, &mut statistics)
            .unwrap()
    }

    fn run_resolve_lock_write_phase<E: Engine>(
        engine: &mut E,
        command: ResolveLock,
    ) -> ProcessResult {
        let ctx = Context::default();
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let cm = ConcurrencyManager::new(TimeStamp::new(100));
        let lock_mgr = MockLockManager::new();
        let write_context = WriteContext {
            lock_mgr: &lock_mgr,
            concurrency_manager: cm,
            extra_op: Default::default(),
            statistics: &mut Default::default(),
            async_apply_prewrite: false,
            raw_ext: None,
            txn_status_cache: Arc::new(TxnStatusCache::new_for_test()),
        };
        let result = command.process_write(snapshot, write_context).unwrap();
        write(engine, &ctx, result.to_be_write.modifies);
        result.pr
    }

    #[test]
    fn test_resolve_lock_read_shared_pessimistic() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let key = b"read-shared-pessimistic";
        let pk1 = b"pk1";
        let pk2 = b"pk2";

        must_acquire_shared_pessimistic_lock(&mut engine, key, pk1, 10, 30, 3000);
        must_shared_prewrite_lock(&mut engine, key, pk1, 10, 15);
        must_acquire_shared_pessimistic_lock(&mut engine, key, pk2, 20, 20, 3000);
        must_shared_prewrite_lock(&mut engine, key, pk2, 20, 25);
        must_acquire_shared_pessimistic_lock(&mut engine, key, pk2, 40, 40, 3000);
        must_shared_prewrite_lock(&mut engine, key, pk2, 40, 45);

        let mut txn_status = HashMap::default();
        txn_status.insert(20.into(), 30.into());
        txn_status.insert(40.into(), 0.into());
        let pr = run_resolve_lock_read_phase(&mut engine, txn_status, None);
        match pr {
            ProcessResult::NextCommand {
                cmd: Command::ResolveLock(next),
            } => {
                assert_eq!(next.key_locks.len(), 2);
                let expected_key = Key::from_raw(key);

                let locks_by_start_ts: HashMap<_, _> = next
                    .key_locks
                    .iter()
                    .map(|(k, lock)| {
                        assert_eq!(k, &expected_key);
                        // In current branch, Lock type is never shared - shared locks use
                        // SharedLocks type
                        (lock.ts, lock)
                    })
                    .collect();

                assert_eq!(locks_by_start_ts.len(), 2);
                assert!(!locks_by_start_ts.contains_key(&10.into()));
                assert!(locks_by_start_ts.contains_key(&20.into()));
                assert!(locks_by_start_ts.contains_key(&40.into()));
            }
            _ => panic!("expected resolve lock command"),
        }
    }

    #[test]
    fn test_resolve_lock_readphase_large_single_shared_key_progress() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let key = b"resolve-lock-large-shared";
        let sub_lock_count = RESOLVE_LOCK_BATCH_SIZE + 64;

        let mut txn_status = HashMap::default();
        for i in 0..sub_lock_count {
            let start_ts = TimeStamp::new(100 + i as u64);
            let primary = format!("pk-{i}");
            must_acquire_shared_pessimistic_lock(
                &mut engine,
                key,
                primary.as_bytes(),
                start_ts,
                start_ts,
                3000,
            );
            must_shared_prewrite_lock(&mut engine, key, primary.as_bytes(), start_ts, start_ts);
            txn_status.insert(start_ts, TimeStamp::zero());
        }

        assert_eq!(
            must_load_shared_lock(&mut engine, key).len(),
            sub_lock_count
        );

        let expected_key = Key::from_raw(key);
        let mut seen_read_batches: HashSet<Vec<u64>> = HashSet::default();
        let mut scan_key = None;
        let mut read_rounds = 0;
        loop {
            read_rounds += 1;
            assert!(
                read_rounds <= sub_lock_count + 1,
                "resolve-lock read/write loop did not terminate"
            );

            let read_pr =
                run_resolve_lock_read_phase(&mut engine, txn_status.clone(), scan_key.clone());
            let resolve_cmd = match read_pr {
                ProcessResult::Res => break,
                ProcessResult::NextCommand {
                    cmd: Command::ResolveLock(next),
                } => next,
                _ => panic!("unexpected process result from resolve-lock read phase"),
            };

            let mut batch = Vec::with_capacity(resolve_cmd.key_locks.len());
            for (k, lock) in &resolve_cmd.key_locks {
                assert_eq!(k, &expected_key);
                batch.push(lock.ts.into_inner());
            }
            batch.sort_unstable();
            assert!(
                seen_read_batches.insert(batch.clone()),
                "duplicate unresolved subset observed between iterations: {:?}",
                batch
            );

            let write_pr = run_resolve_lock_write_phase(&mut engine, resolve_cmd);
            match write_pr {
                ProcessResult::Res => break,
                ProcessResult::NextCommand {
                    cmd: Command::ResolveLockReadPhase(next),
                } => {
                    scan_key = next.scan_key;
                }
                _ => panic!("unexpected process result from resolve-lock write phase"),
            }
        }

        assert!(
            seen_read_batches.len() > 1,
            "test did not exercise multi-batch resolve-lock read phase"
        );
        assert!(matches!(
            run_resolve_lock_read_phase(&mut engine, txn_status, None),
            ProcessResult::Res
        ));
        must_unlocked(&mut engine, key);
    }
}
