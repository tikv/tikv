// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use collections::HashMap;
use tikv_util::Either;
use txn_types::{Key, Lock, TimeStamp};

use crate::storage::{
    ScanMode, Snapshot, Statistics,
    mvcc::MvccReader,
    txn::{
        ProcessResult, RESOLVE_LOCK_BATCH_SIZE, Result,
        commands::{Command, CommandExt, ReadCommand, ResolveLock, TypedCommand},
        sched_pool::tls_collect_keyread_histogram_vec,
    },
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
            |_, lock| txn_status.keys().any(|ts| lock.contains_start_ts(ts)),
            RESOLVE_LOCK_BATCH_SIZE,
        );
        statistics.add(&reader.statistics);
        let (kv_pairs, has_remain) = result?;
        let kv_pairs: Vec<(Key, Lock)> = kv_pairs
            .into_iter()
            .map(|(key, lock)| {
                let lock = match lock {
                    Either::Left(lock) => lock,
                    Either::Right(_shared_locks) => unimplemented!(
                        "SharedLocks returned from scan_locks_from_storage is not supported here"
                    ),
                };
                (key, lock)
            })
            .collect();
        tls_collect_keyread_histogram_vec(tag.get_str(), kv_pairs.len() as f64);

        let flatten_pairs = if kv_pairs.iter().any(|(_, lock)| lock.is_shared()) {
            let mut flatten_pairs = Vec::with_capacity(kv_pairs.len());
            for (key, mut lock) in kv_pairs {
                if lock.is_shared() {
                    for sub_lock in lock.flatten_shared_locks().unwrap() {
                        flatten_pairs.push((key.clone(), sub_lock));
                    }
                } else {
                    flatten_pairs.push((key, lock));
                }
            }
            flatten_pairs
        } else {
            kv_pairs
        };


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
    use kvproto::kvrpcpb::Context;

    use super::*;
    use crate::storage::{kv::Engine, txn::tests::*, TestEngineBuilder};

    fn run_resolve_lock_read_phase<E: Engine>(
        engine: &mut E,
        txn_status: HashMap<TimeStamp, TimeStamp>,
    ) -> ProcessResult {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut statistics = Statistics::default();
        ResolveLockReadPhase::new(txn_status, None, Context::default())
            .cmd
            .process_read(snapshot, &mut statistics)
            .unwrap()
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
        let pr = run_resolve_lock_read_phase(&mut engine, txn_status);
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
                        assert!(!lock.is_shared());
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
}
