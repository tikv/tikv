// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use collections::HashMap;
use txn_types::{Key, Lock, TimeStamp};

use crate::storage::{
    ProcessResult, Snapshot,
    kv::WriteData,
    lock_manager::LockManager,
    mvcc::{
        Error as MvccError, ErrorInner as MvccErrorInner, MAX_TXN_WRITE_SIZE, MvccTxn,
        SnapshotReader,
    },
    txn::{
        Error, ErrorInner, Result, cleanup,
        commands::{
            Command, CommandExt, ReaderWithStats, ReleasedLocks, ResolveLockReadPhase,
            ResponsePolicy, TypedCommand, WriteCommand, WriteContext, WriteResult,
        },
        commit,
    },
};

command! {
    /// Resolve locks according to `txn_status`.
    ///
    /// During the GC operation, this should be called to clean up stale locks whose timestamp is
    /// before safe point.
    /// This should follow after a `ResolveLockReadPhase`.
    ResolveLock:
        cmd_ty => (),
        display => {
            "kv::resolve_lock {:?} scan_key({:?}) key_locks({:?})",
            (txn_status, scan_key, key_locks),
        }
        content => {
            /// Maps lock_ts to commit_ts. If a transaction was rolled back, it is mapped to 0.
            ///
            /// For example, let `txn_status` be `{ 100: 101, 102: 0 }`, then it means that the transaction
            /// whose start_ts is 100 was committed with commit_ts `101`, and the transaction whose
            /// start_ts is 102 was rolled back. If there are these keys in the db:
            ///
            /// * "k1", lock_ts = 100
            /// * "k2", lock_ts = 102
            /// * "k3", lock_ts = 104
            /// * "k4", no lock
            ///
            /// Here `"k1"`, `"k2"` and `"k3"` each has a not-yet-committed version, because they have
            /// locks. After calling resolve_lock, `"k1"` will be committed with commit_ts = 101 and `"k2"`
            /// will be rolled back.  `"k3"` will not be affected, because its lock_ts is not contained in
            /// `txn_status`. `"k4"` will not be affected either, because it doesn't have a non-committed
            /// version.
            txn_status: HashMap<TimeStamp, TimeStamp>,
            scan_key: Option<Key>,
            key_locks: Vec<(Key, Lock)>,
        }
        in_heap => {
            txn_status,
            scan_key,
            key_locks,
        }
}

impl CommandExt for ResolveLock {
    ctx!();
    tag!(resolve_lock);
    request_type!(KvResolveLock);
    property!(is_sys_cmd);

    fn write_bytes(&self) -> usize {
        self.key_locks
            .iter()
            .map(|(key, _)| key.as_encoded().len())
            .sum()
    }

    gen_lock!(key_locks: multiple(|(key, _)| key));
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for ResolveLock {
    fn process_write(mut self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        let (ctx, txn_status, key_locks) = (self.ctx, self.txn_status, self.key_locks);

        let mut txn = MvccTxn::new(TimeStamp::zero(), context.concurrency_manager);
        let mut reader = ReaderWithStats::new(
            SnapshotReader::new_with_ctx(TimeStamp::zero(), snapshot, &ctx),
            context.statistics,
        );

        let mut scan_key = self.scan_key.take();
        let rows = key_locks.len();
        let mut released_locks = ReleasedLocks::new();
        let mut known_txn_status = vec![];
        for (current_key, current_lock) in key_locks {
            // No special casing for shared locks here, `cleanup` and `commit` will handle
            // them.
            txn.start_ts = current_lock.ts;
            reader.start_ts = current_lock.ts;
            let commit_ts = *txn_status
                .get(&current_lock.ts)
                .expect("txn status not found");

            let released = if commit_ts.is_zero() {
                cleanup(
                    &mut txn,
                    &mut reader,
                    current_key.clone(),
                    TimeStamp::zero(),
                    false,
                )?
            } else if commit_ts > current_lock.ts {
                // Continue to resolve locks if the not found committed locks are pessimistic
                // type. They could be left if the transaction is finally committed and
                // pessimistic conflict retry happens during execution.
                match commit(&mut txn, &mut reader, current_key.clone(), commit_ts, None) {
                    Ok(res) => {
                        known_txn_status.push((current_lock.ts, commit_ts));
                        res
                    }
                    Err(MvccError(box MvccErrorInner::TxnLockNotFound { .. }))
                        if current_lock.is_pessimistic_lock() =>
                    {
                        None
                    }
                    Err(err) => return Err(err.into()),
                }
            } else {
                return Err(Error::from(ErrorInner::InvalidTxnTso {
                    start_ts: current_lock.ts,
                    commit_ts,
                }));
            };
            released_locks.push(released);

            if txn.write_size() >= MAX_TXN_WRITE_SIZE {
                scan_key = Some(current_key);
                break;
            }
        }

        known_txn_status.sort();
        known_txn_status.dedup();

        let pr = if scan_key.is_none() {
            ProcessResult::Res
        } else {
            let next_cmd = ResolveLockReadPhase {
                ctx: ctx.clone(),
                deadline: self.deadline,
                txn_status,
                scan_key,
            };
            ProcessResult::NextCommand {
                cmd: Command::ResolveLockReadPhase(next_cmd),
            }
        };
        let new_acquired_locks = txn.take_new_locks();
        let mut write_data = WriteData::from_modifies(txn.into_modifies());
        write_data.set_allowed_on_disk_almost_full();
        Ok(WriteResult {
            ctx,
            to_be_write: write_data,
            rows,
            pr,
            lock_info: vec![],
            released_locks,
            new_acquired_locks,
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
            known_txn_status,
        })
    }
}

// To resolve a key, the write size is about 100~150 bytes, depending on key and
// value length. The write batch will be around 32KB if we scan 256 keys each
// time.
pub const RESOLVE_LOCK_BATCH_SIZE: usize = 256;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use concurrency_manager::ConcurrencyManager;
    use kvproto::kvrpcpb::Context;
    use tikv_util::deadline::Deadline;

    use super::*;
    use crate::storage::{
        TestEngineBuilder,
        kv::Engine,
        lock_manager::MockLockManager,
        mvcc::tests::{must_get_rollback_ts, must_unlocked, write},
        txn::{
            commands::{WriteCommand, WriteContext},
            scheduler::DEFAULT_EXECUTION_DURATION_LIMIT,
            tests::{must_acquire_shared_pessimistic_lock, must_shared_prewrite_lock},
            txn_status_cache::TxnStatusCache,
        },
    };

    fn run_resolve_lock<E: Engine>(
        engine: &mut E,
        txn_status: HashMap<TimeStamp, TimeStamp>,
        key_locks: Vec<(Key, Lock)>,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let cm = ConcurrencyManager::new_for_test(TimeStamp::new(100));
        let command = ResolveLock {
            ctx: ctx.clone(),
            txn_status,
            scan_key: None,
            key_locks,
            deadline: Deadline::from_now(DEFAULT_EXECUTION_DURATION_LIMIT),
        };
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
    }

    /// This test demonstrates a bug where resolving multiple sub-locks from the
    /// same SharedLocks in a single batch results in incorrect behavior.
    ///
    /// The bug occurs because:
    /// 1. ResolveLockReadPhase flattens SharedLocks into individual (Key, Lock)
    ///    pairs
    /// 2. ResolveLock processes each pair in a loop using the SAME snapshot
    /// 3. Each iteration reads the ORIGINAL SharedLocks (all sub-locks present)
    /// 4. Each iteration removes ONE sub-lock and writes the remaining
    /// 5. Since the snapshot doesn't see pending writes, each iteration
    ///    overwrites the previous one
    /// 6. The LAST write wins, leaving incorrect locks
    ///
    /// Example:
    /// - SharedLocks on key: [ts=10, ts=20], both need rollback
    /// - Iteration 1: reads [10,20], removes 10, writes [20]
    /// - Iteration 2: reads [10,20] (same snapshot!), removes 20, writes [10]
    /// - Final result: [10] (should be empty!)
    #[test]
    fn test_resolve_multiple_shared_locks_same_key() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let key = b"shared-resolve-key";
        let pk1 = b"pk1";
        let pk2 = b"pk2";

        // Create SharedLocks with two sub-locks: ts=10 and ts=20
        must_acquire_shared_pessimistic_lock(&mut engine, key, pk1, 10, 15, 3000);
        must_shared_prewrite_lock(&mut engine, key, pk1, 10, 15);
        must_acquire_shared_pessimistic_lock(&mut engine, key, pk2, 20, 25, 3000);
        must_shared_prewrite_lock(&mut engine, key, pk2, 20, 25);

        // Load the SharedLocks to get the Lock structures
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = crate::storage::mvcc::MvccReader::new(snapshot, None, true);
        let lock_or_shared = reader.load_lock(&Key::from_raw(key)).unwrap().unwrap();
        let mut shared_locks = match lock_or_shared {
            tikv_util::Either::Right(sl) => sl,
            _ => panic!("expected SharedLocks"),
        };
        assert_eq!(shared_locks.len(), 2);

        // Extract individual locks (simulating what ResolveLockReadPhase does)
        let lock1 = shared_locks.get_lock(&10.into()).unwrap().unwrap().clone();
        let lock2 = shared_locks.get_lock(&20.into()).unwrap().unwrap().clone();

        // Build txn_status: rollback both transactions
        let mut txn_status = HashMap::default();
        txn_status.insert(10.into(), 0.into()); // rollback ts=10
        txn_status.insert(20.into(), 0.into()); // rollback ts=20

        // Simulate what happens when ResolveLock processes both locks from the
        // same SharedLocks in a single batch. The key_locks contains the same
        // key twice with different locks.
        let key_locks = vec![(Key::from_raw(key), lock1), (Key::from_raw(key), lock2)];

        run_resolve_lock(&mut engine, txn_status, key_locks);

        // Both transactions should have rollback records
        must_get_rollback_ts(&mut engine, key, 10);
        must_get_rollback_ts(&mut engine, key, 20);

        // After resolving both locks, the key should be unlocked (no locks
        // remaining).
        must_unlocked(&mut engine, key);
    }
}
