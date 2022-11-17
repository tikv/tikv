// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use txn_types::{Key, Lock, WriteType};

use crate::storage::{
    kv::WriteData,
    lock_manager::LockManager,
    mvcc::{LockType, MvccTxn, SnapshotReader, TimeStamp, TxnCommitRecord},
    txn::{
        actions::check_txn_status::{
            collapse_prev_rollback, make_rollback, update_last_change_for_rollback,
        },
        commands::{
            Command, CommandExt, ReaderWithStats, ReleasedLocks, ResponsePolicy, TypedCommand,
            WriteCommand, WriteContext, WriteResult,
        },
        Result,
    },
    types::SecondaryLocksStatus,
    ProcessResult, Snapshot,
};

command! {
    /// Check secondary locks of an async commit transaction.
    ///
    /// If all prewritten locks exist, the lock information is returned.
    /// Otherwise, it returns the commit timestamp of the transaction.
    ///
    /// If the lock does not exist or is a pessimistic lock, to prevent the
    /// status being changed, a rollback may be written.
    CheckSecondaryLocks:
        cmd_ty => SecondaryLocksStatus,
        display => "kv::command::CheckSecondaryLocks {:?} keys@{} | {:?}", (keys, start_ts, ctx),
        content => {
            /// The keys of secondary locks.
            keys: Vec<Key>,
            /// The start timestamp of the transaction.
            start_ts: txn_types::TimeStamp,
        }
}

impl CommandExt for CheckSecondaryLocks {
    ctx!();
    tag!(check_secondary_locks);
    request_type!(KvCheckSecondaryLocks);
    ts!(start_ts);
    write_bytes!(keys: multiple);
    gen_lock!(keys: multiple);
}

#[derive(Debug, PartialEq)]
enum SecondaryLockStatus {
    Locked(Lock),
    Committed(TimeStamp),
    RolledBack,
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for CheckSecondaryLocks {
    fn process_write(self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        // It is not allowed for commit to overwrite a protected rollback. So we update
        // max_ts to prevent this case from happening.
        context.concurrency_manager.update_max_ts(self.start_ts);

        let mut txn = MvccTxn::new(self.start_ts, context.concurrency_manager);
        let mut reader = ReaderWithStats::new(
            SnapshotReader::new_with_ctx(self.start_ts, snapshot, &self.ctx),
            context.statistics,
        );
        let mut released_locks = ReleasedLocks::new();
        let mut result = SecondaryLocksStatus::Locked(Vec::new());

        for key in self.keys {
            let mut released_lock = None;
            let mut mismatch_lock = None;
            // Checks whether the given secondary lock exists.
            let (status, need_rollback, rollback_overlapped_write) = match reader.load_lock(&key)? {
                // The lock exists, the lock information is returned.
                Some(lock) if lock.ts == self.start_ts => {
                    if lock.lock_type == LockType::Pessimistic {
                        released_lock = txn.unlock_key(key.clone(), true, TimeStamp::zero());
                        let overlapped_write = reader.get_txn_commit_record(&key)?.unwrap_none();
                        (SecondaryLockStatus::RolledBack, true, overlapped_write)
                    } else {
                        (SecondaryLockStatus::Locked(lock), false, None)
                    }
                }
                // Searches the write CF for the commit record of the lock and returns the commit
                // timestamp (0 if the lock is not committed).
                l => {
                    mismatch_lock = l;
                    match reader.get_txn_commit_record(&key)? {
                        TxnCommitRecord::SingleRecord { commit_ts, write } => {
                            let status = if write.write_type != WriteType::Rollback {
                                SecondaryLockStatus::Committed(commit_ts)
                            } else {
                                SecondaryLockStatus::RolledBack
                            };
                            // We needn't write a rollback once there is a write record for it:
                            // If it's a committed record, it cannot be changed.
                            // If it's a rollback record, it either comes from another
                            // check_secondary_lock (thus protected) or the client stops commit
                            // actively. So we don't need to make it protected again.
                            (status, false, None)
                        }
                        TxnCommitRecord::OverlappedRollback { .. } => {
                            (SecondaryLockStatus::RolledBack, false, None)
                        }
                        TxnCommitRecord::None { overlapped_write } => {
                            (SecondaryLockStatus::RolledBack, true, overlapped_write)
                        }
                    }
                }
            };
            // If the lock does not exist or is a pessimistic lock, to prevent the
            // status being changed, a rollback may be written and this rollback
            // needs to be protected.
            if need_rollback {
                if let Some(l) = mismatch_lock {
                    txn.mark_rollback_on_mismatching_lock(&key, l, true);
                }
                // We must protect this rollback in case this rollback is collapsed and a stale
                // acquire_pessimistic_lock and prewrite succeed again.
                if let Some(mut write) =
                    make_rollback(self.start_ts, true, rollback_overlapped_write)
                {
                    update_last_change_for_rollback(&mut reader, &mut write, &key, self.start_ts)?;
                    txn.put_write(key.clone(), self.start_ts, write.as_ref().to_bytes());
                    collapse_prev_rollback(&mut txn, &mut reader, &key)?;
                }
            }
            released_locks.push(released_lock);
            match status {
                SecondaryLockStatus::Locked(lock) => {
                    result.push(lock.into_lock_info(key.to_raw()?));
                }
                SecondaryLockStatus::Committed(commit_ts) => {
                    result = SecondaryLocksStatus::Committed(commit_ts);
                    break;
                }
                SecondaryLockStatus::RolledBack => {
                    result = SecondaryLocksStatus::RolledBack;
                    break;
                }
            }
        }

        let mut rows = 0;
        if let SecondaryLocksStatus::RolledBack = &result {
            // One row is mutated only when a secondary lock is rolled back.
            rows = 1;
        }
        let pr = ProcessResult::SecondaryLocksStatus { status: result };
        let mut write_data = WriteData::from_modifies(txn.into_modifies());
        write_data.set_allowed_on_disk_almost_full();
        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write: write_data,
            rows,
            pr,
            lock_info: vec![],
            released_locks,
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
        })
    }
}

#[cfg(test)]
pub mod tests {
    use concurrency_manager::ConcurrencyManager;
    use kvproto::kvrpcpb::Context;
    use tikv_kv::Statistics;
    use tikv_util::deadline::Deadline;
    use txn_types::Mutation;

    use super::*;
    use crate::storage::{
        kv::TestEngineBuilder,
        lock_manager::MockLockManager,
        mvcc::tests::*,
        txn::{
            commands::{test_util::prewrite_with_cm, WriteCommand},
            scheduler::DEFAULT_EXECUTION_DURATION_LIMIT,
            tests::*,
        },
        Engine,
    };

    pub fn must_success<E: Engine>(
        engine: &mut E,
        key: &[u8],
        lock_ts: impl Into<TimeStamp>,
        expect_status: SecondaryLocksStatus,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let lock_ts = lock_ts.into();
        let cm = ConcurrencyManager::new(lock_ts);
        let command = crate::storage::txn::commands::CheckSecondaryLocks {
            ctx: ctx.clone(),
            keys: vec![Key::from_raw(key)],
            start_ts: lock_ts,
            deadline: Deadline::from_now(DEFAULT_EXECUTION_DURATION_LIMIT),
        };
        let result = command
            .process_write(
                snapshot,
                WriteContext {
                    lock_mgr: &MockLockManager::new(),
                    concurrency_manager: cm,
                    extra_op: Default::default(),
                    statistics: &mut Default::default(),
                    async_apply_prewrite: false,
                    raw_ext: None,
                },
            )
            .unwrap();
        if let ProcessResult::SecondaryLocksStatus { status } = result.pr {
            assert_eq!(status, expect_status);
            write(engine, &ctx, result.to_be_write.modifies);
        } else {
            unreachable!();
        }
    }

    #[test]
    fn test_check_async_commit_secondary_locks() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let mut engine_clone = engine.clone();
        let ctx = Context::default();
        let cm = ConcurrencyManager::new(1.into());

        let mut check_secondary = |key, ts| {
            let snapshot = engine_clone.snapshot(Default::default()).unwrap();
            let key = Key::from_raw(key);
            let ts = TimeStamp::new(ts);
            let command = crate::storage::txn::commands::CheckSecondaryLocks {
                ctx: Default::default(),
                keys: vec![key],
                start_ts: ts,
                deadline: Deadline::from_now(DEFAULT_EXECUTION_DURATION_LIMIT),
            };
            let result = command
                .process_write(
                    snapshot,
                    WriteContext {
                        lock_mgr: &MockLockManager::new(),
                        concurrency_manager: cm.clone(),
                        extra_op: Default::default(),
                        statistics: &mut Default::default(),
                        async_apply_prewrite: false,
                        raw_ext: None,
                    },
                )
                .unwrap();
            if !result.to_be_write.modifies.is_empty() {
                engine_clone.write(&ctx, result.to_be_write).unwrap();
            }
            if let ProcessResult::SecondaryLocksStatus { status } = result.pr {
                status
            } else {
                unreachable!();
            }
        };

        must_prewrite_lock(&mut engine, b"k1", b"key", 1);
        must_commit(&mut engine, b"k1", 1, 3);
        must_rollback(&mut engine, b"k1", 5, false);
        must_prewrite_lock(&mut engine, b"k1", b"key", 7);
        must_commit(&mut engine, b"k1", 7, 9);

        // Lock CF has no lock
        //
        // LOCK CF       | WRITE CF
        // --------------+---------------------
        //               | 9: start_ts = 7
        //               | 5: rollback
        //               | 3: start_ts = 1

        assert_eq!(
            check_secondary(b"k1", 7),
            SecondaryLocksStatus::Committed(9.into())
        );
        must_get_commit_ts(&mut engine, b"k1", 7, 9);
        assert_eq!(check_secondary(b"k1", 5), SecondaryLocksStatus::RolledBack);
        must_get_rollback_ts(&mut engine, b"k1", 5);
        assert_eq!(
            check_secondary(b"k1", 1),
            SecondaryLocksStatus::Committed(3.into())
        );
        must_get_commit_ts(&mut engine, b"k1", 1, 3);
        assert_eq!(check_secondary(b"k1", 6), SecondaryLocksStatus::RolledBack);
        must_get_rollback_protected(&mut engine, b"k1", 6, true);

        // ----------------------------

        must_acquire_pessimistic_lock(&mut engine, b"k1", b"key", 11, 11);

        // Lock CF has a pessimistic lock
        //
        // LOCK CF       | WRITE CF
        // ------------------------------------
        // ts = 11 (pes) | 9: start_ts = 7
        //               | 5: rollback
        //               | 3: start_ts = 1

        let status = check_secondary(b"k1", 11);
        assert_eq!(status, SecondaryLocksStatus::RolledBack);
        must_get_rollback_protected(&mut engine, b"k1", 11, true);

        // ----------------------------

        must_prewrite_lock(&mut engine, b"k1", b"key", 13);

        // Lock CF has an optimistic lock
        //
        // LOCK CF       | WRITE CF
        // ------------------------------------
        // ts = 13 (opt) | 11: rollback
        //               |  9: start_ts = 7
        //               |  5: rollback
        //               |  3: start_ts = 1

        match check_secondary(b"k1", 13) {
            SecondaryLocksStatus::Locked(_) => {}
            res => panic!("unexpected lock status: {:?}", res),
        }
        must_locked(&mut engine, b"k1", 13);

        // ----------------------------

        must_commit(&mut engine, b"k1", 13, 15);

        // Lock CF has an optimistic lock
        //
        // LOCK CF       | WRITE CF
        // ------------------------------------
        //               | 15: start_ts = 13
        //               | 11: rollback
        //               |  9: start_ts = 7
        //               |  5: rollback
        //               |  3: start_ts = 1

        match check_secondary(b"k1", 14) {
            SecondaryLocksStatus::RolledBack => {}
            res => panic!("unexpected lock status: {:?}", res),
        }
        must_get_rollback_protected(&mut engine, b"k1", 14, true);

        match check_secondary(b"k1", 15) {
            SecondaryLocksStatus::RolledBack => {}
            res => panic!("unexpected lock status: {:?}", res),
        }
        must_get_overlapped_rollback(&mut engine, b"k1", 15, 13, WriteType::Lock, Some(0));
    }

    // The main logic is almost identical to
    // test_rollback_calculate_last_change_info of check_txn_status. But the small
    // differences about handling lock CF make it difficult to reuse code.
    #[test]
    fn test_rollback_calculate_last_change_info() {
        use pd_client::FeatureGate;

        use crate::storage::txn::sched_pool::set_tls_feature_gate;

        let mut engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new(1.into());
        let k = b"k";
        let mut statistics = Statistics::default();

        must_prewrite_put(&mut engine, k, b"v1", k, 5);
        must_commit(&mut engine, k, 5, 6);
        must_prewrite_put(&mut engine, k, b"v2", k, 7);
        must_commit(&mut engine, k, 7, 8);
        must_prewrite_put(&mut engine, k, b"v3", k, 30);
        must_commit(&mut engine, k, 30, 35);

        // TiKV 6.4 should not write last_change_ts.
        let feature_gate = FeatureGate::default();
        feature_gate.set_version("6.4.0").unwrap();
        set_tls_feature_gate(feature_gate);
        must_success(&mut engine, k, 40, SecondaryLocksStatus::RolledBack);
        let rollback = must_written(&mut engine, k, 40, 40, WriteType::Rollback);
        assert!(rollback.last_change_ts.is_zero());
        assert_eq!(rollback.versions_to_last_change, 0);

        let feature_gate = FeatureGate::default();
        feature_gate.set_version("6.5.0").unwrap();
        set_tls_feature_gate(feature_gate);

        must_prewrite_put(&mut engine, k, b"v4", k, 45);
        must_commit(&mut engine, k, 45, 50);

        // Rollback when there is no lock; prev writes:
        // - 50: PUT
        must_success(&mut engine, k, 55, SecondaryLocksStatus::RolledBack);
        let rollback = must_written(&mut engine, k, 55, 55, WriteType::Rollback);
        assert_eq!(rollback.last_change_ts, 50.into());
        assert_eq!(rollback.versions_to_last_change, 1);

        // Write a LOCK; prev writes:
        // - 55: ROLLBACK
        // - 50: PUT
        let res = prewrite_with_cm(
            &mut engine,
            cm,
            &mut statistics,
            vec![Mutation::make_lock(Key::from_raw(k))],
            k.to_vec(),
            60,
            Some(70),
        )
        .unwrap();
        assert!(!res.one_pc_commit_ts.is_zero());
        let lock_commit_ts = res.one_pc_commit_ts;
        let lock = must_written(&mut engine, k, 60, res.one_pc_commit_ts, WriteType::Lock);
        assert_eq!(lock.last_change_ts, 50.into());
        assert_eq!(lock.versions_to_last_change, 2);

        // Write another ROLLBACK by rolling back a pessimistic lock; prev writes:
        // - 61: LOCK
        // - 55: ROLLBACK
        // - 50: PUT
        must_acquire_pessimistic_lock(&mut engine, k, b"pk", 70, 75);
        must_success(&mut engine, k, 70, SecondaryLocksStatus::RolledBack);
        let rollback = must_written(&mut engine, k, 70, 70, WriteType::Rollback);
        assert_eq!(rollback.last_change_ts, 50.into());
        assert_eq!(rollback.versions_to_last_change, 3);

        // last_change_ts should point to the latest record before start_ts; prev
        // writes:
        // - 8: PUT
        must_acquire_pessimistic_lock(&mut engine, k, k, 10, 75);
        must_success(&mut engine, k, 10, SecondaryLocksStatus::RolledBack);
        must_unlocked(&mut engine, k);
        let rollback = must_written(&mut engine, k, 10, 10, WriteType::Rollback);
        assert_eq!(rollback.last_change_ts, 8.into());
        assert_eq!(rollback.versions_to_last_change, 1);

        // Overlapped rollback should not update the last_change_ts of PUT; prev writes:
        // - 8: PUT <- rollback overlaps
        // - 6: PUT
        must_success(&mut engine, k, 8, SecondaryLocksStatus::RolledBack);
        let put = must_written(&mut engine, k, 7, 8, WriteType::Put);
        assert!(put.last_change_ts.is_zero());
        assert_eq!(put.versions_to_last_change, 0);
        assert!(put.has_overlapped_rollback);

        // Overlapped rollback can update the last_change_ts of LOCK; writes:
        // - 61: PUT <- rollback overlaps
        // - 57: ROLLBACK (inserted later)
        // - 55: ROLLBACK
        // - 50: PUT
        must_rollback(&mut engine, k, 57, true);
        let rollback = must_written(&mut engine, k, 57, 57, WriteType::Rollback);
        assert_eq!(rollback.last_change_ts, 50.into());
        assert_eq!(rollback.versions_to_last_change, 2);
        must_success(
            &mut engine,
            k,
            lock_commit_ts,
            SecondaryLocksStatus::RolledBack,
        );
        let lock = must_written(&mut engine, k, 60, lock_commit_ts, WriteType::Lock);
        assert_eq!(lock.last_change_ts, 50.into());
        assert_eq!(lock.versions_to_last_change, 3);
    }
}
