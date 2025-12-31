// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use protobuf::Message;
use resource_metering::record_network_out_bytes;
use txn_types::{Key, Lock, WriteType};

use crate::storage::{
    ProcessResult, Snapshot,
    kv::WriteData,
    lock_manager::LockManager,
    mvcc::{MvccTxn, OverlappedWrite, ReleasedLock, SnapshotReader, TimeStamp, TxnCommitRecord},
    txn::{
        Result,
        actions::check_txn_status::{collapse_prev_rollback, make_rollback},
        commands::{
            Command, CommandExt, ReaderWithStats, ReleasedLocks, ResponsePolicy, TypedCommand,
            WriteCommand, WriteContext, WriteResult,
        },
    },
    types::SecondaryLocksStatus,
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
        display => { "kv::command::CheckSecondaryLocks {:?} keys@{} | {:?}", (keys, start_ts, ctx), }
        content => {
            /// The keys of secondary locks.
            keys: Vec<Key>,
            /// The start timestamp of the transaction.
            start_ts: txn_types::TimeStamp,
        }
        in_heap => {
            keys,
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

// The returned `bool` indicates whether the rollback record should be written,
// it should be true if and only if the txn commit record is not found, thus
// a rollback record would be written later.
fn check_determined_txn_status<S: Snapshot>(
    reader: &mut ReaderWithStats<'_, S>,
    key: &Key,
) -> Result<(SecondaryLockStatus, bool, Option<OverlappedWrite>)> {
    match reader.get_txn_commit_record(key)? {
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
            Ok((status, false, None))
        }
        TxnCommitRecord::OverlappedRollback { .. } => {
            Ok((SecondaryLockStatus::RolledBack, false, None))
        }
        TxnCommitRecord::None { overlapped_write } => {
            Ok((SecondaryLockStatus::RolledBack, true, overlapped_write))
        }
    }
}

fn check_status_from_lock<S: Snapshot>(
    txn: &mut MvccTxn,
    reader: &mut ReaderWithStats<'_, S>,
    lock: Lock,
    key: &Key,
    region_id: u64,
) -> Result<(
    SecondaryLockStatus,
    bool,
    Option<OverlappedWrite>,
    Option<ReleasedLock>,
)> {
    let mut overlapped_write = None;
    if lock.is_pessimistic_lock_with_conflict() {
        assert!(lock.is_pessimistic_lock());
        let (status, need_rollback, rollback_overlapped_write) =
            check_determined_txn_status(reader, key)?;
        // If there exists commit or rollback record, the pessimistic lock is stale, in
        // this case the returned need_rollback is false.
        if !need_rollback {
            let released_lock = txn.unlock_key(key.clone(), true, TimeStamp::zero());
            return Ok((
                status,
                need_rollback,
                rollback_overlapped_write,
                released_lock,
            ));
        }
        overlapped_write = rollback_overlapped_write;
    }

    if lock.is_pessimistic_lock() {
        let released_lock = txn.unlock_key(key.clone(), true, TimeStamp::zero());
        // If the `is_pessimistic_lock_with_conflict` is true, the `overlapped_write` is
        // already fetched in the above `check_determined_txn_status` call. So
        // we don't need to fetch it again and the `overlapped_write` could be
        // reused here.
        let overlapped_write_res = if lock.is_pessimistic_lock_with_conflict() {
            overlapped_write
        } else {
            reader.get_txn_commit_record(key)?.unwrap_none(region_id)
        };
        Ok((
            SecondaryLockStatus::RolledBack,
            true,
            overlapped_write_res,
            released_lock,
        ))
    } else {
        Ok((SecondaryLockStatus::Locked(lock), false, None, None))
    }
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for CheckSecondaryLocks {
    fn process_write(self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        // It is not allowed for commit to overwrite a protected rollback. So we update
        // max_ts to prevent this case from happening.
        let region_id = self.ctx.get_region_id();
        context
            .concurrency_manager
            .update_max_ts(self.start_ts, || {
                format!("check_secondary_locks-{}", self.start_ts)
            })?;

        let mut txn = MvccTxn::new(self.start_ts, context.concurrency_manager);
        let mut reader = ReaderWithStats::new(
            SnapshotReader::new_with_ctx(self.start_ts, snapshot, &self.ctx),
            context.statistics,
        );
        let mut released_locks = ReleasedLocks::new();
        let mut result = SecondaryLocksStatus::Locked(Vec::new());
        let mut result_size: u64 = 0;
        for key in self.keys {
            let mut released_lock = None;
            let mut mismatch_lock = None;
            // Checks whether the given secondary lock exists.
            let (status, need_rollback, rollback_overlapped_write) = match reader.load_lock(&key)? {
                // The lock exists, the lock information is returned.
                Some(lock) if lock.ts == self.start_ts => {
                    let (status, need_rollback, rollback_overlapped_write, lock_released) =
                        check_status_from_lock(&mut txn, &mut reader, lock, &key, region_id)?;
                    released_lock = lock_released;
                    (status, need_rollback, rollback_overlapped_write)
                }
                // Searches the write CF for the commit record of the lock and returns the commit
                // timestamp (0 if the lock is not committed).
                l => {
                    mismatch_lock = l;
                    check_determined_txn_status(&mut reader, &key)?
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
                if let Some(write) = make_rollback(self.start_ts, true, rollback_overlapped_write) {
                    txn.put_write(key.clone(), self.start_ts, write.as_ref().to_bytes());
                    collapse_prev_rollback(&mut txn, &mut reader, &key)?;
                }
            }
            released_locks.push(released_lock);
            match status {
                SecondaryLockStatus::Locked(lock) => {
                    let lock_info = lock.into_lock_info(key.to_raw()?);
                    result_size += lock_info.compute_size() as u64;
                    result.push(lock_info);
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

        record_network_out_bytes(result_size);
        let write_result_known_txn_status =
            if let SecondaryLocksStatus::Committed(commit_ts) = &result {
                vec![(self.start_ts, *commit_ts)]
            } else {
                vec![]
            };
        let mut rows = 0;
        if let SecondaryLocksStatus::RolledBack = &result {
            // One row is mutated only when a secondary lock is rolled back.
            rows = 1;
        }
        let pr = ProcessResult::SecondaryLocksStatus { status: result };
        let new_acquired_locks = txn.take_new_locks();
        let mut write_data = WriteData::from_modifies(txn.into_modifies());
        write_data.set_allowed_on_disk_almost_full();
        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write: write_data,
            rows,
            pr,
            lock_info: vec![],
            released_locks,
            new_acquired_locks,
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
            known_txn_status: write_result_known_txn_status,
        })
    }
}

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use concurrency_manager::ConcurrencyManager;
    use kvproto::kvrpcpb::Context;
    use tikv_util::deadline::Deadline;

    use super::*;
    use crate::storage::{
        Engine,
        kv::TestEngineBuilder,
        lock_manager::MockLockManager,
        mvcc::tests::*,
        txn::{
            commands::WriteCommand, scheduler::DEFAULT_EXECUTION_DURATION_LIMIT, tests::*,
            txn_status_cache::TxnStatusCache,
        },
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
        let cm = ConcurrencyManager::new_for_test(lock_ts);
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
                    txn_status_cache: Arc::new(TxnStatusCache::new_for_test()),
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
        let cm = ConcurrencyManager::new_for_test(1.into());

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
                        txn_status_cache: Arc::new(TxnStatusCache::new_for_test()),
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

        // Lock CF has an stale pessimistic lock, the transaction is already committed
        // or rolled back.
        //
        // LOCK CF       | WRITE CF
        // ------------------------------------
        //               | 15: start_ts = 13 with overlapped rollback
        //               | 14: rollback
        //               | 11: rollback
        //               |  9: start_ts = 7
        //               |  5: rollback
        //               |  3: start_ts = 1
        must_acquire_pessimistic_lock_allow_lock_with_conflict(
            &mut engine,
            b"k1",
            b"key",
            7,
            7,
            true,
            false,
            10,
        )
        .assert_locked_with_conflict(None, 15);
        match check_secondary(b"k1", 7) {
            SecondaryLocksStatus::Committed(ts) => {
                assert!(ts.eq(&9.into()));
            }
            res => panic!("unexpected lock status: {:?}", res),
        }
        must_unlocked(&mut engine, b"k1");

        // Lock CF has an pessimistic lock, the transaction status is not found
        // in storage.
        must_acquire_pessimistic_lock_allow_lock_with_conflict(
            &mut engine,
            b"k1",
            b"key",
            8,
            8,
            true,
            false,
            10,
        )
        .assert_locked_with_conflict(None, 15);
        match check_secondary(b"k1", 8) {
            SecondaryLocksStatus::RolledBack => {}
            res => panic!("unexpected lock status: {:?}", res),
        }
        must_unlocked(&mut engine, b"k1");
    }
}
