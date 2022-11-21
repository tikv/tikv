// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use txn_types::{Key, TimeStamp, Write, WriteType};

use crate::storage::{
    mvcc::{
        metrics::{MVCC_CONFLICT_COUNTER, MVCC_DUPLICATE_CMD_COUNTER_VEC},
        ErrorInner, LockType, MvccTxn, ReleasedLock, Result as MvccResult, SnapshotReader,
    },
    Snapshot,
};

pub fn commit<S: Snapshot>(
    txn: &mut MvccTxn,
    reader: &mut SnapshotReader<S>,
    key: Key,
    commit_ts: TimeStamp,
) -> MvccResult<Option<ReleasedLock>> {
    fail_point!("commit", |err| Err(
        crate::storage::mvcc::txn::make_txn_error(err, &key, reader.start_ts,).into()
    ));

    let mut lock = match reader.load_lock(&key)? {
        Some(mut lock) if lock.ts == reader.start_ts => {
            // A lock with larger min_commit_ts than current commit_ts can't be committed
            if commit_ts < lock.min_commit_ts {
                info!(
                    "trying to commit with smaller commit_ts than min_commit_ts";
                    "key" => %key,
                    "start_ts" => reader.start_ts,
                    "commit_ts" => commit_ts,
                    "min_commit_ts" => lock.min_commit_ts,
                );
                return Err(ErrorInner::CommitTsExpired {
                    start_ts: reader.start_ts,
                    commit_ts,
                    key: key.into_raw()?,
                    min_commit_ts: lock.min_commit_ts,
                }
                .into());
            }

            // It's an abnormal routine since pessimistic locks shouldn't be committed in
            // our transaction model. But a pessimistic lock will be left if the pessimistic
            // rollback request fails to send and the transaction need not to acquire this
            // lock again(due to WriteConflict). If the transaction is committed, we should
            // commit this pessimistic lock too.
            if lock.lock_type == LockType::Pessimistic {
                warn!(
                    "commit a pessimistic lock with Lock type";
                    "key" => %key,
                    "start_ts" => reader.start_ts,
                    "commit_ts" => commit_ts,
                );
                // Commit with WriteType::Lock.
                lock.lock_type = LockType::Lock;
            }
            lock
        }
        _ => {
            return match reader.get_txn_commit_record(&key)?.info() {
                Some((_, WriteType::Rollback)) | None => {
                    MVCC_CONFLICT_COUNTER.commit_lock_not_found.inc();
                    // None: related Rollback has been collapsed.
                    // Rollback: rollback by concurrent transaction.
                    info!(
                        "txn conflict (lock not found)";
                        "key" => %key,
                        "start_ts" => reader.start_ts,
                        "commit_ts" => commit_ts,
                    );
                    Err(ErrorInner::TxnLockNotFound {
                        start_ts: reader.start_ts,
                        commit_ts,
                        key: key.into_raw()?,
                    }
                    .into())
                }
                // Committed by concurrent transaction.
                Some((_, WriteType::Put))
                | Some((_, WriteType::Delete))
                | Some((_, WriteType::Lock)) => {
                    MVCC_DUPLICATE_CMD_COUNTER_VEC.commit.inc();
                    Ok(None)
                }
            };
        }
    };
    let mut write = Write::new(
        WriteType::from_lock_type(lock.lock_type).unwrap(),
        reader.start_ts,
        lock.short_value.take(),
    )
    .set_last_change(lock.last_change_ts, lock.versions_to_last_change)
    .set_txn_source(lock.txn_source);

    for ts in &lock.rollback_ts {
        if *ts == commit_ts {
            write = write.set_overlapped_rollback(true, None);
            break;
        }
    }

    txn.put_write(key.clone(), commit_ts, write.as_ref().to_bytes());
    Ok(txn.unlock_key(key, lock.is_pessimistic_txn(), commit_ts))
}

pub mod tests {
    use concurrency_manager::ConcurrencyManager;
    use kvproto::kvrpcpb::Context;
    #[cfg(test)]
    use kvproto::kvrpcpb::PrewriteRequestPessimisticAction::*;
    use tikv_kv::SnapContext;
    use txn_types::TimeStamp;

    use super::*;
    #[cfg(test)]
    use crate::storage::txn::tests::{
        must_acquire_pessimistic_lock_for_large_txn, must_prewrite_delete, must_prewrite_lock,
        must_prewrite_put, must_prewrite_put_for_large_txn, must_prewrite_put_impl,
        must_prewrite_put_with_txn_soucre, must_rollback,
    };
    #[cfg(test)]
    use crate::storage::{
        mvcc::SHORT_VALUE_MAX_LEN, txn::commands::check_txn_status, TestEngineBuilder, TxnStatus,
    };
    use crate::storage::{
        mvcc::{tests::*, MvccTxn},
        Engine,
    };

    pub fn must_succeed<E: Engine>(
        engine: &mut E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
    ) {
        must_succeed_impl(engine, key, start_ts, commit_ts, None);
    }

    pub fn must_succeed_on_region<E: Engine>(
        engine: &mut E,
        region_id: u64,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
    ) {
        must_succeed_impl(engine, key, start_ts, commit_ts, Some(region_id));
    }

    fn must_succeed_impl<E: Engine>(
        engine: &mut E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
        region_id: Option<u64>,
    ) {
        let mut ctx = Context::default();
        if let Some(region_id) = region_id {
            ctx.region_id = region_id;
        }
        let snap_ctx = SnapContext {
            pb_ctx: &ctx,
            ..Default::default()
        };
        let snapshot = engine.snapshot(snap_ctx).unwrap();
        let start_ts = start_ts.into();
        let cm = ConcurrencyManager::new(start_ts);
        let mut txn = MvccTxn::new(start_ts, cm);
        let mut reader = SnapshotReader::new(start_ts, snapshot, true);
        commit(&mut txn, &mut reader, Key::from_raw(key), commit_ts.into()).unwrap();
        write(engine, &ctx, txn.into_modifies());
    }

    pub fn must_err<E: Engine>(
        engine: &mut E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
    ) {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let start_ts = start_ts.into();
        let cm = ConcurrencyManager::new(start_ts);
        let mut txn = MvccTxn::new(start_ts, cm);
        let mut reader = SnapshotReader::new(start_ts, snapshot, true);
        commit(&mut txn, &mut reader, Key::from_raw(key), commit_ts.into()).unwrap_err();
    }

    #[cfg(test)]
    fn test_commit_ok_imp(k1: &[u8], v1: &[u8], k2: &[u8], k3: &[u8]) {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        must_prewrite_put(&mut engine, k1, v1, k1, 10);
        must_prewrite_lock(&mut engine, k2, k1, 10);
        must_prewrite_delete(&mut engine, k3, k1, 10);
        must_locked(&mut engine, k1, 10);
        must_locked(&mut engine, k2, 10);
        must_locked(&mut engine, k3, 10);
        must_succeed(&mut engine, k1, 10, 15);
        must_succeed(&mut engine, k2, 10, 15);
        must_succeed(&mut engine, k3, 10, 15);
        must_written(&mut engine, k1, 10, 15, WriteType::Put);
        must_written(&mut engine, k2, 10, 15, WriteType::Lock);
        must_written(&mut engine, k3, 10, 15, WriteType::Delete);
        // commit should be idempotent
        must_succeed(&mut engine, k1, 10, 15);
        must_succeed(&mut engine, k2, 10, 15);
        must_succeed(&mut engine, k3, 10, 15);
    }

    #[test]
    fn test_commit_ok() {
        test_commit_ok_imp(b"x", b"v", b"y", b"z");

        let long_value = "v".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_commit_ok_imp(b"x", &long_value, b"y", b"z");
    }

    #[cfg(test)]
    fn test_commit_err_imp(k: &[u8], v: &[u8]) {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        // Not prewrite yet
        must_err(&mut engine, k, 1, 2);
        must_prewrite_put(&mut engine, k, v, k, 5);
        // start_ts not match
        must_err(&mut engine, k, 4, 5);
        must_rollback(&mut engine, k, 5, false);
        // commit after rollback
        must_err(&mut engine, k, 5, 6);
    }

    #[test]
    fn test_commit_err() {
        test_commit_err_imp(b"k", b"v");

        let long_value = "v".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_commit_err_imp(b"k2", &long_value);
    }

    #[test]
    fn test_min_commit_ts() {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        let (k, v) = (b"k", b"v");

        // Shortcuts
        let ts = TimeStamp::compose;
        let uncommitted = |ttl, min_commit_ts| {
            move |s| {
                if let TxnStatus::Uncommitted { lock, .. } = s {
                    lock.ttl == ttl && lock.min_commit_ts == min_commit_ts
                } else {
                    false
                }
            }
        };

        must_prewrite_put_for_large_txn(&mut engine, k, v, k, ts(10, 0), 100, 0);
        check_txn_status::tests::must_success(
            &mut engine,
            k,
            ts(10, 0),
            ts(20, 0),
            ts(20, 0),
            true,
            false,
            false,
            uncommitted(100, ts(20, 1)),
        );
        // The min_commit_ts should be ts(20, 1)
        must_err(&mut engine, k, ts(10, 0), ts(15, 0));
        must_err(&mut engine, k, ts(10, 0), ts(20, 0));
        must_succeed(&mut engine, k, ts(10, 0), ts(20, 1));

        must_prewrite_put_for_large_txn(&mut engine, k, v, k, ts(30, 0), 100, 0);
        check_txn_status::tests::must_success(
            &mut engine,
            k,
            ts(30, 0),
            ts(40, 0),
            ts(40, 0),
            true,
            false,
            false,
            uncommitted(100, ts(40, 1)),
        );
        must_succeed(&mut engine, k, ts(30, 0), ts(50, 0));

        // If the min_commit_ts of the pessimistic lock is greater than prewrite's, use
        // it.
        must_acquire_pessimistic_lock_for_large_txn(&mut engine, k, k, ts(60, 0), ts(60, 0), 100);
        check_txn_status::tests::must_success(
            &mut engine,
            k,
            ts(60, 0),
            ts(70, 0),
            ts(70, 0),
            true,
            false,
            false,
            uncommitted(100, ts(70, 1)),
        );
        must_prewrite_put_impl(
            &mut engine,
            k,
            v,
            k,
            &None,
            ts(60, 0),
            DoPessimisticCheck,
            50,
            ts(60, 0),
            1,
            ts(60, 1),
            TimeStamp::zero(),
            false,
            kvproto::kvrpcpb::Assertion::None,
            kvproto::kvrpcpb::AssertionLevel::Off,
        );
        // The min_commit_ts is ts(70, 0) other than ts(60, 1) in prewrite request.
        must_large_txn_locked(&mut engine, k, ts(60, 0), 100, ts(70, 1), false);
        must_err(&mut engine, k, ts(60, 0), ts(65, 0));
        must_succeed(&mut engine, k, ts(60, 0), ts(80, 0));
    }

    #[test]
    fn test_inherit_last_change_info_from_lock() {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        let k = b"k";
        must_prewrite_put(&mut engine, k, b"v1", k, 5);
        must_succeed(&mut engine, k, 5, 10);

        // WriteType is Lock
        must_prewrite_lock(&mut engine, k, k, 15);
        let lock = must_locked(&mut engine, k, 15);
        assert_eq!(lock.last_change_ts, 10.into());
        assert_eq!(lock.versions_to_last_change, 1);
        must_succeed(&mut engine, k, 15, 20);
        let write = must_written(&mut engine, k, 15, 20, WriteType::Lock);
        assert_eq!(write.last_change_ts, 10.into());
        assert_eq!(write.versions_to_last_change, 1);

        // WriteType is Put
        must_prewrite_put(&mut engine, k, b"v2", k, 25);
        let lock = must_locked(&mut engine, k, 25);
        assert!(lock.last_change_ts.is_zero());
        assert_eq!(lock.versions_to_last_change, 0);
        must_succeed(&mut engine, k, 25, 30);
        let write = must_written(&mut engine, k, 25, 30, WriteType::Put);
        assert!(write.last_change_ts.is_zero());
        assert_eq!(write.versions_to_last_change, 0);
    }

    #[test]
    fn test_2pc_with_txn_source() {
        for source in [0x1, 0x85] {
            let mut engine = TestEngineBuilder::new().build().unwrap();

            let k = b"k";
            // WriteType is Put
            must_prewrite_put_with_txn_soucre(&mut engine, k, b"v2", k, 25, source);
            let lock = must_locked(&mut engine, k, 25);
            assert_eq!(lock.txn_source, source);
            must_succeed(&mut engine, k, 25, 30);
            let write = must_written(&mut engine, k, 25, 30, WriteType::Put);
            assert_eq!(write.txn_source, source);
        }
    }
}
