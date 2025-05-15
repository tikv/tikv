// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use txn_types::{CommitRole, Key, TimeStamp, Write, WriteType};

use crate::storage::{
    Snapshot,
    mvcc::{
        ErrorInner, MvccInfo, MvccTxn, ReleasedLock, Result as MvccResult, SnapshotReader,
        metrics::{MVCC_CONFLICT_COUNTER, MVCC_DUPLICATE_CMD_COUNTER_VEC},
    },
    txn::actions::mvcc::collect_mvcc_info_for_debug,
};

pub fn commit<S: Snapshot>(
    txn: &mut MvccTxn,
    reader: &mut SnapshotReader<S>,
    key: Key,
    commit_ts: TimeStamp,
    commit_role: Option<CommitRole>,
) -> MvccResult<Option<ReleasedLock>> {
    fail_point!("commit", |err| Err(
        crate::storage::mvcc::txn::make_txn_error(err, &key, reader.start_ts,).into()
    ));

    let collect_mvcc = |reader: &SnapshotReader<S>| {
        collect_mvcc_info_for_debug(reader.reader.snapshot().clone(), &key).map(
            |(lock, writes, values)| MvccInfo {
                lock,
                writes,
                values,
            },
        )
    };

    let (mut lock, commit) = match reader.load_lock(&key)? {
        Some(lock) if lock.ts == reader.start_ts => {
            // A lock with larger min_commit_ts than current commit_ts can't be committed
            if commit_ts < lock.min_commit_ts {
                let primary_key = Key::from_raw(&lock.primary);
                // The min_commit_ts can be pushed to a larger value before
                // committing the primary.
                // When committing the secondary, the commit_ts must be determined and
                // should always be greater than the min_commit_ts.
                // If not, it is an unexpected case and may be a bug.
                let unexpected = key != primary_key;
                // collect mvcc info when an unexpected case happens.
                let mvcc_info = unexpected.then(|| collect_mvcc(reader)).flatten();
                info_or_error!(
                    !unexpected;
                    "trying to commit with smaller commit_ts than min_commit_ts";
                    "key" => %key,
                    "primary_key" => %primary_key,
                    "start_ts" => reader.start_ts,
                    "commit_ts" => commit_ts,
                    "min_commit_ts" => lock.min_commit_ts,
                    "mvcc_info" => ?mvcc_info,
                );
                return Err(ErrorInner::CommitTsExpired {
                    start_ts: reader.start_ts,
                    commit_ts,
                    key: key.into_raw()?,
                    min_commit_ts: lock.min_commit_ts,
                    mvcc_info,
                }
                .into());
            }

            // It's an abnormal routine since pessimistic locks shouldn't be committed in
            // our transaction model. But a pessimistic lock will be left if the pessimistic
            // rollback request fails to send or TiKV receives duplicated stale pessimistic
            // lock request, and the transaction need not to acquire this lock again(due to
            // WriteConflict). If the transaction is committed, we should remove the
            // pessimistic lock (like pessimistic_rollback) instead of committing.
            if lock.is_pessimistic_lock() {
                warn!(
                    "rollback a pessimistic lock when trying to commit";
                    "key" => %key,
                    "start_ts" => reader.start_ts,
                    "commit_ts" => commit_ts,
                    // Though it may not be a bug here, but we still want to collect the mvcc
                    // info here for further debugging if needed.
                    "mvcc_info" => ?collect_mvcc(reader),
                );
                (lock, false)
            } else {
                (lock, true)
            }
        }
        _ => {
            return match reader.get_txn_commit_record(&key)?.info() {
                Some((_, WriteType::Rollback)) | None => {
                    // None: related Rollback has been collapsed.
                    // Rollback: rollback by concurrent transaction.
                    MVCC_CONFLICT_COUNTER.commit_lock_not_found.inc();
                    // The lock is expected to be present for secondary commits.
                    // If not, there maybe something wrong with the txn state, and we should
                    // collect more information for debugging.
                    let unexpected = matches!(commit_role, Some(CommitRole::Secondary));
                    // only collect the mvcc information if an unexpected case happens.
                    let mvcc_info = unexpected.then(|| collect_mvcc(reader)).flatten();
                    info_or_error!(
                        !unexpected;
                        "txn conflict (lock not found)";
                        "key" => %key,
                        "start_ts" => reader.start_ts,
                        "commit_ts" => commit_ts,
                        "mvcc_info" => ?mvcc_info,
                    );
                    Err(ErrorInner::TxnLockNotFound {
                        start_ts: reader.start_ts,
                        commit_ts,
                        key: key.into_raw()?,
                        mvcc_info,
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

    if !commit {
        // Rollback a stale pessimistic lock. This function must be called by
        // resolve-lock in this case.
        assert!(lock.is_pessimistic_lock());
        return Ok(txn.unlock_key(key, lock.is_pessimistic_txn(), TimeStamp::zero()));
    }

    let mut write = Write::new(
        WriteType::from_lock_type(lock.lock_type).unwrap(),
        reader.start_ts,
        lock.short_value.take(),
    )
    .set_last_change(lock.last_change.clone())
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
    #[cfg(test)]
    use std::assert_matches::assert_matches;

    use concurrency_manager::ConcurrencyManager;
    use kvproto::kvrpcpb::Context;
    #[cfg(test)]
    use kvproto::kvrpcpb::PrewriteRequestPessimisticAction::*;
    #[cfg(test)]
    use kvproto::kvrpcpb::{Assertion, AssertionLevel};
    use tikv_kv::SnapContext;
    #[cfg(test)]
    use txn_types::{LastChange, TimeStamp};

    use super::*;
    #[cfg(test)]
    use crate::storage::txn::tests::{
        must_acquire_pessimistic_lock_for_large_txn, must_find_mvcc_infos, must_prewrite_delete,
        must_prewrite_lock, must_prewrite_put, must_prewrite_put_for_large_txn,
        must_prewrite_put_impl, must_prewrite_put_with_txn_soucre, must_rollback,
    };
    use crate::storage::{
        Engine,
        mvcc::{Error, MvccTxn, tests::*},
    };
    #[cfg(test)]
    use crate::storage::{
        TestEngineBuilder, TxnStatus,
        mvcc::SHORT_VALUE_MAX_LEN,
        txn::commands::check_txn_status,
        txn::tests::{must_acquire_pessimistic_lock, must_pessimistic_prewrite_put},
    };

    pub fn must_succeed<E: Engine>(
        engine: &mut E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
    ) -> Option<ReleasedLock> {
        must_succeed_impl(engine, key, start_ts, commit_ts, None)
    }

    pub fn must_succeed_on_region<E: Engine>(
        engine: &mut E,
        region_id: u64,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
    ) -> Option<ReleasedLock> {
        must_succeed_impl(engine, key, start_ts, commit_ts, Some(region_id))
    }

    fn must_succeed_impl<E: Engine>(
        engine: &mut E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
        region_id: Option<u64>,
    ) -> Option<ReleasedLock> {
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
        let cm = ConcurrencyManager::new_for_test(start_ts);
        let mut txn = MvccTxn::new(start_ts, cm);
        let mut reader = SnapshotReader::new(start_ts, snapshot, true);
        let res = commit(
            &mut txn,
            &mut reader,
            Key::from_raw(key),
            commit_ts.into(),
            None,
        )
        .unwrap();
        write(engine, &ctx, txn.into_modifies());
        res
    }

    pub fn must_err<E: Engine>(
        engine: &mut E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
        commit_role: Option<CommitRole>,
    ) -> Error {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let start_ts = start_ts.into();
        let cm = ConcurrencyManager::new_for_test(start_ts);
        let mut txn = MvccTxn::new(start_ts, cm);
        let mut reader = SnapshotReader::new(start_ts, snapshot, true);
        commit(
            &mut txn,
            &mut reader,
            Key::from_raw(key),
            commit_ts.into(),
            commit_role,
        )
        .unwrap_err()
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
        must_err(&mut engine, k, 1, 2, None);
        must_prewrite_put(&mut engine, k, v, k, 5);
        // start_ts not match
        must_err(&mut engine, k, 4, 5, None);
        must_rollback(&mut engine, k, 5, false);
        // commit after rollback
        must_err(&mut engine, k, 5, 6, None);
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
        let (k2, v2) = (b"k2", b"v2");

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
        must_prewrite_put_impl(
            &mut engine,
            k2,
            v2,
            k,
            &None,
            ts(10, 0),
            SkipPessimisticCheck,
            100,
            TimeStamp::default(),
            0,
            ts(20, 1),
            TimeStamp::default(),
            false,
            Assertion::None,
            AssertionLevel::Off,
        );
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

        fn check_commit_ts_expired_err(
            err: Error,
            expected_key: &[u8],
            expected_start_ts: TimeStamp,
            expected_commit_ts: TimeStamp,
            expected_min_commit_ts: TimeStamp,
            has_mvcc: bool,
        ) {
            assert_matches!(err, Error(box ErrorInner::CommitTsExpired {
                start_ts,
                commit_ts,
                key,
                min_commit_ts,
                mvcc_info,
            }) if {
                assert_eq!(key, expected_key.to_vec());
                assert_eq!(start_ts,  expected_start_ts);
                assert_eq!(commit_ts,  expected_commit_ts);
                assert_eq!(min_commit_ts,  expected_min_commit_ts);
                assert_eq!(has_mvcc, mvcc_info.is_some());
                true
            })
        }

        // The min_commit_ts should be ts(20, 1)
        check_commit_ts_expired_err(
            must_err(&mut engine, k, ts(10, 0), ts(15, 0), None),
            k,
            ts(10, 0),
            ts(15, 0),
            ts(20, 1),
            // The primary key should not collect mvcc because it is an expected case.
            false,
        );
        check_commit_ts_expired_err(
            must_err(
                &mut engine,
                k2,
                ts(10, 0),
                ts(15, 0),
                Some(CommitRole::Secondary),
            ),
            k2,
            ts(10, 0),
            ts(15, 0),
            ts(20, 1),
            // The secondary key should not collect mvcc because it may be a bug.
            true,
        );
        check_commit_ts_expired_err(
            must_err(&mut engine, k2, ts(10, 0), ts(20, 0), None),
            k2,
            ts(10, 0),
            ts(20, 0),
            ts(20, 1),
            // The mvcc info should be collected committing secondary keys as it could be a bug.
            // Although the commit role is none, the primary key could be read from the lock.
            true,
        );
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
        must_err(&mut engine, k, ts(60, 0), ts(65, 0), None);
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
        assert_eq!(lock.last_change, LastChange::make_exist(10.into(), 1));
        must_succeed(&mut engine, k, 15, 20);
        let write = must_written(&mut engine, k, 15, 20, WriteType::Lock);
        assert_eq!(write.last_change, LastChange::make_exist(10.into(), 1));

        // WriteType is Put
        must_prewrite_put(&mut engine, k, b"v2", k, 25);
        let lock = must_locked(&mut engine, k, 25);
        assert_eq!(lock.last_change, LastChange::Unknown);
        must_succeed(&mut engine, k, 25, 30);
        let write = must_written(&mut engine, k, 25, 30, WriteType::Put);
        assert_eq!(write.last_change, LastChange::Unknown);
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

    #[test]
    fn test_commit_rollback_pessimistic_lock() {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        let k1 = b"k1";
        let k2 = b"k2";

        must_acquire_pessimistic_lock(&mut engine, k1, k1, 10, 10);
        must_acquire_pessimistic_lock(&mut engine, k2, k1, 10, 10);
        must_pessimistic_prewrite_put(&mut engine, k1, b"v1", k1, 10, 10, DoPessimisticCheck);
        let res = must_succeed(&mut engine, k1, 10, 20).unwrap();
        assert_eq!(res.key, Key::from_raw(k1));
        assert_eq!(res.start_ts, 10.into());
        assert_eq!(res.commit_ts, 20.into());

        let res = must_succeed(&mut engine, k2, 10, 20).unwrap();
        assert_eq!(res.key, Key::from_raw(k2));
        assert_eq!(res.start_ts, 10.into());
        assert_eq!(res.commit_ts, 0.into());

        must_written(&mut engine, k1, 10, 20, WriteType::Put);
        must_not_have_write(&mut engine, k2, 20);
        must_not_have_write(&mut engine, k2, 10);
    }

    #[test]
    fn test_commit_txn_lock_not_found() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let (start_ts, commit_ts) = (10, 20);
        let (k1, k2, k3, k4) = (b"k1", b"k2", b"k3", b"k4");
        must_prewrite_put(&mut engine, k1, b"v1", k1, start_ts);
        must_rollback(&mut engine, k1, start_ts, false);
        must_prewrite_put(&mut engine, k2, b"v2", k1, start_ts);
        must_rollback(&mut engine, k2, start_ts, false);
        must_prewrite_put(&mut engine, k4, b"v4", k1, start_ts + 1);

        // Do not collect mvcc case
        for k in [k1, k2, k3] {
            let key_str = String::from_utf8_lossy(k);
            let role = if k == k1 {
                // CommitRole is `Primary` and lock rollback.
                // It is an expected case (rollback by other txn) and should not collect mvcc.
                Some(CommitRole::Primary)
            } else {
                // When cannot determine the commit role, do not collect mvcc because it may be
                // the primary commit and it is an expected case.
                None
            };
            let err = must_err(&mut engine, k, start_ts, commit_ts, role);
            assert_matches!(
                err,
                Error(box ErrorInner::TxnLockNotFound {
                    start_ts,
                    commit_ts,
                    key,
                    mvcc_info,
                }) if {
                    assert_eq!(key, k.to_vec());
                    assert_eq!(start_ts,  10.into(), "key: {}", key_str);
                    assert_eq!( commit_ts,  20.into(), "key: {}", key_str);
                    assert_matches!(mvcc_info.clone(), None, "key: {}", key_str);
                    true
                }
            );
        }

        // Should collect mvcc for debugging when secondary commit returns an error
        // TxnLockNotFound.
        // - k2, lock rollback.
        // - k3, no lock.
        // - k4, lock but start_ts not match
        for k in [k2, k3, k4] {
            let key_str = String::from_utf8_lossy(k);
            let (expected_lock, expected_writes, expected_values) =
                must_find_mvcc_infos(&mut engine, k);
            let err = must_err(
                &mut engine,
                k,
                start_ts,
                commit_ts,
                Some(CommitRole::Secondary),
            );
            assert_matches!(
                err,
                Error(box ErrorInner::TxnLockNotFound {
                    start_ts,
                    commit_ts,
                    key,
                    mvcc_info: Some(MvccInfo {
                        lock,
                        writes,
                        values,
                    }),
                }) if {
                    assert_eq!(key, k.to_vec());
                    assert_eq!(start_ts, 10.into(), "key: {}", key_str);
                    assert_eq!( commit_ts,  20.into(), "key: {}", key_str);
                    assert_eq!(lock.clone(),  expected_lock, "key: {}", key_str);
                    assert_eq!(writes.clone(),  expected_writes, "key: {}", key_str);
                    assert_eq!(values.clone(), expected_values, "key: {}", key_str);
                    true
                }
            );
        }
    }
}
