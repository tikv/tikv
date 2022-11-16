// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use txn_types::{Key, Lock, TimeStamp, Write, WriteType};

use crate::storage::{
    mvcc::{MvccReader, MvccTxn, SnapshotReader, MAX_TXN_WRITE_SIZE},
    txn::{actions::check_txn_status::rollback_lock, Result as TxnResult},
    Snapshot, Statistics,
};

pub const FLASHBACK_BATCH_SIZE: usize = 256 + 1 /* To store the next key for multiple batches */;

pub fn flashback_to_version_read_lock<S: Snapshot>(
    reader: &mut MvccReader<S>,
    next_lock_key: Key,
    end_key: &Key,
    statistics: &mut Statistics,
) -> TxnResult<(Vec<(Key, Lock)>, bool)> {
    let key_locks_result = reader.scan_locks(
        Some(&next_lock_key),
        Some(end_key),
        // To flashback `CF_LOCK`, we need to delete all locks.
        |_| true,
        FLASHBACK_BATCH_SIZE,
    );
    statistics.add(&reader.statistics);
    Ok(key_locks_result?)
}

pub fn flashback_to_version_read_write<S: Snapshot>(
    reader: &mut MvccReader<S>,
    next_write_key: Key,
    end_key: &Key,
    flashback_version: TimeStamp,
    flashback_commit_ts: TimeStamp,
    statistics: &mut Statistics,
) -> TxnResult<Vec<(Key, Option<Write>)>> {
    // To flashback the data, we need to get all the latest keys first by scanning
    // every unique key in `CF_WRITE` and to get its corresponding old MVCC write
    // record if exists.
    let result = reader.scan_writes(
        Some(&next_write_key),
        Some(end_key),
        Some(flashback_version),
        |_, latest_commit_ts| {
            // There is no any other write could happen after the flashback begins.
            assert!(latest_commit_ts <= flashback_commit_ts);
            // - No need to find an old version for the key if its latest `commit_ts` is
            // smaller than or equal to the flashback version.
            // - No need to flashback a key twice if its latest `commit_ts` is equal to the
            //   flashback `commit_ts`.
            latest_commit_ts > flashback_version && latest_commit_ts < flashback_commit_ts
        },
        FLASHBACK_BATCH_SIZE,
    );
    statistics.add(&reader.statistics);
    let (key_old_writes, _) = result?;
    Ok(key_old_writes)
}

// To flashback the `CF_LOCK`, we need to delete all locks records whose
// `start_ts` is greater than the specified version, and if it's not a
// short-value `LockType::Put`, we need to delete the actual data from
// `CF_DEFAULT` as well.
// TODO: `resolved_ts` should be taken into account.
pub fn flashback_to_version_lock(
    txn: &mut MvccTxn,
    reader: &mut SnapshotReader<impl Snapshot>,
    key_locks: Vec<(Key, Lock)>,
) -> TxnResult<Option<Key>> {
    for (key, lock) in key_locks {
        if txn.write_size() >= MAX_TXN_WRITE_SIZE {
            return Ok(Some(key));
        }
        // To guarantee rollback with start ts of the locks
        reader.start_ts = lock.ts;
        rollback_lock(
            txn,
            reader,
            key.clone(),
            &lock,
            lock.is_pessimistic_txn(),
            true,
        )?;
    }
    Ok(None)
}

// To flashback the `CF_WRITE` and `CF_DEFAULT`, we need to write a new MVCC
// record for each key in `self.keys` with its old value at `self.version`,
// specifically, the flashback will have the following behavior:
//   - If a key doesn't exist at `self.version`, it will be put a
//     `WriteType::Delete`.
//   - If a key exists at `self.version`, it will be put the exact same record
//     in `CF_WRITE` and `CF_DEFAULT` with `self.commit_ts` and `self.start_ts`.
pub fn flashback_to_version_write(
    txn: &mut MvccTxn,
    reader: &mut SnapshotReader<impl Snapshot>,
    key_old_writes: Vec<(Key, Option<Write>)>,
    start_ts: TimeStamp,
    commit_ts: TimeStamp,
) -> TxnResult<Option<Key>> {
    for (key, old_write) in key_old_writes {
        #[cfg(feature = "failpoints")]
        {
            let should_skip = || {
                fail::fail_point!("flashback_skip_1_key_in_write", |_| true);
                false
            };
            if should_skip() {
                continue;
            }
        }
        if txn.write_size() >= MAX_TXN_WRITE_SIZE {
            return Ok(Some(key.clone()));
        }
        let new_write = if let Some(old_write) = old_write {
            // If it's not a short value and it's a `WriteType::Put`, we should put the old
            // value in `CF_DEFAULT` with `self.start_ts` as well.
            if old_write.short_value.is_none() && old_write.write_type == WriteType::Put {
                txn.put_value(
                    key.clone(),
                    start_ts,
                    reader.load_data(&key, old_write.clone())?,
                );
            }
            Write::new(
                old_write.write_type,
                start_ts,
                old_write.short_value.clone(),
            )
        } else {
            // If the old write doesn't exist, we should put a `WriteType::Delete` record to
            // delete the current key when needed.
            Write::new(WriteType::Delete, start_ts, None)
        };
        txn.put_write(key.clone(), commit_ts, new_write.as_ref().to_bytes());
    }
    Ok(None)
}

#[cfg(test)]
pub mod tests {
    use concurrency_manager::ConcurrencyManager;
    use kvproto::kvrpcpb::Context;
    use tikv_kv::ScanMode;
    use txn_types::TimeStamp;

    use super::*;
    use crate::storage::{
        mvcc::tests::{must_get, must_get_none, write},
        txn::{
            actions::{
                acquire_pessimistic_lock::tests::must_pessimistic_locked,
                commit::tests::must_succeed as must_commit,
                tests::{must_prewrite_delete, must_prewrite_put, must_rollback},
            },
            tests::{must_acquire_pessimistic_lock, must_pessimistic_prewrite_put_err},
        },
        Engine, TestEngineBuilder,
    };

    fn must_flashback_to_version<E: Engine>(
        engine: &mut E,
        key: &[u8],
        version: impl Into<TimeStamp>,
        start_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
    ) -> usize {
        let next_key = Key::from_raw(keys::next_key(key).as_slice());
        let key = Key::from_raw(key);
        let (version, start_ts, commit_ts) = (version.into(), start_ts.into(), commit_ts.into());
        let ctx = Context::default();
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = MvccReader::new_with_ctx(snapshot, Some(ScanMode::Forward), &ctx);
        let mut statistics = Statistics::default();
        // Flashback the locks.
        let (key_locks, has_remain_locks) =
            flashback_to_version_read_lock(&mut reader, key.clone(), &next_key, &mut statistics)
                .unwrap();
        assert!(!has_remain_locks);
        let cm = ConcurrencyManager::new(TimeStamp::zero());
        let mut txn = MvccTxn::new(start_ts, cm.clone());
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut snap_reader = SnapshotReader::new_with_ctx(version, snapshot, &ctx);
        flashback_to_version_lock(&mut txn, &mut snap_reader, key_locks).unwrap();
        let mut rows = txn.modifies.len();
        write(engine, &ctx, txn.into_modifies());
        // Flashback the writes.
        let key_old_writes = flashback_to_version_read_write(
            &mut reader,
            key,
            &next_key,
            version,
            commit_ts,
            &mut statistics,
        )
        .unwrap();
        let mut txn = MvccTxn::new(start_ts, cm);
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut snap_reader = SnapshotReader::new_with_ctx(version, snapshot, &ctx);
        flashback_to_version_write(
            &mut txn,
            &mut snap_reader,
            key_old_writes,
            start_ts,
            commit_ts,
        )
        .unwrap();
        rows += txn.modifies.len();
        write(engine, &ctx, txn.into_modifies());
        rows
    }

    #[test]
    fn test_flashback_to_version() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let mut ts = TimeStamp::zero();
        let k = b"k";
        // Prewrite and commit Put(k -> v1) with stat_ts = 1, commit_ts = 2.
        let v1 = b"v1";
        must_prewrite_put(&mut engine, k, v1, k, *ts.incr());
        must_commit(&mut engine, k, ts, *ts.incr());
        must_get(&mut engine, k, *ts.incr(), v1);
        // Prewrite and rollback Put(k -> v2) with stat_ts = 4.
        let v2 = b"v2";
        must_prewrite_put(&mut engine, k, v2, k, *ts.incr());
        must_rollback(&mut engine, k, ts, false);
        must_get(&mut engine, k, *ts.incr(), v1);
        // Prewrite and rollback Delete(k) with stat_ts = 6.
        must_prewrite_delete(&mut engine, k, k, *ts.incr());
        must_rollback(&mut engine, k, ts, false);
        must_get(&mut engine, k, *ts.incr(), v1);
        // Prewrite and commit Delete(k) with stat_ts = 8, commit_ts = 9.
        must_prewrite_delete(&mut engine, k, k, *ts.incr());
        must_commit(&mut engine, k, ts, *ts.incr());
        must_get_none(&mut engine, k, *ts.incr());
        // Prewrite and commit Put(k -> v2) with stat_ts = 11, commit_ts = 12.
        must_prewrite_put(&mut engine, k, v2, k, *ts.incr());
        must_commit(&mut engine, k, ts, *ts.incr());
        must_get(&mut engine, k, *ts.incr(), v2);
        // Flashback to version 1 with start_ts = 14, commit_ts = 15.
        assert_eq!(
            must_flashback_to_version(&mut engine, k, 1, *ts.incr(), *ts.incr()),
            1
        );
        must_get_none(&mut engine, k, *ts.incr());
        // Flashback to version 2 with start_ts = 17, commit_ts = 18.
        assert_eq!(
            must_flashback_to_version(&mut engine, k, 2, *ts.incr(), *ts.incr()),
            1
        );
        must_get(&mut engine, k, *ts.incr(), v1);
        // Flashback to version 5 with start_ts = 20, commit_ts = 21.
        assert_eq!(
            must_flashback_to_version(&mut engine, k, 5, *ts.incr(), *ts.incr()),
            1
        );
        must_get(&mut engine, k, *ts.incr(), v1);
        // Flashback to version 7 with start_ts = 23, commit_ts = 24.
        assert_eq!(
            must_flashback_to_version(&mut engine, k, 7, *ts.incr(), *ts.incr()),
            1
        );
        must_get(&mut engine, k, *ts.incr(), v1);
        // Flashback to version 10 with start_ts = 26, commit_ts = 27.
        assert_eq!(
            must_flashback_to_version(&mut engine, k, 10, *ts.incr(), *ts.incr()),
            1
        );
        must_get_none(&mut engine, k, *ts.incr());
        // Flashback to version 13 with start_ts = 29, commit_ts = 30.
        assert_eq!(
            must_flashback_to_version(&mut engine, k, 13, *ts.incr(), *ts.incr()),
            1
        );
        must_get(&mut engine, k, *ts.incr(), v2);
        // Flashback to version 27 with start_ts = 32, commit_ts = 33.
        assert_eq!(
            must_flashback_to_version(&mut engine, k, 27, *ts.incr(), *ts.incr()),
            1
        );
        must_get_none(&mut engine, k, *ts.incr());
    }

    #[test]
    fn test_flashback_to_version_deleted() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let mut ts = TimeStamp::zero();
        let (k, v) = (b"k", b"v");
        must_prewrite_put(&mut engine, k, v, k, *ts.incr());
        must_commit(&mut engine, k, ts, *ts.incr());
        must_get(&mut engine, k, ts, v);
        must_prewrite_delete(&mut engine, k, k, *ts.incr());
        must_commit(&mut engine, k, ts, *ts.incr());
        // Though the key has been deleted, flashback to version 1 still needs to write
        // a new `WriteType::Delete` with the flashback `commit_ts`.
        assert_eq!(
            must_flashback_to_version(&mut engine, k, 1, *ts.incr(), *ts.incr()),
            1
        );
        must_get_none(&mut engine, k, ts);
    }

    #[test]
    fn test_flashback_to_version_pessimistic() {
        use kvproto::kvrpcpb::PrewriteRequestPessimisticAction::*;

        let mut engine = TestEngineBuilder::new().build().unwrap();
        let k = b"k";
        let (v1, v2, v3) = (b"v1", b"v2", b"v3");
        // Prewrite and commit Put(k -> v1) with stat_ts = 10, commit_ts = 15.
        must_prewrite_put(&mut engine, k, v1, k, 10);
        must_commit(&mut engine, k, 10, 15);
        // Prewrite and commit Put(k -> v2) with stat_ts = 20, commit_ts = 25.
        must_prewrite_put(&mut engine, k, v2, k, 20);
        must_commit(&mut engine, k, 20, 25);

        must_acquire_pessimistic_lock(&mut engine, k, k, 30, 30);
        must_pessimistic_locked(&mut engine, k, 30, 30);

        // Flashback to version 17 with start_ts = 35, commit_ts = 40.
        // Distinguish from pessimistic start_ts 30 to make sure rollback ts is by lock
        // ts.
        assert_eq!(must_flashback_to_version(&mut engine, k, 17, 35, 40), 3);

        // Pessimistic Prewrite Put(k -> v3) with stat_ts = 30 will be error with
        // Rollback.
        must_pessimistic_prewrite_put_err(&mut engine, k, v3, k, 30, 30, DoPessimisticCheck);
        must_get(&mut engine, k, 45, v1);
    }

    #[test]
    fn test_duplicated_flashback_to_version() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let mut ts = TimeStamp::zero();
        let (k, v) = (b"k", b"v");
        must_prewrite_put(&mut engine, k, v, k, *ts.incr());
        must_commit(&mut engine, k, ts, *ts.incr());
        must_get(&mut engine, k, ts, v);
        let start_ts = *ts.incr();
        let commit_ts = *ts.incr();
        assert_eq!(
            must_flashback_to_version(&mut engine, k, 1, start_ts, commit_ts),
            1
        );
        must_get_none(&mut engine, k, ts);
        // Flashback again with the same `start_ts` and `commit_ts` should not do
        // anything.
        assert_eq!(
            must_flashback_to_version(&mut engine, k, 1, start_ts, commit_ts),
            0
        );
    }
}
