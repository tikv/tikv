// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use txn_types::{Key, Lock, LockType, TimeStamp, Write, WriteType};

use crate::storage::{
    mvcc::{MvccReader, MvccTxn, SnapshotReader, MAX_TXN_WRITE_SIZE},
    txn::{Error, ErrorInner, Result as TxnResult},
    Snapshot, Statistics,
};

pub const FLASHBACK_BATCH_SIZE: usize = 256 + 1 /* To store the next key for multiple batches */;

// TODO: we should resolve all locks before starting a flashback.
pub fn flashback_to_version_read_lock<S: Snapshot>(
    reader: &mut MvccReader<S>,
    next_lock_key: &Option<Key>,
    end_key: &Option<Key>,
    statistics: &mut Statistics,
) -> TxnResult<(Vec<(Key, Lock)>, bool)> {
    if next_lock_key.is_none() {
        return Ok((vec![], false));
    }
    let key_locks_result = reader.scan_locks(
        next_lock_key.as_ref(),
        end_key.as_ref(),
        // To flashback `CF_LOCK`, we need to delete all locks.
        |_| true,
        FLASHBACK_BATCH_SIZE,
    );
    statistics.add(&reader.statistics);
    Ok(key_locks_result?)
}

pub fn flashback_to_version_read_write<S: Snapshot>(
    reader: &mut MvccReader<S>,
    key_locks_len: usize,
    next_write_key: &Option<Key>,
    end_key: &Option<Key>,
    flashback_version: TimeStamp,
    flashback_start_ts: TimeStamp,
    flashback_commit_ts: TimeStamp,
    statistics: &mut Statistics,
) -> TxnResult<(Vec<(Key, Option<Write>)>, bool)> {
    if next_write_key.is_none() {
        return Ok((vec![], false));
    } else if key_locks_len >= FLASHBACK_BATCH_SIZE {
        // The batch is full, we need to read the writes in the next batch later.
        return Ok((vec![], true));
    }
    // To flashback the data, we need to get all the latest keys first by scanning
    // every unique key in `CF_WRITE` and to get its corresponding old MVCC write
    // record if exists.
    let (key_ts_old_writes, has_remain_writes) = reader.scan_writes(
        next_write_key.as_ref(),
        end_key.as_ref(),
        Some(flashback_version),
        // No need to find an old version for the key if its latest `commit_ts` is smaller
        // than or equal to the version.
        |key| key.decode_ts().unwrap_or(TimeStamp::zero()) > flashback_version,
        FLASHBACK_BATCH_SIZE - key_locks_len,
    )?;
    statistics.add(&reader.statistics);
    let mut key_old_writes = Vec::with_capacity(FLASHBACK_BATCH_SIZE - key_locks_len);
    // Check the latest commit ts to make sure there is no commit change during the
    // flashback, otherwise, we need to abort the flashback.
    for (key, commit_ts, old_write) in key_ts_old_writes {
        if commit_ts >= flashback_commit_ts {
            return Err(Error::from(ErrorInner::InvalidTxnTso {
                start_ts: flashback_start_ts,
                commit_ts: flashback_commit_ts,
            }));
        }
        key_old_writes.push((key, old_write));
    }
    Ok((key_old_writes, has_remain_writes))
}

pub fn flashback_to_version<S: Snapshot>(
    txn: &mut MvccTxn,
    reader: &mut SnapshotReader<S>,
    next_lock_key: &mut Option<Key>,
    next_write_key: &mut Option<Key>,
    key_locks: Vec<(Key, Lock)>,
    key_old_writes: Vec<(Key, Option<Write>)>,
    version: TimeStamp,
    start_ts: TimeStamp,
    commit_ts: TimeStamp,
) -> TxnResult<usize> {
    let mut rows = 0;
    // To flashback the `CF_LOCK`, we need to delete all locks records whose
    // `start_ts` is greater than the specified version, and if it's not a
    // short-value `LockType::Put`, we need to delete the actual data from
    // `CF_DEFAULT` as well.
    // TODO: `resolved_ts` should be taken into account.
    for (key, lock) in key_locks {
        if txn.write_size() >= MAX_TXN_WRITE_SIZE {
            *next_lock_key = Some(key);
            break;
        }
        txn.unlock_key(key.clone(), lock.is_pessimistic_txn());
        rows += 1;
        // If the short value is none and it's a `LockType::Put`, we should delete the
        // corresponding key from `CF_DEFAULT` as well.
        if lock.short_value.is_none() && lock.lock_type == LockType::Put {
            txn.delete_value(key, lock.ts);
            rows += 1;
        }
    }
    // To flashback the `CF_WRITE` and `CF_DEFAULT`, we need to write a new MVCC
    // record for each key in `self.keys` with its old value at `self.version`,
    // specifically, the flashback will have the following behavior:
    //   - If a key doesn't exist at `self.version`, it will be put a
    //     `WriteType::Delete`.
    //   - If a key exists at `self.version`, it will be put the exact same record
    //     in `CF_WRITE` and `CF_DEFAULT` if needed with `self.commit_ts` and
    //     `self.start_ts`.
    for (key, old_write) in key_old_writes {
        if txn.write_size() >= MAX_TXN_WRITE_SIZE {
            *next_write_key = Some(key);
            break;
        }
        let new_write = if let Some(old_write) = old_write {
            // If it's not a short value and it's a `WriteType::Put`, we should put the old
            // value in `CF_DEFAULT` with `self.start_ts` as well.
            if old_write.short_value.is_none() && old_write.write_type == WriteType::Put {
                if let Some(old_value) = reader.get(&key, version)? {
                    txn.put_value(key.clone(), start_ts, old_value);
                    rows += 1;
                }
            }
            Write::new(old_write.write_type, start_ts, old_write.short_value)
        } else {
            // If the old write doesn't exist, we should put a `WriteType::Delete` record to
            // delete the current key when needed.
            if let Some((_, latest_write)) = reader.seek_write(&key, commit_ts)? {
                if latest_write.write_type == WriteType::Delete {
                    continue;
                }
            }
            Write::new(WriteType::Delete, start_ts, None)
        };
        txn.put_write(key.clone(), commit_ts, new_write.as_ref().to_bytes());
        rows += 1;
    }
    Ok(rows)
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
        txn::actions::{
            commit::tests::must_succeed as must_commit,
            tests::{must_prewrite_delete, must_prewrite_put, must_rollback},
        },
        Engine, TestEngineBuilder,
    };

    fn must_flashback_write<E: Engine>(
        engine: &E,
        key: &[u8],
        version: impl Into<TimeStamp>,
        start_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
    ) -> usize {
        let key = Key::from_raw(key);
        let (version, start_ts, commit_ts) = (version.into(), start_ts.into(), commit_ts.into());
        let ctx = Context::default();
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = MvccReader::new_with_ctx(snapshot, Some(ScanMode::Forward), &ctx);
        let mut statistics = Statistics::default();
        let (key_old_writes, has_remain_writes) = flashback_to_version_read_write(
            &mut reader,
            0,
            &Some(key.clone()),
            &None,
            version,
            start_ts,
            commit_ts,
            &mut statistics,
        )
        .unwrap();
        assert!(!has_remain_writes);
        let cm = ConcurrencyManager::new(TimeStamp::zero());
        let mut txn = MvccTxn::new(start_ts, cm);
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = SnapshotReader::new_with_ctx(version, snapshot, &ctx);
        let rows = flashback_to_version(
            &mut txn,
            &mut reader,
            &mut None,
            &mut Some(key),
            vec![],
            key_old_writes,
            version,
            start_ts,
            commit_ts,
        )
        .unwrap();
        write(engine, &ctx, txn.into_modifies());
        rows
    }

    #[test]
    fn test_flashback_to_version() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let mut ts = TimeStamp::zero();
        let k = b"k";
        // Prewrite and commit Put(k -> v1) with stat_ts = 1, commit_ts = 2.
        let v1 = b"v1";
        must_prewrite_put(&engine, k, v1, k, *ts.incr());
        must_commit(&engine, k, ts, *ts.incr());
        must_get(&engine, k, *ts.incr(), v1);
        // Prewrite and rollback Put(k -> v2) with stat_ts = 4.
        let v2 = b"v2";
        must_prewrite_put(&engine, k, v2, k, *ts.incr());
        must_rollback(&engine, k, ts, false);
        must_get(&engine, k, *ts.incr(), v1);
        // Prewrite and rollback Delete(k) with stat_ts = 6.
        must_prewrite_delete(&engine, k, k, *ts.incr());
        must_rollback(&engine, k, ts, false);
        must_get(&engine, k, *ts.incr(), v1);
        // Prewrite and commit Delete(k) with stat_ts = 8, commit_ts = 9.
        must_prewrite_delete(&engine, k, k, *ts.incr());
        must_commit(&engine, k, ts, *ts.incr());
        must_get_none(&engine, k, *ts.incr());
        // Prewrite and commit Put(k -> v2) with stat_ts = 11, commit_ts = 12.
        must_prewrite_put(&engine, k, v2, k, *ts.incr());
        must_commit(&engine, k, ts, *ts.incr());
        must_get(&engine, k, *ts.incr(), v2);
        // Flashback to version 1 with start_ts = 14, commit_ts = 15.
        assert_eq!(
            must_flashback_write(&engine, k, 1, *ts.incr(), *ts.incr()),
            1
        );
        must_get_none(&engine, k, *ts.incr());
        // Flashback to version 2 with start_ts = 17, commit_ts = 18.
        assert_eq!(
            must_flashback_write(&engine, k, 2, *ts.incr(), *ts.incr()),
            1
        );
        must_get(&engine, k, *ts.incr(), v1);
        // Flashback to version 5 with start_ts = 20, commit_ts = 21.
        assert_eq!(
            must_flashback_write(&engine, k, 5, *ts.incr(), *ts.incr()),
            1
        );
        must_get(&engine, k, *ts.incr(), v1);
        // Flashback to version 7 with start_ts = 23, commit_ts = 24.
        assert_eq!(
            must_flashback_write(&engine, k, 7, *ts.incr(), *ts.incr()),
            1
        );
        must_get(&engine, k, *ts.incr(), v1);
        // Flashback to version 10 with start_ts = 26, commit_ts = 27.
        assert_eq!(
            must_flashback_write(&engine, k, 10, *ts.incr(), *ts.incr()),
            1
        );
        must_get_none(&engine, k, *ts.incr());
        // Flashback to version 13 with start_ts = 29, commit_ts = 30.
        assert_eq!(
            must_flashback_write(&engine, k, 13, *ts.incr(), *ts.incr()),
            1
        );
        must_get(&engine, k, *ts.incr(), v2);
        // Flashback to version 27 with start_ts = 32, commit_ts = 33.
        assert_eq!(
            must_flashback_write(&engine, k, 27, *ts.incr(), *ts.incr()),
            1
        );
        must_get_none(&engine, k, *ts.incr());
    }

    #[test]
    fn test_flashback_to_version_deleted() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let mut ts = TimeStamp::zero();
        let (k, v) = (b"k", b"v");
        must_prewrite_put(&engine, k, v, k, *ts.incr());
        must_commit(&engine, k, ts, *ts.incr());
        must_get(&engine, k, ts, v);
        must_prewrite_delete(&engine, k, k, *ts.incr());
        must_commit(&engine, k, ts, *ts.incr());
        // Since the key has been deleted, flashback to version 1 should not do
        // anything.
        assert_eq!(
            must_flashback_write(&engine, k, ts, *ts.incr(), *ts.incr()),
            0
        );
        must_get_none(&engine, k, ts);
    }
}
