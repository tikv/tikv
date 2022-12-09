// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::ops::Bound;

use txn_types::{Key, Lock, LockType, TimeStamp, Write, WriteType};

use crate::storage::{
    mvcc::{MvccReader, MvccTxn, SnapshotReader, MAX_TXN_WRITE_SIZE},
    txn::{actions::check_txn_status::rollback_lock, Result as TxnResult},
    Snapshot,
};

pub const FLASHBACK_BATCH_SIZE: usize = 256 + 1 /* To store the next key for multiple batches */;

pub fn flashback_to_version_read_lock(
    reader: &mut MvccReader<impl Snapshot>,
    next_lock_key: Key,
    end_key: &Key,
) -> TxnResult<Vec<(Key, Lock)>> {
    let result = reader.scan_locks(
        Some(&next_lock_key),
        Some(end_key),
        |_| true,
        FLASHBACK_BATCH_SIZE,
    );
    let (key_locks, _) = result?;
    Ok(key_locks)
}

pub fn flashback_to_version_read_write(
    reader: &mut MvccReader<impl Snapshot>,
    next_write_key: Key,
    start_key: &Key,
    end_key: &Key,
    flashback_version: TimeStamp,
    flashback_commit_ts: TimeStamp,
) -> TxnResult<Vec<Key>> {
    // Filter out the SST that does not have a newer version than
    // `flashback_version` in `CF_WRITE`, i.e, whose latest `commit_ts` <=
    // `flashback_version`. By doing this, we can only flashback those keys that
    // have version changed since `flashback_version` as much as possible.
    reader.set_hint_min_ts(Some(Bound::Excluded(flashback_version)));
    // To flashback the data, we need to get all the latest visible keys first by
    // scanning every unique key in `CF_WRITE`.
    let keys_result = reader.scan_latest_user_keys(
        Some(&next_write_key),
        Some(end_key),
        |key, latest_commit_ts| {
            // There is no any other write could happen after the flashback begins.
            assert!(latest_commit_ts <= flashback_commit_ts);
            // - Skip the `start_key` which as prewrite key.
            // - No need to find an old version for the key if its latest `commit_ts` is
            // smaller than or equal to the flashback version.
            // - No need to flashback a key twice if its latest `commit_ts` is equal to the
            //   flashback `commit_ts`.
            key != start_key
                && latest_commit_ts > flashback_version
                && latest_commit_ts < flashback_commit_ts
        },
        FLASHBACK_BATCH_SIZE,
    );
    let (keys, _) = keys_result?;
    Ok(keys)
}

// At the very first beginning of flashback, we need to rollback all locks in
// `CF_LOCK`.
pub fn rollback_locks(
    txn: &mut MvccTxn,
    snapshot: impl Snapshot,
    key_locks: Vec<(Key, Lock)>,
) -> TxnResult<Option<Key>> {
    let mut reader = SnapshotReader::new(txn.start_ts, snapshot, false);
    for (key, lock) in key_locks {
        if txn.write_size() >= MAX_TXN_WRITE_SIZE {
            return Ok(Some(key));
        }
        // To guarantee rollback with start ts of the locks
        reader.start_ts = lock.ts;
        rollback_lock(
            txn,
            &mut reader,
            key.clone(),
            &lock,
            lock.is_pessimistic_txn(),
            true,
        )?;
    }
    Ok(None)
}

// To flashback the `CF_WRITE` and `CF_DEFAULT`, we need to write a new MVCC
// record for each key in keys with its old value at `flashback_version`,
// specifically, the flashback will have the following behavior:
//   - If a key doesn't exist or isn't invisible at `flashback_version`, it will
//     be put a `WriteType::Delete`.
//   - If a key exists and is visible at `flashback_version`, it will be put the
//     exact same record in `CF_WRITE` and `CF_DEFAULT` with `self.commit_ts`
//     and `self.start_ts`.
pub fn flashback_to_version_write(
    txn: &mut MvccTxn,
    reader: &mut MvccReader<impl Snapshot>,
    keys: Vec<Key>,
    flashback_version: TimeStamp,
    flashback_start_ts: TimeStamp,
    flashback_commit_ts: TimeStamp,
) -> TxnResult<Option<Key>> {
    for key in keys {
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
        let old_write = reader.get_write(&key, flashback_version, None)?;
        let new_write = if let Some(old_write) = old_write {
            // If it's a `WriteType::Put` without the short value, we should put the old
            // value in `CF_DEFAULT` with `self.start_ts` as well.
            if old_write.write_type == WriteType::Put && old_write.short_value.is_none() {
                txn.put_value(
                    key.clone(),
                    flashback_start_ts,
                    reader.load_data(&key, old_write.clone())?,
                );
            }
            Write::new(
                old_write.write_type,
                flashback_start_ts,
                old_write.short_value.clone(),
            )
        } else {
            // If the old write doesn't exist, we should put a `WriteType::Delete` record to
            // delete the current key when needed.
            Write::new(WriteType::Delete, flashback_start_ts, None)
        };
        txn.put_write(key, flashback_commit_ts, new_write.as_ref().to_bytes());
    }
    Ok(None)
}

// Prewrite the `key_to_lock`, namely the `self.start_key`, to do a special 2PC
// transaction.
pub fn prewrite_flashback_key(
    txn: &mut MvccTxn,
    reader: &mut MvccReader<impl Snapshot>,
    key_to_lock: &Key,
    flashback_version: TimeStamp,
    flashback_start_ts: TimeStamp,
) -> TxnResult<()> {
    let old_write = reader.get_write(key_to_lock, flashback_version, None)?;
    // Flashback the value in `CF_DEFAULT` as well if the old write is a
    // `WriteType::Put` without the short value.
    if let Some(old_write) = old_write.as_ref() {
        if old_write.write_type == WriteType::Put
            && old_write.short_value.is_none()
            // If the value with `flashback_start_ts` already exists, we don't need to write again.
            && reader.get_value(key_to_lock, flashback_start_ts)?.is_none()
        {
            txn.put_value(
                key_to_lock.clone(),
                flashback_start_ts,
                reader.load_data(key_to_lock, old_write.clone())?,
            );
        }
    }
    txn.put_lock(
        key_to_lock.clone(),
        &Lock::new(
            old_write.as_ref().map_or(LockType::Delete, |write| {
                if write.write_type == WriteType::Delete {
                    LockType::Delete
                } else {
                    LockType::Put
                }
            }),
            key_to_lock.as_encoded().to_vec(),
            flashback_start_ts,
            0,
            old_write.and_then(|write| write.short_value),
            TimeStamp::zero(),
            1,
            TimeStamp::zero(),
        ),
    );
    Ok(())
}

pub fn commit_flashback_key(
    txn: &mut MvccTxn,
    reader: &mut MvccReader<impl Snapshot>,
    key_to_commit: &Key,
    flashback_start_ts: TimeStamp,
    flashback_commit_ts: TimeStamp,
) -> TxnResult<()> {
    if let Some(mut lock) = reader.load_lock(key_to_commit)? {
        txn.put_write(
            key_to_commit.clone(),
            flashback_commit_ts,
            Write::new(
                WriteType::from_lock_type(lock.lock_type).unwrap(),
                flashback_start_ts,
                lock.short_value.take(),
            )
            .set_last_change(lock.last_change_ts, lock.versions_to_last_change)
            .set_txn_source(lock.txn_source)
            .as_ref()
            .to_bytes(),
        );
        txn.unlock_key(
            key_to_commit.clone(),
            lock.is_pessimistic_txn(),
            flashback_commit_ts,
        );
    }
    Ok(())
}

pub fn get_first_user_key(
    reader: &mut MvccReader<impl Snapshot>,
    start_key: &Key,
    end_key: &Key,
) -> TxnResult<Option<Key>> {
    let (mut keys_result, _) =
        reader.scan_latest_user_keys(Some(start_key), Some(end_key), |_, _| true, 1)?;
    Ok(keys_result.pop())
}

#[cfg(test)]
pub mod tests {
    use concurrency_manager::ConcurrencyManager;
    use kvproto::kvrpcpb::{Context, PrewriteRequestPessimisticAction::DoPessimisticCheck};
    use tikv_kv::ScanMode;
    use txn_types::{TimeStamp, SHORT_VALUE_MAX_LEN};

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

    fn must_rollback_lock<E: Engine>(
        engine: &mut E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
    ) -> usize {
        let next_key = Key::from_raw(keys::next_key(key).as_slice());
        let key = Key::from_raw(key);
        let ctx = Context::default();
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = MvccReader::new_with_ctx(snapshot.clone(), Some(ScanMode::Forward), &ctx);
        let key_locks = flashback_to_version_read_lock(&mut reader, key, &next_key).unwrap();
        let cm = ConcurrencyManager::new(TimeStamp::zero());
        let mut txn = MvccTxn::new(start_ts.into(), cm);
        rollback_locks(&mut txn, snapshot, key_locks).unwrap();
        let rows = txn.modifies.len();
        write(engine, &ctx, txn.into_modifies());
        rows
    }

    fn must_prewrite_flashback_key<E: Engine>(
        engine: &mut E,
        key: &[u8],
        version: impl Into<TimeStamp>,
        start_ts: impl Into<TimeStamp>,
    ) -> usize {
        let (version, start_ts) = (version.into(), start_ts.into());
        let cm = ConcurrencyManager::new(TimeStamp::zero());
        let mut txn = MvccTxn::new(start_ts, cm);
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let ctx = Context::default();
        let mut reader = MvccReader::new_with_ctx(snapshot, Some(ScanMode::Forward), &ctx);
        let prewrite_key = if let Some(first_key) =
            get_first_user_key(&mut reader, &Key::from_raw(key), &Key::from_raw(b"z")).unwrap()
        {
            first_key
        } else {
            // If the key is None return directly
            return 0;
        };
        prewrite_flashback_key(&mut txn, &mut reader, &prewrite_key, version, start_ts).unwrap();
        let rows = txn.modifies.len();
        write(engine, &ctx, txn.into_modifies());
        rows
    }

    fn must_flashback_write_to_version<E: Engine>(
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
        // Flashback the writes.
        let keys = flashback_to_version_read_write(
            &mut reader,
            key,
            &Key::from_raw(b""),
            &next_key,
            version,
            commit_ts,
        )
        .unwrap();
        let cm = ConcurrencyManager::new(TimeStamp::zero());
        let mut txn = MvccTxn::new(start_ts, cm);
        flashback_to_version_write(&mut txn, &mut reader, keys, version, start_ts, commit_ts)
            .unwrap();
        let rows = txn.modifies.len();
        write(engine, &ctx, txn.into_modifies());
        rows
    }

    fn must_commit_flashback_key<E: Engine>(
        engine: &mut E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
    ) -> usize {
        let (start_ts, commit_ts) = (start_ts.into(), commit_ts.into());
        let cm = ConcurrencyManager::new(TimeStamp::zero());
        let mut txn = MvccTxn::new(start_ts, cm);
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let ctx = Context::default();
        let mut reader = MvccReader::new_with_ctx(snapshot, Some(ScanMode::Forward), &ctx);
        let key_to_lock =
            get_first_user_key(&mut reader, &Key::from_raw(key), &Key::from_raw(b"z"))
                .unwrap()
                .unwrap();
        commit_flashback_key(&mut txn, &mut reader, &key_to_lock, start_ts, commit_ts).unwrap();
        let rows = txn.modifies.len();
        write(engine, &ctx, txn.into_modifies());
        rows
    }

    #[test]
    fn test_flashback_write_to_version() {
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
            must_flashback_write_to_version(&mut engine, k, 1, *ts.incr(), *ts.incr()),
            1
        );
        must_get_none(&mut engine, k, *ts.incr());
        // Flashback to version 2 with start_ts = 17, commit_ts = 18.
        assert_eq!(
            must_flashback_write_to_version(&mut engine, k, 2, *ts.incr(), *ts.incr()),
            1
        );
        must_get(&mut engine, k, *ts.incr(), v1);
        // Flashback to version 5 with start_ts = 20, commit_ts = 21.
        assert_eq!(
            must_flashback_write_to_version(&mut engine, k, 5, *ts.incr(), *ts.incr()),
            1
        );
        must_get(&mut engine, k, *ts.incr(), v1);
        // Flashback to version 7 with start_ts = 23, commit_ts = 24.
        assert_eq!(
            must_flashback_write_to_version(&mut engine, k, 7, *ts.incr(), *ts.incr()),
            1
        );
        must_get(&mut engine, k, *ts.incr(), v1);
        // Flashback to version 10 with start_ts = 26, commit_ts = 27.
        assert_eq!(
            must_flashback_write_to_version(&mut engine, k, 10, *ts.incr(), *ts.incr()),
            1
        );
        must_get_none(&mut engine, k, *ts.incr());
        // Flashback to version 13 with start_ts = 29, commit_ts = 30.
        assert_eq!(
            must_flashback_write_to_version(&mut engine, k, 13, *ts.incr(), *ts.incr()),
            1
        );
        must_get(&mut engine, k, *ts.incr(), v2);
        // Flashback to version 27 with start_ts = 32, commit_ts = 33.
        assert_eq!(
            must_flashback_write_to_version(&mut engine, k, 27, *ts.incr(), *ts.incr()),
            1
        );
        must_get_none(&mut engine, k, *ts.incr());
    }

    #[test]
    fn test_flashback_write_to_version_deleted() {
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
            must_flashback_write_to_version(&mut engine, k, 1, *ts.incr(), *ts.incr()),
            1
        );
        must_get_none(&mut engine, k, ts);
    }

    #[test]
    fn test_flashback_write_to_version_pessimistic() {
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
        assert_eq!(must_rollback_lock(&mut engine, k, 35), 2);
        assert_eq!(
            must_flashback_write_to_version(&mut engine, k, 17, 35, 40),
            1
        );

        // Pessimistic Prewrite Put(k -> v3) with stat_ts = 30 will be error with
        // Rollback.
        must_pessimistic_prewrite_put_err(&mut engine, k, v3, k, 30, 30, DoPessimisticCheck);
        must_get(&mut engine, k, 45, v1);
    }

    #[test]
    fn test_duplicated_flashback_write_to_version() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let mut ts = TimeStamp::zero();
        let (k, v) = (b"k", b"v");
        must_prewrite_put(&mut engine, k, v, k, *ts.incr());
        must_commit(&mut engine, k, ts, *ts.incr());
        must_get(&mut engine, k, ts, v);
        let start_ts = *ts.incr();
        let commit_ts = *ts.incr();
        assert_eq!(
            must_flashback_write_to_version(&mut engine, k, 1, start_ts, commit_ts),
            1
        );
        must_get_none(&mut engine, k, ts);
        // Flashback again with the same `start_ts` and `commit_ts` should not do
        // anything.
        assert_eq!(
            must_flashback_write_to_version(&mut engine, k, 1, start_ts, commit_ts),
            0
        );
    }

    #[test]
    fn test_duplicated_prewrite_flashback_key() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let mut ts = TimeStamp::zero();
        let (k, v) = (b"k", [u8::MAX; SHORT_VALUE_MAX_LEN + 1]);
        must_prewrite_put(&mut engine, k, &v, k, *ts.incr());
        must_commit(&mut engine, k, ts, *ts.incr());
        must_get(&mut engine, k, ts, &v);

        let flashback_start_ts = *ts.incr();
        // Rollback nothing.
        assert_eq!(must_rollback_lock(&mut engine, k, flashback_start_ts), 0);
        // Lock and write the value of `k`.
        assert_eq!(
            must_prewrite_flashback_key(&mut engine, k, 2, flashback_start_ts),
            2
        );
        // Retry Prepare
        // Unlock `k`, put rollback record and delete the value of `k`.
        assert_eq!(must_rollback_lock(&mut engine, k, flashback_start_ts), 3);
        // Lock and write the value of `k`.
        assert_eq!(
            must_prewrite_flashback_key(&mut engine, k, 2, flashback_start_ts),
            2
        );
        // Retry Prepare
        // Only unlock `k` since there is an overlapped rollback record.
        assert_eq!(must_rollback_lock(&mut engine, k, flashback_start_ts), 1);
        // Only lock `k` since the value of `k` has already existed.
        assert_eq!(
            must_prewrite_flashback_key(&mut engine, k, 2, flashback_start_ts),
            1
        );
    }

    #[test]
    fn test_prewrite_with_special_key() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let mut ts = TimeStamp::zero();
        let (prewrite_key, prewrite_val) = (b"b", b"val");
        must_prewrite_put(
            &mut engine,
            prewrite_key,
            prewrite_val,
            prewrite_key,
            *ts.incr(),
        );
        must_commit(&mut engine, prewrite_key, ts, *ts.incr());
        must_get(&mut engine, prewrite_key, ts, prewrite_val);
        let (k, v1, v2) = (b"c", b"v1", b"v2");
        must_prewrite_put(&mut engine, k, v1, k, *ts.incr());
        must_commit(&mut engine, k, ts, *ts.incr());
        must_prewrite_put(&mut engine, k, v2, k, *ts.incr());
        must_commit(&mut engine, k, ts, *ts.incr());
        must_get(&mut engine, k, ts, v2);
        // Check for prewrite key b"b".
        let ctx = Context::default();
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = MvccReader::new_with_ctx(snapshot, Some(ScanMode::Forward), &ctx);
        let first_key = get_first_user_key(&mut reader, &Key::from_raw(b""), &Key::from_raw(b"z"))
            .unwrap_or_else(|_| Some(Key::from_raw(b"")))
            .unwrap();
        assert_eq!(first_key, Key::from_raw(prewrite_key));

        // case 1: start key is before all keys, flashback b"c".
        let start_key = b"a";
        let (flashback_start_ts, flashback_commit_ts) = (*ts.incr(), *ts.incr());
        // Rollback nothing.
        assert_eq!(must_rollback_lock(&mut engine, k, flashback_start_ts), 0);
        // Prewrite "prewrite_key" not "start_key".
        assert_eq!(
            must_prewrite_flashback_key(&mut engine, start_key, 4, flashback_start_ts),
            1
        );
        // Flashback (b"c", v2) to (b"c", v1).
        assert_eq!(
            must_flashback_write_to_version(
                &mut engine,
                k,
                4,
                flashback_start_ts,
                flashback_commit_ts
            ),
            1
        );
        // Put prewrite record and Unlock, will commit "prewrite_key" not "start_key".
        assert_eq!(
            must_commit_flashback_key(
                &mut engine,
                start_key,
                flashback_start_ts,
                flashback_commit_ts
            ),
            2
        );
        must_get(&mut engine, k, ts, v1);
        must_get(&mut engine, prewrite_key, ts, prewrite_val);

        // case 2: start key is after all keys, prewrite will return None.
        let start_key = b"d";
        let flashback_start_ts = *ts.incr();
        // Rollback nothing.
        assert_eq!(must_rollback_lock(&mut engine, k, flashback_start_ts), 0);
        // Prewrite null.
        assert_eq!(
            must_prewrite_flashback_key(&mut engine, start_key, 4, flashback_start_ts),
            0
        );
        // case 3: start key is valid, end_key is invalid, prewrite key will be None.
        let first_key = get_first_user_key(&mut reader, &Key::from_raw(b"a"), &Key::from_raw(b""))
            .unwrap_or_else(|_| Some(Key::from_raw(b"")));
        assert_eq!(first_key, None);
    }
}
