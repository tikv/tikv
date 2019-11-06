// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::mvcc::default_not_found_error;
use crate::storage::mvcc::{Error, Result, TsSet};
use crate::storage::mvcc::{Lock, LockType, Write};
use crate::storage::{Cursor, Iterator, Key, Statistics, Value};

/// Checks whether the lock conflicts with the given `ts`. If `ts == MaxU64`, the primary lock will be ignored.
#[inline]
pub fn check_lock(key: &Key, ts: u64, lock: Lock, bypass_locks: &TsSet) -> Result<()> {
    if lock.ts > ts || lock.lock_type == LockType::Lock || lock.lock_type == LockType::Pessimistic {
        // Ignore lock when lock.ts > ts or lock's type is Lock or Pessimistic
        return Ok(());
    }

    if bypass_locks.contains(lock.ts) {
        return Ok(());
    }

    let raw_key = key.to_raw()?;

    if ts == std::u64::MAX && raw_key == lock.primary {
        // When `ts == u64::MAX` (which means to get latest committed version for
        // primary key), and current key is the primary key, we ignore this lock.
        return Ok(());
    }

    // There is a pending lock. Client should wait or clean it.
    Err(Error::KeyIsLocked(lock.into_lock_info(raw_key)))
}

/// Reads user key's value in default CF according to the given write CF value
/// (`write`).
///
/// Internally, there will be a `near_seek` operation.
///
/// Notice that the value may be already carried in the `write` (short value). In this
/// case, you should not call this function.
///
/// # Panics
///
/// Panics if there is a short value carried in the given `write`.
///
/// Panics if key in default CF does not exist. This means there is a data corruption.
pub fn near_load_data_by_write<I>(
    default_cursor: &mut Cursor<I>, // TODO: make it `ForwardCursor`.
    user_key: &Key,
    write: Write,
    statistics: &mut Statistics,
) -> Result<Value>
where
    I: Iterator,
{
    assert!(write.short_value.is_none());
    let seek_key = user_key.clone().append_ts(write.start_ts);
    default_cursor.near_seek(&seek_key, &mut statistics.data)?;
    if !default_cursor.valid()?
        || default_cursor.key(&mut statistics.data) != seek_key.as_encoded().as_slice()
    {
        return Err(default_not_found_error(
            user_key.to_raw()?,
            write,
            "near_load_data_by_write",
        ));
    }
    statistics.data.processed += 1;
    Ok(default_cursor.value(&mut statistics.data).to_vec())
}

/// Similar to `near_load_data_by_write`, but accepts a `BackwardCursor` and use
/// `near_seek_for_prev` internally.
pub fn near_reverse_load_data_by_write<I>(
    default_cursor: &mut Cursor<I>, // TODO: make it `BackwardCursor`.
    user_key: &Key,
    write: Write,
    statistics: &mut Statistics,
) -> Result<Value>
where
    I: Iterator,
{
    assert!(write.short_value.is_none());
    let seek_key = user_key.clone().append_ts(write.start_ts);
    default_cursor.near_seek_for_prev(&seek_key, &mut statistics.data)?;
    if !default_cursor.valid()?
        || default_cursor.key(&mut statistics.data) != seek_key.as_encoded().as_slice()
    {
        return Err(default_not_found_error(
            user_key.to_raw()?,
            write,
            "near_reverse_load_data_by_write",
        ));
    }
    statistics.data.processed += 1;
    Ok(default_cursor.value(&mut statistics.data).to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_lock() {
        let key = Key::from_raw(b"foo");
        let mut lock = Lock::new(LockType::Put, vec![], 100, 3, None, 0, 1, 0);

        // Ignore the lock if read ts is less than the lock version
        check_lock(&key, 50, lock.clone(), &Default::default()).unwrap();

        // Returns the lock if read ts >= lock version
        check_lock(&key, 110, lock.clone(), &Default::default()).unwrap_err();

        // Ignore locks that occurs in the `bypass_locks` set.
        check_lock(&key, 110, lock.clone(), &TsSet::new(vec![109])).unwrap_err();
        check_lock(&key, 110, lock.clone(), &TsSet::new(vec![110])).unwrap_err();
        check_lock(&key, 110, lock.clone(), &TsSet::new(vec![100])).unwrap();
        check_lock(
            &key,
            110,
            lock.clone(),
            &TsSet::new(vec![99, 101, 102, 100, 80]),
        )
        .unwrap();

        // Ignore the lock if it is Lock or Pessimistic.
        lock.lock_type = LockType::Lock;
        check_lock(&key, 110, lock.clone(), &Default::default()).unwrap();
        lock.lock_type = LockType::Pessimistic;
        check_lock(&key, 110, lock.clone(), &Default::default()).unwrap();

        // Ignore the primary lock when reading the latest committed version by setting u64::MAX as ts
        lock.lock_type = LockType::Put;
        lock.primary = b"foo".to_vec();
        check_lock(&key, std::u64::MAX, lock.clone(), &Default::default()).unwrap();

        // Should not ignore the secondary lock even though reading the latest version
        lock.primary = b"bar".to_vec();
        check_lock(&key, std::u64::MAX, lock.clone(), &Default::default()).unwrap_err();
    }
}
