// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::mvcc::default_not_found_error;
use crate::storage::mvcc::{Error, Result};
use crate::storage::mvcc::{Lock, LockType, Write};
use crate::storage::{Cursor, Iterator, Key, Statistics, Value};

/// Representing check lock result.
#[derive(Debug)]
pub enum CheckLockResult {
    /// Key is locked. The key lock error is included.
    Locked(Error),

    /// Key is not locked.
    NotLocked,

    /// Key's lock exists but was ignored because of requesting the latest committed version
    /// for the primary key. The committed version is included.
    Ignored(u64),
}

/// Checks whether the lock conflicts with the given `ts`. If `ts == MaxU64`, the latest
/// committed version will be returned for primary key instead of leading to lock conflicts.
#[inline]
pub fn check_lock(key: &Key, ts: u64, lock: &Lock) -> Result<CheckLockResult> {
    if lock.ts > ts || lock.lock_type == LockType::Lock || lock.lock_type == LockType::Pessimistic {
        // Ignore lock when lock.ts > ts or lock's type is Lock or Pessimistic
        return Ok(CheckLockResult::NotLocked);
    }

    let raw_key = key.to_raw()?;

    if ts == std::u64::MAX && raw_key == lock.primary {
        // When `ts == u64::MAX` (which means to get latest committed version for
        // primary key), and current key is the primary key, we return the latest
        // committed version.
        return Ok(CheckLockResult::Ignored(lock.ts - 1));
    }

    // There is a pending lock. Client should wait or clean it.
    let mut info = kvproto::kvrpcpb::LockInfo::default();
    info.set_primary_lock(lock.primary.clone());
    info.set_lock_version(lock.ts);
    info.set_key(raw_key);
    info.set_lock_ttl(lock.ttl);
    info.set_txn_size(lock.txn_size);
    Ok(CheckLockResult::Locked(Error::KeyIsLocked(info)))
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
