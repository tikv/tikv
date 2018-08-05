// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use storage::mvcc::{Error, Result};
use storage::mvcc::{Lock, LockType, Write};
use storage::{Cursor, Iterator, Key, Snapshot, Statistics, Value, CF_LOCK};

/// Get the lock of a user key in the lock CF.
///
/// Internally, there is a db `get`.
///
/// You may want to use the wrapper `mvcc::reader::CFReader` instead.
pub fn load_lock<S>(snapshot: &S, key: &Key, statistics: &mut Statistics) -> Result<Option<Lock>>
where
    S: Snapshot,
{
    let lock_value = snapshot.get_cf(CF_LOCK, key)?;
    if let Some(ref lock_value) = lock_value {
        statistics.lock.get += 1;
        statistics.lock.flow_stats.read_keys += 1;
        statistics.lock.flow_stats.read_bytes += key.encoded().len() + lock_value.len();
        statistics.lock.processed += 1;
        Ok(Some(Lock::parse(lock_value)?))
    } else {
        Ok(None)
    }
}

/// Get a lock of a user key in the lock CF. If lock exists, it will be checked to
/// see whether it conflicts with the given `ts`. If there is no conflict or no lock,
/// the safe `ts` will be returned.
///
/// Internally, there is a db `get`.
///
/// You may want to use the wrapper `mvcc::reader::CFReader` instead.
pub fn load_and_check_lock<S>(
    snapshot: &S,
    key: &Key,
    ts: u64,
    statistics: &mut Statistics,
) -> Result<u64>
where
    S: Snapshot,
{
    if let Some(lock) = load_lock(snapshot, key, statistics)? {
        return check_lock(key, ts, &lock);
    }
    Ok(ts)
}

/// Checks whether the lock conflicts with the given `ts`. If there is no conflict,
/// the safe `ts` will be returned.
pub fn check_lock(key: &Key, ts: u64, lock: &Lock) -> Result<u64> {
    if lock.ts > ts || lock.lock_type == LockType::Lock {
        // Ignore lock when lock.ts > ts or lock's type is Lock
        return Ok(ts);
    }

    let raw_key = key.raw()?;

    if ts == ::std::u64::MAX && raw_key == lock.primary {
        // When `ts == u64::MAX` (which means to get latest committed version for
        // primary key), and current key is the primary key, we return the latest
        // committed version.
        return Ok(lock.ts - 1);
    }

    // There is a pending lock. Client should wait or clean it.
    Err(Error::KeyIsLocked {
        key: raw_key,
        primary: lock.primary.clone(),
        ts: lock.ts,
        ttl: lock.ttl,
    })
}

/// Reads user key's value in default CF according to the given write CF value
/// (`write`).
///
/// Internally, there will be a `near_seek_get` operation.
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
    let seek_key = user_key.clone().append_ts(write.start_ts); // TODO: eliminate clone.

    // `allow_reseek` must be `true`, because if a seek key (`${user_key}_${ts}`) that smaller
    // than current cursor key is given, it must be valid and we should try to get it.

    match default_cursor.near_seek_get(&seek_key, true, &mut statistics.data)? {
        None => panic!(
            "Mvcc data for key {} is not found, start_ts = {}",
            user_key, write.start_ts
        ),
        Some(v) => {
            statistics.data.processed += 1;
            Ok(v.to_vec())
        }
    }
}
