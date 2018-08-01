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

mod cursor_builder;

use storage::mvcc::{Error, Result};
use storage::mvcc::{Lock, LockType, Write};
use storage::{Cursor, Iterator, Key, Snapshot, Statistics, Value, CF_LOCK};

pub use self::cursor_builder::CursorBuilder;

/// Get the lock of a user key in the lock CF.
///
/// Internally, a db get will be performed.
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

/// Get a lock of a user key in the lock CF. If lock exists, it will be checked to see whether
/// it conflicts with the given `ts`. If there is no conflict or no lock, the safe `ts` will be
/// returned.
///
/// Internally, a db get will be performed.
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

/// Checks whether the lock conflicts with the given `ts`. If there is no conflict, the safe `ts`
/// will be returned.
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

/// Iterate and get all locks in the lock CF that `predicate` returns `true` within the given
/// key space (specified by `start_key` and `limit`). If `limit` is `0`, the key space only
/// has left bound.
///
/// You may want to use the wrapper `mvcc::reader::CFReader` instead.
pub fn scan_locks<I, F>(
    lock_cursor: &mut Cursor<I>, // TODO: make it `ForwardCursor`.
    predicate: F,
    start_key: Option<&Key>,
    limit: usize,
    statistics: &mut Statistics,
) -> Result<Vec<(Key, Lock)>>
where
    I: Iterator,
    F: Fn(&Lock) -> bool,
{
    let ok = match start_key {
        Some(ref start_key) => lock_cursor.seek(start_key, &mut statistics.lock)?,
        None => lock_cursor.seek_to_first(&mut statistics.lock),
    };
    if !ok {
        return Ok(vec![]);
    }
    let mut locks = Vec::with_capacity(limit);
    loop {
        let key = Key::from_encoded(lock_cursor.key(&mut statistics.lock).to_vec());
        let lock = Lock::parse(lock_cursor.value(&mut statistics.lock))?;
        if predicate(&lock) {
            locks.push((key, lock));
            if limit > 0 && locks.len() >= limit {
                // Reach limit
                break;
            }
        }
        if !lock_cursor.next(&mut statistics.lock) {
            // No more keys
            break;
        }
    }
    statistics.lock.processed += locks.len();
    Ok(locks)
}

/// Reads user key's value in default CF according to the given write CF value (`write`).
///
/// Internally, a `near_seek` will be performed.
///
/// Notice that the value may be already carried in the `write` (short value). In this case,
/// you should not call this function.
///
/// # Panics
///
/// Panics if there is a short value carried in the given `write`.
///
/// Panics if key in default CF does not exist. This means there is a data corruption.
pub fn load_data_by_write<I>(
    default_cursor: &mut Cursor<I>, // TODO: make it `ForwardCursor`.
    key: &Key,
    write: Write,
    statistics: &mut Statistics,
) -> Result<Value>
where
    I: Iterator,
{
    assert!(write.short_value.is_none());
    let key = key.append_ts(write.start_ts); // TODO: eliminate clone.
    match default_cursor.near_seek_get(&key, &mut statistics.data)? {
        None => panic!(
            "Mvcc data for key {} is not found, start_ts = {}",
            key, write.start_ts
        ),
        Some(v) => {
            statistics.data.processed += 1;
            Ok(v.to_vec())
        }
    }
}

/// Iterate and get all user keys in the write CF within the given key space (specified by
/// `start_key` and `limit`). `limit` must not be `0`.
///
/// The return type is `(keys, next_start_key)`. `next_start_key` is the `start_key` that
/// can be used to continue scanning keys. If `next_start_key` is `None`, it means that
/// there is no more keys.
///
/// You may want to use the wrapper `mvcc::reader::CFReader` instead.
///
/// # Panics
///
/// Panics if `limit` is `0`.
pub fn scan_keys<I>(
    write_cursor: &mut Cursor<I>, // TODO: make it `ForwardCursor`.
    start_key: Option<&Key>,
    limit: usize,
    statistics: &mut Statistics,
) -> Result<(Vec<Key>, Option<Key>)>
where
    I: Iterator,
{
    assert!(limit > 0);

    let ok = match start_key {
        Some(ref x) => write_cursor.near_seek(x, &mut statistics.write)?,
        None => write_cursor.seek_to_first(&mut statistics.write),
    };
    if !ok {
        return Ok((vec![], None));
    }
    let mut keys = Vec::with_capacity(limit);
    let mut next_start_key;
    loop {
        // TODO: Eliminate memory copy
        let key =
            Key::from_encoded(write_cursor.key(&mut statistics.write).to_vec()).truncate_ts()?;
        // Jump to the last version of the key. We assumed that there is no key that ts == 0.
        next_start_key = Some(key.append_ts(0)); // TODO: Eliminate clone (might not be possible?)
        keys.push(key);
        if !write_cursor.near_seek(next_start_key.as_ref().unwrap(), &mut statistics.write)? {
            // No more keys found, we don't need to scan keys next time
            next_start_key = None;
            break;
        }
        if keys.len() >= limit {
            // Reach limit
            break;
        }
    }
    statistics.write.processed += keys.len();
    Ok((keys, next_start_key))
}
