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
use storage::mvcc::{Lock, LockType, Write, WriteType};
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
    // TODO: We need to ensure that cursor is not prefix seek.

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
/// Internally, several `near_seek` will be performed.
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
    // TODO: We need to ensure that cursor is not prefix seek.
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
        // TODO: We don't really need to copy slice to a vector here.
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

/// Iterate and get all `Write`s for a key whose commit_ts <= `max_ts`.
///
/// Internally, there will be a `near_seek` operation for first iterate and
/// `next` operation for other iterations.
///
/// The return value is a `Vec` of type `(commit_ts, write)`.
///
/// You may want to use the wrapper `mvcc::reader::CFReader` instead.
pub fn scan_writes<I>(
    write_cursor: &mut Cursor<I>, // TODO: make it `ForwardCursor`.
    user_key: &Key,
    max_ts: u64,
    statistics: &mut Statistics,
) -> Result<Vec<(u64, Write)>>
where
    I: Iterator,
{
    // TODO: We need to ensure that cursor is not prefix seek.

    let mut writes = vec![];
    write_cursor.near_seek(&user_key.append_ts(max_ts), &mut statistics.write)?;
    while write_cursor.valid() {
        // TODO: We don't really need to copy slice to a vector here.
        let current_key = Key::from_encoded(write_cursor.key(&mut statistics.write).to_vec());
        let commit_ts = current_key.decode_ts()?;
        let current_user_key = current_key.truncate_ts()?;
        if *user_key != current_user_key {
            // Meet another key: don't need to scan more.
            break;
        }

        let write = Write::parse(write_cursor.value(&mut statistics.write))?;
        writes.push((commit_ts, write));
        statistics.write.processed += 1;

        write_cursor.next(&mut statistics.write);
    }
    Ok(writes)
}

/// Iterate and get values of all versions for a given key in the default CF.
///
/// Notice that small values are embedded in `Write`, which will not be retrieved
/// by this function.
///
/// Internally, there will be a `near_seek` operation for first iterate and
/// `next` operation for other iterations.
///
/// The return value is a `Vec` of type `(start_ts, value)`.
///
/// You may want to use the wrapper `mvcc::reader::CFReader` instead.
pub fn scan_values<I>(
    default_cursor: &mut Cursor<I>, // TODO: make it `ForwardCursor`.
    user_key: &Key,
    statistics: &mut Statistics,
) -> Result<Vec<(u64, Value)>>
where
    I: Iterator,
{
    // TODO: We need to ensure that cursor is not prefix seek.

    let mut values = vec![];
    default_cursor.near_seek(user_key, &mut statistics.data)?;
    while default_cursor.valid() {
        // TODO: We don't really need to copy slice to a vector here.
        let current_key = Key::from_encoded(default_cursor.key(&mut statistics.data).to_vec());
        let start_ts = current_key.decode_ts()?;
        let current_user_key = current_key.truncate_ts()?;
        if *user_key != current_user_key {
            // Meet another key: don't need to scan more.
            break;
        }

        let value = default_cursor.value(&mut statistics.data).to_vec();
        statistics.data.processed += 1;

        values.push((start_ts, value));

        default_cursor.next(&mut statistics.write);
    }
    Ok(values)
}

/// Seek for the first committed user key with the given `start_ts`.
///
/// WARN: This function may perform a full scan. Use with caution.
///
/// You may want to use the wrapper `mvcc::reader::CFReader` instead.
pub fn slowly_seek_key_by_start_ts<I>(
    write_cursor: &mut Cursor<I>, // TODO: make it `ForwardCursor`.
    start_ts: u64,
    statistics: &mut Statistics,
) -> Result<Option<Key>>
where
    I: Iterator,
{
    // TODO: We need to ensure that cursor is not prefix seek.

    write_cursor.seek_to_first(&mut statistics.write);
    while write_cursor.valid() {
        let write = Write::parse(write_cursor.value(&mut statistics.write))?;
        statistics.write.processed += 1;
        if write.start_ts == start_ts {
            // TODO: We don't really need to copy slice to a vector here.
            let write_key = Key::from_encoded(write_cursor.key(&mut statistics.write).to_vec());;
            return Ok(Some(write_key.truncate_ts()?));
        }
        write_cursor.next(&mut statistics.write);
    }
    Ok(None)
}

/// Seek for a `Write` of a given key in the write CF by the given `start_ts`.
///
/// Internally, backward seek and backward iterate will be performed.
///
/// The return value is a `Vec` of type `(commit_ts, write)`.
///
/// You may want to use the wrapper `mvcc::reader::CFReader` instead.
///
/// # Algorithm Explanation
///
/// We start from finding the first `write` whose `commit_ts` <= given `start_ts`
/// (note that we must have `this_write.start_ts` < given `start_ts` because
/// `this_write.start_ts` < `this_write.commit_ts` <= given `start_ts`).
///
/// Then, we move cursor backward so that we will get `write`s with larger
/// `commit_ts`. It is done repeatly until we get a `write` that
/// `this_write.start_ts` == given `start_ts`.
///
/// The loop will break when we get a `write` that `this_write.start_ts` > given
/// `start_ts` and `write` is not a rollback. In this case, there must be no
/// future `write`s whose `start_ts` matches our given `start_ts`.
///
/// The rollback is an exception. We may have:
///
/// ```
/// KEY_10 => PUT_1
/// KEY_5 => ROLLBACK_5
/// ```
///
/// In this case if we want to find by `start_ts == 1`, we will meet rollback's
/// larger `start_ts == 5` first. So we just ignore it.
pub fn reverse_seek_write_by_start_ts<I>(
    write_cursor: &mut Cursor<I>, // TODO: make it `BackwardCursor`.
    user_key: &Key,
    start_ts: u64,
    statistics: &mut Statistics,
) -> Result<Option<(u64, Write)>>
where
    I: Iterator,
{
    write_cursor.near_seek_for_prev(&user_key.append_ts(start_ts), &mut statistics.write)?;
    while write_cursor.valid() {
        // TODO: We don't really need to copy slice to a vector here.
        let current_key = Key::from_encoded(write_cursor.key(&mut statistics.write).to_vec());
        let commit_ts = current_key.decode_ts()?;
        let current_user_key = current_key.truncate_ts()?;
        if *user_key != current_user_key {
            // Meet another key: don't need to scan more.
            break;
        }
        let write = Write::parse(write_cursor.value(&mut statistics.write))?;
        statistics.write.processed += 1;

        if write.start_ts == start_ts {
            return Ok(Some((commit_ts, write)));
        }

        // If we reach a commit version whose type is not Rollback and start ts is
        // larger than the given start ts, stop searching.
        if write.write_type != WriteType::Rollback && write.start_ts > start_ts {
            break;
        }

        write_cursor.prev(&mut statistics.write);
    }
    Ok(None)
}
