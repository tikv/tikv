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
use storage::{Cursor, Iterator, Key, Snapshot, Statistics, Value};
use storage::{CF_LOCK, CF_WRITE};
use util::properties::MvccProperties;

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
    if lock.ts > ts || lock.lock_type == LockType::Lock {
        // Ignore lock when lock.ts > ts or lock's type is Lock
        return Ok(CheckLockResult::NotLocked);
    }

    let raw_key = key.raw()?;

    if ts == ::std::u64::MAX && raw_key == lock.primary {
        // When `ts == u64::MAX` (which means to get latest committed version for
        // primary key), and current key is the primary key, we return the latest
        // committed version.
        return Ok(CheckLockResult::Ignored(lock.ts - 1));
    }

    // There is a pending lock. Client should wait or clean it.
    Ok(CheckLockResult::Locked(Error::KeyIsLocked {
        key: raw_key,
        primary: lock.primary.clone(),
        ts: lock.ts,
        ttl: lock.ttl,
    }))
}

/// Get the lock of a user key in the lock CF.
///
/// Internally, there is a db `get`.
///
/// You may want to use the wrapper `mvcc::reader::CfReader` instead.
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
/// You may want to use the wrapper `mvcc::reader::CfReader` instead.
pub fn load_and_check_lock<S>(
    snapshot: &S,
    key: &Key,
    ts: u64,
    statistics: &mut Statistics,
) -> Result<CheckLockResult>
where
    S: Snapshot,
{
    if let Some(lock) = load_lock(snapshot, key, statistics)? {
        return check_lock(key, ts, &lock);
    }
    Ok(CheckLockResult::NotLocked)
}

/// Load the lock from current cursor and check lock.
/// This function requires that the cursor is pointing at the lock of the user key.
#[inline]
pub fn load_and_check_lock_from_cursor<I>(
    lock_cursor: &mut Cursor<I>,
    user_key: &Key,
    ts: u64,
    statistics: &mut Statistics,
) -> Result<CheckLockResult>
where
    I: Iterator,
{
    let lock_value = lock_cursor.value(&mut statistics.lock);
    let lock = Lock::parse(lock_value)?;
    check_lock(user_key, ts, &lock)
}

/// Reads user key's value in default CF according to the given write CF value
/// (`write`).
///
/// Internally, there will be a `near_seek` operation with `allow_reseek == true`.
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
    default_cursor.near_seek(&seek_key, true, &mut statistics.data)?;
    assert!(default_cursor.valid());
    assert!(default_cursor.key(&mut statistics.data) == seek_key.encoded().as_slice());
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
    default_cursor.near_seek_for_prev(&seek_key, true, &mut statistics.data)?;
    assert!(default_cursor.valid());
    assert!(default_cursor.key(&mut statistics.data) == seek_key.encoded().as_slice());
    statistics.data.processed += 1;
    Ok(default_cursor.value(&mut statistics.data).to_vec())
}

/// Get Mvcc properties stored in each sstable's metadata.
///
// TODO: Move it out of mvcc::reader.
pub fn get_mvcc_properties<S>(snapshot: &S, safe_point: u64) -> Option<MvccProperties>
where
    S: Snapshot,
{
    let collection = match snapshot.get_properties_cf(CF_WRITE) {
        Ok(v) => v,
        Err(_) => return None,
    };
    if collection.is_empty() {
        return None;
    }
    // Aggregate MVCC properties.
    let mut props = MvccProperties::new();
    for (_, v) in &*collection {
        let mvcc = match MvccProperties::decode(v.user_collected_properties()) {
            Ok(v) => v,
            Err(_) => return None,
        };
        // Filter out properties after safe_point.
        if mvcc.min_ts > safe_point {
            continue;
        }
        props.add(&mvcc);
    }
    Some(props)
}

const GC_MAX_ROW_VERSIONS_THRESHOLD: u64 = 100;

/// Returns true if it needs gc.
/// This is for optimization purpose, does not mean to be accurate.
///
// TODO: Move it out of mvcc::reader.
pub fn need_gc<S>(snapshot: &S, safe_point: u64, ratio_threshold: f64) -> bool
where
    S: Snapshot,
{
    // Always GC.
    if ratio_threshold < 1.0 {
        return true;
    }

    let props = match get_mvcc_properties(snapshot, safe_point) {
        Some(v) => v,
        None => return true,
    };

    // No data older than safe_point to GC.
    if props.min_ts > safe_point {
        return false;
    }

    // Note: Since the properties are file-based, it can be false positive.
    // For example, multiple files can have a different version of the same row.

    // A lot of MVCC versions to GC.
    if props.num_versions as f64 > props.num_rows as f64 * ratio_threshold {
        return true;
    }
    // A lot of non-effective MVCC versions to GC.
    if props.num_versions as f64 > props.num_puts as f64 * ratio_threshold {
        return true;
    }

    // A lot of MVCC versions of a single row to GC.
    props.max_row_versions > GC_MAX_ROW_VERSIONS_THRESHOLD
}
