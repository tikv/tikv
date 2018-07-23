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
use storage::mvcc::{Lock, LockType};
use storage::{Cursor, Iterator, Key, Snapshot, Statistics, CF_LOCK};

/// Get the lock of a user key in the lock CF.
///
/// Internally, a db get will be performed.
///
/// You may want to use the wrapper `mvcc::reader::CFReader` instead.
pub fn load_lock<S>(snapshot: &S, key: &Key, statistics: &mut Statistics) -> Result<Option<Lock>>
where
    S: Snapshot,
{
    let res = match snapshot.get_cf(CF_LOCK, key)? {
        Some(v) => Some(Lock::parse(&v)?),
        None => None,
    };
    if res.is_some() {
        statistics.lock.processed += 1;
    }
    Ok(res)
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
        return check_lock(key, ts, lock);
    }
    Ok(ts)
}

/// Checks whether the lock conflicts with the given `ts`. If there is no conflict, the safe `ts`
/// will be returned.
pub fn check_lock(key: &Key, ts: u64, lock: Lock) -> Result<u64> {
    if lock.ts > ts || lock.lock_type == LockType::Lock {
        // Ignore lock when lock.ts > ts or lock's type is Lock
        return Ok(ts);
    }

    if ts == ::std::u64::MAX && key.raw()? == lock.primary {
        // When `ts == u64::MAX` (which means to get latest committed version for
        // primary key), and current key is the primary key, we return the latest
        // committed version.
        return Ok(lock.ts - 1);
    }

    // There is a pending lock. Client should wait or clean it.
    Err(Error::KeyIsLocked {
        key: key.raw()?,
        primary: lock.primary,
        ts: lock.ts,
        ttl: lock.ttl,
    })
}

/// Iterate and get all locks in the lock CF that `predicate` returns `true` within the given
/// key space (specified by `start_key` and `limit`).
#[cfg_attr(feature = "cargo-clippy", allow(type_complexity))]
pub fn scan_lock<I, F>(
    lock_cursor: &mut Cursor<I>,
    predicate: F,
    start_key: Option<Key>,
    limit: usize,
    statistics: &mut Statistics,
) -> Result<(Vec<(Key, Lock)>, Option<Key>)>
where
    I: Iterator,
    F: Fn(&Lock) -> bool,
{
    let ok = match start_key {
        Some(ref start_key) => lock_cursor.seek(start_key, &mut statistics.lock)?,
        None => lock_cursor.seek_to_first(&mut statistics.lock),
    };
    if !ok {
        return Ok((vec![], None));
    }
    let mut locks = vec![];
    while lock_cursor.valid() {
        let key = Key::from_encoded(lock_cursor.key().to_vec());
        let lock = Lock::parse(lock_cursor.value())?;
        if predicate(&lock) {
            locks.push((key.clone(), lock));
            if limit > 0 && locks.len() >= limit {
                return Ok((locks, Some(key)));
            }
        }
        lock_cursor.next(&mut statistics.lock);
    }
    statistics.lock.processed += locks.len();
    Ok((locks, None))
}
