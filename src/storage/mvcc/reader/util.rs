// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::mvcc::{Error, Result};
use crate::storage::mvcc::{Lock, LockType};
use crate::storage::Key;

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
