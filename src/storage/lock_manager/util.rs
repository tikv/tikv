// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::Lock;
use crate::storage::mvcc::Error as MvccError;
use crate::storage::txn::Error as TxnError;
use crate::storage::Error as StorageError;
use crate::storage::Key;
use crate::tikv_util::time;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const PHYSICAL_SHIFT_BITS: usize = 18;

// In milliseconds
pub fn extract_physical_timestamp(ts: u64) -> u64 {
    ts >> PHYSICAL_SHIFT_BITS
}

pub fn calculate_wait_for_lock_timeout(start_ts: u64, ttl: u64) -> u64 {
    let limit = Duration::from_millis(extract_physical_timestamp(start_ts) + ttl);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|e| {
            // If it failed, timeout will be `max_wait_for_lock_timeout`.
            warn!("get duration since UNIX_EPOCH failed"; "err" => ?e);
            Duration::from_millis(0)
        });
    if let Some(timeout) = limit.checked_sub(now) {
        time::duration_to_ms(timeout)
    } else {
        0
    }
}

pub fn extract_lock_from_result(res: &Result<(), StorageError>) -> (Lock, u64) {
    match res {
        Err(StorageError::Txn(TxnError::Mvcc(MvccError::KeyIsLocked { key, ts, ttl, .. }))) => (
            Lock {
                ts: *ts,
                hash: gen_key_hash(&Key::from_raw(&key)),
            },
            *ttl,
        ),
        _ => panic!("unsupported mvcc error"),
    }
}

pub fn gen_key_hash(key: &Key) -> u64 {
    let mut s = DefaultHasher::new();
    key.hash(&mut s);
    s.finish()
}

pub fn gen_key_hashes(keys: &[Key]) -> Vec<u64> {
    keys.iter().map(|key| gen_key_hash(key)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_lock_from_result() {
        let raw_key = b"key".to_vec();
        let key = Key::from_raw(&raw_key);
        let ts = 100;
        let case = StorageError::from(TxnError::from(MvccError::KeyIsLocked {
            key: raw_key,
            primary: vec![],
            ts,
            ttl: 100,
        }));
        let (lock, ttl) = extract_lock_from_result(&Err(case));
        assert_eq!(lock.ts, ts);
        assert_eq!(lock.hash, gen_key_hash(&key));
        assert_eq!(ttl, 100);
    }

    #[test]
    fn test_calculate_wait_for_lock_timeout() {
        let start_ts = (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            << PHYSICAL_SHIFT_BITS) as u64;
        assert!(calculate_wait_for_lock_timeout(start_ts, 1000) > 500);
        assert_eq!(calculate_wait_for_lock_timeout(0, 1000), 0);
    }
}
