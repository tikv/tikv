// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::Lock;
use crate::storage::mvcc::Error as MvccError;
use crate::storage::txn::{Error as TxnError, ProcessResult};
use crate::storage::Error as StorageError;
use crate::storage::Key;
use farmhash;

pub const PHYSICAL_SHIFT_BITS: usize = 18;

// In milliseconds
pub fn extract_physical_timestamp(ts: u64) -> u64 {
    ts >> PHYSICAL_SHIFT_BITS
}

pub fn extract_lock_from_result(res: &Result<(), StorageError>) -> Lock {
    match res {
        Err(StorageError::Txn(TxnError::Mvcc(MvccError::KeyIsLocked(info)))) => Lock {
            ts: info.get_lock_version(),
            hash: gen_key_hash(&Key::from_raw(info.get_key())),
        },
        _ => panic!("unexpected mvcc error"),
    }
}

// TiDB uses the same hash algorithm.
pub fn gen_key_hash(key: &Key) -> u64 {
    farmhash::fingerprint64(&key.to_raw().unwrap())
}

pub fn gen_key_hashes(keys: &[Key]) -> Vec<u64> {
    keys.iter().map(|key| gen_key_hash(key)).collect()
}

pub fn extract_raw_key_from_process_result(pr: &ProcessResult) -> &[u8] {
    match pr {
        ProcessResult::MultiRes { results } => {
            assert!(results.len() == 1);
            match &results[0] {
                Err(StorageError::Txn(TxnError::Mvcc(MvccError::KeyIsLocked(info)))) => {
                    info.get_key()
                }
                _ => panic!("unexpected mvcc error"),
            }
        }
        _ => panic!("unexpected progress result"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use kvproto::kvrpcpb::LockInfo;

    #[test]
    fn test_extract_lock_from_result() {
        let raw_key = b"key".to_vec();
        let key = Key::from_raw(&raw_key);
        let ts = 100;
        let case = StorageError::from(TxnError::from(MvccError::KeyIsLocked(LockInfo::default())));
        let lock = extract_lock_from_result(&Err(case));
        assert_eq!(lock.ts, ts);
        assert_eq!(lock.hash, gen_key_hash(&key));
    }

    #[test]
    fn test_extract_raw_key_from_process_result() {
        let raw_key = b"foo".to_vec();
        let mut info = LockInfo::default();
        info.set_key(raw_key.clone());
        let pr = ProcessResult::MultiRes {
            results: vec![Err(StorageError::from(TxnError::from(
                MvccError::KeyIsLocked(info),
            )))],
        };
        assert_eq!(raw_key, extract_raw_key_from_process_result(&pr));
    }
}
