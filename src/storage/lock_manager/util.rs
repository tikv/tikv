// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::Lock;
use crate::storage::mvcc::Error as MvccError;
use crate::storage::txn::{Error as TxnError, ProcessResult};
use crate::storage::Error as StorageError;
use crate::storage::Key;
use farmhash;

pub fn extract_lock_from_result(res: &Result<(), StorageError>) -> Lock {
    match res {
        Err(StorageError::Txn(TxnError::Mvcc(MvccError::KeyIsLocked { key, ts, .. }))) => Lock {
            ts: *ts,
            hash: gen_key_hash(&Key::from_raw(&key)),
        },
        _ => panic!("unexpected mvcc error"),
    }
}

// TiDB uses the same hash algorithm.
pub fn gen_key_hash(key: &Key) -> u64 {
    farmhash::fingerprint64(key.as_encoded())
}

pub fn gen_key_hashes(keys: &[Key]) -> Vec<u64> {
    keys.iter().map(|key| gen_key_hash(key)).collect()
}

pub fn extract_raw_key_from_process_result(pr: &ProcessResult) -> &[u8] {
    match pr {
        ProcessResult::MultiRes { results } => {
            assert!(results.len() == 1);
            match results[0] {
                Err(StorageError::Txn(TxnError::Mvcc(MvccError::KeyIsLocked {
                    ref key, ..
                }))) => key,
                _ => panic!("unexpected mvcc error"),
            }
        }
        _ => panic!("unexpected progress result"),
    }
}

pub fn gen_raw_key_hash_from_process_result(pr: &ProcessResult) -> u64 {
    farmhash::fingerprint64(extract_raw_key_from_process_result(pr))
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
            txn_size: 0,
        }));
        let lock = extract_lock_from_result(&Err(case));
        assert_eq!(lock.ts, ts);
        assert_eq!(lock.hash, gen_key_hash(&key));
    }

    #[test]
    fn test_extract_raw_key_from_process_result() {
        let raw_key = b"foo".to_vec();
        let pr = ProcessResult::MultiRes {
            results: vec![Err(StorageError::from(TxnError::from(
                MvccError::KeyIsLocked {
                    key: raw_key.clone(),
                    primary: vec![],
                    ts: 0,
                    ttl: 0,
                    txn_size: 0,
                },
            )))],
        };
        assert_eq!(raw_key, extract_raw_key_from_process_result(&pr));
    }

    #[test]
    fn test_gen_raw_key_hash_from_process_result() {
        let raw_key = b"foo".to_vec();
        let pr = ProcessResult::MultiRes {
            results: vec![Err(StorageError::from(TxnError::from(
                MvccError::KeyIsLocked {
                    key: raw_key.clone(),
                    primary: vec![],
                    ts: 0,
                    ttl: 0,
                    txn_size: 0,
                },
            )))],
        };
        assert_eq!(
            farmhash::fingerprint64(&raw_key),
            gen_raw_key_hash_from_process_result(&pr)
        );
    }
}
