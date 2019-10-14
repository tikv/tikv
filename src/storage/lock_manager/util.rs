// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::borrow::Borrow;

use super::Lock;
use crate::storage::mvcc::Error as MvccError;
use crate::storage::txn::Error as TxnError;
use crate::storage::Error as StorageError;
use crate::storage::Key;

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

pub fn gen_key_hashes<K: Borrow<Key>>(keys: &[K]) -> Vec<u64> {
    keys.iter().map(|key| gen_key_hash(key.borrow())).collect()
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
        let mut info = LockInfo::default();
        info.set_key(raw_key);
        info.set_lock_version(ts);
        info.set_lock_ttl(100);
        let case = StorageError::from(TxnError::from(MvccError::KeyIsLocked(info)));
        let lock = extract_lock_from_result(&Err(case));
        assert_eq!(lock.ts, ts);
        assert_eq!(lock.hash, gen_key_hash(&key));
    }
}
