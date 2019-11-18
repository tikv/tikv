// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::borrow::Borrow;

use crate::storage::mvcc::{Error as MvccError, ErrorInner as MvccErrorInner};
use crate::storage::txn::{Error as TxnError, ProcessResult};
use crate::storage::Error as StorageError;
use crate::storage::{Key, StorageCb};

#[derive(Clone, Copy, PartialEq, Debug, Default)]
pub struct Lock {
    pub ts: u64,
    pub hash: u64,
}

/// `LockMgr` manages transactions waiting for locks holden by other transactions.
/// It has responsibility to handle deadlocks between transactions.
pub trait LockMgr: Clone + Send + 'static {
    /// Transaction with `start_ts` waits for `lock` released.
    ///
    /// If the lock is released or waiting times out or deadlock occurs, the transaction
    /// should be waken up and call `cb` with `pr` to notify the caller.
    ///
    /// If the lock is the first lock the transaction waits for, it won't result in deadlock.
    fn wait_for(
        &self,
        start_ts: u64,
        cb: StorageCb,
        pr: ProcessResult,
        lock: Lock,
        is_first_lock: bool,
        timeout: i64,
    );

    /// The locks with `lock_ts` and `hashes` are released, trys to wake up transactions.
    fn wake_up(
        &self,
        lock_ts: u64,
        hashes: Option<Vec<u64>>,
        commit_ts: u64,
        is_pessimistic_txn: bool,
    );

    /// Returns true if there are waiters in the `LockMgr`.
    ///
    /// This function is used to avoid useless calculation and wake-up.
    fn has_waiter(&self) -> bool {
        true
    }
}

// For test
#[derive(Clone)]
pub struct DummyLockMgr;

impl LockMgr for DummyLockMgr {
    fn wait_for(
        &self,
        _start_ts: u64,
        _cb: StorageCb,
        _pr: ProcessResult,
        _lock: Lock,
        _is_first_lock: bool,
        _wait_timeout: i64,
    ) {
    }

    fn wake_up(
        &self,
        _lock_ts: u64,
        _hashes: Option<Vec<u64>>,
        _commit_ts: u64,
        _is_pessimistic_txn: bool,
    ) {
    }
}

pub fn extract_lock_from_result(res: &Result<(), StorageError>) -> Lock {
    match res {
        Err(StorageError::Txn(TxnError::Mvcc(MvccError(box MvccErrorInner::KeyIsLocked(
            info,
        ))))) => Lock {
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
        let case = StorageError::from(TxnError::from(MvccError::from(
            MvccErrorInner::KeyIsLocked(info),
        )));
        let lock = extract_lock_from_result(&Err(case));
        assert_eq!(lock.ts, ts);
        assert_eq!(lock.hash, gen_key_hash(&key));
    }
}
