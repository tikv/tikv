// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use super::metrics::{GC_DELETE_VERSIONS_HISTOGRAM, MVCC_VERSIONS_HISTOGRAM};
use crate::storage::kv::Modify;
use concurrency_manager::{ConcurrencyManager, KeyHandleGuard};
use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE};
use std::fmt;
use txn_types::{Key, Lock, PessimisticLock, TimeStamp, Value};

pub const MAX_TXN_WRITE_SIZE: usize = 32 * 1024;

#[derive(Default, Clone, Copy, Debug)]
pub struct GcInfo {
    pub found_versions: usize,
    pub deleted_versions: usize,
    pub is_completed: bool,
}

impl GcInfo {
    pub fn report_metrics(&self) {
        MVCC_VERSIONS_HISTOGRAM.observe(self.found_versions as f64);
        if self.deleted_versions > 0 {
            GC_DELETE_VERSIONS_HISTOGRAM.observe(self.deleted_versions as f64);
        }
    }
}

/// `ReleasedLock` contains the information of the lock released by `commit`, `rollback` and so on.
/// It's used by `LockManager` to wake up transactions waiting for locks.
#[derive(Debug, PartialEq)]
pub struct ReleasedLock {
    /// The hash value of the lock.
    pub hash: u64,
    /// Whether it is a pessimistic lock.
    pub pessimistic: bool,
}

impl ReleasedLock {
    fn new(key: &Key, pessimistic: bool) -> Self {
        Self {
            hash: key.gen_hash(),
            pessimistic,
        }
    }
}

/// An abstraction of a locally-transactional MVCC key-value store
pub struct MvccTxn {
    pub(crate) start_ts: TimeStamp,
    pub(crate) write_size: usize,
    pub(crate) modifies: Vec<Modify>,
    // When 1PC is enabled, locks will be collected here instead of marshalled and put into `writes`,
    // so it can be further processed. The elements are tuples representing
    // (key, lock, remove_pessimistic_lock)
    pub(crate) locks_for_1pc: Vec<(Key, Lock, bool)>,
    // `concurrency_manager` is used to set memory locks for prewritten keys.
    // Prewritten locks of async commit transactions should be visible to
    // readers before they are written to the engine.
    pub(crate) concurrency_manager: ConcurrencyManager,
    // After locks are stored in memory in prewrite, the KeyHandleGuard
    // needs to be stored here.
    // When the locks are written to the underlying engine, subsequent
    // reading requests should be able to read the locks from the engine.
    // So these guards can be released after finishing writing.
    pub(crate) guards: Vec<KeyHandleGuard>,
}

impl MvccTxn {
    pub fn new(start_ts: TimeStamp, concurrency_manager: ConcurrencyManager) -> MvccTxn {
        // FIXME: use session variable to indicate fill cache or not.

        MvccTxn {
            start_ts,
            write_size: 0,
            modifies: vec![],
            locks_for_1pc: Vec::new(),
            concurrency_manager,
            guards: vec![],
        }
    }

    pub fn into_modifies(self) -> Vec<Modify> {
        assert!(self.locks_for_1pc.is_empty());
        self.modifies
    }

    pub fn take_guards(&mut self) -> Vec<KeyHandleGuard> {
        std::mem::take(&mut self.guards)
    }

    pub fn write_size(&self) -> usize {
        self.write_size
    }

    pub(crate) fn put_lock(&mut self, key: Key, lock: &Lock) {
        let write = Modify::Put(CF_LOCK, key, lock.to_bytes());
        self.write_size += write.size();
        self.modifies.push(write);
    }

    pub(crate) fn put_locks_for_1pc(&mut self, key: Key, lock: Lock, remove_pessimstic_lock: bool) {
        self.locks_for_1pc.push((key, lock, remove_pessimstic_lock));
    }

    pub(crate) fn put_pessimistic_lock(&mut self, key: Key, lock: PessimisticLock) {
        self.modifies.push(Modify::PessimisticLock(key, lock))
    }

    pub(crate) fn unlock_key(&mut self, key: Key, pessimistic: bool) -> Option<ReleasedLock> {
        let released = ReleasedLock::new(&key, pessimistic);
        let write = Modify::Delete(CF_LOCK, key);
        self.write_size += write.size();
        self.modifies.push(write);
        Some(released)
    }

    pub(crate) fn put_value(&mut self, key: Key, ts: TimeStamp, value: Value) {
        let write = Modify::Put(CF_DEFAULT, key.append_ts(ts), value);
        self.write_size += write.size();
        self.modifies.push(write);
    }

    pub(crate) fn delete_value(&mut self, key: Key, ts: TimeStamp) {
        let write = Modify::Delete(CF_DEFAULT, key.append_ts(ts));
        self.write_size += write.size();
        self.modifies.push(write);
    }

    pub(crate) fn put_write(&mut self, key: Key, ts: TimeStamp, value: Value) {
        let write = Modify::Put(CF_WRITE, key.append_ts(ts), value);
        self.write_size += write.size();
        self.modifies.push(write);
    }

    pub(crate) fn delete_write(&mut self, key: Key, ts: TimeStamp) {
        let write = Modify::Delete(CF_WRITE, key.append_ts(ts));
        self.write_size += write.size();
        self.modifies.push(write);
    }

    /// Add the timestamp of the current rollback operation to another transaction's lock if
    /// necessary.
    ///
    /// When putting rollback record on a key that's locked by another transaction, the second
    /// transaction may overwrite the current rollback record when it's committed. Sometimes it may
    /// break consistency. To solve the problem, add the timestamp of the current rollback to the
    /// lock. So when the lock is committed, it can check if it will overwrite a rollback record
    /// by checking the information in the lock.
    pub(crate) fn mark_rollback_on_mismatching_lock(
        &mut self,
        key: &Key,
        mut lock: Lock,
        is_protected: bool,
    ) {
        assert_ne!(lock.ts, self.start_ts);

        if !is_protected {
            // A non-protected rollback record is ok to be overwritten, so do nothing in this case.
            return;
        }

        if self.start_ts < lock.min_commit_ts {
            // The rollback will surely not be overwritten by committing the lock. Do nothing.
            return;
        }

        if !lock.use_async_commit {
            // Currently only async commit may use calculated commit_ts. Do nothing if it's not a
            // async commit transaction.
            return;
        }

        lock.rollback_ts.push(self.start_ts);
        self.put_lock(key.clone(), &lock);
    }

    pub(crate) fn clear(&mut self) {
        self.write_size = 0;
        self.modifies.clear();
        self.locks_for_1pc.clear();
        self.guards.clear();
    }
}

impl fmt::Debug for MvccTxn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "txn @{}", self.start_ts)
    }
}

#[cfg(feature = "failpoints")]
pub(crate) fn make_txn_error(
    s: Option<String>,
    key: &Key,
    start_ts: TimeStamp,
) -> crate::storage::mvcc::ErrorInner {
    use crate::storage::mvcc::ErrorInner;
    if let Some(s) = s {
        match s.to_ascii_lowercase().as_str() {
            "keyislocked" => {
                let mut info = kvproto::kvrpcpb::LockInfo::default();
                info.set_key(key.to_raw().unwrap());
                info.set_primary_lock(key.to_raw().unwrap());
                info.set_lock_ttl(3000);
                ErrorInner::KeyIsLocked(info)
            }
            "committed" => ErrorInner::Committed {
                start_ts,
                commit_ts: start_ts.next(),
                key: key.to_raw().unwrap(),
            },
            "pessimisticlockrolledback" => ErrorInner::PessimisticLockRolledBack {
                start_ts,
                key: key.to_raw().unwrap(),
            },
            "txnlocknotfound" => ErrorInner::TxnLockNotFound {
                start_ts,
                commit_ts: TimeStamp::zero(),
                key: key.to_raw().unwrap(),
            },
            "txnnotfound" => ErrorInner::TxnNotFound {
                start_ts,
                key: key.to_raw().unwrap(),
            },
            "locktypenotmatch" => ErrorInner::LockTypeNotMatch {
                start_ts,
                key: key.to_raw().unwrap(),
                pessimistic: false,
            },
            "writeconflict" => ErrorInner::WriteConflict {
                start_ts,
                conflict_start_ts: TimeStamp::zero(),
                conflict_commit_ts: TimeStamp::zero(),
                key: key.to_raw().unwrap(),
                primary: vec![],
            },
            "deadlock" => ErrorInner::Deadlock {
                start_ts,
                lock_ts: TimeStamp::zero(),
                lock_key: key.to_raw().unwrap(),
                deadlock_key_hash: 0,
                wait_chain: vec![],
            },
            "alreadyexist" => ErrorInner::AlreadyExist {
                key: key.to_raw().unwrap(),
            },
            "committsexpired" => ErrorInner::CommitTsExpired {
                start_ts,
                commit_ts: TimeStamp::zero(),
                key: key.to_raw().unwrap(),
                min_commit_ts: TimeStamp::zero(),
            },
            "pessimisticlocknotfound" => ErrorInner::PessimisticLockNotFound {
                start_ts,
                key: key.to_raw().unwrap(),
            },
            _ => ErrorInner::Other(box_err!("unexpected error string")),
        }
    } else {
        ErrorInner::Other(box_err!("empty error string"))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    use crate::storage::kv::{RocksEngine, ScanMode, WriteData};
    use crate::storage::mvcc::tests::*;
    use crate::storage::mvcc::{Error, ErrorInner, Mutation, MvccReader, SnapshotReader};
    use crate::storage::txn::commands::*;
    use crate::storage::txn::tests::*;
    use crate::storage::txn::{
        commit, prewrite, CommitKind, TransactionKind, TransactionProperties,
    };
    use crate::storage::SecondaryLocksStatus;
    use crate::storage::{
        kv::{Engine, TestEngineBuilder},
        TxnStatus,
    };
    use kvproto::kvrpcpb::{AssertionLevel, Context};
    use txn_types::{TimeStamp, WriteType, SHORT_VALUE_MAX_LEN};

    fn test_mvcc_txn_read_imp(k1: &[u8], k2: &[u8], v: &[u8]) {
        let engine = TestEngineBuilder::new().build().unwrap();

        must_get_none(&engine, k1, 1);

        must_prewrite_put(&engine, k1, v, k1, 2);
        must_rollback(&engine, k1, 2, false);
        // should ignore rollback
        must_get_none(&engine, k1, 3);

        must_prewrite_lock(&engine, k1, k1, 3);
        must_commit(&engine, k1, 3, 4);
        // should ignore read lock
        must_get_none(&engine, k1, 5);

        must_prewrite_put(&engine, k1, v, k1, 5);
        must_prewrite_put(&engine, k2, v, k1, 5);
        // should not be affected by later locks
        must_get_none(&engine, k1, 4);
        // should read pending locks
        must_get_err(&engine, k1, 7);
        // should ignore the primary lock and get none when reading the latest record
        must_get_none(&engine, k1, u64::max_value());
        // should read secondary locks even when reading the latest record
        must_get_err(&engine, k2, u64::max_value());

        must_commit(&engine, k1, 5, 10);
        must_commit(&engine, k2, 5, 10);
        must_get_none(&engine, k1, 3);
        // should not read with ts < commit_ts
        must_get_none(&engine, k1, 7);
        // should read with ts > commit_ts
        must_get(&engine, k1, 13, v);
        // should read the latest record if `ts == u64::max_value()`
        must_get(&engine, k1, u64::max_value(), v);

        must_prewrite_delete(&engine, k1, k1, 15);
        // should ignore the lock and get previous record when reading the latest record
        must_get(&engine, k1, u64::max_value(), v);
        must_commit(&engine, k1, 15, 20);
        must_get_none(&engine, k1, 3);
        must_get_none(&engine, k1, 7);
        must_get(&engine, k1, 13, v);
        must_get(&engine, k1, 17, v);
        must_get_none(&engine, k1, 23);

        // intersecting timestamps with pessimistic txn
        // T1: start_ts = 25, commit_ts = 27
        // T2: start_ts = 23, commit_ts = 31
        must_prewrite_put(&engine, k1, v, k1, 25);
        must_commit(&engine, k1, 25, 27);
        must_acquire_pessimistic_lock(&engine, k1, k1, 23, 29);
        must_get(&engine, k1, 30, v);
        must_pessimistic_prewrite_delete(&engine, k1, k1, 23, 29, true);
        must_get_err(&engine, k1, 30);
        // should read the latest record when `ts == u64::max_value()`
        // even if lock.start_ts(23) < latest write.commit_ts(27)
        must_get(&engine, k1, u64::max_value(), v);
        must_commit(&engine, k1, 23, 31);
        must_get(&engine, k1, 30, v);
        must_get_none(&engine, k1, 32);
    }

    #[test]
    fn test_mvcc_txn_read() {
        test_mvcc_txn_read_imp(b"k1", b"k2", b"v1");

        let long_value = "v".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_mvcc_txn_read_imp(b"k1", b"k2", &long_value);
    }

    fn test_mvcc_txn_prewrite_imp(k: &[u8], v: &[u8]) {
        let engine = TestEngineBuilder::new().build().unwrap();

        must_prewrite_put(&engine, k, v, k, 5);
        // Key is locked.
        must_locked(&engine, k, 5);
        // Retry prewrite.
        must_prewrite_put(&engine, k, v, k, 5);
        // Conflict.
        must_prewrite_lock_err(&engine, k, k, 6);

        must_commit(&engine, k, 5, 10);
        must_written(&engine, k, 5, 10, WriteType::Put);
        // Delayed prewrite request after committing should do nothing.
        must_prewrite_put_err(&engine, k, v, k, 5);
        must_unlocked(&engine, k);
        // Write conflict.
        must_prewrite_lock_err(&engine, k, k, 6);
        must_unlocked(&engine, k);
        // Not conflict.
        must_prewrite_lock(&engine, k, k, 12);
        must_locked(&engine, k, 12);
        must_rollback(&engine, k, 12, false);
        must_unlocked(&engine, k);
        must_written(&engine, k, 12, 12, WriteType::Rollback);
        // Cannot retry Prewrite after rollback.
        must_prewrite_lock_err(&engine, k, k, 12);
        // Can prewrite after rollback.
        must_prewrite_delete(&engine, k, k, 13);
        must_rollback(&engine, k, 13, false);
        must_unlocked(&engine, k);
    }

    #[test]
    fn test_mvcc_txn_prewrite_insert() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let (k1, v1, v2, v3) = (b"k1", b"v1", b"v2", b"v3");
        must_prewrite_put(&engine, k1, v1, k1, 1);
        must_commit(&engine, k1, 1, 2);

        // "k1" already exist, returns AlreadyExist error.
        assert!(matches!(
            try_prewrite_insert(&engine, k1, v2, k1, 3),
            Err(Error(box ErrorInner::AlreadyExist { .. }))
        ));

        // Delete "k1"
        must_prewrite_delete(&engine, k1, k1, 4);

        // There is a lock, returns KeyIsLocked error.
        assert!(matches!(
            try_prewrite_insert(&engine, k1, v2, k1, 6),
            Err(Error(box ErrorInner::KeyIsLocked(_)))
        ));

        must_commit(&engine, k1, 4, 5);

        // After delete "k1", insert returns ok.
        assert!(try_prewrite_insert(&engine, k1, v2, k1, 6).is_ok());
        must_commit(&engine, k1, 6, 7);

        // Rollback
        must_prewrite_put(&engine, k1, v3, k1, 8);
        must_rollback(&engine, k1, 8, false);

        assert!(matches!(
            try_prewrite_insert(&engine, k1, v3, k1, 9),
            Err(Error(box ErrorInner::AlreadyExist { .. }))
        ));

        // Delete "k1" again
        must_prewrite_delete(&engine, k1, k1, 10);
        must_commit(&engine, k1, 10, 11);

        // Rollback again
        must_prewrite_put(&engine, k1, v3, k1, 12);
        must_rollback(&engine, k1, 12, false);

        // After delete "k1", insert returns ok.
        assert!(try_prewrite_insert(&engine, k1, v2, k1, 13).is_ok());
        must_commit(&engine, k1, 13, 14);
    }

    #[test]
    fn test_mvcc_txn_prewrite_check_not_exist() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let (k1, v1, v2, v3) = (b"k1", b"v1", b"v2", b"v3");
        must_prewrite_put(&engine, k1, v1, k1, 1);
        must_commit(&engine, k1, 1, 2);

        // "k1" already exist, returns AlreadyExist error.
        assert!(try_prewrite_check_not_exists(&engine, k1, k1, 3).is_err());

        // Delete "k1"
        must_prewrite_delete(&engine, k1, k1, 4);
        must_commit(&engine, k1, 4, 5);

        // After delete "k1", check_not_exists returns ok.
        assert!(try_prewrite_check_not_exists(&engine, k1, k1, 6).is_ok());

        assert!(try_prewrite_insert(&engine, k1, v2, k1, 7).is_ok());
        must_commit(&engine, k1, 7, 8);

        // Rollback
        must_prewrite_put(&engine, k1, v3, k1, 9);
        must_rollback(&engine, k1, 9, false);
        assert!(try_prewrite_check_not_exists(&engine, k1, k1, 10).is_err());

        // Delete "k1" again
        must_prewrite_delete(&engine, k1, k1, 11);
        must_commit(&engine, k1, 11, 12);

        // Rollback again
        must_prewrite_put(&engine, k1, v3, k1, 13);
        must_rollback(&engine, k1, 13, false);

        // After delete "k1", check_not_exists returns ok.
        assert!(try_prewrite_check_not_exists(&engine, k1, k1, 14).is_ok());
    }

    #[test]
    fn test_mvcc_txn_pessmistic_prewrite_check_not_exist() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let k = b"k1";
        assert!(try_pessimistic_prewrite_check_not_exists(&engine, k, k, 3).is_err())
    }

    #[test]
    fn test_rollback_lock_optimistic() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let (k, v) = (b"k1", b"v1");
        must_prewrite_put(&engine, k, v, k, 5);
        must_commit(&engine, k, 5, 10);

        // Lock
        must_prewrite_lock(&engine, k, k, 15);
        must_locked(&engine, k, 15);

        // Rollback lock
        must_rollback(&engine, k, 15, false);
        // Rollbacks of optimistic transactions needn't be protected
        must_get_rollback_protected(&engine, k, 15, false);
    }

    #[test]
    fn test_rollback_lock_pessimistic() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let (k1, k2, v) = (b"k1", b"k2", b"v1");

        must_acquire_pessimistic_lock(&engine, k1, k1, 5, 5);
        must_acquire_pessimistic_lock(&engine, k2, k1, 5, 7);
        must_rollback(&engine, k1, 5, false);
        must_rollback(&engine, k2, 5, false);
        // The rollback of the primary key should be protected
        must_get_rollback_protected(&engine, k1, 5, true);
        // The rollback of the secondary key needn't be protected
        must_get_rollback_protected(&engine, k2, 5, false);

        must_acquire_pessimistic_lock(&engine, k1, k1, 15, 15);
        must_acquire_pessimistic_lock(&engine, k2, k1, 15, 17);
        must_pessimistic_prewrite_put(&engine, k1, v, k1, 15, 17, true);
        must_pessimistic_prewrite_put(&engine, k2, v, k1, 15, 17, true);
        must_rollback(&engine, k1, 15, false);
        must_rollback(&engine, k2, 15, false);
        // The rollback of the primary key should be protected
        must_get_rollback_protected(&engine, k1, 15, true);
        // The rollback of the secondary key needn't be protected
        must_get_rollback_protected(&engine, k2, 15, false);
    }

    #[test]
    fn test_rollback_del() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let (k, v) = (b"k1", b"v1");
        must_prewrite_put(&engine, k, v, k, 5);
        must_commit(&engine, k, 5, 10);

        // Prewrite delete
        must_prewrite_delete(&engine, k, k, 15);
        must_locked(&engine, k, 15);

        // Rollback delete
        must_rollback(&engine, k, 15, false);
    }

    #[test]
    fn test_rollback_overlapped() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let (k1, v1) = (b"key1", b"v1");
        let (k2, v2) = (b"key2", b"v2");

        must_prewrite_put(&engine, k1, v1, k1, 10);
        must_prewrite_put(&engine, k2, v2, k2, 11);
        must_commit(&engine, k1, 10, 20);
        must_commit(&engine, k2, 11, 20);
        let w1 = must_written(&engine, k1, 10, 20, WriteType::Put);
        let w2 = must_written(&engine, k2, 11, 20, WriteType::Put);
        assert!(!w1.has_overlapped_rollback);
        assert!(!w2.has_overlapped_rollback);

        must_cleanup(&engine, k1, 20, 0);
        must_rollback(&engine, k2, 20, false);

        let w1r = must_written(&engine, k1, 10, 20, WriteType::Put);
        assert!(w1r.has_overlapped_rollback);
        // The only difference between w1r and w1 is the overlapped_rollback flag.
        assert_eq!(w1r.set_overlapped_rollback(false, None), w1);

        let w2r = must_written(&engine, k2, 11, 20, WriteType::Put);
        // Rollback is invoked on secondaries, so the rollback is not protected and overlapped_rollback
        // won't be set.
        assert_eq!(w2r, w2);
    }

    #[test]
    fn test_mvcc_txn_prewrite() {
        test_mvcc_txn_prewrite_imp(b"k1", b"v1");

        let long_value = "v".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_mvcc_txn_prewrite_imp(b"k2", &long_value);
    }

    #[test]
    fn test_mvcc_txn_rollback_after_commit() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let k = b"k";
        let v = b"v";
        let t1 = 1;
        let t2 = 10;
        let t3 = 20;
        let t4 = 30;

        must_prewrite_put(&engine, k, v, k, t1);

        must_rollback(&engine, k, t2, false);
        must_rollback(&engine, k, t2, false);
        must_rollback(&engine, k, t4, false);

        must_commit(&engine, k, t1, t3);
        // The rollback should be failed since the transaction
        // was committed before.
        must_rollback_err(&engine, k, t1);
        must_get(&engine, k, t4, v);
    }

    fn test_mvcc_txn_rollback_imp(k: &[u8], v: &[u8]) {
        let engine = TestEngineBuilder::new().build().unwrap();

        must_prewrite_put(&engine, k, v, k, 5);
        must_rollback(&engine, k, 5, false);
        // Rollback should be idempotent
        must_rollback(&engine, k, 5, false);
        // Lock should be released after rollback
        must_unlocked(&engine, k);
        must_prewrite_lock(&engine, k, k, 10);
        must_rollback(&engine, k, 10, false);
        // data should be dropped after rollback
        must_get_none(&engine, k, 20);

        // Can't rollback committed transaction.
        must_prewrite_put(&engine, k, v, k, 25);
        must_commit(&engine, k, 25, 30);
        must_rollback_err(&engine, k, 25);
        must_rollback_err(&engine, k, 25);

        // Can't rollback other transaction's lock
        must_prewrite_delete(&engine, k, k, 35);
        must_rollback(&engine, k, 34, true);
        must_rollback(&engine, k, 36, true);
        must_written(&engine, k, 34, 34, WriteType::Rollback);
        must_written(&engine, k, 36, 36, WriteType::Rollback);
        must_locked(&engine, k, 35);
        must_commit(&engine, k, 35, 40);
        must_get(&engine, k, 39, v);
        must_get_none(&engine, k, 41);
    }

    #[test]
    fn test_mvcc_txn_rollback() {
        test_mvcc_txn_rollback_imp(b"k", b"v");

        let long_value = "v".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_mvcc_txn_rollback_imp(b"k2", &long_value);
    }

    #[test]
    fn test_mvcc_txn_rollback_before_prewrite() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let key = b"key";
        must_rollback(&engine, key, 5, false);
        must_prewrite_lock_err(&engine, key, key, 5);
    }

    fn test_write_imp(k: &[u8], v: &[u8], k2: &[u8]) {
        let engine = TestEngineBuilder::new().build().unwrap();

        must_prewrite_put(&engine, k, v, k, 5);
        must_seek_write_none(&engine, k, 5);

        must_commit(&engine, k, 5, 10);
        must_seek_write(&engine, k, TimeStamp::max(), 5, 10, WriteType::Put);
        must_seek_write_none(&engine, k2, TimeStamp::max());
        must_get_commit_ts(&engine, k, 5, 10);

        must_prewrite_delete(&engine, k, k, 15);
        must_rollback(&engine, k, 15, false);
        must_seek_write(&engine, k, TimeStamp::max(), 15, 15, WriteType::Rollback);
        must_get_commit_ts(&engine, k, 5, 10);
        must_get_commit_ts_none(&engine, k, 15);

        must_prewrite_lock(&engine, k, k, 25);
        must_commit(&engine, k, 25, 30);
        must_seek_write(&engine, k, TimeStamp::max(), 25, 30, WriteType::Lock);
        must_get_commit_ts(&engine, k, 25, 30);
    }

    #[test]
    fn test_write() {
        test_write_imp(b"kk", b"v1", b"k");

        let v2 = "x".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_write_imp(b"kk", &v2, b"k");
    }

    fn test_scan_keys_imp(keys: Vec<&[u8]>, values: Vec<&[u8]>) {
        let engine = TestEngineBuilder::new().build().unwrap();
        must_prewrite_put(&engine, keys[0], values[0], keys[0], 1);
        must_commit(&engine, keys[0], 1, 10);
        must_prewrite_lock(&engine, keys[1], keys[1], 1);
        must_commit(&engine, keys[1], 1, 5);
        must_prewrite_delete(&engine, keys[2], keys[2], 1);
        must_commit(&engine, keys[2], 1, 20);
        must_prewrite_put(&engine, keys[3], values[1], keys[3], 1);
        must_prewrite_lock(&engine, keys[4], keys[4], 10);
        must_prewrite_delete(&engine, keys[5], keys[5], 5);

        must_scan_keys(&engine, None, 100, vec![keys[0], keys[1], keys[2]], None);
        must_scan_keys(&engine, None, 3, vec![keys[0], keys[1], keys[2]], None);
        must_scan_keys(&engine, None, 2, vec![keys[0], keys[1]], Some(keys[1]));
        must_scan_keys(&engine, Some(keys[1]), 1, vec![keys[1]], Some(keys[1]));
    }

    #[test]
    fn test_scan_keys() {
        test_scan_keys_imp(vec![b"a", b"c", b"e", b"b", b"d", b"f"], vec![b"a", b"b"]);

        let v1 = "x".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        let v4 = "v".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_scan_keys_imp(vec![b"a", b"c", b"e", b"b", b"d", b"f"], vec![&v1, &v4]);
    }

    pub fn txn_props(
        start_ts: TimeStamp,
        primary: &[u8],
        commit_kind: CommitKind,
        for_update_ts: Option<TimeStamp>,
        txn_size: u64,
        skip_constraint_check: bool,
    ) -> TransactionProperties<'_> {
        let kind = if let Some(ts) = for_update_ts {
            TransactionKind::Pessimistic(ts)
        } else {
            TransactionKind::Optimistic(skip_constraint_check)
        };

        TransactionProperties {
            start_ts,
            kind,
            commit_kind,
            primary,
            txn_size,
            lock_ttl: 0,
            min_commit_ts: TimeStamp::default(),
            need_old_value: false,
            is_retry_request: false,
            assertion_level: AssertionLevel::Off,
        }
    }

    fn test_write_size_imp(k: &[u8], v: &[u8], pk: &[u8]) {
        let engine = TestEngineBuilder::new().build().unwrap();
        let ctx = Context::default();
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let cm = ConcurrencyManager::new(10.into());
        let mut txn = MvccTxn::new(10.into(), cm.clone());
        let mut reader = SnapshotReader::new(10.into(), snapshot, true);
        let key = Key::from_raw(k);
        assert_eq!(txn.write_size(), 0);

        prewrite(
            &mut txn,
            &mut reader,
            &txn_props(10.into(), pk, CommitKind::TwoPc, None, 0, false),
            Mutation::make_put(key.clone(), v.to_vec()),
            &None,
            false,
        )
        .unwrap();
        assert!(txn.write_size() > 0);
        engine
            .write(&ctx, WriteData::from_modifies(txn.into_modifies()))
            .unwrap();

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut txn = MvccTxn::new(10.into(), cm);
        let mut reader = SnapshotReader::new(10.into(), snapshot, true);
        commit(&mut txn, &mut reader, key, 15.into()).unwrap();
        assert!(txn.write_size() > 0);
        engine
            .write(&ctx, WriteData::from_modifies(txn.into_modifies()))
            .unwrap();
    }

    #[test]
    fn test_write_size() {
        test_write_size_imp(b"key", b"value", b"pk");

        let v = "x".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_write_size_imp(b"key", &v, b"pk");
    }

    #[test]
    fn test_skip_constraint_check() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let (key, value) = (b"key", b"value");

        must_prewrite_put(&engine, key, value, key, 5);
        must_commit(&engine, key, 5, 10);

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let cm = ConcurrencyManager::new(10.into());
        let mut txn = MvccTxn::new(5.into(), cm.clone());
        let mut reader = SnapshotReader::new(5.into(), snapshot, true);
        assert!(
            prewrite(
                &mut txn,
                &mut reader,
                &txn_props(5.into(), key, CommitKind::TwoPc, None, 0, false),
                Mutation::make_put(Key::from_raw(key), value.to_vec()),
                &None,
                false,
            )
            .is_err()
        );

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut txn = MvccTxn::new(5.into(), cm);
        let mut reader = SnapshotReader::new(5.into(), snapshot, true);
        prewrite(
            &mut txn,
            &mut reader,
            &txn_props(5.into(), key, CommitKind::TwoPc, None, 0, true),
            Mutation::make_put(Key::from_raw(key), value.to_vec()),
            &None,
            false,
        )
        .unwrap();
    }

    #[test]
    fn test_read_commit() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let (key, v1, v2) = (b"key", b"v1", b"v2");

        must_prewrite_put(&engine, key, v1, key, 5);
        must_commit(&engine, key, 5, 10);
        must_prewrite_put(&engine, key, v2, key, 15);
        must_get_err(&engine, key, 20);
        must_get_no_lock_check(&engine, key, 12, v1);
        must_get_no_lock_check(&engine, key, 20, v1);
    }

    #[test]
    fn test_collapse_prev_rollback() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let (key, value) = (b"key", b"value");

        // Add a Rollback whose start ts is 1.
        must_prewrite_put(&engine, key, value, key, 1);
        must_rollback(&engine, key, 1, false);
        must_get_rollback_ts(&engine, key, 1);

        // Add a Rollback whose start ts is 2, the previous Rollback whose
        // start ts is 1 will be collapsed.
        must_prewrite_put(&engine, key, value, key, 2);
        must_rollback(&engine, key, 2, false);
        must_get_none(&engine, key, 2);
        must_get_rollback_ts(&engine, key, 2);
        must_get_rollback_ts_none(&engine, key, 1);

        // Rollback arrive before Prewrite, it will collapse the
        // previous rollback whose start ts is 2.
        must_rollback(&engine, key, 3, false);
        must_get_none(&engine, key, 3);
        must_get_rollback_ts(&engine, key, 3);
        must_get_rollback_ts_none(&engine, key, 2);
    }

    #[test]
    fn test_scan_values_in_default() {
        let engine = TestEngineBuilder::new().build().unwrap();

        must_prewrite_put(
            &engine,
            &[2],
            "v".repeat(SHORT_VALUE_MAX_LEN + 1).as_bytes(),
            &[2],
            3,
        );
        must_commit(&engine, &[2], 3, 3);

        must_prewrite_put(
            &engine,
            &[3],
            "a".repeat(SHORT_VALUE_MAX_LEN + 1).as_bytes(),
            &[3],
            3,
        );
        must_commit(&engine, &[3], 3, 4);

        must_prewrite_put(
            &engine,
            &[3],
            "b".repeat(SHORT_VALUE_MAX_LEN + 1).as_bytes(),
            &[3],
            5,
        );
        must_commit(&engine, &[3], 5, 5);

        must_prewrite_put(
            &engine,
            &[6],
            "x".repeat(SHORT_VALUE_MAX_LEN + 1).as_bytes(),
            &[6],
            3,
        );
        must_commit(&engine, &[6], 3, 6);

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, Some(ScanMode::Forward), true);

        let v = reader.scan_values_in_default(&Key::from_raw(&[3])).unwrap();
        assert_eq!(v.len(), 2);
        assert_eq!(
            v[1],
            (3.into(), "a".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes())
        );
        assert_eq!(
            v[0],
            (5.into(), "b".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes())
        );
    }

    #[test]
    fn test_seek_ts() {
        let engine = TestEngineBuilder::new().build().unwrap();

        must_prewrite_put(&engine, &[2], b"vv", &[2], 3);
        must_commit(&engine, &[2], 3, 3);

        must_prewrite_put(
            &engine,
            &[3],
            "a".repeat(SHORT_VALUE_MAX_LEN + 1).as_bytes(),
            &[3],
            4,
        );
        must_commit(&engine, &[3], 4, 4);

        must_prewrite_put(
            &engine,
            &[5],
            "b".repeat(SHORT_VALUE_MAX_LEN + 1).as_bytes(),
            &[5],
            2,
        );
        must_commit(&engine, &[5], 2, 5);

        must_prewrite_put(&engine, &[6], b"xxx", &[6], 3);
        must_commit(&engine, &[6], 3, 6);

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, Some(ScanMode::Forward), true);

        assert_eq!(
            reader.seek_ts(3.into()).unwrap().unwrap(),
            Key::from_raw(&[2])
        );
    }

    #[test]
    fn test_pessimistic_txn_ttl() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let (k, v) = (b"k", b"v");

        // Pessimistic prewrite keeps the larger TTL of the prewrite request and the original
        // pessimisitic lock.
        must_acquire_pessimistic_lock_with_ttl(&engine, k, k, 10, 10, 100);
        must_pessimistic_locked(&engine, k, 10, 10);
        must_pessimistic_prewrite_put_with_ttl(&engine, k, v, k, 10, 10, true, 110);
        must_locked_with_ttl(&engine, k, 10, 110);

        must_rollback(&engine, k, 10, false);

        // TTL not changed if the pessimistic lock's TTL is larger than that provided in the
        // prewrite request.
        must_acquire_pessimistic_lock_with_ttl(&engine, k, k, 20, 20, 100);
        must_pessimistic_locked(&engine, k, 20, 20);
        must_pessimistic_prewrite_put_with_ttl(&engine, k, v, k, 20, 20, true, 90);
        must_locked_with_ttl(&engine, k, 20, 100);
    }

    #[test]
    fn test_constraint_check_with_overlapping_txn() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let k = b"k1";
        let v = b"v1";

        must_prewrite_put(&engine, k, v, k, 10);
        must_commit(&engine, k, 10, 11);
        must_acquire_pessimistic_lock(&engine, k, k, 5, 12);
        must_pessimistic_prewrite_lock(&engine, k, k, 5, 12, true);
        must_commit(&engine, k, 5, 15);

        // Now in write cf:
        // start_ts = 10, commit_ts = 11, Put("v1")
        // start_ts = 5,  commit_ts = 15, Lock

        must_get(&engine, k, 19, v);
        assert!(try_prewrite_insert(&engine, k, v, k, 20).is_err());
    }

    #[test]
    fn test_lock_info_validation() {
        use kvproto::kvrpcpb::{LockInfo, Op};

        let engine = TestEngineBuilder::new().build().unwrap();
        let k = b"k";
        let v = b"v";

        let assert_lock_info_eq = |e, expected_lock_info: &LockInfo| match e {
            Error(box ErrorInner::KeyIsLocked(info)) => assert_eq!(info, *expected_lock_info),
            _ => panic!("unexpected error"),
        };

        for is_optimistic in &[false, true] {
            let mut expected_lock_info = LockInfo::default();
            expected_lock_info.set_primary_lock(k.to_vec());
            expected_lock_info.set_lock_version(10);
            expected_lock_info.set_key(k.to_vec());
            expected_lock_info.set_lock_ttl(3);
            if *is_optimistic {
                expected_lock_info.set_txn_size(10);
                expected_lock_info.set_lock_type(Op::Put);
                // Write an optimistic lock.
                must_prewrite_put_impl(
                    &engine,
                    expected_lock_info.get_key(),
                    v,
                    expected_lock_info.get_primary_lock(),
                    &None,
                    expected_lock_info.get_lock_version().into(),
                    false,
                    expected_lock_info.get_lock_ttl(),
                    TimeStamp::zero(),
                    expected_lock_info.get_txn_size(),
                    TimeStamp::zero(),
                    TimeStamp::zero(),
                    false,
                    kvproto::kvrpcpb::Assertion::None,
                    kvproto::kvrpcpb::AssertionLevel::Off,
                );
            } else {
                expected_lock_info.set_lock_type(Op::PessimisticLock);
                expected_lock_info.set_lock_for_update_ts(10);
                // Write a pessimistic lock.
                must_acquire_pessimistic_lock_impl(
                    &engine,
                    expected_lock_info.get_key(),
                    expected_lock_info.get_primary_lock(),
                    expected_lock_info.get_lock_version(),
                    false,
                    expected_lock_info.get_lock_ttl(),
                    expected_lock_info.get_lock_for_update_ts(),
                    false,
                    false,
                    TimeStamp::zero(),
                );
            }

            assert_lock_info_eq(
                must_prewrite_put_err(&engine, k, v, k, 20),
                &expected_lock_info,
            );

            assert_lock_info_eq(
                must_acquire_pessimistic_lock_err(&engine, k, k, 30, 30),
                &expected_lock_info,
            );

            // If the lock is not expired, cleanup will return the lock info.
            assert_lock_info_eq(must_cleanup_err(&engine, k, 10, 1), &expected_lock_info);

            expected_lock_info.set_lock_ttl(0);
            assert_lock_info_eq(
                must_pessimistic_prewrite_put_err(&engine, k, v, k, 40, 40, false),
                &expected_lock_info,
            );

            // Delete the lock
            if *is_optimistic {
                must_rollback(&engine, k, expected_lock_info.get_lock_version(), false);
            } else {
                pessimistic_rollback::tests::must_success(
                    &engine,
                    k,
                    expected_lock_info.get_lock_version(),
                    expected_lock_info.get_lock_for_update_ts(),
                );
            }
        }
    }

    #[test]
    fn test_non_pessimistic_lock_conflict_with_optimistic_txn() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let k = b"k1";
        let v = b"v1";

        must_prewrite_put(&engine, k, v, k, 2);
        must_locked(&engine, k, 2);
        must_pessimistic_prewrite_put_err(&engine, k, v, k, 1, 1, false);
        must_pessimistic_prewrite_put_err(&engine, k, v, k, 3, 3, false);
    }

    #[test]
    fn test_non_pessimistic_lock_conflict_with_pessismitic_txn() {
        let engine = TestEngineBuilder::new().build().unwrap();

        // k1 is a row key, k2 is the corresponding index key.
        let (k1, v1) = (b"k1", b"v1");
        let (k2, v2) = (b"k2", b"v2");
        let (k3, v3) = (b"k3", b"v3");

        // Commit k3 at 20.
        must_prewrite_put(&engine, k3, v3, k3, 1);
        must_commit(&engine, k3, 1, 20);

        // Txn-10 acquires pessimistic locks on k1 and k3.
        must_acquire_pessimistic_lock(&engine, k1, k1, 10, 10);
        must_acquire_pessimistic_lock_err(&engine, k3, k1, 10, 10);
        // Update for_update_ts to 20 due to write conflict
        must_acquire_pessimistic_lock(&engine, k3, k1, 10, 20);
        must_pessimistic_prewrite_put(&engine, k1, v1, k1, 10, 20, true);
        must_pessimistic_prewrite_put(&engine, k3, v3, k1, 10, 20, true);
        // Write a non-pessimistic lock with for_update_ts 20.
        must_pessimistic_prewrite_put(&engine, k2, v2, k1, 10, 20, false);
        // Roll back the primary key due to timeout, but the non-pessimistic lock is not rolled
        // back.
        must_rollback(&engine, k1, 10, false);

        // Txn-15 acquires pessimistic locks on k1.
        must_acquire_pessimistic_lock(&engine, k1, k1, 15, 15);
        must_pessimistic_prewrite_put(&engine, k1, v1, k1, 15, 15, true);
        // There is a non-pessimistic lock conflict here.
        match must_pessimistic_prewrite_put_err(&engine, k2, v2, k1, 15, 15, false) {
            Error(box ErrorInner::KeyIsLocked(info)) => assert_eq!(info.get_lock_ttl(), 0),
            e => panic!("unexpected error: {}", e),
        };
    }

    #[test]
    fn test_commit_pessimistic_lock() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let k = b"k";
        must_acquire_pessimistic_lock(&engine, k, k, 10, 10);
        must_commit_err(&engine, k, 20, 30);
        must_commit(&engine, k, 10, 20);
        must_seek_write(&engine, k, 30, 10, 20, WriteType::Lock);
    }

    #[test]
    fn test_amend_pessimistic_lock() {
        fn fail_to_write_pessimistic_lock<E: Engine>(
            engine: &E,
            key: &[u8],
            start_ts: impl Into<TimeStamp>,
            for_update_ts: impl Into<TimeStamp>,
        ) {
            let start_ts = start_ts.into();
            let for_update_ts = for_update_ts.into();
            must_acquire_pessimistic_lock(engine, key, key, start_ts, for_update_ts);
            // Delete the pessimistic lock to pretend write failure.
            pessimistic_rollback::tests::must_success(engine, key, start_ts, for_update_ts);
        }

        let engine = TestEngineBuilder::new().build().unwrap();
        let (k, mut v) = (b"k", b"v".to_vec());

        // Key not exist; should succeed.
        fail_to_write_pessimistic_lock(&engine, k, 10, 10);
        must_pessimistic_prewrite_put(&engine, k, &v, k, 10, 10, true);
        must_commit(&engine, k, 10, 20);
        must_get(&engine, k, 20, &v);

        // for_update_ts(30) >= start_ts(30) > commit_ts(20); should succeed.
        v.push(0);
        fail_to_write_pessimistic_lock(&engine, k, 30, 30);
        must_pessimistic_prewrite_put(&engine, k, &v, k, 30, 30, true);
        must_commit(&engine, k, 30, 40);
        must_get(&engine, k, 40, &v);

        // for_update_ts(40) >= commit_ts(40) > start_ts(35); should fail.
        fail_to_write_pessimistic_lock(&engine, k, 35, 40);
        must_pessimistic_prewrite_put_err(&engine, k, &v, k, 35, 40, true);

        // KeyIsLocked; should fail.
        must_acquire_pessimistic_lock(&engine, k, k, 50, 50);
        must_pessimistic_prewrite_put_err(&engine, k, &v, k, 60, 60, true);
        pessimistic_rollback::tests::must_success(&engine, k, 50, 50);

        // The txn has been rolled back; should fail.
        must_acquire_pessimistic_lock(&engine, k, k, 80, 80);
        must_cleanup(&engine, k, 80, TimeStamp::max());
        must_pessimistic_prewrite_put_err(&engine, k, &v, k, 80, 80, true);
    }

    #[test]
    fn test_async_prewrite_primary() {
        // copy must_prewrite_put_impl, check that the key is written with the correct secondaries and the right timestamp

        let engine = TestEngineBuilder::new().build().unwrap();
        let ctx = Context::default();
        let cm = ConcurrencyManager::new(42.into());

        let do_prewrite = || {
            let snapshot = engine.snapshot(Default::default()).unwrap();
            let mut txn = MvccTxn::new(TimeStamp::new(2), cm.clone());
            let mut reader = SnapshotReader::new(TimeStamp::new(2), snapshot, true);
            let mutation = Mutation::make_put(Key::from_raw(b"key"), b"value".to_vec());
            let (min_commit_ts, _) = prewrite(
                &mut txn,
                &mut reader,
                &txn_props(
                    TimeStamp::new(2),
                    b"key",
                    CommitKind::Async(TimeStamp::zero()),
                    None,
                    0,
                    false,
                ),
                mutation,
                &Some(vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()]),
                false,
            )
            .unwrap();
            let modifies = txn.into_modifies();
            if !modifies.is_empty() {
                engine
                    .write(&ctx, WriteData::from_modifies(modifies))
                    .unwrap();
            }
            min_commit_ts
        };

        assert_eq!(do_prewrite(), 43.into());

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true);
        let lock = reader.load_lock(&Key::from_raw(b"key")).unwrap().unwrap();
        assert_eq!(lock.ts, TimeStamp::new(2));
        assert_eq!(lock.use_async_commit, true);
        assert_eq!(
            lock.secondaries,
            vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()]
        );

        // max_ts in the concurrency manager is 42, so the min_commit_ts is 43.
        assert_eq!(lock.min_commit_ts, TimeStamp::new(43));

        // A duplicate prewrite request should return the min_commit_ts in the primary key
        assert_eq!(do_prewrite(), 43.into());
    }

    #[test]
    fn test_async_pessimistic_prewrite_primary() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let ctx = Context::default();
        let cm = ConcurrencyManager::new(42.into());

        must_acquire_pessimistic_lock(&engine, b"key", b"key", 2, 2);

        let do_pessimistic_prewrite = || {
            let snapshot = engine.snapshot(Default::default()).unwrap();
            let mut txn = MvccTxn::new(TimeStamp::new(2), cm.clone());
            let mut reader = SnapshotReader::new(TimeStamp::new(2), snapshot, true);
            let mutation = Mutation::make_put(Key::from_raw(b"key"), b"value".to_vec());
            let (min_commit_ts, _) = prewrite(
                &mut txn,
                &mut reader,
                &txn_props(
                    TimeStamp::new(2),
                    b"key",
                    CommitKind::Async(TimeStamp::zero()),
                    Some(4.into()),
                    4,
                    false,
                ),
                mutation,
                &Some(vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()]),
                true,
            )
            .unwrap();
            let modifies = txn.into_modifies();
            if !modifies.is_empty() {
                engine
                    .write(&ctx, WriteData::from_modifies(modifies))
                    .unwrap();
            }
            min_commit_ts
        };

        assert_eq!(do_pessimistic_prewrite(), 43.into());

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true);
        let lock = reader.load_lock(&Key::from_raw(b"key")).unwrap().unwrap();
        assert_eq!(lock.ts, TimeStamp::new(2));
        assert_eq!(lock.use_async_commit, true);
        assert_eq!(
            lock.secondaries,
            vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()]
        );

        // max_ts in the concurrency manager is 42, so the min_commit_ts is 43.
        assert_eq!(lock.min_commit_ts, TimeStamp::new(43));

        // A duplicate prewrite request should return the min_commit_ts in the primary key
        assert_eq!(do_pessimistic_prewrite(), 43.into());
    }

    #[test]
    fn test_async_commit_pushed_min_commit_ts() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new(42.into());

        // Simulate that min_commit_ts is pushed forward larger than latest_ts
        must_acquire_pessimistic_lock_impl(
            &engine, b"key", b"key", 2, false, 20000, 2, false, false, 100,
        );

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut txn = MvccTxn::new(TimeStamp::new(2), cm);
        let mut reader = SnapshotReader::new(TimeStamp::new(2), snapshot, true);
        let mutation = Mutation::make_put(Key::from_raw(b"key"), b"value".to_vec());
        let (min_commit_ts, _) = prewrite(
            &mut txn,
            &mut reader,
            &txn_props(
                TimeStamp::new(2),
                b"key",
                CommitKind::Async(TimeStamp::zero()),
                Some(4.into()),
                4,
                false,
            ),
            mutation,
            &Some(vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()]),
            true,
        )
        .unwrap();
        assert_eq!(min_commit_ts.into_inner(), 100);
    }

    #[test]
    fn test_txn_timestamp_overlapping() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let (k, v) = (b"k1", b"v1");

        // Prepare a committed transaction.
        must_prewrite_put(&engine, k, v, k, 10);
        must_locked(&engine, k, 10);
        must_commit(&engine, k, 10, 20);
        must_unlocked(&engine, k);
        must_written(&engine, k, 10, 20, WriteType::Put);

        // Optimistic transaction allows the start_ts equals to another transaction's commit_ts
        // on the same key.
        must_prewrite_put(&engine, k, v, k, 20);
        must_locked(&engine, k, 20);
        must_commit(&engine, k, 20, 30);
        must_unlocked(&engine, k);

        // ...but it can be rejected by overlapped rollback flag.
        must_cleanup(&engine, k, 30, 0);
        let w = must_written(&engine, k, 20, 30, WriteType::Put);
        assert!(w.has_overlapped_rollback);
        must_unlocked(&engine, k);
        must_prewrite_put_err(&engine, k, v, k, 30);
        must_unlocked(&engine, k);

        // Prepare a committed transaction.
        must_prewrite_put(&engine, k, v, k, 40);
        must_locked(&engine, k, 40);
        must_commit(&engine, k, 40, 50);
        must_unlocked(&engine, k);
        must_written(&engine, k, 40, 50, WriteType::Put);

        // Pessimistic transaction also works in the same case.
        must_acquire_pessimistic_lock(&engine, k, k, 50, 50);
        must_pessimistic_locked(&engine, k, 50, 50);
        must_pessimistic_prewrite_put(&engine, k, v, k, 50, 50, true);
        must_commit(&engine, k, 50, 60);
        must_unlocked(&engine, k);
        must_written(&engine, k, 50, 60, WriteType::Put);

        // .. and it can also be rejected by overlapped rollback flag.
        must_cleanup(&engine, k, 60, 0);
        let w = must_written(&engine, k, 50, 60, WriteType::Put);
        assert!(w.has_overlapped_rollback);
        must_unlocked(&engine, k);
        must_acquire_pessimistic_lock_err(&engine, k, k, 60, 60);
        must_unlocked(&engine, k);
    }

    #[test]
    fn test_rollback_while_other_transaction_running() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let (k, v) = (b"k1", b"v1");

        must_prewrite_put_async_commit(&engine, k, v, k, &Some(vec![]), 10, 0);
        must_cleanup(&engine, k, 15, 0);
        must_commit(&engine, k, 10, 15);
        let w = must_written(&engine, k, 10, 15, WriteType::Put);
        assert!(w.has_overlapped_rollback);
        // GC fence shouldn't be set in this case.
        assert!(w.gc_fence.is_none());

        must_prewrite_put_async_commit(&engine, k, v, k, &Some(vec![]), 20, 0);
        check_txn_status::tests::must_success(&engine, k, 25, 0, 0, true, false, false, |s| {
            s == TxnStatus::LockNotExist
        });
        must_commit(&engine, k, 20, 25);
        let w = must_written(&engine, k, 20, 25, WriteType::Put);
        assert!(w.has_overlapped_rollback);
        assert!(w.gc_fence.is_none());

        must_prewrite_put_async_commit(&engine, k, v, k, &Some(vec![]), 30, 0);
        check_secondary_locks::tests::must_success(
            &engine,
            k,
            35,
            SecondaryLocksStatus::RolledBack,
        );
        must_commit(&engine, k, 30, 35);
        let w = must_written(&engine, k, 30, 35, WriteType::Put);
        assert!(w.has_overlapped_rollback);
        assert!(w.gc_fence.is_none());

        // Do not commit with overlapped_rollback if the rollback ts doesn't equal to commit_ts.
        must_prewrite_put_async_commit(&engine, k, v, k, &Some(vec![]), 40, 0);
        must_cleanup(&engine, k, 44, 0);
        must_commit(&engine, k, 40, 45);
        let w = must_written(&engine, k, 40, 45, WriteType::Put);
        assert!(!w.has_overlapped_rollback);

        // Do not put rollback mark to the lock if the lock is not async commit or if lock.ts is
        // before start_ts or min_commit_ts.
        must_prewrite_put(&engine, k, v, k, 50);
        must_cleanup(&engine, k, 55, 0);
        let l = must_locked(&engine, k, 50);
        assert!(l.rollback_ts.is_empty());
        must_commit(&engine, k, 50, 56);

        must_prewrite_put_async_commit(&engine, k, v, k, &Some(vec![]), 60, 0);
        must_cleanup(&engine, k, 59, 0);
        let l = must_locked(&engine, k, 60);
        assert!(l.rollback_ts.is_empty());
        must_commit(&engine, k, 60, 65);

        must_prewrite_put_async_commit(&engine, k, v, k, &Some(vec![]), 70, 75);
        must_cleanup(&engine, k, 74, 0);
        must_cleanup(&engine, k, 75, 0);
        let l = must_locked(&engine, k, 70);
        assert_eq!(l.min_commit_ts, 75.into());
        assert_eq!(l.rollback_ts, vec![75.into()]);
    }

    #[test]
    fn test_gc_fence() {
        let rollback = |engine: &RocksEngine, k: &[u8], start_ts: u64| {
            must_cleanup(engine, k, start_ts, 0);
        };
        let check_status = |engine: &RocksEngine, k: &[u8], start_ts: u64| {
            check_txn_status::tests::must_success(
                engine,
                k,
                start_ts,
                0,
                0,
                true,
                false,
                false,
                |_| true,
            );
        };
        let check_secondary = |engine: &RocksEngine, k: &[u8], start_ts: u64| {
            check_secondary_locks::tests::must_success(
                engine,
                k,
                start_ts,
                SecondaryLocksStatus::RolledBack,
            );
        };

        for &rollback in &[rollback, check_status, check_secondary] {
            let engine = TestEngineBuilder::new().build().unwrap();

            // Get gc fence without any newer versions.
            must_prewrite_put(&engine, b"k1", b"v1", b"k1", 101);
            must_commit(&engine, b"k1", 101, 102);
            rollback(&engine, b"k1", 102);
            must_get_overlapped_rollback(&engine, b"k1", 102, 101, WriteType::Put, Some(0));

            // Get gc fence with a newer put.
            must_prewrite_put(&engine, b"k1", b"v1", b"k1", 103);
            must_commit(&engine, b"k1", 103, 104);
            must_prewrite_put(&engine, b"k1", b"v1", b"k1", 105);
            must_commit(&engine, b"k1", 105, 106);
            rollback(&engine, b"k1", 104);
            must_get_overlapped_rollback(&engine, b"k1", 104, 103, WriteType::Put, Some(106));

            // Get gc fence with a newer delete.
            must_prewrite_put(&engine, b"k1", b"v1", b"k1", 107);
            must_commit(&engine, b"k1", 107, 108);
            must_prewrite_delete(&engine, b"k1", b"k1", 109);
            must_commit(&engine, b"k1", 109, 110);
            rollback(&engine, b"k1", 108);
            must_get_overlapped_rollback(&engine, b"k1", 108, 107, WriteType::Put, Some(110));

            // Get gc fence with a newer rollback and lock.
            must_prewrite_put(&engine, b"k1", b"v1", b"k1", 111);
            must_commit(&engine, b"k1", 111, 112);
            must_prewrite_put(&engine, b"k1", b"v1", b"k1", 113);
            must_rollback(&engine, b"k1", 113, false);
            must_prewrite_lock(&engine, b"k1", b"k1", 115);
            must_commit(&engine, b"k1", 115, 116);
            rollback(&engine, b"k1", 112);
            must_get_overlapped_rollback(&engine, b"k1", 112, 111, WriteType::Put, Some(0));

            // Get gc fence with a newer put after some rollbacks and locks.
            must_prewrite_put(&engine, b"k1", b"v1", b"k1", 121);
            must_commit(&engine, b"k1", 121, 122);
            must_prewrite_put(&engine, b"k1", b"v1", b"k1", 123);
            must_rollback(&engine, b"k1", 123, false);
            must_prewrite_lock(&engine, b"k1", b"k1", 125);
            must_commit(&engine, b"k1", 125, 126);
            must_prewrite_put(&engine, b"k1", b"v1", b"k1", 127);
            must_commit(&engine, b"k1", 127, 128);
            rollback(&engine, b"k1", 122);
            must_get_overlapped_rollback(&engine, b"k1", 122, 121, WriteType::Put, Some(128));

            // A key's gc fence won't be another MVCC key.
            must_prewrite_put(&engine, b"k1", b"v1", b"k1", 131);
            must_commit(&engine, b"k1", 131, 132);
            must_prewrite_put(&engine, b"k0", b"v1", b"k0", 133);
            must_commit(&engine, b"k0", 133, 134);
            must_prewrite_put(&engine, b"k2", b"v1", b"k2", 133);
            must_commit(&engine, b"k2", 133, 134);
            rollback(&engine, b"k1", 132);
            must_get_overlapped_rollback(&engine, b"k1", 132, 131, WriteType::Put, Some(0));
        }
    }

    #[test]
    fn test_overlapped_ts_commit_before_rollback() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let (k1, v1) = (b"key1", b"v1");
        let (k2, v2) = (b"key2", b"v2");
        let key2 = k2.to_vec();
        let secondaries = Some(vec![key2]);

        // T1, start_ts = 10, commit_ts = 20; write k1, k2
        must_prewrite_put_async_commit(&engine, k1, v1, k1, &secondaries, 10, 0);
        must_prewrite_put_async_commit(&engine, k2, v2, k1, &secondaries, 10, 0);
        must_commit(&engine, k1, 10, 20);
        must_commit(&engine, k2, 10, 20);

        let w = must_written(&engine, k1, 10, 20, WriteType::Put);
        assert!(!w.has_overlapped_rollback);

        // T2, start_ts = 20
        must_acquire_pessimistic_lock(&engine, k2, k2, 20, 25);
        must_pessimistic_prewrite_put(&engine, k2, v2, k2, 20, 25, true);

        must_cleanup(&engine, k2, 20, 0);

        let w = must_written(&engine, k2, 10, 20, WriteType::Put);
        assert!(w.has_overlapped_rollback);
        must_get(&engine, k2, 30, v2);
        must_acquire_pessimistic_lock_err(&engine, k2, k2, 20, 25);
    }

    #[test]
    fn test_overlapped_ts_prewrite_before_rollback() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let (k1, v1) = (b"key1", b"v1");
        let (k2, v2) = (b"key2", b"v2");
        let key2 = k2.to_vec();
        let secondaries = Some(vec![key2]);

        // T1, start_ts = 10
        must_prewrite_put_async_commit(&engine, k1, v1, k1, &secondaries, 10, 0);
        must_prewrite_put_async_commit(&engine, k2, v2, k1, &secondaries, 10, 0);

        // T2, start_ts = 20
        must_prewrite_put_err(&engine, k2, v2, k2, 20);
        must_cleanup(&engine, k2, 20, 0);

        // commit T1
        must_commit(&engine, k1, 10, 20);
        must_commit(&engine, k2, 10, 20);

        let w = must_written(&engine, k2, 10, 20, WriteType::Put);
        assert!(w.has_overlapped_rollback);
        must_prewrite_put_err(&engine, k2, v2, k2, 20);
    }
}
