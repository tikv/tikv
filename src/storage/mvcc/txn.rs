// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use super::lock::{Lock, LockType};
use super::metrics::*;
use super::reader::MvccReader;
use super::write::{Write, WriteType};
use super::{Error, Result};
use crate::storage::kv::{Modify, ScanMode, Snapshot};
use crate::storage::{
    is_short_value, Key, Mutation, Options, Statistics, Value, CF_DEFAULT, CF_LOCK, CF_WRITE,
};
use kvproto::kvrpcpb::IsolationLevel;
use std::fmt;

pub const MAX_TXN_WRITE_SIZE: usize = 32 * 1024;

pub struct GcInfo {
    pub found_versions: usize,
    pub deleted_versions: usize,
    pub is_completed: bool,
}

pub struct MvccTxn<S: Snapshot> {
    reader: MvccReader<S>,
    gc_reader: MvccReader<S>,
    start_ts: u64,
    writes: Vec<Modify>,
    write_size: usize,
    // collapse continuous rollbacks.
    collapse_rollback: bool,
}

impl<S: Snapshot> fmt::Debug for MvccTxn<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "txn @{}", self.start_ts)
    }
}

impl<S: Snapshot> MvccTxn<S> {
    pub fn new(snapshot: S, start_ts: u64, fill_cache: bool) -> Result<Self> {
        Ok(Self {
            // Todo: use session variable to indicate fill cache or not
            // ScanMode is `None`, since in prewrite and other operations, keys are not given in
            // order and we use prefix seek for each key. An exception is GC, which uses forward
            // scan only.
            // IsolationLevel is `SI`, actually the method we use in MvccTxn does not rely on
            // isolation level, so it can be any value.
            reader: MvccReader::new(
                snapshot.clone(),
                None,
                fill_cache,
                None,
                None,
                IsolationLevel::SI,
            ),
            gc_reader: MvccReader::new(
                snapshot,
                Some(ScanMode::Forward),
                fill_cache,
                None,
                None,
                IsolationLevel::SI,
            ),
            start_ts,
            writes: vec![],
            write_size: 0,
            collapse_rollback: true,
        })
    }

    pub fn collapse_rollback(&mut self, collapse: bool) {
        self.collapse_rollback = collapse;
    }

    pub fn into_modifies(self) -> Vec<Modify> {
        self.writes
    }

    pub fn take_statistics(&mut self) -> Statistics {
        let mut statistics = Statistics::default();
        self.reader.collect_statistics_into(&mut statistics);
        statistics
    }

    pub fn write_size(&self) -> usize {
        self.write_size
    }

    fn lock_key(
        &mut self,
        key: Key,
        lock_type: LockType,
        primary: Vec<u8>,
        ttl: u64,
        short_value: Option<Value>,
    ) {
        let lock = Lock::new(lock_type, primary, self.start_ts, ttl, short_value).to_bytes();
        self.write_size += CF_LOCK.len() + key.as_encoded().len() + lock.len();
        self.writes.push(Modify::Put(CF_LOCK, key, lock));
    }

    fn unlock_key(&mut self, key: Key) {
        self.write_size += CF_LOCK.len() + key.as_encoded().len();
        self.writes.push(Modify::Delete(CF_LOCK, key));
    }

    fn put_value(&mut self, key: Key, ts: u64, value: Value) {
        let key = key.append_ts(ts);
        self.write_size += key.as_encoded().len() + value.len();
        self.writes.push(Modify::Put(CF_DEFAULT, key, value));
    }

    fn delete_value(&mut self, key: Key, ts: u64) {
        let key = key.append_ts(ts);
        self.write_size += key.as_encoded().len();
        self.writes.push(Modify::Delete(CF_DEFAULT, key));
    }

    fn put_write(&mut self, key: Key, ts: u64, value: Value) {
        let key = key.append_ts(ts);
        self.write_size += CF_WRITE.len() + key.as_encoded().len() + value.len();
        self.writes.push(Modify::Put(CF_WRITE, key, value));
    }

    fn delete_write(&mut self, key: Key, ts: u64) {
        let key = key.append_ts(ts);
        self.write_size += CF_WRITE.len() + key.as_encoded().len();
        self.writes.push(Modify::Delete(CF_WRITE, key));
    }

    fn key_exist(&mut self, key: &Key, ts: u64) -> Result<bool> {
        Ok(self.reader.get_write(&key, ts)?.is_some())
    }

    pub fn prewrite(
        &mut self,
        mutation: Mutation,
        primary: &[u8],
        options: &Options,
    ) -> Result<()> {
        let lock_type = LockType::from_mutation(&mutation);
        let (key, value, should_not_exist) = match mutation {
            Mutation::Put((key, value)) => (key, Some(value), false),
            Mutation::Delete(key) => (key, None, false),
            Mutation::Lock(key) => (key, None, false),
            Mutation::Insert((key, value)) => (key, Some(value), true),
        };

        if let Some(lock) = self.reader.load_lock(&key)? {
            // Abort on lock belonging to other transaction.
            if lock.ts != self.start_ts {
                return Err(Error::KeyIsLocked {
                    key: key.into_raw()?,
                    primary: lock.primary,
                    ts: lock.ts,
                    ttl: lock.ttl,
                });
            }
            if lock.lock_type != LockType::Pessimistic {
                // Duplicated command. No need to overwrite the lock and data.
                MVCC_DUPLICATE_CMD_COUNTER_VEC.prewrite.inc();
                return Ok(());
            }
        } else if options.prewrite_pessimistic_lock {
            // Pessimistic lock does not exist, the transaction should be aborted.
            info!("prewrite failed (pessimistic lock not found)";
                    "start_ts" => self.start_ts,
                    "key" => %key);

            return Err(Error::PessimisticLockNotFound {
                start_ts: self.start_ts,
                key: key.into_raw()?,
            });
        }

        if !options.skip_constraint_check {
            if let Some((commit_ts, write)) = self.reader.seek_write(&key, u64::max_value())? {
                // Abort on writes after our start timestamp ...
                // If exists a commit version whose commit timestamp is larger than or equal to
                // current start timestamp, we should abort current prewrite, even if the commit
                // type is Rollback.
                //
                // If the transaction is pessimistic, the data conflict is resolved by `for_update_ts`
                if commit_ts == self.start_ts
                    || (options.is_pessimistic_lock.is_empty() && commit_ts > self.start_ts)
                {
                    MVCC_CONFLICT_COUNTER.prewrite_write_conflict.inc();
                    return Err(Error::WriteConflict {
                        start_ts: self.start_ts,
                        conflict_start_ts: write.start_ts,
                        conflict_commit_ts: commit_ts,
                        key: key.into_raw()?,
                        primary: primary.to_vec(),
                    });
                }
                if should_not_exist {
                    if write.write_type == WriteType::Put
                        || (write.write_type != WriteType::Delete
                            && self.key_exist(&key, write.start_ts - 1)?)
                    {
                        return Err(Error::AlreadyExist {
                            key: key.into_raw()?,
                        });
                    }
                }
            }
        }

        if value.is_none() || is_short_value(value.as_ref().unwrap()) {
            self.lock_key(key, lock_type, primary.to_vec(), options.lock_ttl, value);
        } else {
            // value is long
            let ts = self.start_ts;
            self.put_value(key.clone(), ts, value.unwrap());

            self.lock_key(key, lock_type, primary.to_vec(), options.lock_ttl, None);
        }

        Ok(())
    }

    pub fn acquire_pessimistic_lock(
        &mut self,
        key: Key,
        primary: &[u8],
        for_update_ts: u64,
        options: &Options,
    ) -> Result<()> {
        if let Some(lock) = self.reader.load_lock(&key)? {
            if lock.ts != self.start_ts {
                return Err(Error::KeyIsLocked {
                    key: key.into_raw()?,
                    primary: lock.primary,
                    ts: lock.ts,
                    ttl: lock.ttl,
                });
            }
            if lock.lock_type != LockType::Pessimistic {
                return Err(Error::LockTypeNotMatch {
                    start_ts: self.start_ts,
                    key: key.into_raw()?,
                    pessimistic: false,
                });
            }
            MVCC_DUPLICATE_CMD_COUNTER_VEC.pessimistic_lock.inc();
            return Ok(());
        }

        if let Some((commit_ts, _)) = self.reader.seek_write(&key, u64::max_value())? {
            // The isolation level of pessimistic transactions is RC. `for_update_ts` is
            // the commit_ts of the data this transaction read. If exists a commit version
            // whose commit timestamp is larger than current `for_update_ts`, the
            // transaction should retry to get the latest data.
            if commit_ts > for_update_ts {
                MVCC_CONFLICT_COUNTER.pessimistic_lock_conflict.inc();
                return Err(Error::WriteConflict {
                    start_ts: self.start_ts,
                    // TiDB will try to extract the latest `commit_ts` from WriteConflict
                    // error to avoid getting a timestamp from tso. So we set
                    // `conflict_start_ts` to `commit_ts` here.
                    conflict_start_ts: commit_ts,
                    conflict_commit_ts: commit_ts,
                    key: key.into_raw()?,
                    primary: primary.to_vec(),
                });
            }

            // Handle rollback.
            // If the commit timestamp is equal to transaction's start timestamp, the
            // lock is already rollbacked.
            if commit_ts == self.start_ts {
                return Err(Error::Rollbacked {
                    start_ts: self.start_ts,
                    key: key.into_raw()?,
                });
            }
            // If `commit_ts` we seek is already before `start_ts`, the rollback must not exist.
            if commit_ts > self.start_ts {
                if let Some((commit_ts, _)) = self.reader.seek_write(&key, self.start_ts)? {
                    // Rollback's commit_ts is equal to start_ts
                    if commit_ts == self.start_ts {
                        return Err(Error::Rollbacked {
                            start_ts: self.start_ts,
                            key: key.into_raw()?,
                        });
                    }
                }
            }
        }

        self.lock_key(
            key,
            LockType::Pessimistic,
            primary.to_vec(),
            options.lock_ttl,
            None,
        );

        Ok(())
    }

    pub fn commit(&mut self, key: Key, commit_ts: u64) -> Result<()> {
        let (lock_type, short_value) = match self.reader.load_lock(&key)? {
            Some(ref mut lock) if lock.ts == self.start_ts => {
                // A pessimistic lock cannot be committed.
                if lock.lock_type == LockType::Pessimistic {
                    error!(
                        "trying to committing a pessimistic lock";
                        "key" => %key,
                        "start_ts" => self.start_ts,
                        "commit_ts" => commit_ts,
                    );
                    return Err(Error::LockTypeNotMatch {
                        start_ts: self.start_ts,
                        key: key.into_raw()?,
                        pessimistic: true,
                    });
                }
                (lock.lock_type, lock.short_value.take())
            }
            _ => {
                return match self.reader.get_txn_commit_info(&key, self.start_ts)? {
                    Some((_, WriteType::Rollback)) | None => {
                        MVCC_CONFLICT_COUNTER.commit_lock_not_found.inc();
                        // None: related Rollback has been collapsed.
                        // Rollback: rollback by concurrent transaction.
                        info!(
                            "txn conflict (lock not found)";
                            "key" => %key,
                            "start_ts" => self.start_ts,
                            "commit_ts" => commit_ts,
                        );
                        Err(Error::TxnLockNotFound {
                            start_ts: self.start_ts,
                            commit_ts,
                            key: key.into_raw()?,
                        })
                    }
                    // Committed by concurrent transaction.
                    Some((_, WriteType::Put))
                    | Some((_, WriteType::Delete))
                    | Some((_, WriteType::Lock)) => {
                        MVCC_DUPLICATE_CMD_COUNTER_VEC.commit.inc();
                        Ok(())
                    }
                };
            }
        };
        let write = Write::new(
            WriteType::from_lock_type(lock_type).unwrap(),
            self.start_ts,
            short_value,
        );
        self.put_write(key.clone(), commit_ts, write.to_bytes());
        self.unlock_key(key);
        Ok(())
    }

    pub fn rollback(&mut self, key: Key) -> Result<()> {
        match self.reader.load_lock(&key)? {
            Some(ref lock) if lock.ts == self.start_ts => {
                // If prewrite type is DEL or LOCK or PESSIMISTIC, it is no need to delete value.
                if lock.short_value.is_none() && lock.lock_type == LockType::Put {
                    self.delete_value(key.clone(), lock.ts);
                }
            }
            _ => {
                return match self.reader.get_txn_commit_info(&key, self.start_ts)? {
                    Some((ts, write_type)) => {
                        if write_type == WriteType::Rollback {
                            // return Ok on Rollback already exist
                            MVCC_DUPLICATE_CMD_COUNTER_VEC.rollback.inc();
                            Ok(())
                        } else {
                            MVCC_CONFLICT_COUNTER.rollback_committed.inc();
                            info!(
                                "txn conflict (committed)";
                                "key" => %key,
                                "start_ts" => self.start_ts,
                                "commit_ts" => ts,
                            );
                            Err(Error::Committed { commit_ts: ts })
                        }
                    }
                    None => {
                        let ts = self.start_ts;

                        // collapse previous rollback if exist.
                        if self.collapse_rollback {
                            self.collapse_prev_rollback(key.clone())?;
                        }

                        // insert a Rollback to WriteCF when receives Rollback before Prewrite
                        let write = Write::new(WriteType::Rollback, ts, None);
                        self.put_write(key, ts, write.to_bytes());
                        Ok(())
                    }
                };
            }
        }
        let write = Write::new(WriteType::Rollback, self.start_ts, None);
        let ts = self.start_ts;
        self.put_write(key.clone(), ts, write.to_bytes());
        self.unlock_key(key.clone());
        if self.collapse_rollback {
            self.collapse_prev_rollback(key)?;
        }
        Ok(())
    }

    fn collapse_prev_rollback(&mut self, key: Key) -> Result<()> {
        if let Some((commit_ts, write)) = self.reader.seek_write(&key, self.start_ts)? {
            if write.write_type == WriteType::Rollback {
                self.delete_write(key, commit_ts);
            }
        }
        Ok(())
    }

    pub fn gc(&mut self, key: Key, safe_point: u64) -> Result<GcInfo> {
        let mut remove_older = false;
        let mut ts: u64 = u64::max_value();
        let mut found_versions = 0;
        let mut deleted_versions = 0;
        let mut latest_delete = None;
        let mut is_completed = true;
        while let Some((commit, write)) = self.gc_reader.seek_write(&key, ts)? {
            ts = commit - 1;
            found_versions += 1;

            if self.write_size >= MAX_TXN_WRITE_SIZE {
                // Cannot remove latest delete when we haven't iterate all versions.
                latest_delete = None;
                is_completed = false;
                break;
            }

            if remove_older {
                self.delete_write(key.clone(), commit);
                if write.write_type == WriteType::Put && write.short_value.is_none() {
                    self.delete_value(key.clone(), write.start_ts);
                }
                deleted_versions += 1;
                continue;
            }

            if commit > safe_point {
                continue;
            }

            // Set `remove_older` after we find the latest value.
            match write.write_type {
                WriteType::Put | WriteType::Delete => {
                    remove_older = true;
                }
                WriteType::Rollback | WriteType::Lock => {}
            }

            // Latest write before `safe_point` can be deleted if its type is Delete,
            // Rollback or Lock.
            match write.write_type {
                WriteType::Delete => {
                    latest_delete = Some(commit);
                }
                WriteType::Rollback | WriteType::Lock => {
                    self.delete_write(key.clone(), commit);
                    deleted_versions += 1;
                }
                WriteType::Put => {}
            }
        }
        if let Some(commit) = latest_delete {
            self.delete_write(key, commit);
            deleted_versions += 1;
        }
        MVCC_VERSIONS_HISTOGRAM.observe(found_versions as f64);
        if deleted_versions > 0 {
            GC_DELETE_VERSIONS_HISTOGRAM.observe(deleted_versions as f64);
        }
        Ok(GcInfo {
            found_versions,
            deleted_versions,
            is_completed,
        })
    }
}

#[cfg(test)]
mod tests {
    use kvproto::kvrpcpb::{Context, IsolationLevel};

    use crate::storage::kv::Engine;
    use crate::storage::mvcc::tests::*;
    use crate::storage::mvcc::WriteType;
    use crate::storage::mvcc::{MvccReader, MvccTxn};
    use crate::storage::{
        Key, Mutation, Options, ScanMode, TestEngineBuilder, SHORT_VALUE_MAX_LEN,
    };

    fn test_mvcc_txn_read_imp(k: &[u8], v: &[u8]) {
        let engine = TestEngineBuilder::new().build().unwrap();

        must_get_none(&engine, k, 1);

        must_prewrite_put(&engine, k, v, k, 5);
        must_get_none(&engine, k, 3);
        must_get_err(&engine, k, 7);

        must_commit(&engine, k, 5, 10);
        must_get_none(&engine, k, 3);
        must_get_none(&engine, k, 7);
        must_get(&engine, k, 13, v);
        must_prewrite_delete(&engine, k, k, 15);
        must_commit(&engine, k, 15, 20);
        must_get_none(&engine, k, 3);
        must_get_none(&engine, k, 7);
        must_get(&engine, k, 13, v);
        must_get(&engine, k, 17, v);
        must_get_none(&engine, k, 23);
    }

    #[test]
    fn test_mvcc_txn_read() {
        test_mvcc_txn_read_imp(b"k1", b"v1");

        let long_value = "v".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_mvcc_txn_read_imp(b"k2", &long_value);
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
        // Write conflict.
        must_prewrite_lock_err(&engine, k, k, 6);
        must_unlocked(&engine, k);
        // Not conflict.
        must_prewrite_lock(&engine, k, k, 12);
        must_locked(&engine, k, 12);
        must_rollback(&engine, k, 12);
        must_unlocked(&engine, k);
        must_written(&engine, k, 12, 12, WriteType::Rollback);
        // Cannot retry Prewrite after rollback.
        must_prewrite_lock_err(&engine, k, k, 12);
        // Can prewrite after rollback.
        must_prewrite_delete(&engine, k, k, 13);
        must_rollback(&engine, k, 13);
        must_unlocked(&engine, k);
    }

    #[test]
    fn test_mvcc_txn_prewrite_insert() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let (k1, v1, v2, v3) = (b"k1", b"v1", b"v2", b"v3");
        must_prewrite_put(&engine, k1, v1, k1, 1);
        must_commit(&engine, k1, 1, 2);

        // "k1" already exist, returns AlreadyExist error.
        assert!(try_prewrite_insert(&engine, k1, v2, k1, 3).is_err());

        // Delete "k1"
        must_prewrite_delete(&engine, k1, k1, 4);
        must_commit(&engine, k1, 4, 5);

        // After delete "k1", insert returns ok.
        assert!(try_prewrite_insert(&engine, k1, v2, k1, 6).is_ok());
        must_commit(&engine, k1, 6, 7);

        // Rollback
        must_prewrite_put(&engine, k1, v3, k1, 8);
        must_rollback(&engine, k1, 8);

        assert!(try_prewrite_insert(&engine, k1, v3, k1, 9).is_err());

        // Delete "k1" again
        must_prewrite_delete(&engine, k1, k1, 10);
        must_commit(&engine, k1, 10, 11);

        // Rollback again
        must_prewrite_put(&engine, k1, v3, k1, 12);
        must_rollback(&engine, k1, 12);

        // After delete "k1", insert returns ok.
        assert!(try_prewrite_insert(&engine, k1, v2, k1, 13).is_ok());
        must_commit(&engine, k1, 13, 14);
    }

    #[test]
    fn test_rollback_lock() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let (k, v) = (b"k1", b"v1");
        must_prewrite_put(&engine, k, v, k, 5);
        must_commit(&engine, k, 5, 10);

        // Lock
        must_prewrite_lock(&engine, k, k, 15);
        must_locked(&engine, k, 15);

        // Rollback lock
        must_rollback(&engine, k, 15);
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
        must_rollback(&engine, k, 15);
    }

    #[test]
    fn test_mvcc_txn_prewrite() {
        test_mvcc_txn_prewrite_imp(b"k1", b"v1");

        let long_value = "v".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_mvcc_txn_prewrite_imp(b"k2", &long_value);
    }

    fn test_mvcc_txn_commit_ok_imp(k1: &[u8], v1: &[u8], k2: &[u8], k3: &[u8]) {
        let engine = TestEngineBuilder::new().build().unwrap();
        must_prewrite_put(&engine, k1, v1, k1, 10);
        must_prewrite_lock(&engine, k2, k1, 10);
        must_prewrite_delete(&engine, k3, k1, 10);
        must_locked(&engine, k1, 10);
        must_locked(&engine, k2, 10);
        must_locked(&engine, k3, 10);
        must_commit(&engine, k1, 10, 15);
        must_commit(&engine, k2, 10, 15);
        must_commit(&engine, k3, 10, 15);
        must_written(&engine, k1, 10, 15, WriteType::Put);
        must_written(&engine, k2, 10, 15, WriteType::Lock);
        must_written(&engine, k3, 10, 15, WriteType::Delete);
        // commit should be idempotent
        must_commit(&engine, k1, 10, 15);
        must_commit(&engine, k2, 10, 15);
        must_commit(&engine, k3, 10, 15);
    }

    #[test]
    fn test_mvcc_txn_commit_ok() {
        test_mvcc_txn_commit_ok_imp(b"x", b"v", b"y", b"z");

        let long_value = "v".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_mvcc_txn_commit_ok_imp(b"x", &long_value, b"y", b"z");
    }

    fn test_mvcc_txn_commit_err_imp(k: &[u8], v: &[u8]) {
        let engine = TestEngineBuilder::new().build().unwrap();

        // Not prewrite yet
        must_commit_err(&engine, k, 1, 2);
        must_prewrite_put(&engine, k, v, k, 5);
        // start_ts not match
        must_commit_err(&engine, k, 4, 5);
        must_rollback(&engine, k, 5);
        // commit after rollback
        must_commit_err(&engine, k, 5, 6);
    }

    #[test]
    fn test_mvcc_txn_commit_err() {
        test_mvcc_txn_commit_err_imp(b"k", b"v");

        let long_value = "v".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_mvcc_txn_commit_err_imp(b"k2", &long_value);
    }

    fn test_mvcc_txn_rollback_imp(k: &[u8], v: &[u8]) {
        let engine = TestEngineBuilder::new().build().unwrap();

        must_prewrite_put(&engine, k, v, k, 5);
        must_rollback(&engine, k, 5);
        // rollback should be idempotent
        must_rollback(&engine, k, 5);
        // lock should be released after rollback
        must_unlocked(&engine, k);
        must_prewrite_lock(&engine, k, k, 10);
        must_rollback(&engine, k, 10);
        // data should be dropped after rollback
        must_get_none(&engine, k, 20);
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

        must_rollback(&engine, k, t2);
        must_rollback(&engine, k, t2);
        must_rollback(&engine, k, t4);

        must_commit(&engine, k, t1, t3);
        // The rollback should be failed since the transaction
        // was committed before.
        must_rollback_err(&engine, k, t1);
        must_get(&engine, k, t4, v);
    }

    #[test]
    fn test_mvcc_txn_rollback() {
        test_mvcc_txn_rollback_imp(b"k", b"v");

        let long_value = "v".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_mvcc_txn_rollback_imp(b"k2", &long_value);
    }

    fn test_mvcc_txn_rollback_err_imp(k: &[u8], v: &[u8]) {
        let engine = TestEngineBuilder::new().build().unwrap();

        must_prewrite_put(&engine, k, v, k, 5);
        must_commit(&engine, k, 5, 10);
        must_rollback_err(&engine, k, 5);
        must_rollback_err(&engine, k, 5);
    }

    #[test]
    fn test_mvcc_txn_rollback_err() {
        test_mvcc_txn_rollback_err_imp(b"k", b"v");

        let long_value = "v".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_mvcc_txn_rollback_err_imp(b"k2", &long_value);
    }

    #[test]
    fn test_mvcc_txn_rollback_before_prewrite() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let key = b"key";
        must_rollback(&engine, key, 5);
        must_prewrite_lock_err(&engine, key, key, 5);
    }

    fn test_gc_imp(k: &[u8], v1: &[u8], v2: &[u8], v3: &[u8], v4: &[u8]) {
        let engine = TestEngineBuilder::new().build().unwrap();

        must_prewrite_put(&engine, k, v1, k, 5);
        must_commit(&engine, k, 5, 10);
        must_prewrite_put(&engine, k, v2, k, 15);
        must_commit(&engine, k, 15, 20);
        must_prewrite_delete(&engine, k, k, 25);
        must_commit(&engine, k, 25, 30);
        must_prewrite_put(&engine, k, v3, k, 35);
        must_commit(&engine, k, 35, 40);
        must_prewrite_lock(&engine, k, k, 45);
        must_commit(&engine, k, 45, 50);
        must_prewrite_put(&engine, k, v4, k, 55);
        must_rollback(&engine, k, 55);

        // Transactions:
        // startTS commitTS Command
        // --
        // 55      -        PUT "x55" (Rollback)
        // 45      50       LOCK
        // 35      40       PUT "x35"
        // 25      30       DELETE
        // 15      20       PUT "x15"
        //  5      10       PUT "x5"

        // CF data layout:
        // ts CFDefault   CFWrite
        // --
        // 55             Rollback(PUT,50)
        // 50             Commit(LOCK,45)
        // 45
        // 40             Commit(PUT,35)
        // 35   x35
        // 30             Commit(Delete,25)
        // 25
        // 20             Commit(PUT,15)
        // 15   x15
        // 10             Commit(PUT,5)
        // 5    x5

        must_gc(&engine, k, 12);
        must_get(&engine, k, 12, v1);

        must_gc(&engine, k, 22);
        must_get(&engine, k, 22, v2);
        must_get_none(&engine, k, 12);

        must_gc(&engine, k, 32);
        must_get_none(&engine, k, 22);
        must_get_none(&engine, k, 35);

        must_gc(&engine, k, 60);
        must_get(&engine, k, 62, v3);
    }

    #[test]
    fn test_gc() {
        test_gc_imp(b"k1", b"v1", b"v2", b"v3", b"v4");

        let v1 = "x".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        let v2 = "y".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        let v3 = "z".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        let v4 = "v".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_gc_imp(b"k2", &v1, &v2, &v3, &v4);
    }

    fn test_write_imp(k: &[u8], v: &[u8], k2: &[u8], k3: &[u8]) {
        let engine = TestEngineBuilder::new().build().unwrap();

        must_prewrite_put(&engine, k, v, k, 5);
        must_seek_write_none(&engine, k, 5);

        must_commit(&engine, k, 5, 10);
        must_seek_write(&engine, k, u64::max_value(), 5, 10, WriteType::Put);
        must_reverse_seek_write(&engine, k, 5, 5, 10, WriteType::Put);
        must_seek_write_none(&engine, k2, u64::max_value());
        must_reverse_seek_write_none(&engine, k3, 5);
        must_get_commit_ts(&engine, k, 5, 10);

        must_prewrite_delete(&engine, k, k, 15);
        must_rollback(&engine, k, 15);
        must_seek_write(&engine, k, u64::max_value(), 15, 15, WriteType::Rollback);
        must_reverse_seek_write(&engine, k, 15, 15, 15, WriteType::Rollback);
        must_get_commit_ts(&engine, k, 5, 10);
        must_get_commit_ts_none(&engine, k, 15);

        must_prewrite_lock(&engine, k, k, 25);
        must_commit(&engine, k, 25, 30);
        must_seek_write(&engine, k, u64::max_value(), 25, 30, WriteType::Lock);
        must_reverse_seek_write(&engine, k, 25, 25, 30, WriteType::Lock);
        must_get_commit_ts(&engine, k, 25, 30);
    }

    #[test]
    fn test_write() {
        test_write_imp(b"kk", b"v1", b"k", b"kkk");

        let v2 = "x".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_write_imp(b"kk", &v2, b"k", b"kkk");
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

    fn test_write_size_imp(k: &[u8], v: &[u8], pk: &[u8]) {
        let engine = TestEngineBuilder::new().build().unwrap();
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, 10, true).unwrap();
        let key = Key::from_raw(k);
        assert_eq!(txn.write_size, 0);

        txn.prewrite(
            Mutation::Put((key.clone(), v.to_vec())),
            pk,
            &Options::default(),
        )
        .unwrap();
        assert!(txn.write_size() > 0);
        engine.write(&ctx, txn.into_modifies()).unwrap();

        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, 10, true).unwrap();
        txn.commit(key, 15).unwrap();
        assert!(txn.write_size() > 0);
        engine.write(&ctx, txn.into_modifies()).unwrap();
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

        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, 5, true).unwrap();
        assert!(txn
            .prewrite(
                Mutation::Put((Key::from_raw(key), value.to_vec())),
                key,
                &Options::default()
            )
            .is_err());

        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, 5, true).unwrap();
        let mut opt = Options::default();
        opt.skip_constraint_check = true;
        assert!(txn
            .prewrite(
                Mutation::Put((Key::from_raw(key), value.to_vec())),
                key,
                &opt
            )
            .is_ok());
    }

    #[test]
    fn test_read_commit() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let (key, v1, v2) = (b"key", b"v1", b"v2");

        must_prewrite_put(&engine, key, v1, key, 5);
        must_commit(&engine, key, 5, 10);
        must_prewrite_put(&engine, key, v2, key, 15);
        must_get_err(&engine, key, 20);
        must_get_rc(&engine, key, 12, v1);
        must_get_rc(&engine, key, 20, v1);
    }

    #[test]
    fn test_collapse_prev_rollback() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let (key, value) = (b"key", b"value");

        // Add a Rollback whose start ts is 1.
        must_prewrite_put(&engine, key, value, key, 1);
        must_rollback_collapsed(&engine, key, 1);
        must_get_rollback_ts(&engine, key, 1);

        // Add a Rollback whose start ts is 2, the previous Rollback whose
        // start ts is 1 will be collapsed.
        must_prewrite_put(&engine, key, value, key, 2);
        must_rollback_collapsed(&engine, key, 2);
        must_get_none(&engine, key, 2);
        must_get_rollback_ts(&engine, key, 2);
        must_get_rollback_ts_none(&engine, key, 1);

        // Rollback arrive before Prewrite, it will collapse the
        // previous rollback whose start ts is 2.
        must_rollback_collapsed(&engine, key, 3);
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

        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut reader = MvccReader::new(
            snapshot,
            Some(ScanMode::Forward),
            true,
            None,
            None,
            IsolationLevel::SI,
        );

        let v = reader.scan_values_in_default(&Key::from_raw(&[3])).unwrap();
        assert_eq!(v.len(), 2);
        assert_eq!(v[1], (3, "a".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes()));
        assert_eq!(v[0], (5, "b".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes()));
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

        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut reader = MvccReader::new(
            snapshot,
            Some(ScanMode::Forward),
            true,
            None,
            None,
            IsolationLevel::SI,
        );

        assert_eq!(reader.seek_ts(3).unwrap().unwrap(), Key::from_raw(&[2]));
    }

    #[test]
    fn test_pessimistic_lock() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let k = b"k1";
        let v = b"v1";

        // Normal
        must_acquire_pessimistic_lock(&engine, k, k, 1, 1);
        must_pessimistic_locked(&engine, k, 1);
        must_prewrite_pessimistic_put(&engine, k, v, k, 1);
        must_locked(&engine, k, 1);
        must_commit(&engine, k, 1, 2);
        must_unlocked(&engine, k);

        // Lock conflict
        must_prewrite_put(&engine, k, v, k, 3);
        must_acquire_pessimistic_lock_err(&engine, k, k, 4, 4);
        must_rollback(&engine, k, 3);
        must_unlocked(&engine, k);
        must_acquire_pessimistic_lock(&engine, k, k, 5, 5);
        must_prewrite_lock_err(&engine, k, k, 6);
        must_acquire_pessimistic_lock_err(&engine, k, k, 6, 6);
        must_rollback(&engine, k, 5);
        must_unlocked(&engine, k);

        // Data conflict
        must_prewrite_put(&engine, k, v, k, 7);
        must_commit(&engine, k, 7, 9);
        must_unlocked(&engine, k);
        must_prewrite_lock_err(&engine, k, k, 8);
        must_acquire_pessimistic_lock_err(&engine, k, k, 8, 8);
        must_acquire_pessimistic_lock(&engine, k, k, 8, 9);
        must_prewrite_pessimistic_put(&engine, k, v, k, 8);
        must_commit(&engine, k, 8, 10);
        must_unlocked(&engine, k);

        // Rollback
        must_acquire_pessimistic_lock(&engine, k, k, 11, 11);
        must_pessimistic_locked(&engine, k, 11);
        must_rollback(&engine, k, 11);
        must_acquire_pessimistic_lock_err(&engine, k, k, 11, 11);
        must_prewrite_pessimistic_put_err(&engine, k, v, k, 11);
        must_prewrite_lock_err(&engine, k, k, 11);
        must_unlocked(&engine, k);

        must_acquire_pessimistic_lock(&engine, k, k, 12, 12);
        must_prewrite_pessimistic_put(&engine, k, v, k, 12);
        must_locked(&engine, k, 12);
        must_rollback(&engine, k, 12);
        must_acquire_pessimistic_lock_err(&engine, k, k, 12, 12);
        must_prewrite_pessimistic_put_err(&engine, k, v, k, 12);
        must_prewrite_lock_err(&engine, k, k, 12);
        must_unlocked(&engine, k);

        // Duplicated
        must_acquire_pessimistic_lock(&engine, k, k, 13, 13);
        must_pessimistic_locked(&engine, k, 13);
        must_acquire_pessimistic_lock(&engine, k, k, 13, 13);
        must_prewrite_pessimistic_put(&engine, k, v, k, 13);
        must_locked(&engine, k, 13);
        must_prewrite_pessimistic_put(&engine, k, v, k, 13);
        must_locked(&engine, k, 13);
        must_commit(&engine, k, 13, 14);
        must_unlocked(&engine, k);

        // Pessimistic lock doesn't block reads.
        must_acquire_pessimistic_lock(&engine, k, k, 15, 15);
        must_pessimistic_locked(&engine, k, 15);
        must_get(&engine, k, 16, v);
        must_prewrite_pessimistic_delete(&engine, k, k, 15);
        must_get_err(&engine, k, 16);
        must_commit(&engine, k, 15, 17);

        // Rollback
        must_acquire_pessimistic_lock(&engine, k, k, 18, 18);
        must_rollback(&engine, k, 18);
        must_unlocked(&engine, k);
        must_prewrite_put(&engine, k, v, k, 19);
        must_commit(&engine, k, 19, 20);
        must_acquire_pessimistic_lock_err(&engine, k, k, 18, 21);
        must_unlocked(&engine, k);

        // Prewrite non-exist pessimistic lock
        must_prewrite_pessimistic_put_err(&engine, k, v, k, 22);

        // LockTypeNotMatch
        must_prewrite_put(&engine, k, v, k, 23);
        must_locked(&engine, k, 23);
        must_acquire_pessimistic_lock_err(&engine, k, k, 23, 23);
        must_rollback(&engine, k, 23);
        must_acquire_pessimistic_lock(&engine, k, k, 24, 24);
        must_pessimistic_locked(&engine, k, 24);
        must_commit_err(&engine, k, 24, 25);
        must_rollback(&engine, k, 24);

        // start_ts and commit_ts interlacing
        for start_ts in &[40, 50, 60] {
            let for_update_ts = start_ts + 48;
            let commit_ts = start_ts + 50;
            must_acquire_pessimistic_lock(&engine, k, k, *start_ts, for_update_ts);
            must_prewrite_pessimistic_put(&engine, k, v, k, *start_ts);
            must_commit(&engine, k, *start_ts, commit_ts);
        }

        must_rollback(&engine, k, 70);

        // Now the data should be like: (start_ts -> commit_ts)
        // 40 -> 90
        // 50 -> 100
        // 60 -> 110
        // 70 -> rollback
        must_get_commit_ts(&engine, k, 40, 90);
        must_get_commit_ts(&engine, k, 50, 100);
        must_get_commit_ts(&engine, k, 60, 110);
        must_get_rollback_ts(&engine, k, 70);
    }
}
