// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use super::lock::{Lock, LockType};
use super::metrics::*;
use super::reader::MvccReader;
use super::write::{Write, WriteType};
use super::{extract_physical, Error, Result};
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
                IsolationLevel::Si,
            ),
            gc_reader: MvccReader::new(
                snapshot,
                Some(ScanMode::Forward),
                fill_cache,
                None,
                None,
                IsolationLevel::Si,
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

    fn put_lock(&mut self, key: Key, lock: &Lock) {
        let lock = lock.to_bytes();
        self.write_size += CF_LOCK.len() + key.as_encoded().len() + lock.len();
        self.writes.push(Modify::Put(CF_LOCK, key, lock));
    }

    fn lock_key(
        &mut self,
        key: Key,
        lock_type: LockType,
        primary: Vec<u8>,
        short_value: Option<Value>,
        options: &Options,
    ) {
        let lock = Lock::new(
            lock_type,
            primary,
            self.start_ts,
            options.lock_ttl,
            short_value,
            options.for_update_ts,
            options.txn_size,
            options.min_commit_ts,
        );
        self.put_lock(key, &lock);
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

    fn prewrite_key_value(
        &mut self,
        key: Key,
        lock_type: LockType,
        primary: Vec<u8>,
        value: Option<Value>,
        options: &Options,
    ) {
        if let Some(value) = value {
            if is_short_value(&value) {
                // If the value is short, embed it in Lock.
                self.lock_key(key, lock_type, primary, Some(value), options);
            } else {
                // value is long
                let ts = self.start_ts;
                self.put_value(key.clone(), ts, value);

                self.lock_key(key, lock_type, primary, None, options);
            }
        } else {
            self.lock_key(key, lock_type, primary, None, options);
        }
    }

    fn rollback_lock(&mut self, key: Key, lock: &Lock, is_pessimistic_txn: bool) -> Result<()> {
        // If prewrite type is DEL or LOCK or PESSIMISTIC, it is no need to delete value.
        if lock.short_value.is_none() && lock.lock_type == LockType::Put {
            self.delete_value(key.clone(), lock.ts);
        }

        // Only the primary key of a pessimistic transaction needs to be protected.
        let protected: bool = is_pessimistic_txn && key.is_encoded_from(&lock.primary);
        let write = Write::new_rollback(self.start_ts, protected);
        self.put_write(key.clone(), self.start_ts, write.to_bytes());
        self.unlock_key(key.clone());
        if self.collapse_rollback {
            self.collapse_prev_rollback(key)?;
        }
        Ok(())
    }

    /// Checks the existence of the key according to `should_not_exist`.
    /// If not, returns an `AlreadyExist` error.
    fn check_data_constraint(
        &mut self,
        should_not_exist: bool,
        write: &Write,
        write_commit_ts: u64,
        key: &Key,
    ) -> Result<()> {
        if !should_not_exist || write.write_type == WriteType::Delete {
            return Ok(());
        }

        // The current key exists under any of the following conditions:
        // 1.The current write type is `PUT`
        // 2.The current write type is `Rollback` or `Lock`, and the key have an older version.
        if write.write_type == WriteType::Put || self.key_exist(&key, write_commit_ts - 1)? {
            return Err(Error::AlreadyExist { key: key.to_raw()? });
        }

        Ok(())
    }

    // Pessimistic transactions only acquire pessimistic locks on row keys.
    // The corrsponding index keys are not locked until pessimistic prewrite.
    // It's possible that lock conflict occours on them, but the isolation is
    // guaranteed by pessimistic locks on row keys, so let TiDB resolves these
    // locks immediately.
    fn handle_non_pessimistic_lock_conflict(
        &self,
        key: Key,
        for_update_ts: u64,
        lock: Lock,
    ) -> Result<()> {
        // The previous pessimistic transaction has been committed or aborted.
        // Resolve it immediately.
        //
        // Because the row key is locked, the optimistic transaction will
        // abort. Resolve it immediately.
        // Optimistic lock's for_update_ts is zero.
        if for_update_ts > lock.for_update_ts {
            let mut info = kvproto::kvrpcpb::LockInfo::default();
            info.set_primary_lock(lock.primary);
            info.set_lock_version(lock.ts);
            info.set_key(key.into_raw()?);
            // Set ttl to 0 so TiDB will resolve lock immediately.
            info.set_lock_ttl(0);
            info.set_txn_size(lock.txn_size);
            Err(Error::KeyIsLocked(info))
        } else {
            Err(Error::Other("stale request".into()))
        }
    }

    pub fn acquire_pessimistic_lock(
        &mut self,
        key: Key,
        primary: &[u8],
        should_not_exist: bool,
        options: &Options,
    ) -> Result<()> {
        let for_update_ts = options.for_update_ts;
        if let Some(lock) = self.reader.load_lock(&key)? {
            if lock.ts != self.start_ts {
                let mut info = kvproto::kvrpcpb::LockInfo::default();
                info.set_primary_lock(lock.primary);
                info.set_lock_version(lock.ts);
                info.set_key(key.into_raw()?);
                info.set_lock_ttl(lock.ttl);
                info.set_txn_size(lock.txn_size);
                return Err(Error::KeyIsLocked(info));
            }
            if lock.lock_type != LockType::Pessimistic {
                return Err(Error::LockTypeNotMatch {
                    start_ts: self.start_ts,
                    key: key.into_raw()?,
                    pessimistic: false,
                });
            }
            // Overwrite the lock with small for_update_ts
            if for_update_ts > lock.for_update_ts {
                self.lock_key(key, LockType::Pessimistic, primary.to_vec(), None, options);
            } else {
                MVCC_DUPLICATE_CMD_COUNTER_VEC
                    .acquire_pessimistic_lock
                    .inc();
            }
            return Ok(());
        }

        if let Some((commit_ts, write)) = self.reader.seek_write(&key, u64::max_value())? {
            // The isolation level of pessimistic transactions is RC. `for_update_ts` is
            // the commit_ts of the data this transaction read. If exists a commit version
            // whose commit timestamp is larger than current `for_update_ts`, the
            // transaction should retry to get the latest data.
            if commit_ts > for_update_ts {
                MVCC_CONFLICT_COUNTER
                    .acquire_pessimistic_lock_conflict
                    .inc();
                return Err(Error::WriteConflict {
                    start_ts: self.start_ts,
                    conflict_start_ts: write.start_ts,
                    conflict_commit_ts: commit_ts,
                    key: key.into_raw()?,
                    primary: primary.to_vec(),
                });
            }

            // Handle rollback.
            // If the start timestamp of write is equal to transaction's start timestamp
            // as well as commit timestamp, the lock is already rollbacked.
            if write.start_ts == self.start_ts && commit_ts == self.start_ts {
                assert!(write.write_type == WriteType::Rollback);
                return Err(Error::PessimisticLockRollbacked {
                    start_ts: self.start_ts,
                    key: key.into_raw()?,
                });
            }
            // If `commit_ts` we seek is already before `start_ts`, the rollback must not exist.
            if commit_ts > self.start_ts {
                if let Some((commit_ts, write)) = self.reader.seek_write(&key, self.start_ts)? {
                    if write.start_ts == self.start_ts {
                        assert!(
                            commit_ts == self.start_ts && write.write_type == WriteType::Rollback
                        );
                        return Err(Error::PessimisticLockRollbacked {
                            start_ts: self.start_ts,
                            key: key.into_raw()?,
                        });
                    }
                }
            }

            // Check data constraint when acquiring pessimistic lock.
            self.check_data_constraint(should_not_exist, &write, commit_ts, &key)?;
        }

        self.lock_key(key, LockType::Pessimistic, primary.to_vec(), None, options);

        Ok(())
    }

    pub fn pessimistic_prewrite(
        &mut self,
        mutation: Mutation,
        primary: &[u8],
        is_pessimistic_lock: bool,
        options: &Options,
    ) -> Result<()> {
        let lock_type = LockType::from_mutation(&mutation);
        let (key, value) = mutation.into_key_value();
        if let Some(lock) = self.reader.load_lock(&key)? {
            if lock.ts != self.start_ts {
                // Abort on lock belonging to other transaction if
                // prewrites a pessimistic lock.
                if is_pessimistic_lock {
                    warn!(
                        "prewrite failed (pessimistic lock not found)";
                        "start_ts" => self.start_ts,
                        "key" => %key,
                        "lock_ts" => lock.ts
                    );
                    return Err(Error::PessimisticLockNotFound {
                        start_ts: self.start_ts,
                        key: key.into_raw()?,
                    });
                }
                return self.handle_non_pessimistic_lock_conflict(key, options.for_update_ts, lock);
            } else {
                if lock.lock_type != LockType::Pessimistic {
                    // Duplicated command. No need to overwrite the lock and data.
                    MVCC_DUPLICATE_CMD_COUNTER_VEC.prewrite.inc();
                    return Ok(());
                }
                // The lock is pessimistic and owned by this txn, go through to overwrite it.
            }
        } else if is_pessimistic_lock {
            // Pessimistic lock does not exist, the transaction should be aborted.
            warn!(
                "prewrite failed (pessimistic lock not found)";
                "start_ts" => self.start_ts,
                "key" => %key
            );

            return Err(Error::PessimisticLockNotFound {
                start_ts: self.start_ts,
                key: key.into_raw()?,
            });
        }

        // No need to check data constraint, it's resolved by pessimistic locks.
        self.prewrite_key_value(key, lock_type, primary.to_vec(), value, options);
        Ok(())
    }

    pub fn prewrite(
        &mut self,
        mutation: Mutation,
        primary: &[u8],
        options: &Options,
    ) -> Result<()> {
        let lock_type = LockType::from_mutation(&mutation);
        // For the insert operation, the old key should not be in the system.
        let should_not_exist = mutation.is_insert();
        let (key, value) = mutation.into_key_value();
        // Check whether there is a newer version.
        if !options.skip_constraint_check {
            if let Some((commit_ts, write)) = self.reader.seek_write(&key, u64::max_value())? {
                // Abort on writes after our start timestamp ...
                // If exists a commit version whose commit timestamp is larger than or equal to
                // current start timestamp, we should abort current prewrite, even if the commit
                // type is Rollback.
                if commit_ts >= self.start_ts {
                    MVCC_CONFLICT_COUNTER.prewrite_write_conflict.inc();
                    return Err(Error::WriteConflict {
                        start_ts: self.start_ts,
                        conflict_start_ts: write.start_ts,
                        conflict_commit_ts: commit_ts,
                        key: key.into_raw()?,
                        primary: primary.to_vec(),
                    });
                }
                self.check_data_constraint(should_not_exist, &write, commit_ts, &key)?;
            }
        }

        // Check whether the current key is locked at any timestamp.
        if let Some(lock) = self.reader.load_lock(&key)? {
            if lock.ts != self.start_ts {
                let mut info = kvproto::kvrpcpb::LockInfo::default();
                info.set_primary_lock(lock.primary);
                info.set_lock_version(lock.ts);
                info.set_key(key.into_raw()?);
                info.set_lock_ttl(lock.ttl);
                info.set_txn_size(lock.txn_size);
                return Err(Error::KeyIsLocked(info));
            }
            // TODO: remove it in future
            if lock.lock_type == LockType::Pessimistic {
                return Err(Error::LockTypeNotMatch {
                    start_ts: self.start_ts,
                    key: key.into_raw()?,
                    pessimistic: true,
                });
            }
            // Duplicated command. No need to overwrite the lock and data.
            MVCC_DUPLICATE_CMD_COUNTER_VEC.prewrite.inc();
            return Ok(());
        }

        self.prewrite_key_value(key, lock_type, primary.to_vec(), value, options);
        Ok(())
    }

    pub fn commit(&mut self, key: Key, commit_ts: u64) -> Result<bool> {
        let (lock_type, short_value, is_pessimistic_txn) = match self.reader.load_lock(&key)? {
            Some(ref mut lock) if lock.ts == self.start_ts => {
                // A pessimistic lock cannot be committed.
                if lock.lock_type == LockType::Pessimistic {
                    error!(
                        "trying to commit a pessimistic lock";
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
                (
                    lock.lock_type,
                    lock.short_value.take(),
                    lock.for_update_ts != 0,
                )
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
                        Ok(false)
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
        Ok(is_pessimistic_txn)
    }

    pub fn rollback(&mut self, key: Key) -> Result<bool> {
        self.cleanup(key, 0)
    }

    /// Cleanup the lock if it's TTL has expired, comparing with `current_ts`. If `current_ts` is 0,
    /// cleanup the lock without checking TTL. If the lock is the primary lock of a pessimistic
    /// transaction, the rollback record is protected from being collapsed.
    ///
    /// Returns whether the lock is a pessimistic lock. Returns error if the key has already been
    /// committed.
    pub fn cleanup(&mut self, key: Key, current_ts: u64) -> Result<bool> {
        match self.reader.load_lock(&key)? {
            Some(ref mut lock) if lock.ts == self.start_ts => {
                // If current_ts is not 0, check the Lock's TTL.
                // If the lock is not expired, do not rollback it but report key is locked.
                if current_ts > 0
                    && extract_physical(lock.ts) + lock.ttl >= extract_physical(current_ts)
                {
                    // The `lock.primary` field will not be accessed again. Use mem::replace to
                    // avoid cloning.
                    let primary = ::std::mem::replace(&mut lock.primary, Default::default());
                    let mut info = kvproto::kvrpcpb::LockInfo::default();
                    info.set_primary_lock(primary);
                    info.set_lock_version(lock.ts);
                    info.set_key(key.into_raw()?);
                    info.set_lock_ttl(lock.ttl);
                    info.set_txn_size(lock.txn_size);
                    return Err(Error::KeyIsLocked(info));
                }

                let is_pessimistic_txn = lock.for_update_ts != 0;
                self.rollback_lock(key, lock, is_pessimistic_txn)?;
                Ok(is_pessimistic_txn)
            }
            _ => {
                match self.reader.get_txn_commit_info(&key, self.start_ts)? {
                    Some((ts, write_type)) => {
                        if write_type == WriteType::Rollback {
                            // return Ok on Rollback already exist
                            MVCC_DUPLICATE_CMD_COUNTER_VEC.rollback.inc();
                            Ok(false)
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

                        // Insert a Rollback to Write CF in case that a stale prewrite command
                        // is received after a cleanup command.
                        // Pessimistic transactions prewrite successfully only if all its
                        // pessimistic locks exist. So collapsing the rollback of a pessimistic
                        // lock is safe. After a pessimistic transaction acquires all its locks,
                        // it is impossible that neither a lock nor a write record is found.
                        // Therefore, we don't need to protect the rollback here.
                        let write = Write::new_rollback(ts, false);
                        self.put_write(key, ts, write.to_bytes());
                        Ok(false)
                    }
                }
            }
        }
    }

    /// Delete any pessimistic lock with small for_update_ts belongs to this transaction.
    pub fn pessimistic_rollback(&mut self, key: Key, for_update_ts: u64) -> Result<()> {
        if let Some(lock) = self.reader.load_lock(&key)? {
            if lock.lock_type == LockType::Pessimistic
                && lock.ts == self.start_ts
                && lock.for_update_ts <= for_update_ts
            {
                self.unlock_key(key);
            }
        }
        Ok(())
    }

    fn collapse_prev_rollback(&mut self, key: Key) -> Result<()> {
        if let Some((commit_ts, write)) = self.reader.seek_write(&key, self.start_ts)? {
            if write.write_type == WriteType::Rollback && !write.is_protected() {
                self.delete_write(key, commit_ts);
            }
        }
        Ok(())
    }

    /// Update a primary key's TTL if `advise_ttl > lock.ttl`.
    ///
    /// Returns the new TTL.
    pub fn txn_heart_beat(&mut self, primary_key: Key, advise_ttl: u64) -> Result<u64> {
        if let Some(mut lock) = self.reader.load_lock(&primary_key)? {
            if lock.ts == self.start_ts {
                if lock.ttl < advise_ttl {
                    lock.ttl = advise_ttl;
                    self.put_lock(primary_key, &lock);
                } else {
                    debug!(
                        "txn_heart_beat with advise_ttl not large than current ttl";
                        "primary_key" => %primary_key,
                        "start_ts" => self.start_ts,
                        "advise_ttl" => advise_ttl,
                        "current_ttl" => lock.ttl,
                    );
                }
                return Ok(lock.ttl);
            }
        }

        debug!(
            "txn_heart_beat invoked but lock is absent";
            "primary_key" => %primary_key,
            "start_ts" => self.start_ts,
            "advise_ttl" => advise_ttl,
        );
        Err(Error::TxnLockNotFound {
            start_ts: self.start_ts,
            commit_ts: 0,
            key: primary_key.into_raw()?,
        })
    }

    /// Check the status of a transaction.
    ///
    /// This operation checks whether a transaction has expired its primary lock's TTL, rollback the
    /// transaction if expired, or update the transaction's min_commit_ts according to the metadata
    /// in the primary lock.
    ///
    /// When transaction T1 meets T2's lock, it may invoke this on T2's primary key. In this
    /// situation, `self.start_ts` is T2's `start_ts`, `caller_start_ts` is T1's `start_ts`, and
    /// the `current_ts` is literally the timestamp when this function is invoked. It may not be
    /// accurate.
    ///
    /// Returns (`lock_ttl`, `commit_ts`, `is_pessimistic_txn`).
    /// After checking, if the lock is still alive, it retrieves the Lock's TTL; if the transaction
    /// is committed, get the commit_ts; otherwise, if the transaction is rolled back or there's
    /// no information about the transaction, results will be both 0.
    pub fn check_txn_status(
        &mut self,
        primary_key: Key,
        caller_start_ts: u64,
        current_ts: u64,
    ) -> Result<(u64, u64, bool)> {
        match self.reader.load_lock(&primary_key)? {
            Some(ref mut lock) if lock.ts == self.start_ts => {
                let is_pessimistic_txn = lock.for_update_ts != 0;

                if extract_physical(lock.ts) + lock.ttl < extract_physical(current_ts) {
                    // If the lock is expired, clean it up.
                    self.rollback_lock(primary_key, lock, is_pessimistic_txn)?;
                    MVCC_CHECK_TXN_STATUS_COUNTER_VEC.rollback.inc();
                    return Ok((0, 0, is_pessimistic_txn));
                }

                let lock_ttl = lock.ttl;
                // If this is a large transaction and the lock is active, push forward the minCommitTS.
                // lock.minCommitTS == 0 may be a secondary lock, or not a large transaction.
                if lock.min_commit_ts > 0 && caller_start_ts >= lock.min_commit_ts {
                    lock.min_commit_ts = caller_start_ts + 1;

                    if lock.min_commit_ts < current_ts {
                        lock.min_commit_ts = current_ts;
                    }

                    self.put_lock(primary_key, lock);
                    MVCC_CHECK_TXN_STATUS_COUNTER_VEC.update_ts.inc();
                }

                Ok((lock_ttl, 0, is_pessimistic_txn))
            }
            _ => {
                MVCC_CHECK_TXN_STATUS_COUNTER_VEC.get_commit_info.inc();
                match self
                    .reader
                    .get_txn_commit_info(&primary_key, self.start_ts)?
                {
                    Some((ts, write_type)) => {
                        if write_type == WriteType::Rollback {
                            Ok((0, 0, false))
                        } else {
                            Ok((0, ts, false))
                        }
                    }
                    None => {
                        let ts = self.start_ts;

                        // collapse previous rollback if exist.
                        if self.collapse_rollback {
                            self.collapse_prev_rollback(primary_key.clone())?;
                        }

                        // Insert a Rollback to Write CF in case that a stale prewrite command
                        // is received after a cleanup command.
                        // Pessimistic transactions prewrite successfully only if all its
                        // pessimistic locks exist. So collapsing the rollback of a pessimistic
                        // lock is safe. After a pessimistic transaction acquires all its locks,
                        // it is impossible that neither a lock nor a write record is found.
                        // Therefore, we don't need to protect the rollback here.
                        let write = Write::new_rollback(ts, false);
                        self.put_write(primary_key, ts, write.to_bytes());
                        MVCC_CHECK_TXN_STATUS_COUNTER_VEC.rollback.inc();

                        Ok((0, 0, false))
                    }
                }
            }
        }
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
    use crate::storage::mvcc::{Error, MvccReader, MvccTxn};
    use crate::storage::{
        Key, Mutation, Options, ScanMode, TestEngineBuilder, SHORT_VALUE_MAX_LEN,
    };

    use std::u64;

    fn test_mvcc_txn_read_imp(k1: &[u8], k2: &[u8], v: &[u8]) {
        let engine = TestEngineBuilder::new().build().unwrap();

        must_get_none(&engine, k1, 1);

        must_prewrite_put(&engine, k1, v, k1, 2);
        must_rollback(&engine, k1, 2);
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
        must_get_none(&engine, k1, u64::MAX);
        // should read secondary locks even when reading the latest record
        must_get_err(&engine, k2, u64::MAX);

        must_commit(&engine, k1, 5, 10);
        must_commit(&engine, k2, 5, 10);
        must_get_none(&engine, k1, 3);
        // should not read with ts < commit_ts
        must_get_none(&engine, k1, 7);
        // should read with ts > commit_ts
        must_get(&engine, k1, 13, v);
        // should read the latest record if `ts == u64::MAX`
        must_get(&engine, k1, u64::MAX, v);

        must_prewrite_delete(&engine, k1, k1, 15);
        // should ignore the lock and get previous record when reading the latest record
        must_get(&engine, k1, u64::MAX, v);
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
        // should read the latest record when `ts == u64::MAX`
        // even if lock.start_ts(23) < latest write.commit_ts(27)
        must_get(&engine, k1, u64::MAX, v);
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
    fn test_rollback_lock_optimistic() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let (k, v) = (b"k1", b"v1");
        must_prewrite_put(&engine, k, v, k, 5);
        must_commit(&engine, k, 5, 10);

        // Lock
        must_prewrite_lock(&engine, k, k, 15);
        must_locked(&engine, k, 15);

        // Rollback lock
        must_rollback(&engine, k, 15);
        // Rollbacks of optimistic transactions needn't be protected
        must_get_rollback_protected(&engine, k, 15, false);
    }

    #[test]
    fn test_rollback_lock_pessimistic() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let (k1, k2, v) = (b"k1", b"k2", b"v1");

        must_acquire_pessimistic_lock(&engine, k1, k1, 5, 5);
        must_acquire_pessimistic_lock(&engine, k2, k1, 5, 7);
        must_rollback(&engine, k1, 5);
        must_rollback(&engine, k2, 5);
        // The rollback of the primary key should be protected
        must_get_rollback_protected(&engine, k1, 5, true);
        // The rollback of the secondary key needn't be protected
        must_get_rollback_protected(&engine, k2, 5, false);

        must_acquire_pessimistic_lock(&engine, k1, k1, 15, 15);
        must_acquire_pessimistic_lock(&engine, k2, k1, 15, 17);
        must_pessimistic_prewrite_put(&engine, k1, v, k1, 15, 17, true);
        must_pessimistic_prewrite_put(&engine, k2, v, k1, 15, 17, true);
        must_rollback(&engine, k1, 15);
        must_rollback(&engine, k2, 15);
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
        must_rollback(&engine, k, 15);
    }

    #[test]
    fn test_cleanup() {
        // Cleanup's logic is mostly similar to rollback, except the TTL check. Tests that not
        // related to TTL check should be covered by other test cases.
        let engine = TestEngineBuilder::new().build().unwrap();

        // Shorthand for composing ts.
        let ts = super::super::compose_ts;

        let (k, v) = (b"k", b"v");

        must_prewrite_put(&engine, k, v, k, ts(10, 0));
        must_locked(&engine, k, ts(10, 0));
        must_txn_heart_beat(&engine, k, ts(10, 0), 100, 100);
        // Check the last txn_heart_beat has set the lock's TTL to 100.
        must_txn_heart_beat(&engine, k, ts(10, 0), 90, 100);

        // TTL not expired. Do nothing but returns an error.
        must_cleanup_err(&engine, k, ts(10, 0), ts(20, 0));
        must_locked(&engine, k, ts(10, 0));

        // Try to cleanup another transaction's lock. Does nothing.
        must_cleanup(&engine, k, ts(10, 1), ts(120, 0));
        // If there is no exisiting lock when cleanup, it cannot be a pessimistic transaction,
        // so the rollback needn't be protected.
        must_get_rollback_protected(&engine, k, ts(10, 1), false);
        must_locked(&engine, k, ts(10, 0));

        // TTL expired. The lock should be removed.
        must_cleanup(&engine, k, ts(10, 0), ts(120, 0));
        must_unlocked(&engine, k);
        // Rollbacks of optimistic transactions needn't be protected
        must_get_rollback_protected(&engine, k, ts(10, 0), false);
        must_get_rollback_ts(&engine, k, ts(10, 0));

        // Rollbacks of primary keys in pessimistic transactions should be protected
        must_acquire_pessimistic_lock(&engine, k, k, ts(11, 1), ts(12, 1));
        must_cleanup(&engine, k, ts(11, 1), ts(120, 0));
        must_get_rollback_protected(&engine, k, ts(11, 1), true);

        must_acquire_pessimistic_lock(&engine, k, k, ts(13, 1), ts(14, 1));
        must_pessimistic_prewrite_put(&engine, k, v, k, ts(13, 1), ts(14, 1), true);
        must_cleanup(&engine, k, ts(13, 1), ts(120, 0));
        must_get_rollback_protected(&engine, k, ts(13, 1), true);
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

    fn test_mvcc_txn_rollback_imp(k: &[u8], v: &[u8]) {
        let engine = TestEngineBuilder::new().build().unwrap();

        must_prewrite_put(&engine, k, v, k, 5);
        must_rollback(&engine, k, 5);
        // Rollback should be idempotent
        must_rollback(&engine, k, 5);
        // Lock should be released after rollback
        must_unlocked(&engine, k);
        must_prewrite_lock(&engine, k, k, 10);
        must_rollback(&engine, k, 10);
        // data should be dropped after rollback
        must_get_none(&engine, k, 20);

        // Can't rollback committed transaction.
        must_prewrite_put(&engine, k, v, k, 25);
        must_commit(&engine, k, 25, 30);
        must_rollback_err(&engine, k, 25);
        must_rollback_err(&engine, k, 25);

        // Can't rollback other transaction's lock
        must_prewrite_delete(&engine, k, k, 35);
        must_rollback(&engine, k, 34);
        must_rollback(&engine, k, 36);
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

    fn test_write_imp(k: &[u8], v: &[u8], k2: &[u8]) {
        let engine = TestEngineBuilder::new().build().unwrap();

        must_prewrite_put(&engine, k, v, k, 5);
        must_seek_write_none(&engine, k, 5);

        must_commit(&engine, k, 5, 10);
        must_seek_write(&engine, k, u64::max_value(), 5, 10, WriteType::Put);
        must_seek_write_none(&engine, k2, u64::max_value());
        must_get_commit_ts(&engine, k, 5, 10);

        must_prewrite_delete(&engine, k, k, 15);
        must_rollback(&engine, k, 15);
        must_seek_write(&engine, k, u64::max_value(), 15, 15, WriteType::Rollback);
        must_get_commit_ts(&engine, k, 5, 10);
        must_get_commit_ts_none(&engine, k, 15);

        must_prewrite_lock(&engine, k, k, 25);
        must_commit(&engine, k, 25, 30);
        must_seek_write(&engine, k, u64::max_value(), 25, 30, WriteType::Lock);
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

    fn test_write_size_imp(k: &[u8], v: &[u8], pk: &[u8]) {
        let engine = TestEngineBuilder::new().build().unwrap();
        let ctx = Context::default();
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

        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, 5, true).unwrap();
        assert!(txn
            .prewrite(
                Mutation::Put((Key::from_raw(key), value.to_vec())),
                key,
                &Options::default()
            )
            .is_err());

        let ctx = Context::default();
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

        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut reader = MvccReader::new(
            snapshot,
            Some(ScanMode::Forward),
            true,
            None,
            None,
            IsolationLevel::Si,
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

        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut reader = MvccReader::new(
            snapshot,
            Some(ScanMode::Forward),
            true,
            None,
            None,
            IsolationLevel::Si,
        );

        assert_eq!(reader.seek_ts(3).unwrap().unwrap(), Key::from_raw(&[2]));
    }

    #[test]
    fn test_pessimistic_lock() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let k = b"k1";
        let v = b"v1";

        // TODO: Some corner cases don't give proper results. Although they are not important, we
        // should consider whether they are better to be fixed.

        // Normal
        must_acquire_pessimistic_lock(&engine, k, k, 1, 1);
        must_pessimistic_locked(&engine, k, 1, 1);
        must_pessimistic_prewrite_put(&engine, k, v, k, 1, 1, true);
        must_locked(&engine, k, 1);
        must_commit(&engine, k, 1, 2);
        must_unlocked(&engine, k);

        // Lock conflict
        must_prewrite_put(&engine, k, v, k, 3);
        must_acquire_pessimistic_lock_err(&engine, k, k, 4, 4);
        must_cleanup(&engine, k, 3, 0);
        must_unlocked(&engine, k);
        must_acquire_pessimistic_lock(&engine, k, k, 5, 5);
        must_prewrite_lock_err(&engine, k, k, 6);
        must_acquire_pessimistic_lock_err(&engine, k, k, 6, 6);
        must_cleanup(&engine, k, 5, 0);
        must_unlocked(&engine, k);

        // Data conflict
        must_prewrite_put(&engine, k, v, k, 7);
        must_commit(&engine, k, 7, 9);
        must_unlocked(&engine, k);
        must_prewrite_lock_err(&engine, k, k, 8);
        must_acquire_pessimistic_lock_err(&engine, k, k, 8, 8);
        must_acquire_pessimistic_lock(&engine, k, k, 8, 9);
        must_pessimistic_prewrite_put(&engine, k, v, k, 8, 8, true);
        must_commit(&engine, k, 8, 10);
        must_unlocked(&engine, k);

        // Rollback
        must_acquire_pessimistic_lock(&engine, k, k, 11, 11);
        must_pessimistic_locked(&engine, k, 11, 11);
        must_cleanup(&engine, k, 11, 0);
        must_acquire_pessimistic_lock_err(&engine, k, k, 11, 11);
        must_pessimistic_prewrite_put_err(&engine, k, v, k, 11, 11, true);
        must_prewrite_lock_err(&engine, k, k, 11);
        must_unlocked(&engine, k);

        must_acquire_pessimistic_lock(&engine, k, k, 12, 12);
        must_pessimistic_prewrite_put(&engine, k, v, k, 12, 12, true);
        must_locked(&engine, k, 12);
        must_cleanup(&engine, k, 12, 0);
        must_acquire_pessimistic_lock_err(&engine, k, k, 12, 12);
        must_pessimistic_prewrite_put_err(&engine, k, v, k, 12, 12, true);
        must_prewrite_lock_err(&engine, k, k, 12);
        must_unlocked(&engine, k);

        // Duplicated
        must_acquire_pessimistic_lock(&engine, k, k, 13, 13);
        must_pessimistic_locked(&engine, k, 13, 13);
        must_acquire_pessimistic_lock(&engine, k, k, 13, 13);
        must_pessimistic_locked(&engine, k, 13, 13);
        must_pessimistic_prewrite_put(&engine, k, v, k, 13, 13, true);
        must_locked(&engine, k, 13);
        must_pessimistic_prewrite_put(&engine, k, v, k, 13, 13, true);
        must_locked(&engine, k, 13);
        must_commit(&engine, k, 13, 14);
        must_unlocked(&engine, k);
        must_commit(&engine, k, 13, 14);
        must_unlocked(&engine, k);

        // Pessimistic lock doesn't block reads.
        must_acquire_pessimistic_lock(&engine, k, k, 15, 15);
        must_pessimistic_locked(&engine, k, 15, 15);
        must_get(&engine, k, 16, v);
        must_pessimistic_prewrite_delete(&engine, k, k, 15, 15, true);
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
        must_pessimistic_prewrite_put_err(&engine, k, v, k, 22, 22, true);

        // LockTypeNotMatch
        must_prewrite_put(&engine, k, v, k, 23);
        must_locked(&engine, k, 23);
        must_acquire_pessimistic_lock_err(&engine, k, k, 23, 23);
        must_cleanup(&engine, k, 23, 0);
        must_acquire_pessimistic_lock(&engine, k, k, 24, 24);
        must_pessimistic_locked(&engine, k, 24, 24);
        must_commit_err(&engine, k, 24, 25);
        must_rollback(&engine, k, 24);

        // Acquire lock on a prewritten key should fail.
        must_acquire_pessimistic_lock(&engine, k, k, 26, 26);
        must_pessimistic_locked(&engine, k, 26, 26);
        must_pessimistic_prewrite_delete(&engine, k, k, 26, 26, true);
        must_locked(&engine, k, 26);
        must_acquire_pessimistic_lock_err(&engine, k, k, 26, 26);
        must_locked(&engine, k, 26);

        // Acquire lock on a committed key should fail.
        must_commit(&engine, k, 26, 27);
        must_unlocked(&engine, k);
        must_get_none(&engine, k, 28);
        must_acquire_pessimistic_lock_err(&engine, k, k, 26, 26);
        must_unlocked(&engine, k);
        must_get_none(&engine, k, 28);
        // Pessimistic prewrite on a committed key should fail.
        must_pessimistic_prewrite_put_err(&engine, k, v, k, 26, 26, true);
        must_unlocked(&engine, k);
        must_get_none(&engine, k, 28);
        // Currently we cannot avoid this.
        must_acquire_pessimistic_lock(&engine, k, k, 26, 29);
        must_pessimistic_rollback(&engine, k, 26, 29);
        must_unlocked(&engine, k);

        // Non pessimistic key in pessimistic transaction.
        must_pessimistic_prewrite_put(&engine, k, v, k, 30, 30, false);
        must_locked(&engine, k, 30);
        must_commit(&engine, k, 30, 31);
        must_unlocked(&engine, k);
        must_get_commit_ts(&engine, k, 30, 31);

        // Rollback collapsed.
        must_rollback_collapsed(&engine, k, 32);
        must_rollback_collapsed(&engine, k, 33);
        must_acquire_pessimistic_lock_err(&engine, k, k, 32, 32);
        // Currently we cannot avoid this.
        must_acquire_pessimistic_lock(&engine, k, k, 32, 34);
        must_pessimistic_rollback(&engine, k, 32, 34);
        must_unlocked(&engine, k);

        // Acquire lock when there is lock with different for_update_ts.
        must_acquire_pessimistic_lock(&engine, k, k, 35, 36);
        must_pessimistic_locked(&engine, k, 35, 36);
        must_acquire_pessimistic_lock(&engine, k, k, 35, 35);
        must_pessimistic_locked(&engine, k, 35, 36);
        must_acquire_pessimistic_lock(&engine, k, k, 35, 37);
        must_pessimistic_locked(&engine, k, 35, 37);

        // Cannot prewrite when there is another transaction's pessimistic lock.
        must_pessimistic_prewrite_put_err(&engine, k, v, k, 36, 36, true);
        must_pessimistic_prewrite_put_err(&engine, k, v, k, 36, 38, true);
        must_pessimistic_locked(&engine, k, 35, 37);
        // Cannot prewrite when there is another transaction's non-pessimistic lock.
        must_pessimistic_prewrite_put(&engine, k, v, k, 35, 37, true);
        must_locked(&engine, k, 35);
        must_pessimistic_prewrite_put_err(&engine, k, v, k, 36, 38, true);
        must_locked(&engine, k, 35);

        // Commit pessimistic transaction's key but with smaller commit_ts than for_update_ts.
        // Currently not checked, so in this case it will actually be successfully committed.
        must_commit(&engine, k, 35, 36);
        must_unlocked(&engine, k);
        must_get_commit_ts(&engine, k, 35, 36);

        // Prewrite meets pessimistic lock on a non-pessimistic key.
        // Currently not checked, so prewrite will success.
        must_acquire_pessimistic_lock(&engine, k, k, 40, 40);
        must_pessimistic_locked(&engine, k, 40, 40);
        must_pessimistic_prewrite_put(&engine, k, v, k, 40, 40, false);
        must_locked(&engine, k, 40);
        must_commit(&engine, k, 40, 41);
        must_unlocked(&engine, k);

        // Prewrite with different for_update_ts.
        // Currently not checked.
        must_acquire_pessimistic_lock(&engine, k, k, 42, 45);
        must_pessimistic_locked(&engine, k, 42, 45);
        must_pessimistic_prewrite_put(&engine, k, v, k, 42, 43, true);
        must_locked(&engine, k, 42);
        must_commit(&engine, k, 42, 45);
        must_unlocked(&engine, k);

        must_acquire_pessimistic_lock(&engine, k, k, 46, 47);
        must_pessimistic_locked(&engine, k, 46, 47);
        must_pessimistic_prewrite_put(&engine, k, v, k, 46, 48, true);
        must_locked(&engine, k, 46);
        must_commit(&engine, k, 46, 49);
        must_unlocked(&engine, k);

        // Prewrite on non-pessimistic key meets write with larger commit_ts than current
        // for_update_ts (non-pessimistic data conflict).
        // Normally non-pessimistic keys in pessimistic transactions are used when we are sure that
        // there won't be conflicts. So this case is also not checked, and prewrite will succeeed.
        must_pessimistic_prewrite_put(&engine, k, v, k, 47, 48, false);
        must_locked(&engine, k, 47);
        must_cleanup(&engine, k, 47, 0);
        must_unlocked(&engine, k);

        // The rollback of the primary key in a pessimistic transaction should be protected from
        // being collapsed.
        must_acquire_pessimistic_lock(&engine, k, k, 49, 60);
        must_pessimistic_prewrite_put(&engine, k, v, k, 49, 60, true);
        must_locked(&engine, k, 49);
        must_cleanup(&engine, k, 49, 0);
        must_get_rollback_protected(&engine, k, 49, true);
        must_prewrite_put(&engine, k, v, k, 51);
        must_rollback_collapsed(&engine, k, 51);
        must_acquire_pessimistic_lock_err(&engine, k, k, 49, 60);

        // start_ts and commit_ts interlacing
        for start_ts in &[140, 150, 160] {
            let for_update_ts = start_ts + 48;
            let commit_ts = start_ts + 50;
            must_acquire_pessimistic_lock(&engine, k, k, *start_ts, for_update_ts);
            must_pessimistic_prewrite_put(&engine, k, v, k, *start_ts, for_update_ts, true);
            must_commit(&engine, k, *start_ts, commit_ts);
            must_get(&engine, k, commit_ts + 1, v);
        }

        must_rollback(&engine, k, 170);

        // Now the data should be like: (start_ts -> commit_ts)
        // 140 -> 190
        // 150 -> 200
        // 160 -> 210
        // 170 -> rollback
        must_get_commit_ts(&engine, k, 140, 190);
        must_get_commit_ts(&engine, k, 150, 200);
        must_get_commit_ts(&engine, k, 160, 210);
        must_get_rollback_ts(&engine, k, 170);
    }

    #[test]
    fn test_non_pessimistic_lock_conflict() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let k = b"k1";
        let v = b"v1";

        must_prewrite_put(&engine, k, v, k, 2);
        must_locked(&engine, k, 2);
        must_pessimistic_prewrite_put_err(&engine, k, v, k, 1, 1, false);
        must_pessimistic_prewrite_put_err(&engine, k, v, k, 3, 3, false);
    }

    #[test]
    fn test_pessimistic_rollback() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let k = b"k1";
        let v = b"v1";

        // Normal
        must_acquire_pessimistic_lock(&engine, k, k, 1, 1);
        must_pessimistic_locked(&engine, k, 1, 1);
        must_pessimistic_rollback(&engine, k, 1, 1);
        must_unlocked(&engine, k);
        must_get_commit_ts_none(&engine, k, 1);
        // Pessimistic rollback is idempotent
        must_pessimistic_rollback(&engine, k, 1, 1);
        must_unlocked(&engine, k);
        must_get_commit_ts_none(&engine, k, 1);

        // Succeed if the lock doesn't exist.
        must_pessimistic_rollback(&engine, k, 2, 2);

        // Do nothing if meets other transaction's pessimistic lock
        must_acquire_pessimistic_lock(&engine, k, k, 2, 3);
        must_pessimistic_rollback(&engine, k, 1, 1);
        must_pessimistic_rollback(&engine, k, 1, 2);
        must_pessimistic_rollback(&engine, k, 1, 3);
        must_pessimistic_rollback(&engine, k, 1, 4);
        must_pessimistic_rollback(&engine, k, 3, 3);
        must_pessimistic_rollback(&engine, k, 4, 4);

        // Succeed if for_update_ts is larger; do nothing if for_update_ts is smaller.
        must_pessimistic_locked(&engine, k, 2, 3);
        must_pessimistic_rollback(&engine, k, 2, 2);
        must_pessimistic_locked(&engine, k, 2, 3);
        must_pessimistic_rollback(&engine, k, 2, 4);
        must_unlocked(&engine, k);

        // Do nothing if rollbacks a non-pessimistic lock.
        must_prewrite_put(&engine, k, v, k, 3);
        must_locked(&engine, k, 3);
        must_pessimistic_rollback(&engine, k, 3, 3);
        must_locked(&engine, k, 3);

        // Do nothing if meets other transaction's optimistic lock
        must_pessimistic_rollback(&engine, k, 2, 2);
        must_pessimistic_rollback(&engine, k, 2, 3);
        must_pessimistic_rollback(&engine, k, 2, 4);
        must_pessimistic_rollback(&engine, k, 4, 4);
        must_locked(&engine, k, 3);

        // Do nothing if committed
        must_commit(&engine, k, 3, 4);
        must_unlocked(&engine, k);
        must_get_commit_ts(&engine, k, 3, 4);
        must_pessimistic_rollback(&engine, k, 3, 3);
        must_pessimistic_rollback(&engine, k, 3, 4);
        must_pessimistic_rollback(&engine, k, 3, 5);
    }

    #[test]
    fn test_overwrite_pessimistic_lock() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let k = b"k1";

        must_acquire_pessimistic_lock(&engine, k, k, 1, 2);
        must_pessimistic_locked(&engine, k, 1, 2);
        must_acquire_pessimistic_lock(&engine, k, k, 1, 1);
        must_pessimistic_locked(&engine, k, 1, 2);
        must_acquire_pessimistic_lock(&engine, k, k, 1, 3);
        must_pessimistic_locked(&engine, k, 1, 3);
    }

    #[test]
    fn test_txn_heart_beat() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let (k, v) = (b"k1", b"v1");

        let test = |ts| {
            // Do nothing if advise_ttl is less smaller than current TTL.
            must_txn_heart_beat(&engine, k, ts, 90, 100);
            // Return the new TTL if the TTL when the TTL is updated.
            must_txn_heart_beat(&engine, k, ts, 110, 110);
            // The lock's TTL is updated and persisted into the db.
            must_txn_heart_beat(&engine, k, ts, 90, 110);
            // Heart beat another transaction's lock will lead to an error.
            must_txn_heart_beat_err(&engine, k, ts - 1, 150);
            must_txn_heart_beat_err(&engine, k, ts + 1, 150);
            // The existing lock is not changed.
            must_txn_heart_beat(&engine, k, ts, 90, 110);
        };

        // No lock.
        must_txn_heart_beat_err(&engine, k, 5, 100);

        // Create a lock with TTL=100.
        // The initial TTL will be set to 0 after calling must_prewrite_put. Update it first.
        must_prewrite_put(&engine, k, v, k, 5);
        must_locked(&engine, k, 5);
        must_txn_heart_beat(&engine, k, 5, 100, 100);

        test(5);

        must_locked(&engine, k, 5);
        must_commit(&engine, k, 5, 10);
        must_unlocked(&engine, k);

        // No lock.
        must_txn_heart_beat_err(&engine, k, 5, 100);
        must_txn_heart_beat_err(&engine, k, 10, 100);

        must_acquire_pessimistic_lock(&engine, k, k, 8, 15);
        must_pessimistic_locked(&engine, k, 8, 15);
        must_txn_heart_beat(&engine, k, 8, 100, 100);

        test(8);

        must_pessimistic_locked(&engine, k, 8, 15);
    }

    #[test]
    fn test_check_txn_status() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let (k, v) = (b"k1", b"v1");

        let ts = super::super::compose_ts;

        // Try to check a not exist thing.
        must_check_txn_status(&engine, k, ts(3, 0), ts(3, 1), ts(3, 2), 0, 0);
        // A rollback record will be written.
        must_seek_write(
            &engine,
            k,
            u64::max_value(),
            ts(3, 0),
            ts(3, 0),
            WriteType::Rollback,
        );

        // Lock the key with TTL=100.
        must_prewrite_put_for_large_txn(&engine, k, v, k, ts(5, 0), 100, 0);
        // The initial min_commit_ts is start_ts + 1.
        must_large_txn_locked(&engine, k, ts(5, 0), 100, ts(5, 1), false);

        // Update min_commit_ts to current_ts.
        must_check_txn_status(&engine, k, ts(5, 0), ts(6, 0), ts(7, 0), 100, 0);
        must_large_txn_locked(&engine, k, ts(5, 0), 100, ts(7, 0), false);

        // Update min_commit_ts to caller_start_ts + 1 if current_ts < caller_start_ts.
        // This case should be impossible. But if it happens, we prevents it.
        must_check_txn_status(&engine, k, ts(5, 0), ts(9, 0), ts(8, 0), 100, 0);
        must_large_txn_locked(&engine, k, ts(5, 0), 100, ts(9, 1), false);

        // caller_start_ts < lock.min_commit_ts < current_ts
        // When caller_start_ts < lock.min_commit_ts, no need to update it.
        must_check_txn_status(&engine, k, ts(5, 0), ts(8, 0), ts(10, 0), 100, 0);
        must_large_txn_locked(&engine, k, ts(5, 0), 100, ts(9, 1), false);

        // current_ts < lock.min_commit_ts < caller_start_ts
        must_check_txn_status(&engine, k, ts(5, 0), ts(11, 0), ts(9, 0), 100, 0);
        must_large_txn_locked(&engine, k, ts(5, 0), 100, ts(11, 1), false);

        // For same caller_start_ts and current_ts, update min_commit_ts to caller_start_ts + 1
        must_check_txn_status(&engine, k, ts(5, 0), ts(12, 0), ts(12, 0), 100, 0);
        must_large_txn_locked(&engine, k, ts(5, 0), 100, ts(12, 1), false);

        // Logical time is also considered in the comparing
        must_check_txn_status(&engine, k, ts(5, 0), ts(13, 1), ts(13, 3), 100, 0);
        must_large_txn_locked(&engine, k, ts(5, 0), 100, ts(13, 3), false);

        must_commit(&engine, k, ts(5, 0), ts(15, 0));
        must_unlocked(&engine, k);

        // Check committed key will get the commit ts.
        must_check_txn_status(&engine, k, ts(5, 0), ts(12, 0), ts(12, 0), 0, ts(15, 0));
        must_unlocked(&engine, k);

        must_prewrite_put_for_large_txn(&engine, k, v, k, ts(20, 0), 100, 0);

        // Check a committed transaction when there is another lock. Expect getting the commit ts.
        must_check_txn_status(&engine, k, ts(5, 0), ts(12, 0), ts(12, 0), 0, ts(15, 0));

        // Check a not existing transaction, gets nothing.
        must_check_txn_status(&engine, k, ts(6, 0), ts(12, 0), ts(12, 0), 0, 0);
        // And a rollback record will be written.
        must_seek_write(
            &engine,
            k,
            ts(6, 0),
            ts(6, 0),
            ts(6, 0),
            WriteType::Rollback,
        );

        // TTL check is based on physical time (in ms). When logical time's difference is larger
        // than TTL, the lock won't be resolved.
        must_check_txn_status(&engine, k, ts(20, 0), ts(21, 105), ts(21, 105), 100, 0);
        must_large_txn_locked(&engine, k, ts(20, 0), 100, ts(21, 106), false);

        // If physical time's difference exceeds TTL, lock will be resolved.
        must_check_txn_status(&engine, k, ts(20, 0), ts(121, 0), ts(121, 0), 0, 0);
        must_unlocked(&engine, k);
        must_seek_write(
            &engine,
            k,
            u64::max_value(),
            ts(20, 0),
            ts(20, 0),
            WriteType::Rollback,
        );

        must_acquire_pessimistic_lock_for_large_txn(&engine, k, k, ts(4, 0), ts(130, 0), 100);
        must_large_txn_locked(&engine, k, ts(4, 0), 100, 0, true);

        // Pessimistic lock do not have the min_commit_ts field, so it will not be updated.
        must_check_txn_status(&engine, k, ts(4, 0), ts(10, 0), ts(10, 0), 100, 0);
        must_large_txn_locked(&engine, k, ts(4, 0), 100, 0, true);

        // Commit the key.
        must_pessimistic_prewrite_put(&engine, k, v, k, ts(4, 0), ts(130, 0), true);
        must_commit(&engine, k, ts(4, 0), ts(140, 0));
        must_unlocked(&engine, k);
        must_get_commit_ts(&engine, k, ts(4, 0), ts(140, 0));

        // Now the transactions are intersecting:
        // T1: start_ts = 5, commit_ts = 15
        // T2: start_ts = 20, rollback
        // T3: start_ts = 4, commit_ts = 140
        must_check_txn_status(&engine, k, ts(4, 0), ts(10, 0), ts(10, 0), 0, ts(140, 0));
        must_check_txn_status(&engine, k, ts(5, 0), ts(10, 0), ts(10, 0), 0, ts(15, 0));
        must_check_txn_status(&engine, k, ts(20, 0), ts(10, 0), ts(10, 0), 0, 0);

        // Rollback expired pessimistic lock.
        must_acquire_pessimistic_lock_for_large_txn(&engine, k, k, ts(150, 0), ts(150, 0), 100);
        must_check_txn_status(&engine, k, ts(150, 0), ts(160, 0), ts(160, 0), 100, 0);
        must_large_txn_locked(&engine, k, ts(150, 0), 100, 0, true);
        must_check_txn_status(&engine, k, ts(150, 0), ts(160, 0), ts(260, 0), 0, 0);
        must_unlocked(&engine, k);
        // Rolling back a pessimistic lock should leave Rollback mark.
        must_seek_write(
            &engine,
            k,
            u64::max_value(),
            ts(150, 0),
            ts(150, 0),
            WriteType::Rollback,
        );

        // Rollback when current_ts is u64::MAX
        must_prewrite_put_for_large_txn(&engine, k, v, k, ts(270, 0), 100, 0);
        must_large_txn_locked(&engine, k, ts(270, 0), 100, ts(270, 1), false);
        must_check_txn_status(&engine, k, ts(270, 0), ts(271, 0), u64::max_value(), 0, 0);
        must_unlocked(&engine, k);
        must_seek_write(
            &engine,
            k,
            u64::max_value(),
            ts(270, 0),
            ts(270, 0),
            WriteType::Rollback,
        );

        must_acquire_pessimistic_lock_for_large_txn(&engine, k, k, ts(280, 0), ts(280, 0), 100);
        must_large_txn_locked(&engine, k, ts(280, 0), 100, 0, true);
        must_check_txn_status(&engine, k, ts(280, 0), ts(281, 0), u64::max_value(), 0, 0);
        must_unlocked(&engine, k);
        must_seek_write(
            &engine,
            k,
            u64::max_value(),
            ts(280, 0),
            ts(280, 0),
            WriteType::Rollback,
        );
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
        let engine = TestEngineBuilder::new().build().unwrap();

        let k = b"k";
        let v = b"v";

        let mut options = Options::default();
        options.lock_ttl = 3;
        options.txn_size = 10;

        let mut expected_lock_info = kvproto::kvrpcpb::LockInfo::default();
        expected_lock_info.set_primary_lock(k.to_vec());
        expected_lock_info.set_lock_version(10);
        expected_lock_info.set_key(k.to_vec());
        expected_lock_info.set_lock_ttl(options.lock_ttl);
        expected_lock_info.set_txn_size(options.txn_size);

        let assert_lock_info_eq = |e, expected_lock_info: &kvproto::kvrpcpb::LockInfo| match e {
            Error::KeyIsLocked(info) => assert_eq!(info, *expected_lock_info),
            _ => panic!("unexpected error"),
        };

        must_prewrite_put_impl(&engine, k, v, k, 10, false, options);

        assert_lock_info_eq(
            must_prewrite_put_err(&engine, k, v, k, 20),
            &expected_lock_info,
        );

        assert_lock_info_eq(
            must_acquire_pessimistic_lock_err(&engine, k, k, 30, 30),
            &expected_lock_info,
        );

        assert_lock_info_eq(must_cleanup_err(&engine, k, 10, 1), &expected_lock_info);

        expected_lock_info.set_lock_ttl(0);
        assert_lock_info_eq(
            must_pessimistic_prewrite_put_err(&engine, k, v, k, 40, 40, false),
            &expected_lock_info,
        );
    }
}
