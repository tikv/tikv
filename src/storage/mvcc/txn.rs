// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::kv::{Modify, ScanMode, Snapshot, Statistics, WriteData};
use crate::storage::mvcc::{
    metrics::*, reader::MvccReader, reader::TxnCommitRecord, ErrorInner, Result,
};
use crate::storage::types::TxnStatus;
use concurrency_manager::{ConcurrencyManager, KeyHandleGuard};
use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE};
use kvproto::kvrpcpb::{ExtraOp, IsolationLevel};
use std::fmt;
use txn_types::{
    Key, Lock, LockType, MutationType, OldValue, TimeStamp, TxnExtra, Value, Write, WriteType,
};

pub const MAX_TXN_WRITE_SIZE: usize = 32 * 1024;

#[derive(Default, Clone, Copy)]
pub struct GcInfo {
    pub found_versions: usize,
    pub deleted_versions: usize,
    pub is_completed: bool,
}

/// Generate the Write record that should be written that means to to perform a specified rollback
/// operation.
pub(crate) fn make_rollback(
    start_ts: TimeStamp,
    protected: bool,
    overlapped_write: Option<Write>,
) -> Option<Write> {
    match overlapped_write {
        Some(write) => {
            assert!(start_ts > write.start_ts);
            if protected {
                Some(write.set_overlapped_rollback(true))
            } else {
                // No need to update the original write.
                None
            }
        }
        None => Some(Write::new_rollback(start_ts, protected)),
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum MissingLockAction {
    Rollback,
    ProtectedRollback,
    ReturnError,
}

impl MissingLockAction {
    fn rollback_protect(protect_rollback: bool) -> MissingLockAction {
        if protect_rollback {
            MissingLockAction::ProtectedRollback
        } else {
            MissingLockAction::Rollback
        }
    }

    pub(crate) fn rollback(rollback_if_not_exist: bool) -> MissingLockAction {
        if rollback_if_not_exist {
            MissingLockAction::ProtectedRollback
        } else {
            MissingLockAction::ReturnError
        }
    }

    fn construct_write(&self, ts: TimeStamp, overlapped_write: Option<Write>) -> Option<Write> {
        match self {
            MissingLockAction::Rollback => make_rollback(ts, false, overlapped_write),
            MissingLockAction::ProtectedRollback => make_rollback(ts, true, overlapped_write),
            _ => unreachable!(),
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

#[derive(Debug, PartialEq)]
pub enum SecondaryLockStatus {
    Locked(Lock),
    Committed(TimeStamp),
    RolledBack,
}

/// An abstraction of a locally-transactional MVCC key-value store
pub struct MvccTxn<S: Snapshot> {
    pub(crate) reader: MvccReader<S>,
    pub(crate) start_ts: TimeStamp,
    write_size: usize,
    writes: WriteData,
    // collapse continuous rollbacks.
    pub(crate) collapse_rollback: bool,
    pub extra_op: ExtraOp,
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

impl<S: Snapshot> MvccTxn<S> {
    pub fn new(
        snapshot: S,
        start_ts: TimeStamp,
        fill_cache: bool,
        concurrency_manager: ConcurrencyManager,
    ) -> MvccTxn<S> {
        // FIXME: use session variable to indicate fill cache or not.

        // ScanMode is `None`, since in prewrite and other operations, keys are not given in
        // order and we use prefix seek for each key. An exception is GC, which uses forward
        // scan only.
        // IsolationLevel is `Si`, actually the method we use in MvccTxn does not rely on
        // isolation level, so it can be any value.
        Self::from_reader(
            MvccReader::new(snapshot, None, fill_cache, IsolationLevel::Si),
            start_ts,
            concurrency_manager,
        )
    }

    // Use `ScanMode::Forward` when gc.
    // When `scan_mode` is `Some(ScanMode::Forward)`, all keys must be written by
    // in ascending order.
    pub fn for_scan(
        snapshot: S,
        scan_mode: Option<ScanMode>,
        start_ts: TimeStamp,
        fill_cache: bool,
        concurrency_manager: ConcurrencyManager,
    ) -> MvccTxn<S> {
        Self::from_reader(
            MvccReader::new(snapshot, scan_mode, fill_cache, IsolationLevel::Si),
            start_ts,
            concurrency_manager,
        )
    }

    fn from_reader(
        reader: MvccReader<S>,
        start_ts: TimeStamp,
        concurrency_manager: ConcurrencyManager,
    ) -> MvccTxn<S> {
        MvccTxn {
            reader,
            start_ts,
            write_size: 0,
            writes: WriteData::default(),
            collapse_rollback: true,
            extra_op: ExtraOp::Noop,
            concurrency_manager,
            guards: vec![],
        }
    }

    pub fn collapse_rollback(&mut self, collapse: bool) {
        self.collapse_rollback = collapse;
    }

    pub fn set_start_ts(&mut self, start_ts: TimeStamp) {
        self.start_ts = start_ts;
    }

    pub fn into_modifies(self) -> Vec<Modify> {
        self.writes.modifies
    }

    pub fn take_extra(&mut self) -> TxnExtra {
        std::mem::take(&mut self.writes.extra)
    }

    pub fn take_guards(&mut self) -> Vec<KeyHandleGuard> {
        std::mem::take(&mut self.guards)
    }

    pub fn take_statistics(&mut self) -> Statistics {
        let mut statistics = Statistics::default();
        self.reader.collect_statistics_into(&mut statistics);
        statistics
    }

    pub fn write_size(&self) -> usize {
        self.write_size
    }

    pub(crate) fn put_lock(&mut self, key: Key, lock: &Lock) {
        let write = Modify::Put(CF_LOCK, key, lock.to_bytes());
        self.write_size += write.size();
        self.writes.modifies.push(write);
    }

    pub(crate) fn unlock_key(&mut self, key: Key, pessimistic: bool) -> Option<ReleasedLock> {
        let released = ReleasedLock::new(&key, pessimistic);
        let write = Modify::Delete(CF_LOCK, key);
        self.write_size += write.size();
        self.writes.modifies.push(write);
        Some(released)
    }

    pub(crate) fn put_value(&mut self, key: Key, ts: TimeStamp, value: Value) {
        let write = Modify::Put(CF_DEFAULT, key.append_ts(ts), value);
        self.write_size += write.size();
        self.writes.modifies.push(write);
    }

    fn delete_value(&mut self, key: Key, ts: TimeStamp) {
        let write = Modify::Delete(CF_DEFAULT, key.append_ts(ts));
        self.write_size += write.size();
        self.writes.modifies.push(write);
    }

    pub(crate) fn put_write(&mut self, key: Key, ts: TimeStamp, value: Value) {
        let write = Modify::Put(CF_WRITE, key.append_ts(ts), value);
        self.write_size += write.size();
        self.writes.modifies.push(write);
    }

    fn delete_write(&mut self, key: Key, ts: TimeStamp) {
        let write = Modify::Delete(CF_WRITE, key.append_ts(ts));
        self.write_size += write.size();
        self.writes.modifies.push(write);
    }

    fn key_exist(&mut self, key: &Key, ts: TimeStamp) -> Result<bool> {
        Ok(self.reader.get_write(&key, ts)?.is_some())
    }
    // Check whether there's an overlapped write record, and then perform rollback. The actual behavior
    // to do the rollback differs according to whether there's an overlapped write record.
    pub(crate) fn check_write_and_rollback_lock(
        &mut self,
        key: Key,
        lock: &Lock,
        is_pessimistic_txn: bool,
    ) -> Result<Option<ReleasedLock>> {
        let overlapped_write = self
            .reader
            .get_txn_commit_record(&key, self.start_ts)?
            .unwrap_none();
        self.rollback_lock(key, lock, is_pessimistic_txn, overlapped_write)
    }

    fn rollback_lock(
        &mut self,
        key: Key,
        lock: &Lock,
        is_pessimistic_txn: bool,
        overlapped_write: Option<Write>,
    ) -> Result<Option<ReleasedLock>> {
        // If prewrite type is DEL or LOCK or PESSIMISTIC, it is no need to delete value.
        if lock.short_value.is_none() && lock.lock_type == LockType::Put {
            self.delete_value(key.clone(), lock.ts);
        }

        // Only the primary key of a pessimistic transaction needs to be protected.
        let protected: bool = is_pessimistic_txn && key.is_encoded_from(&lock.primary);
        if let Some(write) = make_rollback(self.start_ts, protected, overlapped_write) {
            self.put_write(key.clone(), self.start_ts, write.as_ref().to_bytes());
        }
        if self.collapse_rollback {
            self.collapse_prev_rollback(key.clone())?;
        }
        Ok(self.unlock_key(key, is_pessimistic_txn))
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

    /// Checks the existence of the key according to `should_not_exist`.
    /// If not, returns an `AlreadyExist` error.
    pub(crate) fn check_data_constraint(
        &mut self,
        should_not_exist: bool,
        write: &Write,
        write_commit_ts: TimeStamp,
        key: &Key,
    ) -> Result<()> {
        if !should_not_exist || write.write_type == WriteType::Delete {
            return Ok(());
        }

        // The current key exists under any of the following conditions:
        // 1.The current write type is `PUT`
        // 2.The current write type is `Rollback` or `Lock`, and the key have an older version.
        if write.write_type == WriteType::Put || self.key_exist(&key, write_commit_ts.prev())? {
            return Err(ErrorInner::AlreadyExist { key: key.to_raw()? }.into());
        }

        Ok(())
    }

    // Pessimistic transactions only acquire pessimistic locks on row keys.
    // The corresponding index keys are not locked until pessimistic prewrite.
    // It's possible that lock conflict occurs on them, but the isolation is
    // guaranteed by pessimistic locks on row keys, so let TiDB resolves these
    // locks immediately.
    pub(crate) fn handle_non_pessimistic_lock_conflict(&self, key: Key, lock: Lock) -> Result<()> {
        // The previous pessimistic transaction has been committed or aborted.
        // Resolve it immediately.
        //
        // Because the row key is locked, the optimistic transaction will
        // abort. Resolve it immediately.
        let mut info = lock.into_lock_info(key.into_raw()?);
        // Set ttl to 0 so TiDB will resolve lock immediately.
        info.set_lock_ttl(0);
        Err(ErrorInner::KeyIsLocked(info).into())
    }

    pub fn acquire_pessimistic_lock(
        &mut self,
        key: Key,
        primary: &[u8],
        should_not_exist: bool,
        lock_ttl: u64,
        for_update_ts: TimeStamp,
        need_value: bool,
        min_commit_ts: TimeStamp,
    ) -> Result<Option<Value>> {
        fail_point!("acquire_pessimistic_lock", |err| Err(make_txn_error(
            err,
            &key,
            self.start_ts,
        )
        .into()));

        fn pessimistic_lock(
            primary: &[u8],
            start_ts: TimeStamp,
            lock_ttl: u64,
            for_update_ts: TimeStamp,
            min_commit_ts: TimeStamp,
        ) -> Lock {
            Lock::new(
                LockType::Pessimistic,
                primary.to_vec(),
                start_ts,
                lock_ttl,
                None,
                for_update_ts,
                0,
                min_commit_ts,
            )
        }

        let mut val = None;
        if let Some(lock) = self.reader.load_lock(&key)? {
            if lock.ts != self.start_ts {
                return Err(ErrorInner::KeyIsLocked(lock.into_lock_info(key.into_raw()?)).into());
            }
            if lock.lock_type != LockType::Pessimistic {
                return Err(ErrorInner::LockTypeNotMatch {
                    start_ts: self.start_ts,
                    key: key.into_raw()?,
                    pessimistic: false,
                }
                .into());
            }
            if need_value {
                val = self.reader.get(&key, for_update_ts, true)?;
            }
            // Overwrite the lock with small for_update_ts
            if for_update_ts > lock.for_update_ts {
                let lock = pessimistic_lock(
                    primary,
                    self.start_ts,
                    lock_ttl,
                    for_update_ts,
                    min_commit_ts,
                );
                self.put_lock(key, &lock);
            } else {
                MVCC_DUPLICATE_CMD_COUNTER_VEC
                    .acquire_pessimistic_lock
                    .inc();
            }
            return Ok(val);
        }

        if let Some((commit_ts, write)) = self.reader.seek_write(&key, TimeStamp::max())? {
            // The isolation level of pessimistic transactions is RC. `for_update_ts` is
            // the commit_ts of the data this transaction read. If exists a commit version
            // whose commit timestamp is larger than current `for_update_ts`, the
            // transaction should retry to get the latest data.
            if commit_ts > for_update_ts {
                MVCC_CONFLICT_COUNTER
                    .acquire_pessimistic_lock_conflict
                    .inc();
                return Err(ErrorInner::WriteConflict {
                    start_ts: self.start_ts,
                    conflict_start_ts: write.start_ts,
                    conflict_commit_ts: commit_ts,
                    key: key.into_raw()?,
                    primary: primary.to_vec(),
                }
                .into());
            }

            // Handle rollback.
            // The rollack informathin may come from either a Rollback record or a record with
            // `has_overlapped_rollback` flag.
            if commit_ts == self.start_ts
                && (write.write_type == WriteType::Rollback || write.has_overlapped_rollback)
            {
                assert!(write.has_overlapped_rollback || write.start_ts == commit_ts);
                return Err(ErrorInner::PessimisticLockRolledBack {
                    start_ts: self.start_ts,
                    key: key.into_raw()?,
                }
                .into());
            }
            // If `commit_ts` we seek is already before `start_ts`, the rollback must not exist.
            if commit_ts > self.start_ts {
                if let Some((commit_ts, write)) = self.reader.seek_write(&key, self.start_ts)? {
                    if write.start_ts == self.start_ts {
                        assert!(
                            commit_ts == self.start_ts && write.write_type == WriteType::Rollback
                        );
                        return Err(ErrorInner::PessimisticLockRolledBack {
                            start_ts: self.start_ts,
                            key: key.into_raw()?,
                        }
                        .into());
                    }
                }
            }

            // Check data constraint when acquiring pessimistic lock.
            self.check_data_constraint(should_not_exist, &write, commit_ts, &key)?;

            if need_value {
                val = match write.write_type {
                    // If it's a valid Write, no need to read again.
                    WriteType::Put => Some(self.reader.load_data(&key, write)?),
                    WriteType::Delete => None,
                    WriteType::Lock | WriteType::Rollback => {
                        self.reader.get(&key, commit_ts.prev(), true)?
                    }
                };
            }
        }

        let lock = pessimistic_lock(
            primary,
            self.start_ts,
            lock_ttl,
            for_update_ts,
            min_commit_ts,
        );
        self.put_lock(key, &lock);

        Ok(val)
    }

    // TiKV may fails to write pessimistic locks due to pipelined process.
    // If the data is not changed after acquiring the lock, we can still prewrite the key.
    pub(crate) fn amend_pessimistic_lock(
        &mut self,
        pipelined_pessimistic_lock: bool,
        key: &Key,
    ) -> Result<()> {
        if !pipelined_pessimistic_lock {
            // Pessimistic lock does not exist, the transaction should be aborted.
            warn!(
                "prewrite failed (pessimistic lock not found)";
                "start_ts" => self.start_ts,
                "key" => %key
            );
            return Err(ErrorInner::PessimisticLockNotFound {
                start_ts: self.start_ts,
                key: key.clone().into_raw()?,
            }
            .into());
        }
        if let Some((commit_ts, _)) = self.reader.seek_write(key, TimeStamp::max())? {
            // The invariants of pessimistic locks are:
            //   1. lock's for_update_ts >= key's latest commit_ts
            //   2. lock's for_update_ts >= txn's start_ts
            //   3. If the data is changed after acquiring the pessimistic lock, key's new commit_ts > lock's for_update_ts
            //
            // So, if the key's latest commit_ts is still less than or equal to lock's for_update_ts, the data is not changed.
            // However, we can't get lock's for_update_ts in current implementation (txn's for_update_ts is updated for each DML),
            // we can only use txn's start_ts to check -- If the key's commit_ts is less than txn's start_ts, it's less than
            // lock's for_update_ts too.
            if commit_ts >= self.start_ts {
                warn!(
                    "prewrite failed (pessimistic lock not found)";
                    "start_ts" => self.start_ts,
                    "commit_ts" => commit_ts,
                    "key" => %key
                );
                MVCC_CONFLICT_COUNTER
                    .pipelined_acquire_pessimistic_lock_amend_fail
                    .inc();
                return Err(ErrorInner::PessimisticLockNotFound {
                    start_ts: self.start_ts,
                    key: key.clone().into_raw()?,
                }
                .into());
            }
        }
        // Used pipelined pessimistic lock acquiring in this txn but failed
        // Luckily no other txn modified this lock, amend it by treat it as optimistic txn.
        MVCC_CONFLICT_COUNTER
            .pipelined_acquire_pessimistic_lock_amend_success
            .inc();
        Ok(())
    }

    pub fn rollback(&mut self, key: Key) -> Result<Option<ReleasedLock>> {
        fail_point!("rollback", |err| Err(make_txn_error(
            err,
            &key,
            self.start_ts,
        )
        .into()));

        // Rollback is called only if the transaction is known to fail. Under the circumstances,
        // the rollback record needn't be protected.
        self.cleanup(key, TimeStamp::zero(), false)
    }

    pub(crate) fn check_txn_status_missing_lock(
        &mut self,
        primary_key: Key,
        mismatch_lock: Option<Lock>,
        action: MissingLockAction,
    ) -> Result<TxnStatus> {
        MVCC_CHECK_TXN_STATUS_COUNTER_VEC.get_commit_info.inc();

        match self
            .reader
            .get_txn_commit_record(&primary_key, self.start_ts)?
        {
            TxnCommitRecord::SingleRecord { commit_ts, write } => {
                if write.write_type == WriteType::Rollback {
                    Ok(TxnStatus::RolledBack)
                } else {
                    Ok(TxnStatus::committed(commit_ts))
                }
            }
            TxnCommitRecord::OverlappedRollback { .. } => Ok(TxnStatus::RolledBack),
            TxnCommitRecord::None { overlapped_write } => {
                if MissingLockAction::ReturnError == action {
                    return Err(ErrorInner::TxnNotFound {
                        start_ts: self.start_ts,
                        key: primary_key.into_raw()?,
                    }
                    .into());
                }

                let ts = self.start_ts;

                // collapse previous rollback if exist.
                if self.collapse_rollback {
                    self.collapse_prev_rollback(primary_key.clone())?;
                }

                if let (Some(l), None) = (mismatch_lock, overlapped_write.as_ref()) {
                    self.mark_rollback_on_mismatching_lock(
                        &primary_key,
                        l,
                        action == MissingLockAction::ProtectedRollback,
                    );
                }

                // Insert a Rollback to Write CF in case that a stale prewrite
                // command is received after a cleanup command.
                if let Some(write) = action.construct_write(ts, overlapped_write) {
                    self.put_write(primary_key, ts, write.as_ref().to_bytes());
                }
                MVCC_CHECK_TXN_STATUS_COUNTER_VEC.rollback.inc();

                Ok(TxnStatus::LockNotExist)
            }
        }
    }

    /// Cleanup the lock if it's TTL has expired, comparing with `current_ts`. If `current_ts` is 0,
    /// cleanup the lock without checking TTL. If the lock is the primary lock of a pessimistic
    /// transaction, the rollback record is protected from being collapsed.
    ///
    /// Returns the released lock. Returns error if the key is locked or has already been
    /// committed.
    pub fn cleanup(
        &mut self,
        key: Key,
        current_ts: TimeStamp,
        protect_rollback: bool,
    ) -> Result<Option<ReleasedLock>> {
        fail_point!("cleanup", |err| Err(make_txn_error(
            err,
            &key,
            self.start_ts,
        )
        .into()));

        match self.reader.load_lock(&key)? {
            Some(ref lock) if lock.ts == self.start_ts => {
                // If current_ts is not 0, check the Lock's TTL.
                // If the lock is not expired, do not rollback it but report key is locked.
                if !current_ts.is_zero() && lock.ts.physical() + lock.ttl >= current_ts.physical() {
                    return Err(ErrorInner::KeyIsLocked(
                        lock.clone().into_lock_info(key.into_raw()?),
                    )
                    .into());
                }

                let is_pessimistic_txn = !lock.for_update_ts.is_zero();
                self.check_write_and_rollback_lock(key, lock, is_pessimistic_txn)
            }
            l => match self.check_txn_status_missing_lock(
                key,
                l,
                MissingLockAction::rollback_protect(protect_rollback),
            )? {
                TxnStatus::Committed { commit_ts } => {
                    MVCC_CONFLICT_COUNTER.rollback_committed.inc();
                    Err(ErrorInner::Committed { commit_ts }.into())
                }
                TxnStatus::RolledBack => {
                    // Return Ok on Rollback already exist.
                    MVCC_DUPLICATE_CMD_COUNTER_VEC.rollback.inc();
                    Ok(None)
                }
                TxnStatus::LockNotExist => Ok(None),
                _ => unreachable!(),
            },
        }
    }

    pub(crate) fn collapse_prev_rollback(&mut self, key: Key) -> Result<()> {
        if let Some((commit_ts, write)) = self.reader.seek_write(&key, self.start_ts)? {
            if write.write_type == WriteType::Rollback && !write.as_ref().is_protected() {
                self.delete_write(key, commit_ts);
            }
        }
        Ok(())
    }

    pub fn gc(&mut self, key: Key, safe_point: TimeStamp) -> Result<GcInfo> {
        let mut remove_older = false;
        let mut ts = TimeStamp::max();
        let mut found_versions = 0;
        let mut deleted_versions = 0;
        let mut latest_delete = None;
        let mut is_completed = true;
        while let Some((commit, write)) = self.reader.seek_write(&key, ts)? {
            ts = commit.prev();
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

    // Check and execute the extra operation.
    // Currently we use it only for reading the old value for CDC.
    pub fn check_extra_op(
        &mut self,
        key: &Key,
        mutation_type: MutationType,
        prev_write: Option<Write>,
    ) -> Result<()> {
        use crate::storage::mvcc::reader::seek_for_valid_write;

        if self.extra_op == ExtraOp::ReadOldValue
            && (mutation_type == MutationType::Put || mutation_type == MutationType::Delete)
        {
            let old_value = if let Some(w) = prev_write {
                // If write is Rollback or Lock, seek for valid write record.
                if w.write_type == WriteType::Rollback || w.write_type == WriteType::Lock {
                    let write_cursor = self.reader.write_cursor.as_mut().unwrap();
                    // Skip the current write record.
                    write_cursor.next(&mut self.reader.statistics.write);
                    let write = seek_for_valid_write(
                        write_cursor,
                        key,
                        self.start_ts,
                        &mut self.reader.statistics,
                    )?;
                    write.map(|w| OldValue {
                        short_value: w.short_value,
                        start_ts: w.start_ts,
                    })
                } else {
                    Some(OldValue {
                        short_value: w.short_value,
                        start_ts: w.start_ts,
                    })
                }
            } else {
                None
            };
            // If write is None or cannot find a previously valid write record.
            self.writes.extra.add_old_value(
                key.clone().append_ts(self.start_ts),
                old_value,
                mutation_type,
            );
        }
        Ok(())
    }
}

impl<S: Snapshot> fmt::Debug for MvccTxn<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "txn @{}", self.start_ts)
    }
}

#[cfg(feature = "failpoints")]
pub(crate) fn make_txn_error(s: Option<String>, key: &Key, start_ts: TimeStamp) -> ErrorInner {
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
                commit_ts: TimeStamp::zero(),
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
mod tests {
    use super::*;

    use crate::storage::kv::{Engine, RocksEngine, TestEngineBuilder};
    use crate::storage::mvcc::tests::*;
    use crate::storage::mvcc::{Error, ErrorInner, Mutation, MvccReader};
    use crate::storage::txn::commands::*;
    use crate::storage::txn::tests::*;
    use crate::storage::txn::{commit, pessimistic_prewrite, prewrite};
    use crate::storage::SecondaryLocksStatus;
    use kvproto::kvrpcpb::Context;
    use txn_types::{TimeStamp, SHORT_VALUE_MAX_LEN};

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
        must_rollback(&engine, k1, 9);
        assert!(try_prewrite_check_not_exists(&engine, k1, k1, 10).is_err());

        // Delete "k1" again
        must_prewrite_delete(&engine, k1, k1, 11);
        must_commit(&engine, k1, 11, 12);

        // Rollback again
        must_prewrite_put(&engine, k1, v3, k1, 13);
        must_rollback(&engine, k1, 13);

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
        must_rollback(&engine, k2, 20);

        let w1r = must_written(&engine, k1, 10, 20, WriteType::Put);
        assert!(w1r.has_overlapped_rollback);
        // The only difference between w1r and w1 is the overlapped_rollback flag.
        assert_eq!(w1r.set_overlapped_rollback(false), w1);

        let w2r = must_written(&engine, k2, 11, 20, WriteType::Put);
        // Rollback is invoked on secondaries, so the rollback is not protected and overlapped_rollback
        // won't be set.
        assert_eq!(w2r, w2);
    }

    #[test]
    fn test_cleanup() {
        // Cleanup's logic is mostly similar to rollback, except the TTL check. Tests that not
        // related to TTL check should be covered by other test cases.
        let engine = TestEngineBuilder::new().build().unwrap();

        // Shorthand for composing ts.
        let ts = TimeStamp::compose;

        let (k, v) = (b"k", b"v");

        must_prewrite_put(&engine, k, v, k, ts(10, 0));
        must_locked(&engine, k, ts(10, 0));
        txn_heart_beat::tests::must_success(&engine, k, ts(10, 0), 100, 100);
        // Check the last txn_heart_beat has set the lock's TTL to 100.
        txn_heart_beat::tests::must_success(&engine, k, ts(10, 0), 90, 100);

        // TTL not expired. Do nothing but returns an error.
        must_cleanup_err(&engine, k, ts(10, 0), ts(20, 0));
        must_locked(&engine, k, ts(10, 0));

        // Try to cleanup another transaction's lock. Does nothing.
        must_cleanup(&engine, k, ts(10, 1), ts(120, 0));
        // If there is no exisiting lock when cleanup, it may be a pessimistic transaction,
        // so the rollback should be protected.
        must_get_rollback_protected(&engine, k, ts(10, 1), true);
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

    fn test_gc_imp<F>(k: &[u8], v1: &[u8], v2: &[u8], v3: &[u8], v4: &[u8], gc: F)
    where
        F: Fn(&RocksEngine, &[u8], u64),
    {
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

        gc(&engine, k, 12);
        must_get(&engine, k, 12, v1);

        gc(&engine, k, 22);
        must_get(&engine, k, 22, v2);
        must_get_none(&engine, k, 12);

        gc(&engine, k, 32);
        must_get_none(&engine, k, 22);
        must_get_none(&engine, k, 35);

        gc(&engine, k, 60);
        must_get(&engine, k, 62, v3);
    }

    #[test]
    fn test_gc() {
        test_gc_imp(b"k1", b"v1", b"v2", b"v3", b"v4", must_gc);

        let v1 = "x".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        let v2 = "y".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        let v3 = "z".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        let v4 = "v".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_gc_imp(b"k2", &v1, &v2, &v3, &v4, must_gc);
    }

    #[test]
    fn test_gc_with_compaction_filter() {
        use crate::server::gc_worker::gc_by_compact;

        test_gc_imp(b"k1", b"v1", b"v2", b"v3", b"v4", gc_by_compact);

        let v1 = "x".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        let v2 = "y".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        let v3 = "z".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        let v4 = "v".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_gc_imp(b"k2", &v1, &v2, &v3, &v4, gc_by_compact);
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
        must_rollback(&engine, k, 15);
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

    fn test_write_size_imp(k: &[u8], v: &[u8], pk: &[u8]) {
        let engine = TestEngineBuilder::new().build().unwrap();
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let cm = ConcurrencyManager::new(10.into());
        let mut txn = MvccTxn::new(snapshot, 10.into(), true, cm.clone());
        let key = Key::from_raw(k);
        assert_eq!(txn.write_size(), 0);

        prewrite(
            &mut txn,
            Mutation::Put((key.clone(), v.to_vec())),
            pk,
            &None,
            false,
            0,
            0,
            TimeStamp::default(),
        )
        .unwrap();
        assert!(txn.write_size() > 0);
        engine
            .write(&ctx, WriteData::from_modifies(txn.into_modifies()))
            .unwrap();

        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, 10.into(), true, cm);
        commit(&mut txn, key, 15.into()).unwrap();
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

        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let cm = ConcurrencyManager::new(10.into());
        let mut txn = MvccTxn::new(snapshot, 5.into(), true, cm.clone());
        assert!(prewrite(
            &mut txn,
            Mutation::Put((Key::from_raw(key), value.to_vec())),
            key,
            &None,
            false,
            0,
            0,
            TimeStamp::default(),
        )
        .is_err());

        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, 5.into(), true, cm);
        assert!(prewrite(
            &mut txn,
            Mutation::Put((Key::from_raw(key), value.to_vec())),
            key,
            &None,
            true,
            0,
            0,
            TimeStamp::default(),
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
        let mut reader =
            MvccReader::new(snapshot, Some(ScanMode::Forward), true, IsolationLevel::Si);

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

        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut reader =
            MvccReader::new(snapshot, Some(ScanMode::Forward), true, IsolationLevel::Si);

        assert_eq!(
            reader.seek_ts(3.into()).unwrap().unwrap(),
            Key::from_raw(&[2])
        );
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
        must_prewrite_put_err(&engine, k, v, k, 24);
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
        pessimistic_rollback::tests::must_success(&engine, k, 26, 29);
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
        pessimistic_rollback::tests::must_success(&engine, k, 32, 34);
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
        must_commit(&engine, k, 46, 50);
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

        // Overlapped rollback record will be written when the current start_ts equals to another write
        // records' commit ts. Now there is a commit record with commit_ts = 50.
        must_acquire_pessimistic_lock(&engine, k, k, 50, 61);
        must_pessimistic_prewrite_put(&engine, k, v, k, 50, 61, true);
        must_locked(&engine, k, 50);
        must_cleanup(&engine, k, 50, 0);
        must_get_overlapped_rollback(&engine, k, 50, 46, WriteType::Put);

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
    fn test_pessimistic_txn_ttl() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let (k, v) = (b"k", b"v");

        // Pessimistic prewrite keeps the larger TTL of the prewrite request and the original
        // pessimisitic lock.
        must_acquire_pessimistic_lock_with_ttl(&engine, k, k, 10, 10, 100);
        must_pessimistic_locked(&engine, k, 10, 10);
        must_pessimistic_prewrite_put_with_ttl(&engine, k, v, k, 10, 10, true, 110);
        must_locked_with_ttl(&engine, k, 10, 110);

        must_rollback(&engine, k, 10);

        // TTL not changed if the pessimistic lock's TTL is larger than that provided in the
        // prewrite request.
        must_acquire_pessimistic_lock_with_ttl(&engine, k, k, 20, 20, 100);
        must_pessimistic_locked(&engine, k, 20, 20);
        must_pessimistic_prewrite_put_with_ttl(&engine, k, v, k, 20, 20, true, 90);
        must_locked_with_ttl(&engine, k, 20, 100);
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
                    false,
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
                    expected_lock_info.get_lock_ttl(),
                    expected_lock_info.get_lock_for_update_ts(),
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
                must_rollback(&engine, k, expected_lock_info.get_lock_version());
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
        must_rollback(&engine, k1, 10);

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
    fn test_pessimistic_lock_return_value() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let (k, v) = (b"k", b"v");

        assert_eq!(
            must_acquire_pessimistic_lock_return_value(&engine, k, k, 10, 10),
            None
        );
        must_pessimistic_locked(&engine, k, 10, 10);
        pessimistic_rollback::tests::must_success(&engine, k, 10, 10);

        // Put
        must_prewrite_put(&engine, k, v, k, 10);
        // KeyIsLocked
        match must_acquire_pessimistic_lock_return_value_err(&engine, k, k, 20, 20) {
            Error(box ErrorInner::KeyIsLocked(_)) => (),
            e => panic!("unexpected error: {}", e),
        };
        must_commit(&engine, k, 10, 20);
        // WriteConflict
        match must_acquire_pessimistic_lock_return_value_err(&engine, k, k, 15, 15) {
            Error(box ErrorInner::WriteConflict { .. }) => (),
            e => panic!("unexpected error: {}", e),
        };
        assert_eq!(
            must_acquire_pessimistic_lock_return_value(&engine, k, k, 25, 25),
            Some(v.to_vec())
        );
        must_pessimistic_locked(&engine, k, 25, 25);
        pessimistic_rollback::tests::must_success(&engine, k, 25, 25);

        // Skip Write::Lock
        must_prewrite_lock(&engine, k, k, 30);
        must_commit(&engine, k, 30, 40);
        assert_eq!(
            must_acquire_pessimistic_lock_return_value(&engine, k, k, 45, 45),
            Some(v.to_vec())
        );
        must_pessimistic_locked(&engine, k, 45, 45);
        pessimistic_rollback::tests::must_success(&engine, k, 45, 45);

        // Skip Write::Rollback
        must_rollback(&engine, k, 50);
        assert_eq!(
            must_acquire_pessimistic_lock_return_value(&engine, k, k, 55, 55),
            Some(v.to_vec())
        );
        must_pessimistic_locked(&engine, k, 55, 55);
        pessimistic_rollback::tests::must_success(&engine, k, 55, 55);

        // Delete
        must_prewrite_delete(&engine, k, k, 60);
        must_commit(&engine, k, 60, 70);
        assert_eq!(
            must_acquire_pessimistic_lock_return_value(&engine, k, k, 75, 75),
            None
        );
        // Duplicated command
        assert_eq!(
            must_acquire_pessimistic_lock_return_value(&engine, k, k, 75, 75),
            None
        );
        assert_eq!(
            must_acquire_pessimistic_lock_return_value(&engine, k, k, 75, 55),
            Some(v.to_vec())
        );
        must_pessimistic_locked(&engine, k, 75, 75);
        pessimistic_rollback::tests::must_success(&engine, k, 75, 75);
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
        must_pipelined_pessimistic_prewrite_put(&engine, k, &v, k, 10, 10, true);
        must_commit(&engine, k, 10, 20);
        must_get(&engine, k, 20, &v);

        // for_update_ts(30) >= start_ts(30) > commit_ts(20); should succeed.
        v.push(0);
        fail_to_write_pessimistic_lock(&engine, k, 30, 30);
        must_pipelined_pessimistic_prewrite_put(&engine, k, &v, k, 30, 30, true);
        must_commit(&engine, k, 30, 40);
        must_get(&engine, k, 40, &v);

        // for_update_ts(40) >= commit_ts(40) > start_ts(35); should fail.
        fail_to_write_pessimistic_lock(&engine, k, 35, 40);
        must_pipelined_pessimistic_prewrite_put_err(&engine, k, &v, k, 35, 40, true);

        // KeyIsLocked; should fail.
        must_acquire_pessimistic_lock(&engine, k, k, 50, 50);
        must_pipelined_pessimistic_prewrite_put_err(&engine, k, &v, k, 60, 60, true);
        pessimistic_rollback::tests::must_success(&engine, k, 50, 50);

        // Pessimistic lock not exist and not pipelined; should fail.
        must_pessimistic_prewrite_put_err(&engine, k, &v, k, 70, 70, true);

        // The txn has been rolled back; should fail.
        must_acquire_pessimistic_lock(&engine, k, k, 80, 80);
        must_cleanup(&engine, k, 80, TimeStamp::max());
        must_pipelined_pessimistic_prewrite_put_err(&engine, k, &v, k, 80, 80, true);
    }

    #[test]
    fn test_extra_op_old_value() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let key = Key::from_raw(b"key");
        let ctx = Context::default();

        let new_old_value = |short_value, start_ts| OldValue {
            short_value,
            start_ts,
        };

        let cases = vec![
            (
                Mutation::Put((key.clone(), b"v0".to_vec())),
                false,
                5,
                5,
                None,
                true,
            ),
            (
                Mutation::Put((key.clone(), b"v1".to_vec())),
                false,
                6,
                6,
                Some(new_old_value(Some(b"v0".to_vec()), 5.into())),
                true,
            ),
            (Mutation::Lock(key.clone()), false, 7, 7, None, false),
            (
                Mutation::Lock(key.clone()),
                false,
                8,
                8,
                Some(new_old_value(Some(b"v1".to_vec()), 6.into())),
                false,
            ),
            (
                Mutation::Put((key.clone(), vec![b'0'; 5120])),
                false,
                9,
                9,
                Some(new_old_value(Some(b"v1".to_vec()), 6.into())),
                true,
            ),
            (
                Mutation::Put((key.clone(), b"v3".to_vec())),
                false,
                10,
                10,
                Some(new_old_value(None, 9.into())),
                true,
            ),
            (
                Mutation::Put((key.clone(), b"v4".to_vec())),
                true,
                11,
                11,
                None,
                true,
            ),
        ];

        let write = |modifies| {
            engine.write(&ctx, modifies).unwrap();
        };

        let new_txn = |start_ts, cm| {
            let snapshot = engine.snapshot(&ctx).unwrap();
            MvccTxn::new(snapshot, start_ts, true, cm)
        };

        for case in cases {
            let (mutation, is_pessimistic, start_ts, commit_ts, old_value, check_old_value) = case;
            let mutation_type = mutation.mutation_type();
            let cm = ConcurrencyManager::new(start_ts.into());
            let mut txn = new_txn(start_ts.into(), cm.clone());

            txn.extra_op = ExtraOp::ReadOldValue;
            if is_pessimistic {
                txn.acquire_pessimistic_lock(
                    key.clone(),
                    b"key",
                    false,
                    0,
                    start_ts.into(),
                    false,
                    TimeStamp::zero(),
                )
                .unwrap();
                write(WriteData::from_modifies(txn.into_modifies()));
                txn = new_txn(start_ts.into(), cm.clone());
                txn.extra_op = ExtraOp::ReadOldValue;
                pessimistic_prewrite(
                    &mut txn,
                    mutation,
                    b"key",
                    &None,
                    true,
                    0,
                    start_ts.into(),
                    0,
                    TimeStamp::zero(),
                    false,
                )
                .unwrap();
            } else {
                prewrite(
                    &mut txn,
                    mutation,
                    b"key",
                    &None,
                    false,
                    0,
                    0,
                    TimeStamp::default(),
                )
                .unwrap();
            }
            if check_old_value {
                let extra = txn.take_extra();
                let ts_key = key.clone().append_ts(start_ts.into());
                assert!(
                    extra.old_values.get(&ts_key).is_some(),
                    "{}@{}",
                    ts_key,
                    start_ts
                );
                assert_eq!(extra.old_values[&ts_key], (old_value, mutation_type));
            }
            write(WriteData::from_modifies(txn.into_modifies()));
            let mut txn = new_txn(start_ts.into(), cm);
            commit(&mut txn, key.clone(), commit_ts.into()).unwrap();
            engine
                .write(&ctx, WriteData::from_modifies(txn.into_modifies()))
                .unwrap();
        }
    }

    #[test]
    fn test_async_prewrite_primary() {
        // copy must_prewrite_put_impl, check that the key is written with the correct secondaries and the right timestamp

        let engine = TestEngineBuilder::new().build().unwrap();
        let ctx = Context::default();
        let cm = ConcurrencyManager::new(42.into());

        let do_prewrite = || {
            let snapshot = engine.snapshot(&ctx).unwrap();
            let mut txn = MvccTxn::new(snapshot, TimeStamp::new(2), true, cm.clone());
            let mutation = Mutation::Put((Key::from_raw(b"key"), b"value".to_vec()));
            let min_commit_ts = prewrite(
                &mut txn,
                mutation,
                b"key",
                &Some(vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()]),
                false,
                0,
                4,
                TimeStamp::zero(),
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

        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, IsolationLevel::Si);
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
            let snapshot = engine.snapshot(&ctx).unwrap();
            let mut txn = MvccTxn::new(snapshot, TimeStamp::new(2), true, cm.clone());
            let mutation = Mutation::Put((Key::from_raw(b"key"), b"value".to_vec()));
            let min_commit_ts = pessimistic_prewrite(
                &mut txn,
                mutation,
                b"key",
                &Some(vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()]),
                true,
                0,
                4.into(),
                4,
                TimeStamp::zero(),
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

        assert_eq!(do_pessimistic_prewrite(), 43.into());

        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, IsolationLevel::Si);
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
        let ctx = Context::default();
        let cm = ConcurrencyManager::new(42.into());

        // Simulate that min_commit_ts is pushed forward larger than latest_ts
        must_acquire_pessimistic_lock_impl(&engine, b"key", b"key", 2, 20000, 2, false, 100);

        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, TimeStamp::new(2), true, cm);
        let mutation = Mutation::Put((Key::from_raw(b"key"), b"value".to_vec()));
        let min_commit_ts = pessimistic_prewrite(
            &mut txn,
            mutation,
            b"key",
            &Some(vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()]),
            true,
            0,
            4.into(),
            4,
            TimeStamp::zero(),
            false,
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

        must_prewrite_put_async_commit(&engine, k, v, k, &Some(vec![]), 20, 0);
        check_txn_status::tests::must_success(&engine, k, 25, 0, 0, true, |s| {
            s == TxnStatus::LockNotExist
        });
        must_commit(&engine, k, 20, 25);
        let w = must_written(&engine, k, 20, 25, WriteType::Put);
        assert!(w.has_overlapped_rollback);

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
}
