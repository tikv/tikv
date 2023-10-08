// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::cmp;

use fail::fail_point;
use kvproto::kvrpcpb::{
    self, Assertion, AssertionLevel,
    PrewriteRequestPessimisticAction::{self, *},
    WriteConflictReason,
};
use txn_types::{
    is_short_value, Key, LastChange, Mutation, MutationType, OldValue, TimeStamp, Value, Write,
    WriteType,
};

use crate::storage::{
    mvcc::{
        metrics::{
            MVCC_CONFLICT_COUNTER, MVCC_DUPLICATE_CMD_COUNTER_VEC,
            MVCC_PREWRITE_ASSERTION_PERF_COUNTER_VEC,
        },
        Error, ErrorInner, Lock, LockType, MvccTxn, PessimisticLockNotFoundReason, Result,
        SnapshotReader,
    },
    txn::{
        actions::{check_data_constraint::check_data_constraint, common::next_last_change_info},
        sched_pool::tls_can_enable,
        scheduler::LAST_CHANGE_TS,
        LockInfo,
    },
    Snapshot,
};

/// Prewrite a single mutation by creating and storing a lock and value.
pub fn prewrite<S: Snapshot>(
    txn: &mut MvccTxn,
    reader: &mut SnapshotReader<S>,
    txn_props: &TransactionProperties<'_>,
    mutation: Mutation,
    secondary_keys: &Option<Vec<Vec<u8>>>,
    pessimistic_action: PrewriteRequestPessimisticAction,
    expected_for_update_ts: Option<TimeStamp>,
) -> Result<(TimeStamp, OldValue)> {
    let mut mutation =
        PrewriteMutation::from_mutation(mutation, secondary_keys, pessimistic_action, txn_props)?;

    // Update max_ts for Insert operation to guarantee linearizability and snapshot
    // isolation
    if mutation.should_not_exist {
        txn.concurrency_manager.update_max_ts(txn_props.start_ts);
    }

    fail_point!(
        if txn_props.is_pessimistic() {
            "pessimistic_prewrite"
        } else {
            "prewrite"
        },
        |err| Err(crate::storage::mvcc::txn::make_txn_error(
            err,
            &mutation.key,
            mutation.txn_props.start_ts
        )
        .into())
    );

    let mut lock_amended = false;

    let lock_status = match reader.load_lock(&mutation.key)? {
        Some(lock) => mutation.check_lock(lock, pessimistic_action, expected_for_update_ts)?,
        None if matches!(pessimistic_action, DoPessimisticCheck) => {
            amend_pessimistic_lock(&mut mutation, reader)?;
            lock_amended = true;
            LockStatus::None
        }
        None => LockStatus::None,
    };

    if let LockStatus::Locked(ts) = lock_status {
        return Ok((ts, OldValue::Unspecified));
    }

    // Note that the `prev_write` may have invalid GC fence.
    let (mut prev_write, mut prev_write_loaded) = if !mutation.skip_constraint_check() {
        (mutation.check_for_newer_version(reader)?, true)
    } else {
        (None, false)
    };

    // Check assertion if necessary. There are couple of different cases:
    // * If the write is already loaded, then assertion can be checked without
    //   introducing too much performance overhead. So do assertion in this case.
    // * If `amend_pessimistic_lock` has happened, assertion can be done during
    //   amending. Skip it.
    // * If constraint check is skipped thus `prev_write` is not loaded, doing
    //   assertion here introduces too much overhead. However, we'll do it anyway if
    //   `assertion_level` is set to `Strict` level.
    // Assertion level will be checked within the `check_assertion` function.
    if !lock_amended {
        let (reloaded_prev_write, reloaded) =
            mutation.check_assertion(reader, &prev_write, prev_write_loaded)?;
        if reloaded {
            prev_write = reloaded_prev_write;
            prev_write_loaded = true;
        }
    }

    let prev_write = prev_write.map(|(w, _)| w);

    if mutation.should_not_write {
        // `checkNotExists` is equivalent to a get operation, so it should update the
        // max_ts.
        txn.concurrency_manager.update_max_ts(txn_props.start_ts);
        let min_commit_ts = if mutation.need_min_commit_ts() {
            // Don't calculate the min_commit_ts according to the concurrency manager's
            // max_ts for a should_not_write mutation because it's not persisted and doesn't
            // change data.
            cmp::max(txn_props.min_commit_ts, txn_props.start_ts.next())
        } else {
            TimeStamp::zero()
        };
        return Ok((min_commit_ts, OldValue::Unspecified));
    }

    let old_value = if txn_props.need_old_value
        && matches!(
            mutation.mutation_type,
            // Only Put, Delete and Insert may have old value.
            MutationType::Put | MutationType::Delete | MutationType::Insert
        ) {
        if mutation.mutation_type == MutationType::Insert {
            // The previous write of an Insert is guaranteed to be None.
            OldValue::None
        } else if mutation.skip_constraint_check() {
            if mutation.txn_props.is_pessimistic() {
                // Pessimistic transaction always skip constraint check in
                // "prewrite" stage, as it checks constraint in
                // "acquire pessimistic lock" stage.
                OldValue::Unspecified
            } else {
                // In optimistic transaction, caller ensures that there is no
                // previous write for the mutation, so there is no old value.
                //
                // FIXME: This may not hold when prewrite request set
                // skip_constraint_check explicitly. For now, no one sets it.
                OldValue::None
            }
        } else {
            // The mutation reads and get a previous write.
            let ts = match txn_props.kind {
                TransactionKind::Optimistic(_) => txn_props.start_ts,
                TransactionKind::Pessimistic(for_update_ts) => for_update_ts,
            };
            reader.get_old_value(&mutation.key, ts, prev_write_loaded, prev_write)?
        }
    } else {
        OldValue::Unspecified
    };

    let is_new_lock = !matches!(pessimistic_action, DoPessimisticCheck) || lock_amended;

    let final_min_commit_ts = mutation.write_lock(lock_status, txn, is_new_lock)?;

    fail_point!("after_prewrite_one_key");

    Ok((final_min_commit_ts, old_value))
}

#[derive(Clone, Debug)]
pub struct TransactionProperties<'a> {
    pub start_ts: TimeStamp,
    pub kind: TransactionKind,
    pub commit_kind: CommitKind,
    pub primary: &'a [u8],
    pub txn_size: u64,
    pub lock_ttl: u64,
    pub min_commit_ts: TimeStamp,
    pub need_old_value: bool,
    pub is_retry_request: bool,
    pub assertion_level: AssertionLevel,
    pub txn_source: u64,
}

impl<'a> TransactionProperties<'a> {
    fn max_commit_ts(&self) -> TimeStamp {
        match &self.commit_kind {
            CommitKind::TwoPc => unreachable!(),
            CommitKind::OnePc(ts) => *ts,
            CommitKind::Async(ts) => *ts,
        }
    }

    fn is_pessimistic(&self) -> bool {
        match &self.kind {
            TransactionKind::Optimistic(_) => false,
            TransactionKind::Pessimistic(_) => true,
        }
    }

    fn for_update_ts(&self) -> TimeStamp {
        match &self.kind {
            TransactionKind::Optimistic(_) => TimeStamp::zero(),
            TransactionKind::Pessimistic(ts) => *ts,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CommitKind {
    TwoPc,
    /// max_commit_ts
    OnePc(TimeStamp),
    /// max_commit_ts
    Async(TimeStamp),
}

#[derive(Clone, Debug)]
pub enum TransactionKind {
    // bool is skip_constraint_check
    Optimistic(bool),
    // for_update_ts
    Pessimistic(TimeStamp),
}

#[derive(Clone, Copy)]
enum LockStatus {
    // Lock has already been locked; min_commit_ts of lock.
    Locked(TimeStamp),
    // Key is pessimistic-locked; for_update_ts of lock.
    Pessimistic(TimeStamp),
    None,
}

impl LockStatus {
    fn has_pessimistic_lock(&self) -> bool {
        matches!(self, LockStatus::Pessimistic(_))
    }
}

/// A single mutation to be prewritten.
#[derive(Debug)]
struct PrewriteMutation<'a> {
    key: Key,
    value: Option<Value>,
    mutation_type: MutationType,
    secondary_keys: &'a Option<Vec<Vec<u8>>>,
    min_commit_ts: TimeStamp,
    pessimistic_action: PrewriteRequestPessimisticAction,

    lock_type: Option<LockType>,
    lock_ttl: u64,
    last_change: LastChange,

    should_not_exist: bool,
    should_not_write: bool,
    assertion: Assertion,
    txn_props: &'a TransactionProperties<'a>,
}

impl<'a> PrewriteMutation<'a> {
    fn from_mutation(
        mutation: Mutation,
        secondary_keys: &'a Option<Vec<Vec<u8>>>,
        pessimistic_action: PrewriteRequestPessimisticAction,
        txn_props: &'a TransactionProperties<'a>,
    ) -> Result<PrewriteMutation<'a>> {
        let should_not_write = mutation.should_not_write();

        if txn_props.is_pessimistic() && should_not_write {
            return Err(box_err!(
                "cannot handle checkNotExists in pessimistic prewrite"
            ));
        }

        let should_not_exist = mutation.should_not_exists();
        let mutation_type = mutation.mutation_type();
        let lock_type = LockType::from_mutation(&mutation);
        let assertion = mutation.get_assertion();
        let (key, value) = mutation.into_key_value();
        Ok(PrewriteMutation {
            key,
            value,
            mutation_type,
            secondary_keys,
            min_commit_ts: txn_props.min_commit_ts,
            pessimistic_action,

            lock_type,
            lock_ttl: txn_props.lock_ttl,
            last_change: LastChange::default(),

            should_not_exist,
            should_not_write,
            assertion,
            txn_props,
        })
    }

    // Pessimistic transactions only acquire pessimistic locks on row keys and
    // unique index keys. The corresponding secondary index keys are not locked
    // until pessimistic prewrite. It's possible that lock conflict occurs on
    // them, but the isolation is guaranteed by pessimistic locks, so let TiDB
    // resolves these locks immediately.
    fn lock_info(&self, lock: Lock) -> Result<LockInfo> {
        let mut info = lock.into_lock_info(self.key.to_raw()?);
        if self.txn_props.is_pessimistic() {
            info.set_lock_ttl(0);
        }
        Ok(info)
    }

    /// Check whether the current key is locked at any timestamp.
    fn check_lock(
        &mut self,
        lock: Lock,
        pessimistic_action: PrewriteRequestPessimisticAction,
        expected_for_update_ts: Option<TimeStamp>,
    ) -> Result<LockStatus> {
        if lock.ts != self.txn_props.start_ts {
            // Abort on lock belonging to other transaction if
            // prewrites a pessimistic lock.
            if matches!(pessimistic_action, DoPessimisticCheck) {
                warn!(
                    "prewrite failed (pessimistic lock not found)";
                    "start_ts" => self.txn_props.start_ts,
                    "key" => %self.key,
                    "lock_ts" => lock.ts
                );
                return Err(ErrorInner::PessimisticLockNotFound {
                    start_ts: self.txn_props.start_ts,
                    key: self.key.to_raw()?,
                    reason: PessimisticLockNotFoundReason::LockTsMismatch,
                }
                .into());
            }

            return Err(ErrorInner::KeyIsLocked(self.lock_info(lock)?).into());
        }

        self.last_change = lock.last_change.clone();

        if lock.is_pessimistic_lock() {
            // TODO: remove it in future
            if !self.txn_props.is_pessimistic() {
                return Err(ErrorInner::LockTypeNotMatch {
                    start_ts: self.txn_props.start_ts,
                    key: self.key.to_raw()?,
                    pessimistic: true,
                }
                .into());
            }

            if let Some(ts) = expected_for_update_ts && lock.for_update_ts != ts {
                // The constraint on for_update_ts of the pessimistic lock is violated.
                // Consider the following case:
                //
                // 1. A pessimistic lock of transaction `T1` succeeded with`WakeUpModeForceLock`
                //    enabled, then it returns to the client and the client continues its
                //    execution.
                // 2. The lock is lost for some reason such as pipelined locking or in-memory
                //    pessimistic lock.
                // 3. Another transaction `T2` writes the key and committed.
                // 4. The key then receives a stale pessimistic lock request of `T1` that has
                //    been received in step 1 (maybe because of retrying due to network issue
                //    in step 1). Since it allows locking with conflict, though there's a newer
                //    version that's later than the request's `for_update_ts`, the request can
                //    still acquire the lock. However no one will check the response, which
                //    tells the latest commit_ts it met.
                // 5. The transaction `T1` commits. When it prewrites it checks if each key is
                //    pessimistic-locked.
                //
                // Transaction `T1` won't notice anything wrong without this check since it
                // does have a pessimistic lock of the same transaction. However, actually
                // one of the key is locked in a larger version than that the client would
                // expect. As a result, the conflict between transaction `T1` and `T2` is
                // missed.
                // To avoid this problem, we check the for_update_ts written on the
                // pessimistic locks that's acquired in force-locking mode. If it doesn't match
                // the one known by the client, the lock that we expected to have will be
                // regarded as missing.
                //
                // It's actually theoretically safe to allow `lock.for_update_ts` <
                // `expected_for_update_ts`, but the possibility to encounter this case is very
                // low. For simplicity, we don't consider that case and only allow
                // `lock.for_update_ts` to exactly match that we expect.
                warn!("pessimistic lock have different for_update_ts than expected. the expected lock must have been lost";
                    "key" => %self.key,
                    "start_ts" => self.txn_props.start_ts,
                    "expected_for_update_ts" => ts,
                    "lock" => ?lock);

                return Err(ErrorInner::PessimisticLockNotFound {
                    start_ts: self.txn_props.start_ts,
                    key: self.key.to_raw()?,
                    reason: PessimisticLockNotFoundReason::LockForUpdateTsMismatch,
                }
                .into());
            }

            // The lock is pessimistic and owned by this txn, go through to overwrite it.
            // The ttl and min_commit_ts of the lock may have been pushed forward.
            self.lock_ttl = std::cmp::max(self.lock_ttl, lock.ttl);
            self.min_commit_ts = std::cmp::max(self.min_commit_ts, lock.min_commit_ts);

            return Ok(LockStatus::Pessimistic(lock.for_update_ts));
        }

        // Duplicated command. No need to overwrite the lock and data.
        MVCC_DUPLICATE_CMD_COUNTER_VEC.prewrite.inc();
        let min_commit_ts = if lock.use_async_commit {
            lock.min_commit_ts
        } else {
            TimeStamp::zero()
        };
        Ok(LockStatus::Locked(min_commit_ts))
    }

    fn check_for_newer_version<S: Snapshot>(
        &mut self,
        reader: &mut SnapshotReader<S>,
    ) -> Result<Option<(Write, TimeStamp)>> {
        let mut seek_ts = TimeStamp::max();
        while let Some((commit_ts, write)) = reader.seek_write(&self.key, seek_ts)? {
            // If there's a write record whose commit_ts equals to our start ts, the current
            // transaction is ok to continue, unless the record means that the current
            // transaction has been rolled back.
            if commit_ts == self.txn_props.start_ts
                && (write.write_type == WriteType::Rollback || write.has_overlapped_rollback)
            {
                MVCC_CONFLICT_COUNTER.rolled_back.inc();
                // TODO: Maybe we need to add a new error for the rolled back case.
                self.write_conflict_error(&write, commit_ts, WriteConflictReason::SelfRolledBack)?;
            }
            if seek_ts == TimeStamp::max() {
                self.last_change =
                    next_last_change_info(&self.key, &write, reader.start_ts, reader, commit_ts)?;
            }
            match self.txn_props.kind {
                TransactionKind::Optimistic(_) => {
                    if commit_ts > self.txn_props.start_ts {
                        MVCC_CONFLICT_COUNTER.prewrite_write_conflict.inc();
                        self.write_conflict_error(
                            &write,
                            commit_ts,
                            WriteConflictReason::Optimistic,
                        )?;
                    }
                }
                // Note: PessimisticLockNotFound can happen on a non-pessimistically locked key,
                // if it is a retrying prewrite request.
                TransactionKind::Pessimistic(for_update_ts) => {
                    if let DoConstraintCheck = self.pessimistic_action {
                        // Do the same as optimistic transactions if constraint checks are needed.
                        if commit_ts > self.txn_props.start_ts {
                            MVCC_CONFLICT_COUNTER.prewrite_write_conflict.inc();
                            self.write_conflict_error(
                                &write,
                                commit_ts,
                                WriteConflictReason::LazyUniquenessCheck,
                            )?;
                        }
                    }
                    if commit_ts > for_update_ts {
                        // Don't treat newer Rollback records as write conflicts. They can cause
                        // false positive errors because they can be written even if the pessimistic
                        // lock of the corresponding row key exists.
                        // Rollback records are only used to prevent retried prewrite from
                        // succeeding. Even if the Rollback record of the current transaction is
                        // collapsed by a newer record, it is safe to prewrite this non-pessimistic
                        // key because either the primary key is rolled back or it's protected
                        // because it's written by CheckSecondaryLocks.
                        if write.write_type == WriteType::Rollback {
                            seek_ts = commit_ts.prev();
                            continue;
                        }

                        warn!("conflicting write was found, pessimistic lock must be lost for the corresponding row key";
                            "key" => %self.key,
                            "start_ts" => self.txn_props.start_ts,
                            "for_update_ts" => for_update_ts,
                            "conflicting_start_ts" => write.start_ts,
                            "conflicting_commit_ts" => commit_ts);
                        return Err(ErrorInner::PessimisticLockNotFound {
                            start_ts: self.txn_props.start_ts,
                            key: self.key.clone().into_raw()?,
                            reason: PessimisticLockNotFoundReason::NonLockKeyConflict,
                        }
                        .into());
                    }
                }
            }
            // Should check it when no lock exists, otherwise it can report error when there
            // is a lock belonging to a committed transaction which deletes the key.
            check_data_constraint(reader, self.should_not_exist, &write, commit_ts, &self.key)?;

            return Ok(Some((write, commit_ts)));
        }
        // If seek_ts is max and it goes here, there is no write record for this key.
        if seek_ts == TimeStamp::max() {
            self.last_change = LastChange::NotExist;
        }
        Ok(None)
    }

    fn write_lock(
        self,
        lock_status: LockStatus,
        txn: &mut MvccTxn,
        is_new_lock: bool,
    ) -> Result<TimeStamp> {
        let mut try_one_pc = self.try_one_pc();

        let for_update_ts_to_write = match (self.txn_props.for_update_ts(), lock_status) {
            (from_prewrite_req, LockStatus::Pessimistic(from_pessimistic_lock)) => {
                std::cmp::max(from_prewrite_req, from_pessimistic_lock)
            }
            (for_update_ts_from_req, _) => for_update_ts_from_req,
        };

        let mut lock = Lock::new(
            self.lock_type.unwrap(),
            self.txn_props.primary.to_vec(),
            self.txn_props.start_ts,
            self.lock_ttl,
            None,
            for_update_ts_to_write,
            self.txn_props.txn_size,
            self.min_commit_ts,
            false,
        )
        .set_txn_source(self.txn_props.txn_source);
        // Only Lock needs to record `last_change_ts` in its write record, Put or Delete
        // records themselves are effective changes.
        if tls_can_enable(LAST_CHANGE_TS) && self.lock_type == Some(LockType::Lock) {
            lock = lock.set_last_change(self.last_change);
        }

        if let Some(value) = self.value {
            if is_short_value(&value) {
                // If the value is short, embed it in Lock.
                lock.short_value = Some(value);
            } else {
                // value is long
                txn.put_value(self.key.clone(), self.txn_props.start_ts, value);
            }
        }

        if let Some(secondary_keys) = self.secondary_keys {
            lock.use_async_commit = true;
            lock.secondaries = secondary_keys.to_owned();
        }

        let final_min_commit_ts = if lock.use_async_commit || try_one_pc {
            let res = async_commit_timestamps(
                &self.key,
                &mut lock,
                self.txn_props.start_ts,
                self.txn_props.for_update_ts(),
                self.txn_props.max_commit_ts(),
                txn,
            );
            fail_point!("after_calculate_min_commit_ts");
            if let Err(Error(box ErrorInner::CommitTsTooLarge { .. })) = &res {
                try_one_pc = false;
                lock.use_async_commit = false;
                lock.secondaries = Vec::new();
            }
            res
        } else {
            Ok(TimeStamp::zero())
        };

        if try_one_pc {
            txn.put_locks_for_1pc(self.key, lock, lock_status.has_pessimistic_lock());
        } else {
            txn.put_lock(self.key, &lock, is_new_lock);
        }

        final_min_commit_ts
    }

    fn write_conflict_error(
        &self,
        write: &Write,
        commit_ts: TimeStamp,
        reason: kvrpcpb::WriteConflictReason,
    ) -> Result<()> {
        Err(ErrorInner::WriteConflict {
            start_ts: self.txn_props.start_ts,
            conflict_start_ts: write.start_ts,
            conflict_commit_ts: commit_ts,
            key: self.key.to_raw()?,
            primary: self.txn_props.primary.to_vec(),
            reason,
        }
        .into())
    }

    fn check_assertion<S: Snapshot>(
        &mut self,
        reader: &mut SnapshotReader<S>,
        write: &Option<(Write, TimeStamp)>,
        write_loaded: bool,
    ) -> Result<(Option<(Write, TimeStamp)>, bool)> {
        if self.assertion == Assertion::None
            || self.txn_props.assertion_level == AssertionLevel::Off
        {
            MVCC_PREWRITE_ASSERTION_PERF_COUNTER_VEC.none.inc();
            return Ok((None, false));
        }

        if self.txn_props.assertion_level != AssertionLevel::Strict && !write_loaded {
            MVCC_PREWRITE_ASSERTION_PERF_COUNTER_VEC
                .write_not_loaded_skip
                .inc();
            return Ok((None, false));
        }

        let mut reloaded_write = None;
        let mut reloaded = false;

        // To pass the compiler's lifetime check.
        let mut write = write;

        if write_loaded
            && write.as_ref().map_or(
                false,
                |(w, _)| matches!(w.gc_fence, Some(gc_fence_ts) if !gc_fence_ts.is_zero()),
            )
        {
            // The previously-loaded write record has an invalid gc_fence. Regard it as
            // none.
            write = &None;
        }

        // Load the most recent version if prev write is not loaded yet, or the prev
        // write is not a data version (`Put` or `Delete`)
        let need_reload = !write_loaded
            || write.as_ref().map_or(false, |(w, _)| {
                w.write_type != WriteType::Put && w.write_type != WriteType::Delete
            });
        if need_reload {
            if write_loaded {
                MVCC_PREWRITE_ASSERTION_PERF_COUNTER_VEC
                    .non_data_version_reload
                    .inc();
            } else {
                MVCC_PREWRITE_ASSERTION_PERF_COUNTER_VEC
                    .write_not_loaded_reload
                    .inc();
            }

            let reload_ts = write.as_ref().map_or(TimeStamp::max(), |(_, ts)| *ts);
            reloaded_write = reader.get_write_with_commit_ts(&self.key, reload_ts)?;
            write = &reloaded_write;
            reloaded = true;
        } else {
            MVCC_PREWRITE_ASSERTION_PERF_COUNTER_VEC.write_loaded.inc();
        }

        let assertion_err = match (self.assertion, write) {
            (Assertion::Exist, None) => {
                self.assertion_failed_error(TimeStamp::zero(), TimeStamp::zero())
            }
            (Assertion::Exist, Some((w, commit_ts))) if w.write_type == WriteType::Delete => {
                self.assertion_failed_error(w.start_ts, *commit_ts)
            }
            (Assertion::NotExist, Some((w, commit_ts))) if w.write_type == WriteType::Put => {
                self.assertion_failed_error(w.start_ts, *commit_ts)
            }
            _ => Ok(()),
        };

        // Assertion error can be caused by a rollback. So make up a constraint check if
        // the check was skipped before.
        if assertion_err.is_err() {
            if self.skip_constraint_check() {
                self.check_for_newer_version(reader)?;
            }
            let (write, commit_ts) = write
                .as_ref()
                .map(|(w, ts)| (Some(w), Some(ts)))
                .unwrap_or((None, None));
            error!("assertion failure"; "assertion" => ?self.assertion, "write" => ?write,
            "commit_ts" => commit_ts, "mutation" => ?self);
            assertion_err?;
        }

        Ok((reloaded_write, reloaded))
    }

    fn assertion_failed_error(
        &self,
        existing_start_ts: TimeStamp,
        existing_commit_ts: TimeStamp,
    ) -> Result<()> {
        Err(ErrorInner::AssertionFailed {
            start_ts: self.txn_props.start_ts,
            key: self.key.to_raw()?,
            assertion: self.assertion,
            existing_start_ts,
            existing_commit_ts,
        }
        .into())
    }

    fn skip_constraint_check(&self) -> bool {
        match &self.txn_props.kind {
            TransactionKind::Optimistic(s) => *s,
            TransactionKind::Pessimistic(_) => {
                match self.pessimistic_action {
                    DoPessimisticCheck => true,
                    // For non-pessimistic-locked keys, do not skip constraint check when retrying.
                    // This intents to protect idempotency.
                    // Ref: https://github.com/tikv/tikv/issues/11187
                    SkipPessimisticCheck => !self.txn_props.is_retry_request,
                    // For keys that postpones constraint check to prewrite, do not skip constraint
                    // check.
                    PrewriteRequestPessimisticAction::DoConstraintCheck => false,
                }
            }
        }
    }

    fn need_min_commit_ts(&self) -> bool {
        matches!(
            &self.txn_props.commit_kind,
            CommitKind::Async(_) | CommitKind::OnePc(_)
        )
    }

    fn try_one_pc(&self) -> bool {
        matches!(&self.txn_props.commit_kind, CommitKind::OnePc(_))
    }
}

// The final_min_commit_ts will be calculated if either async commit or 1PC is
// enabled. It's allowed to enable 1PC without enabling async commit.
fn async_commit_timestamps(
    key: &Key,
    lock: &mut Lock,
    start_ts: TimeStamp,
    for_update_ts: TimeStamp,
    max_commit_ts: TimeStamp,
    txn: &mut MvccTxn,
) -> Result<TimeStamp> {
    // This operation should not block because the latch makes sure only one thread
    // is operating on this key.
    let key_guard = ::futures_executor::block_on(txn.concurrency_manager.lock_key(key));

    let final_min_commit_ts = key_guard.with_lock(|l| {
        let max_ts = txn.concurrency_manager.max_ts();
        fail_point!("before-set-lock-in-memory");
        let min_commit_ts = cmp::max(cmp::max(max_ts, start_ts), for_update_ts).next();
        let min_commit_ts = cmp::max(lock.min_commit_ts, min_commit_ts);

        #[cfg(feature = "failpoints")]
        let injected_fallback = (|| {
            fail_point!("async_commit_1pc_force_fallback", |_| {
                info!("[failpoint] injected fallback for async commit/1pc transaction"; "start_ts" => start_ts);
                true
            });
            false
        })();
        #[cfg(not(feature = "failpoints"))]
        let injected_fallback = false;

        let max_commit_ts = max_commit_ts;
        if (!max_commit_ts.is_zero() && min_commit_ts > max_commit_ts) || injected_fallback {
            warn!("commit_ts is too large, fallback to normal 2PC";
                "key" => log_wrappers::Value::key(key.as_encoded()),
                "start_ts" => start_ts,
                "min_commit_ts" => min_commit_ts,
                "max_commit_ts" => max_commit_ts,
                "lock" => ?lock);
            return Err(ErrorInner::CommitTsTooLarge {
                start_ts,
                min_commit_ts,
                max_commit_ts,
            });
        }

        lock.min_commit_ts = min_commit_ts;
        *l = Some(lock.clone());
        Ok(min_commit_ts)
    })?;

    txn.guards.push(key_guard);

    Ok(final_min_commit_ts)
}

// TiKV may fails to write pessimistic locks due to pipelined process.
// If the data is not changed after acquiring the lock, we can still prewrite
// the key.
fn amend_pessimistic_lock<S: Snapshot>(
    mutation: &mut PrewriteMutation<'_>,
    reader: &mut SnapshotReader<S>,
) -> Result<()> {
    let write = reader.seek_write(&mutation.key, TimeStamp::max())?;
    if let Some((commit_ts, write)) = write.as_ref() {
        // The invariants of pessimistic locks are:
        //   1. lock's for_update_ts >= key's latest commit_ts
        //   2. lock's for_update_ts >= txn's start_ts
        //   3. If the data is changed after acquiring the pessimistic lock, key's new
        // commit_ts > lock's for_update_ts
        //
        // So, if the key's latest commit_ts is still less than or equal to lock's
        // for_update_ts, the data is not changed. However, we can't get lock's
        // for_update_ts in current implementation (txn's for_update_ts is updated for
        // each DML), we can only use txn's start_ts to check -- If the key's
        // commit_ts is less than txn's start_ts, it's less than
        // lock's for_update_ts too.
        if *commit_ts >= reader.start_ts {
            warn!(
                "prewrite failed (pessimistic lock not found)";
                "start_ts" => reader.start_ts,
                "commit_ts" => *commit_ts,
                "key" => %mutation.key
            );
            MVCC_CONFLICT_COUNTER
                .pipelined_acquire_pessimistic_lock_amend_fail
                .inc();
            return Err(ErrorInner::PessimisticLockNotFound {
                start_ts: reader.start_ts,
                key: mutation.key.clone().into_raw()?,
                reason: PessimisticLockNotFoundReason::LockMissingAmendFail,
            }
            .into());
        }
        mutation.last_change =
            next_last_change_info(&mutation.key, write, reader.start_ts, reader, *commit_ts)?;
    } else {
        mutation.last_change = LastChange::NotExist;
    }
    // Used pipelined pessimistic lock acquiring in this txn but failed
    // Luckily no other txn modified this lock, amend it by treat it as optimistic
    // txn.
    MVCC_CONFLICT_COUNTER
        .pipelined_acquire_pessimistic_lock_amend_success
        .inc();

    // Check assertion after amending.
    mutation.check_assertion(reader, &write.map(|(w, ts)| (ts, w)), true)?;

    Ok(())
}

pub mod tests {
    #[cfg(test)]
    use std::sync::Arc;

    use concurrency_manager::ConcurrencyManager;
    use kvproto::kvrpcpb::Context;
    #[cfg(test)]
    use rand::{Rng, SeedableRng};
    #[cfg(test)]
    use tikv_kv::RocksEngine;
    #[cfg(test)]
    use txn_types::OldValue;

    use super::*;
    #[cfg(test)]
    use crate::storage::{
        kv::RocksSnapshot,
        txn::{
            commands::pessimistic_rollback::tests::must_success as must_pessimistic_rollback,
            commands::prewrite::fallback_1pc_locks, tests::*,
        },
    };
    use crate::storage::{mvcc::tests::*, Engine};

    fn optimistic_txn_props(primary: &[u8], start_ts: TimeStamp) -> TransactionProperties<'_> {
        TransactionProperties {
            start_ts,
            kind: TransactionKind::Optimistic(false),
            commit_kind: CommitKind::TwoPc,
            primary,
            txn_size: 0,
            lock_ttl: 0,
            min_commit_ts: TimeStamp::default(),
            need_old_value: false,
            is_retry_request: false,
            assertion_level: AssertionLevel::Off,
            txn_source: 0,
        }
    }

    #[cfg(test)]
    fn optimistic_async_props(
        primary: &[u8],
        start_ts: TimeStamp,
        max_commit_ts: TimeStamp,
        txn_size: u64,
        one_pc: bool,
    ) -> TransactionProperties<'_> {
        TransactionProperties {
            start_ts,
            kind: TransactionKind::Optimistic(false),
            commit_kind: if one_pc {
                CommitKind::OnePc(max_commit_ts)
            } else {
                CommitKind::Async(max_commit_ts)
            },
            primary,
            txn_size,
            lock_ttl: 2000,
            min_commit_ts: 10.into(),
            need_old_value: true,
            is_retry_request: false,
            assertion_level: AssertionLevel::Off,
            txn_source: 0,
        }
    }

    // Insert has a constraint that key should not exist
    pub fn try_prewrite_insert<E: Engine>(
        engine: &mut E,
        key: &[u8],
        value: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
    ) -> Result<()> {
        let ctx = Context::default();
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let ts = ts.into();
        let cm = ConcurrencyManager::new(ts);
        let mut txn = MvccTxn::new(ts, cm);
        let mut reader = SnapshotReader::new(ts, snapshot, true);

        let mut props = optimistic_txn_props(pk, ts);
        props.need_old_value = true;
        let (_, old_value) = prewrite(
            &mut txn,
            &mut reader,
            &props,
            Mutation::make_insert(Key::from_raw(key), value.to_vec()),
            &None,
            SkipPessimisticCheck,
            None,
        )?;
        // Insert must be None if the key is not lock, or be Unspecified if the
        // key is already locked.
        assert!(
            matches!(old_value, OldValue::None | OldValue::Unspecified),
            "{:?}",
            old_value
        );
        write(engine, &ctx, txn.into_modifies());
        Ok(())
    }

    pub fn try_prewrite_check_not_exists<E: Engine>(
        engine: &mut E,
        key: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
    ) -> Result<()> {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let ts = ts.into();
        let cm = ConcurrencyManager::new(ts);
        let mut txn = MvccTxn::new(ts, cm);
        let mut reader = SnapshotReader::new(ts, snapshot, true);

        let (_, old_value) = prewrite(
            &mut txn,
            &mut reader,
            &optimistic_txn_props(pk, ts),
            Mutation::make_check_not_exists(Key::from_raw(key)),
            &None,
            DoPessimisticCheck,
            None,
        )?;
        assert_eq!(old_value, OldValue::Unspecified);
        Ok(())
    }

    #[test]
    fn test_async_commit_prewrite_check_max_commit_ts() {
        let mut engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new(42.into());

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut txn = MvccTxn::new(10.into(), cm.clone());
        let mut reader = SnapshotReader::new(10.into(), snapshot, true);

        // calculated commit_ts = 43 ≤ 50, ok
        let (_, old_value) = prewrite(
            &mut txn,
            &mut reader,
            &optimistic_async_props(b"k1", 10.into(), 50.into(), 2, false),
            Mutation::make_put(Key::from_raw(b"k1"), b"v1".to_vec()),
            &Some(vec![b"k2".to_vec()]),
            SkipPessimisticCheck,
            None,
        )
        .unwrap();
        assert_eq!(old_value, OldValue::None);

        cm.update_max_ts(60.into());
        // calculated commit_ts = 61 > 50, err
        let err = prewrite(
            &mut txn,
            &mut reader,
            &optimistic_async_props(b"k1", 10.into(), 50.into(), 1, false),
            Mutation::make_put(Key::from_raw(b"k2"), b"v2".to_vec()),
            &Some(vec![]),
            SkipPessimisticCheck,
            None,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            Error(box ErrorInner::CommitTsTooLarge { .. })
        ));

        let modifies = txn.into_modifies();
        assert_eq!(modifies.len(), 2); // the mutation that meets CommitTsTooLarge still exists
        write(&engine, &Default::default(), modifies);
        assert!(must_locked(&mut engine, b"k1", 10).use_async_commit);
        // The written lock should not have use_async_commit flag.
        assert!(!must_locked(&mut engine, b"k2", 10).use_async_commit);
    }

    #[test]
    fn test_async_commit_prewrite_min_commit_ts() {
        let mut engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new(41.into());
        let snapshot = engine.snapshot(Default::default()).unwrap();

        // should_not_write mutations don't write locks or change data so that they
        // needn't ask the concurrency manager for max_ts. Its min_commit_ts may
        // be less than or equal to max_ts.
        let mut props = optimistic_async_props(b"k0", 10.into(), 50.into(), 2, false);
        props.min_commit_ts = 11.into();
        let mut txn = MvccTxn::new(10.into(), cm.clone());
        let mut reader = SnapshotReader::new(10.into(), snapshot.clone(), false);
        let (min_ts, old_value) = prewrite(
            &mut txn,
            &mut reader,
            &props,
            Mutation::make_check_not_exists(Key::from_raw(b"k0")),
            &Some(vec![]),
            SkipPessimisticCheck,
            None,
        )
        .unwrap();
        assert!(min_ts > props.start_ts);
        assert!(min_ts >= props.min_commit_ts);
        assert!(min_ts < 41.into());
        assert_eq!(old_value, OldValue::Unspecified);

        // `checkNotExists` is equivalent to a get operation, so it should update the
        // max_ts.
        let mut props = optimistic_txn_props(b"k0", 42.into());
        props.min_commit_ts = 43.into();
        let mut txn = MvccTxn::new(42.into(), cm.clone());
        let mut reader = SnapshotReader::new(42.into(), snapshot.clone(), false);
        let (_, old_value) = prewrite(
            &mut txn,
            &mut reader,
            &props,
            Mutation::make_check_not_exists(Key::from_raw(b"k0")),
            &Some(vec![]),
            SkipPessimisticCheck,
            None,
        )
        .unwrap();
        assert_eq!(cm.max_ts(), props.start_ts);
        assert_eq!(old_value, OldValue::Unspecified);

        // should_write mutations' min_commit_ts must be > max_ts
        let mut txn = MvccTxn::new(10.into(), cm.clone());
        let mut reader = SnapshotReader::new(10.into(), snapshot.clone(), false);
        let (min_ts, old_value) = prewrite(
            &mut txn,
            &mut reader,
            &optimistic_async_props(b"k1", 10.into(), 50.into(), 2, false),
            Mutation::make_put(Key::from_raw(b"k1"), b"v1".to_vec()),
            &Some(vec![b"k2".to_vec()]),
            SkipPessimisticCheck,
            None,
        )
        .unwrap();
        assert!(min_ts > 42.into());
        assert!(min_ts < 50.into());
        assert_eq!(old_value, OldValue::None);

        for &should_not_write in &[false, true] {
            let mutation = if should_not_write {
                Mutation::make_check_not_exists(Key::from_raw(b"k3"))
            } else {
                Mutation::make_put(Key::from_raw(b"k3"), b"v1".to_vec())
            };

            // min_commit_ts must be > start_ts
            let mut txn = MvccTxn::new(44.into(), cm.clone());
            let mut reader = SnapshotReader::new(44.into(), snapshot.clone(), false);
            let (min_ts, old_value) = prewrite(
                &mut txn,
                &mut reader,
                &optimistic_async_props(b"k3", 44.into(), 50.into(), 2, false),
                mutation.clone(),
                &Some(vec![b"k4".to_vec()]),
                SkipPessimisticCheck,
                None,
            )
            .unwrap();
            assert!(min_ts > 44.into());
            assert!(min_ts < 50.into());
            txn.take_guards();
            if should_not_write {
                assert_eq!(old_value, OldValue::Unspecified);
            } else {
                assert_eq!(old_value, OldValue::None);
            }

            // min_commit_ts must be > for_update_ts
            if !should_not_write {
                let mut props = optimistic_async_props(b"k5", 44.into(), 50.into(), 2, false);
                props.kind = TransactionKind::Pessimistic(45.into());
                let (min_ts, old_value) = prewrite(
                    &mut txn,
                    &mut reader,
                    &props,
                    mutation.clone(),
                    &Some(vec![b"k6".to_vec()]),
                    SkipPessimisticCheck,
                    None,
                )
                .unwrap();
                assert!(min_ts > 45.into());
                assert!(min_ts < 50.into());
                txn.take_guards();
                // Pessimistic txn skips constraint check, does not read previous write.
                assert_eq!(old_value, OldValue::Unspecified);
            }

            // min_commit_ts must be >= txn min_commit_ts
            let mut props = optimistic_async_props(b"k7", 44.into(), 50.into(), 2, false);
            props.min_commit_ts = 46.into();
            let (min_ts, old_value) = prewrite(
                &mut txn,
                &mut reader,
                &props,
                mutation.clone(),
                &Some(vec![b"k8".to_vec()]),
                SkipPessimisticCheck,
                None,
            )
            .unwrap();
            assert!(min_ts >= 46.into());
            assert!(min_ts < 50.into());
            txn.take_guards();
            if should_not_write {
                assert_eq!(old_value, OldValue::Unspecified);
            } else {
                assert_eq!(old_value, OldValue::None);
            }
        }
    }

    #[test]
    fn test_1pc_check_max_commit_ts() {
        let mut engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new(42.into());

        let snapshot = engine.snapshot(Default::default()).unwrap();

        let mut txn = MvccTxn::new(10.into(), cm.clone());
        let mut reader = SnapshotReader::new(10.into(), snapshot, false);
        // calculated commit_ts = 43 ≤ 50, ok
        let (_, old_value) = prewrite(
            &mut txn,
            &mut reader,
            &optimistic_async_props(b"k1", 10.into(), 50.into(), 2, true),
            Mutation::make_put(Key::from_raw(b"k1"), b"v1".to_vec()),
            &None,
            SkipPessimisticCheck,
            None,
        )
        .unwrap();
        assert_eq!(old_value, OldValue::None);

        cm.update_max_ts(60.into());
        // calculated commit_ts = 61 > 50, err
        let err = prewrite(
            &mut txn,
            &mut reader,
            &optimistic_async_props(b"k1", 10.into(), 50.into(), 1, true),
            Mutation::make_put(Key::from_raw(b"k2"), b"v2".to_vec()),
            &None,
            SkipPessimisticCheck,
            None,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            Error(box ErrorInner::CommitTsTooLarge { .. })
        ));

        fallback_1pc_locks(&mut txn);
        let modifies = txn.into_modifies();
        assert_eq!(modifies.len(), 2); // the mutation that meets CommitTsTooLarge still exists
        write(&engine, &Default::default(), modifies);
        // success 1pc prewrite needs to be transformed to locks
        assert!(!must_locked(&mut engine, b"k1", 10).use_async_commit);
        assert!(!must_locked(&mut engine, b"k2", 10).use_async_commit);
    }

    pub fn try_pessimistic_prewrite_check_not_exists<E: Engine>(
        engine: &mut E,
        key: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
    ) -> Result<()> {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let ts = ts.into();
        let cm = ConcurrencyManager::new(ts);
        let mut txn = MvccTxn::new(ts, cm);
        let mut reader = SnapshotReader::new(ts, snapshot, false);

        let (_, old_value) = prewrite(
            &mut txn,
            &mut reader,
            &TransactionProperties {
                start_ts: ts,
                kind: TransactionKind::Pessimistic(TimeStamp::default()),
                commit_kind: CommitKind::TwoPc,
                primary: pk,
                txn_size: 0,
                lock_ttl: 0,
                min_commit_ts: TimeStamp::default(),
                need_old_value: true,
                is_retry_request: false,
                assertion_level: AssertionLevel::Off,
                txn_source: 0,
            },
            Mutation::make_check_not_exists(Key::from_raw(key)),
            &None,
            SkipPessimisticCheck,
            None,
        )?;
        assert_eq!(old_value, OldValue::Unspecified);
        Ok(())
    }

    #[test]
    fn test_async_commit_pessimistic_prewrite_check_max_commit_ts() {
        let mut engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new(42.into());

        must_acquire_pessimistic_lock(&mut engine, b"k1", b"k1", 10, 10);
        must_acquire_pessimistic_lock(&mut engine, b"k2", b"k1", 10, 10);

        let snapshot = engine.snapshot(Default::default()).unwrap();

        let mut txn = MvccTxn::new(10.into(), cm.clone());
        let mut reader = SnapshotReader::new(10.into(), snapshot, false);
        let txn_props = TransactionProperties {
            start_ts: 10.into(),
            kind: TransactionKind::Pessimistic(20.into()),
            commit_kind: CommitKind::Async(50.into()),
            primary: b"k1",
            txn_size: 2,
            lock_ttl: 2000,
            min_commit_ts: 10.into(),
            need_old_value: true,
            is_retry_request: false,
            assertion_level: AssertionLevel::Off,
            txn_source: 0,
        };
        // calculated commit_ts = 43 ≤ 50, ok
        let (_, old_value) = prewrite(
            &mut txn,
            &mut reader,
            &txn_props,
            Mutation::make_put(Key::from_raw(b"k1"), b"v1".to_vec()),
            &Some(vec![b"k2".to_vec()]),
            DoPessimisticCheck,
            None,
        )
        .unwrap();
        // Pessimistic txn skips constraint check, does not read previous write.
        assert_eq!(old_value, OldValue::Unspecified);

        cm.update_max_ts(60.into());
        // calculated commit_ts = 61 > 50, ok
        prewrite(
            &mut txn,
            &mut reader,
            &txn_props,
            Mutation::make_put(Key::from_raw(b"k2"), b"v2".to_vec()),
            &Some(vec![]),
            DoPessimisticCheck,
            None,
        )
        .unwrap_err();
    }

    #[test]
    fn test_1pc_pessimistic_prewrite_check_max_commit_ts() {
        let mut engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new(42.into());

        must_acquire_pessimistic_lock(&mut engine, b"k1", b"k1", 10, 10);
        must_acquire_pessimistic_lock(&mut engine, b"k2", b"k1", 10, 10);

        let snapshot = engine.snapshot(Default::default()).unwrap();

        let mut txn = MvccTxn::new(10.into(), cm.clone());
        let mut reader = SnapshotReader::new(10.into(), snapshot, false);
        let txn_props = TransactionProperties {
            start_ts: 10.into(),
            kind: TransactionKind::Pessimistic(20.into()),
            commit_kind: CommitKind::OnePc(50.into()),
            primary: b"k1",
            txn_size: 2,
            lock_ttl: 2000,
            min_commit_ts: 10.into(),
            need_old_value: true,
            is_retry_request: false,
            assertion_level: AssertionLevel::Off,
            txn_source: 0,
        };
        // calculated commit_ts = 43 ≤ 50, ok
        let (_, old_value) = prewrite(
            &mut txn,
            &mut reader,
            &txn_props,
            Mutation::make_put(Key::from_raw(b"k1"), b"v1".to_vec()),
            &None,
            DoPessimisticCheck,
            None,
        )
        .unwrap();
        // Pessimistic txn skips constraint check, does not read previous write.
        assert_eq!(old_value, OldValue::Unspecified);

        cm.update_max_ts(60.into());
        // calculated commit_ts = 61 > 50, ok
        prewrite(
            &mut txn,
            &mut reader,
            &txn_props,
            Mutation::make_put(Key::from_raw(b"k2"), b"v2".to_vec()),
            &None,
            DoPessimisticCheck,
            None,
        )
        .unwrap_err();
    }

    #[test]
    fn test_prewrite_check_gc_fence() {
        let mut engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new(1.into());

        // PUT,           Read
        //  `------^
        must_prewrite_put(&mut engine, b"k1", b"v1", b"k1", 10);
        must_commit(&mut engine, b"k1", 10, 30);
        must_cleanup_with_gc_fence(&mut engine, b"k1", 30, 0, 40, true);

        // PUT,           Read
        //  * (GC fence ts = 0)
        must_prewrite_put(&mut engine, b"k2", b"v2", b"k2", 11);
        must_commit(&mut engine, b"k2", 11, 30);
        must_cleanup_with_gc_fence(&mut engine, b"k2", 30, 0, 0, true);

        // PUT, LOCK,   LOCK, Read
        //  `---------^
        must_prewrite_put(&mut engine, b"k3", b"v3", b"k3", 12);
        must_commit(&mut engine, b"k3", 12, 30);
        must_prewrite_lock(&mut engine, b"k3", b"k3", 37);
        must_commit(&mut engine, b"k3", 37, 38);
        must_cleanup_with_gc_fence(&mut engine, b"k3", 30, 0, 40, true);
        must_prewrite_lock(&mut engine, b"k3", b"k3", 42);
        must_commit(&mut engine, b"k3", 42, 43);

        // PUT, LOCK,   LOCK, Read
        //  *
        must_prewrite_put(&mut engine, b"k4", b"v4", b"k4", 13);
        must_commit(&mut engine, b"k4", 13, 30);
        must_prewrite_lock(&mut engine, b"k4", b"k4", 37);
        must_commit(&mut engine, b"k4", 37, 38);
        must_prewrite_lock(&mut engine, b"k4", b"k4", 42);
        must_commit(&mut engine, b"k4", 42, 43);
        must_cleanup_with_gc_fence(&mut engine, b"k4", 30, 0, 0, true);

        // PUT,   PUT,    READ
        //  `-----^ `------^
        must_prewrite_put(&mut engine, b"k5", b"v5", b"k5", 14);
        must_commit(&mut engine, b"k5", 14, 20);
        must_prewrite_put(&mut engine, b"k5", b"v5x", b"k5", 21);
        must_commit(&mut engine, b"k5", 21, 30);
        must_cleanup_with_gc_fence(&mut engine, b"k5", 20, 0, 30, false);
        must_cleanup_with_gc_fence(&mut engine, b"k5", 30, 0, 40, true);

        // PUT,   PUT,    READ
        //  `-----^ *
        must_prewrite_put(&mut engine, b"k6", b"v6", b"k6", 15);
        must_commit(&mut engine, b"k6", 15, 20);
        must_prewrite_put(&mut engine, b"k6", b"v6x", b"k6", 22);
        must_commit(&mut engine, b"k6", 22, 30);
        must_cleanup_with_gc_fence(&mut engine, b"k6", 20, 0, 30, false);
        must_cleanup_with_gc_fence(&mut engine, b"k6", 30, 0, 0, true);

        // PUT,  LOCK,    READ
        //  `----------^
        // Note that this case is special because usually the `LOCK` is the first write
        // already got during prewrite/acquire_pessimistic_lock and will continue
        // searching an older version from the `LOCK` record.
        must_prewrite_put(&mut engine, b"k7", b"v7", b"k7", 16);
        must_commit(&mut engine, b"k7", 16, 30);
        must_prewrite_lock(&mut engine, b"k7", b"k7", 37);
        must_commit(&mut engine, b"k7", 37, 38);
        must_cleanup_with_gc_fence(&mut engine, b"k7", 30, 0, 40, true);

        // 1. Check GC fence when doing constraint check with the older version.
        let snapshot = engine.snapshot(Default::default()).unwrap();

        let mut txn = MvccTxn::new(50.into(), cm.clone());
        let mut reader = SnapshotReader::new(50.into(), snapshot.clone(), false);
        let txn_props = TransactionProperties {
            start_ts: 50.into(),
            kind: TransactionKind::Optimistic(false),
            commit_kind: CommitKind::TwoPc,
            primary: b"k1",
            txn_size: 6,
            lock_ttl: 2000,
            min_commit_ts: 51.into(),
            need_old_value: true,
            is_retry_request: false,
            assertion_level: AssertionLevel::Off,
            txn_source: 0,
        };

        let cases = vec![
            (b"k1", true),
            (b"k2", false),
            (b"k3", true),
            (b"k4", false),
            (b"k5", true),
            (b"k6", false),
            (b"k7", true),
        ];

        for (key, success) in cases {
            let res = prewrite(
                &mut txn,
                &mut reader,
                &txn_props,
                Mutation::make_check_not_exists(Key::from_raw(key)),
                &None,
                SkipPessimisticCheck,
                None,
            );
            if success {
                let res = res.unwrap();
                assert_eq!(res.1, OldValue::Unspecified);
            } else {
                res.unwrap_err();
            }

            let res = prewrite(
                &mut txn,
                &mut reader,
                &txn_props,
                Mutation::make_insert(Key::from_raw(key), b"value".to_vec()),
                &None,
                SkipPessimisticCheck,
                None,
            );
            if success {
                let res = res.unwrap();
                assert_eq!(res.1, OldValue::None);
            } else {
                res.unwrap_err();
            }
        }
        // Don't actually write the txn so that the test data is not changed.
        drop(txn);

        // 2. Check GC fence when reading the old value.
        let mut txn = MvccTxn::new(50.into(), cm);
        let mut reader = SnapshotReader::new(50.into(), snapshot, false);
        let txn_props = TransactionProperties {
            start_ts: 50.into(),
            kind: TransactionKind::Optimistic(false),
            commit_kind: CommitKind::TwoPc,
            primary: b"k1",
            txn_size: 6,
            lock_ttl: 2000,
            min_commit_ts: 51.into(),
            need_old_value: true,
            is_retry_request: false,
            assertion_level: AssertionLevel::Off,
            txn_source: 0,
        };

        let cases: Vec<_> = vec![
            (b"k1" as &[u8], None),
            (b"k2", Some(b"v2" as &[u8])),
            (b"k3", None),
            (b"k4", Some(b"v4")),
            (b"k5", None),
            (b"k6", Some(b"v6x")),
            (b"k7", None),
        ]
        .into_iter()
        .map(|(k, v)| {
            let old_value = v
                .map(|value| OldValue::Value {
                    value: value.to_vec(),
                })
                .unwrap_or(OldValue::None);
            (Key::from_raw(k), old_value)
        })
        .collect();

        for (key, expected_value) in &cases {
            let (_, old_value) = prewrite(
                &mut txn,
                &mut reader,
                &txn_props,
                Mutation::make_put(key.clone(), b"value".to_vec()),
                &None,
                SkipPessimisticCheck,
                None,
            )
            .unwrap();
            assert_eq!(&old_value, expected_value, "key: {}", key);
        }
    }

    #[test]
    fn test_resend_prewrite_non_pessimistic_lock() {
        let mut engine = crate::storage::TestEngineBuilder::new().build().unwrap();

        must_acquire_pessimistic_lock(&mut engine, b"k1", b"k1", 10, 10);
        must_pessimistic_prewrite_put_async_commit(
            &mut engine,
            b"k1",
            b"v1",
            b"k1",
            &Some(vec![b"k2".to_vec()]),
            10,
            10,
            DoPessimisticCheck,
            15,
        );
        must_pessimistic_prewrite_put_async_commit(
            &mut engine,
            b"k2",
            b"v2",
            b"k1",
            &Some(vec![]),
            10,
            10,
            SkipPessimisticCheck,
            15,
        );

        // The transaction may be committed by another reader.
        must_commit(&mut engine, b"k1", 10, 20);
        must_commit(&mut engine, b"k2", 10, 20);

        // This is a re-sent prewrite. It should report a PessimisticLockNotFound. In
        // production, the caller will need to check if the current transaction is
        // already committed before, in order to provide the idempotency.
        let err = must_retry_pessimistic_prewrite_put_err(
            &mut engine,
            b"k2",
            b"v2",
            b"k1",
            &Some(vec![]),
            10,
            10,
            SkipPessimisticCheck,
            0,
        );
        assert!(matches!(
            err,
            Error(box ErrorInner::PessimisticLockNotFound { .. })
        ));
        // Commit repeatedly, these operations should have no effect.
        must_commit(&mut engine, b"k1", 10, 25);
        must_commit(&mut engine, b"k2", 10, 25);

        // Seek from 30, we should read commit_ts = 20 instead of 25.
        must_seek_write(&mut engine, b"k1", 30, 10, 20, WriteType::Put);
        must_seek_write(&mut engine, b"k2", 30, 10, 20, WriteType::Put);

        // Write another version to the keys.
        must_prewrite_put(&mut engine, b"k1", b"v11", b"k1", 35);
        must_prewrite_put(&mut engine, b"k2", b"v22", b"k1", 35);
        must_commit(&mut engine, b"k1", 35, 40);
        must_commit(&mut engine, b"k2", 35, 40);

        // A retrying non-pessimistic-lock prewrite request should not skip constraint
        // checks. It reports a PessimisticLockNotFound.
        let err = must_retry_pessimistic_prewrite_put_err(
            &mut engine,
            b"k2",
            b"v2",
            b"k1",
            &Some(vec![]),
            10,
            10,
            SkipPessimisticCheck,
            0,
        );
        assert!(matches!(
            err,
            Error(box ErrorInner::PessimisticLockNotFound { .. })
        ));
        must_unlocked(&mut engine, b"k2");

        let err = must_retry_pessimistic_prewrite_put_err(
            &mut engine,
            b"k2",
            b"v2",
            b"k1",
            &None,
            10,
            10,
            SkipPessimisticCheck,
            0,
        );
        assert!(matches!(
            err,
            Error(box ErrorInner::PessimisticLockNotFound { .. })
        ));
        must_unlocked(&mut engine, b"k2");
        // Committing still does nothing.
        must_commit(&mut engine, b"k2", 10, 25);
        // Try a different txn start ts (which haven't been successfully committed
        // before).
        let err = must_retry_pessimistic_prewrite_put_err(
            &mut engine,
            b"k2",
            b"v2",
            b"k1",
            &None,
            11,
            11,
            SkipPessimisticCheck,
            0,
        );
        assert!(matches!(
            err,
            Error(box ErrorInner::PessimisticLockNotFound { .. })
        ));
        must_unlocked(&mut engine, b"k2");
        // However conflict still won't be checked if there's a non-retry request
        // arriving.
        must_prewrite_put_impl(
            &mut engine,
            b"k2",
            b"v2",
            b"k1",
            &None,
            12.into(),
            SkipPessimisticCheck,
            100,
            12.into(),
            1,
            15.into(),
            TimeStamp::default(),
            false,
            kvproto::kvrpcpb::Assertion::None,
            kvproto::kvrpcpb::AssertionLevel::Off,
        );
        must_locked(&mut engine, b"k2", 12);
        must_rollback(&mut engine, b"k2", 12, false);

        // And conflict check is according to the for_update_ts for pessimistic
        // prewrite. So, it will not report error if for_update_ts is large
        // enough.
        must_prewrite_put_impl(
            &mut engine,
            b"k2",
            b"v2",
            b"k1",
            &None,
            13.into(),
            SkipPessimisticCheck,
            100,
            55.into(),
            1,
            60.into(),
            TimeStamp::default(),
            true,
            kvproto::kvrpcpb::Assertion::None,
            kvproto::kvrpcpb::AssertionLevel::Off,
        );
        must_locked(&mut engine, b"k2", 13);
        must_rollback(&mut engine, b"k2", 13, false);

        // Write a Rollback at 50 first. A retried prewrite at the same ts should
        // report WriteConflict.
        must_rollback(&mut engine, b"k2", 50, false);
        let err = must_retry_pessimistic_prewrite_put_err(
            &mut engine,
            b"k2",
            b"v2",
            b"k1",
            &None,
            50,
            50,
            SkipPessimisticCheck,
            0,
        );
        assert!(
            matches!(err, Error(box ErrorInner::WriteConflict { .. })),
            "{:?}",
            err
        );
        // But prewriting at 48 can succeed because a newer rollback is allowed.
        must_prewrite_put_impl(
            &mut engine,
            b"k2",
            b"v2",
            b"k1",
            &None,
            48.into(),
            SkipPessimisticCheck,
            100,
            48.into(),
            1,
            49.into(),
            TimeStamp::default(),
            true,
            kvproto::kvrpcpb::Assertion::None,
            kvproto::kvrpcpb::AssertionLevel::Off,
        );
        must_locked(&mut engine, b"k2", 48);
    }

    #[test]
    fn test_old_value_rollback_and_lock() {
        let mut engine_rollback = crate::storage::TestEngineBuilder::new().build().unwrap();

        must_prewrite_put(&mut engine_rollback, b"k1", b"v1", b"k1", 10);
        must_commit(&mut engine_rollback, b"k1", 10, 30);

        must_prewrite_put(&mut engine_rollback, b"k1", b"v2", b"k1", 40);
        must_rollback(&mut engine_rollback, b"k1", 40, false);

        let mut engine_lock = crate::storage::TestEngineBuilder::new().build().unwrap();

        must_prewrite_put(&mut engine_lock, b"k1", b"v1", b"k1", 10);
        must_commit(&mut engine_lock, b"k1", 10, 30);

        must_prewrite_lock(&mut engine_lock, b"k1", b"k1", 40);
        must_commit(&mut engine_lock, b"k1", 40, 45);

        for engine in &mut [engine_rollback, engine_lock] {
            let start_ts = TimeStamp::from(50);
            let txn_props = TransactionProperties {
                start_ts,
                kind: TransactionKind::Optimistic(false),
                commit_kind: CommitKind::TwoPc,
                primary: b"k1",
                txn_size: 0,
                lock_ttl: 0,
                min_commit_ts: TimeStamp::default(),
                need_old_value: true,
                is_retry_request: false,
                assertion_level: AssertionLevel::Off,
                txn_source: 0,
            };
            let snapshot = engine.snapshot(Default::default()).unwrap();
            let cm = ConcurrencyManager::new(start_ts);
            let mut txn = MvccTxn::new(start_ts, cm);
            let mut reader = SnapshotReader::new(start_ts, snapshot, true);
            let (_, old_value) = prewrite(
                &mut txn,
                &mut reader,
                &txn_props,
                Mutation::make_put(Key::from_raw(b"k1"), b"value".to_vec()),
                &None,
                SkipPessimisticCheck,
                None,
            )
            .unwrap();
            assert_eq!(
                old_value,
                OldValue::Value {
                    value: b"v1".to_vec(),
                }
            );
        }
    }

    // Prepares a test case that put, delete and lock a key and returns
    // a timestamp for testing the case.
    #[cfg(test)]
    pub fn old_value_put_delete_lock_insert<E: Engine>(engine: &mut E, key: &[u8]) -> TimeStamp {
        must_prewrite_put(engine, key, b"v1", key, 10);
        must_commit(engine, key, 10, 20);

        must_prewrite_delete(engine, key, key, 30);
        must_commit(engine, key, 30, 40);

        must_prewrite_lock(engine, key, key, 50);
        must_commit(engine, key, 50, 60);

        70.into()
    }

    #[test]
    fn test_old_value_put_delete_lock_insert() {
        let mut engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let start_ts = old_value_put_delete_lock_insert(&mut engine, b"k1");
        let txn_props = TransactionProperties {
            start_ts,
            kind: TransactionKind::Optimistic(false),
            commit_kind: CommitKind::TwoPc,
            primary: b"k1",
            txn_size: 0,
            lock_ttl: 0,
            min_commit_ts: TimeStamp::default(),
            need_old_value: true,
            is_retry_request: false,
            assertion_level: AssertionLevel::Off,
            txn_source: 0,
        };
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let cm = ConcurrencyManager::new(start_ts);
        let mut txn = MvccTxn::new(start_ts, cm);
        let mut reader = SnapshotReader::new(start_ts, snapshot, true);
        let (_, old_value) = prewrite(
            &mut txn,
            &mut reader,
            &txn_props,
            Mutation::make_insert(Key::from_raw(b"k1"), b"v2".to_vec()),
            &None,
            SkipPessimisticCheck,
            None,
        )
        .unwrap();
        assert_eq!(old_value, OldValue::None);
    }

    #[cfg(test)]
    pub type OldValueRandomTest = Box<dyn Fn(Arc<RocksSnapshot>, TimeStamp) -> Result<OldValue>>;
    #[cfg(test)]
    pub fn old_value_random(
        key: &[u8],
        require_old_value_none: bool,
        tests: Vec<OldValueRandomTest>,
    ) {
        let mut ts = 1u64;
        let mut tso = || {
            ts += 1;
            ts
        };

        use std::time::SystemTime;
        // A simple valid operation sequence: p[prld]*
        // p: put, r: rollback, l: lock, d: delete
        let seed = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut rg = rand::rngs::StdRng::seed_from_u64(seed);

        // Generate 1000 random cases;
        let mut engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let cases = 1000;
        for _ in 0..cases {
            // At most 12 ops per-case.
            let ops_count = rg.gen::<u8>() % 12;
            let ops = (0..ops_count)
                .into_iter()
                .enumerate()
                .map(|(i, _)| {
                    if i == 0 {
                        // The first op must be put.
                        0
                    } else {
                        rg.gen::<u8>() % 4
                    }
                })
                .collect::<Vec<_>>();

            for (i, op) in ops.iter().enumerate() {
                let start_ts = tso();
                let commit_ts = tso();

                match op {
                    0 => {
                        must_prewrite_put(&mut engine, key, &[i as u8], key, start_ts);
                        must_commit(&mut engine, key, start_ts, commit_ts);
                    }
                    1 => {
                        must_prewrite_delete(&mut engine, key, key, start_ts);
                        must_commit(&mut engine, key, start_ts, commit_ts);
                    }
                    2 => {
                        must_prewrite_lock(&mut engine, key, key, start_ts);
                        must_commit(&mut engine, key, start_ts, commit_ts);
                    }
                    3 => {
                        must_prewrite_put(&mut engine, key, &[i as u8], key, start_ts);
                        must_rollback(&mut engine, key, start_ts, false);
                    }
                    _ => unreachable!(),
                }
            }
            let start_ts = TimeStamp::from(tso());
            let snapshot = engine.snapshot(Default::default()).unwrap();
            let expect = {
                let mut reader = SnapshotReader::new(start_ts, snapshot.clone(), true);
                if let Some(write) = reader
                    .reader
                    .get_write(&Key::from_raw(key), start_ts, Some(start_ts))
                    .unwrap()
                {
                    assert_eq!(write.write_type, WriteType::Put);
                    match write.short_value {
                        Some(value) => OldValue::Value { value },
                        None => OldValue::ValueTimeStamp {
                            start_ts: write.start_ts,
                        },
                    }
                } else {
                    OldValue::None
                }
            };
            if require_old_value_none && expect != OldValue::None {
                continue;
            }
            for test in &tests {
                match test(snapshot.clone(), start_ts) {
                    Ok(old_value) => {
                        assert_eq!(old_value, expect, "seed: {} ops: {:?}", seed, ops);
                    }
                    Err(e) => {
                        panic!("error: {:?} seed: {} ops: {:?}", e, seed, ops);
                    }
                }
            }
        }
    }

    #[test]
    fn test_old_value_random() {
        let key = b"k1";
        let require_old_value_none = false;
        old_value_random(
            key,
            require_old_value_none,
            vec![Box::new(move |snapshot, start_ts| {
                let cm = ConcurrencyManager::new(start_ts);
                let mut txn = MvccTxn::new(start_ts, cm);
                let mut reader = SnapshotReader::new(start_ts, snapshot, true);
                let txn_props = TransactionProperties {
                    start_ts,
                    kind: TransactionKind::Optimistic(false),
                    commit_kind: CommitKind::TwoPc,
                    primary: key,
                    txn_size: 0,
                    lock_ttl: 0,
                    min_commit_ts: TimeStamp::default(),
                    need_old_value: true,
                    is_retry_request: false,
                    assertion_level: AssertionLevel::Off,
                    txn_source: 0,
                };
                let (_, old_value) = prewrite(
                    &mut txn,
                    &mut reader,
                    &txn_props,
                    Mutation::make_put(Key::from_raw(key), b"v2".to_vec()),
                    &None,
                    SkipPessimisticCheck,
                    None,
                )?;
                Ok(old_value)
            })],
        )
    }

    #[test]
    fn test_old_value_random_none() {
        let key = b"k1";
        let require_old_value_none = true;
        old_value_random(
            key,
            require_old_value_none,
            vec![Box::new(move |snapshot, start_ts| {
                let cm = ConcurrencyManager::new(start_ts);
                let mut txn = MvccTxn::new(start_ts, cm);
                let mut reader = SnapshotReader::new(start_ts, snapshot, true);
                let txn_props = TransactionProperties {
                    start_ts,
                    kind: TransactionKind::Optimistic(false),
                    commit_kind: CommitKind::TwoPc,
                    primary: key,
                    txn_size: 0,
                    lock_ttl: 0,
                    min_commit_ts: TimeStamp::default(),
                    need_old_value: true,
                    is_retry_request: false,
                    assertion_level: AssertionLevel::Off,
                    txn_source: 0,
                };
                let (_, old_value) = prewrite(
                    &mut txn,
                    &mut reader,
                    &txn_props,
                    Mutation::make_insert(Key::from_raw(key), b"v2".to_vec()),
                    &None,
                    SkipPessimisticCheck,
                    None,
                )?;
                Ok(old_value)
            })],
        )
    }

    #[test]
    fn test_prewrite_with_assertion() {
        let mut engine = crate::storage::TestEngineBuilder::new().build().unwrap();

        fn prewrite_put<E: Engine>(
            engine: &mut E,
            key: &[u8],
            value: &[u8],
            ts: u64,
            pessimistic_action: PrewriteRequestPessimisticAction,
            for_update_ts: u64,
            assertion: Assertion,
            assertion_level: AssertionLevel,
            expect_success: bool,
        ) {
            if expect_success {
                must_prewrite_put_impl(
                    engine,
                    key,
                    value,
                    key,
                    &None,
                    ts.into(),
                    pessimistic_action,
                    100,
                    for_update_ts.into(),
                    1,
                    (ts + 1).into(),
                    0.into(),
                    false,
                    assertion,
                    assertion_level,
                );
            } else {
                let err = must_prewrite_put_err_impl(
                    engine,
                    key,
                    value,
                    key,
                    &None,
                    ts,
                    for_update_ts,
                    pessimistic_action,
                    0,
                    false,
                    assertion,
                    assertion_level,
                );
                assert!(matches!(err, Error(box ErrorInner::AssertionFailed { .. })));
            }
        }

        let mut test =
            |key_prefix: &[u8],
             assertion_level,
             prepare: &mut dyn for<'a> FnMut(&mut RocksEngine, &'a [u8])| {
                let k1 = [key_prefix, b"k1"].concat();
                let k2 = [key_prefix, b"k2"].concat();
                let k3 = [key_prefix, b"k3"].concat();
                let k4 = [key_prefix, b"k4"].concat();

                for k in &[&k1, &k2, &k3, &k4] {
                    prepare(&mut engine, k.as_slice());
                }

                // Assertion passes (optimistic).
                prewrite_put(
                    &mut engine,
                    &k1,
                    b"v1",
                    10,
                    SkipPessimisticCheck,
                    0,
                    Assertion::NotExist,
                    assertion_level,
                    true,
                );
                must_commit(&mut engine, &k1, 10, 15);

                prewrite_put(
                    &mut engine,
                    &k1,
                    b"v1",
                    20,
                    SkipPessimisticCheck,
                    0,
                    Assertion::Exist,
                    assertion_level,
                    true,
                );
                must_commit(&mut engine, &k1, 20, 25);

                // Assertion passes (pessimistic).
                prewrite_put(
                    &mut engine,
                    &k2,
                    b"v2",
                    10,
                    DoPessimisticCheck,
                    11,
                    Assertion::NotExist,
                    assertion_level,
                    true,
                );
                must_commit(&mut engine, &k2, 10, 15);

                prewrite_put(
                    &mut engine,
                    &k2,
                    b"v2",
                    20,
                    DoPessimisticCheck,
                    21,
                    Assertion::Exist,
                    assertion_level,
                    true,
                );
                must_commit(&mut engine, &k2, 20, 25);

                // Optimistic transaction assertion fail on fast/strict level.
                let pass = assertion_level == AssertionLevel::Off;
                prewrite_put(
                    &mut engine,
                    &k1,
                    b"v1",
                    30,
                    SkipPessimisticCheck,
                    0,
                    Assertion::NotExist,
                    assertion_level,
                    pass,
                );
                prewrite_put(
                    &mut engine,
                    &k3,
                    b"v3",
                    30,
                    SkipPessimisticCheck,
                    0,
                    Assertion::Exist,
                    assertion_level,
                    pass,
                );
                must_rollback(&mut engine, &k1, 30, true);
                must_rollback(&mut engine, &k3, 30, true);

                // Pessimistic transaction assertion fail on fast/strict level if assertion
                // happens during amending pessimistic lock.
                let pass = assertion_level == AssertionLevel::Off;
                prewrite_put(
                    &mut engine,
                    &k2,
                    b"v2",
                    30,
                    DoPessimisticCheck,
                    31,
                    Assertion::NotExist,
                    assertion_level,
                    pass,
                );
                prewrite_put(
                    &mut engine,
                    &k4,
                    b"v4",
                    30,
                    DoPessimisticCheck,
                    31,
                    Assertion::Exist,
                    assertion_level,
                    pass,
                );
                must_rollback(&mut engine, &k2, 30, true);
                must_rollback(&mut engine, &k4, 30, true);

                // Pessimistic transaction fail on strict level no matter what
                // `pessimistic_action` is.
                let pass = assertion_level != AssertionLevel::Strict;
                prewrite_put(
                    &mut engine,
                    &k1,
                    b"v1",
                    40,
                    SkipPessimisticCheck,
                    41,
                    Assertion::NotExist,
                    assertion_level,
                    pass,
                );
                prewrite_put(
                    &mut engine,
                    &k3,
                    b"v3",
                    40,
                    SkipPessimisticCheck,
                    41,
                    Assertion::Exist,
                    assertion_level,
                    pass,
                );
                must_rollback(&mut engine, &k1, 40, true);
                must_rollback(&mut engine, &k3, 40, true);

                must_acquire_pessimistic_lock(&mut engine, &k2, &k2, 40, 41);
                must_acquire_pessimistic_lock(&mut engine, &k4, &k4, 40, 41);
                prewrite_put(
                    &mut engine,
                    &k2,
                    b"v2",
                    40,
                    DoPessimisticCheck,
                    41,
                    Assertion::NotExist,
                    assertion_level,
                    pass,
                );
                prewrite_put(
                    &mut engine,
                    &k4,
                    b"v4",
                    40,
                    DoPessimisticCheck,
                    41,
                    Assertion::Exist,
                    assertion_level,
                    pass,
                );
                must_rollback(&mut engine, &k1, 40, true);
                must_rollback(&mut engine, &k3, 40, true);
            };

        let mut prepare_rollback =
            |engine: &mut RocksEngine, k: &'_ _| must_rollback(engine, k, 3, true);
        let mut prepare_lock_record = |engine: &mut RocksEngine, k: &'_ _| {
            must_prewrite_lock(engine, k, k, 3);
            must_commit(engine, k, 3, 5);
        };
        let mut prepare_delete = |engine: &mut RocksEngine, k: &'_ _| {
            must_prewrite_put(engine, k, b"deleted-value", k, 3);
            must_commit(engine, k, 3, 5);
            must_prewrite_delete(engine, k, k, 7);
            must_commit(engine, k, 7, 9);
        };
        let mut prepare_gc_fence = |engine: &mut RocksEngine, k: &'_ _| {
            must_prewrite_put(engine, k, b"deleted-value", k, 3);
            must_commit(engine, k, 3, 5);
            must_cleanup_with_gc_fence(engine, k, 5, 0, 7, true);
        };

        // Test multiple cases without recreating the engine. So use a increasing key
        // prefix to avoid each case interfering each other.
        let mut key_prefix = b'a';

        let mut test_all_levels = |prepare: &mut dyn for<'a> FnMut(&mut RocksEngine, &'a [u8])| {
            test(&[key_prefix], AssertionLevel::Off, prepare);
            key_prefix += 1;
            test(&[key_prefix], AssertionLevel::Fast, prepare);
            key_prefix += 1;
            test(&[key_prefix], AssertionLevel::Strict, prepare);
            key_prefix += 1;
        };

        test_all_levels(&mut |_, _| ());
        test_all_levels(&mut prepare_rollback);
        test_all_levels(&mut prepare_lock_record);
        test_all_levels(&mut prepare_delete);
        test_all_levels(&mut prepare_gc_fence);
    }

    #[test]
    fn test_deferred_constraint_check() {
        let mut engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let key = b"key";
        let key2 = b"key2";
        let value = b"value";

        // 1. write conflict
        must_prewrite_put(&mut engine, key, value, key, 1);
        must_commit(&mut engine, key, 1, 5);
        must_pessimistic_prewrite_insert(&mut engine, key2, value, key, 3, 3, SkipPessimisticCheck);
        let err = must_pessimistic_prewrite_insert_err(
            &mut engine,
            key,
            value,
            key,
            3,
            3,
            DoConstraintCheck,
        );
        assert!(matches!(
            err,
            Error(box ErrorInner::WriteConflict {
                reason: WriteConflictReason::LazyUniquenessCheck,
                ..
            })
        ));

        // 2. unique constraint fail
        must_prewrite_put(&mut engine, key, value, key, 11);
        must_commit(&mut engine, key, 11, 12);
        let err = must_pessimistic_prewrite_insert_err(
            &mut engine,
            key,
            value,
            key,
            13,
            13,
            DoConstraintCheck,
        );
        assert!(matches!(err, Error(box ErrorInner::AlreadyExist { .. })));

        // 3. success
        must_prewrite_delete(&mut engine, key, key, 21);
        must_commit(&mut engine, key, 21, 22);
        must_pessimistic_prewrite_insert(&mut engine, key, value, key, 23, 23, DoConstraintCheck);
    }

    #[cfg(test)]
    fn test_calculate_last_change_ts_from_latest_write_impl(
        prewrite_func: impl Fn(&mut RocksEngine, LockType, /* start_ts */ u64),
    ) {
        use engine_traits::CF_WRITE;
        use pd_client::FeatureGate;

        use crate::storage::txn::sched_pool::set_tls_feature_gate;

        let mut engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let key = b"k";

        // Latest version does not exist
        prewrite_func(&mut engine, LockType::Lock, 2);
        let lock = must_locked(&mut engine, key, 2);
        assert_eq!(lock.last_change, LastChange::NotExist);
        must_rollback(&mut engine, key, 2, false);

        // Latest change ts should not be enabled on TiKV 6.4
        let feature_gate = FeatureGate::default();
        feature_gate.set_version("6.4.0").unwrap();
        set_tls_feature_gate(feature_gate);
        let write = Write::new(WriteType::Put, 5.into(), Some(b"value".to_vec()));
        engine
            .put_cf(
                Default::default(),
                CF_WRITE,
                Key::from_raw(key).append_ts(8.into()),
                write.as_ref().to_bytes(),
            )
            .unwrap();
        prewrite_func(&mut engine, LockType::Lock, 10);
        let lock = must_locked(&mut engine, key, 10);
        assert_eq!(lock.last_change, LastChange::Unknown);
        must_rollback(&mut engine, key, 10, false);

        let feature_gate = FeatureGate::default();
        feature_gate.set_version("6.5.0").unwrap();
        set_tls_feature_gate(feature_gate);

        // Latest version is a PUT. But as we are prewriting a PUT, no need to record
        // `last_change_ts`.
        let write = Write::new(WriteType::Put, 15.into(), Some(b"value".to_vec()));
        engine
            .put_cf(
                Default::default(),
                CF_WRITE,
                Key::from_raw(key).append_ts(20.into()),
                write.as_ref().to_bytes(),
            )
            .unwrap();
        prewrite_func(&mut engine, LockType::Put, 25);
        let lock = must_locked(&mut engine, key, 25);
        assert_eq!(lock.last_change, LastChange::Unknown);
        must_rollback(&mut engine, key, 25, false);

        // Latest version is a PUT
        let write = Write::new(WriteType::Put, 30.into(), Some(b"value".to_vec()));
        engine
            .put_cf(
                Default::default(),
                CF_WRITE,
                Key::from_raw(key).append_ts(35.into()),
                write.as_ref().to_bytes(),
            )
            .unwrap();
        prewrite_func(&mut engine, LockType::Lock, 40);
        let lock = must_locked(&mut engine, key, 40);
        assert_eq!(lock.last_change, LastChange::make_exist(35.into(), 1));
        must_rollback(&mut engine, key, 40, false);

        // Latest version is a DELETE
        let write = Write::new(WriteType::Delete, 45.into(), None);
        engine
            .put_cf(
                Default::default(),
                CF_WRITE,
                Key::from_raw(key).append_ts(50.into()),
                write.as_ref().to_bytes(),
            )
            .unwrap();
        prewrite_func(&mut engine, LockType::Lock, 55);
        let lock = must_locked(&mut engine, key, 55);
        assert_eq!(lock.last_change, LastChange::make_exist(50.into(), 1));
        must_rollback(&mut engine, key, 55, false);

        // Latest version is a LOCK without last_change_ts. It iterates back to find the
        // actual last write. In this case it is a DELETE, so it returns
        // (last_change_ts == 0 && estimated_versions_to_last_change == 1), indicating
        // the key does not exist.
        let write = Write::new(WriteType::Lock, 60.into(), None);
        engine
            .put_cf(
                Default::default(),
                CF_WRITE,
                Key::from_raw(key).append_ts(65.into()),
                write.as_ref().to_bytes(),
            )
            .unwrap();
        prewrite_func(&mut engine, LockType::Lock, 70);
        let lock = must_locked(&mut engine, key, 70);
        assert_eq!(lock.last_change, LastChange::NotExist);
        must_rollback(&mut engine, key, 70, false);

        // Latest version is a ROLLBACK without last_change_ts. Iterate back to find the
        // DELETE.
        let write = Write::new(WriteType::Rollback, 75.into(), None);
        engine
            .put_cf(
                Default::default(),
                CF_WRITE,
                Key::from_raw(key).append_ts(80.into()),
                write.as_ref().to_bytes(),
            )
            .unwrap();
        prewrite_func(&mut engine, LockType::Lock, 85);
        let lock = must_locked(&mut engine, key, 85);
        assert_eq!(lock.last_change, LastChange::NotExist);
        must_rollback(&mut engine, key, 85, false);

        // Latest version is a LOCK with last_change_ts
        let write = Write::new(WriteType::Lock, 90.into(), None)
            .set_last_change(LastChange::make_exist(20.into(), 6));
        engine
            .put_cf(
                Default::default(),
                CF_WRITE,
                Key::from_raw(key).append_ts(95.into()),
                write.as_ref().to_bytes(),
            )
            .unwrap();
        prewrite_func(&mut engine, LockType::Lock, 100);
        let lock = must_locked(&mut engine, key, 100);
        assert_eq!(lock.last_change, LastChange::make_exist(20.into(), 7));
        must_rollback(&mut engine, key, 100, false);

        // Latest version is a LOCK with last_change_ts
        let write = Write::new(WriteType::Lock, 105.into(), None)
            .set_last_change(LastChange::make_exist(20.into(), 8));
        engine
            .put_cf(
                Default::default(),
                CF_WRITE,
                Key::from_raw(key).append_ts(110.into()),
                write.as_ref().to_bytes(),
            )
            .unwrap();
        prewrite_func(&mut engine, LockType::Lock, 120);
        let lock = must_locked(&mut engine, key, 120);
        assert_eq!(lock.last_change, LastChange::make_exist(20.into(), 9));
        must_rollback(&mut engine, key, 120, false);
    }

    #[test]
    fn test_optimistic_txn_calculate_last_change_ts() {
        test_calculate_last_change_ts_from_latest_write_impl(|engine, tp, start_ts| match tp {
            LockType::Put => must_prewrite_put(engine, b"k", b"value", b"k", start_ts),
            LockType::Delete => must_prewrite_delete(engine, b"k", b"k", start_ts),
            LockType::Lock => must_prewrite_lock(engine, b"k", b"k", start_ts),
            _ => unreachable!(),
        });
    }

    #[test]
    fn test_pessimistic_amend_txn_calculate_last_change_ts() {
        test_calculate_last_change_ts_from_latest_write_impl(|engine, tp, start_ts| match tp {
            LockType::Put => must_pessimistic_prewrite_put(
                engine,
                b"k",
                b"value",
                b"k",
                start_ts,
                start_ts,
                DoPessimisticCheck,
            ),
            LockType::Delete => must_pessimistic_prewrite_delete(
                engine,
                b"k",
                b"k",
                start_ts,
                start_ts,
                DoPessimisticCheck,
            ),
            LockType::Lock => must_pessimistic_prewrite_lock(
                engine,
                b"k",
                b"k",
                start_ts,
                start_ts,
                DoPessimisticCheck,
            ),
            _ => unreachable!(),
        });
    }

    #[test]
    fn test_inherit_last_change_ts_from_pessimistic_lock() {
        use engine_traits::CF_LOCK;

        let mut engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let key = b"k";
        let put_lock = |engine: &mut RocksEngine,
                        ts: u64,
                        last_change_ts: u64,
                        estimated_versions_to_last_change| {
            let lock = Lock::new(
                LockType::Pessimistic,
                key.to_vec(),
                ts.into(),
                100,
                None,
                ts.into(),
                5,
                ts.into(),
                false,
            )
            .set_last_change(LastChange::from_parts(
                last_change_ts.into(),
                estimated_versions_to_last_change,
            ));
            engine
                .put_cf(
                    Default::default(),
                    CF_LOCK,
                    Key::from_raw(key),
                    lock.to_bytes(),
                )
                .unwrap();
        };

        // Prewrite LOCK from pessimistic lock without `last_change_ts`
        put_lock(&mut engine, 10, 0, 0);
        must_pessimistic_prewrite_lock(&mut engine, key, key, 10, 10, DoPessimisticCheck);
        let lock = must_locked(&mut engine, key, 10);
        assert_eq!(lock.last_change, LastChange::Unknown);
        must_rollback(&mut engine, key, 10, false);

        // Prewrite LOCK from pessimistic lock with `last_change_ts`
        put_lock(&mut engine, 20, 15, 3);
        must_pessimistic_prewrite_lock(&mut engine, key, key, 20, 20, DoPessimisticCheck);
        let lock = must_locked(&mut engine, key, 20);
        assert_eq!(lock.last_change, LastChange::make_exist(15.into(), 3));
        must_rollback(&mut engine, key, 20, false);

        // Prewrite PUT from pessimistic lock with `last_change_ts`
        put_lock(&mut engine, 30, 15, 5);
        must_pessimistic_prewrite_put(&mut engine, key, b"value", key, 30, 30, DoPessimisticCheck);
        let lock = must_locked(&mut engine, key, 30);
        assert_eq!(lock.last_change, LastChange::Unknown);
        must_rollback(&mut engine, key, 30, false);

        // Prewrite DELETE from pessimistic lock with `last_change_ts`
        put_lock(&mut engine, 40, 15, 5);
        must_pessimistic_prewrite_delete(&mut engine, key, key, 40, 30, DoPessimisticCheck);
        let lock = must_locked(&mut engine, key, 40);
        assert_eq!(lock.last_change, LastChange::Unknown);
        must_rollback(&mut engine, key, 40, false);
    }

    #[test]
    fn test_pessimistic_prewrite_check_for_update_ts() {
        let mut engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let key = b"k";
        let value = b"v";

        let prewrite = &must_pessimistic_prewrite_put_check_for_update_ts;
        let prewrite_err = &must_pessimistic_prewrite_put_check_for_update_ts_err;

        let mut test_normal = |start_ts: u64,
                               lock_for_update_ts: u64,
                               prewrite_req_for_update_ts: u64,
                               expected_for_update_ts: u64,
                               success: bool,
                               commit_ts: u64| {
            // In actual cases these kinds of pessimistic locks should be locked in
            // `allow_locking_with_conflict` mode. For simplicity, we pass a large
            // for_update_ts to the pessimistic lock to simulate that case.
            must_acquire_pessimistic_lock(&mut engine, key, key, start_ts, lock_for_update_ts);
            must_pessimistic_locked(&mut engine, key, start_ts, lock_for_update_ts);
            if success {
                prewrite(
                    &mut engine,
                    key,
                    value,
                    key,
                    start_ts,
                    prewrite_req_for_update_ts,
                    Some(expected_for_update_ts),
                );
                must_locked(&mut engine, key, start_ts);
                // Test idempotency.
                prewrite(
                    &mut engine,
                    key,
                    value,
                    key,
                    start_ts,
                    prewrite_req_for_update_ts,
                    Some(expected_for_update_ts),
                );
                let prewrite_lock = must_locked(&mut engine, key, start_ts);
                assert_le!(
                    TimeStamp::from(lock_for_update_ts),
                    prewrite_lock.for_update_ts
                );
                must_commit(&mut engine, key, start_ts, commit_ts);
                must_unlocked(&mut engine, key);
            } else {
                let e = prewrite_err(
                    &mut engine,
                    key,
                    value,
                    key,
                    start_ts,
                    prewrite_req_for_update_ts,
                    Some(expected_for_update_ts),
                );
                match e {
                    Error(box ErrorInner::PessimisticLockNotFound { .. }) => (),
                    e => panic!("unexpected error: {:?}", e),
                }
                must_pessimistic_locked(&mut engine, key, start_ts, lock_for_update_ts);
                must_pessimistic_rollback(&mut engine, key, start_ts, lock_for_update_ts);
                must_unlocked(&mut engine, key);
            }
        };

        test_normal(10, 10, 10, 10, true, 19);
        // Note that the `for_update_ts` field in prewrite request is not guaranteed to
        // be greater or equal to the max for_update_ts that has been written to
        // a pessimistic lock during the transaction.
        test_normal(20, 20, 20, 24, false, 0);
        test_normal(30, 35, 30, 35, true, 39);
        test_normal(40, 45, 40, 40, false, 0);
        test_normal(50, 55, 56, 51, false, 0);

        // Amend pessimistic lock cases. Once amend-lock is passed, it can be guaranteed
        // there are no conflict, so the check won't fail.
        // Amending succeeds.
        must_unlocked(&mut engine, key);
        prewrite(&mut engine, key, value, key, 100, 105, Some(102));
        must_locked(&mut engine, key, 100);
        must_commit(&mut engine, key, 100, 125);

        // Amending fails.
        must_unlocked(&mut engine, key);
        prewrite_err(&mut engine, key, value, key, 120, 120, Some(120));
        must_unlocked(&mut engine, key);
        prewrite_err(&mut engine, key, value, key, 120, 130, Some(130));
        must_unlocked(&mut engine, key);
    }
}
