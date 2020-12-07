// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::{
    mvcc::{
        metrics::{
            CONCURRENCY_MANAGER_LOCK_DURATION_HISTOGRAM, MVCC_CONFLICT_COUNTER,
            MVCC_DUPLICATE_CMD_COUNTER_VEC,
        },
        Error, ErrorInner, Lock, LockType, MvccTxn, Result,
    },
    txn::actions::check_data_constraint::check_data_constraint,
    txn::LockInfo,
    Snapshot,
};
use fail::fail_point;
use std::cmp;
use txn_types::{is_short_value, Key, Mutation, MutationType, TimeStamp, Value, Write, WriteType};

/// Prewrite a single mutation by creating and storing a lock and value.
pub fn prewrite<S: Snapshot>(
    txn: &mut MvccTxn<S>,
    txn_props: &TransactionProperties,
    mutation: Mutation,
    secondary_keys: &Option<Vec<Vec<u8>>>,
    is_pessimistic_lock: bool,
) -> Result<TimeStamp> {
    let mut mutation = PrewriteMutation::from_mutation(mutation, secondary_keys, txn_props)?;

    fail_point!("prewrite", |err| Err(
        crate::storage::mvcc::txn::make_txn_error(err, &mutation.key, mutation.txn_props.start_ts)
            .into()
    ));

    let lock_status = match txn.reader.load_lock(&mutation.key)? {
        Some(lock) => mutation.check_lock(lock, is_pessimistic_lock)?,
        None if is_pessimistic_lock => {
            amend_pessimistic_lock(&mutation.key, txn)?;
            LockStatus::None
        }
        None => LockStatus::None,
    };

    if let LockStatus::Locked(ts) = lock_status {
        return Ok(ts);
    }

    let prev_write =
        if !mutation.skip_constraint_check() && !lock_status.fallback_from_async_commit() {
            mutation.check_for_newer_version(txn)?
        } else {
            None
        };

    if mutation.should_not_write {
        return Ok(TimeStamp::zero());
    }

    if lock_status.fallback_from_async_commit() {
        mutation.value = None;
    } else {
        txn.check_extra_op(&mutation.key, mutation.mutation_type, prev_write)?;
    }

    let final_min_commit_ts = mutation.write_lock(lock_status, txn)?;

    fail_point!("after_prewrite_one_key");

    Ok(final_min_commit_ts)
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

enum LockStatus {
    // Lock has already been locked; min_commit_ts of lock.
    Locked(TimeStamp),
    AsyncFallback,
    Pessimistic,
    None,
}

impl LockStatus {
    fn has_pessimistic_lock(&self) -> bool {
        matches!(self, LockStatus::Pessimistic)
    }

    fn fallback_from_async_commit(&self) -> bool {
        matches!(self, LockStatus::AsyncFallback)
    }
}

/// A single mutation to be prewritten.
struct PrewriteMutation<'a> {
    key: Key,
    value: Option<Value>,
    mutation_type: MutationType,
    secondary_keys: &'a Option<Vec<Vec<u8>>>,
    min_commit_ts: TimeStamp,

    lock_type: Option<LockType>,
    lock_ttl: u64,

    should_not_exist: bool,
    should_not_write: bool,
    txn_props: &'a TransactionProperties<'a>,
}

impl<'a> PrewriteMutation<'a> {
    fn from_mutation(
        mutation: Mutation,
        secondary_keys: &'a Option<Vec<Vec<u8>>>,
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
        let (key, value) = mutation.into_key_value();
        Ok(PrewriteMutation {
            key,
            value,
            mutation_type,
            secondary_keys,
            min_commit_ts: txn_props.min_commit_ts,

            lock_type,
            lock_ttl: txn_props.lock_ttl,

            should_not_exist,
            should_not_write,
            txn_props,
        })
    }

    // Pessimistic transactions only acquire pessimistic locks on row keys and unique index keys.
    // The corresponding secondary index keys are not locked until pessimistic prewrite.
    // It's possible that lock conflict occurs on them, but the isolation is
    // guaranteed by pessimistic locks, so let TiDB resolves these locks immediately.
    fn lock_info(&self, lock: Lock) -> Result<LockInfo> {
        let mut info = lock.into_lock_info(self.key.to_raw()?);
        if self.txn_props.is_pessimistic() {
            info.set_lock_ttl(0);
        }
        Ok(info)
    }

    /// Check whether the current key is locked at any timestamp.
    fn check_lock(&mut self, lock: Lock, is_pessimistic_lock: bool) -> Result<LockStatus> {
        if lock.ts != self.txn_props.start_ts {
            // Abort on lock belonging to other transaction if
            // prewrites a pessimistic lock.
            if is_pessimistic_lock {
                warn!(
                    "prewrite failed (pessimistic lock not found)";
                    "start_ts" => self.txn_props.start_ts,
                    "key" => %self.key,
                    "lock_ts" => lock.ts
                );
                return Err(ErrorInner::PessimisticLockNotFound {
                    start_ts: self.txn_props.start_ts,
                    key: self.key.to_raw()?,
                }
                .into());
            }

            return Err(ErrorInner::KeyIsLocked(self.lock_info(lock)?).into());
        }

        if lock.lock_type == LockType::Pessimistic {
            // TODO: remove it in future
            if !self.txn_props.is_pessimistic() {
                return Err(ErrorInner::LockTypeNotMatch {
                    start_ts: self.txn_props.start_ts,
                    key: self.key.to_raw()?,
                    pessimistic: true,
                }
                .into());
            }

            // The lock is pessimistic and owned by this txn, go through to overwrite it.
            // The ttl and min_commit_ts of the lock may have been pushed forward.
            self.lock_ttl = std::cmp::max(self.lock_ttl, lock.ttl);
            self.min_commit_ts = std::cmp::max(self.min_commit_ts, lock.min_commit_ts);

            return Ok(LockStatus::Pessimistic);
        }

        // Allow to overwrite the primary lock to fallback from async commit.
        if lock.use_async_commit && !self.has_secondary_keys() {
            info!("fallback from async commit"; "start_ts" => self.txn_props.start_ts);
            // needn't clear pessimistic locks
            return Ok(LockStatus::AsyncFallback);
        }

        // Duplicated command. No need to overwrite the lock and data.
        MVCC_DUPLICATE_CMD_COUNTER_VEC.prewrite.inc();
        Ok(LockStatus::Locked(lock.min_commit_ts))
    }

    fn check_for_newer_version<S: Snapshot>(&self, txn: &mut MvccTxn<S>) -> Result<Option<Write>> {
        match txn.reader.seek_write(&self.key, TimeStamp::max())? {
            Some((commit_ts, write)) => {
                // Abort on writes after our start timestamp ...
                // If exists a commit version whose commit timestamp is larger than current start
                // timestamp, we should abort current prewrite.
                if commit_ts > self.txn_props.start_ts {
                    MVCC_CONFLICT_COUNTER.prewrite_write_conflict.inc();
                    self.write_conflict_error(&write, commit_ts)?;
                }
                // If there's a write record whose commit_ts equals to our start ts, the current
                // transaction is ok to continue, unless the record means that the current
                // transaction has been rolled back.
                if commit_ts == self.txn_props.start_ts
                    && (write.write_type == WriteType::Rollback || write.has_overlapped_rollback)
                {
                    MVCC_CONFLICT_COUNTER.rolled_back.inc();
                    // TODO: Maybe we need to add a new error for the rolled back case.
                    self.write_conflict_error(&write, commit_ts)?;
                }
                // Should check it when no lock exists, otherwise it can report error when there is
                // a lock belonging to a committed transaction which deletes the key.
                check_data_constraint(txn, self.should_not_exist, &write, commit_ts, &self.key)?;

                Ok(Some(write))
            }
            None => Ok(None),
        }
    }

    fn write_lock<S: Snapshot>(
        self,
        lock_status: LockStatus,
        txn: &mut MvccTxn<S>,
    ) -> Result<TimeStamp> {
        let mut try_one_pc = self.try_one_pc();

        let mut lock = Lock::new(
            self.lock_type.unwrap(),
            self.txn_props.primary.to_vec(),
            self.txn_props.start_ts,
            self.lock_ttl,
            None,
            self.txn_props.for_update_ts(),
            self.txn_props.txn_size,
            self.min_commit_ts,
        );

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
            txn.put_lock(self.key, &lock);
        }

        final_min_commit_ts
    }

    fn write_conflict_error(&self, write: &Write, commit_ts: TimeStamp) -> Result<()> {
        Err(ErrorInner::WriteConflict {
            start_ts: self.txn_props.start_ts,
            conflict_start_ts: write.start_ts,
            conflict_commit_ts: commit_ts,
            key: self.key.to_raw()?,
            primary: self.txn_props.primary.to_vec(),
        }
        .into())
    }

    fn has_secondary_keys(&self) -> bool {
        self.secondary_keys.is_some()
    }

    fn skip_constraint_check(&self) -> bool {
        match &self.txn_props.kind {
            TransactionKind::Optimistic(s) => *s,
            TransactionKind::Pessimistic(_) => true,
        }
    }

    fn try_one_pc(&self) -> bool {
        match &self.txn_props.commit_kind {
            CommitKind::TwoPc => false,
            CommitKind::OnePc(_) => true,
            CommitKind::Async(_) => false,
        }
    }
}

// The final_min_commit_ts will be calculated if either async commit or 1PC is enabled.
// It's allowed to enable 1PC without enabling async commit.
fn async_commit_timestamps<S: Snapshot>(
    key: &Key,
    lock: &mut Lock,
    start_ts: TimeStamp,
    for_update_ts: TimeStamp,
    max_commit_ts: TimeStamp,
    txn: &mut MvccTxn<S>,
) -> Result<TimeStamp> {
    // This operation should not block because the latch makes sure only one thread
    // is operating on this key.
    let key_guard = CONCURRENCY_MANAGER_LOCK_DURATION_HISTOGRAM.observe_closure_duration(|| {
        ::futures_executor::block_on(txn.concurrency_manager.lock_key(key))
    });

    let final_min_commit_ts = key_guard.with_lock(|l| {
        let max_ts = txn.concurrency_manager.max_ts();
        fail_point!("before-set-lock-in-memory");
        let mut min_commit_ts = cmp::max(cmp::max(max_ts, start_ts), for_update_ts).next();
        min_commit_ts = cmp::max(lock.min_commit_ts, min_commit_ts);

        let max_commit_ts = max_commit_ts;
        if !max_commit_ts.is_zero() && min_commit_ts > max_commit_ts {
            warn!("commit_ts is too large, fallback to normal 2PC";
                "start_ts" => start_ts,
                "min_commit_ts" => min_commit_ts,
                "max_commit_ts" => max_commit_ts);
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
// If the data is not changed after acquiring the lock, we can still prewrite the key.
fn amend_pessimistic_lock<S: Snapshot>(key: &Key, txn: &mut MvccTxn<S>) -> Result<()> {
    if let Some((commit_ts, _)) = txn.reader.seek_write(key, TimeStamp::max())? {
        // The invariants of pessimistic locks are:
        //   1. lock's for_update_ts >= key's latest commit_ts
        //   2. lock's for_update_ts >= txn's start_ts
        //   3. If the data is changed after acquiring the pessimistic lock, key's new commit_ts > lock's for_update_ts
        //
        // So, if the key's latest commit_ts is still less than or equal to lock's for_update_ts, the data is not changed.
        // However, we can't get lock's for_update_ts in current implementation (txn's for_update_ts is updated for each DML),
        // we can only use txn's start_ts to check -- If the key's commit_ts is less than txn's start_ts, it's less than
        // lock's for_update_ts too.
        if commit_ts >= txn.start_ts {
            warn!(
                "prewrite failed (pessimistic lock not found)";
                "start_ts" => txn.start_ts,
                "commit_ts" => commit_ts,
                "key" => %key
            );
            MVCC_CONFLICT_COUNTER
                .pipelined_acquire_pessimistic_lock_amend_fail
                .inc();
            return Err(ErrorInner::PessimisticLockNotFound {
                start_ts: txn.start_ts,
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

pub mod tests {
    use super::*;
    #[cfg(test)]
    use crate::storage::txn::{
        commands::prewrite::fallback_1pc_locks, tests::must_acquire_pessimistic_lock,
    };
    use crate::storage::{mvcc::tests::*, Engine};
    use concurrency_manager::ConcurrencyManager;
    use kvproto::kvrpcpb::Context;

    fn optimistic_txn_props(primary: &[u8], start_ts: TimeStamp) -> TransactionProperties<'_> {
        TransactionProperties {
            start_ts,
            kind: TransactionKind::Optimistic(false),
            commit_kind: CommitKind::TwoPc,
            primary,
            txn_size: 0,
            lock_ttl: 0,
            min_commit_ts: TimeStamp::default(),
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
        }
    }

    // Insert has a constraint that key should not exist
    pub fn try_prewrite_insert<E: Engine>(
        engine: &E,
        key: &[u8],
        value: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
    ) -> Result<()> {
        let ctx = Context::default();
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let ts = ts.into();
        let cm = ConcurrencyManager::new(ts);
        let mut txn = MvccTxn::new(snapshot, ts, true, cm);

        prewrite(
            &mut txn,
            &optimistic_txn_props(pk, ts),
            Mutation::Insert((Key::from_raw(key), value.to_vec())),
            &None,
            false,
        )?;
        write(engine, &ctx, txn.into_modifies());
        Ok(())
    }

    pub fn try_prewrite_check_not_exists<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
    ) -> Result<()> {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let ts = ts.into();
        let cm = ConcurrencyManager::new(ts);
        let mut txn = MvccTxn::new(snapshot, ts, true, cm);

        prewrite(
            &mut txn,
            &optimistic_txn_props(pk, ts),
            Mutation::CheckNotExists(Key::from_raw(key)),
            &None,
            false,
        )?;
        Ok(())
    }

    #[test]
    fn test_async_commit_prewrite_check_max_commit_ts() {
        let engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new(42.into());

        let snapshot = engine.snapshot(Default::default()).unwrap();

        let mut txn = MvccTxn::new(snapshot, 10.into(), false, cm.clone());
        // calculated commit_ts = 43 ≤ 50, ok
        prewrite(
            &mut txn,
            &optimistic_async_props(b"k1", 10.into(), 50.into(), 2, false),
            Mutation::Put((Key::from_raw(b"k1"), b"v1".to_vec())),
            &Some(vec![b"k2".to_vec()]),
            false,
        )
        .unwrap();

        cm.update_max_ts(60.into());
        // calculated commit_ts = 61 > 50, err
        let err = prewrite(
            &mut txn,
            &optimistic_async_props(b"k1", 10.into(), 50.into(), 1, false),
            Mutation::Put((Key::from_raw(b"k2"), b"v2".to_vec())),
            &Some(vec![]),
            false,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            Error(box ErrorInner::CommitTsTooLarge { .. })
        ));

        let modifies = txn.into_modifies();
        assert_eq!(modifies.len(), 2); // the mutation that meets CommitTsTooLarge still exists
        write(&engine, &Default::default(), modifies);
        assert!(must_locked(&engine, b"k1", 10).use_async_commit);
        // The written lock should not have use_async_commit flag.
        assert!(!must_locked(&engine, b"k2", 10).use_async_commit);
    }

    #[test]
    fn test_1pc_check_max_commit_ts() {
        let engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new(42.into());

        let snapshot = engine.snapshot(Default::default()).unwrap();

        let mut txn = MvccTxn::new(snapshot, 10.into(), false, cm.clone());
        // calculated commit_ts = 43 ≤ 50, ok
        prewrite(
            &mut txn,
            &optimistic_async_props(b"k1", 10.into(), 50.into(), 2, true),
            Mutation::Put((Key::from_raw(b"k1"), b"v1".to_vec())),
            &None,
            false,
        )
        .unwrap();

        cm.update_max_ts(60.into());
        // calculated commit_ts = 61 > 50, err
        let err = prewrite(
            &mut txn,
            &optimistic_async_props(b"k1", 10.into(), 50.into(), 1, true),
            Mutation::Put((Key::from_raw(b"k2"), b"v2".to_vec())),
            &None,
            false,
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
        assert!(!must_locked(&engine, b"k1", 10).use_async_commit);
        assert!(!must_locked(&engine, b"k2", 10).use_async_commit);
    }

    #[test]
    fn test_fallback_from_async_commit() {
        use super::super::tests::*;
        use crate::storage::mvcc::MvccReader;
        use kvproto::kvrpcpb::IsolationLevel;

        let engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        must_prewrite_put_async_commit(&engine, b"k", b"v", b"k", &Some(vec![vec![1]]), 10, 20);
        must_prewrite_put(&engine, b"k", b"v", b"k", 10);
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, false, IsolationLevel::Si);
        let lock = reader.load_lock(&Key::from_raw(b"k")).unwrap().unwrap();
        assert!(!lock.use_async_commit);
        assert!(lock.secondaries.is_empty());

        // deny overwrites with async commit prewrit
        must_prewrite_put_async_commit(&engine, b"k", b"v", b"k", &Some(vec![vec![1]]), 10, 20);
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, false, IsolationLevel::Si);
        let lock = reader.load_lock(&Key::from_raw(b"k")).unwrap().unwrap();
        assert!(!lock.use_async_commit);
        assert!(lock.secondaries.is_empty());
    }

    pub fn try_pessimistic_prewrite_check_not_exists<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
    ) -> Result<()> {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let ts = ts.into();
        let cm = ConcurrencyManager::new(ts);
        let mut txn = MvccTxn::new(snapshot, ts, true, cm);

        prewrite(
            &mut txn,
            &TransactionProperties {
                start_ts: ts,
                kind: TransactionKind::Pessimistic(TimeStamp::default()),
                commit_kind: CommitKind::TwoPc,
                primary: pk,
                txn_size: 0,
                lock_ttl: 0,
                min_commit_ts: TimeStamp::default(),
            },
            Mutation::CheckNotExists(Key::from_raw(key)),
            &None,
            false,
        )?;
        Ok(())
    }

    #[test]
    fn test_async_commit_pessimistic_prewrite_check_max_commit_ts() {
        let engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new(42.into());

        must_acquire_pessimistic_lock(&engine, b"k1", b"k1", 10, 10);
        must_acquire_pessimistic_lock(&engine, b"k2", b"k1", 10, 10);

        let snapshot = engine.snapshot(Default::default()).unwrap();

        let mut txn = MvccTxn::new(snapshot, 10.into(), false, cm.clone());
        let txn_props = TransactionProperties {
            start_ts: 10.into(),
            kind: TransactionKind::Pessimistic(20.into()),
            commit_kind: CommitKind::Async(50.into()),
            primary: b"k1",
            txn_size: 2,
            lock_ttl: 2000,
            min_commit_ts: 10.into(),
        };
        // calculated commit_ts = 43 ≤ 50, ok
        prewrite(
            &mut txn,
            &txn_props,
            Mutation::Put((Key::from_raw(b"k1"), b"v1".to_vec())),
            &Some(vec![b"k2".to_vec()]),
            true,
        )
        .unwrap();

        cm.update_max_ts(60.into());
        // calculated commit_ts = 61 > 50, ok
        prewrite(
            &mut txn,
            &txn_props,
            Mutation::Put((Key::from_raw(b"k2"), b"v2".to_vec())),
            &Some(vec![]),
            true,
        )
        .unwrap_err();
    }

    #[test]
    fn test_1pc_pessimistic_prewrite_check_max_commit_ts() {
        let engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new(42.into());

        must_acquire_pessimistic_lock(&engine, b"k1", b"k1", 10, 10);
        must_acquire_pessimistic_lock(&engine, b"k2", b"k1", 10, 10);

        let snapshot = engine.snapshot(Default::default()).unwrap();

        let mut txn = MvccTxn::new(snapshot, 10.into(), false, cm.clone());
        let txn_props = TransactionProperties {
            start_ts: 10.into(),
            kind: TransactionKind::Pessimistic(20.into()),
            commit_kind: CommitKind::OnePc(50.into()),
            primary: b"k1",
            txn_size: 2,
            lock_ttl: 2000,
            min_commit_ts: 10.into(),
        };
        // calculated commit_ts = 43 ≤ 50, ok
        prewrite(
            &mut txn,
            &txn_props,
            Mutation::Put((Key::from_raw(b"k1"), b"v1".to_vec())),
            &None,
            true,
        )
        .unwrap();

        cm.update_max_ts(60.into());
        // calculated commit_ts = 61 > 50, ok
        prewrite(
            &mut txn,
            &txn_props,
            Mutation::Put((Key::from_raw(b"k2"), b"v2".to_vec())),
            &None,
            true,
        )
        .unwrap_err();
    }

    #[test]
    fn test_fallback_from_async_commit_pessimistic() {
        use super::super::tests::*;
        use crate::storage::mvcc::MvccReader;
        use kvproto::kvrpcpb::IsolationLevel;

        let engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        must_acquire_pessimistic_lock(&engine, b"k", b"k", 10, 10);

        must_pessimistic_prewrite_put_async_commit(
            &engine,
            b"k",
            b"v",
            b"k",
            &Some(vec![vec![1]]),
            10,
            10,
            true,
            20,
        );
        must_pessimistic_prewrite_put(&engine, b"k", b"v", b"k", 10, 20, true);
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, false, IsolationLevel::Si);
        let lock = reader.load_lock(&Key::from_raw(b"k")).unwrap().unwrap();
        assert!(!lock.use_async_commit);
        assert!(lock.secondaries.is_empty());

        // deny overwrites with async commit prewrit
        must_pessimistic_prewrite_put_async_commit(
            &engine,
            b"k",
            b"v",
            b"k",
            &Some(vec![vec![1]]),
            10,
            10,
            true,
            20,
        );
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, false, IsolationLevel::Si);
        let lock = reader.load_lock(&Key::from_raw(b"k")).unwrap().unwrap();
        assert!(!lock.use_async_commit);
        assert!(lock.secondaries.is_empty());
    }
}
