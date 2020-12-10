use crate::storage::mvcc::metrics::{
    CONCURRENCY_MANAGER_LOCK_DURATION_HISTOGRAM, MVCC_CONFLICT_COUNTER,
    MVCC_DUPLICATE_CMD_COUNTER_VEC,
};
use crate::storage::mvcc::{
    Error, Key, Lock, LockType, Mutation, MvccTxn, TimeStamp, Value, Write, WriteType,
};
use crate::storage::txn::actions::check_data_constraint::check_data_constraint;
use crate::storage::txn::{CommitKind, LockInfo, TransactionKind, TransactionProperties};
use crate::storage::{mvcc, mvcc::ErrorInner, Snapshot};
use failure::_core::cmp;
use txn_types::{is_short_value, MutationType};

#[derive(Debug)]
pub(crate) enum LockStatus {
    // Lock has already been locked; min_commit_ts of lock.
    Locked(TimeStamp),
    Pessimistic,
    None,
}

impl LockStatus {
    fn has_pessimistic_lock(&self) -> bool {
        matches!(self, LockStatus::Pessimistic)
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
) -> mvcc::Result<TimeStamp> {
    // This operation should not block because the latch makes sure only one thread
    // is operating on this key.
    let key_guard = CONCURRENCY_MANAGER_LOCK_DURATION_HISTOGRAM.observe_closure_duration(|| {
        ::futures_executor::block_on(txn.concurrency_manager.lock_key(key))
    });

    let final_min_commit_ts = key_guard.with_lock(|l| {
        let max_ts = txn.concurrency_manager.max_ts();
        fail_point!("before-set-lock-in-memory");
        let min_commit_ts = cmp::max(cmp::max(max_ts, start_ts), for_update_ts).next();
        let min_commit_ts = cmp::max(lock.min_commit_ts, min_commit_ts);

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

/// A single mutation to be prewritten.
#[derive(Debug)]
pub struct PrewriteMutation<'a> {
    pub(crate) key: Key,
    value: Option<Value>,
    pub(crate) mutation_type: MutationType,
    secondary_keys: &'a Option<Vec<Vec<u8>>>,
    min_commit_ts: TimeStamp,

    lock_type: Option<LockType>,
    lock_ttl: u64,

    should_not_exist: bool,
    pub(crate) should_not_write: bool,
    pub(crate) txn_props: &'a TransactionProperties<'a>,
}

impl<'a> PrewriteMutation<'a> {
    /// convert a trivial Mutation into a PrewriteMutation
    pub(crate) fn from_mutation(
        mutation: Mutation,
        secondary_keys: &'a Option<Vec<Vec<u8>>>,
        txn_props: &'a TransactionProperties<'a>,
    ) -> mvcc::Result<PrewriteMutation<'a>> {
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
    fn lock_info(&self, lock: Lock) -> mvcc::Result<LockInfo> {
        let mut info = lock.into_lock_info(self.key.to_raw()?);
        if self.txn_props.is_pessimistic() {
            info.set_lock_ttl(0);
        }
        Ok(info)
    }

    /// Check whether the current key is locked at any timestamp.
    pub(crate) fn check_lock(
        &mut self,
        lock: Lock,
        is_pessimistic_lock: bool,
    ) -> mvcc::Result<LockStatus> {
        debug_assert_eq!(self.txn_props.primary, lock.primary);
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

        // Duplicated command. No need to overwrite the lock and data.
        MVCC_DUPLICATE_CMD_COUNTER_VEC.prewrite.inc();
        Ok(LockStatus::Locked(lock.min_commit_ts))
    }

    pub(crate) fn check_for_newer_version<S: Snapshot>(
        &self,
        txn: &mut MvccTxn<S>,
    ) -> mvcc::Result<Option<Write>> {
        debug_assert!(!self.skip_constraint_check());
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

    /// Write `lock_status` into `txn`
    pub(crate) fn write_lock<S: Snapshot>(
        self,
        lock_status: LockStatus,
        txn: &mut MvccTxn<S>,
    ) -> mvcc::Result<TimeStamp> {
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

    fn write_conflict_error(&self, write: &Write, commit_ts: TimeStamp) -> mvcc::Result<()> {
        Err(ErrorInner::WriteConflict {
            start_ts: self.txn_props.start_ts,
            conflict_start_ts: write.start_ts,
            conflict_commit_ts: commit_ts,
            key: self.key.to_raw()?,
            primary: self.txn_props.primary.to_vec(),
        }
        .into())
    }

    pub(crate) fn skip_constraint_check(&self) -> bool {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::mvcc::tests::write;
    use crate::storage::txn::actions::prewrite::txn_props;
    use crate::storage::Context;
    use crate::storage::{Engine, TestEngineBuilder};
    use concurrency_manager::ConcurrencyManager;

    #[test]
    fn from_mutation() {
        struct Case<'a> {
            expected: mvcc::Result<PrewriteMutation<'a>>,

            mutation: Mutation,
            txn_props: &'a TransactionProperties<'a>,
        }
        let txn_props = vec![
            txn_props::test_util::TxnPropsBuilder::optimistic(b"a", 5.into()).build(),
            txn_props::test_util::TxnPropsBuilder::pessimistic(b"a", 5.into(), 6.into()).build(),
        ];
        let cases = vec![
            Case {
                expected: Ok(PrewriteMutation {
                    key: Key::from_raw(b"a"),
                    value: Some(Value::from("b")),
                    mutation_type: MutationType::Put,
                    secondary_keys: &None,
                    min_commit_ts: 0.into(),
                    lock_type: Some(LockType::Put),
                    lock_ttl: 0,
                    should_not_exist: false,
                    should_not_write: false,
                    txn_props: &txn_props[0],
                }),
                mutation: Mutation::Put((Key::from_raw(b"a"), Value::from("b"))),
                txn_props: &txn_props[0],
            },
            Case {
                expected: Ok(PrewriteMutation {
                    key: Key::from_raw(b"a"),
                    value: Some(Value::from("b")),
                    mutation_type: MutationType::Put,
                    secondary_keys: &None,
                    min_commit_ts: 0.into(),
                    lock_type: Some(LockType::Put),
                    lock_ttl: 0,
                    should_not_exist: false,
                    should_not_write: false,
                    txn_props: &txn_props[1],
                }),
                mutation: Mutation::Put((Key::from_raw(b"a"), Value::from("b"))),
                txn_props: &txn_props[1],
            },
            Case {
                expected: Ok(PrewriteMutation {
                    key: Key::from_raw(b"a"),
                    value: None,
                    mutation_type: MutationType::Other,
                    secondary_keys: &None,
                    min_commit_ts: 0.into(),
                    lock_type: None,
                    lock_ttl: 0,
                    should_not_exist: true,
                    should_not_write: true,
                    txn_props: &txn_props[0],
                }),
                mutation: Mutation::CheckNotExists(Key::from_raw(b"a")),
                txn_props: &txn_props[0],
            },
            Case {
                expected: Err(box_err!(
                    "cannot handle checkNotExists in pessimistic prewrite"
                )),

                mutation: Mutation::CheckNotExists(Key::from_raw(b"a")),
                txn_props: &txn_props[1],
            },
        ];
        for case in cases {
            let result = PrewriteMutation::from_mutation(case.mutation, &None, case.txn_props);
            if case.expected.is_ok() {
                assert_eq!(format!("{:?}", result), format!("{:?}", case.expected));
            } else {
                assert!(result.is_err());
            }
        }
    }

    #[test]
    fn check_lock() {
        let txn_props = vec![
            txn_props::test_util::TxnPropsBuilder::optimistic(b"a", 5.into()).build(),
            txn_props::test_util::TxnPropsBuilder::pessimistic(b"a", 5.into(), 6.into()).build(),
        ];

        let locks = vec![
            Lock::new(
                LockType::Put,
                "a".into(),
                6.into(),
                0,
                None,
                0.into(),
                0,
                0.into(),
            ),
            Lock::new(
                LockType::Pessimistic,
                "a".into(),
                5.into(),
                100,
                None,
                6.into(),
                0,
                6.into(),
            ),
            Lock::new(
                LockType::Put,
                "a".into(),
                5.into(),
                0,
                None,
                0.into(),
                0,
                6.into(),
            ),
        ];

        struct Case<'a> {
            expected: mvcc::Result<LockStatus>,

            mutation: (Mutation, &'a TransactionProperties<'a>),
            lock: &'a Lock,
            is_pessimistic_lock: bool,

            // side effects on PrewriteMutation
            lock_ttl: u64,
            min_commit_ts: TimeStamp,
        }

        let cases = vec![
            Case {
                // should return PessimisticLockNotFound when prewriting a pessimistic lock and met another lock
                expected: Err(ErrorInner::PessimisticLockNotFound {
                    start_ts: 5.into(),
                    key: b"a".to_vec(),
                }
                .into()),
                mutation: (
                    Mutation::Put((Key::from_raw(b"a"), Value::from("b"))),
                    &txn_props[1],
                ),
                lock: &locks[0],
                is_pessimistic_lock: true,
                lock_ttl: 0,
                min_commit_ts: 0.into(),
            },
            Case {
                // should return KeyIsLocked when met a lock
                expected: Err(ErrorInner::KeyIsLocked(
                    locks[0].clone().into_lock_info(b"a".to_vec()),
                )
                .into()),
                mutation: (
                    Mutation::Put((Key::from_raw(b"a"), Value::from("b"))),
                    &txn_props[1],
                ),
                lock: &locks[0],
                is_pessimistic_lock: false,
                lock_ttl: 0,
                min_commit_ts: 0.into(),
            },
            Case {
                // should return LockStatus::Pessimistic when met a Pessimistic lock
                expected: Ok(LockStatus::Pessimistic),
                mutation: (
                    Mutation::Put((Key::from_raw(b"a"), Value::from("b"))),
                    &txn_props[1],
                ),
                lock: &locks[1],
                is_pessimistic_lock: false,

                // and we also need to check the side effects on lock_ttl and min_commit_ts
                lock_ttl: 100,
                min_commit_ts: 6.into(),
            },
            Case {
                // else return just LockStatus::Locked
                expected: Ok(LockStatus::Locked(6.into())),
                mutation: (
                    Mutation::Put((Key::from_raw(b"a"), Value::from("b"))),
                    &txn_props[0],
                ),
                lock: &locks[2],
                is_pessimistic_lock: false,

                lock_ttl: 0,
                min_commit_ts: 0.into(),
            },
        ];
        for case in cases {
            let mut mutation =
                PrewriteMutation::from_mutation(case.mutation.0, &None, case.mutation.1).unwrap();
            let result = mutation.check_lock(case.lock.clone(), case.is_pessimistic_lock);
            assert_eq!(format!("{:?}", result), format!("{:?}", case.expected));
            assert_eq!(mutation.min_commit_ts, case.min_commit_ts);
            assert_eq!(mutation.lock_ttl, case.lock_ttl);
        }
    }

    #[test]
    fn check_for_newer_version() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new(42.into());
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut txn = MvccTxn::new(snapshot, TimeStamp::new(2), true, cm.clone());
        txn.put_write(
            Key::from_raw(b"b"),
            TimeStamp::new(6),
            Write::new(WriteType::Lock, TimeStamp::new(3), None)
                .as_ref()
                .to_bytes(),
        );
        txn.put_write(
            Key::from_raw(b"c"),
            TimeStamp::new(6),
            Write::new(WriteType::Put, TimeStamp::new(3), None)
                .as_ref()
                .to_bytes(),
        );
        txn.put_write(
            Key::from_raw(b"c"),
            TimeStamp::new(7),
            Write::new(WriteType::Lock, TimeStamp::new(4), None)
                .set_overlapped_rollback(true, None)
                .as_ref()
                .to_bytes(),
        );
        txn.put_write(
            Key::from_raw(b"d"),
            TimeStamp::new(10),
            Write::new(WriteType::Lock, TimeStamp::new(5), None)
                .set_overlapped_rollback(true, None)
                .as_ref()
                .to_bytes(),
        );
        txn.put_write(
            Key::from_raw(b"e"),
            TimeStamp::new(20),
            Write::new(WriteType::Put, TimeStamp::new(6), None)
                .as_ref()
                .to_bytes(),
        );
        txn.put_write(
            Key::from_raw(b"f"),
            TimeStamp::new(10),
            Write::new(WriteType::Rollback, TimeStamp::new(7), None)
                .as_ref()
                .to_bytes(),
        );
        write(&engine, &Context::default(), txn.into_modifies());
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut txn = MvccTxn::new(snapshot, TimeStamp::new(3), true, cm);

        let txn_prop = txn_props::test_util::TxnPropsBuilder::optimistic(b"a", 10.into()).build();

        struct Case<'a> {
            expected: mvcc::Result<Option<Write>>,

            mutation: (Mutation, &'a TransactionProperties<'a>),
        }

        let cases = vec![
            Case {
                // should detect write after this txn starts
                expected: Err(ErrorInner::WriteConflict {
                    start_ts: 10.into(),
                    conflict_start_ts: 6.into(),
                    conflict_commit_ts: 20.into(),
                    key: b"e".to_vec(),
                    primary: b"a".to_vec(),
                }
                .into()),
                mutation: (
                    Mutation::Put((Key::from_raw(b"e"), Value::from("b"))),
                    &txn_prop,
                ),
            },
            Case {
                // should detect rollback, when write.write_type == WriteType::Rollback
                expected: Err(ErrorInner::WriteConflict {
                    start_ts: 10.into(),
                    conflict_start_ts: 7.into(),
                    conflict_commit_ts: 10.into(),
                    key: b"f".to_vec(),
                    primary: b"a".to_vec(),
                }
                .into()),
                mutation: (
                    Mutation::Put((Key::from_raw(b"f"), Value::from("b"))),
                    &txn_prop,
                ),
            },
            Case {
                // should detect rollback, when write.has_overlapped_rollback
                expected: Err(ErrorInner::WriteConflict {
                    start_ts: 10.into(),
                    conflict_start_ts: 5.into(),
                    conflict_commit_ts: 10.into(),
                    key: b"d".to_vec(),
                    primary: b"a".to_vec(),
                }
                .into()),
                mutation: (
                    Mutation::Put((Key::from_raw(b"d"), Value::from("b"))),
                    &txn_prop,
                ),
            },
            Case {
                // should propagate error from check_data_constraint
                expected: Err(ErrorInner::AlreadyExist { key: b"c".to_vec() }.into()),
                mutation: (
                    Mutation::Insert((Key::from_raw(b"c"), Value::from("b"))),
                    &txn_prop,
                ),
            },
            Case {
                // should return the new version if there is one
                // and it doesn't has conflict or violates the data constraint mentioned above
                expected: Ok(Some(Write::new(WriteType::Lock, TimeStamp::new(3), None))),
                mutation: (
                    Mutation::Insert((Key::from_raw(b"b"), Value::from("b"))),
                    &txn_prop,
                ),
            },
            Case {
                // should return None otherwise
                expected: Ok(None),
                mutation: (
                    Mutation::Put((Key::from_raw(b"g"), Value::from("b"))),
                    &txn_prop,
                ),
            },
        ];
        for case in cases {
            let mutation =
                PrewriteMutation::from_mutation(case.mutation.0, &None, case.mutation.1).unwrap();
            let result = mutation.check_for_newer_version(&mut txn);
            assert_eq!(format!("{:?}", result), format!("{:?}", case.expected));
        }
    }
}
