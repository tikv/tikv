// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use pd_client::PdClient;
use txn_types::{Key, Lock as TxnLock, LockType, TimeStamp, Value, WriteType};

use crate::storage::kv::WriteData;
use crate::storage::lock_manager::{Lock, LockManager, WaitTimeout};
use crate::storage::mvcc::metrics::{MVCC_CONFLICT_COUNTER, MVCC_DUPLICATE_CMD_COUNTER_VEC};
use crate::storage::mvcc::txn::make_txn_error;
use crate::storage::mvcc::{
    Error as MvccError, ErrorInner as MvccErrorInner, MvccTxn, Result as MvccResult,
};
use crate::storage::txn::commands::{
    Command, CommandExt, TypedCommand, WriteCommand, WriteContext, WriteResult,
};
use crate::storage::txn::{Error, ErrorInner, Result};
use crate::storage::{
    Error as StorageError, ErrorInner as StorageErrorInner, PessimisticLockRes, ProcessResult,
    Result as StorageResult, Snapshot,
};
use std::mem;

command! {
    /// Acquire a Pessimistic lock on the keys.
    ///
    /// This can be rolled back with a [`PessimisticRollback`](Command::PessimisticRollback) command.
    AcquirePessimisticLock:
        cmd_ty => StorageResult<PessimisticLockRes>,
        display => "kv::command::acquirepessimisticlock keys({}) @ {} {} | {:?}", (keys.len, start_ts, for_update_ts, ctx),
        content => {
            /// The set of keys to lock.
            keys: Vec<(Key, bool)>,
            /// The primary lock. Secondary locks (from `keys`) will refer to the primary lock.
            primary: Vec<u8>,
            /// The transaction timestamp.
            start_ts: TimeStamp,
            lock_ttl: u64,
            is_first_lock: bool,
            for_update_ts: TimeStamp,
            /// Time to wait for lock released in milliseconds when encountering locks.
            wait_timeout: Option<WaitTimeout>,
            /// If it is true, TiKV will return values of the keys if no error, so TiDB can cache the values for
            /// later read in the same transaction.
            return_values: bool,
            min_commit_ts: TimeStamp,
        }
}

impl CommandExt for AcquirePessimisticLock {
    ctx!();
    tag!(acquire_pessimistic_lock);
    ts!(start_ts);
    command_method!(can_be_pipelined, bool, true);

    fn write_bytes(&self) -> usize {
        self.keys
            .iter()
            .map(|(key, _)| key.as_encoded().len())
            .sum()
    }

    gen_lock!(keys: multiple(|x| &x.0));
}

fn extract_lock_from_result<T>(res: &StorageResult<T>) -> Lock {
    match res {
        Err(StorageError(box StorageErrorInner::Txn(Error(box ErrorInner::Mvcc(MvccError(
            box MvccErrorInner::KeyIsLocked(info),
        )))))) => Lock {
            ts: info.get_lock_version().into(),
            hash: Key::from_raw(info.get_key()).gen_hash(),
        },
        _ => panic!("unexpected mvcc error"),
    }
}

impl AcquirePessimisticLock {
    pub fn acquire_pessimistic_lock<S: Snapshot, P: PdClient + 'static>(
        &mut self,
        txn: &mut MvccTxn<S, P>,
        key: Key,
        should_not_exist: bool,
    ) -> MvccResult<Option<Value>> {
        fail_point!("acquire_pessimistic_lock", |err| Err(make_txn_error(
            err,
            &key,
            txn.start_ts,
        )
        .into()));

        fn pessimistic_lock(
            primary: &[u8],
            start_ts: TimeStamp,
            lock_ttl: u64,
            for_update_ts: TimeStamp,
            min_commit_ts: TimeStamp,
        ) -> TxnLock {
            TxnLock::new(
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
        if let Some(lock) = txn.reader.load_lock(&key)? {
            if lock.ts != txn.start_ts {
                return Err(
                    MvccErrorInner::KeyIsLocked(lock.into_lock_info(key.into_raw()?)).into(),
                );
            }
            if lock.lock_type != LockType::Pessimistic {
                return Err(MvccErrorInner::LockTypeNotMatch {
                    start_ts: txn.start_ts,
                    key: key.into_raw()?,
                    pessimistic: false,
                }
                .into());
            }
            if self.return_values {
                val = txn.reader.get(&key, self.for_update_ts, true)?;
            }
            // Overwrite the lock with small for_update_ts
            if self.for_update_ts > lock.for_update_ts {
                let lock = pessimistic_lock(
                    &self.primary,
                    txn.start_ts,
                    self.lock_ttl,
                    self.for_update_ts,
                    self.min_commit_ts,
                );
                txn.put_lock(key, &lock);
            } else {
                MVCC_DUPLICATE_CMD_COUNTER_VEC
                    .acquire_pessimistic_lock
                    .inc();
            }
            return Ok(val);
        }

        if let Some((commit_ts, write)) = txn.reader.seek_write(&key, TimeStamp::max())? {
            // The isolation level of pessimistic transactions is RC. `for_update_ts` is
            // the commit_ts of the data this transaction read. If exists a commit version
            // whose commit timestamp is larger than current `for_update_ts`, the
            // transaction should retry to get the latest data.
            if commit_ts > self.for_update_ts {
                MVCC_CONFLICT_COUNTER
                    .acquire_pessimistic_lock_conflict
                    .inc();
                return Err(MvccErrorInner::WriteConflict {
                    start_ts: txn.start_ts,
                    conflict_start_ts: write.start_ts,
                    conflict_commit_ts: commit_ts,
                    key: key.into_raw()?,
                    primary: self.primary.to_vec(),
                }
                .into());
            }

            // Handle rollback.
            // The rollack informathin may come from either a Rollback record or a record with
            // `has_overlapped_rollback` flag.
            if commit_ts == txn.start_ts
                && (write.write_type == WriteType::Rollback || write.has_overlapped_rollback)
            {
                assert!(write.has_overlapped_rollback || write.start_ts == commit_ts);
                return Err(MvccErrorInner::PessimisticLockRolledBack {
                    start_ts: txn.start_ts,
                    key: key.into_raw()?,
                }
                .into());
            }
            // If `commit_ts` we seek is already before `start_ts`, the rollback must not exist.
            if commit_ts > txn.start_ts {
                if let Some((commit_ts, write)) = txn.reader.seek_write(&key, txn.start_ts)? {
                    if write.start_ts == txn.start_ts {
                        assert!(
                            commit_ts == txn.start_ts && write.write_type == WriteType::Rollback
                        );
                        return Err(MvccErrorInner::PessimisticLockRolledBack {
                            start_ts: txn.start_ts,
                            key: key.into_raw()?,
                        }
                        .into());
                    }
                }
            }

            // Check data constraint when acquiring pessimistic lock.
            txn.check_data_constraint(should_not_exist, &write, commit_ts, &key)?;

            if self.return_values {
                val = match write.write_type {
                    // If it's a valid Write, no need to read again.
                    WriteType::Put => Some(txn.reader.load_data(&key, write)?),
                    WriteType::Delete => None,
                    WriteType::Lock | WriteType::Rollback => {
                        txn.reader.get(&key, commit_ts.prev(), true)?
                    }
                };
            }
        }

        let lock = pessimistic_lock(
            &self.primary,
            txn.start_ts,
            self.lock_ttl,
            self.for_update_ts,
            self.min_commit_ts,
        );
        txn.put_lock(key, &lock);

        Ok(val)
    }
}

impl<S: Snapshot, L: LockManager, P: PdClient + 'static> WriteCommand<S, L, P>
    for AcquirePessimisticLock
{
    fn process_write(
        mut self,
        snapshot: S,
        context: WriteContext<'_, L, P>,
    ) -> Result<WriteResult> {
        let (start_ts, ctx, keys) = (
            mem::take(&mut self.start_ts),
            mem::take(&mut self.ctx),
            mem::take(&mut self.keys),
        );
        let mut txn = MvccTxn::new(
            snapshot,
            start_ts,
            !ctx.get_not_fill_cache(),
            context.pd_client,
        );
        let rows = keys.len();
        let mut res = if self.return_values {
            Ok(PessimisticLockRes::Values(vec![]))
        } else {
            Ok(PessimisticLockRes::Empty)
        };
        for (k, should_not_exist) in keys {
            match self.acquire_pessimistic_lock(&mut txn, k, should_not_exist) {
                Ok(val) => {
                    if self.return_values {
                        res.as_mut().unwrap().push(val);
                    }
                }
                Err(e @ MvccError(box MvccErrorInner::KeyIsLocked { .. })) => {
                    res = Err(e).map_err(Error::from).map_err(StorageError::from);
                    break;
                }
                Err(e) => return Err(Error::from(e)),
            }
        }

        context.statistics.add(&txn.take_statistics());
        // no conflict
        let (pr, to_be_write, rows, ctx, lock_info) = if res.is_ok() {
            let pr = ProcessResult::PessimisticLockRes { res };
            let write_data = WriteData::from_modifies(txn.into_modifies());
            (pr, write_data, rows, ctx, None)
        } else {
            let lock = extract_lock_from_result(&res);
            let pr = ProcessResult::PessimisticLockRes { res };
            let lock_info = Some((lock, self.is_first_lock, self.wait_timeout));
            // Wait for lock released
            (pr, WriteData::default(), 0, ctx, lock_info)
        };
        Ok(WriteResult {
            ctx,
            to_be_write,
            rows,
            pr,
            lock_info,
        })
    }
}

#[test]
fn test_extract_lock_from_result() {
    use crate::storage::txn::LockInfo;

    let raw_key = b"key".to_vec();
    let key = Key::from_raw(&raw_key);
    let ts = 100;
    let mut info = LockInfo::default();
    info.set_key(raw_key);
    info.set_lock_version(ts);
    info.set_lock_ttl(100);
    let case = StorageError::from(StorageErrorInner::Txn(Error::from(ErrorInner::Mvcc(
        MvccError::from(MvccErrorInner::KeyIsLocked(info)),
    ))));
    let lock = extract_lock_from_result::<()>(&Err(case));
    assert_eq!(lock.ts, ts.into());
    assert_eq!(lock.hash, key.gen_hash());
}
