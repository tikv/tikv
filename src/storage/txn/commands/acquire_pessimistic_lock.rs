// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use txn_types::{Key, TimeStamp};

use crate::storage::kv::WriteData;
use crate::storage::lock_manager::{Lock, LockManager, WaitTimeout};
use crate::storage::mvcc::{Error as MvccError, ErrorInner as MvccErrorInner, MvccTxn};
use crate::storage::txn::commands::{
    Command, CommandExt, ResponsePolicy, TypedCommand, WriteCommand, WriteContext, WriteResult,
};
use crate::storage::txn::{acquire_pessimistic_lock, Error, ErrorInner, Result};
use crate::storage::{
    Error as StorageError, ErrorInner as StorageErrorInner, PessimisticLockRes, ProcessResult,
    Result as StorageResult, Snapshot,
};

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
            key: info.get_key().to_vec(),
            hash: Key::from_raw(info.get_key()).gen_hash(),
        },
        _ => panic!("unexpected mvcc error"),
    }
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for AcquirePessimisticLock {
    fn process_write(self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        let (start_ts, ctx, keys) = (self.start_ts, self.ctx, self.keys);
        let mut txn = MvccTxn::new(
            snapshot,
            start_ts,
            !ctx.get_not_fill_cache(),
            context.concurrency_manager,
        );
        let rows = keys.len();
        let mut res = if self.return_values {
            Ok(PessimisticLockRes::Values(vec![]))
        } else {
            Ok(PessimisticLockRes::Empty)
        };
        for (k, should_not_exist) in keys {
            match acquire_pessimistic_lock(
                &mut txn,
                k,
                &self.primary,
                should_not_exist,
                self.lock_ttl,
                self.for_update_ts,
                self.return_values,
                self.min_commit_ts,
            ) {
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

        // Some values are read, update max_ts
        if let Ok(PessimisticLockRes::Values(values)) = &res {
            if !values.is_empty() {
                txn.concurrency_manager.update_max_ts(self.for_update_ts);
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
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnProposed,
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
