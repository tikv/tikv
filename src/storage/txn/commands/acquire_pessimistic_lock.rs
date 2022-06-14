// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use kvproto::kvrpcpb::{ExtraOp, LockInfo};
use txn_types::{Key, OldValues, TimeStamp, TxnExtra};

use crate::storage::{
    kv::WriteData,
    lock_manager::{LockManager, WaitTimeout},
    mvcc::{Error as MvccError, ErrorInner as MvccErrorInner, MvccTxn, SnapshotReader},
    txn::{
        acquire_pessimistic_lock,
        commands::{
            Command, CommandExt, ReaderWithStats, ResponsePolicy, TypedCommand, WriteCommand,
            WriteContext, WriteResult, WriteResultLockInfo,
        },
        Error, ErrorInner, Result,
    },
    Error as StorageError, ErrorInner as StorageErrorInner, PessimisticLockRes, ProcessResult,
    Result as StorageResult, Snapshot,
};

command! {
    /// Acquire a Pessimistic lock on the keys.
    ///
    /// This can be rolled back with a [`PessimisticRollback`](Command::PessimisticRollback) command.
    AcquirePessimisticLock:
        cmd_ty => StorageResult<PessimisticLockRes>,
        display => "kv::command::acquirepessimisticlock keys({:?}) @ {} {} {} {:?} {} {} | {:?}",
        (keys, start_ts, lock_ttl, for_update_ts, wait_timeout, min_commit_ts, check_existence, ctx),
        content => {
            /// The set of keys to lock.
            keys: Vec<(Key, bool)>,
            /// The primary lock. Secondary locks (from `keys`) will refer to the primary lock.
            primary: Vec<u8>,
            /// The transaction timestamp.
            start_ts: TimeStamp,
            /// The Time To Live of the lock, in milliseconds
            lock_ttl: u64,
            is_first_lock: bool,
            for_update_ts: TimeStamp,
            /// Time to wait for lock released in milliseconds when encountering locks.
            wait_timeout: Option<WaitTimeout>,
            /// If it is true, TiKV will return values of the keys if no error, so TiDB can cache the values for
            /// later read in the same transaction.
            return_values: bool,
            min_commit_ts: TimeStamp,
            old_values: OldValues,
            check_existence: bool,
        }
}

impl CommandExt for AcquirePessimisticLock {
    ctx!();
    tag!(acquire_pessimistic_lock);
    ts!(start_ts);
    property!(can_be_pipelined);

    fn write_bytes(&self) -> usize {
        self.keys
            .iter()
            .map(|(key, _)| key.as_encoded().len())
            .sum()
    }

    gen_lock!(keys: multiple(|x| &x.0));
}

fn extract_lock_info_from_result<T>(res: &StorageResult<T>) -> &LockInfo {
    match res {
        Err(StorageError(box StorageErrorInner::Txn(Error(box ErrorInner::Mvcc(MvccError(
            box MvccErrorInner::KeyIsLocked(info),
        )))))) => info,
        _ => panic!("unexpected mvcc error"),
    }
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for AcquirePessimisticLock {
    fn process_write(mut self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        let (start_ts, ctx, keys) = (self.start_ts, self.ctx, self.keys);
        let mut txn = MvccTxn::new(start_ts, context.concurrency_manager);
        let mut reader = ReaderWithStats::new(
            SnapshotReader::new_with_ctx(start_ts, snapshot, &ctx),
            context.statistics,
        );

        let rows = keys.len();
        let mut res = if self.return_values {
            Ok(PessimisticLockRes::Values(vec![]))
        } else if self.check_existence {
            // If return_value is set, the existence status is implicitly included in the result.
            // So check_existence only need to be explicitly handled if `return_values` is not set.
            Ok(PessimisticLockRes::Existence(vec![]))
        } else {
            Ok(PessimisticLockRes::Empty)
        };
        let need_old_value = context.extra_op == ExtraOp::ReadOldValue;
        for (k, should_not_exist) in keys {
            match acquire_pessimistic_lock(
                &mut txn,
                &mut reader,
                k.clone(),
                &self.primary,
                should_not_exist,
                self.lock_ttl,
                self.for_update_ts,
                self.return_values,
                self.check_existence,
                self.min_commit_ts,
                need_old_value,
            ) {
                Ok((val, old_value)) => {
                    if self.return_values || self.check_existence {
                        res.as_mut().unwrap().push(val);
                    }
                    if old_value.resolved() {
                        let key = k.append_ts(txn.start_ts);
                        // MutationType is unknown in AcquirePessimisticLock stage.
                        let mutation_type = None;
                        self.old_values.insert(key, (old_value, mutation_type));
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
        match &res {
            Ok(PessimisticLockRes::Values(values)) if !values.is_empty() => {
                txn.concurrency_manager.update_max_ts(self.for_update_ts);
            }
            Ok(PessimisticLockRes::Existence(values)) if !values.is_empty() => {
                txn.concurrency_manager.update_max_ts(self.for_update_ts);
            }
            _ => (),
        }

        // no conflict
        let (pr, to_be_write, rows, ctx, lock_info) = if res.is_ok() {
            let pr = ProcessResult::PessimisticLockRes { res };
            let extra = TxnExtra {
                old_values: self.old_values,
                // One pc status is unkown AcquirePessimisticLock stage.
                one_pc: false,
            };
            let write_data = WriteData::new(txn.into_modifies(), extra);
            (pr, write_data, rows, ctx, None)
        } else {
            let lock_info_pb = extract_lock_info_from_result(&res);
            let lock_info = WriteResultLockInfo::from_lock_info_pb(
                lock_info_pb,
                self.is_first_lock,
                self.wait_timeout,
            );
            let pr = ProcessResult::PessimisticLockRes { res };
            // Wait for lock released
            (pr, WriteData::default(), 0, ctx, Some(lock_info))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gen_lock_info_from_result() {
        let raw_key = b"key".to_vec();
        let key = Key::from_raw(&raw_key);
        let ts = 100;
        let is_first_lock = true;
        let wait_timeout = WaitTimeout::from_encoded(200);

        let mut info = LockInfo::default();
        info.set_key(raw_key.clone());
        info.set_lock_version(ts);
        info.set_lock_ttl(100);
        let case = StorageError::from(StorageErrorInner::Txn(Error::from(ErrorInner::Mvcc(
            MvccError::from(MvccErrorInner::KeyIsLocked(info)),
        ))));
        let lock_info = WriteResultLockInfo::from_lock_info_pb(
            extract_lock_info_from_result::<()>(&Err(case)),
            is_first_lock,
            wait_timeout,
        );
        assert_eq!(lock_info.lock.ts, ts.into());
        assert_eq!(lock_info.lock.hash, key.gen_hash());
        assert_eq!(lock_info.key, raw_key);
        assert_eq!(lock_info.is_first_lock, is_first_lock);
        assert_eq!(lock_info.wait_timeout, wait_timeout);
    }
}
