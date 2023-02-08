// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use kvproto::kvrpcpb::ExtraOp;
use tikv_kv::Modify;
use txn_types::{insert_old_value_if_resolved, Key, OldValues, TimeStamp, TxnExtra};

use crate::storage::{
    kv::WriteData,
    lock_manager::{LockManager, WaitTimeout},
    mvcc::{Error as MvccError, ErrorInner as MvccErrorInner, MvccTxn, SnapshotReader},
    txn::{
        acquire_pessimistic_lock,
        commands::{
            Command, CommandExt, ReaderWithStats, ReleasedLocks, ResponsePolicy, TypedCommand,
            WriteCommand, WriteContext, WriteResult, WriteResultLockInfo,
        },
        Error, ErrorInner, Result,
    },
    types::{PessimisticLockParameters, PessimisticLockResults},
    Error as StorageError, PessimisticLockKeyResult, ProcessResult, Result as StorageResult,
    Snapshot,
};

command! {
    /// Acquire a Pessimistic lock on the keys.
    ///
    /// This can be rolled back with a [`PessimisticRollback`](Command::PessimisticRollback) command.
    AcquirePessimisticLock:
        cmd_ty => StorageResult<PessimisticLockResults>,
        display => "kv::command::acquirepessimisticlock keys({:?}) @ {} {} {} {:?} {} {} {} | {:?}",
        (keys, start_ts, lock_ttl, for_update_ts, wait_timeout, min_commit_ts, check_existence, lock_only_if_exists, ctx),
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
            check_existence: bool,
            lock_only_if_exists: bool,
            allow_lock_with_conflict: bool,
        }
}

impl CommandExt for AcquirePessimisticLock {
    ctx!();
    tag!(acquire_pessimistic_lock);
    request_type!(KvPessimisticLock);
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

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for AcquirePessimisticLock {
    fn process_write(self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        if self.allow_lock_with_conflict && self.keys.len() > 1 {
            // Currently multiple keys with `allow_lock_with_conflict` set is not supported.
            return Err(Error::from(ErrorInner::Other(box_err!(
                "multiple keys in a single request with allowed_lock_with_conflict set is not allowed"
            ))));
        }

        let (start_ts, ctx, keys) = (self.start_ts, self.ctx, self.keys);
        let mut txn = MvccTxn::new(start_ts, context.concurrency_manager);
        let mut reader = ReaderWithStats::new(
            SnapshotReader::new_with_ctx(start_ts, snapshot, &ctx),
            context.statistics,
        );

        let total_keys = keys.len();
        let mut res = PessimisticLockResults::with_capacity(total_keys);
        let mut encountered_locks = vec![];
        let need_old_value = context.extra_op == ExtraOp::ReadOldValue;
        let mut old_values = OldValues::default();
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
                self.lock_only_if_exists,
                self.allow_lock_with_conflict,
            ) {
                Ok((key_res, old_value)) => {
                    res.push(key_res);
                    // MutationType is unknown in AcquirePessimisticLock stage.
                    insert_old_value_if_resolved(&mut old_values, k, txn.start_ts, old_value, None);
                }
                Err(MvccError(box MvccErrorInner::KeyIsLocked(lock_info))) => {
                    let request_parameters = PessimisticLockParameters {
                        pb_ctx: ctx.clone(),
                        primary: self.primary.clone(),
                        start_ts,
                        lock_ttl: self.lock_ttl,
                        for_update_ts: self.for_update_ts,
                        wait_timeout: self.wait_timeout,
                        return_values: self.return_values,
                        min_commit_ts: self.min_commit_ts,
                        check_existence: self.check_existence,
                        is_first_lock: self.is_first_lock,
                        lock_only_if_exists: self.lock_only_if_exists,
                        allow_lock_with_conflict: self.allow_lock_with_conflict,
                    };
                    let lock_info = WriteResultLockInfo::new(
                        lock_info,
                        request_parameters,
                        k,
                        should_not_exist,
                    );
                    encountered_locks.push(lock_info);
                    // Do not lock previously succeeded keys.
                    txn.clear();
                    res.0.clear();
                    res.push(PessimisticLockKeyResult::Waiting);
                    break;
                }
                Err(e) => return Err(Error::from(e)),
            }
        }

        let new_acquired_locks = txn.take_new_locks();
        let modifies = txn.into_modifies();

        let mut res = Ok(res);

        // If encountered lock and `wait_timeout` is `None` (which means no wait),
        // return error directly here.
        if !encountered_locks.is_empty() && self.wait_timeout.is_none() {
            // Mind the difference of the protocols of legacy requests and resumable
            // requests. For resumable requests (allow_lock_with_conflict ==
            // true), key errors are considered key by key instead of for the
            // whole request.
            let lock_info = encountered_locks.drain(..).next().unwrap().lock_info_pb;
            let err = StorageError::from(Error::from(MvccError::from(
                MvccErrorInner::KeyIsLocked(lock_info),
            )));
            if self.allow_lock_with_conflict {
                res.as_mut().unwrap().0[0] = PessimisticLockKeyResult::Failed(err.into())
            } else {
                res = Err(err)
            }
        }

        let rows = if res.is_ok() { total_keys } else { 0 };

        let pr = ProcessResult::PessimisticLockRes { res };

        let to_be_write = make_write_data(modifies, old_values);

        Ok(WriteResult {
            ctx,
            to_be_write,
            rows,
            pr,
            lock_info: encountered_locks,
            released_locks: ReleasedLocks::new(),
            new_acquired_locks,
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnProposed,
        })
    }
}

pub(super) fn make_write_data(modifies: Vec<Modify>, old_values: OldValues) -> WriteData {
    if !modifies.is_empty() {
        let extra = TxnExtra {
            old_values,
            // One pc status is unknown in AcquirePessimisticLock stage.
            one_pc: false,
            allowed_in_flashback: false,
        };
        WriteData::new(modifies, extra)
    } else {
        WriteData::default()
    }
}
