// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::kv::WriteData;
use crate::storage::lock_manager::{LockManager, WaitTimeout};
use crate::storage::mvcc::Error as MvccError;
use crate::storage::mvcc::ErrorInner as MvccErrorInner;
use crate::storage::mvcc::MvccTxn;
use crate::storage::txn::commands::{Command, CommandExt, TypedCommand, WriteCommand};
use crate::storage::txn::process::{extract_lock_from_result, WriteResult};
use crate::storage::txn::Error;
use crate::storage::txn::Result;
use crate::storage::Error as StorageError;
use crate::storage::Result as StorageResult;
use crate::storage::{PessimisticLockRes, ProcessResult, Snapshot, Statistics};
use kvproto::kvrpcpb::ExtraOp;
use pd_client::PdClient;
use std::sync::Arc;
use txn_types::{Key, TimeStamp};

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
    command_method!(requires_pessimistic_txn, bool, true);
    command_method!(can_be_pipelined, bool, true);

    fn write_bytes(&self) -> usize {
        self.keys
            .iter()
            .map(|(key, _)| key.as_encoded().len())
            .sum()
    }

    gen_lock!(keys: multiple(|x| &x.0));
}

impl<S: Snapshot, L: LockManager, P: PdClient + 'static> WriteCommand<S, L, P>
    for AcquirePessimisticLock
{
    fn process_write(
        &mut self,
        snapshot: S,
        _lock_mgr: &L,
        pd_client: Arc<P>,
        _extra_op: ExtraOp,
        statistics: &mut Statistics,
        _pipelined_pessimistic_lock: bool,
    ) -> Result<WriteResult> {
        let mut txn = MvccTxn::new(
            snapshot,
            self.start_ts,
            !self.ctx.get_not_fill_cache(),
            pd_client,
        );
        let rows = self.keys.len();
        let mut res = if self.return_values {
            Ok(PessimisticLockRes::Values(vec![]))
        } else {
            Ok(PessimisticLockRes::Empty)
        };
        for (k, should_not_exist) in &self.keys {
            match txn.acquire_pessimistic_lock(
                k.clone(),
                &self.primary,
                *should_not_exist,
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

        statistics.add(&txn.take_statistics());
        // no conflict
        let (pr, to_be_write, rows, ctx, lock_info) = if res.is_ok() {
            let pr = ProcessResult::PessimisticLockRes { res };
            let write_data = WriteData::from_modifies(txn.into_modifies());
            (pr, write_data, rows, self.ctx.clone(), None)
        } else {
            let lock = extract_lock_from_result(&res);
            let pr = ProcessResult::PessimisticLockRes { res };
            let lock_info = Some((lock, self.is_first_lock, self.wait_timeout));
            // Wait for lock released
            (pr, WriteData::default(), 0, self.ctx.clone(), lock_info)
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
