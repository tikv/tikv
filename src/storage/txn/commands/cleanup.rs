// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use pd_client::PdClient;
use txn_types::{Key, TimeStamp};

use crate::storage::kv::WriteData;
use crate::storage::lock_manager::LockManager;
use crate::storage::mvcc::metrics::{MVCC_CONFLICT_COUNTER, MVCC_DUPLICATE_CMD_COUNTER_VEC};
use crate::storage::mvcc::txn::make_txn_error;
use crate::storage::mvcc::txn::MissingLockAction;
use crate::storage::mvcc::{
    ErrorInner as MvccErrorInner, MvccTxn, ReleasedLock, Result as MvccResult,
};
use crate::storage::txn::commands::{
    Command, CommandExt, ReleasedLocks, TypedCommand, WriteCommand, WriteContext, WriteResult,
};
use crate::storage::txn::Result;
use crate::storage::{ProcessResult, Snapshot, TxnStatus};
use std::mem;

command! {
    /// Rollback mutations on a single key.
    ///
    /// This should be following a [`Prewrite`](Command::Prewrite) on the given key.
    Cleanup:
        cmd_ty => (),
        display => "kv::command::cleanup {} @ {} | {:?}", (key, start_ts, ctx),
        content => {
            key: Key,
            /// The transaction timestamp.
            start_ts: TimeStamp,
            /// The approximate current ts when cleanup request is invoked, which is used to check the
            /// lock's TTL. 0 means do not check TTL.
            current_ts: TimeStamp,
        }
}

impl CommandExt for Cleanup {
    ctx!();
    tag!(cleanup);
    ts!(start_ts);
    write_bytes!(key);
    gen_lock!(key);
}

impl Cleanup {
    /// Cleanup the lock if it's TTL has expired, comparing with `current_ts`. If `current_ts` is 0,
    /// cleanup the lock without checking TTL. If the lock is the primary lock of a pessimistic
    /// transaction, the rollback record is protected from being collapsed.
    ///
    /// Returns the released lock. Returns error if the key is locked or has already been
    /// committed.
    pub fn cleanup<S: Snapshot, P: PdClient + 'static>(
        self,
        txn: &mut MvccTxn<S, P>,
        protect_rollback: bool,
    ) -> MvccResult<Option<ReleasedLock>> {
        fail_point!("cleanup", |err| Err(make_txn_error(
            err,
            &self.key,
            txn.start_ts,
        )
        .into()));

        match txn.reader.load_lock(&self.key)? {
            Some(ref lock) if lock.ts == txn.start_ts => {
                // If current_ts is not 0, check the Lock's TTL.
                // If the lock is not expired, do not rollback it but report key is locked.
                if !self.current_ts.is_zero()
                    && lock.ts.physical() + lock.ttl >= self.current_ts.physical()
                {
                    return Err(MvccErrorInner::KeyIsLocked(
                        lock.clone().into_lock_info(self.key.into_raw()?),
                    )
                    .into());
                }

                let is_pessimistic_txn = !lock.for_update_ts.is_zero();
                txn.check_write_and_rollback_lock(self.key, lock, is_pessimistic_txn)
            }
            _ => match txn.check_txn_status_missing_lock(
                self.key,
                MissingLockAction::rollback_protect(protect_rollback),
            )? {
                TxnStatus::Committed { commit_ts } => {
                    MVCC_CONFLICT_COUNTER.rollback_committed.inc();
                    Err(MvccErrorInner::Committed { commit_ts }.into())
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
}

impl<S: Snapshot, L: LockManager, P: PdClient + 'static> WriteCommand<S, L, P> for Cleanup {
    fn process_write(
        mut self,
        snapshot: S,
        context: WriteContext<'_, L, P>,
    ) -> Result<WriteResult> {
        let mut txn = MvccTxn::new(
            snapshot,
            self.start_ts,
            !self.ctx.get_not_fill_cache(),
            context.pd_client,
        );

        let mut released_locks = ReleasedLocks::new(self.start_ts, TimeStamp::zero());
        let ctx = mem::take(&mut self.ctx);
        // The rollback must be protected, see more on
        // [issue #7364](https://github.com/tikv/tikv/issues/7364)
        released_locks.push(self.cleanup(&mut txn, true)?);
        released_locks.wake_up(context.lock_mgr);

        context.statistics.add(&txn.take_statistics());
        let write_data = WriteData::from_modifies(txn.into_modifies());
        Ok(WriteResult {
            ctx,
            to_be_write: write_data,
            rows: 1,
            pr: ProcessResult::Res,
            lock_info: None,
        })
    }
}
