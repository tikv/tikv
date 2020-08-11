// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use pd_client::PdClient;
use txn_types::{Key, LockType, TimeStamp, Write, WriteType};

use crate::storage::kv::WriteData;
use crate::storage::lock_manager::LockManager;
use crate::storage::mvcc::metrics::{MVCC_CONFLICT_COUNTER, MVCC_DUPLICATE_CMD_COUNTER_VEC};
use crate::storage::mvcc::{
    has_data_in_range, txn::make_txn_error, Error as MvccError, ErrorInner as MvccErrorInner,
    Result as MvccResult,
};
use crate::storage::mvcc::{MvccTxn, ReleasedLock};
use crate::storage::txn::commands::{
    Command, CommandExt, ReleasedLocks, TypedCommand, WriteCommand, WriteContext, WriteResult,
};
use crate::storage::txn::{Error, ErrorInner, Result};
use crate::storage::{ProcessResult, Snapshot, TxnStatus};
use std::mem;

command! {
    /// Commit the transaction that started at `lock_ts`.
    ///
    /// This should be following a [`Prewrite`](Command::Prewrite).
    Commit:
        cmd_ty => TxnStatus,
        display => "kv::command::commit {} {} -> {} | {:?}", (keys.len, lock_ts, commit_ts, ctx),
        content => {
            /// The keys affected.
            keys: Vec<Key>,
            /// The lock timestamp.
            lock_ts: txn_types::TimeStamp,
            /// The commit timestamp.
            commit_ts: txn_types::TimeStamp,
        }
}

impl CommandExt for Commit {
    ctx!();
    tag!(commit);
    ts!(commit_ts);
    write_bytes!(keys: multiple);
    gen_lock!(keys: multiple);
}

impl Commit {
    pub fn commit<S: Snapshot, P: PdClient + 'static>(
        &mut self,
        txn: &mut MvccTxn<S, P>,
        key: Key,
    ) -> MvccResult<Option<ReleasedLock>> {
        fail_point!("commit", |err| Err(
            make_txn_error(err, &key, txn.start_ts,).into()
        ));

        let (lock_type, short_value, is_pessimistic_txn) = match txn.reader.load_lock(&key)? {
            Some(ref mut lock) if lock.ts == txn.start_ts => {
                // A lock with larger min_commit_ts than current commit_ts can't be committed
                if self.commit_ts < lock.min_commit_ts {
                    info!(
                        "trying to commit with smaller commit_ts than min_commit_ts";
                        "key" => %key,
                        "start_ts" => txn.start_ts,
                        "commit_ts" => self.commit_ts,
                        "min_commit_ts" => lock.min_commit_ts,
                    );
                    return Err(MvccErrorInner::CommitTsExpired {
                        start_ts: txn.start_ts,
                        commit_ts: self.commit_ts,
                        key: key.into_raw()?,
                        min_commit_ts: lock.min_commit_ts,
                    }
                    .into());
                }

                // It's an abnormal routine since pessimistic locks shouldn't be committed in our
                // transaction model. But a pessimistic lock will be left if the pessimistic
                // rollback request fails to send and the transaction need not to acquire
                // this lock again(due to WriteConflict). If the transaction is committed, we
                // should commit this pessimistic lock too.
                if lock.lock_type == LockType::Pessimistic {
                    warn!(
                        "commit a pessimistic lock with Lock type";
                        "key" => %key,
                        "start_ts" => txn.start_ts,
                        "commit_ts" => self.commit_ts,
                    );
                    // Commit with WriteType::Lock.
                    lock.lock_type = LockType::Lock;
                }
                (
                    lock.lock_type,
                    lock.short_value.take(),
                    !lock.for_update_ts.is_zero(),
                )
            }
            _ => {
                return match txn.reader.get_txn_commit_record(&key, txn.start_ts)?.info() {
                    Some((_, WriteType::Rollback)) | None => {
                        MVCC_CONFLICT_COUNTER.commit_lock_not_found.inc();
                        // None: related Rollback has been collapsed.
                        // Rollback: rollback by concurrent transaction.
                        info!(
                            "txn conflict (lock not found)";
                            "key" => %key,
                            "start_ts" => txn.start_ts,
                            "commit_ts" => self.commit_ts,
                        );
                        Err(MvccErrorInner::TxnLockNotFound {
                            start_ts: txn.start_ts,
                            commit_ts: self.commit_ts,
                            key: key.into_raw()?,
                        }
                        .into())
                    }
                    // Committed by concurrent transaction.
                    Some((_, WriteType::Put))
                    | Some((_, WriteType::Delete))
                    | Some((_, WriteType::Lock)) => {
                        MVCC_DUPLICATE_CMD_COUNTER_VEC.commit.inc();
                        Ok(None)
                    }
                };
            }
        };
        let write = Write::new(
            WriteType::from_lock_type(lock_type).unwrap(),
            txn.start_ts,
            short_value,
        );
        txn.put_write(key.clone(), self.commit_ts, write.as_ref().to_bytes());
        Ok(txn.unlock_key(key, is_pessimistic_txn))
    }
}

impl<S: Snapshot, L: LockManager, P: PdClient + 'static> WriteCommand<S, L, P> for Commit {
    fn process_write(
        mut self,
        snapshot: S,
        context: WriteContext<'_, L, P>,
    ) -> Result<WriteResult> {
        if self.commit_ts <= self.lock_ts {
            return Err(Error::from(ErrorInner::InvalidTxnTso {
                start_ts: self.lock_ts,
                commit_ts: self.commit_ts,
            }));
        }
        let mut txn = MvccTxn::new(
            snapshot,
            self.lock_ts,
            !self.ctx.get_not_fill_cache(),
            context.pd_client,
        );

        let keys = mem::take(&mut self.keys);
        let rows = keys.len();
        // Pessimistic txn needs key_hashes to wake up waiters
        let mut released_locks = ReleasedLocks::new(self.lock_ts, self.commit_ts);
        for k in keys {
            released_locks.push(self.commit(&mut txn, k)?);
        }
        released_locks.wake_up(context.lock_mgr);

        context.statistics.add(&txn.take_statistics());
        let pr = ProcessResult::TxnStatus {
            txn_status: TxnStatus::committed(self.commit_ts),
        };
        let write_data = WriteData::from_modifies(txn.into_modifies());
        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write: write_data,
            rows,
            pr,
            lock_info: None,
        })
    }
}
