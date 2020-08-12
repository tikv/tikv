// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use pd_client::PdClient;
use txn_types::{Key, LockType, Mutation, TimeStamp};

use crate::storage::kv::WriteData;
use crate::storage::lock_manager::LockManager;
use crate::storage::mvcc::metrics::MVCC_DUPLICATE_CMD_COUNTER_VEC;
use crate::storage::mvcc::txn::make_txn_error;
use crate::storage::mvcc::MvccTxn;
use crate::storage::mvcc::{
    Error as MvccError, ErrorInner as MvccErrorInner, Result as MvccResult,
};
use crate::storage::txn::commands::{
    Command, CommandExt, TypedCommand, WriteCommand, WriteContext, WriteResult,
};
use crate::storage::txn::{Error, Result};
use crate::storage::types::PrewriteResult;
use crate::storage::{Error as StorageError, ProcessResult, Snapshot};

command! {
    /// The prewrite phase of a transaction using pessimistic locking. The first phase of 2PC.
    ///
    /// This prepares the system to commit the transaction. Later a [`Commit`](Command::Commit)
    /// or a [`Rollback`](Command::Rollback) should follow.
    PrewritePessimistic:
        cmd_ty => PrewriteResult,
        display => "kv::command::prewrite_pessimistic mutations({}) @ {} | {:?}", (mutations.len, start_ts, ctx),
        content => {
            /// The set of mutations to apply; the bool = is pessimistic lock.
            mutations: Vec<(Mutation, bool)>,
            /// The primary lock. Secondary locks (from `mutations`) will refer to the primary lock.
            primary: Vec<u8>,
            /// The transaction timestamp.
            start_ts: TimeStamp,
            lock_ttl: u64,
            for_update_ts: TimeStamp,
            /// How many keys this transaction involved.
            txn_size: u64,
            min_commit_ts: TimeStamp,
            /// All secondary keys in the whole transaction (i.e., as sent to all nodes, not only
            /// this node). Only present if using async commit.
            secondary_keys: Option<Vec<Vec<u8>>>,
        }
}

impl CommandExt for PrewritePessimistic {
    ctx!();
    tag!(prewrite);
    ts!(start_ts);

    fn write_bytes(&self) -> usize {
        let mut bytes = 0;
        for (m, _) in &self.mutations {
            match *m {
                Mutation::Put((ref key, ref value)) | Mutation::Insert((ref key, ref value)) => {
                    bytes += key.as_encoded().len();
                    bytes += value.len();
                }
                Mutation::Delete(ref key) | Mutation::Lock(ref key) => {
                    bytes += key.as_encoded().len();
                }
                Mutation::CheckNotExists(_) => (),
            }
        }
        bytes
    }

    gen_lock!(mutations: multiple(|(x, _)| x.key()));
}

impl PrewritePessimistic {
    pub fn pessimistic_prewrite<S: Snapshot, P: PdClient + 'static>(
        &mut self,
        txn: &mut MvccTxn<S, P>,
        mutation: Mutation,
        secondary_keys: &Option<Vec<Vec<u8>>>,
        is_pessimistic_lock: bool,
        pipelined_pessimistic_lock: bool,
    ) -> MvccResult<TimeStamp> {
        let mut lock_ttl = self.lock_ttl;
        let mut min_commit_ts = self.min_commit_ts;
        if mutation.should_not_write() {
            return Err(box_err!(
                "cannot handle checkNotExists in pessimistic prewrite"
            ));
        }
        let mutation_type = mutation.mutation_type();
        let lock_type = LockType::from_mutation(&mutation);
        let (key, value) = mutation.into_key_value();

        fail_point!("pessimistic_prewrite", |err| Err(make_txn_error(
            err,
            &key,
            self.start_ts,
        )
        .into()));

        if let Some(lock) = txn.reader.load_lock(&key)? {
            if lock.ts != self.start_ts {
                // Abort on lock belonging to other transaction if
                // prewrites a pessimistic lock.
                if is_pessimistic_lock {
                    warn!(
                        "prewrite failed (pessimistic lock not found)";
                        "start_ts" => self.start_ts,
                        "key" => %key,
                        "lock_ts" => lock.ts
                    );
                    return Err(MvccErrorInner::PessimisticLockNotFound {
                        start_ts: self.start_ts,
                        key: key.into_raw()?,
                    }
                    .into());
                }
                return Err(txn
                    .handle_non_pessimistic_lock_conflict(key, lock)
                    .unwrap_err());
            } else {
                if lock.lock_type != LockType::Pessimistic {
                    // Duplicated command. No need to overwrite the lock and data.
                    MVCC_DUPLICATE_CMD_COUNTER_VEC.prewrite.inc();
                    return Ok(lock.min_commit_ts);
                }
                // The lock is pessimistic and owned by this txn, go through to overwrite it.
                // The ttl and min_commit_ts of the lock may have been pushed forward.
                lock_ttl = std::cmp::max(self.lock_ttl, lock.ttl);
                min_commit_ts = std::cmp::max(self.min_commit_ts, lock.min_commit_ts);
            }
        } else if is_pessimistic_lock {
            txn.amend_pessimistic_lock(pipelined_pessimistic_lock, &key)?;
        }

        txn.check_extra_op(&key, mutation_type, None)?;
        // No need to check data constraint, it's resolved by pessimistic locks.
        txn.prewrite_key_value(
            key,
            lock_type.unwrap(),
            &self.primary,
            secondary_keys,
            value,
            lock_ttl,
            self.for_update_ts,
            self.txn_size,
            min_commit_ts,
        )
    }
}

impl<S: Snapshot, L: LockManager, P: PdClient + 'static> WriteCommand<S, L, P>
    for PrewritePessimistic
{
    fn process_write(
        mut self,
        snapshot: S,
        context: WriteContext<'_, L, P>,
    ) -> Result<WriteResult> {
        let rows = self.mutations.len();
        let mut txn = MvccTxn::new(
            snapshot,
            self.start_ts,
            !self.ctx.get_not_fill_cache(),
            context.pd_client,
        );
        // Althrough pessimistic prewrite doesn't read the write record for checking conflict, we still set extra op here
        // for getting the written keys.
        txn.extra_op = context.extra_op;

        let async_commit_pk: Option<Key> = self
            .secondary_keys
            .as_ref()
            .filter(|keys| !keys.is_empty())
            .map(|_| Key::from_raw(&self.primary));

        let mut locks = vec![];
        let mut async_commit_ts = TimeStamp::zero();
        for (m, is_pessimistic_lock) in self.mutations.clone().into_iter() {
            let mut secondaries = self.secondary_keys.clone().map(|_| vec![]);

            if Some(m.key()) == async_commit_pk.as_ref() {
                secondaries = self.secondary_keys.clone();
            }
            match self.pessimistic_prewrite(
                &mut txn,
                m,
                &secondaries,
                is_pessimistic_lock,
                context.pipelined_pessimistic_lock,
            ) {
                Ok(ts) => {
                    if secondaries.is_some() {
                        async_commit_ts = ts;
                    }
                }
                e @ Err(MvccError(box MvccErrorInner::KeyIsLocked { .. })) => {
                    locks.push(
                        e.map(|_| ())
                            .map_err(Error::from)
                            .map_err(StorageError::from),
                    );
                }
                Err(e) => return Err(Error::from(e)),
            }
        }
        context.statistics.add(&txn.take_statistics());
        let (pr, to_be_write, rows, ctx, lock_info) = if locks.is_empty() {
            let pr = ProcessResult::PrewriteResult {
                result: PrewriteResult {
                    locks: vec![],
                    min_commit_ts: async_commit_ts,
                },
            };
            let txn_extra = txn.take_extra();
            let write_data = WriteData::new(txn.into_modifies(), txn_extra);
            (pr, write_data, rows, self.ctx, None)
        } else {
            // Skip write stage if some keys are locked.
            let pr = ProcessResult::PrewriteResult {
                result: PrewriteResult {
                    locks,
                    min_commit_ts: async_commit_ts,
                },
            };
            (pr, WriteData::default(), 0, self.ctx, None)
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
