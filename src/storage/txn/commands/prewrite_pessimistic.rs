// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::kv::WriteData;
use crate::storage::lock_manager::LockManager;
use crate::storage::mvcc::MvccTxn;
use crate::storage::mvcc::{Error as MvccError, ErrorInner as MvccErrorInner};
use crate::storage::txn::commands::{Command, CommandExt, TypedCommand, WriteCommand};
use crate::storage::txn::process::WriteResult;
use crate::storage::txn::Error;
use crate::storage::txn::Result;
use crate::storage::types::PrewriteResult;
use crate::storage::Error as StorageError;
use crate::storage::{ProcessResult, Snapshot, Statistics};
use kvproto::kvrpcpb::ExtraOp;
use pd_client::PdClient;
use std::sync::Arc;
use txn_types::{Mutation, TimeStamp};

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
        }
}

impl CommandExt for PrewritePessimistic {
    ctx!();
    tag!(prewrite);
    ts!(start_ts);
    command_method!(requires_pessimistic_txn, bool, true);

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

impl<S: Snapshot, L: LockManager, P: PdClient + 'static> WriteCommand<S, L, P>
    for PrewritePessimistic
{
    fn process_write(
        &mut self,
        snapshot: S,
        _lock_mgr: &L,
        pd_client: Arc<P>,
        extra_op: ExtraOp,
        statistics: &mut Statistics,
        pipelined_pessimistic_lock: bool,
    ) -> Result<WriteResult> {
        let rows = self.mutations.len();
        let mut txn = MvccTxn::new(
            snapshot,
            self.start_ts,
            !self.ctx.get_not_fill_cache(),
            pd_client,
        );
        // Althrough pessimistic prewrite doesn't read the write record for checking conflict, we still set extra op here
        // for getting the written keys.
        txn.extra_op = extra_op;

        let mut locks = vec![];
        for (m, is_pessimistic_lock) in self.mutations.clone().into_iter() {
            match txn.pessimistic_prewrite(
                m,
                &self.primary,
                is_pessimistic_lock,
                self.lock_ttl,
                self.for_update_ts,
                self.txn_size,
                self.min_commit_ts,
                pipelined_pessimistic_lock,
            ) {
                Ok(_) => {}
                e @ Err(MvccError(box MvccErrorInner::KeyIsLocked { .. })) => {
                    locks.push(e.map_err(Error::from).map_err(StorageError::from));
                }
                Err(e) => return Err(Error::from(e)),
            }
        }

        statistics.add(&txn.take_statistics());
        let (pr, to_be_write, rows, ctx, lock_info) = if locks.is_empty() {
            let pr = ProcessResult::PrewriteResult {
                result: PrewriteResult {
                    locks: vec![],
                    min_commit_ts: TimeStamp::zero(),
                },
            };
            let txn_extra = txn.take_extra();
            let write_data = WriteData::new(txn.into_modifies(), txn_extra);
            (pr, write_data, rows, self.ctx.clone(), None)
        } else {
            // Skip write stage if some keys are locked.
            let pr = ProcessResult::PrewriteResult {
                result: PrewriteResult {
                    locks,
                    min_commit_ts: TimeStamp::zero(),
                },
            };
            (pr, WriteData::default(), 0, self.ctx.clone(), None)
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
