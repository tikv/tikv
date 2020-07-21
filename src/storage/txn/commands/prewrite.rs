// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::kv::WriteData;
use crate::storage::lock_manager::LockManager;
use crate::storage::mvcc::{has_data_in_range, MvccTxn};
use crate::storage::mvcc::{Error as MvccError, ErrorInner as MvccErrorInner};
use crate::storage::txn::commands::WriteCommand;
use crate::storage::txn::process::WriteResult;
use crate::storage::txn::Error;
use crate::storage::txn::Result;
use crate::storage::Error as StorageError;
use crate::storage::{
    txn::commands::{Command, CommandExt, TypedCommand},
    types::PrewriteResult,
    Context, ProcessResult, ScanMode, Snapshot, Statistics,
};
use engine_traits::CF_WRITE;
use kvproto::kvrpcpb::ExtraOp;
use pd_client::PdClient;
use std::sync::Arc;
use txn_types::{Key, Mutation, TimeStamp};

const FORWARD_MIN_MUTATIONS_NUM: usize = 12;

command! {
    /// The prewrite phase of a transaction. The first phase of 2PC.
    ///
    /// This prepares the system to commit the transaction. Later a [`Commit`](Command::Commit)
    /// or a [`Rollback`](Command::Rollback) should follow.
    Prewrite:
        cmd_ty => PrewriteResult,
        display => "kv::command::prewrite mutations({}) @ {} | {:?}", (mutations.len, start_ts, ctx),
        content => {
            /// The set of mutations to apply.
            mutations: Vec<Mutation>,
            /// The primary lock. Secondary locks (from `mutations`) will refer to the primary lock.
            primary: Vec<u8>,
            /// The transaction timestamp.
            start_ts: TimeStamp,
            lock_ttl: u64,
            skip_constraint_check: bool,
            /// How many keys this transaction involved.
            txn_size: u64,
            min_commit_ts: TimeStamp,
            /// All secondary keys in the whole transaction (i.e., as sent to all nodes, not only
            /// this node). Only present if using async commit.
            secondary_keys: Option<Vec<Vec<u8>>>,
        }
}

impl CommandExt for Prewrite {
    ctx!();
    tag!(prewrite);
    ts!(start_ts);

    fn write_bytes(&self) -> usize {
        let mut bytes = 0;
        for m in &self.mutations {
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

    gen_lock!(mutations: multiple(|x| x.key()));
}

impl Prewrite {
    #[cfg(test)]
    pub fn with_defaults(
        mutations: Vec<Mutation>,
        primary: Vec<u8>,
        start_ts: TimeStamp,
    ) -> TypedCommand<PrewriteResult> {
        Prewrite::new(
            mutations,
            primary,
            start_ts,
            0,
            false,
            0,
            TimeStamp::default(),
            None,
            Context::default(),
        )
    }

    #[cfg(test)]
    pub fn with_lock_ttl(
        mutations: Vec<Mutation>,
        primary: Vec<u8>,
        start_ts: TimeStamp,
        lock_ttl: u64,
    ) -> TypedCommand<PrewriteResult> {
        Prewrite::new(
            mutations,
            primary,
            start_ts,
            lock_ttl,
            false,
            0,
            TimeStamp::default(),
            None,
            Context::default(),
        )
    }

    pub fn with_context(
        mutations: Vec<Mutation>,
        primary: Vec<u8>,
        start_ts: TimeStamp,
        ctx: Context,
    ) -> TypedCommand<PrewriteResult> {
        Prewrite::new(
            mutations,
            primary,
            start_ts,
            0,
            false,
            0,
            TimeStamp::default(),
            None,
            ctx,
        )
    }
}

impl<S: Snapshot, L: LockManager, P: PdClient + 'static> WriteCommand<S, L, P> for Prewrite {
    fn process_write(
        &mut self,
        snapshot: S,
        _lock_mgr: &L,
        pd_client: Arc<P>,
        extra_op: ExtraOp,
        statistics: &mut Statistics,
        _pipelined_pessimistic_lock: bool,
    ) -> Result<WriteResult> {
        let mut scan_mode = None;
        let rows = self.mutations.len();
        if rows > FORWARD_MIN_MUTATIONS_NUM {
            self.mutations.sort_by(|a, b| a.key().cmp(b.key()));
            let left_key = self.mutations.first().unwrap().key();
            let right_key = self
                .mutations
                .last()
                .unwrap()
                .key()
                .clone()
                .append_ts(TimeStamp::zero());
            if !has_data_in_range(
                snapshot.clone(),
                CF_WRITE,
                left_key,
                &right_key,
                &mut statistics.write,
            )? {
                // If there is no data in range, we could skip constraint check, and use Forward seek for CF_LOCK.
                // Because in most instances, there won't be more than one transaction write the same key. Seek
                // operation could skip nonexistent key in CF_LOCK.
                self.skip_constraint_check = true;
                scan_mode = Some(ScanMode::Forward)
            }
        }
        let mut txn = if scan_mode.is_some() {
            MvccTxn::for_scan(
                snapshot,
                scan_mode,
                self.start_ts,
                !self.ctx.get_not_fill_cache(),
                pd_client,
            )
        } else {
            MvccTxn::new(
                snapshot,
                self.start_ts,
                !self.ctx.get_not_fill_cache(),
                pd_client,
            )
        };

        // Set extra op here for getting the write record when check write conflict in prewrite.
        txn.extra_op = extra_op;

        let primary_key = Key::from_raw(&self.primary);
        let mut locks = vec![];
        let mut async_commit_ts = TimeStamp::zero();
        for m in self.mutations.clone() {
            let mut secondaries = &self.secondary_keys.as_ref().map(|_| vec![]);

            if m.key() == &primary_key {
                secondaries = &self.secondary_keys;
            }
            match txn.prewrite(
                m,
                &self.primary,
                secondaries,
                self.skip_constraint_check,
                self.lock_ttl,
                self.txn_size,
                self.min_commit_ts,
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

        statistics.add(&txn.take_statistics());
        let (pr, to_be_write, rows, ctx, lock_info) = if locks.is_empty() {
            let pr = ProcessResult::PrewriteResult {
                result: PrewriteResult {
                    locks: vec![],
                    min_commit_ts: async_commit_ts,
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
                    min_commit_ts: async_commit_ts,
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
