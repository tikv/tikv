// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! Functionality for handling optimistic and pessimistic prewrites. These are separate commands
//! (although maybe they shouldn't be since there is only one protobuf), but
//! handling of the commands is similar. We therefore have a single type (Prewriter) to handle both
//! kinds of prewrite.

use crate::storage::{
    kv::WriteData,
    lock_manager::LockManager,
    mvcc::{has_data_in_range, Error as MvccError, ErrorInner as MvccErrorInner, MvccTxn},
    txn::{
        actions::prewrite::{prewrite, CommitKind, TransactionKind, TransactionProperties},
        commands::{
            Command, CommandExt, ReleasedLocks, ResponsePolicy, TypedCommand, WriteCommand,
            WriteContext, WriteResult,
        },
        Error, ErrorInner, Result,
    },
    types::PrewriteResult,
    Context, Error as StorageError, ProcessResult, Snapshot,
};
use engine_traits::CF_WRITE;
use kvproto::kvrpcpb::ExtraOp;
use std::mem;
use txn_types::{Key, Mutation, OldValues, TimeStamp, TxnExtra, Write, WriteType};

pub(crate) const FORWARD_MIN_MUTATIONS_NUM: usize = 12;

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
            /// Limits the maximum value of commit ts of async commit and 1PC, which can be used to
            /// avoid inconsistency with schema change.
            max_commit_ts: TimeStamp,
            /// All secondary keys in the whole transaction (i.e., as sent to all nodes, not only
            /// this node). Only present if using async commit.
            secondary_keys: Option<Vec<Vec<u8>>>,
            /// When the transaction involves only one region, it's possible to commit the
            /// transaction directly with 1PC protocol.
            try_one_pc: bool,
        }
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
            TimeStamp::default(),
            None,
            false,
            Context::default(),
        )
    }

    #[cfg(test)]
    pub fn with_1pc(
        mutations: Vec<Mutation>,
        primary: Vec<u8>,
        start_ts: TimeStamp,
        max_commit_ts: TimeStamp,
    ) -> TypedCommand<PrewriteResult> {
        Prewrite::new(
            mutations,
            primary,
            start_ts,
            0,
            false,
            0,
            TimeStamp::default(),
            max_commit_ts,
            None,
            true,
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
            TimeStamp::default(),
            None,
            false,
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
            TimeStamp::default(),
            None,
            false,
            ctx,
        )
    }

    fn into_prewriter(self) -> Prewriter<Optimistic> {
        Prewriter {
            kind: Optimistic {
                skip_constraint_check: self.skip_constraint_check,
            },
            mutations: self.mutations,
            start_ts: self.start_ts,
            lock_ttl: self.lock_ttl,
            txn_size: self.txn_size,
            try_one_pc: self.try_one_pc,
            min_commit_ts: self.min_commit_ts,
            max_commit_ts: self.max_commit_ts,

            primary: self.primary,
            secondary_keys: self.secondary_keys,

            ctx: self.ctx,
            old_values: OldValues::default(),
        }
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

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for Prewrite {
    fn process_write(self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        self.into_prewriter().process_write(snapshot, context)
    }
}

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
            /// Limits the maximum value of commit ts of 1PC and async commit, which can be used to
            /// avoid inconsistency with schema change.
            max_commit_ts: TimeStamp,
            /// All secondary keys in the whole transaction (i.e., as sent to all nodes, not only
            /// this node). Only present if using async commit.
            secondary_keys: Option<Vec<Vec<u8>>>,
            /// When the transaction involves only one region, it's possible to commit the
            /// transaction directly with 1PC protocol.
            try_one_pc: bool,
        }
}

impl PrewritePessimistic {
    #[cfg(test)]
    pub fn with_defaults(
        mutations: Vec<(Mutation, bool)>,
        primary: Vec<u8>,
        start_ts: TimeStamp,
        for_update_ts: TimeStamp,
    ) -> TypedCommand<PrewriteResult> {
        PrewritePessimistic::new(
            mutations,
            primary,
            start_ts,
            0,
            for_update_ts,
            0,
            TimeStamp::default(),
            TimeStamp::default(),
            None,
            false,
            Context::default(),
        )
    }

    #[cfg(test)]
    pub fn with_1pc(
        mutations: Vec<(Mutation, bool)>,
        primary: Vec<u8>,
        start_ts: TimeStamp,
        for_update_ts: TimeStamp,
        max_commit_ts: TimeStamp,
    ) -> TypedCommand<PrewriteResult> {
        PrewritePessimistic::new(
            mutations,
            primary,
            start_ts,
            0,
            for_update_ts,
            0,
            TimeStamp::default(),
            max_commit_ts,
            None,
            true,
            Context::default(),
        )
    }

    fn into_prewriter(self) -> Prewriter<Pessimistic> {
        Prewriter {
            kind: Pessimistic {
                for_update_ts: self.for_update_ts,
            },
            start_ts: self.start_ts,
            txn_size: self.txn_size,
            primary: self.primary,
            mutations: self.mutations,

            try_one_pc: self.try_one_pc,
            secondary_keys: self.secondary_keys,
            lock_ttl: self.lock_ttl,
            min_commit_ts: self.min_commit_ts,
            max_commit_ts: self.max_commit_ts,

            ctx: self.ctx,
            old_values: OldValues::default(),
        }
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

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for PrewritePessimistic {
    fn process_write(self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        self.into_prewriter().process_write(snapshot, context)
    }
}

/// Handles both kinds of prewrite (K statically indicates either optimistic or pessimistic).
struct Prewriter<K: PrewriteKind> {
    kind: K,
    mutations: Vec<K::Mutation>,
    primary: Vec<u8>,
    start_ts: TimeStamp,
    lock_ttl: u64,
    txn_size: u64,
    min_commit_ts: TimeStamp,
    max_commit_ts: TimeStamp,
    secondary_keys: Option<Vec<Vec<u8>>>,
    old_values: OldValues,
    try_one_pc: bool,

    ctx: Context,
}

impl<K: PrewriteKind> Prewriter<K> {
    /// Entry point for handling a prewrite by Prewriter.
    fn process_write(
        mut self,
        snapshot: impl Snapshot,
        mut context: WriteContext<'_, impl LockManager>,
    ) -> Result<WriteResult> {
        self.kind
            .can_skip_constraint_check(&mut self.mutations, &snapshot, &mut context)?;
        self.check_max_ts_synced(&snapshot)?;

        let mut txn = MvccTxn::new(
            snapshot,
            self.start_ts,
            !self.ctx.get_not_fill_cache(),
            context.concurrency_manager,
        );
        // Set extra op here for getting the write record when check write conflict in prewrite.

        let rows = self.mutations.len();
        let (locks, final_min_commit_ts) = self.prewrite(&mut txn, context.extra_op)?;

        context.statistics.add(&txn.take_statistics());

        Ok(self.write_result(
            locks,
            txn,
            final_min_commit_ts,
            rows,
            context.async_apply_prewrite,
            context.lock_mgr,
        ))
    }

    // Async commit requires the max timestamp in the concurrency manager to be up-to-date.
    // If it is possibly stale due to leader transfer or region merge, return an error.
    // TODO: Fallback to non-async commit if not synced instead of returning an error.
    fn check_max_ts_synced(&self, snapshot: &impl Snapshot) -> Result<()> {
        if (self.secondary_keys.is_some() || self.try_one_pc) && !snapshot.is_max_ts_synced() {
            Err(ErrorInner::MaxTimestampNotSynced {
                region_id: self.ctx.get_region_id(),
                start_ts: self.start_ts,
            }
            .into())
        } else {
            Ok(())
        }
    }

    /// The core part of the prewrite action. In the abstract, this method iterates over the mutations
    /// in the prewrite and prewrites each one. It keeps track of any locks encountered and (if it's
    /// an async commit transaction) the min_commit_ts, these are returned by the method.
    fn prewrite(
        &mut self,
        txn: &mut MvccTxn<impl Snapshot>,
        extra_op: ExtraOp,
    ) -> Result<(Vec<std::result::Result<(), StorageError>>, TimeStamp)> {
        let commit_kind = match (&self.secondary_keys, self.try_one_pc) {
            (_, true) => CommitKind::OnePc(self.max_commit_ts),
            (&Some(_), false) => CommitKind::Async(self.max_commit_ts),
            (&None, false) => CommitKind::TwoPc,
        };

        let mut props = TransactionProperties {
            start_ts: self.start_ts,
            kind: self.kind.txn_kind(),
            commit_kind,
            primary: &self.primary,
            txn_size: self.txn_size,
            lock_ttl: self.lock_ttl,
            min_commit_ts: self.min_commit_ts,
            need_old_value: extra_op == ExtraOp::ReadOldValue,
        };

        let async_commit_pk = self
            .secondary_keys
            .as_ref()
            .filter(|keys| !keys.is_empty())
            .map(|_| Key::from_raw(&self.primary));
        let mut async_commit_pk = async_commit_pk.as_ref();

        let mut final_min_commit_ts = TimeStamp::zero();
        let mut locks = Vec::new();

        for m in mem::take(&mut self.mutations) {
            let is_pessimistic_lock = m.is_pessimistic_lock();
            let m = m.into_mutation();
            let key = m.key().clone();
            let mutation_type = m.mutation_type();
            let mut secondaries = &self.secondary_keys.as_ref().map(|_| vec![]);
            if Some(m.key()) == async_commit_pk {
                secondaries = &self.secondary_keys;
            }

            let prewrite_result = prewrite(txn, &props, m, secondaries, is_pessimistic_lock);
            match prewrite_result {
                Ok((ts, old_value)) => {
                    if (secondaries.is_some() || self.try_one_pc) && final_min_commit_ts < ts {
                        final_min_commit_ts = ts;
                    }
                    if old_value.specified() {
                        self.old_values.insert(key, (old_value, mutation_type));
                    }
                }
                Err(MvccError(box MvccErrorInner::CommitTsTooLarge { .. })) => {
                    // fallback to not using async commit or 1pc
                    props.commit_kind = CommitKind::TwoPc;
                    async_commit_pk = None;
                    self.secondary_keys = None;
                    self.try_one_pc = false;
                    fallback_1pc_locks(txn);
                    // release memory locks
                    txn.guards = Vec::new();
                    final_min_commit_ts = TimeStamp::zero();
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

        Ok((locks, final_min_commit_ts))
    }

    /// Prepare a WriteResult object from the results of executing the prewrite.
    fn write_result(
        self,
        locks: Vec<std::result::Result<(), StorageError>>,
        mut txn: MvccTxn<impl Snapshot>,
        final_min_commit_ts: TimeStamp,
        rows: usize,
        async_apply_prewrite: bool,
        lock_manager: &impl LockManager,
    ) -> WriteResult {
        let async_commit_ts = if self.secondary_keys.is_some() {
            final_min_commit_ts
        } else {
            TimeStamp::zero()
        };

        let mut result = if locks.is_empty() {
            let pr = ProcessResult::PrewriteResult {
                result: PrewriteResult {
                    locks: vec![],
                    min_commit_ts: async_commit_ts,
                    one_pc_commit_ts: one_pc_commit_ts(
                        self.try_one_pc,
                        &mut txn,
                        final_min_commit_ts,
                        lock_manager,
                    ),
                },
            };
            let extra = TxnExtra {
                old_values: self.old_values,
                // Set one_pc flag in TxnExtra to let CDC skip handling the resolver.
                one_pc: self.try_one_pc,
            };
            // Here the lock guards are taken and will be released after the write finishes.
            // If an error (KeyIsLocked or WriteConflict) occurs before, these lock guards
            // are dropped along with `txn` automatically.
            let lock_guards = txn.take_guards();
            WriteResult {
                ctx: self.ctx,
                to_be_write: WriteData::new(txn.into_modifies(), extra),
                rows,
                pr,
                lock_info: None,
                lock_guards,
                response_policy: ResponsePolicy::OnApplied,
            }
        } else {
            // Skip write stage if some keys are locked.
            let pr = ProcessResult::PrewriteResult {
                result: PrewriteResult {
                    locks,
                    min_commit_ts: async_commit_ts,
                    one_pc_commit_ts: TimeStamp::zero(),
                },
            };
            WriteResult {
                ctx: self.ctx,
                to_be_write: WriteData::default(),
                rows,
                pr,
                lock_info: None,
                lock_guards: vec![],
                response_policy: ResponsePolicy::OnApplied,
            }
        };

        // Currently if `try_one_pc` is set, it must have succeeded here.
        if (!async_commit_ts.is_zero() || self.try_one_pc) && async_apply_prewrite {
            result.response_policy = ResponsePolicy::OnCommitted
        }

        result
    }
}

/// Encapsulates things which must be done differently for optimistic or pessimistic transactions.
trait PrewriteKind {
    /// The type of mutation and, optionally, its extra information, differing for the
    /// optimistic and pessimistic transaction.
    type Mutation: MutationLock;

    fn txn_kind(&self) -> TransactionKind;

    fn can_skip_constraint_check(
        &mut self,
        _mutations: &mut [Self::Mutation],
        _snapshot: &impl Snapshot,
        _context: &mut WriteContext<'_, impl LockManager>,
    ) -> Result<()> {
        Ok(())
    }
}

/// Optimistic `PreWriteKind`.
struct Optimistic {
    skip_constraint_check: bool,
}

impl PrewriteKind for Optimistic {
    type Mutation = Mutation;

    fn txn_kind(&self) -> TransactionKind {
        TransactionKind::Optimistic(self.skip_constraint_check)
    }

    // If there is no data in range, we could skip constraint check.
    fn can_skip_constraint_check(
        &mut self,
        mutations: &mut [Self::Mutation],
        snapshot: &impl Snapshot,
        context: &mut WriteContext<'_, impl LockManager>,
    ) -> Result<()> {
        if mutations.len() > FORWARD_MIN_MUTATIONS_NUM {
            mutations.sort_by(|a, b| a.key().cmp(b.key()));
            let left_key = mutations.first().unwrap().key();
            let right_key = mutations
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
                &mut context.statistics.write,
            )? {
                self.skip_constraint_check = true;
            }
        }
        Ok(())
    }
}

/// Pessimistic `PreWriteKind`.
struct Pessimistic {
    for_update_ts: TimeStamp,
}

impl PrewriteKind for Pessimistic {
    type Mutation = (Mutation, bool);

    fn txn_kind(&self) -> TransactionKind {
        TransactionKind::Pessimistic(self.for_update_ts)
    }
}

/// The type of mutation and, optionally, its extra information, differing for the
/// optimistic and pessimistic transaction.
/// For optimistic txns, this is `Mutation`.
/// For pessimistic txns, this is `(Mutation, bool)`, where the bool indicates
/// whether the mutation takes a pessimistic lock or not.
trait MutationLock {
    fn is_pessimistic_lock(&self) -> bool;
    fn into_mutation(self) -> Mutation;
}

impl MutationLock for Mutation {
    fn is_pessimistic_lock(&self) -> bool {
        false
    }

    fn into_mutation(self) -> Mutation {
        self
    }
}

impl MutationLock for (Mutation, bool) {
    fn is_pessimistic_lock(&self) -> bool {
        self.1
    }

    fn into_mutation(self) -> Mutation {
        self.0
    }
}

/// Compute the commit ts of a 1pc transaction.
pub fn one_pc_commit_ts(
    try_one_pc: bool,
    txn: &mut MvccTxn<impl Snapshot>,
    final_min_commit_ts: TimeStamp,
    lock_manager: &impl LockManager,
) -> TimeStamp {
    if try_one_pc {
        assert_ne!(final_min_commit_ts, TimeStamp::zero());
        // All keys can be successfully locked and `try_one_pc` is set. Try to directly
        // commit them.
        let released_locks = handle_1pc_locks(txn, final_min_commit_ts);
        if !released_locks.is_empty() {
            released_locks.wake_up(lock_manager);
        }
        final_min_commit_ts
    } else {
        assert!(txn.locks_for_1pc.is_empty());
        TimeStamp::zero()
    }
}

/// Commit and delete all 1pc locks in txn.
fn handle_1pc_locks<S: Snapshot>(txn: &mut MvccTxn<S>, commit_ts: TimeStamp) -> ReleasedLocks {
    let mut released_locks = ReleasedLocks::new(txn.start_ts, commit_ts);

    for (key, lock, delete_pessimistic_lock) in std::mem::take(&mut txn.locks_for_1pc) {
        let write = Write::new(
            WriteType::from_lock_type(lock.lock_type).unwrap(),
            txn.start_ts,
            lock.short_value,
        );
        // Transactions committed with 1PC should be impossible to overwrite rollback records.
        txn.put_write(key.clone(), commit_ts, write.as_ref().to_bytes());
        if delete_pessimistic_lock {
            released_locks.push(txn.unlock_key(key, true));
        }
    }

    released_locks
}

/// Change all 1pc locks in txn to 2pc locks.
pub(in crate::storage::txn) fn fallback_1pc_locks<S: Snapshot>(txn: &mut MvccTxn<S>) {
    for (key, lock, _) in std::mem::take(&mut txn.locks_for_1pc) {
        txn.put_lock(key, &lock);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{
        mvcc::{tests::*, Error as MvccError, ErrorInner as MvccErrorInner},
        txn::{
            commands::test_util::prewrite_command,
            commands::test_util::{
                commit, pessimsitic_prewrite_with_cm, prewrite, prewrite_with_cm, rollback,
            },
            tests::{must_acquire_pessimistic_lock, must_rollback},
            Error, ErrorInner,
        },
        Engine, Snapshot, Statistics, TestEngineBuilder,
    };
    use concurrency_manager::ConcurrencyManager;
    use engine_traits::CF_WRITE;
    use kvproto::kvrpcpb::Context;
    use txn_types::{Key, Mutation, TimeStamp};

    fn inner_test_prewrite_skip_constraint_check(pri_key_number: u8, write_num: usize) {
        let mut mutations = Vec::default();
        let pri_key = &[pri_key_number];
        for i in 0..write_num {
            mutations.push(Mutation::Insert((
                Key::from_raw(&[i as u8]),
                b"100".to_vec(),
            )));
        }
        let mut statistic = Statistics::default();
        let engine = TestEngineBuilder::new().build().unwrap();
        prewrite(
            &engine,
            &mut statistic,
            vec![Mutation::Put((
                Key::from_raw(&[pri_key_number]),
                b"100".to_vec(),
            ))],
            pri_key.to_vec(),
            99,
            None,
        )
        .unwrap();
        assert_eq!(1, statistic.write.seek);
        let e = prewrite(
            &engine,
            &mut statistic,
            mutations.clone(),
            pri_key.to_vec(),
            100,
            None,
        )
        .err()
        .unwrap();
        assert_eq!(2, statistic.write.seek);
        match e {
            Error(box ErrorInner::Mvcc(MvccError(box MvccErrorInner::KeyIsLocked(_)))) => (),
            _ => panic!("error type not match"),
        }
        commit(
            &engine,
            &mut statistic,
            vec![Key::from_raw(&[pri_key_number])],
            99,
            102,
        )
        .unwrap();
        assert_eq!(2, statistic.write.seek);
        let e = prewrite(
            &engine,
            &mut statistic,
            mutations.clone(),
            pri_key.to_vec(),
            101,
            None,
        )
        .err()
        .unwrap();
        match e {
            Error(box ErrorInner::Mvcc(MvccError(box MvccErrorInner::WriteConflict {
                ..
            }))) => (),
            _ => panic!("error type not match"),
        }
        let e = prewrite(
            &engine,
            &mut statistic,
            mutations.clone(),
            pri_key.to_vec(),
            104,
            None,
        )
        .err()
        .unwrap();
        match e {
            Error(box ErrorInner::Mvcc(MvccError(box MvccErrorInner::AlreadyExist { .. }))) => (),
            _ => panic!("error type not match"),
        }

        statistic.write.seek = 0;
        let ctx = Context::default();
        engine
            .delete_cf(
                &ctx,
                CF_WRITE,
                Key::from_raw(&[pri_key_number]).append_ts(102.into()),
            )
            .unwrap();
        prewrite(
            &engine,
            &mut statistic,
            mutations.clone(),
            pri_key.to_vec(),
            104,
            None,
        )
        .unwrap();
        // All keys are prewrited successful with only one seek operations.
        assert_eq!(1, statistic.write.seek);
        let keys: Vec<Key> = mutations.iter().map(|m| m.key().clone()).collect();
        commit(&engine, &mut statistic, keys.clone(), 104, 105).unwrap();
        let snap = engine.snapshot(Default::default()).unwrap();
        for k in keys {
            let v = snap.get_cf(CF_WRITE, &k.append_ts(105.into())).unwrap();
            assert!(v.is_some());
        }
    }

    #[test]
    fn test_prewrite_skip_constraint_check() {
        inner_test_prewrite_skip_constraint_check(0, FORWARD_MIN_MUTATIONS_NUM + 1);
        inner_test_prewrite_skip_constraint_check(5, FORWARD_MIN_MUTATIONS_NUM + 1);
        inner_test_prewrite_skip_constraint_check(
            FORWARD_MIN_MUTATIONS_NUM as u8,
            FORWARD_MIN_MUTATIONS_NUM + 1,
        );
    }

    #[test]
    fn test_prewrite_skip_too_many_tombstone() {
        use crate::server::gc_worker::gc_by_compact;
        use crate::storage::kv::PerfStatisticsInstant;
        use engine_rocks::{set_perf_level, PerfLevel};
        let mut mutations = Vec::default();
        let pri_key_number = 0;
        let pri_key = &[pri_key_number];
        for i in 0..40 {
            mutations.push(Mutation::Insert((
                Key::from_raw(&[b'z', i as u8]),
                b"100".to_vec(),
            )));
        }
        let engine = TestEngineBuilder::new().build().unwrap();
        let keys: Vec<Key> = mutations.iter().map(|m| m.key().clone()).collect();
        let mut statistic = Statistics::default();
        prewrite(
            &engine,
            &mut statistic,
            mutations.clone(),
            pri_key.to_vec(),
            100,
            None,
        )
        .unwrap();
        // Rollback to make tombstones in lock-cf.
        rollback(&engine, &mut statistic, keys, 100).unwrap();
        // Gc rollback flags store in write-cf to make sure the next prewrite operation will skip
        // seek write cf.
        gc_by_compact(&engine, pri_key, 101);
        set_perf_level(PerfLevel::EnableTimeExceptForMutex);
        let perf = PerfStatisticsInstant::new();
        let mut statistic = Statistics::default();
        while mutations.len() > FORWARD_MIN_MUTATIONS_NUM + 1 {
            mutations.pop();
        }
        prewrite(
            &engine,
            &mut statistic,
            mutations,
            pri_key.to_vec(),
            110,
            None,
        )
        .unwrap();
        let d = perf.delta();
        assert_eq!(1, statistic.write.seek);
        assert_eq!(d.0.internal_delete_skipped_count, 0);
    }

    #[test]
    fn test_prewrite_1pc() {
        use crate::storage::mvcc::tests::{must_get, must_get_commit_ts, must_unlocked};

        let engine = TestEngineBuilder::new().build().unwrap();
        let cm = concurrency_manager::ConcurrencyManager::new(1.into());

        let key = b"k";
        let value = b"v";
        let mutations = vec![Mutation::Put((Key::from_raw(key), value.to_vec()))];

        let mut statistics = Statistics::default();
        prewrite_with_cm(
            &engine,
            cm.clone(),
            &mut statistics,
            mutations,
            key.to_vec(),
            10,
            Some(15),
        )
        .unwrap();
        must_unlocked(&engine, key);
        must_get(&engine, key, 12, value);
        must_get_commit_ts(&engine, key, 10, 11);

        cm.update_max_ts(50.into());

        let mutations = vec![Mutation::Put((Key::from_raw(key), value.to_vec()))];

        let mut statistics = Statistics::default();
        let res = prewrite_with_cm(
            &engine,
            cm.clone(),
            &mut statistics,
            mutations,
            key.to_vec(),
            20,
            Some(30),
        )
        .unwrap();
        assert!(res.min_commit_ts.is_zero());
        assert!(res.one_pc_commit_ts.is_zero());
        must_locked(&engine, key, 20);

        must_rollback(&engine, key, 20);
        let mutations = vec![
            Mutation::Put((Key::from_raw(key), value.to_vec())),
            Mutation::CheckNotExists(Key::from_raw(b"non_exist")),
        ];
        let mut statistics = Statistics::default();
        prewrite_with_cm(
            &engine,
            cm,
            &mut statistics,
            mutations,
            key.to_vec(),
            40,
            Some(60),
        )
        .unwrap();
    }

    #[test]
    fn test_prewrite_pessimsitic_1pc() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let cm = concurrency_manager::ConcurrencyManager::new(1.into());
        let key = b"k";
        let value = b"v";

        must_acquire_pessimistic_lock(&engine, key, key, 10, 10);

        let mutations = vec![(Mutation::Put((Key::from_raw(key), value.to_vec())), true)];
        let mut statistics = Statistics::default();
        pessimsitic_prewrite_with_cm(
            &engine,
            cm.clone(),
            &mut statistics,
            mutations,
            key.to_vec(),
            10,
            10,
            Some(15),
        )
        .unwrap();

        must_unlocked(&engine, key);
        must_get(&engine, key, 12, value);
        must_get_commit_ts(&engine, key, 10, 11);

        let (k1, v1) = (b"k", b"v");
        let (k2, v2) = (b"k2", b"v2");

        must_acquire_pessimistic_lock(&engine, k1, k1, 8, 12);

        let mutations = vec![
            (Mutation::Put((Key::from_raw(k1), v1.to_vec())), true),
            (Mutation::Put((Key::from_raw(k2), v2.to_vec())), false),
        ];
        statistics = Statistics::default();
        pessimsitic_prewrite_with_cm(
            &engine,
            cm.clone(),
            &mut statistics,
            mutations,
            k1.to_vec(),
            8,
            12,
            Some(15),
        )
        .unwrap();

        must_unlocked(&engine, k1);
        must_unlocked(&engine, k2);
        must_get(&engine, k1, 16, v1);
        must_get(&engine, k2, 16, v2);
        must_get_commit_ts(&engine, k1, 8, 13);
        must_get_commit_ts(&engine, k2, 8, 13);

        cm.update_max_ts(50.into());
        must_acquire_pessimistic_lock(&engine, k1, k1, 20, 20);

        let mutations = vec![(Mutation::Put((Key::from_raw(k1), v1.to_vec())), true)];
        statistics = Statistics::default();
        let res = pessimsitic_prewrite_with_cm(
            &engine,
            cm,
            &mut statistics,
            mutations,
            k1.to_vec(),
            20,
            20,
            Some(30),
        )
        .unwrap();
        assert!(res.min_commit_ts.is_zero());
        assert!(res.one_pc_commit_ts.is_zero());
        must_locked(&engine, k1, 20);
    }

    #[test]
    fn test_prewrite_async_commit() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let cm = concurrency_manager::ConcurrencyManager::new(1.into());

        let key = b"k";
        let value = b"v";
        let mutations = vec![Mutation::Put((Key::from_raw(key), value.to_vec()))];

        let mut statistics = Statistics::default();
        let cmd = super::Prewrite::new(
            mutations,
            key.to_vec(),
            10.into(),
            0,
            false,
            1,
            TimeStamp::default(),
            TimeStamp::default(),
            Some(vec![]),
            false,
            Context::default(),
        );

        let res = prewrite_command(&engine, cm.clone(), &mut statistics, cmd).unwrap();
        assert!(!res.min_commit_ts.is_zero());
        assert_eq!(res.one_pc_commit_ts, TimeStamp::zero());
        must_locked(&engine, key, 10);

        cm.update_max_ts(50.into());

        let (k1, v1) = (b"k1", b"v1");
        let (k2, v2) = (b"k2", b"v2");

        let mutations = vec![
            Mutation::Put((Key::from_raw(k1), v1.to_vec())),
            Mutation::Put((Key::from_raw(k2), v2.to_vec())),
        ];
        let mut statistics = Statistics::default();
        // calculated_ts > max_commit_ts
        let cmd = super::Prewrite::new(
            mutations,
            k1.to_vec(),
            20.into(),
            0,
            false,
            2,
            TimeStamp::default(),
            40.into(),
            Some(vec![k2.to_vec()]),
            false,
            Context::default(),
        );

        let res = prewrite_command(&engine, cm, &mut statistics, cmd).unwrap();
        assert!(res.min_commit_ts.is_zero());
        assert!(res.one_pc_commit_ts.is_zero());
        assert!(!must_locked(&engine, k1, 20).use_async_commit);
        assert!(!must_locked(&engine, k2, 20).use_async_commit);
    }

    #[test]
    fn test_prewrite_pessimsitic_async_commit() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let cm = concurrency_manager::ConcurrencyManager::new(1.into());

        let key = b"k";
        let value = b"v";

        must_acquire_pessimistic_lock(&engine, key, key, 10, 10);

        let mutations = vec![(Mutation::Put((Key::from_raw(key), value.to_vec())), true)];
        let mut statistics = Statistics::default();
        let cmd = super::PrewritePessimistic::new(
            mutations,
            key.to_vec(),
            10.into(),
            0,
            10.into(),
            1,
            TimeStamp::default(),
            TimeStamp::default(),
            Some(vec![]),
            false,
            Context::default(),
        );

        let res = prewrite_command(&engine, cm.clone(), &mut statistics, cmd).unwrap();
        assert!(!res.min_commit_ts.is_zero());
        assert_eq!(res.one_pc_commit_ts, TimeStamp::zero());
        must_locked(&engine, key, 10);

        cm.update_max_ts(50.into());

        let (k1, v1) = (b"k1", b"v1");
        let (k2, v2) = (b"k2", b"v2");

        must_acquire_pessimistic_lock(&engine, k1, k1, 20, 20);
        must_acquire_pessimistic_lock(&engine, k2, k1, 20, 20);

        let mutations = vec![
            (Mutation::Put((Key::from_raw(k1), v1.to_vec())), true),
            (Mutation::Put((Key::from_raw(k2), v2.to_vec())), true),
        ];
        let mut statistics = Statistics::default();
        // calculated_ts > max_commit_ts
        let cmd = super::PrewritePessimistic::new(
            mutations,
            k1.to_vec(),
            20.into(),
            0,
            20.into(),
            2,
            TimeStamp::default(),
            40.into(),
            Some(vec![k2.to_vec()]),
            false,
            Context::default(),
        );

        let res = prewrite_command(&engine, cm, &mut statistics, cmd).unwrap();
        assert!(res.min_commit_ts.is_zero());
        assert!(res.one_pc_commit_ts.is_zero());
        assert!(!must_locked(&engine, k1, 20).use_async_commit);
        assert!(!must_locked(&engine, k2, 20).use_async_commit);
    }

    #[test]
    fn test_out_of_sync_max_ts() {
        use crate::storage::{
            kv::Result, CfName, ConcurrencyManager, Cursor, DummyLockManager, IterOptions,
            ScanMode, Value,
        };
        use engine_rocks::RocksEngineIterator;
        use engine_traits::ReadOptions;
        use kvproto::kvrpcpb::ExtraOp;
        #[derive(Clone)]
        struct MockSnapshot;

        impl Snapshot for MockSnapshot {
            type Iter = RocksEngineIterator;

            fn get(&self, _: &Key) -> Result<Option<Value>> {
                unimplemented!()
            }
            fn get_cf(&self, _: CfName, _: &Key) -> Result<Option<Value>> {
                unimplemented!()
            }
            fn get_cf_opt(&self, _: ReadOptions, _: CfName, _: &Key) -> Result<Option<Value>> {
                unimplemented!()
            }
            fn iter(&self, _: IterOptions, _: ScanMode) -> Result<Cursor<Self::Iter>> {
                unimplemented!()
            }
            fn iter_cf(
                &self,
                _: CfName,
                _: IterOptions,
                _: ScanMode,
            ) -> Result<Cursor<Self::Iter>> {
                unimplemented!()
            }
            fn is_max_ts_synced(&self) -> bool {
                false
            }
        }

        macro_rules! context {
            () => {
                WriteContext {
                    lock_mgr: &DummyLockManager {},
                    concurrency_manager: ConcurrencyManager::new(10.into()),
                    extra_op: ExtraOp::Noop,
                    statistics: &mut Statistics::default(),
                    async_apply_prewrite: false,
                }
            };
        }

        macro_rules! assert_max_ts_err {
            ($e: expr) => {
                match $e {
                    Err(Error(box ErrorInner::MaxTimestampNotSynced { .. })) => {}
                    _ => panic!("Should have returned an error"),
                }
            };
        }

        // 2pc should be ok
        let cmd = Prewrite::with_defaults(vec![], vec![1, 2, 3], 10.into());
        cmd.cmd.process_write(MockSnapshot, context!()).unwrap();
        // But 1pc should return an error
        let cmd = Prewrite::with_1pc(vec![], vec![1, 2, 3], 10.into(), 20.into());
        assert_max_ts_err!(cmd.cmd.process_write(MockSnapshot, context!()));
        // And so should async commit
        let mut cmd = Prewrite::with_defaults(vec![], vec![1, 2, 3], 10.into());
        if let Command::Prewrite(p) = &mut cmd.cmd {
            p.secondary_keys = Some(vec![]);
        }
        assert_max_ts_err!(cmd.cmd.process_write(MockSnapshot, context!()));

        // And the same for pessimistic prewrites.
        let cmd = PrewritePessimistic::with_defaults(vec![], vec![1, 2, 3], 10.into(), 15.into());
        cmd.cmd.process_write(MockSnapshot, context!()).unwrap();
        let cmd =
            PrewritePessimistic::with_1pc(vec![], vec![1, 2, 3], 10.into(), 15.into(), 20.into());
        assert_max_ts_err!(cmd.cmd.process_write(MockSnapshot, context!()));
        let mut cmd =
            PrewritePessimistic::with_defaults(vec![], vec![1, 2, 3], 10.into(), 15.into());
        if let Command::PrewritePessimistic(p) = &mut cmd.cmd {
            p.secondary_keys = Some(vec![]);
        }
        assert_max_ts_err!(cmd.cmd.process_write(MockSnapshot, context!()));
    }

    // this test shows which stage in raft can we return the response
    #[test]
    fn test_response_stage() {
        use crate::storage::DummyLockManager;
        use kvproto::kvrpcpb::ExtraOp;
        let cm = ConcurrencyManager::new(42.into());
        let start_ts = TimeStamp::new(10);
        let keys = [b"k1", b"k2"];
        let values = [b"v1", b"v2"];
        let mutations = vec![
            Mutation::Put((Key::from_raw(keys[0]), keys[0].to_vec())),
            Mutation::Put((Key::from_raw(keys[1]), values[1].to_vec())),
        ];
        let mut statistics = Statistics::default();

        #[derive(Clone)]
        struct Case {
            expected: ResponsePolicy,

            // inputs
            // optimistic/pessimistic prewrite
            pessimistic: bool,
            // async commit on/off
            async_commit: bool,
            // 1pc on/off
            one_pc: bool,
            // async_apply_prewrite enabled in config
            async_apply_prewrite: bool,
        }

        let cases = vec![
            Case {
                // basic case
                expected: ResponsePolicy::OnApplied,

                pessimistic: false,
                async_commit: false,
                one_pc: false,
                async_apply_prewrite: false,
            },
            Case {
                // async_apply_prewrite does not affect non-async/1pc prewrite
                expected: ResponsePolicy::OnApplied,

                pessimistic: false,
                async_commit: false,
                one_pc: false,
                async_apply_prewrite: true,
            },
            Case {
                // works on async prewrite
                expected: ResponsePolicy::OnCommitted,

                pessimistic: false,
                async_commit: true,
                one_pc: false,
                async_apply_prewrite: true,
            },
            Case {
                // early return can be turned on/off by async_apply_prewrite in context
                expected: ResponsePolicy::OnApplied,

                pessimistic: false,
                async_commit: true,
                one_pc: false,
                async_apply_prewrite: false,
            },
            Case {
                // works on 1pc
                expected: ResponsePolicy::OnCommitted,

                pessimistic: false,
                async_commit: false,
                one_pc: true,
                async_apply_prewrite: true,
            },
        ];
        let cases = cases
            .iter()
            .cloned()
            .chain(cases.iter().cloned().map(|mut it| {
                it.pessimistic = true;
                it
            }));

        for case in cases {
            let secondary_keys = if case.async_commit {
                Some(vec![])
            } else {
                None
            };
            let cmd = if case.pessimistic {
                PrewritePessimistic::new(
                    mutations.iter().map(|it| (it.clone(), false)).collect(),
                    keys[0].to_vec(),
                    start_ts,
                    0,
                    11.into(),
                    1,
                    TimeStamp::default(),
                    TimeStamp::default(),
                    secondary_keys,
                    case.one_pc,
                    Context::default(),
                )
            } else {
                Prewrite::new(
                    mutations.clone(),
                    keys[0].to_vec(),
                    start_ts,
                    0,
                    false,
                    1,
                    TimeStamp::default(),
                    TimeStamp::default(),
                    secondary_keys,
                    case.one_pc,
                    Context::default(),
                )
            };
            let context = WriteContext {
                lock_mgr: &DummyLockManager {},
                concurrency_manager: cm.clone(),
                extra_op: ExtraOp::Noop,
                statistics: &mut statistics,
                async_apply_prewrite: case.async_apply_prewrite,
            };
            let engine = TestEngineBuilder::new().build().unwrap();
            let snap = engine.snapshot(Default::default()).unwrap();
            let result = cmd.cmd.process_write(snap, context).unwrap();
            assert_eq!(result.response_policy, case.expected);
        }
    }
}
