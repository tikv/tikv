// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
//! Functionality for handling optimistic and pessimistic prewrites. These are
//! separate commands (although maybe they shouldn't be since there is only one
//! protobuf), but handling of the commands is similar. We therefore have a
//! single type (Prewriter) to handle both kinds of prewrite.

use std::mem;

use engine_traits::CF_WRITE;
use kvproto::kvrpcpb::{
    AssertionLevel, ExtraOp, PrewriteRequestForUpdateTsConstraint,
    PrewriteRequestPessimisticAction::{self, *},
};
use tikv_kv::SnapshotExt;
use txn_types::{
    Key, Mutation, OldValues, TimeStamp, TxnExtra, Write, WriteType, insert_old_value_if_resolved,
};

use super::ReaderWithStats;
use crate::storage::{
    Context, Error as StorageError, ProcessResult, Snapshot,
    kv::WriteData,
    lock_manager::LockManager,
    mvcc::{
        Error as MvccError, ErrorInner as MvccErrorInner, MvccTxn, SnapshotReader,
        has_data_in_range, metrics::*,
    },
    txn::{
        Error, ErrorInner, Result,
        actions::{
            common::check_committed_record_on_err,
            prewrite::{CommitKind, TransactionKind, TransactionProperties, prewrite},
        },
        commands::{
            Command, CommandExt, ReleasedLocks, ResponsePolicy, TypedCommand, WriteCommand,
            WriteContext, WriteResult,
        },
    },
    types::PrewriteResult,
};

pub(crate) const FORWARD_MIN_MUTATIONS_NUM: usize = 12;

command! {
    /// The prewrite phase of a transaction. The first phase of 2PC.
    ///
    /// This prepares the system to commit the transaction. Later a [`Commit`](Command::Commit)
    /// or a [`Rollback`](Command::Rollback) should follow.
    Prewrite:
        cmd_ty => PrewriteResult,
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
            /// Controls how strict the assertions should be.
            /// Assertions is a mechanism to check the constraint on the previous version of data
            /// that must be satisfied as long as data is consistent.
            assertion_level: AssertionLevel,
        }
        in_heap => {
            primary, mutations,
        }
}

impl std::fmt::Display for Prewrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "kv::command::prewrite mutations({:?}) primary({:?}) secondary_len({:?})@ {} {} {} {} {} {} {} {:?} | {:?}",
            self.mutations,
            log_wrappers::Value::key(self.primary.as_slice()),
            self.secondary_keys.as_ref().map(|sk| sk.len()),
            self.start_ts,
            self.lock_ttl,
            self.skip_constraint_check,
            self.txn_size,
            self.min_commit_ts,
            self.max_commit_ts,
            self.try_one_pc,
            self.assertion_level,
            self.ctx,
        )
    }
}

impl std::fmt::Debug for Prewrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
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
            AssertionLevel::Off,
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
            AssertionLevel::Off,
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
            AssertionLevel::Off,
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
            AssertionLevel::Off,
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

            assertion_level: self.assertion_level,

            ctx: self.ctx,
            old_values: OldValues::default(),
        }
    }
}

impl CommandExt for Prewrite {
    ctx!();
    tag!(prewrite);
    request_type!(KvPrewrite);
    ts!(start_ts);

    fn write_bytes(&self) -> usize {
        let mut bytes = 0;
        for m in &self.mutations {
            match *m {
                Mutation::Put((ref key, ref value), _)
                | Mutation::Insert((ref key, ref value), _) => {
                    bytes += key.as_encoded().len();
                    bytes += value.len();
                }
                Mutation::Delete(ref key, _) | Mutation::Lock(ref key, _) => {
                    bytes += key.as_encoded().len();
                }
                Mutation::CheckNotExists(..) => (),
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
        content => {
            /// The set of mutations to apply; the bool = is pessimistic lock.
            mutations: Vec<(Mutation, PrewriteRequestPessimisticAction)>,
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
            /// Controls how strict the assertions should be.
            /// Assertions is a mechanism to check the constraint on the previous version of data
            /// that must be satisfied as long as data is consistent.
            assertion_level: AssertionLevel,
            /// Constraints on the pessimistic locks that have to be checked when prewriting.
            for_update_ts_constraints: Vec<PrewriteRequestForUpdateTsConstraint>,
        }
        in_heap => {
            primary,
            secondary_keys,
            // TODO: for_update_ts_constraints, mutations
        }
}

impl std::fmt::Display for PrewritePessimistic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "kv::command::pessimistic_prewrite mutations({:?}) primary({:?}) secondary_len({:?})@ {} {} {} {} {} {} {:?} (for_update_ts constraints: {:?}) | {:?}",
            self.mutations,
            log_wrappers::Value::key(self.primary.as_slice()),
            self.secondary_keys.as_ref().map(|sk| sk.len()),
            self.start_ts,
            self.lock_ttl,
            self.txn_size,
            self.min_commit_ts,
            self.max_commit_ts,
            self.try_one_pc,
            self.assertion_level,
            self.for_update_ts_constraints,
            self.ctx,
        )
    }
}
impl std::fmt::Debug for PrewritePessimistic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl PrewritePessimistic {
    #[cfg(test)]
    pub fn with_defaults(
        mutations: Vec<(Mutation, PrewriteRequestPessimisticAction)>,
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
            AssertionLevel::Off,
            vec![],
            Context::default(),
        )
    }

    #[cfg(test)]
    pub fn with_1pc(
        mutations: Vec<(Mutation, PrewriteRequestPessimisticAction)>,
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
            AssertionLevel::Off,
            vec![],
            Context::default(),
        )
    }

    #[cfg(test)]
    pub fn with_for_update_ts_constraints(
        mutations: Vec<(Mutation, PrewriteRequestPessimisticAction)>,
        primary: Vec<u8>,
        start_ts: TimeStamp,
        for_update_ts: TimeStamp,
        for_update_ts_constraints: impl IntoIterator<Item = (usize, TimeStamp)>,
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
            AssertionLevel::Off,
            for_update_ts_constraints
                .into_iter()
                .map(|(index, expected_for_update_ts)| {
                    let mut constraint = PrewriteRequestForUpdateTsConstraint::default();
                    constraint.set_index(index as u32);
                    constraint.set_expected_for_update_ts(expected_for_update_ts.into_inner());
                    constraint
                })
                .collect(),
            Context::default(),
        )
    }

    fn into_prewriter(self) -> Result<Prewriter<Pessimistic>> {
        let mut mutations: Vec<PessimisticMutation> =
            self.mutations.into_iter().map(Into::into).collect();
        for item in self.for_update_ts_constraints {
            let index = item.index as usize;
            if index >= mutations.len() {
                return Err(ErrorInner::Other(box_err!("prewrite request invalid: for_update_ts constraint set for index {} while {} mutations were given", index, mutations.len())).into());
            }
            mutations[index].expected_for_update_ts = Some(item.expected_for_update_ts.into());
        }
        Ok(Prewriter {
            kind: Pessimistic {
                for_update_ts: self.for_update_ts,
            },
            start_ts: self.start_ts,
            txn_size: self.txn_size,
            primary: self.primary,
            mutations,

            try_one_pc: self.try_one_pc,
            secondary_keys: self.secondary_keys,
            lock_ttl: self.lock_ttl,
            min_commit_ts: self.min_commit_ts,
            max_commit_ts: self.max_commit_ts,

            assertion_level: self.assertion_level,

            ctx: self.ctx,
            old_values: OldValues::default(),
        })
    }
}

impl CommandExt for PrewritePessimistic {
    ctx!();
    tag!(prewrite);
    request_type!(KvPrewrite);
    ts!(start_ts);

    fn write_bytes(&self) -> usize {
        let mut bytes = 0;
        for (m, _) in &self.mutations {
            match m {
                Mutation::Put((ref key, ref value), _)
                | Mutation::Insert((ref key, ref value), _) => {
                    bytes += key.as_encoded().len();
                    bytes += value.len();
                }
                Mutation::Delete(ref key, _) | Mutation::Lock(ref key, _) => {
                    bytes += key.as_encoded().len();
                }
                Mutation::CheckNotExists(..) => (),
            }
        }
        bytes
    }

    gen_lock!(mutations: multiple(|(x, _)| x.key()));
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for PrewritePessimistic {
    fn process_write(self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        self.into_prewriter()?.process_write(snapshot, context)
    }
}

/// Handles both kinds of prewrite (K statically indicates either optimistic or
/// pessimistic).
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
    assertion_level: AssertionLevel,

    ctx: Context,
}

impl<K: PrewriteKind> Prewriter<K> {
    /// Entry point for handling a prewrite by Prewriter.
    fn process_write(
        mut self,
        snapshot: impl Snapshot,
        mut context: WriteContext<'_, impl LockManager>,
    ) -> Result<WriteResult> {
        // Handle special cases about retried prewrite requests for pessimistic
        // transactions.
        if let TransactionKind::Pessimistic(_) = self.kind.txn_kind() {
            if let Some(commit_ts) = context
                .txn_status_cache
                .get_committed_no_promote(self.start_ts)
            {
                fail_point!("before_prewrite_txn_status_cache_hit");
                if self.ctx.is_retry_request {
                    MVCC_PREWRITE_REQUEST_AFTER_COMMIT_COUNTER_VEC
                        .retry_req
                        .inc();
                } else {
                    MVCC_PREWRITE_REQUEST_AFTER_COMMIT_COUNTER_VEC
                        .non_retry_req
                        .inc();
                }
                warn!("prewrite request received due to transaction is known to be already committed"; "start_ts" => %self.start_ts, "commit_ts" => %commit_ts);
                // In normal cases if the transaction is committed, then the key should have
                // been already prewritten successfully. But in order to
                // simplify code as well as prevent possible corner cases or
                // special cases in the future, we disallow skipping constraint
                // check in this case.
                // We regard this request as a retried request no matter if it really is (the
                // original request may arrive later than retried request due to
                // network latency, in which case we'd better handle it like a
                // retried request).
                self.ctx.is_retry_request = true;
            } else {
                fail_point!("before_prewrite_txn_status_cache_miss");
            }
        }

        self.kind
            .can_skip_constraint_check(&mut self.mutations, &snapshot, &mut context)?;
        self.check_max_ts_synced(&snapshot)?;

        let mut txn = MvccTxn::new(self.start_ts, context.concurrency_manager);
        let mut reader = ReaderWithStats::new(
            SnapshotReader::new_with_ctx(self.start_ts, snapshot, &self.ctx),
            context.statistics,
        );
        // Set extra op here for getting the write record when check write conflict in
        // prewrite.

        let rows = self.mutations.len();
        let res = self.prewrite(&mut txn, &mut reader, context.extra_op);
        let (locks, final_min_commit_ts) = res?;

        Ok(self.write_result(
            locks,
            txn,
            final_min_commit_ts,
            rows,
            context.async_apply_prewrite,
        ))
    }

    // Async commit requires the max timestamp in the concurrency manager to be
    // up-to-date. If it is possibly stale due to leader transfer or region
    // merge, return an error. TODO: Fallback to non-async commit if not synced
    // instead of returning an error.
    fn check_max_ts_synced(&self, snapshot: &impl Snapshot) -> Result<()> {
        if (self.secondary_keys.is_some() || self.try_one_pc) && !snapshot.ext().is_max_ts_synced()
        {
            Err(ErrorInner::MaxTimestampNotSynced {
                region_id: self.ctx.get_region_id(),
                start_ts: self.start_ts,
            }
            .into())
        } else {
            Ok(())
        }
    }

    /// The core part of the prewrite action. In the abstract, this method
    /// iterates over the mutations in the prewrite and prewrites each one.
    /// It keeps track of any locks encountered and (if it's an async commit
    /// transaction) the min_commit_ts, these are returned by the method.
    fn prewrite(
        &mut self,
        txn: &mut MvccTxn,
        reader: &mut SnapshotReader<impl Snapshot>,
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
            is_retry_request: self.ctx.is_retry_request,
            assertion_level: self.assertion_level,
            txn_source: self.ctx.get_txn_source(),
        };

        let async_commit_pk = self
            .secondary_keys
            .as_ref()
            .filter(|keys| !keys.is_empty())
            .map(|_| Key::from_raw(&self.primary));
        let mut async_commit_pk = async_commit_pk.as_ref();

        let mut final_min_commit_ts = TimeStamp::zero();
        let mut locks = Vec::new();

        // If there are other errors, return other error prior to `AssertionFailed`.
        let mut assertion_failure = None;

        for m in mem::take(&mut self.mutations) {
            let pessimistic_action = m.pessimistic_action();
            let expected_for_update_ts = m.pessimistic_expected_for_update_ts();
            let m = m.into_mutation();
            let key = m.key().clone();
            let mutation_type = m.mutation_type();

            let mut secondaries = &self.secondary_keys.as_ref().map(|_| vec![]);
            if Some(m.key()) == async_commit_pk {
                secondaries = &self.secondary_keys;
            }

            let need_min_commit_ts = secondaries.is_some() || self.try_one_pc;
            let prewrite_result = prewrite(
                txn,
                reader,
                &props,
                m,
                secondaries,
                pessimistic_action,
                expected_for_update_ts,
            );
            match prewrite_result {
                Ok((ts, old_value)) if !(need_min_commit_ts && ts.is_zero()) => {
                    if need_min_commit_ts && final_min_commit_ts < ts {
                        final_min_commit_ts = ts;
                    }
                    insert_old_value_if_resolved(
                        &mut self.old_values,
                        key,
                        txn.start_ts,
                        old_value,
                        Some(mutation_type),
                    );
                }
                Ok((..)) => {
                    // If it needs min_commit_ts but min_commit_ts is zero, the lock
                    // has been prewritten and has fallen back from async commit or 1PC.
                    // We should let later keys prewrite in the old 2PC way.
                    props.commit_kind = CommitKind::TwoPc;
                    async_commit_pk = None;
                    self.secondary_keys = None;
                    self.try_one_pc = false;
                    fallback_1pc_locks(txn);
                    // release memory locks
                    txn.guards = Vec::new();
                    final_min_commit_ts = TimeStamp::zero();
                }
                Err(MvccError(box MvccErrorInner::WriteConflict {
                    start_ts,
                    conflict_commit_ts,
                    ..
                })) if conflict_commit_ts > start_ts => {
                    return check_committed_record_on_err(prewrite_result, txn, reader, &key);
                }
                Err(MvccError(box MvccErrorInner::PessimisticLockNotFound { .. })) => {
                    return check_committed_record_on_err(prewrite_result, txn, reader, &key);
                }
                Err(MvccError(box MvccErrorInner::CommitTsTooLarge { .. })) => {
                    // The prewrite might be a retry and the record may have been committed.
                    // So, we need to prevent the fallback to avoid duplicate commits.
                    if let Ok(res) =
                        check_committed_record_on_err(prewrite_result, txn, reader, &key)
                    {
                        return Ok(res);
                    }
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
                Err(MvccError(box MvccErrorInner::KeyIsLocked { .. })) => {
                    match check_committed_record_on_err(prewrite_result, txn, reader, &key) {
                        Ok(res) => return Ok(res),
                        Err(e) => locks.push(Err(e.into())),
                    }
                }
                Err(e @ MvccError(box MvccErrorInner::AssertionFailed { .. })) => {
                    if assertion_failure.is_none() {
                        assertion_failure = Some(e);
                    }
                }
                Err(e) => return Err(Error::from(e)),
            }
        }

        if let Some(e) = assertion_failure {
            return Err(Error::from(e));
        }
        Ok((locks, final_min_commit_ts))
    }

    /// Prepare a WriteResult object from the results of executing the prewrite.
    fn write_result(
        self,
        locks: Vec<std::result::Result<(), StorageError>>,
        mut txn: MvccTxn,
        final_min_commit_ts: TimeStamp,
        rows: usize,
        async_apply_prewrite: bool,
    ) -> WriteResult {
        let async_commit_ts = if self.secondary_keys.is_some() {
            final_min_commit_ts
        } else {
            TimeStamp::zero()
        };

        let mut result = if locks.is_empty() {
            let (one_pc_commit_ts, released_locks) =
                one_pc_commit(self.try_one_pc, &mut txn, final_min_commit_ts);

            let pr = ProcessResult::PrewriteResult {
                result: PrewriteResult {
                    locks: vec![],
                    min_commit_ts: async_commit_ts,
                    one_pc_commit_ts,
                },
            };
            let extra = TxnExtra {
                old_values: self.old_values,
                // Set one_pc flag in TxnExtra to let CDC skip handling the resolver.
                one_pc: self.try_one_pc,
                allowed_in_flashback: false,
            };
            // Here the lock guards are taken and will be released after the write finishes.
            // If an error (KeyIsLocked or WriteConflict) occurs before, these lock guards
            // are dropped along with `txn` automatically.
            let lock_guards = txn.take_guards();
            let new_acquired_locks = txn.take_new_locks();
            let mut to_be_write = WriteData::new(txn.into_modifies(), extra);
            to_be_write.set_disk_full_opt(self.ctx.get_disk_full_opt());

            WriteResult {
                ctx: self.ctx,
                to_be_write,
                rows,
                pr,
                lock_info: vec![],
                released_locks,
                new_acquired_locks,
                lock_guards,
                response_policy: ResponsePolicy::OnApplied,
                known_txn_status: if !one_pc_commit_ts.is_zero() {
                    vec![(self.start_ts, one_pc_commit_ts)]
                } else {
                    vec![]
                },
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
                lock_info: vec![],
                released_locks: ReleasedLocks::new(),
                new_acquired_locks: vec![],
                lock_guards: vec![],
                response_policy: ResponsePolicy::OnApplied,
                known_txn_status: vec![],
            }
        };

        // Currently if `try_one_pc` is set, it must have succeeded here.
        if (!async_commit_ts.is_zero() || self.try_one_pc) && async_apply_prewrite {
            result.response_policy = ResponsePolicy::OnCommitted
        }

        result
    }
}

/// Encapsulates things which must be done differently for optimistic or
/// pessimistic transactions.
trait PrewriteKind {
    /// The type of mutation and, optionally, its extra information, differing
    /// for the optimistic and pessimistic transaction.
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
    type Mutation = PessimisticMutation;

    fn txn_kind(&self) -> TransactionKind {
        TransactionKind::Pessimistic(self.for_update_ts)
    }
}

/// The type of mutation and, optionally, its extra information, differing for
/// the optimistic and pessimistic transaction.
/// For optimistic txns, this is `Mutation`.
/// For pessimistic txns, this is `PessimisticMutation` which contains a
/// `Mutation` and some other extra information necessary for pessimistic txns.
trait MutationLock {
    fn pessimistic_action(&self) -> PrewriteRequestPessimisticAction;
    fn pessimistic_expected_for_update_ts(&self) -> Option<TimeStamp>;
    fn into_mutation(self) -> Mutation;
}

impl MutationLock for Mutation {
    fn pessimistic_action(&self) -> PrewriteRequestPessimisticAction {
        SkipPessimisticCheck
    }

    fn pessimistic_expected_for_update_ts(&self) -> Option<TimeStamp> {
        None
    }

    fn into_mutation(self) -> Mutation {
        self
    }
}

#[derive(Debug)]
pub struct PessimisticMutation {
    pub mutation: Mutation,
    /// Indicates what kind of operations(checks) need to be performed, and also
    /// implies the type of the lock status.
    pub pessimistic_action: PrewriteRequestPessimisticAction,
    /// Specifies whether it needs to check the `for_update_ts` field in the
    /// pessimistic lock during prewrite. If any, the check only passes if the
    /// `for_update_ts` field in pessimistic lock is not greater than the
    /// expected value.
    pub expected_for_update_ts: Option<TimeStamp>,
}

impl MutationLock for PessimisticMutation {
    fn pessimistic_action(&self) -> PrewriteRequestPessimisticAction {
        self.pessimistic_action
    }

    fn pessimistic_expected_for_update_ts(&self) -> Option<TimeStamp> {
        self.expected_for_update_ts
    }

    fn into_mutation(self) -> Mutation {
        self.mutation
    }
}

impl PessimisticMutation {
    pub fn new(mutation: Mutation, pessimistic_action: PrewriteRequestPessimisticAction) -> Self {
        Self {
            mutation,
            pessimistic_action,
            expected_for_update_ts: None,
        }
    }
}

impl From<(Mutation, PrewriteRequestPessimisticAction)> for PessimisticMutation {
    fn from(value: (Mutation, PrewriteRequestPessimisticAction)) -> Self {
        PessimisticMutation::new(value.0, value.1)
    }
}

/// Commits a 1pc transaction if possible, returns the commit ts and released
/// locks on success.
pub fn one_pc_commit(
    try_one_pc: bool,
    txn: &mut MvccTxn,
    final_min_commit_ts: TimeStamp,
) -> (TimeStamp, ReleasedLocks) {
    if try_one_pc {
        assert_ne!(final_min_commit_ts, TimeStamp::zero());
        // All keys can be successfully locked and `try_one_pc` is set. Try to directly
        // commit them.
        let released_locks = handle_1pc_locks(txn, final_min_commit_ts);
        (final_min_commit_ts, released_locks)
    } else {
        assert!(txn.locks_for_1pc.is_empty());
        (TimeStamp::zero(), ReleasedLocks::new())
    }
}

/// Commit and delete all 1pc locks in txn.
fn handle_1pc_locks(txn: &mut MvccTxn, commit_ts: TimeStamp) -> ReleasedLocks {
    let mut released_locks = ReleasedLocks::new();

    for (key, lock, delete_pessimistic_lock) in std::mem::take(&mut txn.locks_for_1pc) {
        let write = Write::new(
            WriteType::from_lock_type(lock.lock_type).unwrap(),
            txn.start_ts,
            lock.short_value,
        )
        .set_last_change(lock.last_change)
        .set_txn_source(lock.txn_source);
        // Transactions committed with 1PC should be impossible to overwrite rollback
        // records.
        txn.put_write(key.clone(), commit_ts, write.as_ref().to_bytes());
        if delete_pessimistic_lock {
            released_locks.push(txn.unlock_key(key, true, commit_ts));
        }
    }

    released_locks
}

/// Change all 1pc locks in txn to 2pc locks.
pub(in crate::storage::txn) fn fallback_1pc_locks(txn: &mut MvccTxn) {
    for (key, mut lock, remove_pessimistic_lock) in std::mem::take(&mut txn.locks_for_1pc) {
        lock.use_one_pc = false;
        let is_new_lock = !remove_pessimistic_lock;
        txn.put_lock(key, &lock, is_new_lock);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use concurrency_manager::ConcurrencyManager;
    use engine_rocks::ReadPerfInstant;
    use engine_traits::CF_WRITE;
    use kvproto::kvrpcpb::{Assertion, Context, ExtraOp};
    use txn_types::{Key, LastChange, Mutation, TimeStamp};

    use super::*;
    use crate::storage::{
        Engine, MockLockManager, Snapshot, Statistics, TestEngineBuilder,
        mvcc::{Error as MvccError, ErrorInner as MvccErrorInner, tests::*},
        txn::{
            Error, ErrorInner,
            actions::{
                acquire_pessimistic_lock::tests::must_pessimistic_locked,
                tests::{
                    must_pessimistic_prewrite_put_async_commit, must_prewrite_delete,
                    must_prewrite_put, must_prewrite_put_async_commit,
                },
            },
            commands::{
                check_txn_status::tests::must_success as must_check_txn_status,
                test_util::{
                    commit, pessimistic_prewrite_check_for_update_ts, pessimistic_prewrite_with_cm,
                    prewrite, prewrite_command, prewrite_with_cm, rollback,
                },
            },
            tests::{
                must_acquire_pessimistic_lock, must_acquire_pessimistic_lock_err, must_commit,
                must_prewrite_put_err_impl, must_prewrite_put_impl, must_rollback,
            },
            txn_status_cache::TxnStatusCache,
        },
        types::TxnStatus,
    };

    fn inner_test_prewrite_skip_constraint_check(pri_key_number: u8, write_num: usize) {
        let mut mutations = Vec::default();
        let pri_key = &[pri_key_number];
        for i in 0..write_num {
            mutations.push(Mutation::make_insert(
                Key::from_raw(&[i as u8]),
                b"100".to_vec(),
            ));
        }
        let mut statistic = Statistics::default();
        let mut engine = TestEngineBuilder::new().build().unwrap();
        prewrite(
            &mut engine,
            &mut statistic,
            vec![Mutation::make_put(
                Key::from_raw(&[pri_key_number]),
                b"100".to_vec(),
            )],
            pri_key.to_vec(),
            99,
            None,
        )
        .unwrap();
        assert_eq!(1, statistic.write.seek);
        let e = prewrite(
            &mut engine,
            &mut statistic,
            mutations.clone(),
            pri_key.to_vec(),
            100,
            None,
        )
        .err()
        .unwrap();
        assert_eq!(3, statistic.write.seek);
        match e {
            Error(box ErrorInner::Mvcc(MvccError(box MvccErrorInner::KeyIsLocked(_)))) => (),
            _ => panic!("error type not match"),
        }
        commit(
            &mut engine,
            &mut statistic,
            vec![Key::from_raw(&[pri_key_number])],
            99,
            102,
        )
        .unwrap();
        assert_eq!(3, statistic.write.seek);
        let e = prewrite(
            &mut engine,
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
            &mut engine,
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
            &mut engine,
            &mut statistic,
            mutations.clone(),
            pri_key.to_vec(),
            104,
            None,
        )
        .unwrap();
        // All keys are prewritten successful with only one seek operations.
        assert_eq!(1, statistic.write.seek);
        let keys: Vec<Key> = mutations.iter().map(|m| m.key().clone()).collect();
        commit(&mut engine, &mut statistic, keys.clone(), 104, 105).unwrap();
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
        use engine_rocks::{PerfLevel, set_perf_level};

        use crate::server::gc_worker::gc_by_compact;
        let mut mutations = Vec::default();
        let pri_key_number = 0;
        let pri_key = &[pri_key_number];
        for i in 0..40 {
            mutations.push(Mutation::make_insert(
                Key::from_raw(&[b'z', i as u8]),
                b"100".to_vec(),
            ));
        }
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let keys: Vec<Key> = mutations.iter().map(|m| m.key().clone()).collect();
        let mut statistic = Statistics::default();
        prewrite(
            &mut engine,
            &mut statistic,
            mutations.clone(),
            pri_key.to_vec(),
            100,
            None,
        )
        .unwrap();
        // Rollback to make tombstones in lock-cf.
        rollback(&mut engine, &mut statistic, keys, 100).unwrap();
        // Gc rollback flags store in write-cf to make sure the next prewrite operation
        // will skip seek write cf.
        gc_by_compact(&mut engine, pri_key, 101);
        set_perf_level(PerfLevel::EnableTimeExceptForMutex);
        let perf = ReadPerfInstant::new();
        let mut statistic = Statistics::default();
        while mutations.len() > FORWARD_MIN_MUTATIONS_NUM + 1 {
            mutations.pop();
        }
        prewrite(
            &mut engine,
            &mut statistic,
            mutations,
            pri_key.to_vec(),
            110,
            None,
        )
        .unwrap();
        let d = perf.delta();
        assert_eq!(1, statistic.write.seek);
        assert_eq!(d.internal_delete_skipped_count, 0);
    }

    #[test]
    fn test_prewrite_1pc_with_txn_source() {
        use crate::storage::mvcc::tests::{must_get, must_get_commit_ts, must_unlocked};

        let mut engine = TestEngineBuilder::new().build().unwrap();
        let cm = concurrency_manager::ConcurrencyManager::new_for_test(1.into());

        let key = b"k";
        let value = b"v";
        let mutations = vec![Mutation::make_put(Key::from_raw(key), value.to_vec())];

        let mut statistics = Statistics::default();
        let mut ctx = Context::default();
        ctx.set_txn_source(1);
        let cmd = Prewrite::new(
            mutations,
            key.to_vec(),
            TimeStamp::from(10),
            0,
            false,
            0,
            TimeStamp::default(),
            TimeStamp::from(15),
            None,
            true,
            AssertionLevel::Off,
            ctx,
        );
        prewrite_command(&mut engine, cm, &mut statistics, cmd).unwrap();

        must_unlocked(&mut engine, key);
        must_get(&mut engine, key, 12, value);
        must_get_commit_ts(&mut engine, key, 10, 11);
        must_get_txn_source(&mut engine, key, 11, 1);
    }

    #[test]
    fn test_prewrite_1pc() {
        use crate::storage::mvcc::tests::{must_get, must_get_commit_ts, must_unlocked};

        let mut engine = TestEngineBuilder::new().build().unwrap();
        let cm = concurrency_manager::ConcurrencyManager::new_for_test(1.into());

        let key = b"k";
        let value = b"v";
        let mutations = vec![Mutation::make_put(Key::from_raw(key), value.to_vec())];

        let mut statistics = Statistics::default();
        prewrite_with_cm(
            &mut engine,
            cm.clone(),
            &mut statistics,
            mutations,
            key.to_vec(),
            10,
            Some(15),
        )
        .unwrap();
        must_unlocked(&mut engine, key);
        must_get(&mut engine, key, 12, value);
        must_get_commit_ts(&mut engine, key, 10, 11);

        cm.update_max_ts(50.into(), "").unwrap();

        let mutations = vec![Mutation::make_put(Key::from_raw(key), value.to_vec())];

        let mut statistics = Statistics::default();
        // Test the idempotency of prewrite when falling back to 2PC.
        for _ in 0..2 {
            let res = prewrite_with_cm(
                &mut engine,
                cm.clone(),
                &mut statistics,
                mutations.clone(),
                key.to_vec(),
                20,
                Some(30),
            )
            .unwrap();
            assert!(res.min_commit_ts.is_zero());
            assert!(res.one_pc_commit_ts.is_zero());
            must_locked(&mut engine, key, 20);
        }

        must_rollback(&mut engine, key, 20, false);
        let mutations = vec![
            Mutation::make_put(Key::from_raw(key), value.to_vec()),
            Mutation::make_check_not_exists(Key::from_raw(b"non_exist")),
        ];
        let mut statistics = Statistics::default();
        prewrite_with_cm(
            &mut engine,
            cm.clone(),
            &mut statistics,
            mutations,
            key.to_vec(),
            40,
            Some(60),
        )
        .unwrap();

        // Test a 1PC request should not be partially written when encounters error on
        // the halfway. If some of the keys are successfully written as committed state,
        // the atomicity will be broken.
        let (k1, v1) = (b"k1", b"v1");
        let (k2, v2) = (b"k2", b"v2");
        // Lock k2.
        let mut statistics = Statistics::default();
        prewrite_with_cm(
            &mut engine,
            cm.clone(),
            &mut statistics,
            vec![Mutation::make_put(Key::from_raw(k2), v2.to_vec())],
            k2.to_vec(),
            50,
            None,
        )
        .unwrap();
        // Try 1PC on the two keys and it will fail on the second one.
        let mutations = vec![
            Mutation::make_put(Key::from_raw(k1), v1.to_vec()),
            Mutation::make_put(Key::from_raw(k2), v2.to_vec()),
        ];
        prewrite_with_cm(
            &mut engine,
            cm,
            &mut statistics,
            mutations,
            k1.to_vec(),
            60,
            Some(70),
        )
        .unwrap_err();
        must_unlocked(&mut engine, k1);
        must_locked(&mut engine, k2, 50);
        must_get_commit_ts_none(&mut engine, k1, 60);
        must_get_commit_ts_none(&mut engine, k2, 60);
    }

    #[test]
    fn test_prewrite_pessimsitic_1pc() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let cm = concurrency_manager::ConcurrencyManager::new_for_test(1.into());
        let key = b"k";
        let value = b"v";

        must_acquire_pessimistic_lock(&mut engine, key, key, 10, 10);

        let mutations = vec![(
            Mutation::make_put(Key::from_raw(key), value.to_vec()),
            DoPessimisticCheck,
        )];
        let mut statistics = Statistics::default();
        pessimistic_prewrite_with_cm(
            &mut engine,
            cm.clone(),
            &mut statistics,
            mutations,
            key.to_vec(),
            10,
            10,
            Some(15),
        )
        .unwrap();

        must_unlocked(&mut engine, key);
        must_get(&mut engine, key, 12, value);
        must_get_commit_ts(&mut engine, key, 10, 11);

        let (k1, v1) = (b"k", b"v");
        let (k2, v2) = (b"k2", b"v2");

        must_acquire_pessimistic_lock(&mut engine, k1, k1, 8, 12);

        let mutations = vec![
            (
                Mutation::make_put(Key::from_raw(k1), v1.to_vec()),
                DoPessimisticCheck,
            ),
            (
                Mutation::make_put(Key::from_raw(k2), v2.to_vec()),
                SkipPessimisticCheck,
            ),
        ];
        statistics = Statistics::default();
        pessimistic_prewrite_with_cm(
            &mut engine,
            cm.clone(),
            &mut statistics,
            mutations,
            k1.to_vec(),
            8,
            12,
            Some(15),
        )
        .unwrap();

        must_unlocked(&mut engine, k1);
        must_unlocked(&mut engine, k2);
        must_get(&mut engine, k1, 16, v1);
        must_get(&mut engine, k2, 16, v2);
        must_get_commit_ts(&mut engine, k1, 8, 13);
        must_get_commit_ts(&mut engine, k2, 8, 13);

        cm.update_max_ts(50.into(), "").unwrap();
        must_acquire_pessimistic_lock(&mut engine, k1, k1, 20, 20);

        let mutations = vec![(
            Mutation::make_put(Key::from_raw(k1), v1.to_vec()),
            DoPessimisticCheck,
        )];
        statistics = Statistics::default();
        let res = pessimistic_prewrite_with_cm(
            &mut engine,
            cm.clone(),
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
        must_locked(&mut engine, k1, 20);

        must_rollback(&mut engine, k1, 20, true);

        // Test a 1PC request should not be partially written when encounters error on
        // the halfway. If some of the keys are successfully written as committed state,
        // the atomicity will be broken.

        // Lock k2 with a optimistic lock.
        let mut statistics = Statistics::default();
        prewrite_with_cm(
            &mut engine,
            cm.clone(),
            &mut statistics,
            vec![Mutation::make_put(Key::from_raw(k2), v2.to_vec())],
            k2.to_vec(),
            50,
            None,
        )
        .unwrap();
        // Try 1PC on the two keys and it will fail on the second one.
        let mutations = vec![
            (
                Mutation::make_put(Key::from_raw(k1), v1.to_vec()),
                DoPessimisticCheck,
            ),
            (
                Mutation::make_put(Key::from_raw(k2), v2.to_vec()),
                SkipPessimisticCheck,
            ),
        ];
        must_acquire_pessimistic_lock(&mut engine, k1, k1, 60, 60);
        pessimistic_prewrite_with_cm(
            &mut engine,
            cm,
            &mut statistics,
            mutations,
            k1.to_vec(),
            60,
            60,
            Some(70),
        )
        .unwrap_err();
        must_pessimistic_locked(&mut engine, k1, 60, 60);
        must_locked(&mut engine, k2, 50);
        must_get_commit_ts_none(&mut engine, k1, 60);
        must_get_commit_ts_none(&mut engine, k2, 60);
    }

    #[test]
    fn test_prewrite_async_commit() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let cm = concurrency_manager::ConcurrencyManager::new_for_test(1.into());

        let key = b"k";
        let value = b"v";
        let mutations = vec![Mutation::make_put(Key::from_raw(key), value.to_vec())];

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
            AssertionLevel::Off,
            Context::default(),
        );

        let res = prewrite_command(&mut engine, cm.clone(), &mut statistics, cmd).unwrap();
        assert!(!res.min_commit_ts.is_zero());
        assert_eq!(res.one_pc_commit_ts, TimeStamp::zero());
        must_locked(&mut engine, key, 10);

        cm.update_max_ts(50.into(), "").unwrap();

        let (k1, v1) = (b"k1", b"v1");
        let (k2, v2) = (b"k2", b"v2");

        let mutations = vec![
            Mutation::make_put(Key::from_raw(k1), v1.to_vec()),
            Mutation::make_put(Key::from_raw(k2), v2.to_vec()),
        ];
        let mut statistics = Statistics::default();
        // calculated_ts > max_commit_ts
        // Test the idempotency of prewrite when falling back to 2PC.
        for _ in 0..2 {
            let cmd = super::Prewrite::new(
                mutations.clone(),
                k1.to_vec(),
                20.into(),
                0,
                false,
                2,
                21.into(),
                40.into(),
                Some(vec![k2.to_vec()]),
                false,
                AssertionLevel::Off,
                Context::default(),
            );

            let res = prewrite_command(&mut engine, cm.clone(), &mut statistics, cmd).unwrap();
            assert!(res.min_commit_ts.is_zero());
            assert!(res.one_pc_commit_ts.is_zero());
            assert!(!must_locked(&mut engine, k1, 20).use_async_commit);
            assert!(!must_locked(&mut engine, k2, 20).use_async_commit);
        }
    }

    #[test]
    fn test_prewrite_pessimsitic_async_commit() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let cm = concurrency_manager::ConcurrencyManager::new_for_test(1.into());

        let key = b"k";
        let value = b"v";

        must_acquire_pessimistic_lock(&mut engine, key, key, 10, 10);

        let mutations = vec![(
            Mutation::make_put(Key::from_raw(key), value.to_vec()),
            DoPessimisticCheck,
        )];
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
            AssertionLevel::Off,
            vec![],
            Context::default(),
        );

        let res = prewrite_command(&mut engine, cm.clone(), &mut statistics, cmd).unwrap();
        assert!(!res.min_commit_ts.is_zero());
        assert_eq!(res.one_pc_commit_ts, TimeStamp::zero());
        must_locked(&mut engine, key, 10);

        cm.update_max_ts(50.into(), "").unwrap();

        let (k1, v1) = (b"k1", b"v1");
        let (k2, v2) = (b"k2", b"v2");

        must_acquire_pessimistic_lock(&mut engine, k1, k1, 20, 20);
        must_acquire_pessimistic_lock(&mut engine, k2, k1, 20, 20);

        let mutations = vec![
            (
                Mutation::make_put(Key::from_raw(k1), v1.to_vec()),
                DoPessimisticCheck,
            ),
            (
                Mutation::make_put(Key::from_raw(k2), v2.to_vec()),
                DoPessimisticCheck,
            ),
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
            AssertionLevel::Off,
            vec![],
            Context::default(),
        );

        let res = prewrite_command(&mut engine, cm, &mut statistics, cmd).unwrap();
        assert!(res.min_commit_ts.is_zero());
        assert!(res.one_pc_commit_ts.is_zero());
        assert!(!must_locked(&mut engine, k1, 20).use_async_commit);
        assert!(!must_locked(&mut engine, k2, 20).use_async_commit);
    }

    #[test]
    // FIXME: Either implement storage::kv traits for all engine types, or avoid using raw engines
    // in this test.
    #[cfg(feature = "test-engine-kv-rocksdb")]
    fn test_out_of_sync_max_ts() {
        use engine_test::kv::KvTestEngineIterator;
        use engine_traits::{IterOptions, ReadOptions};
        use kvproto::kvrpcpb::ExtraOp;

        use crate::storage::{CfName, ConcurrencyManager, MockLockManager, Value, kv::Result};
        #[derive(Clone)]
        struct MockSnapshot;

        struct MockSnapshotExt;

        impl SnapshotExt for MockSnapshotExt {
            fn is_max_ts_synced(&self) -> bool {
                false
            }
        }

        impl Snapshot for MockSnapshot {
            type Iter = KvTestEngineIterator;
            type Ext<'a> = MockSnapshotExt;

            fn get(&self, _: &Key) -> Result<Option<Value>> {
                unimplemented!()
            }
            fn get_cf(&self, _: CfName, _: &Key) -> Result<Option<Value>> {
                unimplemented!()
            }
            fn get_cf_opt(&self, _: ReadOptions, _: CfName, _: &Key) -> Result<Option<Value>> {
                unimplemented!()
            }
            fn iter(&self, _: CfName, _: IterOptions) -> Result<Self::Iter> {
                unimplemented!()
            }
            fn ext(&self) -> MockSnapshotExt {
                MockSnapshotExt
            }
        }

        macro_rules! context {
            () => {
                WriteContext {
                    lock_mgr: &MockLockManager::new(),
                    concurrency_manager: ConcurrencyManager::new_for_test(10.into()),
                    extra_op: ExtraOp::Noop,
                    statistics: &mut Statistics::default(),
                    async_apply_prewrite: false,
                    raw_ext: None,
                    txn_status_cache: Arc::new(TxnStatusCache::new_for_test()),
                }
            };
        }

        macro_rules! assert_max_ts_err {
            ($e:expr) => {
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
        let cm = ConcurrencyManager::new_for_test(42.into());
        let start_ts = TimeStamp::new(10);
        let keys = [b"k1", b"k2"];
        let values = [b"v1", b"v2"];
        let mutations = vec![
            Mutation::make_put(Key::from_raw(keys[0]), keys[0].to_vec()),
            Mutation::make_put(Key::from_raw(keys[1]), values[1].to_vec()),
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

        let cases = [
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
                    mutations
                        .iter()
                        .map(|it| (it.clone(), SkipPessimisticCheck))
                        .collect(),
                    keys[0].to_vec(),
                    start_ts,
                    0,
                    11.into(),
                    1,
                    TimeStamp::default(),
                    TimeStamp::default(),
                    secondary_keys,
                    case.one_pc,
                    AssertionLevel::Off,
                    vec![],
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
                    AssertionLevel::Off,
                    Context::default(),
                )
            };
            let context = WriteContext {
                lock_mgr: &MockLockManager::new(),
                concurrency_manager: cm.clone(),
                extra_op: ExtraOp::Noop,
                statistics: &mut statistics,
                async_apply_prewrite: case.async_apply_prewrite,
                raw_ext: None,
                txn_status_cache: Arc::new(TxnStatusCache::new_for_test()),
            };
            let mut engine = TestEngineBuilder::new().build().unwrap();
            let snap = engine.snapshot(Default::default()).unwrap();
            let result = cmd.cmd.process_write(snap, context).unwrap();
            assert_eq!(result.response_policy, case.expected);
        }
    }

    // this test for prewrite with should_not_exist flag
    #[test]
    fn test_prewrite_should_not_exist() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        // concurency_manager.max_tx = 5
        let cm = ConcurrencyManager::new_for_test(5.into());
        let mut statistics = Statistics::default();

        let (key, value) = (b"k", b"val");

        // T1: start_ts = 3, commit_ts = 5, put key:value
        must_prewrite_put(&mut engine, key, value, key, 3);
        must_commit(&mut engine, key, 3, 5);

        // T2: start_ts = 15, prewrite on k, with should_not_exist flag set.
        let res = prewrite_with_cm(
            &mut engine,
            cm.clone(),
            &mut statistics,
            vec![Mutation::make_check_not_exists(Key::from_raw(key))],
            key.to_vec(),
            15,
            None,
        )
        .unwrap_err();
        assert!(matches!(
            res,
            Error(box ErrorInner::Mvcc(MvccError(box MvccErrorInner::AlreadyExist { .. })))
        ));

        assert_eq!(cm.max_ts().into_inner(), 15);

        // T3: start_ts = 8, commit_ts = max_ts + 1 = 16, prewrite a DELETE operation on
        // k
        must_prewrite_delete(&mut engine, key, key, 8);
        must_commit(&mut engine, key, 8, cm.max_ts().into_inner() + 1);

        // T1: start_ts = 10, repeatedly prewrite on k, with should_not_exist flag set
        let res = prewrite_with_cm(
            &mut engine,
            cm,
            &mut statistics,
            vec![Mutation::make_check_not_exists(Key::from_raw(key))],
            key.to_vec(),
            10,
            None,
        )
        .unwrap_err();
        assert!(matches!(
            res,
            Error(box ErrorInner::Mvcc(MvccError(box MvccErrorInner::WriteConflict { .. })))
        ));
    }

    #[test]
    fn test_optimistic_prewrite_committed_transaction() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new_for_test(1.into());
        let mut statistics = Statistics::default();

        let key = b"k";

        // T1: start_ts = 5, commit_ts = 10, async commit
        must_prewrite_put_async_commit(&mut engine, key, b"v1", key, &Some(vec![]), 5, 10);
        must_commit(&mut engine, key, 5, 10);

        // T2: start_ts = 15, commit_ts = 16, 1PC
        let cmd = Prewrite::with_1pc(
            vec![Mutation::make_put(Key::from_raw(key), b"v2".to_vec())],
            key.to_vec(),
            15.into(),
            TimeStamp::default(),
        );
        let result = prewrite_command(&mut engine, cm.clone(), &mut statistics, cmd).unwrap();
        let one_pc_commit_ts = result.one_pc_commit_ts;

        // T3 is after T1 and T2
        must_prewrite_put(&mut engine, key, b"v3", key, 20);
        must_commit(&mut engine, key, 20, 25);

        // Repeating the T1 prewrite request
        let cmd = Prewrite::new(
            vec![Mutation::make_put(Key::from_raw(key), b"v1".to_vec())],
            key.to_vec(),
            5.into(),
            200,
            false,
            1,
            10.into(),
            TimeStamp::default(),
            Some(vec![]),
            false,
            AssertionLevel::Off,
            Context::default(),
        );
        let context = WriteContext {
            lock_mgr: &MockLockManager::new(),
            concurrency_manager: cm.clone(),
            extra_op: ExtraOp::Noop,
            statistics: &mut statistics,
            async_apply_prewrite: false,
            raw_ext: None,
            txn_status_cache: Arc::new(TxnStatusCache::new_for_test()),
        };
        let snap = engine.snapshot(Default::default()).unwrap();
        let result = cmd.cmd.process_write(snap, context).unwrap();
        assert!(result.to_be_write.modifies.is_empty()); // should not make real modifies
        assert!(result.lock_guards.is_empty());
        match result.pr {
            ProcessResult::PrewriteResult { result } => {
                assert!(result.locks.is_empty());
                assert_eq!(result.min_commit_ts, 10.into()); // equals to the real commit ts
                assert_eq!(result.one_pc_commit_ts, 0.into()); // not using 1PC
            }
            res => panic!("unexpected result {:?}", res),
        }

        // Repeating the T2 prewrite request
        let cmd = Prewrite::with_1pc(
            vec![Mutation::make_put(Key::from_raw(key), b"v2".to_vec())],
            key.to_vec(),
            15.into(),
            TimeStamp::default(),
        );
        let context = WriteContext {
            lock_mgr: &MockLockManager::new(),
            concurrency_manager: cm,
            extra_op: ExtraOp::Noop,
            statistics: &mut statistics,
            async_apply_prewrite: false,
            raw_ext: None,
            txn_status_cache: Arc::new(TxnStatusCache::new_for_test()),
        };
        let snap = engine.snapshot(Default::default()).unwrap();
        let result = cmd.cmd.process_write(snap, context).unwrap();
        assert!(result.to_be_write.modifies.is_empty()); // should not make real modifies
        assert!(result.lock_guards.is_empty());
        match result.pr {
            ProcessResult::PrewriteResult { result } => {
                assert!(result.locks.is_empty());
                assert_eq!(result.min_commit_ts, 0.into()); // 1PC does not need this
                assert_eq!(result.one_pc_commit_ts, one_pc_commit_ts); // equals to the previous 1PC commit_ts
            }
            res => panic!("unexpected result {:?}", res),
        }
    }

    #[test]
    fn test_pessimistic_prewrite_committed_transaction() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new_for_test(1.into());
        let mut statistics = Statistics::default();

        let key = b"k";

        // T1: start_ts = 5, commit_ts = 10, async commit
        must_acquire_pessimistic_lock(&mut engine, key, key, 5, 5);
        must_pessimistic_prewrite_put_async_commit(
            &mut engine,
            key,
            b"v1",
            key,
            &Some(vec![]),
            5,
            5,
            DoPessimisticCheck,
            10,
        );
        must_commit(&mut engine, key, 5, 10);

        // T2: start_ts = 15, commit_ts = 16, 1PC
        must_acquire_pessimistic_lock(&mut engine, key, key, 15, 15);
        let cmd = PrewritePessimistic::with_1pc(
            vec![(
                Mutation::make_put(Key::from_raw(key), b"v2".to_vec()),
                DoPessimisticCheck,
            )],
            key.to_vec(),
            15.into(),
            15.into(),
            TimeStamp::default(),
        );
        let result = prewrite_command(&mut engine, cm.clone(), &mut statistics, cmd).unwrap();
        let one_pc_commit_ts = result.one_pc_commit_ts;

        // T3 is after T1 and T2
        must_prewrite_put(&mut engine, key, b"v3", key, 20);
        must_commit(&mut engine, key, 20, 25);

        // Repeating the T1 prewrite request
        let cmd = PrewritePessimistic::new(
            vec![(
                Mutation::make_put(Key::from_raw(key), b"v1".to_vec()),
                DoPessimisticCheck,
            )],
            key.to_vec(),
            5.into(),
            200,
            5.into(),
            1,
            10.into(),
            TimeStamp::default(),
            Some(vec![]),
            false,
            AssertionLevel::Off,
            vec![],
            Context::default(),
        );
        let context = WriteContext {
            lock_mgr: &MockLockManager::new(),
            concurrency_manager: cm.clone(),
            extra_op: ExtraOp::Noop,
            statistics: &mut statistics,
            async_apply_prewrite: false,
            raw_ext: None,
            txn_status_cache: Arc::new(TxnStatusCache::new_for_test()),
        };
        let snap = engine.snapshot(Default::default()).unwrap();
        let result = cmd.cmd.process_write(snap, context).unwrap();
        assert!(result.to_be_write.modifies.is_empty()); // should not make real modifies
        assert!(result.lock_guards.is_empty());
        match result.pr {
            ProcessResult::PrewriteResult { result } => {
                assert!(result.locks.is_empty());
                assert_eq!(result.min_commit_ts, 10.into()); // equals to the real commit ts
                assert_eq!(result.one_pc_commit_ts, 0.into()); // not using 1PC
            }
            res => panic!("unexpected result {:?}", res),
        }

        // Repeating the T2 prewrite request
        let cmd = PrewritePessimistic::with_1pc(
            vec![(
                Mutation::make_put(Key::from_raw(key), b"v2".to_vec()),
                DoPessimisticCheck,
            )],
            key.to_vec(),
            15.into(),
            15.into(),
            TimeStamp::default(),
        );
        let context = WriteContext {
            lock_mgr: &MockLockManager::new(),
            concurrency_manager: cm,
            extra_op: ExtraOp::Noop,
            statistics: &mut statistics,
            async_apply_prewrite: false,
            raw_ext: None,
            txn_status_cache: Arc::new(TxnStatusCache::new_for_test()),
        };
        let snap = engine.snapshot(Default::default()).unwrap();
        let result = cmd.cmd.process_write(snap, context).unwrap();
        assert!(result.to_be_write.modifies.is_empty()); // should not make real modifies
        assert!(result.lock_guards.is_empty());
        match result.pr {
            ProcessResult::PrewriteResult { result } => {
                assert!(result.locks.is_empty());
                assert_eq!(result.min_commit_ts, 0.into()); // 1PC does not need this
                assert_eq!(result.one_pc_commit_ts, one_pc_commit_ts); // equals to the previous 1PC commit_ts
            }
            res => panic!("unexpected result {:?}", res),
        }
    }

    #[test]
    fn test_repeated_pessimistic_prewrite_1pc() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new_for_test(1.into());
        let mut statistics = Statistics::default();

        must_acquire_pessimistic_lock(&mut engine, b"k2", b"k2", 5, 5);
        // The second key needs a pessimistic lock
        let mutations = vec![
            (
                Mutation::make_put(Key::from_raw(b"k1"), b"v1".to_vec()),
                SkipPessimisticCheck,
            ),
            (
                Mutation::make_put(Key::from_raw(b"k2"), b"v2".to_vec()),
                DoPessimisticCheck,
            ),
        ];
        let res = pessimistic_prewrite_with_cm(
            &mut engine,
            cm.clone(),
            &mut statistics,
            mutations.clone(),
            b"k2".to_vec(),
            5,
            5,
            Some(100),
        )
        .unwrap();
        let commit_ts = res.one_pc_commit_ts;
        cm.update_max_ts(commit_ts.next(), "").unwrap();
        // repeate the prewrite
        let res = pessimistic_prewrite_with_cm(
            &mut engine,
            cm,
            &mut statistics,
            mutations,
            b"k2".to_vec(),
            5,
            5,
            Some(100),
        )
        .unwrap();
        // The new commit ts should be same as before.
        assert_eq!(res.one_pc_commit_ts, commit_ts);
        must_seek_write(&mut engine, b"k1", 100, 5, commit_ts, WriteType::Put);
        must_seek_write(&mut engine, b"k2", 100, 5, commit_ts, WriteType::Put);
    }

    #[test]
    fn test_repeated_prewrite_non_pessimistic_lock() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new_for_test(1.into());
        let mut statistics = Statistics::default();

        let cm = &cm;
        fn prewrite_with_retry_flag<E: Engine>(
            key: &[u8],
            value: &[u8],
            pk: &[u8],
            secondary_keys: Option<Vec<Vec<u8>>>,
            ts: u64,
            pessimistic_action: PrewriteRequestPessimisticAction,
            is_retry_request: bool,
            engine: &mut E,
            cm: &ConcurrencyManager,
            statistics: &mut Statistics,
        ) -> Result<PrewriteResult> {
            let mutation = Mutation::make_put(Key::from_raw(key), value.to_vec());
            let mut ctx = Context::default();
            ctx.set_is_retry_request(is_retry_request);
            let cmd = PrewritePessimistic::new(
                vec![(mutation, pessimistic_action)],
                pk.to_vec(),
                ts.into(),
                100,
                ts.into(),
                1,
                (ts + 1).into(),
                0.into(),
                secondary_keys,
                false,
                AssertionLevel::Off,
                vec![],
                ctx,
            );
            prewrite_command(engine, cm.clone(), statistics, cmd)
        }

        must_acquire_pessimistic_lock(&mut engine, b"k1", b"k1", 10, 10);
        must_pessimistic_prewrite_put_async_commit(
            &mut engine,
            b"k1",
            b"v1",
            b"k1",
            &Some(vec![b"k2".to_vec()]),
            10,
            10,
            DoPessimisticCheck,
            15,
        );
        must_pessimistic_prewrite_put_async_commit(
            &mut engine,
            b"k2",
            b"v2",
            b"k1",
            &Some(vec![]),
            10,
            10,
            SkipPessimisticCheck,
            15,
        );

        // The transaction may be committed by another reader.
        must_commit(&mut engine, b"k1", 10, 20);
        must_commit(&mut engine, b"k2", 10, 20);

        // This is a re-sent prewrite.
        prewrite_with_retry_flag(
            b"k2",
            b"v2",
            b"k1",
            Some(vec![]),
            10,
            SkipPessimisticCheck,
            true,
            &mut engine,
            cm,
            &mut statistics,
        )
        .unwrap();
        // Commit repeatedly, these operations should have no effect.
        must_commit(&mut engine, b"k1", 10, 25);
        must_commit(&mut engine, b"k2", 10, 25);

        // Seek from 30, we should read commit_ts = 20 instead of 25.
        must_seek_write(&mut engine, b"k1", 30, 10, 20, WriteType::Put);
        must_seek_write(&mut engine, b"k2", 30, 10, 20, WriteType::Put);

        // Write another version to the keys.
        must_prewrite_put(&mut engine, b"k1", b"v11", b"k1", 35);
        must_prewrite_put(&mut engine, b"k2", b"v22", b"k1", 35);
        must_commit(&mut engine, b"k1", 35, 40);
        must_commit(&mut engine, b"k2", 35, 40);

        // A retrying non-pessimistic-lock prewrite request should not skip constraint
        // checks. Here it should take no effect, even there's already a newer version
        // after it. (No matter if it's async commit).
        prewrite_with_retry_flag(
            b"k2",
            b"v2",
            b"k1",
            Some(vec![]),
            10,
            SkipPessimisticCheck,
            true,
            &mut engine,
            cm,
            &mut statistics,
        )
        .unwrap();
        must_unlocked(&mut engine, b"k2");

        prewrite_with_retry_flag(
            b"k2",
            b"v2",
            b"k1",
            None,
            10,
            SkipPessimisticCheck,
            true,
            &mut engine,
            cm,
            &mut statistics,
        )
        .unwrap();
        must_unlocked(&mut engine, b"k2");
        // Committing still does nothing.
        must_commit(&mut engine, b"k2", 10, 25);
        // Try a different txn start ts (which haven't been successfully committed
        // before). It should report a PessimisticLockNotFound.
        let err = prewrite_with_retry_flag(
            b"k2",
            b"v2",
            b"k1",
            None,
            11,
            SkipPessimisticCheck,
            true,
            &mut engine,
            cm,
            &mut statistics,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            Error(box ErrorInner::Mvcc(MvccError(box MvccErrorInner::PessimisticLockNotFound {
                ..
            })))
        ));
        must_unlocked(&mut engine, b"k2");
        // However conflict still won't be checked if there's a non-retry request
        // arriving.
        prewrite_with_retry_flag(
            b"k2",
            b"v2",
            b"k1",
            None,
            10,
            SkipPessimisticCheck,
            false,
            &mut engine,
            cm,
            &mut statistics,
        )
        .unwrap();
        must_locked(&mut engine, b"k2", 10);
    }

    #[test]
    fn test_prewrite_rolledback_transaction() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new_for_test(1.into());
        let mut statistics = Statistics::default();

        let k1 = b"k1";
        let v1 = b"v1";
        let v2 = b"v2";

        // Test the write conflict path.
        must_acquire_pessimistic_lock(&mut engine, k1, v1, 1, 1);
        must_rollback(&mut engine, k1, 1, true);
        must_prewrite_put(&mut engine, k1, v2, k1, 5);
        must_commit(&mut engine, k1, 5, 6);
        let prewrite_cmd = Prewrite::new(
            vec![Mutation::make_put(Key::from_raw(k1), v1.to_vec())],
            k1.to_vec(),
            1.into(),
            10,
            false,
            2,
            2.into(),
            10.into(),
            Some(vec![]),
            false,
            AssertionLevel::Off,
            Context::default(),
        );
        let context = WriteContext {
            lock_mgr: &MockLockManager::new(),
            concurrency_manager: cm.clone(),
            extra_op: ExtraOp::Noop,
            statistics: &mut statistics,
            async_apply_prewrite: false,
            raw_ext: None,
            txn_status_cache: Arc::new(TxnStatusCache::new_for_test()),
        };
        let snap = engine.snapshot(Default::default()).unwrap();
        assert!(prewrite_cmd.cmd.process_write(snap, context).is_err());

        // Test the pessimistic lock is not found path.
        must_acquire_pessimistic_lock(&mut engine, k1, v1, 10, 10);
        must_rollback(&mut engine, k1, 10, true);
        must_acquire_pessimistic_lock(&mut engine, k1, v1, 15, 15);
        let prewrite_cmd = PrewritePessimistic::with_defaults(
            vec![(
                Mutation::make_put(Key::from_raw(k1), v1.to_vec()),
                DoPessimisticCheck,
            )],
            k1.to_vec(),
            10.into(),
            10.into(),
        );
        let context = WriteContext {
            lock_mgr: &MockLockManager::new(),
            concurrency_manager: cm,
            extra_op: ExtraOp::Noop,
            statistics: &mut statistics,
            async_apply_prewrite: false,
            raw_ext: None,
            txn_status_cache: Arc::new(TxnStatusCache::new_for_test()),
        };
        let snap = engine.snapshot(Default::default()).unwrap();
        assert!(prewrite_cmd.cmd.process_write(snap, context).is_err());
    }

    #[test]
    fn test_assertion_fail_on_conflicting_index_key() {
        let mut engine = crate::storage::TestEngineBuilder::new().build().unwrap();

        // Simulate two transactions that tries to insert the same row with a secondary
        // index, and the second one canceled the first one (by rolling back its lock).

        let t1_start_ts = TimeStamp::compose(1, 0);
        let t2_start_ts = TimeStamp::compose(2, 0);
        let t2_commit_ts = TimeStamp::compose(3, 0);

        // txn1 acquires lock on the row key.
        must_acquire_pessimistic_lock(&mut engine, b"row", b"row", t1_start_ts, t1_start_ts);
        // txn2 rolls it back.
        let err = must_acquire_pessimistic_lock_err(
            &mut engine,
            b"row",
            b"row",
            t2_start_ts,
            t2_start_ts,
        );
        assert!(matches!(err, MvccError(box MvccErrorInner::KeyIsLocked(_))));
        must_check_txn_status(
            &mut engine,
            b"row",
            t1_start_ts,
            t2_start_ts,
            t2_start_ts,
            false,
            false,
            true,
            |status| status == TxnStatus::PessimisticRollBack,
        );
        // And then txn2 acquire continues and finally commits
        must_acquire_pessimistic_lock(&mut engine, b"row", b"row", t2_start_ts, t2_start_ts);
        must_prewrite_put_impl(
            &mut engine,
            b"row",
            b"value",
            b"row",
            &None,
            t2_start_ts,
            DoPessimisticCheck,
            1000,
            t2_start_ts,
            1,
            t2_start_ts.next(),
            0.into(),
            false,
            Assertion::NotExist,
            AssertionLevel::Strict,
        );
        must_prewrite_put_impl(
            &mut engine,
            b"index",
            b"value",
            b"row",
            &None,
            t2_start_ts,
            SkipPessimisticCheck,
            1000,
            t2_start_ts,
            1,
            t2_start_ts.next(),
            0.into(),
            false,
            Assertion::NotExist,
            AssertionLevel::Strict,
        );
        must_commit(&mut engine, b"row", t2_start_ts, t2_commit_ts);
        must_commit(&mut engine, b"index", t2_start_ts, t2_commit_ts);

        // Txn1 continues. If the two keys are sent in the single prewrite request, the
        // AssertionFailed error won't be returned since there are other error.
        let cm = ConcurrencyManager::new_for_test(1.into());
        let mut stat = Statistics::default();
        // Two keys in single request:
        let cmd = PrewritePessimistic::with_defaults(
            vec![
                (
                    Mutation::make_put(Key::from_raw(b"row"), b"value".to_vec()),
                    DoPessimisticCheck,
                ),
                (
                    Mutation::make_put(Key::from_raw(b"index"), b"value".to_vec()),
                    SkipPessimisticCheck,
                ),
            ],
            b"row".to_vec(),
            t1_start_ts,
            t2_start_ts,
        );
        let err = prewrite_command(&mut engine, cm.clone(), &mut stat, cmd).unwrap_err();
        assert!(matches!(
            err,
            Error(box ErrorInner::Mvcc(MvccError(box MvccErrorInner::PessimisticLockNotFound {
                ..
            })))
        ));
        // Passing keys in different order gets the same result:
        let cmd = PrewritePessimistic::with_defaults(
            vec![
                (
                    Mutation::make_put(Key::from_raw(b"index"), b"value".to_vec()),
                    SkipPessimisticCheck,
                ),
                (
                    Mutation::make_put(Key::from_raw(b"row"), b"value".to_vec()),
                    DoPessimisticCheck,
                ),
            ],
            b"row".to_vec(),
            t1_start_ts,
            t2_start_ts,
        );
        let err = prewrite_command(&mut engine, cm, &mut stat, cmd).unwrap_err();
        assert!(matches!(
            err,
            Error(box ErrorInner::Mvcc(MvccError(box MvccErrorInner::PessimisticLockNotFound {
                ..
            })))
        ));

        // If the two keys are sent in different requests, it would be the client's duty
        // to ignore the assertion error.
        let err = must_prewrite_put_err_impl(
            &mut engine,
            b"row",
            b"value",
            b"row",
            &None,
            t1_start_ts,
            t1_start_ts,
            DoPessimisticCheck,
            0,
            false,
            Assertion::NotExist,
            AssertionLevel::Strict,
        );
        assert!(matches!(
            err,
            MvccError(box MvccErrorInner::PessimisticLockNotFound { .. })
        ));
        let err = must_prewrite_put_err_impl(
            &mut engine,
            b"index",
            b"value",
            b"row",
            &None,
            t1_start_ts,
            t1_start_ts,
            SkipPessimisticCheck,
            0,
            false,
            Assertion::NotExist,
            AssertionLevel::Strict,
        );
        assert!(matches!(
            err,
            MvccError(box MvccErrorInner::PessimisticLockNotFound { .. })
        ));
    }

    #[test]
    fn test_prewrite_committed_encounter_newer_lock() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let mut statistics = Statistics::default();

        let k1 = b"k1";
        let v1 = b"v1";
        let v2 = b"v2";

        must_prewrite_put_async_commit(&mut engine, k1, v1, k1, &Some(vec![]), 5, 10);
        // This commit may actually come from a ResolveLock command
        must_commit(&mut engine, k1, 5, 15);

        // Another transaction prewrites
        must_prewrite_put(&mut engine, k1, v2, k1, 20);

        // A retried prewrite of the first transaction should be idempotent.
        let prewrite_cmd = Prewrite::new(
            vec![Mutation::make_put(Key::from_raw(k1), v1.to_vec())],
            k1.to_vec(),
            5.into(),
            2000,
            false,
            1,
            5.into(),
            1000.into(),
            Some(vec![]),
            false,
            AssertionLevel::Off,
            Context::default(),
        );
        let context = WriteContext {
            lock_mgr: &MockLockManager::new(),
            concurrency_manager: ConcurrencyManager::new_for_test(20.into()),
            extra_op: ExtraOp::Noop,
            statistics: &mut statistics,
            async_apply_prewrite: false,
            raw_ext: None,
            txn_status_cache: Arc::new(TxnStatusCache::new_for_test()),
        };
        let snap = engine.snapshot(Default::default()).unwrap();
        let res = prewrite_cmd.cmd.process_write(snap, context).unwrap();
        match res.pr {
            ProcessResult::PrewriteResult { result } => {
                assert!(result.locks.is_empty(), "{:?}", result);
                assert_eq!(result.min_commit_ts, 15.into(), "{:?}", result);
            }
            _ => panic!("unexpected result {:?}", res.pr),
        }
    }

    #[test]
    fn test_repeated_prewrite_commit_ts_too_large() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new_for_test(1.into());
        let mut statistics = Statistics::default();

        // First, prewrite and commit normally.
        must_acquire_pessimistic_lock(&mut engine, b"k1", b"k1", 5, 10);
        must_pessimistic_prewrite_put_async_commit(
            &mut engine,
            b"k1",
            b"v1",
            b"k1",
            &Some(vec![b"k2".to_vec()]),
            5,
            10,
            DoPessimisticCheck,
            15,
        );
        must_prewrite_put_impl(
            &mut engine,
            b"k2",
            b"v2",
            b"k1",
            &Some(vec![]),
            5.into(),
            SkipPessimisticCheck,
            100,
            10.into(),
            1,
            15.into(),
            20.into(),
            false,
            Assertion::None,
            AssertionLevel::Off,
        );
        must_commit(&mut engine, b"k1", 5, 18);
        must_commit(&mut engine, b"k2", 5, 18);

        // Update max_ts to be larger than the max_commit_ts.
        cm.update_max_ts(50.into(), "").unwrap();

        // Retry the prewrite on non-pessimistic key.
        // (is_retry_request flag is not set, here we don't rely on it.)
        let mutation = Mutation::make_put(Key::from_raw(b"k2"), b"v2".to_vec());
        let cmd = PrewritePessimistic::new(
            vec![(mutation, SkipPessimisticCheck)],
            b"k1".to_vec(),
            5.into(),
            100,
            10.into(),
            1,
            15.into(),
            20.into(),
            Some(vec![]),
            false,
            AssertionLevel::Off,
            vec![],
            Context::default(),
        );
        let res = prewrite_command(&mut engine, cm, &mut statistics, cmd).unwrap();
        // It should return the real commit TS as the min_commit_ts in the result.
        assert_eq!(res.min_commit_ts, 18.into(), "{:?}", res);
        must_unlocked(&mut engine, b"k2");
    }

    #[test]
    fn test_1pc_calculate_last_change_ts() {
        use pd_client::FeatureGate;

        use crate::storage::txn::sched_pool::set_tls_feature_gate;

        let mut engine = TestEngineBuilder::new().build().unwrap();
        let cm = concurrency_manager::ConcurrencyManager::new_for_test(1.into());

        let key = b"k";
        let value = b"v";
        must_prewrite_put(&mut engine, key, value, key, 10);
        must_commit(&mut engine, key, 10, 20);

        // 1PC write a new LOCK
        let mutations = vec![Mutation::make_lock(Key::from_raw(key))];
        let mut statistics = Statistics::default();
        let res = prewrite_with_cm(
            &mut engine,
            cm.clone(),
            &mut statistics,
            mutations.clone(),
            key.to_vec(),
            30,
            Some(40),
        )
        .unwrap();
        must_unlocked(&mut engine, key);
        let write = must_written(&mut engine, key, 30, res.one_pc_commit_ts, WriteType::Lock);
        assert_eq!(write.last_change, LastChange::make_exist(20.into(), 1));

        // 1PC write another LOCK
        let res = prewrite_with_cm(
            &mut engine,
            cm.clone(),
            &mut statistics,
            mutations,
            key.to_vec(),
            50,
            Some(60),
        )
        .unwrap();
        must_unlocked(&mut engine, key);
        let write = must_written(&mut engine, key, 50, res.one_pc_commit_ts, WriteType::Lock);
        assert_eq!(write.last_change, LastChange::make_exist(20.into(), 2));

        // 1PC write a PUT
        let mutations = vec![Mutation::make_put(Key::from_raw(key), b"v2".to_vec())];
        let res = prewrite_with_cm(
            &mut engine,
            cm.clone(),
            &mut statistics,
            mutations,
            key.to_vec(),
            70,
            Some(80),
        )
        .unwrap();
        must_unlocked(&mut engine, key);
        let write = must_written(&mut engine, key, 70, res.one_pc_commit_ts, WriteType::Put);
        assert_eq!(write.last_change, LastChange::Unknown);

        // TiKV 6.4 should not have last_change_ts.
        let feature_gate = FeatureGate::default();
        feature_gate.set_version("6.4.0").unwrap();
        set_tls_feature_gate(feature_gate);
        let mutations = vec![Mutation::make_lock(Key::from_raw(key))];
        let res = prewrite_with_cm(
            &mut engine,
            cm,
            &mut statistics,
            mutations,
            key.to_vec(),
            80,
            Some(90),
        )
        .unwrap();
        must_unlocked(&mut engine, key);
        let write = must_written(&mut engine, key, 80, res.one_pc_commit_ts, WriteType::Lock);
        assert_eq!(write.last_change, LastChange::Unknown);
    }

    #[test]
    fn test_pessimistic_1pc_calculate_last_change_ts() {
        use pd_client::FeatureGate;

        use crate::storage::txn::sched_pool::set_tls_feature_gate;

        let mut engine = TestEngineBuilder::new().build().unwrap();
        let cm = concurrency_manager::ConcurrencyManager::new_for_test(1.into());

        let key = b"k";
        let value = b"v";
        must_prewrite_put(&mut engine, key, value, key, 10);
        must_commit(&mut engine, key, 10, 20);

        // Pessimistic 1PC write a new LOCK
        must_acquire_pessimistic_lock(&mut engine, key, key, 30, 30);
        let mutations = vec![(Mutation::make_lock(Key::from_raw(key)), DoPessimisticCheck)];
        let mut statistics = Statistics::default();
        let res = pessimistic_prewrite_with_cm(
            &mut engine,
            cm.clone(),
            &mut statistics,
            mutations.clone(),
            key.to_vec(),
            30,
            30,
            Some(40),
        )
        .unwrap();
        must_unlocked(&mut engine, key);
        let write = must_written(&mut engine, key, 30, res.one_pc_commit_ts, WriteType::Lock);
        assert_eq!(write.last_change, LastChange::make_exist(20.into(), 1));

        // Pessimistic 1PC write another LOCK
        must_acquire_pessimistic_lock(&mut engine, key, key, 50, 50);
        let res = pessimistic_prewrite_with_cm(
            &mut engine,
            cm.clone(),
            &mut statistics,
            mutations,
            key.to_vec(),
            50,
            50,
            Some(60),
        )
        .unwrap();
        must_unlocked(&mut engine, key);
        let write = must_written(&mut engine, key, 50, res.one_pc_commit_ts, WriteType::Lock);
        assert_eq!(write.last_change, LastChange::make_exist(20.into(), 2));

        // Pessimistic 1PC write a PUT
        must_acquire_pessimistic_lock(&mut engine, key, key, 70, 70);
        let mutations = vec![(
            Mutation::make_put(Key::from_raw(key), b"v2".to_vec()),
            DoPessimisticCheck,
        )];
        let res = pessimistic_prewrite_with_cm(
            &mut engine,
            cm.clone(),
            &mut statistics,
            mutations,
            key.to_vec(),
            70,
            70,
            Some(80),
        )
        .unwrap();
        must_unlocked(&mut engine, key);
        let write = must_written(&mut engine, key, 70, res.one_pc_commit_ts, WriteType::Put);
        assert_eq!(write.last_change, LastChange::Unknown);

        // TiKV 6.4 should not have last_change_ts.
        let feature_gate = FeatureGate::default();
        feature_gate.set_version("6.4.0").unwrap();
        set_tls_feature_gate(feature_gate);
        must_acquire_pessimistic_lock(&mut engine, key, key, 80, 80);
        let mutations = vec![(Mutation::make_lock(Key::from_raw(key)), DoPessimisticCheck)];
        let res = pessimistic_prewrite_with_cm(
            &mut engine,
            cm,
            &mut statistics,
            mutations,
            key.to_vec(),
            80,
            80,
            Some(90),
        )
        .unwrap();
        must_unlocked(&mut engine, key);
        let write = must_written(&mut engine, key, 80, res.one_pc_commit_ts, WriteType::Lock);
        assert_eq!(write.last_change, LastChange::Unknown);
    }

    #[test]
    fn test_pessimistic_prewrite_check_for_update_ts() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let mut statistics = Statistics::default();

        let k1 = b"k1";
        let k2 = b"k2";
        let k3 = b"k3";

        // In actual cases these kinds of pessimistic locks should be locked in
        // `allow_locking_with_conflict` mode. For simplicity, we pass a large
        // for_update_ts to the pessimistic lock to simulate that case.
        must_acquire_pessimistic_lock(&mut engine, k1, k1, 10, 10);
        must_acquire_pessimistic_lock(&mut engine, k2, k1, 10, 20);
        must_acquire_pessimistic_lock(&mut engine, k3, k1, 10, 20);

        let check_lock_unchanged = |engine: &mut _| {
            must_pessimistic_locked(engine, k1, 10, 10);
            must_pessimistic_locked(engine, k2, 10, 20);
            must_pessimistic_locked(engine, k3, 10, 20);
        };

        let must_be_pessimistic_lock_not_found = |e| match e {
            Error(box ErrorInner::Mvcc(MvccError(
                box MvccErrorInner::PessimisticLockNotFound { .. },
            ))) => (),
            e => panic!(
                "error type not match: expected PessimisticLockNotFound, got {:?}",
                e
            ),
        };

        let mutations = vec![
            (
                Mutation::make_put(Key::from_raw(k1), b"v1".to_vec()),
                DoPessimisticCheck,
            ),
            (
                Mutation::make_put(Key::from_raw(k2), b"v2".to_vec()),
                DoPessimisticCheck,
            ),
            (
                Mutation::make_put(Key::from_raw(k3), b"v3".to_vec()),
                DoPessimisticCheck,
            ),
        ];

        let e = pessimistic_prewrite_check_for_update_ts(
            &mut engine,
            &mut statistics,
            mutations.clone(),
            k1.to_vec(),
            10,
            15,
            vec![(1, 15)],
        )
        .unwrap_err();
        must_be_pessimistic_lock_not_found(e);
        check_lock_unchanged(&mut engine);

        let e = pessimistic_prewrite_check_for_update_ts(
            &mut engine,
            &mut statistics,
            mutations.clone(),
            k1.to_vec(),
            10,
            15,
            vec![(0, 15), (1, 15), (2, 15)],
        )
        .unwrap_err();
        must_be_pessimistic_lock_not_found(e);
        check_lock_unchanged(&mut engine);

        let e = pessimistic_prewrite_check_for_update_ts(
            &mut engine,
            &mut statistics,
            mutations.clone(),
            k1.to_vec(),
            10,
            15,
            vec![(2, 15), (0, 20)],
        )
        .unwrap_err();
        must_be_pessimistic_lock_not_found(e);
        check_lock_unchanged(&mut engine);

        // lock.for_update_ts < expected is disallowed too.
        let e = pessimistic_prewrite_check_for_update_ts(
            &mut engine,
            &mut statistics,
            mutations.clone(),
            k1.to_vec(),
            10,
            15,
            vec![(0, 15), (2, 20)],
        )
        .unwrap_err();
        must_be_pessimistic_lock_not_found(e);
        check_lock_unchanged(&mut engine);

        // Index out of bound (invalid request).
        pessimistic_prewrite_check_for_update_ts(
            &mut engine,
            &mut statistics,
            mutations.clone(),
            k1.to_vec(),
            10,
            15,
            vec![(3, 30)],
        )
        .unwrap_err();
        check_lock_unchanged(&mut engine);

        pessimistic_prewrite_check_for_update_ts(
            &mut engine,
            &mut statistics,
            mutations,
            k1.to_vec(),
            10,
            15,
            vec![(0, 10), (2, 20)],
        )
        .unwrap();
        must_locked(&mut engine, k1, 10);
        must_locked(&mut engine, k2, 10);
        must_locked(&mut engine, k3, 10);
    }
}
