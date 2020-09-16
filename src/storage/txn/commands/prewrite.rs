// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::CF_WRITE;
use txn_types::{Key, Mutation, TimeStamp};

use crate::storage::kv::WriteData;
use crate::storage::lock_manager::LockManager;
use crate::storage::mvcc::{
    has_data_in_range, Error as MvccError, ErrorInner as MvccErrorInner, MvccTxn,
};
use crate::storage::txn::actions::prewrite::prewrite;
use crate::storage::txn::commands::{WriteCommand, WriteContext, WriteResult};
use crate::storage::txn::{Error, ErrorInner, Result};
use crate::storage::{
    txn::commands::{Command, CommandExt, TypedCommand},
    types::PrewriteResult,
    Context, Error as StorageError, ProcessResult, Snapshot,
};

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

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for Prewrite {
    fn process_write(mut self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
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
                &mut context.statistics.write,
            )? {
                // If there is no data in range, we could skip constraint check.
                self.skip_constraint_check = true;
            }
        }

        // If async commit is disabled in TiKV, set the secondary_keys in the request to None
        // so we won't do anything for async commit.
        if !context.enable_async_commit {
            self.secondary_keys = None;
        }

        // Async commit requires the max timestamp in the concurrency manager to be up-to-date.
        // If it is possibly stale due to leader transfer or region merge, return an error.
        // TODO: Fallback to non-async commit if not synced instead of returning an error.
        if self.secondary_keys.is_some() && !snapshot.is_max_ts_synced() {
            return Err(ErrorInner::MaxTimestampNotSynced {
                region_id: self.get_ctx().get_region_id(),
                start_ts: self.start_ts,
            }
            .into());
        }

        let mut txn = MvccTxn::new(
            snapshot,
            self.start_ts,
            !self.ctx.get_not_fill_cache(),
            context.concurrency_manager,
        );

        // Set extra op here for getting the write record when check write conflict in prewrite.
        txn.extra_op = context.extra_op;

        let async_commit_pk: Option<Key> = self
            .secondary_keys
            .as_ref()
            .filter(|keys| !keys.is_empty())
            .map(|_| Key::from_raw(&self.primary));

        let mut locks = vec![];
        let mut async_commit_ts = TimeStamp::zero();
        for m in self.mutations {
            let mut secondaries = &self.secondary_keys.as_ref().map(|_| vec![]);

            if Some(m.key()) == async_commit_pk.as_ref() {
                secondaries = &self.secondary_keys;
            }
            match prewrite(
                &mut txn,
                m,
                &self.primary,
                secondaries,
                self.skip_constraint_check,
                self.lock_ttl,
                self.txn_size,
                self.min_commit_ts,
            ) {
                Ok(ts) => {
                    if secondaries.is_some() && async_commit_ts < ts {
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
        let (pr, to_be_write, rows, ctx, lock_info, lock_guards) = if locks.is_empty() {
            let pr = ProcessResult::PrewriteResult {
                result: PrewriteResult {
                    locks: vec![],
                    min_commit_ts: async_commit_ts,
                },
            };
            let txn_extra = txn.take_extra();
            // Here the lock guards are taken and will be released after the write finishes.
            // If an error (KeyIsLocked or WriteConflict) occurs before, these lock guards
            // are dropped along with `txn` automatically.
            let lock_guards = txn.take_guards();
            let write_data = WriteData::new(txn.into_modifies(), txn_extra);
            (pr, write_data, rows, self.ctx, None, lock_guards)
        } else {
            // Skip write stage if some keys are locked.
            let pr = ProcessResult::PrewriteResult {
                result: PrewriteResult {
                    locks,
                    min_commit_ts: async_commit_ts,
                },
            };
            (pr, WriteData::default(), 0, self.ctx, None, vec![])
        };
        Ok(WriteResult {
            ctx,
            to_be_write,
            rows,
            pr,
            lock_info,
            lock_guards,
        })
    }
}

#[cfg(test)]
mod tests {
    use kvproto::kvrpcpb::{Context, ExtraOp};

    use concurrency_manager::ConcurrencyManager;
    use engine_traits::CF_WRITE;
    use txn_types::TimeStamp;
    use txn_types::{Key, Mutation};

    use crate::storage::mvcc::{Error as MvccError, ErrorInner as MvccErrorInner};
    use crate::storage::txn::commands::{
        Commit, Prewrite, Rollback, WriteContext, FORWARD_MIN_MUTATIONS_NUM,
    };
    use crate::storage::txn::LockInfo;
    use crate::storage::txn::{Error, ErrorInner, Result};
    use crate::storage::DummyLockManager;
    use crate::storage::{
        Engine, PrewriteResult, ProcessResult, Snapshot, Statistics, TestEngineBuilder,
    };

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
        )
        .unwrap();
        assert_eq!(1, statistic.write.seek);
        let e = prewrite(
            &engine,
            &mut statistic,
            mutations.clone(),
            pri_key.to_vec(),
            100,
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
        )
        .unwrap();
        // All keys are prewrited successful with only one seek operations.
        assert_eq!(1, statistic.write.seek);
        let keys: Vec<Key> = mutations.iter().map(|m| m.key().clone()).collect();
        commit(&engine, &mut statistic, keys.clone(), 104, 105).unwrap();
        let snap = engine.snapshot(&ctx).unwrap();
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
                Key::from_raw(&[i as u8]),
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
        prewrite(&engine, &mut statistic, mutations, pri_key.to_vec(), 110).unwrap();
        let d = perf.delta();
        assert_eq!(1, statistic.write.seek);
        assert_eq!(d.0.internal_delete_skipped_count, 0);
    }

    fn prewrite<E: Engine>(
        engine: &E,
        statistics: &mut Statistics,
        mutations: Vec<Mutation>,
        primary: Vec<u8>,
        start_ts: u64,
    ) -> Result<()> {
        let ctx = Context::default();
        let snap = engine.snapshot(&ctx)?;
        let concurrency_manager = ConcurrencyManager::new(start_ts.into());
        let cmd = Prewrite::with_defaults(mutations, primary, TimeStamp::from(start_ts));
        let context = WriteContext {
            lock_mgr: &DummyLockManager {},
            concurrency_manager,
            extra_op: ExtraOp::Noop,
            statistics,
            pipelined_pessimistic_lock: false,
            enable_async_commit: true,
        };
        let ret = cmd.cmd.process_write(snap, context)?;
        if let ProcessResult::PrewriteResult {
            result: PrewriteResult { locks, .. },
        } = ret.pr
        {
            if !locks.is_empty() {
                let info = LockInfo::default();
                return Err(Error::from(ErrorInner::Mvcc(MvccError::from(
                    MvccErrorInner::KeyIsLocked(info),
                ))));
            }
        }
        let ctx = Context::default();
        engine.write(&ctx, ret.to_be_write).unwrap();
        Ok(())
    }

    fn commit<E: Engine>(
        engine: &E,
        statistics: &mut Statistics,
        keys: Vec<Key>,
        lock_ts: u64,
        commit_ts: u64,
    ) -> Result<()> {
        let ctx = Context::default();
        let snap = engine.snapshot(&ctx)?;
        let concurrency_manager = ConcurrencyManager::new(lock_ts.into());
        let cmd = Commit::new(
            keys,
            TimeStamp::from(lock_ts),
            TimeStamp::from(commit_ts),
            ctx,
        );

        let context = WriteContext {
            lock_mgr: &DummyLockManager {},
            concurrency_manager,
            extra_op: ExtraOp::Noop,
            statistics,
            pipelined_pessimistic_lock: false,
            enable_async_commit: true,
        };

        let ret = cmd.cmd.process_write(snap, context)?;
        let ctx = Context::default();
        engine.write(&ctx, ret.to_be_write).unwrap();
        Ok(())
    }

    fn rollback<E: Engine>(
        engine: &E,
        statistics: &mut Statistics,
        keys: Vec<Key>,
        start_ts: u64,
    ) -> Result<()> {
        let ctx = Context::default();
        let snap = engine.snapshot(&ctx)?;
        let concurrency_manager = ConcurrencyManager::new(start_ts.into());
        let cmd = Rollback::new(keys, TimeStamp::from(start_ts), ctx);
        let context = WriteContext {
            lock_mgr: &DummyLockManager {},
            concurrency_manager,
            extra_op: ExtraOp::Noop,
            statistics,
            pipelined_pessimistic_lock: false,
            enable_async_commit: true,
        };

        let ret = cmd.cmd.process_write(snap, context)?;
        let ctx = Context::default();
        engine.write(&ctx, ret.to_be_write).unwrap();
        Ok(())
    }
}
