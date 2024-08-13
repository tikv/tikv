// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::mem;

use kvproto::kvrpcpb::{AssertionLevel, ExtraOp, PrewriteRequestPessimisticAction};
// #[PerformanceCriticalPath]
use txn_types::{insert_old_value_if_resolved, Mutation, OldValues, TimeStamp, TxnExtra};

use crate::storage::{
    kv::WriteData,
    lock_manager::LockManager,
    mvcc::{MvccTxn, SnapshotReader},
    txn::{
        actions::{common::check_committed_record_on_err, prewrite::prewrite_with_generation},
        commands::{
            CommandExt, ReaderWithStats, ReleasedLocks, ResponsePolicy, WriteCommand, WriteContext,
            WriteResult,
        },
        CommitKind, Error, ErrorInner, Result, TransactionKind, TransactionProperties,
    },
    Command, ProcessResult, Result as StorageResult, Snapshot, TypedCommand,
};

command! {
    Flush:
        cmd_ty => Vec<StorageResult<()>>,
        display => { "kv::command::flush keys({:?}) @ {} | gen={}, {:?}", (mutations, start_ts, generation, ctx), }
        content => {
            start_ts: TimeStamp,
            primary: Vec<u8>,
            mutations: Vec<Mutation>,
            generation: u64,
            lock_ttl: u64,
            assertion_level: AssertionLevel,
        }
        in_heap => {
            mutations,
            primary,
        }
}

impl CommandExt for Flush {
    ctx!();
    tag!(flush);
    request_type!(KvFlush);
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

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for Flush {
    fn process_write(mut self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        if self.generation == 0 {
            return Err(ErrorInner::Other(box_err!(
                "generation should be greater than 0 for Flush requests"
            ))
            .into());
        }
        let rows = self.mutations.len();
        let mut txn = MvccTxn::new(self.start_ts, context.concurrency_manager);
        let mut reader = ReaderWithStats::new(
            SnapshotReader::new_with_ctx(self.start_ts, snapshot, &self.ctx),
            context.statistics,
        );
        let mut old_values = Default::default();

        let res = self.flush(&mut txn, &mut reader, &mut old_values, context.extra_op);
        let locks = res?;
        let extra = TxnExtra {
            old_values,
            one_pc: false,
            allowed_in_flashback: false,
        };
        let new_locks = txn.take_new_locks();
        let guards = txn.take_guards();
        assert!(guards.is_empty());
        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write: WriteData::new(txn.into_modifies(), extra),
            rows,
            pr: ProcessResult::MultiRes { results: locks },
            lock_info: vec![],
            released_locks: ReleasedLocks::new(),
            new_acquired_locks: new_locks,
            lock_guards: guards,
            response_policy: ResponsePolicy::OnApplied,
            known_txn_status: vec![],
        })
    }
}

impl Flush {
    fn flush(
        &mut self,
        txn: &mut MvccTxn,
        reader: &mut SnapshotReader<impl Snapshot>,
        old_values: &mut OldValues,
        extra_op: ExtraOp,
    ) -> Result<Vec<std::result::Result<(), crate::storage::errors::Error>>> {
        let props = TransactionProperties {
            start_ts: self.start_ts,
            kind: TransactionKind::Optimistic(false),
            commit_kind: CommitKind::TwoPc,
            primary: &self.primary,
            // txn_size is unknown, set it to max to avoid unexpected resolve_lock_lite
            txn_size: u64::MAX,
            lock_ttl: self.lock_ttl,
            // min_commit_ts == 0 will disallow readers pushing it
            min_commit_ts: self.start_ts.next(),
            need_old_value: extra_op == ExtraOp::ReadOldValue, // FIXME?
            is_retry_request: self.ctx.is_retry_request,
            assertion_level: self.assertion_level,
            txn_source: self.ctx.get_txn_source(),
        };
        let mut locks = Vec::new();
        // If there are other errors, return other error prior to `AssertionFailed`.
        let mut assertion_failure = None;

        for m in mem::take(&mut self.mutations) {
            let key = m.key().clone();
            let mutation_type = m.mutation_type();
            let prewrite_result = prewrite_with_generation(
                txn,
                reader,
                &props,
                m,
                &None,
                PrewriteRequestPessimisticAction::SkipPessimisticCheck,
                None,
                self.generation,
            );
            match prewrite_result {
                Ok((_ts, old_value)) => {
                    insert_old_value_if_resolved(
                        old_values,
                        key,
                        txn.start_ts,
                        old_value,
                        Some(mutation_type),
                    );
                }
                Err(crate::storage::mvcc::Error(
                    box crate::storage::mvcc::ErrorInner::WriteConflict {
                        start_ts,
                        conflict_commit_ts,
                        ..
                    },
                )) if conflict_commit_ts > start_ts => {
                    return check_committed_record_on_err(prewrite_result, txn, reader, &key)
                        .map(|(locks, _)| locks);
                }
                Err(crate::storage::mvcc::Error(
                    box crate::storage::mvcc::ErrorInner::PessimisticLockNotFound { .. },
                ))
                | Err(crate::storage::mvcc::Error(
                    box crate::storage::mvcc::ErrorInner::CommitTsTooLarge { .. },
                )) => {
                    unreachable!();
                }
                Err(crate::storage::mvcc::Error(
                    box crate::storage::mvcc::ErrorInner::KeyIsLocked { .. },
                )) => match check_committed_record_on_err(prewrite_result, txn, reader, &key) {
                    Ok(res) => return Ok(res.0),
                    Err(e) => locks.push(Err(e.into())),
                },
                Err(
                    e @ crate::storage::mvcc::Error(
                        box crate::storage::mvcc::ErrorInner::AssertionFailed { .. },
                    ),
                ) => {
                    if assertion_failure.is_none() {
                        assertion_failure = Some(e);
                    }
                }
                Err(crate::storage::mvcc::Error(
                    box crate::storage::mvcc::ErrorInner::GenerationOutOfOrder(
                        generation,
                        key,
                        lock,
                    ),
                )) => {
                    info!(
                        "generation in Flush is smaller than that in lock, ignore this mutation";
                        "key" => ?key,
                        "start_ts" => self.start_ts,
                        "generation" => generation,
                        "lock" => ?lock,
                    );
                }
                Err(e) => return Err(Error::from(e)),
            }
        }
        if let Some(e) = assertion_failure {
            return Err(Error::from(e));
        }
        Ok(locks)
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use concurrency_manager::ConcurrencyManager;
    use kvproto::kvrpcpb::{Assertion, AssertionLevel, Context, ExtraOp};
    use tikv_kv::{Engine, Statistics};
    use txn_types::{Key, Mutation, TimeStamp};

    use crate::storage::{
        lock_manager::MockLockManager,
        mvcc::{
            tests::{must_get, must_locked},
            Error as MvccError, ErrorInner as MvccErrorInner,
        },
        txn,
        txn::{
            commands::{Flush, WriteContext, WriteResult},
            tests::{
                must_acquire_pessimistic_lock, must_acquire_pessimistic_lock_err, must_commit,
                must_pessimistic_locked, must_prewrite_put, must_prewrite_put_err,
            },
            txn_status_cache::TxnStatusCache,
            Error, ErrorInner,
        },
        ProcessResult, TestEngineBuilder,
    };

    pub fn flush_put_impl<E: Engine>(
        engine: &mut E,
        key: &[u8],
        value: impl Into<Vec<u8>>,
        pk: impl Into<Vec<u8>>,
        start_ts: impl Into<TimeStamp>,
        generation: u64,
        should_not_exist: bool,
    ) -> txn::Result<WriteResult> {
        flush_put_impl_with_assertion(
            engine,
            key,
            value,
            pk,
            start_ts,
            generation,
            should_not_exist,
            Assertion::None,
        )
    }

    pub fn flush_put_impl_with_assertion<E: Engine>(
        engine: &mut E,
        key: &[u8],
        value: impl Into<Vec<u8>>,
        pk: impl Into<Vec<u8>>,
        start_ts: impl Into<TimeStamp>,
        generation: u64,
        should_not_exist: bool,
        assertion: Assertion,
    ) -> txn::Result<WriteResult> {
        let key = Key::from_raw(key);
        let start_ts = start_ts.into();
        let mut m = if should_not_exist {
            Mutation::make_insert(key, value.into())
        } else {
            Mutation::make_put(key, value.into())
        };
        m.set_assertion(assertion);
        let cmd = Flush::new(
            start_ts,
            pk.into(),
            vec![m],
            generation,
            3000,
            AssertionLevel::Strict,
            Context::new(),
        );
        let mut statistics = Statistics::default();
        let cm = ConcurrencyManager::new(start_ts);
        let context = WriteContext {
            lock_mgr: &MockLockManager::new(),
            concurrency_manager: cm.clone(),
            extra_op: ExtraOp::Noop,
            statistics: &mut statistics,
            async_apply_prewrite: false,
            raw_ext: None,
            txn_status_cache: &TxnStatusCache::new_for_test(),
        };
        let snapshot = engine.snapshot(Default::default()).unwrap();
        cmd.cmd.process_write(snapshot.clone(), context)
    }

    pub fn must_flush_put<E: Engine>(
        engine: &mut E,
        key: &[u8],
        value: impl Into<Vec<u8>>,
        pk: impl Into<Vec<u8>>,
        start_ts: impl Into<TimeStamp>,
        generation: u64,
    ) {
        let res = flush_put_impl(engine, key, value, pk, start_ts, generation, false);
        assert!(res.is_ok());
        let res = res.unwrap();
        let to_be_write = res.to_be_write;
        if to_be_write.modifies.is_empty() {
            return;
        }
        engine.write(&Context::new(), to_be_write).unwrap();
    }

    pub fn must_flush_put_with_assertion<E: Engine>(
        engine: &mut E,
        key: &[u8],
        value: impl Into<Vec<u8>>,
        pk: impl Into<Vec<u8>>,
        start_ts: impl Into<TimeStamp>,
        generation: u64,
        assertion: Assertion,
    ) {
        let res = flush_put_impl_with_assertion(
            engine, key, value, pk, start_ts, generation, false, assertion,
        );
        assert!(res.is_ok());
        let res = res.unwrap();
        let to_be_write = res.to_be_write;
        if to_be_write.modifies.is_empty() {
            return;
        }
        engine.write(&Context::new(), to_be_write).unwrap();
    }

    pub fn must_flush_put_meet_lock<E: Engine>(
        engine: &mut E,
        key: &[u8],
        value: impl Into<Vec<u8>>,
        pk: impl Into<Vec<u8>>,
        start_ts: impl Into<TimeStamp>,
        generation: u64,
    ) {
        let res = flush_put_impl(engine, key, value, pk, start_ts, generation, false).unwrap();
        if let ProcessResult::MultiRes { results } = res.pr {
            assert!(!results.is_empty());
        } else {
            panic!("flush return type error");
        }
    }

    #[allow(unused)]
    pub fn must_flush_put_err<E: Engine>(
        engine: &mut E,
        key: &[u8],
        value: impl Into<Vec<u8>>,
        pk: impl Into<Vec<u8>>,
        start_ts: impl Into<TimeStamp>,
        generation: u64,
    ) -> txn::Error {
        let res = flush_put_impl(engine, key, value, pk, start_ts, generation, false);
        assert!(res.is_err());
        res.err().unwrap()
    }

    pub fn must_flush_insert_err<E: Engine>(
        engine: &mut E,
        key: &[u8],
        value: impl Into<Vec<u8>>,
        pk: impl Into<Vec<u8>>,
        start_ts: impl Into<TimeStamp>,
        generation: u64,
    ) -> txn::Error {
        let res = flush_put_impl(engine, key, value, pk, start_ts, generation, true);
        assert!(res.is_err());
        res.err().unwrap()
    }

    #[test]
    fn test_flush() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let k = b"key";
        let v = b"value";
        let start_ts = 1;
        must_flush_put(&mut engine, k, *v, k, start_ts, 1);
        must_locked(&mut engine, k, start_ts);
        must_commit(&mut engine, k, start_ts, start_ts + 1);
        must_get(&mut engine, k, start_ts + 1, v);
    }

    #[test]
    fn test_write_conflict() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let k = b"key";
        let v = b"value";
        // flush x {flush, pessimistic lock, prewrite}
        must_flush_put(&mut engine, k, *v, k, 1, 1);
        must_locked(&mut engine, k, 1);
        must_flush_put_meet_lock(&mut engine, k, *v, k, 2, 2);
        must_acquire_pessimistic_lock_err(&mut engine, k, k, 2, 2);
        must_prewrite_put_err(&mut engine, k, v, k, 2);

        // pessimistic lock x flush
        let k = b"key2";
        must_acquire_pessimistic_lock(&mut engine, k, k, 1, 1);
        must_pessimistic_locked(&mut engine, k, 1, 1);
        must_flush_put_meet_lock(&mut engine, k, v, k, 2, 3);

        // prewrite x flush
        let k = b"key3";
        must_prewrite_put(&mut engine, k, v, k, 1);
        must_locked(&mut engine, k, 1);
        must_flush_put_meet_lock(&mut engine, k, v, k, 2, 4);
    }

    #[test]
    fn test_flush_overwrite() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let k = b"key";
        let v = b"value";
        must_flush_put(&mut engine, k, *v, k, 1, 1);
        let v2 = b"value2";
        must_flush_put(&mut engine, k, v2, k, 1, 2);
        must_commit(&mut engine, k, 1, 2);
        must_get(&mut engine, k, 3, v2);
    }

    #[test]
    fn test_flush_out_of_order() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let k = b"key";
        let v = b"value";

        // generation == 0 will be rejected
        assert_matches!(
            must_flush_put_err(&mut engine, k, *v, k, 1, 0),
            Error(box ErrorInner::Other(s)) if s.to_string().contains("generation should be greater than 0")
        );

        must_flush_put(&mut engine, k, *v, k, 1, 2);
        must_locked(&mut engine, k, 1);

        // the following flush should have no effect
        let v2 = b"value2";
        must_flush_put(&mut engine, k, *v2, k, 1, 1);
        must_locked(&mut engine, k, 1);
        must_commit(&mut engine, k, 1, 2);
        must_get(&mut engine, k, 3, v);
    }

    #[test]
    fn test_flushed_existence_check() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let k = b"key";
        let v = b"value";
        must_flush_put(&mut engine, k, *v, k, 1, 1);
        must_locked(&mut engine, k, 1);
        assert_matches!(
            must_flush_insert_err(&mut engine, k, *v, k, 1, 2),
            Error(box ErrorInner::Mvcc(MvccError(box MvccErrorInner::AlreadyExist { key, existing_start_ts})))
            if key == k  && existing_start_ts == 1.into()
        );
        must_commit(&mut engine, k, 1, 2);
        assert_matches!(
            must_flush_insert_err(&mut engine, k, *v, k, 3, 1),
            Error(box ErrorInner::Mvcc(MvccError(box MvccErrorInner::AlreadyExist { key, existing_start_ts})))
            if key == k  && existing_start_ts == 1.into()
        );
    }

    #[test]
    fn test_flush_overwrite_assertion() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let k = b"key";
        let v = b"value";
        must_flush_put_with_assertion(&mut engine, k, *v, k, 1, 1, Assertion::NotExist);
        must_locked(&mut engine, k, 1);
        let v2 = b"value2";
        must_flush_put_with_assertion(&mut engine, k, *v2, k, 1, 2, Assertion::Exist);
        must_commit(&mut engine, k, 1, 2);
        must_get(&mut engine, k, 3, v2);
    }
}
