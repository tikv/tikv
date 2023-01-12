// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{Debug, Formatter},
    sync::Arc,
};

// #[PerformanceCriticalPath]
use kvproto::kvrpcpb::ExtraOp;
use txn_types::{insert_old_value_if_resolved, Key, OldValues};

use crate::storage::{
    lock_manager::{
        lock_wait_context::LockWaitContextSharedState, lock_waiting_queue::LockWaitEntry,
        LockManager, LockWaitToken,
    },
    mvcc::{Error as MvccError, ErrorInner as MvccErrorInner, MvccTxn, SnapshotReader},
    txn::{
        acquire_pessimistic_lock,
        commands::{
            acquire_pessimistic_lock::make_write_data, Command, CommandExt, ReleasedLocks,
            ResponsePolicy, TypedCommand, WriteCommand, WriteContext, WriteResult,
            WriteResultLockInfo,
        },
        Error, Result,
    },
    types::{PessimisticLockParameters, PessimisticLockResults},
    Error as StorageError, PessimisticLockKeyResult, ProcessResult, Result as StorageResult,
    Snapshot,
};

pub struct ResumedPessimisticLockItem {
    pub key: Key,
    pub should_not_exist: bool,
    pub params: PessimisticLockParameters,
    pub lock_wait_token: LockWaitToken,
    pub req_states: Arc<LockWaitContextSharedState>,
}

impl Debug for ResumedPessimisticLockItem {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResumedPessimisticLockItem")
            .field("key", &self.key)
            .field("should_not_exist", &self.should_not_exist)
            .field("params", &self.params)
            .field("lock_wait_token", &self.lock_wait_token)
            .finish()
    }
}

command! {
    /// Acquire a Pessimistic lock on the keys.
    ///
    /// This can be rolled back with a [`PessimisticRollback`](Command::PessimisticRollback) command.
    AcquirePessimisticLockResumed:
        cmd_ty => StorageResult<PessimisticLockResults>,
        display => "kv::command::acquirepessimisticlockresumed {:?}",
        (items),
        content => {
            items: Vec<ResumedPessimisticLockItem>,
        }
}

impl CommandExt for AcquirePessimisticLockResumed {
    ctx!();
    tag!(acquire_pessimistic_lock_resumed);
    request_type!(KvPessimisticLock);

    property!(can_be_pipelined);

    fn write_bytes(&self) -> usize {
        self.items
            .iter()
            .map(|item| item.key.as_encoded().len())
            .sum()
    }

    gen_lock!(items: multiple(|x| &x.key));
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for AcquirePessimisticLockResumed {
    fn process_write(self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        fail_point!("acquire_pessimistic_lock_resumed_before_process_write");
        let mut modifies = vec![];
        let mut new_acquired_locks = vec![];
        let mut txn = None;
        let mut reader: Option<SnapshotReader<S>> = None;

        let total_keys = self.items.len();
        let mut res = PessimisticLockResults::with_capacity(total_keys);
        let mut encountered_locks = vec![];
        let need_old_value = context.extra_op == ExtraOp::ReadOldValue;
        let mut old_values = OldValues::default();

        let mut new_locked_keys = Vec::with_capacity(total_keys);

        for item in self.items.into_iter() {
            let ResumedPessimisticLockItem {
                key,
                should_not_exist,
                params,
                lock_wait_token,
                req_states,
            } = item;

            // TODO: Refine the code for rebuilding txn state.
            if txn
                .as_ref()
                .map_or(true, |t: &MvccTxn| t.start_ts != params.start_ts)
            {
                if let Some(mut prev_txn) = txn.replace(MvccTxn::new(
                    params.start_ts,
                    context.concurrency_manager.clone(),
                )) {
                    new_acquired_locks.extend(prev_txn.take_new_locks());
                    modifies.extend(prev_txn.into_modifies());
                }
                // TODO: Is it possible to reuse the same reader but change the start_ts stored
                // in it?
                if let Some(mut prev_reader) = reader.replace(SnapshotReader::new_with_ctx(
                    params.start_ts,
                    snapshot.clone(),
                    &self.ctx,
                )) {
                    context.statistics.add(&prev_reader.take_statistics());
                }
            }
            let txn = txn.as_mut().unwrap();
            let reader = reader.as_mut().unwrap();

            match acquire_pessimistic_lock(
                txn,
                reader,
                key.clone(),
                &params.primary,
                should_not_exist,
                params.lock_ttl,
                params.for_update_ts,
                params.return_values,
                params.check_existence,
                params.min_commit_ts,
                need_old_value,
                params.lock_only_if_exists,
                true,
            ) {
                Ok((key_res, old_value)) => {
                    res.push(key_res);
                    new_locked_keys.push((params.start_ts, key.clone()));

                    insert_old_value_if_resolved(
                        &mut old_values,
                        key,
                        params.start_ts,
                        old_value,
                        None,
                    );
                }
                Err(MvccError(box MvccErrorInner::KeyIsLocked(lock_info))) => {
                    let mut lock_info =
                        WriteResultLockInfo::new(lock_info, params, key, should_not_exist);
                    lock_info.lock_wait_token = lock_wait_token;
                    lock_info.req_states = Some(req_states);
                    res.push(PessimisticLockKeyResult::Waiting);
                    encountered_locks.push(lock_info);
                }
                Err(e) => {
                    res.push(PessimisticLockKeyResult::Failed(
                        StorageError::from(Error::from(e)).into(),
                    ));
                }
            };
        }

        if let Some(mut txn) = txn {
            if !txn.is_empty() {
                new_acquired_locks.extend(txn.take_new_locks());
                modifies.extend(txn.into_modifies());
            }
        }
        if let Some(mut reader) = reader {
            context.statistics.add(&reader.take_statistics());
        }

        let pr = ProcessResult::PessimisticLockRes { res: Ok(res) };
        let to_be_write = make_write_data(modifies, old_values);

        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write,
            rows: total_keys,
            pr,
            lock_info: encountered_locks,
            released_locks: ReleasedLocks::new(),
            new_acquired_locks,
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnProposed,
        })
    }
}

impl AcquirePessimisticLockResumed {
    pub fn from_lock_wait_entries(
        lock_wait_entries: impl IntoIterator<Item = Box<LockWaitEntry>>,
    ) -> TypedCommand<StorageResult<PessimisticLockResults>> {
        let items: Vec<_> = lock_wait_entries
            .into_iter()
            .map(|item| {
                assert!(item.key_cb.is_none());
                ResumedPessimisticLockItem {
                    key: item.key,
                    should_not_exist: item.should_not_exist,
                    params: item.parameters,
                    lock_wait_token: item.lock_wait_token,
                    req_states: item.req_states,
                }
            })
            .collect();

        assert!(!items.is_empty());
        let ctx = items[0].params.pb_ctx.clone();
        // TODO: May it cause problem by using the first one as the pb_ctx of the
        // Command?
        Self::new(items, ctx)
    }
}

#[cfg(test)]
mod tests {
    use concurrency_manager::ConcurrencyManager;
    use kvproto::kvrpcpb::Context;
    use rand::random;
    use tikv_kv::Engine;
    use txn_types::TimeStamp;

    use super::*;
    use crate::storage::{
        lock_manager::{MockLockManager, WaitTimeout},
        mvcc::tests::{must_locked, write},
        txn::{
            commands::pessimistic_rollback::tests::must_success as must_pessimistic_rollback,
            tests::{must_commit, must_pessimistic_locked, must_prewrite_put, must_rollback},
        },
        TestEngineBuilder,
    };

    #[allow(clippy::vec_box)]
    fn must_success<E: Engine>(
        engine: &mut E,
        lock_wait_entries: Vec<Box<LockWaitEntry>>,
    ) -> PessimisticLockResults {
        let ctx = Context::default();
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let cm = ConcurrencyManager::new(TimeStamp::zero());

        let items_info: Vec<_> = lock_wait_entries
            .iter()
            .map(|item| {
                (
                    item.lock_wait_token,
                    item.key.clone(),
                    item.parameters.clone(),
                    item.should_not_exist,
                )
            })
            .collect();

        let command = AcquirePessimisticLockResumed::from_lock_wait_entries(lock_wait_entries).cmd;
        let result = command
            .process_write(
                snapshot,
                WriteContext {
                    lock_mgr: &MockLockManager::new(),
                    concurrency_manager: cm,
                    extra_op: Default::default(),
                    statistics: &mut Default::default(),
                    async_apply_prewrite: false,
                    raw_ext: None,
                },
            )
            .unwrap();
        let res = if let ProcessResult::PessimisticLockRes { res } = result.pr {
            res.unwrap()
        } else {
            panic!("unexpected process result: {:?}", result.pr);
        };

        // Check correctness of returned lock info.
        let mut lock_info_index = 0;
        for (i, res) in res.0.iter().enumerate() {
            if let PessimisticLockKeyResult::Waiting = res {
                let (token, key, params, should_not_exist) = &items_info[i];
                let lock_info: &WriteResultLockInfo = &result.lock_info[lock_info_index];
                lock_info_index += 1;

                assert_eq!(lock_info.lock_wait_token, *token);
                assert_eq!(&lock_info.key, key);
                assert_eq!(&lock_info.parameters, params);
                assert_eq!(lock_info.should_not_exist, *should_not_exist);
            }
        }
        assert_eq!(lock_info_index, result.lock_info.len());

        write(engine, &ctx, result.to_be_write.modifies);
        res
    }

    fn make_lock_waiting(
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        for_update_ts: impl Into<TimeStamp>,
        return_values: bool,
        check_existence: bool,
    ) -> Box<LockWaitEntry> {
        let start_ts = start_ts.into();
        let for_update_ts = for_update_ts.into();
        assert!(for_update_ts >= start_ts);
        let parameters = PessimisticLockParameters {
            pb_ctx: Context::default(),
            primary: key.to_vec(),
            start_ts,
            lock_ttl: 1000,
            for_update_ts,
            wait_timeout: Some(WaitTimeout::Millis(1000)),
            return_values,
            min_commit_ts: for_update_ts.next(),
            check_existence,
            is_first_lock: false,
            lock_only_if_exists: false,
            allow_lock_with_conflict: true,
        };

        let key = Key::from_raw(key);
        let lock_hash = key.gen_hash();
        let token = LockWaitToken(Some(random()));
        // The tests in this file doesn't need a valid req_state. Set a dummy value
        // here.
        let req_states = Arc::new(LockWaitContextSharedState::new_dummy(token, key.clone()));
        let entry = LockWaitEntry {
            key,
            lock_hash,
            parameters,
            should_not_exist: false,
            lock_wait_token: token,
            legacy_wake_up_index: Some(0),
            req_states,
            key_cb: None,
        };
        Box::new(entry)
    }

    #[test]
    fn test_acquire_pessimistic_lock_resumed() {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        let res = must_success(
            &mut engine,
            vec![make_lock_waiting(b"k1", 10, 15, false, false)],
        );
        assert_eq!(res.0.len(), 1);
        res.0[0].assert_empty();
        must_pessimistic_locked(&mut engine, b"k1", 10, 15);
        must_pessimistic_rollback(&mut engine, b"k1", 10, 15);

        let res = must_success(
            &mut engine,
            vec![
                make_lock_waiting(b"k1", 20, 25, false, false),
                make_lock_waiting(b"k2", 20, 25, false, false),
                make_lock_waiting(b"k3", 21, 26, false, false),
            ],
        );
        assert_eq!(res.0.len(), 3);
        res.0.iter().for_each(|x| x.assert_empty());
        must_pessimistic_locked(&mut engine, b"k1", 20, 25);
        must_pessimistic_locked(&mut engine, b"k2", 20, 25);
        must_pessimistic_locked(&mut engine, b"k3", 21, 26);

        must_pessimistic_rollback(&mut engine, b"k1", 20, 25);
        must_pessimistic_rollback(&mut engine, b"k2", 20, 25);
        must_pessimistic_rollback(&mut engine, b"k3", 21, 26);

        must_prewrite_put(&mut engine, b"k1", b"v1", b"k1", 30);
        must_commit(&mut engine, b"k1", 30, 35);
        must_prewrite_put(&mut engine, b"k2", b"v2", b"k1", 30);
        must_prewrite_put(&mut engine, b"k3", b"v3", b"k3", 28);
        must_commit(&mut engine, b"k3", 28, 29);
        let res = must_success(
            &mut engine,
            vec![
                make_lock_waiting(b"k1", 31, 31, false, false),
                make_lock_waiting(b"k2", 32, 32, false, false),
                make_lock_waiting(b"k3", 33, 33, true, false),
                make_lock_waiting(b"k4", 34, 34, false, true),
                make_lock_waiting(b"k5", 35, 35, false, false),
            ],
        );
        assert_eq!(res.0.len(), 5);
        res.0[0].assert_locked_with_conflict(Some(b"v1"), 35);
        res.0[1].assert_waiting();
        res.0[2].assert_value(Some(b"v3"));
        res.0[3].assert_existence(false);
        res.0[4].assert_empty();
        must_pessimistic_locked(&mut engine, b"k1", 31, 35);
        must_locked(&mut engine, b"k2", 30);
        must_pessimistic_locked(&mut engine, b"k3", 33, 33);
        must_pessimistic_locked(&mut engine, b"k4", 34, 34);
        must_pessimistic_locked(&mut engine, b"k5", 35, 35);

        must_pessimistic_rollback(&mut engine, b"k1", 31, 35);
        must_pessimistic_rollback(&mut engine, b"k3", 33, 33);
        must_pessimistic_rollback(&mut engine, b"k4", 34, 34);
        must_pessimistic_rollback(&mut engine, b"k5", 35, 35);

        must_prewrite_put(&mut engine, b"k4", b"v4", b"k4", 40);
        must_prewrite_put(&mut engine, b"k6", b"v6", b"k4", 40);
        let res = must_success(
            &mut engine,
            vec![
                make_lock_waiting(b"k1", 41, 41, false, false),
                make_lock_waiting(b"k2", 41, 41, false, false),
                make_lock_waiting(b"k3", 42, 42, false, false),
                make_lock_waiting(b"k4", 42, 42, false, false),
                make_lock_waiting(b"k5", 43, 43, false, false),
                make_lock_waiting(b"k6", 43, 43, false, false),
            ],
        );
        assert_eq!(res.0.len(), 6);
        for &i in &[0, 2, 4] {
            res.0[i].assert_empty();
        }
        for &i in &[1, 3, 5] {
            res.0[i].assert_waiting();
        }
        must_pessimistic_locked(&mut engine, b"k1", 41, 41);
        must_pessimistic_locked(&mut engine, b"k3", 42, 42);
        must_pessimistic_locked(&mut engine, b"k5", 43, 43);

        must_pessimistic_rollback(&mut engine, b"k1", 41, 41);
        must_rollback(&mut engine, b"k2", 30, false);
        must_pessimistic_rollback(&mut engine, b"k3", 43, 43);
        must_rollback(&mut engine, b"k2", 40, false);
        must_pessimistic_rollback(&mut engine, b"k5", 45, 45);
        must_rollback(&mut engine, b"k2", 40, false);
    }
}
