// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::collections::HashSet;

use txn_types::{Key, TimeStamp};

use crate::storage::{
    ProcessResult, Snapshot,
    kv::WriteData,
    lock_manager::LockManager,
    mvcc::{MvccTxn, SnapshotReader},
    txn::{
        Result, cleanup,
        commands::{
            Command, CommandExt, ReaderWithStats, ReleasedLocks, ResponsePolicy, TypedCommand,
            WriteCommand, WriteContext, WriteResult,
        },
        commit,
    },
};

command! {
    /// Resolve locks on `resolve_keys` according to `start_ts` and `commit_ts`.
    ResolveLockLite:
        cmd_ty => (),
        display => {
            "kv::resolve_lock_lite resolve_keys({:?}) {} {} | {:?}",
            (resolve_keys, start_ts, commit_ts, ctx),
        }
        content => {
            /// The transaction timestamp.
            start_ts: TimeStamp,
            /// The transaction commit timestamp.
            commit_ts: TimeStamp,
            /// The keys to resolve.
            resolve_keys: Vec<Key>,
        }
        in_heap => {
            resolve_keys,
        }
}

impl CommandExt for ResolveLockLite {
    ctx!();
    tag!(resolve_lock_lite);
    request_type!(KvResolveLock);
    ts!(start_ts);
    property!(is_sys_cmd);
    write_bytes!(resolve_keys: multiple);
    gen_lock!(resolve_keys: multiple);
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for ResolveLockLite {
    fn process_write(self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        let mut txn = MvccTxn::new(self.start_ts, context.concurrency_manager);
        let mut reader = ReaderWithStats::new(
            SnapshotReader::new_with_ctx(self.start_ts, snapshot, &self.ctx),
            context.statistics,
        );

        // ti-client guarantees the size of resolve_keys will not too large, so no
        // necessary to control the write_size as ResolveLock.
        let mut released_locks = ReleasedLocks::new();
        // A request may list the same key more than once: the key list is built
        // from lock errors that can repeat, and the batch resolver groups the
        // list without deduplicating it. Resolving a key is idempotent, but
        // running it twice within one command writes the same CF_WRITE record
        // twice, and a duplicate MVCC write is not valid output: the resolved-ts
        // change log rejects it with `assert!(row.write.is_none())`, which
        // panics TiKV. Resolve every key at most once.
        let mut resolved_keys = HashSet::with_capacity(self.resolve_keys.len());
        let mut rows = 0;
        for key in self.resolve_keys {
            if !resolved_keys.insert(key.clone()) {
                continue;
            }
            rows += 1;
            released_locks.push(if !self.commit_ts.is_zero() {
                commit(&mut txn, &mut reader, key, self.commit_ts, None)?
            } else {
                cleanup(&mut txn, &mut reader, key, TimeStamp::zero(), false)?
            });
        }

        let known_txn_status = if !self.commit_ts.is_zero() {
            vec![(self.start_ts, self.commit_ts)]
        } else {
            vec![]
        };
        let new_acquired_locks = txn.take_new_locks();
        let mut write_data = WriteData::from_modifies(txn.into_modifies());
        write_data.set_allowed_on_disk_almost_full();
        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write: write_data,
            rows,
            pr: ProcessResult::Res,
            lock_info: vec![],
            released_locks,
            new_acquired_locks,
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
            known_txn_status,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use concurrency_manager::ConcurrencyManager;
    use engine_traits::{CF_LOCK, CF_WRITE};
    use kvproto::kvrpcpb::Context;
    use tikv_kv::Modify;

    use super::*;
    use crate::storage::{
        TestEngineBuilder,
        kv::Engine,
        lock_manager::MockLockManager,
        txn::{
            commands::{WriteCommand, WriteContext},
            tests::must_prewrite_put,
            txn_status_cache::TxnStatusCache,
        },
    };

    fn run_resolve_lock_lite<E: Engine>(
        engine: &mut E,
        start_ts: u64,
        commit_ts: u64,
        keys: Vec<Key>,
    ) -> Vec<Modify> {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let cm = ConcurrencyManager::new_for_test(TimeStamp::new(100));
        let typed =
            ResolveLockLite::new(start_ts.into(), commit_ts.into(), keys, Context::default());
        let command = match Command::from(typed) {
            Command::ResolveLockLite(cmd) => cmd,
            _ => unreachable!(),
        };
        let lock_mgr = MockLockManager::new();
        let write_context = WriteContext {
            lock_mgr: &lock_mgr,
            concurrency_manager: cm,
            extra_op: Default::default(),
            statistics: &mut Default::default(),
            async_apply_prewrite: false,
            raw_ext: None,
            txn_status_cache: Arc::new(TxnStatusCache::new_for_test()),
        };
        command
            .process_write(snapshot, write_context)
            .unwrap()
            .to_be_write
            .modifies
    }

    /// A command may be asked to resolve the same key twice, because the key
    /// list a client sends is built from lock errors that can repeat. Resolving
    /// a key twice used to write its write record twice, which the resolved-ts
    /// change log rejects by panicking.
    #[test]
    fn test_resolve_lock_lite_duplicate_keys_rollback_once() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        must_prewrite_put(&mut engine, b"dup", b"value", b"dup", 10);

        let key = Key::from_raw(b"dup");
        let modifies = run_resolve_lock_lite(&mut engine, 10, 0, vec![key.clone(), key.clone()]);

        assert_no_duplicate_modify(&modifies);
        assert_eq!(count_writes(&modifies, CF_WRITE), 1);
        assert_eq!(count_writes(&modifies, CF_LOCK), 1);
    }

    #[test]
    fn test_resolve_lock_lite_duplicate_keys_commit_once() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        must_prewrite_put(&mut engine, b"dup", b"value", b"dup", 10);

        let key = Key::from_raw(b"dup");
        let modifies = run_resolve_lock_lite(&mut engine, 10, 20, vec![key.clone(), key.clone()]);

        assert_no_duplicate_modify(&modifies);
        assert_eq!(count_writes(&modifies, CF_WRITE), 1);
        assert_eq!(count_writes(&modifies, CF_LOCK), 1);
    }

    /// Distinct keys in one request are still all resolved.
    #[test]
    fn test_resolve_lock_lite_distinct_keys() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        must_prewrite_put(&mut engine, b"k1", b"v1", b"k1", 10);
        must_prewrite_put(&mut engine, b"k2", b"v2", b"k2", 11);

        let modifies = run_resolve_lock_lite(
            &mut engine,
            10,
            0,
            vec![Key::from_raw(b"k1"), Key::from_raw(b"k2")],
        );
        assert_no_duplicate_modify(&modifies);
        assert_eq!(count_writes(&modifies, CF_WRITE), 2);
    }

    fn count_writes(modifies: &[Modify], cf_name: &str) -> usize {
        modifies
            .iter()
            .filter(|m| match m {
                Modify::Put(cf, ..) | Modify::Delete(cf, _) => *cf == cf_name,
                _ => false,
            })
            .count()
    }

    fn assert_no_duplicate_modify(modifies: &[Modify]) {
        let mut seen = HashSet::new();
        for m in modifies {
            if let Modify::Put(cf, key, _) | Modify::Delete(cf, key) = m {
                assert!(
                    seen.insert((*cf, key.clone())),
                    "key {:?} in {} modified twice by one command",
                    key,
                    cf
                );
            }
        }
    }
}
