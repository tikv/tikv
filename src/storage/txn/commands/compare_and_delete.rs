// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use api_version::ApiV2;
use engine_traits::CfName;
use kvproto::kvrpcpb::ApiVersion;
use raw::RawStore;
use tikv_kv::Statistics;
use txn_types::{Key, Value};

use crate::storage::{
    ProcessResult, Snapshot,
    kv::{Modify, WriteData},
    lock_manager::LockManager,
    raw,
    txn::{
        Result,
        commands::{
            Command, CommandExt, ReleasedLocks, ResponsePolicy, TypedCommand, WriteCommand,
            WriteContext, WriteResult,
        },
    },
};

command! {
    /// RawCompareAndDelete checks whether the previous value of the key equals to the given value.
    /// If they are equal, delete the key. The bool indicates whether they are equal.
    /// The previous value is always returned regardless of whether the key is deleted.
    RawCompareAndDelete:
        cmd_ty => (Option<Value>, bool),
        display => { "kv::command::raw_compare_and_delete {:?}", (ctx), }
        content => {
            cf: CfName,
            key: Key,
            previous_value: Value,
            api_version: ApiVersion,
        }
        in_heap => {
            key,
            previous_value,
        }
}

impl CommandExt for RawCompareAndDelete {
    ctx!();
    tag!(raw_compare_and_delete);
    gen_lock!(key);

    // Note: the size returned is an underestimate of the actual bytes written for
    // API V2, since the key is appended with a timestamp. This underestimation
    // is consistent with the other write_bytes() implementations (e.g
    // atomic_store.rs and compare_and_swap.rs). If this needs fixing it should
    // be fixed everywhere for consistency.
    fn write_bytes(&self) -> usize {
        if self.api_version == ApiVersion::V2 {
            self.key.as_encoded().len() + ApiV2::ENCODED_LOGICAL_DELETE.len()
        } else {
            self.key.as_encoded().len()
        }
    }
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for RawCompareAndDelete {
    fn process_write(self, snapshot: S, wctx: WriteContext<'_, L>) -> Result<WriteResult> {
        let (cf, mut key, previous_value, ctx, raw_ext) = (
            self.cf,
            self.key,
            self.previous_value,
            self.ctx,
            wctx.raw_ext,
        );

        let mut data = vec![];
        let old_value = RawStore::new(snapshot, self.api_version).raw_get_key_value(
            cf,
            &key,
            &mut Statistics::default(),
        )?;

        let (pr, lock_guards) = if old_value.as_ref() == Some(&previous_value) {
            if let Some(ref raw_ext) = raw_ext {
                key = key.append_ts(raw_ext.ts);
            }

            let m = match self.api_version {
                ApiVersion::V2 => Modify::Put(cf, key, ApiV2::ENCODED_LOGICAL_DELETE.to_vec()),
                _ => Modify::Delete(cf, key),
            };
            data.push(m);
            (
                ProcessResult::RawCompareAndSwapRes {
                    previous_value: old_value,
                    succeed: true,
                },
                raw_ext.into_iter().map(|r| r.key_guard).collect(),
            )
        } else {
            (
                ProcessResult::RawCompareAndSwapRes {
                    previous_value: old_value,
                    succeed: false,
                },
                vec![],
            )
        };
        fail_point!("txn_commands_compare_and_delete");
        let rows = data.len();
        let mut to_be_write = WriteData::from_modifies(data);
        to_be_write.set_allowed_on_disk_almost_full();
        Ok(WriteResult {
            ctx,
            to_be_write,
            rows,
            pr,
            lock_info: vec![],
            released_locks: ReleasedLocks::new(),
            new_acquired_locks: vec![],
            lock_guards,
            response_policy: ResponsePolicy::OnApplied,
            known_txn_status: vec![],
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api_version::{ApiV2, KvFormat, test_kv_format_impl};
    use causal_ts::CausalTsProviderImpl;
    use concurrency_manager::ConcurrencyManager;
    use engine_traits::CF_DEFAULT;
    use futures::executor::block_on;
    use kvproto::kvrpcpb::Context;
    use txn_types::Value;

    use super::*;
    use crate::storage::{
        Engine, Statistics, TestEngineBuilder,
        lock_manager::MockLockManager,
        txn::{scheduler::get_raw_ext, txn_status_cache::TxnStatusCache},
    };

    fn sched_cad_command<E: Engine>(
        engine: &mut E,
        cm: ConcurrencyManager,
        cmd: TypedCommand<(Option<Value>, bool)>,
        ts_provider: Option<Arc<CausalTsProviderImpl>>,
    ) -> Result<(Option<Value>, bool)> {
        sched_compare_command(engine, cm, cmd, ts_provider)
    }

    fn sched_cas_command<E: Engine>(
        engine: &mut E,
        cm: ConcurrencyManager,
        cmd: TypedCommand<(Option<Value>, bool)>,
        ts_provider: Option<Arc<CausalTsProviderImpl>>,
    ) -> Result<(Option<Value>, bool)> {
        sched_compare_command(engine, cm, cmd, ts_provider)
    }

    fn sched_compare_command<E: Engine>(
        engine: &mut E,
        cm: ConcurrencyManager,
        cmd: TypedCommand<(Option<Value>, bool)>,
        ts_provider: Option<Arc<CausalTsProviderImpl>>,
    ) -> Result<(Option<Value>, bool)> {
        let snap = engine.snapshot(Default::default())?;
        use kvproto::kvrpcpb::ExtraOp;
        let mut statistic = Statistics::default();

        let raw_ext = block_on(get_raw_ext(ts_provider, cm.clone(), true, &cmd.cmd)).unwrap();
        let context = WriteContext {
            lock_mgr: &MockLockManager::new(),
            concurrency_manager: cm,
            extra_op: ExtraOp::Noop,
            statistics: &mut statistic,
            async_apply_prewrite: false,
            raw_ext,
            txn_status_cache: Arc::new(TxnStatusCache::new_for_test()),
        };
        let ret = cmd.cmd.process_write(snap, context)?;
        match ret.pr {
            ProcessResult::RawCompareAndSwapRes {
                previous_value,
                succeed,
            } => {
                if succeed {
                    let ctx = Context::default();
                    engine.write(&ctx, ret.to_be_write).unwrap();
                }
                Ok((previous_value, succeed))
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_cad_basic() {
        test_kv_format_impl!(test_cad_basic_impl);
    }

    /// Note: for API V2, TestEngine don't support MVCC reading, so
    /// `pre_propose` observer is ignored, and no timestamp will be appended
    /// to key. The full test of `RawCompareAndSwap` is in
    /// `src/storage/mod.rs`.
    fn test_cad_basic_impl<F: KvFormat>() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let ts_provider = super::super::test_util::gen_ts_provider(F::TAG);
        let cm = concurrency_manager::ConcurrencyManager::new_for_test(1.into());
        let key = b"rk";

        let encoded_key = F::encode_raw_key(key, None);

        // First, insert a value using CAS (set key to "v1").
        let cas_cmd = super::super::RawCompareAndSwap::new(
            CF_DEFAULT,
            encoded_key.clone(),
            None,
            b"v1".to_vec(),
            0,
            F::TAG,
            Context::default(),
        );
        let (prev_val, succeed) =
            sched_cas_command(&mut engine, cm.clone(), cas_cmd, ts_provider.clone()).unwrap();
        assert!(prev_val.is_none());
        assert!(succeed);

        // Attempt to delete with wrong previous_value — should fail.
        let cmd = RawCompareAndDelete::new(
            CF_DEFAULT,
            encoded_key.clone(),
            b"v_wrong".to_vec(),
            F::TAG,
            Context::default(),
        );
        let (prev_val, succeed) =
            sched_cad_command(&mut engine, cm.clone(), cmd, ts_provider.clone()).unwrap();
        assert_eq!(prev_val, Some(b"v1".to_vec()));
        assert!(!succeed);

        // Delete with correct previous_value — should succeed.
        let cmd = RawCompareAndDelete::new(
            CF_DEFAULT,
            encoded_key.clone(),
            b"v1".to_vec(),
            F::TAG,
            Context::default(),
        );
        let (prev_val, succeed) =
            sched_cad_command(&mut engine, cm.clone(), cmd, ts_provider.clone()).unwrap();
        assert_eq!(prev_val, Some(b"v1".to_vec()));
        assert!(succeed);

        // Attempt to delete again — key no longer exists, should fail.
        let cmd = RawCompareAndDelete::new(
            CF_DEFAULT,
            encoded_key.clone(),
            b"v1".to_vec(),
            F::TAG,
            Context::default(),
        );
        let (prev_val, succeed) =
            sched_cad_command(&mut engine, cm.clone(), cmd, ts_provider.clone()).unwrap();
        assert!(prev_val.is_none());
        assert!(!succeed);

        // Insert a key with an empty value
        let cas_cmd = super::super::RawCompareAndSwap::new(
            CF_DEFAULT,
            encoded_key.clone(),
            None,
            b"".to_vec(),
            0,
            F::TAG,
            Context::default(),
        );
        let (prev_val, succeed) =
            sched_cas_command(&mut engine, cm.clone(), cas_cmd, ts_provider.clone()).unwrap();
        assert!(prev_val.is_none());
        assert!(succeed);

        // Attempt to delete with correct previous_value — should succeed.
        let cmd = RawCompareAndDelete::new(
            CF_DEFAULT,
            encoded_key.clone(),
            b"".to_vec(),
            F::TAG,
            Context::default(),
        );
        let (prev_val, succeed) =
            sched_cad_command(&mut engine, cm.clone(), cmd, ts_provider.clone()).unwrap();
        assert_eq!(prev_val, Some(b"".to_vec()));
        assert!(succeed);
    }

    #[test]
    fn test_cad_process_write() {
        test_kv_format_impl!(test_cad_process_write_impl);
    }

    /// Tests that `process_write` produces the correct low-level `Modify` and
    /// lock-guard output for `RawCompareAndDelete`.
    ///
    /// Specifically:
    /// - For V1/V1Ttl: a `Modify::Delete` at the plain (un-timestamped) key.
    /// - For V2: a `Modify::Put` at `key@ts` containing
    ///   `ENCODED_LOGICAL_DELETE`, plus exactly one lock guard at
    ///   `RAW_KEY_PREFIX@key_guard_ts`.
    fn test_cad_process_write_impl<F: KvFormat>() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let ts_provider = super::super::test_util::gen_ts_provider(F::TAG);
        let cm = concurrency_manager::ConcurrencyManager::new_for_test(1.into());
        let raw_key = b"rk";
        let raw_value = b"valuek";

        // Write a value into the engine so there is something to compare against.
        // For V2 this consumes ts 100 (key_guard) and 101 (write ts), so the
        // subsequent CAD call will use ts 102 (key_guard) and 103 (write ts).
        let cas_cmd = super::super::RawCompareAndSwap::new(
            CF_DEFAULT,
            F::encode_raw_key(raw_key, None),
            None,
            raw_value.to_vec(),
            0,
            F::TAG,
            Context::default(),
        );
        sched_cas_command(&mut engine, cm.clone(), cas_cmd, ts_provider.clone()).unwrap();

        // Now invoke process_write directly for the CAD command.
        let cmd = RawCompareAndDelete::new(
            CF_DEFAULT,
            F::encode_raw_key(raw_key, None),
            raw_value.to_vec(),
            F::TAG,
            Context::default(),
        );
        let mut statistic = Statistics::default();
        let snap = engine.snapshot(Default::default()).unwrap();
        let raw_ext = block_on(get_raw_ext(ts_provider, cm.clone(), true, &cmd.cmd)).unwrap();
        let context = WriteContext {
            lock_mgr: &MockLockManager::new(),
            concurrency_manager: cm,
            extra_op: kvproto::kvrpcpb::ExtraOp::Noop,
            statistics: &mut statistic,
            async_apply_prewrite: false,
            raw_ext,
            txn_status_cache: Arc::new(TxnStatusCache::new_for_test()),
        };
        let cmd: Command = cmd.into();
        let write_result = cmd.process_write(snap, context).unwrap();

        if F::TAG == ApiVersion::V2 {
            // V2: logical-delete tombstone written as a Put at key@write_ts (103).
            let expected_modifies = vec![Modify::Put(
                CF_DEFAULT,
                F::encode_raw_key(raw_key, Some(103.into())),
                ApiV2::ENCODED_LOGICAL_DELETE.to_vec(),
            )];
            assert_eq!(write_result.to_be_write.modifies, expected_modifies);
            // Exactly one lock guard acquired at key_guard_ts (102).
            assert_eq!(write_result.lock_guards.len(), 1);
            let lock_raw_key = vec![api_version::api_v2::RAW_KEY_PREFIX];
            let encoded_lock_key = ApiV2::encode_raw_key(&lock_raw_key, Some(102.into()));
            assert_eq!(
                write_result.lock_guards.first().unwrap().key(),
                &encoded_lock_key
            );
        } else {
            let expected_modifies =
                vec![Modify::Delete(CF_DEFAULT, F::encode_raw_key(raw_key, None))];
            assert_eq!(write_result.to_be_write.modifies, expected_modifies);
            assert!(write_result.lock_guards.is_empty());
        }
    }
}
