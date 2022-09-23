// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::sync::Arc;

use api_version::{match_template_api_version, KvFormat, RawValue};
use causal_ts::CausalTsProviderImpl;
use engine_traits::{raw_ttl::ttl_to_expire_ts, CfName};
use futures::executor::block_on;
use kvproto::kvrpcpb::ApiVersion;
use raw::RawStore;
use tikv_kv::{SnapshotExt, Statistics};
use txn_types::{Key, TimeStamp, Value};

use crate::storage::{
    get_causal_ts, get_raw_key_guard,
    kv::{Modify, WriteData},
    lock_manager::LockManager,
    raw,
    txn::{
        commands::{
            Command, CommandExt, ResponsePolicy, TypedCommand, WriteCommand, WriteContext,
            WriteResult,
        },
        ErrorInner, Result,
    },
    Error as StorageError, ProcessResult, Snapshot,
};

// TODO: consider add `KvFormat` generic parameter.
command! {
    /// RawCompareAndSwap checks whether the previous value of the key equals to the given value.
    /// If they are equal, write the new value. The bool indicates whether they are equal.
    /// The previous value is always returned regardless of whether the new value is set.
    RawCompareAndSwap:
        cmd_ty => (Option<Value>, bool),
        display => "kv::command::raw_compare_and_swap {:?}", (ctx),
        content => {
            cf: CfName,
            key: Key,
            previous_value: Option<Value>,
            value: Value,
            ttl: u64,
            api_version: ApiVersion,
            causal_ts_provider: Option<Arc<CausalTsProviderImpl>>,
        }
}

impl CommandExt for RawCompareAndSwap {
    ctx!();
    tag!(raw_compare_and_swap);
    gen_lock!(key);

    fn write_bytes(&self) -> usize {
        self.key.as_encoded().len() + self.value.len()
    }
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for RawCompareAndSwap {
    fn process_write(self, snapshot: S, wctx: WriteContext<'_, L>) -> Result<WriteResult> {
        if !snapshot.ext().is_max_ts_synced() {
            return Err(ErrorInner::MaxTimestampNotSynced {
                region_id: self.ctx.get_region_id(),
                start_ts: TimeStamp::zero(),
            }
            .into());
        }

        let provider = self.causal_ts_provider.clone();
        let concurrency_manager = wctx.concurrency_manager;
        let (cf, mut key, value, previous_value, ctx) =
            (self.cf, self.key, self.value, self.previous_value, self.ctx);
        let mut data = vec![];
        let old_value = RawStore::new(snapshot, self.api_version).raw_get_key_value(
            cf,
            &key,
            &mut Statistics::default(),
        )?;

        let (pr, lock_guards) = if old_value == previous_value {
            let raw_value = RawValue {
                user_value: value,
                expire_ts: ttl_to_expire_ts(self.ttl),
                is_delete: false,
            };
            let encoded_raw_value = match_template_api_version!(
                API,
                match self.api_version {
                    ApiVersion::API => API::encode_raw_value_owned(raw_value),
                }
            );

            let lock_guard = block_on(get_raw_key_guard(&provider, concurrency_manager)).map_err(
                |err: StorageError| ErrorInner::Other(box_err!("failed to key guard: {:?}", err)),
            )?;
            let lock_guards = if let Some(lock_guard) = lock_guard {
                vec![lock_guard]
            } else {
                vec![]
            };
            let ts = block_on(get_causal_ts(&provider)).map_err(|err: StorageError| {
                ErrorInner::Other(box_err!("failed to get casual ts: {:?}", err))
            })?;
            if let Some(ts) = ts {
                key = key.append_ts(ts);
            }

            let m = Modify::Put(cf, key, encoded_raw_value);
            data.push(m);
            (
                ProcessResult::RawCompareAndSwapRes {
                    previous_value: old_value,
                    succeed: true,
                },
                lock_guards,
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
        fail_point!("txn_commands_compare_and_swap");
        let rows = data.len();
        let mut to_be_write = WriteData::from_modifies(data);
        to_be_write.set_allowed_on_disk_almost_full();
        Ok(WriteResult {
            ctx,
            to_be_write,
            rows,
            pr,
            lock_info: None,
            lock_guards,
            response_policy: ResponsePolicy::OnApplied,
        })
    }
}

#[cfg(test)]
mod tests {
    use api_version::test_kv_format_impl;
    use concurrency_manager::ConcurrencyManager;
    use engine_traits::CF_DEFAULT;
    use kvproto::kvrpcpb::Context;

    use super::*;
    use crate::storage::{lock_manager::DummyLockManager, Engine, Statistics, TestEngineBuilder};

    #[test]
    fn test_cas_basic() {
        test_kv_format_impl!(test_cas_basic_impl);
    }

    /// Note: for API V2, TestEngine don't support MVCC reading, so
    /// `pre_propose` observer is ignored, and no timestamp will be append
    /// to key. The full test of `RawCompareAndSwap` is in
    /// `src/storage/mod.rs`.
    fn test_cas_basic_impl<F: KvFormat>() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let cm = concurrency_manager::ConcurrencyManager::new(1.into());
        let key = b"rk";

        let encoded_key = F::encode_raw_key(key, None);
        let ts_provider = if F::TAG == kvproto::kvrpcpb::ApiVersion::V2 {
            let test_provider: causal_ts::CausalTsProviderImpl =
                causal_ts::tests::TestProvider::default().into();
            Some(Arc::new(test_provider))
        } else {
            None
        };

        let cmd = RawCompareAndSwap::new(
            CF_DEFAULT,
            encoded_key.clone(),
            None,
            b"v1".to_vec(),
            0,
            F::TAG,
            ts_provider.clone(),
            Context::default(),
        );
        let (prev_val, succeed) = sched_command(&engine, cm.clone(), cmd).unwrap();
        assert!(prev_val.is_none());
        assert!(succeed);

        let cmd = RawCompareAndSwap::new(
            CF_DEFAULT,
            encoded_key.clone(),
            None,
            b"v2".to_vec(),
            1,
            F::TAG,
            ts_provider.clone(),
            Context::default(),
        );
        let (prev_val, succeed) = sched_command(&engine, cm.clone(), cmd).unwrap();
        assert_eq!(prev_val, Some(b"v1".to_vec()));
        assert!(!succeed);

        let cmd = RawCompareAndSwap::new(
            CF_DEFAULT,
            encoded_key,
            Some(b"v1".to_vec()),
            b"v3".to_vec(),
            2,
            F::TAG,
            ts_provider,
            Context::default(),
        );
        let (prev_val, succeed) = sched_command(&engine, cm, cmd).unwrap();
        assert_eq!(prev_val, Some(b"v1".to_vec()));
        assert!(succeed);
    }

    pub fn sched_command<E: Engine>(
        engine: &E,
        cm: ConcurrencyManager,
        cmd: TypedCommand<(Option<Value>, bool)>,
    ) -> Result<(Option<Value>, bool)> {
        let snap = engine.snapshot(Default::default())?;
        use kvproto::kvrpcpb::ExtraOp;
        let mut statistic = Statistics::default();
        let context = WriteContext {
            lock_mgr: &DummyLockManager {},
            concurrency_manager: cm,
            extra_op: ExtraOp::Noop,
            statistics: &mut statistic,
            async_apply_prewrite: false,
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
    fn test_cas_process_write() {
        test_kv_format_impl!(test_cas_process_write_impl);
    }

    fn test_cas_process_write_impl<F: KvFormat>() {
        let ts_provider = if F::TAG == kvproto::kvrpcpb::ApiVersion::V2 {
            let test_provider: causal_ts::CausalTsProviderImpl =
                causal_ts::tests::TestProvider::default().into();
            Some(Arc::new(test_provider))
        } else {
            None
        };
        let engine = TestEngineBuilder::new().build().unwrap();
        let cm = concurrency_manager::ConcurrencyManager::new(1.into());
        let raw_key = b"rk";
        let raw_value = b"valuek";
        let ttl = 30;
        let encode_value = RawValue {
            user_value: raw_value.to_vec(),
            expire_ts: ttl_to_expire_ts(ttl),
            is_delete: false,
        };
        let cmd = RawCompareAndSwap::new(
            CF_DEFAULT,
            F::encode_raw_key(raw_key, None),
            None,
            raw_value.to_vec(),
            ttl,
            F::TAG,
            ts_provider,
            Context::default(),
        );
        let mut statistic = Statistics::default();
        let snap = engine.snapshot(Default::default()).unwrap();
        let context = WriteContext {
            lock_mgr: &DummyLockManager {},
            concurrency_manager: cm,
            extra_op: kvproto::kvrpcpb::ExtraOp::Noop,
            statistics: &mut statistic,
            async_apply_prewrite: false,
        };
        let cmd: Command = cmd.into();
        let write_result = cmd.process_write(snap, context).unwrap();
        let modifies_with_ts = vec![Modify::Put(
            CF_DEFAULT,
            F::encode_raw_key(raw_key, Some(101.into())),
            F::encode_raw_value_owned(encode_value),
        )];
        assert_eq!(write_result.to_be_write.modifies, modifies_with_ts)
    }
}
