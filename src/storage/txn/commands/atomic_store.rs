// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use crate::storage::kv::{Modify, WriteData};
use crate::storage::lock_manager::LockManager;
use crate::storage::txn::commands::{
    Command, CommandExt, ResponsePolicy, TypedCommand, WriteCommand, WriteContext, WriteResult,
};
use crate::storage::txn::Result;
use crate::storage::{ProcessResult, Snapshot};
use api_version::{match_template_api_version, APIVersion, RawValue};
use engine_traits::raw_ttl::ttl_to_expire_ts;
use engine_traits::CfName;
use kvproto::kvrpcpb::ApiVersion;
use txn_types::RawMutation;

command! {
    /// Run Put or Delete for keys which may be changed by `RawCompareAndSwap`.
    RawAtomicStore:
        cmd_ty => (),
        display => "kv::command::atomic_store {:?}", (ctx),
        content => {
            /// The set of mutations to apply.
            cf: CfName,
            mutations: Vec<RawMutation>,
            api_version: ApiVersion,
        }
}

impl CommandExt for RawAtomicStore {
    ctx!();
    tag!(raw_atomic_store);
    gen_lock!(mutations: multiple(|x| x.key()));

    fn write_bytes(&self) -> usize {
        let mut bytes = 0;
        for m in &self.mutations {
            match *m {
                RawMutation::Put {
                    ref key,
                    ref value,
                    ttl: _,
                } => {
                    bytes += key.as_encoded().len();
                    bytes += value.len();
                }
                RawMutation::Delete { ref key } => {
                    bytes += key.as_encoded().len();
                }
            }
        }
        bytes
    }
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for RawAtomicStore {
    fn process_write(self, _: S, _: WriteContext<'_, L>) -> Result<WriteResult> {
        let mut data = vec![];
        let rows = self.mutations.len();
        let (cf, mutations, ctx) = (self.cf, self.mutations, self.ctx);
        match_template_api_version!(
            API,
            match self.api_version {
                ApiVersion::API => {
                    for m in mutations {
                        match m {
                            RawMutation::Put { key, value, ttl } => {
                                let raw_value = RawValue {
                                    user_value: value,
                                    expire_ts: ttl_to_expire_ts(ttl),
                                };
                                let m =
                                    Modify::Put(cf, key, API::encode_raw_value_owned(raw_value));
                                data.push(m);
                            }
                            RawMutation::Delete { key } => {
                                data.push(Modify::Delete(cf, key));
                            }
                        }
                    }
                }
            }
        );
        let mut to_be_write = WriteData::from_modifies(data);
        to_be_write.set_allowed_on_disk_almost_full();
        Ok(WriteResult {
            ctx,
            to_be_write,
            rows,
            pr: ProcessResult::Res,
            lock_info: None,
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
            known_txn_status: vec![],
        })
    }
}
<<<<<<< HEAD
=======

#[cfg(test)]
mod tests {
    use api_version::{test_kv_format_impl, ApiV2, KvFormat, RawValue};
    use engine_traits::CF_DEFAULT;
    use futures::executor::block_on;
    use kvproto::kvrpcpb::{ApiVersion, Context};
    use tikv_kv::Engine;

    use super::*;
    use crate::storage::{
        lock_manager::MockLockManager,
        txn::{scheduler::get_raw_ext, txn_status_cache::TxnStatusCache},
        Statistics, TestEngineBuilder,
    };

    #[test]
    fn test_atomic_process_write() {
        test_kv_format_impl!(test_atomic_process_write_impl);
    }

    fn test_atomic_process_write_impl<F: KvFormat>() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let cm = concurrency_manager::ConcurrencyManager::new(1.into());
        let raw_keys = vec![b"ra", b"rz"];
        let raw_values = vec![b"valuea", b"valuez"];
        let ts_provider = super::super::test_util::gen_ts_provider(F::TAG);

        let mut modifies = vec![];
        for i in 0..raw_keys.len() {
            let raw_value = RawValue {
                user_value: raw_values[i].to_vec(),
                expire_ts: Some(u64::MAX),
                is_delete: false,
            };
            modifies.push(Modify::Put(
                CF_DEFAULT,
                F::encode_raw_key_owned(raw_keys[i].to_vec(), None),
                F::encode_raw_value_owned(raw_value),
            ));
        }
        let cmd = RawAtomicStore::new(CF_DEFAULT, modifies, Context::default());
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
            txn_status_cache: &TxnStatusCache::new_for_test(),
        };
        let cmd: Command = cmd.into();
        let write_result = cmd.process_write(snap, context).unwrap();
        let mut modifies_with_ts = vec![];
        for i in 0..raw_keys.len() {
            let raw_value = RawValue {
                user_value: raw_values[i].to_vec(),
                expire_ts: Some(u64::MAX),
                is_delete: false,
            };
            modifies_with_ts.push(Modify::Put(
                CF_DEFAULT,
                F::encode_raw_key_owned(raw_keys[i].to_vec(), Some(101.into())),
                F::encode_raw_value_owned(raw_value),
            ));
        }
        assert_eq!(write_result.to_be_write.modifies, modifies_with_ts);
        if F::TAG == ApiVersion::V2 {
            assert_eq!(write_result.lock_guards.len(), 1);
            let raw_key = vec![api_version::api_v2::RAW_KEY_PREFIX];
            let encoded_key = ApiV2::encode_raw_key(&raw_key, Some(100.into()));
            assert_eq!(
                write_result.lock_guards.first().unwrap().key(),
                &encoded_key
            );
        }
    }
}
>>>>>>> 0a34c6f479 (txn: Fix to the prewrite requests retry problem by using TxnStatusCache (#15658))
