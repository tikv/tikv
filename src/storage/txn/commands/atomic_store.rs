// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use engine_traits::CfName;
use txn_types::TimeStamp;

use crate::storage::{
    kv::{Modify, WriteData},
    lock_manager::LockManager,
    txn::{
        commands::{
            Command, CommandExt, ResponsePolicy, TypedCommand, WriteCommand, WriteContext,
            WriteResult,
        },
        Result,
    },
    ProcessResult, Snapshot,
};

command! {
    /// Run Put or Delete for keys which may be changed by `RawCompareAndSwap`.
    RawAtomicStore:
        cmd_ty => (),
        display => "kv::command::atomic_store {:?}", (ctx),
        content => {
            /// The set of mutations to apply.
            cf: CfName,
            mutations: Vec<Modify>,
            data_ts: Option<TimeStamp>,
        }
}

impl CommandExt for RawAtomicStore {
    ctx!();
    tag!(raw_atomic_store);
    gen_lock!(mutations: multiple(|x| x.key()));

    fn write_bytes(&self) -> usize {
        self.mutations.iter().map(|x| x.size()).sum()
    }
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for RawAtomicStore {
    fn process_write(self, _: S, _: WriteContext<'_, L>) -> Result<WriteResult> {
        let rows = self.mutations.len();
        let (mut mutations, ctx) = (self.mutations, self.ctx);
        if let Some(ts) = self.data_ts {
            for mutation in &mut mutations {
                if let Modify::Put(_, ref mut key, _) = mutation {
                    key.append_ts_inplace(ts);
                }
            }
        };
        let mut to_be_write = WriteData::from_modifies(mutations);
        to_be_write.set_allowed_on_disk_almost_full();
        Ok(WriteResult {
            ctx,
            to_be_write,
            rows,
            pr: ProcessResult::Res,
            lock_info: None,
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
        })
    }
}

#[cfg(test)]
mod tests {
    use api_version::{test_kv_format_impl, KvFormat, RawValue};
    use engine_traits::CF_DEFAULT;
    use kvproto::kvrpcpb::Context;
    use tikv_kv::Engine;

    use super::*;
    use crate::storage::{lock_manager::DummyLockManager, Statistics, TestEngineBuilder};

    #[test]
    fn test_atomic_process_write() {
        test_kv_format_impl!(test_atomic_process_write_impl);
    }

    fn test_atomic_process_write_impl<F: KvFormat>() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let cm = concurrency_manager::ConcurrencyManager::new(1.into());
        let raw_keys = vec![b"ra", b"rz"];
        let raw_values = vec![b"valuea", b"valuez"];
        let encode_ts = if F::TAG == kvproto::kvrpcpb::ApiVersion::V2 {
            Some(TimeStamp::from(100))
        } else {
            None
        };
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
        let cmd = RawAtomicStore::new(CF_DEFAULT, modifies, encode_ts, Context::default());
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
        let mut modifies_with_ts = vec![];
        for i in 0..raw_keys.len() {
            let raw_value = RawValue {
                user_value: raw_values[i].to_vec(),
                expire_ts: Some(u64::MAX),
                is_delete: false,
            };
            modifies_with_ts.push(Modify::Put(
                CF_DEFAULT,
                F::encode_raw_key_owned(raw_keys[i].to_vec(), encode_ts),
                F::encode_raw_value_owned(raw_value),
            ));
        }
        assert_eq!(write_result.to_be_write.modifies, modifies_with_ts)
    }
}
