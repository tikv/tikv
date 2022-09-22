// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::sync::Arc;

use causal_ts::CausalTs;
use engine_traits::CfName;
use futures::executor::block_on;
use tikv_kv::SnapshotExt;
use txn_types::TimeStamp;

use crate::storage::{
    get_causal_ts, get_raw_key_guard,
    kv::{Modify, WriteData},
    lock_manager::LockManager,
    txn::{
        commands::{
            Command, CommandExt, ResponsePolicy, TypedCommand, WriteCommand, WriteContext,
            WriteResult,
        },
        ErrorInner, Result,
    },
    Error as StorageError, ProcessResult, Snapshot,
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
            causal_ts_provider: Option<Arc<CausalTs>>,
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
    fn process_write(self, snapshot: S, wctx: WriteContext<'_, L>) -> Result<WriteResult> {
        if !snapshot.ext().is_max_ts_synced() {
            return Err(ErrorInner::MaxTimestampNotSynced {
                region_id: self.ctx.get_region_id(),
                start_ts: TimeStamp::zero(),
            }
            .into());
        }

        let provider = self.causal_ts_provider.clone();
        let concurrency_manager = wctx.concurrency_manager.clone();
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

        let rows = self.mutations.len();
        let (mut mutations, ctx) = (self.mutations, self.ctx);
        if let Some(ts) = ts {
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
            lock_guards,
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
<<<<<<< HEAD
        let ts_provider = if F::TAG == kvproto::kvrpcpb::ApiVersion::V2 {
            let test_provider: causal_ts::CausalTs =
                causal_ts::tests::TestProvider::default().into();
            Some(Arc::new(test_provider))
=======
        let encode_ts = if F::TAG == kvproto::kvrpcpb::ApiVersion::V2 {
            Some(TimeStamp::from(100))
>>>>>>> async-causal-ts
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
<<<<<<< HEAD
        let cmd = RawAtomicStore::new(CF_DEFAULT, modifies, ts_provider, Context::default());
=======
        let cmd = RawAtomicStore::new(CF_DEFAULT, modifies, encode_ts, Context::default());
>>>>>>> async-causal-ts
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
<<<<<<< HEAD
                F::encode_raw_key_owned(raw_keys[i].to_vec(), Some(100.into())),
=======
                F::encode_raw_key_owned(raw_keys[i].to_vec(), encode_ts),
>>>>>>> async-causal-ts
                F::encode_raw_value_owned(raw_value),
            ));
        }
        assert_eq!(write_result.to_be_write.modifies, modifies_with_ts)
    }
}
