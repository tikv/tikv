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
        })
    }
}
