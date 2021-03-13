// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::kv::{Modify, WriteData};
use crate::storage::lock_manager::LockManager;
use crate::storage::txn::commands::{
    Command, CommandExt, ResponsePolicy, TypedCommand, WriteCommand, WriteContext, WriteResult,
};
use crate::storage::txn::Result;
use crate::storage::{ProcessResult, Snapshot};
use engine_traits::CfName;
use txn_types::Mutation;

command! {
    /// Run Put or Delete for keys which may be changed by `AtomicCompareAndSet`.
    AtomicStore:
        cmd_ty => (),
        display => "kv::command::atomic_store {:?}", (ctx),
        content => {
            /// The set of mutations to apply.
            cf: CfName,
            mutations: Vec<Mutation>,
        }
}

impl CommandExt for AtomicStore {
    ctx!();
    tag!(raw_atomic_store);
    gen_lock!(mutations: multiple(|x| x.key()));

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
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for AtomicStore {
    fn process_write(self, _: S, _: WriteContext<'_, L>) -> Result<WriteResult> {
        let mut data = vec![];
        let rows = self.mutations.len();
        let (cf, mutations, ctx) = (self.cf, self.mutations, self.ctx);
        for m in mutations {
            match m {
                Mutation::Put((key, value)) => {
                    data.push(Modify::Put(cf, key, value));
                }
                Mutation::Delete(key) => {
                    data.push(Modify::Delete(cf, key));
                }
                _ => panic!("Not support mutation type"),
            }
        }
        let to_be_write = WriteData::from_modifies(data);
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
