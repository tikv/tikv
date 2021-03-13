// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::kv::{Modify, WriteData};
use crate::storage::lock_manager::LockManager;
use crate::storage::raw;
use crate::storage::txn::commands::{
    Command, CommandExt, ResponsePolicy, TypedCommand, WriteCommand, WriteContext, WriteResult,
};
use crate::storage::txn::Result;
use crate::storage::{ProcessResult, Snapshot};
use engine_traits::CfName;
use txn_types::{Key, Value};

command! {
    /// CompareAndSet to check whether the previous value of the key equals to the given value.
    /// If they are equal, write the new value, otherwise return the previous value.
    AtomicCompareAndSet:
        cmd_ty => Option<Value>,
        display => "kv::command::atomic_compare_and_set {:?}", (ctx),
        content => {
            cf: CfName,
            key: Key,
            previous_value: Option<Value>,
            value: Value,
            enable_ttl: bool,
            ttl: u64,
        }
}

impl CommandExt for AtomicCompareAndSet {
    ctx!();
    tag!(raw_compare_and_set);
    gen_lock!(key);

    fn write_bytes(&self) -> usize {
        self.key.as_encoded().len() + self.value.len()
    }
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for AtomicCompareAndSet {
    fn process_write(self, snapshot: S, _: WriteContext<'_, L>) -> Result<WriteResult> {
        let (cf, key, value, previous_value, ctx) =
            (self.cf, self.key, self.value, self.previous_value, self.ctx);
        let mut data = vec![];
        let old_value = if self.enable_ttl {
            raw::TTLSnapshot::from(snapshot).get_cf(cf, &key)?
        } else {
            snapshot.get_cf(cf, &key)?
        };
        let expire_ts = if self.ttl == 0 {
            0
        } else {
            self.ttl + raw::TTLSnapshot::<S>::current_ts()
        };
        let pr = if old_value == previous_value {
            let mut m = Modify::Put(cf, key, value);
            if self.enable_ttl {
                m.with_ttl(expire_ts);
            }
            data.push(m);
            ProcessResult::CompareAndSetRes {
                previous_value: None,
            }
        } else {
            ProcessResult::CompareAndSetRes {
                previous_value: old_value,
            }
        };
        fail_point!("txn_commands_compare_and_set");
        let rows = data.len();
        let to_be_write = WriteData::from_modifies(data);
        Ok(WriteResult {
            ctx,
            to_be_write,
            rows,
            pr,
            lock_info: None,
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{Engine, Statistics, TestEngineBuilder};
    use concurrency_manager::ConcurrencyManager;
    use engine_traits::CF_DEFAULT;
    use kvproto::kvrpcpb::Context;
    use txn_types::Key;

    #[test]
    fn test_cas_basic() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let cm = concurrency_manager::ConcurrencyManager::new(1.into());
        let key = b"k";
        let cmd = AtomicCompareAndSet::new(
            CF_DEFAULT,
            Key::from_encoded(key.to_vec()),
            None,
            b"v1".to_vec(),
            false,
            0,
            Context::default(),
        );

        let ret = sched_command(&engine, cm.clone(), cmd).unwrap();
        assert!(ret.is_none());
        let cmd = AtomicCompareAndSet::new(
            CF_DEFAULT,
            Key::from_encoded(key.to_vec()),
            None,
            b"v2".to_vec(),
            false,
            0,
            Context::default(),
        );
        let ret = sched_command(&engine, cm, cmd).unwrap();
        assert!(ret.is_some());
        assert_eq!(ret.unwrap(), b"v1".to_vec());
    }

    pub fn sched_command<E: Engine>(
        engine: &E,
        cm: ConcurrencyManager,
        cmd: TypedCommand<Option<Value>>,
    ) -> Result<Option<Value>> {
        let snap = engine.snapshot(Default::default())?;
        use crate::storage::DummyLockManager;
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
            ProcessResult::CompareAndSetRes { previous_value } => {
                if previous_value.is_some() {
                    return Ok(previous_value);
                }
            }
            _ => unreachable!(),
        };
        let ctx = Context::default();
        engine.write(&ctx, ret.to_be_write).unwrap();
        Ok(None)
    }
}
